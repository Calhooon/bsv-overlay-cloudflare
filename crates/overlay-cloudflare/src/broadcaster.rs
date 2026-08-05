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
/// `(http_status, body)`. `Err` is transport-only (fetch/DNS/connect failure);
/// the HTTP verdict is the CALLER's to classify ([`arc_verdict`] for the
/// primary gate, [`corroborator_verdict`] for the #214 corroboration leg).
/// `api_key: None` posts keyless (GorillaPool).
async fn post_arc_raw(
    base_url: &str,
    api_key: Option<&str>,
    tx_hex: &str,
) -> Result<(u16, String), String> {
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
    Ok((status, text))
}

/// One raw `{ "rawTx": <hex> }` POST to an ARC-compatible `/v1/tx`, returning
/// the classified verdict. `api_key: None` posts keyless (GorillaPool).
async fn post_arc_tx(
    base_url: &str,
    api_key: Option<&str>,
    tx_hex: &str,
) -> Result<ArcOutcome, String> {
    let (status, text) = post_arc_raw(base_url, api_key, tx_hex).await?;
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
    worker::console_log!(
        "broadcast-gated: TAAL transport trouble ({taal_err}); trying GorillaPool"
    );
    match post_arc_tx(GORILLAPOOL_ARC_URL, None, tx_hex).await {
        Ok(outcome) => Ok(outcome),
        Err(gp_err) => Err(format!("taal: {taal_err}; gorillapool: {gp_err}")),
    }
}

// ============================================================================
// #214 corroboration — Arcade's REJECTED is never authoritative uncorroborated
// ============================================================================
//
// Mainnet ground truth (bsv-low #214, 2026-07-20/21): Arcade-v2-us-1's
// validator view went STALE. It async-REJECTED (`PROCESSING (4)`) txs that
// TAAL accepted as SEEN within seconds and that MINED in block 958776; the
// REJECTED verdict was STICKY (persisted ≥28 min, across 3 blocks, `GET /tx`
// still REJECTED for a 3-conf tx) and CASCADED ("parent rejected") to every
// descendant, while its /health reported healthy throughout. No timing / wait /
// co-delivery strategy can outlast that (the full-batch rung already
// co-delivers and failed every attempt), so before the exhausted ladder is
// allowed to become a DEFINITIVE 422 refusal, a SECOND independent broadcaster
// (TAAL → GorillaPool) must corroborate the rejection. The #192/#193 invariant
// is untouched: admission still requires a REAL network accept — just not
// specifically Arcade's word for it.

/// PURE (#214): STRICT accept semantics for the corroborating broadcaster.
///
/// This deliberately does NOT reuse [`arc_verdict`]'s accept arm: `arc_verdict`
/// treats ANY parseable 2xx with a non-error `txStatus` (including `RECEIVED`,
/// `STORED`, or an empty string) as `Accepted`, which is fine for a primary
/// gate that ALSO polls, but a corroborator's word overrides another
/// broadcaster's explicit REJECTED — a 200-shaped ack without a real
/// network-accept marker must NOT do that. The bar is the SAME one the primary
/// Arcade gate uses: `txStatus` rank ≥ [`ARCADE_GATE_STATUS`]
/// (`SEEN_ON_NETWORK`; ARC and Arcade share the status vocabulary), or an
/// already-known dress (the network provably HAS the tx).
///
/// Three-way contract (mirrors [`arc_verdict`]'s):
/// - `Ok(Accepted)` — a REAL network accept: `txStatus` ≥ SEEN_ON_NETWORK
///   (SEEN/SEEN_MULTIPLE/MINED/IMMUTABLE) or already-known in any dress;
/// - `Ok(Rejected)` — a definitive per-tx refusal: 2xx error `txStatus`
///   (REJECTED/DOUBLE_SPEND_ATTEMPTED/INVALID/MALFORMED) or HTTP 460–479;
/// - `Err` — transport trouble OR an INCONCLUSIVE answer (sub-SEEN status,
///   unparseable body, ORPHAN view). Inconclusive is grouped with transport on
///   purpose: it neither confirms nor refutes Arcade's rejection, and the
///   fail direction must be an honest 502 ("unavailable"), never a false 422
///   ("refused") — the client's direct-ARC fallback keeps money moving.
///
/// ORPHAN is classified INCONCLUSIVE here (unlike [`arc_verdict`], which
/// rejects it): "I cannot see the parent" is exactly the stale-view failure
/// mode this corroboration exists to catch, not a script/fee refusal of the
/// subject — an orphan answer must never CONFIRM another provider's REJECTED.
pub fn corroborator_verdict(status: u16, body: &str) -> Result<ArcOutcome, String> {
    if (200..300).contains(&status) {
        let arc_resp: ArcResponse = match serde_json::from_str(body) {
            Ok(r) => r,
            Err(e) => return Err(format!("corroborator: unparseable 2xx body: {e} — {body}")),
        };
        let upper_status = arc_resp.tx_status.to_uppercase();
        let is_orphan = arc_resp.extra_info.to_uppercase().contains("ORPHAN")
            || upper_status.contains("ORPHAN");
        if is_orphan {
            return Err(format!(
                "corroborator: orphan view (inconclusive): {} {}",
                arc_resp.tx_status, arc_resp.extra_info
            ));
        }
        let error_statuses = ["DOUBLE_SPEND_ATTEMPTED", "REJECTED", "INVALID", "MALFORMED"];
        if error_statuses.iter().any(|s| upper_status == *s) {
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
        // THE accept gate: the corroborator's own network-accept marker, the
        // same bar as the primary Arcade gate. MINED_IN_STALE_BLOCK, RECEIVED,
        // STORED, ACCEPTED_BY_NETWORK, empty and unknown statuses all rank
        // below it → inconclusive.
        if arcade_status_rank(&upper_status) >= arcade_status_rank(ARCADE_GATE_STATUS) {
            return Ok(ArcOutcome::Accepted(arc_resp.txid));
        }
        return Err(format!(
            "corroborator: 2xx without a network-accept marker (txStatus {:?}) — inconclusive",
            arc_resp.tx_status
        ));
    }
    // Non-2xx: an already-known/mined body = the network HAS the tx = accept.
    if already_known(body) {
        let txid = serde_json::from_str::<ArcResponse>(body)
            .map(|r| r.txid)
            .unwrap_or_default();
        return Ok(ArcOutcome::Accepted(txid));
    }
    if (460..480).contains(&status) {
        return Ok(ArcOutcome::Rejected(format!("ARC HTTP {status}: {body}")));
    }
    Err(format!("corroborator: ARC HTTP {status}: {body}"))
}

/// PURE (#214): fold the corroborator's word into an EXHAUSTED Arcade ladder.
/// Only ever reached via [`GateStep::AsyncRejected`] (a synchronous validation
/// failure returns from [`ladder_step`] long before exhaustion — see the
/// SyncRejected note there).
///
/// - Corroborator ACCEPTED → `Ok(Accepted(subject))` — the #192/#193 invariant
///   holds: the tx IS network-accepted, on a second broadcaster's real accept
///   marker. Arcade's stale REJECTED does not get to refuse it. We return OUR
///   subject txid, never the corroborator's echoed one (identity discipline —
///   same rule as `submit_once_and_gate`'s txid-mismatch guard).
/// - Corroborator REJECTED → `Ok(Rejected)` — two independent broadcasters
///   refused it; the 422 is genuinely definitive.
/// - Corroborator transport/inconclusive → `Err` → 502. Better an honest
///   "unavailable" (the client's direct-ARC fallback keeps money moving) than
///   a false "refused" (which the client treats as terminal, by design).
fn corroborated_exhaustion(
    corroborator: Result<ArcOutcome, String>,
    subject_txid: &str,
) -> Result<ArcOutcome, String> {
    match corroborator {
        Ok(ArcOutcome::Accepted(_)) => Ok(ArcOutcome::Accepted(subject_txid.to_string())),
        Ok(ArcOutcome::Rejected(r)) => Ok(ArcOutcome::Rejected(format!(
            "network did not accept {subject_txid}; retried; corroborated by second broadcaster: {r}"
        ))),
        Err(t) => Err(format!(
            "Arcade did not accept {subject_txid} and the corroborating broadcaster was inconclusive — not admitting, not refusing: {t}"
        )),
    }
}

/// PURE (#267): fold the corroborator's word into an Arcade ACCEPT claim for
/// a subject with UNPROVEN ancestry. The bsv-low #267 incident: a degraded
/// Arcade holding a JOIN only in its ORPHAN pool (parents 0-conf, absent from
/// its node) echoed a gate-satisfying txStatus and the overlay admitted a tx
/// the public network never received — because an ACCEPT was never
/// corroborated (#214 corroborates only rejections). So for an
/// unproven-parent subject, Arcade's accept claim is a CLAIM, not an admit:
///
/// - Corroborator ACCEPTED → `Ok(Accepted(subject))` — a second broadcaster's
///   REAL network-accept marker confirms the claim (the #192/#193 invariant
///   now holds against a degraded-accept Arcade too). Our subject txid, never
///   the corroborator's echo (same identity discipline as
///   `submit_once_and_gate`).
/// - Corroborator REJECTED → `Err` → 502, deliberately NOT a 422: the two
///   providers CONFLICT (Arcade says accepted, the corroborator says
///   refused), so neither single-provider verdict is the network's — #214's
///   own doctrine. Fail CLOSED on admission (refuse to admit) but keep the
///   refusal honest ("unavailable", retryable), never terminal.
/// - Corroborator transport/inconclusive → `Err` → 502. A degraded
///   corroborator must NEVER fall back to trusting Arcade alone — that is
///   the incident. The client's direct-ARC fallback keeps money moving.
///
/// AVAILABILITY, stated honestly: the corroborator hosts (TAAL → GorillaPool)
/// are the SAME hosts the client's direct-ARC fallback uses. An
/// unproven-parent overlay admission is therefore available only when BOTH
/// Arcade AND (TAAL ∪ GorillaPool) answer — min(Arcade, TAAL∪GP), a strict
/// availability reduction versus pre-#267. That is the correct fail-closed
/// trade, not a free one: in the window where TAAL∪GP are down, the client's
/// own fallback is down too, and the only thing an uncorroborated admit
/// could have bought is the #267 incident.
fn corroborated_accept_claim(
    corroborator: Result<ArcOutcome, String>,
    subject_txid: &str,
) -> Result<ArcOutcome, String> {
    match corroborator {
        Ok(ArcOutcome::Accepted(_)) => Ok(ArcOutcome::Accepted(subject_txid.to_string())),
        Ok(ArcOutcome::Rejected(r)) => Err(format!(
            "Arcade claims {subject_txid} accepted but the corroborating broadcaster rejected it ({r}) — conflicting single-provider verdicts; not admitting, not refusing"
        )),
        Err(t) => Err(format!(
            "Arcade claims {subject_txid} accepted but the corroborating broadcaster could not confirm it — not admitting an unproven-parent subject on one provider's word (#267): {t}"
        )),
    }
}

/// PURE (#268): fold the corroborator's word into a SUBMITTER-ASSERTED
/// "already mined" claim (`efs` empty — every tx in the BEEF, including the
/// SUBJECT, carries a bump). The bump is NEVER validated at this layer
/// (`HistoricalTxNoSpv`), so before #268 this claim admitted with ZERO
/// network contact: a fake bump on the subject itself made `beef_to_ef_batch`
/// return no legs and the gate said "already mined — skipping broadcast".
/// Now the claim must be corroborated against a real provider — the
/// subject's RAW bytes are re-broadcast (TAAL → GorillaPool): a genuinely
/// mined tx comes back "already known/mined" (= accept, idempotent); a
/// still-valid-but-unmined tx gets genuinely network-accepted (which is
/// exactly the gate's bar); a fabricated claim yields orphan/inconclusive
/// or a refusal.
///
/// - Corroborator ACCEPTED → `Ok(Accepted(subject))` (our subject txid,
///   never the corroborator's echo — the usual identity discipline).
/// - Corroborator REJECTED → `Err` → 502, deliberately NOT a 422: one
///   provider's definitive refusal contradicting the submitter's bump is
///   still ONE provider's word (#214) — refuse admission, keep it retryable.
/// - Transport/inconclusive → `Err` → 502. Never admit on the submitter's
///   unvalidated bump alone — that is the #268 hole itself.
fn corroborated_mined_claim(
    corroborator: Result<ArcOutcome, String>,
    subject_txid: &str,
) -> Result<ArcOutcome, String> {
    match corroborator {
        Ok(ArcOutcome::Accepted(_)) => Ok(ArcOutcome::Accepted(subject_txid.to_string())),
        Ok(ArcOutcome::Rejected(r)) => Err(format!(
            "submitter claims {subject_txid} already mined (bump attached) but the corroborating broadcaster rejected it ({r}) — refusing to admit an unverified mined-claim (#268); not refusing definitively"
        )),
        Err(t) => Err(format!(
            "submitter claims {subject_txid} already mined (bump attached) but the corroborating broadcaster could not confirm it — not admitting on an unvalidated bump (#268): {t}"
        )),
    }
}

/// PURE (bsv-low#268 gate LOW-M): fold the two corroborator hosts' verdicts
/// under the #214 TWO-PROVIDER bar in BOTH directions. Reached only when
/// the FIRST host did not accept (an accept short-circuits before this):
///
/// - EITHER host's genuine accept → `Accepted` (the network provably has /
///   took the tx — one real accept marker suffices, as ever);
/// - BOTH hosts definitively rejected → `Rejected` (a definitive refusal —
///   which flows to a terminal 422 via `corroborated_exhaustion` — now
///   requires two independent providers, the same bar #214 demands of a
///   rejection before it may terminate the ladder; previously ONE host's
///   460–479/REJECTED settled the corroboration alone);
/// - anything else (one-sided reject + inconclusive, double inconclusive)
///   → `Err` — an honest "unavailable" (502, retryable), never a
///   single-provider refusal.
fn fold_refuse_bar(
    first: Result<ArcOutcome, String>,
    second: Result<ArcOutcome, String>,
) -> Result<ArcOutcome, String> {
    match (first, second) {
        (Ok(ArcOutcome::Accepted(t)), _) | (_, Ok(ArcOutcome::Accepted(t))) => {
            Ok(ArcOutcome::Accepted(t))
        }
        (Ok(ArcOutcome::Rejected(a)), Ok(ArcOutcome::Rejected(b))) => Ok(ArcOutcome::Rejected(
            format!("both corroborators rejected — taal: {a}; gorillapool: {b}"),
        )),
        (Ok(ArcOutcome::Rejected(a)), Err(b)) => Err(format!(
            "one-provider rejection is not definitive (taal rejected: {a}; gorillapool inconclusive: {b})"
        )),
        (Err(a), Ok(ArcOutcome::Rejected(b))) => Err(format!(
            "one-provider rejection is not definitive (taal inconclusive: {a}; gorillapool rejected: {b})"
        )),
        (Err(a), Err(b)) => Err(format!("taal: {a}; gorillapool: {b}")),
    }
}

/// Corroborate one tx hex (the subject's EF — ARC accepts Extended Format in
/// `rawTx`) against TAAL, then GorillaPool. A genuine ACCEPT from TAAL
/// short-circuits; every other TAAL answer — INCLUDING a definitive
/// rejection (bsv-low#268 gate LOW-M) — consults GorillaPool before the
/// verdict settles, folded under the two-provider refuse bar
/// ([`fold_refuse_bar`]): a definitive `Rejected` now needs BOTH hosts'
/// word, exactly as #214 demands before a refusal may become terminal.
/// This is a REAL broadcast attempt, not a status read — deliberately: the
/// corroborator proves network acceptance by the same means the client's
/// direct-ARC fallback would, and a re-broadcast of an already-accepted tx
/// is idempotent (already-known = accept).
async fn corroborate_tx_hex(
    taal_api_key: Option<&str>,
    tx_hex: &str,
) -> Result<ArcOutcome, String> {
    let taal = match post_arc_raw(WorkerArcBroadcaster::ARC_URL, taal_api_key, tx_hex).await {
        Ok((status, body)) => corroborator_verdict(status, &body),
        Err(e) => Err(e),
    };
    if matches!(taal, Ok(ArcOutcome::Accepted(_))) {
        return taal;
    }
    worker::console_log!(
        "corroborate: TAAL did not accept ({}); consulting GorillaPool (two-provider bar)",
        match &taal {
            Ok(ArcOutcome::Rejected(r)) => format!("rejected: {r}"),
            Err(e) => format!("inconclusive: {e}"),
            Ok(ArcOutcome::Accepted(_)) => unreachable!("accept short-circuits"),
        }
    );
    let gp = match post_arc_raw(GORILLAPOOL_ARC_URL, None, tx_hex).await {
        Ok((status, body)) => corroborator_verdict(status, &body),
        Err(e) => Err(e),
    };
    fold_refuse_bar(taal, gp)
}

/// PURE (#216): corroborate a subject WITH its ancestry. Feed each ANCESTOR ef
/// to the corroborating broadcaster FIRST to prime its mempool, THEN corroborate
/// the SUBJECT — the subject's verdict is the ONLY thing that decides
/// accept/reject (identical strict semantics to [`corroborate_tx_hex`]).
///
/// WHY: during an Arcade validator outage (#214), a pre-signed refund that
/// spends a still-0-conf pot could not be corroborated — the corroborator
/// (TAAL/GorillaPool) only ever received the SUBJECT alone, so with a
/// partial/stale UTXO view it saw a "missing parent" → inconclusive → 502, and
/// the refund was un-broadcastable until Arcade recovered (bsv-low #216, the
/// stuck refund `1d65d2fe…` on 2026-07-21). Submitting the parent(s) first lets
/// a degraded broadcaster ingest the parent chain into its own mempool and
/// validate the child standalone — the corroboration analogue of the #209
/// recast-parent-then-child doctrine (btc-relay-rs `RecastParentThenChild`,
/// dHouse `provenAncestryBeef`).
///
/// The `submit_one` closure is the transport (TAAL→GorillaPool for the real
/// path; a mock in tests) — this function is the PURE control flow, so the
/// ordering ("parents primed before the subject decides") and the strict
/// "only-the-subject-verdict-decides" semantics are unit-tested natively without
/// the worker runtime.
///
/// - SUBJECT-FIRST fast path (bsv-low #272): the subject's EF is submitted
///   FIRST, alone — EF inlines every input's source script + satoshis, so a
///   HEALTHY corroborator validates it standalone and answers with a real
///   network-accept in one POST. Only when that first attempt does NOT
///   accept (inconclusive/transport — the #216 partial-UTXO-view shape — or
///   a rejection that may be a missing-parent artifact) are the ancestors
///   primed and the subject retried; the PRIMED attempt's verdict is then
///   final, which is byte-identical to the pre-#272 always-primed semantics.
///   This cut the measured JOIN-submit corroboration from N+1 serial POSTs
///   to 1 on the happy path (the 15–16 s regression that sat ON the tower's
///   15 s overlay-leg cap).
/// - Parents are primed in ANCESTRY ORDER (the caller's EF batch is already
///   parents-before-children, subject last — [`beef_to_ef_batch`]); the
///   subject is SKIPPED in the parent loop and decided last.
/// - Per-parent verdicts are IGNORED (a parent already-known / SEEN / even a
///   transport blip is fine — the submit only primes the mempool). A parent
///   submit NEVER causes an Accept, and a parent submit FAILURE never flips the
///   subject's verdict.
async fn corroborate_batch_with<F, Fut>(
    efs: &[EfTx],
    subject_txid: &str,
    mut submit_one: F,
) -> Result<ArcOutcome, String>
where
    F: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = Result<ArcOutcome, String>>,
{
    // #267 hardening: WORK BOUND, checked before any submit. Over the cap
    // the whole corroboration is INCONCLUSIVE (Err → 502, the client's
    // fallback — the same fail direction as the routes.rs 429 byte bound),
    // never a truncated prime-loop whose partial corroboration could admit.
    if efs.len() > MAX_CORROBORATION_LEGS {
        return Err(format!(
            "corroboration leg cap: {} EF legs > {MAX_CORROBORATION_LEGS} — inconclusive, refusing to corroborate (and therefore to admit)",
            efs.len()
        ));
    }

    let subject_ef = efs
        .iter()
        .find(|e| e.txid == subject_txid)
        .ok_or_else(|| format!("subject {subject_txid} not present in EF batch"))?;

    // SUBJECT-FIRST (#272): only a genuine ACCEPT short-circuits — the same
    // strict bar as everywhere ([`corroborator_verdict`]'s SEEN-or-better /
    // already-known). Anything else (inconclusive, transport, or a rejection
    // that may be a missing-parent artifact of the corroborator's own view)
    // falls through to the primed attempt, whose verdict is final.
    let first = submit_one(hex::encode(&subject_ef.ef)).await;
    if matches!(first, Ok(ArcOutcome::Accepted(_))) {
        return first;
    }

    // Prime the corroborator's mempool with each ANCESTOR — best-effort,
    // verdicts discarded (they only prime). Ancestry order is preserved; the
    // subject is skipped here and decided last.
    for ef in efs {
        if ef.txid == subject_txid {
            continue;
        }
        let _ = submit_one(hex::encode(&ef.ef)).await;
    }

    // ONLY the subject's (primed) verdict decides accept/reject — a primed
    // parent can never admit on its own (the #192/#193 invariant: admission
    // still requires a REAL network-accept marker on the SUBJECT).
    submit_one(hex::encode(&subject_ef.ef)).await
}

/// #267 hardening (review finding, DoS): bound the corroboration work.
/// [`corroborate_batch_with`] primes each ancestor with its own SERIAL POST;
/// the only pre-existing bound is routes.rs's BYTE cap (2 MB batch), which at
/// ~100 B minimal txs still admits a batch of ~20k legs — and the #267 orphan
/// shortcut makes that max-work path attacker-reachable (a fabricated-parent
/// subject is free to construct and lands on the ancestry rungs by design).
/// Real LOW ancestry runs ~8 unproven legs deep (#211 observed subjects 8+
/// ancestors deep); 32 gives 4× headroom while bounding the worst case to
/// ~32 serial corroborator POSTs (~10 s). Over the cap → inconclusive
/// Err/502 with ZERO submits, never a partial corroboration that could admit.
const MAX_CORROBORATION_LEGS: usize = 32;

/// PURE (#216/#267): does this EF batch carry UNPROVEN ancestry — legs beyond
/// the subject itself? The one signal, used two ways:
/// - #216: the exhaustion corroboration primes ancestors first
///   ([`corroborate_batch_with`]) only when there IS an ancestor to prime; a
///   single-leg batch stays subject-only ([`corroborate_tx_hex`]).
/// - #267: an Arcade ACCEPT claim is corroborated before it may admit only
///   for unproven-parent subjects ([`corroborated_accept_claim`]); a
///   proven-parent (single-EF) subject keeps the uncorroborated fast path —
///   see the risk-class note in `gate_accept_claim`.
fn has_unproven_ancestry(efs_len: usize) -> bool {
    efs_len > 1
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
///
/// #214 — **Arcade REJECTED is never authoritative uncorroborated.** On
/// 2026-07-20/21 Arcade-v2-us-1's stale validator view reported REJECTED for
/// txs that TAAL accepted in seconds and that MINED in block 958776; the
/// verdict was sticky (≥28 min, still REJECTED for a 3-conf tx) and cascaded
/// ("parent rejected") to descendants. These statuses therefore terminate a
/// GATE STEP (stop waiting), but an exhausted ladder must pass through the
/// second-broadcaster corroboration in `broadcast_efs_gated` before it may
/// become a definitive `Rejected`/422. Note also: Arcade's MINED callback
/// (/arc-ingest) will never fire for a txid its view holds at REJECTED — proof
/// completion for such txs rides the Bitails/WoC couriers (`proof_fetcher.rs`
/// ladder, which treats a non-MINED Arcade answer as merely "no proof here",
/// never as terminal).
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
    /// An ORPHAN view (`SEEN_IN_ORPHAN_MEMPOOL`, #267): Arcade holds the tx
    /// but cannot see its parents. NOT Pending (waiting cannot resolve it —
    /// Arcade orphan-and-forgets, it never re-evaluates when the parents
    /// arrive), NOT Reached (an orphan-pool residence is not a network
    /// accept — the bsv-low #267 incident admitted a JOIN the public network
    /// never held on exactly this view), NOT Fatal (the tx bytes are fine;
    /// the parents are merely missing from Arcade's view). The orphan answer
    /// means "missing parents", so the caller answers it with parents: route
    /// to the ancestry rungs (full-batch resubmit + ancestry-primed
    /// corroboration) instead of the Pending→timeout→502 spiral.
    Orphan,
    /// A non-terminal status below the target → keep waiting.
    Pending,
}

fn classify_arcade_status(status: &str, target: &str) -> GateVerdict {
    // #267: the orphan check comes FIRST — the same orphan-before-anything
    // ordering `arc_verdict`/`corroborator_verdict` use (and whose absence in
    // the TS mirror is the client half of #267). `SEEN_IN_ORPHAN_MEMPOOL`
    // contains "SEEN": any rule that consulted the text before the orphan
    // check could mistake an orphan-pool view for a network accept.
    if status.to_ascii_uppercase().contains("ORPHAN") {
        return GateVerdict::Orphan;
    }
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
/// wasm — the pure classification tests never call it. `pub(crate)`: the
/// scheduled handler's step deadlines (bsv-low#257) race against it.
pub(crate) async fn sleep_ms(ms: u64) {
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
    /// #228 (arcade#260, LIVE 2026-07-22): the ADDITIVE ARC status code some
    /// 400 bodies now carry. It is a STRUCTURED verdict field (never prose):
    ///  - `476` — the tx is NON-FINAL (nLockTime/sequence not yet satisfiable
    ///    against the node's chain view). Arcade persists NOTHING server-side
    ///    for it, so it is RETRYABLE by definition — classing it definitive
    ///    would manufacture the exact false-verdict bug class #213 fixed,
    ///    inverted.
    ///  - `466` — CONFLICT, a VIEW verdict. Verified against arcade's actual
    ///    code table (`errors/errors.go`: `StatusConflict = 466`;
    ///    `services/propagation/propagator.go classifyFailureLine`:
    ///    `TX_CONFLICTING (36) → 466`): a double-spend verdict computed from
    ///    the PEER'S mempool/UTXO view during propagation — exactly the
    ///    provably stale-able view class #214 says must never terminate
    ///    uncorroborated. It re-enters the ladder like an async rejection;
    ///    its exhausted ladder ends at the #214 corroborator, never at an
    ///    uncorroborated 422.
    ///  - `467` — TERMINAL. `StatusGeneric = 467` wraps `TX_INVALID (31)`
    ///    (script/fee/policy), computed from the tx bytes themselves —
    ///    provider-independent, the same definitive class as the structured
    ///    validation-failure body.
    ///  - ABSENT — a pre-#260 responder; classification falls through to the
    ///    `error`-field equality below, byte-for-byte unchanged (tolerance).
    #[serde(default)]
    status: Option<u16>,
}

/// The exact value of Arcade's `error` field for a definitive per-tx
/// validation failure. A whole-FIELD equality (not a prose substring).
const ARCADE_VALIDATION_FAILED_ERROR: &str = "transaction failed validation";

/// #228 (arcade#260): additive `status` code for a NON-FINAL submit — nothing
/// persisted server-side; retryable after locktime/chain-view catch-up.
const ARCADE_STATUS_NON_FINAL: u16 = 476;
/// #228 (arcade#260): additive `status` code for a CONFLICT verdict — arcade's
/// `StatusConflict` (`TX_CONFLICTING` from the propagation peer's mempool/UTXO
/// view). A VIEW verdict, not a bytes verdict: per #214 it must never terminate
/// the ladder uncorroborated (see `ArcadeSubmitError::status`).
const ARCADE_STATUS_CONFLICT: u16 = 466;
/// #228 (arcade#260): additive `status` codes that are TERMINAL per-tx
/// verdicts. 466 is deliberately NOT here — arcade's own table says it is a
/// conflict from the peer's stale-able view (the #214/#213 false-verdict
/// class), so it re-enters the ladder and corroborates on exhaustion.
const ARCADE_STATUS_TERMINAL: [u16; 1] = [467];

/// #228 (arcade#260): the stable cascade-rejection label. Arcade condemns a
/// descendant of a rejected ancestor with
/// `parent rejected (ancestor <txid>): retryable` — the prefix is a committed
/// stable API (arcade#260 kept it deliberately), and the `: retryable` hint
/// says a resubmit CAN succeed once the ancestor recovers. Unlike the
/// script/fee validation failure (provider-independent, computed from the EF
/// body alone), a cascade verdict depends on Arcade's — provably stale-able
/// (#214) — view of the ANCESTOR, so it must never terminate the ladder.
///
/// Matching bias: both halves of the committed label must be present. A false
/// positive here converts a terminal Return into a Retry, which lands at the
/// exhausted-ladder corroboration (#214) — the fail-safe direction (never a
/// fabricated success, never a false 422). A false negative keeps today's
/// behaviour. So `contains` on the stable label is safe on this money path.
const ARCADE_CASCADE_PREFIX: &str = "parent rejected (ancestor ";
const ARCADE_CASCADE_RETRYABLE_HINT: &str = "): retryable";

/// True iff `reason` carries Arcade's #260 cascade-rejection retryable label
/// (see [`ARCADE_CASCADE_PREFIX`]). Wrappers (`arcade_fatal_reason`, the
/// `error: reason` join) put text around the label, so this is containment of
/// BOTH committed halves, not a whole-field equality.
fn cascade_retryable(reason: &str) -> bool {
    reason.contains(ARCADE_CASCADE_PREFIX) && reason.contains(ARCADE_CASCADE_RETRYABLE_HINT)
}

/// Classified outcome of ONE Arcade EF submit POST (`POST /tx` | `POST /txs`).
#[derive(Debug, PartialEq, Eq)]
enum SubmitOutcome {
    /// 2xx accept-for-processing — carries the response body for status parse.
    Processing(String),
    /// A SYNCHRONOUS, DEFINITIVE per-tx rejection (#213): `HTTP 400` carrying
    /// the structured `{"error":"transaction failed validation", …}` body.
    /// Script/fee already failed — a resubmit cannot change it; admit nothing.
    SyncRejected(String),
    /// A rejection computed from the provider's OWN (provably stale-able,
    /// #214) mempool/UTXO VIEW — today the arcade#260 `status: 466` conflict
    /// verdict (`TX_CONFLICTING` seen by a propagation peer). NOT terminal:
    /// it re-enters the resubmit ladder exactly like an async rejection, and
    /// its exhausted ladder must pass the #214 corroborator before any 422.
    ViewRejected(String),
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
    /// #267: Arcade holds the subject only as an ORPHAN
    /// (`SEEN_IN_ORPHAN_MEMPOOL` on the echo/poll) — it cannot see the
    /// parents. Unlike `AsyncRejected` this is NOT ambiguous: the answer IS
    /// "missing parents", so a subject-only resubmit is pointless — the
    /// caller jumps straight to the ancestry rungs (full-batch resubmit +
    /// ancestry-primed corroboration). Never admits, never definitively
    /// rejects, on the orphan view alone.
    Orphan(String),
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
            let reason = if err.reason.is_empty() {
                err.error.clone()
            } else {
                format!("{}: {}", err.error, err.reason)
            };
            // #228 (arcade#260): the ADDITIVE structured `status` field is
            // consulted FIRST — when present it is the verdict.
            match err.status {
                // 476 NON-FINAL: nothing persisted server-side; retryable
                // after locktime/chain-view catch-up. Classing this definitive
                // would be a manufactured false verdict (the #213 class,
                // inverted) — it is TRANSPORT (429-style: the caller falls
                // back / retries; routes.rs never turns it into a 422).
                Some(ARCADE_STATUS_NON_FINAL) => {
                    return SubmitOutcome::Transport(format!(
                        "Arcade non-final (status 476, retryable — nothing persisted): {reason}"
                    ));
                }
                // 466 CONFLICT: a verdict from the peer's stale-able
                // mempool/UTXO view (arcade `StatusConflict` ←
                // `TX_CONFLICTING`), the #214/#213 class — never terminal
                // uncorroborated. Re-enters the ladder; exhaustion ends at
                // the corroborator.
                Some(ARCADE_STATUS_CONFLICT) => {
                    return SubmitOutcome::ViewRejected(format!(
                        "(status 466 conflict — provider-view verdict, corroborate) {reason}"
                    ));
                }
                // 467: terminal per-tx verdict (TX_INVALID — script/fee/policy
                // computed from the tx bytes) — same definitive class as the
                // structured validation-failure body.
                Some(s) if ARCADE_STATUS_TERMINAL.contains(&s) => {
                    return SubmitOutcome::SyncRejected(format!("(status {s}) {reason}"));
                }
                // Absent (pre-#260 responder) or an unrecognised code: fall
                // through to the pre-#260 classification, unchanged.
                _ => {}
            }
            if err.error == ARCADE_VALIDATION_FAILED_ERROR {
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
    /// #267: an ORPHAN view — skip the remaining subject-only rungs (a
    /// subject resubmit cannot supply the missing parents) and jump straight
    /// to the ancestry rungs: the full-batch resubmit, then the
    /// ancestry-primed exhaustion corroboration.
    Ancestry,
}

/// How one classified submit outcome enters the gate (PURE — the real
/// producer between [`classify_submit_response`] and [`ladder_step`], so the
/// "466 conflict retries the ladder / 467 terminates it" behaviour is
/// unit-tested end-to-end without the worker runtime).
#[derive(Debug, PartialEq, Eq)]
enum SubmitEntry {
    /// 2xx accepted-for-processing — proceed to the SEEN gate with this body.
    Proceed(String),
    /// A gate step decided synchronously by the submit response itself:
    /// - `SyncRejected` → terminal via [`ladder_step`] (unless cascade);
    /// - a [`SubmitOutcome::ViewRejected`] (466 conflict) maps to
    ///   `AsyncRejected` — the ambiguous/retryable class whose exhausted
    ///   ladder ends at the #214 corroborator, never an uncorroborated 422.
    Step(GateStep),
    /// Transport trouble — the caller falls back (fail-closed).
    Transport(String),
}

fn submit_entry(outcome: SubmitOutcome) -> SubmitEntry {
    match outcome {
        SubmitOutcome::Processing(body) => SubmitEntry::Proceed(body),
        SubmitOutcome::SyncRejected(r) => SubmitEntry::Step(GateStep::SyncRejected(r)),
        // #228/#214: a provider-VIEW verdict (466 conflict) joins the async
        // class — retry the ladder, corroborate on exhaustion.
        SubmitOutcome::ViewRejected(r) => SubmitEntry::Step(GateStep::AsyncRejected(r)),
        SubmitOutcome::Transport(e) => SubmitEntry::Transport(e),
    }
}

fn ladder_step(step: GateStep, subject_txid: &str) -> Ladder {
    match step {
        GateStep::Accepted => Ladder::Return(ArcOutcome::Accepted(subject_txid.to_string())),
        // A SYNCHRONOUS validation failure (bad script / low fee) is definitive
        // — a resubmit cannot change it. Admit nothing; do NOT retry.
        //
        // #228 (arcade#260): a cascade rejection carrying the stable
        // `parent rejected (ancestor <txid>): retryable` label RETRIES, even
        // in synchronous dress. The #214 provider-independence rationale
        // below does NOT cover it: a cascade verdict is computed from
        // Arcade's (provably stale-able) view of the ANCESTOR, not from the
        // EF body alone — so it belongs with the ambiguous/retryable class,
        // and its exhausted ladder still ends at the #214 corroborator, never
        // at an uncorroborated 422.
        GateStep::SyncRejected(r) if cascade_retryable(&r) => Ladder::Retry,
        // #214: (non-cascade) sync stays UNCORROBORATED on purpose. The
        // corroboration leg exists because Arcade's ASYNC verdict depends on
        // its (provably stale-able) network/UTXO view; the synchronous 400 is
        // computed from the EF body alone — script and fee, with every
        // input's source script and satoshis inlined — so it is
        // provider-independent: any honest validator returns the same answer.
        // The #214 repro confirms the sync path was NOT implicated: every
        // false rejection that night was the 202-then-async-REJECTED shape
        // (`PROCESSING (4)`), never the structured sync-400 body. Returning
        // here (never `Retry`) is also what keeps the corroborator out of the
        // sync path by construction — corroboration lives strictly past the
        // exhausted ladder.
        GateStep::SyncRejected(r) => Ladder::Return(ArcOutcome::Rejected(r)),
        // Network did not accept — ambiguous (missing parent vs double-spend
        // vs a stale Arcade validator view, #214); an explicit resubmit is the
        // only recovery. Retry.
        GateStep::AsyncRejected(_) => Ladder::Retry,
        // #267: an orphan view is NOT ambiguous — the answer is "missing
        // parents", so answer it with parents (the ancestry rungs). Never a
        // Return: an orphan view alone may neither admit nor reject.
        GateStep::Orphan(_) => Ladder::Ancestry,
    }
}

/// Which rung of the gated ladder a submit serves ([`broadcast_efs_gated_with`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubmitRung {
    /// `POST /tx` of the subject's EF alone (attempts 1 and 2).
    SubjectOnly,
    /// `POST /txs` of the full ancestry batch (attempt 3).
    FullBatch,
}

/// Which corroborating broadcast the ladder requests: the #214 subject-only
/// leg, the #216 ancestry-primed batch, or the #268 mined-claim probe (the
/// subject's RAW bytes re-broadcast to a real provider — a genuinely mined
/// tx answers "already known/mined"; a fake-bumped one cannot).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CorroborationKind {
    SubjectOnly,
    WithAncestry,
    MinedClaim,
}

/// Gate-flow logging that is safe on the native host: the wiring tests drive
/// [`broadcast_efs_gated_with`] natively, where a `worker::console_log!` call
/// would abort (js-sys imported functions cannot be called off-wasm — the
/// same constraint `sleep_ms` documents).
fn gate_log(msg: &str) {
    #[cfg(target_arch = "wasm32")]
    worker::console_log!("{msg}");
    #[cfg(not(target_arch = "wasm32"))]
    let _ = msg;
}

/// #267: gate a would-be-terminal ladder outcome before it may leave the
/// gated broadcast. Rejections pass through untouched (the SyncRejected 422
/// semantics are #213's, unchanged). An ACCEPT claim:
///
/// - UNPROVEN ancestry (`efs_len > 1` — the #216 signal): Arcade's word
///   alone must NOT admit. This is the incident class: Arcade can hold the
///   subject only as an ORPHAN (parents 0-conf, absent from its node) or in
///   an otherwise-degraded view and still echo a gate-satisfying txStatus,
///   vouching for a tx the public network never received. Run the #216
///   ancestry-first corroboration (`corroborate(WithAncestry)` → the
///   [`corroborate_batch_with`] flow: ancestors primed to the corroborator
///   first, subject last, the subject's verdict ALONE decides) and admit
///   only on the corroborator's genuine accept ([`corroborated_accept_claim`]
///   — anything else fails CLOSED to Err/502, never back to trusting
///   Arcade). The wall-clock lands in the `corroborate` Server-Timing
///   segment via `ArcadeBroadcaster::corroborate_ms`.
/// - Single-EF subject (`efs_len == 1`) — SUBJECT-ONLY corroborate-on-accept
///   (bsv-low#268, closing the #267 review's residual). One leg means every
///   parent arrived with a merkle-path bump ATTACHED in the submitted BEEF
///   (`beef_to_ef_batch` skips a leg on `has_proof()` PRESENCE — the bump is
///   NOT SPV-validated at this layer, `HistoricalTxNoSpv`), i.e. "parents
///   are mined" is a SUBMITTER-ASSERTED signal, not a verified fact: a
///   fabricated parent bump used to make an unproven subject look single-EF
///   and ride the uncorroborated fast path (needing only a degraded/false-
///   SEEN Arcade — the proven-to-occur #267 condition). The fast path is
///   REMOVED: the accept claim is corroborated subject-only (the EF inlines
///   every input's source script + satoshis, so a healthy second broadcaster
///   can validate it standalone; ~1 corroborator POST on the happy path).
///   Fail direction unchanged: anything but the corroborator's genuine
///   accept refuses admission (Err → 502, the client retries) — never
///   admit-on-unknown, never a single-provider 422.
async fn gate_accept_claim_with<C, CFut>(
    outcome: ArcOutcome,
    efs_len: usize,
    subject_txid: &str,
    corroborate: &mut C,
) -> Result<ArcOutcome, String>
where
    C: FnMut(CorroborationKind) -> CFut,
    CFut: std::future::Future<Output = Result<ArcOutcome, String>>,
{
    match outcome {
        ArcOutcome::Rejected(_) => Ok(outcome),
        ArcOutcome::Accepted(_) => {
            let kind = if has_unproven_ancestry(efs_len) {
                CorroborationKind::WithAncestry
            } else {
                CorroborationKind::SubjectOnly
            };
            gate_log(&format!(
                "[arcade] {subject_txid} accept claim ({} unproven ancestor(s)) — corroborating before admit (#267/#268, {kind:?})",
                efs_len.saturating_sub(1)
            ));
            let corroborated = corroborate(kind).await;
            corroborated_accept_claim(corroborated, subject_txid)
        }
    }
}

/// PURE (#267 review hardening): the gated-broadcast LADDER control flow,
/// generic over the submit/poll transport (`submit_gate`) and the
/// corroborating broadcaster (`corroborate`) — the same injectable-closure
/// pattern as [`corroborate_batch_with`], so the ENFORCEMENT WIRING is
/// unit-tested natively, not just the leaf classifiers: "an Arcade-echoed
/// SEEN with unproven ancestry cannot return Accepted without a corroborator
/// accept" and "an orphan answer skips the subject-only rungs" are pinned at
/// THIS level (reverting a `gate_accept_claim_with` callsite to
/// `return Ok(outcome)` fails the wiring tests, not just a leaf test).
///
/// `submit_gate(rung)` performs one submit+SEEN-gate of the subject alone or
/// of the full ancestry batch; `corroborate(kind)` runs the #214
/// subject-only or #216 ancestry-primed corroborating broadcast; `efs_len`
/// is the EF leg count ([`has_unproven_ancestry`]). The real transports are
/// injected by [`ArcadeBroadcaster::broadcast_efs_gated`].
async fn broadcast_efs_gated_with<S, SFut, C, CFut>(
    efs_len: usize,
    subject_txid: &str,
    mut submit_gate: S,
    mut corroborate: C,
) -> Result<ArcOutcome, String>
where
    S: FnMut(SubmitRung) -> SFut,
    SFut: std::future::Future<Output = Result<GateStep, String>>,
    C: FnMut(CorroborationKind) -> CFut,
    CFut: std::future::Future<Output = Result<ArcOutcome, String>>,
{
    // #268: an EMPTY EF batch is the submitter's claim that EVERYTHING —
    // including the SUBJECT — is already mined (bump attached, NEVER
    // validated at this layer). It used to admit with ZERO network contact
    // ("already mined — skipping broadcast"); now the claim must be
    // corroborated against a real provider, and anything but the
    // corroborator's genuine accept refuses admission (Err → 502, the
    // client retries — never admit-on-unknown).
    if efs_len == 0 {
        gate_log(&format!(
            "[arcade] {subject_txid} all-proven BEEF (submitter-asserted mined) — corroborating the claim before admit (#268)"
        ));
        let corroborated = corroborate(CorroborationKind::MinedClaim).await;
        return corroborated_mined_claim(corroborated, subject_txid);
    }

    // #267: set when an ORPHAN view routes us straight to the ancestry
    // rungs (attempt 3 + the ancestry-primed exhaustion corroboration) —
    // the remaining subject-only rungs cannot supply the missing parents.
    let mut orphan_shortcut = false;

    // ── Attempt 1: SUBJECT ONLY. Arcade sources unconfirmed parents itself.
    gate_log(&format!(
        "[arcade] submitting subject-only {subject_txid} → gating on {ARCADE_GATE_STATUS}"
    ));
    let step = submit_gate(SubmitRung::SubjectOnly).await?;
    match ladder_step(step, subject_txid) {
        Ladder::Return(outcome) => {
            return gate_accept_claim_with(outcome, efs_len, subject_txid, &mut corroborate).await;
        }
        Ladder::Ancestry => {
            gate_log(&format!(
                "[arcade] {subject_txid} held as ORPHAN — routing to ancestry rungs (#267)"
            ));
            orphan_shortcut = true;
        }
        Ladder::Retry => {}
    }

    // ── Attempt 2: RESUBMIT the subject alone (waiting is proven useless;
    // Arcade needs an explicit resubmit to re-attempt orphan resolution).
    // Skipped on the orphan shortcut — a subject-only resubmit cannot
    // supply the missing parents.
    if !orphan_shortcut {
        gate_log(&format!(
            "[arcade] {subject_txid} not accepted — resubmitting subject-only"
        ));
        let step = submit_gate(SubmitRung::SubjectOnly).await?;
        match ladder_step(step, subject_txid) {
            Ladder::Return(outcome) => {
                return gate_accept_claim_with(outcome, efs_len, subject_txid, &mut corroborate)
                    .await;
            }
            Ladder::Ancestry => {
                gate_log(&format!(
                    "[arcade] {subject_txid} held as ORPHAN — routing to ancestry rungs (#267)"
                ));
                orphan_shortcut = true;
            }
            Ladder::Retry => {}
        }
    }

    // ── CORROBORATE — deliberately BEFORE the full-batch rung (#214).
    //
    // Ordering decision (the rung-3 poisoning question): under a stale
    // Arcade validator view, re-submitting ANCESTORS in the batch rung can
    // WORSEN state — a previously-SEEN ancestor gets re-validated against
    // the same stale view, its stored status can flip to REJECTED, and
    // Arcade's "parent rejected" cascade then condemns every descendant
    // (observed on 2026-07-20/21: sticky REJECTED for txs MINED in
    // 958776). So before feeding Arcade any ancestors, ask a SECOND
    // broadcaster about the SUBJECT:
    //  - corroborator ACCEPTS → return Accepted now — the batch rung is
    //    skipped entirely, so a stale Arcade never gets an ancestor
    //    resubmit to poison;
    //  - corroborator REJECTS → two independent broadcasters refused the
    //    subject — definitively Rejected, and the batch rung is pointless;
    //  - corroborator transport/inconclusive → the batch rung is still the
    //    right move: a GENUINE missing-parent orphan (the shape #211 built
    //    this rung for) is only fixable by feeding Arcade the ancestry.
    //    The poisoning residual survives ONLY in this arm (both
    //    broadcasters unable to confirm), which is exactly when we have no
    //    better information anyway — and the final corroboration below
    //    still stands between any fallout and a definitive 422.
    //
    // #267: skipped on the orphan shortcut — the orphan answer already
    // says "missing parents", and a subject-only corroboration would see
    // the same missing parent (the #216 lesson); go straight to the
    // ancestry rungs, whose exhaustion corroboration is ancestry-primed.
    if !orphan_shortcut {
        let corroborated = corroborate(CorroborationKind::SubjectOnly).await;
        match corroborated_exhaustion(corroborated, subject_txid) {
            Ok(outcome) => {
                gate_log(&format!(
                    "[arcade] {subject_txid} corroborated pre-batch → {outcome:?}"
                ));
                return Ok(outcome);
            }
            Err(inconclusive) if has_unproven_ancestry(efs_len) => {
                gate_log(&format!(
                    "[arcade] corroborator inconclusive for {subject_txid} ({inconclusive}) — trying full ancestry batch"
                ));
            }
            // No ancestors to feed — the ladder is exhausted and the
            // corroborator could not decide. Honest 502, never a false 422.
            Err(inconclusive) => return Err(inconclusive),
        }
    }

    // ── Attempt 3: FULL ANCESTRY BATCH — feed any parent Arcade could not
    // source from the live network (reached when the corroborator could
    // not decide — see the ordering note above — or directly on the #267
    // orphan shortcut, whose answer IS "missing parents").
    gate_log(&format!(
        "[arcade] {subject_txid} still not accepted — resubmitting full batch ({efs_len} legs)"
    ));
    let step = submit_gate(SubmitRung::FullBatch).await?;
    match ladder_step(step, subject_txid) {
        Ladder::Return(outcome) => {
            return gate_accept_claim_with(outcome, efs_len, subject_txid, &mut corroborate).await;
        }
        // Retry (async-rejected) and Ancestry (still orphan) both fall
        // through to the exhaustion corroboration — the ladder has no
        // rungs left, and for a still-orphan view the ancestry-primed
        // corroborator below is exactly the remaining move.
        Ladder::Retry | Ladder::Ancestry => {}
    }

    // Exhausted the resubmit ladder. Tonight's #214 outage proved Arcade's
    // async REJECTED alone is NOT trustworthy here, so the exhausted
    // verdict is whatever the corroborating broadcaster says (second
    // attempt — the pre-batch one was transport/inconclusive, and both
    // transports can be transient): Accepted admits (real network accept),
    // Rejected → 422 (two broadcasters agree), inconclusive → Err → 502.
    //
    // #216: when there IS ancestry (efs_len > 1), corroborate WITH it —
    // prime the corroborator's mempool with the parent chain FIRST so a
    // degraded broadcaster with a partial UTXO view can validate a subject
    // that spends a still-0-conf parent (the stuck-refund scenario). With a
    // single leg there is no parent to feed, so it stays subject-only. The
    // #214 semantics are UNCHANGED: only the subject's real network-accept
    // marker admits; a corroborator Rejected is the definitive 422; anything
    // else is an honest Err/502. Priming ancestors is safe — the
    // corroborator (TAAL/GorillaPool) is the HEALTHY broadcaster here, not
    // the stale Arcade view (see `corroborate_batch`'s poisoning note).
    let corroborated = if has_unproven_ancestry(efs_len) {
        corroborate(CorroborationKind::WithAncestry).await
    } else {
        corroborate(CorroborationKind::SubjectOnly).await
    };
    corroborated_exhaustion(corroborated, subject_txid)
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
    /// TAAL key for the #214 corroborating broadcaster. `None` still
    /// corroborates: TAAL is tried keyless (its 401 is transport) and
    /// GorillaPool is keyless by design.
    corroborator_taal_key: Option<String>,
    /// Wall-clock spent in the #214 corroboration leg(s), for the `corroborate`
    /// Server-Timing segment (#195 — the new leg must be attributable, not
    /// smeared into `arcade-broadcast`). `Cell`: the worker isolate is
    /// single-threaded and every async path here is `?Send`.
    corroborate_ms: std::cell::Cell<f64>,
    /// Wall-clock spent inside the SEEN-gate poll loop (`poll_for_status`),
    /// for the `arcade-poll` Server-Timing segment (bsv-low #272 — the
    /// 15–16 s JOIN-submit budget must be attributable per slice: submit
    /// POSTs vs poll waits vs corroboration).
    poll_ms: std::cell::Cell<f64>,
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
            corroborator_taal_key: None,
            corroborate_ms: std::cell::Cell::new(0.0),
            poll_ms: std::cell::Cell::new(0.0),
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

    /// Provide the TAAL key for the #214 corroborating broadcaster (optional —
    /// corroboration runs keyless without it; see `corroborator_taal_key`).
    #[must_use]
    pub fn with_corroborator_key(mut self, key: Option<String>) -> Self {
        self.corroborator_taal_key = key.filter(|k| !k.trim().is_empty());
        self
    }

    /// Milliseconds ACCUMULATED across every corroboration leg this
    /// broadcaster instance has run (each `corroborate_subject` /
    /// `corroborate_batch` call ADDS its wall-clock; nothing resets it — 0
    /// only if no corroboration ever ran). routes.rs constructs a fresh
    /// `ArcadeBroadcaster` per /submit request, so read there this is the
    /// request's total `corroborate` Server-Timing segment (#195).
    pub fn corroborate_ms(&self) -> f64 {
        self.corroborate_ms.get()
    }

    /// Milliseconds ACCUMULATED inside the SEEN-gate poll loop across this
    /// broadcaster instance (bsv-low #272 — read per request in routes.rs
    /// as the `arcade-poll` Server-Timing segment; the remainder of
    /// `arcade-broadcast` is then submit-POST wall-clock).
    pub fn poll_wait_ms(&self) -> f64 {
        self.poll_ms.get()
    }

    /// Run the #214 corroborating broadcast for the subject's EF hex,
    /// accounting its wall-clock into [`Self::corroborate_ms`].
    async fn corroborate_subject(&self, subject_ef: &EfTx) -> Result<ArcOutcome, String> {
        let started = worker::js_sys::Date::now();
        let res = corroborate_tx_hex(
            self.corroborator_taal_key.as_deref(),
            &hex::encode(&subject_ef.ef),
        )
        .await;
        self.corroborate_ms
            .set(self.corroborate_ms.get() + (worker::js_sys::Date::now() - started));
        res
    }

    /// Run the #268 mined-claim corroborating broadcast: the subject's RAW
    /// bytes (its EF does not exist — the submitter attached a bump, so
    /// `beef_to_ef_batch` skipped it) re-broadcast to TAAL → GorillaPool. A
    /// genuinely mined tx is answered "already known/mined" by any honest
    /// provider (raw suffices — its parents are on-chain); a fake-bumped
    /// unmined tx yields orphan/missing-input inconclusive or a refusal.
    /// Wall-clock accounted into [`Self::corroborate_ms`].
    async fn corroborate_mined_raw(&self, subject_raw: &[u8]) -> Result<ArcOutcome, String> {
        let started = worker::js_sys::Date::now();
        let res = corroborate_tx_hex(
            self.corroborator_taal_key.as_deref(),
            &hex::encode(subject_raw),
        )
        .await;
        self.corroborate_ms
            .set(self.corroborate_ms.get() + (worker::js_sys::Date::now() - started));
        res
    }

    /// Run the #216 corroborating broadcast WITH ancestry: prime the
    /// corroborator with each ancestor EF first, then corroborate the subject
    /// ([`corroborate_batch_with`]), accounting wall-clock into
    /// [`Self::corroborate_ms`].
    ///
    /// POISONING NOTE: feeding ancestors to the CORROBORATOR (TAAL →
    /// GorillaPool) is safe — TAAL/GorillaPool are NOT the stale-view
    /// broadcaster in the #214/#216 scenario (Arcade is), and the parents here
    /// (the pot / the JOIN funding the pot) are valid, already-broadcast funded
    /// txs. This is DELIBERATELY not a resubmit to ARCADE: re-feeding ancestors
    /// to a stale Arcade validator is the #214 poisoning risk (a previously-SEEN
    /// ancestor can flip to REJECTED and cascade "parent rejected"), which is
    /// exactly why the pre-batch corroboration and the Arcade batch rung stay
    /// subject-first. Here the target is a HEALTHY second broadcaster whose
    /// mempool we WANT to hold the parent so it can validate the child.
    async fn corroborate_batch(
        &self,
        efs: &[EfTx],
        subject_txid: &str,
    ) -> Result<ArcOutcome, String> {
        let started = worker::js_sys::Date::now();
        let key = self.corroborator_taal_key.clone();
        let res = corroborate_batch_with(efs, subject_txid, |tx_hex| {
            let key = key.clone();
            async move { corroborate_tx_hex(key.as_deref(), &tx_hex).await }
        })
        .await;
        self.corroborate_ms
            .set(self.corroborate_ms.get() + (worker::js_sys::Date::now() - started));
        res
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
        // #268: when every leg claims a bump (efs empty), the subject's RAW
        // is the mined-claim corroboration body.
        let mined_subject_raw = if efs.is_empty() {
            crate::ef::proven_subject_raw(&beef_bytes)
        } else {
            None
        };
        self.broadcast_efs_gated(&efs, &subject_txid, mined_subject_raw.as_deref())
            .await
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
    /// An empty `efs` (every tx CLAIMS a bump — including the subject) is NO
    /// LONGER an ungated no-op success (bsv-low#268): the bump is
    /// submitter-asserted and never validated here, so the "already mined"
    /// claim is corroborated against a real provider via the subject's RAW
    /// bytes (`mined_subject_raw`); without the corroborator's genuine
    /// accept — or without the raw itself — admission is refused (`Err` →
    /// 502, retryable).
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
    ///
    /// #214: Arcade's async REJECTED is additionally NOT trusted on its own —
    /// its validator view has gone provably stale (REJECTED for txs that MINED)
    /// — so before an exhausted ladder becomes a definitive `Rejected` (→ 422,
    /// which the client treats as terminal by design), a SECOND broadcaster
    /// (TAAL → GorillaPool, [`corroborate_tx_hex`]) must corroborate. Its real
    /// network accept admits; its rejection confirms the 422; anything else is
    /// an honest `Err`/502. A synchronous validation failure (SyncRejected)
    /// stays uncorroborated — see [`ladder_step`].
    ///
    /// #267: Arcade's ACCEPT is not trusted on its own either, when the
    /// subject has UNPROVEN ancestry — every accept-shaped exit passes
    /// [`gate_accept_claim_with`] (corroborate-on-accept; a single-EF subject
    /// keeps the fast path — see that function's honest-residual note and
    /// bsv-low#268). And an ORPHAN view (`SEEN_IN_ORPHAN_MEMPOOL`)
    /// short-circuits the subject-only rungs straight to the ancestry rungs —
    /// the orphan answer means "missing parents", so it is answered with
    /// parents, never with a Pending→timeout→502 and never with an admit.
    ///
    /// This is a thin transport-injection wrapper: the ladder control flow
    /// lives in [`broadcast_efs_gated_with`] (natively wiring-tested); this
    /// method supplies the real Arcade submit/poll and TAAL→GorillaPool
    /// corroboration transports.
    pub async fn broadcast_efs_gated(
        &self,
        efs: &[EfTx],
        subject_txid: &str,
        mined_subject_raw: Option<&[u8]>,
    ) -> Result<ArcOutcome, String> {
        // The subject's own EF is what we broadcast first (subject-only).
        // `None` ONLY on the #268 mined-claim path (efs empty), where no
        // submit rung ever runs — the pure control flow corroborates the
        // claim and returns before touching a rung.
        let subject_ef = efs.iter().find(|e| e.txid == subject_txid);
        if !efs.is_empty() && subject_ef.is_none() {
            return Err(format!("subject {subject_txid} not present in EF batch"));
        }

        broadcast_efs_gated_with(
            efs.len(),
            subject_txid,
            |rung| async move {
                let subject_ef = subject_ef
                    .ok_or_else(|| format!("subject {subject_txid} has no EF leg"))?;
                match rung {
                    SubmitRung::SubjectOnly => {
                        self.submit_once_and_gate(
                            &self.tx_endpoint(),
                            &subject_ef.ef,
                            subject_txid,
                            1,
                        )
                        .await
                    }
                    SubmitRung::FullBatch => {
                        let concat = concat_efs(efs);
                        self.submit_once_and_gate(
                            &self.txs_endpoint(),
                            &concat,
                            subject_txid,
                            efs.len(),
                        )
                        .await
                    }
                }
            },
            |kind| async move {
                match kind {
                    CorroborationKind::SubjectOnly => {
                        let subject_ef = subject_ef
                            .ok_or_else(|| format!("subject {subject_txid} has no EF leg"))?;
                        self.corroborate_subject(subject_ef).await
                    }
                    CorroborationKind::WithAncestry => {
                        self.corroborate_batch(efs, subject_txid).await
                    }
                    // #268: the "already mined" claim probe. No raw in the
                    // BEEF (txid-only subject) → inconclusive → refuse.
                    CorroborationKind::MinedClaim => match mined_subject_raw {
                        Some(raw) => self.corroborate_mined_raw(raw).await,
                        None => Err(format!(
                            "all-proven BEEF for {subject_txid} carries no subject raw — cannot corroborate the mined-claim; refusing to admit (#268)"
                        )),
                    },
                }
            },
        )
        .await
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
        let submit_body = match submit_entry(self.submit_ef(endpoint, subject_txid, body).await) {
            SubmitEntry::Proceed(b) => b,
            SubmitEntry::Step(step) => return Ok(step),
            SubmitEntry::Transport(e) => return Err(e),
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
                    // #267: an echoed ORPHAN view stops the gate here — no
                    // poll (waiting cannot conjure the missing parents).
                    GateVerdict::Orphan => {
                        return Ok(GateStep::Orphan(arcade_fatal_reason(
                            subject_txid,
                            &parsed.tx_status,
                            &parsed.extra_info,
                        )));
                    }
                    GateVerdict::Pending => {}
                }
            }
        }

        self.poll_for_status(subject_txid).await
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
    /// hits a fatal status, surfaces an ORPHAN view (#267 — routed to the
    /// ancestry rungs, not waited out), or the deadline elapses. Timeout →
    /// `Err` (never admit a tx that never became SEEN).
    async fn poll_for_status(&self, txid: &str) -> Result<GateStep, String> {
        let started = worker::js_sys::Date::now();
        // Accumulate this loop's wall-clock into `poll_ms` on EVERY exit —
        // scopeguard-free: the loop has 3 exits, each stamps before return.
        let stamp = |cell: &std::cell::Cell<f64>| {
            cell.set(cell.get() + (worker::js_sys::Date::now() - started));
        };
        let mut waited = 0u64;
        loop {
            if let Some(resp) = self.tx_status(txid).await {
                // GATE on `tx_status` ONLY — never `extra_info` (stale-extraInfo
                // trap, #213: a recovered orphan returns a healthy status with
                // the OLD failure extraInfo still attached).
                match classify_arcade_status(&resp.tx_status, ARCADE_GATE_STATUS) {
                    GateVerdict::Reached => {
                        worker::console_log!(
                            "[arcade] {txid} reached {} (polled {waited} ms)",
                            resp.tx_status
                        );
                        stamp(&self.poll_ms);
                        return Ok(GateStep::Accepted);
                    }
                    GateVerdict::Fatal => {
                        // #209: fold the captured extra_info into the reason text
                        // (reason ONLY — the gate above already decided on status).
                        stamp(&self.poll_ms);
                        return Ok(GateStep::AsyncRejected(arcade_fatal_reason(
                            txid,
                            &resp.tx_status,
                            &resp.extra_info,
                        )));
                    }
                    // #267: an ORPHAN view ends the poll immediately —
                    // `SEEN_IN_ORPHAN_MEMPOOL` used to rank 0 → Pending →
                    // 20s timeout → 502, when the answer ("missing parents")
                    // was already in hand. Route to the ancestry rungs.
                    GateVerdict::Orphan => {
                        stamp(&self.poll_ms);
                        return Ok(GateStep::Orphan(arcade_fatal_reason(
                            txid,
                            &resp.tx_status,
                            &resp.extra_info,
                        )));
                    }
                    GateVerdict::Pending => {}
                }
            }
            if waited >= ARCADE_WAIT_TIMEOUT_MS {
                stamp(&self.poll_ms);
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
                matches!(
                    classify_submit_response(status, body),
                    SubmitOutcome::Transport(_)
                ),
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

    // ── #228 (arcade#260): the ADDITIVE `status` field in the 400 body ──────
    //
    // Arcade v0.10.1-alpha.1 (LIVE 2026-07-22) adds a structured `status` code
    // to the synchronous 400 JSON. 476 = NON-FINAL (nothing persisted
    // server-side → retryable); 466/467 = terminal; ABSENT = a pre-#260
    // responder, classified byte-for-byte as before. All through the real
    // producer, `classify_submit_response`.

    /// The post-#260 NON-FINAL 400: additive `status: 476` + retryable hint.
    const ARCADE_400_STATUS_476_NON_FINAL: &str = r#"{"error":"transaction failed validation","reason":"TX_INVALID (31): GoBDK fail to ValidateTransaction -> UNKNOWN (0): non-final transaction: retryable","status":476}"#;

    #[test]
    fn post260_status_476_non_final_is_retryable_transport_never_a_rejection() {
        // The load-bearing #228 fix: a 476 non-final 400 persists NOTHING
        // server-side and is retryable — classing it SyncRejected would
        // manufacture a definitive 422 for a tx that simply isn't final YET
        // (the #213 false-verdict class, inverted). It must be Transport (the
        // caller falls back / retries), even though the body ALSO carries the
        // pre-#260 `error: "transaction failed validation"` value.
        match classify_submit_response(400, ARCADE_400_STATUS_476_NON_FINAL) {
            SubmitOutcome::Transport(reason) => {
                assert!(reason.contains("476"), "{reason}");
                assert!(reason.contains("retryable"), "{reason}");
            }
            other => panic!("status 476 must be Transport (retryable), got {other:?}"),
        }
        // Same verdict when the error field is absent — the structured status
        // code alone decides.
        let bare = r#"{"reason":"non-final transaction","status":476}"#;
        assert!(matches!(
            classify_submit_response(400, bare),
            SubmitOutcome::Transport(_)
        ));
    }

    #[test]
    fn post260_status_467_is_terminal_and_does_not_retry() {
        // 467 = arcade StatusGeneric ← TX_INVALID (31): script/fee/policy,
        // computed from the tx bytes — provider-independent, definitive.
        let body = r#"{"error":"transaction failed validation","reason":"TX_INVALID (31): terminal verdict","status":467}"#;
        let step = match classify_submit_response(400, body) {
            SubmitOutcome::SyncRejected(r) => {
                assert!(r.contains("status 467"), "{r}");
                r
            }
            other => panic!("status 467 must be SyncRejected, got {other:?}"),
        };
        // Terminal all the way through the real producers: submit_entry keeps
        // it a SyncRejected step and ladder_step Returns, never Retries.
        let entry = submit_entry(SubmitOutcome::SyncRejected(step));
        let SubmitEntry::Step(gate_step) = entry else {
            panic!("467 must enter the gate as a step, got {entry:?}");
        };
        assert!(
            matches!(
                ladder_step(gate_step, "ab"),
                Ladder::Return(ArcOutcome::Rejected(_))
            ),
            "status 467 must terminate the ladder"
        );
    }

    #[test]
    fn post260_status_466_conflict_retries_and_corroborates_never_terminal() {
        // 466 = arcade StatusConflict ← TX_CONFLICTING (36): a double-spend
        // verdict from the propagation PEER'S mempool/UTXO view — the provably
        // stale-able #214/#213 class. Verified against arcade's own table
        // (errors/errors.go + propagator.go classifyFailureLine). Classing it
        // terminal would manufacture an uncorroborated false 422 for a tx a
        // stale view merely BELIEVES conflicted. Through the real producers:
        // classify → ViewRejected → submit_entry → AsyncRejected → ladder
        // Retry (whose exhaustion path is the #214 corroborator).
        let body = r#"{"error":"transaction failed validation","reason":"TX_CONFLICTING (36): double spend detected","status":466}"#;
        let outcome = classify_submit_response(400, body);
        let SubmitOutcome::ViewRejected(reason) = outcome else {
            panic!("status 466 must be ViewRejected (view verdict), got {outcome:?}");
        };
        assert!(reason.contains("466"), "{reason}");

        let entry = submit_entry(SubmitOutcome::ViewRejected(reason));
        let SubmitEntry::Step(step) = entry else {
            panic!("466 must enter the gate as a step, got {entry:?}");
        };
        assert!(
            matches!(step, GateStep::AsyncRejected(_)),
            "466 must join the async/ambiguous class, got {step:?}"
        );
        assert_eq!(
            ladder_step(step, "ab"),
            Ladder::Retry,
            "a 466 conflict must resubmit (and corroborate on exhaustion), never 422 outright"
        );
    }

    #[test]
    fn post260_absent_or_unrecognised_status_keeps_pre260_classification() {
        // ABSENT `status` (a pre-#260 responder): the original #213 bodies
        // classify exactly as before — SyncRejected on the whole-field error
        // equality. (Tolerance requirement of #228.)
        for body in [ARCADE_SYNC_400_LOW_FEE, ARCADE_SYNC_400_BAD_SIG] {
            assert!(matches!(
                classify_submit_response(400, body),
                SubmitOutcome::SyncRejected(_)
            ));
        }
        // An UNRECOGNISED status code falls through to the same pre-#260 path:
        // with the validation-failure error → SyncRejected …
        let unrecognised =
            r#"{"error":"transaction failed validation","reason":"fee too low","status":465}"#;
        assert!(matches!(
            classify_submit_response(400, unrecognised),
            SubmitOutcome::SyncRejected(_)
        ));
        // … and without it → Transport (never a fabricated rejection).
        let unrecognised_other = r#"{"error":"bad request","reason":"","status":465}"#;
        assert!(matches!(
            classify_submit_response(400, unrecognised_other),
            SubmitOutcome::Transport(_)
        ));
    }

    // ── #228 (arcade#260): cascade `parent rejected … : retryable` label ────

    /// The post-#260 cascade label around a REAL ledger txid (stable prefix,
    /// explicit retryable hint).
    const CASCADE_ANCESTOR: &str =
        "2c50a257da80421f8a31c98bedc728b19e437edff0e2e84b74278f4b20d82256";

    #[test]
    fn post260_cascade_retryable_sync_rejection_retries_the_ladder() {
        // A cascade rejection in SYNCHRONOUS dress: classified SyncRejected by
        // the real classifier, but the ladder must RETRY it — the verdict
        // depends on Arcade's (stale-able, #214) view of the ANCESTOR, not on
        // the EF body, so it is not the provider-independent terminal class.
        let body = format!(
            r#"{{"error":"transaction failed validation","reason":"parent rejected (ancestor {CASCADE_ANCESTOR}): retryable"}}"#
        );
        let step = match classify_submit_response(400, &body) {
            SubmitOutcome::SyncRejected(r) => GateStep::SyncRejected(r),
            other => panic!("cascade sync 400 must classify SyncRejected, got {other:?}"),
        };
        assert_eq!(
            ladder_step(step, "ab"),
            Ladder::Retry,
            "a cascade-retryable sync rejection must resubmit, not 422"
        );
    }

    #[test]
    fn post260_cascade_retryable_async_rejection_still_retries() {
        // The same label in ASYNC dress (poll → fatal status, extra_info
        // carries the cascade) — already a Retry today; pinned so the #228
        // adoption can never regress it.
        let reason = arcade_fatal_reason(
            "ab",
            "REJECTED",
            &format!("parent rejected (ancestor {CASCADE_ANCESTOR}): retryable"),
        );
        assert_eq!(
            ladder_step(GateStep::AsyncRejected(reason), "ab"),
            Ladder::Retry
        );
    }

    #[test]
    fn post260_cascade_without_the_retryable_hint_stays_terminal() {
        // Only the COMMITTED label (both halves) retries. A cascade-looking
        // reason WITHOUT the `: retryable` hint keeps today's terminal
        // behaviour — we never invent a retry from half a label.
        let body = format!(
            r#"{{"error":"transaction failed validation","reason":"parent rejected (ancestor {CASCADE_ANCESTOR}): double spend"}}"#
        );
        let step = match classify_submit_response(400, &body) {
            SubmitOutcome::SyncRejected(r) => GateStep::SyncRejected(r),
            other => panic!("must classify SyncRejected, got {other:?}"),
        };
        assert!(matches!(
            ladder_step(step, "ab"),
            Ladder::Return(ArcOutcome::Rejected(_))
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

    // ── #214: exhausted-ladder corroboration (pure control-flow producers) ──
    //
    // Ground truth: Arcade-v2-us-1's stale validator view async-REJECTED txs
    // that TAAL SEENed in seconds and that MINED in block 958776 — sticky,
    // cascading, /health green throughout. The exhausted ladder therefore may
    // not become a definitive 422 on Arcade's word alone; these tests pin the
    // corroboration semantics through the REAL pure producers
    // (`corroborator_verdict` classifies the corroborator's actual
    // (status, body) wire answer; `corroborated_exhaustion` folds it into the
    // exhausted verdict that `broadcast_efs_gated` returns and routes.rs maps
    // to 200/422/502).

    /// The corroborator's REAL accept shape: TAAL answers `/v1/tx` 200 with a
    /// SEEN_ON_NETWORK txStatus.
    const CORR_SEEN_BODY: &str = r#"{"txid":"ab","txStatus":"SEEN_ON_NETWORK","extraInfo":""}"#;

    #[test]
    fn corroborator_accept_requires_a_real_network_accept_marker() {
        // Genuine accepts: SEEN_ON_NETWORK or better.
        for status in [
            "SEEN_ON_NETWORK",
            "SEEN_MULTIPLE_NODES",
            "MINED",
            "IMMUTABLE",
        ] {
            let body = format!(r#"{{"txid":"ab","txStatus":"{status}","extraInfo":""}}"#);
            assert_eq!(
                corroborator_verdict(200, &body).unwrap(),
                ArcOutcome::Accepted("ab".into()),
                "{status} is a real network accept"
            );
        }
        // A 200-SHAPED ACK WITHOUT THE ACCEPT MARKER IS NOT AN ACCEPT — the
        // load-bearing #214 requirement. Sub-SEEN statuses, an empty status
        // object and MINED_IN_STALE_BLOCK are all INCONCLUSIVE (Err), because
        // the corroborator's word here overrides another broadcaster's
        // explicit REJECTED and must therefore be a real accept, never an ack.
        for body in [
            r#"{"txid":"ab","txStatus":"RECEIVED"}"#.to_string(),
            r#"{"txid":"ab","txStatus":"STORED"}"#.to_string(),
            r#"{"txid":"ab","txStatus":"ANNOUNCED_TO_NETWORK"}"#.to_string(),
            r#"{"txid":"ab","txStatus":"ACCEPTED_BY_NETWORK"}"#.to_string(),
            r#"{"txid":"ab","txStatus":"MINED_IN_STALE_BLOCK"}"#.to_string(),
            r#"{"txid":"ab","txStatus":""}"#.to_string(),
            "{}".to_string(),
        ] {
            assert!(
                corroborator_verdict(200, &body).is_err(),
                "200 without an accept marker must be inconclusive: {body}"
            );
        }
        // Unparseable 2xx junk is inconclusive too, never an accept.
        assert!(corroborator_verdict(200, "<html>gateway junk</html>").is_err());
    }

    #[test]
    fn corroborator_definitive_rejections_and_transport_classify_apart() {
        // Definitive: 2xx error txStatus and the 460–479 verdict class.
        for s in ["REJECTED", "DOUBLE_SPEND_ATTEMPTED", "INVALID", "MALFORMED"] {
            let body = format!(r#"{{"txid":"ab","txStatus":"{s}","extraInfo":"fee too low"}}"#);
            assert!(
                matches!(
                    corroborator_verdict(200, &body).unwrap(),
                    ArcOutcome::Rejected(_)
                ),
                "{s} must corroborate the rejection"
            );
        }
        for status in [460u16, 461, 465, 473] {
            assert!(matches!(
                corroborator_verdict(status, "invalid").unwrap(),
                ArcOutcome::Rejected(_)
            ));
        }
        // Transport stays transport (Err) — 5xx, auth, misroute, rate-limit.
        for status in [400u16, 401, 403, 404, 429, 500, 502, 503] {
            assert!(
                corroborator_verdict(status, "trouble").is_err(),
                "HTTP {status}"
            );
        }
        // Already-known in any dress = the network HAS the tx = accept.
        assert!(matches!(
            corroborator_verdict(422, "txn-already-known (code 257)").unwrap(),
            ArcOutcome::Accepted(_)
        ));
        let dressed =
            r#"{"txid":"ab","txStatus":"REJECTED","extraInfo":"transaction already mined"}"#;
        assert!(matches!(
            corroborator_verdict(200, dressed).unwrap(),
            ArcOutcome::Accepted(_)
        ));
        // ORPHAN is INCONCLUSIVE for a corroborator (unlike arc_verdict): "I
        // can't see the parent" is the stale-view failure mode itself and must
        // never CONFIRM another provider's REJECTED.
        let orphan = r#"{"txid":"ab","txStatus":"SEEN_IN_ORPHAN_MEMPOOL","extraInfo":""}"#;
        assert!(corroborator_verdict(200, orphan).is_err());
    }

    #[test]
    fn exhausted_ladder_with_corroborator_accept_admits() {
        // exhausted + corroborator-accepts ⇒ Accepted. The #192/#193 invariant
        // holds — admission rides the corroborator's REAL network accept — and
        // the returned txid is OUR subject, never the corroborator's echo.
        let subject = "2c50a257da80421f8a31c98bedc728b19e437edff0e2e84b74278f4b20d82256";
        let corroborator = corroborator_verdict(200, CORR_SEEN_BODY);
        assert_eq!(
            corroborated_exhaustion(corroborator, subject).unwrap(),
            ArcOutcome::Accepted(subject.to_string()),
            "a second broadcaster's real accept must override Arcade's stale REJECTED"
        );
    }

    #[test]
    fn exhausted_ladder_with_corroborator_reject_stays_rejected() {
        // exhausted + corroborator-rejects ⇒ Rejected (→ 422): two independent
        // broadcasters refused the subject.
        let body = r#"{"txid":"ab","txStatus":"REJECTED","extraInfo":"fee too low"}"#;
        let corroborator = corroborator_verdict(200, body);
        match corroborated_exhaustion(corroborator, "ab").unwrap() {
            ArcOutcome::Rejected(reason) => {
                assert!(reason.contains("network did not accept ab"), "{reason}");
                assert!(reason.contains("corroborated"), "{reason}");
            }
            other => panic!("must stay Rejected, got {other:?}"),
        }
    }

    #[test]
    fn exhausted_ladder_with_corroborator_transport_is_err_never_a_false_422() {
        // exhausted + corroborator-transport/inconclusive ⇒ Err (→ 502): the
        // client's direct-ARC fallback keeps money moving — better an honest
        // "unavailable" than a false "refused" (which the client treats as
        // terminal, by design).
        for corroborator in [
            Err("taal: fetch failed; gorillapool: fetch failed".to_string()),
            corroborator_verdict(503, "unavailable"),
            // A 200-shaped ack without the accept marker lands here too.
            corroborator_verdict(200, r#"{"txid":"ab","txStatus":"RECEIVED"}"#),
        ] {
            assert!(
                corroborated_exhaustion(corroborator, "ab").is_err(),
                "inconclusive corroboration must be Err/502, never Rejected/422"
            );
        }
    }

    #[test]
    fn sync_400_is_definitive_and_never_reaches_the_corroborator() {
        // #214 item 2: the synchronous structured 400 is provider-independent
        // (script/fee computed from the EF body alone — no network/UTXO view
        // involved, so a stale validator view cannot produce it; every falsy
        // in the #214 repro was the ASYNC 202-then-REJECTED shape). It stays
        // an immediate definitive 422 with NO corroborator call — proven via
        // the real control-flow producers: `classify_submit_response` yields
        // SyncRejected, and `ladder_step(SyncRejected)` RETURNS (never
        // `Retry`), so `broadcast_efs_gated` exits before the corroboration
        // legs, which live strictly past the exhausted ladder.
        let step = match classify_submit_response(400, ARCADE_SYNC_400_LOW_FEE) {
            SubmitOutcome::SyncRejected(r) => GateStep::SyncRejected(r),
            other => panic!("sync 400 must classify SyncRejected, got {other:?}"),
        };
        match ladder_step(step, "ab") {
            Ladder::Return(ArcOutcome::Rejected(reason)) => {
                assert!(reason.contains("transaction failed validation"), "{reason}");
            }
            other => panic!("sync rejection must Return(Rejected) — never Retry (which is the only path to corroboration) — got {other:?}"),
        }
    }

    // ── #216: corroborate WITH ancestry (pure batch control flow) ───────────
    //
    // Ground truth: during the #214 Arcade outage a pre-signed refund spending a
    // still-0-conf pot could not be corroborated — the corroborator only ever
    // received the SUBJECT alone, saw a "missing parent" and returned
    // inconclusive → 502. `corroborate_batch_with` primes the corroborator's
    // mempool with the ANCESTORS first, then lets ONLY the subject's verdict
    // decide. These tests drive the REAL pure producer with a mocked transport
    // (the same `submit_one` closure shape the worker path injects
    // `corroborate_tx_hex` into), so ordering + the strict "subject decides"
    // semantics are proven natively.

    /// A parent + subject EF batch (ancestry order, subject last). Subject EF
    /// bytes `[4,5]` (hex "0405"); parent `[1,2,3]` (hex "010203").
    fn parent_and_subject() -> (Vec<EfTx>, String) {
        (
            vec![
                EfTx {
                    txid: "parent".into(),
                    ef: vec![1, 2, 3],
                },
                EfTx {
                    txid: "subject".into(),
                    ef: vec![4, 5],
                },
            ],
            "subject".to_string(),
        )
    }

    #[tokio::test]
    async fn batch_accepts_subject_only_after_parent_primed() {
        // (a) The corroborator SEENs the subject ONLY once the parent has been
        // primed into its mempool (subject-alone stays inconclusive). Priming
        // the parent first flips it to Accepted — the whole point of #216.
        //
        // RED-VERIFY: neuter the parent-submit loop in `corroborate_batch_with`
        // (backup copy) → parent_seen stays false → the subject-alone answer is
        // RECEIVED → inconclusive Err → this `unwrap` panics → the test fails.
        let (efs, subject) = parent_and_subject();
        let subject_hex = hex::encode([4u8, 5]);
        let parent_seen = std::cell::Cell::new(false);
        let out = corroborate_batch_with(&efs, &subject, |tx_hex| {
            let is_subject = tx_hex == subject_hex;
            if !is_subject {
                parent_seen.set(true);
            }
            let seen = parent_seen.get();
            async move {
                if is_subject {
                    // With the parent primed the corroborator can validate the
                    // child (SEEN); without it, it can only see RECEIVED.
                    let status = if seen { "SEEN_ON_NETWORK" } else { "RECEIVED" };
                    corroborator_verdict(
                        200,
                        &format!(r#"{{"txid":"subject","txStatus":"{status}"}}"#),
                    )
                } else {
                    corroborator_verdict(200, r#"{"txid":"parent","txStatus":"SEEN_ON_NETWORK"}"#)
                }
            }
        })
        .await;
        assert_eq!(
            out.unwrap(),
            ArcOutcome::Accepted("subject".into()),
            "parent primed before the subject → the corroborator admits the subject"
        );
    }

    #[tokio::test]
    async fn batch_subject_rejection_is_rejected_regardless_of_parents() {
        // (b) The subject's verdict is the ONLY arbiter: a corroborator that
        // rejects the subject ⇒ Rejected, no matter how the parents fared.
        let (efs, subject) = parent_and_subject();
        let subject_hex = hex::encode([4u8, 5]);
        let out = corroborate_batch_with(&efs, &subject, |tx_hex| {
            let is_subject = tx_hex == subject_hex;
            async move {
                if is_subject {
                    corroborator_verdict(
                        200,
                        r#"{"txid":"subject","txStatus":"REJECTED","extraInfo":"fee too low"}"#,
                    )
                } else {
                    corroborator_verdict(200, r#"{"txid":"parent","txStatus":"SEEN_ON_NETWORK"}"#)
                }
            }
        })
        .await;
        assert!(matches!(out.unwrap(), ArcOutcome::Rejected(_)));
    }

    #[tokio::test]
    async fn batch_subject_transport_failure_is_err() {
        // (c) Transport trouble on the subject ⇒ Err (→ 502), even with a
        // healthy parent prime.
        let (efs, subject) = parent_and_subject();
        let subject_hex = hex::encode([4u8, 5]);
        let out = corroborate_batch_with(&efs, &subject, |tx_hex| {
            let is_subject = tx_hex == subject_hex;
            async move {
                if is_subject {
                    Err::<ArcOutcome, String>(
                        "taal: fetch failed; gorillapool: fetch failed".into(),
                    )
                } else {
                    corroborator_verdict(200, r#"{"txid":"parent","txStatus":"SEEN_ON_NETWORK"}"#)
                }
            }
        })
        .await;
        assert!(out.is_err());
    }

    #[tokio::test]
    async fn batch_subject_200_without_seen_marker_is_never_accepted() {
        // (d) THE #192/#193 guard on the batch path: a 200-shaped ack WITHOUT a
        // real network-accept marker must be inconclusive (Err), NEVER Accept —
        // a primed parent can never manufacture an accept out of a sub-SEEN
        // subject answer.
        let (efs, subject) = parent_and_subject();
        let subject_hex = hex::encode([4u8, 5]);
        for status in ["RECEIVED", "STORED", "ACCEPTED_BY_NETWORK", ""] {
            let out = corroborate_batch_with(&efs, &subject, |tx_hex| {
                let is_subject = tx_hex == subject_hex;
                let status = status.to_string();
                async move {
                    if is_subject {
                        corroborator_verdict(
                            200,
                            &format!(r#"{{"txid":"subject","txStatus":"{status}"}}"#),
                        )
                    } else {
                        corroborator_verdict(
                            200,
                            r#"{"txid":"parent","txStatus":"SEEN_ON_NETWORK"}"#,
                        )
                    }
                }
            })
            .await;
            assert!(
                out.is_err(),
                "200 txStatus {status:?} without SEEN must be inconclusive, never Accept"
            );
        }
    }

    #[tokio::test]
    async fn batch_parent_submit_failures_do_not_flip_the_subject_verdict() {
        // (e) A parent submit that hard-fails (transport boom) is IGNORED — it
        // only primes the mempool. The subject's real SEEN still admits.
        let (efs, subject) = parent_and_subject();
        let subject_hex = hex::encode([4u8, 5]);
        let out = corroborate_batch_with(&efs, &subject, |tx_hex| {
            let is_subject = tx_hex == subject_hex;
            async move {
                if is_subject {
                    corroborator_verdict(200, r#"{"txid":"subject","txStatus":"SEEN_ON_NETWORK"}"#)
                } else {
                    Err::<ArcOutcome, String>("parent transport boom".into())
                }
            }
        })
        .await;
        assert_eq!(
            out.unwrap(),
            ArcOutcome::Accepted("subject".into()),
            "a failed parent prime must not flip the subject's accept"
        );
    }

    #[tokio::test]
    async fn batch_subject_first_fast_path_skips_the_primes_on_a_genuine_accept() {
        // bsv-low#272: a HEALTHY corroborator SEENs the subject standalone
        // (EF inlines source scripts + sats), so the happy path is ONE POST —
        // no ancestor primes at all. This is the N+1→1 latency fix.
        let efs = vec![
            EfTx {
                txid: "g".into(),
                ef: vec![0xaa],
            }, // grandparent
            EfTx {
                txid: "p".into(),
                ef: vec![0xbb],
            }, // parent
            EfTx {
                txid: "subject".into(),
                ef: vec![0xcc],
            }, // subject last
        ];
        let order = std::cell::RefCell::new(Vec::<String>::new());
        let out = corroborate_batch_with(&efs, "subject", |tx_hex| {
            order.borrow_mut().push(tx_hex.clone());
            async move {
                corroborator_verdict(200, r#"{"txid":"subject","txStatus":"SEEN_ON_NETWORK"}"#)
            }
        })
        .await;
        assert_eq!(out.unwrap(), ArcOutcome::Accepted("subject".into()));
        assert_eq!(
            *order.borrow(),
            vec![hex::encode([0xccu8])],
            "a genuine accept on the subject-first attempt must skip every prime"
        );
    }

    #[tokio::test]
    async fn batch_degraded_path_primes_parents_in_ancestry_order_then_subject_decides() {
        // The #216 shape: subject-alone is inconclusive (the corroborator's
        // partial UTXO view can't validate it) → parents are primed in
        // ANCESTRY ORDER, then the subject is retried LAST and its primed
        // verdict is final — byte-identical to the pre-#272 semantics.
        let efs = vec![
            EfTx {
                txid: "g".into(),
                ef: vec![0xaa],
            }, // grandparent
            EfTx {
                txid: "p".into(),
                ef: vec![0xbb],
            }, // parent
            EfTx {
                txid: "subject".into(),
                ef: vec![0xcc],
            }, // subject last
        ];
        let order = std::cell::RefCell::new(Vec::<String>::new());
        let primed = std::cell::Cell::new(false);
        let out = corroborate_batch_with(&efs, "subject", |tx_hex| {
            order.borrow_mut().push(tx_hex.clone());
            let is_subject = tx_hex == hex::encode([0xccu8]);
            if !is_subject {
                primed.set(true);
            }
            let seen = primed.get();
            async move {
                if is_subject {
                    let status = if seen { "SEEN_ON_NETWORK" } else { "RECEIVED" };
                    corroborator_verdict(
                        200,
                        &format!(r#"{{"txid":"subject","txStatus":"{status}"}}"#),
                    )
                } else {
                    corroborator_verdict(200, r#"{"txid":"p","txStatus":"SEEN_ON_NETWORK"}"#)
                }
            }
        })
        .await;
        assert_eq!(out.unwrap(), ArcOutcome::Accepted("subject".into()));
        assert_eq!(
            *order.borrow(),
            vec![
                hex::encode([0xccu8]), // subject-first probe (inconclusive)
                hex::encode([0xaau8]), // primes, ancestry order
                hex::encode([0xbbu8]),
                hex::encode([0xccu8]), // subject decided last
            ],
            "degraded path: subject probe, parents primed in ancestry order, subject last"
        );
    }

    #[tokio::test]
    async fn batch_missing_subject_in_efs_is_an_error() {
        let efs = vec![EfTx {
            txid: "parent".into(),
            ef: vec![1, 2, 3],
        }];
        let out = corroborate_batch_with(&efs, "subject", |_tx_hex| async {
            corroborator_verdict(200, r#"{"txStatus":"SEEN_ON_NETWORK"}"#)
        })
        .await;
        assert!(out.is_err(), "a batch without the subject leg is an error");
    }

    #[test]
    fn unproven_ancestry_signal_gates_batch_corroboration_and_accept_claims() {
        // The ONE signal, two consumers (#216/#267/#268): efs.len()==1
        // (claimed-proven ancestry) keeps corroboration SUBJECT-ONLY — but
        // since #268 the accept claim is corroborated on this arm too (the
        // uncorroborated fast path is dead); efs.len()>1 routes the
        // exhaustion corroboration through the ancestry-carrying
        // `corroborate_batch` AND the #267 corroborate-on-accept gate.
        assert!(
            !has_unproven_ancestry(1),
            "single leg → subject-only corroboration (still corroborated on accept, #268)"
        );
        assert!(
            has_unproven_ancestry(2),
            "ancestry present → batch corroboration, corroborate-on-accept"
        );
        assert!(has_unproven_ancestry(9));
    }

    // ── #267 hardening: the corroboration leg cap (work bound) ──────────────

    /// `count` parent legs + the subject leg, through the real batch producer.
    fn capped_batch(count: usize) -> (Vec<EfTx>, String) {
        let mut efs: Vec<EfTx> = (0..count)
            .map(|i| EfTx {
                txid: format!("p{i}"),
                ef: vec![(i % 250) as u8, (i / 250) as u8],
            })
            .collect();
        efs.push(EfTx {
            txid: "subject".into(),
            ef: vec![0xff, 0xff, 0xff],
        });
        (efs, "subject".to_string())
    }

    #[tokio::test]
    async fn batch_over_the_leg_cap_is_inconclusive_with_zero_submits() {
        // One leg over the cap → inconclusive Err (→ 502, client fallback)
        // BEFORE any submit: a truncated/partial corroboration must never
        // exist, let alone admit. The routes.rs byte bound alone admits ~20k
        // minimal legs — this is the serial-POST work bound.
        let (efs, subject) = capped_batch(MAX_CORROBORATION_LEGS); // +subject ⇒ cap+1 legs
        assert_eq!(efs.len(), MAX_CORROBORATION_LEGS + 1);
        let submits = std::cell::Cell::new(0usize);
        let out = corroborate_batch_with(&efs, &subject, |_tx_hex| {
            submits.set(submits.get() + 1);
            async { corroborator_verdict(200, r#"{"txid":"x","txStatus":"SEEN_ON_NETWORK"}"#) }
        })
        .await;
        let err = out.expect_err("over-cap batch must be inconclusive, never corroborated");
        assert!(err.contains("leg cap"), "{err}");
        assert_eq!(
            submits.get(),
            0,
            "zero submits over the cap — no partial corroboration"
        );
        // …and the #267 accept-claim fold on that Err refuses the admit.
        assert!(corroborated_accept_claim(Err(err), &subject).is_err());
    }

    #[tokio::test]
    async fn batch_at_the_leg_cap_still_corroborates_and_subject_decides() {
        // Exactly at the cap the corroboration runs in full and the subject's
        // verdict decides, as ever.
        let (efs, subject) = capped_batch(MAX_CORROBORATION_LEGS - 1); // +subject ⇒ cap legs
        assert_eq!(efs.len(), MAX_CORROBORATION_LEGS);
        let subject_hex = hex::encode([0xffu8, 0xff, 0xff]);
        let out = corroborate_batch_with(&efs, &subject, |tx_hex| {
            let is_subject = tx_hex == subject_hex;
            async move {
                if is_subject {
                    corroborator_verdict(200, r#"{"txid":"subject","txStatus":"SEEN_ON_NETWORK"}"#)
                } else {
                    corroborator_verdict(200, r#"{"txid":"p","txStatus":"SEEN_ON_NETWORK"}"#)
                }
            }
        })
        .await;
        assert_eq!(out.unwrap(), ArcOutcome::Accepted("subject".into()));
    }

    // ── #267: corroborate-on-accept — Arcade's ACCEPT is never authoritative
    //    for an unproven-parent subject ─────────────────────────────────────
    //
    // Ground truth (bsv-low #267, 2026-07-27/28): a degraded Arcade held a
    // JOIN only in its ORPHAN pool (parents 0-conf, absent from its node) yet
    // echoed a gate-satisfying txStatus; the overlay admitted, the hand
    // played out, and BOTH the JOIN and the settle were WoC/Bitails-404
    // twenty minutes later (Server-Timing showed corroborate=0.0 — #214
    // corroborates only REJECTIONS). These tests pin the corroborate-on-accept
    // fold through the REAL pure producers: `corroborator_verdict` classifies
    // the corroborator's wire answer, `corroborate_batch_with` runs the #216
    // ancestry-first control flow, and `corroborated_accept_claim` folds the
    // result into the accept claim that `gate_accept_claim` returns.

    #[test]
    fn accept_claim_with_corroborator_accept_admits_with_our_subject_txid() {
        // A second broadcaster's REAL network accept confirms Arcade's claim
        // → admit, under OUR subject txid (never the corroborator's echo).
        let subject = "2c50a257da80421f8a31c98bedc728b19e437edff0e2e84b74278f4b20d82256";
        let corroborator = corroborator_verdict(200, CORR_SEEN_BODY);
        assert_eq!(
            corroborated_accept_claim(corroborator, subject).unwrap(),
            ArcOutcome::Accepted(subject.to_string())
        );
    }

    #[test]
    fn accept_claim_with_corroborator_inconclusive_fails_closed_never_trusting_arcade() {
        // THE #267 fix: when the corroborator cannot confirm, the admit is
        // REFUSED (Err → 502) — never a fall-back to trusting Arcade's word
        // alone (which IS the incident). Every inconclusive dress: transport
        // failure on both hosts, a 200-shaped sub-SEEN ack, and the
        // incident's own shape — the corroborator too holding only an ORPHAN
        // view.
        for corroborator in [
            Err("taal: fetch failed; gorillapool: fetch failed".to_string()),
            corroborator_verdict(503, "unavailable"),
            corroborator_verdict(200, r#"{"txid":"ab","txStatus":"RECEIVED"}"#),
            corroborator_verdict(
                200,
                r#"{"txid":"ab","txStatus":"SEEN_IN_ORPHAN_MEMPOOL","extraInfo":""}"#,
            ),
        ] {
            let out = corroborated_accept_claim(corroborator, "ab");
            let err = out.expect_err("inconclusive corroboration must refuse the admit");
            assert!(err.contains("not admitting"), "{err}");
        }
    }

    #[test]
    fn accept_claim_with_corroborator_reject_refuses_admission_without_a_false_422() {
        // Arcade says accepted, the corroborator says refused: CONFLICTING
        // single-provider verdicts. Fail closed on admission (never admit on
        // Arcade's word) but never mint a definitive 422 from one provider's
        // rejection either (#214's own doctrine) — Err → an honest,
        // retryable 502.
        let body = r#"{"txid":"ab","txStatus":"REJECTED","extraInfo":"fee too low"}"#;
        let corroborator = corroborator_verdict(200, body);
        let out = corroborated_accept_claim(corroborator, "ab");
        let err = out.expect_err("a conflicting reject must be Err/502, never Ok");
        assert!(err.contains("conflicting"), "{err}");
    }

    #[tokio::test]
    async fn accept_claim_primed_parent_can_never_manufacture_a_subject_accept() {
        // The #211/#212-class invariant, extended through the NEW accept-claim
        // path end-to-end (real producers: corroborate_batch_with →
        // corroborated_accept_claim): a parent primed as SEEN plus a subject
        // that never gets past a sub-SEEN ack must NOT admit — the subject's
        // verdict alone decides, and an ack is not a verdict.
        //
        // RED-VERIFY: neuter `corroborated_accept_claim`'s Err arm (backup
        // copy) to return Ok(Accepted) — "trust Arcade when the corroborator
        // can't confirm" — and this test fails.
        let (efs, subject) = parent_and_subject();
        let subject_hex = hex::encode([4u8, 5]);
        for status in ["RECEIVED", "STORED", "ACCEPTED_BY_NETWORK", ""] {
            let corroborated = corroborate_batch_with(&efs, &subject, |tx_hex| {
                let is_subject = tx_hex == subject_hex;
                let status = status.to_string();
                async move {
                    if is_subject {
                        corroborator_verdict(
                            200,
                            &format!(r#"{{"txid":"subject","txStatus":"{status}"}}"#),
                        )
                    } else {
                        corroborator_verdict(
                            200,
                            r#"{"txid":"parent","txStatus":"SEEN_ON_NETWORK"}"#,
                        )
                    }
                }
            })
            .await;
            assert!(
                corroborated_accept_claim(corroborated, &subject).is_err(),
                "primed parent + sub-SEEN subject ({status:?}) must never admit"
            );
        }
    }

    #[tokio::test]
    async fn incident_267_orphan_vouching_arcade_cannot_admit_uncorroborated() {
        // The incident, replayed through the real producers: Arcade claims the
        // subject accepted, but the corroborator — even AFTER the parents are
        // primed — holds it only as an orphan. The accept claim must be
        // refused (Err → 502), never admitted.
        let (efs, subject) = parent_and_subject();
        let subject_hex = hex::encode([4u8, 5]);
        let corroborated = corroborate_batch_with(&efs, &subject, |tx_hex| {
            let is_subject = tx_hex == subject_hex;
            async move {
                if is_subject {
                    corroborator_verdict(
                        200,
                        r#"{"txid":"subject","txStatus":"SEEN_IN_ORPHAN_MEMPOOL","extraInfo":""}"#,
                    )
                } else {
                    corroborator_verdict(200, r#"{"txid":"parent","txStatus":"SEEN_ON_NETWORK"}"#)
                }
            }
        })
        .await;
        assert!(
            corroborated_accept_claim(corroborated, &subject).is_err(),
            "an orphan-view corroboration must never confirm Arcade's accept claim"
        );
    }

    // ── #267 hardening: the ENFORCEMENT WIRING itself ───────────────────────
    //
    // The leaf tests above prove the classifiers and the fold; these prove
    // the LADDER actually routes through them — `broadcast_efs_gated_with` is
    // the REAL control flow the worker path runs (the method injects only
    // transports), so reverting a `gate_accept_claim_with` callsite to
    // `return Ok(outcome)`, or un-wiring the orphan shortcut, fails HERE even
    // while every leaf test stays green.

    #[tokio::test]
    async fn wiring_arcade_accept_with_unproven_ancestry_never_admits_uncorroborated() {
        // Arcade echoes SEEN on the very first rung; the subject has an
        // unproven parent. Without a genuine corroborator accept the flow
        // must NOT return Accepted — and the corroboration it runs must be
        // the ancestry-primed batch.
        let kinds = std::cell::RefCell::new(Vec::new());
        let out = broadcast_efs_gated_with(
            2,
            "subject",
            |_rung| async { Ok(GateStep::Accepted) },
            |kind| {
                kinds.borrow_mut().push(kind);
                async { Err::<ArcOutcome, String>("corroborator unavailable".into()) }
            },
        )
        .await;
        assert!(
            out.is_err(),
            "Arcade's word alone must never admit an unproven-parent subject"
        );
        assert_eq!(
            *kinds.borrow(),
            vec![CorroborationKind::WithAncestry],
            "the accept claim must be corroborated WITH ancestry"
        );

        // …and WITH a genuine corroborator accept it admits — under OUR
        // subject txid, never the corroborator's echo.
        let out = broadcast_efs_gated_with(
            2,
            "subject",
            |_rung| async { Ok(GateStep::Accepted) },
            |_kind| async { Ok(ArcOutcome::Accepted("corroborator-echo".into())) },
        )
        .await;
        assert_eq!(out.unwrap(), ArcOutcome::Accepted("subject".into()));
    }

    #[tokio::test]
    async fn wiring_single_leg_accept_corroborates_subject_only_never_admits_uncorroborated() {
        // bsv-low#268: the single-EF fast path is DEAD. "Proven parents" is
        // bump PRESENCE — submitter-asserted, never validated — so a
        // fabricated parent bump made an unproven subject look single-EF and
        // ride an uncorroborated admit. Now a single-leg accept claim must
        // corroborate SUBJECT-ONLY, and without the corroborator's genuine
        // accept it must NOT admit.
        //
        // RED-VERIFY: restore `ArcOutcome::Accepted(_) if
        // !has_unproven_ancestry(efs_len) => Ok(outcome)` in
        // `gate_accept_claim_with` (backup copy) and this test fails.
        let kinds = std::cell::RefCell::new(Vec::new());
        let out = broadcast_efs_gated_with(
            1,
            "subject",
            |_rung| async { Ok(GateStep::Accepted) },
            |kind| {
                kinds.borrow_mut().push(kind);
                async { Err::<ArcOutcome, String>("corroborator unavailable".into()) }
            },
        )
        .await;
        assert!(
            out.is_err(),
            "a single-leg accept claim must never admit uncorroborated (#268)"
        );
        assert_eq!(
            *kinds.borrow(),
            vec![CorroborationKind::SubjectOnly],
            "single-leg accept must corroborate subject-only"
        );

        // …and WITH a genuine corroborator accept it admits — under OUR
        // subject txid, never the corroborator's echo.
        let out = broadcast_efs_gated_with(
            1,
            "subject",
            |_rung| async { Ok(GateStep::Accepted) },
            |_kind| async { Ok(ArcOutcome::Accepted("corroborator-echo".into())) },
        )
        .await;
        assert_eq!(out.unwrap(), ArcOutcome::Accepted("subject".into()));
    }

    #[tokio::test]
    async fn wiring_empty_efs_mined_claim_never_admits_without_corroboration() {
        // bsv-low#268, the worse sibling: a fake bump on the SUBJECT itself
        // made efs empty → "already mined — skipping broadcast" → ADMIT WITH
        // ZERO NETWORK CONTACT. Now the mined-claim must be corroborated; no
        // submit rung ever runs, and anything but a genuine corroborator
        // accept refuses admission (Err → 502, retryable — never a 422 off
        // one provider's word, never an admit-on-unknown).
        //
        // RED-VERIFY: restore the `if efs.is_empty() { return Ok(Accepted) }`
        // shortcut in `broadcast_efs_gated` (backup copy) — the worker path
        // then bypasses this control flow and the routes admit ungated.
        let rungs = std::cell::Cell::new(0usize);
        let kinds = std::cell::RefCell::new(Vec::new());
        // Inconclusive corroborator → refuse.
        let out = broadcast_efs_gated_with(
            0,
            "subject",
            |_rung| {
                rungs.set(rungs.get() + 1);
                async { Ok(GateStep::Accepted) }
            },
            |kind| {
                kinds.borrow_mut().push(kind);
                async { Err::<ArcOutcome, String>("corroborator unavailable".into()) }
            },
        )
        .await;
        let err = out.expect_err("an unconfirmable mined-claim must refuse admission");
        assert!(err.contains("not admitting"), "{err}");
        assert_eq!(rungs.get(), 0, "no submit rung may run for a mined-claim");
        assert_eq!(*kinds.borrow(), vec![CorroborationKind::MinedClaim]);

        // Corroborator REJECTED → still Err (one provider's word, #214) —
        // refuse admission without minting a definitive 422.
        let out = broadcast_efs_gated_with(
            0,
            "subject",
            |_rung| async { Ok(GateStep::Accepted) },
            |_kind| async { Ok(ArcOutcome::Rejected("orphan / missing inputs".into())) },
        )
        .await;
        assert!(
            out.is_err(),
            "a single-provider rejection must be Err/502, never Ok"
        );

        // Genuine corroborator accept (already-known/mined) → admit, under
        // OUR subject txid.
        let out = broadcast_efs_gated_with(
            0,
            "subject",
            |_rung| async { Ok(GateStep::Accepted) },
            |_kind| async { Ok(ArcOutcome::Accepted("echo".into())) },
        )
        .await;
        assert_eq!(out.unwrap(), ArcOutcome::Accepted("subject".into()));
    }

    #[test]
    fn refuse_bar_requires_both_providers_for_a_definitive_rejection() {
        // bsv-low#268 gate LOW-M: a definitive corroborator Rejected (which
        // corroborated_exhaustion turns into a terminal 422) now needs BOTH
        // hosts' word — the #214 two-provider bar in the refuse direction.
        let rej = |r: &str| Ok(ArcOutcome::Rejected(r.into()));
        let acc = || Ok(ArcOutcome::Accepted("ab".into()));
        let inc = |e: &str| Err::<ArcOutcome, String>(e.into());

        // Both rejected → definitive.
        assert!(matches!(
            fold_refuse_bar(rej("fee"), rej("fee")),
            Ok(ArcOutcome::Rejected(_))
        ));
        // Either accepted → accepted (a real network-accept always wins).
        assert!(matches!(
            fold_refuse_bar(rej("fee"), acc()),
            Ok(ArcOutcome::Accepted(_))
        ));
        assert!(matches!(
            fold_refuse_bar(inc("down"), acc()),
            Ok(ArcOutcome::Accepted(_))
        ));
        // ONE-SIDED rejection (other host inconclusive) → Err/502, never a
        // single-provider 422 — in BOTH orders.
        let e = fold_refuse_bar(rej("fee"), inc("down")).unwrap_err();
        assert!(e.contains("not definitive"), "{e}");
        let e = fold_refuse_bar(inc("down"), rej("fee")).unwrap_err();
        assert!(e.contains("not definitive"), "{e}");
        // Double inconclusive → Err.
        assert!(fold_refuse_bar(inc("a"), inc("b")).is_err());
    }

    #[test]
    fn mined_claim_fold_semantics() {
        // The pure #268 fold: accept → Accepted(subject); reject → Err
        // (refuse, retryable — never a single-provider 422); inconclusive →
        // Err. An "already mined" body IS the accept dress a genuinely mined
        // tx produces on re-broadcast.
        let subject = "2c50a257da80421f8a31c98bedc728b19e437edff0e2e84b74278f4b20d82256";
        let already = corroborator_verdict(422, "txn-already-known (code 257)");
        assert_eq!(
            corroborated_mined_claim(already, subject).unwrap(),
            ArcOutcome::Accepted(subject.to_string())
        );
        let rejected = corroborator_verdict(461, "unlock invalid");
        let err = corroborated_mined_claim(rejected, subject)
            .expect_err("one provider's rejection must not admit OR mint a 422");
        assert!(err.contains("#268"), "{err}");
        let inconclusive =
            corroborator_verdict(200, r#"{"txid":"ab","txStatus":"SEEN_IN_ORPHAN_MEMPOOL"}"#);
        assert!(corroborated_mined_claim(inconclusive, subject).is_err());
    }

    #[tokio::test]
    async fn wiring_orphan_poll_answer_routes_to_the_ancestry_rungs() {
        // Every Arcade answer is the ORPHAN view. The flow must skip the
        // subject-only resubmit AND the subject-only pre-batch corroborate,
        // go straight to the FULL BATCH, and (still orphan) end at the
        // ancestry-primed exhaustion corroboration — whose genuine accept
        // then admits.
        let rungs = std::cell::RefCell::new(Vec::new());
        let kinds = std::cell::RefCell::new(Vec::new());
        let out = broadcast_efs_gated_with(
            2,
            "subject",
            |rung| {
                rungs.borrow_mut().push(rung);
                let step = GateStep::Orphan("Arcade SEEN_IN_ORPHAN_MEMPOOL subject".into());
                async move { Ok(step) }
            },
            |kind| {
                kinds.borrow_mut().push(kind);
                async { Ok(ArcOutcome::Accepted("subject".into())) }
            },
        )
        .await;
        assert_eq!(out.unwrap(), ArcOutcome::Accepted("subject".into()));
        assert_eq!(
            *rungs.borrow(),
            vec![SubmitRung::SubjectOnly, SubmitRung::FullBatch],
            "orphan must skip the subject-only resubmit (attempt 2)"
        );
        assert_eq!(
            *kinds.borrow(),
            vec![CorroborationKind::WithAncestry],
            "orphan must skip the subject-only pre-batch corroborate"
        );
    }

    #[tokio::test]
    async fn wiring_async_reject_ladder_sequence_is_preserved() {
        // The pre-#267 ladder, untouched: subject-only → subject-only →
        // subject-only pre-batch corroborate (inconclusive) → full batch →
        // ancestry-primed exhaustion corroborate decides (#211/#214/#216).
        let rungs = std::cell::RefCell::new(Vec::new());
        let kinds = std::cell::RefCell::new(Vec::new());
        let out = broadcast_efs_gated_with(
            2,
            "subject",
            |rung| {
                rungs.borrow_mut().push(rung);
                let step = GateStep::AsyncRejected("Arcade REJECTED subject".into());
                async move { Ok(step) }
            },
            |kind| {
                kinds.borrow_mut().push(kind);
                let res = match kind {
                    CorroborationKind::SubjectOnly => {
                        Err("corroborator: missing parent — inconclusive".into())
                    }
                    CorroborationKind::WithAncestry => Ok(ArcOutcome::Accepted("subject".into())),
                    CorroborationKind::MinedClaim => {
                        Err("unreachable: no mined-claim on a 2-leg batch".into())
                    }
                };
                async move { res }
            },
        )
        .await;
        assert_eq!(out.unwrap(), ArcOutcome::Accepted("subject".into()));
        assert_eq!(
            *rungs.borrow(),
            vec![
                SubmitRung::SubjectOnly,
                SubmitRung::SubjectOnly,
                SubmitRung::FullBatch,
            ]
        );
        assert_eq!(
            *kinds.borrow(),
            vec![
                CorroborationKind::SubjectOnly,
                CorroborationKind::WithAncestry,
            ]
        );
    }

    // ── #267: SEEN_IN_ORPHAN_MEMPOOL short-circuits to the ancestry rungs ───

    #[test]
    fn orphan_status_classifies_orphan_never_reached_fatal_or_pending() {
        // Before #267 SEEN_IN_ORPHAN_MEMPOOL ranked 0 → Pending → 20s
        // timeout → 502, when the answer ("missing parents") was already in
        // hand. It must classify Orphan — and NEVER Reached, even though the
        // status text contains "SEEN" (the orphan check runs first).
        assert_eq!(
            classify_arcade_status("SEEN_IN_ORPHAN_MEMPOOL", ARCADE_GATE_STATUS),
            GateVerdict::Orphan
        );
        // The rank stays 0: an orphan view can never satisfy the SEEN gate
        // through the rank comparison either (belt and braces).
        assert_eq!(arcade_status_rank("SEEN_IN_ORPHAN_MEMPOOL"), 0);
        // Healthy and fatal classifications are untouched.
        assert_eq!(
            classify_arcade_status("SEEN_ON_NETWORK", ARCADE_GATE_STATUS),
            GateVerdict::Reached
        );
        assert_eq!(
            classify_arcade_status("REJECTED", ARCADE_GATE_STATUS),
            GateVerdict::Fatal
        );
        assert_eq!(
            classify_arcade_status("RECEIVED", ARCADE_GATE_STATUS),
            GateVerdict::Pending
        );
    }

    #[test]
    fn ladder_routes_orphan_to_the_ancestry_rungs_never_terminal() {
        // An orphan view alone may neither admit nor definitively reject —
        // it routes to the ancestry rungs (full-batch resubmit + the
        // ancestry-primed exhaustion corroboration).
        assert_eq!(
            ladder_step(
                GateStep::Orphan("Arcade SEEN_IN_ORPHAN_MEMPOOL ab".into()),
                "ab"
            ),
            Ladder::Ancestry
        );
    }

    #[test]
    fn concat_efs_is_dependency_order_concatenation() {
        // Subject-only vs full-batch bodies: attempt 1 submits ONE tx (the
        // subject's EF); the fallback batch is the concatenation of all legs.
        let efs = vec![
            EfTx {
                txid: "parent".into(),
                ef: vec![1, 2, 3],
            },
            EfTx {
                txid: "subject".into(),
                ef: vec![4, 5],
            },
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
        assert_eq!(
            arcade_fatal_reason("ab", "REJECTED", ""),
            "Arcade REJECTED ab"
        );
        assert_eq!(
            arcade_fatal_reason("ab", "REJECTED", "PROCESSING (4): failed to validate"),
            "Arcade REJECTED ab (PROCESSING (4): failed to validate)"
        );
    }

    #[test]
    fn verdict_accepts_2xx_ok_status() {
        let body = r#"{"txid":"ab","txStatus":"SEEN_ON_NETWORK","extraInfo":""}"#;
        assert_eq!(
            arc_verdict(200, body).unwrap(),
            ArcOutcome::Accepted("ab".into())
        );
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
        assert!(matches!(
            arc_verdict(200, body).unwrap(),
            ArcOutcome::Rejected(_)
        ));
    }

    #[test]
    fn verdict_4xx_verdict_class_is_a_definitive_rejection_never_fallback() {
        // The 460–479 class: a REAL per-tx verdict — the gate must refuse,
        // not shop for a second opinion.
        let v = arc_verdict(465, r#"{"detail":"fee too low"}"#).unwrap();
        assert!(matches!(v, ArcOutcome::Rejected(_)));
        assert!(matches!(
            arc_verdict(460, "bad").unwrap(),
            ArcOutcome::Rejected(_)
        ));
        assert!(matches!(
            arc_verdict(473, "policy").unwrap(),
            ArcOutcome::Rejected(_)
        ));
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
        assert!(matches!(
            arc_verdict(200, dressed).unwrap(),
            ArcOutcome::Accepted(_)
        ));
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
        assert!(
            collisions > 5,
            "vacuous corpus: only {collisions} '257' hits"
        );
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
            let two_xx =
                format!(r#"{{"txid":"{txid}","txStatus":"REJECTED","extraInfo":"{prose}"}}"#);
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
        assert!(already_known(&overlay_422(&format!(
            "ARC HTTP 465: {body}"
        ))));
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
        for s in [
            "MINED",
            "transaction already mined",
            "tx was mined in block",
        ] {
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
        assert_eq!(
            b.status_endpoint("deadbeef"),
            "https://h.example/tx/deadbeef"
        );
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
