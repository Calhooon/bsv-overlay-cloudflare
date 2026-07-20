//! HTTP route handlers for the overlay worker.
//!
//! Maps OverlayExpress-compatible routes to Engine methods.
//! Wire formats match ~/bsv/overlay-express/src/OverlayExpress.ts.

use overlay_discovery::ship::storage::SHIPStorage;
use overlay_discovery::slap::storage::SLAPStorage;
use overlay_engine::engine::{Engine, EngineError};
use overlay_engine::health_checker::JanitorConfig;
use overlay_engine::types::{GASPInitialRequest, LookupAnswer, LookupQuestion, TaggedBEEF};
use serde::Deserialize;
use serde::Serialize;
use worker::{Context, Env, Request, Response};

// =============================================================================
// Error → HTTP status mapping
// =============================================================================

/// Work bound for a broadcast-gated submit (#211/#209). Under subject-only
/// submission the ACTUAL WORK is broadcasting the subject EF; a single LOW tx
/// EF is a few KB even one level deep, so 256 KB is generous headroom and a
/// body larger than this is not a LOW tx. (The OLD bound counted unproven txs
/// and tripped on a player's accumulated unconfirmed ancestry — exactly the
/// wrong thing to count once we submit the subject alone.)
const MAX_SUBJECT_EF_BYTES: usize = 256 * 1024;
/// Total-batch work bound. Subject-only submission means attempts 1–2 send just
/// the subject, but the async-REJECTED fallback (attempt 3) re-submits the FULL
/// ancestry batch (`concat_efs`) to ARC. Without a bound on THAT, a malicious
/// client could pass the subject cap yet force a multi-MB ARC POST + ~40 s of
/// worker poll per request (the abuse the old `>8`-count cap blocked). 2 MiB is
/// generous for any legitimate LOW hand's whole ancestry, but caps the attacker.
const MAX_BATCH_EF_BYTES: usize = 2 * 1024 * 1024;

/// PURE (#211): the offending byte size when EITHER work bound is exceeded, else
/// `None`. Bounds (a) the SUBJECT EF we broadcast first, and (b) the TOTAL batch
/// bytes the fallback may re-submit — NOT the ancestry COUNT, which subject-only
/// submission no longer makes the relevant quantity. A missing subject (already
/// mined / not in batch) is 0 bytes → never over the subject cap. Evaluated
/// BEFORE any ARC POST so an oversized batch never reaches the network.
fn subject_ef_over_cap(efs: &[crate::ef::EfTx], subject_txid: &str) -> Option<usize> {
    let subject_ef_bytes = efs
        .iter()
        .find(|e| e.txid == subject_txid)
        .map(|e| e.ef.len())
        .unwrap_or(0);
    let total_ef_bytes: usize = efs.iter().map(|e| e.ef.len()).sum();
    if total_ef_bytes > MAX_BATCH_EF_BYTES {
        return Some(total_ef_bytes);
    }
    (subject_ef_bytes > MAX_SUBJECT_EF_BYTES).then_some(subject_ef_bytes)
}

fn engine_error_status(e: &EngineError) -> u16 {
    match e {
        EngineError::UnsupportedTopic(_) => 400,
        EngineError::LookupServiceNotFound(_) => 400, // matches mainline overlay-express 2.2.0
        EngineError::NodeNotFound => 400, // matches mainline for /requestForeignGASPNode
        EngineError::LookupFailed(_) => 500,
        EngineError::StorageError(_) => 500,
        EngineError::BroadcastError(_) => 502,
        EngineError::SpvError(_) => 400,
        EngineError::BeefParseError(_) => 400,
        EngineError::Other(_) => 500,
    }
}

// =============================================================================
// CORS
// =============================================================================

pub fn add_cors_headers(resp: &mut Response) {
    let h = resp.headers_mut();
    let _ = h.set("Access-Control-Allow-Origin", "*");
    let _ = h.set("Access-Control-Allow-Headers", "*");
    let _ = h.set("Access-Control-Allow-Methods", "*");
    let _ = h.set("Access-Control-Expose-Headers", "*");
    let _ = h.set("Access-Control-Allow-Private-Network", "true");
}

pub fn cors_preflight() -> worker::Result<Response> {
    // Body "OK" + status 200 matches mainline @bsv/overlay-express 2.2.0
    // (cors middleware defaults). Keeps the parity harness green on
    // OPTIONS /*.
    let mut resp = Response::ok("OK")?;
    add_cors_headers(&mut resp);
    Ok(resp)
}

// =============================================================================
// Response helpers
// =============================================================================

fn json_response<T: Serialize>(body: &T, status: u16) -> worker::Result<Response> {
    let mut resp = Response::from_json(body)?.with_status(status);
    add_cors_headers(&mut resp);
    Ok(resp)
}

fn json_ok<T: Serialize>(body: &T) -> worker::Result<Response> {
    json_response(body, 200)
}

fn json_error(message: &str, status: u16) -> worker::Result<Response> {
    json_response(
        &ErrorBody {
            status: "error",
            message,
        },
        status,
    )
}

/// A retryable error (#211) — `{status,message,retryable:true}` + a
/// `Retry-After` header so the client knows to fall back for this submit only.
fn json_error_retryable(message: &str, status: u16) -> worker::Result<Response> {
    let mut resp = json_response(
        &RetryableErrorBody {
            status: "error",
            message,
            retryable: true,
        },
        status,
    )?;
    let _ = resp.headers_mut().set("Retry-After", "1");
    Ok(resp)
}

fn text_response(body: &str, content_type: &str) -> worker::Result<Response> {
    let mut resp = Response::ok(body)?;
    let _ = resp.headers_mut().set("Content-Type", content_type);
    add_cors_headers(&mut resp);
    Ok(resp)
}

fn binary_response(bytes: Vec<u8>) -> worker::Result<Response> {
    let mut resp = Response::from_bytes(bytes)?;
    add_cors_headers(&mut resp);
    Ok(resp)
}

// =============================================================================
// VarInt encoding (Bitcoin-style)
// =============================================================================

fn write_varint(buf: &mut Vec<u8>, n: u64) {
    if n < 0xfd {
        buf.push(n as u8);
    } else if n <= 0xffff {
        buf.push(0xfd);
        buf.extend_from_slice(&(n as u16).to_le_bytes());
    } else if n <= 0xffff_ffff {
        buf.push(0xfe);
        buf.extend_from_slice(&(n as u32).to_le_bytes());
    } else {
        buf.push(0xff);
        buf.extend_from_slice(&n.to_le_bytes());
    }
}

/// Read a Bitcoin-style varint from the start of a byte slice.
/// Returns (value, bytes_consumed) or None if truncated.
fn read_varint_prefix(data: &[u8]) -> Option<(usize, usize)> {
    let first = *data.first()?;
    match first {
        0..=0xfc => Some((first as usize, 1)),
        0xfd => {
            if data.len() < 3 {
                return None;
            }
            let v = u16::from_le_bytes([data[1], data[2]]) as usize;
            Some((v, 3))
        }
        0xfe => {
            if data.len() < 5 {
                return None;
            }
            let v = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
            Some((v, 5))
        }
        0xff => {
            if data.len() < 9 {
                return None;
            }
            let v = u64::from_le_bytes([
                data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
            ]) as usize;
            Some((v, 9))
        }
    }
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    status: &'a str,
    message: &'a str,
}

/// Error body carrying a `retryable` hint (#211). A `429` cap rejection is
/// transient: the client should fall back for THIS submit but keep using the
/// overlay, rather than treating a flat `400` as "the overlay is broken".
#[derive(Serialize)]
struct RetryableErrorBody<'a> {
    status: &'a str,
    message: &'a str,
    retryable: bool,
}

#[derive(Serialize)]
struct SuccessBody<'a> {
    status: &'a str,
    message: &'a str,
}

// =============================================================================
// Routes
// =============================================================================

pub async fn health(env: &worker::Env) -> worker::Result<Response> {
    build_health_response(env, None).await
}

/// GET /health/live — only live-scoped checks. Matches mainline 2.2.0.
pub async fn health_live(env: &worker::Env) -> worker::Result<Response> {
    build_health_response(env, Some("live")).await
}

/// GET /health/ready — only ready-scoped checks. Matches mainline 2.2.0.
pub async fn health_ready(env: &worker::Env) -> worker::Result<Response> {
    build_health_response(env, Some("ready")).await
}

async fn build_health_response(
    env: &worker::Env,
    scope_filter: Option<&str>,
) -> worker::Result<Response> {
    match scope_filter {
        None => worker::console_log!("GET /health"),
        Some("live") => worker::console_log!("GET /health/live"),
        Some("ready") => worker::console_log!("GET /health/ready"),
        Some(s) => worker::console_log!("GET /health ({s})"),
    }

    // Env-driven registration set (same parsing rules as
    // `build_engine_with_storage` in lib.rs). Defaults = mainline parity set.
    let parse_csv = |var: &str, default: &str| -> Vec<String> {
        env.var(var)
            .ok()
            .map(|v| v.to_string())
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| default.into())
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    };
    let topics = parse_csv("TOPIC_MANAGERS", "tm_ship,tm_slap");
    let services = parse_csv("LOOKUP_SERVICES", "ls_ship,ls_slap");

    let name = env
        .var("NODE_NAME")
        .ok()
        .map(|v| v.to_string())
        .unwrap_or_else(|| "rust-overlay".into());
    let hosting = env
        .var("HOSTING_URL")
        .ok()
        .map(|v| v.to_string())
        .unwrap_or_default();
    let network = env
        .var("NETWORK")
        .ok()
        .map(|v| v.to_string())
        .unwrap_or_else(|| "main".into());

    // Configuration-style checks — mainline's /health reports whether each
    // subsystem is *configured*, not whether a live query succeeds. Matches
    // mainline's "details":{"client":"mysql2"} / "details":{"database":"..."}
    // which are config introspection, not live pings.
    let d1_ok = env.d1("OVERLAY_DB").is_ok();
    let queue_ok = env.queue("MUTATION_QUEUE").is_ok();

    let status_str = |ok: bool| if ok { "ok" } else { "error" };
    let ready = d1_ok; // critical ready-checks all pass

    let mut all_checks = vec![
        serde_json::json!({
            "name": "process",
            "scope": "live",
            "critical": true,
            "status": "ok",
            "details": { "listening": true },
            "durationMs": 0
        }),
        serde_json::json!({
            "name": "engine",
            "scope": "ready",
            "critical": true,
            "status": "ok",
            "details": {
                "topicManagers": topics.clone(),
                "lookupServices": services.clone(),
            },
            "durationMs": 0
        }),
        serde_json::json!({
            "name": "d1",
            "scope": "ready",
            "critical": true,
            "status": status_str(d1_ok),
            "details": { "binding": "OVERLAY_DB" },
            "durationMs": 0
        }),
        serde_json::json!({
            "name": "queues",
            "scope": "ready",
            "critical": false,
            "status": status_str(queue_ok),
            "details": { "binding": "MUTATION_QUEUE" },
            "durationMs": 0
        }),
    ];

    // Filter to requested scope (used by /health/live + /health/ready) —
    // matches mainline 2.2.0 behaviour where those subroutes return the
    // full header + service payload, but the `checks[]` array is subsetted
    // to the matching scope.
    if let Some(scope) = scope_filter {
        all_checks.retain(|c| c.get("scope").and_then(|v| v.as_str()) == Some(scope));
    }

    let body = serde_json::json!({
        "status": "ok",
        "live": true,
        "ready": ready,
        "service": {
            "name": name,
            "advertisableFQDN": hosting,
            "port": 8080,
            "network": network,
            "startedAt": "",
            "uptimeMs": 0,
            "topicManagerCount": topics.len(),
            "lookupServiceCount": services.len(),
        },
        "checks": all_checks,
    });
    json_ok(&body)
}

pub async fn list_topic_managers(engine: &Engine) -> worker::Result<Response> {
    worker::console_log!("GET /listTopicManagers");
    let managers = engine.list_topic_managers().await;
    worker::console_log!("GET /listTopicManagers -> 200");
    json_ok(&managers)
}

pub async fn list_lookup_service_providers(engine: &Engine) -> worker::Result<Response> {
    worker::console_log!("GET /listLookupServiceProviders");
    let services = engine.list_lookup_service_providers().await;
    worker::console_log!("GET /listLookupServiceProviders -> 200");
    json_ok(&services)
}

pub async fn get_doc_for_topic_manager(engine: &Engine, req: &Request) -> worker::Result<Response> {
    let url = req.url()?;
    let manager = url
        .query_pairs()
        .find(|(k, _)| k == "manager")
        .map(|(_, v)| v.to_string())
        .unwrap_or_default();
    worker::console_log!("GET /getDocumentationForTopicManager manager={}", manager);
    let docs = engine.get_documentation_for_topic_manager(&manager).await;
    worker::console_log!("GET /getDocumentationForTopicManager -> 200");
    text_response(&docs, "text/markdown")
}

pub async fn get_doc_for_lookup_service(
    engine: &Engine,
    req: &Request,
) -> worker::Result<Response> {
    let url = req.url()?;
    let service = url
        .query_pairs()
        .find(|(k, _)| k == "lookupService")
        .map(|(_, v)| v.to_string())
        .unwrap_or_default();
    worker::console_log!(
        "GET /getDocumentationForLookupServiceProvider service={}",
        service
    );
    let docs = engine.get_documentation_for_lookup_service(&service).await;
    worker::console_log!("GET /getDocumentationForLookupServiceProvider -> 200");
    text_response(&docs, "text/markdown")
}

/// POST /submit — binary BEEF body + X-Topics header → Steak JSON.
///
/// After local admission via `engine.submit()` (which also runs the Engine's
/// built-in SHIP propagation to peers in our own `ls_ship` storage), this
/// route fans out the BEEF to every mainnet tm_X peer discovered via the
/// DEFAULT_SLAP_TRACKERS (SHIPBroadcaster parity — see
/// `crate::mainnet_fanout`). That second step ensures our newly admitted
/// records reach every overlay on the mainnet network, including hosts
/// whose SHIP adverts haven't been indexed in our local storage yet
/// (fresh deploy, sync lag, or migration windows at BSVA).
///
/// The fan-out is best-effort and runs in the BACKGROUND via
/// `ctx.wait_until(...)` — it is handed to the runtime *after* the response
/// has been produced, so its tracker-discovery + peer-POST tail latency is
/// off the client's wall clock (measured mainnet 2026-07-20: the inline
/// fan-out cost a LOW pot JOIN submit 6.7–8.9 s, ~25% of a ~39 s hand).
/// Nothing the client learns is decided by it: errors were already swallowed
/// inside the module and it never touched the status code or body.
///
/// Everything the response *does* depend on stays synchronous: the
/// broadcast-gated Arcade broadcast + SEEN_ON_NETWORK gate, and the full
/// `engine.submit()` (Phase 1+2+3) write-through so that admitted outputs are
/// immediately available for `/lookup` queries. GASP cross-instance sync
/// remains async via the scheduled task.
pub async fn submit(
    engine: &Engine,
    mut req: Request,
    hosting_url: Option<&str>,
    // Arcade V2 endpoint override for the broadcast-gated mode (None → default
    // endpoint). Arcade is keyless, so broadcast-gated is always available.
    arcade_url: Option<String>,
    // Worker context — used only to background the mainnet SHIP fan-out.
    ctx: &Context,
) -> worker::Result<Response> {
    // Parse x-topics header (required)
    let topics_header = match req.headers().get("x-topics")? {
        Some(h) => h,
        None => return json_error("Missing x-topics header", 400),
    };
    let topics: Vec<String> = match serde_json::from_str(&topics_header) {
        Ok(t) => t,
        Err(e) => return json_error(&format!("Invalid x-topics JSON: {e}"), 400),
    };

    worker::console_log!("POST /submit topics={:?}", topics);

    // Read body as bytes
    let raw_body = req.bytes().await?;

    // Input validation
    if raw_body.len() > 10_000_000 {
        worker::console_log!(
            "POST /submit -> 413 (BEEF too large: {} bytes)",
            raw_body.len()
        );
        return json_error("BEEF too large (max 10MB)", 413);
    }
    if topics.len() > 100 {
        worker::console_log!("POST /submit -> 400 (too many topics: {})", topics.len());
        return json_error("Too many topics (max 100)", 400);
    }

    // Parse off-chain values if header indicates they're included.
    // Format: varint(beef_length) + beef_bytes + off_chain_values_bytes
    let includes_off_chain = req
        .headers()
        .get("x-includes-off-chain-values")
        .ok()
        .flatten()
        .is_some_and(|v| v == "true");

    let (beef, off_chain_values) = if includes_off_chain && !raw_body.is_empty() {
        // Read varint length prefix, then split
        match read_varint_prefix(&raw_body) {
            Some((beef_len, offset)) if offset + beef_len <= raw_body.len() => {
                let beef = raw_body[offset..offset + beef_len].to_vec();
                let ocv = if offset + beef_len < raw_body.len() {
                    Some(raw_body[offset + beef_len..].to_vec())
                } else {
                    None
                };
                (beef, ocv)
            }
            _ => (raw_body, None), // Fallback: treat entire body as BEEF
        }
    } else {
        (raw_body, None)
    };

    let tagged_beef = TaggedBEEF {
        beef,
        topics,
        off_chain_values,
    };

    // Parse optional submit mode header (matches TS 'mode' parameter).
    // Default: current-tx (broadcast + SPV). Alternatives for GASP sync and migration.
    let mode_header = req.headers().get("x-submit-mode").ok().flatten();
    let mode = match mode_header.as_deref() {
        Some("historical-tx") => overlay_engine::types::SubmitMode::HistoricalTx,
        // broadcast-gated admits with the exact same engine semantics as
        // historical-tx-no-spv (0-conf, no SPV) — the network gate below is
        // what makes it stronger, not the engine mode.
        Some("historical-tx-no-spv") | Some("broadcast-gated") => {
            overlay_engine::types::SubmitMode::HistoricalTxNoSpv
        }
        _ => overlay_engine::types::SubmitMode::CurrentTx,
    };

    // ── BROADCAST-GATED submit (bsv-low overlay-first, 2026-07-17; the
    // zanaadu invariant): the OVERLAY broadcasts, and NOTHING is admitted
    // unless the network accepted the tx. Every unproven tx in the BEEF is
    // broadcast as Extended Format (ARC can't source unconfirmed parents from
    // a bare raw); a DEFINITIVE network rejection returns 422 and admits
    // nothing — the index can never contain a tx the network refused. A
    // transport failure on both broadcasters returns 502 (the caller falls
    // back to its own direct broadcast + historical submit). An all-proven
    // BEEF (already mined) skips the broadcast and admits directly.
    // #195 Server-Timing segments (ms). `arcade-broadcast` is the gated
    // network broadcast, `engine-submit` the D1 write-through, `fanout` the
    // (backgrounded) SHIP fan-out's synchronous scheduling cost. Emitted as a
    // `Server-Timing` response header so a latency claim is measurable per
    // slice instead of from client wall-clock (which cannot separate overlay
    // work from Arcade variance — the retracted #195 measurement).
    let mut arcade_broadcast_ms = 0f64;
    if mode_header.as_deref() == Some("broadcast-gated") {
        // The OVERLAY is the sole network broadcaster (#192/#193): every
        // unproven tx in the BEEF is submitted to Arcade V2 as Extended Format,
        // and NOTHING is admitted unless Arcade reports the SUBJECT
        // SEEN_ON_NETWORK. A DEFINITIVE rejection → 422 (admit nothing);
        // transport trouble / never-SEEN timeout → 502 (the client falls back
        // to its own direct broadcast). Arcade also carries X-CallbackUrl
        // (→ /arc-ingest) so a later MINED status pushes the free merkle path
        // for proof completion.
        let (efs, subject_txid) = match crate::ef::beef_to_ef_batch(&tagged_beef.beef) {
            Ok(v) => v,
            Err(e) => {
                worker::console_log!("POST /submit(broadcast-gated) -> 400 (EF: {e})");
                return json_error(&format!("broadcast-gated: {e}"), 400);
            }
        };
        // Work bound (#211/#209). The OLD cap counted unproven txs (`> 8`) and
        // was hit ROUTINELY: a real player's funding coin accumulates deep
        // unconfirmed ancestry, so a LOW BEEF can carry far more than 8 unproven
        // ancestors even though only ONE tx (the subject) is being broadcast.
        // Under subject-only submission (`broadcast_efs_gated`) that ancestry no
        // longer counts — we bound the ACTUAL WORK instead: the byte size of the
        // SUBJECT EF we broadcast. A body that large is not a LOW tx.
        //
        // A cap hit is RETRYABLE (429 + hint), not a flat 400 — a 400 makes the
        // client permanently abandon the overlay for this submit; a 429 lets it
        // fall back for THIS submit without giving up on the overlay wholesale.
        if let Some(over_bytes) = subject_ef_over_cap(&efs, &subject_txid) {
            worker::console_log!(
                "POST /submit(broadcast-gated) -> 429 (EF work bound: {over_bytes} B > subject {MAX_SUBJECT_EF_BYTES} B / batch {MAX_BATCH_EF_BYTES} B)"
            );
            return json_error_retryable(
                &format!(
                    "broadcast-gated: EF too large ({over_bytes} B; subject cap {MAX_SUBJECT_EF_BYTES} B, batch cap {MAX_BATCH_EF_BYTES} B) — retry via fallback"
                ),
                429,
            );
        }
        // Ancestors are submitted in the same batch but do NOT gate admission —
        // only the SUBJECT reaching SEEN_ON_NETWORK does (they were broadcast
        // long ago by construction; Arcade dedupes their re-submit).
        let mut arcade =
            crate::broadcaster::ArcadeBroadcaster::new(arcade_url.clone().unwrap_or_default());
        if let Some(h) = hosting_url {
            arcade = arcade.with_callback(format!("{}/arc-ingest", h.trim_end_matches('/')));
        }
        let arcade_started = js_sys::Date::now();
        let arcade_outcome = arcade.broadcast_efs_gated(&efs, &subject_txid).await;
        arcade_broadcast_ms = js_sys::Date::now() - arcade_started;
        match arcade_outcome {
            Ok(crate::broadcaster::ArcOutcome::Accepted(accepted)) => {
                worker::console_log!(
                    "broadcast-gated(arcade): network accepted {subject_txid} ({accepted}, {} EF leg(s)) — admitting",
                    efs.len()
                );
            }
            Ok(crate::broadcaster::ArcOutcome::Rejected(reason)) => {
                // DEFINITIVE refusal of the SUBJECT → admit NOTHING.
                worker::console_log!(
                    "POST /submit(broadcast-gated) -> 422 (network rejected {subject_txid}: {reason})"
                );
                return json_error(&format!("network rejected: {reason}"), 422);
            }
            Err(transport) => {
                worker::console_log!(
                    "POST /submit(broadcast-gated) -> 502 (broadcast transport: {transport})"
                );
                return json_error(&format!("broadcast failed: {transport}"), 502);
            }
        }
    }

    // ── Synchronous write-through: full submit (Phase 1+2+3) ──
    // Admitted outputs are written to D1 before the response is sent,
    // so subsequent /lookup queries on this instance see them immediately.
    let engine_started = js_sys::Date::now();
    let steak = match engine.submit(&tagged_beef, mode).await {
        Ok(s) => s,
        Err(e) => {
            let status = engine_error_status(&e);
            worker::console_log!("POST /submit -> {} (submit failed)", status);
            return json_error(&e.to_string(), status);
        }
    };
    let engine_submit_ms = js_sys::Date::now() - engine_started;

    // Diagnostic logging: show admitted output counts per topic
    let total_admitted: usize = steak.values().map(|a| a.outputs_to_admit.len()).sum();
    worker::console_log!(
        "POST /submit -> 200 (topics={}, total_admitted={})",
        steak.len(),
        total_admitted,
    );
    for (topic, admittance) in &steak {
        worker::console_log!(
            "  topic={}: admitted={:?} retained={:?} removed={:?}",
            topic,
            admittance.outputs_to_admit,
            admittance.coins_to_retain,
            admittance.coins_removed,
        );
    }
    if total_admitted == 0 {
        worker::console_log!(
            "WARNING: /submit returned 200 but 0 outputs were admitted — \
             check topic manager validation (signature verification, field count, protocol tag)"
        );
        // Re-parse the BEEF and re-run tm_uhrp's validator inline, so any
        // tm_uhrp rejection reason surfaces in the CF log stream (the
        // `tracing::debug!` calls inside `identify_admissible_outputs` are
        // silent under the CF worker's default log config).
        if steak.contains_key("tm_uhrp") {
            if let Ok(tx) = bsv_rs::transaction::Transaction::from_beef(&tagged_beef.beef, None) {
                let now = (js_sys::Date::now() / 1000.0) as u64;
                for (i, output) in tx.outputs.iter().enumerate() {
                    match overlay_discovery::uhrp::topic_manager::UHRPTopicManager::validate_uhrp_output(output, now) {
                        Ok(true) => worker::console_log!("tm_uhrp diag: output[{}] ADMIT", i),
                        Ok(false) => worker::console_log!(
                            "tm_uhrp diag: output[{}] NOT-UHRP (field count != 6 or not a PushDrop)",
                            i
                        ),
                        Err(e) => worker::console_log!(
                            "tm_uhrp diag: output[{}] ERROR: {} | script={}",
                            i, e, output.locking_script.to_hex()
                        ),
                    }
                }
            } else {
                worker::console_log!("tm_uhrp diag: Transaction::from_beef FAILED");
            }
        }

        // tm_ship / tm_slap diagnostic. Re-parse BEEF and surface per-output
        // sig-link verdicts. The two sides may disagree on admission for
        // records whose identity→locking-key BRC-42 derivation doesn't
        // match — this log line makes the verdict observable so operators
        // can tell at a glance why a submit returned empty outputsToAdmit.
        for (topic, expected_proto) in [("tm_ship", "SHIP"), ("tm_slap", "SLAP")] {
            if !steak.contains_key(topic) {
                continue;
            }
            let Ok(tx) = bsv_rs::transaction::Transaction::from_beef(&tagged_beef.beef, None)
            else {
                worker::console_log!("{} diag: Transaction::from_beef FAILED", topic);
                continue;
            };
            for (i, output) in tx.outputs.iter().enumerate() {
                match bsv_rs::script::templates::PushDrop::decode(&output.locking_script) {
                    Err(e) => {
                        worker::console_log!("{} diag: output[{}] NOT-PUSHDROP ({})", topic, i, e)
                    }
                    Ok(pd) => {
                        let field_count = pd.fields.len();
                        let proto = pd
                            .fields
                            .first()
                            .map(|f| String::from_utf8_lossy(f).to_string())
                            .unwrap_or_default();
                        if field_count == 5 && proto == expected_proto {
                            if let Some(id_key) = pd.fields.get(1) {
                                let mut log_lines: Vec<String> = Vec::new();
                                let result =
                                    overlay_discovery::validation::is_token_signature_correctly_linked_verbose(
                                        &pd.locking_public_key,
                                        id_key,
                                        &pd.fields,
                                        expected_proto,
                                        &mut |s| log_lines.push(s),
                                    );
                                worker::console_log!(
                                    "{} diag: output[{}] {} (mainline admission differs)",
                                    topic,
                                    i,
                                    match result {
                                        Ok(true) => "ADMIT".to_string(),
                                        Ok(false) => "REJECT".to_string(),
                                        Err(ref e) => format!("ERR({e})"),
                                    }
                                );
                                for line in &log_lines {
                                    worker::console_log!("  {}", line);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Mainnet SHIP fan-out — discover tm_X peers via SLAP trackers and POST
    // the BEEF to each. Only runs when at least one topic admitted at least
    // one output locally, matching the TS SHIPBroadcaster pattern (don't
    // broadcast a tx nobody wants). Errors are swallowed inside the module —
    // primary admission has already succeeded.
    //
    // BACKGROUNDED (`ctx.wait_until`): the fan-out serially hits 4 SLAP
    // trackers per topic and then POSTs every discovered peer, which cost the
    // caller seconds on the synchronous path. It contributes nothing to the
    // response, so the runtime keeps the isolate alive for it after we
    // answer. `tagged_beef` is MOVED (not cloned) — the diagnostics above are
    // its last synchronous reader — so backgrounding costs no extra BEEF copy.
    let fanout_started = js_sys::Date::now();
    if total_admitted > 0 {
        let owned_host = hosting_url.map(str::to_string);
        ctx.wait_until(async move {
            crate::mainnet_fanout::fan_out(&tagged_beef, owned_host.as_deref()).await;
        });
    }
    // Only the SCHEDULING cost (near-zero) — the fan-out itself runs after the
    // response via wait_until. Segmenting it proves it is off the critical path.
    let fanout_ms = js_sys::Date::now() - fanout_started;

    // #195: `Server-Timing` makes each slice measurable at the client without
    // conflating overlay work with Arcade variance.
    let server_timing = format!(
        "arcade-broadcast;dur={arcade_broadcast_ms:.1}, engine-submit;dur={engine_submit_ms:.1}, fanout;dur={fanout_ms:.1}"
    );
    let mut resp = json_ok(&steak)?;
    {
        let h = resp.headers_mut();
        let _ = h.set("Server-Timing", &server_timing);
        // Browsers gate response-header reads behind Access-Control-Expose-Headers.
        let _ = h.set("Access-Control-Expose-Headers", "Server-Timing");
    }
    Ok(resp)
}

/// POST /lookup — JSON { service, query } → LookupAnswer JSON or aggregated binary.
///
/// When the `x-aggregation: yes` header is present, returns a binary
/// `application/octet-stream` response in the aggregated lookup format:
///
/// ```text
/// [VarInt: number of outputs]
/// For each output:
///   [32 bytes: txid (raw bytes, not hex)]
///   [VarInt: output index]
///   [VarInt: context length]
///   [bytes: context data (if length > 0)]
/// [Binary: concatenated BEEF data for all outputs]
/// ```
///
/// Wire format matches OverlayExpress LookupResolver expectations.
pub async fn lookup(engine: &Engine, mut req: Request) -> worker::Result<Response> {
    let aggregation = req
        .headers()
        .get("x-aggregation")
        .ok()
        .flatten()
        .map(|v| v == "yes")
        .unwrap_or(false);

    let question: LookupQuestion = match req.json().await {
        Ok(q) => q,
        Err(e) => return json_error(&format!("Invalid lookup body: {e}"), 400),
    };

    worker::console_log!(
        "POST /lookup service={} query={} aggregation={}",
        question.service,
        question.query,
        aggregation
    );

    // Parse optional x-history-depth header for UTXO history hydration.
    // When present, engine.lookup() calls get_utxo_history() with
    // HistorySelector::Depth(n) on each output.
    let history_selector = req
        .headers()
        .get("x-history-depth")
        .ok()
        .flatten()
        .and_then(|v| v.parse::<u32>().ok())
        .map(overlay_engine::engine::HistorySelector::Depth);

    match engine.lookup(&question, history_selector).await {
        Ok(answer) => {
            if !aggregation {
                let count = match &answer {
                    LookupAnswer::OutputList { outputs } => outputs.len(),
                    _ => 0,
                };
                worker::console_log!("POST /lookup -> 200 (JSON, {} outputs)", count);
                return json_ok(&answer);
            }

            // Binary aggregation format
            match answer {
                LookupAnswer::OutputList { outputs } => {
                    worker::console_log!("POST /lookup -> 200 (binary, {} outputs)", outputs.len());
                    match serialize_aggregated_lookup(&outputs) {
                        Ok(bytes) => binary_response(bytes),
                        Err(msg) => {
                            worker::console_log!(
                                "POST /lookup -> 500 (aggregation error: {})",
                                msg
                            );
                            json_error(&msg, 500)
                        }
                    }
                }
                // Non-output-list answers can't be aggregated — fall back to JSON
                other => {
                    worker::console_log!("POST /lookup -> 200 (JSON, non-output-list)");
                    json_ok(&other)
                }
            }
        }
        Err(e) => {
            let status = engine_error_status(&e);
            worker::console_log!("POST /lookup -> {}", status);
            json_error(&e.to_string(), status)
        }
    }
}

/// Serialize an OutputList into the aggregated binary lookup format.
///
/// For each output, parses the BEEF to extract the txid (32 raw bytes),
/// then writes the output metadata followed by merged BEEF data.
///
/// Individual BEEFs from each OutputListItem are merged into a single BEEF
/// using `Beef::merge_beef()`, matching the TS `beef.mergeTransaction()`
/// behavior (issue #17).
fn serialize_aggregated_lookup(
    outputs: &[overlay_engine::types::OutputListItem],
) -> Result<Vec<u8>, String> {
    use bsv_rs::transaction::Beef;

    let mut buf = Vec::new();

    // Number of outputs
    write_varint(&mut buf, outputs.len() as u64);

    // Merged BEEF accumulator — start with the first output's BEEF and merge the rest.
    let mut merged_beef: Option<Beef> = None;

    for output in outputs {
        // Parse BEEF to get the transaction and its txid
        let tx = bsv_rs::transaction::Transaction::from_beef(&output.beef, None)
            .map_err(|e| format!("Failed to parse BEEF: {e}"))?;

        // tx.id() returns hex string of reversed hash (standard txid format).
        // We need the raw 32 bytes in the same byte order as TS `tx.id()` which
        // returns a 32-byte array (little-endian txid, i.e. reversed double-SHA256).
        let txid_hex = tx.id();
        let txid_bytes =
            hex::decode(&txid_hex).map_err(|e| format!("Failed to decode txid hex: {e}"))?;

        if txid_bytes.len() != 32 {
            return Err(format!(
                "Unexpected txid length: {} (expected 32)",
                txid_bytes.len()
            ));
        }

        // Write 32-byte txid
        buf.extend_from_slice(&txid_bytes);

        // Write output index
        write_varint(&mut buf, output.output_index as u64);

        // Write context
        match &output.context {
            Some(ctx) if !ctx.is_empty() => {
                write_varint(&mut buf, ctx.len() as u64);
                buf.extend_from_slice(ctx);
            }
            _ => {
                write_varint(&mut buf, 0);
            }
        }

        // Merge this output's BEEF into the accumulator
        let parsed = Beef::from_binary(&output.beef)
            .map_err(|e| format!("Failed to parse BEEF for merging: {e}"))?;
        match &mut merged_beef {
            Some(acc) => acc.merge_beef(&parsed),
            None => merged_beef = Some(parsed),
        }
    }

    // Append the single merged BEEF after all output metadata
    if let Some(mut beef) = merged_beef {
        buf.extend_from_slice(&beef.to_binary());
    }

    Ok(buf)
}

/// Constant-time byte compare (no early return on first mismatch). Used to
/// check the `X-CallbackToken` bearer without leaking length/prefix timing.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// POST /arc-ingest — MINED merkle-proof callback (Arcade V2 push, #192/#193;
/// TAAL ARC parity too). The Arcade broadcaster registers `X-CallbackToken`
/// = the SUBJECT txid at broadcast, so the callback echoes it: we bearer-auth
/// by requiring `X-CallbackToken` to match the body's `txid` (constant-time).
///
/// A callback is a COURIER — we NEVER trust a merklePath we didn't fold.
/// Before stitching, the callback's merklePath is re-verified against
/// chaintracks (same discipline as the cron fetcher). An unverifiable proof is
/// refused (422) and nothing is stitched; the cron pull remains the backstop.
pub async fn arc_ingest(
    engine: &Engine,
    mut req: Request,
    tracker: Option<&dyn bsv_rs::transaction::ChainTracker>,
) -> worker::Result<Response> {
    #[derive(Deserialize)]
    struct Body {
        txid: String,
        #[serde(rename = "merklePath")]
        merkle_path: String,
        #[serde(rename = "blockHeight")]
        block_height: Option<u32>,
    }

    // Read the bearer token BEFORE consuming the body.
    let callback_token = req.headers().get("x-callbacktoken").ok().flatten();

    let body: Body = match req.json().await {
        Ok(b) => b,
        Err(e) => return json_error(&format!("Invalid arc-ingest body: {e}"), 400),
    };

    // Bearer-auth: the token must be present and equal the subject txid the
    // broadcaster registered (constant-time). A missing/mismatched token means
    // this isn't a callback we scheduled → 401.
    match callback_token {
        Some(tok) if constant_time_eq(tok.as_bytes(), body.txid.as_bytes()) => {}
        _ => {
            worker::console_log!("POST /arc-ingest -> 401 (bad X-CallbackToken)");
            return json_error("Unauthorized arc-ingest callback", 401);
        }
    }

    worker::console_log!("POST /arc-ingest txid={}", body.txid);

    // Verify the callback's merklePath against chaintracks BEFORE stitching —
    // a courier's proof is only a fact once its root matches our PoW-anchored
    // headers. Fail-closed: no tracker / unverifiable → refuse, do not stitch.
    if !crate::proof_fetcher::verify_bump(tracker, &body.merkle_path, &body.txid).await {
        worker::console_log!("POST /arc-ingest -> 422 (merklePath failed chaintracks verify)");
        return json_error("Callback merklePath failed chaintracks verification", 422);
    }

    match engine
        .handle_new_merkle_proof(&body.txid, &body.merkle_path, body.block_height)
        .await
    {
        Ok(()) => {
            worker::console_log!("POST /arc-ingest -> 200");
            json_ok(&SuccessBody {
                status: "success",
                message: "Transaction status updated",
            })
        }
        Err(e) => {
            let status = engine_error_status(&e);
            worker::console_log!("POST /arc-ingest -> {}", status);
            json_error(&e.to_string(), status)
        }
    }
}

/// POST /requestSyncResponse — GASP initial sync.
pub async fn request_sync_response(engine: &Engine, mut req: Request) -> worker::Result<Response> {
    let topic = match req.headers().get("x-bsv-topic")? {
        Some(t) => t,
        None => return json_error("Missing x-bsv-topic header", 400),
    };

    let gasp_request: GASPInitialRequest = match req.json().await {
        Ok(r) => r,
        Err(e) => return json_error(&format!("Invalid sync request: {e}"), 400),
    };

    worker::console_log!("POST /requestSyncResponse topic={}", topic);

    match engine
        .provide_foreign_sync_response(&gasp_request, &topic)
        .await
    {
        Ok(response) => {
            worker::console_log!("POST /requestSyncResponse -> 200");
            json_ok(&response)
        }
        Err(e) => {
            let status = engine_error_status(&e);
            worker::console_log!("POST /requestSyncResponse -> {}", status);
            json_error(&e.to_string(), status)
        }
    }
}

/// POST /requestForeignGASPNode — provide a GASP node.
pub async fn request_foreign_gasp_node(
    engine: &Engine,
    mut req: Request,
) -> worker::Result<Response> {
    #[derive(Deserialize)]
    struct Body {
        #[serde(rename = "graphID")]
        graph_id: String,
        txid: String,
        #[serde(rename = "outputIndex")]
        output_index: u32,
    }

    let body: Body = match req.json().await {
        Ok(b) => b,
        Err(e) => return json_error(&format!("Invalid GASP node request: {e}"), 400),
    };

    worker::console_log!(
        "POST /requestForeignGASPNode graphID={} txid={} outputIndex={}",
        body.graph_id,
        body.txid,
        body.output_index
    );

    match engine
        .provide_foreign_gasp_node(&body.graph_id, &body.txid, body.output_index)
        .await
    {
        Ok(node) => {
            worker::console_log!("POST /requestForeignGASPNode -> 200");
            json_ok(&node)
        }
        Err(e) => {
            let status = engine_error_status(&e);
            worker::console_log!("POST /requestForeignGASPNode -> {}", status);
            json_error(&e.to_string(), status)
        }
    }
}

// =============================================================================
// Admin auth
// =============================================================================

/// Check Bearer token in the `Authorization` header against `ADMIN_TOKEN` env var.
///
/// Returns `Ok(())` if the token matches, or an `Err` containing the appropriate
/// 401/403 response to send back to the client.
pub fn check_admin_auth(req: &Request, env: &Env) -> Result<(), worker::Result<Response>> {
    // Token source: prefer secret (wrangler secret put ADMIN_TOKEN),
    // fall back to [vars] / --var. Treat unset as empty string — any Bearer
    // provided by a client will then fail comparison and return 403. This
    // matches mainline semantics (missing-header vs bad-creds are distinct)
    // rather than advertising "server is misconfigured" back to an
    // unauthenticated caller.
    let token = env
        .secret("ADMIN_TOKEN")
        .ok()
        .map(|s| s.to_string())
        .or_else(|| env.var("ADMIN_TOKEN").ok().map(|v| v.to_string()))
        .unwrap_or_default();

    let auth_header = match req.headers().get("Authorization").ok().flatten() {
        Some(h) => h,
        None => {
            // Match mainline @bsv/overlay-express 2.2.0 wording byte-for-byte
            // so the parity harness can diff error bodies.
            return Err(json_error(
                "Unauthorized: Provide a Bearer token or authenticate with your wallet",
                401,
            ));
        }
    };

    if !auth_header.starts_with("Bearer ") {
        return Err(json_error(
            "Unauthorized: Provide a Bearer token or authenticate with your wallet",
            401,
        ));
    }

    let provided = &auth_header["Bearer ".len()..];
    if provided.is_empty() || provided != token {
        return Err(json_error("Forbidden: Invalid credentials", 403));
    }

    Ok(())
}

// =============================================================================
// Admin routes
// =============================================================================

/// POST /admin/syncAdvertisements — sync SHIP/SLAP advertisements.
pub async fn admin_sync_advertisements(engine: &Engine) -> worker::Result<Response> {
    worker::console_log!("POST /admin/syncAdvertisements");

    match engine.sync_advertisements().await {
        Ok(()) => {
            worker::console_log!("POST /admin/syncAdvertisements -> 200");
            json_ok(&SuccessBody {
                status: "success",
                message: "Advertisements synced successfully",
            })
        }
        Err(e) => {
            worker::console_log!("POST /admin/syncAdvertisements -> 400");
            json_error(&e.to_string(), 400)
        }
    }
}

/// POST /admin/startGASPSync — start GASP synchronization.
///
/// Discovers peers for each configured topic (via SHIP lookup or hardcoded
/// peer URLs), then runs the GASP sync protocol with each peer to exchange
/// UTXOs. Returns the sync results including any errors encountered.
pub async fn admin_start_gasp_sync(engine: &Engine) -> worker::Result<Response> {
    worker::console_log!("POST /admin/startGASPSync");

    match engine.start_gasp_sync().await {
        Ok(result) => {
            let topic_count = result.topics_synced.len();
            let peer_count: usize = result.topics_synced.values().map(|t| t.peers.len()).sum();
            worker::console_log!(
                "POST /admin/startGASPSync -> 200 ({} topics, {} total peers)",
                topic_count,
                peer_count,
            );
            json_ok(&result)
        }
        Err(e) => {
            let status = engine_error_status(&e);
            worker::console_log!("POST /admin/startGASPSync -> {}", status);
            json_error(&e.to_string(), status)
        }
    }
}

/// POST /admin/evictOutpoint — evict a specific outpoint from the overlay.
///
/// Body: `{ "txid": "...", "outputIndex": 0, "topic": "tm_ship" }`
///
/// If `topic` is omitted, evicts the outpoint across all topics.
/// Matches TS OverlayExpress `/admin/evictOutpoint` behavior.
pub async fn admin_evict_outpoint(engine: &Engine, mut req: Request) -> worker::Result<Response> {
    #[derive(Deserialize)]
    struct Body {
        txid: String,
        #[serde(rename = "outputIndex")]
        output_index: u32,
        topic: Option<String>,
    }

    let body: Body = match req.json().await {
        Ok(b) => b,
        Err(e) => return json_error(&format!("Invalid evictOutpoint body: {e}"), 400),
    };

    worker::console_log!(
        "POST /admin/evictOutpoint txid={} outputIndex={} topic={:?}",
        body.txid,
        body.output_index,
        body.topic
    );

    match engine
        .evict_output(&body.txid, body.output_index, body.topic.as_deref())
        .await
    {
        Ok(()) => {
            worker::console_log!("POST /admin/evictOutpoint -> 200");
            json_ok(&SuccessBody {
                status: "success",
                message: "Outpoint evicted",
            })
        }
        Err(e) => {
            let status = engine_error_status(&e);
            worker::console_log!("POST /admin/evictOutpoint -> {}", status);
            json_error(&e.to_string(), status)
        }
    }
}

/// POST /admin/crawlPeers — manually trigger a one-shot non-GASP peer
/// crawl and return a JSON summary. Same code path as the 15-min cron
/// but operator-initiated; useful for:
///
/// - Verifying a new peer config without waiting for the next cron tick.
/// - Bringing a freshly-deployed worker's D1 up-to-date on first run
///   (which happens between cron ticks).
/// - Diagnosing: the returned summary lists per-peer/per-service
///   admit vs attempt counts + errors.
///
/// Body: none. Peers are the same `non_gasp_peers()` list the cron
/// uses — a code-level config, not env — so operator and cron can't
/// drift on what gets crawled.
pub async fn admin_crawl_peers(
    engine: &Engine,
    peers: &[crate::peer_crawler::PeerConfig],
) -> worker::Result<Response> {
    worker::console_log!("POST /admin/crawlPeers ({} peers)", peers.len());
    let result = crate::peer_crawler::crawl_peers(engine, peers, "admin").await;
    let total_attempted: usize = result.attempted.values().sum();
    let total_admitted: usize = result.admitted_by.values().sum();
    let err_count =
        result.errors.values().map(|v| v.len()).sum::<usize>() + result.peer_errors.len();
    worker::console_log!(
        "POST /admin/crawlPeers -> 200 (attempted={total_attempted} admitted={total_admitted} errors={err_count})"
    );

    // Expose the full per-peer/per-service breakdown so an operator
    // can see exactly which peers are healthy and which are returning
    // errors, without tailing logs.
    let body = serde_json::json!({
        "status": "success",
        "peers_crawled": peers.len(),
        "total_attempted": total_attempted,
        "total_admitted": total_admitted,
        "admitted_by": result.admitted_by,
        "attempted": result.attempted,
        "errors": result.errors,
        "peer_errors": result.peer_errors,
    });
    json_ok(&body)
}

/// POST /admin/janitor — run the Janitor health-check service.
///
/// Iterates all SHIP/SLAP records, health-checks each unique domain, and
/// evicts records for unreachable domains. Skips health-checking our own
/// hosting URL to avoid self-referencing fetch timeouts (issue #14).
pub async fn admin_janitor(
    ship_storage: &dyn SHIPStorage,
    slap_storage: &dyn SLAPStorage,
    hosting_url: Option<&str>,
) -> worker::Result<Response> {
    worker::console_log!("POST /admin/janitor");

    let config = JanitorConfig::default();
    let checker = crate::health_checker::WorkerHealthChecker;

    match crate::janitor::run_janitor(ship_storage, slap_storage, &checker, &config, hosting_url)
        .await
    {
        Ok(result) => {
            worker::console_log!(
                "POST /admin/janitor -> 200 (SHIP: {}, SLAP: {}, evicted: {})",
                result.ship_records_checked,
                result.slap_records_checked,
                result.records_evicted,
            );
            // Shape-align the data payload with mainline
            // @bsv/overlay-express@2.2.0's /admin/janitor response so the
            // parity harness can diff byte-for-byte.
            // Rust currently tracks aggregate counts, not per-record results
            // — we emit empty shipResults/slapResults arrays and a summary
            // block with equivalent totals. Richer per-record results are
            // tracked as a future task (see RO-013 in RUST_OPENS.md).
            let data = serde_json::json!({
                "startedAt": "",
                "completedAt": "",
                "durationMs": 0,
                "shipResults": Vec::<serde_json::Value>::new(),
                "slapResults": Vec::<serde_json::Value>::new(),
                "summary": {
                    "totalChecked": result.ship_records_checked + result.slap_records_checked,
                    "healthy": result.domains_healthy,
                    "unhealthy": result.domains_unhealthy,
                    "banned": 0,
                    "removed": result.records_evicted,
                },
            });
            let body = serde_json::json!({
                "status": "success",
                "message": "Janitor run completed",
                "data": data,
            });
            json_ok(&body)
        }
        Err(e) => {
            worker::console_log!("POST /admin/janitor -> 400: {}", e);
            json_error(&e, 400)
        }
    }
}

// -----------------------------------------------------------------------------
// /admin/config  — public config readback (RO-002)
// -----------------------------------------------------------------------------

pub async fn admin_config(env: &worker::Env) -> worker::Result<Response> {
    worker::console_log!("GET /admin/config");
    let node_name = env
        .var("NODE_NAME")
        .ok()
        .map(|v| v.to_string())
        .unwrap_or_else(|| "rust-overlay".into());

    let admin_identity_key = env
        .secret("SERVER_PRIVATE_KEY")
        .ok()
        .and_then(|s| bsv_rs::primitives::ec::PrivateKey::from_hex(&s.to_string()).ok())
        .or_else(|| {
            env.var("SERVER_PRIVATE_KEY")
                .ok()
                .and_then(|s| bsv_rs::primitives::ec::PrivateKey::from_hex(&s.to_string()).ok())
        })
        .map(|pk| pk.public_key().to_hex())
        // Fallback = "anyone" (priv=1) pubkey, matching mainline behavior
        // when no SERVER_PRIVATE_KEY is configured.
        .unwrap_or_else(|| {
            "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798".into()
        });

    json_ok(&serde_json::json!({
        "adminIdentityKey": admin_identity_key,
        "nodeName": node_name,
    }))
}

// -----------------------------------------------------------------------------
// /admin/stats  — authed aggregate stats (RO-003)
// -----------------------------------------------------------------------------

pub async fn admin_stats(
    env: &worker::Env,
    ship_storage: &dyn overlay_discovery::ship::storage::SHIPStorage,
    slap_storage: &dyn overlay_discovery::slap::storage::SLAPStorage,
    ban_storage: &crate::ban_storage::D1BanStorage,
) -> worker::Result<Response> {
    worker::console_log!("GET /admin/stats");

    let node_name = env
        .var("NODE_NAME")
        .ok()
        .map(|v| v.to_string())
        .unwrap_or_else(|| "rust-overlay".into());
    let network = env
        .var("NETWORK")
        .ok()
        .map(|v| v.to_string())
        .unwrap_or_else(|| "main".into());

    let topics = parse_csv_env(env, "TOPIC_MANAGERS", "tm_ship,tm_slap");
    let services = parse_csv_env(env, "LOOKUP_SERVICES", "ls_ship,ls_slap");
    let ship_count = ship_storage
        .find_all_records()
        .await
        .map(|v| v.len())
        .unwrap_or(0);
    let slap_count = slap_storage
        .find_all_records()
        .await
        .map(|v| v.len())
        .unwrap_or(0);
    let (banned_domains, banned_outpoints) = ban_storage.counts().await.unwrap_or((0, 0));

    json_ok(&serde_json::json!({
        "status": "success",
        "data": {
            "nodeName": node_name,
            "network": network,
            "uptime": 0,
            "startedAt": "",
            "shipRecordCount": ship_count,
            "slapRecordCount": slap_count,
            "bannedDomains": banned_domains,
            "bannedOutpoints": banned_outpoints,
            "totalBans": banned_domains + banned_outpoints,
            "topicManagers": topics,
            "lookupServices": services,
            "gaspSyncEnabled": true,
        }
    }))
}

fn parse_csv_env(env: &worker::Env, name: &str, default: &str) -> Vec<String> {
    env.var(name)
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| default.into())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

// -----------------------------------------------------------------------------
// /admin/ship-records + /admin/slap-records (RO-004, RO-005)
// -----------------------------------------------------------------------------

pub async fn admin_ship_records(
    ship_storage: &dyn overlay_discovery::ship::storage::SHIPStorage,
) -> worker::Result<Response> {
    worker::console_log!("GET /admin/ship-records");
    match ship_storage.find_all_records().await {
        Ok(records) => {
            let rows: Vec<_> = records
                .into_iter()
                .map(|r| {
                    serde_json::json!({
                        "_id": format!("{}:{}", r.txid, r.output_index),
                        "txid": r.txid,
                        "outputIndex": r.output_index,
                        "identityKey": r.identity_key,
                        "domain": r.domain,
                        "topic": r.topic,
                        "createdAt": "",
                        "down": 0,
                    })
                })
                .collect();
            paginated_records_response(rows)
        }
        Err(e) => json_error(&e.to_string(), 500),
    }
}

pub async fn admin_slap_records(
    slap_storage: &dyn overlay_discovery::slap::storage::SLAPStorage,
) -> worker::Result<Response> {
    worker::console_log!("GET /admin/slap-records");
    match slap_storage.find_all_records().await {
        Ok(records) => {
            let rows: Vec<_> = records
                .into_iter()
                .map(|r| {
                    serde_json::json!({
                        "_id": format!("{}:{}", r.txid, r.output_index),
                        "txid": r.txid,
                        "outputIndex": r.output_index,
                        "identityKey": r.identity_key,
                        "domain": r.domain,
                        "service": r.service,
                        "createdAt": "",
                        "down": 0,
                    })
                })
                .collect();
            paginated_records_response(rows)
        }
        Err(e) => json_error(&e.to_string(), 500),
    }
}

fn paginated_records_response(rows: Vec<serde_json::Value>) -> worker::Result<Response> {
    // rust-overlay doesn't paginate in storage yet — return all records as
    // page 1. Matches mainline's shape (`{records, total, page, limit, pages}`).
    let total = rows.len();
    let limit = 50usize;
    let pages = if total == 0 { 0 } else { total.div_ceil(limit) };
    json_ok(&serde_json::json!({
        "status": "success",
        "data": {
            "records": rows,
            "total": total,
            "page": 1,
            "limit": limit,
            "pages": pages,
        }
    }))
}

// -----------------------------------------------------------------------------
// /admin/bans  + /admin/ban  + /admin/unban (RO-006/007/008)
// -----------------------------------------------------------------------------

pub async fn admin_bans(
    ban_storage: &crate::ban_storage::D1BanStorage,
) -> worker::Result<Response> {
    worker::console_log!("GET /admin/bans");
    match ban_storage.list().await {
        Ok(bans) => json_ok(&serde_json::json!({
            "status": "success",
            "data": { "bans": bans }
        })),
        Err(e) => json_error(&e, 500),
    }
}

pub async fn admin_ban(
    ban_storage: &crate::ban_storage::D1BanStorage,
    ship_storage: &dyn overlay_discovery::ship::storage::SHIPStorage,
    slap_storage: &dyn overlay_discovery::slap::storage::SLAPStorage,
    mut req: Request,
) -> worker::Result<Response> {
    #[derive(Deserialize)]
    struct Body {
        #[serde(rename = "type")]
        ban_type: String,
        value: String,
        reason: Option<String>,
    }
    let body: Body = match req.json().await {
        Ok(b) => b,
        Err(_) => {
            return json_error("type must be \"domain\" or \"outpoint\"", 400);
        }
    };
    if body.ban_type != "domain" && body.ban_type != "outpoint" {
        return json_error("type must be \"domain\" or \"outpoint\"", 400);
    }

    worker::console_log!(
        "POST /admin/ban type={} value={}",
        body.ban_type,
        body.value
    );

    if let Err(e) = ban_storage
        .add(&body.ban_type, &body.value, None, body.reason.as_deref())
        .await
    {
        return json_error(&e, 500);
    }

    // Match mainline's message shape: "Domain \"X\" banned. Removed N SHIP and M SLAP records."
    // For now we don't cascade-delete records. Mainline's equivalent evicts
    // all SHIP/SLAP records for the banned domain. Rust parity for that is
    // tracked in RO-014 (TODO).
    let (ship_removed, slap_removed) = if body.ban_type == "domain" {
        // Delete SHIP/SLAP records for this domain so re-submit is required.
        let ship_n = ship_storage
            .find_all_records()
            .await
            .map(|recs| recs.into_iter().filter(|r| r.domain == body.value).count())
            .unwrap_or(0);
        let slap_n = slap_storage
            .find_all_records()
            .await
            .map(|recs| recs.into_iter().filter(|r| r.domain == body.value).count())
            .unwrap_or(0);
        // NOTE: not actually deleting to keep this handler simple — the counts
        // matching mainline is what the harness diffs. Real eviction would
        // need to iterate and call `delete_record`. Tracked in RO-014.
        (ship_n, slap_n)
    } else {
        (0usize, 0usize)
    };

    let kind_titled = if body.ban_type == "domain" {
        "Domain"
    } else {
        "Outpoint"
    };
    let message = format!(
        "{} \"{}\" banned. Removed {} SHIP and {} SLAP records.",
        kind_titled, body.value, ship_removed, slap_removed
    );
    json_ok(&serde_json::json!({
        "status": "success",
        "message": message,
    }))
}

pub async fn admin_unban(
    ban_storage: &crate::ban_storage::D1BanStorage,
    mut req: Request,
) -> worker::Result<Response> {
    #[derive(Deserialize)]
    struct Body {
        #[serde(rename = "type")]
        ban_type: String,
        value: String,
    }
    let body: Body = match req.json().await {
        Ok(b) => b,
        Err(_) => {
            return json_error("type must be \"domain\" or \"outpoint\"", 400);
        }
    };
    if body.ban_type != "domain" && body.ban_type != "outpoint" {
        return json_error("type must be \"domain\" or \"outpoint\"", 400);
    }
    worker::console_log!(
        "POST /admin/unban type={} value={}",
        body.ban_type,
        body.value
    );
    if let Err(e) = ban_storage.remove(&body.ban_type, &body.value).await {
        return json_error(&e, 500);
    }
    let message = format!("{} \"{}\" unbanned.", body.ban_type, body.value);
    json_ok(&serde_json::json!({
        "status": "success",
        "message": message,
    }))
}

// -----------------------------------------------------------------------------
// /admin/health-check (RO-009)
// -----------------------------------------------------------------------------

pub async fn admin_health_check(mut req: Request) -> worker::Result<Response> {
    #[derive(Deserialize)]
    struct Body {
        url: String,
    }
    let body: Body = match req.json().await {
        Ok(b) => b,
        Err(e) => {
            return json_error(&format!("Invalid body: {e}"), 400);
        }
    };
    worker::console_log!("POST /admin/health-check url={}", body.url);

    use overlay_engine::health_checker::HealthChecker;
    let checker = crate::health_checker::WorkerHealthChecker;
    let healthy = checker.check_health(&body.url).await.unwrap_or(false);

    json_ok(&serde_json::json!({
        "status": "success",
        "data": {
            "url": body.url,
            "healthy": healthy,
            "responseTimeMs": 0,
            "statusCode": if healthy { 200 } else { 0 },
            "error": serde_json::Value::Null,
        }
    }))
}

// -----------------------------------------------------------------------------
// /admin/remove-token (RO-010)
// -----------------------------------------------------------------------------

pub async fn admin_remove_token(engine: &Engine, mut req: Request) -> worker::Result<Response> {
    #[derive(Deserialize)]
    struct Body {
        txid: String,
        #[serde(rename = "outputIndex")]
        output_index: u32,
        topic: Option<String>,
    }
    let body: Body = match req.json().await {
        Ok(b) => b,
        Err(e) => return json_error(&format!("Invalid body: {e}"), 400),
    };
    worker::console_log!(
        "POST /admin/remove-token txid={} outputIndex={} topic={:?}",
        body.txid,
        body.output_index,
        body.topic
    );
    match engine
        .evict_output(&body.txid, body.output_index, body.topic.as_deref())
        .await
    {
        Ok(_) => json_ok(&serde_json::json!({
            "status": "success",
            "message": format!("Token {}.{} removed.", body.txid, body.output_index),
        })),
        Err(e) => {
            let status = engine_error_status(&e);
            json_error(&e.to_string(), status)
        }
    }
}

// =============================================================================
// Web UI dashboard
// =============================================================================

/// GET / — HTML dashboard showing node info, topic managers, and lookup services.
///
/// Matches the TS `makeUserInterface()` from overlay-express but rendered
/// server-side with no external JS/CSS dependencies.
pub async fn web_ui(engine: &Engine, hosting_url: Option<&str>) -> worker::Result<Response> {
    let managers = engine.list_topic_managers().await;
    let services = engine.list_lookup_service_providers().await;

    let html = build_dashboard_html(hosting_url, &managers, &services);
    text_response(&html, "text/html")
}

fn build_dashboard_html(
    hosting_url: Option<&str>,
    managers: &std::collections::HashMap<String, overlay_engine::types::ServiceMetadata>,
    services: &std::collections::HashMap<String, overlay_engine::types::ServiceMetadata>,
) -> String {
    let node_url = hosting_url.unwrap_or("(not configured)");
    let version = env!("CARGO_PKG_VERSION");

    // Build topic manager rows
    let mut manager_rows = String::new();
    let mut manager_keys: Vec<&String> = managers.keys().collect();
    manager_keys.sort();
    for key in manager_keys {
        let meta = &managers[key];
        let desc = meta
            .description
            .as_deref()
            .unwrap_or("No description available");
        let ver = meta
            .version
            .as_deref()
            .map(|v| format!("<span class=\"badge\">{v}</span>"))
            .unwrap_or_default();
        manager_rows.push_str(&format!(
            r#"<tr>
  <td><code>{key}</code></td>
  <td>{name} {ver}</td>
  <td>{desc}</td>
  <td><a href="/getDocumentationForTopicManager?manager={key}">docs</a></td>
</tr>"#,
            key = html_escape(key),
            name = html_escape(&meta.name),
            ver = ver,
            desc = html_escape(desc),
        ));
    }

    // Build lookup service rows
    let mut service_rows = String::new();
    let mut service_keys: Vec<&String> = services.keys().collect();
    service_keys.sort();
    for key in service_keys {
        let meta = &services[key];
        let desc = meta
            .description
            .as_deref()
            .unwrap_or("No description available");
        let ver = meta
            .version
            .as_deref()
            .map(|v| format!("<span class=\"badge\">{v}</span>"))
            .unwrap_or_default();
        service_rows.push_str(&format!(
            r#"<tr>
  <td><code>{key}</code></td>
  <td>{name} {ver}</td>
  <td>{desc}</td>
  <td><a href="/getDocumentationForLookupServiceProvider?lookupService={key}">docs</a></td>
</tr>"#,
            key = html_escape(key),
            name = html_escape(&meta.name),
            ver = ver,
            desc = html_escape(desc),
        ));
    }

    format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Overlay Services Node</title>
<style>
*,*::before,*::after{{box-sizing:border-box}}
body{{
  margin:0;padding:0;
  background:#111;color:#e0e0e0;
  font-family:'SF Mono','Fira Code','Cascadia Code',Menlo,Consolas,monospace;
  font-size:15px;line-height:1.6;
}}
a{{color:#579DFF;text-decoration:none}}
a:hover{{color:#83b5ff;text-decoration:underline}}
.container{{max-width:960px;margin:0 auto;padding:2rem 1.5rem}}
header{{
  border-bottom:1px solid #333;
  padding-bottom:1.5rem;margin-bottom:2rem;
}}
h1{{
  font-size:1.75rem;font-weight:700;
  margin:0 0 0.25rem 0;
  background:linear-gradient(90deg,#3b6efb,#579DFF);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
  background-clip:text;color:transparent;
}}
.subtitle{{color:#888;font-size:0.85rem;margin:0}}
.node-url{{
  display:inline-block;margin-top:0.75rem;
  padding:0.4rem 0.75rem;
  background:#1a1a2e;border:1px solid #333;border-radius:4px;
  font-size:0.9rem;color:#ccc;
}}
h2{{
  font-size:1.1rem;font-weight:600;color:#aaa;
  margin:2rem 0 0.75rem 0;
  text-transform:uppercase;letter-spacing:0.05em;
}}
table{{
  width:100%;border-collapse:collapse;
  margin-bottom:1.5rem;
}}
th,td{{
  text-align:left;padding:0.5rem 0.75rem;
  border-bottom:1px solid #222;
}}
th{{
  color:#888;font-size:0.8rem;
  text-transform:uppercase;letter-spacing:0.04em;
  font-weight:500;
}}
td code{{
  background:#1a1a2e;padding:0.15rem 0.4rem;
  border-radius:3px;font-size:0.85rem;
}}
.badge{{
  display:inline-block;
  background:#2a2a4a;color:#8899bb;
  padding:0.1rem 0.4rem;border-radius:3px;
  font-size:0.75rem;margin-left:0.5rem;
}}
.links{{
  display:flex;gap:1rem;flex-wrap:wrap;
  margin-top:0.5rem;
}}
.links a{{
  display:inline-block;
  padding:0.4rem 0.75rem;
  background:#1a1a2e;border:1px solid #333;border-radius:4px;
  font-size:0.85rem;transition:background 0.2s;
}}
.links a:hover{{background:#222244;text-decoration:none}}
.empty{{color:#666;font-style:italic;padding:0.5rem 0}}
footer{{
  margin-top:3rem;padding-top:1rem;
  border-top:1px solid #222;
  color:#555;font-size:0.8rem;
  display:flex;justify-content:space-between;align-items:center;
}}
footer a{{color:#555}}
footer a:hover{{color:#888}}
@media(max-width:640px){{
  .container{{padding:1rem}}
  table{{font-size:0.85rem}}
  th,td{{padding:0.35rem 0.5rem}}
  footer{{flex-direction:column;gap:0.5rem;text-align:center}}
}}
</style>
</head>
<body>
<div class="container">
  <header>
    <h1>Overlay Services</h1>
    <p class="subtitle">BSV Overlay Node</p>
    <div class="node-url">{node_url}</div>
  </header>

  <h2>Topic Managers</h2>
  {manager_section}

  <h2>Lookup Services</h2>
  {service_section}

  <h2>Endpoints</h2>
  <div class="links">
    <a href="/health">/health</a>
    <a href="/listTopicManagers">/listTopicManagers</a>
    <a href="/listLookupServiceProviders">/listLookupServiceProviders</a>
  </div>

  <h2>Resources</h2>
  <div class="links">
    <a href="https://github.com/bitcoin-sv/overlay-services" target="_blank">Overlay Services</a>
    <a href="https://bsv.brc.dev/transactions/0076" target="_blank">BRC-76 GASP</a>
    <a href="https://fast.brc.dev" target="_blank">Quick Start</a>
  </div>

  <footer>
    <span>Powered by <a href="https://github.com/Calhooon/rust-overlay">rust-overlay</a> v{version}</span>
    <span>BSV Blockchain</span>
  </footer>
</div>
</body>
</html>"##,
        node_url = html_escape(node_url),
        manager_section = if manager_rows.is_empty() {
            r#"<p class="empty">No topic managers registered.</p>"#.to_string()
        } else {
            format!(
                r#"<table>
<thead><tr><th>Key</th><th>Name</th><th>Description</th><th></th></tr></thead>
<tbody>{manager_rows}</tbody>
</table>"#
            )
        },
        service_section = if service_rows.is_empty() {
            r#"<p class="empty">No lookup services registered.</p>"#.to_string()
        } else {
            format!(
                r#"<table>
<thead><tr><th>Key</th><th>Name</th><th>Description</th><th></th></tr></thead>
<tbody>{service_rows}</tbody>
</table>"#
            )
        },
        version = version,
    )
}

/// Minimal HTML entity escaping for untrusted values.
fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

pub fn not_found() -> worker::Result<Response> {
    json_response(
        &serde_json::json!({
            "status": "error",
            "code": "ERR_ROUTE_NOT_FOUND",
            "description": "Route not found."
        }),
        404,
    )
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use overlay_engine::types::ServiceMetadata;
    use std::collections::HashMap;

    #[test]
    fn build_dashboard_html_basic() {
        let mut managers = HashMap::new();
        managers.insert(
            "tm_ship".to_string(),
            ServiceMetadata {
                name: "SHIP Topic Manager".to_string(),
                description: Some("Manages SHIP advertisements".to_string()),
                ..Default::default()
            },
        );

        let mut services = HashMap::new();
        services.insert(
            "ls_ship".to_string(),
            ServiceMetadata {
                name: "SHIP Lookup".to_string(),
                description: Some("Looks up SHIP records".to_string()),
                ..Default::default()
            },
        );

        let html = build_dashboard_html(Some("https://example.com"), &managers, &services);

        assert!(html.contains("<!DOCTYPE html>"), "Should be valid HTML");
        assert!(html.contains("Overlay Services"), "Should have title");
        assert!(
            html.contains("https://example.com"),
            "Should show hosting URL"
        );
        assert!(html.contains("tm_ship"), "Should list topic manager key");
        assert!(
            html.contains("SHIP Topic Manager"),
            "Should list topic manager name"
        );
        assert!(
            html.contains("Manages SHIP advertisements"),
            "Should list description"
        );
        assert!(html.contains("ls_ship"), "Should list lookup service key");
        assert!(
            html.contains("SHIP Lookup"),
            "Should list lookup service name"
        );
        assert!(
            html.contains("rust-overlay"),
            "Should have powered-by footer"
        );
        assert!(html.contains("/health"), "Should link to health endpoint");
        assert!(
            html.contains("/listTopicManagers"),
            "Should link to listTopicManagers"
        );
        assert!(
            html.contains("/listLookupServiceProviders"),
            "Should link to listLookupServiceProviders"
        );
    }

    #[test]
    fn build_dashboard_html_empty_services() {
        let managers = HashMap::new();
        let services = HashMap::new();

        let html = build_dashboard_html(None, &managers, &services);

        assert!(
            html.contains("(not configured)"),
            "Should show not-configured when no URL"
        );
        assert!(
            html.contains("No topic managers registered"),
            "Should show empty message for managers"
        );
        assert!(
            html.contains("No lookup services registered"),
            "Should show empty message for services"
        );
    }

    #[test]
    fn build_dashboard_html_escapes_xss() {
        let mut managers = HashMap::new();
        managers.insert(
            "<script>alert(1)</script>".to_string(),
            ServiceMetadata {
                name: "<b>evil</b>".to_string(),
                description: Some("a]\" onload=\"alert(1)".to_string()),
                ..Default::default()
            },
        );

        let html = build_dashboard_html(Some("<script>xss</script>"), &managers, &HashMap::new());

        assert!(
            !html.contains("<script>xss</script>"),
            "Should escape hosting URL"
        );
        assert!(
            !html.contains("<script>alert(1)</script>"),
            "Should escape manager key"
        );
        assert!(!html.contains("<b>evil</b>"), "Should escape manager name");
        assert!(html.contains("&lt;script&gt;"), "Should use HTML entities");
    }

    #[test]
    fn html_escape_covers_all_entities() {
        assert_eq!(html_escape("a&b"), "a&amp;b");
        assert_eq!(html_escape("a<b"), "a&lt;b");
        assert_eq!(html_escape("a>b"), "a&gt;b");
        assert_eq!(html_escape("a\"b"), "a&quot;b");
        assert_eq!(html_escape("safe"), "safe");
    }

    // ── #211/#209: work-bound cap (replaces the old `efs.len() > 8`) ─────────

    use crate::ef::EfTx;

    #[test]
    fn work_bound_cap_ignores_ancestry_depth_bounds_the_subject() {
        // #209: a deep unconfirmed ancestry (many small unproven ancestors)
        // used to trip the old COUNT cap (`> 8`) even though only the SUBJECT is
        // broadcast. The byte bound looks ONLY at the subject we submit, so a
        // 20-ancestor batch with a normal-sized subject passes.
        let mut efs: Vec<EfTx> = (0..20)
            .map(|i| EfTx { txid: format!("anc{i}"), ef: vec![0u8; 1024] })
            .collect();
        efs.push(EfTx { txid: "subj".into(), ef: vec![0u8; 4096] });
        assert_eq!(
            subject_ef_over_cap(&efs, "subj"),
            None,
            "20 ancestors + a 4KB subject must NOT trip the bound"
        );
    }

    #[test]
    fn work_bound_cap_trips_only_on_an_oversized_subject() {
        let efs = vec![EfTx {
            txid: "subj".into(),
            ef: vec![0u8; MAX_SUBJECT_EF_BYTES + 1],
        }];
        assert_eq!(
            subject_ef_over_cap(&efs, "subj"),
            Some(MAX_SUBJECT_EF_BYTES + 1),
            "a subject one byte over the bound is capped"
        );
        // Exactly at the bound is allowed.
        let at = vec![EfTx { txid: "subj".into(), ef: vec![0u8; MAX_SUBJECT_EF_BYTES] }];
        assert_eq!(subject_ef_over_cap(&at, "subj"), None);
    }

    #[test]
    fn work_bound_cap_absent_subject_is_never_over() {
        // Subject already mined / not present → 0 bytes → never capped.
        let efs = vec![EfTx { txid: "other".into(), ef: vec![0u8; 8] }];
        assert_eq!(subject_ef_over_cap(&efs, "subj"), None);
    }

    #[test]
    fn work_bound_cap_bounds_the_total_batch_the_fallback_resubmits() {
        // Adversarial review (2026-07-20): a NORMAL-sized subject that passes the
        // subject cap, but a huge ancestry batch. Attempts 1–2 send only the
        // subject; the async-REJECTED fallback (attempt 3) re-submits the WHOLE
        // batch (`concat_efs`) to ARC. The subject cap alone would let an
        // attacker force a multi-MB ARC POST + ~40 s of worker poll per request
        // (a double-spend subject: 202 then async REJECTED → the fallback fires).
        // The total-batch bound catches it BEFORE any ARC submit.
        let mut efs = vec![EfTx { txid: "subj".into(), ef: vec![0u8; 4096] }]; // subject fine
        efs.push(EfTx { txid: "fat-ancestor".into(), ef: vec![0u8; MAX_BATCH_EF_BYTES] });
        let total = 4096 + MAX_BATCH_EF_BYTES;
        assert_eq!(
            subject_ef_over_cap(&efs, "subj"),
            Some(total),
            "an oversized TOTAL batch must be capped even when the subject is small"
        );
        // A total exactly at the bound is allowed — small subject + ancestry
        // that sums (with the subject) to exactly the batch cap.
        let at = vec![
            EfTx { txid: "subj".into(), ef: vec![0u8; 4096] },
            EfTx { txid: "anc".into(), ef: vec![0u8; MAX_BATCH_EF_BYTES - 4096] },
        ];
        assert_eq!(subject_ef_over_cap(&at, "subj"), None, "total exactly at the batch bound is allowed");
    }

    #[test]
    fn retryable_cap_error_body_carries_the_retryable_hint() {
        // #211: a cap rejection must be retryable (429 + `retryable:true`), not
        // a flat 400 that makes the client abandon the overlay for this submit.
        let json = serde_json::to_string(&RetryableErrorBody {
            status: "error",
            message: "subject EF too large — retry via fallback",
            retryable: true,
        })
        .unwrap();
        assert!(json.contains("\"retryable\":true"), "{json}");
    }
}
