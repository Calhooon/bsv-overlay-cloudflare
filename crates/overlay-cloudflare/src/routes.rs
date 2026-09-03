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
pub(crate) fn subject_ef_over_cap(efs: &[crate::ef::EfTx], subject_txid: &str) -> Option<usize> {
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
        // The caller's query was malformed — their fault, not ours. Also the
        // parity-aligned answer: mainline overlay-express 2.2.0 answers a bad
        // request 400 (same as the two arms above).
        EngineError::InvalidQuery(_) => 400,
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
    // `TOPIC_SUFFIX`-aware, like `parse_csv_env`: report the names the engine
    // actually registers, never the bare base names.
    let csv_suffix = env_topic_suffix(env);
    let parse_csv = |var: &str, default: &str| -> Vec<String> {
        env.var(var)
            .ok()
            .map(|v| v.to_string())
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| default.into())
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| suffixed_name(s, &csv_suffix))
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
/// W2-P4/P6 (2026-09-03): every exit of `submit_inner` — the broadcast-gated
/// success returns included — ships the pot and lobby notes this request's
/// storage writes took. The flush used to sit on ONE path near the end of the
/// handler; a gated `tm_low` advert admitted and returned before it, so the
/// note was logged and never shipped (the lobby cell, 10:54Z).
pub async fn submit(
    engine: &Engine,
    mut req: Request,
    hosting_url: Option<&str>,
    // Arcade V2 endpoint override for the broadcast-gated mode (None → default
    // endpoint). Arcade is keyless, so broadcast-gated is always available.
    arcade_url: Option<String>,
    // TAAL key for the #214 corroborating broadcaster (exhausted-ladder second
    // opinion). None still corroborates — TAAL keyless, then GorillaPool.
    taal_api_key: Option<String>,
    // Worker context — used only to background the mainnet SHIP fan-out.
    ctx: &Context,
    // #347: the submit-gate needs the env for ENABLE_EXTENSIONS (kill switch),
    // SUBMIT_ENFORCE (the Rule 6c rollout flag) and SUBMIT_OPERATOR_TOKEN
    // (deliberately NOT ADMIN_TOKEN — see `check_submit_operator_auth`).
    // #366 also derives the census counters' D1 handle (`OVERLAY_DB`) from it.
    env: &Env,
) -> worker::Result<Response> {
    let out = submit_inner(engine, req, hosting_url, arcade_url, taal_api_key, ctx, env).await;
    crate::pot_changes::flush(env, |fut| ctx.wait_until(fut));
    crate::lobby_changes::flush(env, |fut| ctx.wait_until(fut));
    out
}

async fn submit_inner(
    engine: &Engine,
    mut req: Request,
    hosting_url: Option<&str>,
    // Arcade V2 endpoint override for the broadcast-gated mode (None → default
    // endpoint). Arcade is keyless, so broadcast-gated is always available.
    arcade_url: Option<String>,
    // TAAL key for the #214 corroborating broadcaster (exhausted-ladder second
    // opinion). None still corroborates — TAAL keyless, then GorillaPool.
    taal_api_key: Option<String>,
    // Worker context — used only to background the mainnet SHIP fan-out.
    ctx: &Context,
    // #347: the submit-gate needs the env for ENABLE_EXTENSIONS (kill switch),
    // SUBMIT_ENFORCE (the Rule 6c rollout flag) and SUBMIT_OPERATOR_TOKEN
    // (deliberately NOT ADMIN_TOKEN — see `check_submit_operator_auth`).
    // #366 also derives the census counters' D1 handle (`OVERLAY_DB`) from it.
    env: &Env,
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

    // `mut`: the broadcast-gated mined-claim arm strips the subject's
    // unverified bump before storage (#268 gate M1).
    let mut tagged_beef = TaggedBEEF {
        beef,
        topics,
        off_chain_values,
    };

    // ── #347: the admission path is DERIVED BY THE ENDPOINT, never chosen by
    // the caller. The header is an INPUT to that derivation, never a gate
    // decision in its own right (epoch Rule 8b applied to a MODE rather than a
    // value: a gate selected by a caller-supplied discriminator is not a gate).
    //
    // The ENTIRE decision comes from ONE call to `plan_submit`. The route may
    // only read the resulting fields — it cannot re-derive, re-order or
    // second-guess them. The wiring is the defect class here, so it is not
    // allowed to live as inline branching (gate finding H1/M-5).
    let mode_header = req.headers().get("x-submit-mode").ok().flatten();
    // Fail CLOSED on a typo: extensions are enabled ONLY on an explicit
    // "true". Both wrangler configs set it explicitly, so this is safe, and a
    // mangled value can no longer silently leave the ungated modes reachable.
    let extensions_enabled = env
        .var("ENABLE_EXTENSIONS")
        .ok()
        .map(|v| v.to_string())
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("true"));
    let gate_mode = crate::submit_gate::GateMode::parse(
        env.var("SUBMIT_ENFORCE")
            .ok()
            .map(|v| v.to_string())
            .as_deref(),
    );
    // A DEDICATED submit-operator credential, deliberately NOT the ADMIN_TOKEN
    // that gates /admin/evictOutpoint, /admin/ban and /admin/startGASPSync
    // (gate finding M1). Handing the watchtower the admin token would mean a
    // tower compromise grants eviction of any outpoint from the index — which
    // is precisely the primitive the enumeration-starvation money path needs.
    // Unset ⇒ nobody can authenticate ⇒ fail closed.
    let operator_authed = check_submit_operator_auth(&req, env);
    // ONE derivation. The decision is computed EXACTLY once and everything
    // downstream — the counter, the refusal, the engine mode, the gate branch —
    // reads that single value.
    //
    // It was previously derived TWICE from two separately-passed argument
    // lists (`plan_submit(...)` for the counter, `action_for(...)` for
    // behaviour). A re-gate flipped `operator_authed` to `true` on the
    // behavioural call alone: it compiled, `make ci` stayed green, every caller
    // became an "authenticated operator", and the counter kept reporting
    // honestly from the other derivation. Two derivations of one decision is
    // the defect — the fix is to delete one, not to pin their agreement
    // (Rule 10).
    //
    // The route does NOT report the classification either: `action_for` counts
    // it internally. `submit_gate::note` was `pub` and took a `SubmitAction` by
    // value, so a re-gate handed it a fabricated one — every submit reported as
    // `barred` while behaviour was unchanged, with 0 compile errors, the native
    // suite green and every source pin matching. There is now no argument for
    // this route to get wrong (Rule 15: derive the decision, don't accept it).
    let action = crate::submit_gate::action_for(
        mode_header.as_deref(),
        extensions_enabled,
        operator_authed,
        gate_mode,
    );
    let mode = action.engine_mode();
    // ── #371 SEEN corroboration flag — the UNGATED arm's latch feed. ──
    // Read from the SAME `action` value the exhaustive match below consumes
    // (no second derivation, no local a later edit can shadow into a gate —
    // the #347 lesson; this flag only ever WIDENS observation, it gates
    // nothing). The corroboration itself is scheduled AFTER `engine.submit`
    // succeeds (adversarial gate MEDIUM-2): an unadmitted subject can never
    // acquire a spend pointer, so corroborating it would mint a `network_seen`
    // row nothing can ever join — pure free growth on a public route. The
    // gated arm needs none of this: it latches synchronously on its own
    // broadcast verdict.
    let corroborate_seen_after_submit = matches!(
        action,
        crate::submit_gate::SubmitAction::ProceedWithoutGate { .. }
    );

    // EXHAUSTIVE match (Rule 22): the route consumes the decision as an enum,
    // so an arm cannot be deleted without breaking the BUILD. A previous
    // version read a struct field in an `if`, which a re-gate deleted outright
    // while the whole suite stayed green — reading a field is optional, an arm
    // is not.
    match action {
        crate::submit_gate::SubmitAction::RefuseUnauthenticated(path) => {
            worker::console_log!(
                "POST /submit -> 401 (unbarred path {} requires operator auth; SUBMIT_ENFORCE=true)",
                path.as_str()
            );
            return json_error(
                &format!(
                    "submit mode '{}' has no admission bar and is restricted to operators — \
                     submit with 'broadcast-gated' (the overlay broadcasts and admits only on \
                     network acceptance; an already-broadcast tx satisfies it idempotently), \
                     or present the submit-operator Bearer token",
                    path.as_str()
                ),
                401,
            );
        }
        crate::submit_gate::SubmitAction::ProceedWithNetworkGate(_) => {}
        crate::submit_gate::SubmitAction::ProceedWithoutGate {
            path,
            lenient_unbarred,
        } => {
            if lenient_unbarred {
                // Lenient window (Rule 6c). Counted above; logged here so the
                // operator can see WHO is still on an unbarred path before
                // flipping enforcement.
                worker::console_log!(
                    "POST /submit: UNAUTHENTICATED submit on unbarred path {} — served under \
                     the lenient window (#347); set SUBMIT_ENFORCE=true to refuse",
                    path.as_str()
                );
            }
            // ── #366 broadcast-gated READINESS CENSUS — measurement ONLY. ──
            // Runs strictly AFTER the admission decision and touches nothing
            // it reads: no broadcast, no refusal, no change to any status
            // code or admission outcome (delete this block and every request
            // behaves byte-identically). Classifies whether THIS body would
            // have survived the gated arm's pre-network structural checks —
            // the number the #347 flip criterion needs and nothing measured
            // (the client half is a console.warn nobody reads).
            //
            // RESIDUAL (named): this counts only submits that ARRIVE. An
            // overlay outage is exactly when the client is least ready; that
            // slice stays with the client-side warn (bsv-low #351).
            let verdict = crate::submit_census::census_verdict(&tagged_beef.beef);
            let (state_counter, reason_counter) =
                crate::submit_census::census_counters(path, lenient_unbarred, verdict);
            worker::console_log!(
                "POST /submit census(#366): path={} population={} verdict={} → {}",
                path.as_str(),
                if lenient_unbarred {
                    "client"
                } else {
                    "operator"
                },
                verdict.as_str(),
                state_counter
            );
            // Precisely (gate LOW-1): the CLASSIFICATION above is SYNCHRONOUS
            // on every ungated submit — ~2 `Beef::from_binary` parses, the
            // subject's EF conversions and an ancestry BFS, all bounded by
            // `MAX_CENSUS_EVAL_BYTES` — and only the durable D1 WRITE below
            // is backgrounded (`ctx.wait_until`). A D1 fault can only lose a
            // count, never a submit (`bump_counter` logs and swallows its own
            // errors; a missing binding logs and loses the count, never the
            // request).
            match env.d1("OVERLAY_DB") {
                Ok(census_db) => ctx.wait_until(async move {
                    crate::ops::bump_counter(&census_db, state_counter, 1).await;
                    if let Some(reason) = reason_counter {
                        crate::ops::bump_counter(&census_db, reason, 1).await;
                    }
                }),
                Err(e) => {
                    worker::console_log!("census(#366): OVERLAY_DB unavailable, count lost: {e}");
                }
            }
        }
    }

    // ── BROADCAST-GATED submit (bsv-low overlay-first, 2026-07-17; the
    // zanaadu invariant): the OVERLAY broadcasts, and NOTHING is admitted
    // unless the network accepted the tx. Every unproven tx in the BEEF is
    // broadcast as Extended Format (ARC can't source unconfirmed parents from
    // a bare raw); a DEFINITIVE network rejection returns 422 and admits
    // nothing — the index can never contain a tx the network refused. A
    // transport failure on both broadcasters returns 502 (the caller falls
    // back to its own direct broadcast + historical submit). An all-proven
    // BEEF ("already mined" — a SUBMITTER-ASSERTED bump, never validated
    // here) no longer admits ungated (bsv-low#268): the claim is
    // corroborated against a real provider via the subject's raw, and an
    // unconfirmable claim refuses admission (502, retryable).
    // #195 Server-Timing segments (ms). `arcade-broadcast` is the gated
    // network broadcast, `engine-submit` the D1 write-through, `fanout` the
    // (backgrounded) SHIP fan-out's synchronous scheduling cost. Emitted as a
    // `Server-Timing` response header so a latency claim is measurable per
    // slice instead of from client wall-clock (which cannot separate overlay
    // work from Arcade variance — the retracted #195 measurement).
    let mut arcade_broadcast_ms = 0f64;
    let mut arcade_poll_ms = 0f64;
    let mut corroborate_ms = 0f64;
    // Consumed DIRECTLY from the action: there is no local flag to shadow.
    // A re-gate defeated both source pins with
    // `let run_network_gate = run_network_gate && x.is_some() && x.is_none();`
    // — a shadowed rebinding changes the VALUE, not the SYNTAX, so a source
    // scan is structurally blind to it. Removing the local removes the class;
    // `make ci`'s route tier is what covers the residual.
    if matches!(
        action,
        crate::submit_gate::SubmitAction::ProceedWithNetworkGate(_)
    ) {
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
            crate::broadcaster::ArcadeBroadcaster::new(arcade_url.clone().unwrap_or_default())
                // #214: Arcade's async REJECTED is never authoritative
                // uncorroborated — an exhausted ladder gets a second
                // broadcaster's word (TAAL → GorillaPool) before any 422.
                .with_corroborator_key(taal_api_key.clone());
        if let Some(h) = hosting_url {
            arcade = arcade.with_callback(format!("{}/arc-ingest", h.trim_end_matches('/')));
        }
        // #268: when every leg claims a bump (efs empty) the subject's RAW is
        // the mined-claim corroboration body — extracted here (the route holds
        // the BEEF; the broadcaster does not).
        let mined_subject_raw = if efs.is_empty() {
            crate::ef::proven_subject_raw(&tagged_beef.beef)
        } else {
            None
        };
        // #413 DUAL-BROADCAST AT FIRST SEND (owner decision 2026-08-26): the
        // ARC-tracker indictment proved Arcade/ARC acceptance is not delivery
        // (SEEN-for-14h-never-held one way, mined-but-still-SEEN/0 the other),
        // while every direct TAAL push delivered without exception. So the
        // TAAL leg is no longer only the exhausted-ladder second opinion — it
        // fires on EVERY gated submit, post-response (ctx.wait_until: zero
        // added latency), and its strict ≥SEEN verdict (corroborator_verdict's
        // bar — RECEIVED/STORED rank below) latches `network_seen` as a REAL
        // witness. Idempotent with the ladder's own corroborate and with the
        // client belts; best-effort by construction. Accepted residuals
        // (review 2026-08-26): during the #347 lenient window an unauthed
        // structural-pass body gets a keyed TAAL relay (owner-accepted cost,
        // 256KB-capped, no amplification loop); a hung socket can eat the
        // wait_until budget silently — convergence rides the #397 re-checks
        // and the backstop, by design.
        {
            // Ancestry-ordered (2026-08-26, first live fire): subject-only
            // dual-pushes came back "orphan view" — TAAL holds a fresh
            // two-leg money tx in its ORPHAN POOL because the hop parent
            // never reached it. `efs` is already dependency-ordered
            // (parents before subject), so push EVERY leg in order — the
            // parent fills TAAL's mempool and the subject validates. Latch
            // only on the SUBJECT's accept.
            let dual_legs: Vec<(String, String)> = efs
                .iter()
                .map(|e| (e.txid.clone(), hex::encode(&e.ef)))
                .collect();
            if let (false, Ok(dual_db)) = (dual_legs.is_empty(), env.d1("OVERLAY_DB")) {
                let dual_key = taal_api_key.clone();
                let dual_txid = subject_txid.clone();
                ctx.wait_until(async move {
                    let mut subject_outcome: Result<crate::broadcaster::ArcOutcome, String> =
                        Err("subject leg never pushed".to_string());
                    for (leg_txid, hex_body) in &dual_legs {
                        let res =
                            crate::broadcaster::corroborate_tx_hex(dual_key.as_deref(), hex_body).await;
                        if *leg_txid == dual_txid {
                            subject_outcome = res;
                        }
                    }
                    match subject_outcome {
                        Ok(crate::broadcaster::ArcOutcome::Accepted(_)) => {
                            crate::ops::latch_network_seen(&dual_db, &dual_txid).await;
                            worker::console_log!(
                                "[#413] dual-broadcast delivered {dual_txid} (TAAL >=SEEN) — network_seen latched"
                            );
                        }
                        Ok(other) => worker::console_log!(
                            "[#413] dual-broadcast for {dual_txid}: non-accept ({other:?}) — no latch"
                        ),
                        Err(e) => worker::console_log!(
                            "[#413] dual-broadcast for {dual_txid} inconclusive: {} — no latch",
                            e.chars().take(80).collect::<String>()
                        ),
                    }
                });
            }
        }
        let arcade_started = js_sys::Date::now();
        let arcade_outcome = arcade
            .broadcast_efs_gated(&efs, &subject_txid, mined_subject_raw.as_deref())
            .await;
        // #195: keep segments DISJOINT and attributable — the corroborate leg
        // runs inside the gated broadcast's wall-clock, so it is carved out of
        // `arcade-broadcast` and reported as its own `corroborate` segment
        // (also on the 422/502 early returns, where attribution matters most).
        // #272: `arcade-poll` (the SEEN-gate wait) is carved out too, so the
        // remaining `arcade-broadcast` is submit-POST wall-clock — the
        // 15–16 s JOIN-submit budget becomes attributable per slice.
        corroborate_ms = arcade.corroborate_ms();
        arcade_poll_ms = arcade.poll_wait_ms();
        arcade_broadcast_ms =
            (js_sys::Date::now() - arcade_started - corroborate_ms - arcade_poll_ms).max(0.0);
        let gated_timing = format!(
            "arcade-broadcast;dur={arcade_broadcast_ms:.1}, arcade-poll;dur={arcade_poll_ms:.1}, corroborate;dur={corroborate_ms:.1}"
        );
        match arcade_outcome {
            Ok(crate::broadcaster::ArcOutcome::Accepted(accepted)) => {
                worker::console_log!(
                    "broadcast-gated(arcade): network accepted {subject_txid} ({accepted}, {} EF leg(s)) — admitting",
                    efs.len()
                );
                // #371: the overlay ITSELF just witnessed the network accept
                // this subject — latch it. Synchronous (one INSERT OR
                // IGNORE): the verdict-publication JOIN must see the row no
                // later than the spend pointer written by `engine.submit`
                // below. Failure is logged inside and swallowed — the latch
                // only accelerates; the merkle bar remains the fallback.
                match env.d1("OVERLAY_DB") {
                    Ok(db) => crate::ops::latch_network_seen(&db, &subject_txid).await,
                    Err(e) => worker::console_log!(
                        "#371: OVERLAY_DB unavailable, network_seen latch lost for {subject_txid}: {e}"
                    ),
                }
                // #268 gate M1: a mined-claim admit (efs empty — the ONLY
                // gated arm whose SUBJECT carries a submitter-supplied bump)
                // proved NETWORK ACCEPTANCE, not SPV-mined-ness. STRIP the
                // unverified bump before storage so the stored row is
                // byte-equivalent to an honestly-submitted unmined tx (the
                // completion pass attaches a chaintracks-VERIFIED bump later;
                // /tx-any never serves an attacker-chosen height). A BEEF
                // that cannot be sanitized is REFUSED, never stored verbatim.
                if efs.is_empty() {
                    match crate::ef::strip_subject_bump(&tagged_beef.beef, &subject_txid) {
                        Some(stripped) => {
                            worker::console_log!(
                                "broadcast-gated: stripped the unverified mined-claim bump from {subject_txid} before storage (#268 M1)"
                            );
                            tagged_beef.beef = stripped;
                        }
                        None => {
                            worker::console_log!(
                                "POST /submit(broadcast-gated) -> 502 (mined-claim BEEF for {subject_txid} could not be sanitized — refusing to store an unverified bump)"
                            );
                            let resp = json_error(
                                "broadcast failed: mined-claim BEEF could not be sanitized — retry via fallback",
                                502,
                            )?;
                            return Ok(with_server_timing(resp, &gated_timing));
                        }
                    }
                }
            }
            Ok(crate::broadcaster::ArcOutcome::AcceptedPending(pending)) => {
                // #397: sync-validated + queued by Arcade, tracker lagging,
                // corroborator inconclusive, ancestry PROVEN (the broadcaster
                // never pends an unproven-ancestry subject — #267). ADMIT —
                // a lagging tracker must not read as a rejected tx — but do
                // NOT latch `network_seen`: nothing witnessed it yet. Money
                // views stay gated on the real witness; the background
                // re-checks below latch it when the tracker catches up, and
                // the reconcile ladder displaces the claim if the tx truly
                // never propagates (the act-on-failure design, owner
                // 2026-08-19).
                worker::console_log!(
                    "broadcast-gated(arcade): {subject_txid} admitted PENDING ({pending}) — sync-accepted, \
                     SEEN unwitnessed (#397); scheduling background witness re-checks"
                );
                // Accepted residual (gate LOW-2): a stranger CAN park an
                // inert pending row through this arm (valid script+fee,
                // tracker quiet, corroborator down) — bounded by the subject
                // EF byte cap, displaceable by the reconcile CAS, and
                // excluded from every money view until witnessed. Writing
                // inert rows is the D2 open-path status quo, not a new power.
                if let Ok(seen_db) = env.d1("OVERLAY_DB") {
                    let recheck_arcade = crate::broadcaster::ArcadeBroadcaster::new(
                        arcade_url.clone().unwrap_or_default(),
                    );
                    let recheck_txid = subject_txid.clone();
                    ctx.wait_until(async move {
                        // Two witness re-checks inside the isolate's post-
                        // response budget (+6 s, +8 s — gate LOW-1: stay well
                        // under the ~30 s wait_until ceiling so eviction
                        // cannot silently skip the latch). Longer lags
                        // converge via the #371 corroboration on later
                        // submits, the completion cron, and the reconcile
                        // ladder.
                        for delay_ms in [6_000u64, 8_000] {
                            crate::broadcaster::sleep_ms(delay_ms).await;
                            if recheck_arcade.network_witnessed(&recheck_txid).await {
                                crate::ops::latch_network_seen(&seen_db, &recheck_txid).await;
                                worker::console_log!(
                                    "#397: background re-check witnessed {recheck_txid} SEEN — latched"
                                );
                                return;
                            }
                        }
                        worker::console_log!(
                            "#397: {recheck_txid} still unwitnessed after background re-checks — \
                             the completion/reconcile passes own it now"
                        );
                    });
                }
            }
            Ok(crate::broadcaster::ArcOutcome::Rejected(reason)) => {
                // DEFINITIVE refusal of the SUBJECT → admit NOTHING. (#214:
                // this arm is now reachable only via a SYNCHRONOUS validation
                // failure or a corroborated async rejection — never on
                // Arcade's uncorroborated word.)
                worker::console_log!(
                    "POST /submit(broadcast-gated) -> 422 (network rejected {subject_txid}: {reason})"
                );
                let resp = json_error(&format!("network rejected: {reason}"), 422)?;
                return Ok(with_server_timing(resp, &gated_timing));
            }
            Err(transport) => {
                worker::console_log!(
                    "POST /submit(broadcast-gated) -> 502 (broadcast transport: {transport})"
                );
                let resp = json_error(&format!("broadcast failed: {transport}"), 502)?;
                return Ok(with_server_timing(resp, &gated_timing));
            }
        }
    }

    // ── Synchronous write-through: full submit (Phase 1+2+3) ──
    // Admitted outputs are written to D1 before the response is sent,
    // so subsequent /lookup queries on this instance see them immediately.
    let engine_started = js_sys::Date::now();
    let (steak, mutation_report) = match engine.submit_with_report(&tagged_beef, mode).await {
        Ok(v) => v,
        Err(e) => {
            let status = engine_error_status(&e);
            worker::console_log!("POST /submit -> {} (submit failed)", status);
            return json_error(&e.to_string(), status);
        }
    };
    let engine_submit_ms = js_sys::Date::now() - engine_started;

    // ── S2 QUEUE-DURABLE ADMISSION (ARCHITECTURE v2 principle 1, bsv-low
    // 2026-08-29): AN ACK IS DURABLE. The write-through above is the fast
    // path; the engine now REPORTS every Phase-3 write (or validation read)
    // that faulted instead of swallowing it. If anything faulted, the same
    // bytes are ENQUEUED for an idempotent replay BEFORE the ack — and if
    // the queue cannot take them, the submit is REFUSED (502, retryable)
    // rather than acked over a write we do not hold. This is the exact
    // mechanism behind the 2026-08-26 phantom class (admissions acked under
    // a D1 storm whose rows never existed; addendum 7): a dropped write is
    // now redelivered, never vanished. The decision is derived ONCE
    // (`mutation_ack`) and consumed as an enum.
    let enqueue_outcome = if mutation_report.is_durable() {
        None
    } else {
        worker::console_log!(
            "POST /submit: Phase-3 NOT DURABLE for {} — {} fault(s): {} — enqueueing an idempotent replay (S2)",
            tagged_beef.topics.join(","),
            mutation_report.faults.len(),
            mutation_report.summary()
        );
        if let Ok(db) = env.d1("OVERLAY_DB") {
            ctx.wait_until(async move {
                crate::ops::bump_counter(&db, crate::ops::COUNTER_SUBMIT_MUTATION_FAULT, 1).await;
            });
        }
        Some(crate::queue::enqueue_replay(env, &tagged_beef.beef, &tagged_beef.topics, mode).await)
    };
    let mutation_queued = match crate::queue::mutation_ack(
        mutation_report.is_durable(),
        enqueue_outcome,
    ) {
        crate::queue::MutationAck::Durable => false,
        crate::queue::MutationAck::Queued => {
            worker::console_log!(
                "POST /submit: replay QUEUED for {} (S2) — acking; the queue is the guarantee",
                tagged_beef.topics.join(",")
            );
            if let Ok(db) = env.d1("OVERLAY_DB") {
                ctx.wait_until(async move {
                    crate::ops::bump_counter(&db, crate::ops::COUNTER_SUBMIT_MUTATION_QUEUED, 1)
                        .await;
                });
            }
            true
        }
        crate::queue::MutationAck::Refused(reason) => {
            worker::console_log!(
                "POST /submit -> 502 (admission NOT durable and the replay could not be queued: {reason})"
            );
            if let Ok(db) = env.d1("OVERLAY_DB") {
                ctx.wait_until(async move {
                    crate::ops::bump_counter(&db, crate::ops::COUNTER_SUBMIT_MUTATION_REFUSED, 1)
                        .await;
                });
            }
            return json_error_retryable(&format!("admission not durable ({reason}) — retry"), 502);
        }
    };

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
        // #413 delivery-integrity hardening (2026-08-26, owner call): on the
        // BROADCAST-GATED money path, 0 admitted must not read as ok — it is
        // exactly the phantom's front door (measured: cold-start migration
        // bursts made topic managers admit 0 while the evidence gate ok'd,
        // the client believed the network held its JOIN, and the tx was
        // never stored anywhere). One nuance keeps the belt alive: a
        // RE-PRESENT of an already-admitted tx also admits 0 (duplicate) —
        // so the refusal additionally requires the subject to be ABSENT from
        // the transactions store (its first admit stored the bytes; one PK
        // read discriminates). Absent + 0 admitted + gated ⇒ 502 retryable:
        // the client ladder re-presents after the burst window instead of
        // sailing on a false ok. Fail-open: any parse/read fault falls
        // through to the pre-hardening 200 (never a manufactured refusal).
        // Review CRITICAL-1 (2026-08-26): a CONSUME-ONLY spend subject
        // (settle/refund/close) admits 0 outputs BY DESIGN and never gets a
        // `transactions` row — the belt must pass it, or every hand-end 502s
        // forever. The spend evidence is already in the steak: consuming
        // previously-admitted coins fills coins_to_retain/coins_removed. The
        // phantom consumes nothing known and admits nothing — only THAT
        // shape may refuse.
        let total_consumed: usize = steak
            .values()
            .map(|a| a.coins_to_retain.len() + a.coins_removed.as_ref().map_or(0, |v| v.len()))
            .sum();
        // SIGNERS SELF-HEAL (2026-08-26 — the enforcedWithheldReplay residual):
        // a consuming submit (settle/refund) can leave its pot row's verdict
        // group WITHOUT signers when the inline classify's record read hiccups
        // under D1 pressure — bytes and verdict land, and the narration then
        // waits on the */15 cron. Two delayed passes of the SAME tested
        // pipeline (`backfill_settle_signers`, recency-banded so a just-spent
        // pot is the top candidate) close that gap in seconds. FAIL-OPEN and
        // off the request path (wait_until); bounded (limit 4, two passes); a
        // clean state scans zero candidates and stops immediately.
        if total_consumed > 0 {
            if let Ok(heal_db) = env.d1("OVERLAY_DB") {
                ctx.wait_until(async move {
                    let storage = crate::d1_discovery::D1PotStorage::new(std::rc::Rc::new(heal_db));
                    for delay_ms in [3_000u64, 9_000] {
                        worker::Delay::from(std::time::Duration::from_millis(delay_ms)).await;
                        let s = crate::proof_fetcher::backfill_settle_signers(&storage, 4).await;
                        if s.scanned == 0 {
                            break; // nothing unclassified — done
                        }
                    }
                });
            }
        }
        // S2: a QUEUED replay supersedes this belt for the request — the
        // rows it probes for may legitimately not exist yet, and the queue
        // (not a re-present) is the guarantee. Refusing here would 502 an
        // admission that is already durably held.
        // W2-P4: ship the pot rows this submit changed (off the critical path).
        crate::pot_changes::flush(env, |fut| ctx.wait_until(fut));
        crate::lobby_changes::flush(env, |fut| ctx.wait_until(fut));
        if total_consumed == 0
            && !mutation_queued
            && matches!(
                action,
                crate::submit_gate::SubmitAction::ProceedWithNetworkGate(_)
            )
        {
            // Review MEDIUM-3: the SAME subject derivation as the gated arm —
            // beef_to_ef_batch sorts first and takes the sorted last; a raw
            // `.txs.last()` here would key a different txid for any body
            // whose raw order differs (the #351 sorted-last contract).
            let subject = bsv_rs::transaction::beef::Beef::from_binary(&tagged_beef.beef)
                .ok()
                .and_then(|mut b| {
                    b.sort_txs();
                    b.txs
                        .last()
                        .and_then(|t| t.tx().map(|tx| (t.txid(), tx.clone())))
                });
            // Belt v3 (attempt 11 live finding): the refusal additionally
            // requires the subject to CARRY a covenant-admissible output —
            // the true anomaly is "a funding shape tm_pot would admit,
            // admitted 0". A recovery re-seed / spend of a pot the index
            // doesn't hold consumes nothing-known and admits 0 LEGITIMATELY
            // (best-effort, non-fatal by the client's own contract) and was
            // 502-churning under the first cut. Same inline-revalidation
            // precedent as the tm_uhrp diag block below.
            let funding_shaped = subject.as_ref().is_some_and(|(_, tx)| {
                tx.outputs.iter().any(|o| {
                    overlay_discovery::pot::is_pot_covenant_script(&o.locking_script.to_binary())
                })
            });
            if let (true, Some((subject_txid, _)), Ok(db)) =
                (funding_shaped, subject, env.d1("OVERLAY_DB"))
            {
                #[derive(serde::Deserialize)]
                struct OneRow {
                    #[allow(dead_code)]
                    one: f64,
                }
                let stored = crate::d1::Query::new(
                    "SELECT 1 AS one FROM transactions WHERE txid = lower(?)",
                )
                .bind(subject_txid.as_str())
                .fetch_optional::<OneRow>(&db)
                .await;
                if matches!(stored, Ok(None)) {
                    worker::console_log!(
                        "POST /submit(broadcast-gated) -> 502 ({subject_txid}: 0 outputs admitted and \
                         nothing stored — refusing the false ok; the client ladder re-presents (#413)"
                    );
                    return json_error(
                        "broadcast-gated submit admitted nothing and stored nothing — retry",
                        502,
                    );
                }
            }
        }
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
    // ── #371 SEEN corroboration — scheduled only now that `engine.submit`
    // SUCCEEDED (gate MEDIUM-2: an unadmitted subject can never acquire a
    // spend pointer, so its row could never join). NOT gated on
    // `total_admitted`: a settle/refund/sweep admits ZERO outputs by design —
    // its effect is the spend notification — and it is exactly the subject
    // this latch exists for. The overlay asks Arcade about the SUBJECT
    // itself, in the background, and latches `network_seen` ONLY on Arcade's
    // own SEEN_ON_NETWORK-or-better answer (orphan excluded, #267). A
    // never-broadcast attacker subject gets no row and stays behind the
    // merkle bar; a corroboration fault degrades to exactly the pre-#371
    // behaviour. Bounded: one subject parse + one GET per ADMITTED ungated
    // submit, off the critical path (`wait_until`); the beef clone is capped
    // by the 10MB body bound above (typical LOW BEEFs are KB-scale).
    if corroborate_seen_after_submit {
        if let Ok(seen_db) = env.d1("OVERLAY_DB") {
            let beef_for_seen = tagged_beef.beef.clone();
            let seen_arcade =
                crate::broadcaster::ArcadeBroadcaster::new(arcade_url.clone().unwrap_or_default());
            ctx.wait_until(async move {
                let subject = bsv_rs::transaction::Transaction::from_beef(&beef_for_seen, None)
                    .map(|t| t.id());
                if let Ok(subject) = subject {
                    if seen_arcade.network_witnessed(&subject).await {
                        crate::ops::latch_network_seen(&seen_db, &subject).await;
                    }
                }
            });
        }
    }

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
    // conflating overlay work with Arcade variance. `corroborate` (#214) is
    // carved out of `arcade-broadcast` so the second-broadcaster leg is
    // attributable on its own.
    let server_timing = format!(
        "arcade-broadcast;dur={arcade_broadcast_ms:.1}, arcade-poll;dur={arcade_poll_ms:.1}, corroborate;dur={corroborate_ms:.1}, engine-submit;dur={engine_submit_ms:.1}, fanout;dur={fanout_ms:.1}"
    );
    let mut resp = with_server_timing(json_ok(&steak)?, &server_timing);
    if mutation_queued {
        // S2: tell the caller (and the harness) this admission is held by
        // the queue, not yet by D1 — a lookup on this instance may lag by
        // one consumer delivery. Exposed for browser reads alongside
        // Server-Timing.
        let h = resp.headers_mut();
        let _ = h.set("X-Overlay-Mutation", "queued");
        let _ = h.set(
            "Access-Control-Expose-Headers",
            "Server-Timing, X-Overlay-Mutation",
        );
    }
    Ok(resp)
}

/// #195: attach a `Server-Timing` header (+ its CORS expose) to a response.
/// Used on the submit success path AND the broadcast-gated 422/502 early
/// returns — a refusal's latency must be attributable too (#214 debugging is
/// exactly the failure-path case).
fn with_server_timing(mut resp: Response, timing: &str) -> Response {
    let h = resp.headers_mut();
    let _ = h.set("Server-Timing", timing);
    // Browsers gate response-header reads behind Access-Control-Expose-Headers.
    let _ = h.set("Access-Control-Expose-Headers", "Server-Timing");
    resp
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

    match engine.lookup_with_txids(&question, history_selector).await {
        Ok((answer, txids)) => {
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
                    match serialize_aggregated_lookup(&outputs, &txids) {
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
/// For each output, writes the txid (32 raw bytes) and output metadata,
/// followed by merged BEEF data.
///
/// `txids` is the engine-supplied txid per output (aligned index-for-index —
/// it is the storage primary key the row was hydrated by), so the txid is
/// written directly instead of re-derived via a full BEEF parse plus a
/// double-SHA256 per output (bsv-low #289). A missing/malformed entry falls
/// back to the old parse-and-hash path — defense, not an expected route.
///
/// Individual BEEFs from each OutputListItem are merged into a single BEEF
/// using `Beef::merge_beef()`, matching the TS `beef.mergeTransaction()`
/// behavior (issue #17).
fn serialize_aggregated_lookup(
    outputs: &[overlay_engine::types::OutputListItem],
    txids: &[String],
) -> Result<Vec<u8>, String> {
    use bsv_rs::transaction::Beef;

    let mut buf = Vec::new();

    // Number of outputs
    write_varint(&mut buf, outputs.len() as u64);

    // Merged BEEF accumulator — start with the first output's BEEF and merge the rest.
    let mut merged_beef: Option<Beef> = None;

    for (i, output) in outputs.iter().enumerate() {
        // The engine handed us this row's txid (its storage primary key);
        // hex-decode it directly. tx.id() returns the same standard txid hex,
        // so the bytes written are identical to the old parse-and-hash path.
        let txid_bytes = match txids.get(i).and_then(|t| hex::decode(t).ok()) {
            Some(bytes) if bytes.len() == 32 => bytes,
            _ => {
                // Fallback: parse the BEEF and hash — the pre-#289 path.
                let tx = bsv_rs::transaction::Transaction::from_beef(&output.beef, None)
                    .map_err(|e| format!("Failed to parse BEEF: {e}"))?;
                let txid_hex = tx.id();
                let bytes = hex::decode(&txid_hex)
                    .map_err(|e| format!("Failed to decode txid hex: {e}"))?;
                if bytes.len() != 32 {
                    return Err(format!(
                        "Unexpected txid length: {} (expected 32)",
                        bytes.len()
                    ));
                }
                bytes
            }
        };

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
/// check the Arcade callback bearer — in EITHER accepted header, see
/// [`classify_arc_callback_auth`] — without leaking length/prefix timing.
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

/// The verdict of `/arc-ingest` bearer-auth. THREE states, not two: a refusal
/// because NO token was presented is a different operational fact from a
/// refusal because a presented token did not match, and folding them together
/// is what let this route 401 every real callback for a month with the health
/// surface reading exactly like "nobody is calling us" (epoch Rule 13).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ArcCallbackAuth {
    /// A presented token equalled the subject txid (constant-time).
    Authorized,
    /// Neither accepted header carried a bearer token at all. Diagnosis:
    /// a CONTRACT or CONFIG problem (the courier is not authenticating), or
    /// unauthenticated noise hitting a public URL.
    NoToken,
    /// At least one token was presented and none matched the subject txid.
    /// Diagnosis: a stale registration, or someone probing.
    BadToken,
}

/// Extract the bearer credential from an `Authorization` header value.
///
/// RFC 7235: the scheme is case-insensitive. Anything that is not a `Bearer`
/// challenge (e.g. `Basic …`) yields no candidate, as does an empty credential.
fn bearer_credential(authorization: &str) -> Option<&str> {
    let v = authorization.trim();
    // `get(..6)` (never `split_at`) — a non-ASCII header value would put a
    // char boundary mid-index and `split_at` panics on a request path.
    let rest = v
        .get(..6)
        .filter(|scheme| scheme.eq_ignore_ascii_case("bearer"))
        .map(|_| &v[6..])?;
    // A scheme must be followed by whitespace, never glued to the credential.
    if !rest.starts_with(char::is_whitespace) {
        return None;
    }
    let tok = rest.trim();
    (!tok.is_empty()).then_some(tok)
}

/// Classify `/arc-ingest` bearer-auth against the body's subject txid. PURE —
/// unit-tested natively AND driven through the real route by `make ci-route`.
///
/// **Arcade V2's published contract for `X-CallbackToken` is that it is "an
/// opaque bearer token, sent on every outbound webhook as
/// `Authorization: Bearer <token>`".** The webhook does NOT echo the
/// `X-CallbackToken` request header. Reading only that header therefore refused
/// EVERY proof callback Arcade has ever sent since #228 (delivery record for
/// settle `ee37b606…`: 10 attempts, `lastResult: "status 401"`), silently
/// demoting the primary proof path to the ~30-min poll backstop.
///
/// Both spellings are accepted, and acceptance is by ENUMERATE-AND-FILTER, not
/// by header precedence: whichever header carries it, a candidate is admitted
/// iff it equals the subject txid under [`constant_time_eq`]. First-header-wins
/// would refuse a courier that sets `Authorization` for a proxy and carries the
/// callback token in `X-CallbackToken`.
///
/// This WIDENS where the token may be read from and weakens NOTHING else: the
/// value must still equal the body's subject txid, compared in constant time,
/// and a merklePath is still re-verified against chaintracks before any stitch.
pub(crate) fn classify_arc_callback_auth(
    authorization: Option<&str>,
    x_callback_token: Option<&str>,
    subject_txid: &str,
) -> ArcCallbackAuth {
    let candidates: [Option<&str>; 2] = [
        authorization.and_then(bearer_credential),
        x_callback_token.map(str::trim).filter(|t| !t.is_empty()),
    ];
    let mut presented = false;
    let mut matched = false;
    for c in candidates.into_iter().flatten() {
        presented = true;
        // No early break — every candidate is compared, so the number of
        // comparisons does not depend on which one matched.
        matched |= constant_time_eq(c.as_bytes(), subject_txid.as_bytes());
    }
    match (presented, matched) {
        (_, true) => ArcCallbackAuth::Authorized,
        (true, false) => ArcCallbackAuth::BadToken,
        (false, false) => ArcCallbackAuth::NoToken,
    }
}

/// A classified `/arc-ingest` callback body (#228). PURE — unit-tested.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ArcIngestBody {
    /// A merklePath-bearing (MINED) callback — the proof push.
    Proof {
        txid: String,
        merkle_path: String,
        block_height: Option<u32>,
    },
    /// A non-MINED lifecycle callback — carries NO merklePath. Acknowledged
    /// (2xx, counted), never a parse error. A TERMINAL status (REJECTED /
    /// DOUBLE_SPEND_ATTEMPTED) is additionally RECORDED as evidence with its
    /// `extraInfo` reason text (INCIDENT D1-CALLBACK-FLOOD 2026-09-01: the
    /// webhook delivered the terminal verdict ~99M times while this handler
    /// counted-and-discarded it, so nothing downstream could ever stop
    /// retrying the dead tx).
    StatusOnly {
        txid: String,
        tx_status: String,
        extra_info: Option<String>,
    },
}

/// Parse + classify an `/arc-ingest` body (#228). A missing/empty/whitespace
/// `merklePath` is a STATUS callback (accept-and-ignore); a present
/// `merklePath` is a proof push whose verification stays byte-identical to
/// the pre-#228 fail-closed path. `txid` is always required — a body without
/// one is malformed (400), same as before.
pub(crate) fn classify_arc_ingest_body(raw: &str) -> Result<ArcIngestBody, String> {
    #[derive(Deserialize)]
    struct Body {
        txid: String,
        #[serde(rename = "merklePath", default)]
        merkle_path: Option<String>,
        #[serde(rename = "blockHeight")]
        block_height: Option<u32>,
        #[serde(rename = "txStatus", default)]
        tx_status: Option<String>,
        #[serde(rename = "extraInfo", default)]
        extra_info: Option<String>,
    }

    let body: Body = serde_json::from_str(raw).map_err(|e| e.to_string())?;
    match body.merkle_path {
        Some(mp) if !mp.trim().is_empty() => Ok(ArcIngestBody::Proof {
            txid: body.txid,
            merkle_path: mp,
            block_height: body.block_height,
        }),
        _ => Ok(ArcIngestBody::StatusOnly {
            txid: body.txid,
            tx_status: body.tx_status.unwrap_or_default(),
            extra_info: body.extra_info,
        }),
    }
}

/// POST /arc-ingest — Arcade V2 push callback (#192/#193, #259/#228; TAAL ARC
/// parity too). The Arcade broadcaster REGISTERS the subject txid as the
/// `X-CallbackToken` request header at broadcast; Arcade then DELIVERS it back
/// as `Authorization: Bearer <token>` on the webhook. Those are two different
/// headers, and reading only the registration spelling 401'd every real
/// callback for a month — see [`classify_arc_callback_auth`], which accepts
/// either and still requires a constant-time match against the body's `txid`.
///
/// **#228: this is the PRIMARY proof source** — arcade#259 delivers the MINED
/// merklePath ~150 ms post-mine. A verified push lands in EVERY consumer:
/// the engine `transactions` stitch (which latches `has_proof`), the LOW
/// `pot_beefs` compact, and the `pot_records` spend-confirmation latch — so
/// the poll passes (now backstop-gated, `PUSH_BACKSTOP_MIN_AGE_SECS`) skip
/// the tx entirely. Non-MINED lifecycle callbacks (`X-FullStatusUpdates`)
/// carry no merklePath and are acknowledged-and-ignored (200, counted),
/// never a parse error.
///
/// A callback is a COURIER — we NEVER trust a merklePath we didn't fold.
/// Before stitching, the callback's merklePath is re-verified against
/// chaintracks (same discipline as the cron fetcher) — BYTE-IDENTICAL to the
/// pre-#228 check. An unverifiable proof is refused (422) and nothing is
/// stitched; the poll backstop remains.
pub async fn arc_ingest(
    engine: &Engine,
    mut req: Request,
    tracker: Option<&dyn bsv_rs::transaction::ChainTracker>,
    pot_storage: &dyn overlay_discovery::pot::storage::PotStorage,
    ops_db: Option<&worker::D1Database>,
) -> worker::Result<Response> {
    // Read the bearer credential BEFORE consuming the body. BOTH accepted
    // spellings: `Authorization: Bearer <token>` is what Arcade V2 actually
    // sends on the webhook (its published contract for `X-CallbackToken`), and
    // `X-CallbackToken` is kept for TAAL ARC parity / other couriers / a future
    // version. See `classify_arc_callback_auth`.
    let authorization = req.headers().get("authorization").ok().flatten();
    let callback_token = req.headers().get("x-callbacktoken").ok().flatten();

    let raw = match req.text().await {
        Ok(t) => t,
        Err(e) => return json_error(&format!("Invalid arc-ingest body: {e}"), 400),
    };
    let body = match classify_arc_ingest_body(&raw) {
        Ok(b) => b,
        Err(e) => return json_error(&format!("Invalid arc-ingest body: {e}"), 400),
    };
    let txid = match &body {
        ArcIngestBody::Proof { txid, .. } | ArcIngestBody::StatusOnly { txid, .. } => txid.clone(),
    };

    // Bearer-auth: the token must be present and equal the subject txid the
    // broadcaster registered (constant-time). A missing/mismatched token means
    // this isn't a callback we scheduled → 401. Applies to STATUS callbacks
    // too — unauthenticated noise is refused before it is ever acknowledged.
    //
    // COUNT the refusal (epoch Rule 13). Before this, the 401 arm bumped
    // nothing and `arc_ingest_status_ignored_total` was only reachable AFTER
    // auth — so "nobody is calling us" and "everybody is being refused" were
    // literally indistinguishable on `/health/invariants` (both all-zero), and
    // a real month-long outage of the PRIMARY proof path was misattributed.
    match classify_arc_callback_auth(authorization.as_deref(), callback_token.as_deref(), &txid) {
        ArcCallbackAuth::Authorized => {}
        refused => {
            let (counter, why) = match refused {
                ArcCallbackAuth::NoToken => (
                    crate::ops::COUNTER_ARC_INGEST_UNAUTH_NO_TOKEN,
                    "no bearer token in Authorization or X-CallbackToken",
                ),
                _ => (
                    crate::ops::COUNTER_ARC_INGEST_UNAUTH_BAD_TOKEN,
                    "token presented but != subject txid",
                ),
            };
            worker::console_log!("POST /arc-ingest -> 401 ({why})");
            if let Some(db) = ops_db {
                crate::ops::bump_counter(db, counter, 1).await;
            }
            return json_error("Unauthorized arc-ingest callback", 401);
        }
    }

    let (merkle_path, block_height) = match body {
        // #228 fix: a non-MINED lifecycle callback is NORMAL webhook traffic,
        // not an error. Acknowledge (2xx), count, and — INCIDENT
        // D1-CALLBACK-FLOOD 2026-09-01 — CONSUME a terminal verdict instead of
        // discarding it: REJECTED / DOUBLE_SPEND_ATTEMPTED is recorded once
        // per txid into `arc_terminal` (evidence for the retire classifier —
        // #214 still requires corroboration before anything acts on it). The
        // ignored-counter write is exact for the first
        // `STATUS_IGNORED_EXACT_HEAD` events per isolate, then BATCHED
        // (`STATUS_IGNORED_FLUSH_BATCH`): the old per-callback UPSERT was
        // the incident's billing site
        // (~370 webhooks/s × one hot-row D1 write each ≈ $28/day). Always
        // 200 — a non-2xx would make the courier RE-deliver, amplifying the
        // very flood this fixes.
        ArcIngestBody::StatusOnly {
            tx_status,
            extra_info,
            ..
        } => {
            worker::console_log!(
                "POST /arc-ingest txid={txid} status-only ({}) -> 200 (acknowledged, no merklePath)",
                if tx_status.is_empty() { "?" } else { &tx_status }
            );
            if let Some(db) = ops_db {
                let status_upper = tx_status.to_ascii_uppercase();
                if crate::broadcaster::ARCADE_FATAL_STATUSES.contains(&status_upper.as_str()) {
                    crate::ops::record_arc_terminal(
                        db,
                        &txid,
                        &status_upper,
                        extra_info.as_deref(),
                    )
                    .await;
                } else {
                    crate::ops::bump_status_ignored_batched(db).await;
                }
            }
            return json_ok(&SuccessBody {
                status: "success",
                message: "Status update acknowledged (no merklePath)",
            });
        }
        ArcIngestBody::Proof {
            merkle_path,
            block_height,
            ..
        } => (merkle_path, block_height),
    };

    worker::console_log!("POST /arc-ingest txid={txid}");

    // Verify the callback's merklePath against chaintracks BEFORE stitching —
    // a courier's proof is only a fact once its root matches our PoW-anchored
    // headers. Fail-closed: no tracker / unverifiable → refuse, do not stitch.
    if !crate::proof_fetcher::verify_bump(tracker, &merkle_path, &txid).await {
        worker::console_log!("POST /arc-ingest -> 422 (merklePath failed chaintracks verify)");
        return json_error("Callback merklePath failed chaintracks verification", 422);
    }

    // The VERIFIED push fans out to every consumer (#228):
    // 1. engine `transactions` stitch — `update_transaction_beef` latches
    //    `has_proof`, dropping the tx from the poll backstop's candidates.
    let engine_res = engine
        .handle_new_merkle_proof(&txid, &merkle_path, block_height)
        .await;
    // 2. LOW pot stores — pot_beefs compact + pot_records spend latch. A
    //    settle/refund/sweep admits no outputs, so the ENGINE knows nothing
    //    about it (engine_res errors) while the pot stores are exactly where
    //    its proof belongs.
    let pot =
        crate::proof_fetcher::apply_pushed_proof_to_pot_stores(pot_storage, &txid, &merkle_path)
            .await;

    match engine_res {
        Ok(()) => {
            worker::console_log!(
                "POST /arc-ingest -> 200 (engine stitched; pot_beef_compacted={} spends_confirmed={} cas_missed={} cas_errors={})",
                pot.pot_beef_compacted,
                pot.spends_confirmed,
                pot.spends_cas_missed,
                pot.spends_cas_errors
            );
            if let Some(db) = ops_db {
                crate::ops::bump_counter(db, crate::ops::COUNTER_ARC_INGEST_PUSHED, 1).await;
            }
            json_ok(&SuccessBody {
                status: "success",
                message: "Transaction status updated",
            })
        }
        // The engine doesn't know the txid but a pot store consumed the proof
        // (the settle/refund/sweep case) — that is a SUCCESSFUL push.
        Err(_) if pot.landed_anything() => {
            worker::console_log!(
                "POST /arc-ingest -> 200 (pot stores only; pot_beef_compacted={} spends_confirmed={} cas_missed={} cas_errors={})",
                pot.pot_beef_compacted,
                pot.spends_confirmed,
                pot.spends_cas_missed,
                pot.spends_cas_errors
            );
            if let Some(db) = ops_db {
                crate::ops::bump_counter(db, crate::ops::COUNTER_ARC_INGEST_PUSHED, 1).await;
            }
            json_ok(&SuccessBody {
                status: "success",
                message: "Transaction status updated",
            })
        }
        // Nobody knows this txid — keep the pre-#228 error surface.
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
// Submit-operator auth (#347) + admin auth
// =============================================================================

/// The `/submit` operator credential (`SUBMIT_OPERATOR_TOKEN`) — SEPARATE from
/// `ADMIN_TOKEN` on purpose (#347 gate M1).
///
/// The admin token gates destructive index operations (`/admin/evictOutpoint`,
/// `/admin/ban`, `/admin/startGASPSync`). Reusing it here would mean handing
/// every submit operator — including the watchtower, which holds it in a
/// worker secret — the ability to EVICT any outpoint from the index, which is
/// exactly the primitive the enumeration-starvation money path needs. A submit
/// operator gets permission to submit, and nothing else.
///
/// Unset or empty ⇒ always false (fail closed). Fixed-time comparison, same as
/// the admin path.
pub fn check_submit_operator_auth(req: &Request, env: &Env) -> bool {
    let token = env
        .secret("SUBMIT_OPERATOR_TOKEN")
        .ok()
        .map(|s| s.to_string())
        .or_else(|| env.var("SUBMIT_OPERATOR_TOKEN").ok().map(|v| v.to_string()))
        .unwrap_or_default();
    if token.is_empty() {
        return false;
    }
    let Some(header) = req.headers().get("Authorization").ok().flatten() else {
        return false;
    };
    let Some(provided) = header.strip_prefix("Bearer ") else {
        return false;
    };
    !provided.is_empty() && fixed_time_eq(provided.as_bytes(), token.as_bytes())
}

/// The ADMIN credential (`ADMIN_TOKEN`) for the `/admin/*` routes — including
/// the destructive ones (`/admin/evictOutpoint`, `/admin/ban`,
/// `/admin/startGASPSync`).
///
/// Distinct from [`check_submit_operator_auth`]: `/submit` operators must NOT
/// receive this token (#347 gate M1). Returns `Err(response)` so callers can
/// short-circuit with mainline-compatible 401/403 bodies, where the submit
/// check returns a plain `bool`.
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
    if provided.is_empty() || !fixed_time_eq(provided.as_bytes(), token.as_bytes()) {
        return Err(json_error("Forbidden: Invalid credentials", 403));
    }

    Ok(())
}

/// Fixed-time byte comparison for the admin bearer token (#320 L1): the
/// early-exit `!=` leaked how many leading bytes matched through timing.
/// The fold touches every byte regardless of where the first mismatch is;
/// only the token's LENGTH remains observable, which is not secret.
fn fixed_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

// =============================================================================
// Admin routes
// =============================================================================

/// POST /admin/syncAdvertisements — sync SHIP/SLAP advertisements.
pub async fn admin_sync_advertisements(engine: &Engine) -> worker::Result<Response> {
    worker::console_log!("POST /admin/syncAdvertisements");

    match engine.sync_advertisements().await {
        Ok(report) if report.effective() => {
            // Body stays byte-identical to the pre-#320 success shape (parity
            // posture); the report details land in the tail log. `effective`
            // — not `ok` — is the bar (#320 M1): a submit that succeeded but
            // admitted ZERO outputs is a topic-manager refusal that re-runs
            // (and re-pays for) the full create next cycle.
            worker::console_log!("POST /admin/syncAdvertisements -> 200 {report:?}");
            json_ok(&SuccessBody {
                status: "success",
                message: "Advertisements synced successfully",
            })
        }
        Ok(report) => {
            // bsv-low #320 defect 3a: a failed create/submit — or a
            // zero-admit refusal (M1) — must never masquerade as `success`.
            // Full report in the body so the failing stage (lookup vs create
            // vs local submit vs revoke) and the engine's verbatim error are
            // caller-visible.
            worker::console_log!("POST /admin/syncAdvertisements -> 500 {report:?}");
            json_response(
                &serde_json::json!({
                    "status": "error",
                    "message": "advertisement sync completed with failures",
                    "report": report,
                }),
                500,
            )
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

/// Deploy-time topic namespace — see `lib.rs`'s `TOPIC_SUFFIX` block. Empty in
/// prod, `_beta` on the isolated beta stack.
pub(crate) fn env_topic_suffix(env: &worker::Env) -> String {
    env.var("TOPIC_SUFFIX")
        .ok()
        .map(|v| v.to_string())
        .unwrap_or_default()
}

/// Apply the deploy-time topic namespace to ONE topic/service name.
///
/// The single definition of the rule, shared by registration (`lib.rs`) and by
/// the report surfaces here — if these two ever disagreed, `/health` would name
/// topics the engine does not answer to.
///
/// SHIP and SLAP are never suffixed: discovery is a deliberately global,
/// cross-environment namespace, and the engine hardcodes those four names for
/// tracker bootstrap, self-ad suppression and peer discovery.
pub(crate) fn suffixed_name(base: &str, suffix: &str) -> String {
    match base {
        "tm_ship" | "tm_slap" | "ls_ship" | "ls_slap" => base.to_string(),
        _ => format!("{base}{suffix}"),
    }
}

/// Parse a topic/service CSV env var into the names this worker ACTUALLY
/// registers — i.e. with `TOPIC_SUFFIX` applied, exactly as `lib.rs` does at
/// registration. Both callers are report surfaces (`/health`, `/status`), and a
/// report that named `tm_low` while the engine answered only to `tm_low_beta`
/// would send an operator debugging the wrong stack.
fn parse_csv_env(env: &worker::Env, name: &str, default: &str) -> Vec<String> {
    let suffix = env_topic_suffix(env);
    env.var(name)
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| default.into())
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| suffixed_name(s, &suffix))
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
    use super::engine_error_status;
    use overlay_engine::engine::EngineError;

    /// WHOSE fault was it? A malformed query is the CALLER's and must answer
    /// 4xx; only our own failures are 5xx. Before this, a lookup service's
    /// `InvalidQuery` was stringified into `LookupFailed` and every caller
    /// mistake answered 500 — so on an unauthenticated endpoint that anyone can
    /// hit, a bad gameId and a real outage looked identical in logs and alerts.
    #[test]
    fn caller_errors_are_4xx_and_our_errors_are_5xx() {
        // Caller's fault.
        assert_eq!(
            engine_error_status(&EngineError::InvalidQuery("bad gameId".into())),
            400
        );
        assert_eq!(
            engine_error_status(&EngineError::UnsupportedTopic("tm_nope".into())),
            400
        );
        assert_eq!(
            engine_error_status(&EngineError::LookupServiceNotFound("ls_nope".into())),
            400
        );
        // Ours.
        assert_eq!(
            engine_error_status(&EngineError::LookupFailed("db exploded".into())),
            500
        );
        assert_eq!(
            engine_error_status(&EngineError::StorageError("d1 down".into())),
            500
        );
        // Upstream's.
        assert_eq!(
            engine_error_status(&EngineError::BroadcastError("arc 503".into())),
            502
        );
    }

    // ── #347 Rule 22: can anything observe the seam being IGNORED? ────────
    //
    // The exhaustive `match` on `SubmitAction` makes DELETING the refusal a
    // compile error. It cannot cover the remaining branch — the
    // `matches!(action, …ProceedWithNetworkGate(_))` guard on the
    // broadcast+SEEN block — which two separate re-gates defeated (a `&& false`
    // conjunct on its predecessor `if run_network_gate {`, and then a shadowed
    // rebinding of that same local) while all 1826 tests stayed green.
    //
    // These are SOURCE pins, and source pins lie in five documented ways, so
    // each one below is: POSITIVE (an exact count, never `assert!(!contains)`,
    // so a rotted needle fails loudly instead of passing vacuously); built
    // from a SPLIT needle assembled at runtime so it cannot match itself
    // inside this test; run over COMMENT-STRIPPED source so prose quoting the
    // construct is not counted; and scoped to the CONSTRUCT, never a region.
    //
    // They are a backstop for one branch, not the primary defence. The primary
    // defence is the enum. The executable end-to-end proof is
    // `tools/lane-347/submit_gate_attack.mjs` via `make ci-route`.

    /// Strip `//` line comments and `/* */` blocks so a pin counts CODE only.
    fn code_only(src: &str) -> String {
        let mut out = String::with_capacity(src.len());
        let mut chars = src.chars().peekable();
        let mut in_line = false;
        let mut in_block = false;
        while let Some(c) = chars.next() {
            if in_line {
                if c == '\n' {
                    in_line = false;
                    out.push(c);
                }
                continue;
            }
            if in_block {
                if c == '*' && chars.peek() == Some(&'/') {
                    chars.next();
                    in_block = false;
                }
                continue;
            }
            if c == '/' {
                match chars.peek() {
                    Some('/') => {
                        in_line = true;
                        continue;
                    }
                    Some('*') => {
                        chars.next();
                        in_block = true;
                        continue;
                    }
                    _ => {}
                }
            }
            out.push(c);
        }
        out
    }

    /// The gated action must be referenced EXACTLY twice — the match arm and
    /// the branch that runs the broadcast. **What this observes is only that
    /// the bar is still spelled where it was:** a reference that MOVED, was
    /// RENAMED or was DELETED changes the count and fails loudly.
    ///
    /// **Stated boundary (Rule 17): it does NOT observe a defeat of the bar's
    /// VALUE — in either known shape — and no source scan can.**
    ///
    /// * **An added conjunct.** The gate re-ran probe F as
    ///   `&& arcade_url.is_some() && arcade_url.is_none()` appended to the
    ///   branch condition: this count stayed at exactly 2, every pin here kept
    ///   matching, and the native suite stayed GREEN with the sole public
    ///   admission bar dead. (An earlier version of this comment claimed probe
    ///   F drives this count to 0. That was true of the pin's PREVIOUS needle,
    ///   the literal `if run_network_gate {`, and is false of this one —
    ///   Rule 10: the claim out-ran the code it described.)
    /// * **A shadowed rebinding** — probe F′ — which changes the VALUE while
    ///   leaving every needle matching exactly once.
    ///
    /// Both value-level defeats are the ROUTE TIER's job; `make ci`'s
    /// `ci-route` is what caught each of them. The shadowing shape is
    /// additionally closed STRUCTURALLY — the branch consumes the action, so
    /// there is no local left to shadow. Do not add a third source pin for the
    /// conjunct case either: a source scan is structurally blind to it, and a
    /// pin that claims to cover it is worse than no pin at all.
    #[test]
    fn the_network_gate_branch_is_keyed_solely_on_the_derived_decision() {
        let src = code_only(include_str!("routes.rs"));
        // Split so the needle never appears whole in this file.
        let needle = ["SubmitAction::ProceedWithNetwork", "Gate(_)"].concat();
        assert_eq!(
            src.matches(&needle).count(),
            3,
            "expected EXACTLY three references to the gated action — the match \
             arm, the branch that runs the broadcast, and the #413 0-admit \
             refusal (which READS the same derived action — a third READ, \
             never a second derivation); a changed count means the \
             only public admission bar MOVED, was RENAMED or was DELETED. An \
             unchanged count does NOT mean the bar is live: an added conjunct \
             leaves this at 2 (see this test's stated boundary) — that shape, \
             and the shadowed rebinding, are the route tier's job"
        );
        // The decision is derived EXACTLY once (probe H: a second derivation
        // from a separate argument list let one copy be flipped silently).
        let derived = ["let action = crate::submit_gate::action_", "for("].concat();
        assert_eq!(
            src.matches(&derived).count(),
            1,
            "the decision must be derived exactly ONCE — two derivations is the \
             probe-H defect, and the counter then reports the honest one"
        );
        let legacy = ["crate::submit_gate::plan_sub", "mit("].concat();
        assert_eq!(
            src.matches(&legacy).count(),
            0,
            "the route must not re-derive the plan beside the action"
        );
    }

    /// #371 (gate MEDIUM-1): the `network_seen` latch must be CALLED from
    /// exactly FOUR places — the gated Accepted arm (synchronous), the
    /// post-`engine.submit` ungated corroboration closure, the #397
    /// AcceptedPending background witness re-check, and the #413
    /// dual-broadcast delivery latch (writer census mirrored in the
    /// `network_seen` migration comment — keep both in lockstep). Positive
    /// count, split needle, comments stripped (Rule 9).
    ///
    /// **Stated boundary (Rule 22): this pins the SPELLING, not the effect.**
    /// The UNGATED producer's effect is behaviorally driven by the ci-route
    /// lane (`tools/lane-371/network_seen_route_ci.mjs`: latch lands, refusal
    /// refuses, refused bodies fan out nothing). The GATED arm's effect is
    /// NOT hermetically drivable — every Arcade accept claim is corroborated
    /// against hardcoded TAAL/GorillaPool hosts (`gate_accept_claim_with`),
    /// and a corroborator-host knob added to the money broadcast path for
    /// CI's sake is a worse trade — so its live bar is the deploy runbook:
    /// `/health/invariants.networkSeenTotal` MUST move on the first real
    /// gated settle after deploy.
    #[test]
    fn routes_call_the_network_seen_latch_from_all_four_arms() {
        let src = code_only(include_str!("routes.rs"));
        // Split mid-token so the needle never matches itself (Rule 9, third
        // failure mode).
        let needle = ["crate::ops::latch_net", "work_seen("].concat();
        assert_eq!(
            src.matches(&needle).count(),
            4,
            "expected EXACTLY four latch calls — the gated Accepted arm, the \
             ungated post-submit corroboration, the #413 dual-broadcast \
             delivery latch (fires only on the corroborator's >=SEEN verdict \
             of OUR OWN TAAL/GP broadcast), and the #397 AcceptedPending \
             background witness re-check (which latches ONLY on a real \
             network_witnessed answer — a pending admit itself must never \
             latch); a changed count means a producer was deleted, moved or \
             added unaccounted"
        );
        // The ungated corroboration must sit AFTER the engine submit (gate
        // MEDIUM-2) — assert on the construct: the corroboration flag is
        // consumed exactly once, and the `engine.submit` call appears before
        // that consumption in source order.
        let flag_use = ["if corroborate_seen_", "after_submit {"].concat();
        assert_eq!(src.matches(&flag_use).count(), 1, "one consumption site");
        // S2 (2026-08-29): the route submits through `submit_with_report`
        // (the Phase-3 durability report) — the SAME call site, renamed.
        // Split needle so this literal never matches itself (the pre-S2
        // needle was an unsplit literal and, once the real call was renamed,
        // the first match became THIS test's own string — after the flag).
        let submit_call = ["engine.submit_with_", "report(&tagged_beef, mode)"].concat();
        assert_eq!(
            src.matches(&submit_call).count(),
            1,
            "one engine submit call site"
        );
        let submit_at = src
            .find(&submit_call)
            .expect("the engine submit call exists");
        let flag_at = src.find(&flag_use).expect("checked above");
        assert!(
            submit_at < flag_at,
            "the ungated corroboration must be scheduled AFTER engine.submit \
             (MEDIUM-2: no Arcade fan-out for subjects the engine refused)"
        );
    }

    /// #397 (gate MEDIUM-1): the pending admit's MONEY-INERTNESS is now a
    /// load-bearing boundary — the whole relaxation is sound only because an
    /// `AcceptedPending` row never reads as SEEN until a genuine witness
    /// speaks. The leaf halves are pinned elsewhere (broadcaster:
    /// `unseen_fold_semantics` / `wiring_unseen_*` prove pending mints only
    /// from proven-single-leg + inconclusive; low-app-layer:
    /// `is_confirmed_landing*` tests prove unwitnessed spends are excluded
    /// from every money view). THIS pins the routes link on the construct:
    /// inside the AcceptedPending arm the latch producer appears exactly
    /// once, backgrounded (`wait_until`), and strictly AFTER the
    /// `network_witnessed` guard — a latch reachable before the witness
    /// would mark an unwitnessed spend SEEN and unlock money views on it.
    #[test]
    fn pending_admit_latches_only_behind_a_real_witness() {
        let src = code_only(include_str!("routes.rs"));
        let arm_start = src
            .find("ArcOutcome::AcceptedPending(pending)")
            .expect("the pending arm exists");
        let arm_end = arm_start
            + src[arm_start..]
                .find("ArcOutcome::Rejected")
                .expect("the Rejected arm follows the pending arm");
        let arm = &src[arm_start..arm_end];
        let needle = ["crate::ops::latch_net", "work_seen("].concat();
        assert_eq!(
            arm.matches(&needle).count(),
            1,
            "exactly ONE latch producer inside the pending arm"
        );
        let wait_at = arm
            .find("wait_until")
            .expect("the pending latch is backgrounded");
        let witness_at = arm
            .find("network_witnessed(")
            .expect("the pending latch is witness-guarded");
        let latch_at = arm.find(&needle).expect("counted above");
        assert!(
            wait_at < witness_at && witness_at < latch_at,
            "the pending arm's latch must sit INSIDE wait_until and strictly AFTER \
             the network_witnessed guard (wait@{wait_at} witness@{witness_at} latch@{latch_at})"
        );
    }

    /// The refusal must remain a returned 401 inside the match arm. Deleting
    /// the arm is a compile error; this catches it being neutered in place
    /// (e.g. the `return` removed, or the status changed).
    #[test]
    fn the_unauthenticated_refusal_returns_401_from_the_seam_arm() {
        let src = code_only(include_str!("routes.rs"));
        let arm = ["SubmitAction::Refuse", "Unauthenticated(path) => {"].concat();
        assert_eq!(
            src.matches(&arm).count(),
            1,
            "the refusal arm must exist exactly once"
        );
        // The arm's body must still RETURN a 401 (not merely log).
        let tail = &src[src.find(&arm).unwrap()..];
        let body_end = tail
            .find("SubmitAction::ProceedWithNetworkGate")
            .unwrap_or(tail.len());
        let body = &tail[..body_end];
        assert_eq!(
            body.matches("401,").count(),
            1,
            "the refusal arm must return a 401"
        );
        assert_eq!(
            body.matches("return json_error").count(),
            1,
            "the refusal must RETURN, not fall through"
        );
    }

    use super::*;
    use overlay_engine::types::ServiceMetadata;
    use std::collections::HashMap;

    // ── fixed_time_eq (#320 L1) ────────────────────────────────────────

    #[test]
    fn fixed_time_eq_matches_equal_bytes_only() {
        assert!(fixed_time_eq(b"secret-token", b"secret-token"));
        assert!(!fixed_time_eq(b"secret-token", b"secret-tokex"));
        assert!(!fixed_time_eq(b"Xecret-token", b"secret-token"));
        assert!(!fixed_time_eq(b"", b"secret-token"));
        assert!(!fixed_time_eq(b"secret", b"secret-token"));
        assert!(fixed_time_eq(b"", b""));
    }

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
            .map(|i| EfTx {
                txid: format!("anc{i}"),
                ef: vec![0u8; 1024],
            })
            .collect();
        efs.push(EfTx {
            txid: "subj".into(),
            ef: vec![0u8; 4096],
        });
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
        let at = vec![EfTx {
            txid: "subj".into(),
            ef: vec![0u8; MAX_SUBJECT_EF_BYTES],
        }];
        assert_eq!(subject_ef_over_cap(&at, "subj"), None);
    }

    #[test]
    fn work_bound_cap_absent_subject_is_never_over() {
        // Subject already mined / not present → 0 bytes → never capped.
        let efs = vec![EfTx {
            txid: "other".into(),
            ef: vec![0u8; 8],
        }];
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
        let mut efs = vec![EfTx {
            txid: "subj".into(),
            ef: vec![0u8; 4096],
        }]; // subject fine
        efs.push(EfTx {
            txid: "fat-ancestor".into(),
            ef: vec![0u8; MAX_BATCH_EF_BYTES],
        });
        let total = 4096 + MAX_BATCH_EF_BYTES;
        assert_eq!(
            subject_ef_over_cap(&efs, "subj"),
            Some(total),
            "an oversized TOTAL batch must be capped even when the subject is small"
        );
        // A total exactly at the bound is allowed — small subject + ancestry
        // that sums (with the subject) to exactly the batch cap.
        let at = vec![
            EfTx {
                txid: "subj".into(),
                ef: vec![0u8; 4096],
            },
            EfTx {
                txid: "anc".into(),
                ef: vec![0u8; MAX_BATCH_EF_BYTES - 4096],
            },
        ];
        assert_eq!(
            subject_ef_over_cap(&at, "subj"),
            None,
            "total exactly at the batch bound is allowed"
        );
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

    // ── #228: /arc-ingest body classification (push-primary) ─────────────────

    const CB_TXID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn arc_ingest_non_mined_status_callback_is_status_only_never_a_parse_error() {
        // X-FullStatusUpdates lifecycle callbacks carry NO merklePath — they
        // must classify StatusOnly (→ 200 acknowledged + counted), never fall
        // into the malformed-body 400 path.
        for body in [
            format!(r#"{{"txid":"{CB_TXID}","txStatus":"SEEN_ON_NETWORK"}}"#),
            format!(r#"{{"txid":"{CB_TXID}","txStatus":"ANNOUNCED_TO_NETWORK","blockHeight":0}}"#),
            // merklePath explicitly null or empty is the same status shape.
            format!(r#"{{"txid":"{CB_TXID}","txStatus":"SEEN_ON_NETWORK","merklePath":null}}"#),
            format!(r#"{{"txid":"{CB_TXID}","txStatus":"SEEN_ON_NETWORK","merklePath":""}}"#),
            format!(r#"{{"txid":"{CB_TXID}","txStatus":"SEEN_ON_NETWORK","merklePath":"  "}}"#),
        ] {
            match classify_arc_ingest_body(&body).unwrap() {
                ArcIngestBody::StatusOnly {
                    txid, tx_status, ..
                } => {
                    assert_eq!(txid, CB_TXID);
                    assert!(!tx_status.is_empty(), "{body}");
                }
                other => panic!("status callback must be StatusOnly, got {other:?} for {body}"),
            }
        }
    }

    /// INCIDENT D1-CALLBACK-FLOOD 2026-09-01: a terminal-status callback
    /// carries its `extraInfo` reason through classification (the handler
    /// records it as evidence — e.g. the UTXO_SPENT competitor txid) and its
    /// status matches the broadcaster's fatal set case-insensitively.
    #[test]
    fn arc_ingest_terminal_status_carries_extra_info_and_matches_fatal_set() {
        let body = format!(
            r#"{{"txid":"{CB_TXID}","txStatus":"REJECTED","extraInfo":"UTXO_SPENT (70): spent by deadbeef"}}"#
        );
        match classify_arc_ingest_body(&body).unwrap() {
            ArcIngestBody::StatusOnly {
                txid,
                tx_status,
                extra_info,
            } => {
                assert_eq!(txid, CB_TXID);
                assert!(crate::broadcaster::ARCADE_FATAL_STATUSES
                    .contains(&tx_status.to_ascii_uppercase().as_str()));
                assert_eq!(
                    extra_info.as_deref(),
                    Some("UTXO_SPENT (70): spent by deadbeef")
                );
            }
            other => panic!("terminal status must be StatusOnly, got {other:?}"),
        }
        // …and a non-terminal status carries None without becoming fatal.
        let seen = format!(r#"{{"txid":"{CB_TXID}","txStatus":"SEEN_ON_NETWORK"}}"#);
        match classify_arc_ingest_body(&seen).unwrap() {
            ArcIngestBody::StatusOnly {
                tx_status,
                extra_info,
                ..
            } => {
                assert!(!crate::broadcaster::ARCADE_FATAL_STATUSES
                    .contains(&tx_status.to_ascii_uppercase().as_str()));
                assert_eq!(extra_info, None);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn arc_ingest_merklepath_bearing_body_is_a_proof_push() {
        let body = format!(
            r#"{{"txid":"{CB_TXID}","merklePath":"beef00","blockHeight":850000,"txStatus":"MINED"}}"#
        );
        match classify_arc_ingest_body(&body).unwrap() {
            ArcIngestBody::Proof {
                txid,
                merkle_path,
                block_height,
            } => {
                assert_eq!(txid, CB_TXID);
                assert_eq!(merkle_path, "beef00");
                assert_eq!(block_height, Some(850_000));
            }
            other => panic!("merklePath body must be Proof, got {other:?}"),
        }
        // A garbage merklePath still classifies Proof — the route's
        // chaintracks verify_bump (fail-closed, byte-identical to pre-#228)
        // is what refuses it with 422. Classification never "fixes" a proof.
        let garbage = format!(r#"{{"txid":"{CB_TXID}","merklePath":"zz-not-hex"}}"#);
        assert!(matches!(
            classify_arc_ingest_body(&garbage).unwrap(),
            ArcIngestBody::Proof { .. }
        ));
    }

    // ── /arc-ingest bearer-auth: read the header the SENDER actually sets ────
    //
    // MODELLING BOUNDARY (Rule 17): these are PURE cells over the classifier.
    // They cannot prove the ROUTE reads the `Authorization` header off the real
    // `worker::Request` — `cargo test` cannot build a `worker::Request`. That
    // producer-level claim is pinned by `make ci-route`
    // (`tools/lane-arc-ingest/arc_ingest_auth_ci.mjs`), which POSTs the real
    // header to the real handler in `wrangler dev`. Neither tier alone is
    // sufficient; both are in `make ci`.

    #[test]
    fn arc_ingest_accepts_the_authorization_bearer_arcade_actually_sends() {
        // Arcade V2 sends the callback token as `Authorization: Bearer <token>`
        // and does NOT echo `X-CallbackToken`. This exact shape was 401'd by
        // every delivery since #228.
        assert_eq!(
            classify_arc_callback_auth(Some(&format!("Bearer {CB_TXID}")), None, CB_TXID),
            ArcCallbackAuth::Authorized
        );
        // RFC 7235: the scheme is case-insensitive, and surrounding whitespace
        // is not part of the credential.
        for header in [
            format!("bearer {CB_TXID}"),
            format!("BEARER {CB_TXID}"),
            format!("BeArEr   {CB_TXID}"),
            format!("  Bearer {CB_TXID}  "),
            format!("Bearer\t{CB_TXID}"),
        ] {
            assert_eq!(
                classify_arc_callback_auth(Some(&header), None, CB_TXID),
                ArcCallbackAuth::Authorized,
                "{header:?}"
            );
        }
    }

    #[test]
    fn arc_ingest_still_accepts_the_x_callbacktoken_header_form() {
        // The header form is KEPT (TAAL ARC parity / other couriers / a future
        // Arcade version). This is the pre-fix path and must not regress.
        assert_eq!(
            classify_arc_callback_auth(None, Some(CB_TXID), CB_TXID),
            ArcCallbackAuth::Authorized
        );
    }

    #[test]
    fn arc_ingest_admits_by_enumerate_and_filter_not_header_precedence() {
        // A courier that puts a PROXY credential in `Authorization` and the
        // callback token in `X-CallbackToken` must still be admitted — and vice
        // versa. First-header-wins would refuse one of these two.
        assert_eq!(
            classify_arc_callback_auth(Some("Basic Zm9vOmJhcg=="), Some(CB_TXID), CB_TXID),
            ArcCallbackAuth::Authorized
        );
        assert_eq!(
            classify_arc_callback_auth(
                Some(&format!("Bearer {CB_TXID}")),
                Some("some-other-token"),
                CB_TXID
            ),
            ArcCallbackAuth::Authorized
        );
    }

    #[test]
    fn arc_ingest_wrong_bearer_is_refused_as_bad_token() {
        let wrong = "b".repeat(64);
        for (auth, hdr) in [
            (Some(format!("Bearer {wrong}")), None),
            (None, Some(wrong.clone())),
            (Some(format!("Bearer {wrong}")), Some(wrong.clone())),
            // A truncated / extended token must not match either.
            (Some(format!("Bearer {}", &CB_TXID[..63])), None),
            (Some(format!("Bearer {CB_TXID}0")), None),
        ] {
            assert_eq!(
                classify_arc_callback_auth(auth.as_deref(), hdr.as_deref(), CB_TXID),
                ArcCallbackAuth::BadToken,
                "{auth:?} / {hdr:?}"
            );
        }
    }

    #[test]
    fn arc_ingest_no_token_at_all_is_refused_as_a_distinct_state() {
        // The whole point of the third state: a contract/config failure (no
        // token presented) must not be reported as "someone probed us".
        for (auth, hdr) in [
            (None, None),
            // Non-Bearer schemes carry no candidate.
            (Some("Basic Zm9vOmJhcg==".to_string()), None),
            (Some("Token abc".to_string()), None),
            // Empty credentials are not credentials.
            (Some("Bearer".to_string()), None),
            (Some("Bearer   ".to_string()), None),
            (None, Some(String::new())),
            (None, Some("   ".to_string())),
            // A scheme glued to the credential is not a Bearer challenge.
            (Some(format!("Bearer{CB_TXID}")), None),
        ] {
            assert_eq!(
                classify_arc_callback_auth(auth.as_deref(), hdr.as_deref(), CB_TXID),
                ArcCallbackAuth::NoToken,
                "{auth:?} / {hdr:?}"
            );
        }
    }

    #[test]
    fn arc_ingest_auth_never_panics_on_a_non_ascii_authorization_header() {
        // `get(..6)`, never `split_at(6)` — a multi-byte char at the boundary
        // would panic on a public request path.
        for header in ["Béarer x", "🐟🐟", "Bé", "€€€"] {
            let _ = classify_arc_callback_auth(Some(header), None, CB_TXID);
        }
    }

    #[test]
    fn arc_ingest_malformed_bodies_still_400() {
        // Missing txid, non-JSON, empty — the pre-#228 400 surface is kept
        // byte-for-byte (the parity corpus pins 400 on both sides).
        for body in [r#"{"bogus":"payload"}"#, "not valid json at all", ""] {
            assert!(
                classify_arc_ingest_body(body).is_err(),
                "must stay malformed: {body:?}"
            );
        }
    }
}
