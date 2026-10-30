//! Route handlers — thin worker glue over the pure helpers in
//! [`crate::logic`]. Every handler is a READ: `SELECT`s against the
//! low-overlay D1 (`OVERLAY_DB`) or a GET through the `CHAINTRACKS` service
//! binding, never a write. Infrastructure faults (missing binding, D1
//! error, chaintracks error) map to 5xx JSON with `no-store` — a fault is
//! never cached and never shaped like a real answer.
//!
//! NO CACHING (owner call, 2026-07-14): the Cache API misbehaves on
//! workers.dev (intermittent CF 1042s observed live) and the scaling win is
//! the QUERY COLLAPSE, not the cache — `/utxo-status` answers a whole batch
//! of outpoints with a batched D1 query (`batch_where_sql`), so a home-mount
//! gather is one request → few queries. Every response is `no-store`.
//!
//! D1 100-PARAM CAP: the batch WHERE binds 2 params per outpoint and D1 caps a
//! statement at 100 bound params, so a single query of >50 outpoints 503s (the
//! mainnet Leaderboard bug: a 57-pot batch → HTTP 503 → swallowed → empty
//! board). The handlers chunk internally at [`logic::D1_CHUNK_OUTPOINTS`] and
//! merge — the public contract (input, output shape, MAX_OUTPOINTS cap) is
//! unchanged; the server never 503s on a legitimately-sized request regardless
//! of client chunk size. A chunk's D1 error still surfaces as the same 503.

use crate::d1_ledger::Counted;
use overlay_engine::beef_limits;
use serde::Deserialize;
use worker::wasm_bindgen::JsValue;
use worker::{console_warn, Headers, Method, Request, RequestInit, Response, Result, RouteContext};

use crate::auth::{AuthState, IdentityDecision};

use crate::logic::{
    assemble_pots_view, assemble_recovery_view, assemble_statuses, attach_evictions,
    batch_where_sql, beef_body, chunk_outpoints, decode_beef_hex, distinct_db_txids,
    evictions_by_txid_sql, health_body, parse_outpoints, pots_view_body, pots_view_join_sql,
    recovery_view_body, recovery_view_sql, tip_body, utxo_status_body, valid_identity, valid_txid,
    EvictionLedgerRow, Outpoint, PotRecordRow, PotsViewRow, RecoveryRow, ResultMarkerRow,
};

/// Build a JSON response (always `no-store` — see the module note).
pub(crate) fn json_response(body: String, status: u16) -> Result<Response> {
    let mut resp = Response::ok(body)?.with_status(status);
    resp.headers_mut().set("Content-Type", "application/json")?;
    resp.headers_mut().set("Cache-Control", "no-store")?;
    Ok(resp)
}

/// JSON error.
pub(crate) fn json_error(msg: &str, status: u16) -> Result<Response> {
    json_response(serde_json::json!({ "error": msg }).to_string(), status)
}

/// #411: JSON response the edge MAY cache briefly — for GLOBAL display-tier
/// views whose answer is identical for every caller (no identity in the
/// query). Under a 16-pair burst, 32 clients polling `/leaderboard` each paid
/// the full whole-history window (9-13 s measured live, 2026-08-26); a 5 s
/// shared edge cache collapses that to ~1 D1 hit per window. Money views and
/// anything identity-scoped keep `json_response`'s `no-store` — display
/// freshness is the ONLY thing traded, and the SEEN-bar gates stay
/// server-side.
/// Rebuild a Durable-Object-forwarded response into one with MUTABLE headers.
///
/// A `Response` that came out of `stub.fetch_with_str` wraps an immutable JS
/// `Headers` object — every later `headers_mut().set(...)` fails, and the
/// worker-level `cors::add_cors_headers` swallows those failures by design
/// (`let _ =`). Returning such a response verbatim therefore ships WITHOUT
/// `Access-Control-Allow-Origin`, and every BROWSER caller loses the route
/// while curl/node keep working — exactly the 2026-08-27 regression that
/// blanked the board for every client after the BoardView fronting landed
/// (the page fell back to the degraded client gather; the harness saw
/// "0 wins / all unconfirmed" with a PERFECT served payload). Reading the
/// body and re-wrapping it costs one copy and restores header mutability;
/// the DO's own cache header is re-applied here (5 s — the actor's SWR
/// freshness), and CORS attaches at the single worker-level site.
async fn rebuild_do_response(resp: &mut Response, view_route: &'static str) -> Result<Response> {
    let status = resp.status_code();
    let timing = resp.headers().get("Server-Timing").ok().flatten();
    let body = resp.text().await?;
    let mut out = json_response_cached(body, status, 5)?;
    // bsv-low #499: the view actor's compute ran detached from this request; its D1 figures (`d1view`) ride the
    // answer and are recorded here, in the worker's isolate, under `view_route` (the census reads them on /health)
    if let Some(t) = timing.as_deref() {
        if let Some(view) = crate::d1_ledger::parse_segment(t, "d1view") {
            crate::d1_ledger::record(view_route, view);
        }
        out.headers_mut().set("Server-Timing", t)?;
    }
    Ok(out)
}

pub(crate) fn json_response_cached(
    body: String,
    status: u16,
    max_age_secs: u32,
) -> Result<Response> {
    let mut resp = Response::ok(body)?.with_status(status);
    resp.headers_mut().set("Content-Type", "application/json")?;
    resp.headers_mut().set(
        "Cache-Control",
        &format!("public, max-age={max_age_secs}, s-maxage={max_age_secs}"),
    )?;
    Ok(resp)
}

/// The resolved view identity for an identity-scoped route, or the refusal
/// response to return instead.
pub(crate) enum ViewIdentity {
    /// The effective identity (lowercase; empty string = none resolved — the
    /// route's existing empty-view behavior applies).
    Identity(String),
    /// A refusal (mismatch 403 / strict-unauth 401) — return it as-is.
    Refuse(Result<Response>),
}

/// THE identity seam for the five identity-scoped routes (bsv-low #318,
/// Rule 15 — handlers receive the resolved identity from ONE place; none of
/// them re-chooses between the session identity and the `?identity=` claim).
///
/// The decision itself is [`crate::auth::resolve_view_identity`]; this
/// wrapper only extracts the query param and maps refusals to honest JSON:
/// * session identity ≠ `?identity=` → 403 naming BOTH keys (never a silent
///   preference for either — identity keys are public in this system, so
///   echoing them is honest, not a leak);
/// * strict mode + anonymous → 401 (defense in depth; the front door already
///   refuses these before routing).
///
/// Errors go through `json_error`, never `?` (the live-view LOW-7 rule: an
/// escaped error is a response with neither wildcard CORS nor `no-store`).
pub(crate) fn view_identity(req: &Request, ctx: &RouteContext<AuthState>) -> ViewIdentity {
    let url = match req.url() {
        Ok(u) => u,
        Err(e) => {
            console_warn!("[auth] request URL unavailable: {e}");
            return ViewIdentity::Refuse(json_error("request url unavailable", 503));
        }
    };
    let query = url
        .query_pairs()
        .find(|(k, _)| k == "identity")
        .map(|(_, v)| v.into_owned());
    match crate::auth::resolve_view_identity(ctx.data.mode, &ctx.data.caller, query.as_deref()) {
        IdentityDecision::Serve(id) => ViewIdentity::Identity(id.unwrap_or_default()),
        IdentityDecision::RefuseMismatch {
            session_identity,
            query_identity,
        } => {
            crate::auth::count_mismatch_refused();
            ViewIdentity::Refuse(json_response(
                serde_json::json!({
                    "error": "identity mismatch: the authenticated BRC-103/104 identity does not match the ?identity= parameter",
                    "authenticatedIdentity": session_identity,
                    "queryIdentity": query_identity,
                })
                .to_string(),
                403,
            ))
        }
        IdentityDecision::RefuseUnauthenticated => {
            crate::auth::count_strict_refused_unauthenticated();
            ViewIdentity::Refuse(json_error(
                "authentication required: AUTH_ENFORCE is on — authenticate via the BRC-103/104 handshake at /.well-known/auth",
                401,
            ))
        }
    }
}

/// #375 — the pre-launch era write-off cutoff (ms since epoch), read once
/// per request from the `WRITTEN_OFF_BEFORE_MS` var (the `STORAGE_EPOCH`
/// read pattern) and threaded into the money-listing SQL builders as an
/// `Option<i64>`. `None` (unset/malformed var) is INERT — every query runs
/// byte-identical to the un-configured build. The routes that consume it
/// append the cutoff as ONE extra bind iff `Some`, matching the exactly-one
/// placeholder [`crate::logic::era_filter_sql`] emits.
fn written_off_before_ms(ctx: &RouteContext<AuthState>) -> Option<i64> {
    written_off_before_ms_env(&ctx.env)
}

/// Env-taking twin (S3a): the BoardView actor computes outside any route ctx.
pub(crate) fn written_off_before_ms_env(env: &worker::Env) -> Option<i64> {
    let raw = crate::logic::normalize_written_off_before_ms(
        env.var("WRITTEN_OFF_BEFORE_MS").ok().map(|v| v.to_string()),
    );
    // Review MED-2 — the future-cutoff belt (`clamp_future_cutoff` docs): a
    // well-formed WRONG value (extra digit, pasted future instant) must
    // never blank every money view; refuse it LOUDLY, never silently.
    let now_ms = worker::Date::now().as_millis() as i64;
    let clamped = crate::logic::clamp_future_cutoff(raw, now_ms);
    if let (Some(raw_ms), None) = (raw, clamped) {
        console_warn!(
            "[era] WRITTEN_OFF_BEFORE_MS={raw_ms} is at/after now={now_ms} — MISCONFIGURED \
             (the #375 cutoff must pre-date launch); treating as unset"
        );
    }
    clamped
}

/// The #375 cutoff as a D1 bind value. Ms-since-epoch fits f64 exactly
/// (`2^53` headroom) — the crate's number-bind convention.
/// The #375 HEIGHT cutoff (client-protective only — never SQL; see the
/// normalizer's doc). Served verbatim on `/epoch` + echoed on `/health`.
fn written_off_before_height(ctx: &RouteContext<AuthState>) -> Option<i64> {
    crate::logic::normalize_written_off_before_height(
        ctx.env
            .var("WRITTEN_OFF_BEFORE_HEIGHT")
            .ok()
            .map(|v| v.to_string()),
    )
}

fn era_bind(ms: i64) -> JsValue {
    JsValue::from_f64(ms as f64)
}

/// `pot_records` row as D1 returns it (numbers as f64 — codebase convention,
/// see overlay-cloudflare `d1_discovery.rs`). Converted to the pure
/// [`PotRecordRow`] for input-order assembly in `logic`.
#[derive(Deserialize)]
struct PotRowD1 {
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: f64,
    spent: f64,
    #[serde(rename = "spendingTxid")]
    spending_txid: Option<String>,
    /// `serde(default)` (0.0) tolerates a read that races the overlay's
    /// additive `spentConfirmed` migration.
    #[serde(rename = "spentConfirmed", default)]
    spent_confirmed: f64,
    /// #371 witness pair; `default` tolerates a read racing the additive
    /// migration — absent = no third arm (the strict confirmed bar).
    #[serde(rename = "spenderFinal", default)]
    spender_final: Option<f64>,
    #[serde(rename = "spenderSeen", default)]
    spender_seen: Option<f64>,
    /// admit-fast step 4: `fundingSeen` (the pot's own witness).
    #[serde(rename = "fundingSeen", default)]
    funding_seen: Option<f64>,
    /// bsv-low #621 (L5): the funding latch's witness pair and its legacy stamp (`network_seen` columns).
    #[serde(rename = "fundingSeenAt", default)]
    funding_seen_at: Option<f64>,
    #[serde(rename = "fundingSeenBy", default)]
    funding_seen_by: Option<String>,
    #[serde(rename = "fundingSeenByAt", default)]
    funding_seen_by_at: Option<f64>,
}

impl PotRowD1 {
    fn into_row(self) -> PotRecordRow {
        PotRecordRow {
            txid: self.txid,
            vout: self.output_index as u32,
            spent: self.spent != 0.0,
            spending_txid: self.spending_txid,
            spent_confirmed: self.spent_confirmed != 0.0,
            spender_final: self.spender_final.map(|v| v != 0.0),
            spender_seen: self.spender_seen.map(|v| v != 0.0),
            funding_seen: self.funding_seen.map(|v| v != 0.0),
            // bsv-low #621: the shared mapping (the named pair, or a legacy latch's `seenAt` in ms with no source)
            funding_seen_by: overlay_discovery::pot::storage::seen_witness_of_row(
                self.funding_seen.is_some_and(|v| v != 0.0),
                self.funding_seen_by,
                self.funding_seen_by_at.map(|v| v as i64),
                self.funding_seen_at.map(|v| v as i64),
            ),
        }
    }
}

/// `transactions` row: the BEEF BLOB read back as hex — the exact read-back
/// idiom the engine itself uses (`d1_storage.rs` `hex(t.beef) as beef`),
/// avoiding D1 BLOB→JS deserialization quirks.
#[derive(Deserialize)]
struct BeefRow {
    /// `hex(NULL)` is NULL, so a row with an empty beef column arrives `None`.
    beef: Option<String>,
}

/// `GET /utxo-status?outpoints=<txid>.<vout>,…` — spent-status of up to 64
/// pot outpoints from the durable `pot_records` landing-proof index, in ONE
/// batched D1 query.
///
/// Fail-safe shape: an outpoint with no row is `known:false, spent:null` —
/// this surface never asserts "unspent" for an outpoint it has never seen.
pub async fn utxo_status(req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    let url = req.url()?;
    let Some(param) = url
        .query_pairs()
        .find(|(k, _)| k == "outpoints")
        .map(|(_, v)| v.into_owned())
    else {
        return json_error("missing outpoints query parameter", 400);
    };
    let outpoints = match parse_outpoints(&param) {
        Ok(ops) => ops,
        Err(msg) => return json_error(&msg, 400),
    };

    let db = match ctx.env.d1("OVERLAY_DB") {
        Ok(db) => db,
        Err(e) => {
            console_warn!("[utxo-status] OVERLAY_DB binding unavailable: {e}");
            return json_error("database unavailable", 503);
        }
    };

    // One D1 query PER CHUNK (≤ D1_CHUNK_OUTPOINTS outpoints ⇒ ≤ 90 binds),
    // merged into one response. A single un-chunked query of >50 outpoints
    // exceeds D1's 100 bound-param cap and 503s (the mainnet Leaderboard bug);
    // chunking keeps every statement under the cap for any request up to
    // MAX_OUTPOINTS. Chunks run sequentially (simple, correct — no race).
    // FAIL-SAFE: any chunk's D1 error returns the SAME 503 the caller already
    // handles and serves no body — a failed chunk is unknown-for-those-rows,
    // never a fabricated all-unknown result. Rows merge across chunks;
    // assemble_statuses re-keys them onto the requested outpoints (order-free).
    let mut rows: Vec<PotRecordRow> = Vec::with_capacity(outpoints.len());
    for chunk in chunk_outpoints(&outpoints) {
        let mut binds: Vec<JsValue> = Vec::with_capacity(chunk.len() * 2);
        for op in chunk {
            binds.push(JsValue::from_str(&op.db_txid()));
            binds.push(JsValue::from_f64(f64::from(op.vout)));
        }
        let stmt = db.prepare(batch_where_sql(chunk.len())).bind(&binds)?;
        match stmt
            .counted_all()
            .await
            .and_then(|r| r.results::<PotRowD1>())
        {
            Ok(chunk_rows) => rows.extend(chunk_rows.into_iter().map(PotRowD1::into_row)),
            Err(e) => {
                console_warn!("[utxo-status] pot_records batch query failed: {e}");
                return json_error("database query failed", 503);
            }
        }
    }

    let mut entries = assemble_statuses(&outpoints, &rows);
    // bsv-low E649 L6: the eviction ledger by txid, so an evicted pot is SERVED as evicted (the client keeps
    // no memory of having seen it held). One PK read per chunk of distinct txids; a fault leaves the key
    // absent on every entry (never a fabricated `null`, which would read as "no eviction").
    if let Some(ledger) = read_eviction_ledger(&db, &distinct_db_txids(&outpoints)).await {
        attach_evictions(&mut entries, &ledger);
    }
    json_response(
        crate::tracker_host::runtime::outpoint_body(&ctx.env, &utxo_status_body(&entries)).await?,
        200,
    )
}

/// bsv-low E649 L6: read `pot_evictions` for these lowercase txids. `None` on any fault (the caller omits the
/// field); read only.
async fn read_eviction_ledger(
    db: &worker::D1Database,
    txids: &[String],
) -> Option<Vec<EvictionLedgerRow>> {
    #[derive(Deserialize)]
    struct EvictionD1 {
        txid: String,
        #[serde(rename = "evictedAt")]
        evicted_at: f64,
        #[serde(rename = "readmittedAt", default)]
        readmitted_at: Option<f64>,
        #[serde(default)]
        reason: Option<String>,
    }
    let mut out = Vec::new();
    for chunk in txids.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
        let binds: Vec<JsValue> = chunk.iter().map(|t| JsValue::from_str(t)).collect();
        let rows = match db.prepare(evictions_by_txid_sql(chunk.len())).bind(&binds) {
            Ok(stmt) => stmt
                .counted_all()
                .await
                .and_then(|r| r.results::<EvictionD1>()),
            Err(e) => Err(e),
        };
        match rows {
            Ok(rows) => out.extend(rows.into_iter().map(|r| EvictionLedgerRow {
                txid: r.txid.to_ascii_lowercase(),
                evicted_at: r.evicted_at as i64,
                readmitted_at: r.readmitted_at.map(|v| v as i64),
                reason: r.reason.unwrap_or_default(),
            })),
            Err(e) => {
                console_warn!(
                    "[utxo-status] eviction ledger read failed (the evicted field is omitted): {e}"
                );
                return None;
            }
        }
    }
    Some(out)
}

/// `GET /beef/:txid` — the BEEF bytes for a txid, read from `pot_beefs`
/// FIRST, then the engine's `transactions` table.
///
/// `pot_beefs` is the DURABLE pot-tx store (`txid TEXT PRIMARY KEY, beef
/// BLOB NOT NULL`): `ls_pot` writes the funding beef on admit and the
/// settle/refund beef on spend, and nothing ever deletes a row — it survives
/// the engine's lifecycle. `transactions` is best-effort for anything else:
/// the engine only writes it on `insert_output` (a settle, which admits no
/// outputs, never gets a row) and the deep-delete removes it when a spent
/// unretained coin is cleaned up. Missing everywhere (no row, NULL/empty
/// beef, undecodable) → 404, so the answer upgrades by itself once the
/// overlay stores the tx.
/// Why a stored BEEF could not be served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoadFault {
    /// The read faulted (never shaped like a definitive not-found).
    Db,
    /// bsv-low M19 R2: a bump the current header refutes could not be
    /// stripped (a trimmed store): 503 until the overlay re-anchors.
    Refuted(u64),
    OverLimit,
}

/// Load ONE stored BEEF by txid, with the same trust/guard/compaction
/// semantics every serving route needs. `Ok(None)` = genuinely absent.
///
/// THE one loader (round-2 review M3): `/beef` and every step of the
/// `/credit-beef` walk read through it, so the read-side reorg guard
/// (`beef_guard`) and the trust-licensed compaction apply to both, and an
/// assembled ancestry can never trust different bytes than the single-txid
/// route.
async fn load_stored_beef(
    env: &worker::Env,
    db: &worker::D1Database,
    txid: &str,
) -> std::result::Result<Option<Vec<u8>>, LoadFault> {
    let key = txid.to_ascii_lowercase();
    let mut faulted = false;
    for (table, sql, legacy_sql) in [
        (
            "pot_beefs",
            POT_BEEFS_TRUST_SQL,
            "SELECT hex(beef) AS beef FROM pot_beefs WHERE txid = ?",
        ),
        (
            "transactions",
            TRANSACTIONS_TRUST_SQL,
            "SELECT hex(beef) AS beef FROM transactions WHERE txid = ?",
        ),
    ] {
        let Ok(stmt) = db.prepare(sql).bind(&[JsValue::from_str(&key)]) else {
            faulted = true;
            continue;
        };
        let (row, proof_verified): (Option<BeefRow>, bool) =
            match stmt.counted_first::<BeefTrustRow>().await {
                Ok(row) => {
                    let verified =
                        row.as_ref().and_then(|r| r.proof_verified).unwrap_or(0.0) != 0.0;
                    (row.map(|r| BeefRow { beef: r.beef }), verified)
                }
                Err(_) => {
                    let Ok(stmt) = db.prepare(legacy_sql).bind(&[JsValue::from_str(&key)]) else {
                        faulted = true;
                        continue;
                    };
                    match stmt.counted_first::<BeefRow>().await {
                        Ok(row) => (row, false),
                        Err(e) => {
                            console_warn!("[credit-beef] {table} query failed: {e}");
                            faulted = true;
                            continue;
                        }
                    }
                }
            };
        if let Some(bytes) = row.and_then(|r| r.beef).and_then(|h| decode_beef_hex(&h)) {
            // bsv-low M19 R2: never serve a bump the current header refutes
            // (a verified row: its recent bumps; an unverified row: every
            // bump). A refuted one is stripped, or the row is refused.
            let bytes = match crate::beef_guard::guard_served_beef(
                env,
                Some(db),
                &key,
                &bytes,
                proof_verified,
            )
            .await
            {
                crate::beef_guard::Guarded::Serve(bytes) => bytes,
                crate::beef_guard::Guarded::Refuted { height } => {
                    return Err(LoadFault::Refuted(height))
                }
                crate::beef_guard::Guarded::OverLimit => return Err(LoadFault::OverLimit),
            };
            // Serve-time compaction (#192/#193, P4), licensed ONLY by the
            // row's VERIFIED proof latch (bsv-low#304): trimming decides
            // mined-ness from IN-BEEF bump presence, and an unverified row's
            // bumps are submitter bytes. An unverified row is served
            // byte-for-byte as stored (weaker, never wrong).
            return Ok(Some(if proof_verified {
                crate::compaction::compact_beef(&key, &bytes)
            } else {
                bytes
            }));
        }
    }
    if faulted {
        return Err(LoadFault::Db);
    }
    Ok(None)
}

/// The one answer for a [`LoadFault`] (both routes).
fn load_fault_response(fault: LoadFault) -> Result<Response> {
    match fault {
        LoadFault::Db => json_error("database query failed", 503),
        LoadFault::Refuted(height) => json_error(&crate::beef_guard::refuted_body(height), 503),
        LoadFault::OverLimit => json_error("stored BEEF exceeds parser limits", 413),
    }
}

/// `GET /credit-beef/:txid` — the ancestry a WALLET needs to credit this
/// subject, assembled from the index so the client stores nothing.
///
/// See `credit_beef` for why this exists and why `/lookup` +
/// `x-history-depth` is not the vehicle (it strips bumps). The walk follows
/// the TRANSACTION GRAPH via each unproven tx's input txids, merging stored
/// BEEFs and preserving their proofs, and stops at any transaction that
/// carries a bump.
///
/// `complete: false` is served with a 200 and the partial ancestry: the
/// caller decides whether to retry (a parent may still be propagating). It is
/// never dressed up as success — the flag is the contract.
pub async fn credit_beef(_req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    let Some(txid) = ctx.param("txid").cloned() else {
        return json_error("missing txid", 400);
    };
    if !valid_txid(&txid) {
        return json_error("invalid txid (expect 64 hex chars)", 400);
    }
    let db = match ctx.env.d1("OVERLAY_DB") {
        Ok(db) => db,
        Err(e) => {
            console_warn!("[credit-beef] OVERLAY_DB binding unavailable: {e}");
            return json_error("database unavailable", 503);
        }
    };

    let subject = match load_stored_beef(&ctx.env, &db, &txid).await {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return json_error(&format!("BEEF not found for txid: {txid}"), 404),
        Err(fault) => return load_fault_response(fault),
    };
    let acc = match beef_limits::parse_beef(&subject, &beef_limits::APP_BEEF_LIMITS) {
        Ok(b) => b,
        Err(e) => {
            console_warn!("[credit-beef] stored subject BEEF unparseable for {txid}: {e}");
            return json_error("stored BEEF unparseable", 500);
        }
    };

    // Drive the SAME state machine the unit tests drive (`credit_beef::Walk`):
    // the decisions — what to fetch, when to stop, what counts as complete —
    // live in one place, and only the I/O differs here.
    let mut walk = crate::credit_beef::Walk::new(acc);
    while let Some(wanted) = walk.next_wanted() {
        let mut progressed = false;
        for parent in wanted {
            match load_stored_beef(&ctx.env, &db, &parent).await {
                Ok(bytes) => {
                    // A parent we do not hold (a foreign tx never submitted
                    // through us) still counts as a fetch, so an absent row
                    // ends the walk instead of spinning it.
                    if walk.absorb(bytes.as_deref()) {
                        progressed = true;
                    }
                }
                // a refuted parent refuses the whole ancestry: the wallet
                // must never internalize over an orphan's bump
                Err(fault) => return load_fault_response(fault),
            }
        }
        if !progressed {
            break;
        }
    }

    let complete = walk.is_complete();
    let fetches = walk.fetches();
    let bytes = walk.into_beef().to_binary();
    json_response(
        serde_json::json!({ "txid": txid, "beef": bytes, "complete": complete, "fetches": fetches }).to_string(),
        200,
    )
}

pub async fn beef(_req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    let Some(txid) = ctx.param("txid").cloned() else {
        return json_error("missing txid", 400);
    };
    if !valid_txid(&txid) {
        return json_error("invalid txid (expect 64 hex chars)", 400);
    }

    let db = match ctx.env.d1("OVERLAY_DB") {
        Ok(db) => db,
        Err(e) => {
            console_warn!("[beef] OVERLAY_DB binding unavailable: {e}");
            return json_error("database unavailable", 503);
        }
    };

    // THE one loader (round-2 review M3): pot_beefs first (durable),
    // transactions second (lifecycle-managed); the read-side reorg guard and
    // the trust-licensed compaction live inside it, shared with the
    // `/credit-beef` walk. A miss after any fault is 503, never 404 (a fault
    // must not be shaped like a definitive not-found).
    match load_stored_beef(&ctx.env, &db, &txid).await {
        Ok(Some(bytes)) => json_response(beef_body(&txid, &bytes), 200),
        Ok(None) => json_error(&format!("BEEF not found for txid: {txid}"), 404),
        Err(fault) => load_fault_response(fault),
    }
}

/// The same verified header height `/tip` serves. Embedded view tips must
/// agree: `/pots-view` primes the client's tip cache. A fault is `tip: null`
/// on those views so their other facts still serve. Freshness is /tip-only.
async fn chaintracks_present_height(
    ctx: &RouteContext<AuthState>,
    tag: &str,
) -> std::result::Result<u64, (&'static str, u16)> {
    chaintracks_present_height_env(&ctx.env, tag).await
}

/// [`chaintracks_present_height`] over a bare `Env` (the `/beef` read-side
/// guard has no route context; bsv-low M19 R2).
pub(crate) async fn chaintracks_present_height_env(
    env: &worker::Env,
    _tag: &str,
) -> std::result::Result<u64, (&'static str, u16)> {
    chaintracks_present_height_with(|route| async move {
        crate::tracker_host::chaintracks_get(env, &route).await
    })
    .await
}

async fn chaintracks_present_height_with<G, F>(
    get: G,
) -> std::result::Result<u64, (&'static str, u16)>
where
    G: Fn(String) -> F,
    F: std::future::Future<
        Output = std::result::Result<(u16, serde_json::Value), bsv_tracker::HeaderError>,
    >,
{
    crate::tracker_host::verified_tip_with(get)
        .await
        .map(|tip| u64::from(tip.height))
        // Callers own the fault's context.
        .map_err(|_| ("verified header tip unavailable", 503))
}

/// `GET /tip`: the proof tracker's verified header tip and its pass age.
/// An unavailable or over-age snapshot is a named 503, with diagnostics only.
pub async fn tip(_req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    let reply = tip_reply_env(&ctx.env).await;
    let mut response = json_response(reply.body.to_string(), reply.status)?;
    if let Some(retry_after) = reply.retry_after {
        response.headers_mut().set("Retry-After", retry_after)?;
    }
    Ok(response)
}

async fn tip_reply_env(env: &worker::Env) -> TipReply {
    tip_reply_with(
        |route| async move { crate::tracker_host::chaintracks_get(env, &route).await },
        crate::tracker_host::last_verified_tip,
        || worker::Date::now().as_millis(),
    )
    .await
}

const TIP_MAX_AGE_MS: u64 = 30 * 60 * 1_000;
const TIP_REORG_MARGIN: u32 = 1;
const TIP_RETRY_AFTER_SECS: &str = "60";

struct TipReply {
    status: u16,
    body: serde_json::Value,
    retry_after: Option<&'static str>,
}

async fn tip_reply_with<G, F, L, N>(get: G, last_known: L, now_ms: N) -> TipReply
where
    G: Fn(String) -> F,
    F: std::future::Future<
        Output = std::result::Result<(u16, serde_json::Value), bsv_tracker::HeaderError>,
    >,
    L: FnOnce() -> Option<crate::tracker_host::VerifiedTip>,
    N: FnOnce() -> u64,
{
    let current = crate::tracker_host::tip_snapshot_with(get).await.ok();
    let last_known = last_known();
    let now = now_ms();
    let available = current.as_ref().is_some_and(|tip| {
        tip.last_pass_at
            .is_none_or(|at| now.saturating_sub(at) <= TIP_MAX_AGE_MS)
            && tip
                .last_seen_height
                .is_none_or(|height| height <= tip.height.saturating_add(TIP_REORG_MARGIN))
    });
    let mut body = tip_body(current.as_ref().or(last_known.as_ref()), now);
    body["freshness"] = serde_json::json!(if !available {
        "stale"
    } else if current.as_ref().and_then(|tip| tip.last_pass_at).is_some() {
        "fresh"
    } else {
        "unknown"
    });
    if !available {
        body["code"] = serde_json::json!("ERR_TIP_UNAVAILABLE");
        body["error"] = serde_json::json!("verified header tip unavailable");
        body["stale"] = serde_json::json!(true);
    }
    TipReply {
        status: if available { 200 } else { 503 },
        body,
        retry_after: (!available).then_some(TIP_RETRY_AFTER_SECS),
    }
}

/// `/pots-view` joined row as D1 returns it: the `PotRowD1` fields plus the
/// LEFT-JOINed `hex(pot_beefs.beef)` for the recorded spender (NULL when the
/// outpoint is unspent or the spender's BEEF was never stored).
#[derive(Deserialize)]
struct PotsViewRowD1 {
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: f64,
    spent: f64,
    #[serde(rename = "spendingTxid")]
    spending_txid: Option<String>,
    #[serde(rename = "spentConfirmed", default)]
    spent_confirmed: f64,
    #[serde(rename = "spenderBeef")]
    spender_beef: Option<String>,
}

impl PotsViewRowD1 {
    fn into_row(self) -> PotsViewRow {
        PotsViewRow {
            record: PotRecordRow {
                txid: self.txid,
                vout: self.output_index as u32,
                spent: self.spent != 0.0,
                spending_txid: self.spending_txid,
                spent_confirmed: self.spent_confirmed != 0.0,
                // /pots-view serves display hints, not counting inputs — the
                // #371 witness pair stays absent (strict bar) here.
                spender_final: None,
                spender_seen: None,
                funding_seen: None,
                funding_seen_by: None,
            },
            spender_beef_hex: self.spender_beef,
        }
    }
}

/// `GET /pots-view?outpoints=<txid>.<vout>,…` — the batched DERIVED view
/// (GH bsv-low#163): everything a home/History surface pass needs in ONE
/// request and ONE D1 query. Per outpoint: the `/utxo-status` facts plus
/// `spenderRawHex` (the recorded spender's raw tx, extracted from its stored
/// BEEF — a HINT the client hash-verifies against `spendingTxid`); plus the
/// chain `tip` in the same body (`null` on a chaintracks fault — the D1
/// facts still serve, and the client falls back to `/tip`).
pub async fn pots_view(req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    let url = req.url()?;
    let Some(param) = url
        .query_pairs()
        .find(|(k, _)| k == "outpoints")
        .map(|(_, v)| v.into_owned())
    else {
        return json_error("missing outpoints query parameter", 400);
    };
    let outpoints: Vec<Outpoint> = match parse_outpoints(&param) {
        Ok(ops) => ops,
        Err(msg) => return json_error(&msg, 400),
    };

    let db = match ctx.env.d1("OVERLAY_DB") {
        Ok(db) => db,
        Err(e) => {
            console_warn!("[pots-view] OVERLAY_DB binding unavailable: {e}");
            return json_error("database unavailable", 503);
        }
    };

    // One joined query PER CHUNK (records + spender BEEFs), merged into one
    // response — same D1 100-bound-param discipline as /utxo-status (the join
    // still binds 2 params per outpoint, so a >50-outpoint single query 503s).
    // FAIL-SAFE: any chunk's D1 error returns the SAME 503 and no body — a
    // failed chunk is unknown-for-those-rows, never a fabricated partial view.
    // #375: a written-off pot's row is excluded by the SQL, so the outpoint
    // assembles as the fail-safe `known:false` shape below — never an error.
    let era = written_off_before_ms(&ctx);
    let mut rows: Vec<PotsViewRow> = Vec::with_capacity(outpoints.len());
    for chunk in chunk_outpoints(&outpoints) {
        let mut binds: Vec<JsValue> = Vec::with_capacity(chunk.len() * 2 + 1);
        for op in chunk {
            binds.push(JsValue::from_str(&op.db_txid()));
            binds.push(JsValue::from_f64(f64::from(op.vout)));
        }
        if let Some(ms) = era {
            binds.push(era_bind(ms));
        }
        let stmt = db
            .prepare(pots_view_join_sql(chunk.len(), era))
            .bind(&binds)?;
        match stmt
            .counted_all()
            .await
            .and_then(|r| r.results::<PotsViewRowD1>())
        {
            Ok(chunk_rows) => rows.extend(chunk_rows.into_iter().map(PotsViewRowD1::into_row)),
            Err(e) => {
                console_warn!("[pots-view] pot_records join query failed: {e}");
                return json_error("database query failed", 503);
            }
        }
    }

    let entries = assemble_pots_view(&outpoints, &rows);
    let tip = chaintracks_present_height(&ctx, "pots-view").await.ok();
    json_response(pots_view_body(&entries, tip), 200)
}

/// `/recovery-view` joined row as D1 returns it: the caller's potparty facts
/// plus the LEFT-JOINed pot-spend status and the recorded spender's stored
/// BEEF. The pot-spend columns are `Option` because the join can MISS (a
/// party marker whose pot output isn't in `pot_records` yet — NULL columns).
#[derive(Deserialize)]
struct RecoveryRowD1 {
    #[serde(rename = "gameId")]
    game_id: String,
    #[serde(rename = "potTxid")]
    pot_txid: String,
    #[serde(rename = "potVout")]
    pot_vout: f64,
    #[serde(rename = "recoveryHeight")]
    recovery_height: f64,
    #[serde(rename = "opponentIdentity")]
    opponent_identity: String,
    /// NULL when the pot output has no `pot_records` row yet.
    spent: Option<f64>,
    #[serde(rename = "spendingTxid")]
    spending_txid: Option<String>,
    /// NULL when no row; `serde(default)` also tolerates a read that races
    /// the overlay's additive `spentConfirmed` migration.
    #[serde(rename = "spentConfirmed", default)]
    spent_confirmed: Option<f64>,
    #[serde(rename = "spenderBeef")]
    spender_beef: Option<String>,
    /// #323 MEDIUM-3 — the covenant-committed height from `pot_records`;
    /// chain truth, preferred over the marker's unverified value.
    #[serde(rename = "covRecoveryHeight")]
    cov_recovery_height: Option<f64>,
    /// #343 — the pot's COMMITTED covenant keys (`pot_records`' #284 decoded
    /// columns). `default` tolerates a read racing the additive migration;
    /// an incomplete or malformed set collapses to `None` in `into_row`.
    #[serde(rename = "covPubA", default)]
    cov_pub_a: Option<String>,
    #[serde(rename = "covPubB", default)]
    cov_pub_b: Option<String>,
    #[serde(rename = "covPayPkhA", default)]
    cov_pay_pkh_a: Option<String>,
    #[serde(rename = "covPayPkhB", default)]
    cov_pay_pkh_b: Option<String>,
}

impl RecoveryRowD1 {
    fn into_row(self) -> RecoveryRow {
        RecoveryRow {
            game_id: self.game_id,
            pot_txid: self.pot_txid,
            pot_vout: self.pot_vout as u32,
            recovery_height: self.recovery_height as u32,
            cov_recovery_height: self.cov_recovery_height.map(|v| v as u64),
            opponent_identity: self.opponent_identity,
            spent: self.spent.map(|v| v != 0.0),
            spending_txid: self.spending_txid,
            spent_confirmed: self.spent_confirmed.map(|v| v != 0.0),
            spender_beef_hex: self.spender_beef,
            // #343 — THE shared predicate, so `/recovery-view` and `/results`
            // cannot disagree about what a servable key set is: all four
            // present and structurally right, or None.
            committed_keys: crate::results::CommittedKeys::from_columns(
                self.cov_pub_a.as_deref(),
                self.cov_pub_b.as_deref(),
                self.cov_pay_pkh_a.as_deref(),
                self.cov_pay_pkh_b.as_deref(),
            ),
        }
    }
}

/// `GET /recovery-view?identity=<66-hex>` — the seed-only BY-IDENTITY
/// recovery view (bsv-low#189). A recovering client that holds only its
/// identity key gets, in ONE request / ONE D1 query, every pot it is a party
/// to (`potparty_records`, bsv-low#188) JOINed to that pot's on-chain spend
/// status (`pot_records`) and the spender's raw tx (extracted from its stored
/// BEEF — a HINT the client hash-verifies against `spendingTxid`); plus the
/// chain `tip` in the same body (the recovery-height gate needs it; `null` on
/// a chaintracks fault). This replaces a lookup-then-per-outpoint `/pots-view`
/// fan-out.
///
/// Fail-safe shape: a missing/invalid/empty `identity` is an EMPTY result
/// (`{"tip":null,"entries":[]}`, HTTP 200), never a 4xx — a seed-only client
/// with nothing indexed sees the same well-formed empty answer. A pot with a
/// party marker but no `pot_records` row yet is `spent:null` (never asserted
/// unspent). Public data only, read-only, no secrets.
///
/// #252 stage A (read-behind, additive): each entry also carries `collected`
/// (a `collected_markers_v2` row exists for this identity+game — presence
/// hint only, tri-state) and the `/results`-derived `outcome`/`outcomeSource`
/// honesty pair (Rule 15: the one derivation, reused). Both best-effort:
/// `null` on any fault, and the pre-existing fields never move.
pub async fn recovery_view(req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    // #318: identity comes from the ONE auth seam (session identity wins;
    // mismatch refuses; anonymous lenient = the legacy query-param claim).
    let identity = match view_identity(&req, &ctx) {
        ViewIdentity::Identity(id) => id,
        ViewIdentity::Refuse(resp) => return resp,
    };

    // Missing / empty / malformed identity → empty result, not an error.
    if !valid_identity(&identity) {
        return json_response(
            recovery_view_body(
                &crate::logic::apply_recovery_extras(Vec::new(), None, None),
                None,
                false,
                0,
            ),
            200,
        );
    }

    let db = match ctx.env.d1("OVERLAY_DB") {
        Ok(db) => db,
        Err(e) => {
            console_warn!("[recovery-view] OVERLAY_DB binding unavailable: {e}");
            return json_error("database unavailable", 503);
        }
    };

    // ONE query: the caller's potparty rows JOINed to pot spend-status +
    // spender BEEFs. `potparty_records.identity` is lowercase hex.
    // #375: the era cutoff rides as the LAST bind iff configured.
    let era = written_off_before_ms(&ctx);
    let mut binds: Vec<JsValue> = vec![JsValue::from_str(&identity.to_ascii_lowercase())];
    if let Some(ms) = era {
        binds.push(era_bind(ms));
    }
    // #398's cursor on the recovery surface (brain-cutover M2c): `after`
    // slides the rank window so a caller can WALK a truncated set to
    // completion. Clamped to the ceiling; a malformed value is 0 (the first
    // page), never an error — this is a recovery surface.
    let after: usize = req
        .url()
        .ok()
        .and_then(|u| {
            u.query_pairs()
                .find(|(k, _)| k == "after")
                .and_then(|(_, v)| v.parse::<usize>().ok())
        })
        .map(|a| a.min(crate::logic::RECOVERY_VIEW_AFTER_MAX))
        .unwrap_or(0);
    let stmt = db.prepare(recovery_view_sql(era, after)).bind(&binds)?;
    let rows: Vec<RecoveryRow> = match stmt
        .counted_all()
        .await
        .and_then(|r| r.results::<RecoveryRowD1>())
    {
        Ok(rows) => rows.into_iter().map(RecoveryRowD1::into_row).collect(),
        Err(e) => {
            console_warn!("[recovery-view] potparty join query failed: {e}");
            return json_error("database query failed", 503);
        }
    };

    let (entries, truncated) = assemble_recovery_view(rows);

    // #252 stage A — the two READ-BEHIND extras, both BEST-EFFORT (any fault
    // serves `null` for its field and never disturbs the view's original
    // fields or its 200): `outcome` REUSES the whole `/results` derivation
    // for the same identity (Rule 15 — one derivation, never a re-spelled
    // one), and `collected` is `collected_markers_v2` PRESENCE (the client
    // still verifies marker sigs itself). The fold is the pure
    // `apply_recovery_extras` (natively pinned); these two fetches are the
    // route's, like every other view's D1 legs. An EMPTY view (identity with
    // no pots — every fresh visitor) skips both side reads outright: there is
    // nothing to annotate, and the /results gather is the expensive leg.
    let outcome_entries = if entries.is_empty() {
        None
    } else {
        // #375: the same cutoff — the reused /results derivation must not
        // resurrect a written-off pot through the outcome fold.
        // The paging round: the outcome fold follows THIS page's cursor —
        // /results and /recovery-view share the window ordering and the
        // 100-pot cap by design (see RECOVERY_VIEW_MAX_ROWS' doc), so page N
        // of one is page N of the other.
        match gather_result_entries(&db, &identity.to_ascii_lowercase(), era, after).await {
            Ok((v, _)) => Some(v),
            Err(e) => {
                console_warn!(
                    "[recovery-view] results derivation unavailable (outcome served null): {e}"
                );
                None
            }
        }
    };
    let collected = collected_games_for(
        &db,
        &identity.to_ascii_lowercase(),
        entries.iter().map(|e| e.game_id.to_ascii_lowercase()),
    )
    .await;
    let entries = crate::logic::apply_recovery_extras(
        entries,
        outcome_entries.as_deref(),
        collected.as_ref(),
    );

    let tip = chaintracks_present_height(&ctx, "recovery-view").await.ok();
    json_response(recovery_view_body(&entries, tip, truncated, after), 200)
}

/// #252 stage A — which of these games carry a `collected_markers_v2` row for
/// this identity? `Some(set)` = asked successfully (a game absent from the
/// set was ASKED about and has no row); `None` = could not ask (bind/query
/// fault, racing pre-migration schema) — the caller serves `null`, never a
/// false "not collected". Chunked like every IN(...) read here.
async fn collected_games_for(
    db: &worker::D1Database,
    identity_lc: &str,
    game_ids: impl Iterator<Item = String>,
) -> Option<std::collections::HashSet<String>> {
    #[derive(Deserialize)]
    struct GameIdRowD1 {
        #[serde(rename = "gameId")]
        game_id: String,
    }
    let mut ids: Vec<String> = game_ids.collect();
    ids.sort_unstable();
    ids.dedup();
    let mut out = std::collections::HashSet::new();
    if ids.is_empty() {
        return Some(out);
    }
    for chunk in ids.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
        let mut binds: Vec<JsValue> = Vec::with_capacity(chunk.len() + 1);
        binds.push(JsValue::from_str(identity_lc));
        for g in chunk {
            binds.push(JsValue::from_str(g));
        }
        let stmt = match db
            .prepare(crate::logic::collected_presence_sql(chunk.len()))
            .bind(&binds)
        {
            Ok(s) => s,
            Err(e) => {
                console_warn!("[recovery-view] collected bind failed (collected served null): {e}");
                return None;
            }
        };
        match stmt
            .counted_all()
            .await
            .and_then(|r| r.results::<GameIdRowD1>())
        {
            Ok(rows) => out.extend(rows.into_iter().map(|r| r.game_id.to_ascii_lowercase())),
            Err(e) => {
                // A whole-set None rather than a partial set: a partial answer
                // would read `Some(false)` ("asked, no row") for games in the
                // failed chunk — the one conflation the tri-state forbids.
                console_warn!(
                    "[recovery-view] collected query failed (collected served null): {e}"
                );
                return None;
            }
        }
    }
    Some(out)
}

/// `result_markers_v2` row as D1 returns it. `potTxid`/`settleTxid`/
/// `winnerSigHex` are nullable in the (superseded) original schema, so they
/// arrive `Option` — a row missing any of them is a malformed marker that
/// cannot be anchored or counted, dropped in [`ResultRowD1::into_marker`].
/// `createdAt` is nullable (mirrors the client's `createdAt: number | null`).
#[derive(Deserialize)]
pub(crate) struct ResultRowD1 {
    #[serde(rename = "gameId")]
    game_id: String,
    winner: String,
    loser: String,
    #[serde(rename = "potTxid")]
    pot_txid: Option<String>,
    #[serde(rename = "settleTxid")]
    settle_txid: Option<String>,
    #[serde(rename = "winnerSigHex")]
    winner_sig_hex: Option<String>,
    #[serde(rename = "loserSigHex")]
    loser_sig_hex: Option<String>,
    #[serde(rename = "cardsHex")]
    cards_hex: Option<String>,
    txid: String,
    #[serde(rename = "createdAt")]
    created_at: Option<f64>,
    #[serde(rename = "claimValid")]
    claim_valid: Option<f64>,
}

impl ResultRowD1 {
    /// Host row, or `None` when a required byte field is NULL (a malformed
    /// marker that could never anchor or count — never fabricated).
    pub(crate) fn into_marker(self) -> Option<ResultMarkerRow> {
        Some(ResultMarkerRow {
            game_id: self.game_id,
            winner: self.winner,
            loser: self.loser,
            pot_txid: self.pot_txid?,
            settle_txid: self.settle_txid?,
            winner_sig_hex: self.winner_sig_hex?,
            loser_sig_hex: self.loser_sig_hex,
            cards_hex: self.cards_hex,
            txid: self.txid,
            created_at: self.created_at.map(|v| v as i64),
            claim_valid: self.claim_valid.map(|v| v as i64),
        })
    }
}

/// `proof_markers` pointer row — only the (gameId, winner) key and the marker
/// txid; the ~10-15 KB transcript `bundle` is never read here (the CLIENT
/// fetches + verifies it — this surface only points at where it lives).
#[derive(Deserialize)]
struct ProofPostedRowD1 {
    #[serde(rename = "gameId")]
    game_id: String,
    winner: String,
}

#[derive(Deserialize)]
struct ProofPointerRowD1 {
    #[serde(rename = "gameId")]
    game_id: String,
    winner: String,
    txid: String,
}

/// `GET /leaderboard?limit=200` — the server-side leaderboard join + rank
/// (bsv-low #38), collapsing the client's ~110-round-trip N+1 (`result.ts
/// gatherBoard`: 1 `ls_result` + up to 50 `ls_proof` + ~57 `/beef` + a
/// `/utxo-status` batch, ranked client-side) into ONE call.
///
/// Reads the recent `result_markers_v2` markers through the anti-flood
/// WINDOW ([`crate::logic::leaderboard_markers_sql`] — `limit` counts
/// DISTINCT POTS since #332, per-pot superset, unknown-pot quota,
/// admission-stamp rank, `limit + 1` truncation probe), JOINs each against
/// the `pot_records` spend-status (the SAME table `/utxo-status` reads —
/// CHUNKED at [`crate::logic::D1_CHUNK_OUTPOINTS`] so a large result set
/// never trips D1's 100-bound-param cap), fetches `proof_markers` pointers
/// KEYED to the window's own `(gameId, winner)` pairs, and aggregates +
/// ranks. See the `logic` module note for the #332 trust model: counting
/// inputs are the chain anchor + VERIFIED signatures + the #230 chain
/// attribution; the markers themselves are display hints, returned verbatim
/// in `evidence` so the client re-verifies and can falsify.
///
/// FAIL-SAFE: a `pot_records` (or marker) D1 fault is the SAME 5xx the client
/// already handles — NEVER a fabricated empty/all-zero board. The `proof_markers`
/// join is best-effort: a fault there only drops the `proofTxid` hint (null),
/// never a count and never a 5xx. An over-full window is reported via the
/// body's `truncated` bit — never a complete-looking partial answer.
pub async fn leaderboard(req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    // S3a (ARCHITECTURE v2): forward to the BoardView ACTOR — one warm copy
    // serves every poller from memory (single-flight by construction), D1
    // stays the durable truth underneath. The worker-Cache layer this
    // replaces is DELETED (a superseded mechanism is a re-derivation). On
    // ANY actor fault the route falls back to direct compute — never a new
    // failure mode.
    let url = req.url()?;
    let limit_raw = url
        .query_pairs()
        .find(|(k, _)| k == "limit")
        .and_then(|(_, v)| v.parse::<u32>().ok());
    // #403: the rank cursor (owners page). Absent ⇒ the first page.
    let after_raw = url
        .query_pairs()
        .find(|(k, _)| k == "after")
        .and_then(|(_, v)| v.parse::<u32>().ok());
    // #642: the keyset cursor (the body's `nextCursor`). One this route did not
    // serve is refused here, before the actor is asked.
    let cursor_raw = url
        .query_pairs()
        .find(|(k, _)| k == "cursor")
        .map(|(_, v)| v.into_owned());
    if let Some(c) = &cursor_raw {
        if crate::logic::parse_leaderboard_cursor(c).is_none() {
            return json_error("invalid cursor", 400);
        }
    }
    if let Ok(ns) = ctx.env.durable_object("BOARD_VIEW") {
        if let Ok(stub) = ns.id_from_name("board:v1").and_then(|id| id.get_stub()) {
            let mut q: Vec<String> = Vec::new();
            if let Some(l) = limit_raw {
                q.push(format!("limit={l}"));
            }
            if let Some(a) = after_raw {
                q.push(format!("after={a}"));
            }
            if let Some(c) = &cursor_raw {
                q.push(format!("cursor={c}"));
            }
            let q = if q.is_empty() {
                String::new()
            } else {
                format!("?{}", q.join("&"))
            };
            match stub
                .fetch_with_str(&format!("https://board-view/leaderboard{q}"))
                .await
            {
                // NEVER return the DO response verbatim — its headers are
                // immutable and CORS would silently fail to attach (see
                // `rebuild_do_response`).
                Ok(mut resp) => return rebuild_do_response(&mut resp, "/leaderboard:view").await,
                Err(e) => {
                    console_warn!("[leaderboard] BoardView unreachable ({e}) — direct compute")
                }
            }
        }
    }
    let (status, body) =
        compute_leaderboard_body_string(&ctx.env, limit_raw, after_raw, cursor_raw.as_deref())
            .await?;
    json_response_cached(body, status, 15)
}

/// S3a — the WHOLE leaderboard pipeline as a callable: the BoardView actor
/// and the route's direct fallback both serve EXACTLY this. Returns
/// (status, body-json).
///
/// #403 BOARD PAGING (2026-08-29) — see the `logic` module note "the
/// whole-era CHAIN-WINS SPINE". The pipeline, in order:
///
/// 1. **The owner page** ([`crate::logic::chain_wins_spine_sql`]): every
///    era pot with a fresh winner verdict, decoded committed keys and a
///    confirmed landing, grouped by the latched owner, ranked `wins DESC,
///    owner ASC`, `?limit` owners from rank `?after`, `+1` probe ⇒ the
///    honest `truncated` bit + `nextAfter`. ONE aggregate over indexed
///    columns, whatever the era holds — the 500-pot window is gone.
/// 2. **Their pots** ([`crate::logic::chain_wins_owners_sql`], chunked):
///    the counted wins of exactly those owners — the fold's candidate set.
/// 3. **Statuses** (`batch_where_sql`, chunked — the SAME table
///    `/utxo-status` reads) and **classification** (`classify_spent_pots`,
///    column tier: verdict + committed params + settleSigners) for those
///    pots; **attribution** built from the spine's own latched resolution
///    ([`crate::logic::attributions_from_pot_rows`]) so the fold and the
///    page can never disagree.
/// 4. **Markers**: each pot's oldest `RESULT_ROWS_PER_POT` markers
///    ([`crate::logic::pot_markers_sql`], chunked) plus the ERA-WIDE lowest
///    verified winner hands ([`crate::logic::era_hands_sql`]) — both bounded
///    by the page, never by the history.
/// 5. **The fold** (`aggregate_leaderboard_attributed`, UNCHANGED): wins
///    minted from chain facts only, the winner's own verified marker
///    decorating; then the board is cut to the page's owners in rank order
///    (the hands pots' owners may lie beyond the page).
/// 6. **Proof decoration** (bsv-low #685, RC-L32): over the PAGE-LOCAL
///    evidence set, in DISPLAY priority (`leaderboard_decoration_pairs`: the
///    lowest-hand board, each identity's pinned best, then the top rows), the
///    `proof_markers` pointer superset and the `proof_posts` posted-set are
///    fetched for the first [`crate::logic::LEADERBOARD_PROOF_PAIRS_CAP`]
///    pairs and stamped on. A pair past that budget is served `proofPending`
///    (`stamp_leaderboard_decoration`), NEVER a false `proofPosted: false`.
///
/// bsv-low #674: steps 1 and 2 read the tables the overlay derives at the
/// write (`board_owners` by its rank index, `board_wins` by owner, `board_hands`
/// by its rank index) once its backfill stamped `board-v1` and its `board_era`
/// stamp is this era cutoff; otherwise #642's bounded candidate read
/// ([`leaderboard_page_body`]).
///
/// FAIL-SAFE, as before: a spine / pots / statuses D1 fault is the SAME 503
/// the client handles — NEVER a fabricated empty board; the marker, hands
/// and proof-pointer joins are best-effort (a fault there only omits
/// decoration, never a count).
pub async fn compute_leaderboard_body_string(
    env: &worker::Env,
    limit_raw: Option<u32>,
    after_raw: Option<u32>,
    cursor_raw: Option<&str>,
) -> Result<(u16, String)> {
    let page = crate::logic::clamp_leaderboard_page(limit_raw);
    let start = match cursor_raw {
        Some(raw) => match crate::logic::parse_leaderboard_cursor(raw) {
            Some(start) => start,
            None => return Ok((400, "{\"error\":\"invalid cursor\"}".to_string())),
        },
        None => crate::logic::PageStart::Offset(crate::logic::clamp_leaderboard_after(after_raw)),
    };

    let db = match env.d1("OVERLAY_DB") {
        Ok(db) => db,
        Err(e) => {
            console_warn!("[leaderboard] OVERLAY_DB binding unavailable: {e}");
            return Ok((503, "{\"error\":\"database unavailable\"}".to_string()));
        }
    };
    let era = written_off_before_ms_env(env);
    let computed_at = (worker::Date::now().as_millis() / 1000) as i64;
    Ok(leaderboard_page_body(
        &crate::board_db::D1Board(&db),
        page,
        &start,
        era,
        crate::logic::LEADERBOARD_CANDIDATE_WINS,
        computed_at,
    )
    .await)
}

/// The `/leaderboard:view` pipeline over the [`crate::board_db::BoardDb`] port
/// (the Worker's D1, or rusqlite in the native pins). bsv-low #674: with the
/// board tables stamped (`board-v1`) and derived under the route's own era
/// cutoff (the overlay's `board_era`, which the scheduled pass stamps:
/// [`sync_board_era`]), the page is the indexed read of the tables the overlay
/// derives at the write ([`leaderboard_table_body`]); otherwise (an unmigrated
/// database, a stamp that is not this cutoff yet) the #642 candidate read
/// ([`leaderboard_candidate_body`]).
pub(crate) async fn leaderboard_page_body<B: crate::board_db::BoardDb>(
    db: &B,
    page: usize,
    start: &crate::logic::PageStart,
    era: Option<i64>,
    candidate_cap: usize,
    computed_at: i64,
) -> (u16, String) {
    if let Some(total) = board_tables_ready(db, era).await {
        return leaderboard_table_body(db, page, start, total, computed_at).await;
    }
    leaderboard_candidate_body(db, page, start, era, candidate_cap, computed_at).await
}

/// The count of every counted win when the board tables are derived (the
/// overlay's `board-v1` stamp) under the era cutoff `era` (the `board_era`
/// stamp equal to it, `None` to none), else `None`: a database the overlay has
/// not migrated fails the statement, and the route reads the candidates.
pub(crate) async fn board_tables_ready<B: crate::board_db::BoardDb>(
    db: &B,
    era: Option<i64>,
) -> Option<usize> {
    #[derive(Deserialize)]
    struct ReadyRowD1 {
        #[serde(default)]
        ready: Option<f64>,
        #[serde(default)]
        total: Option<f64>,
        #[serde(default)]
        era: Option<f64>,
        #[serde(rename = "cutoffMs", default)]
        cutoff_ms: Option<f64>,
    }
    match db
        .all::<ReadyRowD1>(crate::logic::BOARD_READY_SQL.to_string(), Vec::new())
        .await
    {
        Ok(rows) => rows
            .first()
            .filter(|r| {
                r.ready == Some(1.0) && r.era == Some(1.0) && r.cutoff_ms == era.map(|ms| ms as f64)
            })
            .map(|r| r.total.unwrap_or(0.0).max(0.0) as usize),
        Err(e) => {
            crate::board_db::warn(&format!(
                "[leaderboard] board tables unread ({e}): the candidate read"
            ));
            None
        }
    }
}

/// bsv-low #674, the cutoff: stamp the era cutoff `era` (`None`: none) into the
/// overlay's `board_era`, the cutoff the board tables are derived under. A
/// no-op when it already is; otherwise the overlay's `board_era_update` trigger
/// re-derives the whole board under it inside this one statement, so the stamp
/// never names a cutoff the tables were not derived under. Until it lands
/// [`board_tables_ready`] finds another stamp and the route reads #642's
/// candidates. A database the overlay has not migrated fails the statement.
pub(crate) async fn sync_board_era<B: crate::board_db::BoardDb>(
    db: &B,
    era: Option<i64>,
) -> std::result::Result<(), crate::board_db::BoardDbError> {
    use crate::board_db::Bind;
    let (sql, binds) = match era {
        Some(ms) => (crate::logic::BOARD_ERA_SET_SQL, vec![Bind::Num(ms as f64)]),
        None => (crate::logic::BOARD_ERA_CLEAR_SQL, Vec::new()),
    };
    db.all::<serde_json::Value>(sql.to_string(), binds)
        .await
        .map(|_| ())
}

/// [`sync_board_era`] on the Worker's D1 with the route's own cutoff
/// ([`written_off_before_ms_env`]), on the scheduled pass only (never a read).
pub async fn sync_board_era_env(env: &worker::Env) {
    let db = match env.d1("OVERLAY_DB") {
        Ok(db) => db,
        Err(e) => {
            console_warn!("[leaderboard] board era: OVERLAY_DB unavailable ({e})");
            return;
        }
    };
    if let Err(e) = sync_board_era(
        &crate::board_db::D1Board(&db),
        written_off_before_ms_env(env),
    )
    .await
    {
        console_warn!("[leaderboard] board era not stamped ({e}): the candidate read stands");
    }
}

/// The #642 pipeline: ONE candidate read
/// ([`crate::logic::chain_wins_candidates_sql`], at most `candidate_cap`
/// counted wins), the page, the hands board and the hands pots' attribution
/// cut from it in Rust ([`crate::logic::leaderboard_candidate_page`]), then
/// steps 3 to 6 of [`compute_leaderboard_body_string`] unchanged.
pub(crate) async fn leaderboard_candidate_body<B: crate::board_db::BoardDb>(
    db: &B,
    page: usize,
    start: &crate::logic::PageStart,
    era: Option<i64>,
    candidate_cap: usize,
    computed_at: i64,
) -> (u16, String) {
    use crate::board_db::{warn, Bind};

    // 1) + 2) The candidates: the page, its owners' pots and the hands board.
    let mut binds = vec![Bind::Num((candidate_cap + 1) as f64)];
    if let Some(ms) = era {
        binds.push(Bind::Num(ms as f64));
    }
    let rows: Vec<ChainWinCandidateRowD1> = match db
        .all(crate::logic::chain_wins_candidates_sql(era), binds)
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            warn(&format!(
                "[leaderboard] chain-wins candidates query failed: {e}"
            ));
            return (503, "{\"error\":\"database query failed\"}".to_string());
        }
    };
    let bounded = rows.len() > candidate_cap;
    let mut cands: Vec<crate::logic::ChainWinCandidate> = Vec::with_capacity(rows.len());
    let mut hand_markers: Vec<Option<ResultMarkerRow>> = Vec::with_capacity(rows.len());
    for r in rows.into_iter().take(candidate_cap) {
        let (c, m) = r.into_candidate();
        cands.push(c);
        hand_markers.push(m);
    }
    let sel = crate::logic::leaderboard_candidate_page(&cands, start, page, page);
    let hands: Vec<ResultMarkerRow> = sel
        .hands
        .iter()
        .filter_map(|&i| hand_markers[i].clone())
        .collect();
    let head = BoardPageHead {
        owners: sel.owners,
        truncated: sel.truncated,
        next_after: sel.next_after,
        extras: crate::logic::LeaderboardPageExtras {
            next_cursor: sel.next_cursor,
            candidates_bounded: bounded.then_some(candidate_cap),
            candidates_whole: None,
        },
        pot_rows: sel.pot_rows,
        hands,
    };
    leaderboard_page_tail(
        db,
        page,
        head,
        HandsPotRows::Candidates(&cands),
        computed_at,
    )
    .await
}

/// The #674 table read: the owner page by the rank index (by offset, or by the
/// keyset cursor and its rank), the page owners' counted wins and the era hands
/// by their indexes, then steps 3 to 6 of [`compute_leaderboard_body_string`]
/// unchanged. A fault before the decoration is the same 503.
pub(crate) async fn leaderboard_table_body<B: crate::board_db::BoardDb>(
    db: &B,
    page: usize,
    start: &crate::logic::PageStart,
    total: usize,
    computed_at: i64,
) -> (u16, String) {
    use crate::board_db::{warn, Bind};
    #[derive(Deserialize)]
    struct OwnerRowD1 {
        owner: String,
        wins: f64,
    }
    #[derive(Deserialize)]
    struct RankRowD1 {
        rank: f64,
    }
    let failed = |what: &str, e: crate::board_db::BoardDbError| {
        warn(&format!("[leaderboard] board {what} query failed: {e}"));
        (503, "{\"error\":\"database query failed\"}".to_string())
    };

    // 1) The owner page and its rank offset.
    let probe = Bind::Num((page + 1) as f64);
    let (first, mut ranked): (usize, Vec<(String, usize)>) = match start {
        crate::logic::PageStart::Offset(after) => {
            match db
                .all::<OwnerRowD1>(
                    crate::logic::BOARD_OWNERS_PAGE_SQL.to_string(),
                    vec![probe, Bind::Num(*after as f64)],
                )
                .await
            {
                Ok(rows) => (
                    *after,
                    rows.into_iter()
                        .map(|r| (r.owner, r.wins as usize))
                        .collect(),
                ),
                Err(e) => return failed("owners", e),
            }
        }
        crate::logic::PageStart::After { wins, owner } => {
            let key = || vec![Bind::Num(*wins as f64), Bind::Text(owner.clone())];
            let rank = match db
                .all::<RankRowD1>(crate::logic::BOARD_CURSOR_RANK_SQL.to_string(), key())
                .await
            {
                Ok(rows) => rows.first().map_or(0, |r| r.rank.max(0.0) as usize),
                Err(e) => return failed("cursor rank", e),
            };
            let mut b = key();
            b.push(probe);
            match db
                .all::<OwnerRowD1>(crate::logic::BOARD_OWNERS_AFTER_SQL.to_string(), b)
                .await
            {
                Ok(rows) => (
                    rank,
                    rows.into_iter()
                        .map(|r| (r.owner, r.wins as usize))
                        .collect(),
                ),
                Err(e) => return failed("owners", e),
            }
        }
    };
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let truncated = ranked.len() > page;
    ranked.truncate(page);
    let next_cursor = truncated
        .then(|| ranked.last().map(|(o, w)| format!("{w}.{o}")))
        .flatten();
    let owners: Vec<String> = ranked.into_iter().map(|(o, _)| o).collect();

    // 2) The page owners' counted wins, from their cards (bsv-low #674 fold,
    // MEDIUM-1: one `board_owners` row per owner holds every win and every row
    // steps 3 to 6 read for it, so the page never reads its owners' per-win
    // rows). An owner whose card does not hold all its wins (past the card's
    // bound, or not readable) is read by its wins, as before. Chunked at the
    // D1 bind cap; each chunk's wins in `board_wins_owners_sql`'s order.
    let mut pot_rows: Vec<crate::logic::ChainWinPotRow> = Vec::new();
    let mut pre = PagePrefetch::default();
    for chunk in owners.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
        let b: Vec<Bind> = chunk.iter().map(|o| Bind::Text(o.clone())).collect();
        let cards = match db
            .all::<BoardCardRowD1>(crate::logic::board_cards_sql(chunk.len()), b)
            .await
        {
            Ok(rows) => rows,
            Err(e) => return failed("cards", e),
        };
        let mut wins: Vec<(Option<f64>, crate::logic::ChainWinPotRow)> = Vec::new();
        let mut unpacked: Vec<String> = Vec::new();
        for owner in chunk {
            match cards
                .iter()
                .find(|c| c.owner == *owner)
                .and_then(BoardCardRowD1::entries)
            {
                Some(entries) => {
                    for e in entries {
                        pre.take(&e);
                        wins.push((e.pot_created_at, e.row));
                    }
                }
                None => unpacked.push(owner.clone()),
            }
        }
        if !unpacked.is_empty() {
            let b: Vec<Bind> = unpacked.iter().map(|o| Bind::Text(o.clone())).collect();
            match db
                .all::<ChainWinCandidateRowD1>(
                    crate::logic::board_wins_owners_sql(unpacked.len()),
                    b,
                )
                .await
            {
                Ok(rows) => wins.extend(rows.into_iter().map(|r| {
                    let c = r.into_candidate().0;
                    (c.pot_created_at.map(|v| v as f64), c.row)
                })),
                Err(e) => return failed("wins", e),
            }
        }
        // `ORDER BY owner ASC, potCreatedAt DESC, potTxid ASC` (a NULL stamp last).
        wins.sort_by(|(at_a, a), (at_b, b)| {
            a.owner
                .cmp(&b.owner)
                .then_with(|| {
                    at_b.unwrap_or(f64::NEG_INFINITY)
                        .total_cmp(&at_a.unwrap_or(f64::NEG_INFINITY))
                })
                .then_with(|| a.pot_txid.cmp(&b.pot_txid))
        });
        pot_rows.extend(wins.into_iter().map(|(_, r)| r));
    }

    // The era hands, BEST-EFFORT (a fault omits the hands board, never a count).
    let hands: Vec<ResultMarkerRow> = match db
        .all::<ResultRowD1>(
            crate::logic::BOARD_HANDS_SQL.to_string(),
            vec![Bind::Num(page as f64)],
        )
        .await
    {
        Ok(rows) => rows
            .into_iter()
            .filter_map(ResultRowD1::into_marker)
            .collect(),
        Err(e) => {
            warn(&format!(
                "[leaderboard] board hands query failed (hands omitted): {e}"
            ));
            Vec::new()
        }
    };
    let head = BoardPageHead {
        next_after: crate::logic::leaderboard_next_after(first, page, truncated),
        owners,
        truncated,
        extras: crate::logic::LeaderboardPageExtras {
            next_cursor,
            candidates_bounded: None,
            candidates_whole: Some(total),
        },
        pot_rows,
        hands,
    };
    leaderboard_page_tail_with(db, page, head, HandsPotRows::Table, &pre, computed_at).await
}

/// One `board_owners` card row (bsv-low #674 fold): the owner, its counted
/// wins and the card holding `packed` of them.
#[derive(Deserialize)]
struct BoardCardRowD1 {
    owner: String,
    wins: f64,
    packed: f64,
    card: Option<String>,
}

/// One win of a card: the owners query's row and the rows the page tail reads
/// for its pot.
struct CardWin {
    row: crate::logic::ChainWinPotRow,
    pot_created_at: Option<f64>,
    status: PotRecordRow,
    decoded: DecodedPotRowD1,
    markers: Vec<ResultMarkerRow>,
    /// Each marker's proof key (`lower(gameId)`, `lower(winner)`), its proof
    /// pointers in `proof_pointers_sql`'s order and its posted bundle.
    proofs: Vec<((String, String), Vec<String>, bool)>,
}

impl BoardCardRowD1 {
    /// The card's wins, or `None` when it does not hold every win of the owner
    /// (or a byte of it does not read): the owner is then read by its wins.
    fn entries(&self) -> Option<Vec<CardWin>> {
        if self.packed != self.wins {
            return None;
        }
        let card: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(self.card.as_deref()?).ok()?;
        if card.len() as f64 != self.wins {
            return None;
        }
        card.into_values().map(CardWin::from_entry).collect()
    }
}

impl CardWin {
    fn from_entry(mut entry: serde_json::Value) -> Option<CardWin> {
        let mut frag = entry.as_object_mut()?.remove("f")?;
        let c = serde_json::from_value::<ChainWinCandidateRowD1>(entry)
            .ok()?
            .into_candidate()
            .0;
        let f = frag.as_object_mut()?;
        let status = serde_json::from_value::<PotRowD1>(f.remove("s")?)
            .ok()?
            .into_row();
        let decoded = serde_json::from_value::<DecodedPotRowD1>(f.remove("d")?).ok()?;
        let mut rows: Vec<(f64, serde_json::Value)> = Vec::new();
        for mut m in f.remove("m")?.as_array()?.clone() {
            let rn = m.as_object_mut()?.remove("rn")?.as_f64()?;
            rows.push((rn, m));
        }
        rows.sort_by(|a, b| a.0.total_cmp(&b.0));
        let mut markers = Vec::new();
        let mut proofs = Vec::new();
        for (_, mut m) in rows {
            let o = m.as_object_mut()?;
            let pointers = o.remove("pp")?;
            let posted = o.remove("po")?.as_f64()? != 0.0;
            let key = (
                o.get("gameId")?.as_str()?.to_ascii_lowercase(),
                o.get("winner")?.as_str()?.to_ascii_lowercase(),
            );
            // `[createdAt, rowid, txid]`, ordered `createdAt DESC, rowid DESC`
            // (a NULL stamp last), as `proof_pointers_sql`'s window numbers them.
            let mut pp: Vec<(Option<f64>, f64, String)> = Vec::new();
            for p in pointers.as_array()? {
                let p = p.as_array()?;
                pp.push((
                    p.first()?.as_f64(),
                    p.get(1)?.as_f64()?,
                    p.get(2)?.as_str()?.to_string(),
                ));
            }
            pp.sort_by(|a, b| {
                b.0.unwrap_or(f64::NEG_INFINITY)
                    .total_cmp(&a.0.unwrap_or(f64::NEG_INFINITY))
                    .then_with(|| b.1.total_cmp(&a.1))
            });
            proofs.push((key, pp.into_iter().map(|p| p.2).collect(), posted));
            if let Some(marker) = serde_json::from_value::<ResultRowD1>(m).ok()?.into_marker() {
                markers.push(marker);
            }
        }
        Some(CardWin {
            pot_created_at: c.pot_created_at.map(|v| v as f64),
            row: c.row,
            status,
            decoded,
            markers,
            proofs,
        })
    }
}

/// The rows steps 3 to 6 would read for the page's own pots, taken from the
/// page owners' cards (bsv-low #674 fold, MEDIUM-1). A pot or a proof key not
/// held here is read from D1 as before.
#[derive(Default)]
pub(crate) struct PagePrefetch {
    /// The status row, by the pot's txid as `board_wins` holds it.
    status: std::collections::HashMap<String, PotRecordRow>,
    /// The decoded row, by the lowercase pot txid.
    decoded: std::collections::HashMap<String, DecodedPotRowD1>,
    /// The pot's markers in `pot_markers_sql`'s order, by the pot's txid.
    markers: std::collections::HashMap<String, Vec<ResultMarkerRow>>,
    /// The proof pointers and posted bundles of every key a held marker names.
    proofs: std::collections::HashMap<(String, String), (Vec<String>, bool)>,
}

impl PagePrefetch {
    fn take(&mut self, w: &CardWin) {
        let pot = w.row.pot_txid.clone();
        self.status.insert(pot.clone(), w.status.clone());
        self.decoded
            .insert(pot.to_ascii_lowercase(), w.decoded.clone());
        self.markers.insert(pot, w.markers.clone());
        for (key, pp, posted) in &w.proofs {
            self.proofs.insert(key.clone(), (pp.clone(), *posted));
        }
    }
}

/// What steps 1 and 2 hand the tail: the page, its owners' counted wins and the
/// hands board's markers in order.
pub(crate) struct BoardPageHead {
    owners: Vec<String>,
    truncated: bool,
    next_after: Option<usize>,
    extras: crate::logic::LeaderboardPageExtras,
    pot_rows: Vec<crate::logic::ChainWinPotRow>,
    hands: Vec<ResultMarkerRow>,
}

/// Where the hands pots beyond the page take their attribution from: the
/// candidates (every candidate win of those pots' winners), or `board_wins` by
/// pot. The fold reads an attribution only for a pot it classified, so both
/// answer the same for the hands pots.
pub(crate) enum HandsPotRows<'a> {
    Candidates(&'a [crate::logic::ChainWinCandidate]),
    Table,
}

/// Steps 3 to 6 of [`compute_leaderboard_body_string`]: the statuses, the
/// classification, the markers, the hands pots, the fold and the #685
/// decoration, then the body.
pub(crate) async fn leaderboard_page_tail<B: crate::board_db::BoardDb>(
    db: &B,
    page: usize,
    head: BoardPageHead,
    hands_pots: HandsPotRows<'_>,
    computed_at: i64,
) -> (u16, String) {
    leaderboard_page_tail_with(
        db,
        page,
        head,
        hands_pots,
        &PagePrefetch::default(),
        computed_at,
    )
    .await
}

/// [`leaderboard_page_tail`] over the rows the page owners' cards already hold
/// (`pre`): a pot or a proof key held there is not read again.
pub(crate) async fn leaderboard_page_tail_with<B: crate::board_db::BoardDb>(
    db: &B,
    page: usize,
    head: BoardPageHead,
    hands_pots: HandsPotRows<'_>,
    pre: &PagePrefetch,
    computed_at: i64,
) -> (u16, String) {
    use crate::board_db::{warn, Bind};
    let BoardPageHead {
        owners,
        truncated,
        next_after,
        extras,
        pot_rows,
        hands,
    } = head;

    // 3) Statuses (the SAME table /utxo-status reads) — FAIL-SAFE 503.
    let mut outpoints: Vec<crate::logic::Outpoint> = Vec::new();
    {
        let mut seen = std::collections::HashSet::new();
        for r in &pot_rows {
            if seen.insert(r.pot_txid.to_ascii_lowercase()) {
                outpoints.push(crate::logic::Outpoint {
                    txid: r.pot_txid.clone(),
                    vout: crate::logic::LEADERBOARD_POT_VOUT,
                });
            }
        }
    }
    let mut status_rows: Vec<PotRecordRow> = Vec::with_capacity(outpoints.len());
    let unheld: Vec<crate::logic::Outpoint> = outpoints
        .iter()
        .filter(|o| match pre.status.get(&o.txid) {
            Some(row) => {
                status_rows.push(row.clone());
                false
            }
            None => true,
        })
        .cloned()
        .collect();
    for chunk in chunk_outpoints(&unheld) {
        let mut b: Vec<Bind> = Vec::with_capacity(chunk.len() * 2);
        for op in chunk {
            b.push(Bind::Text(op.db_txid()));
            b.push(Bind::Num(f64::from(op.vout)));
        }
        match db.all::<PotRowD1>(batch_where_sql(chunk.len()), b).await {
            Ok(rows) => status_rows.extend(rows.into_iter().map(PotRowD1::into_row)),
            Err(e) => {
                warn(&format!(
                    "[leaderboard] pot_records batch query failed: {e}"
                ));
                return (503, "{\"error\":\"database query failed\"}".to_string());
            }
        }
    }
    let statuses = assemble_statuses(&outpoints, &status_rows);
    // Classification (column tier — verdict, committed params, signers) for
    // the page's pots; attribution from the page rows' own latched resolution.
    let (verdicts, params_by_pot, signers_by_pot) =
        classify_spent_pots_with(db, &statuses, &pre.decoded).await;
    let attributions = crate::logic::attributions_from_pot_rows(&pot_rows);

    // 4) Decoration — BEST-EFFORT: a fault omits decoration, never a count.
    let mut markers: Vec<ResultMarkerRow> = Vec::new();
    let mut marker_seen: std::collections::HashSet<(String, String, String)> =
        std::collections::HashSet::new();
    let mut push_marker = |m: ResultMarkerRow| {
        let key = (
            m.txid.to_ascii_lowercase(),
            m.game_id.to_ascii_lowercase(),
            m.winner.to_ascii_lowercase(),
        );
        if marker_seen.insert(key) {
            markers.push(m);
        }
    };
    let pot_txids: Vec<String> = outpoints.iter().map(|o| o.txid.clone()).collect();
    for chunk in pot_txids.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
        // A chunk's markers in `pot_markers_sql`'s order (`potTxid ASC`, then
        // each pot's own order), the held pots' from their cards.
        let mut by_pot: Vec<(String, Vec<ResultMarkerRow>)> = Vec::new();
        let unheld: Vec<&String> = chunk
            .iter()
            .filter(|t| match pre.markers.get(*t) {
                Some(ms) => {
                    by_pot.push(((*t).clone(), ms.clone()));
                    false
                }
                None => true,
            })
            .collect();
        if !unheld.is_empty() {
            let b: Vec<Bind> = unheld.iter().map(|t| Bind::Text((*t).clone())).collect();
            match db
                .all::<ResultRowD1>(crate::logic::pot_markers_sql(unheld.len()), b)
                .await
            {
                Ok(rows) => {
                    for m in rows.into_iter().filter_map(ResultRowD1::into_marker) {
                        match by_pot.last_mut() {
                            Some((pot, ms)) if *pot == m.pot_txid => ms.push(m),
                            _ => by_pot.push((m.pot_txid.clone(), vec![m])),
                        }
                    }
                }
                Err(e) => {
                    warn(&format!(
                        "[leaderboard] pot markers query failed (decoration omitted): {e}"
                    ));
                }
            }
        }
        by_pot.sort_by(|a, b| a.0.cmp(&b.0));
        for m in by_pot.into_iter().flat_map(|(_, ms)| ms) {
            push_marker(m);
        }
    }
    // The hands board (the era hands query's rows).
    for m in hands {
        push_marker(m);
    }
    // The hands pots may lie beyond the page: their statuses/verdicts are
    // needed for the fold to count (and so decorate) them. Top up the
    // status + classification inputs for any hands-only pot.
    let extra_outpoints: Vec<crate::logic::Outpoint> = {
        let have: std::collections::HashSet<String> = outpoints
            .iter()
            .map(|o| o.txid.to_ascii_lowercase())
            .collect();
        crate::logic::leaderboard_pot_outpoints(&markers)
            .into_iter()
            .filter(|o| !have.contains(&o.txid.to_ascii_lowercase()))
            .collect()
    };
    let (statuses, verdicts, params_by_pot, signers_by_pot, attributions) = if extra_outpoints
        .is_empty()
    {
        (
            statuses,
            verdicts,
            params_by_pot,
            signers_by_pot,
            attributions,
        )
    } else {
        let mut extra_rows: Vec<PotRecordRow> = Vec::new();
        for chunk in chunk_outpoints(&extra_outpoints) {
            let mut b: Vec<Bind> = Vec::with_capacity(chunk.len() * 2);
            for op in chunk {
                b.push(Bind::Text(op.db_txid()));
                b.push(Bind::Num(f64::from(op.vout)));
            }
            match db.all::<PotRowD1>(batch_where_sql(chunk.len()), b).await {
                Ok(rows) => extra_rows.extend(rows.into_iter().map(PotRowD1::into_row)),
                Err(e) => warn(&format!(
                    "[leaderboard] hands pots status query failed (hands omitted): {e}"
                )),
            }
        }
        let extra_statuses = assemble_statuses(&extra_outpoints, &extra_rows);
        let (ev, ep, es) = classify_spent_pots(db, &extra_statuses).await;
        let mut hands_attr = attributions;
        match hands_pots {
            // The hands pots' attribution comes from the SAME candidate
            // resolution (the hands are only identity-resolved winners):
            // every candidate win of those pots' winners, as the owners
            // query read them before #642.
            HandsPotRows::Candidates(cands) => {
                let hands_owners: std::collections::HashSet<String> = markers
                    .iter()
                    .filter(|m| {
                        extra_outpoints
                            .iter()
                            .any(|o| o.txid.eq_ignore_ascii_case(&m.pot_txid))
                    })
                    .map(|m| m.winner.to_ascii_lowercase())
                    .collect();
                let rows: Vec<crate::logic::ChainWinPotRow> = cands
                    .iter()
                    .filter(|c| hands_owners.contains(&c.row.owner))
                    .map(|c| c.row.clone())
                    .collect();
                hands_attr.extend(crate::logic::attributions_from_pot_rows(&rows));
            }
            // #674: the hands pots' own `board_wins` rows. BEST-EFFORT.
            HandsPotRows::Table => {
                let pots: Vec<String> = extra_outpoints.iter().map(|o| o.txid.clone()).collect();
                for chunk in pots.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
                    let b: Vec<Bind> = chunk.iter().map(|t| Bind::Text(t.clone())).collect();
                    match db
                        .all::<ChainWinCandidateRowD1>(
                            crate::logic::board_wins_pots_sql(chunk.len()),
                            b,
                        )
                        .await
                    {
                        Ok(rows) => {
                            let rows: Vec<crate::logic::ChainWinPotRow> =
                                rows.into_iter().map(|r| r.into_candidate().0.row).collect();
                            hands_attr.extend(crate::logic::attributions_from_pot_rows(&rows));
                        }
                        Err(e) => warn(&format!(
                            "[leaderboard] hands pots wins query failed (hands unattributed): {e}"
                        )),
                    }
                }
            }
        }
        let mut statuses = statuses;
        statuses.extend(extra_statuses);
        let mut verdicts = verdicts;
        verdicts.extend(ev);
        let mut params_by_pot = params_by_pot;
        params_by_pot.extend(ep);
        let mut signers_by_pot = signers_by_pot;
        signers_by_pot.extend(es);
        (
            statuses,
            verdicts,
            params_by_pot,
            signers_by_pot,
            hands_attr,
        )
    };

    // 5) + 6) The fold, cut to the page, then the #685 decoration.
    let (lb, result_count) = leaderboard_fold_and_decorate_with(
        db,
        &markers,
        &statuses,
        page,
        &verdicts,
        &attributions,
        &params_by_pot,
        &signers_by_pot,
        &owners,
        pre,
    )
    .await;
    (
        200,
        crate::logic::leaderboard_body_paged(
            &lb,
            computed_at,
            result_count,
            truncated,
            next_after,
            &extras,
        ),
    )
}

/// Steps 5 and 6 of [`compute_leaderboard_body_string`]: the fold (UNCHANGED),
/// the cut to the page's owners, and the #685 proof decoration over the
/// page-local set. Returns the board and `resultCount` (the markers read).
/// The proof pointers and posted bundles the page owners' cards hold (`pre`,
/// bsv-low #674 fold) are not read again.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn leaderboard_fold_and_decorate_with<B: crate::board_db::BoardDb>(
    db: &B,
    markers: &[ResultMarkerRow],
    statuses: &[crate::logic::OutpointStatus],
    page: usize,
    verdicts: &std::collections::HashMap<String, crate::results::PotVerdict>,
    attributions: &std::collections::HashMap<String, crate::results::SeatAttribution>,
    params_by_pot: &std::collections::HashMap<String, crate::results::CovenantParams>,
    signers_by_pot: &std::collections::HashMap<String, String>,
    owners: &[String],
    pre: &PagePrefetch,
) -> (crate::logic::Leaderboard, usize) {
    use crate::board_db::{warn, Bind};
    // 5) The fold (UNCHANGED) — wins minted from chain facts, the winner's own
    // verified marker decorating — then cut to the page in rank order. Proof
    // decoration is DEFERRED to the page-local set below (#685): the aggregator
    // is handed NO pointers, so the budget is spent on the served rows in
    // display priority, not on an incidental marker-order prefix.
    let mut lb = crate::logic::aggregate_leaderboard_attributed(
        markers,
        statuses,
        &std::collections::HashMap::new(),
        page,
        verdicts,
        attributions,
        params_by_pot,
        signers_by_pot,
    );
    crate::logic::retain_page_owners(&mut lb, owners);

    // Proof DECORATION as a budget of ROOM over the PAGE-LOCAL evidence set,
    // ordered by display priority (bsv-low #685, RC-L32): the lowest-hand
    // board, each identity's pinned best, then the top-ranked rows are fetched
    // FIRST. Since #685 MEDIUM-1 the budget (LEADERBOARD_PROOF_PAIRS_CAP) is
    // the page-local set's own bound (LEADERBOARD_MAX_LIMIT times
    // RESULT_ROWS_PER_POT), so every rendered row is decorated and carries its
    // filed proof; the display priority and the `proofPending` tail stay a
    // defensive backstop (a page past that bound would drop only its deep tail,
    // never the rows a client renders, and never a false `proofPosted: false`).
    // The two chunk loops stay bounded by the PAGE, never the history; the
    // pairs are already lowercase (the page's own evidence keys).
    let decorate_pairs =
        crate::logic::leaderboard_decoration_pairs(&lb, crate::logic::LEADERBOARD_PROOF_PAIRS_CAP);
    // The pairs the page owners' cards hold answer from there (bsv-low #674
    // fold); the rest are read.
    let mut proof_map: std::collections::HashMap<(String, String), Vec<String>> =
        std::collections::HashMap::new();
    let mut posted: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
    let unheld_pairs: Vec<(String, String)> = decorate_pairs
        .iter()
        .filter(|k| match pre.proofs.get(*k) {
            Some((pointers, is_posted)) => {
                if !pointers.is_empty() {
                    proof_map.insert((*k).clone(), pointers.clone());
                }
                if *is_posted {
                    posted.insert((*k).clone());
                }
                false
            }
            None => true,
        })
        .cloned()
        .collect();

    // proof_markers pointers, KEYED to the page-local (gameId, winner) pairs —
    // the bounded SUPERSET per key (#332 HIGH-1; the client filters by
    // transcript validity). BEST-EFFORT: a fault omits the hint, never a count.
    for chunk in unheld_pairs.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
        let mut b: Vec<Bind> = Vec::with_capacity(chunk.len() * 2);
        for (g, w) in chunk {
            b.push(Bind::Text(g.clone()));
            b.push(Bind::Text(w.clone()));
        }
        match db
            .all::<ProofPointerRowD1>(crate::logic::proof_pointers_sql(chunk.len()), b)
            .await
        {
            Ok(rows) => {
                for pr in rows {
                    proof_map
                        .entry((
                            pr.game_id.to_ascii_lowercase(),
                            pr.winner.to_ascii_lowercase(),
                        ))
                        .or_default()
                        .push(pr.txid);
                }
            }
            Err(e) => warn(&format!(
                "[leaderboard] proof_markers query failed (proofTxid omitted): {e}"
            )),
        }
    }

    // Proof-in-DB (2026-09-02): the posted-bundle set over the same page-local
    // pairs (a posted proof is not a marker; the aggregator has nothing to
    // count). BEST-EFFORT.
    for chunk in unheld_pairs.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
        let mut b: Vec<Bind> = Vec::with_capacity(chunk.len() * 2);
        for (g, w) in chunk {
            b.push(Bind::Text(g.clone()));
            b.push(Bind::Text(w.clone()));
        }
        match db
            .all::<ProofPostedRowD1>(crate::proof_post::proof_posted_sql(chunk.len()), b)
            .await
        {
            Ok(rows) => {
                for pr in rows {
                    posted.insert((
                        pr.game_id.to_ascii_lowercase(),
                        pr.winner.to_ascii_lowercase(),
                    ));
                }
            }
            Err(e) => warn(&format!(
                "[leaderboard] proof_posts query failed (proofPosted omitted): {e}"
            )),
        }
    }

    // Stamp the decorated rows; a row the budget could not reach is served
    // `proofPending`, counted for the budget-hit signal (#685).
    let budget_hits =
        crate::logic::stamp_leaderboard_decoration(&mut lb, &decorate_pairs, &proof_map, &posted);
    if budget_hits > 0 {
        crate::courier::note_presence_n(
            crate::logic::LEADERBOARD_DECORATION_BUDGET_HIT,
            budget_hits as u64,
        );
    }
    (lb, markers.len())
}

/// One row of the #642 candidate read
/// ([`crate::logic::chain_wins_candidates_sql`]): the owners query's columns,
/// the pot's stamp and rowid, and the winner's hand marker as JSON.
#[derive(Deserialize)]
struct ChainWinCandidateRowD1 {
    owner: String,
    #[serde(rename = "identityIsKey", default)]
    identity_is_key: Option<f64>,
    #[serde(rename = "potTxid")]
    pot_txid: String,
    #[serde(rename = "settleTxid")]
    settle_txid: String,
    verdict: String,
    #[serde(rename = "pubA")]
    pub_a: String,
    #[serde(rename = "pubB")]
    pub_b: String,
    #[serde(rename = "settleSigners", default)]
    settle_signers: Option<String>,
    #[serde(rename = "identityA", default)]
    identity_a: Option<String>,
    #[serde(rename = "identityB", default)]
    identity_b: Option<String>,
    #[serde(rename = "potCreatedAt", default)]
    pot_created_at: Option<f64>,
    /// The hand marker (`json_object` of the `result_markers_v2` columns plus
    /// `handScore`), or NULL.
    #[serde(default)]
    hand: Option<String>,
}

impl ChainWinCandidateRowD1 {
    /// The candidate and its hand marker through the production
    /// [`ResultRowD1::into_marker`] (a hand whose marker does not map still
    /// holds its place in the hands order, as its row did in the SQL `LIMIT`).
    fn into_candidate(self) -> (crate::logic::ChainWinCandidate, Option<ResultMarkerRow>) {
        let hand_json: Option<serde_json::Value> = self
            .hand
            .as_deref()
            .and_then(|h| serde_json::from_str(h).ok());
        let hand = hand_json.as_ref().map(|v| crate::logic::CandidateHand {
            score: v["handScore"]
                .as_i64()
                .or_else(|| v["handScore"].as_f64().map(|f| f as i64))
                .unwrap_or(0),
            created_at: v["createdAt"]
                .as_i64()
                .or_else(|| v["createdAt"].as_f64().map(|f| f as i64)),
            game_id: v["gameId"].as_str().unwrap_or_default().to_string(),
        });
        let marker = hand_json
            .and_then(|v| serde_json::from_value::<ResultRowD1>(v).ok())
            .and_then(ResultRowD1::into_marker);
        (
            crate::logic::ChainWinCandidate {
                row: crate::logic::ChainWinPotRow {
                    owner: self.owner,
                    identity_is_key: self.identity_is_key.is_some_and(|v| v != 0.0),
                    pot_txid: self.pot_txid,
                    settle_txid: self.settle_txid,
                    verdict: self.verdict,
                    pub_a: self.pub_a,
                    pub_b: self.pub_b,
                    settle_signers: self.settle_signers,
                    identity_a: self.identity_a,
                    identity_b: self.identity_b,
                },
                pot_created_at: self.pot_created_at.map(|v| v as i64),
                hand,
            },
            marker,
        )
    }
}

/// `pot_beefs` row for the classification fold: txid + `hex(beef)`.
#[derive(Deserialize)]
struct PotBeefRowD1 {
    txid: String,
    beef: Option<String>,
}

/// `pot_records` decoded-column row for the #284 classification partition
/// (`decoded_pots_sql`). All optional/`default` — a legacy row is NULLs.
#[derive(Deserialize, Clone)]
pub(crate) struct DecodedPotRowD1 {
    txid: String,
    #[serde(rename = "spendingTxid", default)]
    spending_txid: Option<String>,
    #[serde(rename = "lockKind", default)]
    lock_kind: Option<String>,
    #[serde(rename = "pubA", default)]
    pub_a: Option<String>,
    #[serde(rename = "pubB", default)]
    pub_b: Option<String>,
    #[serde(rename = "pubTower", default)]
    pub_tower: Option<String>,
    #[serde(rename = "payPkhA", default)]
    pay_pkh_a: Option<String>,
    #[serde(rename = "payPkhB", default)]
    pay_pkh_b: Option<String>,
    #[serde(rename = "rakePkh", default)]
    rake_pkh: Option<String>,
    #[serde(rename = "stakeA", default)]
    stake_a: Option<f64>,
    #[serde(rename = "stakeB", default)]
    stake_b: Option<f64>,
    #[serde(rename = "feeSats", default)]
    fee_sats: Option<f64>,
    #[serde(rename = "covRecoveryHeight", default)]
    cov_recovery_height: Option<f64>,
    #[serde(rename = "verdict", default)]
    verdict: Option<String>,
    #[serde(rename = "verdictTxid", default)]
    verdict_txid: Option<String>,
    /// bsv-low #406: who signed the recorded spend — rides the verdict group,
    /// so the SAME freshness guard (`verdictTxid == spendingTxid == spender`)
    /// covers it. `serde(default)` tolerates a pre-migration read.
    #[serde(rename = "settleSigners", default)]
    settle_signers: Option<String>,
}

impl DecodedPotRowD1 {
    /// Strict column reconstruction of the committed params (length/hex
    /// validated) — `None` falls back to the BLOB path, never a shortcut.
    pub(crate) fn covenant_params(&self) -> Option<crate::results::CovenantParams> {
        if self.lock_kind.as_deref() != Some("covenant") {
            return None;
        }
        crate::results::covenant_params_from_hex(
            self.pub_a.as_deref()?,
            self.pub_b.as_deref()?,
            self.pub_tower.as_deref()?,
            self.pay_pkh_a.as_deref()?,
            self.pay_pkh_b.as_deref()?,
            self.rake_pkh.as_deref()?,
            self.stake_a? as u64,
            self.stake_b? as u64,
            self.fee_sats? as u64,
            self.cov_recovery_height? as u64,
        )
    }
}

/// Classify the recorded spends of the SPENT pots in `statuses` (vout 0 —
/// the leaderboard anchor). #284 two-tier sourcing:
///
/// 1. **Column partition (uncapped, no BLOBs):** pots whose `pot_records`
///    row carries a FRESH stored verdict (`verdictTxid` equal to the spender
///    being attributed — write-time classified, conservation enforced by the
///    overlay) plus strict column params. A pure column read.
/// 2. **Legacy fallback (bounded by the marker WINDOW, #332):** the pre-#284
///    path — stored `pot_beefs` bytes, hash-verified, classified per request.
///    Covers un-backfilled rows and stale verdicts; dies entirely once the
///    backfill completes. It carried a separate 64-pot cap until #332; that
///    cap was ordered by attacker-advanceable marker recency, so it could
///    push a victim's pot out of classification (and out of attribution,
///    un-counting a tower-enforced win). The window's own bound replaces it.
///
/// Returns a lowercase-pot-txid → verdict map PLUS each classified pot's
/// committed covenant params (the #230 attribution needs its lock keys);
/// every fault or ambiguity simply omits that pot (see `results.rs` for the
/// conservatism contract).
async fn classify_spent_pots<B: crate::board_db::BoardDb>(
    db: &B,
    statuses: &[crate::logic::OutpointStatus],
) -> (
    std::collections::HashMap<String, crate::results::PotVerdict>,
    std::collections::HashMap<String, crate::results::CovenantParams>,
    std::collections::HashMap<String, String>,
) {
    classify_spent_pots_with(db, statuses, &std::collections::HashMap::new()).await
}

/// [`classify_spent_pots`] with the decoded rows the page owners' cards hold
/// (`held`, by lowercase pot txid; bsv-low #674 fold): the column tier reads
/// only the pots not held. The tiers are unchanged.
async fn classify_spent_pots_with<B: crate::board_db::BoardDb>(
    db: &B,
    statuses: &[crate::logic::OutpointStatus],
    held: &std::collections::HashMap<String, DecodedPotRowD1>,
) -> (
    std::collections::HashMap<String, crate::results::PotVerdict>,
    std::collections::HashMap<String, crate::results::CovenantParams>,
    std::collections::HashMap<String, String>,
) {
    let mut verdicts = std::collections::HashMap::new();
    let mut params_by_pot = std::collections::HashMap::new();
    // bsv-low #406: who signed each pot's settle — COLUMN-TIER ONLY (the
    // latch + backfill write it; this read path never runs ECDSA). A pot the
    // sweep has not reached serves nothing and the client says "not
    // established" — self-draining, exactly the claimValid pattern.
    let mut signers_by_pot: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();

    // ALL spent pots with a recorded spender, deduped, in WINDOW RANK order
    // (#332: `statuses` follows the marker window, whose pot order is the
    // pot's own admission stamp — not attacker-advanceable marker recency).
    let mut all_pairs: Vec<(String, String)> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for s in statuses {
        // #323 HIGH-3 — CONFIRMED spends only. The overlay's unconfirmed
        // write path stamps `verdict`/`verdictTxid` while leaving
        // `spentConfirmed = 0`, so a PARKED spender would mint a real
        // `PotVerdict` here — which can both CREATE a chain-attributed win
        // and ERASE an honest claim via `chain_contradicted`. `spent_confirmed`
        // was already in the struct; it was simply not read.
        if crate::logic::is_confirmed_landing(s) {
            if let Some(spender) = &s.spending_txid {
                let pot = s.txid.to_ascii_lowercase();
                if seen.insert(pot.clone()) {
                    all_pairs.push((pot, spender.to_ascii_lowercase()));
                }
            }
        }
    }
    if all_pairs.is_empty() {
        return (verdicts, params_by_pot, signers_by_pot);
    }

    // ── Tier 1: the decoded-column partition (no BLOB fetch, NO CAP) ──────
    // BEST-EFFORT: any fault leaves rows unresolved here and the fallback
    // picks them up — never a 5xx, never a guessed verdict.
    let mut column_resolved: std::collections::HashSet<String> = std::collections::HashSet::new();
    let (held_pairs, read_pairs): (Vec<_>, Vec<_>) = all_pairs
        .iter()
        .cloned()
        .partition(|(pot, _): &(String, String)| held.contains_key(pot));
    let mut chunks: Vec<(&[(String, String)], bool)> = vec![(&held_pairs, true)];
    chunks.extend(
        read_pairs
            .chunks(crate::logic::D1_CHUNK_OUTPOINTS)
            .map(|c| (c, false)),
    );
    for (chunk, is_held) in chunks {
        let read = if is_held {
            Ok(Vec::new())
        } else {
            let sql = crate::results::decoded_pots_sql(chunk.len());
            let mut binds: Vec<crate::board_db::Bind> = Vec::with_capacity(chunk.len() * 2);
            for (pot, _) in chunk {
                binds.push(crate::board_db::Bind::Text(pot.clone()));
                binds.push(crate::board_db::Bind::Num(f64::from(
                    crate::logic::LEADERBOARD_POT_VOUT,
                )));
            }
            db.all::<DecodedPotRowD1>(sql, binds).await
        };
        match read {
            Err(crate::board_db::BoardDbError::Bind(e)) => {
                crate::board_db::warn(&format!(
                    "[leaderboard] decoded-pots bind failed (column tier skipped): {e}"
                ));
                break;
            }
            Ok(rows) => {
                let by_txid: std::collections::HashMap<String, DecodedPotRowD1> = rows
                    .into_iter()
                    .map(|r| (r.txid.to_ascii_lowercase(), r))
                    .collect();
                for (pot, spender) in chunk {
                    let Some(row) = by_txid.get(pot).or_else(|| held.get(pot)) else {
                        continue;
                    };
                    // The stored verdict counts ONLY for the spender being
                    // attributed (freshness: verdictTxid == the pointer this
                    // board read; a stale/differing one falls back).
                    let fresh = row
                        .verdict_txid
                        .as_deref()
                        .is_some_and(|vt| vt.eq_ignore_ascii_case(spender))
                        && row
                            .spending_txid
                            .as_deref()
                            .is_some_and(|st| st.eq_ignore_ascii_case(spender));
                    if !fresh {
                        continue;
                    }
                    let (Some(v), Some(params)) = (
                        row.verdict
                            .as_deref()
                            .and_then(crate::results::PotVerdict::from_wire),
                        row.covenant_params(),
                    ) else {
                        continue;
                    };
                    verdicts.insert(pot.clone(), v);
                    params_by_pot.insert(pot.clone(), params);
                    // #406: the signer classification rides the verdict
                    // group, so `fresh` above IS its guard; `from_wire`
                    // drops 'unresolved'/garbage (→ not established).
                    if let Some(sig) = row
                        .settle_signers
                        .as_deref()
                        .and_then(crate::results::SettleSigners::from_wire)
                    {
                        signers_by_pot.insert(pot.clone(), sig.as_str().to_string());
                    }
                    column_resolved.insert(pot.clone());
                }
            }
            Err(e) => {
                crate::board_db::warn(&format!(
                    "[leaderboard] decoded-pots query failed (column tier partial): {e}"
                ));
            }
        }
    }

    // ── Tier 2: the legacy BLOB fallback — bounded by the WINDOW, not a
    // fixed cap (#332). The former `LEADERBOARD_CLASSIFY_CAP = 64` was
    // ordered by attacker-controllable marker recency, so a flood could push
    // a victim's pot past the cap, deny it a verdict + attribution, and —
    // now the win is chain-derived — un-count a tower-enforced win (erasure
    // through the cap). Post-#332 the partition is bounded at ≤ `limit + 1`
    // pots by the marker window (admission-stamp-ranked), and every pot the
    // column tier resolved costs no BLOB at all.
    //
    // COST, stated honestly (#332 MEDIUM-3 — the commit's earlier "≈23"
    // counted only THIS tier). The worst case is `?limit=500`, every windowed
    // pot legacy-unbackfilled: the request fans out to roughly the marker
    // window (1) + `pot_records` status chunks (≈12) + proof-pointer chunks
    // (≈12) + decoded-pots chunks (≈12) + `pot_beefs` chunks (≈23) + seat-
    // marker chunks (≈21) ≈ **~81 D1 statements**, single-digit MB transient.
    // (#399 gate S5: the chain candidate window can add up to limit+1 more
    // pots when fully disjoint from the marker window, roughly doubling that
    // worst case — still bounded by the same window arithmetic.)
    // Every one of those pots is a REAL admitted `tm_pot` row (an on-chain
    // LOW-template funding tx — not a dust marker), so the fan-out is priced
    // in real funding, and the whole tier-2 partition shrinks to zero as the
    // #284 column backfill completes. Attribution (the only counting input an
    // attacker cares about) is unaffected by this fan-out — it is
    // committed-key-bound (`seat_markers_sql`), so a flood cannot enlarge the
    // COUNTED set, only this best-effort classification work.
    let pairs: Vec<(String, String)> = all_pairs
        .into_iter()
        .filter(|(pot, _)| !column_resolved.contains(pot))
        .collect();
    if pairs.is_empty() {
        return (verdicts, params_by_pot, signers_by_pot);
    }

    // One IN-query per ≤45-key chunk over the DISTINCT txids (funding +
    // spender interleaved) — the same bound-param discipline as /utxo-status.
    let mut keys: Vec<String> = Vec::with_capacity(pairs.len() * 2);
    for (pot, spender) in &pairs {
        keys.push(pot.clone());
        keys.push(spender.clone());
    }
    keys.sort_unstable();
    keys.dedup();
    let mut beefs: std::collections::HashMap<String, Vec<u8>> = std::collections::HashMap::new();
    for chunk in keys.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
        let sql = crate::logic::leaderboard_pot_beefs_sql(chunk.len());
        let binds: Vec<crate::board_db::Bind> = chunk
            .iter()
            .map(|k| crate::board_db::Bind::Text(k.clone()))
            .collect();
        match db.all::<PotBeefRowD1>(sql, binds).await {
            Err(crate::board_db::BoardDbError::Bind(e)) => {
                crate::board_db::warn(&format!(
                    "[leaderboard] pot_beefs bind failed (classification omitted): {e}"
                ));
                return (verdicts, params_by_pot, signers_by_pot);
            }
            Ok(rows) => {
                for r in rows {
                    if let Some(bytes) = r.beef.and_then(|h| decode_beef_hex(&h)) {
                        beefs.insert(r.txid.to_ascii_lowercase(), bytes);
                    }
                }
            }
            Err(e) => {
                crate::board_db::warn(&format!(
                    "[leaderboard] pot_beefs query failed (classification partial): {e}"
                ));
                // Keep whatever chunks already loaded — a missing BEEF only
                // leaves its pot unclassified.
            }
        }
    }

    for (pot, spender) in &pairs {
        let (Some(fb), Some(sb)) = (beefs.get(pot), beefs.get(spender)) else {
            continue;
        };
        let funding_raw =
            crate::logic::extract_raw_tx_hex(fb, pot).and_then(|h| hex::decode(h).ok());
        let spender_raw =
            crate::logic::extract_raw_tx_hex(sb, spender).and_then(|h| hex::decode(h).ok());
        let (Some(fraw), Some(sraw)) = (funding_raw, spender_raw) else {
            continue;
        };
        if let Some(v) = crate::results::classify_pot_spend(&crate::results::PotSpendFacts {
            pot_txid: pot,
            pot_vout: crate::logic::LEADERBOARD_POT_VOUT,
            funding_raw: &fraw,
            spender_txid: spender,
            spender_raw: &sraw,
            marker_recovery_height: None, // no potparty join here — bare pots stay unclassified
        }) {
            verdicts.insert(pot.clone(), v);
            // #230: keep the classified pot's COMMITTED lock params (from
            // the hash-verified funding bytes) for the seat attribution.
            // Reads through the ONE shared funding-raw walk — this was the
            // third inline copy, and the only one shaped differently.
            if let Some(params) = crate::results::covenant_params_from_funding_raw(
                &fraw,
                pot,
                crate::logic::LEADERBOARD_POT_VOUT,
            ) {
                params_by_pot.insert(pot.clone(), params);
            }
        }
    }
    (verdicts, params_by_pot, signers_by_pot)
}

/// `potparty_records` v2 row for the #230 attribution join.
#[derive(Deserialize)]
pub(crate) struct SeatMarkerRowD1 {
    identity: String,
    #[serde(rename = "opponentIdentity")]
    opponent_identity: String,
    #[serde(rename = "gameId")]
    game_id: String,
    #[serde(rename = "potTxid")]
    pot_txid: String,
    #[serde(rename = "potVout")]
    pot_vout: f64,
    #[serde(rename = "recoveryHeight")]
    recovery_height: f64,
    #[serde(rename = "seatSettlePubkey")]
    seat_settle_pubkey: Option<String>,
    #[serde(rename = "seatSigHex")]
    seat_sig_hex: Option<String>,
    /// The marker's IDENTITY signature — required by the F1 binding check.
    #[serde(rename = "sigHex")]
    sig_hex: Option<String>,
    /// The admission-latched verdict (brain-cutover M1). `None` = legacy row
    /// (or a read racing the additive migration — `default` tolerates it).
    #[serde(rename = "sigValid", default)]
    sig_valid: Option<f64>,
}

/// One `hopparty_records` row as D1 returns it for the seat fallback.
#[derive(Deserialize)]
struct HopSeatRowD1 {
    identity: String,
    #[serde(rename = "seatSettlePubkey")]
    seat_settle_pubkey: String,
    #[serde(rename = "markerValid")]
    marker_valid: Option<f64>,
    /// bsv-low P4 slice 2: the container tx + hop outpoint + money facts
    /// (`default` tolerates a read racing the additive migrations).
    #[serde(default)]
    txid: String,
    #[serde(rename = "hopVout", default)]
    hop_vout: Option<f64>,
    #[serde(rename = "hopSats", default)]
    hop_sats: Option<f64>,
    #[serde(rename = "sizeBytes", default)]
    size_bytes: Option<f64>,
    #[serde(rename = "feeSats", default)]
    fee_sats: Option<f64>,
}

// ── /results — server-derived settle results (bsv-low #227) ─────────────────

/// `/results` joined row as D1 returns it (the `results_sql` shape): the
/// caller's potparty facts + spend pointer + BOTH stored BEEFs as hex.
#[derive(Deserialize)]
pub(crate) struct ResultsRowD1 {
    identity: String,
    #[serde(rename = "gameId")]
    game_id: String,
    #[serde(rename = "potTxid")]
    pot_txid: String,
    #[serde(rename = "potVout")]
    pot_vout: f64,
    #[serde(rename = "recoveryHeight")]
    recovery_height: f64,
    #[serde(rename = "opponentIdentity")]
    opponent_identity: String,
    spent: Option<f64>,
    #[serde(rename = "spendingTxid")]
    spending_txid: Option<String>,
    #[serde(rename = "spentConfirmed", default)]
    spent_confirmed: Option<f64>,
    #[serde(rename = "fundingBeef")]
    funding_beef: Option<String>,
    #[serde(rename = "spenderBeef")]
    spender_beef: Option<String>,
    /// #230 v2 seat-binding fields — NULL for v1 rows; `default` tolerates a
    /// read racing the overlay's additive migration.
    #[serde(rename = "seatSettlePubkey", default)]
    seat_settle_pubkey: Option<String>,
    #[serde(rename = "seatSigHex", default)]
    seat_sig_hex: Option<String>,
    /// The marker's IDENTITY signature (F1 binding check).
    #[serde(rename = "sigHex", default)]
    sig_hex: Option<String>,
    /// #284 decoded pot_records columns — NULL for legacy un-backfilled
    /// rows; `default` tolerates a read racing the additive migrations.
    #[serde(rename = "lockKind", default)]
    lock_kind: Option<String>,
    #[serde(rename = "pubA", default)]
    pub_a: Option<String>,
    #[serde(rename = "pubB", default)]
    pub_b: Option<String>,
    #[serde(rename = "pubTower", default)]
    pub_tower: Option<String>,
    #[serde(rename = "payPkhA", default)]
    pay_pkh_a: Option<String>,
    #[serde(rename = "payPkhB", default)]
    pay_pkh_b: Option<String>,
    #[serde(rename = "rakePkh", default)]
    rake_pkh: Option<String>,
    #[serde(rename = "stakeA", default)]
    stake_a: Option<f64>,
    #[serde(rename = "stakeB", default)]
    stake_b: Option<f64>,
    #[serde(rename = "feeSats", default)]
    fee_sats: Option<f64>,
    #[serde(rename = "covRecoveryHeight", default)]
    cov_recovery_height: Option<f64>,
    #[serde(rename = "potSats", default)]
    pot_sats: Option<f64>,
    #[serde(rename = "verdict", default)]
    verdict: Option<String>,
    #[serde(rename = "verdictTxid", default)]
    verdict_txid: Option<String>,
    #[serde(rename = "settleSigners", default)]
    settle_signers: Option<String>,
    #[serde(rename = "spentHeight", default)]
    spent_height: Option<f64>,
    /// bsv-low#304: the spender pot_beefs row's VERIFIED proof latch
    /// (`sb.proof_verified`); `default` tolerates a read racing the
    /// overlay's additive migration — absent = unverified (fail-safe).
    #[serde(rename = "spenderProofVerified", default)]
    spender_proof_verified: Option<f64>,
    /// #371: the overlay's own network witness (`network_seen` join — the
    /// SQL projects `ns.txid IS NOT NULL`, so 0 = no row) and the spender
    /// bytes-finality latch. `default` tolerates a read racing the additive
    /// migration — absent = no third arm (fail-safe to the merkle bar).
    #[serde(rename = "spenderSeen", default)]
    spender_seen: Option<f64>,
    #[serde(rename = "spenderFinal", default)]
    spender_final: Option<f64>,
    /// bsv-low P4 slice 2 money facts (display tier) — `default` tolerates a
    /// read racing the overlay's additive migrations (absent = not named).
    #[serde(rename = "fundingSizeBytes", default)]
    funding_size_bytes: Option<f64>,
    #[serde(rename = "fundingFeeSats", default)]
    funding_fee_sats: Option<f64>,
    #[serde(rename = "spenderFactsTxid", default)]
    spender_facts_txid: Option<String>,
    #[serde(rename = "spenderSizeBytes", default)]
    spender_size_bytes: Option<f64>,
    #[serde(rename = "spenderFeeSats", default)]
    spender_fee_sats: Option<f64>,
}

impl ResultsRowD1 {
    pub(crate) fn into_row(self) -> crate::results::ResultsRow {
        crate::results::ResultsRow {
            funding_decoded: None,
            spender_decoded: None,
            settle_signers: self.settle_signers,
            identity: self.identity,
            game_id: self.game_id,
            pot_txid: self.pot_txid,
            pot_vout: self.pot_vout as u32,
            recovery_height: self.recovery_height as u32,
            opponent_identity: self.opponent_identity,
            spent: self.spent.map(|v| v != 0.0),
            spending_txid: self.spending_txid,
            spent_confirmed: self.spent_confirmed.map(|v| v != 0.0),
            funding_beef_hex: self.funding_beef,
            spender_beef_hex: self.spender_beef,
            seat_settle_pubkey: self.seat_settle_pubkey,
            seat_sig_hex: self.seat_sig_hex,
            marker_sig_hex: self.sig_hex,
            lock_kind: self.lock_kind,
            pub_a: self.pub_a,
            pub_b: self.pub_b,
            pub_tower: self.pub_tower,
            pay_pkh_a: self.pay_pkh_a,
            pay_pkh_b: self.pay_pkh_b,
            rake_pkh: self.rake_pkh,
            stake_a: self.stake_a.map(|v| v as u64),
            stake_b: self.stake_b.map(|v| v as u64),
            fee_sats: self.fee_sats.map(|v| v as u64),
            cov_recovery_height: self.cov_recovery_height.map(|v| v as u64),
            pot_sats: self.pot_sats.map(|v| v as u64),
            // #468: the payouts ride the page overlay (`apply_page_overlay`), never the window query
            spender_pay_a_sats: None,
            spender_pay_b_sats: None,
            verdict: self.verdict,
            verdict_txid: self.verdict_txid,
            spent_height: self.spent_height.map(|v| v as u64),
            spender_proof_verified: self.spender_proof_verified.map(|v| v != 0.0),
            spender_seen: self.spender_seen.map(|v| v != 0.0),
            spender_final: self.spender_final.map(|v| v != 0.0),
            funding_size_bytes: self.funding_size_bytes.map(|v| v as u64),
            funding_fee_sats: self.funding_fee_sats.map(|v| v as u64),
            spender_facts_txid: self.spender_facts_txid,
            spender_size_bytes: self.spender_size_bytes.map(|v| v as u64),
            spender_fee_sats: self.spender_fee_sats.map(|v| v as u64),
        }
    }
}

/// `GET /results?identity=<66-hex>` — server-derived settle results (bsv-low
/// #227): the chain-truth classification of every indexed pot spend the
/// identity is party to, matched against the four covenant-mandated exit
/// templates derived from the pot's OWN committed lock params. The result
/// never depends on the winner's client publishing a claim: `tie`/`refund`
/// outcomes are pure chain truth; a winner-template classification is
/// exposed verbatim (`verdict`) and upgrades to a per-identity won/lost only
/// when unanimous on-record claims corroborate it (`outcomeSource` says
/// which). Full trust model + conservatism rules: `results.rs` module docs.
///
/// Fail-safe shape mirrors `/recovery-view`: a missing/invalid identity is
/// an EMPTY 200 result; a D1 fault on the primary query is a 503; a claims
/// (result_markers_v2) fault only degrades won/lost attribution to
/// `unresolved` — never a 5xx, never a guessed outcome. Bounded per the
/// over-50-outpoint 503 lesson: newest [`crate::results::RESULTS_MAX_ROWS`]
/// marker rows, claims queried in chunks of at most
/// [`crate::logic::D1_CHUNK_OUTPOINTS`] binds.
pub async fn results(req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    // #318: identity comes from the ONE auth seam (session identity wins;
    // mismatch refuses; anonymous lenient = the legacy query-param claim).
    let identity_lc = match view_identity(&req, &ctx) {
        ViewIdentity::Identity(id) => id,
        ViewIdentity::Refuse(resp) => return resp,
    };

    // The paging cursor (2026-08-21; the /recovery-view #398 contract):
    // absent/garbage `after` is 0 — the unchanged first page — and the value
    // clamps at the ceiling so a walker can never be handed a loop.
    let after = req
        .url()
        .ok()
        .and_then(|u| {
            u.query_pairs()
                .find(|(k, _)| k == "after")
                .and_then(|(_, v)| v.parse::<usize>().ok())
        })
        .unwrap_or(0)
        .min(crate::results::RESULTS_VIEW_AFTER_MAX);

    // S3 second iteration (2026-08-27): forward to the PER-IDENTITY view
    // actor. The #318 auth seam is untouched — identity resolves HERE (the
    // one seam), and the actor instance is NAMED by the resolved identity,
    // so it can only ever serve its own view. Any actor fault falls back to
    // direct compute (no new failure mode).
    if let Ok(ns) = ctx.env.durable_object("BOARD_VIEW") {
        if let Ok(stub) = ns
            .id_from_name(&format!("results:{identity_lc}"))
            .and_then(|id| id.get_stub())
        {
            let target = format!("https://board-view/results?identity={identity_lc}&after={after}");
            match stub.fetch_with_str(&target).await {
                // Same immutable-headers hazard as the board forward above.
                Ok(mut resp) => return rebuild_do_response(&mut resp, "/results:view").await,
                Err(e) => console_warn!("[results] view actor unreachable ({e}) — direct compute"),
            }
        }
    }
    let (status, body) = compute_results_body_string(&ctx.env, &identity_lc, after).await?;
    json_response_cached(body, status, 5)
}

/// S3 — the whole `/results` pipeline as a callable (auth already resolved
/// by the caller; see the seam note above). The view actor and the route's
/// direct fallback both serve EXACTLY this.
pub async fn compute_results_body_string(
    env: &worker::Env,
    identity_lc: &str,
    after: usize,
) -> Result<(u16, String)> {
    if !crate::logic::valid_identity(identity_lc) {
        return Ok((
            200,
            crate::results::results_body(identity_lc, &[], false, after),
        ));
    }

    let db = match env.d1("OVERLAY_DB") {
        Ok(db) => db,
        Err(e) => {
            console_warn!("[results] OVERLAY_DB binding unavailable: {e}");
            return Ok((503, "{\"error\":\"database unavailable\"}".to_string()));
        }
    };

    let (entries, truncated) = match gather_result_entries(
        &db,
        identity_lc,
        written_off_before_ms_env(env),
        after,
    )
    .await
    {
        Ok(v) => v,
        Err(e) => {
            console_warn!("[results] {e}");
            return Ok((503, "{\"error\":\"database query failed\"}".to_string()));
        }
    };
    Ok((
        200,
        crate::results::results_body(identity_lc, &entries, truncated, after),
    ))
}

/// The whole `/results` derivation for one identity — rows, claims, seat
/// proofs, outcome words — extracted so `/recovery-view` REUSES it for its
/// `outcome` field (#252 stage A, Rule 15: derive once; a second
/// implementation of the outcome word would drift). `Err` = the PRIMARY
/// results query failed (each caller keeps its own posture: `/results`
/// answers 503, `/recovery-view` serves null outcomes — best-effort). The
/// claims/seat legs stay BEST-EFFORT inside, exactly as before.
/// W2-P4, E649 L4: `POST /internal/pot-changed` (bearer `INTERNAL_TOKEN`; the overlay's pot storage notes every
/// changed outpoint and ships this once per unit of work). The answer path is D1 alone
/// (`pot_hook::serve_pot_changed` over `pot_hook::HookD1`): per outpoint the committed params and the seat
/// IDENTITIES their verified seat markers attribute; for an outpoint with no attributed seat, the identities the
/// seats' own markers name, marked stale; and every wake-up of the body (each outpoint's pots-room broadcast, one
/// `pot` event per attributed seat) PARKED durably in the same write batch as the stale marks (H1). The relay calls
/// are SCHEDULED, not made: the detached announce (`ctx.wait_until`, under one wall) sends them after the answer,
/// and what it does not deliver stays parked for the scheduled drain. A body the park's ceiling refuses WHOLE (no room
/// to make, `pot_hook::PARK_GUARD_SQL`) is answered 503 like a faulted batch. So the answer's `filed` names the seat events
/// scheduled and parked, not delivered. The `pot` event carries an EMPTY `entry` (`pot_hook::carrier_entry`): no
/// `/results` entry is assembled, the seat re-reads its served rows. A write batch that faulted is answered 503
/// retryable. Nothing here is money truth: the client still verifies landings before any credit.
pub(crate) async fn internal_pot_changed(
    mut req: Request,
    env: &worker::Env,
    ctx: &worker::Context,
) -> Result<Response> {
    let t0 = worker::Date::now().as_millis();
    if !crate::internal_events::internal_bearer_ok(&req, env) {
        return Response::error("unauthorized", 401);
    }
    let raw = req.bytes().await?;
    let (outpoints, dropped) = crate::internal_events::parse_pot_changed_counted(&raw);
    // bsv-low #668 (step 0, hop A): the overlay's ship time, carried onto the pots-room push (additive, optional).
    let admitted_at_ms = crate::internal_events::parse_pot_admitted_at(&raw);
    if outpoints.is_empty() {
        return Response::error("body must be {\"outpoints\":[{\"txid\",\"vout\"}]}", 400);
    }
    // bsv-low #436: the overlay chunks its flush at `POT_CHANGED_MAX`, so nothing is over the cap from our own
    // producer. A body that still carries more is never trimmed silently: logged, counted, and answered.
    if dropped > 0 {
        console_warn!(
            "[pot-changed] body carried {} outpoint(s) past the cap of {}: {dropped} NOT announced and NOT filed (the producer must chunk)",
            outpoints.len() + dropped,
            crate::internal_events::POT_CHANGED_MAX
        );
        let counter_db = env.d1("OVERLAY_DB").ok();
        ctx.wait_until(async move {
            crate::courier::flush(
                counter_db,
                vec![(
                    crate::internal_events::COUNTER_POT_CHANGED_DROPPED.to_string(),
                    dropped as u64,
                )],
            )
            .await;
        });
    }
    // E649 L4: the hook is a WRITE. Its answer path is `pot_hook::serve_pot_changed` over the D1 port alone; the
    // pots-room announce of every parsed outpoint (loop 10 D2) and the attributed seats' `pot` events are handed to
    // `ctx.wait_until` under one wall, counted; no `/results` gather.
    let store = std::rc::Rc::new(crate::pot_hook::HookD1 {
        sql: D1Sql::of(env)?,
    });
    let relay = std::rc::Rc::new(crate::pot_hook::WorkerRelay { env: env.clone() });
    let wall = worker::Delay::from(core::time::Duration::from_millis(
        crate::pot_hook::ANNOUNCE_WALL_MS,
    ));
    // bsv-low #668 (hop C): a seconds-cadence re-drive of a walled announce, within the same `wait_until` window,
    // before the wake-up settles to the `*/15` drain. The relay/hub cold-DO band clears on a fresh subrequest.
    let redrive = crate::pot_hook::Redrive {
        tries: crate::pot_hook::ANNOUNCE_REDRIVE_TRIES,
        attempt_wall: Box::new(|| {
            Box::pin(worker::Delay::from(core::time::Duration::from_millis(
                crate::pot_hook::ANNOUNCE_REDRIVE_WALL_MS,
            ))) as crate::pot_hook::Detached
        }),
        backoff: Box::new(|| {
            Box::pin(worker::Delay::from(core::time::Duration::from_millis(
                crate::pot_hook::ANNOUNCE_REDRIVE_BACKOFF_MS,
            ))) as crate::pot_hook::Detached
        }),
    };
    let detach = |f: crate::pot_hook::Detached| ctx.wait_until(f);
    let outcome = crate::pot_hook::serve_pot_changed_at(
        store,
        relay,
        wall,
        &detach,
        &outpoints,
        t0,
        admitted_at_ms,
        redrive,
    )
    .await;
    if let Some(e) = &outcome.write_fault {
        console_warn!("[pot-changed] write batch (stale marks and parks) failed: {e}");
    }
    // The overlay source transaction already marked every affected game. bsv-low #911 (D2-ACTOR-A, MEDIUM-7): the
    // felt's announce above is unchanged; the judge's trigger is the `owed-events` consumer, and the overlay's write
    // published these outpoints' `pot` events at its own flush (a publish that faulted there is the sweep's), so the
    // hook publishes only what THIS invocation noted (nothing, today: no statement) and never judges
    // (D2-ACTOR-A-FOLD-4: re-publishing the pairs cost the hook a seq bump and a read per outpoint for nothing).
    if crate::owed_events::configured(env).await {
        crate::owed_events::publish_noted(env).await;
        // D2-ACTOR-D (bsv-low #952): what this invocation published, its actors called after the answer.
        crate::owed_events::schedule_direct(env, ctx);
    } else {
        let tip = chaintracks_present_height_env(env, "owed").await.ok();
        let judge = crate::owed_judge::D1Judge::new(env, tip, written_off_before_ms_env(env))?;
        let identities = outcome.owed_identities;
        match crate::owed_judge::drain_identities(&judge, &identities, "pot-changed").await {
            Ok(report) => for (_, error) in report.faults { console_warn!("[owed] pot judgement deferred: {error}"); },
            Err(error) => console_warn!("[owed] pot drain failed: {error}"),
        }
    }
    // FIX-935-FOLD: the judge's `owed-changed` pushes are queued, never sent in the answer path; handed here.
    crate::push_coalesce::flush_after(ctx, env);
    let ms = worker::Date::now().as_millis().saturating_sub(t0);
    worker::console_log!(
        "[pot-changed] answered in {ms} ms, announce detached ({} outpoints)",
        outcome.plan.outpoints.len()
    );
    // H1: a body whose wake-ups could not be parked is not acknowledged: 503 keeps it on the producer's retry path.
    // N2: nothing of that body was announced (the retry announces once). N6: so is a body the park's ceiling refused
    // whole (`parkFull` names its wake-ups): a wake-up is never acknowledged unparked.
    let (status, ok) = match &outcome.write_fault {
        None => (200, true),
        Some(_) => (503, false),
    };
    let mut resp = json_response(
        serde_json::json!({ "ok": ok, "filed": outcome.filed, "skipped": outcome.skipped, "dropped": dropped, "announce": if ok { "detached" } else { "none" }, "parked": outcome.parked, "parkFull": outcome.park_full, "retryable": !ok })
            .to_string(),
        status,
    )?;
    resp.headers_mut()
        .set("Server-Timing", &crate::pot_hook::hook_timing(ms))?;
    Ok(resp)
}

/// E649 L4 H1: `POST /internal/pot-announce-drain` (bearer `INTERNAL_TOKEN`): ONE drain pass of the pot hook's
/// parked wake-ups, the function the scheduled tick runs (`pot_hook::drain_parked`), answered with its tally. For an
/// operator, and for the D1 budget tier, which measures the pass's statements.
pub(crate) async fn internal_pot_announce_drain(
    req: Request,
    env: &worker::Env,
) -> Result<Response> {
    if !crate::internal_events::internal_bearer_ok(&req, env) {
        return Response::error("unauthorized", 401);
    }
    let deadline = worker_deadline(
        crate::pot_hook::DRAIN_WALL_MS,
        crate::pot_hook::DRAIN_SETTLE_RESERVE_MS,
    );
    let t = drain_pot_announce(env, &deadline).await?;
    json_response(
        serde_json::json!({ "ok": true, "rounds": t.rounds, "due": t.due, "drained": t.drained, "skipped": t.skipped, "retryFailed": t.retry_failed, "exhausted": t.exhausted, "aged": t.aged, "swept": t.swept, "faults": t.faults })
            .to_string(),
        200,
    )
}

/// E649 L4 N7: the worker's [`crate::pot_hook::Deadline`]: `budget_ms` from now on `Date::now`, timed by
/// `worker::Delay` (one timer per await, for the time left).
pub(crate) fn worker_deadline(budget_ms: u64, reserve_ms: u64) -> crate::pot_hook::Deadline {
    crate::pot_hook::Deadline::new(
        budget_ms,
        reserve_ms,
        Box::new(|| worker::Date::now().as_millis()),
        Box::new(|ms| {
            Box::pin(async move {
                worker::Delay::from(core::time::Duration::from_millis(ms)).await;
            })
        }),
    )
}

/// One drain pass over the worker's D1 and relay, under the caller's one deadline (N4, N7).
pub(crate) async fn drain_pot_announce(
    env: &worker::Env,
    deadline: &crate::pot_hook::Deadline,
) -> Result<crate::pot_hook::DrainTally> {
    let store = crate::pot_hook::HookD1 {
        sql: D1Sql::of(env)?,
    };
    let relay = crate::pot_hook::WorkerRelay { env: env.clone() };
    Ok(crate::pot_hook::drain_parked(&store, &relay, deadline).await)
}

/// E649 L4: the hooks' SQL port over D1 (`pot_hook::HookD1` holds the statements): one `batch` is one
/// `counted_batch` round trip; the counters ride `courier::flush`.
pub(crate) struct D1Sql {
    pub(crate) db: worker::D1Database,
    pub(crate) env: worker::Env,
}

impl D1Sql {
    pub(crate) fn of(env: &worker::Env) -> Result<Self> {
        Ok(D1Sql {
            db: env.d1("OVERLAY_DB")?,
            env: env.clone(),
        })
    }
}

impl crate::pot_hook::SqlPort for D1Sql {
    async fn run_batch(
        &self,
        stmts: Vec<crate::pot_hook::Stmt>,
    ) -> std::result::Result<Vec<crate::pot_hook::StmtOut>, String> {
        use crate::pot_hook::Bind;
        if stmts.is_empty() {
            return Ok(Vec::new());
        }
        let mut prepared = Vec::with_capacity(stmts.len());
        for st in stmts {
            let binds: Vec<JsValue> = st
                .binds
                .iter()
                .map(|b| match b {
                    Bind::Text(t) => JsValue::from_str(t),
                    Bind::Int(n) => JsValue::from_f64(*n as f64),
                    Bind::Null => JsValue::NULL,
                })
                .collect();
            prepared.push(
                self.db
                    .prepare(st.sql)
                    .bind(&binds)
                    .map_err(|e| format!("bind: {e}"))?,
            );
        }
        let results = crate::d1_ledger::counted_batch(&self.db, prepared)
            .await
            .map_err(|e| e.to_string())?;
        results
            .into_iter()
            .map(|r| {
                let rows = r
                    .results::<serde_json::Value>()
                    .map_err(|e| e.to_string())?;
                let changes = r
                    .meta()
                    .ok()
                    .flatten()
                    .and_then(|m| m.changes)
                    .map(|n| n as u64);
                Ok(crate::pot_hook::StmtOut { rows, changes })
            })
            .collect()
    }
    async fn counters(&self, deltas: Vec<(String, u64)>) {
        crate::courier::flush(self.env.d1("OVERLAY_DB").ok(), deltas).await;
    }
    fn now_ms(&self) -> u64 {
        worker::Date::now().as_millis()
    }
    fn log(&self, line: &str) {
        worker::console_log!("{line}");
    }
}

/// The overlay has committed the source write and its dirty marks before sending this note.
/// Judge at most sixteen affected subjects before answering; remaining marks belong to the pass.
/// Each committed judge batch sends its own owed-changed push.
pub(crate) async fn internal_hop_changed(
    mut req: Request,
    env: &worker::Env,
    ctx: &worker::Context,
) -> Result<Response> {
    let t0 = worker::Date::now().as_millis();
    if !crate::internal_events::internal_bearer_ok(&req, env) {
        return Response::error("unauthorized", 401);
    }
    let raw = req.bytes().await?;
    let (identities, dropped) = crate::internal_events::parse_hop_changed_counted(&raw);
    if identities.is_empty() {
        return Response::error("body must be {\"identities\":[\"02…\"]}", 400);
    }
    // bsv-low #436 (lens fold): the overlay chunks its flush at `HOP_CHANGED_MAX`, so nothing is over the cap from
    // our own producer. A body that still carries more is never trimmed silently: logged, counted, and answered.
    if dropped > 0 {
        console_warn!(
            "[hop-changed] body carried {} identity(ies) past the cap of {}: {dropped} NOT marked stale and NOT re-derived (the producer must chunk)",
            identities.len() + dropped,
            crate::internal_events::HOP_CHANGED_MAX
        );
        let counter_db = env.d1("OVERLAY_DB").ok();
        ctx.wait_until(async move {
            crate::courier::flush(
                counter_db,
                vec![(
                    crate::internal_events::COUNTER_HOP_CHANGED_DROPPED.to_string(),
                    dropped as u64,
                )],
            )
            .await;
        });
    }
    // bsv-low #911 (D2-ACTOR-A): an identity route per identity for the `owed-events` consumer, never a judge here
    // (with no queue binding the bounded judgement runs as before).
    if crate::owed_events::configured(env).await {
        let pairs: Vec<(&str, String)> = identities
            .iter()
            .map(|id| (overlay_discovery::owed_event::ROUTE_IDENTITY, id.to_ascii_lowercase()))
            .collect();
        crate::owed_events::publish_pairs(env, &pairs).await;
        // D2-ACTOR-D (bsv-low #952): the published identity routes' actors called after the answer.
        crate::owed_events::schedule_direct(env, ctx);
    } else {
        let tip = chaintracks_present_height_env(env, "owed").await.ok();
        let judge = crate::owed_judge::D1Judge::new(env, tip, written_off_before_ms_env(env))?;
        match crate::owed_judge::drain_identities(&judge, &identities, "hop-changed").await {
            Ok(report) => for (_, error) in report.faults { console_warn!("[owed] hop judgement deferred: {error}"); },
            Err(error) => console_warn!("[owed] hop drain failed: {error}"),
        }
    }
    // FIX-935-FOLD: the judge's queued pushes, after the answer.
    crate::push_coalesce::flush_after(ctx, env);
    let ms = worker::Date::now().as_millis().saturating_sub(t0);
    worker::console_log!(
        "[hop-changed] answered in {ms} ms, bounded judgement ({} identities)",
        identities.len()
    );
    let mut resp = json_response(
        serde_json::json!({ "ok": true, "identities": identities, "dropped": dropped }).to_string(),
        200,
    )?;
    resp.headers_mut()
        .set("Server-Timing", &crate::pot_hook::hook_timing(ms))?;
    Ok(resp)
}

/// `GET /owed?identity=` serves the write-derived rows and their current words.
pub async fn owed(req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    let identity = match view_identity(&req, &ctx) {
        ViewIdentity::Identity(id) => id.to_ascii_lowercase(),
        ViewIdentity::Refuse(resp) => return resp,
    };
    if !crate::logic::valid_identity(&identity) {
        let body = crate::owed_read::Snapshot::default()
            .body(&identity, None, &Default::default())
            .map_err(worker::Error::RustError)?;
        return json_response(body.to_string(), 200);
    }
    let db = D1Sql::of(&ctx.env)?;
    // bsv-low #912 (Phase B, `OWED_ACTOR_READ`): the actor's rows, the words joined from D1 as below.
    let snapshot = if crate::owed_actor::flag_on(
        ctx.env
            .var(crate::owed_actor::READ_FLAG)
            .ok()
            .map(|v| v.to_string()),
    ) {
        match crate::owed_actor::object::read_snapshot(&ctx.env, &db, &identity).await {
            Ok(crate::owed_actor::object::ActorRead::Rows(snapshot)) => snapshot,
            // The D2-ACTOR-B lens's MEDIUM-2: the display copy in the owed wire's shape, every row display only.
            Ok(crate::owed_actor::object::ActorRead::Fallback(
                crate::owed_actor::Fallback::Display(body),
            )) => return json_response(body.to_string(), 200),
            // No copy: a 503 naming `pending` (the client keeps its last list), never `rows: []`.
            Ok(crate::owed_actor::object::ActorRead::Fallback(
                crate::owed_actor::Fallback::Unavailable(body),
            )) => {
                let mut resp = json_response(body.to_string(), 503)?;
                resp.headers_mut().set("Retry-After", "2")?;
                return Ok(resp);
            }
            Err(error) => {
                console_warn!("[owed] stored read failed: {error}");
                return json_error("database query failed", 503);
            }
        }
    } else {
        match crate::owed_read::read(&db, &identity).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                console_warn!("[owed] stored read failed: {error}");
                return json_error("database query failed", 503);
            }
        }
    };
    let tip = if snapshot.needs_tip() {
        crate::beef_guard::present_tip(&ctx.env).await
    } else {
        None
    };
    let stored_words = snapshot.word_rows().map_err(worker::Error::RustError)?;
    let words = crate::tracker_host::runtime::views(&ctx.env, &stored_words).await;
    let body = snapshot
        .body(&identity, tip, &words)
        .map_err(worker::Error::RustError)?;
    json_response(body.to_string(), 200)
}

async fn gather_result_entries(
    db: &worker::D1Database,
    identity_lc: &str,
    written_off_before_ms: Option<i64>,
    after: usize,
) -> std::result::Result<(Vec<crate::results::ResultEntry>, bool), String> {
    // #375: the SPINE filter — the claims/seat legs below are keyed to this
    // page's games/pots, so they inherit the write-off without a clause.
    let mut binds: Vec<JsValue> = vec![JsValue::from_str(identity_lc)];
    if let Some(ms) = written_off_before_ms {
        binds.push(era_bind(ms));
    }
    let stmt = db
        .prepare(crate::results::results_sql(written_off_before_ms, after))
        .bind(&binds)
        .map_err(|e| format!("results bind failed: {e}"))?;
    let mut rows: Vec<crate::results::ResultsRow> = match stmt
        .counted_all()
        .await
        .and_then(|r| r.results::<ResultsRowD1>())
    {
        Ok(rows) => rows.into_iter().map(ResultsRowD1::into_row).collect(),
        Err(e) => return Err(format!("potparty join query failed: {e}")),
    };
    // Truncation is decided by what the QUERY returned (the #399 gate-S2
    // lesson: never by downstream consumption): the window probes
    // RESULTS_MAX_ROWS + 1, so an extra row means MORE pots exist past this
    // page. The probe row is dropped BEFORE the claims/seat/hand legs so no
    // leg is keyed to a pot the page does not serve.
    let truncated = rows.len() > crate::results::RESULTS_MAX_ROWS;
    rows.truncate(crate::results::RESULTS_MAX_ROWS);

    gather_result_rows(db, identity_lc, rows, truncated).await
}

async fn gather_result_rows(
    db: &worker::D1Database,
    identity_lc: &str,
    rows: Vec<crate::results::ResultsRow>,
    truncated: bool,
) -> std::result::Result<(Vec<crate::results::ResultEntry>, bool), String> {
    let mut rows = rows;
    // #406 (2026-08-27) — the PAGE OVERLAY, page-batched. The nested
    // window query is left byte-identical (adding a column inside it
    // emptied the page — see the decision log); instead ONE PK-indexed
    // batched read over this page's ≤100 outpoints fills `settleSigners`
    // and (bsv-low P4 slice 2) the money facts, and each value keeps its
    // own serving bar in the fold (the verdict pointer guard for signers,
    // `spenderFactsTxid == spendingTxid` for the spender facts). BEST-
    // EFFORT: a fault leaves the fields null (the narration then simply
    // doesn't claim an ending kind; the receipt names the gap), never a 503.
    {
        let mut ops: Vec<(String, u32)> = rows
            .iter()
            .map(|r| (r.pot_txid.to_ascii_lowercase(), r.pot_vout))
            .collect();
        ops.sort_unstable();
        ops.dedup();
        let mut overlay: Vec<crate::results::PageOverlay> = Vec::with_capacity(ops.len());
        for chunk in ops.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
            let sql = crate::results::page_overlay_sql(chunk.len());
            let mut binds: Vec<JsValue> = Vec::with_capacity(chunk.len() * 2);
            for (t, v) in chunk {
                binds.push(JsValue::from_str(t));
                binds.push(JsValue::from_f64(f64::from(*v)));
            }
            match db.prepare(&sql).bind(&binds) {
                Ok(stmt) => match stmt
                    .counted_all()
                    .await
                    .and_then(|r| r.results::<crate::results::PageOverlay>())
                {
                    Ok(orows) => overlay.extend(orows),
                    Err(e) => console_warn!("[results] page overlay chunk failed: {e}"),
                },
                Err(e) => console_warn!("[results] page overlay bind failed: {e}"),
            }
        }
        crate::results::apply_page_overlay(&mut rows, &overlay);
    }

    // Claims (won/lost attribution) — BEST-EFFORT: a fault here only leaves
    // winner-verdict games `unresolved`, never a hard failure (the
    // chain-truth tie/refund outcomes and the verdict field still serve).
    let mut game_ids: Vec<String> = rows
        .iter()
        .map(|r| r.game_id.to_ascii_lowercase())
        .collect();
    game_ids.sort_unstable();
    game_ids.dedup();
    let mut claim_markers: Vec<ResultMarkerRow> = Vec::new();
    for chunk in game_ids.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
        let binds: Vec<JsValue> = chunk.iter().map(|g| JsValue::from_str(g)).collect();
        let stmt = match db
            .prepare(crate::results::claims_sql(chunk.len()))
            .bind(&binds)
        {
            Ok(s) => s,
            Err(e) => {
                console_warn!("[results] claims bind failed (chunk omitted): {e}");
                continue;
            }
        };
        match stmt
            .counted_all()
            .await
            .and_then(|r| r.results::<ResultRowD1>())
        {
            Ok(rows) => claim_markers.extend(rows.into_iter().filter_map(ResultRowD1::into_marker)),
            Err(e) => {
                console_warn!("[results] result_markers_v2 query failed (claims omitted): {e}");
            }
        }
    }
    let claims = crate::results::claims_by_game(&claim_markers);

    // #281 F1 — the SEAT PROOF comes from its OWN query, bound to each pot's
    // COMMITTED settle keys (read from the hash-verified funding lock), NOT
    // from the `results_sql` page. That page is now one row per pot, and no
    // ordering rule over it is safe: #252's backfill publishes honest v2
    // markers long after a pot txid became public, so a forged row can always
    // be OLDER. Binding the keys removes ordering from the argument entirely
    // — a forged key cannot enter the result set. BEST-EFFORT: a fault here
    // only leaves seat attribution absent (claims still decide), never a 5xx.
    //
    // The committed params are resolved ONCE here and threaded into BOTH
    // consumers. They used to be computed for the seat fetch and DISCARDED,
    // then re-derived per row inside `assemble_results` — a measured 2.0× CPU
    // on the legacy-BEEF leg of a route that takes `identity` unauthenticated
    // and whose row set attacker-writable dust markers populate (#314 class).
    let params_by_pot = crate::results::covenant_params_by_pot(&rows);
    let seat_markers = results_seat_markers(db, &params_by_pot).await;
    // The fund-time fallback: a seat whose end-of-hand potparty marker never
    // landed still published the same committed key at hop time. Best-effort —
    // an empty map is exactly the previous behaviour.
    let hop_markers = results_hop_seat_markers(db, &params_by_pot, &rows).await;
    // Brain-cutover M2: the published hand markers for this page's games —
    // the read the CLIENT used to make against `ls_hand` (plus an ECDSA per
    // row on its main thread, #401). BEST-EFFORT with the same discipline as
    // claims: a fault omits hands from the drill-down, never a 5xx and never
    // a money path (display-only index).
    let hand_facts = results_hand_markers(db, &game_ids).await;
    // bsv-low P1.1 part b: the winner's replayed proof bundle — both hands
    // re-derived at admission (or, for a row admitted before that shipped,
    // replayed here from its retained bytes, bounded). BEST-EFFORT, same
    // discipline as hand markers: a fault leaves the marker path in charge.
    let proof_hands = results_proof_hands(db, &game_ids).await;

    Ok((
        crate::results::assemble_results(
            identity_lc,
            rows,
            &claims,
            &seat_markers,
            &params_by_pot,
            &hop_markers,
            &hand_facts,
            &proof_hands,
        ),
        truncated,
    ))
}

/// `proof_markers` row as D1 returns it for the proof-hands fetch.
#[derive(Deserialize)]
struct ProofHandsRowD1 {
    #[serde(rename = "gameId")]
    game_id: String,
    winner: String,
    #[serde(rename = "bundleValid", default)]
    bundle_valid: Option<f64>,
    #[serde(rename = "winnerCardsHex", default)]
    winner_cards_hex: Option<String>,
    #[serde(rename = "loserCardsHex", default)]
    loser_cards_hex: Option<String>,
    #[serde(rename = "markerRowid")]
    marker_rowid: f64,
}

#[derive(Deserialize)]
struct ProofPostHandsRowD1 {
    #[serde(rename = "gameId")]
    game_id: String,
    winner: String,
    #[serde(rename = "winnerCardsHex", default)]
    winner_cards_hex: Option<String>,
    #[serde(rename = "loserCardsHex", default)]
    loser_cards_hex: Option<String>,
}

#[derive(Deserialize)]
struct ProofBytesRowD1 {
    #[serde(rename = "gameId")]
    game_id: String,
    winner: String,
    #[serde(rename = "bundleHex")]
    bundle_hex: String,
}

/// Replayed proof bundles for this page's games (bsv-low P1.1 part b): the
/// admission-time columns first; rows the overlay admitted before the replay
/// shipped (`bundleValid IS NULL`) are replayed HERE from their retained
/// bytes, at most `PROOF_REPLAYS_PER_REQUEST` per request. Refused rows are
/// never loaded (the SQL filters them).
async fn results_proof_hands(
    db: &worker::D1Database,
    game_ids: &[String],
) -> std::collections::HashMap<String, Vec<crate::results::ProofHandsFact>> {
    let mut out: std::collections::HashMap<String, Vec<crate::results::ProofHandsFact>> =
        std::collections::HashMap::new();
    if game_ids.is_empty() {
        return out;
    }
    let mut pending: Vec<f64> = Vec::new();
    for chunk in game_ids.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
        let binds: Vec<JsValue> = chunk.iter().map(|g| JsValue::from_str(g)).collect();
        let stmt = match db
            .prepare(crate::results::proof_hands_sql(chunk.len()))
            .bind(&binds)
        {
            Ok(s) => s,
            Err(e) => {
                console_warn!("[results] proof-hands bind failed (chunk omitted): {e}");
                continue;
            }
        };
        match stmt
            .counted_all()
            .await
            .and_then(|r| r.results::<ProofHandsRowD1>())
        {
            Ok(fetched) => {
                for r in fetched {
                    match (r.bundle_valid, r.winner_cards_hex) {
                        (Some(v), Some(wc)) if v != 0.0 => {
                            out.entry(r.game_id.to_ascii_lowercase()).or_default().push(
                                crate::results::ProofHandsFact {
                                    game_id: r.game_id.to_ascii_lowercase(),
                                    winner: r.winner.to_ascii_lowercase(),
                                    winner_cards_hex: wc.to_ascii_lowercase(),
                                    loser_cards_hex: r
                                        .loser_cards_hex
                                        .map(|c| c.to_ascii_lowercase()),
                                },
                            );
                        }
                        (None, _) if pending.len() < crate::results::PROOF_REPLAYS_PER_REQUEST => {
                            pending.push(r.marker_rowid);
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                // A racing pre-migration schema or any D1 fault: proof hands
                // are simply absent this pass (markers still serve).
                console_warn!("[results] proof-hands query failed (proof hands omitted): {e}");
            }
        }
    }
    // bsv-low P1.1 proof-in-DB: bundles POSTED to this app-layer (replayed on
    // write) serve exactly like on-chain ones. Same chunking, same shape.
    for chunk in game_ids.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
        let binds: Vec<JsValue> = chunk.iter().map(|g| JsValue::from_str(g)).collect();
        let stmt = match db
            .prepare(crate::proof_post::proof_posts_hands_sql(chunk.len()))
            .bind(&binds)
        {
            Ok(s) => s,
            Err(e) => {
                console_warn!("[results] proof-posts bind failed (chunk omitted): {e}");
                continue;
            }
        };
        match stmt
            .counted_all()
            .await
            .and_then(|r| r.results::<ProofPostHandsRowD1>())
        {
            Ok(rows) => {
                for r in rows {
                    let Some(wc) = r.winner_cards_hex else {
                        continue;
                    };
                    out.entry(r.game_id.to_ascii_lowercase()).or_default().push(
                        crate::results::ProofHandsFact {
                            game_id: r.game_id.to_ascii_lowercase(),
                            winner: r.winner.to_ascii_lowercase(),
                            winner_cards_hex: wc.to_ascii_lowercase(),
                            loser_cards_hex: r.loser_cards_hex.map(|c| c.to_ascii_lowercase()),
                        },
                    );
                }
            }
            Err(e) => {
                console_warn!("[results] proof-posts query failed (posted proofs omitted): {e}")
            }
        }
    }
    if !pending.is_empty() {
        let binds: Vec<JsValue> = pending.iter().map(|r| JsValue::from_f64(*r)).collect();
        match db
            .prepare(crate::results::proof_bundle_bytes_sql(pending.len()))
            .bind(&binds)
        {
            Ok(stmt) => match stmt
                .counted_all()
                .await
                .and_then(|r| r.results::<ProofBytesRowD1>())
            {
                Ok(rows) => {
                    for r in rows {
                        if let Some(f) =
                            crate::results::replay_proof_row(&r.game_id, &r.winner, &r.bundle_hex)
                        {
                            out.entry(f.game_id.clone()).or_default().push(f);
                        }
                    }
                }
                Err(e) => console_warn!(
                    "[results] proof-bytes query failed (read-time replay skipped): {e}"
                ),
            },
            Err(e) => {
                console_warn!("[results] proof-bytes bind failed (read-time replay skipped): {e}")
            }
        }
    }
    out
}

/// `hand_markers` row as D1 returns it (brain-cutover M2).
#[derive(Deserialize)]
struct HandMarkerRowD1 {
    #[serde(rename = "gameId")]
    game_id: String,
    identity: String,
    #[serde(rename = "potTxid")]
    pot_txid: String,
    #[serde(rename = "cardsHex")]
    cards_hex: String,
    #[serde(rename = "sigHex", default)]
    sig_hex: Option<String>,
    /// The admission latch. `None` = a row the sweep has not reached — the
    /// resolver computes it rather than dropping the hand.
    #[serde(rename = "rowValid", default)]
    row_valid: Option<f64>,
}

/// Published hand markers for the games on this `/results` page, keyed by
/// gameId, windowed exactly as `ls_hand` windows them (the SHARED
/// `HAND_ROWS_PER_SEAT`). Verification happens in `resolve_marker_hands`, per
/// seat, against the caller's own row — this only fetches.
async fn results_hand_markers(
    db: &worker::D1Database,
    game_ids: &[String],
) -> std::collections::HashMap<String, Vec<crate::results::HandMarkerFact>> {
    let mut out: std::collections::HashMap<String, Vec<crate::results::HandMarkerFact>> =
        std::collections::HashMap::new();
    if game_ids.is_empty() {
        return out;
    }
    for chunk in game_ids.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
        let binds: Vec<JsValue> = chunk.iter().map(|g| JsValue::from_str(g)).collect();
        let stmt = match db
            .prepare(crate::results::hand_markers_sql(chunk.len()))
            .bind(&binds)
        {
            Ok(s) => s,
            Err(e) => {
                console_warn!("[results] hand-marker bind failed (chunk omitted): {e}");
                continue;
            }
        };
        match stmt
            .counted_all()
            .await
            .and_then(|r| r.results::<HandMarkerRowD1>())
        {
            Ok(fetched) => {
                for r in fetched {
                    out.entry(r.game_id.to_ascii_lowercase()).or_default().push(
                        crate::results::HandMarkerFact {
                            game_id: r.game_id.to_ascii_lowercase(),
                            identity: r.identity.to_ascii_lowercase(),
                            pot_txid: r.pot_txid.to_ascii_lowercase(),
                            cards_hex: r.cards_hex.to_ascii_lowercase(),
                            sig_hex: r.sig_hex,
                            row_valid: r.row_valid.map(|v| v != 0.0),
                        },
                    );
                }
            }
            Err(e) => {
                // A racing pre-migration schema (no rowValid column yet) or
                // any D1 fault: hands are simply absent this pass.
                console_warn!("[results] hand-marker query failed (hands omitted): {e}");
            }
        }
    }
    out
}

/// HOP seat markers for the pots on this `/results` page, fetched under each
/// pot's OWN committed lock keys — the same F2 bar `results_seat_markers` uses,
/// so a row naming a key the pot never committed is filtered out IN SQL.
///
/// Exists because `/results` resolves a winner verdict through the CALLER's own
/// seat proof, and that proof lived only in the potparty marker published at the
/// END of a hand, which races teardown. See `fill_seats_from_hop_markers`.
async fn results_hop_seat_markers(
    db: &worker::D1Database,
    params_by_pot: &std::collections::HashMap<(String, u32), crate::results::CovenantParams>,
    source_rows: &[crate::results::ResultsRow],
) -> std::collections::HashMap<(String, u32), Vec<crate::results::HopSeatRow>> {
    let mut out: std::collections::HashMap<(String, u32), Vec<crate::results::HopSeatRow>> =
        std::collections::HashMap::new();
    if params_by_pot.is_empty() {
        return out;
    }
    let mut keys: Vec<&(String, u32)> = params_by_pot.keys().collect();
    keys.sort_unstable();
    let legacy: std::collections::HashSet<(String, u32)> = source_rows
        .iter()
        .filter(|r| r.pub_a.is_none() || r.pub_b.is_none())
        .map(|r| (r.pot_txid.to_ascii_lowercase(), r.pot_vout))
        .collect();
    for use_legacy in [false, true] {
        let group: Vec<_> = keys
            .iter()
            .copied()
            .filter(|k| legacy.contains(*k) == use_legacy)
            .collect();
        for chunk in group.chunks(crate::results::SEAT_MARKERS_CHUNK_POTS) {
            let sql = if use_legacy {
                crate::results::legacy_hop_seat_markers_sql(chunk.len())
            } else {
                crate::results::hop_seat_markers_sql(chunk.len())
            };
            let mut binds: Vec<JsValue> =
                Vec::with_capacity(chunk.len() * crate::results::HOP_SEAT_BINDS_PER_POT);
            for k in chunk {
                if use_legacy {
                    let p = &params_by_pot[*k];
                    binds.push(JsValue::from_str(&hex::encode(p.pub_a)));
                    binds.push(JsValue::from_str(&hex::encode(p.pub_b)));
                } else {
                    binds.push(JsValue::from_str(&k.0));
                    binds.push(JsValue::from_f64(f64::from(k.1)));
                }
            }
            let Ok(stmt) = db.prepare(&sql).bind(&binds) else {
                continue;
            };
            match stmt
                .counted_all()
                .await
                .and_then(|r| r.results::<HopSeatRowD1>())
            {
                Ok(rows) => {
                    let hops: Vec<crate::results::HopSeatRow> = rows
                        .into_iter()
                        .map(|r| crate::results::HopSeatRow {
                            identity: r.identity,
                            seat_settle_pubkey: r.seat_settle_pubkey,
                            marker_valid: r.marker_valid.map(|v| v != 0.0),
                            txid: r.txid,
                            hop_vout: r.hop_vout.map_or(0, |v| v as u32),
                            hop_sats: r.hop_sats.map(|v| v as u64),
                            size_bytes: r.size_bytes.map(|v| v as u64),
                            fee_sats: r.fee_sats.map(|v| v as u64),
                        })
                        .collect();
                    // Every pot in the chunk gets the chunk's rows; the committed-key
                    // match inside `fill_seats_from_hop_markers` decides which apply.
                    for k in chunk {
                        out.entry((*k).clone())
                            .or_default()
                            .extend(hops.iter().cloned());
                    }
                }
                Err(e) => {
                    console_warn!(
                        "[results] hop seat fallback query failed (binding unchanged): {e}"
                    );
                }
            }
        }
    }
    out
}

/// #230/#281 — the caller's v2 seat markers for the pots on this `/results`
/// page, fetched with [`crate::results::seat_markers_sql`]: each pot bound as
/// `(potTxid, potVout, pubA, pubB)` from its OWN committed lock, windowed per
/// `(pot, committed key)` slot. A row under any other key is filtered out IN
/// SQL, so junk is free to exist and irrelevant; `attribute_seats` then
/// requires BOTH signatures on whatever comes back.
///
/// BEST-EFFORT by construction: every fault path returns what it has, so a
/// D1 error degrades attribution to "absent" (claims still decide the
/// outcome) and never a 5xx.
async fn results_seat_markers(
    db: &worker::D1Database,
    params_by_pot: &std::collections::HashMap<(String, u32), crate::results::CovenantParams>,
) -> std::collections::HashMap<(String, u32), Vec<crate::results::SeatMarkerRow>> {
    let mut out: std::collections::HashMap<(String, u32), Vec<crate::results::SeatMarkerRow>> =
        std::collections::HashMap::new();
    if params_by_pot.is_empty() {
        return out;
    }
    // Chunking + bind construction live in `results::seat_marker_chunks` so
    // they are testable without a Worker (the re-gate's finding #3: this whole
    // delivery path could be deleted with no test failing).
    for chunk in crate::results::seat_marker_chunks(params_by_pot) {
        let sql =
            crate::results::seat_markers_sql(chunk.len(), crate::results::SEAT_MARKERS_PER_KEY);
        let mut binds: Vec<JsValue> =
            Vec::with_capacity(chunk.len() * crate::results::SEAT_MARKERS_BINDS_PER_POT);
        for b in &chunk {
            binds.push(JsValue::from_str(&b.pot_txid));
            binds.push(JsValue::from_f64(f64::from(b.pot_vout)));
            binds.push(JsValue::from_str(&b.pub_a_hex));
            binds.push(JsValue::from_str(&b.pub_b_hex));
        }
        let stmt = match db.prepare(sql).bind(&binds) {
            Ok(s) => s,
            Err(e) => {
                // CONTINUE, never return: one chunk failing to bind must not
                // abandon the seat proofs of every REMAINING chunk (re-gate
                // finding #6).
                console_warn!("[results] seat-marker bind failed (chunk omitted): {e}");
                continue;
            }
        };
        match stmt
            .counted_all()
            .await
            .and_then(|r| r.results::<SeatMarkerRowD1>())
        {
            Ok(fetched) => note_seat_marker_rows(&mut out, fetched),
            Err(e) => {
                // A racing pre-migration schema (no seatSettlePubkey column
                // yet) or any D1 fault: attribution is simply omitted.
                console_warn!("[results] seat-marker query failed (attribution partial): {e}");
            }
        }
    }
    out
}

/// The verified seat-marker rows of a fetched page into the per-pot map (shared by the per-chunk read and the hooks'
/// batched one).
pub(crate) fn note_seat_marker_rows(
    out: &mut std::collections::HashMap<(String, u32), Vec<crate::results::SeatMarkerRow>>,
    fetched: Vec<SeatMarkerRowD1>,
) {
    for r in fetched {
        let (Some(pk), Some(seat_sig), Some(id_sig)) =
            (r.seat_settle_pubkey, r.seat_sig_hex, r.sig_hex)
        else {
            continue;
        };
        out.entry((r.pot_txid.to_ascii_lowercase(), r.pot_vout as u32))
            .or_default()
            .push(crate::results::SeatMarkerRow {
                identity: r.identity.to_ascii_lowercase(),
                opponent_identity: r.opponent_identity.to_ascii_lowercase(),
                game_id: r.game_id.to_ascii_lowercase(),
                pot_txid: r.pot_txid.to_ascii_lowercase(),
                pot_vout: r.pot_vout as u32,
                recovery_height: r.recovery_height as u32,
                seat_settle_pubkey: pk.to_ascii_lowercase(),
                seat_sig_hex: seat_sig.to_ascii_lowercase(),
                identity_sig_hex: id_sig.to_ascii_lowercase(),
                sig_valid: r.sig_valid.map(|v| v != 0.0),
            });
    }
}

// ── /refund-view — per-identity refund status (bsv-low #252 stage 2a) ───────

/// `/refund-view` joined row as D1 returns it (the `refund_view_sql` shape):
/// the caller's potparty facts + the pot's spend/verdict columns + the
/// `potrefund_records` presence bit. Pot-side fields are `Option` because the
/// join can MISS (NULL columns).
///
/// DEPLOY ORDER: the #284 columns come from the OVERLAY worker's additive
/// migrations, so the overlay deploys (and runs its migrations) BEFORE this
/// worker — the same ordering `results_sql` requires. `serde(default)` does
/// NOT buy pre-migration tolerance here: `refund_view_sql` names those
/// columns explicitly, so against a pre-migration schema the whole query
/// faults and the route answers 503 for everyone — the honest fail-safe (a
/// fault is never shaped like an answer). The defaults only cover NULL/
/// absent VALUES on a migrated schema; no default can manufacture a fact.
#[derive(Deserialize)]
pub(crate) struct RefundViewRowD1 {
    #[serde(rename = "gameId")]
    game_id: String,
    #[serde(rename = "potTxid")]
    pot_txid: String,
    #[serde(rename = "potVout")]
    pot_vout: f64,
    #[serde(rename = "recoveryHeight")]
    recovery_height: f64,
    #[serde(rename = "covRecoveryHeight", default)]
    cov_recovery_height: Option<f64>,
    spent: Option<f64>,
    #[serde(rename = "spendingTxid")]
    spending_txid: Option<String>,
    #[serde(rename = "spentConfirmed", default)]
    spent_confirmed: Option<f64>,
    #[serde(rename = "verdict", default)]
    verdict: Option<String>,
    #[serde(rename = "verdictTxid", default)]
    verdict_txid: Option<String>,
    #[serde(rename = "spentHeight", default)]
    spent_height: Option<f64>,
    #[serde(rename = "backupMarkerPresent")]
    backup_marker_present: f64,
    /// #323 MEDIUM-1 — `pot_beefs.proof_verified` for the recorded spender;
    /// the second accepted confirmation signal, shared with `/results`.
    #[serde(rename = "spenderProofVerified")]
    spender_proof_verified: Option<f64>,
    /// #371 third-arm inputs; `default` tolerates a read racing the additive
    /// migration — absent = no third arm (fail-safe to the merkle bar).
    #[serde(rename = "spenderSeen", default)]
    spender_seen: Option<f64>,
    #[serde(rename = "spenderFinal", default)]
    spender_final: Option<f64>,
    /// #217 durable timeline stamps (unix seconds). All three are nullable
    /// and `default` — a join miss, or a row admitted before the
    /// `firstSpentAt` migration, reads as absent rather than as a zero time.
    #[serde(rename = "potAdmittedAt", default)]
    pot_admitted_at: Option<f64>,
    #[serde(rename = "firstPartyMarkerAt", default)]
    first_party_marker_at: Option<f64>,
    #[serde(rename = "firstSpentAt", default)]
    first_spent_at: Option<f64>,
}

impl RefundViewRowD1 {
    pub(crate) fn into_row(self) -> crate::refund_view::RefundViewRow {
        crate::refund_view::RefundViewRow {
            game_id: self.game_id,
            pot_txid: self.pot_txid,
            pot_vout: self.pot_vout as u32,
            marker_recovery_height: self.recovery_height as u32,
            spender_proof_verified: self.spender_proof_verified.map(|v| v != 0.0),
            spender_seen: self.spender_seen.map(|v| v != 0.0),
            spender_final: self.spender_final.map(|v| v != 0.0),
            cov_recovery_height: self.cov_recovery_height.map(|v| v as u64),
            spent: self.spent.map(|v| v != 0.0),
            spending_txid: self.spending_txid,
            spent_confirmed: self.spent_confirmed.map(|v| v != 0.0),
            verdict: self.verdict,
            verdict_txid: self.verdict_txid,
            spent_height: self.spent_height.map(|v| v as u64),
            backup_marker_present: self.backup_marker_present != 0.0,
            // #217 — NULL stays None. A stamp is a unix SECOND, so the f64
            // D1 hands back is exact well past any plausible clock.
            pot_admitted_at: self.pot_admitted_at.map(|v| v as i64),
            first_party_marker_at: self.first_party_marker_at.map(|v| v as i64),
            first_spent_at: self.first_spent_at.map(|v| v as i64),
        }
    }
}

/// `GET /refund-view?identity=<66-hex>` — the per-identity REFUND STATUS
/// view (bsv-low #252 stage 2a): for every pot the identity is a party to,
/// the height-gate math + refund-backup presence + the chain-truth status of
/// the pot's exit (`armed`/`gate-open`/`landed`/`superseded`/`unknown`),
/// with the `/results`-style honesty pair (`status`/`statusSource` — never
/// asserting what the data doesn't prove). Full derivation table + trust
/// model: `refund_view.rs` module docs. DISPLAY-ONLY: serves backup PRESENCE
/// only, never `refundRawHex`.
///
/// Fail-safe shape mirrors `/recovery-view`/`/results`: a missing/invalid
/// identity is an EMPTY 200 result (never an error); a D1 fault is a 503; a
/// chaintracks fault only degrades `tip` (and thus the gate fields) to
/// `null` — the D1 facts still serve. ONE bounded D1 query
/// ([`crate::refund_view::REFUND_VIEW_MAX_ROWS`] pots, one identity bind, no
/// BLOBs).
pub async fn refund_view(req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    // #318: identity comes from the ONE auth seam (session identity wins;
    // mismatch refuses; anonymous lenient = the legacy query-param claim).
    let identity_lc = match view_identity(&req, &ctx) {
        ViewIdentity::Identity(id) => id,
        ViewIdentity::Refuse(resp) => return resp,
    };

    // The paging cursor (2026-08-21; the #398 contract): absent/garbage
    // `after` is 0 — the unchanged first page — clamped at the ceiling.
    let after = req
        .url()
        .ok()
        .and_then(|u| {
            u.query_pairs()
                .find(|(k, _)| k == "after")
                .and_then(|(_, v)| v.parse::<usize>().ok())
        })
        .unwrap_or(0)
        .min(crate::refund_view::REFUND_VIEW_AFTER_MAX);

    if !crate::logic::valid_identity(&identity_lc) {
        return json_response(
            crate::refund_view::refund_view_body(&identity_lc, None, &[], false, after),
            200,
        );
    }

    let db = match ctx.env.d1("OVERLAY_DB") {
        Ok(db) => db,
        Err(e) => {
            console_warn!("[refund-view] OVERLAY_DB binding unavailable: {e}");
            return json_error("database unavailable", 503);
        }
    };

    // #375: the era cutoff rides as the LAST bind iff configured.
    let era = written_off_before_ms(&ctx);
    let mut binds: Vec<JsValue> = vec![JsValue::from_str(&identity_lc)];
    if let Some(ms) = era {
        binds.push(era_bind(ms));
    }
    let stmt = db
        .prepare(crate::refund_view::refund_view_sql(era, after))
        .bind(&binds)?;
    let mut rows: Vec<crate::refund_view::RefundViewRow> = match stmt
        .counted_all()
        .await
        .and_then(|r| r.results::<RefundViewRowD1>())
    {
        Ok(rows) => rows.into_iter().map(RefundViewRowD1::into_row).collect(),
        Err(e) => {
            console_warn!("[refund-view] potparty join query failed: {e}");
            return json_error("database query failed", 503);
        }
    };
    // Truncation is decided by what the QUERY returned (the #399 gate-S2
    // lesson): the window probes MAX + 1; the probe row is dropped before
    // assembly.
    let truncated = rows.len() > crate::refund_view::REFUND_VIEW_MAX_ROWS;
    rows.truncate(crate::refund_view::REFUND_VIEW_MAX_ROWS);

    // The tip AFTER the D1 facts (the gate math needs it; `null` on a fault
    // — gate fields degrade to null/false, statuses stay fail-safe).
    let tip = chaintracks_present_height(&ctx, "refund-view").await.ok();
    let entries = crate::refund_view::assemble_refund_view(rows, tip);
    json_response(
        crate::refund_view::refund_view_body(&identity_lc, tip, &entries, truncated, after),
        200,
    )
}

/// `GET /refund-backups?identity=<66-hex>` — the per-identity REFUND-BACKUP
/// BYTES view (bsv-low W2 client batch B(a); owner ruling 2026-09-03: batched
/// decoded reads live on the app-layer). For every pot the identity is a
/// party to, the indexed `potrefund_records` rows WITH `refundRawHex`, newest
/// first, ≤ `REFUND_BACKUPS_ROWS_PER_POT` per pot, ≤ `REFUND_BACKUPS_MAX_ROWS`
/// in all (`truncated` when the cap was hit). The reader verifies every raw
/// exactly as before (module docs: `refund_backups`). Fail-safe shape mirrors
/// `/refund-view`: an invalid identity is an EMPTY 200; a D1 fault is a 503.
pub async fn refund_backups(req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    let identity_lc = match view_identity(&req, &ctx) {
        ViewIdentity::Identity(id) => id,
        ViewIdentity::Refuse(resp) => return resp,
    };
    if !crate::logic::valid_identity(&identity_lc) {
        return json_response(
            crate::refund_backups::refund_backups_body(&identity_lc, &[], false),
            200,
        );
    }
    let db = match ctx.env.d1("OVERLAY_DB") {
        Ok(db) => db,
        Err(e) => {
            console_warn!("[refund-backups] OVERLAY_DB binding unavailable: {e}");
            return json_error("database unavailable", 503);
        }
    };
    let era = written_off_before_ms(&ctx);
    let mut binds: Vec<JsValue> = vec![JsValue::from_str(&identity_lc)];
    if let Some(ms) = era {
        binds.push(era_bind(ms));
    }
    let stmt = db
        .prepare(crate::refund_backups::refund_backups_sql(era))
        .bind(&binds)?;
    let mut rows: Vec<crate::refund_backups::RefundBackupRow> = match stmt
        .counted_all()
        .await
        .and_then(|r| r.results::<crate::refund_backups::RefundBackupRowD1>())
    {
        Ok(rows) => rows.into_iter().map(|r| r.into_row()).collect(),
        Err(e) => {
            console_warn!("[refund-backups] query failed: {e}");
            return json_error("database query failed", 503);
        }
    };
    let truncated = rows.len() > crate::refund_backups::REFUND_BACKUPS_MAX_ROWS;
    rows.truncate(crate::refund_backups::REFUND_BACKUPS_MAX_ROWS);
    let mut backups = crate::refund_backups::assemble_refund_backups(rows);
    // bsv-low #848: an exploded (handfile) row carries its batch; a chain row never (its bytes unchanged).
    crate::handfile::attach_refund_batches(&db, &mut backups).await;
    json_response(
        crate::refund_backups::refund_backups_body(&identity_lc, &backups, truncated),
        200,
    )
}

// ── /hops-view — per-identity hops-in-flight view (bsv-low #315, stage 2b) ──

/// `/hops-view` joined row as D1 returns it (the `hops_view_sql` shape):
/// the caller's hopparty marker fields + the hop outpoint's `pot_records`
/// spend columns + the spender's verified-proof latch + the ADMITTED hop
/// lock script hex. Pot-side fields are `Option` because both LEFT joins
/// can MISS (NULL columns — fail-safe: never asserted unspent, script
/// absence answers `markerVerified: unknown`).
///
/// DEPLOY ORDER: `hopparty_records` comes from the OVERLAY worker's
/// additive migrations, so the overlay deploys (and runs its migrations)
/// BEFORE this worker — the `refund_view` ordering. Against a
/// pre-migration schema the whole query faults and the route answers 503
/// for everyone (a fault is never shaped like an answer).
#[derive(Deserialize)]
pub(crate) struct HopsViewRowD1 {
    identity: String,
    #[serde(rename = "gameId")]
    game_id: String,
    #[serde(rename = "hopTxid")]
    hop_txid: String,
    #[serde(rename = "hopVout")]
    hop_vout: f64,
    #[serde(rename = "hopSats")]
    hop_sats: f64,
    #[serde(rename = "opponentIdentity")]
    opponent_identity: String,
    #[serde(rename = "seatSettlePubkey")]
    seat_settle_pubkey: String,
    #[serde(rename = "seatSigHex")]
    seat_sig_hex: String,
    #[serde(rename = "identitySigHex")]
    identity_sig_hex: String,
    #[serde(rename = "markerTxid")]
    marker_txid: String,
    #[serde(rename = "markerVout")]
    marker_vout: f64,
    /// The CONTAINER's decoded facts (#310 decode-at-write in the overlay).
    #[serde(rename = "hopLockHex")]
    hop_lock_hex: Option<String>,
    #[serde(rename = "hopSatsOnChain")]
    hop_sats_on_chain: Option<f64>,
    #[serde(rename = "containerOutputs")]
    container_outputs: f64,
    /// The #362 latched verdict. `None` = the row predates the migration.
    #[serde(rename = "markerValid", default)]
    marker_valid: Option<f64>,
    spent: Option<f64>,
    #[serde(rename = "spendingTxid")]
    spending_txid: Option<String>,
    #[serde(rename = "spentConfirmed", default)]
    spent_confirmed: Option<f64>,
    #[serde(rename = "spenderProofVerified")]
    spender_proof_verified: Option<f64>,
    /// #371 third-arm inputs; `default` tolerates a read racing the additive
    /// migration — absent = no third arm (fail-safe to the merkle bar).
    #[serde(rename = "spenderSeen", default)]
    spender_seen: Option<f64>,
    #[serde(rename = "spenderFinal", default)]
    spender_final: Option<f64>,
    #[serde(rename = "markerCreatedAt", default)]
    marker_created_at: Option<f64>,
}

impl HopsViewRowD1 {
    pub(crate) fn into_row(self) -> crate::hops_view::HopsViewRow {
        crate::hops_view::HopsViewRow {
            identity: self.identity,
            game_id: self.game_id,
            hop_txid: self.hop_txid,
            hop_vout: self.hop_vout as u32,
            hop_sats: self.hop_sats as u64,
            opponent_identity: self.opponent_identity,
            seat_settle_pubkey: self.seat_settle_pubkey,
            seat_sig_hex: self.seat_sig_hex,
            identity_sig_hex: self.identity_sig_hex,
            marker_txid: self.marker_txid,
            marker_vout: self.marker_vout as u32,
            spent: self.spent.map(|v| v != 0.0),
            spending_txid: self.spending_txid,
            spent_confirmed: self.spent_confirmed.map(|v| v != 0.0),
            spender_proof_verified: self.spender_proof_verified.map(|v| v != 0.0),
            spender_seen: self.spender_seen.map(|v| v != 0.0),
            spender_final: self.spender_final.map(|v| v != 0.0),
            // Stored as a typed column by the overlay's decode-at-write; an
            // impossible empty string reads as absent (which REFUTES).
            hop_lock_hex: self.hop_lock_hex.filter(|s| !s.is_empty()),
            hop_sats_on_chain: self.hop_sats_on_chain.map(|v| v as u64),
            container_outputs: self.container_outputs as u32,
            // NULL stays NULL — "never evaluated" is a distinct answer from
            // "refuted", and collapsing it here would relabel every legacy
            // row as a refutation (#362).
            marker_valid: self.marker_valid.map(|v| v != 0.0),
            // bsv-low #469 (the gate's HIGH-3): `hopparty_records.createdAt` is unix SECONDS (every marker table
            // here is); the entry carries MILLISECONDS, the unit the owed list's age rule compares.
            marker_created_at: self
                .marker_created_at
                .map(|v| (v as i64).saturating_mul(1000)),
        }
    }
}

/// `GET /hops-view?identity=<66-hex>` — the per-identity HOPS-IN-FLIGHT
/// view (bsv-low #315, #252 stage 2b): every hop the identity has marked
/// (`hopparty_records`), joined to the `tm_lowfund`-indexed hop outpoint
/// for spent/unspent status (honesty pair; an un-indexed hop is `unknown`,
/// never asserted-unspent) and labeled with `markerVerified` — the verdict
/// the OVERLAY latched at admission (seatSig + identitySig + the container's
/// own hop lock AND value). **This route runs no cryptography** (bsv-low
/// #362): it reads `hopparty_records.markerValid` and sorts by it. Labels
/// are for display; rows are never dropped. Full trust model:
/// `hops_view.rs` module docs.
///
/// Optional `&gameId=<64-hex>` scopes the window to one game. It narrows
/// the flood surface — a flood naming OTHER games is escaped completely —
/// but it is **NOT a general escape hatch, and must not be described as
/// one**: the gameId is one of the nine pushes in the victim's own
/// on-chain marker, the same object that reveals the identity, so naming
/// it costs an attacker nothing. Measured: a flood on the SAME gameId
/// leaves the honest row ABSENT with `truncated: true` under the scoped
/// query too. A truncated caller therefore still has no guaranteed way to
/// reach its own row against a TARGETED flood — that gap is the tracked
/// residual documented at `hops_view::assemble_hops_view`, closing via
/// #318 (per-identity auth + quota).
///
/// Fail-safe shape mirrors `/refund-view`: a missing/invalid identity is
/// an EMPTY 200 result (never an error); an invalid `gameId` is ignored
/// (unscoped), never an error; a D1 fault is a 503; a chaintracks fault
/// only degrades `tip` to `null` — the D1 facts still serve. ONE bounded
/// D1 query (≤[`crate::hops_view::HOPS_VIEW_MAX_OUTPOINTS`] hop outpoints
/// ×[`crate::hops_view::HOPS_VIEW_ROWS_PER_OUTPOINT`] rows, no BEEF
/// blobs).
pub async fn hops_view(req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    // #318: identity comes from the ONE auth seam (session identity wins;
    // mismatch refuses; anonymous lenient = the legacy query-param claim).
    let identity_lc = match view_identity(&req, &ctx) {
        ViewIdentity::Identity(id) => id,
        ViewIdentity::Refuse(resp) => return resp,
    };
    // `url` is still needed below for the `?gameId=` escape hatch.
    let url = req.url()?;

    if !crate::logic::valid_identity(&identity_lc) {
        return json_response(
            crate::hops_view::hops_view_body(&identity_lc, None, &[], false, 0),
            200,
        );
    }

    let db = match ctx.env.d1("OVERLAY_DB") {
        Ok(db) => db,
        Err(e) => {
            console_warn!("[hops-view] OVERLAY_DB binding unavailable: {e}");
            return json_error("database unavailable", 503);
        }
    };

    // `?gameId=` is the ESCAPE HATCH for a truncated page (gate HIGH-1):
    // a caller told `truncated` re-asks scoped to its own game and reaches
    // its row regardless of how many outpoints a flood minted. An invalid
    // gameId is IGNORED (treated as unscoped) rather than erroring — the
    // fail-safe direction for a display surface.
    let game_id_lc = url
        .query_pairs()
        .find(|(k, _)| k == "gameId")
        .map(|(_, v)| v.into_owned().to_ascii_lowercase())
        .filter(|g| g.len() == 64 && g.bytes().all(|b| b.is_ascii_hexdigit()));

    // #398: the rank-window cursor. `after` slides the finalRank window so a
    // >cap identity can WALK to its remaining outpoints (the /alerts-trim
    // resolution applied here); clamped so a hostile value cannot format an
    // absurd literal. Non-numeric/absent ⇒ 0 (page 1, byte-identical query).
    let after: usize = url
        .query_pairs()
        .find(|(k, _)| k == "after")
        .and_then(|(_, v)| v.parse::<usize>().ok())
        .map(|a| a.min(crate::hops_view::HOPS_VIEW_AFTER_MAX))
        .unwrap_or(0);

    // #375: the era cutoff is always the LAST numbered bind (`?2` unscoped,
    // `?3` scoped) iff configured.
    let era = written_off_before_ms(&ctx);
    let mut binds = match &game_id_lc {
        Some(g) => vec![JsValue::from_str(&identity_lc), JsValue::from_str(g)],
        None => vec![JsValue::from_str(&identity_lc)],
    };
    if let Some(ms) = era {
        binds.push(era_bind(ms));
    }
    let stmt = db
        .prepare(crate::hops_view::hops_view_sql(
            game_id_lc.is_some(),
            era,
            after,
        ))
        .bind(&binds)?;
    let rows: Vec<crate::hops_view::HopsViewRow> = match stmt
        .counted_all()
        .await
        .and_then(|r| r.results::<HopsViewRowD1>())
    {
        Ok(rows) => rows.into_iter().map(HopsViewRowD1::into_row).collect(),
        Err(e) => {
            console_warn!("[hops-view] hopparty join query failed: {e}");
            return json_error("database query failed", 503);
        }
    };

    let (mut entries, truncated) = crate::hops_view::assemble_hops_view(rows);
    // bsv-low #848: an exploded (handfile) marker carries its batch; a chain marker never (its bytes unchanged).
    crate::handfile::attach_hop_batches(&db, &mut entries).await;
    // B2.1 (bsv-low, 2026-09-02): the index's `unspent` is a non-observation;
    // re-check a bounded number of them against the chain rung (the same
    // corroborated WoC + Bitails read `/spent-any` serves, cached) so a hop
    // swept outside our overlay is never served as recoverable.
    // bsv-low #451 slice C (iv) — THE OWNER'S RULE (2026-09-18): the view serves the INDEX'S word, labelled `index`,
    // and never asks a courier. The 2026-09-02 belt re-checked every index-unspent hop against the chain on every
    // call so a hop the player's own wallet swept outside LOW is never served as recoverable; measured on the budget
    // hand it was 109–183 courier calls per two hands, the whole hot path's hog. The press-time read
    // (`hopSpenderRead`: `/utxo-status`, then `/spent-any` for what the index does not know) corroborates before
    // any sweep, and a hop swept elsewhere fails at broadcast — nothing is released on the view's word. The D1 memo
    // (`hop_chain_probes`, migration 150) and its pure helpers stay for their pins; the route no longer probes.
    // The tip AFTER the D1 facts (`null` on a fault — facts still serve).
    let tip = chaintracks_present_height(&ctx, "hops-view").await.ok();
    json_response(
        crate::hops_view::hops_view_body(&identity_lc, tip, &entries, truncated, after),
        200,
    )
}

/// One `hop_chain_probes` row as D1 returns it (bsv-low #451 slice C; `/spent-any`'s durable memo since the second
/// gate's LOW-4).
#[derive(serde::Deserialize)]
struct ProbeMemoRow {
    outpoint: String,
    #[serde(rename = "probedAtMs")]
    probed_at_ms: i64,
    spent: i64,
    #[serde(rename = "spendingTxid")]
    spending_txid: Option<String>,
    #[serde(rename = "spentConfirmed")]
    spent_confirmed: Option<i64>,
    /// bsv-low #808 (overlay migration 342); absent from a read that does not select it (the latch reads).
    #[serde(rename = "tipHeight", default)]
    tip_height: Option<i64>,
}

/// The memos for `targets`, read in statements of at most `hops_view::PROBE_MEMO_READ_CHUNK` binds (delta fold 2,
/// D-L5: D1 refuses more than 100 a statement). Fail-soft and ALL-OR-NOTHING: a bind/query fault in any statement
/// is an empty answer (every target asked; the owed walk's reorg tombstone rides this read and must never be the
/// only part lost). A fault is COUNTED (the merged lens's LOW-3, `probe_memo_read_faulted`): the empty answer costs
/// the pass its grace mark, its latches and its cursor.
async fn read_probe_memos(
    db: &worker::D1Database,
    targets: &[(String, u32)],
) -> Vec<crate::hops_view::ProbeMemo> {
    let mut rows: Vec<ProbeMemoRow> = Vec::new();
    for keys in crate::hops_view::probe_memo_read_chunks(targets) {
        let binds: Vec<JsValue> = keys.iter().map(|k| JsValue::from_str(k)).collect();
        match db
            .prepare(crate::hops_view::probe_memo_read_sql(keys.len()))
            .bind(&binds)
        {
            Ok(stmt) => match stmt
                .counted_all()
                .await
                .and_then(|r| r.results::<ProbeMemoRow>())
            {
                Ok(chunk) => rows.extend(chunk),
                Err(e) => {
                    console_warn!(
                        "[spent-any] probe memo read failed ({} targets): {e}",
                        targets.len()
                    );
                    probe_memo_read_faulted(db).await;
                    return Vec::new();
                }
            },
            Err(e) => {
                console_warn!(
                    "[spent-any] probe memo bind failed ({} targets): {e}",
                    targets.len()
                );
                probe_memo_read_faulted(db).await;
                return Vec::new();
            }
        }
    }
    rows.into_iter()
        .map(|r| crate::hops_view::ProbeMemo {
            outpoint: r.outpoint,
            probed_at_ms: r.probed_at_ms,
            spent: r.spent != 0,
            spending_txid: r.spending_txid,
            spent_confirmed: r.spent_confirmed.map(|v| v != 0),
            tip_height: r.tip_height.and_then(|t| u64::try_from(t).ok()),
        })
        .collect()
}

/// The merged lens's LOW-3: one faulted memo read, counted under its own name: on this isolate
/// (`/health.owed.probeMemoReadFaults`) and in the overlay's `ops_counters`
/// (`owed::COUNTER_PROBE_MEMO_READ_FAULTS`, on the overlay's `/health/invariants.counters`). One write per fault,
/// on the fault arm only; a counter write that fails too is logged and the isolate's count stands.
async fn probe_memo_read_faulted(db: &worker::D1Database) {
    crate::owed::note_probe_memo_read_fault();
    let binds = [
        JsValue::from_str(crate::owed::COUNTER_PROBE_MEMO_READ_FAULTS),
        JsValue::from_f64(1.0),
    ];
    match db.prepare(crate::beef_guard::BUMP_COUNTER_SQL).bind(&binds) {
        Ok(stmt) => {
            if let Err(e) = stmt.counted_run().await {
                console_warn!("[spent-any] probe memo read fault counter write failed: {e}");
            }
        }
        Err(e) => console_warn!("[spent-any] probe memo read fault counter bind failed: {e}"),
    }
}

/// The fresh memos, ONE batch of upserts. Fail-soft: a failure only forgets (logged).
async fn write_probe_memos(db: &worker::D1Database, memos: &[crate::hops_view::ProbeMemo]) {
    let mut stmts = Vec::with_capacity(memos.len());
    for m in memos {
        let binds = [
            JsValue::from_str(&m.outpoint),
            JsValue::from_f64(m.probed_at_ms as f64),
            JsValue::from_f64(if m.spent { 1.0 } else { 0.0 }),
            m.spending_txid
                .as_deref()
                .map(JsValue::from_str)
                .unwrap_or(JsValue::NULL),
            m.spent_confirmed
                .map(|v| JsValue::from_f64(if v { 1.0 } else { 0.0 }))
                .unwrap_or(JsValue::NULL),
            m.tip_height
                .map(|t| JsValue::from_f64(t as f64))
                .unwrap_or(JsValue::NULL),
        ];
        match db
            .prepare(crate::hops_view::PROBE_MEMO_UPSERT_SQL)
            .bind(&binds)
        {
            Ok(s) => stmts.push(s),
            Err(e) => console_warn!("[spent-any] probe memo bind failed for {}: {e}", m.outpoint),
        }
    }
    if stmts.is_empty() {
        return;
    }
    if let Err(e) = crate::d1_ledger::counted_batch(db, stmts).await {
        console_warn!(
            "[spent-any] probe memo write failed ({} rows): {e}",
            memos.len()
        );
    }
}

/// One `tx_any_verdicts` row as D1 returns it (bsv-low #451 slice C (iii)).
#[derive(serde::Deserialize)]
struct VerdictMemoRow {
    txid: String,
    #[serde(rename = "verdictAtMs")]
    verdict_at_ms: i64,
    #[serde(rename = "inputOutpoint")]
    input_outpoint: String,
    #[serde(rename = "spenderTxid")]
    spender_txid: String,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    evidence: Option<String>,
}

/// The memo that answers for `txid` at `now_ms`, if one does. Fail-soft: no binding / a query fault = none
/// (the couriers are asked, exactly as before).
async fn read_verdict_memo(
    env: &worker::Env,
    txid: &str,
    now_ms: i64,
) -> Option<crate::txany::VerdictMemo> {
    read_verdict_memos(env, std::slice::from_ref(&txid.to_string()), now_ms)
        .await
        .into_iter()
        .next()
}

/// bsv-low #451 slice C (v): the memos that ANSWER for `txids` at `now_ms` — ONE `IN` read (the batched `/tx-any`,
/// `/spent-any`'s outpoints). Fail-soft: a fault is an empty answer (every txid asked as before).
async fn read_verdict_memos(
    env: &worker::Env,
    txids: &[String],
    now_ms: i64,
) -> Vec<crate::txany::VerdictMemo> {
    if txids.is_empty() {
        return Vec::new();
    }
    let Ok(db) = env.d1("OVERLAY_DB") else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for chunk in txids.chunks(crate::txany::TX_ANY_BATCH_D1_CHUNK) {
        let binds: Vec<JsValue> = chunk
            .iter()
            .map(|t| JsValue::from_str(&t.to_ascii_lowercase()))
            .collect();
        let rows: Vec<VerdictMemoRow> = match db
            .prepare(crate::txany::verdict_memo_read_many_sql(chunk.len()))
            .bind(&binds)
        {
            Ok(stmt) => match stmt
                .counted_all()
                .await
                .and_then(|r| r.results::<VerdictMemoRow>())
            {
                Ok(rows) => rows,
                Err(e) => {
                    console_warn!(
                        "[tx-any] verdict memo read failed ({} txids): {e}",
                        chunk.len()
                    );
                    continue;
                }
            },
            Err(e) => {
                console_warn!(
                    "[tx-any] verdict memo bind failed ({} txids): {e}",
                    chunk.len()
                );
                continue;
            }
        };
        for r in rows {
            let Some(kind) = crate::txany::VerdictKind::parse(r.kind.as_deref()) else {
                continue;
            };
            let m = crate::txany::VerdictMemo {
                txid: r.txid.to_ascii_lowercase(),
                verdict_at_ms: r.verdict_at_ms,
                input_outpoint: r.input_outpoint,
                spender_txid: r.spender_txid,
                kind,
                evidence: r.evidence.unwrap_or_default(),
            };
            if crate::txany::verdict_memo_answers(
                &m,
                &m.txid.clone(),
                now_ms,
                crate::txany::VERDICT_MEMO_MAX_AGE_MS,
            ) {
                out.push(m);
            }
        }
    }
    out
}

// ── /live-view — per-identity live-hand view (bsv-low #252 stage 2a step 3) ─

/// `/live-view` joined row as D1 returns it (the `live_view_sql` shape): the
/// caller's potparty facts + the pot's spend columns. Pot-side fields are
/// `Option` because the join can MISS (NULL columns — an unindexed pot is
/// INCLUDED as possibly-live). Same deploy-order note as `RefundViewRowD1`:
/// the SQL names only pre-#284 columns plus `pot_records.recoveryHeight`
/// (a #284 additive), so against a pre-migration schema the whole query
/// faults → 503 for everyone (a fault is never shaped like an answer).
#[derive(Deserialize)]
struct LiveViewRowD1 {
    /// The marker row's own identity column (verification takes it from the
    /// ROW, not the query bind — the #230 F8 discipline). TEXT NOT NULL, but
    /// tolerated as Option: a NULL simply fails corroboration (fail-safe),
    /// never faults the page.
    #[serde(default)]
    identity: Option<String>,
    #[serde(rename = "gameId")]
    game_id: String,
    #[serde(rename = "potTxid")]
    pot_txid: String,
    #[serde(rename = "potVout")]
    pot_vout: f64,
    /// TEXT NOT NULL in the schema, but tolerated as Option — a NULL must
    /// degrade to `null` on the wire, never fault the whole page.
    #[serde(rename = "opponentIdentity", default)]
    opponent_identity: Option<String>,
    #[serde(rename = "recoveryHeight")]
    recovery_height: f64,
    #[serde(rename = "covRecoveryHeight", default)]
    cov_recovery_height: Option<f64>,
    /// The marker's identity-signature push — nullable in the schema.
    #[serde(rename = "sigHex", default)]
    sig_hex: Option<String>,
    /// v2 seat-binding columns of the REPRESENTATIVE marker — NULL on a v1
    /// marker, which is what production files first (so these are usually
    /// NULL; the pot's real v2 marker arrives through the candidate query).
    #[serde(rename = "seatSettlePubkey", default)]
    seat_settle_pubkey: Option<String>,
    #[serde(rename = "seatSigHex", default)]
    seat_sig_hex: Option<String>,
    /// The pot's DECODED committed settle keys (#284) — the free membership
    /// pre-filter + the keyed candidate query's binds. NULL for join-miss
    /// and bare/legacy pots.
    #[serde(rename = "covPubA", default)]
    cov_pub_a: Option<String>,
    #[serde(rename = "covPubB", default)]
    cov_pub_b: Option<String>,
    spent: Option<f64>,
    #[serde(rename = "spendingTxid")]
    spending_txid: Option<String>,
    #[serde(rename = "spentConfirmed", default)]
    spent_confirmed: Option<f64>,
}

impl LiveViewRowD1 {
    fn into_row(self) -> crate::live_view::LiveViewRow {
        crate::live_view::LiveViewRow {
            identity: self.identity.unwrap_or_default(),
            game_id: self.game_id,
            pot_txid: self.pot_txid,
            pot_vout: self.pot_vout as u32,
            opponent_identity: self.opponent_identity,
            marker_recovery_height: self.recovery_height as u32,
            cov_recovery_height: self.cov_recovery_height.map(|v| v as u64),
            identity_sig_hex: self.sig_hex,
            seat_settle_pubkey: self.seat_settle_pubkey,
            seat_sig_hex: self.seat_sig_hex,
            cov_pub_a: self.cov_pub_a,
            cov_pub_b: self.cov_pub_b,
            spent: self.spent.map(|v| v != 0.0),
            spending_txid: self.spending_txid,
            spent_confirmed: self.spent_confirmed.map(|v| v != 0.0),
        }
    }
}

/// Race a future against a soft wall-clock timeout — the tower's own #272
/// `with_timeout` idiom (`worker::Delay` is a `setTimeout` future). `Some`
/// if `fut` resolved first, `None` on timeout. Used to BOUND each tower
/// `/case` fetch so a wedged tower degrades `/live-view` by seconds (cases
/// null), never stalls it.
async fn with_timeout<F: core::future::Future>(fut: F, ms: u64) -> Option<F::Output> {
    use futures_util::future::Either;
    let delay = worker::Delay::from(core::time::Duration::from_millis(ms));
    futures_util::pin_mut!(fut, delay);
    match futures_util::future::select(fut, delay).await {
        Either::Left((out, _)) => Some(out),
        Either::Right(((), _)) => None,
    }
}

/// Fetch + shape ONE tower case through the `TOWER` service binding
/// (`GET /case/:gameId` — the tower front door's PUBLIC no-auth view; only
/// the path matters, the host is resolved by the binding), bounded by
/// [`crate::live_view::CASE_FETCH_TIMEOUT_MS`]. EVERY fault — transport,
/// timeout, non-200 (including the tower's 404 "no case"), oversized or
/// malformed body — is `None` (= `case: null` under a non-success
/// `caseSource`), never an error the route surfaces: the D1 half must serve
/// regardless (the `/results` claims-fault posture).
///
/// The body is bounded BEFORE it is buffered (MEDIUM-3): a `Content-Length`
/// already over [`crate::live_view::CASE_BODY_MAX_BYTES`] rejects without
/// reading a byte, and the read itself streams under a hard byte budget
/// ([`crate::live_view::push_bounded`]) — 8 concurrent multi-MB bodies from
/// a wedged/compromised tower can therefore never approach the Worker's
/// memory limit and take the always-serving D1 half down with them.
/// `parse_case_body`'s own length check stays as the belt (a body of
/// exactly the ceiling is still accepted, +1 rejected).
/// Build the tower's OUTPOINT-SCOPED case URL, or `None` if either id is
/// malformed (the caller then simply does not ask, and `apply_cases` tags the
/// row `tower-unavailable` — the honest "we should have asked and could not").
///
/// Extracted so the thing that actually changed on 2026-08-12 is unit-testable
/// without a Worker: the tower's game-level `/case/:gameId` 404s for every
/// outpoint-scoped case, so asking by name returned nothing for every row.
pub(crate) fn tower_case_url(game_id: &str, pot_txid: &str, pot_vout: u32) -> Option<String> {
    if !valid_txid(game_id) || !valid_txid(pot_txid) {
        return None;
    }
    Some(format!(
        "https://tower/case/{game_id}/{pot_txid}/{pot_vout}"
    ))
}

async fn tower_case_fetch(
    svc: &worker::Fetcher,
    game_id: &str,
    pot_txid: &str,
    pot_vout: u32,
) -> Option<crate::live_view::CaseView> {
    // Belt (LOW-8): this function interpolates both ids into a URL, so it
    // re-asserts their shape itself instead of trusting every caller to
    // have run `fanout_targets` first.
    if !valid_txid(game_id) {
        console_warn!("[live-view] tower case fetch refused: malformed gameId");
        return None;
    }
    if !valid_txid(pot_txid) {
        console_warn!("[live-view] tower case fetch refused: malformed pot txid");
        return None;
    }
    let url = tower_case_url(game_id, pot_txid, pot_vout)?;
    // (URL built by `tower_case_url` above.)
    // ASK BY OUTPOINT, not by name. Two reasons, one fatal and one structural:
    //
    // FATAL: since the co-signer DOs became outpoint-scoped, the tower's
    // game-level `GET /case/:gameId` 404s for every case opened — so this
    // fan-out asked a question the tower stopped answering and EVERY row's
    // case degraded to null. Proven 2026-08-12 against a real enforced settle:
    // `/case/:gameId` 404 while `/case/:gameId/:txid/:vout` served the full J.
    //
    // STRUCTURAL: a gameId is a claimable NAME (zanaadu invariant #2) and the
    // tower's by-name answer carried no pot outpoint, which is exactly why the
    // old tag could not vouch for the case<->pot binding. Asking with the
    // outpoint we are displaying makes the answer bound to THIS pot by
    // construction — the tag can finally vouch.

    let mut init = RequestInit::new();
    init.with_method(Method::Get);
    let headers = Headers::new();
    let _ = headers.set("Accept", "application/json");
    init.with_headers(headers);
    let fut = async {
        let mut resp = match svc.fetch(url.as_str(), Some(init)).await {
            Ok(resp) => resp,
            Err(e) => {
                console_warn!("[live-view] tower /case/{game_id} fetch failed: {e}");
                return None;
            }
        };
        let status = resp.status_code();
        // MEDIUM-3(a): a declared over-budget body is rejected pre-read.
        if let Ok(Some(cl)) = resp.headers().get("Content-Length") {
            if crate::live_view::content_length_over_budget(Some(&cl)) {
                console_warn!(
                    "[live-view] tower /case/{game_id} Content-Length {cl} over budget (case omitted)"
                );
                return None;
            }
        }
        // MEDIUM-3(b): stream the body under a hard byte budget — never
        // `text().await` (which buffers the ENTIRE body first). Every
        // DECISION lives in `live_view::BodyAccumulator` (unit-tested
        // through `read_case_body`); this loop is only the stream. Aborting
        // mid-read drops the ReadableStream, so nothing past the ceiling is
        // ever held in memory.
        let mut stream = match resp.stream() {
            Ok(s) => s,
            Err(e) => {
                console_warn!("[live-view] tower /case/{game_id} body stream failed: {e}");
                return None;
            }
        };
        let mut acc = crate::live_view::BodyAccumulator::new(crate::live_view::CASE_BODY_MAX_BYTES);
        {
            use futures_util::StreamExt;
            loop {
                match stream.next().await {
                    Some(Ok(chunk)) => {
                        if !acc.push(&chunk) {
                            console_warn!(
                                "[live-view] tower /case/{game_id} body exceeded {} bytes (case omitted)",
                                crate::live_view::CASE_BODY_MAX_BYTES
                            );
                            return None;
                        }
                    }
                    Some(Err(e)) => {
                        console_warn!("[live-view] tower /case/{game_id} body read failed: {e}");
                        return None;
                    }
                    None => break,
                }
            }
        }
        acc.finish(status)
    };
    match with_timeout(fut, crate::live_view::CASE_FETCH_TIMEOUT_MS).await {
        Some(shaped) => shaped,
        None => {
            console_warn!(
                "[live-view] tower /case/{game_id} timed out after {}ms (case omitted)",
                crate::live_view::CASE_FETCH_TIMEOUT_MS
            );
            None
        }
    }
}

/// The `/live-view` CANDIDATE fetch (HIGH-A): the caller's v2 seat-binding
/// markers for the page's pots, so corroboration examines the POT's markers
/// instead of the window representative — which production guarantees is the
/// V1 row (v1 is published before v2, `createdAt` is second-granular, and
/// the representative is the oldest). Two bounded queries, both best-effort:
///
/// - pots with DECODED committed keys → [`crate::results::seat_markers_sql`]
///   verbatim (the proven `/results` query: per-`(pot, key)` slot window,
///   membership enforced IN SQL, chunked at
///   [`crate::results::SEAT_MARKERS_CHUNK_POTS`]);
/// - pots without them (join miss / bare lock) →
///   [`crate::live_view::keyless_candidates_sql`] (per-outpoint window,
///   identity-bound), chunked at
///   [`crate::live_view::LIVE_VIEW_CANDIDATE_CHUNK_POTS`].
///
/// BEST-EFFORT by construction (the `/results` posture): a bind or query
/// fault CONTINUES to the next chunk and returns what it has, so a D1 error
/// never 5xxs and never empties the row list. It DOES set the returned
/// `faulted` flag, so the rows are labeled `corroboration-unavailable`
/// ("we could not look") instead of `marker-unverified` ("nothing verified")
/// — R2-2: a fault must never be reported as a property of the data.
/// Chunking + plan construction live in
/// [`crate::live_view::candidate_plan`] so the whole delivery path is
/// testable without a Worker.
async fn live_view_candidates(
    db: &worker::D1Database,
    identity_lc: &str,
    rows: &[crate::live_view::LiveViewRow],
) -> (
    std::collections::HashMap<(String, u32), Vec<crate::results::SeatMarkerRow>>,
    bool,
) {
    let mut out: std::collections::HashMap<(String, u32), Vec<crate::results::SeatMarkerRow>> =
        std::collections::HashMap::new();
    let mut faulted = false;
    let plan = crate::live_view::candidate_plan(rows);

    let collect = |fetched: Vec<SeatMarkerRowD1>,
                   out: &mut std::collections::HashMap<
        (String, u32),
        Vec<crate::results::SeatMarkerRow>,
    >| {
        for r in fetched {
            let (Some(pk), Some(seat_sig), Some(id_sig)) =
                (r.seat_settle_pubkey, r.seat_sig_hex, r.sig_hex)
            else {
                continue; // a v1 row can never corroborate
            };
            out.entry((r.pot_txid.to_ascii_lowercase(), r.pot_vout as u32))
                .or_default()
                .push(crate::results::SeatMarkerRow {
                    identity: r.identity.to_ascii_lowercase(),
                    opponent_identity: r.opponent_identity.to_ascii_lowercase(),
                    game_id: r.game_id.to_ascii_lowercase(),
                    pot_txid: r.pot_txid.to_ascii_lowercase(),
                    pot_vout: r.pot_vout as u32,
                    recovery_height: r.recovery_height as u32,
                    seat_settle_pubkey: pk.to_ascii_lowercase(),
                    seat_sig_hex: seat_sig.to_ascii_lowercase(),
                    identity_sig_hex: id_sig.to_ascii_lowercase(),
                    sig_valid: r.sig_valid.map(|v| v != 0.0),
                });
        }
    };

    for chunk in &plan.keyed {
        let sql =
            crate::results::seat_markers_sql(chunk.len(), crate::results::SEAT_MARKERS_PER_KEY);
        let mut binds: Vec<JsValue> =
            Vec::with_capacity(chunk.len() * crate::results::SEAT_MARKERS_BINDS_PER_POT);
        for b in chunk {
            binds.push(JsValue::from_str(&b.pot_txid));
            binds.push(JsValue::from_f64(f64::from(b.pot_vout)));
            binds.push(JsValue::from_str(&b.pub_a_hex));
            binds.push(JsValue::from_str(&b.pub_b_hex));
        }
        let stmt = match db.prepare(sql).bind(&binds) {
            Ok(s) => s,
            Err(e) => {
                console_warn!("[live-view] keyed candidate bind failed (chunk omitted): {e}");
                faulted = true;
                continue;
            }
        };
        match stmt
            .counted_all()
            .await
            .and_then(|r| r.results::<SeatMarkerRowD1>())
        {
            Ok(fetched) => collect(fetched, &mut out),
            Err(e) => {
                console_warn!("[live-view] keyed candidate query failed (partial): {e}");
                faulted = true;
            }
        }
    }

    for chunk in &plan.keyless {
        let sql = crate::live_view::keyless_candidates_sql(chunk.len());
        let mut binds: Vec<JsValue> =
            Vec::with_capacity(crate::live_view::keyless_chunk_binds(chunk.len()));
        binds.push(JsValue::from_str(identity_lc));
        for (txid, vout) in chunk {
            binds.push(JsValue::from_str(txid));
            binds.push(JsValue::from_f64(f64::from(*vout)));
        }
        let stmt = match db.prepare(sql).bind(&binds) {
            Ok(s) => s,
            Err(e) => {
                console_warn!("[live-view] keyless candidate bind failed (chunk omitted): {e}");
                faulted = true;
                continue;
            }
        };
        match stmt
            .counted_all()
            .await
            .and_then(|r| r.results::<SeatMarkerRowD1>())
        {
            Ok(fetched) => collect(fetched, &mut out),
            Err(e) => {
                console_warn!("[live-view] keyless candidate query failed (partial): {e}");
                faulted = true;
            }
        }
    }
    (out, faulted)
}

/// `GET /live-view?identity=<66-hex>` — the per-identity LIVE-HAND view
/// (bsv-low #252 stage 2a step 3): every pot the identity is a party to with
/// NO confirmed spend (unspent, unconfirmed-spend, or never-indexed — a
/// confirmed spend is `/refund-view`/`/results`' story), with the
/// `/refund-view` gate math, per-POT marker CORROBORATION (a SECOND bounded
/// candidate query — the pot's v2 markers, not the window representative
/// which production guarantees is the v1 row; `markerSource` /
/// `opponentIdentitySource`), and a BOUNDED tower-case fan-out: at most
/// [`crate::live_view::LIVE_VIEW_CASE_FANOUT_CAP`] corroborated gameIds
/// (quality-selected — known pots first, never raw window position) get the
/// tower's public `GET /case/:gameId` (shaped + validated subset). Success
/// is tagged `caseSource: "tower-by-gameid-unverified"` with the fetched
/// `caseGameId` served alongside — the tower's answer for that gameId, NOT
/// a verified case↔pot binding; ANY fault/timeout/cap keeps `case: null`
/// under an honest non-success tag — unknown, never "no case". Full honesty
/// model: `live_view.rs` module docs.
///
/// Fail-safe shape mirrors `/refund-view`: a missing/invalid identity is an
/// EMPTY 200 result; a D1 fault is a 503 (a fault is NEVER an empty "no
/// live hands" answer); a chaintracks fault only degrades `tip`/gate
/// fields; a tower fault only degrades `case`. EVERY exit — including URL
/// and bind faults — goes through `json_response` (wildcard CORS +
/// `no-store`; a default worker error response would carry neither). ONE
/// bounded D1 query ([`crate::live_view::LIVE_VIEW_MAX_ROWS`] pots, one
/// identity bind, no BLOBs) + at most
/// [`crate::live_view::LIVE_VIEW_CASE_FANOUT_CAP`] concurrent bounded
/// subrequests, run CONCURRENTLY with the (also bounded) chaintracks tip
/// hop — the added latency is `max(tip, cases)`, not `tip + cases`.
pub async fn live_view(req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    // #318: identity comes from the ONE auth seam (session identity wins;
    // mismatch refuses; anonymous lenient = the legacy query-param claim).
    // LOW-7 holds: the seam maps a URL fault through `json_error`, never `?`.
    let identity_lc = match view_identity(&req, &ctx) {
        ViewIdentity::Identity(id) => id,
        ViewIdentity::Refuse(resp) => return resp,
    };

    // The paging cursor (2026-08-21; the #398 contract): absent/garbage
    // `after` is 0 — the unchanged first page — clamped at the ceiling.
    let after = req
        .url()
        .ok()
        .and_then(|u| {
            u.query_pairs()
                .find(|(k, _)| k == "after")
                .and_then(|(_, v)| v.parse::<usize>().ok())
        })
        .unwrap_or(0)
        .min(crate::live_view::LIVE_VIEW_AFTER_MAX);

    if !crate::logic::valid_identity(&identity_lc) {
        return json_response(
            crate::live_view::live_view_body(&identity_lc, None, &[], false, after),
            200,
        );
    }

    let db = match ctx.env.d1("OVERLAY_DB") {
        Ok(db) => db,
        Err(e) => {
            console_warn!("[live-view] OVERLAY_DB binding unavailable: {e}");
            return json_error("database unavailable", 503);
        }
    };

    // LOW-7: same rule for the bind fault — through json_error, never `?`.
    // #375: the era cutoff rides as the LAST bind iff configured.
    let era = written_off_before_ms(&ctx);
    let mut binds: Vec<JsValue> = vec![JsValue::from_str(&identity_lc)];
    if let Some(ms) = era {
        binds.push(era_bind(ms));
    }
    let stmt = match db
        .prepare(crate::live_view::live_view_sql(era, after))
        .bind(&binds)
    {
        Ok(stmt) => stmt,
        Err(e) => {
            console_warn!("[live-view] statement bind failed: {e}");
            return json_error("database query failed", 503);
        }
    };
    let mut rows: Vec<crate::live_view::LiveViewRow> = match stmt
        .counted_all()
        .await
        .and_then(|r| r.results::<LiveViewRowD1>())
    {
        Ok(rows) => rows.into_iter().map(LiveViewRowD1::into_row).collect(),
        Err(e) => {
            console_warn!("[live-view] potparty join query failed: {e}");
            return json_error("database query failed", 503);
        }
    };
    // Truncation decided by what the QUERY returned (the #399 gate-S2
    // lesson): the window probes MAX + 1; drop the probe row before any
    // downstream leg (the case fan-out must not chase a pot this page does
    // not serve).
    let truncated = rows.len() > crate::live_view::LIVE_VIEW_MAX_ROWS;
    rows.truncate(crate::live_view::LIVE_VIEW_MAX_ROWS);

    // LOW-6: the tip hop is BOUNDED (same with_timeout idiom as the case
    // fetches — an untimed await here would let a wedged chaintracks stall
    // the whole route) and runs CONCURRENTLY with the candidate query +
    // fan-out. Either fault degrades its own fields only.
    let tip_fut = async {
        match with_timeout(
            chaintracks_present_height(&ctx, "live-view"),
            crate::live_view::TIP_FETCH_TIMEOUT_MS,
        )
        .await
        {
            Some(res) => res.ok(), // inner faults already logged + mapped
            None => {
                console_warn!(
                    "[live-view] chaintracks tip timed out after {}ms (tip null)",
                    crate::live_view::TIP_FETCH_TIMEOUT_MS
                );
                None
            }
        }
    };
    // HIGH-A: corroborate each POT from its OWN v2 candidate markers (the
    // second bounded query), then choose fan-out targets by QUALITY from the
    // corroborated pots — known pots first, never raw window position.
    let cases_fut = async {
        let (candidates, faulted) = live_view_candidates(&db, &identity_lc, &rows).await;
        let mut corr = crate::live_view::corroborate_rows(&identity_lc, &rows, &candidates);
        // R2-2: a candidate-query fault is "we could not look", labeled
        // distinctly — never reported as a property of the marker data.
        corr.unavailable = faulted;
        let targets = crate::live_view::fanout_targets(&rows, &corr.claims);
        if targets.is_empty() {
            return (corr, Vec::new(), std::collections::HashMap::new());
        }
        match ctx.env.service("TOWER") {
            Ok(svc) => {
                let svc_ref = &svc;
                // `run_fanout` returns the EFFECTIVE list, so apply_cases can
                // only ever label targets that were really asked (LOW-F).
                // gameId -> the pot outpoint we are DISPLAYING for it. Built
                // from `rows` (pot_records), so the outpoint is server-held
                // truth, never a caller claim.
                let outpoints: std::collections::HashMap<String, (String, u32)> = rows
                    .iter()
                    .map(|r| {
                        (
                            r.game_id.to_ascii_lowercase(),
                            (r.pot_txid.clone(), r.pot_vout),
                        )
                    })
                    .collect();
                let outpoints_ref = &outpoints;
                let (effective, fetched) =
                    crate::live_view::run_fanout(&targets, move |g: String| async move {
                        // No outpoint for this gameId ⇒ do not ask. The row is
                        // then tagged `tower-unavailable` by `apply_cases`,
                        // which is the honest answer: we should have asked and
                        // could not.
                        let (txid, vout) = outpoints_ref.get(&g)?;
                        tower_case_fetch(svc_ref, &g, txid, *vout).await
                    })
                    .await;
                (corr, effective, fetched)
            }
            Err(e) => {
                // The SELECTED targets are then tagged "tower-unavailable"
                // (empty fetched map): we should have asked and could not —
                // honest unknown, never "no case".
                console_warn!("[live-view] TOWER binding unavailable (cases omitted): {e}");
                (corr, targets, std::collections::HashMap::new())
            }
        }
    };
    let (tip, (corr, targets, fetched)) = futures_util::future::join(tip_fut, cases_fut).await;

    let mut entries = crate::live_view::assemble_live_view(rows, &corr, tip);
    // Unconditional — apply_cases stamps EVERY row's provenance tag, even
    // when nothing was targeted or fetched.
    crate::live_view::apply_cases(&mut entries, &targets, &fetched);

    json_response(
        crate::live_view::live_view_body(&identity_lc, tip, &entries, truncated, after),
        200,
    )
}

// ── /spent-any — server-side legacy outpoint reads (bsv-low #227 addendum) ──

/// One cached `/spent-any` row: the decision fields, without the echo key.
#[derive(Clone)]
struct SpentAnyCached {
    known: bool,
    spent: Option<bool>,
    spending_txid: Option<String>,
    spent_confirmed: Option<bool>,
    /// #323 defect 2 — WHY the answer is `known:false`, cached alongside it
    /// so a fault stays legible for the whole TTL instead of decaying into
    /// an unexplained negative on the next read.
    reason: Option<&'static str>,
}

thread_local! {
    /// In-isolate `/spent-any` cache (outpoint key → (expiry ms, row)).
    /// Deliberately NOT the Cache API (owner call, 2026-07-14: it misbehaves
    /// on workers.dev) — a plain in-memory map with a short TTL bounds
    /// upstream pressure exactly as well for this surface. Isolate recycling
    /// simply empties it (harmless).
    static SPENT_ANY_CACHE: std::cell::RefCell<std::collections::HashMap<String, (f64, SpentAnyCached)>> =
        std::cell::RefCell::new(std::collections::HashMap::new());
}

thread_local! {
    /// 2026-09-04: the WoC api key (`WOC_API_KEY` secret), set once per
    /// isolate by [`set_woc_api_key`] at request entry; every WoC read here
    /// sends it as `woc-api-key` (lifts WoC's free-tier rate limit — the
    /// repo's most-documented outage class). Absent = the free tier, as before.
    static WOC_API_KEY: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

/// Install the WoC api key for this isolate (idempotent; `None` clears).
pub fn set_woc_api_key(key: Option<String>) {
    WOC_API_KEY.with(|k| *k.borrow_mut() = key.filter(|k| !k.trim().is_empty()));
}

fn woc_api_key() -> Option<String> {
    WOC_API_KEY.with(|k| k.borrow().clone())
}

thread_local! {
    /// bsv-low #731: the COURIER SINK (`COURIER_SINK`), set per isolate at every entry (the fetch, the scheduled
    /// tick, the board actor's fetch and alarm), like the WoC key.
    static COURIER_SINK: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}

/// PURE (bsv-low #731): the sink a `COURIER_SINK` value names, accepted ONLY as a plain-http LOOPBACK origin
/// (`http://127.0.0.1:<port>` or `http://localhost:<port>`, no path). Anything else is no sink: the var can blind
/// this worker's couriers (every read a transport fault, "could not look", the fail-safe direction), never send
/// them to another host.
pub fn courier_sink_of(value: Option<&str>) -> Option<String> {
    let v = value?.trim().trim_end_matches('/');
    let rest = v.strip_prefix("http://")?;
    let (host, port) = rest.split_once(':')?;
    if !matches!(host, "127.0.0.1" | "localhost")
        || port.is_empty()
        || !port.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    Some(v.to_string())
}

/// Install the courier sink for this isolate (idempotent; an unset or refused value clears it).
pub fn set_courier_sink(value: Option<String>) {
    COURIER_SINK.with(|s| *s.borrow_mut() = courier_sink_of(value.as_deref()));
}

/// PURE (bsv-low #731): where a courier read goes. With no sink, the URL itself; with one, the sink's origin and the
/// URL's own path and query, so the D1 budget tier's couriers are a closed loopback port (every ask the same
/// transport fault, run after run) and never mainnet. The census still names the ORIGINAL URL's provider.
pub fn courier_target(url: &str, sink: Option<&str>) -> String {
    let Some(sink) = sink else {
        return url.to_string();
    };
    let after_scheme = url.split_once("://").map_or(url, |(_, r)| r);
    let path = after_scheme.find('/').map_or("/", |i| &after_scheme[i..]);
    format!("{sink}{path}")
}

#[cfg(test)]
mod courier_sink_731 {
    use super::{courier_sink_of, courier_target};

    #[test]
    fn only_a_loopback_http_origin_is_a_sink() {
        assert_eq!(
            courier_sink_of(Some("http://127.0.0.1:9")).as_deref(),
            Some("http://127.0.0.1:9")
        );
        assert_eq!(
            courier_sink_of(Some(" http://localhost:4000/ ")).as_deref(),
            Some("http://localhost:4000")
        );
        for refused in [
            "",
            "https://127.0.0.1:9",
            "http://127.0.0.1",
            "http://127.0.0.1:9/path",
            "http://127.0.0.1:x9",
            "http://api.whatsonchain.com:80",
            "http://127.0.0.1.attacker.example:80",
            "http://10.0.0.1:80",
        ] {
            assert_eq!(
                courier_sink_of(Some(refused)),
                None,
                "{refused:?} must not be a sink"
            );
        }
        assert_eq!(courier_sink_of(None), None);
    }

    #[test]
    fn a_sink_keeps_the_path_and_query_and_no_sink_is_the_url() {
        let url = "https://api.whatsonchain.com/v1/bsv/main/tx/ab/0/spent?x=1";
        assert_eq!(courier_target(url, None), url);
        assert_eq!(
            courier_target(url, Some("http://127.0.0.1:9")),
            "http://127.0.0.1:9/v1/bsv/main/tx/ab/0/spent?x=1"
        );
        assert_eq!(
            courier_target("https://bananablocks.com", Some("http://127.0.0.1:9")),
            "http://127.0.0.1:9/"
        );
    }
}

/// bsv-low #451 slice B: the Arcade endpoint `/tx-any`'s first external witness asks (`ARCADE_URL`; the overlay's
/// default when unset). Set per isolate at request entry, like the WoC key.
pub const DEFAULT_ARCADE_URL: &str = "https://arcade-v2-us-1.bsvblockchain.tech";
thread_local! {
    static ARCADE_URL: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
}
pub fn set_arcade_url(url: Option<String>) {
    ARCADE_URL.with(|u| {
        *u.borrow_mut() = url
            .map(|s| s.trim().trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty())
    });
}
pub(crate) fn arcade_url() -> String {
    ARCADE_URL.with(|u| {
        u.borrow()
            .clone()
            .unwrap_or_else(|| DEFAULT_ARCADE_URL.to_string())
    })
}

/// bsv-low #451 slice B: the ARCADE leg of `/tx-any` for a tx the index holds without a verified bump (the JOIN's
/// first minutes; a MINED push not yet landed): ONE read to our broadcaster answers presence (its FRESH `SEEN` word,
/// `txany::parse_arcade_word`) or confirmation — a MINED word is a CLAIM, and becomes `confirmed` only once its
/// merkle path computes, from THIS txid, the root the header chaintracks holds at that height (the gate's MEDIUM-1:
/// the client latches `confirmed` durably, and the index has refused Arcade's pushed bump before on the D7 class).
/// `None` sends the question to the couriers exactly as before. The verified height rides back for the answer.
/// STATED LATENCY (the delta-verify's LOW-D): a tx already mined that Arcade still shows SEEN answers
/// `confirmed:false` where WoC's `confirmations` would have said true, for the earlier of Arcade's MINED flip and
/// the 30-minute freshness window (re-asked every 15 s; never a false positive — the fail-safe direction).
async fn arcade_confirmation_look(
    txid_lc: &str,
    caller: &'static str,
    env: &worker::Env,
) -> Option<(crate::txany::TxObservation, Option<u64>)> {
    use crate::txany::{ArcadeWord, TxObservation};
    let url = format!("{}/tx/{txid_lc}", arcade_url());
    let (status, body) = provider_get(caller, &url).await?;
    let now_ms = worker::Date::now().as_millis() as i64;
    match crate::txany::parse_arcade_subject_word(status, &body, txid_lc, now_ms) {
        ArcadeWord::SeenFresh => Some((
            TxObservation::Present {
                confirmed: false,
                raw_hex: None,
            },
            None,
        )),
        ArcadeWord::Mined { bump_hex, height } => {
            let canonical = crate::beef_guard::canonical_root(env, height).await?;
            if !crate::txany::mined_proof_matches(txid_lc, &bump_hex, height, &canonical) {
                return None;
            }
            Some((
                TxObservation::Present {
                    confirmed: true,
                    raw_hex: None,
                },
                Some(height),
            ))
        }
        ArcadeWord::Unknown => None,
    }
}

/// Fetch a URL, returning `(status, body_bytes)`. Faults map to `None`. A
/// WoC URL carries the api key when one is installed. bsv-low #451 slice B:
/// every call is COUNTED and LOGGED with its caller (`crate::courier::note`)
/// and identifies this worker (Arcade's edge 403s bare agents — the
/// 2026-09-01 incident's lesson on the overlay).
pub(crate) async fn provider_get(caller: &'static str, url: &str) -> Option<(u16, Vec<u8>)> {
    provider_get_with(caller, url, None).await
}

/// `provider_get` with one extra request header (bsv-low #497: CoinMarketCap's key header on the rate ladder).
pub(crate) async fn provider_get_with(
    caller: &'static str,
    url: &str,
    extra_header: Option<(&str, &str)>,
) -> Option<(u16, Vec<u8>)> {
    let started = worker::Date::now().as_millis() as f64;
    let got = provider_get_raw(url, extra_header).await;
    let ms = worker::Date::now().as_millis() as f64 - started;
    crate::courier::note(url, caller, courier_answer(got.as_ref()), ms);
    got.map(|(status, body, _)| (status, body))
}

/// PURE (the W1-OBS-APPLAYER lens's L1): what the courier census is told of one answer, given the status, the body
/// and whether the body READ. A 2xx whose body did not read is a transport fault (`None`), never the body-shape fault
/// an empty body would be read as; any other answer is passed as it is (a non-2xx's outcome is its status).
pub(crate) fn courier_answer(got: Option<&(u16, Vec<u8>, bool)>) -> Option<(u16, &[u8])> {
    match got {
        Some((status, _, false)) if (200..300).contains(status) => None,
        Some((status, body, _)) => Some((*status, body.as_slice())),
        None => None,
    }
}

/// This worker's courier identity (see `provider_get`).
const APP_LAYER_USER_AGENT: &str = "low-app-layer/1.0 (+https://bsvarcade.com)";

/// The status, the body and whether the body read (a failed read answers an empty body, as before).
async fn provider_get_raw(
    url: &str,
    extra_header: Option<(&str, &str)>,
) -> Option<(u16, Vec<u8>, bool)> {
    let mut init = RequestInit::new();
    init.with_method(Method::Get);
    let headers = Headers::new();
    let _ = headers.set("User-Agent", APP_LAYER_USER_AGENT);
    if url.starts_with(WOC_BASE) {
        if let Some(key) = woc_api_key() {
            let _ = headers.set("woc-api-key", &key);
        }
    }
    if let Some((k, v)) = extra_header {
        let _ = headers.set(k, v);
    }
    init.with_headers(headers);
    let target = COURIER_SINK.with(|s| courier_target(url, s.borrow().as_deref()));
    let request = worker::Request::new_with_init(&target, &init).ok()?;
    let mut response = worker::Fetch::Request(request).send().await.ok()?;
    let status = response.status_code();
    let (body, body_read) = match response.bytes().await {
        Ok(b) => (b, true),
        Err(_) => (Vec::new(), false),
    };
    Some((status, body, body_read))
}

const WOC_BASE: &str = "https://api.whatsonchain.com/v1/bsv/main";
const BITAILS_BASE: &str = "https://api.bitails.io";
/// 2026-09-04: the negative corroborator. Bitails runs in pruned mode and
/// removed its per-output spend data (its `/output/{vout}/spent` answers 500
/// on every outpoint), so every "unspent" here had become an honest unknown.
/// BananaBlocks' `/txo/{txid}/{vout}/spend` answers `{"spent":false}` /
/// `{"spent":true,"spentTxid"}` (60 req/min; a 429 is a fault, never a verdict).
const BANANABLOCKS_BASE: &str = "https://bananablocks.com/api/v1";

/// Resolve ONE outpoint against the upstream providers, per the
/// proof-source-order doctrine (see `results.rs`'s `/spent-any` section):
/// positive = WoC pointer + raw hash/input verification (raw from WoC, then
/// Bitails); negative = requires clean Bitails corroboration; any fault =
/// honest unknown.
/// bsv-low #451 slice B: the ONE cache-consulting entry to the spent-any ladder. `/spent-any` always read through
/// the isolate cache; the hops view's chain probes and `/tx-any`'s unconfirmable probe called the ladder DIRECTLY —
/// up to eight courier ladders per `/hops-view` call, twelve to fifteen such calls per seat per hand on the budget
/// hand, none of them cached. One reader, one writer, one TTL (`SPENT_ANY_CACHE_TTL_MS`).
/// `max_age_ms`: how old a cached answer this reader accepts (`SPENT_ANY_CACHE_TTL_MS` for `/spent-any`,
/// `SPENT_ANY_PROBE_MAX_AGE_MS` for the probes); every entry is kept for the longest reader and stamped with its
/// write time, so a reader with a short need never sees an old row.
async fn spent_any_resolve_cached(
    txid_lc: &str,
    vout: u32,
    caller: &'static str,
    max_age_ms: f64,
) -> SpentAnyCached {
    let now = worker::Date::now().as_millis() as f64;
    let key = format!("{txid_lc}.{vout}");
    let cached = SPENT_ANY_CACHE.with(|c| {
        c.borrow()
            .get(&key)
            .filter(|(written_at, _)| {
                crate::results::spent_any_cache_fresh(*written_at, now, max_age_ms)
            })
            .map(|(_, row)| row.clone())
    });
    if let Some(row) = cached {
        return row;
    }
    let row = spent_any_resolve(txid_lc, vout, caller).await;
    SPENT_ANY_CACHE.with(|c| {
        let mut map = c.borrow_mut();
        // Prune entries no reader would accept any more, so the map stays bounded.
        map.retain(|_, (written_at, _)| {
            crate::results::spent_any_cache_fresh(
                *written_at,
                now,
                crate::results::SPENT_ANY_PROBE_MAX_AGE_MS,
            )
        });
        map.insert(key, (now, row.clone()));
    });
    row
}

async fn spent_any_resolve(txid_lc: &str, vout: u32, caller: &'static str) -> SpentAnyCached {
    let (row, _spender_raw) = spent_any_resolve_via(txid_lc, vout, |url: String| async move {
        provider_get(caller, &url).await
    })
    .await;
    row
}

/// The spent-any ladder over a courier GET port (bsv-low #606): the row, and the spender's raw when the ladder read
/// it AND it verified (`spender_raw_verifies`); `None` otherwise.
async fn spent_any_resolve_via<G, F>(
    txid_lc: &str,
    vout: u32,
    get: G,
) -> (SpentAnyCached, Option<Vec<u8>>)
where
    G: Fn(String) -> F,
    F: std::future::Future<Output = Option<(u16, Vec<u8>)>>,
{
    use crate::results::{
        parse_bananablocks_unspent, parse_woc_spent_body, spender_raw_verifies, SpentObservation,
        UnspentCorroboration,
    };

    // Owner ruling 2026-09-04 ("use all three, fallbacks being each other"):
    // the PRIMARY observation is the first provider that can LOOK — WoC,
    // then BananaBlocks, then Bitails' tx body — a fault falls through, a
    // real answer (spent OR not) stops the ladder. Every positive still
    // passes the same raw-verification bar below, whoever named it.
    let woc_spent = get(format!("{WOC_BASE}/tx/{txid_lc}/{vout}/spent")).await;
    let woc_spent_status = woc_spent.as_ref().map(|(s, _)| *s);
    let mut woc = match woc_spent {
        Some((200, body)) => match serde_json::from_slice::<serde_json::Value>(&body) {
            Ok(v) => parse_woc_spent_body(&v),
            Err(_) => SpentObservation::Fault,
        },
        // #323 HIGH-2 — status mapping is PURE and pinned (see
        // `results::woc_spent_status_observation`). This used to be
        // `(400..500) => NotSpent`, which swallowed 429 — this repo's
        // most-documented outage — as "unspent".
        Some((s, _)) => crate::results::woc_spent_status_observation(s),
        None => SpentObservation::Fault,
    };
    let mut primary = "woc";
    if matches!(woc, SpentObservation::Fault) {
        woc = match get(format!("{BANANABLOCKS_BASE}/txo/{txid_lc}/{vout}/spend")).await {
            Some((status, body)) => {
                let v = serde_json::from_slice::<serde_json::Value>(&body).ok();
                crate::results::parse_bananablocks_spent(status, v.as_ref())
            }
            None => SpentObservation::Fault,
        };
        primary = "bananablocks";
    }
    if matches!(woc, SpentObservation::Fault) {
        woc = match get(format!("{BITAILS_BASE}/tx/{txid_lc}")).await {
            Some((status, body)) => {
                let v = serde_json::from_slice::<serde_json::Value>(&body).ok();
                crate::results::parse_bitails_tx_spent(status, v.as_ref(), vout)
            }
            None => SpentObservation::Fault,
        };
        primary = "bitails_tx";
    }

    let mut spender_raw_ok = false;
    let mut verified_raw: Option<Vec<u8>> = None;
    let mut bitails = UnspentCorroboration::Unknown;
    match &woc {
        SpentObservation::Spent { txid: spender, .. } => {
            // Raw verification: WoC hex first, Bitails binary fallback. A
            // positive is served ONLY when the raw hashes to the reported
            // spender AND spends the requested outpoint.
            let raw = match get(format!("{WOC_BASE}/tx/{spender}/hex")).await {
                Some((200, body)) => std::str::from_utf8(&body)
                    .ok()
                    .and_then(|h| hex::decode(h.trim()).ok()),
                _ => None,
            };
            let raw = match raw {
                Some(r) => Some(r),
                None => match get(format!("{BITAILS_BASE}/download/tx/{spender}")).await {
                    Some((200, body)) if !body.is_empty() => Some(body),
                    _ => None,
                },
            };
            // 2026-09-04: the third raw source — BananaBlocks' WoC-compatible
            // `/tx/{txid}/hex` (probed live: the bytes hash to the spender).
            // `spender_raw_verifies` content-addresses whatever came back.
            let raw = match raw {
                Some(r) => Some(r),
                None => match get(format!("{BANANABLOCKS_BASE}/tx/{spender}/hex")).await {
                    Some((200, body)) => std::str::from_utf8(&body)
                        .ok()
                        .and_then(|h| hex::decode(h.trim()).ok()),
                    _ => None,
                },
            };
            if let Some(raw) = raw {
                spender_raw_ok = spender_raw_verifies(&raw, spender, txid_lc, vout);
                if spender_raw_ok {
                    verified_raw = Some(raw);
                }
            }
        }
        SpentObservation::NotSpent => {
            // Negative corroboration: a SECOND provider must cleanly say
            // unspent (never a single-provider negative). 2026-09-04: the
            // corroborators are the providers that did NOT give the primary
            // answer, in order — BananaBlocks, then Bitails' tx body, then
            // WoC — the first clean "unspent" corroborates; a contradiction
            // ("spent" where the primary said not) or a fault is an honest
            // unknown, never a verdict. Bitails retired its per-output
            // endpoint; its tx body reports an unspent output as `spent: ""`,
            // which is unknown here, never corroboration.
            // bsv-low #451 slice B: the healthy pair before pruned Bitails (`unspent_corroborator_order`, pinned).
            for corroborator in crate::results::unspent_corroborator_order(primary) {
                let got = match corroborator {
                    "bananablocks" => {
                        match get(format!("{BANANABLOCKS_BASE}/txo/{txid_lc}/{vout}/spend")).await {
                            Some((status, body)) => {
                                let v = serde_json::from_slice::<serde_json::Value>(&body).ok();
                                parse_bananablocks_unspent(status, v.as_ref())
                            }
                            None => UnspentCorroboration::Unknown,
                        }
                    }
                    "bitails_tx" => match get(format!("{BITAILS_BASE}/tx/{txid_lc}")).await {
                        Some((status, body)) => {
                            let v = serde_json::from_slice::<serde_json::Value>(&body).ok();
                            crate::results::bitails_tx_unspent(status, v.as_ref(), vout)
                        }
                        None => UnspentCorroboration::Unknown,
                    },
                    _ => match get(format!("{WOC_BASE}/tx/{txid_lc}/{vout}/spent")).await {
                        Some((s, _))
                            if matches!(
                                crate::results::woc_spent_status_observation(s),
                                SpentObservation::NotSpent
                            ) =>
                        {
                            UnspentCorroboration::ConfirmedUnspent
                        }
                        _ => UnspentCorroboration::Unknown,
                    },
                };
                if got == UnspentCorroboration::ConfirmedUnspent {
                    bitails = got;
                    break;
                }
            }
        }
        SpentObservation::Fault => {}
    }

    // bsv-low #695: every primary rung faulted after WoC's `/spent` named an unknown tx (its 400): ONE read of WoC's
    // node for the tx itself; its conclusive 404 (the subject is unmined) is `uncorroborated-unspent`, never
    // `provider-fault`. A 429, a 5xx, a transport or a body-shape fault of either read keeps `provider-fault`.
    let woc_node_absent = matches!(woc, SpentObservation::Fault)
        && crate::results::woc_spent_may_name_an_unknown_tx(woc_spent_status)
        && matches!(
            get(format!("{WOC_BASE}/tx/hash/{txid_lc}")).await,
            Some((404, _))
        );
    // bsv-low #723 / #868: a spend's confirmation word is the naming rung's own (`SpentObservation::Spent`): WoC's
    // `status`, BananaBlocks' `spendScope`; a rung that did not say (pruned Bitails, a BananaBlocks answer without the
    // scope) is `null`, never `false`
    let st = crate::results::sharpen_unmined_absence(
        crate::results::decide_spent_any(&woc, spender_raw_ok, bitails),
        woc_node_absent,
    );
    let row = SpentAnyCached {
        known: st.known,
        spent: st.spent,
        spending_txid: st.spending_txid,
        spent_confirmed: st.spent_confirmed,
        reason: st.reason,
    };
    // the raw rides only a row that names its spender (the decision served the positive the raw verified)
    let verified_raw = verified_raw.filter(|_| row.spending_txid.is_some());
    (row, verified_raw)
}

/// `GET /spent-any?outpoints=<txid>.<vout>,…` — spend status for ARBITRARY
/// outpoints (legacy escrows the overlay never indexed), answered by
/// SERVER-SIDE provider reads so the browser stops calling WhatsOnChain
/// directly (bsv-low #227 addendum). Same row shape as `/utxo-status`;
/// capped at [`crate::results::SPENT_ANY_MAX_OUTPOINTS`]; ~15 s in-isolate
/// cache. `known:false` is the honest answer for every provider fault or
/// un-corroborated negative — this surface never asserts what it cannot
/// verify (positives are raw-hash + input-match verified).
pub async fn spent_any(req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    let url = req.url()?;
    let Some(param) = url
        .query_pairs()
        .find(|(k, _)| k == "outpoints")
        .map(|(_, v)| v.into_owned())
    else {
        return json_error("missing outpoints query parameter", 400);
    };
    let outpoints = match parse_outpoints(&param) {
        Ok(ops) => ops,
        Err(msg) => return json_error(&msg, 400),
    };
    if outpoints.len() > crate::results::SPENT_ANY_MAX_OUTPOINTS {
        return json_error(
            &format!(
                "too many outpoints: {} (max {})",
                outpoints.len(),
                crate::results::SPENT_ANY_MAX_OUTPOINTS
            ),
            400,
        );
    }

    let mut entries: Vec<crate::logic::OutpointStatus> = Vec::with_capacity(outpoints.len());
    // bsv-low #451 slice C (v): an outpoint of a tx with a TERMINAL verdict memo (never confirmed: an input conflict,
    // a corroborated refusal or absence) has no spendable output — answered without a courier, `known:false` with
    // the reason (a landing proof never rests on it). One `IN` read for the request's txids.
    let verdicts = {
        let now_ms = worker::Date::now().as_millis() as i64;
        let txids: Vec<String> = outpoints.iter().map(|op| op.db_txid()).collect();
        read_verdict_memos(&ctx.env, &txids, now_ms).await
    };
    // the second gate's LOW-4 (2026-09-18): the D1 probe memo (`hop_chain_probes`, migration 150) is `/spent-any`'s
    // durable memo — a KNOWN answer younger than five minutes serves across isolates; a fresh answer is written
    // back in one batch (a fault never). The press-time read stays honest: a swept outpoint is `spent` for as long
    // as the memo says, and the sweep's broadcast is judged by the network either way.
    let now_ms = worker::Date::now().as_millis() as i64;
    let probe_targets: Vec<(String, u32)> = outpoints
        .iter()
        .filter(|op| !verdicts.iter().any(|m| m.txid == op.db_txid()))
        .map(|op| (op.db_txid(), op.vout))
        .collect();
    let memos = match ctx.env.d1("OVERLAY_DB") {
        Ok(db) => read_probe_memos(&db, &probe_targets).await,
        Err(_) => Vec::new(),
    };
    let (memo_answers, _) = crate::tracker_host::runtime::spent_any_memo_plan(
        &probe_targets,
        &memos,
        now_ms,
        &Default::default(),
    );
    let mut fresh: Vec<crate::hops_view::ProbeMemo> = Vec::new();
    let mut terminals = Vec::with_capacity(outpoints.len());
    for op in &outpoints {
        let txid_lc = op.db_txid();
        terminals.push(
            verdicts
                .iter()
                .find(|m| m.txid == txid_lc)
                .is_some_and(|m| m.kind.terminal()),
        );
        let row = if verdicts.iter().any(|m| m.txid == txid_lc) {
            SpentAnyCached {
                known: false,
                spent: None,
                spending_txid: None,
                spent_confirmed: None,
                reason: Some(crate::txany::SPENT_ANY_REASON_TX_ABSENT),
            }
        } else if let Some((_, _, p)) = memo_answers
            .iter()
            .find(|(t, v, _)| *t == txid_lc && *v == op.vout)
        {
            SpentAnyCached {
                known: true,
                spent: p.spent,
                spending_txid: p.spending_txid.clone(),
                spent_confirmed: p.spent_confirmed,
                reason: None,
            }
        } else {
            let row = spent_any_resolve_cached(
                &txid_lc,
                op.vout,
                "spent_any",
                crate::results::SPENT_ANY_CACHE_TTL_MS,
            )
            .await;
            let probe = crate::hops_view::ChainSpendProbe {
                known: row.known,
                spent: row.spent,
                spending_txid: row.spending_txid.clone(),
                spent_confirmed: row.spent_confirmed,
            };
            if let Some(m) = crate::hops_view::probe_memo_of(&txid_lc, op.vout, &probe, now_ms) {
                fresh.push(m);
            }
            row
        };
        entries.push(crate::logic::OutpointStatus {
            txid: op.txid.clone(),
            vout: op.vout,
            known: row.known,
            spent: row.spent,
            spending_txid: row.spending_txid,
            spent_confirmed: row.spent_confirmed,
            // /spent-any is WoC-backed — no overlay witness, strict bar.
            spender_seen: None,
            spender_final: None,
            reason: row.reason,
            funding_seen: None,
            funding_seen_by: None,
            evicted: crate::logic::ServedEviction::NotRead,
        });
    }

    if !fresh.is_empty() {
        if let Ok(db) = ctx.env.d1("OVERLAY_DB") {
            write_probe_memos(&db, &fresh).await;
        }
    }
    // Look up the served pointer, including a freshly probed replacement, and (bsv-low #617) each entry's own
    // creating txid for its `fundingWord`.
    let ids: Vec<_> = entries
        .iter()
        .filter_map(|entry| entry.spending_txid.clone())
        .chain(entries.iter().map(|entry| entry.txid.to_ascii_lowercase()))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let stored_words = crate::tracker_host::runtime::read_soft(&ctx.env, &ids).await?;
    let views = match stored_words {
        Some(rows) => Some(crate::tracker_host::runtime::views(&ctx.env, &rows).await),
        None => None,
    };
    // bsv-low #723: a KNOWN spend's `spentConfirmed` from a checked proof of its VERIFIED spender where the app
    // layer holds one (the index's verified proof, the tracker's checked mined word, an isolate-held chaintracks-bound
    // `/tx-any` proof), the courier's status only when none is held, `null` when the ladder's confirming rung
    // faulted; the source named (`spentConfirmedSource`). The memo written above keeps the ladder's own word.
    let spenders: Vec<String> = entries
        .iter()
        .filter(|e| e.known && e.spent == Some(true))
        .filter_map(|e| e.spending_txid.as_deref().map(str::to_ascii_lowercase))
        .collect();
    let index_proven = spent_any_index_proven(&ctx.env, &spenders).await;
    let now_f = worker::Date::now().as_millis() as f64;
    let mut sources: Vec<Option<&'static str>> = Vec::with_capacity(entries.len());
    for e in &mut entries {
        let Some(sp) = e
            .spending_txid
            .as_deref()
            .map(str::to_ascii_lowercase)
            .filter(|_| e.known && e.spent == Some(true))
        else {
            sources.push(None);
            continue;
        };
        let (confirmed, source) = crate::results::spent_confirmed_word(
            e.spent_confirmed,
            index_proven.contains(&sp),
            crate::owed::checked_tracker_mined(views.as_ref(), &sp) == Some(true),
            tx_any_cached(&sp, now_f).is_some_and(|a| crate::txany::proof_mined_word(&a)),
        );
        e.spent_confirmed = confirmed;
        sources.push(source);
    }
    let mut value = serde_json::from_str(&crate::txany::spent_any_body_sourced(
        &entries, &terminals, &sources,
    ))?;
    // bsv-low #617: each entry's `fundingWord` and `lock` (a courier's clean negative is `unspentAsFarAsAsked`).
    if let Some(views) = views.as_ref() {
        crate::tracker_host::runtime::decorate_outpoints(
            &mut value,
            views,
            "unspentAsFarAsAsked",
            "hint",
        );
        // bsv-low #862 (L7-DEPTH): the host's spend depth beside each entry's `lock`.
        crate::tracker_host::runtime::stamp_spend_depth(
            &mut value,
            crate::tracker_host::runtime::spend_depth(&ctx.env).depth,
        );
    }
    // Read-only decoration; missing words remain unknown until the scheduled pass.
    json_response(
        crate::tracker_host::runtime::decorate_lookup(value, None, views.as_ref()),
        200,
    )
}

/// bsv-low #723: the spenders of `/spent-any`'s known spends the INDEX holds a VERIFIED proof for
/// (`transactions.has_proof`, latched only by the chaintracks-verified stitch), byte-free, by the PRIMARY KEY (the
/// shipped `hopsweep::sweep_proofs_sql`). No statement when no row is a known spend; a fault is no proof (the
/// courier's word stands, the safe direction), logged.
async fn spent_any_index_proven(
    env: &worker::Env,
    spenders: &[String],
) -> std::collections::HashSet<String> {
    #[derive(Deserialize)]
    struct ProofRowD1 {
        txid: String,
    }
    let mut out = std::collections::HashSet::new();
    if spenders.is_empty() {
        return out;
    }
    let Ok(db) = env.d1("OVERLAY_DB") else {
        return out;
    };
    for chunk in spenders.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
        let binds: Vec<JsValue> = chunk.iter().map(|t| JsValue::from_str(t)).collect();
        let rows = match db
            .prepare(crate::hopsweep::sweep_proofs_sql(chunk.len()))
            .bind(&binds)
        {
            Ok(stmt) => stmt
                .counted_all()
                .await
                .and_then(|r| r.results::<ProofRowD1>()),
            Err(e) => Err(e),
        };
        match rows {
            Ok(rows) => out.extend(rows.into_iter().map(|r| r.txid.to_ascii_lowercase())),
            Err(e) => {
                console_warn!(
                    "[spent-any] index proof read failed ({} spenders): {e}",
                    spenders.len()
                );
                return std::collections::HashSet::new();
            }
        }
    }
    out
}

// ── /tx-any — tx-level presence/confirmation/raw, index-first (bsv-low #229) ──

/// One cached `/tx-any` answer.
type TxAnyCached = crate::txany::TxAnyAnswer;

/// bsv-low #672 (RC-L30): the durable spender-bytes store's age + row-cap sweep, the tracker janitor's shape
/// (`tracker_host::stored::SWEEP`): two bounded DELETEs per scheduled pass. The age sweep (older than the retention)
/// is the primary; the row-cap sweep (oldest beyond the ceiling) is the belt against an adversarial write-rate.
/// Fail-soft: a fault is logged and the next pass retries. Returns nothing.
pub async fn sweep_owed_spender_bytes(env: &worker::Env) {
    let Ok(db) = env.d1("OVERLAY_DB") else {
        return;
    };
    let now_ms = worker::Date::now().as_millis() as i64;
    let cutoff = now_ms.saturating_sub(crate::owed::OWED_SPENDER_BYTES_RETENTION_MS);
    let limit = crate::owed::OWED_SPENDER_BYTES_SWEEP_LIMIT as f64;
    // the age sweep (primary): rows older than the retention, bounded this pass
    match db
        .prepare(crate::owed::OWED_SPENDER_BYTES_SWEEP_SQL)
        .bind(&[JsValue::from_f64(cutoff as f64), JsValue::from_f64(limit)])
    {
        Ok(stmt) => {
            if let Err(e) = stmt.counted_run().await {
                console_warn!("[owed] spender-bytes age sweep failed: {e}");
            }
        }
        Err(e) => console_warn!("[owed] spender-bytes age sweep bind failed: {e}"),
    }
    // the row-cap sweep (belt): the oldest rows beyond the ceiling, bounded this pass
    match db
        .prepare(crate::owed::OWED_SPENDER_BYTES_CAP_SWEEP_SQL)
        .bind(&[
            JsValue::from_f64(crate::owed::OWED_SPENDER_BYTES_ROW_CAP as f64),
            JsValue::from_f64(limit),
        ]) {
        Ok(stmt) => {
            if let Err(e) = stmt.counted_run().await {
                console_warn!("[owed] spender-bytes cap sweep failed: {e}");
            }
        }
        Err(e) => console_warn!("[owed] spender-bytes cap sweep bind failed: {e}"),
    }
    crate::owed::note_spender_store_sweep_pass();
}

pub(crate) fn spender_outputs_of(
    tx: &bsv_rs::transaction::Transaction,
) -> Vec<crate::owed::SpenderOutput> {
    tx.outputs
        .iter()
        .enumerate()
        .map(|(vout, o)| {
            let lock = o.locking_script.to_binary();
            crate::owed::SpenderOutput {
                vout: vout as u32,
                pkh_hex: crate::owed::p2pkh_pkh_hex(&lock),
                sats: o.satoshis.unwrap_or(0),
                spent: None,
                // the gate's M1: a pot covenant output = a JOIN (refused at the door and broadcast around it,
                // or wrongly evicted with its bytes never moved), never a sweep
                pot_lock: overlay_discovery::pot::is_pot_covenant_script(&lock),
            }
        })
        .collect()
}
pub(crate) fn spender_inputs_of(tx: &bsv_rs::transaction::Transaction) -> Vec<(String, u32)> {
    tx.inputs
        .iter()
        .filter_map(|inp| {
            inp.source_txid
                .as_deref()
                .map(|t| (t.to_ascii_lowercase(), inp.source_output_index))
        })
        .collect()
}

/// Display evidence only: finality still comes from the chain spend word.
/// The judge applies it too (the D2-OWED lens's MEDIUM-3: the walk's post-derivation steps), with no isolate answer.
pub(crate) fn mark_pending_spender(
    row: &mut crate::owed::OwedRow,
    answer: Option<&TxAnyCached>,
    now_ms: i64,
) {
    if row.facts["spentConfirmed"] == true || row.at_height.is_some() {
        return;
    }
    if row.facts["chainSpender"]
        .as_str()
        .or_else(|| row.facts["spendingTxid"].as_str())
        .is_none()
    {
        return;
    }
    let source = answer
        .and_then(|a| a.hint_source.or(a.source))
        .unwrap_or("spent-any");
    let at = answer.and_then(|a| a.observed_at_ms).unwrap_or_else(|| {
        now_ms.saturating_sub(row.facts["chainProbeAgeMs"].as_i64().unwrap_or(0))
    });
    row.facts["spenderPresence"] = serde_json::json!({
        "word": if row.facts["spendKind"] == "spent-elsewhere" { "spent-outside" } else { "spent" },
        "tier": "pending", "source": source, "at": at,
    });
}

thread_local! {
    /// In-isolate `/tx-any` cache (txid → (expiry ms, answer)) — the same
    /// pattern (and rationale) as `SPENT_ANY_CACHE` above.
    static TX_ANY_CACHE: std::cell::RefCell<std::collections::HashMap<String, (f64, TxAnyCached)>> =
        std::cell::RefCell::new(std::collections::HashMap::new());
    /// The same memo for WoC's tx route (the second gate's MEDIUM-3a): a WoC 404 reads as ABSENT only behind a
    /// route that served the known-mined anchor this isolate — a rotten or rate-limited route is a fault, never
    /// an absence (and never a memo).
    static ARCADE_ROUTE_HEALTHY: std::cell::Cell<Option<(i64, bool)>> = const { std::cell::Cell::new(None) };
    static WOC_ROUTE_HEALTHY: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
    /// The COURIER-RUNG lens fold (M1): BananaBlocks' native tx route, proven by its anchor (`KNOWN_MINED_TXID` served
    /// as a mined claim) at `(ms, healthy)`; both outcomes held for [`ARCADE_ANCHOR_PROBE_TTL_MS`], as Arcade's.
    static BANANABLOCKS_ROUTE_HEALTHY: std::cell::Cell<Option<(i64, bool)>> = const { std::cell::Cell::new(None) };
}

/// SHIPPED trusted-BEEF reads shared by `/tx-any`'s index leg and `/beef`
/// (bsv-low#304) — consts so the real-SQLite tests execute the production
/// strings. Each table's VERIFIED proof latch is aliased `proofVerified`:
/// `transactions.has_proof` is latched only by the overlay's verifying
/// writers (`insert_output` forces 0 on every admit;
/// `mark_transaction_proven` / the verified stitch flip it), and
/// `pot_beefs.proof_verified` is the #304 verified latch (its `has_proof`
/// sibling is STRUCTURAL — submitter bytes, zero SPV — and is deliberately
/// NOT selected here).
pub(crate) const POT_BEEFS_TRUST_SQL: &str =
    "SELECT hex(beef) AS beef, proof_verified AS proofVerified FROM pot_beefs WHERE txid = ?";
pub(crate) const TRANSACTIONS_TRUST_SQL: &str =
    "SELECT hex(beef) AS beef, has_proof AS proofVerified FROM transactions WHERE txid = ?";

/// A BEEF row + its VERIFIED proof latch. `proofVerified` is
/// Option-tolerant: NULL/absent (a read racing the overlay's additive
/// migration) = UNVERIFIED — the height is withheld, never strengthened.
#[derive(Deserialize)]
struct BeefTrustRow {
    beef: Option<String>,
    #[serde(rename = "proofVerified", default)]
    proof_verified: Option<f64>,
}

/// The INDEX leg: raw hex + BUMP height from the stored BEEF (`pot_beefs`
/// first, `transactions` second — the `/beef` order). `(None, None)` = index
/// miss OR D1 fault (fault logged; the break-glass leg still answers — a
/// read surface must not dead-end on a D1 blip, and the external answer is
/// still truthful).
///
/// bsv-low#304: the served height is GATED on the row's VERIFIED proof
/// latch (`verified_beef_block_height`) — a stored bump WITHOUT the latch
/// (fake-bumped rows admitted via the ungated historical/GASP/peer-crawl
/// paths, or an honest backlog row awaiting re-verify) answers like a
/// bumpless row: raw served, confirmed/height defer to the external leg
/// (the #247 machinery). Weakens only unverified answers, never verified
/// ones.
async fn tx_any_index_leg(
    ctx: &RouteContext<AuthState>,
    txid_lc: &str,
) -> (Option<String>, Option<u64>) {
    let Ok(db) = ctx.env.d1("OVERLAY_DB") else {
        console_warn!("[tx-any] OVERLAY_DB binding unavailable — break-glass leg only");
        return (None, None);
    };
    for (table, sql) in [
        ("pot_beefs", POT_BEEFS_TRUST_SQL),
        ("transactions", TRANSACTIONS_TRUST_SQL),
    ] {
        let Ok(stmt) = db.prepare(sql).bind(&[JsValue::from_str(txid_lc)]) else {
            continue;
        };
        let row: Option<BeefTrustRow> = match stmt.counted_first::<BeefTrustRow>().await {
            Ok(row) => row,
            Err(e) => {
                console_warn!("[tx-any] {table} query failed: {e}");
                continue;
            }
        };
        let Some(row) = row else { continue };
        let proof_verified = row.proof_verified.unwrap_or(0.0) != 0.0;
        if let Some(bytes) = row.beef.and_then(|h| decode_beef_hex(&h)) {
            if let Some(raw_hex) = crate::logic::extract_raw_tx_hex(&bytes, txid_lc) {
                let height =
                    crate::results::verified_beef_block_height(&bytes, txid_lc, proof_verified);
                return (Some(raw_hex), height);
            }
        }
    }
    (None, None)
}

const ARCADE_ANCHOR_PROBE_TTL_MS: i64 = 30_000;

/// The COURIER-RUNG lens fold (M1): BananaBlocks' route memo when it is inside [`ARCADE_ANCHOR_PROBE_TTL_MS`].
fn bananablocks_route_fresh(now_ms: i64) -> Option<bool> {
    BANANABLOCKS_ROUTE_HEALTHY.with(|c| {
        c.get()
            .filter(|(at, _)| now_ms >= *at && now_ms - at < ARCADE_ANCHOR_PROBE_TTL_MS)
            .map(|(_, healthy)| healthy)
    })
}

/// The COURIER-RUNG lens fold (M1, #614): is BananaBlocks' native tx route PROVEN? It is when the same route serves
/// the known-mined anchor (`txany::KNOWN_MINED_TXID`) as a mined claim (`confirmed: true`, a positive `block_height`,
/// the anchor's own txid, no `confirmation_unknown`: `txany::parse_bananablocks_tx_claim`). One GET per isolate per
/// [`ARCADE_ANCHOR_PROBE_TTL_MS`], both outcomes memoised (Arcade's shape: an unproven route is not asked again on
/// every request); counted `tx_any_bananablocks_anchor_{ok,unknown}_total` when asked.
async fn bananablocks_route_healthy<G, F>(now_ms: i64, get: &G) -> bool
where
    G: Fn(String) -> F,
    F: std::future::Future<Output = Option<(u16, Vec<u8>)>>,
{
    if let Some(healthy) = bananablocks_route_fresh(now_ms) {
        return healthy;
    }
    let anchor = crate::txany::KNOWN_MINED_TXID;
    let healthy = match get(format!("{BANANABLOCKS_BASE}/tx/{anchor}")).await {
        Some((200, body)) => serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| crate::txany::parse_bananablocks_tx_claim(&v, anchor))
            .is_some_and(|(confirmed, height)| confirmed && height.is_some_and(|h| h > 0)),
        _ => false,
    };
    crate::courier::note_presence(if healthy {
        "tx_any_bananablocks_anchor_ok_total"
    } else {
        "tx_any_bananablocks_anchor_unknown_total"
    });
    BANANABLOCKS_ROUTE_HEALTHY.with(|c| c.set(Some((now_ms, healthy))));
    healthy
}
fn arcade_anchor_probe_due(now_ms: i64) -> bool {
    ARCADE_ROUTE_HEALTHY.with(|c| {
        c.get()
            .is_none_or(|(at, _)| now_ms < at || now_ms - at >= ARCADE_ANCHOR_PROBE_TTL_MS)
    })
}

const TX_ANY_OURS_SQL: &str = "SELECT txid FROM transactions WHERE txid = ? UNION ALL SELECT txid FROM pot_beefs WHERE txid = ? UNION ALL SELECT refusedTxid AS txid FROM submit_refusals WHERE refusedTxid = ? LIMIT 1";
const TX_ANY_HELD_ANCHOR_SQL: &str = "SELECT t.txid FROM transactions t INNER JOIN network_seen n ON n.txid = t.txid WHERE t.has_proof = 1 LIMIT 1";

/// Scope is local evidence, never Arcade's answer. A DB fault cannot turn a
/// foreign subject into ours. A submit refusal names a subject we attempted.
async fn tx_any_scope(env: &worker::Env, txid: &str) -> (bool, Option<String>) {
    #[derive(serde::Deserialize)]
    struct Id {
        txid: String,
    }
    let Ok(db) = env.d1("OVERLAY_DB") else {
        return (false, None);
    };
    let ours = db
        .prepare(TX_ANY_OURS_SQL)
        .bind(&[
            JsValue::from_str(txid),
            JsValue::from_str(txid),
            JsValue::from_str(txid),
        ])
        .ok();
    let ours = match ours {
        Some(stmt) => stmt.counted_first::<Id>().await.ok().flatten().is_some(),
        None => false,
    };
    let now_ms = worker::Date::now().as_millis() as i64;
    let anchor = if ours && arcade_anchor_probe_due(now_ms) {
        db.prepare(TX_ANY_HELD_ANCHOR_SQL)
            .counted_first::<Id>()
            .await
            .ok()
            .flatten()
            .map(|r| r.txid)
    } else {
        None
    };
    (ours, anchor)
}

/// Foreign subjects go directly to couriers; their silence is never Arcade's vote.
pub(crate) async fn tx_any_external_leg(
    txid_lc: &str,
    caller: &'static str,
    env: &worker::Env,
) -> (
    crate::txany::TxObservation,
    crate::txany::AbsenceCorroboration,
    Option<u64>,
) {
    let (ours, anchor) = tx_any_scope(env, txid_lc).await;
    tx_any_presence_leg_via(
        txid_lc,
        worker::Date::now().as_millis() as i64,
        ours,
        anchor.as_deref(),
        |url: String| async move { provider_get(caller, &url).await },
        |bump: String, height| async move {
            crate::beef_guard::canonical_root(env, height)
                .await
                .is_some_and(|root| {
                    crate::txany::mined_proof_matches(txid_lc, &bump, height, &root)
                })
        },
    )
    .await
}

/// Arcade is first only for OURS; a foreign mined claim requires the same proof check.
async fn tx_any_presence_leg_via<G, F, V, VF>(
    txid: &str,
    now_ms: i64,
    ours: bool,
    held_anchor: Option<&str>,
    get: G,
    verify: V,
) -> (
    crate::txany::TxObservation,
    crate::txany::AbsenceCorroboration,
    Option<u64>,
)
where
    G: Fn(String) -> F,
    F: std::future::Future<Output = Option<(u16, Vec<u8>)>>,
    V: Fn(String, u64) -> VF,
    VF: std::future::Future<Output = bool>,
{
    use crate::txany::{AbsenceCorroboration, ArcadeWord, TxObservation};
    let response = if ours {
        get(format!("{}/tx/{txid}", arcade_url())).await
    } else {
        None
    };
    let word = response
        .as_ref()
        .map(|(status, body)| crate::txany::parse_arcade_subject_word(*status, body, txid, now_ms))
        .unwrap_or(ArcadeWord::Unknown);
    let confirmation = match word {
        ArcadeWord::SeenFresh => Some((false, None)),
        ArcadeWord::Mined { bump_hex, height } if verify(bump_hex.clone(), height).await => {
            Some((true, Some(height)))
        }
        _ => None,
    };
    if let Some((confirmed, height)) = confirmation {
        let raw_hex = tx_any_courier_raw_via(txid, &get).await;
        return (
            TxObservation::FirstPartyPresent { confirmed, raw_hex },
            AbsenceCorroboration::Unknown,
            height,
        );
    }
    // A HELD, proven anchor tests route reachability, never subject absence.
    // Both outcomes expire, so a stale Arcade rank cannot cause an endless probe.
    let held_404 = ours && matches!(response, Some((404, _)));
    let anchor_probe = held_404 && arcade_anchor_probe_due(now_ms);
    // bsv-low #626: a held 404 either writes the anchor memo (the probe below) or is answered by it.
    if held_404 {
        let op = if anchor_probe { "write" } else { "hit" };
        crate::courier::note_presence(&crate::txany::memo_counter(op, "anchor"));
    }
    if anchor_probe {
        let healthy = if let Some(anchor) = held_anchor {
            let response = get(format!("{}/tx/{anchor}", arcade_url())).await;
            let healthy = response.as_ref().is_some_and(|(status, body)| {
                *status == 200
                    && serde_json::from_slice::<serde_json::Value>(body)
                        .ok()
                        .and_then(|v| v["txid"].as_str().map(str::to_owned))
                        .is_some_and(|id| id.eq_ignore_ascii_case(anchor))
            });
            if healthy
                && response.as_ref().is_some_and(|(status, body)| {
                    crate::txany::parse_arcade_subject_word(*status, body, anchor, now_ms)
                        == ArcadeWord::Unknown
                })
            {
                crate::courier::note_presence("tx_any_arcade_anchor_stale_rank_total");
            }
            crate::courier::note_presence(if healthy {
                "tx_any_arcade_anchor_ok_total"
            } else {
                "tx_any_arcade_anchor_unknown_total"
            });
            healthy
        } else {
            crate::courier::note_presence("tx_any_arcade_anchor_unavailable_total");
            false
        };
        ARCADE_ROUTE_HEALTHY.with(|c| c.set(Some((now_ms, healthy))));
    }
    let (mut observation, absence, claimed_height, source) =
        tx_any_courier_leg_via(txid, now_ms, &get).await;
    if let TxObservation::Present { confirmed, raw_hex } = observation {
        if confirmed {
            let proven = if let Some(height) = claimed_height {
                match get(format!("{WOC_BASE}/tx/{txid}/proof/tsc")).await {
                    Some((200, body)) => match crate::txany::courier_tsc_bump(&body, height) {
                        Some(bump) => verify(bump, height).await,
                        None => false,
                    },
                    _ => false,
                }
            } else {
                false
            };
            observation = if proven {
                TxObservation::Present {
                    confirmed: true,
                    raw_hex,
                }
            } else {
                crate::courier::note_presence("tx_any_courier_proof_fault_total");
                TxObservation::CourierHint {
                    raw_hex,
                    source,
                    observed_at_ms: now_ms,
                }
            };
        } else {
            observation = TxObservation::CourierHint {
                raw_hex,
                source,
                observed_at_ms: now_ms,
            };
        }
    }
    if observation == TxObservation::Absent && absence == AbsenceCorroboration::Unknown {
        // WoC's node is one capable source. BananaBlocks' native view is the second (bsv-low #868, probed
        // 2026-10-24); when it did not answer its clean not-found the absence has one vote. Bitails is a
        // positive-only witness and Arcade a submitted-bytes store, so neither supplies it. UNKNOWN, counted, no
        // request-time Absent memo.
        crate::courier::note_presence("tx_any_one_capable_source_unknown_total");
    }
    (observation, absence, claimed_height)
}

/// bsv-low #893 section 5 (the owed belt): the spent-any ladder over the belt's gated courier port. The probe, and the
/// spender's raw when the ladder read it and it verified (it hashes to the named spender and spends this outpoint).
pub(crate) async fn belt_spent_any_via<G, F>(
    txid_lc: &str,
    vout: u32,
    get: G,
) -> (crate::hops_view::ChainSpendProbe, Option<Vec<u8>>)
where
    G: Fn(String) -> F,
    F: std::future::Future<Output = Option<(u16, Vec<u8>)>>,
{
    let (row, raw) = spent_any_resolve_via(txid_lc, vout, get).await;
    (
        crate::hops_view::ChainSpendProbe {
            known: row.known,
            spent: row.spent,
            spending_txid: row.spending_txid,
            spent_confirmed: row.spent_confirmed,
        },
        raw,
    )
}

/// bsv-low #893 section 5: a named spender's hash-verified raw over the belt's gated port (WoC's `/tx/hash` for its
/// claim, the raw from the first courier whose bytes hash to the txid), and the courier's CLAIMED mined height,
/// which the belt binds through WoC's `/proof/tsc` before it is evidence.
pub(crate) async fn belt_spender_via<G, F>(
    txid_lc: &str,
    now_ms: i64,
    get: G,
) -> (Option<Vec<u8>>, Option<u64>)
where
    G: Fn(String) -> F,
    F: std::future::Future<Output = Option<(u16, Vec<u8>)>>,
{
    let (observation, _, claimed_height, _) = tx_any_courier_leg_via(txid_lc, now_ms, &get).await;
    let raw = match observation {
        crate::txany::TxObservation::Present { raw_hex, .. }
        | crate::txany::TxObservation::FirstPartyPresent { raw_hex, .. }
        | crate::txany::TxObservation::CourierHint { raw_hex, .. } => raw_hex,
        _ => None,
    };
    (raw.and_then(|h| hex::decode(h).ok()), claimed_height)
}

/// bsv-low #893 section 5: WoC's TSC proof route, the one proof source the belt asks.
pub(crate) fn belt_proof_url(txid_lc: &str) -> String {
    format!("{WOC_BASE}/tx/{txid_lc}/proof/tsc")
}

/// The raw body is optional for a first-party hint, and always hash checked.
async fn tx_any_courier_raw_via<G, F>(txid: &str, get: &G) -> Option<String>
where
    G: Fn(String) -> F,
    F: std::future::Future<Output = Option<(u16, Vec<u8>)>>,
{
    if let Some((200, body)) = get(format!("{WOC_BASE}/tx/{txid}/hex")).await {
        if let Some(raw) = std::str::from_utf8(&body)
            .ok()
            .and_then(|h| hex::decode(h.trim()).ok())
            .and_then(|raw| crate::txany::verify_raw_bytes(&raw, txid))
        {
            return Some(raw);
        }
    }
    if let Some((200, body)) = get(format!("{BITAILS_BASE}/download/tx/{txid}")).await {
        if let Some(raw) = crate::txany::verify_raw_bytes(&body, txid) {
            return Some(raw);
        }
    }
    // bsv-low #868: the third raw source, BananaBlocks' native `/tx/{txid}/hex` (probed 2026-10-24, `text/plain`)
    match get(format!("{BANANABLOCKS_BASE}/tx/{txid}/hex")).await {
        Some((200, body)) => std::str::from_utf8(&body)
            .ok()
            .and_then(|h| hex::decode(h.trim()).ok())
            .and_then(|raw| crate::txany::verify_raw_bytes(&raw, txid)),
        _ => None,
    }
}

#[cfg(test)]
async fn tx_any_external_leg_via<G, F>(
    txid: &str,
    get: G,
) -> (
    crate::txany::TxObservation,
    crate::txany::AbsenceCorroboration,
    Option<u64>,
)
where
    G: Fn(String) -> F,
    F: std::future::Future<Output = Option<(u16, Vec<u8>)>>,
{
    let (observation, absence, claimed_height, _) = tx_any_courier_leg_via(txid, 0, get).await;
    (observation, absence, claimed_height)
}

/// The external leg over a courier GET port (`get(url)` answers `provider_get`'s `(status, body)`): production passes
/// `provider_get`, the pins a captured body (bsv-low #606).
///
/// bsv-low #868 (lane COURIER-RUNG, the probe `docs/audit/COURIER-CAPABILITY-2026-10-24.md`): the line goes down the
/// couriers. WoC's `/tx/hash/{txid}` first; when it holds no positive, BananaBlocks' NATIVE `/tx/{txid}` (a capable
/// mempool source: `confirmed`, `block_height`); when neither does, Bitails' `/tx/{txid}`, a POSITIVE-ONLY witness
/// (its `confirmations: 0` is presence, its 404 "could not look"). The first positive answers, with its source (the
/// hint's `hintSource`) and its claimed height (a mined claim is proven by the caller exactly as WoC's is). ABSENCE
/// needs TWO capable sources: WoC's clean 404 behind its healthy route AND BananaBlocks' recorded not-found
/// (`txany::bananablocks_tx_not_found`); one of them alone is the honest unknown. The WoC-style mirror of
/// BananaBlocks is never asked. Each rung's answer is counted `tx_any_courier_<rung>_<outcome>_total`.
///
/// The COURIER-RUNG lens fold (M1, #614's rule: absence needs two capable sources that both LOOKED): BananaBlocks'
/// not-found is a look only behind a PROVEN route, as WoC's 404 and Arcade's held 404 are
/// ([`bananablocks_route_healthy`]: its native route serves the known-mined anchor as a mined claim, memoised per
/// isolate for the Arcade anchor's TTL); behind an unproven route it is "could not look"
/// (`tx_any_courier_bananablocks_couldnotlook_total`). Any other BananaBlocks 404 is a fault. The anchor is asked only
/// when the not-found could vote, i.e. beside WoC's own look.
async fn tx_any_courier_leg_via<G, F>(
    txid_lc: &str,
    now_ms: i64,
    get: G,
) -> (
    crate::txany::TxObservation,
    crate::txany::AbsenceCorroboration,
    // bsv-low 2026-09-21: the courier's CLAIMED mined height (WoC's `blockheight`), for `TxAnyAnswer::claimed_height`
    Option<u64>,
    // bsv-low #868: the rung whose answer this is (`woc`, `bananablocks`, `bitails`)
    &'static str,
)
where
    G: Fn(String) -> F,
    F: std::future::Future<Output = Option<(u16, Vec<u8>)>>,
{
    use crate::txany::{AbsenceCorroboration, TxObservation};

    let mut claimed_height: Option<u64> = None;
    let woc = match get(format!("{WOC_BASE}/tx/hash/{txid_lc}")).await {
        Some((200, body)) => match serde_json::from_slice::<serde_json::Value>(&body) {
            // bsv-low #527 LOW-3: a 200 body that is NO claim is a FAULT (unknown), never "present, unconfirmed": the
            // era belt reads an unconfirmed presence as current-era, and every other reader treats unknown as retry.
            // bsv-low #606: WoC's MEMPOOL body carries no `confirmations` field (probed live 2026-10-12: `hash,
            // locktime, size, txid, version, vin, vout`); a body of that shape for the asked txid IS a claim, present
            // and unconfirmed (`parse_woc_tx_claim`), and its raw is fetched and hash-verified below as a mined one's
            Ok(v) => match crate::txany::parse_woc_tx_claim(&v, txid_lc) {
                None => TxObservation::Fault,
                Some(confirmed) => {
                    if confirmed {
                        claimed_height = crate::txany::parse_woc_blockheight(&v);
                    }
                    // Positive presence requires the raw in hand, hash-verified.
                    let raw_hex = tx_any_courier_raw_via(txid_lc, &get).await;
                    TxObservation::Present { confirmed, raw_hex }
                }
            },
            Err(_) => TxObservation::Fault,
        },
        Some((404, _)) => {
            // the second gate's MEDIUM-3a: WoC's 404 is an absence only behind a healthy route (the anchor served
            // once this isolate) — the Bitails rule applied symmetrically; otherwise a fault, never an absence
            if WOC_ROUTE_HEALTHY.with(std::cell::Cell::get) != Some(true) {
                if let Some((200, body)) = get(format!(
                    "{WOC_BASE}/tx/hash/{}",
                    crate::txany::KNOWN_MINED_TXID
                ))
                .await
                {
                    if !body.is_empty() {
                        WOC_ROUTE_HEALTHY.with(|c| c.set(Some(true)));
                    }
                }
            }
            if WOC_ROUTE_HEALTHY.with(std::cell::Cell::get) == Some(true) {
                TxObservation::Absent
            } else {
                TxObservation::Fault
            }
        }
        _ => TxObservation::Fault,
    };
    crate::courier::note_presence(&crate::txany::courier_rung_counter(
        "woc",
        crate::txany::CourierRungOutcome::of(&woc),
    ));
    if matches!(woc, TxObservation::Present { .. }) {
        return (woc, AbsenceCorroboration::Unknown, claimed_height, "woc");
    }

    // bsv-low #868: BananaBlocks' native view, the second capable mempool source
    let mut banana_not_found = false;
    let mut banana_unproven = false;
    let banana = match get(format!("{BANANABLOCKS_BASE}/tx/{txid_lc}")).await {
        Some((200, body)) => match serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| crate::txany::parse_bananablocks_tx_claim(&v, txid_lc))
        {
            Some((confirmed, height)) => {
                claimed_height = height;
                let raw_hex = tx_any_courier_raw_via(txid_lc, &get).await;
                TxObservation::Present { confirmed, raw_hex }
            }
            None => TxObservation::Fault,
        },
        Some((status, body)) if crate::txany::bananablocks_tx_not_found(status, &body) => {
            // the lens fold's M1: a look only behind a proven route, asked only where it could vote (WoC looked)
            banana_not_found = bananablocks_route_fresh(now_ms) == Some(true)
                || (woc == TxObservation::Absent && bananablocks_route_healthy(now_ms, &get).await);
            banana_unproven = !banana_not_found;
            if banana_not_found {
                TxObservation::Absent
            } else {
                TxObservation::Fault
            }
        }
        _ => TxObservation::Fault,
    };
    crate::courier::note_presence(&crate::txany::courier_rung_counter(
        "bananablocks",
        if banana_unproven {
            crate::txany::CourierRungOutcome::CouldNotLook
        } else {
            crate::txany::CourierRungOutcome::of(&banana)
        },
    ));
    if matches!(banana, TxObservation::Present { .. }) {
        return (
            banana,
            AbsenceCorroboration::Unknown,
            claimed_height,
            "bananablocks",
        );
    }

    // bsv-low #868: Bitails, a positive-only witness (its 404 is "could not look", never an absence)
    let bitails = match get(format!("{BITAILS_BASE}/tx/{txid_lc}")).await {
        Some((200, body)) => match serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| crate::txany::parse_bitails_tx_presence(&v, txid_lc))
        {
            Some((confirmed, height)) => {
                claimed_height = height;
                let raw_hex = tx_any_courier_raw_via(txid_lc, &get).await;
                Some(TxObservation::Present { confirmed, raw_hex })
            }
            None => Some(TxObservation::Fault),
        },
        Some((404, _)) => None,
        _ => Some(TxObservation::Fault),
    };
    crate::courier::note_presence(&crate::txany::courier_rung_counter(
        "bitails",
        bitails.as_ref().map_or(
            crate::txany::CourierRungOutcome::CouldNotLook,
            crate::txany::CourierRungOutcome::of,
        ),
    ));
    if let Some(present @ TxObservation::Present { .. }) = bitails {
        return (
            present,
            AbsenceCorroboration::Unknown,
            claimed_height,
            "bitails",
        );
    }

    // No positive anywhere: WoC's clean 404 and BananaBlocks' clean not-found are two capable sources.
    match woc {
        TxObservation::Absent if banana_not_found => {
            crate::courier::note_presence("tx_any_two_capable_sources_absent_total");
            (
                TxObservation::Absent,
                AbsenceCorroboration::CorroboratedAbsent,
                None,
                "woc",
            )
        }
        TxObservation::Absent => (
            TxObservation::Absent,
            AbsenceCorroboration::Unknown,
            None,
            "woc",
        ),
        _ => (
            TxObservation::Fault,
            AbsenceCorroboration::Unknown,
            None,
            "woc",
        ),
    }
}

/// The in-isolate `/tx-any` cache hit for `key`, if still inside its TTL.
fn tx_any_cached(key: &str, now: f64) -> Option<TxAnyCached> {
    let hit = TX_ANY_CACHE.with(|c| {
        c.borrow()
            .get(key)
            .filter(|(expiry, _)| *expiry > now)
            .map(|(_, a)| a.clone())
    });
    // bsv-low #626: the isolate's ten-minute unconfirmable memo, counted when it answers.
    if hit.as_ref().is_some_and(|a| a.unconfirmable) {
        crate::courier::note_presence(&crate::txany::memo_counter("hit", "isolate_unconfirmable"));
    }
    hit
}

/// The in-isolate cache write — ONE writer for the single and the batched
/// route (bsv-low W-C.3), so an answer either route produced is the answer the
/// other serves for the TTL.
fn cache_tx_any_answer(key: &str, answer: &TxAnyCached, now: f64) {
    if answer.unconfirmable {
        crate::courier::note_presence(&crate::txany::memo_counter(
            "write",
            "isolate_unconfirmable",
        ));
    }
    TX_ANY_CACHE.with(|c| {
        let mut map = c.borrow_mut();
        map.retain(|_, (expiry, _)| *expiry > now);
        map.insert(
            key.to_string(),
            (
                now + crate::txany::tx_any_cache_ttl_ms(answer),
                answer.clone(),
            ),
        );
    });
}

fn apply_external_height(
    a: &mut TxAnyCached,
    external: &crate::txany::TxObservation,
    claimed_height: Option<u64>,
) {
    if matches!(
        external,
        crate::txany::TxObservation::FirstPartyPresent {
            confirmed: true,
            ..
        } | crate::txany::TxObservation::Present {
            confirmed: true,
            ..
        }
    ) {
        a.height = claimed_height;
    }
    if matches!(
        external,
        crate::txany::TxObservation::Present { .. }
            | crate::txany::TxObservation::CourierHint { .. }
    ) {
        a.claimed_height = claimed_height;
    }
}

/// Everything AFTER the index leg for ONE txid (the single route's whole
/// decision past the index read; the batched route reuses it only for the
/// txids its index leg DECIDED, never for the external leg — gate MED-1): the
/// external leg when the index has no verified-BUMP answer, the #247/#252
/// unconfirmable probe on an absent tx, and the in-isolate cache write.
async fn resolve_tx_any(
    index_raw: Option<String>,
    index_height: Option<u64>,
    key: &str,
    now: f64,
    caller: &'static str,
    env: &worker::Env,
) -> TxAnyCached {
    // bsv-low #451 slice C (iv) — THE OWNER'S RULE: the couriers are break-glass, never routine. A verdict memo
    // (`tx_any_verdicts`: the janitor's corroborated absence or refusal, or a proven input conflict) answers FIRST
    // for every row without a verified bump, with zero courier calls; an INDEX-HELD row without a memo asks Arcade
    // (our own broadcaster) and otherwise answers the honest `null` — the overlay's dead-letter pass corroborates
    // its absence in the background, bounded per tick, and writes the memo; only an index-UNKNOWN row without a
    // memo (a tx never admitted — the client's own record of a broadcast the overlay refused) still opens the
    // break-glass at request time: scoped Arcade or couriers; one capable absence source is unknown.
    let answer = if index_raw.is_some() && index_height.is_some() {
        crate::txany::decide_tx_any(
            index_raw,
            index_height,
            None,
            crate::txany::AbsenceCorroboration::Unknown,
        )
    } else if let Some(memo) = read_verdict_memo(env, key, now as i64).await {
        crate::courier::note_presence(&crate::txany::memo_counter("hit", memo.kind.as_str()));
        let a = crate::txany::answer_from_verdict_memo(&memo, index_raw);
        cache_tx_any_answer(key, &a, now);
        return a;
    } else if index_raw.is_some() {
        // bsv-low #451 slice B: we HOLD the bytes without a verified bump (the JOIN's first minutes; a MINED push not
        // yet landed). Arcade — the broadcaster whose SEEN is the index's own admission witness — answers presence
        // and confirmation in ONE read (`parse_arcade_word`, pinned); when it does not know or refuses to say, the
        // answer is the honest unknown — never a courier here (slice C (iv)).
        match arcade_confirmation_look(key, caller, env).await {
            Some((external, verified_height)) => {
                let mut a = crate::txany::decide_tx_any(
                    index_raw,
                    index_height,
                    Some(&external),
                    crate::txany::AbsenceCorroboration::Unknown,
                );
                // the chaintracks-verified height (a proven fact, stronger than any courier's claim)
                if a.confirmed == Some(true) {
                    a.height = verified_height;
                }
                a
            }
            None => crate::txany::decide_tx_any(
                index_raw,
                index_height,
                None,
                crate::txany::AbsenceCorroboration::Unknown,
            ),
        }
    } else {
        // the one break-glass left at request time: a tx the index never admitted
        let (external, absence, claimed_height) = tx_any_external_leg(key, caller, env).await;
        let mut a = crate::txany::decide_tx_any(index_raw, index_height, Some(&external), absence);
        apply_external_height(&mut a, &external, claimed_height);
        a
    };
    // The second gate's MEDIUM-1 (2026-09-18): the request-time UNCONFIRMABLE probe (up to three inputs through the
    // spend ladder on a `present:false` answer) is GONE — since the couriers became break-glass the only request-time
    // `present:false` is the index-unknown leg's, which serves no bytes, so the probe could never run. The
    // `unconfirmable` word stays served for the memos written before migration 152 and is the janitor's to derive
    // (a residual on #451: the refund rebroadcast retirement rests on the local direct-ARC failure belt).
    cache_tx_any_answer(key, &answer, now);
    answer
}

/// `GET /tx-any/:txid` — presence / confirmation / hash-verified raw for an
/// arbitrary txid, INDEX-FIRST (the overlay is the system of record for every
/// tx LOW broadcast; external indexers are break-glass for legacy/foreign
/// txids only — owner doctrine, bsv-low #229). ~15 s in-isolate cache.
/// Unknown is the honest answer for every fault (`present: null`). Every
/// answer is `no-store` (the owner call of 2026-07-14, `lib.rs`): a mined
/// fact is demotable (D7, the reorg sweep) and an `unconfirmable` negative is
/// a courier's word, so nothing here may outlive the isolate cache — gate
/// HIGH-1/HIGH-2 of 2026-09-10 retired an `immutable` edge answer.
/// bsv-low #527 (2026-09-21): a CONFIRMED external answer (the couriers' leg, no proven height) also carries
/// `claimedHeight`, WoC's `blockheight` as a CLAIM (`TxAnyAnswer::claimed_height`): display-tier dating of a tx the
/// index no longer holds (a closed table advert judged against the written-off era); `height` stays the
/// chaintracks-verified word and a money reader keyed on the claim is a defect.
pub async fn tx_any(_req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    let Some(txid) = ctx.param("txid").cloned() else {
        return json_error("missing txid", 400);
    };
    if !valid_txid(&txid) {
        return json_error("invalid txid (expect 64 hex chars)", 400);
    }
    crate::courier::note_presence("tracker_public_lookup_only_total");
    let key = txid.to_ascii_lowercase();
    let stored =
        crate::tracker_host::runtime::read_soft(&ctx.env, std::slice::from_ref(&key)).await?;
    let now = worker::Date::now().as_millis() as f64;
    let answer = match tx_any_cached(&key, now) {
        Some(a) => a,
        None => {
            let (index_raw, index_height) = tx_any_index_leg(&ctx, &key).await;
            resolve_tx_any(index_raw, index_height, &key, now, "tx_any", &ctx.env).await
        }
    };
    let legacy = crate::txany::tx_any_body(&key, &answer);
    let value = serde_json::from_str(&legacy)?;
    json_response(
        crate::tracker_host::runtime::body_loaded(&ctx.env, value, Some(&key), stored).await?,
        200,
    )
}

/// One batched index-leg row: the txid alongside what the single leg reads.
#[derive(Deserialize)]
struct BeefTrustRowKeyed {
    txid: String,
    beef: Option<String>,
    #[serde(rename = "proofVerified", default)]
    proof_verified: Option<f64>,
}

/// What the batched index leg found for a set of txids.
struct BatchIndexLeg {
    /// txid → (raw hex, verified-BUMP height): rows the index served AND the
    /// extractor decoded (a hit is final at the table that served it).
    hits: std::collections::HashMap<String, (String, Option<u64>)>,
    /// txids whose chunk faulted at D1 in some table (they were still tried
    /// in the next table; if they end unresolved the reason is the fault).
    faulted: std::collections::HashSet<String>,
}

/// bsv-low W-C.3 — the index leg for MANY txids: one `IN (…)` query per table
/// per chunk of `TX_ANY_BATCH_D1_CHUNK`, each row bounded by
/// `TX_ANY_BATCH_BEEF_MAX_BYTES` (gate MED-2: a caller-chosen set must never
/// materialize an unbounded pile of BEEFs in one isolate; a bigger row is
/// simply not served here and the per-txid route reads it alone), `pot_beefs`
/// first (a hit there is final, as in the single leg), then `transactions` for
/// the rest. The fold of rows into hits/fall-throughs is the pure
/// `crate::txany::fold_index_rows` (pinned); the SQL is the pure
/// `tx_any_index_leg_batch_sql` (prepared against the production schema in
/// `tests/sql_prepares_sqlite.rs`, gate MED-3). A query fault leaves its
/// txids for the next table and marks them `faulted`.
async fn tx_any_index_leg_batch(ctx: &RouteContext<AuthState>, keys: &[String]) -> BatchIndexLeg {
    let mut leg = BatchIndexLeg {
        hits: std::collections::HashMap::new(),
        faulted: std::collections::HashSet::new(),
    };
    let Ok(db) = ctx.env.d1("OVERLAY_DB") else {
        console_warn!("[tx-any] OVERLAY_DB binding unavailable — batch answers nothing");
        leg.faulted.extend(keys.iter().cloned());
        return leg;
    };
    let extract =
        |beef_hex: &str, txid_lc: &str, proof_verified: bool| -> Option<(String, Option<u64>)> {
            let bytes = decode_beef_hex(beef_hex)?;
            let raw_hex = crate::logic::extract_raw_tx_hex(&bytes, txid_lc)?;
            let height =
                crate::results::verified_beef_block_height(&bytes, txid_lc, proof_verified);
            Some((raw_hex, height))
        };
    let mut pending: Vec<String> = keys.to_vec();
    for (table, col) in [
        ("pot_beefs", "proof_verified"),
        ("transactions", "has_proof"),
    ] {
        if pending.is_empty() {
            break;
        }
        let mut next_pending: Vec<String> = Vec::new();
        for chunk in pending.chunks(crate::txany::TX_ANY_BATCH_D1_CHUNK) {
            let sql = crate::txany::tx_any_index_leg_batch_sql(table, col, chunk.len());
            let binds: Vec<JsValue> = chunk.iter().map(|k| JsValue::from_str(k)).collect();
            let rows: Vec<BeefTrustRowKeyed> = match db.prepare(&sql).bind(&binds) {
                Ok(stmt) => match stmt
                    .counted_all()
                    .await
                    .and_then(|r| r.results::<BeefTrustRowKeyed>())
                {
                    Ok(rows) => rows,
                    Err(e) => {
                        console_warn!(
                            "[tx-any] {table} batch query failed ({} txids): {e}",
                            chunk.len()
                        );
                        leg.faulted.extend(chunk.iter().cloned());
                        next_pending.extend(chunk.iter().cloned());
                        continue;
                    }
                },
                Err(e) => {
                    console_warn!(
                        "[tx-any] {table} batch bind failed ({} txids): {e}",
                        chunk.len()
                    );
                    leg.faulted.extend(chunk.iter().cloned());
                    next_pending.extend(chunk.iter().cloned());
                    continue;
                }
            };
            let served: Vec<(String, Option<String>, bool)> = rows
                .into_iter()
                .map(|r| {
                    (
                        r.txid.to_ascii_lowercase(),
                        r.beef,
                        r.proof_verified.unwrap_or(0.0) != 0.0,
                    )
                })
                .collect();
            let (resolved, unresolved) = crate::txany::fold_index_rows(&served, chunk, &extract);
            for (txid, raw, height) in resolved {
                leg.hits.insert(txid, (raw, height));
            }
            next_pending.extend(unresolved);
        }
        pending = next_pending;
    }
    leg
}

/// bsv-low W-C.3 (2026-09-09/10) — `GET /tx-any?txids=<txid>,…` (≤
/// `TX_ANY_BATCH_MAX`): the single route's answer for MANY txids in ONE round
/// trip, INDEX-ONLY. The home surface of a lived-in identity asked
/// `/tx-any/:txid` once per watched tx per pass (92 distinct reads in a 4 s
/// burst per seat on the traced hand); the vast majority are index-held mined
/// txs, and those are answered here by the SAME decision the single route
/// makes for an index hit (`batch_index_answer` == `decide_tx_any` on the
/// index leg alone, pinned) and written to the SAME cache. Every txid the
/// index did not DECIDE (a miss, a proofless row, a D1 fault) is listed under
/// `unknown` with its reason, never a null and never a guess: the client's
/// per-txid read does the external work on its own single-flighted pace
/// (gate MED-1: the batch never touches a courier, so it can neither amplify
/// courier load nor outrun the client's 8 s abort). `no-store` like every
/// answer here.
pub async fn tx_any_batch(req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    let url = match req.url() {
        Ok(u) => u,
        Err(_) => return json_error("unreadable request url", 400),
    };
    let Some(param) = url
        .query_pairs()
        .find(|(k, _)| k == "txids")
        .map(|(_, v)| v.into_owned())
    else {
        return json_error("missing txids query parameter", 400);
    };
    let keys = match crate::txany::parse_txids(&param) {
        Ok(k) => k,
        Err(msg) => return json_error(&msg, 400),
    };
    let mut answers: Vec<(String, TxAnyCached)> = Vec::with_capacity(keys.len());
    let mut misses: Vec<String> = Vec::new();
    let now = worker::Date::now().as_millis() as f64;
    for k in &keys {
        match tx_any_cached(k, now) {
            Some(a) => answers.push((k.clone(), a)),
            None => misses.push(k.clone()),
        }
    }
    let leg = tx_any_index_leg_batch(&ctx, &misses).await;
    let mut unknown: Vec<(String, &'static str)> = Vec::new();
    let mut undecided: Vec<(String, Option<String>, &'static str)> = Vec::new();
    for k in misses {
        let hit = leg.hits.get(&k).cloned();
        match crate::txany::batch_index_answer(
            hit.as_ref().map(|(raw, _)| raw.clone()),
            hit.as_ref().and_then(|(_, h)| *h),
        ) {
            Some(a) => {
                let now = worker::Date::now().as_millis() as f64;
                cache_tx_any_answer(&k, &a, now);
                answers.push((k, a));
            }
            None => {
                let reason = if leg.faulted.contains(&k) {
                    "index-fault"
                } else if hit.is_some() {
                    "index-unproven"
                } else {
                    "index-miss"
                };
                undecided.push((k, hit.map(|(raw, _)| raw), reason));
            }
        }
    }
    // bsv-low #451 slice C (v): what the index could not decide, a verdict memo may — ONE `IN` read; a dead story is
    // answered here, terminally, and never falls to a single read (the boot's singles on a lived-in identity).
    if !undecided.is_empty() {
        let now_ms = worker::Date::now().as_millis() as i64;
        let txids: Vec<String> = undecided.iter().map(|(k, _, _)| k.clone()).collect();
        let memos = read_verdict_memos(&ctx.env, &txids, now_ms).await;
        for (k, index_raw, reason) in undecided {
            match memos.iter().find(|m| m.txid == k) {
                Some(m) => {
                    crate::courier::note_presence(&crate::txany::memo_counter(
                        "hit",
                        m.kind.as_str(),
                    ));
                    let a = crate::txany::answer_from_verdict_memo(m, index_raw);
                    cache_tx_any_answer(&k, &a, now_ms as f64);
                    answers.push((k, a));
                }
                None => unknown.push((k, reason)),
            }
        }
    }
    json_response(
        crate::tracker_host::runtime::batch_body(
            &ctx.env,
            &crate::txany::tx_any_batch_body(&answers, &unknown),
            &keys,
        )
        .await?,
        200,
    )
}

/// `GET /health`: liveness, the isolate's tallies, and (bsv-low #777) the chain-event intake's durable row and counters
/// (`chainEvents`): ONE D1 batch, at most once per 10 s per isolate (the W15E-L4 lens fold's LOW-2).
pub async fn health(_req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    // #318 (Rule 13 — surface, don't consume): the auth mode + per-isolate
    // counters ride the health body, so "unauthenticated but accepted" is a
    // number the operator watches during the lenient soak, never a silent
    // accept. The flip criterion is written at `crate::auth`'s module docs.
    let mut body: serde_json::Value =
        serde_json::from_str(&health_body()).unwrap_or_else(|_| serde_json::json!({}));
    body["auth"] = crate::auth::auth_health_json(
        ctx.data.mode,
        ctx.data.auth_configured,
        &crate::auth::counters_snapshot(),
    );
    // bsv-low M18-2 B (the filing gate's MED-1): the filings per family, the
    // refusals, the cap hits, and how many filings came from an anonymous
    // caller — the poster claim is not a binding, the signature is, so the
    // operator watches the count rather than trusting the word.
    body["record"] = crate::record_post::record_health_json();
    // bsv-low #469: the owed list's recomputes by source, the rows written by family, the faults.
    body["owed"] = crate::owed::owed_health_json();
    // bsv-low #911 (D2-ACTOR-A, MEDIUM-8): the owed events' totals and served ages.
    // D2-ACTOR-A-FOLD-4: the ages from the one dirty snapshot `owed.dirty` reads and the consumer's row (one
    // primary-key read); the totals, the fault memo and the dead letters are the overlay's
    // `/health/invariants.owedEvents`.
    let dirty = match D1Sql::of(&ctx.env) {
        Ok(sql) => crate::owed_judge::dirty_health(&sql).await,
        Err(_) => serde_json::Value::Null,
    };
    // D2-ACTOR-B-FOLD-6: the owed actor's health snapshot rides the consumer row's statement (one row more, no
    // statement more: the land tier's `health-dirty-snapshot`).
    // FIX-998-SERVE (bsv-low #998): the fleet compare's public counts, the ring's snapshot row, a third row of it.
    let (owed_events_body, actor_snapshot, fleet_compare) = match D1Sql::of(&ctx.env) {
        Ok(sql) => crate::owed_events::health_actor_fleet(&sql, &dirty).await,
        Err(_) => (serde_json::Value::Null, None, serde_json::Value::Null),
    };
    body["owedEvents"] = owed_events_body;
    body["owed"]["dirty"] = dirty;
    // bsv-low #912 (D2-ACTOR-B; the lens's MEDIUM-1, MEDIUM-7, MEDIUM-8): the FLEET's bars from their durable class
    // rows (`direct`, `queue`, `mined`; this isolate's under `isolate`), the compare's durable totals (Phase B's
    // gate), the bootstraps beside the identities holding rows, and the actor's own oldest unapplied event (Phases A
    // and B; Phase C's is the fleet's oldest un-acked mark): the writers' snapshot row, read above (D2-ACTOR-B-FOLD-6).
    let d1_judges = crate::owed_actor::d1_judge_on(
        ctx.env
            .var(crate::owed_actor::D1_JUDGE_FLAG)
            .ok()
            .map(|v| v.to_string()),
    );
    body["owedActor"] = crate::owed_actor::health_body(
        actor_snapshot.as_ref(),
        worker::Date::now().as_millis(),
        &body["owedEvents"]["oldestUnackedMarkAgeMs"],
    );
    body["owedActor"]["readFlag"] = serde_json::json!(crate::owed_actor::flag_on(
        ctx.env
            .var(crate::owed_actor::READ_FLAG)
            .ok()
            .map(|v| v.to_string())
    ));
    body["owedActor"]["d1Judge"] = serde_json::json!(d1_judges);
    body["owedActor"]["bound"] =
        serde_json::json!(ctx.env.durable_object(crate::owed_actor::BINDING).is_ok());
    // FIX-998-SERVE: `owedActor.fleetCompare` {identities, diffs, heldByLag, oldestCompareAgeMs, revolution,
    // lastRevolutionMs, lastTickAt, faults, ..}, the summary route's figures (`null` before the ring's first tick).
    body["owedActor"]["fleetCompare"] = fleet_compare;
    // The flags at the design's named place (the harness reads `owedActor.flags.OWED_ACTOR_READ`).
    body["owedActor"]["flags"] = serde_json::json!({
        crate::owed_actor::READ_FLAG: body["owedActor"]["readFlag"],
        crate::owed_actor::D1_JUDGE_FLAG: body["owedActor"]["d1Judge"],
    });
    // bsv-low #499: this isolate's D1 rows ledger, the per-route running maxima since boot (the overlay serves the
    // same body on `/health/invariants.d1Budget`; `scripts/d1-census.py` reads both).
    body["d1Budget"] = crate::d1_ledger::budget_json();
    // bsv-low #451 slice B: the isolate's courier tally (the durable one is the overlay's /health/invariants).
    body["couriers"] = crate::courier::health_json();
    // bsv-low #935: this isolate's push coalescer (the durable totals are the overlay's `/health/invariants.push`).
    body["push"] = crate::push_coalesce::health_json();
    body["trackerReask"] = crate::tracker_host::pacing::health_json();
    // bsv-low #862 (L7-DEPTH): the spend depth the served words carry (`spendDepth`), its source and the var as set.
    body["spendDepth"] = crate::tracker_host::runtime::spend_depth_health_json(
        ctx.env
            .var(crate::tracker_host::runtime::SPEND_DEPTH_VAR)
            .ok()
            .map(|v| v.to_string())
            .as_deref(),
    );
    // bsv-low #771 (L1b), #777: the chain-event intake from its DURABLE row and counters (one batch; a fault serves
    // this isolate's memo, marked `source: "isolate"`), behind this isolate's 10 s memo of that read (the W15E-L4 lens
    // fold's LOW-2: one D1 batch per isolate per 10 s whatever the read rate).
    body["chainEvents"] = match D1Sql::of(&ctx.env) {
        Ok(sql) => crate::tracker_host::chain_events::health_served(&sql).await,
        Err(e) => crate::tracker_host::chain_events::isolate_health(format!("no OVERLAY_DB: {e}")),
    };
    // bsv-low #497: the exchange rate's sample, its age and every rung's counts (this isolate's).
    body["rate"] = crate::rate::health_json(worker::Date::now().as_millis() as f64);
    // bsv-low #532: the identity views' outcomes per route and the budget (this isolate's).
    body["identity"] = crate::identity::health_json();
    // bsv-low #670 (round 2, question 6): the TEST-ONLY knobs this deploy honours (empty on prod: no beta marker).
    body["testKnobs"] = serde_json::json!(crate::auth::test_knobs_honored(&ctx.env));
    // #375 (review MED-2's surface half): the ACTIVE era cutoff — post the
    // future-cutoff belt, i.e. exactly what the views are filtering by and
    // what /epoch serves. `null` = write-off inert. One glance answers
    // "did my var take effect", so a refused misconfiguration is visible
    // here rather than only in a per-request warn log.
    body["writtenOffBeforeMs"] = match written_off_before_ms(&ctx) {
        Some(ms) => serde_json::json!(ms),
        None => serde_json::Value::Null,
    };
    body["writtenOffBeforeHeight"] = match written_off_before_height(&ctx) {
        Some(h) => serde_json::json!(h),
        None => serde_json::Value::Null,
    };
    json_response(body.to_string(), 200)
}

/// `GET /rate` — bsv-low #497: USD per BSV from the app layer's courier ladder behind its cache (`crate::rate`).
pub(crate) async fn rate(_req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    crate::rate::serve(&ctx).await
}

/// `GET /epoch` — the storage-epoch directive (bsv-low THE ORDER item 2,
/// owner-ruled 2026-08-06). Public + static: the `STORAGE_EPOCH` var,
/// verbatim (trimmed), or LITERAL `null` when unset/empty — the client's
/// fail-safe "no wipe directive". No D1 touch, no auth (a wipe directive is
/// not identity-scoped), `no-store` via `json_response` so a bump propagates
/// on the next probe. Bumping the var in wrangler.toml orders every client
/// to clear its local `low_*` state at its next idle home visit
/// (bsv-low `app/src/lib/storageEpoch.ts`).
///
/// #375 (ADDITIVE): the body also carries `writtenOffBeforeMs` — the
/// pre-launch era write-off cutoff the money-listing views filter by, or
/// `null` when unset (the client's fail-safe "no write-off"). The deployed
/// client reads only `storageEpoch` and ignores unknown fields.
pub fn epoch(_req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    let v = crate::logic::normalize_storage_epoch(
        ctx.env.var("STORAGE_EPOCH").ok().map(|v| v.to_string()),
    );
    json_response(
        crate::logic::epoch_body(
            v.as_deref(),
            written_off_before_ms(&ctx),
            written_off_before_height(&ctx),
        ),
        200,
    )
}

/// Catch-all: JSON 404 for any unknown route/method.
pub fn not_found(req: Request, _ctx: RouteContext<AuthState>) -> Result<Response> {
    json_error(&format!("no such route: {}", req.path()), 404)
}

#[cfg(test)]
mod owed_mapper_tests {
    use super::*;

    #[test]
    fn the_hop_rows_marker_time_is_converted_from_the_tables_seconds_to_ms() {
        // bsv-low #469 (the gate's HIGH-3): `hopparty_records.createdAt` is unix SECONDS; the owed list's stranded
        // rule compares MILLISECONDS. A hop filed 60 s ago must not read as 56 years old.
        let row = HopsViewRowD1 {
            identity: "02".repeat(33),
            game_id: "01".repeat(32),
            hop_txid: "07".repeat(32),
            hop_vout: 0.0,
            hop_sats: 20_190.0,
            opponent_identity: "03".repeat(33),
            seat_settle_pubkey: String::new(),
            seat_sig_hex: String::new(),
            identity_sig_hex: String::new(),
            marker_txid: String::new(),
            marker_vout: 0.0,
            hop_lock_hex: None,
            hop_sats_on_chain: None,
            container_outputs: 1.0,
            marker_valid: Some(1.0),
            spent: Some(0.0),
            spending_txid: None,
            spent_confirmed: None,
            spender_proof_verified: None,
            spender_seen: None,
            spender_final: None,
            marker_created_at: Some(1_758_000_000.0),
        };
        // an older row without the column maps to None (never a guess)
        let older = HopsViewRowD1 {
            marker_created_at: None,
            ..row
        };
        assert_eq!(older.into_row().marker_created_at, None);
        let row = HopsViewRowD1 {
            identity: "02".repeat(33),
            game_id: "01".repeat(32),
            hop_txid: "07".repeat(32),
            hop_vout: 0.0,
            hop_sats: 20_190.0,
            opponent_identity: "03".repeat(33),
            seat_settle_pubkey: String::new(),
            seat_sig_hex: String::new(),
            identity_sig_hex: String::new(),
            marker_txid: String::new(),
            marker_vout: 0.0,
            hop_lock_hex: None,
            hop_sats_on_chain: None,
            container_outputs: 1.0,
            marker_valid: Some(1.0),
            spent: Some(0.0),
            spending_txid: None,
            spent_confirmed: None,
            spender_proof_verified: None,
            spender_seen: None,
            spender_final: None,
            marker_created_at: Some(1_758_000_000.0),
        };
        let mapped = row.into_row();
        assert_eq!(mapped.marker_created_at, Some(1_758_000_000_000));
        let now_ms: i64 = 1_758_000_060_000; // 60 s later
        let age = now_ms - mapped.marker_created_at.unwrap();
        assert!(
            age < crate::owed::HOP_STRANDED_AFTER_MS,
            "a minute-old hop is not stranded"
        );
        assert!(
            now_ms + 31 * 60 * 1000 - mapped.marker_created_at.unwrap()
                >= crate::owed::HOP_STRANDED_AFTER_MS
        );
    }
}

#[cfg(test)]
mod tests {
    // ── /live-view asks the tower BY OUTPOINT (2026-08-12) ────────────────
    #[test]
    fn tower_case_url_is_outpoint_scoped() {
        let g = "9c476edee524f3fbdf0a5609eec431c7b344272b83a167810c8febc898ba0c25";
        let pot = "021f165bd921c592af56aff093648b124e001d6dab738cd9d1987a900793483b";
        // The path the tower actually answers. Measured 2026-08-12 against a
        // real enforced settle: `/case/<gid>` → 404, `/case/<gid>/<txid>/<vout>`
        // → the full J. Asking by name is why every live-view row's case was
        // null.
        assert_eq!(
            super::tower_case_url(g, pot, 0).unwrap(),
            format!("https://tower/case/{g}/{pot}/0")
        );
        assert!(super::tower_case_url(g, pot, 3).unwrap().ends_with("/3"));
    }

    /// A malformed id is never interpolated into the URL — the caller does not
    /// ask at all, which `apply_cases` reports as `tower-unavailable`.
    #[test]
    fn tower_case_url_refuses_malformed_ids() {
        let ok = "9c476edee524f3fbdf0a5609eec431c7b344272b83a167810c8febc898ba0c25";
        assert!(super::tower_case_url("nope", ok, 0).is_none());
        assert!(super::tower_case_url(ok, "nope", 0).is_none());
        assert!(super::tower_case_url(ok, "", 0).is_none());
        // No path traversal / injection can reach the tower.
        assert!(super::tower_case_url(ok, "../../admin", 0).is_none());
    }

    use super::*;

    /// bsv-low#304: the SHIPPED trusted-BEEF reads (`/tx-any` index leg +
    /// `/beef`) on the PRODUCTION schema — both tables' verified latches
    /// surface under the shared `proofVerified` alias, and the latch is the
    /// one the verifying writers own: a raw admit-shaped insert leaves it 0
    /// (the fake-bumped case answers untrusted), the verified flip turns
    /// it 1. Executes the exact production strings under real SQLite.
    #[test]
    fn trusted_beef_reads_surface_the_verified_latch_real_sqlite() {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        for sql in bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                let msg = e.to_string().to_ascii_lowercase();
                assert!(
                    msg.contains("duplicate column"),
                    "production migration failed under real SQLite: {e}\n{sql}"
                );
            }
        }
        // A fake-bumped pot row as the ungated admit paths leave it:
        // structural has_proof = 1, verified latch DEFAULT 0.
        conn.execute(
            "INSERT INTO pot_beefs (txid, beef, createdAt, has_proof) VALUES ('pot1', x'beef', 1, 1)",
            [],
        )
        .unwrap();
        // A transactions row as the untrusted admit path writes it
        // (has_proof forced 0 — d1_storage::insert_output).
        conn.execute(
            "INSERT INTO transactions (txid, beef, has_proof) VALUES ('tx1', x'beef', 0)",
            [],
        )
        .unwrap();

        let read = |sql: &str, txid: &str| -> (Option<String>, Option<i64>) {
            conn.query_row(sql, rusqlite::params![txid], |r| Ok((r.get(0)?, r.get(1)?)))
                .unwrap()
        };

        // Both rows answer UNTRUSTED — the pot row's STRUCTURAL has_proof=1
        // does not leak through (the #304 hole: the old read had no flag at
        // all and served the bump height regardless).
        let (beef, verified) = read(POT_BEEFS_TRUST_SQL, "pot1");
        assert_eq!(beef.as_deref(), Some("BEEF"));
        assert_eq!(
            verified,
            Some(0),
            "structural has_proof must NOT read as trust"
        );
        let (_, verified) = read(TRANSACTIONS_TRUST_SQL, "tx1");
        assert_eq!(verified, Some(0));

        // The verifying writers latch — the same reads now answer trusted.
        conn.execute(
            "UPDATE pot_beefs SET proof_verified = 1 WHERE txid = 'pot1'",
            [],
        )
        .unwrap();
        conn.execute(
            "UPDATE transactions SET has_proof = 1 WHERE txid = 'tx1'",
            [],
        )
        .unwrap();
        assert_eq!(read(POT_BEEFS_TRUST_SQL, "pot1").1, Some(1));
        assert_eq!(read(TRANSACTIONS_TRUST_SQL, "tx1").1, Some(1));
    }

    /// `/refund-view`'s D1-row → host-row mapping, field by field: a
    /// representative D1-shaped JSON object (every value DISTINCT, so any
    /// swapped-field bug fails an assertion) deserialized exactly as the
    /// route does, then `into_row` checked slot by slot.
    #[test]
    fn refund_view_d1_row_maps_every_field_to_the_right_slot() {
        let full: RefundViewRowD1 = serde_json::from_value(serde_json::json!({
            "gameId": "game-1",
            "potTxid": "pot-1",
            "potVout": 2.0,
            "recoveryHeight": 900_123.0,
            "covRecoveryHeight": 900_200.0,
            "spent": 1.0,
            "spendingTxid": "spender-1",
            "spentConfirmed": 0.0,
            "verdict": "refund",
            "verdictTxid": "verdict-txid-1",
            "spentHeight": 900_170.0,
            "backupMarkerPresent": 1.0,
        }))
        .expect("deserialize D1-shaped row");
        let r = full.into_row();
        assert_eq!(r.game_id, "game-1");
        assert_eq!(r.pot_txid, "pot-1");
        assert_eq!(r.pot_vout, 2);
        assert_eq!(r.marker_recovery_height, 900_123);
        assert_eq!(r.cov_recovery_height, Some(900_200));
        assert_eq!(r.spent, Some(true));
        assert_eq!(r.spending_txid.as_deref(), Some("spender-1"));
        assert_eq!(
            r.spent_confirmed,
            Some(false),
            "0.0 maps to Some(false), not None"
        );
        assert_eq!(r.verdict.as_deref(), Some("refund"));
        assert_eq!(r.verdict_txid.as_deref(), Some("verdict-txid-1"));
        assert_eq!(r.spent_height, Some(900_170));
        assert!(r.backup_marker_present);

        // The all-NULL pot side (join miss): every Option arrives None —
        // never a fabricated false/0 fact — and the presence bit is false.
        let sparse: RefundViewRowD1 = serde_json::from_value(serde_json::json!({
            "gameId": "game-2",
            "potTxid": "pot-2",
            "potVout": 0.0,
            "recoveryHeight": 0.0,
            "covRecoveryHeight": null,
            "spent": null,
            "spendingTxid": null,
            "spentConfirmed": null,
            "verdict": null,
            "verdictTxid": null,
            "spentHeight": null,
            "backupMarkerPresent": 0.0,
        }))
        .expect("deserialize sparse row");
        let r = sparse.into_row();
        assert_eq!(r.cov_recovery_height, None);
        assert_eq!(r.spent, None);
        assert_eq!(r.spending_txid, None);
        assert_eq!(r.spent_confirmed, None);
        assert_eq!(r.verdict, None);
        assert_eq!(r.verdict_txid, None);
        assert_eq!(r.spent_height, None);
        assert!(!r.backup_marker_present);
    }

    /// `/live-view`'s D1-row → host-row mapping, field by field — same
    /// discipline as the refund-view test above (every value DISTINCT so a
    /// swapped-field bug fails; the NULL pot side maps to None, never a
    /// fabricated fact).
    #[test]
    fn live_view_d1_row_maps_every_field_to_the_right_slot() {
        let full: LiveViewRowD1 = serde_json::from_value(serde_json::json!({
            "identity": "me-1",
            "gameId": "game-1",
            "potTxid": "pot-1",
            "potVout": 2.0,
            "opponentIdentity": "opp-1",
            "recoveryHeight": 900_123.0,
            "covRecoveryHeight": 900_200.0,
            "sigHex": "idsig-1",
            "seatSettlePubkey": "settlepk-1",
            "seatSigHex": "seatsig-1",
            "covPubA": "covpuba-1",
            "covPubB": "covpubb-1",
            "spent": 1.0,
            "spendingTxid": "spender-1",
            "spentConfirmed": 0.0,
        }))
        .expect("deserialize D1-shaped row");
        let r = full.into_row();
        assert_eq!(r.identity, "me-1");
        assert_eq!(r.game_id, "game-1");
        assert_eq!(r.pot_txid, "pot-1");
        assert_eq!(r.pot_vout, 2);
        assert_eq!(r.opponent_identity.as_deref(), Some("opp-1"));
        assert_eq!(r.marker_recovery_height, 900_123);
        assert_eq!(r.cov_recovery_height, Some(900_200));
        assert_eq!(r.identity_sig_hex.as_deref(), Some("idsig-1"));
        assert_eq!(r.seat_settle_pubkey.as_deref(), Some("settlepk-1"));
        assert_eq!(r.seat_sig_hex.as_deref(), Some("seatsig-1"));
        assert_eq!(r.cov_pub_a.as_deref(), Some("covpuba-1"));
        assert_eq!(r.cov_pub_b.as_deref(), Some("covpubb-1"));
        assert_eq!(r.spent, Some(true));
        assert_eq!(r.spending_txid.as_deref(), Some("spender-1"));
        assert_eq!(
            r.spent_confirmed,
            Some(false),
            "0.0 maps to Some(false), not None"
        );

        let sparse: LiveViewRowD1 = serde_json::from_value(serde_json::json!({
            "identity": null,
            "gameId": "game-2",
            "potTxid": "pot-2",
            "potVout": 0.0,
            "opponentIdentity": null,
            "recoveryHeight": 0.0,
            "covRecoveryHeight": null,
            "sigHex": null,
            "seatSettlePubkey": null,
            "seatSigHex": null,
            "covPubA": null,
            "covPubB": null,
            "spent": null,
            "spendingTxid": null,
            "spentConfirmed": null,
        }))
        .expect("deserialize sparse row");
        let r = sparse.into_row();
        assert_eq!(
            r.identity, "",
            "NULL identity degrades (fails corroboration), never faults"
        );
        assert_eq!(r.opponent_identity, None);
        assert_eq!(r.cov_recovery_height, None);
        assert_eq!(r.identity_sig_hex, None);
        assert_eq!(r.seat_settle_pubkey, None);
        assert_eq!(r.seat_sig_hex, None);
        assert_eq!(r.cov_pub_a, None);
        assert_eq!(r.cov_pub_b, None);
        assert_eq!(r.spent, None);
        assert_eq!(r.spending_txid, None);
        assert_eq!(r.spent_confirmed, None);
        // A v1/sparse marker row contributes NO candidate of its own (the
        // pot's real v2 marker must come from the candidate query — HIGH-A),
        // and a keyless row lands in the keyless half of the plan.
        assert!(r.own_marker().is_none());
        let plan = crate::live_view::candidate_plan(std::slice::from_ref(&r));
        assert!(plan.keyed.is_empty());
        assert_eq!(
            plan.keyless,
            vec![vec![(r.pot_txid.to_ascii_lowercase(), 0)]]
        );
    }
}

/// bsv-low #606: the tx-any courier leg and the chain rung over a captured courier, and the owed row judged from the
/// bytes they read. Each RED on `22742afd` with only the port grafted (the leg's rule and the rung's raw kept as they
/// were): the mempool body is a Fault, no bytes, "could not judge the spender".
#[cfg(test)]
mod e606_tests {
    use super::*;
    use crate::hops_view::HopStatus;
    use crate::owed::tests::{chain_confirmed, hop, inputs, tx};
    use crate::owed::{
        derive_owed_rows, OwedFamily, HOP_STRANDED_AFTER_MS, SPENT_ELSEWHERE_REASON,
    };
    use crate::txany::{decide_tx_any, AbsenceCorroboration, TxObservation};
    use bsv_rs::script::{LockingScript, Script, UnlockingScript};
    use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
    use std::collections::{HashMap, HashSet};

    /// The seat's competitor: spends the hop `tx(0x07):0` and pays 20,190 sat to a wallet address that is none of the
    /// game's homes.
    fn competitor() -> (String, Vec<u8>) {
        let mut t = Transaction::new();
        t.inputs.push(TransactionInput {
            source_txid: Some(tx(0x07)),
            source_output_index: 0,
            unlocking_script: Some(UnlockingScript::from_script(Script::new())),
            ..Default::default()
        });
        t.outputs.push(TransactionOutput {
            satoshis: Some(20_190),
            locking_script: LockingScript::from_binary(&overlay_discovery::pot::p2pkh_lock(
                &[0x99u8; 20],
            ))
            .unwrap(),
            change: false,
        });
        let raw = t.to_binary();
        (t.id().to_ascii_lowercase(), raw)
    }
    #[test]
    fn ns2_614_arcade_seen_outranks_courier_404s() {
        WOC_ROUTE_HEALTHY.with(|c| c.set(Some(true)));
        let (txid, _raw) = competitor();
        let now = crate::txany::rfc3339_utc_ms("2026-10-12T18:15:00Z").unwrap();
        let get = |url: String| {
            let body = if url.contains("/tx/hash/") {
                (404, vec![])
            } else if url
                == format!(
                    "{BITAILS_BASE}/download/tx/{}",
                    crate::txany::KNOWN_MINED_TXID
                )
            {
                (200, vec![1])
            } else if url.starts_with(&arcade_url()) {
                (
                    200,
                    serde_json::json!({"txid":txid,"txStatus":"SEEN_ON_NETWORK",
                    "timestamp":"2026-10-12T18:15:00Z"})
                    .to_string()
                    .into_bytes(),
                )
            } else {
                (404, vec![])
            };
            async move { Some(body) }
        };
        let (obs, absence, _) = run(tx_any_presence_leg_via(
            &txid,
            now,
            true,
            None,
            get,
            |_, _| async { false },
        ));
        let a = crate::txany::decide_tx_any(None, None, Some(&obs), absence);
        let would_write_absent_memo = a.present == Some(false)
            && absence == crate::txany::AbsenceCorroboration::CorroboratedAbsent;
        eprintln!(
            "pair13 present={:?}, base request-time Absent writer guard={would_write_absent_memo}",
            a.present
        );
        assert!(!would_write_absent_memo);
        assert_eq!(a.present, Some(true));
        assert_eq!(a.confirmed, Some(false));
        assert_eq!(a.raw_hex, None);
        assert_eq!(absence, crate::txany::AbsenceCorroboration::Unknown);
    }

    #[test]
    fn ns2_614_arcade_unknown_and_two_404s_are_unknown_not_memoable() {
        WOC_ROUTE_HEALTHY.with(|c| c.set(Some(true)));
        let get = |url: String| async move {
            if url
                == format!(
                    "{BITAILS_BASE}/download/tx/{}",
                    crate::txany::KNOWN_MINED_TXID
                )
            {
                Some((200, vec![1]))
            } else {
                Some((404, vec![]))
            }
        };
        let (obs, absence, _) = run(tx_any_presence_leg_via(
            &tx(0x11),
            0,
            true,
            None,
            get,
            |_, _| async { false },
        ));
        let a = crate::txany::decide_tx_any(None, None, Some(&obs), absence);
        assert_eq!(a.present, None);
        assert_eq!(absence, crate::txany::AbsenceCorroboration::Unknown);
    }

    #[test]
    fn ns2_614_seen_without_courier_bytes_is_still_a_presence_hint() {
        let txid = tx(0x11);
        let now = crate::txany::rfc3339_utc_ms("2026-10-12T18:15:00Z").unwrap();
        let urls = std::cell::RefCell::new(Vec::new());
        let get = |url: String| {
            urls.borrow_mut().push(url.clone());
            let body = if url.starts_with(&arcade_url()) {
                (
                    200,
                    serde_json::json!({"txid":txid,"txStatus":"SEEN_ON_NETWORK",
                    "timestamp":"2026-10-12T18:15:00Z"})
                    .to_string()
                    .into_bytes(),
                )
            } else {
                (404, vec![])
            };
            async move { Some(body) }
        };
        let (obs, absence, _) = run(tx_any_presence_leg_via(
            &txid,
            now,
            true,
            None,
            get,
            |_, _| async { false },
        ));
        let a = crate::txany::decide_tx_any(None, None, Some(&obs), absence);
        assert_eq!(a.present, Some(true));
        assert_eq!(a.confirmed, Some(false));
        assert_eq!(a.raw_hex, None);
        assert_eq!(urls.borrow()[0], format!("{}/tx/{txid}", arcade_url()));
    }

    #[test]
    fn ns2_614_mined_needs_verified_proof_and_real_absence_needs_capable_sources() {
        let txid = tx(0x11);
        let now = crate::txany::rfc3339_utc_ms("2026-10-12T18:15:00Z").unwrap();
        let get = |url: String| {
            let body = if url.starts_with(&arcade_url()) {
                (
                    200,
                    serde_json::json!({"txid":txid,"txStatus":"MINED",
                    "blockHeight":970733,"merklePath":"aa"})
                    .to_string()
                    .into_bytes(),
                )
            } else {
                (404, vec![])
            };
            async move { Some(body) }
        };
        let (obs, absence, height) = run(tx_any_presence_leg_via(
            &txid,
            now,
            true,
            None,
            get,
            |_, _| async { true },
        ));
        let a = crate::txany::decide_tx_any(None, None, Some(&obs), absence);
        assert_eq!(a.confirmed, Some(true));
        assert_eq!(height, Some(970733));
        let (obs, absence, height) = run(tx_any_presence_leg_via(
            &txid,
            now,
            true,
            None,
            get,
            |_, _| async { false },
        ));
        assert_eq!(
            crate::txany::decide_tx_any(None, None, Some(&obs), absence).present,
            None
        );
        assert_eq!(height, None);

        WOC_ROUTE_HEALTHY.with(|c| c.set(Some(true)));
        let get = |url: String| {
            let body = if url == format!("{}/tx/{}", arcade_url(), crate::txany::KNOWN_MINED_TXID) {
                (
                    200,
                    serde_json::json!({"txid":crate::txany::KNOWN_MINED_TXID,
                    "txStatus":"MINED","blockHeight":958886,"merklePath":"aa"})
                    .to_string()
                    .into_bytes(),
                )
            } else {
                (404, vec![])
            };
            async move { Some(body) }
        };
        let (obs, absence, _) = run(tx_any_presence_leg_via(
            &txid,
            now,
            true,
            None,
            get,
            |_, _| async { false },
        ));
        assert_eq!(absence, crate::txany::AbsenceCorroboration::Unknown);
        assert_eq!(
            crate::txany::decide_tx_any(None, None, Some(&obs), absence).present,
            None
        );
    }

    #[test]
    fn ns2_m1_scope_and_held_anchor_use_local_rows() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE transactions (txid TEXT PRIMARY KEY, has_proof INTEGER); CREATE TABLE network_seen (txid TEXT PRIMARY KEY); CREATE TABLE pot_beefs (txid TEXT PRIMARY KEY); CREATE TABLE submit_refusals (refusedTxid TEXT);
            INSERT INTO transactions VALUES ('unverified',0); INSERT INTO network_seen VALUES ('unverified'); INSERT INTO submit_refusals VALUES ('submitted');").unwrap();
        let missing: rusqlite::Result<String> =
            db.query_row(TX_ANY_HELD_ANCHOR_SQL, [], |r| r.get(0));
        assert!(
            matches!(missing, Err(rusqlite::Error::QueryReturnedNoRows)),
            "an unverified row is not a HELD anchor"
        );
        db.execute_batch(
            "INSERT INTO transactions VALUES ('held',1); INSERT INTO network_seen VALUES ('held');",
        )
        .unwrap();
        for (id, expected) in [("held", true), ("submitted", true), ("foreign", false)] {
            let exists = db
                .prepare(TX_ANY_OURS_SQL)
                .unwrap()
                .exists([id, id, id])
                .unwrap();
            assert_eq!(exists, expected);
        }
        let anchor: String = db
            .query_row(TX_ANY_HELD_ANCHOR_SQL, [], |r| r.get(0))
            .unwrap();
        assert_eq!(anchor, "held");
    }

    #[test]
    fn ns2_m1_l4_production_binds_both_arcade_consumers_and_has_no_absent_writer() {
        let src = include_str!("routes.rs");
        let indexed = &src[src.find("async fn arcade_confirmation_look(").unwrap()
            ..src.find("async fn provider_get(").unwrap()];
        let external = &src[src.find("async fn tx_any_external_leg(").unwrap()
            ..src.find("async fn tx_any_presence_leg_via<").unwrap()];
        for consumer in [indexed, external] {
            assert!(consumer.contains("crate::txany::mined_proof_matches("));
            assert!(consumer.contains("crate::beef_guard::canonical_root(env, height)"));
        }
        let resolve = &src[src.find("async fn resolve_tx_any(").unwrap()
            ..src.find("pub async fn tx_any(").unwrap()];
        assert!(!resolve.contains("write_verdict_memo("));
        assert!(external.contains("tx_any_scope(env, txid_lc)"));
    }

    #[test]
    fn ns2_m1_foreign_unmined_is_a_timed_courier_hint_without_arcade() {
        let (txid, raw) = competitor();
        let urls = std::cell::RefCell::new(Vec::new());
        let get = |url: String| {
            urls.borrow_mut().push(url.clone());
            let response = if url.contains("/tx/hash/") {
                (200, mempool_body(&txid))
            } else if url.ends_with("/hex") {
                (200, hex::encode(&raw).into_bytes())
            } else {
                (404, vec![])
            };
            std::future::ready(Some(response))
        };
        let (obs, absence, _) = run(tx_any_presence_leg_via(
            &txid,
            1234,
            false,
            None,
            get,
            |_, _| async { panic!("unmined hint has no proof") },
        ));
        let a = decide_tx_any(None, None, Some(&obs), absence);
        assert_eq!(a.present, None);
        assert_eq!(a.hint_source, Some("woc"));
        assert_eq!(a.observed_at_ms, Some(1234));
        assert_eq!(a.raw_hex, Some(hex::encode(raw)));
        assert!(!urls
            .borrow()
            .iter()
            .any(|url| url.starts_with(&arcade_url())));
        assert_eq!(crate::txany::tx_any_value(&txid, &a)["tier"], "hint");
    }

    #[test]
    fn ns2_m1_held_anchor_probe_is_memoised_but_never_a_second_absence_vote() {
        ARCADE_ROUTE_HEALTHY.with(|c| c.set(None));
        WOC_ROUTE_HEALTHY.with(|c| c.set(Some(true)));
        let subject = tx(0x11);
        let held = tx(0x22); // fixture: overlay HELD, network_seen, verified BUMP.
        let anchor_reads = std::cell::Cell::new(0);
        let now = crate::txany::rfc3339_utc_ms("2026-10-12T18:15:00Z").unwrap();
        let get = |url: String| {
            let response = if url == format!("{}/tx/{held}", arcade_url()) {
                anchor_reads.set(anchor_reads.get() + 1);
                (
                    200,
                    serde_json::json!({"txid":held,"txStatus":"SEEN_ON_NETWORK",
                    "timestamp":"2026-10-12T18:15:00Z"})
                    .to_string()
                    .into_bytes(),
                )
            } else {
                (404, vec![])
            };
            std::future::ready(Some(response))
        };
        for _ in 0..2 {
            let (obs, absence, _) = run(tx_any_presence_leg_via(
                &subject,
                now,
                true,
                Some(&held),
                get,
                |_, _| async { false },
            ));
            assert_eq!(decide_tx_any(None, None, Some(&obs), absence).present, None);
            assert_eq!(absence, AbsenceCorroboration::Unknown);
        }
        assert_eq!(anchor_reads.get(), 1);
        ARCADE_ROUTE_HEALTHY.with(|c| c.set(None));
    }

    /// bsv-low #626: the anchor memo's write (the probe) and its hit (a held 404 the memo answered), and the
    /// isolate's ten-minute unconfirmable memo, counted by the producers themselves.
    #[test]
    fn obs_626_the_tx_any_memos_are_counted_by_kind() {
        ARCADE_ROUTE_HEALTHY.with(|c| c.set(None));
        WOC_ROUTE_HEALTHY.with(|c| c.set(Some(true)));
        let subject = tx(0x11);
        let held = tx(0x22);
        let now = crate::txany::rfc3339_utc_ms("2026-10-12T18:15:00Z").unwrap();
        let get = |url: String| {
            let response = if url == format!("{}/tx/{held}", arcade_url()) {
                (
                    200,
                    serde_json::json!({"txid":held,"txStatus":"SEEN_ON_NETWORK",
                    "timestamp":"2026-10-12T18:15:00Z"})
                    .to_string()
                    .into_bytes(),
                )
            } else {
                (404, vec![])
            };
            std::future::ready(Some(response))
        };
        for at in [now, now + 1, now + 2] {
            run(tx_any_presence_leg_via(
                &subject,
                at,
                true,
                Some(&held),
                get,
                |_, _| async { false },
            ));
        }
        ARCADE_ROUTE_HEALTHY.with(|c| c.set(None));
        let key = tx(0x33);
        let a = TxAnyCached {
            unconfirmable: true,
            ..Default::default()
        };
        cache_tx_any_answer(&key, &a, 0.0);
        assert!(tx_any_cached(&key, 1.0).is_some());
        let d: std::collections::BTreeMap<String, u64> = crate::courier::take_pending_if_due(0.0)
            .into_iter()
            .collect();
        assert_eq!(d["applayer_txany_memo_write_anchor_total"], 1, "{d:?}");
        assert_eq!(d["applayer_txany_memo_hit_anchor_total"], 2, "{d:?}");
        assert_eq!(
            d["applayer_txany_memo_write_isolate_unconfirmable_total"],
            1
        );
        assert_eq!(d["applayer_txany_memo_hit_isolate_unconfirmable_total"], 1);
        // the durable verdict memo's hits, by its kind, on both routes' reads
        let src: String = include_str!("routes.rs").split_whitespace().collect();
        assert!(src.contains("crate::courier::note_presence(&crate::txany::memo_counter(\"hit\",memo.kind.as_str()));"));
        assert!(src.contains(
            "crate::courier::note_presence(&crate::txany::memo_counter(\"hit\",m.kind.as_str(),));"
        ));
        assert_eq!(
            crate::txany::memo_counter("hit", crate::txany::VerdictKind::Absent.as_str()),
            "applayer_txany_memo_hit_absent_total"
        );
    }

    #[test]
    fn ns2pd_l3_negative_and_stale_rank_anchor_probes_are_bounded() {
        let subject = tx(0x11);
        let held = tx(0x22);
        let now = crate::txany::rfc3339_utc_ms("2026-10-12T18:15:00Z").unwrap();
        WOC_ROUTE_HEALTHY.with(|c| c.set(Some(true)));
        for anchor_status in [404, 200] {
            ARCADE_ROUTE_HEALTHY.with(|c| c.set(None));
            let reads = std::cell::Cell::new(0);
            let get = |url: String| {
                let response = if url == format!("{}/tx/{held}", arcade_url()) {
                    reads.set(reads.get() + 1);
                    (
                        anchor_status,
                        serde_json::json!({"txid":held,"txStatus":"ACCEPTED_BY_NETWORK",
                        "timestamp":"2026-07-22T18:32:54.263687Z"})
                        .to_string()
                        .into_bytes(),
                    )
                } else {
                    (404, vec![])
                };
                std::future::ready(Some(response))
            };
            for at in [now, now + 1] {
                let (obs, absence, _) = run(tx_any_presence_leg_via(
                    &subject,
                    at,
                    true,
                    Some(&held),
                    get,
                    |_, _| async { false },
                ));
                assert_eq!(decide_tx_any(None, None, Some(&obs), absence).present, None);
                assert_eq!(absence, AbsenceCorroboration::Unknown);
            }
            assert_eq!(
                reads.get(),
                1,
                "both negative and stale-rank success are memoised"
            );
            assert_eq!(
                ARCADE_ROUTE_HEALTHY.with(|c| c.get().unwrap().1),
                anchor_status == 200,
                "pre-SEEN on a proven anchor is stale rank, not absence"
            );
            run(tx_any_presence_leg_via(
                &subject,
                now + 30_001,
                true,
                Some(&held),
                get,
                |_, _| async { false },
            ));
            assert_eq!(reads.get(), 2, "memo expires for either outcome");
        }
        ARCADE_ROUTE_HEALTHY.with(|c| c.set(None));
    }

    #[test]
    fn ns2_m1_mined_foreign_uses_checked_courier_proof_never_arcade() {
        let (txid, raw) = competitor();
        let sibling = tx(0x44);
        let proof = serde_json::json!({"index":0,"txOrId":txid,"nodes":[sibling]})
            .to_string()
            .into_bytes();
        let bump = crate::txany::courier_tsc_bump(&proof, 970733).unwrap();
        let mp = bsv_rs::transaction::MerklePath::from_hex(&bump).unwrap();
        let root = mp.compute_root(Some(&txid)).unwrap();
        let get = |url: String| {
            assert!(!url.starts_with(&arcade_url()));
            let response = if url.contains("/tx/hash/") {
                (200, mined_body(&txid))
            } else if url.ends_with("/proof/tsc") {
                (200, proof.clone())
            } else if url.ends_with("/hex") {
                (200, hex::encode(&raw).into_bytes())
            } else {
                (404, vec![])
            };
            std::future::ready(Some(response))
        };
        let (obs, absence, height) = run(tx_any_presence_leg_via(
            &txid,
            1234,
            false,
            None,
            get,
            |bump, height| {
                std::future::ready(crate::txany::mined_proof_matches(
                    &txid, &bump, height, &root,
                ))
            },
        ));
        let a = decide_tx_any(None, None, Some(&obs), absence);
        assert_eq!(a.present, Some(true));
        assert_eq!(a.confirmed, Some(true));
        assert_eq!(height, Some(970733));
        assert_eq!(a.hint_source, None);
        let (obs, absence, _) = run(tx_any_presence_leg_via(
            &txid,
            1234,
            false,
            None,
            get,
            |_, _| async { false },
        ));
        assert_eq!(
            decide_tx_any(None, None, Some(&obs), absence).confirmed,
            None
        );
    }

    #[test]
    fn ns2pd_l2_unprovable_mined_claim_keeps_verified_content() {
        let (txid, raw) = competitor();
        for proof_status in [200, 404, 429] {
            let get = |url: String| {
                let response = if url.contains("/tx/hash/") {
                    (200, mined_body(&txid))
                } else if url.ends_with("/hex") {
                    (200, hex::encode(&raw).into_bytes())
                } else if url.ends_with("/proof/tsc") {
                    (proof_status, b"{}".to_vec())
                } else {
                    (404, vec![])
                };
                std::future::ready(Some(response))
            };
            let (obs, absence, height) = run(tx_any_presence_leg_via(
                &txid,
                1234,
                false,
                None,
                get,
                |_, _| async { false },
            ));
            let mut a = decide_tx_any(None, None, Some(&obs), absence);
            apply_external_height(&mut a, &obs, height);
            assert_eq!(a.raw_hex.as_deref(), Some(hex::encode(&raw).as_str()));
            assert_eq!(a.present, None);
            assert_eq!(a.confirmed, None);
            assert_eq!(a.height, None);
            assert_eq!(a.claimed_height, Some(970733));
            assert_eq!(a.hint_source, Some("woc"));
            assert_eq!(a.observed_at_ms, Some(1234));
        }
    }

    /// WoC's `/tx/hash` body for a MEMPOOL transaction, the keys as probed live 2026-10-12 (no `confirmations`).
    fn mempool_body(txid: &str) -> Vec<u8> {
        serde_json::json!({"hash": txid, "locktime": 0, "size": 85, "txid": txid, "version": 1,
            "vin": [{"txid": tx(0x07), "vout": 0, "scriptSig": {"asm": "", "hex": ""}, "sequence": 4294967295u64}],
            "vout": [{"value": 0.0002019, "n": 0}]})
        .to_string()
        .into_bytes()
    }
    fn mined_body(txid: &str) -> Vec<u8> {
        let mut v: serde_json::Value = serde_json::from_slice(&mempool_body(txid)).unwrap();
        for (k, x) in [
            ("blockhash", serde_json::json!("00".repeat(32))),
            ("blockheight", serde_json::json!(970_733)),
            ("blocktime", serde_json::json!(1_791_800_000u64)),
            ("time", serde_json::json!(1_791_800_000u64)),
            ("confirmations", serde_json::json!(3)),
        ] {
            v[k] = x;
        }
        v.to_string().into_bytes()
    }
    fn courier(
        map: HashMap<String, (u16, Vec<u8>)>,
    ) -> impl Fn(String) -> std::future::Ready<Option<(u16, Vec<u8>)>> {
        move |url: String| std::future::ready(map.get(&url).cloned())
    }
    fn run<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }
    fn leg(body: Vec<u8>) -> (TxObservation, String, Vec<u8>) {
        let (sp, raw) = competitor();
        let mut m = HashMap::new();
        m.insert(format!("{WOC_BASE}/tx/hash/{sp}"), (200, body));
        m.insert(
            format!("{WOC_BASE}/tx/{sp}/hex"),
            (200, hex::encode(&raw).into_bytes()),
        );
        let (obs, _, _) = run(tx_any_external_leg_via(&sp, courier(m)));
        (obs, sp, raw)
    }
    /// A courier-fetched spender, parsed once: the outpoints it SPENDS and its outputs (the shape the judge's
    /// stored spender bytes hold).
    #[derive(Clone)]
    struct CourierSpenderBytes {
        inputs: Vec<(String, u32)>,
        outputs: Vec<crate::owed::SpenderOutput>,
    }
    fn courier_spender_bytes_of_raw(raw: &[u8], sp: &str) -> Option<CourierSpenderBytes> {
        let tx = bsv_rs::transaction::Transaction::from_binary(raw).ok()?;
        if !tx.id().eq_ignore_ascii_case(sp) {
            return None;
        }
        Some(CourierSpenderBytes {
            inputs: spender_inputs_of(&tx),
            outputs: spender_outputs_of(&tx),
        })
    }
    /// The owed row of an evicted hop (index: unspent, the pointer released; chain: spent by `sp`) whose game's home
    /// is known, judged with the spender bytes `bytes` (or none).
    fn owed_reason(
        sp: &str,
        bytes: Option<CourierSpenderBytes>,
    ) -> (OwedFamily, Option<String>, serde_json::Value) {
        let key = format!("{}:0", tx(0x07));
        let old = [hop(
            HopStatus::Unspent,
            None,
            Some(HOP_STRANDED_AFTER_MS + 1),
        )];
        let by_chain = chain_confirmed(&key, true, Some(true), Some(sp), Some(false));
        let (v, c, no_pots) = (HashMap::new(), HashSet::new(), HashSet::new());
        let mut pkhs: HashMap<String, String> = HashMap::new();
        pkhs.insert(tx(0x01), "11".repeat(20));
        let mut outs: HashMap<String, Vec<crate::owed::SpenderOutput>> = HashMap::new();
        let mut ins: HashMap<String, Vec<(String, u32)>> = HashMap::new();
        let mut couriered: HashSet<String> = HashSet::new();
        if let Some(b) = bytes {
            outs.insert(sp.to_string(), b.outputs);
            ins.insert(sp.to_string(), b.inputs);
            couriered.insert(sp.to_string());
        }
        let mut i = inputs(&[], &[], &old, &v, &c, &no_pots, Some(970_720));
        i.hop_chain = &by_chain;
        i.spender_outputs = &outs;
        i.spender_inputs = &ins;
        i.courier_spenders = &couriered;
        i.my_pkh_by_game = &pkhs;
        let mut rows = derive_owed_rows(&i);
        let answer = tx_any_cached(sp, 1.0);
        mark_pending_spender(&mut rows[0], answer.as_ref(), 1234);
        assert_eq!(rows.len(), 1, "{rows:?}");
        (
            rows[0].family,
            rows[0].reason.clone(),
            rows[0].facts.clone(),
        )
    }

    /// Pin (a): the leg on WoC's captured mempool body answers an unconfirmed presence WITH the hash-verified bytes
    /// (RED on `22742afd`: `Fault`, no raw fetched). Pin (c): a mined body keeps its answer (GREEN before and after).
    #[test]
    fn e606_a_the_leg_reads_a_mempool_body_as_an_unconfirmed_presence_with_its_bytes() {
        let (obs, sp, raw) = leg(mempool_body(&competitor().0));
        assert_eq!(
            obs,
            TxObservation::Present {
                confirmed: false,
                raw_hex: Some(hex::encode(&raw))
            },
            "the mempool body"
        );
        let a = decide_tx_any(None, None, Some(&obs), AbsenceCorroboration::Unknown);
        assert_eq!(
            (a.present, a.raw_hex.as_deref()),
            (Some(true), Some(hex::encode(&raw).as_str()))
        );
        assert!(courier_spender_bytes_of_raw(&raw, &sp).is_some());
        // (c) a mined body: confirmed, its claimed height, its bytes
        let (sp2, _) = competitor();
        let mut m = HashMap::new();
        m.insert(format!("{WOC_BASE}/tx/hash/{sp2}"), (200, mined_body(&sp2)));
        m.insert(
            format!("{WOC_BASE}/tx/{sp2}/hex"),
            (200, hex::encode(&raw).into_bytes()),
        );
        let (obs, _, claimed) = run(tx_any_external_leg_via(&sp2, courier(m)));
        assert_eq!(
            obs,
            TxObservation::Present {
                confirmed: true,
                raw_hex: Some(hex::encode(&raw))
            }
        );
        assert_eq!(claimed, Some(970_733));
        // what stays a fault: no claim at all, a foreign txid's body, a non-200, an unparseable body
        assert_eq!(leg(b"{}".to_vec()).0, TxObservation::Fault);
        assert_eq!(leg(mempool_body(&"ee".repeat(32))).0, TxObservation::Fault);
        assert_eq!(leg(b"not json".to_vec()).0, TxObservation::Fault);
        let mut m = HashMap::new();
        m.insert(format!("{WOC_BASE}/tx/hash/{sp}"), (429, Vec::new()));
        assert_eq!(
            run(tx_any_external_leg_via(&sp, courier(m))).0,
            TxObservation::Fault
        );
    }

    /// Pin (b): the owed row of a hop spent by an UNCONFIRMED foreign transaction reads "spent outside this game" from
    /// the bytes, by either road: the tx-any resolver's leg (the fix) and the chain rung's verified raw (the reuse).
    /// RED on `22742afd`: the leg answers `Fault` (no bytes) and the rung discards its raw, so the row is "could not
    /// judge the spender".
    #[test]
    fn e606_b_an_unconfirmed_foreign_spender_is_judged_spent_elsewhere_from_its_bytes() {
        let (sp, raw) = competitor();
        // without bytes: the pre-fix sentence, the baseline
        let (fam, reason, _) = owed_reason(&sp, None);
        assert_eq!(fam, OwedFamily::Unbound);
        assert!(
            reason
                .as_deref()
                .unwrap_or("")
                .contains("could not judge the spender"),
            "{reason:?}"
        );
        // road 1: the resolver's leg over the mempool body
        let (obs, _, _) = leg(mempool_body(&sp));
        let a = decide_tx_any(None, None, Some(&obs), AbsenceCorroboration::Unknown);
        let bytes = a
            .raw_hex
            .as_deref()
            .and_then(|h| hex::decode(h).ok())
            .and_then(|r| courier_spender_bytes_of_raw(&r, &sp));
        let (fam, reason, facts) = owed_reason(&sp, bytes);
        assert_eq!(fam, OwedFamily::Unbound);
        assert_eq!(
            reason.as_deref(),
            Some(SPENT_ELSEWHERE_REASON),
            "road 1: {facts}"
        );
        assert_eq!(facts["spendKind"], "spent-elsewhere");
        assert!(SPENT_ELSEWHERE_REASON.contains("spent outside this game"));
        // road 2: the chain rung's probe of the hop (WoC names the unconfirmed spender, its raw verified)
        let mut m = HashMap::new();
        m.insert(
            format!("{WOC_BASE}/tx/{}/0/spent", tx(0x07)),
            (
                200,
                serde_json::json!({"txid": sp, "vin": 0, "status": "unconfirmed"})
                    .to_string()
                    .into_bytes(),
            ),
        );
        m.insert(
            format!("{WOC_BASE}/tx/{sp}/hex"),
            (200, hex::encode(&raw).into_bytes()),
        );
        let (row, spender_raw) = run(spent_any_resolve_via(&tx(0x07), 0, courier(m)));
        assert_eq!(
            (row.spent, row.spending_txid.as_deref(), row.spent_confirmed),
            (Some(true), Some(sp.as_str()), Some(false))
        );
        assert_eq!(
            spender_raw.as_deref(),
            Some(raw.as_slice()),
            "the rung keeps the raw it verified"
        );
        let (_, reason, _) = owed_reason(&sp, courier_spender_bytes_of_raw(&raw, &sp));
        assert_eq!(reason.as_deref(), Some(SPENT_ELSEWHERE_REASON), "road 2");
        // a raw that does not verify is never kept (a lying courier's bytes for another tx)
        let mut m = HashMap::new();
        m.insert(
            format!("{WOC_BASE}/tx/{}/0/spent", tx(0x07)),
            (
                200,
                serde_json::json!({"txid": sp, "vin": 0, "status": "unconfirmed"})
                    .to_string()
                    .into_bytes(),
            ),
        );
        m.insert(format!("{WOC_BASE}/tx/{sp}/hex"), (200, b"0100".to_vec()));
        assert_eq!(run(spent_any_resolve_via(&tx(0x07), 0, courier(m))).1, None);
    }

    /// bsv-low #695: the body `/spent-any` serves for one ladder row (the route's own entry shape and body builder).
    fn e695_body(row: &SpentAnyCached) -> String {
        let entry = crate::logic::OutpointStatus {
            txid: tx(0x31),
            vout: 0,
            known: row.known,
            spent: row.spent,
            spending_txid: row.spending_txid.clone(),
            spent_confirmed: row.spent_confirmed,
            spender_seen: None,
            spender_final: None,
            reason: row.reason,
            funding_seen: None,
            funding_seen_by: None,
            evicted: crate::logic::ServedEviction::NotRead,
        };
        crate::txany::spent_any_body(&[entry], &[false])
    }

    /// bsv-low #695 (a), DESIGN-641 option (b), the prod shape of 2026-10-16 (`RC-PROD-MONITOR-BLIND`: WoC `/spent`
    /// 400 for an unknown tx, BananaBlocks 404, Bitails 404) plus WoC's node answering a conclusive 404 for the tx
    /// itself: `known:false, reason:"uncorroborated-unspent"`, never `provider-fault`. The body differs from the
    /// provider-fault body by the reason alone. RED on `b4ae1c6` (run as the mutant `woc_node_absent = false`, the
    /// base's ladder): `provider-fault`.
    #[test]
    fn e695_a_a_conclusive_woc_404_of_an_unmined_subject_is_uncorroborated_unspent() {
        let t = tx(0x31);
        let mut m = HashMap::new();
        m.insert(format!("{WOC_BASE}/tx/{t}/0/spent"), (400, b"bad".to_vec()));
        m.insert(
            format!("{BANANABLOCKS_BASE}/txo/{t}/0/spend"),
            (404, Vec::new()),
        );
        m.insert(format!("{BITAILS_BASE}/tx/{t}"), (404, Vec::new()));
        m.insert(format!("{WOC_BASE}/tx/hash/{t}"), (404, Vec::new()));
        let (row, raw) = run(spent_any_resolve_via(&t, 0, courier(m.clone())));
        assert_eq!(raw, None);
        assert_eq!(
            (row.known, row.spent, row.reason),
            (
                false,
                None,
                Some(crate::logic::SPENT_ANY_REASON_UNCORROBORATED)
            )
        );
        let fault = SpentAnyCached {
            reason: Some(crate::logic::SPENT_ANY_REASON_PROVIDER_FAULT),
            ..row.clone()
        };
        assert_eq!(
            e695_body(&row),
            e695_body(&fault).replace("provider-fault", "uncorroborated-unspent"),
            "nothing else in the body changes"
        );
        assert!(
            !e695_body(&row).contains("terminal"),
            "never a terminal word"
        );
    }

    /// bsv-low #695 (b): `provider-fault` stays for a transport fault or a body-shape fault. A WoC 429 (the extra
    /// read is never made), a 5xx, a transport fault, a 200 body that does not parse; and a 400 whose node read is a
    /// 429, a 200 or a transport fault. The old bodies are byte-identical (frozen), and so is the two-source absent
    /// case (WoC 404, BananaBlocks a clean `spent:false`): `known:true, spent:false`.
    #[test]
    fn e695_b_a_fault_keeps_provider_fault_and_the_old_bodies_are_byte_identical() {
        let t = tx(0x31);
        let frozen_fault = r#"[{"known":false,"networkSeen":null,"reason":"provider-fault","spenderFinal":null,"spenderSeen":null,"spendingTxid":null,"spent":null,"spentConfirmed":null,"txid":"3131313131313131313131313131313131313131313131313131313131313131","vout":0}]"#;
        let base = |woc_spent: Option<(u16, Vec<u8>)>, node: Option<(u16, Vec<u8>)>| {
            let mut m = HashMap::new();
            if let Some(w) = woc_spent {
                m.insert(format!("{WOC_BASE}/tx/{t}/0/spent"), w);
            }
            m.insert(
                format!("{BANANABLOCKS_BASE}/txo/{t}/0/spend"),
                (404, Vec::new()),
            );
            m.insert(format!("{BITAILS_BASE}/tx/{t}"), (404, Vec::new()));
            if let Some(n) = node {
                m.insert(format!("{WOC_BASE}/tx/hash/{t}"), n);
            }
            m
        };
        let absent = Some((404u16, Vec::new()));
        for (label, woc_spent, node) in [
            ("woc 429", Some((429u16, Vec::new())), absent.clone()),
            ("woc 503", Some((503, Vec::new())), absent.clone()),
            ("woc transport", None, absent.clone()),
            (
                "woc 200 not json",
                Some((200, b"<html>".to_vec())),
                absent.clone(),
            ),
            (
                "woc 200 no txid",
                Some((200, b"{}".to_vec())),
                absent.clone(),
            ),
            (
                "400, node 429",
                Some((400, Vec::new())),
                Some((429, Vec::new())),
            ),
            (
                "400, node 200",
                Some((400, Vec::new())),
                Some((200, b"{}".to_vec())),
            ),
            ("400, node transport", Some((400, Vec::new())), None),
        ] {
            let asked = std::cell::RefCell::new(Vec::new());
            let m = base(woc_spent, node);
            let get = |url: String| {
                asked.borrow_mut().push(url.clone());
                std::future::ready(m.get(&url).cloned())
            };
            let (row, _) = run(spent_any_resolve_via(&t, 0, get));
            assert_eq!(e695_body(&row), frozen_fault, "{label}");
            let node_asked = asked.borrow().iter().any(|u| u.contains("/tx/hash/"));
            assert_eq!(
                node_asked,
                label.starts_with("400"),
                "{label}: the node read only after a 400"
            );
        }
        // the two-source absent case: WoC 404, BananaBlocks corroborates, byte-identical, no node read
        let mut m = HashMap::new();
        m.insert(format!("{WOC_BASE}/tx/{t}/0/spent"), (404, Vec::new()));
        m.insert(
            format!("{BANANABLOCKS_BASE}/txo/{t}/0/spend"),
            (200, br#"{"spent":false}"#.to_vec()),
        );
        let (row, _) = run(spent_any_resolve_via(&t, 0, courier(m)));
        assert_eq!(
            e695_body(&row),
            r#"[{"known":true,"networkSeen":null,"reason":null,"spenderFinal":null,"spenderSeen":null,"spendingTxid":null,"spent":false,"spentConfirmed":false,"txid":"3131313131313131313131313131313131313131313131313131313131313131","vout":0}]"#
        );
        // and WoC's own 404 of the OUTPOINT with no corroboration stays the reason it always was
        let mut m = HashMap::new();
        m.insert(format!("{WOC_BASE}/tx/{t}/0/spent"), (404, Vec::new()));
        let (row, _) = run(spent_any_resolve_via(&t, 0, courier(m)));
        assert_eq!(
            row.reason,
            Some(crate::logic::SPENT_ANY_REASON_UNCORROBORATED)
        );
        // the pure rule: only a provider-fault row is sharpened, only on the node's 404
        assert!(crate::results::woc_spent_may_name_an_unknown_tx(Some(400)));
        for s in [None, Some(404), Some(429), Some(500), Some(200)] {
            assert!(
                !crate::results::woc_spent_may_name_an_unknown_tx(s),
                "{s:?}"
            );
        }
    }

    /// bsv-low #723: the body `/spent-any` serves for one ladder row after the held-proof word (the route's own entry
    /// shape, `results::spent_confirmed_word` and `txany::spent_any_body_sourced`).
    fn e723_body(row: &SpentAnyCached, index: bool, tracker: bool, txany: bool) -> String {
        // the route's gate: only a KNOWN SPEND is asked for a held proof
        let (spent_confirmed, source) = if row.known && row.spent == Some(true) {
            crate::results::spent_confirmed_word(row.spent_confirmed, index, tracker, txany)
        } else {
            (row.spent_confirmed, None)
        };
        let entry = crate::logic::OutpointStatus {
            txid: tx(0x07),
            vout: 0,
            known: row.known,
            spent: row.spent,
            spending_txid: row.spending_txid.clone(),
            spent_confirmed,
            spender_seen: None,
            spender_final: None,
            reason: row.reason,
            funding_seen: None,
            funding_seen_by: None,
            evicted: crate::logic::ServedEviction::NotRead,
        };
        crate::txany::spent_any_body_sourced(&[entry], &[false], &[source])
    }

    /// bsv-low #723: the ladder over a WoC `/spent` answer for `tx(0x07):0` (`None`: a transport fault), BananaBlocks
    /// naming the competitor spend, and WoC's hex of it (so the spender verifies).
    fn e723_ladder(woc_spent: Option<(u16, Vec<u8>)>) -> SpentAnyCached {
        let (sp, raw) = competitor();
        let t = tx(0x07);
        let mut m = HashMap::new();
        if let Some(w) = woc_spent {
            m.insert(format!("{WOC_BASE}/tx/{t}/0/spent"), w);
        }
        m.insert(
            format!("{BANANABLOCKS_BASE}/txo/{t}/0/spend"),
            (
                200,
                serde_json::json!({"spent": true, "spentTxid": sp})
                    .to_string()
                    .into_bytes(),
            ),
        );
        m.insert(
            format!("{WOC_BASE}/tx/{sp}/hex"),
            (200, hex::encode(&raw).into_bytes()),
        );
        let (row, raw_kept) = run(spent_any_resolve_via(&t, 0, courier(m)));
        assert!(raw_kept.is_some(), "the spender verified");
        assert_eq!(
            (row.known, row.spent, row.spending_txid.as_deref()),
            (true, Some(true), Some(sp.as_str()))
        );
        row
    }

    /// bsv-low #723 (a): THE FLAP. A long-mined spend whose WoC `/0/spent` rung faults (a 429, a 5xx, a transport
    /// fault): BananaBlocks names the spender and cannot say it is mined, so the ladder's word is `null`, never
    /// `false`; with the INDEX's verified proof of that spender held the row answers `spentConfirmed: true`,
    /// `spentConfirmedSource: "index-proof"` (and the tracker's checked mined word, or a chaintracks-bound `/tx-any`
    /// proof, when the index holds none), so two reads cannot flap without a chain change. A held proof outranks a
    /// courier's "unconfirmed" too. RED on `aa57054`: the row read `spentConfirmed: false` under the fault (run as the
    /// mutant `primary == "woc"` forced true: `left: Some(false)`), and the source and the held-proof word do not
    /// exist there.
    #[test]
    fn e723_a_a_held_proof_answers_confirmed_under_a_faulting_woc_rung() {
        for woc in [Some((429u16, Vec::new())), Some((503, Vec::new())), None] {
            let row = e723_ladder(woc.clone());
            assert_eq!(row.spent_confirmed, None, "{woc:?}: no word, never false");
            let body = e723_body(&row, true, false, false);
            assert!(
                body.contains(r#""spentConfirmed":true,"spentConfirmedSource":"index-proof""#),
                "{body}"
            );
            for (tracker, txany, source) in [
                (true, false, "tracker-mined"),
                (false, true, "txany-proof"),
                (true, true, "tracker-mined"),
            ] {
                let body = e723_body(&row, false, tracker, txany);
                assert!(
                    body.contains(&format!(
                        r#""spentConfirmed":true,"spentConfirmedSource":"{source}""#
                    )),
                    "{body}"
                );
            }
        }
        // a courier's "unconfirmed" against a held index proof: the proof speaks
        let (sp, _) = competitor();
        let row = e723_ladder(Some((
            200,
            serde_json::json!({"txid": sp, "vin": 0, "status": "unconfirmed"})
                .to_string()
                .into_bytes(),
        )));
        assert_eq!(row.spent_confirmed, Some(false), "WoC's own word");
        assert!(e723_body(&row, true, false, false)
            .contains(r#""spentConfirmed":true,"spentConfirmedSource":"index-proof""#));
        // the route: the ladder's null, the held-proof word over the VERIFIED spender, the body with its source
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let src = squash(include_str!("routes.rs"));
        for wired in [
            "crate::results::decide_spent_any(&woc,spender_raw_ok,bitails),",
            "letindex_proven=spent_any_index_proven(&ctx.env,&spenders).await;",
            "crate::results::spent_confirmed_word(e.spent_confirmed,index_proven.contains(&sp),crate::owed::checked_tracker_mined(views.as_ref(),&sp)==Some(true),tx_any_cached(&sp,now_f).is_some_and(|a|crate::txany::proof_mined_word(&a)),);",
            "crate::txany::spent_any_body_sourced(&entries,&terminals,&sources,)",
            ".filter(|_|e.known&&e.spent==Some(true))",
            ".prepare(crate::hopsweep::sweep_proofs_sql(chunk.len()))",
        ] {
            assert!(src.contains(wired), "the route: {wired}");
        }
    }

    /// bsv-low #723 (b): NO HELD PROOF AND A WoC FAULT: `spentConfirmed: null`, `spentConfirmedSource: null` (the
    /// confirming rung could not look; BananaBlocks' placeholder is not a word). RED on `aa57054`: `false`.
    #[test]
    fn e723_b_no_held_proof_and_a_woc_fault_answers_null_and_null() {
        let row = e723_ladder(Some((429, Vec::new())));
        let body = e723_body(&row, false, false, false);
        assert!(
            body.contains(r#""spentConfirmed":null,"spentConfirmedSource":null"#),
            "{body}"
        );
        assert_eq!(
            crate::results::spent_confirmed_word(None, false, false, false),
            (None, None)
        );
    }

    /// bsv-low #723 (c): WHERE NO RUNG FAULTED the bodies are byte-identical: WoC's confirmed spend keeps its word
    /// (`courier`) and its body is the old one plus the additive source; an unspent, an unknown and a provider-fault row
    /// carry no source key at all (the #695 frozen bodies, through the sourced builder).
    #[test]
    fn e723_c_the_bodies_where_no_rung_faulted_are_byte_identical() {
        let (sp, _) = competitor();
        let row = e723_ladder(Some((
            200,
            serde_json::json!({"txid": sp, "vin": 0, "status": "confirmed"})
                .to_string()
                .into_bytes(),
        )));
        assert_eq!(row.spent_confirmed, Some(true));
        let entry = |r: &SpentAnyCached| crate::logic::OutpointStatus {
            txid: tx(0x07),
            vout: 0,
            known: r.known,
            spent: r.spent,
            spending_txid: r.spending_txid.clone(),
            spent_confirmed: r.spent_confirmed,
            spender_seen: None,
            spender_final: None,
            reason: r.reason,
            funding_seen: None,
            funding_seen_by: None,
            evicted: crate::logic::ServedEviction::NotRead,
        };
        let old = crate::txany::spent_any_body(&[entry(&row)], &[false]);
        let new = e723_body(&row, false, false, false);
        let mut v: serde_json::Value = serde_json::from_str(&new).unwrap();
        assert_eq!(v[0]["spentConfirmedSource"], "courier");
        v[0].as_object_mut().unwrap().remove("spentConfirmedSource");
        assert_eq!(v.to_string(), old, "the old body plus the additive source");
        let frozen_fault = r#"[{"known":false,"networkSeen":null,"reason":"provider-fault","spenderFinal":null,"spenderSeen":null,"spendingTxid":null,"spent":null,"spentConfirmed":null,"txid":"0707070707070707070707070707070707070707070707070707070707070707","vout":0}]"#;
        let frozen_unspent = r#"[{"known":true,"networkSeen":null,"reason":null,"spenderFinal":null,"spenderSeen":null,"spendingTxid":null,"spent":false,"spentConfirmed":false,"txid":"0707070707070707070707070707070707070707070707070707070707070707","vout":0}]"#;
        let fault = SpentAnyCached {
            known: false,
            spent: None,
            spending_txid: None,
            spent_confirmed: None,
            reason: Some(crate::logic::SPENT_ANY_REASON_PROVIDER_FAULT),
        };
        let unspent = SpentAnyCached {
            known: true,
            spent: Some(false),
            spending_txid: None,
            spent_confirmed: Some(false),
            reason: None,
        };
        assert_eq!(e723_body(&fault, true, true, true), frozen_fault);
        assert_eq!(e723_body(&unspent, true, true, true), frozen_unspent);
    }

    // ---- bsv-low #868 (lane COURIER-RUNG): BananaBlocks a capable second rung on the two reads where WoC stood
    // alone. Every body below is one the probe recorded (`tests/fixtures/courier_probe_868.json`, verbatim from
    // `COURIER-PROBE/requests.jsonl`, 2026-10-24), its identifiers rewritten only where the ladder must verify a
    // spender's raw (the harness's `competitor()`), its keys and words untouched.
    fn c868_recorded(key: &str) -> (u16, Vec<u8>, String) {
        let v: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/courier_probe_868.json")).unwrap();
        let b = &v["bodies"][key];
        let url = b["url"].as_str().unwrap();
        (
            u16::try_from(b["status"].as_u64().unwrap()).unwrap(),
            b["body"].as_str().unwrap().as_bytes().to_vec(),
            url.to_string(),
        )
    }
    /// The subject a recorded `GET .../tx/{txid}` asked about: its url's last segment.
    fn c868_subject(key: &str) -> String {
        let (_, _, url) = c868_recorded(key);
        url.rsplit('/').next().unwrap().to_string()
    }
    /// A recorded spend body for the harness's outpoint `tx(0x07):0` and its verifiable spender.
    fn c868_spend_body(key: &str, drop_scope: bool) -> Vec<u8> {
        let (sp, _) = competitor();
        let (status, body, _) = c868_recorded(key);
        assert_eq!(status, 200, "{key}");
        let mut v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        v["spentTxid"] = serde_json::json!(sp);
        v["txid"] = serde_json::json!(tx(0x07));
        v["vout"] = serde_json::json!(0);
        if drop_scope {
            v.as_object_mut().unwrap().remove("spendScope");
        }
        v.to_string().into_bytes()
    }

    /// bsv-low #868 (a): THE SPENDER'S MINED WORD has two capable rungs. BananaBlocks' `GET /txo/{txid}/{vout}/spend`
    /// answers `spendScope: "confirmed"` or `"unconfirmed"` (probed 2026-10-24, watched flipping at block 972477); a
    /// spent answer without the scope is unknown (its docs: "read a spent output without `spendScope` as scope
    /// unknown, not as confirmed"). Under a WoC `/spent` fault the ladder's `spentConfirmed` is BananaBlocks' word:
    /// `true`, `false`, `null`. RED on `db3a0931`: `null` for every body (`parse_bananablocks_spent` hard-coded
    /// `confirmed: false` and #723 nulled it).
    #[test]
    fn c868_a_bananablocks_spend_scope_is_the_mined_word_under_a_woc_fault() {
        let (sp, raw) = competitor();
        let t = tx(0x07);
        for (label, body, want) in [
            (
                "confirmed",
                c868_spend_body("bb_spend_confirmed", false),
                Some(true),
            ),
            (
                "unconfirmed",
                c868_spend_body("bb_spend_unconfirmed", false),
                Some(false),
            ),
            (
                "the flip, before 972477",
                c868_spend_body("bb_spend_flip_unconfirmed", false),
                Some(false),
            ),
            (
                "the flip, after 972477",
                c868_spend_body("bb_spend_flip_confirmed", false),
                Some(true),
            ),
            (
                "no scope",
                c868_spend_body("bb_spend_confirmed", true),
                None,
            ),
        ] {
            for woc in [Some((429u16, Vec::new())), Some((503, Vec::new())), None] {
                let mut m = HashMap::new();
                if let Some(w) = woc.clone() {
                    m.insert(format!("{WOC_BASE}/tx/{t}/0/spent"), w);
                }
                m.insert(
                    format!("{BANANABLOCKS_BASE}/txo/{t}/0/spend"),
                    (200, body.clone()),
                );
                m.insert(
                    format!("{WOC_BASE}/tx/{sp}/hex"),
                    (200, hex::encode(&raw).into_bytes()),
                );
                let (row, _) = run(spent_any_resolve_via(&t, 0, courier(m)));
                assert_eq!(
                    (row.known, row.spent, row.spending_txid.as_deref()),
                    (true, Some(true), Some(sp.as_str())),
                    "{label}"
                );
                assert_eq!(row.spent_confirmed, want, "{label} under WoC {woc:?}");
            }
        }
        // WoC's own word is unchanged, and still first (the ladder's order is not touched)
        let mut m = HashMap::new();
        m.insert(
            format!("{WOC_BASE}/tx/{t}/0/spent"),
            (
                200,
                serde_json::json!({"txid": sp, "vin": 0, "status": "unconfirmed"})
                    .to_string()
                    .into_bytes(),
            ),
        );
        m.insert(
            format!("{BANANABLOCKS_BASE}/txo/{t}/0/spend"),
            (200, c868_spend_body("bb_spend_confirmed", false)),
        );
        m.insert(
            format!("{WOC_BASE}/tx/{sp}/hex"),
            (200, hex::encode(&raw).into_bytes()),
        );
        let (row, _) = run(spent_any_resolve_via(&t, 0, courier(m)));
        assert_eq!(row.spent_confirmed, Some(false), "WoC answered: its word");
    }

    /// The `/tx-any` presence leg for a FOREIGN subject over a courier map (WoC's route already known healthy).
    fn c868_leg(
        txid: &str,
        m: HashMap<String, (u16, Vec<u8>)>,
    ) -> (
        crate::txany::TxAnyAnswer,
        AbsenceCorroboration,
        Option<u64>,
        Vec<String>,
    ) {
        WOC_ROUTE_HEALTHY.with(|c| c.set(Some(true)));
        BANANABLOCKS_ROUTE_HEALTHY.with(|c| c.set(None));
        let asked = std::cell::RefCell::new(Vec::new());
        let get = |url: String| {
            asked.borrow_mut().push(url.clone());
            std::future::ready(m.get(&url).cloned())
        };
        let now = crate::txany::rfc3339_utc_ms("2026-10-24T14:40:00Z").unwrap();
        let (obs, absence, height) = run(tx_any_presence_leg_via(
            txid,
            now,
            false,
            None,
            get,
            |_, _| async { false },
        ));
        let mut a = decide_tx_any(None, None, Some(&obs), absence);
        apply_external_height(&mut a, &obs, height);
        let asked = asked.into_inner();
        (a, absence, height, asked)
    }

    /// bsv-low #868 (b): AN UNMINED SUBJECT'S PRESENCE has a capable second rung, BananaBlocks' native
    /// `GET /tx/{txid}` (probed 2026-10-24: `confirmed: false`, `block_height: 0` unmined; `confirmed: true` and
    /// `block_height` mined, `confirmations` omitted at one confirmation; `confirmation_unknown: true` is "could not
    /// look", by its docs, never seen live). Under a WoC fault its unmined answer is the courier HINT (`present:
    /// null`, `hintSource: "bananablocks"`), its mined answer a CLAIM that is served `confirmed` only through the
    /// checked proof (none here: a hint with `claimedHeight`), its `confirmation_unknown` no word. Bitails' 200 with
    /// `confirmations: 0` is presence; its 404 (it dropped `e3340463...` while WoC and BananaBlocks held it) is
    /// "could not look". ABSENCE needs WoC's node AND BananaBlocks' native view, each a clean 404. The WoC-style
    /// mirror is never asked. RED on `db3a0931`: every arm read only WoC (`left: (None, None, None)`, the
    /// two-404 absence `present: None`).
    #[test]
    fn c868_b_bananablocks_native_tx_is_a_capable_presence_rung() {
        let unmined = c868_subject("bb_tx_unmined");
        let (s, b, _) = c868_recorded("bb_tx_unmined");
        // (1) WoC 429, BananaBlocks unmined: a hint from BananaBlocks
        let mut m = HashMap::new();
        m.insert(format!("{WOC_BASE}/tx/hash/{unmined}"), (429, Vec::new()));
        m.insert(format!("{BANANABLOCKS_BASE}/tx/{unmined}"), (s, b.clone()));
        let (a, absence, _, asked) = c868_leg(&unmined, m);
        assert_eq!(
            (a.present, a.confirmed, a.hint_source),
            (None, None, Some("bananablocks")),
            "unmined under a WoC fault"
        );
        assert_eq!(absence, AbsenceCorroboration::Unknown);
        assert!(
            !asked
                .iter()
                .any(|u| u.contains("/bsv/main/tx/hash/") && u.contains("bananablocks")),
            "never the WoC-style mirror: {asked:?}"
        );
        // (2) WoC 429, BananaBlocks mined (the recorded body has no `confirmations`): a claim, no proof served, so a
        // hint carrying the claimed height, never `confirmed`
        let mined = c868_subject("bb_tx_mined");
        let (s, b, _) = c868_recorded("bb_tx_mined");
        let mut m = HashMap::new();
        m.insert(format!("{WOC_BASE}/tx/hash/{mined}"), (429, Vec::new()));
        m.insert(format!("{BANANABLOCKS_BASE}/tx/{mined}"), (s, b.clone()));
        let (a, _, height, _) = c868_leg(&mined, m);
        assert_eq!(
            (a.present, a.confirmed, a.height, a.hint_source, height),
            (None, None, None, Some("bananablocks"), Some(972_476)),
            "a mined claim without a checked proof"
        );
        assert_eq!(a.claimed_height, Some(972_476));
        // (3) `confirmation_unknown: true` (the docs' shape over the recorded body): could not look
        let mut v: serde_json::Value =
            serde_json::from_slice(&c868_recorded("bb_tx_unmined").1).unwrap();
        v["confirmation_unknown"] = serde_json::json!(true);
        v["partial"] = serde_json::json!(true);
        let mut m = HashMap::new();
        m.insert(format!("{WOC_BASE}/tx/hash/{unmined}"), (429, Vec::new()));
        m.insert(
            format!("{BANANABLOCKS_BASE}/tx/{unmined}"),
            (200, v.to_string().into_bytes()),
        );
        let (a, _, _, _) = c868_leg(&unmined, m);
        assert_eq!(
            (a.present, a.confirmed, a.hint_source),
            (None, None, None),
            "confirmation_unknown is no word"
        );
        // (4) WoC's 404 and BananaBlocks' recorded 404, each behind its proven route (the lens fold's M1: BananaBlocks'
        // anchor, `KNOWN_MINED_TXID`, served as the recorded mined body): two capable sources, a corroborated absence
        let invented = c868_subject("bb_tx_not_found");
        let (s, b, _) = c868_recorded("bb_tx_not_found");
        let (bs, bb, _) = c868_recorded("bitails_tx_not_found");
        let (ms, mb, _) = c868_recorded("bb_tx_mined");
        let mut anchor: serde_json::Value = serde_json::from_slice(&mb).unwrap();
        anchor["txid"] = serde_json::json!(crate::txany::KNOWN_MINED_TXID);
        let mut m = HashMap::new();
        m.insert(
            format!("{BANANABLOCKS_BASE}/tx/{}", crate::txany::KNOWN_MINED_TXID),
            (ms, anchor.to_string().into_bytes()),
        );
        m.insert(format!("{WOC_BASE}/tx/hash/{invented}"), (404, Vec::new()));
        m.insert(format!("{BANANABLOCKS_BASE}/tx/{invented}"), (s, b.clone()));
        m.insert(format!("{BITAILS_BASE}/tx/{invented}"), (bs, bb.clone()));
        let (a, absence, _, _) = c868_leg(&invented, m);
        assert_eq!(absence, AbsenceCorroboration::CorroboratedAbsent);
        assert_eq!((a.present, a.hint_source), (Some(false), None));
        // (5) WoC's 404 beside a BananaBlocks fault, and a BananaBlocks 404 beside a WoC fault: one source, unknown
        for (woc, banana) in [
            ((404u16, Vec::new()), (429u16, Vec::new())),
            ((429, Vec::new()), (s, b.clone())),
            // a 404 that is not BananaBlocks' recorded not-found body is no word either
            ((404, Vec::new()), (404, Vec::new())),
        ] {
            let mut m = HashMap::new();
            m.insert(format!("{WOC_BASE}/tx/hash/{invented}"), woc.clone());
            m.insert(format!("{BANANABLOCKS_BASE}/tx/{invented}"), banana.clone());
            let (a, absence, _, _) = c868_leg(&invented, m);
            assert_eq!(absence, AbsenceCorroboration::Unknown, "{woc:?} {banana:?}");
            assert_eq!(a.present, None, "{woc:?} {banana:?}");
        }
        // (6) Bitails' 200 with `confirmations: 0` is presence (WoC and BananaBlocks could not look)
        let dropped = c868_subject("bitails_tx_unmined");
        let (s, b, _) = c868_recorded("bitails_tx_unmined");
        let mut m = HashMap::new();
        m.insert(format!("{WOC_BASE}/tx/hash/{dropped}"), (429, Vec::new()));
        m.insert(
            format!("{BANANABLOCKS_BASE}/tx/{dropped}"),
            (429, Vec::new()),
        );
        m.insert(format!("{BITAILS_BASE}/tx/{dropped}"), (s, b.clone()));
        let (a, _, _, _) = c868_leg(&dropped, m);
        assert_eq!(
            (a.present, a.confirmed, a.hint_source),
            (None, None, Some("bitails")),
            "Bitails' positive"
        );
        // ... and outranks the two clean 404s
        let (ns, nb, _) = c868_recorded("bb_tx_not_found");
        let mut m = HashMap::new();
        m.insert(format!("{WOC_BASE}/tx/hash/{dropped}"), (404, Vec::new()));
        m.insert(
            format!("{BANANABLOCKS_BASE}/tx/{dropped}"),
            (ns, nb.clone()),
        );
        m.insert(format!("{BITAILS_BASE}/tx/{dropped}"), (s, b.clone()));
        let (a, _, _, _) = c868_leg(&dropped, m);
        assert_eq!(
            a.hint_source,
            Some("bitails"),
            "a positive outranks negatives"
        );
        // (7) Bitails' recorded 404 for that same subject (it dropped it) is "could not look", never absence
        let (s, b, _) = c868_recorded("bitails_tx_dropped_404");
        let mut m = HashMap::new();
        m.insert(format!("{WOC_BASE}/tx/hash/{dropped}"), (404, Vec::new()));
        m.insert(
            format!("{BANANABLOCKS_BASE}/tx/{dropped}"),
            (429, Vec::new()),
        );
        m.insert(format!("{BITAILS_BASE}/tx/{dropped}"), (s, b.clone()));
        let (a, absence, _, _) = c868_leg(&dropped, m);
        assert_eq!((a.present, absence), (None, AbsenceCorroboration::Unknown));
    }

    /// Lane COURIER-RUNG-LENS (the one lens on PR 170, bsv-low #868): the negative cases the PR's pins leave out.
    mod courier_rung_lens {
        use super::*;

        /// The presence leg over a courier map with the route health and the proof check chosen by the case.
        fn lens_leg(
            txid: &str,
            m: HashMap<String, (u16, Vec<u8>)>,
            woc_route: Option<bool>,
            proof_ok: bool,
        ) -> (crate::txany::TxAnyAnswer, AbsenceCorroboration, Vec<String>) {
            WOC_ROUTE_HEALTHY.with(|c| c.set(woc_route));
            let asked = std::cell::RefCell::new(Vec::new());
            let get = |url: String| {
                asked.borrow_mut().push(url.clone());
                std::future::ready(m.get(&url).cloned())
            };
            let now = crate::txany::rfc3339_utc_ms("2026-10-24T14:40:00Z").unwrap();
            let (obs, absence, height) = run(tx_any_presence_leg_via(
                txid,
                now,
                false,
                None,
                get,
                move |_, _| async move { proof_ok },
            ));
            let mut a = decide_tx_any(None, None, Some(&obs), absence);
            apply_external_height(&mut a, &obs, height);
            (a, absence, asked.into_inner())
        }

        fn nf() -> (u16, Vec<u8>) {
            let (s, b, _) = c868_recorded("bb_tx_not_found");
            (s, b)
        }

        /// (a) A `spendScope: "confirmed"` whose spender's raw no rung serves (or serves bytes that do not spend the
        /// outpoint) is never a confirmed word: the honest unknown, `spentConfirmed: null`.
        #[test]
        fn lens868_a_a_confirmed_scope_never_survives_an_unverified_spender() {
            let t = tx(0x07);
            let (sp, _) = competitor();
            for raw in [None, Some(vec![0u8; 10])] {
                let mut m = HashMap::new();
                m.insert(format!("{WOC_BASE}/tx/{t}/0/spent"), (503, Vec::new()));
                m.insert(
                    format!("{BANANABLOCKS_BASE}/txo/{t}/0/spend"),
                    (200, c868_spend_body("bb_spend_confirmed", false)),
                );
                if let Some(r) = raw.clone() {
                    m.insert(
                        format!("{WOC_BASE}/tx/{sp}/hex"),
                        (200, hex::encode(r).into_bytes()),
                    );
                }
                let (row, verified) = run(spent_any_resolve_via(&t, 0, courier(m)));
                assert_eq!(
                    (
                        row.known,
                        row.spent,
                        row.spent_confirmed,
                        verified.is_some()
                    ),
                    (false, None, None, false),
                    "raw {raw:?}"
                );
                assert_eq!(
                    row.reason,
                    Some(crate::logic::SPENT_ANY_REASON_UNVERIFIED_SPENDER)
                );
            }
        }

        /// (b) Only the two documented words are a scope; any other spelling is unknown, and a scope beside
        /// `spent: false` (or a spend naming no spender) is no mined word at all.
        #[test]
        fn lens868_b_scope_words_outside_the_two_are_unknown() {
            use crate::results::{
                bananablocks_spend_scope, parse_bananablocks_spent, SpentObservation,
            };
            let sp = "ab".repeat(32);
            for scope in [
                serde_json::json!("CONFIRMED"),
                serde_json::json!("Confirmed"),
                serde_json::json!("mined"),
                serde_json::json!(""),
                serde_json::json!(true),
                serde_json::json!(1),
                serde_json::Value::Null,
            ] {
                let body = serde_json::json!({"spent": true, "spentTxid": sp, "spendScope": scope});
                assert_eq!(bananablocks_spend_scope(&body), None, "{scope}");
                assert_eq!(
                    parse_bananablocks_spent(200, Some(&body)),
                    SpentObservation::Spent {
                        txid: sp.clone(),
                        confirmed: None
                    },
                    "{scope}"
                );
            }
            assert_eq!(
                parse_bananablocks_spent(
                    200,
                    Some(&serde_json::json!({"spent": false, "spendScope": "confirmed"}))
                ),
                SpentObservation::NotSpent
            );
            assert_eq!(
                parse_bananablocks_spent(
                    200,
                    Some(&serde_json::json!({"spent": true, "spendScope": "confirmed"}))
                ),
                SpentObservation::Fault
            );
            // a 5xx or a 429 that happens to carry a scope is never read
            assert_eq!(
                parse_bananablocks_spent(
                    503,
                    Some(
                        &serde_json::json!({"spent": true, "spentTxid": sp, "spendScope": "confirmed"})
                    )
                ),
                SpentObservation::Fault
            );
        }

        /// (c) BananaBlocks' native `/tx` bodies that are no word: a mined claim with no positive height, a
        /// `confirmed` that is not a boolean, `confirmation_unknown` beside `confirmed: true`, a body with no txid,
        /// the WoC-style mirror's MINED body (recorded shape: no `confirmed`, no `confirmations`).
        #[test]
        fn lens868_c_native_tx_bodies_that_are_no_word() {
            use crate::txany::parse_bananablocks_tx_claim;
            let t = "cd".repeat(32);
            for (label, body) in [
                (
                    "mined, height 0",
                    serde_json::json!({"txid": t, "confirmed": true, "block_height": 0}),
                ),
                (
                    "mined, no height",
                    serde_json::json!({"txid": t, "confirmed": true}),
                ),
                (
                    "mined, height a string",
                    serde_json::json!({"txid": t, "confirmed": true, "block_height": "972476"}),
                ),
                (
                    "confirmed a string",
                    serde_json::json!({"txid": t, "confirmed": "false", "block_height": 0}),
                ),
                (
                    "confirmed null",
                    serde_json::json!({"txid": t, "confirmed": null}),
                ),
                (
                    "unknown beside a mined claim",
                    serde_json::json!({"txid": t, "confirmed": true, "block_height": 972_476,
                        "confirmation_unknown": true, "partial": true}),
                ),
                (
                    "no txid",
                    serde_json::json!({"confirmed": false, "block_height": 0}),
                ),
                (
                    "the mirror's mined body",
                    serde_json::json!({"txid": t, "hash": t, "version": 1, "size": 200, "locktime": 0,
                        "vin": [], "vout": [], "blockhash": "00".repeat(32), "time": 1,
                        "blocktime": 1, "blockheight": 972_476}),
                ),
                (
                    "the not-found body under a 200",
                    serde_json::json!({"error": "Transaction not found"}),
                ),
            ] {
                assert_eq!(parse_bananablocks_tx_claim(&body, &t), None, "{label}");
            }
            // the not-found reader needs the 404 AND the recorded body, exactly
            let nf_body = br#"{"error":"Transaction not found"}"#;
            assert!(!crate::txany::bananablocks_tx_not_found(200, nf_body));
            assert!(!crate::txany::bananablocks_tx_not_found(410, nf_body));
            assert!(!crate::txany::bananablocks_tx_not_found(
                404,
                br#"{"error":"transaction not found"}"#
            ));
            assert!(!crate::txany::bananablocks_tx_not_found(
                404,
                b"Transaction not found"
            ));
            // observed, not a defect: `confirmed: false` beside a positive height reads unmined (a hint, never an
            // absence and never a mined fact)
            assert_eq!(
                parse_bananablocks_tx_claim(
                    &serde_json::json!({"txid": t, "confirmed": false, "block_height": 972_476}),
                    &t
                ),
                Some((false, None))
            );
        }

        /// (d) BananaBlocks' not-found never completes an absence when WoC's 404 is not behind a healthy route (the
        /// anchor read faulted): the leg stays unknown.
        #[test]
        fn lens868_d_no_absence_without_wocs_healthy_route() {
            let t = c868_subject("bb_tx_not_found");
            let mut m = HashMap::new();
            m.insert(format!("{WOC_BASE}/tx/hash/{t}"), (404, Vec::new()));
            m.insert(
                format!("{WOC_BASE}/tx/hash/{}", crate::txany::KNOWN_MINED_TXID),
                (503, Vec::new()),
            );
            m.insert(format!("{BANANABLOCKS_BASE}/tx/{t}"), nf());
            let (a, absence, _) = lens_leg(&t, m, None, false);
            assert_eq!((a.present, absence), (None, AbsenceCorroboration::Unknown));
        }

        /// (e) Beside WoC's healthy 404, every BananaBlocks answer that is not its recorded 404 is no vote: its
        /// `confirmation_unknown`, a mined claim with no height, its not-found body under 200 or 410, a 429, a
        /// transport fault.
        #[test]
        fn lens868_e_no_bananablocks_word_beside_wocs_404_is_never_an_absence() {
            let t = c868_subject("bb_tx_unmined");
            let (_, unmined, _) = c868_recorded("bb_tx_unmined");
            let mut unknown: serde_json::Value = serde_json::from_slice(&unmined).unwrap();
            unknown["confirmation_unknown"] = serde_json::json!(true);
            let (_, nf_body) = nf();
            for (label, banana) in [
                (
                    "confirmation_unknown",
                    Some((200u16, unknown.to_string().into_bytes())),
                ),
                (
                    "mined, no height",
                    Some((
                        200,
                        serde_json::json!({"txid": t, "confirmed": true})
                            .to_string()
                            .into_bytes(),
                    )),
                ),
                ("not-found under 200", Some((200, nf_body.clone()))),
                ("not-found under 410", Some((410, nf_body.clone()))),
                ("429", Some((429, Vec::new()))),
                ("transport", None),
            ] {
                let mut m = HashMap::new();
                m.insert(format!("{WOC_BASE}/tx/hash/{t}"), (404, Vec::new()));
                if let Some(b) = banana {
                    m.insert(format!("{BANANABLOCKS_BASE}/tx/{t}"), b);
                }
                let (a, absence, _) = lens_leg(&t, m, Some(true), false);
                assert_eq!(
                    (a.present, a.hint_source, absence),
                    (None, None, AbsenceCorroboration::Unknown),
                    "{label}"
                );
            }
        }

        /// (f) A positive outranks WoC's healthy 404: BananaBlocks' unmined body beside it is the hint, no absence.
        #[test]
        fn lens868_f_bananablocks_presence_outranks_wocs_404() {
            let t = c868_subject("bb_tx_unmined");
            let (s, b, _) = c868_recorded("bb_tx_unmined");
            let mut m = HashMap::new();
            m.insert(format!("{WOC_BASE}/tx/hash/{t}"), (404, Vec::new()));
            m.insert(format!("{BANANABLOCKS_BASE}/tx/{t}"), (s, b));
            let (a, absence, asked) = lens_leg(&t, m, Some(true), false);
            assert_eq!(
                (a.present, a.hint_source, absence),
                (None, Some("bananablocks"), AbsenceCorroboration::Unknown)
            );
            assert!(
                !asked
                    .iter()
                    .any(|u| u.starts_with(BITAILS_BASE) && !u.contains("/download/")),
                "a BananaBlocks positive asks Bitails' presence no further: {asked:?}"
            );
        }

        /// (h) THE LENS'S M1, re-aimed by the fold (lane COURIER-RUNG-FOLD): WoC's 404 counts only behind its proven
        /// route, and so, since the fold, does BananaBlocks' recorded not-found (its anchor: `KNOWN_MINED_TXID` served
        /// `confirmed: true` with a positive `block_height` on the same native route, memoised per isolate for the
        /// Arcade anchor's TTL). A BananaBlocks that answers its not-found for EVERY txid, the known-mined anchor
        /// included (an index that lost history, a partial node behind its balancer), proves no route: the leg is the
        /// honest unknown (`present: null`), counted `tx_any_one_capable_source_unknown_total`, never `false`. RED on
        /// `f5d2d700`: `left: (Some(false), CorroboratedAbsent)`.
        #[test]
        fn lens868_h_bananablocks_not_found_needs_its_route_anchor() {
            let _ = crate::courier::take_pending_if_due(0.0);
            BANANABLOCKS_ROUTE_HEALTHY.with(|c| c.set(None));
            let t = c868_subject("bb_tx_not_found");
            let mut m = HashMap::new();
            m.insert(format!("{WOC_BASE}/tx/hash/{t}"), (404, Vec::new()));
            m.insert(
                format!("{WOC_BASE}/tx/hash/{}", crate::txany::KNOWN_MINED_TXID),
                (200, b"{}".to_vec()),
            );
            m.insert(format!("{BANANABLOCKS_BASE}/tx/{t}"), nf());
            m.insert(
                format!("{BANANABLOCKS_BASE}/tx/{}", crate::txany::KNOWN_MINED_TXID),
                nf(),
            );
            let (a, absence, asked) = lens_leg(&t, m, None, false);
            assert_eq!((a.present, absence), (None, AbsenceCorroboration::Unknown));
            assert!(
                asked.iter().any(|u| u.ends_with(crate::txany::KNOWN_MINED_TXID) && u.starts_with(WOC_BASE)),
                "WoC's route is proven: {asked:?}"
            );
            assert!(
                asked
                    .iter()
                    .any(|u| u.ends_with(crate::txany::KNOWN_MINED_TXID)
                        && u.starts_with(BANANABLOCKS_BASE)),
                "BananaBlocks' route is asked its anchor: {asked:?}"
            );
            let d: std::collections::BTreeMap<String, u64> =
                crate::courier::take_pending_if_due(0.0)
                    .into_iter()
                    .collect();
            assert_eq!(
                d.get("tx_any_one_capable_source_unknown_total"),
                Some(&1),
                "{d:?}"
            );
            assert_eq!(
                d.get("tx_any_two_capable_sources_absent_total"),
                None,
                "{d:?}"
            );
            assert_eq!(
                d.get("tx_any_courier_bananablocks_couldnotlook_total"),
                Some(&1),
                "an unproven route's not-found is could-not-look: {d:?}"
            );
        }

        /// (g) A BananaBlocks MINED claim is served `confirmed` only through WoC's `/proof/tsc` checked against
        /// chaintracks at the CLAIMED height; with the check refusing it is the hint with `claimedHeight`, and with
        /// no proof route answering likewise. Never BananaBlocks' own proof route.
        #[test]
        fn lens868_g_a_bananablocks_mined_claim_needs_the_checked_proof() {
            // the recorded mined body (no `confirmations`), re-keyed to the harness's spender so its raw verifies
            let (t, raw) = competitor();
            let (s, b, _) = c868_recorded("bb_tx_mined");
            let mut v: serde_json::Value = serde_json::from_slice(&b).unwrap();
            v["txid"] = serde_json::json!(t);
            let b = v.to_string().into_bytes();
            let tsc = serde_json::json!({"txOrId": t, "index": 0, "nodes": ["*"]})
                .to_string()
                .into_bytes();
            for (label, proof, check, want_confirmed) in [
                ("proof checked", Some(tsc.clone()), true, Some(true)),
                ("proof refused", Some(tsc.clone()), false, None),
                ("no proof", None, true, None),
            ] {
                let mut m = HashMap::new();
                m.insert(format!("{WOC_BASE}/tx/hash/{t}"), (429, Vec::new()));
                m.insert(format!("{BANANABLOCKS_BASE}/tx/{t}"), (s, b.clone()));
                m.insert(
                    format!("{WOC_BASE}/tx/{t}/hex"),
                    (200, hex::encode(&raw).into_bytes()),
                );
                if let Some(p) = proof {
                    m.insert(format!("{WOC_BASE}/tx/{t}/proof/tsc"), (200, p));
                }
                let (a, _, asked) = lens_leg(&t, m, Some(true), check);
                assert_eq!(a.confirmed, want_confirmed, "{label}");
                // the height is served only where the proof was checked at it
                assert_eq!(
                    a.height,
                    want_confirmed.map(|_| 972_476),
                    "{label}: a height only behind the checked proof"
                );
                if want_confirmed.is_none() {
                    assert_eq!(
                        (a.present, a.hint_source, a.claimed_height),
                        (None, Some("bananablocks"), Some(972_476)),
                        "{label}"
                    );
                }
                assert!(
                    !asked
                        .iter()
                        .any(|u| u.starts_with(BANANABLOCKS_BASE) && u.contains("proof")),
                    "{label}: {asked:?}"
                );
            }
        }

        /// BananaBlocks' anchor body for `KNOWN_MINED_TXID`: the recorded mined shape, re-keyed.
        fn anchor_mined() -> (u16, Vec<u8>) {
            let (s, b, _) = c868_recorded("bb_tx_mined");
            let mut v: serde_json::Value = serde_json::from_slice(&b).unwrap();
            v["txid"] = serde_json::json!(crate::txany::KNOWN_MINED_TXID);
            (s, v.to_string().into_bytes())
        }

        fn two_404s(t: &str, anchor: Option<(u16, Vec<u8>)>) -> HashMap<String, (u16, Vec<u8>)> {
            let mut m = HashMap::new();
            m.insert(format!("{WOC_BASE}/tx/hash/{t}"), (404, Vec::new()));
            m.insert(format!("{BANANABLOCKS_BASE}/tx/{t}"), nf());
            if let Some(a) = anchor {
                m.insert(
                    format!("{BANANABLOCKS_BASE}/tx/{}", crate::txany::KNOWN_MINED_TXID),
                    a,
                );
            }
            m
        }

        fn anchor_asks(asked: &[String]) -> usize {
            asked
                .iter()
                .filter(|u| {
                    u.starts_with(BANANABLOCKS_BASE) && u.ends_with(crate::txany::KNOWN_MINED_TXID)
                })
                .count()
        }

        /// The fold's M1 (lane COURIER-RUNG-FOLD): BOTH capable sources LOOKED (WoC behind its anchor, BananaBlocks
        /// behind its own: `KNOWN_MINED_TXID` served mined on its native route) and both answered their clean
        /// not-found: `present: false`, as #614 allows, counted `tx_any_two_capable_sources_absent_total`. The anchor is
        /// asked once and memoised for `ARCADE_ANCHOR_PROBE_TTL_MS` (both outcomes); a stale memo is asked again; an
        /// anchor that does not answer mined (a 404, its not-found, `confirmation_unknown`, a 5xx, another txid's body,
        /// silence) proves nothing and the leg is unknown. RED on `f5d2d700`: the anchor is never asked (`left: 0
        /// right: 1`), and every unproven arm answers `Some(false)`.
        #[test]
        fn c868f_m1_b_an_absence_needs_both_routes_proven_and_the_anchor_is_memoised() {
            let t = c868_subject("bb_tx_not_found");
            let run_at = |m: HashMap<String, (u16, Vec<u8>)>, now: i64| {
                WOC_ROUTE_HEALTHY.with(|c| c.set(Some(true)));
                let asked = std::cell::RefCell::new(Vec::new());
                let get = |url: String| {
                    asked.borrow_mut().push(url.clone());
                    std::future::ready(m.get(&url).cloned())
                };
                let (obs, absence, height) = run(tx_any_presence_leg_via(
                    &t,
                    now,
                    false,
                    None,
                    get,
                    |_, _| async { false },
                ));
                let mut a = decide_tx_any(None, None, Some(&obs), absence);
                apply_external_height(&mut a, &obs, height);
                (a, absence, asked.into_inner())
            };
            let t0 = 1_800_000_000_000_i64;
            let _ = crate::courier::take_pending_if_due(0.0);
            BANANABLOCKS_ROUTE_HEALTHY.with(|c| c.set(None));
            let (a, absence, asked) = run_at(two_404s(&t, Some(anchor_mined())), t0);
            assert_eq!(anchor_asks(&asked), 1, "{asked:?}");
            assert_eq!(
                (a.present, absence),
                (Some(false), AbsenceCorroboration::CorroboratedAbsent)
            );
            let d: std::collections::BTreeMap<String, u64> =
                crate::courier::take_pending_if_due(0.0)
                    .into_iter()
                    .collect();
            assert_eq!(
                d.get("tx_any_two_capable_sources_absent_total"),
                Some(&1),
                "{d:?}"
            );
            assert_eq!(
                d.get("tx_any_bananablocks_anchor_ok_total"),
                Some(&1),
                "{d:?}"
            );
            // inside the TTL the memo answers (the anchor's map entry is gone: it is not asked)
            let (a, _, asked) = run_at(two_404s(&t, None), t0 + ARCADE_ANCHOR_PROBE_TTL_MS - 1);
            assert_eq!(
                (anchor_asks(&asked), a.present),
                (0, Some(false)),
                "{asked:?}"
            );
            // past it the anchor is asked again; now it answers its not-found, so the leg is unknown, and that
            // outcome is memoised too
            let (a, _, asked) = run_at(two_404s(&t, Some(nf())), t0 + ARCADE_ANCHOR_PROBE_TTL_MS);
            assert_eq!((anchor_asks(&asked), a.present), (1, None), "{asked:?}");
            let (a, _, asked) = run_at(
                two_404s(&t, Some(anchor_mined())),
                t0 + ARCADE_ANCHOR_PROBE_TTL_MS + 1,
            );
            assert_eq!(
                (anchor_asks(&asked), a.present),
                (0, None),
                "an unhealthy memo holds its TTL: {asked:?}"
            );
            // every anchor answer that is not a mined claim for the anchor proves nothing
            let (_, unmined, _) = c868_recorded("bb_tx_unmined");
            let mut unknown: serde_json::Value = serde_json::from_slice(&anchor_mined().1).unwrap();
            unknown["confirmation_unknown"] = serde_json::json!(true);
            let mut other: serde_json::Value = serde_json::from_slice(&anchor_mined().1).unwrap();
            other["txid"] = serde_json::json!("00".repeat(32));
            let mut unmined_anchor: serde_json::Value = serde_json::from_slice(&unmined).unwrap();
            unmined_anchor["txid"] = serde_json::json!(crate::txany::KNOWN_MINED_TXID);
            for (label, anchor) in [
                ("its not-found", Some(nf())),
                ("an empty 404", Some((404u16, Vec::new()))),
                ("a 503", Some((503, Vec::new()))),
                ("a 429", Some((429, Vec::new()))),
                (
                    "confirmation_unknown",
                    Some((200, unknown.to_string().into_bytes())),
                ),
                (
                    "another txid's body",
                    Some((200, other.to_string().into_bytes())),
                ),
                (
                    "unmined",
                    Some((200, unmined_anchor.to_string().into_bytes())),
                ),
                ("silence", None),
            ] {
                BANANABLOCKS_ROUTE_HEALTHY.with(|c| c.set(None));
                let (a, absence, asked) = run_at(two_404s(&t, anchor), t0);
                assert_eq!(anchor_asks(&asked), 1, "{label}: {asked:?}");
                assert_eq!(
                    (a.present, absence),
                    (None, AbsenceCorroboration::Unknown),
                    "{label}"
                );
            }
        }

        /// The fold's M1: a BananaBlocks 404 that is not its recorded not-found body is a FAULT
        /// (`tx_any_courier_bananablocks_fault_total`), never a look: it votes toward no absence, even beside WoC's
        /// healthy 404 and BananaBlocks' proven route, and asks no anchor. (Green on `f5d2d700` in this layer: the
        /// rung already read it a fault; the overlay's twin, `proof_fetcher::c868f`, was RED.)
        #[test]
        fn c868f_m1_c_a_non_standard_bananablocks_404_is_a_fault() {
            let t = c868_subject("bb_tx_not_found");
            for body in [
                br#"{"error":"transaction not found"}"#.to_vec(),
                br#"{"error":"Not Found"}"#.to_vec(),
                b"Transaction not found".to_vec(),
                Vec::new(),
            ] {
                let _ = crate::courier::take_pending_if_due(0.0);
                BANANABLOCKS_ROUTE_HEALTHY.with(|c| c.set(Some((0, true))));
                let mut m = HashMap::new();
                m.insert(format!("{WOC_BASE}/tx/hash/{t}"), (404, Vec::new()));
                m.insert(format!("{BANANABLOCKS_BASE}/tx/{t}"), (404, body.clone()));
                let (a, absence, asked) = lens_leg(&t, m, Some(true), false);
                assert_eq!(
                    (a.present, absence, anchor_asks(&asked)),
                    (None, AbsenceCorroboration::Unknown, 0),
                    "{body:?}"
                );
                let d: std::collections::BTreeMap<String, u64> =
                    crate::courier::take_pending_if_due(0.0)
                        .into_iter()
                        .collect();
                assert_eq!(
                    d.get("tx_any_courier_bananablocks_fault_total"),
                    Some(&1),
                    "{body:?}: {d:?}"
                );
            }
        }
    }
}

#[cfg(test)]
#[path = "../tests/support/served_bodies.rs"]
mod ns2_d1bound_compat;

#[cfg(test)]
#[path = "../tests/support/leaderboard_642.rs"]
mod leaderboard_642;
#[cfg(test)]
#[path = "../tests/support/leaderboard_674.rs"]
mod leaderboard_674;

#[cfg(test)]
#[path = "tip_tests.rs"]
mod tip_tests;
