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

use serde::Deserialize;
use worker::wasm_bindgen::JsValue;
use worker::{console_warn, Headers, Method, Request, RequestInit, Response, Result, RouteContext};

use crate::auth::{AuthState, IdentityDecision};

use crate::logic::{
    assemble_pots_view, assemble_recovery_view, assemble_statuses, batch_where_sql, beef_body,
    chunk_outpoints, clamp_leaderboard_limit, decode_beef_hex, health_body, leaderboard_body,
    leaderboard_pot_outpoints, parse_outpoints, parse_present_height, pots_view_body,
    pots_view_join_sql, recovery_view_body, recovery_view_sql, tip_body, utxo_status_body,
    valid_identity, valid_txid, Outpoint, PotRecordRow, PotsViewRow, RecoveryRow, ResultMarkerRow,
};

/// The chaintracks present-height endpoint, fetched through the service
/// binding (`overlay-cloudflare/src/chain_tracker.rs` calls the same route).
/// Only the PATH matters — the host is resolved by the binding.
const CHAINTRACKS_TIP_URL: &str = "https://chaintracks/getPresentHeight";

/// Build a JSON response (always `no-store` — see the module note).
fn json_response(body: String, status: u16) -> Result<Response> {
    let mut resp = Response::ok(body)?.with_status(status);
    resp.headers_mut().set("Content-Type", "application/json")?;
    resp.headers_mut().set("Cache-Control", "no-store")?;
    Ok(resp)
}

/// JSON error.
fn json_error(msg: &str, status: u16) -> Result<Response> {
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
fn json_response_cached(body: String, status: u16, max_age_secs: u32) -> Result<Response> {
    let mut resp = Response::ok(body)?.with_status(status);
    resp.headers_mut().set("Content-Type", "application/json")?;
    resp.headers_mut()
        .set("Cache-Control", &format!("public, max-age={max_age_secs}, s-maxage={max_age_secs}"))?;
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
    let raw = crate::logic::normalize_written_off_before_ms(
        ctx.env
            .var("WRITTEN_OFF_BEFORE_MS")
            .ok()
            .map(|v| v.to_string()),
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
        match stmt.all().await.and_then(|r| r.results::<PotRowD1>()) {
            Ok(chunk_rows) => rows.extend(chunk_rows.into_iter().map(PotRowD1::into_row)),
            Err(e) => {
                console_warn!("[utxo-status] pot_records batch query failed: {e}");
                return json_error("database query failed", 503);
            }
        }
    }

    let entries = assemble_statuses(&outpoints, &rows);
    json_response(utxo_status_body(&entries), 200)
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
/// Load ONE stored BEEF by txid, with the same trust/compaction semantics
/// `/beef/:txid` serves. `Ok(None)` = genuinely absent; `Err(())` = the read
/// faulted (never shape a fault like a definitive not-found).
///
/// Factored out of `beef` so `/credit-beef` cannot drift from it: an assembled
/// ancestry that trusted different bytes than the single-txid route would be a
/// second source of truth for the same question.
async fn load_stored_beef(
    db: &worker::D1Database,
    txid: &str,
) -> std::result::Result<Option<Vec<u8>>, ()> {
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
            match stmt.first::<BeefTrustRow>(None).await {
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
                    match stmt.first::<BeefRow>(None).await {
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
            return Ok(Some(if proof_verified {
                crate::compaction::compact_beef(&key, &bytes)
            } else {
                bytes
            }));
        }
    }
    if faulted {
        return Err(());
    }
    Ok(None)
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

    let subject = match load_stored_beef(&db, &txid).await {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return json_error(&format!("BEEF not found for txid: {txid}"), 404),
        Err(()) => return json_error("database query failed", 503),
    };
    let acc = match bsv_rs::transaction::Beef::from_binary(&subject) {
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
            match load_stored_beef(&db, &parent).await {
                Ok(bytes) => {
                    // A parent we do not hold (a foreign tx never submitted
                    // through us) still counts as a fetch, so an absent row
                    // ends the walk instead of spinning it.
                    if walk.absorb(bytes.as_deref()) {
                        progressed = true;
                    }
                }
                Err(()) => return json_error("database query failed", 503),
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

    // pot_beefs first (durable), transactions second (lifecycle-managed).
    // Read the BLOB back as hex — the engine's own read-back idiom
    // (SQLite hex() emits uppercase; decode_beef_hex accepts either case).
    // A faulted query (e.g. the overlay worker's migration adding pot_beefs
    // has not run yet) still tries the other table for a hit, but a miss
    // after any fault is 503, never 404 — a fault must not be shaped like a
    // definitive not-found (module note above).
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
        // Trust-flag query first (bsv-low#304); if it faults (e.g. this
        // worker deployed ahead of the overlay's additive migration), fall
        // back to the legacy no-flag read and treat the row as UNVERIFIED —
        // availability is preserved, trust is not strengthened.
        let stmt = db.prepare(sql).bind(&[JsValue::from_str(&key)])?;
        let (row, proof_verified): (Option<BeefRow>, bool) =
            match stmt.first::<BeefTrustRow>(None).await {
                Ok(row) => {
                    let verified =
                        row.as_ref().and_then(|r| r.proof_verified).unwrap_or(0.0) != 0.0;
                    (row.map(|r| BeefRow { beef: r.beef }), verified)
                }
                Err(e) => {
                    console_warn!("[beef] {table} trust query failed ({e}) — legacy no-flag read");
                    let stmt = db.prepare(legacy_sql).bind(&[JsValue::from_str(&key)])?;
                    match stmt.first::<BeefRow>(None).await {
                        Ok(row) => (row, false),
                        Err(e) => {
                            console_warn!("[beef] {table} query failed: {e}");
                            faulted = true;
                            continue;
                        }
                    }
                }
            };
        if let Some(bytes) = row.and_then(|r| r.beef).and_then(|h| decode_beef_hex(&h)) {
            // Serve-time compaction (#192/#193, P4): once the overlay's
            // completion pass / Arcade MINED callback has stitched a
            // chaintracks-verified BUMP into this BEEF, its now-proven
            // ancestry is dead weight the frontend `createAction` chokes on.
            // `compact_beef` trims it — STRICTLY passthrough-on-failure, so a
            // proofless (or already-minimal) BEEF is returned byte-for-byte
            // unchanged. The subject is the lowercase DB key (BEEF txids are
            // lowercase hex).
            //
            // bsv-low#304 TRIMMING LICENSE: compaction is licensed ONLY by
            // the row's VERIFIED proof latch. Trimming decides mined-ness
            // from IN-BEEF bump presence, and an unverified row's bumps are
            // submitter bytes (possibly forged) — trimming on them would
            // drop the very ancestry an honest verifier needs. An
            // unverified row is served byte-for-byte as stored (weaker,
            // never wrong); a verified row keeps trimming exactly as
            // before.
            let compacted = if proof_verified {
                crate::compaction::compact_beef(&key, &bytes)
            } else {
                bytes
            };
            return json_response(beef_body(&txid, &compacted), 200);
        }
    }

    if faulted {
        return json_error("database query failed", 503);
    }
    json_error(&format!("BEEF not found for txid: {txid}"), 404)
}

/// Fetch the present chain height through the `CHAINTRACKS` service binding.
/// `Err((msg, status))` carries the exact error mapping `/tip` has always
/// served (binding 503, upstream 502); `/pots-view` maps any error to a
/// `tip: null` body instead (the D1 facts are still worth serving).
async fn chaintracks_present_height(
    ctx: &RouteContext<AuthState>,
    tag: &str,
) -> std::result::Result<u64, (&'static str, u16)> {
    let svc = match ctx.env.service("CHAINTRACKS") {
        Ok(svc) => svc,
        Err(e) => {
            console_warn!("[{tag}] CHAINTRACKS binding unavailable: {e}");
            return Err(("chaintracks binding unavailable", 503));
        }
    };
    let mut init = RequestInit::new();
    init.with_method(Method::Get);
    let headers = Headers::new();
    let _ = headers.set("Accept", "application/json");
    init.with_headers(headers);

    let mut resp = match svc.fetch(CHAINTRACKS_TIP_URL, Some(init)).await {
        Ok(resp) => resp,
        Err(e) => {
            console_warn!("[{tag}] chaintracks fetch failed: {e}");
            return Err(("chaintracks fetch failed", 502));
        }
    };
    if !(200..300).contains(&resp.status_code()) {
        console_warn!("[{tag}] chaintracks returned HTTP {}", resp.status_code());
        return Err(("chaintracks returned an error", 502));
    }
    let frame: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(e) => {
            console_warn!("[{tag}] chaintracks response not JSON: {e}");
            return Err(("chaintracks returned malformed JSON", 502));
        }
    };
    match parse_present_height(&frame) {
        Some(height) => Ok(height),
        None => {
            console_warn!("[{tag}] chaintracks frame not a success/height: {frame}");
            Err(("chaintracks returned an unexpected frame", 502))
        }
    }
}

/// `GET /tip` — present chain height via the `CHAINTRACKS` service binding
/// (`GET /getPresentHeight`, the same route the overlay's chain tracker
/// calls). A binding fault is 503, an upstream fault 502.
pub async fn tip(_req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    match chaintracks_present_height(&ctx, "tip").await {
        Ok(height) => json_response(tip_body(height), 200),
        Err((msg, status)) => json_error(msg, status),
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
        match stmt.all().await.and_then(|r| r.results::<PotsViewRowD1>()) {
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
    let rows: Vec<RecoveryRow> = match stmt.all().await.and_then(|r| r.results::<RecoveryRowD1>()) {
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
        match stmt.all().await.and_then(|r| r.results::<GameIdRowD1>()) {
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
struct ResultRowD1 {
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
    fn into_marker(self) -> Option<ResultMarkerRow> {
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

/// #411 round 2 — one `lb_marker_rows` page row (the write-time spine).
#[derive(Deserialize)]
struct LbRowD1 {
    #[serde(flatten)]
    marker: ResultRowD1,
    #[serde(rename = "markerRowid")]
    marker_rowid: Option<f64>,
    #[serde(rename = "potFirstMarkerAt")]
    pot_first_marker_at: Option<f64>,
    #[serde(rename = "orderAt")]
    order_at: Option<f64>,
    #[serde(rename = "unknownPot")]
    unknown_pot: Option<f64>,
}

impl LbRowD1 {
    fn into_page_row(self) -> Option<crate::logic::LbPageRow> {
        let marker_rowid = self.marker_rowid.map(|v| v as i64).unwrap_or(0);
        let pot_first_marker_at = self.pot_first_marker_at.map(|v| v as i64);
        let order_at = self.order_at.map(|v| v as i64);
        let unknown_pot = self.unknown_pot.map(|v| v as i64 != 0).unwrap_or(true);
        Some(crate::logic::LbPageRow {
            marker: self.marker.into_marker()?,
            marker_rowid,
            pot_first_marker_at,
            order_at,
            unknown_pot,
        })
    }
}

/// `proof_markers` pointer row — only the (gameId, winner) key and the marker
/// txid; the ~10-15 KB transcript `bundle` is never read here (the CLIENT
/// fetches + verifies it — this surface only points at where it lives).
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
    // #411: serve from the in-worker Cache API when fresh. The edge does NOT
    // zone-cache *.workers.dev responses, so the Cache-Control header alone
    // did nothing (measured 12-24 s under burst with 32 pollers); an explicit
    // cache.put honors the same max-age=5. Global view only - the answer
    // carries no identity. The durable fix (write-time decoded leaderboard
    // table) is tracked on #411.
    let cache_key = req.url()?.to_string();
    let cache = worker::Cache::default();
    if let Ok(Some(hit)) = cache.get(&cache_key, false).await {
        return Ok(hit);
    }
    let url = req.url()?;
    let limit_raw = url
        .query_pairs()
        .find(|(k, _)| k == "limit")
        .and_then(|(_, v)| v.parse::<u32>().ok());
    let limit = clamp_leaderboard_limit(limit_raw);

    let db = match ctx.env.d1("OVERLAY_DB") {
        Ok(db) => db,
        Err(e) => {
            console_warn!("[leaderboard] OVERLAY_DB binding unavailable: {e}");
            return json_error("database unavailable", 503);
        }
    };

    // 1) The recent-marker WINDOW (#332 / #335 item 2): pots newest-first by
    // their own admission stamp, ≤ RESULT_ROWS_PER_POT rows per pot, ghost
    // pots quota-bounded — the flat `ORDER BY createdAt DESC LIMIT ?` this
    // replaces was a flood-to-evict primitive with no incompleteness signal.
    // `limit + 1` pots are probed so truncation is DETECTABLE; the cut (and
    // the honest bit) happen in `leaderboard_window_cut` before any
    // malformed-row filtering can hide a pot.
    let quota = crate::logic::leaderboard_unknown_pot_quota(limit);
    let row_cap = (limit + 1) * overlay_discovery::result::storage::RESULT_ROWS_PER_POT;
    // #375: the era cutoff rides as `?4` iff configured — the board counts
    // from this spine, so every derived leg (statuses, classification,
    // attribution, proof pointers) inherits the write-off.
    let era = written_off_before_ms(&ctx);
    let mut binds: Vec<JsValue> = vec![
        JsValue::from_f64((limit + 1) as f64),
        JsValue::from_f64(quota as f64),
        JsValue::from_f64(row_cap as f64),
    ];
    if let Some(ms) = era {
        binds.push(era_bind(ms));
    }
    // #411 round 2 — FAST PATH first: two PLAIN-indexed pages over the
    // write-time spine (`lb_marker_rows`), tier/quota/rank replayed in Rust
    // (`lb_window_from_pages`). THE ZERO-LIE RULE: this result is served
    // ONLY when the pages PROVE the board over-full (≥ limit+1 distinct
    // pots) — every sparse, un-converged or doubtful board takes the old
    // windowed query below, which also BULK-MATERIALIZES the spine behind
    // it (`lb_backfill_sql`), so a missed companion write self-heals and the
    // fast path only ever replaces answers it can prove. A fast-path D1
    // fault falls through to the old path — never a 503 from this block.
    let mut fast: Option<(Vec<ResultMarkerRow>, bool)> = None;
    {
        let mut kb: Vec<JsValue> = vec![JsValue::from_f64(row_cap as f64)];
        if let Some(ms) = era {
            kb.push(era_bind(ms));
        }
        let mut ub: Vec<JsValue> =
            vec![JsValue::from_f64(crate::logic::LB_UNKNOWN_PAGE_ROWS as f64)];
        if let Some(ms) = era {
            ub.push(era_bind(ms));
        }
        let known_q = db
            .prepare(crate::logic::lb_page_sql(false, era))
            .bind(&kb);
        let unknown_q = db
            .prepare(crate::logic::lb_page_sql(true, era))
            .bind(&ub);
        if let (Ok(kq), Ok(uq)) = (known_q, unknown_q) {
            let known = kq.all().await.and_then(|r| r.results::<LbRowD1>());
            let unknown = uq.all().await.and_then(|r| r.results::<LbRowD1>());
            match (known, unknown) {
                (Ok(krows), Ok(urows)) => {
                    let now = (worker::Date::now().as_millis() / 1000) as i64;
                    let (m, t, distinct) = crate::logic::lb_window_from_pages(
                        krows.into_iter().filter_map(LbRowD1::into_page_row).collect(),
                        urows.into_iter().filter_map(LbRowD1::into_page_row).collect(),
                        limit,
                        quota,
                        now,
                    );
                    if distinct >= limit + 1 {
                        fast = Some((m, t));
                    }
                }
                (k, u) => {
                    if let Err(e) = k {
                        console_warn!("[leaderboard] lb spine known-page failed (fallback): {e}");
                    }
                    if let Err(e) = u {
                        console_warn!("[leaderboard] lb spine unknown-page failed (fallback): {e}");
                    }
                }
            }
        }
    }
    let (markers, mut truncated): (Vec<ResultMarkerRow>, bool) = match fast {
        Some(f) => f,
        None => {
            let stmt = db
                .prepare(crate::logic::leaderboard_markers_sql(era))
                .bind(&binds)?;
            let raw_rows: Vec<ResultRowD1> =
                match stmt.all().await.and_then(|r| r.results::<ResultRowD1>()) {
                    Ok(rows) => rows,
                    Err(e) => {
                        console_warn!("[leaderboard] result_markers_v2 window query failed: {e}");
                        return json_error("database query failed", 503);
                    }
                };
            let pot_keys: Vec<Option<String>> = raw_rows.iter().map(|r| r.pot_txid.clone()).collect();
            let (cut, truncated) = crate::logic::leaderboard_window_cut(&pot_keys, limit);
            let markers: Vec<ResultMarkerRow> = raw_rows
                .into_iter()
                .take(cut)
                .filter_map(ResultRowD1::into_marker)
                .collect();
            // The request just paid the window scan anyway — leave the spine
            // converged behind it. FAIL-OPEN (warn only).
            match db.prepare(crate::logic::lb_backfill_sql()).run().await {
                Ok(_) => {}
                Err(e) => console_warn!("[leaderboard] lb spine backfill failed: {e}"),
            }
            (markers, truncated)
        }
    };

    // 2) Pot spend-status join (potTxid:0), CHUNKED at D1_CHUNK_OUTPOINTS —
    // same discipline as /utxo-status. FAIL-SAFE: a chunk's D1 error is the
    // SAME 503 the client handles and serves no body (never a fabricated
    // all-unknown board that would silently zero every win).
    let mut outpoints = leaderboard_pot_outpoints(&markers);
    let mut pot_rows: Vec<PotRecordRow> = Vec::with_capacity(outpoints.len());
    for chunk in chunk_outpoints(&outpoints) {
        let mut binds: Vec<JsValue> = Vec::with_capacity(chunk.len() * 2);
        for op in chunk {
            binds.push(JsValue::from_str(&op.db_txid()));
            binds.push(JsValue::from_f64(f64::from(op.vout)));
        }
        let stmt = db.prepare(batch_where_sql(chunk.len())).bind(&binds)?;
        match stmt.all().await.and_then(|r| r.results::<PotRowD1>()) {
            Ok(chunk_rows) => pot_rows.extend(chunk_rows.into_iter().map(PotRowD1::into_row)),
            Err(e) => {
                console_warn!("[leaderboard] pot_records batch query failed: {e}");
                return json_error("database query failed", 503);
            }
        }
    }
    // 2b) #399 (OWNER RULED 2026-08-21): the CHAIN candidate window — a
    // marker was never required to WIN, it must not be required to be
    // LISTED. Classified winner pots join the candidate set straight from
    // `pot_records`, so the spine (which has counted from chain facts alone
    // since #332 v3) finally SEES the pots whose claim marker never landed.
    // The stored verdict is TRUSTED here by write-path provenance (only the
    // overlay's own spend classification writes it; `verdictTxid =
    // spendingTxid` is the freshness bar — see `chain_win_pots_sql`'s doc,
    // gate S1); the COUNT bar stays the aggregate's confirmed-landing +
    // spender re-check. BEST-EFFORT: a fault here only narrows the candidate
    // set back to the marker window — exactly yesterday's board, never a 5xx.
    {
        let mut cbinds: Vec<JsValue> = vec![JsValue::from_f64((limit + 1) as f64)];
        if let Some(ms) = era {
            cbinds.push(era_bind(ms));
        }
        match db
            .prepare(crate::logic::chain_win_pots_sql(era))
            .bind(&cbinds)
        {
            Ok(stmt) => match stmt.all().await.and_then(|r| r.results::<PotRowD1>()) {
                Ok(rows) => {
                    // Gate S2: the honesty bit is decided by what the QUERY
                    // returned, not by how the loop consumed it. The first
                    // draft flagged truncation only when `fresh` filled the
                    // budget — but marker-carried dups are skipped without
                    // counting, so a probe page containing dups could be
                    // consumed to the end with candidates beyond the SQL
                    // LIMIT silently invisible: a complete-looking partial
                    // board, the one lie this route may never tell.
                    // Over-flagging on the exact-boundary case is the honest
                    // direction.
                    if rows.len() > limit {
                        truncated = true;
                    }
                    let seen: std::collections::HashSet<(String, u32)> = outpoints
                        .iter()
                        .map(|o| (o.txid.to_ascii_lowercase(), o.vout))
                        .collect();
                    let mut fresh = 0usize;
                    for row in rows.into_iter().map(PotRowD1::into_row) {
                        if fresh >= limit {
                            break; // budget spent (bit already set above)
                        }
                        let key = (row.txid.to_ascii_lowercase(), row.vout);
                        if seen.contains(&key) {
                            continue; // already carried by the marker window
                        }
                        fresh += 1;
                        outpoints.push(crate::logic::Outpoint {
                            txid: row.txid.clone(),
                            vout: row.vout,
                        });
                        pot_rows.push(row);
                    }
                }
                Err(e) => console_warn!(
                    "[leaderboard] chain-win candidate query failed (marker window only): {e}"
                ),
            },
            Err(e) => console_warn!(
                "[leaderboard] chain-win candidate bind failed (marker window only): {e}"
            ),
        }
    }
    let statuses = assemble_statuses(&outpoints, &pot_rows);

    // 3) proof_markers pointers, KEYED to the window's own (gameId, winner)
    // pairs (#332 — this replaces a flat `LIMIT 2000` newest-first scan
    // whose newest-per-key fold was floodable AND a repoint primitive; see
    // `proof_pointers_sql`). The SQL returns a bounded SUPERSET per key
    // (HIGH-1: gameId + winner are claimable names, so a single-pointer slot
    // would be squattable — the client filters the set by transcript
    // validity). BEST-EFFORT: a fault on any chunk only omits those pairs'
    // proofTxids hint, never a 5xx and never a count.
    let mut proof_map: std::collections::HashMap<(String, String), Vec<String>> =
        std::collections::HashMap::new();
    let mut pairs: Vec<(String, String)> = Vec::new();
    let mut seen_pairs = std::collections::HashSet::new();
    for m in &markers {
        if pairs.len() >= crate::logic::LEADERBOARD_PROOF_PAIRS_CAP {
            break; // display-hint bound only — counts are unaffected
        }
        let key = (
            m.game_id.to_ascii_lowercase(),
            m.winner.to_ascii_lowercase(),
        );
        if seen_pairs.insert(key) {
            // Bind the row's VERBATIM values (the same bytes the producer
            // wrote), so the SQL byte-compare matches; map keys stay
            // lowercase (the evidence lookup key).
            pairs.push((m.game_id.clone(), m.winner.clone()));
        }
    }
    for chunk in pairs.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
        let mut binds: Vec<JsValue> = Vec::with_capacity(chunk.len() * 2);
        for (g, w) in chunk {
            binds.push(JsValue::from_str(g));
            binds.push(JsValue::from_str(w));
        }
        let stmt = match db
            .prepare(crate::logic::proof_pointers_sql(chunk.len()))
            .bind(&binds)
        {
            Ok(s) => s,
            Err(e) => {
                console_warn!("[leaderboard] proof_markers bind failed (proofTxid omitted): {e}");
                continue;
            }
        };
        match stmt
            .all()
            .await
            .and_then(|r| r.results::<ProofPointerRowD1>())
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
            Err(e) => {
                console_warn!("[leaderboard] proof_markers query failed (proofTxid omitted): {e}")
            }
        }
    }

    // 4) Server-derived CHAIN classification of the spent pots (bsv-low #227)
    // — an ADDITIVE truth source folded in alongside the client claims.
    // BEST-EFFORT + BOUNDED: bounded by the WINDOW (≤ limit+1 pots, ranked by
    // the pot's own admission stamp — #332 deleted the separate 64-pot cap,
    // which an attacker could ORDER), pot_beefs fetched in ≤45-bind chunks
    // (the D1 param-cap discipline); any fault only omits classifications
    // (counting falls back to the claim rules) — never a 5xx, never a
    // fabricated verdict.
    let (verdicts, params_by_pot, signers_by_pot) = classify_spent_pots(&db, &statuses).await;

    // 5) #230 seat attribution — the DISPLAY identity mapping only (#332 v3:
    // the WIN is counted from the verdict + committed key in the aggregate,
    // so this can never erase a win, only resolve who to show it under). The
    // candidate read is WIDENED to `LEADERBOARD_SEAT_CANDIDATES` (vs
    // `SEAT_MARKERS_PER_KEY` on /results) so a realistic junk flood under the
    // committed key cannot push the one VERIFIED honest marker out of the
    // candidate set before `attribute_seats` validity-filters it. Beyond the
    // cap the identity degrades to UNKNOWN (the aggregate keys the win by the
    // settle key) — never to no-win. BEST-EFFORT: any fault yields an empty
    // map → every counted win shows under its settle key, still never dropped.
    let attributions = seat_attributions(
        &db,
        &params_by_pot,
        crate::results::LEADERBOARD_SEAT_CANDIDATES,
    )
    .await;

    let lb = crate::logic::aggregate_leaderboard_attributed(
        &markers,
        &statuses,
        &proof_map,
        limit,
        &verdicts,
        &attributions,
        &params_by_pot,
        &signers_by_pot,
    );
    let computed_at = (worker::Date::now().as_millis() / 1000) as i64;
    {
        let mut resp = json_response_cached(
            leaderboard_body(&lb, computed_at, markers.len(), truncated),
            200,
            5,
        )?;
        let for_cache = resp.cloned()?;
        let _ = cache.put(&cache_key, for_cache).await;
        Ok(resp)
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
#[derive(Deserialize)]
struct DecodedPotRowD1 {
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
    fn covenant_params(&self) -> Option<crate::results::CovenantParams> {
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
async fn classify_spent_pots(
    db: &worker::D1Database,
    statuses: &[crate::logic::OutpointStatus],
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
    for chunk in all_pairs.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
        let sql = crate::results::decoded_pots_sql(chunk.len());
        let mut binds: Vec<JsValue> = Vec::with_capacity(chunk.len() * 2);
        for (pot, _) in chunk {
            binds.push(JsValue::from_str(pot));
            binds.push(JsValue::from_f64(f64::from(
                crate::logic::LEADERBOARD_POT_VOUT,
            )));
        }
        let stmt = match db.prepare(sql).bind(&binds) {
            Ok(s) => s,
            Err(e) => {
                console_warn!("[leaderboard] decoded-pots bind failed (column tier skipped): {e}");
                break;
            }
        };
        match stmt
            .all()
            .await
            .and_then(|r| r.results::<DecodedPotRowD1>())
        {
            Ok(rows) => {
                let by_txid: std::collections::HashMap<String, DecodedPotRowD1> = rows
                    .into_iter()
                    .map(|r| (r.txid.to_ascii_lowercase(), r))
                    .collect();
                for (pot, spender) in chunk {
                    let Some(row) = by_txid.get(pot) else {
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
                console_warn!("[leaderboard] decoded-pots query failed (column tier partial): {e}");
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
        let placeholders = vec!["?"; chunk.len()].join(",");
        let sql =
            format!("SELECT txid, hex(beef) AS beef FROM pot_beefs WHERE txid IN ({placeholders})");
        let binds: Vec<JsValue> = chunk.iter().map(|k| JsValue::from_str(k)).collect();
        let stmt = match db.prepare(sql).bind(&binds) {
            Ok(s) => s,
            Err(e) => {
                console_warn!("[leaderboard] pot_beefs bind failed (classification omitted): {e}");
                return (verdicts, params_by_pot, signers_by_pot);
            }
        };
        match stmt.all().await.and_then(|r| r.results::<PotBeefRowD1>()) {
            Ok(rows) => {
                for r in rows {
                    if let Some(bytes) = r.beef.and_then(|h| decode_beef_hex(&h)) {
                        beefs.insert(r.txid.to_ascii_lowercase(), bytes);
                    }
                }
            }
            Err(e) => {
                console_warn!("[leaderboard] pot_beefs query failed (classification partial): {e}");
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
struct SeatMarkerRowD1 {
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

/// #230: build the pot → [`crate::results::SeatAttribution`] map for the
/// CLASSIFIED pots — fetch their `LOW/potparty/v2` marker rows (chunked, D1
/// param-cap discipline), VERIFY each seat signature (real secp256k1, in
/// `attribute_seats`) against the pot's committed lock keys, and fold.
/// BEST-EFFORT: any fault yields an empty/partial map — counting falls back
/// to the claim rules, never a guess, never a 5xx.
async fn seat_attributions(
    db: &worker::D1Database,
    params_by_pot: &std::collections::HashMap<String, crate::results::CovenantParams>,
    candidate_cap: usize,
) -> std::collections::HashMap<String, crate::results::SeatAttribution> {
    let mut out = std::collections::HashMap::new();
    if params_by_pot.is_empty() {
        return out;
    }
    let mut pots: Vec<&String> = params_by_pot.keys().collect();
    pots.sort_unstable();
    let mut markers_by_pot: std::collections::HashMap<String, Vec<crate::results::SeatMarkerRow>> =
        std::collections::HashMap::new();
    for chunk in pots.chunks(crate::results::SEAT_MARKERS_CHUNK_POTS) {
        // F2 (2026-07-28 gate): the fetch is filtered to each pot's OWN
        // COMMITTED settle keys and windowed PER KEY SLOT. #332 v3: the
        // WINDOW CAP is the caller's `candidate_cap` — the leaderboard reads a
        // WIDE candidate set so a junk flood under the committed key cannot
        // push the verified honest marker out before `attribute_seats`
        // validity-filters it (the win is already chain-counted, so this only
        // decides IDENTITY vs settle-key display, never win vs no-win).
        let sql = crate::results::seat_markers_sql(chunk.len(), candidate_cap);
        // Four binds per pot: (potTxid, potVout, pubA, pubB) — the keys come
        // from the pot's committed funding lock (decoded columns, or the
        // hash-verified funding bytes on the legacy fallback), never from a
        // stored potparty claim.
        let mut binds: Vec<JsValue> =
            Vec::with_capacity(chunk.len() * crate::results::SEAT_MARKERS_BINDS_PER_POT);
        for pot in chunk {
            let p = &params_by_pot[*pot];
            binds.push(JsValue::from_str(pot));
            // #281: potVout is a BIND now (it was hardcoded) so `/results`
            // can share this query; the board is vout-0 by definition.
            binds.push(JsValue::from_f64(f64::from(
                crate::logic::LEADERBOARD_POT_VOUT,
            )));
            binds.push(JsValue::from_str(&hex::encode(p.pub_a)));
            binds.push(JsValue::from_str(&hex::encode(p.pub_b)));
        }
        let stmt = match db.prepare(sql).bind(&binds) {
            Ok(s) => s,
            Err(e) => {
                console_warn!("[leaderboard] potparty v2 bind failed (attribution omitted): {e}");
                return out;
            }
        };
        match stmt
            .all()
            .await
            .and_then(|r| r.results::<SeatMarkerRowD1>())
        {
            Ok(rows) => {
                for r in rows {
                    let (Some(pk), Some(seat_sig), Some(id_sig)) =
                        (r.seat_settle_pubkey, r.seat_sig_hex, r.sig_hex)
                    else {
                        continue;
                    };
                    markers_by_pot
                        .entry(r.pot_txid.to_ascii_lowercase())
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
            Err(e) => {
                // A racing pre-migration schema (no seatSettlePubkey column
                // yet) or any D1 fault: attribution is simply omitted.
                console_warn!("[leaderboard] potparty v2 query failed (attribution partial): {e}");
            }
        }
    }
    const NO_MARKERS: &[crate::results::SeatMarkerRow] = &[];
    for (pot, params) in params_by_pot {
        // A pot with NO potparty rows is no longer skipped: the hop fallback
        // below can still attribute it, and skipping here was why an entirely
        // marker-less pot could never resolve.
        let markers = markers_by_pot.get(pot).map_or(NO_MARKERS, Vec::as_slice);
        let attr = crate::results::attribute_seats(
            params,
            pot,
            crate::logic::LEADERBOARD_POT_VOUT,
            markers,
        );
        if attr != crate::results::SeatAttribution::default() {
            out.insert(pot.clone(), attr);
        }
    }

    // ── HOP fallback (2026-08-14) ──────────────────────────────────────────
    //
    // The potparty v2 marker is published at the END of a hand and races
    // teardown. On 2026-08-13 four 20k beta hands settled identically on
    // chain and one (`c9a4af3a…`) came back `unresolved` on 3 of 4 rows — the
    // pot was spent, confirmed, `winner-b`, and PAID; only the attribution was
    // missing. The same binding exists in `hopparty_records`, written at FUND
    // time seconds into the hand, and for that game BOTH seats were present
    // and `markerValid` 8 seconds before the pot even existed.
    //
    // Fill-only, and the CHAIN stays the authority — see
    // `fill_seats_from_hop_markers`. Best-effort like everything here: a fault
    // leaves attribution exactly as the pot markers left it.
    let needs_fallback: Vec<&String> = {
        let mut v: Vec<&String> = params_by_pot
            .keys()
            .filter(|pot| {
                out.get(*pot)
                    .is_none_or(|a| a.identity_a.is_none() || a.identity_b.is_none())
            })
            .collect();
        v.sort_unstable();
        v
    };
    for chunk in needs_fallback.chunks(crate::results::SEAT_MARKERS_CHUNK_POTS) {
        let sql = crate::results::hop_seat_markers_sql(chunk.len());
        let mut binds: Vec<JsValue> =
            Vec::with_capacity(chunk.len() * crate::results::HOP_SEAT_BINDS_PER_POT);
        for pot in chunk {
            let p = &params_by_pot[*pot];
            binds.push(JsValue::from_str(&hex::encode(p.pub_a)));
            binds.push(JsValue::from_str(&hex::encode(p.pub_b)));
        }
        let Ok(stmt) = db.prepare(&sql).bind(&binds) else {
            continue;
        };
        match stmt.all().await.and_then(|r| r.results::<HopSeatRowD1>()) {
            Ok(rows) => {
                let hops: Vec<crate::results::HopSeatRow> = rows
                    .into_iter()
                    .map(|r| crate::results::HopSeatRow {
                        identity: r.identity,
                        seat_settle_pubkey: r.seat_settle_pubkey,
                        marker_valid: r.marker_valid.map(|v| v != 0.0),
                    })
                    .collect();
                for pot in chunk {
                    let params = &params_by_pot[*pot];
                    let mut attr = out.get(*pot).cloned().unwrap_or_default();
                    crate::results::fill_seats_from_hop_markers(&mut attr, params, &hops);
                    if attr != crate::results::SeatAttribution::default() {
                        out.insert((*pot).clone(), attr);
                    }
                }
            }
            Err(e) => {
                console_warn!(
                    "[leaderboard] hop seat fallback query failed (attribution unchanged): {e}"
                );
            }
        }
    }
    out
}

/// One `hopparty_records` row as D1 returns it for the seat fallback.
#[derive(Deserialize)]
struct HopSeatRowD1 {
    identity: String,
    #[serde(rename = "seatSettlePubkey")]
    seat_settle_pubkey: String,
    #[serde(rename = "markerValid")]
    marker_valid: Option<f64>,
}

// ── /results — server-derived settle results (bsv-low #227) ─────────────────

/// `/results` joined row as D1 returns it (the `results_sql` shape): the
/// caller's potparty facts + spend pointer + BOTH stored BEEFs as hex.
#[derive(Deserialize)]
struct ResultsRowD1 {
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
}

impl ResultsRowD1 {
    fn into_row(self) -> crate::results::ResultsRow {
        crate::results::ResultsRow {
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
            verdict: self.verdict,
            verdict_txid: self.verdict_txid,
            spent_height: self.spent_height.map(|v| v as u64),
            spender_proof_verified: self.spender_proof_verified.map(|v| v != 0.0),
            spender_seen: self.spender_seen.map(|v| v != 0.0),
            spender_final: self.spender_final.map(|v| v != 0.0),
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

    if !crate::logic::valid_identity(&identity_lc) {
        return json_response(
            crate::results::results_body(&identity_lc, &[], false, after),
            200,
        );
    }

    let db = match ctx.env.d1("OVERLAY_DB") {
        Ok(db) => db,
        Err(e) => {
            console_warn!("[results] OVERLAY_DB binding unavailable: {e}");
            return json_error("database unavailable", 503);
        }
    };

    let (entries, truncated) =
        match gather_result_entries(&db, &identity_lc, written_off_before_ms(&ctx), after).await {
            Ok(v) => v,
            Err(e) => {
                console_warn!("[results] {e}");
                return json_error("database query failed", 503);
            }
        };
    json_response(
        crate::results::results_body(&identity_lc, &entries, truncated, after),
        200,
    )
}

/// The whole `/results` derivation for one identity — rows, claims, seat
/// proofs, outcome words — extracted so `/recovery-view` REUSES it for its
/// `outcome` field (#252 stage A, Rule 15: derive once; a second
/// implementation of the outcome word would drift). `Err` = the PRIMARY
/// results query failed (each caller keeps its own posture: `/results`
/// answers 503, `/recovery-view` serves null outcomes — best-effort). The
/// claims/seat legs stay BEST-EFFORT inside, exactly as before.
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
    let mut rows: Vec<crate::results::ResultsRow> =
        match stmt.all().await.and_then(|r| r.results::<ResultsRowD1>()) {
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
        match stmt.all().await.and_then(|r| r.results::<ResultRowD1>()) {
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
    let hop_markers = results_hop_seat_markers(db, &params_by_pot).await;
    // Brain-cutover M2: the published hand markers for this page's games —
    // the read the CLIENT used to make against `ls_hand` (plus an ECDSA per
    // row on its main thread, #401). BEST-EFFORT with the same discipline as
    // claims: a fault omits hands from the drill-down, never a 5xx and never
    // a money path (display-only index).
    let hand_facts = results_hand_markers(db, &game_ids).await;

    Ok((
        crate::results::assemble_results(
            identity_lc,
            rows,
            &claims,
            &seat_markers,
            &params_by_pot,
            &hop_markers,
            &hand_facts,
        ),
        truncated,
    ))
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
            .all()
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
) -> std::collections::HashMap<(String, u32), Vec<crate::results::HopSeatRow>> {
    let mut out: std::collections::HashMap<(String, u32), Vec<crate::results::HopSeatRow>> =
        std::collections::HashMap::new();
    if params_by_pot.is_empty() {
        return out;
    }
    let mut keys: Vec<&(String, u32)> = params_by_pot.keys().collect();
    keys.sort_unstable();
    for chunk in keys.chunks(crate::results::SEAT_MARKERS_CHUNK_POTS) {
        let sql = crate::results::hop_seat_markers_sql(chunk.len());
        let mut binds: Vec<JsValue> =
            Vec::with_capacity(chunk.len() * crate::results::HOP_SEAT_BINDS_PER_POT);
        for k in chunk {
            let p = &params_by_pot[*k];
            binds.push(JsValue::from_str(&hex::encode(p.pub_a)));
            binds.push(JsValue::from_str(&hex::encode(p.pub_b)));
        }
        let Ok(stmt) = db.prepare(&sql).bind(&binds) else {
            continue;
        };
        match stmt.all().await.and_then(|r| r.results::<HopSeatRowD1>()) {
            Ok(rows) => {
                let hops: Vec<crate::results::HopSeatRow> = rows
                    .into_iter()
                    .map(|r| crate::results::HopSeatRow {
                        identity: r.identity,
                        seat_settle_pubkey: r.seat_settle_pubkey,
                        marker_valid: r.marker_valid.map(|v| v != 0.0),
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
                console_warn!("[results] hop seat fallback query failed (binding unchanged): {e}");
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
            .all()
            .await
            .and_then(|r| r.results::<SeatMarkerRowD1>())
        {
            Ok(fetched) => {
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
            Err(e) => {
                // A racing pre-migration schema (no seatSettlePubkey column
                // yet) or any D1 fault: attribution is simply omitted.
                console_warn!("[results] seat-marker query failed (attribution partial): {e}");
            }
        }
    }
    out
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
struct RefundViewRowD1 {
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
    fn into_row(self) -> crate::refund_view::RefundViewRow {
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
        .all()
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
struct HopsViewRowD1 {
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
}

impl HopsViewRowD1 {
    fn into_row(self) -> crate::hops_view::HopsViewRow {
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
    let rows: Vec<crate::hops_view::HopsViewRow> =
        match stmt.all().await.and_then(|r| r.results::<HopsViewRowD1>()) {
            Ok(rows) => rows.into_iter().map(HopsViewRowD1::into_row).collect(),
            Err(e) => {
                console_warn!("[hops-view] hopparty join query failed: {e}");
                return json_error("database query failed", 503);
            }
        };

    let (entries, truncated) = crate::hops_view::assemble_hops_view(rows);
    // The tip AFTER the D1 facts (`null` on a fault — facts still serve).
    let tip = chaintracks_present_height(&ctx, "hops-view").await.ok();
    json_response(
        crate::hops_view::hops_view_body(&identity_lc, tip, &entries, truncated, after),
        200,
    )
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
            .all()
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
            .all()
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
    let mut rows: Vec<crate::live_view::LiveViewRow> =
        match stmt.all().await.and_then(|r| r.results::<LiveViewRowD1>()) {
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

/// Fetch a URL, returning `(status, body_bytes)`. Faults map to `None`.
async fn provider_get(url: &str) -> Option<(u16, Vec<u8>)> {
    let mut init = RequestInit::new();
    init.with_method(Method::Get);
    let request = worker::Request::new_with_init(url, &init).ok()?;
    let mut response = worker::Fetch::Request(request).send().await.ok()?;
    let status = response.status_code();
    let body = response.bytes().await.unwrap_or_default();
    Some((status, body))
}

const WOC_BASE: &str = "https://api.whatsonchain.com/v1/bsv/main";
const BITAILS_BASE: &str = "https://api.bitails.io";

/// Resolve ONE outpoint against the upstream providers, per the
/// proof-source-order doctrine (see `results.rs`'s `/spent-any` section):
/// positive = WoC pointer + raw hash/input verification (raw from WoC, then
/// Bitails); negative = requires clean Bitails corroboration; any fault =
/// honest unknown.
async fn spent_any_resolve(txid_lc: &str, vout: u32) -> SpentAnyCached {
    use crate::results::{
        parse_bitails_unspent, parse_woc_spent_body, spender_raw_verifies, SpentObservation,
        UnspentCorroboration,
    };

    let woc = match provider_get(&format!("{WOC_BASE}/tx/{txid_lc}/{vout}/spent")).await {
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

    let mut spender_raw_ok = false;
    let mut bitails = UnspentCorroboration::Unknown;
    match &woc {
        SpentObservation::Spent { txid: spender, .. } => {
            // Raw verification: WoC hex first, Bitails binary fallback. A
            // positive is served ONLY when the raw hashes to the reported
            // spender AND spends the requested outpoint.
            let raw = match provider_get(&format!("{WOC_BASE}/tx/{spender}/hex")).await {
                Some((200, body)) => std::str::from_utf8(&body)
                    .ok()
                    .and_then(|h| hex::decode(h.trim()).ok()),
                _ => None,
            };
            let raw = match raw {
                Some(r) => Some(r),
                None => {
                    match provider_get(&format!("{BITAILS_BASE}/download/tx/{spender}")).await {
                        Some((200, body)) if !body.is_empty() => Some(body),
                        _ => None,
                    }
                }
            };
            if let Some(raw) = raw {
                spender_raw_ok = spender_raw_verifies(&raw, spender, txid_lc, vout);
            }
        }
        SpentObservation::NotSpent => {
            // Negative corroboration (never WoC-only). Bitails' outpoint
            // endpoint 500s at the time of writing — parse_bitails_unspent is
            // strict, so that fault surfaces as known:false (fail-safe).
            bitails =
                match provider_get(&format!("{BITAILS_BASE}/tx/{txid_lc}/output/{vout}/spent"))
                    .await
                {
                    Some((status, body)) => {
                        let v = serde_json::from_slice::<serde_json::Value>(&body).ok();
                        parse_bitails_unspent(status, v.as_ref())
                    }
                    None => UnspentCorroboration::Unknown,
                };
        }
        SpentObservation::Fault => {}
    }

    let st = crate::results::decide_spent_any(&woc, spender_raw_ok, bitails);
    SpentAnyCached {
        known: st.known,
        spent: st.spent,
        spending_txid: st.spending_txid,
        spent_confirmed: st.spent_confirmed,
        reason: st.reason,
    }
}

/// `GET /spent-any?outpoints=<txid>.<vout>,…` — spend status for ARBITRARY
/// outpoints (legacy escrows the overlay never indexed), answered by
/// SERVER-SIDE provider reads so the browser stops calling WhatsOnChain
/// directly (bsv-low #227 addendum). Same row shape as `/utxo-status`;
/// capped at [`crate::results::SPENT_ANY_MAX_OUTPOINTS`]; ~15 s in-isolate
/// cache. `known:false` is the honest answer for every provider fault or
/// un-corroborated negative — this surface never asserts what it cannot
/// verify (positives are raw-hash + input-match verified).
pub async fn spent_any(req: Request, _ctx: RouteContext<AuthState>) -> Result<Response> {
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

    let now = worker::Date::now().as_millis() as f64;
    let mut entries: Vec<crate::logic::OutpointStatus> = Vec::with_capacity(outpoints.len());
    for op in &outpoints {
        let key = format!("{}.{}", op.db_txid(), op.vout);
        let cached = SPENT_ANY_CACHE.with(|c| {
            c.borrow()
                .get(&key)
                .filter(|(expiry, _)| *expiry > now)
                .map(|(_, row)| row.clone())
        });
        let row = match cached {
            Some(row) => row,
            None => {
                let row = spent_any_resolve(&op.db_txid(), op.vout).await;
                SPENT_ANY_CACHE.with(|c| {
                    let mut map = c.borrow_mut();
                    // Prune expired entries so the map stays bounded.
                    map.retain(|_, (expiry, _)| *expiry > now);
                    map.insert(
                        key,
                        (now + crate::results::SPENT_ANY_CACHE_TTL_MS, row.clone()),
                    );
                });
                row
            }
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
        });
    }

    json_response(utxo_status_body(&entries), 200)
}

// ── /tx-any — tx-level presence/confirmation/raw, index-first (bsv-low #229) ──

/// One cached `/tx-any` answer.
type TxAnyCached = crate::txany::TxAnyAnswer;

thread_local! {
    /// In-isolate `/tx-any` cache (txid → (expiry ms, answer)) — the same
    /// pattern (and rationale) as `SPENT_ANY_CACHE` above.
    static TX_ANY_CACHE: std::cell::RefCell<std::collections::HashMap<String, (f64, TxAnyCached)>> =
        std::cell::RefCell::new(std::collections::HashMap::new());
    /// Bitails tx-route health memo: `Some(true)` once the known-mined anchor
    /// served (SUCCESS memoized only — a probe fault re-probes next time).
    /// Ported from the client's `bitailsRouteHealthy` route-rot guard.
    static BITAILS_ROUTE_HEALTHY: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
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
        let row: Option<BeefTrustRow> = match stmt.first(None).await {
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

/// The BREAK-GLASS external leg: WoC presence/confirmations + hash-verified
/// raw (WoC hex, Bitails binary fallback); absence corroborated by a Bitails
/// 404 behind a healthy route. See `txany.rs` for the full bar.
async fn tx_any_external_leg(
    txid_lc: &str,
) -> (
    crate::txany::TxObservation,
    crate::txany::AbsenceCorroboration,
) {
    use crate::txany::{AbsenceCorroboration, TxObservation};

    let woc = match provider_get(&format!("{WOC_BASE}/tx/hash/{txid_lc}")).await {
        Some((200, body)) => match serde_json::from_slice::<serde_json::Value>(&body) {
            Ok(v) => {
                let confirmed = crate::txany::parse_woc_confirmations(&v);
                // Positive presence requires the raw in hand, hash-verified
                // (WoC hex first, Bitails binary fallback).
                let raw = match provider_get(&format!("{WOC_BASE}/tx/{txid_lc}/hex")).await {
                    Some((200, body)) => std::str::from_utf8(&body)
                        .ok()
                        .and_then(|h| hex::decode(h.trim()).ok()),
                    _ => None,
                };
                let raw = match raw {
                    Some(r) => Some(r),
                    None => {
                        match provider_get(&format!("{BITAILS_BASE}/download/tx/{txid_lc}")).await {
                            Some((200, body)) if !body.is_empty() => Some(body),
                            _ => None,
                        }
                    }
                };
                let raw_hex = raw.and_then(|r| crate::txany::verify_raw_bytes(&r, txid_lc));
                TxObservation::Present { confirmed, raw_hex }
            }
            Err(_) => TxObservation::Fault,
        },
        Some((404, _)) => TxObservation::Absent,
        _ => TxObservation::Fault,
    };

    let mut absence = AbsenceCorroboration::Unknown;
    if woc == TxObservation::Absent {
        // Corroborate: Bitails must ALSO definitively 404 the txid…
        let bitails_404 = matches!(
            provider_get(&format!("{BITAILS_BASE}/download/tx/{txid_lc}")).await,
            Some((404, _))
        );
        if bitails_404 {
            // …and its tx route must prove healthy against the known-mined
            // anchor (route-rot would otherwise fake absence for every txid).
            if BITAILS_ROUTE_HEALTHY.with(std::cell::Cell::get) != Some(true) {
                if let Some((200, body)) = provider_get(&format!(
                    "{BITAILS_BASE}/download/tx/{}",
                    crate::txany::KNOWN_MINED_TXID
                ))
                .await
                {
                    if !body.is_empty() {
                        BITAILS_ROUTE_HEALTHY.with(|c| c.set(Some(true)));
                    }
                }
            }
            if BITAILS_ROUTE_HEALTHY.with(std::cell::Cell::get) == Some(true) {
                absence = AbsenceCorroboration::CorroboratedAbsent;
            }
        }
    }
    (woc, absence)
}

/// `GET /tx-any/:txid` — presence / confirmation / hash-verified raw for an
/// arbitrary txid, INDEX-FIRST (the overlay is the system of record for every
/// tx LOW broadcast; external indexers are break-glass for legacy/foreign
/// txids only — owner doctrine, bsv-low #229). ~15 s in-isolate cache.
/// Unknown is the honest answer for every fault (`present: null`).
pub async fn tx_any(_req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    let Some(txid) = ctx.param("txid").cloned() else {
        return json_error("missing txid", 400);
    };
    if !valid_txid(&txid) {
        return json_error("invalid txid (expect 64 hex chars)", 400);
    }
    let key = txid.to_ascii_lowercase();

    let now = worker::Date::now().as_millis() as f64;
    let cached = TX_ANY_CACHE.with(|c| {
        c.borrow()
            .get(&key)
            .filter(|(expiry, _)| *expiry > now)
            .map(|(_, a)| a.clone())
    });
    let answer = match cached {
        Some(a) => a,
        None => {
            let (index_raw, index_height) = tx_any_index_leg(&ctx, &key).await;
            // Fully index-native when the BUMP proves the mine — zero
            // external reads. Otherwise consult the break-glass leg: for an
            // admitted-but-unproven tx it now answers the PRESENCE question
            // too (bsv-low #247 — own-store bytes with no BUMP are not
            // network truth); for an index miss it is the whole answer.
            let mut answer = if index_raw.is_some() && index_height.is_some() {
                crate::txany::decide_tx_any(
                    index_raw,
                    index_height,
                    None,
                    crate::txany::AbsenceCorroboration::Unknown,
                )
            } else {
                let (external, absence) = tx_any_external_leg(&key).await;
                crate::txany::decide_tx_any(index_raw, index_height, Some(&external), absence)
            };
            // #247 provably-unconfirmable probe: ONLY for a corroborated
            // network-absent tx whose bytes we hold (rare — the zombie
            // class). If an input is verified spent by a DIFFERENT confirmed
            // tx, this tx can never land: a terminal skip the client may
            // consume to stop bounded rebroadcasts. Bounded to the first 3
            // inputs; every weaker observation proves nothing (stays false).
            if answer.present == Some(false) {
                if let Some(inputs) = answer
                    .raw_hex
                    .as_deref()
                    .and_then(|h| hex::decode(h).ok())
                    .and_then(|b| bsv_rs::transaction::Transaction::from_binary(&b).ok())
                    .map(|tx| tx.inputs)
                {
                    for input in inputs.iter().take(3) {
                        let Some(src_txid) = input.source_txid.as_deref() else {
                            continue;
                        };
                        let st = spent_any_resolve(
                            &src_txid.to_ascii_lowercase(),
                            input.source_output_index,
                        )
                        .await;
                        if crate::txany::input_proves_unconfirmable(
                            &key,
                            st.known,
                            st.spent,
                            st.spending_txid.as_deref(),
                            st.spent_confirmed,
                        ) {
                            console_warn!(
                                "[tx-any] {key} PROVABLY UNCONFIRMABLE — input {}:{} spent by a different confirmed tx",
                                src_txid,
                                input.source_output_index
                            );
                            answer.unconfirmable = true;
                            break;
                        }
                    }
                }
            }
            TX_ANY_CACHE.with(|c| {
                let mut map = c.borrow_mut();
                map.retain(|_, (expiry, _)| *expiry > now);
                map.insert(
                    key.clone(),
                    (now + crate::txany::TX_ANY_CACHE_TTL_MS, answer.clone()),
                );
            });
            answer
        }
    };

    json_response(crate::txany::tx_any_body(&key, &answer), 200)
}

/// `GET /health` — liveness only (no DB touch).
pub fn health(_req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
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
