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
//! of outpoints with ONE D1 query (`batch_where_sql`), so a home-mount
//! gather is one request → one query. Every response is `no-store`.

use serde::Deserialize;
use worker::wasm_bindgen::JsValue;
use worker::{console_warn, Headers, Method, Request, RequestInit, Response, Result, RouteContext};

use crate::logic::{
    assemble_statuses, batch_where_sql, beef_body, decode_beef_hex, health_body, parse_outpoints,
    parse_present_height, tip_body, utxo_status_body, valid_txid, PotRecordRow,
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
}

impl PotRowD1 {
    fn into_row(self) -> PotRecordRow {
        PotRecordRow {
            txid: self.txid,
            vout: self.output_index as u32,
            spent: self.spent != 0.0,
            spending_txid: self.spending_txid,
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
pub async fn utxo_status(req: Request, ctx: RouteContext<()>) -> Result<Response> {
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
        },
    };

    // ONE query for the whole batch: WHERE (txid=? AND outputIndex=?) OR …
    let mut binds: Vec<JsValue> = Vec::with_capacity(outpoints.len() * 2);
    for op in &outpoints {
        binds.push(JsValue::from_str(&op.db_txid()));
        binds.push(JsValue::from_f64(f64::from(op.vout)));
    }
    let stmt = db.prepare(batch_where_sql(outpoints.len())).bind(&binds)?;
    let rows: Vec<PotRecordRow> = match stmt.all().await.and_then(|r| r.results::<PotRowD1>()) {
        Ok(rows) => rows.into_iter().map(PotRowD1::into_row).collect(),
        Err(e) => {
            console_warn!("[utxo-status] pot_records batch query failed: {e}");
            return json_error("database query failed", 503);
        },
    };

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
pub async fn beef(_req: Request, ctx: RouteContext<()>) -> Result<Response> {
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
        },
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
    for (table, sql) in [
        ("pot_beefs", "SELECT hex(beef) AS beef FROM pot_beefs WHERE txid = ?"),
        ("transactions", "SELECT hex(beef) AS beef FROM transactions WHERE txid = ?"),
    ] {
        let stmt = db.prepare(sql).bind(&[JsValue::from_str(&key)])?;
        let row: Option<BeefRow> = match stmt.first(None).await {
            Ok(row) => row,
            Err(e) => {
                console_warn!("[beef] {table} query failed: {e}");
                faulted = true;
                continue;
            },
        };
        if let Some(bytes) = row.and_then(|r| r.beef).and_then(|h| decode_beef_hex(&h)) {
            return json_response(beef_body(&txid, &bytes), 200);
        }
    }

    if faulted {
        return json_error("database query failed", 503);
    }
    json_error(&format!("BEEF not found for txid: {txid}"), 404)
}

/// `GET /tip` — present chain height via the `CHAINTRACKS` service binding
/// (`GET /getPresentHeight`, the same route the overlay's chain tracker
/// calls). A binding fault is 503, an upstream fault 502.
pub async fn tip(_req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let svc = match ctx.env.service("CHAINTRACKS") {
        Ok(svc) => svc,
        Err(e) => {
            console_warn!("[tip] CHAINTRACKS binding unavailable: {e}");
            return json_error("chaintracks binding unavailable", 503);
        },
    };
    let mut init = RequestInit::new();
    init.with_method(Method::Get);
    let headers = Headers::new();
    let _ = headers.set("Accept", "application/json");
    init.with_headers(headers);

    let mut resp = match svc.fetch(CHAINTRACKS_TIP_URL, Some(init)).await {
        Ok(resp) => resp,
        Err(e) => {
            console_warn!("[tip] chaintracks fetch failed: {e}");
            return json_error("chaintracks fetch failed", 502);
        },
    };
    if !(200..300).contains(&resp.status_code()) {
        console_warn!("[tip] chaintracks returned HTTP {}", resp.status_code());
        return json_error("chaintracks returned an error", 502);
    }
    let frame: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(e) => {
            console_warn!("[tip] chaintracks response not JSON: {e}");
            return json_error("chaintracks returned malformed JSON", 502);
        },
    };
    match parse_present_height(&frame) {
        Some(height) => json_response(tip_body(height), 200),
        None => {
            console_warn!("[tip] chaintracks frame not a success/height: {frame}");
            json_error("chaintracks returned an unexpected frame", 502)
        },
    }
}

/// `GET /health` — liveness only (no DB touch).
pub fn health(_req: Request, _ctx: RouteContext<()>) -> Result<Response> {
    json_response(health_body(), 200)
}

/// Catch-all: JSON 404 for any unknown route/method.
pub fn not_found(req: Request, _ctx: RouteContext<()>) -> Result<Response> {
    json_error(&format!("no such route: {}", req.path()), 404)
}
