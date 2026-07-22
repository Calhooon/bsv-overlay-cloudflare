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

use crate::logic::{
    aggregate_leaderboard_with_verdicts, assemble_pots_view, assemble_recovery_view,
    assemble_statuses, batch_where_sql, beef_body, chunk_outpoints, clamp_leaderboard_limit,
    decode_beef_hex, health_body, leaderboard_body, leaderboard_pot_outpoints, parse_outpoints,
    parse_present_height, pots_view_body, pots_view_join_sql, recovery_view_body,
    recovery_view_sql, tip_body, utxo_status_body, valid_identity, valid_txid, Outpoint,
    PotRecordRow, PotsViewRow, RecoveryRow, ResultMarkerRow,
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
    /// `serde(default)` (0.0) tolerates a read that races the overlay's
    /// additive `spentConfirmed` migration.
    #[serde(rename = "spentConfirmed", default)]
    spent_confirmed: f64,
}

impl PotRowD1 {
    fn into_row(self) -> PotRecordRow {
        PotRecordRow {
            txid: self.txid,
            vout: self.output_index as u32,
            spent: self.spent != 0.0,
            spending_txid: self.spending_txid,
            spent_confirmed: self.spent_confirmed != 0.0,
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
            },
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
            // Serve-time compaction (#192/#193, P4): once the overlay's
            // completion pass / Arcade MINED callback has stitched a
            // chaintracks-verified BUMP into this BEEF, its now-proven
            // ancestry is dead weight the frontend `createAction` chokes on.
            // `compact_beef` trims it — STRICTLY passthrough-on-failure, so a
            // proofless (or already-minimal) BEEF is returned byte-for-byte
            // unchanged. The subject is the lowercase DB key (BEEF txids are
            // lowercase hex).
            let compacted = crate::compaction::compact_beef(&key, &bytes);
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
    ctx: &RouteContext<()>,
    tag: &str,
) -> std::result::Result<u64, (&'static str, u16)> {
    let svc = match ctx.env.service("CHAINTRACKS") {
        Ok(svc) => svc,
        Err(e) => {
            console_warn!("[{tag}] CHAINTRACKS binding unavailable: {e}");
            return Err(("chaintracks binding unavailable", 503));
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
            console_warn!("[{tag}] chaintracks fetch failed: {e}");
            return Err(("chaintracks fetch failed", 502));
        },
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
        },
    };
    match parse_present_height(&frame) {
        Some(height) => Ok(height),
        None => {
            console_warn!("[{tag}] chaintracks frame not a success/height: {frame}");
            Err(("chaintracks returned an unexpected frame", 502))
        },
    }
}

/// `GET /tip` — present chain height via the `CHAINTRACKS` service binding
/// (`GET /getPresentHeight`, the same route the overlay's chain tracker
/// calls). A binding fault is 503, an upstream fault 502.
pub async fn tip(_req: Request, ctx: RouteContext<()>) -> Result<Response> {
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
pub async fn pots_view(req: Request, ctx: RouteContext<()>) -> Result<Response> {
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
        },
    };

    // One joined query PER CHUNK (records + spender BEEFs), merged into one
    // response — same D1 100-bound-param discipline as /utxo-status (the join
    // still binds 2 params per outpoint, so a >50-outpoint single query 503s).
    // FAIL-SAFE: any chunk's D1 error returns the SAME 503 and no body — a
    // failed chunk is unknown-for-those-rows, never a fabricated partial view.
    let mut rows: Vec<PotsViewRow> = Vec::with_capacity(outpoints.len());
    for chunk in chunk_outpoints(&outpoints) {
        let mut binds: Vec<JsValue> = Vec::with_capacity(chunk.len() * 2);
        for op in chunk {
            binds.push(JsValue::from_str(&op.db_txid()));
            binds.push(JsValue::from_f64(f64::from(op.vout)));
        }
        let stmt = db.prepare(pots_view_join_sql(chunk.len())).bind(&binds)?;
        match stmt.all().await.and_then(|r| r.results::<PotsViewRowD1>()) {
            Ok(chunk_rows) => rows.extend(chunk_rows.into_iter().map(PotsViewRowD1::into_row)),
            Err(e) => {
                console_warn!("[pots-view] pot_records join query failed: {e}");
                return json_error("database query failed", 503);
            },
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
}

impl RecoveryRowD1 {
    fn into_row(self) -> RecoveryRow {
        RecoveryRow {
            game_id: self.game_id,
            pot_txid: self.pot_txid,
            pot_vout: self.pot_vout as u32,
            recovery_height: self.recovery_height as u32,
            opponent_identity: self.opponent_identity,
            spent: self.spent.map(|v| v != 0.0),
            spending_txid: self.spending_txid,
            spent_confirmed: self.spent_confirmed.map(|v| v != 0.0),
            spender_beef_hex: self.spender_beef,
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
pub async fn recovery_view(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let url = req.url()?;
    let identity = url
        .query_pairs()
        .find(|(k, _)| k == "identity")
        .map(|(_, v)| v.into_owned())
        .unwrap_or_default();

    // Missing / empty / malformed identity → empty result, not an error.
    if !valid_identity(&identity) {
        return json_response(recovery_view_body(&[], None), 200);
    }

    let db = match ctx.env.d1("OVERLAY_DB") {
        Ok(db) => db,
        Err(e) => {
            console_warn!("[recovery-view] OVERLAY_DB binding unavailable: {e}");
            return json_error("database unavailable", 503);
        },
    };

    // ONE query: the caller's potparty rows JOINed to pot spend-status +
    // spender BEEFs. `potparty_records.identity` is lowercase hex.
    let stmt = db
        .prepare(recovery_view_sql())
        .bind(&[JsValue::from_str(&identity.to_ascii_lowercase())])?;
    let rows: Vec<RecoveryRow> = match stmt.all().await.and_then(|r| r.results::<RecoveryRowD1>()) {
        Ok(rows) => rows.into_iter().map(RecoveryRowD1::into_row).collect(),
        Err(e) => {
            console_warn!("[recovery-view] potparty join query failed: {e}");
            return json_error("database query failed", 503);
        },
    };

    let entries = assemble_recovery_view(rows);
    let tip = chaintracks_present_height(&ctx, "recovery-view").await.ok();
    json_response(recovery_view_body(&entries, tip), 200)
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
/// Reads the recent `result_markers_v2` markers, JOINs each against the
/// `pot_records` spend-status (the SAME table `/utxo-status` reads — CHUNKED at
/// [`crate::logic::D1_CHUNK_OUTPOINTS`] so a large result set never trips D1's
/// 100-bound-param cap), joins `proof_markers` for the `proofTxid` pointer, and
/// aggregates + ranks with the client's exact `aggregateBoard` / `lowestHands`
/// rules. See the `logic` module note for the trust decision: the server
/// COUNTS on (both sigs present + anchored) and RETURNS the sigs + anchor so
/// the client re-verifies and can falsify — it never asserts an ECDSA verify it
/// did not perform.
///
/// FAIL-SAFE: a `pot_records` (or marker) D1 fault is the SAME 5xx the client
/// already handles — NEVER a fabricated empty/all-zero board. The `proof_markers`
/// join is best-effort: a fault there only drops the `proofTxid` hint (null),
/// never a count and never a 5xx.
pub async fn leaderboard(req: Request, ctx: RouteContext<()>) -> Result<Response> {
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
        },
    };

    // 1) Recent result markers, newest first (mirrors ls_result recentResults).
    let markers_sql = "SELECT gameId, winner, loser, potTxid, settleTxid, winnerSigHex, \
         loserSigHex, cardsHex, txid, createdAt FROM result_markers_v2 \
         ORDER BY createdAt DESC, rowid DESC LIMIT ?";
    let stmt = db
        .prepare(markers_sql)
        .bind(&[JsValue::from_f64(limit as f64)])?;
    let markers: Vec<ResultMarkerRow> = match stmt.all().await.and_then(|r| r.results::<ResultRowD1>()) {
        Ok(rows) => rows.into_iter().filter_map(ResultRowD1::into_marker).collect(),
        Err(e) => {
            console_warn!("[leaderboard] result_markers_v2 query failed: {e}");
            return json_error("database query failed", 503);
        },
    };

    // 2) Pot spend-status join (potTxid:0), CHUNKED at D1_CHUNK_OUTPOINTS —
    // same discipline as /utxo-status. FAIL-SAFE: a chunk's D1 error is the
    // SAME 503 the client handles and serves no body (never a fabricated
    // all-unknown board that would silently zero every win).
    let outpoints = leaderboard_pot_outpoints(&markers);
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
            },
        }
    }
    let statuses = assemble_statuses(&outpoints, &pot_rows);

    // 3) proof_markers pointers (gameId, winner) → newest marker txid.
    // BEST-EFFORT: a fault here only omits the proofTxid hint, never a 5xx.
    // A generous LIMIT bounds the scan; ORDER BY createdAt DESC + or_insert
    // keeps the newest pointer per (gameId, winner).
    let mut proof_map: std::collections::HashMap<(String, String), String> =
        std::collections::HashMap::new();
    let proof_sql = "SELECT gameId, winner, txid FROM proof_markers \
         ORDER BY createdAt DESC, rowid DESC LIMIT 2000";
    match db.prepare(proof_sql).all().await.and_then(|r| r.results::<ProofPointerRowD1>()) {
        Ok(rows) => {
            for pr in rows {
                proof_map
                    .entry((pr.game_id.to_ascii_lowercase(), pr.winner.to_ascii_lowercase()))
                    .or_insert(pr.txid);
            }
        },
        Err(e) => console_warn!("[leaderboard] proof_markers query failed (proofTxid omitted): {e}"),
    }

    // 4) Server-derived CHAIN classification of the spent pots (bsv-low #227)
    // — an ADDITIVE truth source folded in alongside the client claims.
    // BEST-EFFORT + BOUNDED: at most LEADERBOARD_CLASSIFY_CAP pots (newest
    // marker order), pot_beefs fetched in ≤45-bind chunks (the D1 param-cap
    // discipline); any fault only omits classifications (counting falls back
    // to the pre-#227 claim rules) — never a 5xx, never a fabricated verdict.
    let verdicts = classify_spent_pots(&db, &statuses).await;

    let lb = aggregate_leaderboard_with_verdicts(&markers, &statuses, &proof_map, limit, &verdicts);
    let computed_at = (worker::Date::now().as_millis() / 1000) as i64;
    json_response(leaderboard_body(&lb, computed_at, markers.len()), 200)
}

/// Hard bound on pots classified per `/leaderboard` request (each pot costs
/// two BLOB reads + two BEEF parses; the default 200-marker board is well
/// under it in distinct pots).
const LEADERBOARD_CLASSIFY_CAP: usize = 64;

/// `pot_beefs` row for the classification fold: txid + `hex(beef)`.
#[derive(Deserialize)]
struct PotBeefRowD1 {
    txid: String,
    beef: Option<String>,
}

/// Classify the recorded spends of the SPENT pots in `statuses` (vout 0 —
/// the leaderboard anchor) from their stored `pot_beefs` bytes. Returns a
/// lowercase-pot-txid → verdict map; every fault or ambiguity simply omits
/// that pot (see `results.rs` for the conservatism contract).
async fn classify_spent_pots(
    db: &worker::D1Database,
    statuses: &[crate::logic::OutpointStatus],
) -> std::collections::HashMap<String, crate::results::PotVerdict> {
    let mut verdicts = std::collections::HashMap::new();

    // The spent pots with a recorded spender, capped, deduped, newest first.
    let mut pairs: Vec<(String, String)> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for s in statuses {
        if s.spent == Some(true) {
            if let Some(spender) = &s.spending_txid {
                let pot = s.txid.to_ascii_lowercase();
                if seen.insert(pot.clone()) {
                    pairs.push((pot, spender.to_ascii_lowercase()));
                    if pairs.len() >= LEADERBOARD_CLASSIFY_CAP {
                        break;
                    }
                }
            }
        }
    }
    if pairs.is_empty() {
        return verdicts;
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
        let sql = format!("SELECT txid, hex(beef) AS beef FROM pot_beefs WHERE txid IN ({placeholders})");
        let binds: Vec<JsValue> = chunk.iter().map(|k| JsValue::from_str(k)).collect();
        let stmt = match db.prepare(sql).bind(&binds) {
            Ok(s) => s,
            Err(e) => {
                console_warn!("[leaderboard] pot_beefs bind failed (classification omitted): {e}");
                return verdicts;
            },
        };
        match stmt.all().await.and_then(|r| r.results::<PotBeefRowD1>()) {
            Ok(rows) => {
                for r in rows {
                    if let Some(bytes) = r.beef.and_then(|h| decode_beef_hex(&h)) {
                        beefs.insert(r.txid.to_ascii_lowercase(), bytes);
                    }
                }
            },
            Err(e) => {
                console_warn!("[leaderboard] pot_beefs query failed (classification partial): {e}");
                // Keep whatever chunks already loaded — a missing BEEF only
                // leaves its pot unclassified.
            },
        }
    }

    for (pot, spender) in &pairs {
        let (Some(fb), Some(sb)) = (beefs.get(pot), beefs.get(spender)) else {
            continue;
        };
        let funding_raw = crate::logic::extract_raw_tx_hex(fb, pot).and_then(|h| hex::decode(h).ok());
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
        }
    }
    verdicts
}

// ── /results — server-derived settle results (bsv-low #227) ─────────────────

/// `/results` joined row as D1 returns it (the `results_sql` shape): the
/// caller's potparty facts + spend pointer + BOTH stored BEEFs as hex.
#[derive(Deserialize)]
struct ResultsRowD1 {
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
}

impl ResultsRowD1 {
    fn into_row(self) -> crate::results::ResultsRow {
        crate::results::ResultsRow {
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
pub async fn results(req: Request, ctx: RouteContext<()>) -> Result<Response> {
    let url = req.url()?;
    let identity = url
        .query_pairs()
        .find(|(k, _)| k == "identity")
        .map(|(_, v)| v.into_owned())
        .unwrap_or_default();
    let identity_lc = identity.to_ascii_lowercase();

    if !crate::logic::valid_identity(&identity_lc) {
        return json_response(crate::results::results_body(&identity_lc, &[]), 200);
    }

    let db = match ctx.env.d1("OVERLAY_DB") {
        Ok(db) => db,
        Err(e) => {
            console_warn!("[results] OVERLAY_DB binding unavailable: {e}");
            return json_error("database unavailable", 503);
        },
    };

    let stmt = db
        .prepare(crate::results::results_sql())
        .bind(&[JsValue::from_str(&identity_lc)])?;
    let rows: Vec<crate::results::ResultsRow> =
        match stmt.all().await.and_then(|r| r.results::<ResultsRowD1>()) {
            Ok(rows) => rows.into_iter().map(ResultsRowD1::into_row).collect(),
            Err(e) => {
                console_warn!("[results] potparty join query failed: {e}");
                return json_error("database query failed", 503);
            },
        };

    // Claims (won/lost attribution) — BEST-EFFORT: a fault here only leaves
    // winner-verdict games `unresolved`, never a 5xx (the chain-truth
    // tie/refund outcomes and the verdict field still serve).
    let mut game_ids: Vec<String> = rows.iter().map(|r| r.game_id.to_ascii_lowercase()).collect();
    game_ids.sort_unstable();
    game_ids.dedup();
    let mut claim_markers: Vec<ResultMarkerRow> = Vec::new();
    for chunk in game_ids.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
        let binds: Vec<JsValue> = chunk.iter().map(|g| JsValue::from_str(g)).collect();
        let stmt = db
            .prepare(crate::results::claims_sql(chunk.len()))
            .bind(&binds)?;
        match stmt.all().await.and_then(|r| r.results::<ResultRowD1>()) {
            Ok(rows) => claim_markers.extend(rows.into_iter().filter_map(ResultRowD1::into_marker)),
            Err(e) => {
                console_warn!("[results] result_markers_v2 query failed (claims omitted): {e}");
            },
        }
    }
    let claims = crate::results::claims_by_game(&claim_markers);

    let entries = crate::results::assemble_results(&identity_lc, rows, &claims);
    json_response(crate::results::results_body(&identity_lc, &entries), 200)
}

// ── /spent-any — server-side legacy outpoint reads (bsv-low #227 addendum) ──

/// One cached `/spent-any` row: the decision fields, without the echo key.
#[derive(Clone)]
struct SpentAnyCached {
    known: bool,
    spent: Option<bool>,
    spending_txid: Option<String>,
    spent_confirmed: Option<bool>,
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
        Some((s, _)) if (400..500).contains(&s) => SpentObservation::NotSpent,
        _ => SpentObservation::Fault,
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
                None => match provider_get(&format!("{BITAILS_BASE}/download/tx/{spender}")).await {
                    Some((200, body)) if !body.is_empty() => Some(body),
                    _ => None,
                },
            };
            if let Some(raw) = raw {
                spender_raw_ok = spender_raw_verifies(&raw, spender, txid_lc, vout);
            }
        }
        SpentObservation::NotSpent => {
            // Negative corroboration (never WoC-only). Bitails' outpoint
            // endpoint 500s at the time of writing — parse_bitails_unspent is
            // strict, so that fault surfaces as known:false (fail-safe).
            bitails = match provider_get(&format!("{BITAILS_BASE}/tx/{txid_lc}/output/{vout}/spent"))
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
pub async fn spent_any(req: Request, _ctx: RouteContext<()>) -> Result<Response> {
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
        });
    }

    json_response(utxo_status_body(&entries), 200)
}

/// `GET /health` — liveness only (no DB touch).
pub fn health(_req: Request, _ctx: RouteContext<()>) -> Result<Response> {
    json_response(health_body(), 200)
}

/// Catch-all: JSON 404 for any unknown route/method.
pub fn not_found(req: Request, _ctx: RouteContext<()>) -> Result<Response> {
    json_error(&format!("no such route: {}", req.path()), 404)
}
