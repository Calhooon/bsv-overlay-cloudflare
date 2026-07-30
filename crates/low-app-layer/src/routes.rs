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
        let (row, proof_verified): (Option<BeefRow>, bool) = match stmt.first::<BeefTrustRow>(None).await {
            Ok(row) => {
                let verified = row
                    .as_ref()
                    .and_then(|r| r.proof_verified)
                    .unwrap_or(0.0)
                    != 0.0;
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
                    },
                }
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
    let (verdicts, params_by_pot) = classify_spent_pots(&db, &statuses).await;

    // 5) #230 seat attribution: the classified pots' verified potparty-v2
    // seat-binding markers, joined to each pot's committed lock keys.
    // BEST-EFFORT: any fault yields an empty map (counting falls back to
    // the claim rules) — never a 5xx, never a guessed attribution.
    let attributions = seat_attributions(&db, &params_by_pot).await;

    let lb = crate::logic::aggregate_leaderboard_attributed(
        &markers,
        &statuses,
        &proof_map,
        limit,
        &verdicts,
        &attributions,
    );
    let computed_at = (worker::Date::now().as_millis() / 1000) as i64;
    json_response(leaderboard_body(&lb, computed_at, markers.len()), 200)
}

/// Hard bound on pots classified per `/leaderboard` request via the LEGACY
/// BLOB fallback (each such pot costs two BLOB reads + two BEEF parses).
/// #284: this cap now bounds ONLY the fallback partition — a pot whose
/// decoded columns answer (verdict + params as pure column reads) costs no
/// BLOB and is NOT capped. Once the backfill completes, the fallback
/// partition — and with it this cap's effect — dies entirely.
const LEADERBOARD_CLASSIFY_CAP: usize = 64;

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
/// 2. **Legacy fallback (capped at [`LEADERBOARD_CLASSIFY_CAP`]):** the
///    pre-#284 path — stored `pot_beefs` bytes, hash-verified, classified
///    per request. Covers un-backfilled rows and stale verdicts; dies
///    entirely once the backfill completes.
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
) {
    let mut verdicts = std::collections::HashMap::new();
    let mut params_by_pot = std::collections::HashMap::new();

    // ALL spent pots with a recorded spender, deduped, newest first (the
    // fallback cap is applied AFTER the column partition below).
    let mut all_pairs: Vec<(String, String)> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for s in statuses {
        if s.spent == Some(true) {
            if let Some(spender) = &s.spending_txid {
                let pot = s.txid.to_ascii_lowercase();
                if seen.insert(pot.clone()) {
                    all_pairs.push((pot, spender.to_ascii_lowercase()));
                }
            }
        }
    }
    if all_pairs.is_empty() {
        return (verdicts, params_by_pot);
    }

    // ── Tier 1: the decoded-column partition (no BLOB fetch, NO CAP) ──────
    // BEST-EFFORT: any fault leaves rows unresolved here and the fallback
    // (still capped) picks them up — never a 5xx, never a guessed verdict.
    let mut column_resolved: std::collections::HashSet<String> = std::collections::HashSet::new();
    for chunk in all_pairs.chunks(crate::logic::D1_CHUNK_OUTPOINTS) {
        let sql = crate::results::decoded_pots_sql(chunk.len());
        let mut binds: Vec<JsValue> = Vec::with_capacity(chunk.len() * 2);
        for (pot, _) in chunk {
            binds.push(JsValue::from_str(pot));
            binds.push(JsValue::from_f64(f64::from(crate::logic::LEADERBOARD_POT_VOUT)));
        }
        let stmt = match db.prepare(sql).bind(&binds) {
            Ok(s) => s,
            Err(e) => {
                console_warn!("[leaderboard] decoded-pots bind failed (column tier skipped): {e}");
                break;
            }
        };
        match stmt.all().await.and_then(|r| r.results::<DecodedPotRowD1>()) {
            Ok(rows) => {
                let by_txid: std::collections::HashMap<String, DecodedPotRowD1> = rows
                    .into_iter()
                    .map(|r| (r.txid.to_ascii_lowercase(), r))
                    .collect();
                for (pot, spender) in chunk {
                    let Some(row) = by_txid.get(pot) else { continue };
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
                        row.verdict.as_deref().and_then(crate::results::PotVerdict::from_wire),
                        row.covenant_params(),
                    ) else {
                        continue;
                    };
                    verdicts.insert(pot.clone(), v);
                    params_by_pot.insert(pot.clone(), params);
                    column_resolved.insert(pot.clone());
                }
            }
            Err(e) => {
                console_warn!("[leaderboard] decoded-pots query failed (column tier partial): {e}");
            }
        }
    }

    // ── Tier 2: the legacy BLOB fallback, capped ──────────────────────────
    let pairs: Vec<(String, String)> = all_pairs
        .into_iter()
        .filter(|(pot, _)| !column_resolved.contains(pot))
        .take(LEADERBOARD_CLASSIFY_CAP)
        .collect();
    if pairs.is_empty() {
        return (verdicts, params_by_pot);
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
                return (verdicts, params_by_pot);
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
            // #230: keep the classified pot's COMMITTED lock params (from
            // the hash-verified funding bytes) for the seat attribution.
            if let Some(params) = crate::results::parse_raw_tx_verified(&fraw, pot)
                .and_then(|f| {
                    f.outputs
                        .get(crate::logic::LEADERBOARD_POT_VOUT as usize)
                        .map(|(_, lock)| lock.clone())
                })
                .and_then(|lock| crate::results::extract_covenant_params(&lock))
            {
                params_by_pot.insert(pot.clone(), params);
            }
        }
    }
    (verdicts, params_by_pot)
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
        // COMMITTED settle keys and windowed PER KEY SLOT — order is not
        // load-bearing, because the #252 backfill publishes honest markers
        // long after a pot's txid became public (see `seat_markers_sql`).
        let sql = crate::results::seat_markers_sql(chunk.len());
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
    for (pot, params) in params_by_pot {
        let Some(markers) = markers_by_pot.get(pot) else {
            continue;
        };
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
    out
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

    // #281 F1 — the SEAT PROOF comes from its OWN query, bound to each pot's
    // COMMITTED settle keys (read from the hash-verified funding lock), NOT
    // from the `results_sql` page. That page is now one row per pot, and no
    // ordering rule over it is safe: #252's backfill publishes honest v2
    // markers long after a pot txid became public, so a forged row can always
    // be OLDER. Binding the keys removes ordering from the argument entirely
    // — a forged key cannot enter the result set. BEST-EFFORT: a fault here
    // only leaves seat attribution absent (claims still decide), never a 5xx.
    let seat_markers = results_seat_markers(&db, &rows).await;

    let entries = crate::results::assemble_results(&identity_lc, rows, &claims, &seat_markers);
    json_response(crate::results::results_body(&identity_lc, &entries), 200)
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
    rows: &[crate::results::ResultsRow],
) -> std::collections::HashMap<(String, u32), Vec<crate::results::SeatMarkerRow>> {
    let mut out: std::collections::HashMap<(String, u32), Vec<crate::results::SeatMarkerRow>> =
        std::collections::HashMap::new();
    let params_by_pot = crate::results::covenant_params_by_pot(rows);
    if params_by_pot.is_empty() {
        return out;
    }
    // Chunking + bind construction live in `results::seat_marker_chunks` so
    // they are testable without a Worker (the re-gate's finding #3: this whole
    // delivery path could be deleted with no test failing).
    for chunk in crate::results::seat_marker_chunks(&params_by_pot) {
        let sql = crate::results::seat_markers_sql(chunk.len());
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
    ctx: &RouteContext<()>,
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
            },
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
) -> (crate::txany::TxObservation, crate::txany::AbsenceCorroboration) {
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
                    None => match provider_get(&format!("{BITAILS_BASE}/download/tx/{txid_lc}")).await {
                        Some((200, body)) if !body.is_empty() => Some(body),
                        _ => None,
                    },
                };
                let raw_hex = raw.and_then(|r| crate::txany::verify_raw_bytes(&r, txid_lc));
                TxObservation::Present { confirmed, raw_hex }
            },
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
                if let Some((200, body)) =
                    provider_get(&format!("{BITAILS_BASE}/download/tx/{}", crate::txany::KNOWN_MINED_TXID)).await
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
pub async fn tx_any(_req: Request, ctx: RouteContext<()>) -> Result<Response> {
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
                        let st =
                            spent_any_resolve(&src_txid.to_ascii_lowercase(), input.source_output_index)
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
                map.insert(key.clone(), (now + crate::txany::TX_ANY_CACHE_TTL_MS, answer.clone()));
            });
            answer
        },
    };

    json_response(crate::txany::tx_any_body(&key, &answer), 200)
}

/// `GET /health` — liveness only (no DB touch).
pub fn health(_req: Request, _ctx: RouteContext<()>) -> Result<Response> {
    json_response(health_body(), 200)
}

/// Catch-all: JSON 404 for any unknown route/method.
pub fn not_found(req: Request, _ctx: RouteContext<()>) -> Result<Response> {
    json_error(&format!("no such route: {}", req.path()), 404)
}

#[cfg(test)]
mod tests {
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
        assert_eq!(verified, Some(0), "structural has_proof must NOT read as trust");
        let (_, verified) = read(TRANSACTIONS_TRUST_SQL, "tx1");
        assert_eq!(verified, Some(0));

        // The verifying writers latch — the same reads now answer trusted.
        conn.execute("UPDATE pot_beefs SET proof_verified = 1 WHERE txid = 'pot1'", [])
            .unwrap();
        conn.execute("UPDATE transactions SET has_proof = 1 WHERE txid = 'tx1'", [])
            .unwrap();
        assert_eq!(read(POT_BEEFS_TRUST_SQL, "pot1").1, Some(1));
        assert_eq!(read(TRANSACTIONS_TRUST_SQL, "tx1").1, Some(1));
    }
}
