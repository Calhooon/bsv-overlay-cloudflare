//! low-app-layer — LOW's browser-facing READ surface for chain facts.
//!
//! The LOW game client (a browser app, `bsv-low/app`) must not read
//! WhatsOnChain: a third-party indexer in the money path is both a privacy
//! leak and an availability dependency the game can't control. Instead the
//! client reads THIS worker, which serves from the low-overlay's D1 database
//! — the same `low-overlay-db` the low-overlay worker (tm_pot / ls_pot,
//! `wrangler.low.toml`) writes — with Cloudflare edge caching in front
//! (the zanaadu "app-layer" pattern, mirrored from `~/bsv/zanaadu/app-layer`).
//!
//! Why the overlay D1 is the right source:
//! - `pot_records` is the DURABLE pot-spend landing-proof index: one row per
//!   admitted pot outpoint, updated (never deleted) when the spend is seen.
//!   It survives engine output eviction, so a spent pot remains queryable as
//!   the permanent landing proof a client checks before crediting a payout.
//! - `transactions` holds the engine's admitted BEEF bytes, which the client
//!   needs for SPV-verifiable ancestry without touching an external indexer.
//! - The chain tip comes from the same-account `rust-chaintracks` worker via
//!   a service binding (a plain workers.dev fetch to a same-account worker
//!   loops back to the caller, so the binding is required).
//!
//! STRICTLY READ-ONLY: this worker NEVER writes to the DB. It runs no
//! migrations (the overlay owns the schema), takes no queue, and holds no
//! secrets — every route is a public GET over public chain facts, answered
//! with wildcard CORS so the browser can call it cross-origin.
//!
//! Routes:
//! - `GET /utxo-status?outpoints=<txid>.<vout>,…` — spent-status of up to 64
//!   pot outpoints from `pot_records` in ONE batched D1 query (fail-safe: an
//!   unknown outpoint is reported `known:false`, never asserted unspent).
//! - `GET /pots-view?outpoints=…` — the batched DERIVED view (GH
//!   bsv-low#163): the `/utxo-status` facts PLUS each recorded spender's raw
//!   tx (extracted from its stored BEEF; client hash-verifies) PLUS the
//!   chain tip, in one request / one D1 join — the query that replaces the
//!   client's per-spender `/beef` fan-out.
//! - `GET /recovery-view?identity=<66-hex>` — the seed-only BY-IDENTITY
//!   recovery view (bsv-low#189): every pot the identity is a party to
//!   (`potparty_records`, bsv-low#188) JOINed to its on-chain spend status
//!   (`pot_records`) + spender bytes (`pot_beefs`) + the chain tip, in one
//!   request / one D1 query. A missing/invalid identity is an empty result,
//!   never an error.
//! - `GET /leaderboard?limit=200` — the server-side leaderboard join + rank
//!   (bsv-low #38): the recent `result_markers_v2` markers JOINed to their
//!   `pot_records` anchor (CHUNKED, the same table `/utxo-status` reads) +
//!   `proof_markers` pointers, aggregated + ranked with the client's exact
//!   `aggregateBoard` / `lowestHands` rules — the ONE call that replaces the
//!   client's ~110-round-trip N+1. The server counts on (both sigs +
//!   anchored) and returns the sigs so the client re-verifies (see
//!   `logic`'s trust note); a D1 fault is a 5xx, never a fabricated board.
//! - `GET /beef/:txid` — the admitted BEEF bytes from `transactions`.
//! - `GET /tip` — present chain height via the CHAINTRACKS service binding.
//! - `GET /health` — liveness.
//!
//! NO CACHING (owner call, 2026-07-14): the Cache API misbehaves on
//! workers.dev (intermittent CF 1042s observed on the first deploy), and the
//! scaling mechanism here is the QUERY COLLAPSE — one request → one D1 query
//! for a whole batch of outpoints — not a cache. Every response is
//! `no-store`.

pub mod compaction;
pub mod cors;
pub mod logic;
mod routes;

use worker::{event, Context, Env, Request, Response, Result, Router};

/// Worker entry — HTTP request dispatch.
///
/// OPTIONS preflight is answered before routing (it carries no body and must
/// succeed for the browser to send the real GET). Every other response —
/// success, 4xx, or 5xx — gets wildcard CORS stamped on the way out, so a
/// cross-origin browser always sees the real status instead of an opaque
/// network error.
#[event(fetch)]
pub async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    // Readable wasm panics in `wrangler tail` (set_once → cheap on re-entry).
    console_error_panic_hook::set_once();

    if cors::is_preflight(&req) {
        return cors::preflight();
    }

    let mut resp = router().run(req, env).await?;
    cors::add_cors_headers(&mut resp);
    Ok(resp)
}

/// The route table. All GET, all JSON; unknown paths get a JSON 404 via the
/// `or_else_any_method` catch-alls (worker-rs' default no-match 404 is plain
/// text, so both `/` and the wildcard are registered explicitly).
fn router() -> Router<'static, ()> {
    Router::new()
        .get_async("/utxo-status", routes::utxo_status)
        .get_async("/pots-view", routes::pots_view)
        .get_async("/recovery-view", routes::recovery_view)
        .get_async("/leaderboard", routes::leaderboard)
        .get_async("/beef/:txid", routes::beef)
        .get_async("/tip", routes::tip)
        .get("/health", routes::health)
        .or_else_any_method("/", routes::not_found)
        .or_else_any_method("/*catchall", routes::not_found)
}
