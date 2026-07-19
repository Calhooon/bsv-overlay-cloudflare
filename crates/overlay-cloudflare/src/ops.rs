//! Observability for the BEEF proof-completion pipeline (#192/#193, P4).
//!
//! zanaadu's single most expensive omission was an unobserved completion pass:
//! a dead pass hid for WEEKS. These primitives make a dead pass surface in a
//! DAY, not weeks:
//!
//! - `ops_heartbeat` — a singleton (`id = 0`) upserted at the end of every cron
//!   completion pass. `last_tick_ms` is the wall-clock of the last live pass;
//!   `tick_count` a monotonic pass count.
//! - `ops_counters` — persistent monotonic counters (`proofs_completed_total`,
//!   `fetch_failed_total`, `pot_beefs_compacted_total`), bumped every tick.
//! - `proofless_watch` — a first-seen ledger for proofless txids; a tx still
//!   proofless after 24h is flagged (the signal that a proof genuinely is not
//!   landing, vs. merely not-yet-mined).
//! - `GET /health/invariants?strict=1` — 503 when the completion pass has been
//!   dead longer than the staleness budget (the alarm surface).
//!
//! Schema lives in `d1::OVERLAY_MIGRATIONS`. Every write here is BEST-EFFORT:
//! an observability fault logs and is swallowed — it must never break a
//! completion pass or a request.

use serde::Deserialize;
use serde_json::json;
use worker::{D1Database, Env, Response};

use crate::d1::Query;

/// Counter names (persisted in `ops_counters`).
pub const COUNTER_PROOFS_COMPLETED: &str = "proofs_completed_total";
pub const COUNTER_FETCH_FAILED: &str = "fetch_failed_total";
pub const COUNTER_POT_BEEFS_COMPACTED: &str = "pot_beefs_compacted_total";

/// Default staleness budget for `/health/invariants?strict=1`: 6 hours. The
/// completion cron runs every 15 min (`wrangler.toml crons`), so 6h ≈ 24 dead
/// ticks — well inside the "surface in a day" bar, while tolerant of a couple
/// of skipped/slow ticks. Override with `OPS_INVARIANTS_MAX_STALE_MS`.
pub const DEFAULT_MAX_STALE_MS: i64 = 6 * 60 * 60 * 1000;

/// Proofless-ledger flag threshold: a tx proofless longer than this is flagged.
pub const PROOFLESS_FLAG_MS: i64 = 24 * 60 * 60 * 1000;

/// Per-tick cap on how many proofless txids we newly enrol into the watch
/// ledger from each store (keeps the write bounded; the ledger self-heals over
/// ticks and is GC'd as txs prove).
const WATCH_ENROLL_LIMIT: u32 = 500;

fn now_ms() -> i64 {
    js_sys::Date::now() as i64
}

// ── row shapes (D1 returns numeric columns as f64 — codebase convention) ────

#[derive(Deserialize)]
struct HeartbeatRow {
    last_tick_ms: f64,
    tick_count: f64,
}

#[derive(Deserialize)]
struct CounterRow {
    name: String,
    value: f64,
}

#[derive(Deserialize)]
struct CountRow {
    c: f64,
}

/// Record one live completion pass: upsert the heartbeat singleton and bump the
/// persistent counters. Best-effort — logs and swallows any D1 error.
pub async fn record_completion_tick(
    db: &D1Database,
    proofs_completed: u64,
    fetch_failed: u64,
    pot_beefs_compacted: u64,
) {
    let ts = now_ms();

    // Heartbeat singleton (id = 0): stamp the wall-clock, increment the count.
    let hb = Query::new(
        "INSERT INTO ops_heartbeat (id, last_tick_ms, tick_count) VALUES (0, ?, 1) \
         ON CONFLICT(id) DO UPDATE SET \
             last_tick_ms = excluded.last_tick_ms, \
             tick_count = ops_heartbeat.tick_count + 1",
    )
    .bind(ts);
    if let Err(e) = hb.execute(db).await {
        worker::console_log!("[ops] heartbeat upsert failed: {e}");
    }

    // Persistent monotonic counters (additive upsert).
    for (name, delta) in [
        (COUNTER_PROOFS_COMPLETED, proofs_completed),
        (COUNTER_FETCH_FAILED, fetch_failed),
        (COUNTER_POT_BEEFS_COMPACTED, pot_beefs_compacted),
    ] {
        let q = Query::new(
            "INSERT INTO ops_counters (name, value) VALUES (?, ?) \
             ON CONFLICT(name) DO UPDATE SET value = ops_counters.value + excluded.value",
        )
        .bind(name)
        .bind(delta);
        if let Err(e) = q.execute(db).await {
            worker::console_log!("[ops] counter {name} bump failed: {e}");
        }
    }
}

/// Refresh the proofless first-seen ledger and return the count of txids
/// flagged (proofless > 24h). Best-effort throughout.
///
/// Each tick: (1) enrol currently-proofless txids from both stores with a
/// first-seen stamp (`INSERT OR IGNORE`, so an existing first-seen is never
/// overwritten — the age is real), (2) GC txids that have since proven, (3)
/// count those older than the flag threshold.
pub async fn refresh_proofless_watch(db: &D1Database) -> u64 {
    let ts = now_ms();

    // 1. Enrol proofless txids from both stores (bounded). First-seen is only
    //    set on the FIRST sighting (INSERT OR IGNORE keeps the original stamp).
    for table in ["pot_beefs", "transactions"] {
        let sql = format!(
            "INSERT OR IGNORE INTO proofless_watch (txid, first_seen_ms) \
             SELECT txid, ? FROM {table} WHERE has_proof = 0 LIMIT {WATCH_ENROLL_LIMIT}"
        );
        if let Err(e) = Query::new(sql).bind(ts).execute(db).await {
            worker::console_log!("[ops] proofless_watch enrol ({table}) failed: {e}");
        }
    }

    // 2. GC: drop any txid that has since proven in either store.
    let cleanup = "DELETE FROM proofless_watch WHERE \
         txid IN (SELECT txid FROM pot_beefs WHERE has_proof = 1) OR \
         txid IN (SELECT txid FROM transactions WHERE has_proof = 1)";
    if let Err(e) = Query::new(cleanup).execute(db).await {
        worker::console_log!("[ops] proofless_watch GC failed: {e}");
    }

    // 3. Flag: count txids proofless longer than the threshold.
    let cutoff = ts - PROOFLESS_FLAG_MS;
    let flagged = count_flagged(db, cutoff).await;
    if flagged > 0 {
        worker::console_log!(
            "[ops] proofless_watch: {flagged} tx(s) proofless > 24h (proof not landing)"
        );
    }
    flagged
}

/// Count proofless_watch rows first seen before `cutoff_ms`.
async fn count_flagged(db: &D1Database, cutoff_ms: i64) -> u64 {
    let row: Option<CountRow> = Query::new(
        "SELECT COUNT(*) AS c FROM proofless_watch WHERE first_seen_ms < ?",
    )
    .bind(cutoff_ms)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();
    row.map(|r| r.c.max(0.0) as u64).unwrap_or(0)
}

/// Read the three persistent counters into a JSON object (missing ⇒ 0).
async fn read_counters(db: &D1Database) -> serde_json::Value {
    let rows: Vec<CounterRow> = Query::new("SELECT name, value FROM ops_counters")
        .fetch_all(db)
        .await
        .unwrap_or_default();
    let mut obj = json!({
        COUNTER_PROOFS_COMPLETED: 0,
        COUNTER_FETCH_FAILED: 0,
        COUNTER_POT_BEEFS_COMPACTED: 0,
    });
    for r in rows {
        obj[r.name] = json!(r.value.max(0.0) as u64);
    }
    obj
}

/// `GET /health/invariants[?strict=1]` — the proof-completion liveness surface.
///
/// Reports the heartbeat (last tick wall-clock + monotonic count), the
/// persistent counters, the proofless-watch flagged count, and a computed
/// `dead` verdict: the completion pass is DEAD when it has never run, or its
/// last tick is older than the staleness budget (`OPS_INVARIANTS_MAX_STALE_MS`,
/// default 6h).
///
/// - `strict=1` (or `strict=true`) → HTTP **503** when dead (the alarm can page
///   on the status alone); 200 otherwise.
/// - default (non-strict) → always HTTP 200 with the same JSON body (a probe
///   that reports the verdict without flapping the endpoint's own health).
pub async fn health_invariants(db: &D1Database, env: &Env, strict: bool) -> worker::Result<Response> {
    let now = now_ms();

    let hb: Option<HeartbeatRow> =
        Query::new("SELECT last_tick_ms, tick_count FROM ops_heartbeat WHERE id = 0")
            .fetch_optional(db)
            .await
            .ok()
            .flatten();
    let (last_tick_ms, tick_count) = hb
        .map(|h| (h.last_tick_ms.max(0.0) as i64, h.tick_count.max(0.0) as i64))
        .unwrap_or((0, 0));

    let max_stale_ms = env
        .var("OPS_INVARIANTS_MAX_STALE_MS")
        .ok()
        .and_then(|v| v.to_string().trim().parse::<i64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_MAX_STALE_MS);

    // Never-run (last_tick_ms == 0) is dead; otherwise dead iff too stale.
    let never_ran = last_tick_ms == 0;
    let staleness_ms: i64 = if never_ran { -1 } else { (now - last_tick_ms).max(0) };
    let dead = never_ran || staleness_ms > max_stale_ms;

    let counters = read_counters(db).await;
    let flagged = count_flagged(db, now - PROOFLESS_FLAG_MS).await;

    let status = if strict && dead { 503 } else { 200 };
    let body = json!({
        "ok": !dead,
        "service": "low-overlay",
        "check": "proof-completion",
        "strict": strict,
        "completionPass": {
            "dead": dead,
            "neverRan": never_ran,
            "lastTickMs": last_tick_ms,
            "tickCount": tick_count,
            "stalenessMs": staleness_ms,
            "maxStaleMs": max_stale_ms,
        },
        "counters": counters,
        "prooflessOver24h": flagged,
    });

    let mut resp = Response::from_json(&body)?.with_status(status);
    crate::routes::add_cors_headers(&mut resp);
    let _ = resp.headers_mut().set("Cache-Control", "no-store");
    Ok(resp)
}
