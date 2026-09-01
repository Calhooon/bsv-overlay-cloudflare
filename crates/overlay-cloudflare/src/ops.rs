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
//!   `fetch_failed_total`, `pot_beefs_compacted_total`, `spends_confirmed_total`),
//!   bumped every tick.
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
/// Pot spends UPGRADED to `spentConfirmed = 1` by the spend-confirmation
/// chaser (#186). Name-keyed → additive, no schema change.
pub const COUNTER_SPENDS_CONFIRMED: &str = "spends_confirmed_total";
/// Verified merkle proofs that arrived via the `/arc-ingest` PUSH (#228 — the
/// primary proof source). Compare against `proofs_completed_total` (the poll
/// backstop) to see the push/poll split. Name-keyed → additive.
pub const COUNTER_ARC_INGEST_PUSHED: &str = "arc_ingest_pushed_total";

// ── S2 queue-durable admission (bsv-low 2026-08-29) ──────────────────────
/// `/submit` admissions whose Phase-3 writes did NOT all land (the engine's
/// `MutationReport` carried ≥1 fault). Every one of these was, pre-S2, an
/// ack over a dropped write.
pub const COUNTER_SUBMIT_MUTATION_FAULT: &str = "submit_mutation_fault_total";
/// …of which the replay was QUEUED before the ack (the durable-ack path).
pub const COUNTER_SUBMIT_MUTATION_QUEUED: &str = "submit_mutation_queued_total";
/// …of which the replay could NOT be queued and the submit was REFUSED
/// (502 retryable). Sustained non-zero = the queue itself is unhealthy.
pub const COUNTER_SUBMIT_MUTATION_REFUSED: &str = "submit_mutation_refused_total";
/// Queue consumer: replays whose every write landed (acked).
pub const COUNTER_QUEUE_MUTATION_APPLIED: &str = "queue_mutation_applied_total";
/// Queue consumer: replays that still faulted (or errored) and were handed
/// back for the platform's retry/backoff. After `max_retries` the message
/// dead-letters — a growing DLQ is an operator page, never a silent drop.
pub const COUNTER_QUEUE_MUTATION_RETRIED: &str = "queue_mutation_retried_total";
/// Non-MINED `/arc-ingest` status callbacks (X-FullStatusUpdates bodies with
/// no merklePath) acknowledged-and-ignored (#228). A count here is NORMAL
/// operation, not an error — it proves the webhook stream is alive.
pub const COUNTER_ARC_INGEST_STATUS_IGNORED: &str = "arc_ingest_status_ignored_total";
/// `/arc-ingest` callbacks REFUSED 401 because NO bearer token was presented in
/// either accepted header. Diagnosis: a CONTRACT or CONFIG problem — the
/// courier is not authenticating the way we read it — or unauthenticated noise.
///
/// This counter exists because its absence hid a month-long outage: the 401 arm
/// bumped nothing, and `arc_ingest_status_ignored_total` is only reachable
/// AFTER auth, so `{pushed: 0, statusIgnored: 0}` read identically for "nobody
/// is calling us" and "every caller is being refused" (epoch Rule 13). The
/// wrong one was believed.
pub const COUNTER_ARC_INGEST_UNAUTH_NO_TOKEN: &str = "arc_ingest_unauthorized_no_token_total";
/// `/arc-ingest` callbacks REFUSED 401 because a token WAS presented and did
/// not equal the subject txid. Diagnosis: a stale registration, or a prober.
pub const COUNTER_ARC_INGEST_UNAUTH_BAD_TOKEN: &str = "arc_ingest_unauthorized_bad_token_total";

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
    spends_confirmed: u64,
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
        (COUNTER_SPENDS_CONFIRMED, spends_confirmed),
    ] {
        bump_counter(db, name, delta).await;
    }
}

/// Bump one persistent monotonic counter by `delta` (additive upsert).
/// Best-effort — logs and swallows any D1 error (observability must never
/// break the request that carries it).
/// bsv-low #371: the shipped `network_seen` latch write — the overlay's OWN
/// witness that the network accepted `txid`. Write-once (`INSERT OR IGNORE`;
/// a seen fact never changes), keyed on the lowercased txid, stamped by
/// SQLite (`unixepoch()`) so no clock bind is needed. A const so the
/// real-SQLite tier executes the production string against the production
/// schema.
pub const NETWORK_SEEN_INSERT_SQL: &str =
    "INSERT OR IGNORE INTO network_seen (txid, seenAt) VALUES (lower(?), unixepoch())";

/// Latch [`NETWORK_SEEN_INSERT_SQL`] for `txid`. Best-effort: the latch only
/// ACCELERATES verdict publication (the #371 third arm); a lost write leaves
/// the row behind the merkle bar, which is the pre-#371 behaviour — so a D1
/// fault is logged, never propagated.
pub async fn latch_network_seen(db: &D1Database, txid: &str) {
    let q = Query::new(NETWORK_SEEN_INSERT_SQL).bind(txid);
    if let Err(e) = q.execute(db).await {
        worker::console_log!("[#371] network_seen latch failed for {txid}: {e}");
    }
}

pub async fn bump_counter(db: &D1Database, name: &str, delta: u64) {
    if delta == 0 {
        return;
    }
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

/// INCIDENT D1-CALLBACK-FLOOD 2026-09-01 — how many status-ignored callbacks
/// accumulate per isolate before one billed counter write flushes them.
///
/// The per-callback `bump_counter` was the billing site: every webhook did one
/// D1 UPSERT on the same `ops_counters` row (~370/s sustained ≈ $28/day of
/// rows-written for a CHATTER counter whose whole job is "the stream is
/// alive"). Batching trades exactness for cost: up to `BATCH-1` events per
/// isolate are uncounted at eviction, which the counter's consumers
/// (`arc_ingest_push_health`, an operator eyeballing deltas) never needed.
/// The first [`STATUS_IGNORED_EXACT_HEAD`] events per isolate still flush
/// IMMEDIATELY — the route tier pins "an authorized callback visibly counts"
/// (Rule 13: accepted and silently-dropped must never look alike), and at
/// trickle volume (the normal state) exactness is what an operator actually
/// reads. Batching engages only once one isolate has seen flood-scale
/// volume, which is precisely when per-event exactness stops meaning
/// anything and starts costing $28/day. The 401 counters and
/// `arc_ingest_pushed_total` stay EXACT — they are rare and diagnostic.
pub const STATUS_IGNORED_FLUSH_BATCH: u32 = 256;
/// Events per isolate counted exactly (one write each) before batching.
pub const STATUS_IGNORED_EXACT_HEAD: u32 = 8;

thread_local! {
    /// Workers isolates are single-threaded. `(seen, pending)`: `seen` =
    /// lifetime events in this isolate (saturating), `pending` = events not
    /// yet flushed.
    static STATUS_IGNORED_STATE: std::cell::Cell<(u32, u32)> =
        const { std::cell::Cell::new((0, 0)) };
}

/// PURE fold for one ignored-status event: `(seen, pending)` before →
/// `((seen, pending) after, rows_to_flush_now)`. The first
/// [`STATUS_IGNORED_EXACT_HEAD`] events each flush 1 (exact); past the head
/// every [`STATUS_IGNORED_FLUSH_BATCH`]th accumulated event flushes the
/// accumulation.
pub fn status_ignored_step(state: (u32, u32)) -> ((u32, u32), u64) {
    let (seen, pending) = state;
    let seen = seen.saturating_add(1);
    if seen <= STATUS_IGNORED_EXACT_HEAD {
        return ((seen, pending), 1);
    }
    let p = pending + 1;
    if p >= STATUS_IGNORED_FLUSH_BATCH {
        ((seen, 0), u64::from(p))
    } else {
        ((seen, p), 0)
    }
}

/// Count one ignored status callback (see [`STATUS_IGNORED_FLUSH_BATCH`]):
/// exact for the first [`STATUS_IGNORED_EXACT_HEAD`] events per isolate,
/// batched past that.
pub async fn bump_status_ignored_batched(db: &D1Database) {
    let flush = STATUS_IGNORED_STATE.with(|c| {
        let (next, flush) = status_ignored_step(c.get());
        c.set(next);
        flush
    });
    if flush > 0 {
        bump_counter(db, COUNTER_ARC_INGEST_STATUS_IGNORED, flush).await;
    }
}

/// INCIDENT D1-CALLBACK-FLOOD 2026-09-01 — record a courier-delivered TERMINAL
/// broadcast verdict (REJECTED / DOUBLE_SPEND_ATTEMPTED) as evidence.
///
/// Write-once per txid (INSERT OR IGNORE: the first verdict wins, and the
/// ~7/s repeat deliveries for a wedged txid write ZERO rows — an ignored
/// insert is not a billed row write). This is EVIDENCE for the retire
/// classifier, never a verdict by itself: #214 stands — an async REJECTED is
/// authoritative only once corroborated (both-indexer definitive absence,
/// `proof_fetcher::retire_verdict`).
pub async fn record_arc_terminal(db: &D1Database, txid: &str, status: &str, extra: Option<&str>) {
    let q = Query::new(
        "INSERT OR IGNORE INTO arc_terminal (txid, status, extra, first_ms) VALUES (?, ?, ?, ?)",
    )
    .bind(txid)
    .bind(status)
    .bind(extra.unwrap_or(""))
    .bind(now_ms());
    if let Err(e) = q.execute(db).await {
        worker::console_log!("[ops] arc_terminal record failed for {txid}: {e}");
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
        let sql = proofless_watch_enrol_sql(table);
        if let Err(e) = Query::new(sql).bind(ts).execute(db).await {
            worker::console_log!("[ops] proofless_watch enrol ({table}) failed: {e}");
        }
    }

    // 2. GC: drop any txid that has since proven in either store — or was
    //    RETIRED structurally unprovable (#2b: residue must leave the watch,
    //    or `prooflessOver24h` keeps conflating it with genuine backlog).
    if let Err(e) = Query::new(PROOFLESS_WATCH_GC_SQL).execute(db).await {
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

/// SHIPPED proofless-watch enrol page for one store (factored so the
/// real-SQLite cell executes the production strings).
///
/// ORDER BY RANDOM(): the enrol page is bounded, so with a >500 backlog a
/// fixed (insertion) order would keep sampling the same head every tick
/// and UNDERCOUNT the dead-pass signal (never-mineable rows deeper in the
/// backlog would go unwatched). Random sampling makes every proofless row
/// eventually visible to the flag. (INSERT OR IGNORE still preserves each
/// row's original first-seen stamp, so the age stays real.)
///
/// #2b: the `pot_beefs` page excludes LATCHED structurally-unprovable rows
/// (`IS NOT 1` — NULL/pre-migration rows still enrol, fail-safe toward
/// watching MORE): a retired row is RESIDUE, not backlog, and enrolling it
/// would keep `prooflessOver24h` unable to tell the two apart. The
/// `transactions` page excludes `retired_ms`-latched rows for the same
/// reason (INCIDENT D1-CALLBACK-FLOOD 2026-09-01: that store's retirement
/// twin — corroborated network-dead, e.g. a UTXO_SPENT double-spend).
pub(crate) fn proofless_watch_enrol_sql(table: &str) -> String {
    let unprovable_clause = if table == "pot_beefs" {
        " AND structurally_unprovable IS NOT 1"
    } else {
        " AND retired_ms IS NULL"
    };
    format!(
        "INSERT OR IGNORE INTO proofless_watch (txid, first_seen_ms) \
         SELECT txid, ? FROM {table} WHERE has_proof = 0{unprovable_clause} \
         ORDER BY RANDOM() LIMIT {WATCH_ENROLL_LIMIT}"
    )
}

/// SHIPPED proofless-watch GC: drop txids that have since PROVEN in either
/// store, or were RETIRED (structurally unprovable in `pot_beefs` — #2b —
/// or corroborated network-dead in `transactions` — INCIDENT
/// D1-CALLBACK-FLOOD 2026-09-01), or are ORPHANS — rows with NO backing row
/// in EITHER store. The orphan arm closes the incident's zombie class: 9 of
/// beta's 27 flagged rows pointed at store rows that displacement/cleanup
/// had deleted, and the old GC (proven-only) could never release them, so
/// `prooflessOver24h` counted ghosts forever. All four arms are "no longer
/// genuine backlog", which is the only thing the 24h flag may count.
pub(crate) const PROOFLESS_WATCH_GC_SQL: &str = "DELETE FROM proofless_watch WHERE \
     txid IN (SELECT txid FROM pot_beefs WHERE has_proof = 1) OR \
     txid IN (SELECT txid FROM transactions WHERE has_proof = 1) OR \
     txid IN (SELECT txid FROM pot_beefs WHERE structurally_unprovable = 1) OR \
     txid IN (SELECT txid FROM transactions WHERE retired_ms IS NOT NULL) OR \
     (txid NOT IN (SELECT txid FROM pot_beefs) AND \
      txid NOT IN (SELECT txid FROM transactions))";

/// Count proofless_watch rows first seen before `cutoff_ms`.
async fn count_flagged(db: &D1Database, cutoff_ms: i64) -> u64 {
    let row: Option<CountRow> =
        Query::new("SELECT COUNT(*) AS c FROM proofless_watch WHERE first_seen_ms < ?")
            .bind(cutoff_ms)
            .fetch_optional(db)
            .await
            .ok()
            .flatten();
    row.map(|r| r.c.max(0.0) as u64).unwrap_or(0)
}

/// Read the persistent counters into a JSON object (missing ⇒ 0).
///
/// The #366 census rows share `ops_counters` but are EXCLUDED here — they are
/// served structured under `submitReadinessCensus` (see [`census_json`]), and
/// reporting the same numbers twice under two shapes invites a reader to
/// depend on the flat spelling this module never promised. (Safe filter: no
/// `submit_census_` row existed before #366, so nothing a reader saw is
/// removed.)
async fn read_counters(db: &D1Database) -> serde_json::Value {
    let rows: Vec<CounterRow> =
        Query::new("SELECT name, value FROM ops_counters WHERE name NOT LIKE 'submit_census_%'")
            .fetch_all(db)
            .await
            .unwrap_or_default();
    let mut obj = json!({
        COUNTER_PROOFS_COMPLETED: 0,
        COUNTER_FETCH_FAILED: 0,
        COUNTER_POT_BEEFS_COMPACTED: 0,
        COUNTER_SPENDS_CONFIRMED: 0,
        COUNTER_ARC_INGEST_PUSHED: 0,
        COUNTER_ARC_INGEST_STATUS_IGNORED: 0,
        COUNTER_ARC_INGEST_UNAUTH_NO_TOKEN: 0,
        COUNTER_ARC_INGEST_UNAUTH_BAD_TOKEN: 0,
        // S2 (2026-08-29): seeded so a never-bumped counter reads an explicit
        // 0 on the surface — an ABSENT key is an unknown, and an unknown must
        // not read as fine.
        COUNTER_SUBMIT_MUTATION_FAULT: 0,
        COUNTER_SUBMIT_MUTATION_QUEUED: 0,
        COUNTER_SUBMIT_MUTATION_REFUSED: 0,
        COUNTER_QUEUE_MUTATION_APPLIED: 0,
        COUNTER_QUEUE_MUTATION_RETRIED: 0,
    });
    for r in rows {
        obj[r.name] = json!(r.value.max(0.0) as u64);
    }
    obj
}

/// Derive the `/arc-ingest` push-path verdict from the four arc-ingest counters
/// — the ONE distinction the health surface could not previously express.
///
/// * `"flowing"` — at least one callback got past auth (a proof push or an
///   acknowledged status callback). The push path is live.
/// * `"refusing"` — callbacks ARE arriving and every one of them is being
///   401'd. This is the state that hid for a month: the primary proof source is
///   down, everything is falling through to the ~30-min poll backstop, and the
///   fix is a contract/config one (see the two `unauthorized_*` counters for
///   which). **This is the pageable state.**
/// * `"silent"` — no callback has ever arrived at all. Either nothing has been
///   broadcast, or `X-CallbackUrl` is not being registered.
///
/// MODELLING BOUNDARY, stated here because a reader will otherwise assume more
/// (epoch Rule 17): these are MONOTONIC LIFETIME totals, so this verdict is
/// decisive only until the first accepted callback — after that it latches
/// `"flowing"` forever. A later regression that starts 401ing everything is
/// visible in the DELTA of the `unauthorized_*` counters between two reads
/// (same reader contract as the #366 census), NOT in this field. It is a
/// standing-start diagnosis, not a continuous alarm; wiring a pager to the
/// delta is the follow-on.
///
/// It reports no number the `counters` object already reports — a second
/// spelling of the same value is what `read_counters` deliberately avoids.
fn arc_ingest_push_health(counters: &serde_json::Value) -> &'static str {
    let v = |name: &str| counters.get(name).and_then(|x| x.as_u64()).unwrap_or(0);
    let admitted = v(COUNTER_ARC_INGEST_PUSHED) + v(COUNTER_ARC_INGEST_STATUS_IGNORED);
    let refused = v(COUNTER_ARC_INGEST_UNAUTH_NO_TOKEN) + v(COUNTER_ARC_INGEST_UNAUTH_BAD_TOKEN);
    match (admitted, refused) {
        (0, 0) => "silent",
        (0, _) => "refusing",
        _ => "flowing",
    }
}

/// The #366 broadcast-gated readiness census, shaped for `/health/invariants`.
///
/// Reads the durable `submit_census_*` rows (written by the `/submit` route —
/// `routes.rs`, `ProceedWithoutGate` arm; names owned by
/// [`crate::submit_census`]) and serves them as monotonic totals per
/// (mode, population, state) plus the global reason breakdown.
///
/// READER CONTRACT, stated in the body itself because the reader is a human
/// with `curl` (or a poller diffing two reads):
/// * "the last N" = the DELTA between two reads of these monotonic totals.
/// * a window whose `observed` delta is 0 is NO EVIDENCE — it carries a
///   streak, it never credits one (bsv-low #341's `None`-not-`0` posture).
///   The flip criterion is "client `wouldHaveFailed` delta is 0 across a
///   window whose client `observed` delta is MEANINGFULLY POSITIVE, and the
///   `couldNotEvaluate` bucket is understood" — never "the endpoint read 0".
/// * arrive-only residual: these count only submits the overlay SERVED. A
///   client that could not reach the overlay at all is invisible here, and an
///   overlay outage is exactly when the client is least ready — that slice
///   stays with the client-side warn (bsv-low #351).
pub async fn census_json(db: &D1Database) -> serde_json::Value {
    let rows: Vec<CounterRow> =
        Query::new("SELECT name, value FROM ops_counters WHERE name LIKE 'submit_census_%'")
            .fetch_all(db)
            .await
            .unwrap_or_default();
    let value_of = |name: &str| -> u64 {
        rows.iter()
            .find(|r| r.name == name)
            .map(|r| r.value.max(0.0) as u64)
            .unwrap_or(0)
    };

    // byMode.{mode}.{population} = {gatedReady, wouldHaveFailed,
    // couldNotEvaluate, observed} — driven off the ONE table the writer uses
    // (`CENSUS_STATE_COUNTERS`), so a name cannot drift between write and
    // read. Accumulated in a plain map first (no panic-capable JSON indexing
    // on a request path).
    let mut cells: std::collections::BTreeMap<(&str, &str), (u64, u64, u64)> =
        std::collections::BTreeMap::new();
    for (name, mode, population, state) in crate::submit_census::CENSUS_STATE_COUNTERS {
        let v = value_of(name);
        let cell = cells.entry((mode, population)).or_insert((0, 0, 0));
        match state {
            "ready" => cell.0 += v,
            "would_fail" => cell.1 += v,
            _ => cell.2 += v,
        }
    }
    let mut mode_maps: std::collections::BTreeMap<
        &str,
        serde_json::Map<String, serde_json::Value>,
    > = std::collections::BTreeMap::new();
    for ((mode, population), (ready, fail, uneval)) in cells {
        mode_maps.entry(mode).or_default().insert(
            population.to_string(),
            json!({
                "gatedReady": ready,
                "wouldHaveFailed": fail,
                "couldNotEvaluate": uneval,
                "observed": ready + fail + uneval,
            }),
        );
    }
    let mut by_mode = serde_json::Map::new();
    for (mode, populations) in mode_maps {
        by_mode.insert(mode.to_string(), serde_json::Value::Object(populations));
    }

    let mut reasons = serde_json::Map::new();
    for (name, key) in crate::submit_census::CENSUS_REASON_COUNTERS {
        reasons.insert(key.to_string(), json!(value_of(name)));
    }

    json!({
        // Rule 13: the three states are served distinctly; `couldNotEvaluate`
        // is never folded into either decided state.
        "byMode": serde_json::Value::Object(by_mode),
        "wouldFailAndUnevalReasons": serde_json::Value::Object(reasons),
        // Monotonic totals — durable across isolate recycling (D1), unlike
        // the per-isolate submitAdmission soak counters.
        "semantics": "monotonic totals; 'last N' is a delta between reads",
        "emptyWindowRule": "an observed delta of 0 across a window is NO EVIDENCE — it carries the streak, it never credits it (bsv-low #341)",
        "arriveOnlyResidual": "counts only submits the overlay SERVED; an unreachable overlay is exactly when the client is least ready — that slice stays with the client-side warn (bsv-low #351)",
    })
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
pub async fn health_invariants(
    db: &D1Database,
    env: &Env,
    strict: bool,
) -> worker::Result<Response> {
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
    let staleness_ms: i64 = if never_ran {
        -1
    } else {
        (now - last_tick_ms).max(0)
    };
    let dead = never_ran || staleness_ms > max_stale_ms;

    let counters = read_counters(db).await;
    let flagged = count_flagged(db, now - PROOFLESS_FLAG_MS).await;
    let census = census_json(db).await;
    // #371: total network_seen latches — the observable the ci-route tier
    // asserts a submit MOVES (gate MEDIUM-1: both latch call sites are
    // wasm-route-only, so without a served count the feature could be
    // deleted with every native tier green). -1 = the table is unreadable
    // (pre-migration isolate), distinct from a real 0 (Rule 13).
    let network_seen_total: i64 = Query::new("SELECT COUNT(*) AS c FROM network_seen")
        .fetch_optional::<CountRow>(db)
        .await
        .ok()
        .flatten()
        .map_or(-1, |r| r.c.max(0.0) as i64);
    // #2b: rows RETIRED from the proof-poll pools as structurally
    // unprovable (superseded competing spends of a confirmed pot outpoint,
    // dominated by superseded pre-signed refunds). The Rule 13 surfaced
    // number: `prooflessOver24h` above now counts GENUINE backlog only, and
    // this is where the residue went — a LIVE count (a reorg-cleared latch
    // honestly decrements), -1 = unreadable (pre-migration isolate),
    // distinct from a real 0.
    let retired_unprovable_total: i64 =
        Query::new("SELECT COUNT(*) AS c FROM pot_beefs WHERE structurally_unprovable = 1")
            .fetch_optional::<CountRow>(db)
            .await
            .ok()
            .flatten()
            .map_or(-1, |r| r.c.max(0.0) as i64);
    // INCIDENT D1-CALLBACK-FLOOD 2026-09-01: the two new Rule-13 surfaces.
    // `arcTerminalTotal` = distinct txids with a courier-delivered terminal
    // verdict on record (evidence intake is ALIVE); `txRetiredTotal` = the
    // `transactions` rows retired on corroborated evidence (where the
    // incident's retry-forever population went). -1 = table unreadable
    // (pre-migration isolate), distinct from a real 0.
    let arc_terminal_total: i64 = Query::new("SELECT COUNT(*) AS c FROM arc_terminal")
        .fetch_optional::<CountRow>(db)
        .await
        .ok()
        .flatten()
        .map_or(-1, |r| r.c.max(0.0) as i64);
    let tx_retired_total: i64 =
        Query::new("SELECT COUNT(*) AS c FROM transactions WHERE retired_ms IS NOT NULL")
            .fetch_optional::<CountRow>(db)
            .await
            .ok()
            .flatten()
            .map_or(-1, |r| r.c.max(0.0) as i64);

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
        // Rule 13: "nobody is calling us" vs "everybody is being refused" are
        // now distinguishable. See `arc_ingest_push_health` for the lifetime-
        // total boundary — the continuous instrument is the DELTA of the two
        // `arc_ingest_unauthorized_*_total` counters above.
        "arcIngestPushHealth": arc_ingest_push_health(&counters),
        "counters": counters,
        // #2b: GENUINE backlog only — retired (structurally-unprovable)
        // rows are excluded at enrol AND dropped by the watch GC.
        "prooflessOver24h": flagged,
        // #2b: where the residue went (live count; -1 = table unreadable).
        "retiredUnprovableTotal": retired_unprovable_total,
        // INCIDENT D1-CALLBACK-FLOOD 2026-09-01 (both -1 = unreadable):
        "arcTerminalTotal": arc_terminal_total,
        "txRetiredTotal": tx_retired_total,
        // #371: lifetime network_seen latch count (-1 = table unreadable).
        "networkSeenTotal": network_seen_total,
        // #347 soak signal (Rule 6c closure criterion 3): `unauthenticatedUngated`
        // must reach ~0 before SUBMIT_ENFORCE is flipped to true. Per-isolate and
        // therefore lossy — a soak signal, never an audit log.
        "submitAdmission": crate::submit_gate::counters_json(),
        // #366: would every honest CLIENT submit survive `broadcast-gated`?
        // The flip-criterion instrument for #347 criterion 1 — durable D1
        // totals, three states (Rule 13), reader contract in `census_json`.
        "submitReadinessCensus": census,
    });

    let mut resp = Response::from_json(&body)?.with_status(status);
    crate::routes::add_cors_headers(&mut resp);
    let _ = resp.headers_mut().set("Cache-Control", "no-store");
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build the counters object the way `read_counters` does (all four
    /// arc-ingest names present, defaulting to 0), then set the values.
    fn counters(
        pushed: u64,
        status_ignored: u64,
        no_token: u64,
        bad_token: u64,
    ) -> serde_json::Value {
        json!({
            COUNTER_ARC_INGEST_PUSHED: pushed,
            COUNTER_ARC_INGEST_STATUS_IGNORED: status_ignored,
            COUNTER_ARC_INGEST_UNAUTH_NO_TOKEN: no_token,
            COUNTER_ARC_INGEST_UNAUTH_BAD_TOKEN: bad_token,
        })
    }

    #[test]
    fn arc_ingest_push_health_separates_silent_from_refusing() {
        // THE defect this counter pair exists for: before it, these two rows
        // were the SAME observation (`{pushed: 0, statusIgnored: 0}`) and the
        // wrong one was believed for a month.
        assert_eq!(arc_ingest_push_health(&counters(0, 0, 0, 0)), "silent");
        assert_eq!(arc_ingest_push_health(&counters(0, 0, 9, 0)), "refusing");
        assert_eq!(arc_ingest_push_health(&counters(0, 0, 0, 9)), "refusing");
        assert_ne!(
            arc_ingest_push_health(&counters(0, 0, 0, 0)),
            arc_ingest_push_health(&counters(0, 0, 9, 0)),
        );
    }

    #[test]
    fn arc_ingest_push_health_is_flowing_once_anything_gets_past_auth() {
        // A verified proof push, an acknowledged status callback, or a mix with
        // some refusals — all "flowing": the push path works.
        assert_eq!(arc_ingest_push_health(&counters(1, 0, 0, 0)), "flowing");
        assert_eq!(arc_ingest_push_health(&counters(0, 1, 0, 0)), "flowing");
        assert_eq!(arc_ingest_push_health(&counters(5, 7, 3, 2)), "flowing");
    }

    /// bsv-low handoff #2b, real SQLite over the SHIPPED strings: the
    /// proofless watch neither ENROLS a retired (structurally-unprovable)
    /// row nor keeps one it already holds — `prooflessOver24h` counts
    /// GENUINE backlog only, while NULL (never-examined) rows keep
    /// enrolling (fail-safe: watch MORE).
    #[test]
    fn proofless_watch_excludes_retired_rows_real_sqlite() {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        for sql in crate::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                let msg = e.to_string().to_ascii_lowercase();
                assert!(
                    msg.contains("duplicate column"),
                    "production migration failed under real SQLite: {e}\n{sql}"
                );
            }
        }
        conn.execute_batch(
            "INSERT INTO pot_beefs (txid, beef, has_proof, structurally_unprovable) \
             VALUES ('retired', x'beef', 0, 1); \
             INSERT INTO pot_beefs (txid, beef, has_proof) VALUES ('backlog', x'beef', 0); \
             INSERT INTO transactions (txid, beef, has_proof) VALUES ('enginetx', x'beef', 0);",
        )
        .unwrap();

        // Enrol pages (the shipped per-table strings): the retired row is
        // excluded; the NULL-latch row and the engine row enrol.
        for table in ["pot_beefs", "transactions"] {
            conn.execute(
                &proofless_watch_enrol_sql(table),
                rusqlite::params![1_000i64],
            )
            .unwrap();
        }
        let watched = |txid: &str| -> bool {
            conn.query_row(
                "SELECT COUNT(*) FROM proofless_watch WHERE txid = ?",
                rusqlite::params![txid],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
                > 0
        };
        assert!(!watched("retired"), "a retired row is residue, not backlog");
        assert!(watched("backlog"), "a NULL-latch row still enrols");
        assert!(watched("enginetx"));

        // A row retired AFTER enrolment is dropped by the GC (or the 24h
        // flag would keep conflating residue with backlog forever).
        conn.execute(
            "UPDATE pot_beefs SET structurally_unprovable = 1 WHERE txid = 'backlog'",
            [],
        )
        .unwrap();
        conn.execute(PROOFLESS_WATCH_GC_SQL, []).unwrap();
        assert!(!watched("backlog"), "the GC drops retired rows");
        assert!(watched("enginetx"), "genuine backlog survives the GC");

        // INCIDENT D1-CALLBACK-FLOOD 2026-09-01 — the two new arms.
        //
        // (1) A `transactions` row retired on corroborated network-death
        // (`retired_ms`) stops enrolling AND is dropped if already watched —
        // the 11 UTXO_SPENT double-spends sat flagged for weeks.
        conn.execute(
            "UPDATE transactions SET retired_ms = 1, retired_reason = 'test' \
             WHERE txid = 'enginetx'",
            [],
        )
        .unwrap();
        conn.execute(PROOFLESS_WATCH_GC_SQL, []).unwrap();
        assert!(
            !watched("enginetx"),
            "a retired transactions row is residue, not backlog"
        );
        conn.execute(
            &proofless_watch_enrol_sql("transactions"),
            rusqlite::params![2_000i64],
        )
        .unwrap();
        assert!(
            !watched("enginetx"),
            "a retired transactions row never re-enrols"
        );
        // (2) ORPHANS: a watch row whose store row was deleted (displacement
        // / cleanup) could NEVER leave under the proven-only GC — 9 of beta's
        // 27 flagged rows were exactly this zombie class. The orphan arm
        // drops it; rows with a live store row are untouched.
        conn.execute_batch(
            "INSERT INTO proofless_watch (txid, first_seen_ms) VALUES ('ghost', 1); \
             INSERT INTO pot_beefs (txid, beef, has_proof) VALUES ('alive', x'beef', 0); \
             INSERT INTO proofless_watch (txid, first_seen_ms) VALUES ('alive', 1);",
        )
        .unwrap();
        conn.execute(PROOFLESS_WATCH_GC_SQL, []).unwrap();
        assert!(!watched("ghost"), "an orphan watch row is GC'd");
        assert!(watched("alive"), "a store-backed row survives the orphan arm");
    }

    /// INCIDENT D1-CALLBACK-FLOOD 2026-09-01: the status-ignored counter's
    /// exact-head-then-batch fold — the first [`STATUS_IGNORED_EXACT_HEAD`]
    /// events each write (the route tier's Rule-13 pin: an authorized
    /// callback visibly counts), then writes happen once per
    /// [`STATUS_IGNORED_FLUSH_BATCH`] with no events lost between flushes.
    #[test]
    fn status_ignored_fold_exact_head_then_batch() {
        let mut state = (0u32, 0u32);
        let mut written = 0u64;
        // Head: every event flushes exactly 1.
        for i in 1..=STATUS_IGNORED_EXACT_HEAD {
            let (next, flush) = status_ignored_step(state);
            state = next;
            assert_eq!(flush, 1, "head event {i} must count exactly");
            written += flush;
        }
        assert_eq!(written, u64::from(STATUS_IGNORED_EXACT_HEAD));
        // Past the head: silence until the batch fills, then one flush of the
        // full accumulation — nothing lost.
        let mut post_head_events = 0u64;
        let mut flushed_after_head = 0u64;
        loop {
            let (next, flush) = status_ignored_step(state);
            state = next;
            post_head_events += 1;
            flushed_after_head += flush;
            if flush > 0 {
                assert_eq!(
                    flush,
                    u64::from(STATUS_IGNORED_FLUSH_BATCH),
                    "the batch flush carries the whole accumulation"
                );
                break;
            }
        }
        assert_eq!(post_head_events, u64::from(STATUS_IGNORED_FLUSH_BATCH));
        assert_eq!(flushed_after_head, post_head_events, "no event is lost");
        // Saturation safety at absurd lifetimes.
        let (_, flush) = status_ignored_step((u32::MAX, 0));
        assert_eq!(flush, 0);
    }

    #[test]
    fn arc_ingest_push_health_reads_the_names_read_counters_serves() {
        // Rule 16 (pin the boundary, not the sides): the verdict is computed
        // off the object `read_counters` builds. A name that is ABSENT reads as
        // 0 and can therefore only ever produce "silent" — the exact way this
        // field could rot back into the pre-fix ambiguity. Asserted, not
        // assumed, so a rename that misses one side goes red here.
        let mut c = counters(0, 0, 4, 0);
        assert_eq!(arc_ingest_push_health(&c), "refusing");
        c.as_object_mut()
            .unwrap()
            .remove(COUNTER_ARC_INGEST_UNAUTH_NO_TOKEN);
        assert_eq!(arc_ingest_push_health(&c), "silent");
    }
}
