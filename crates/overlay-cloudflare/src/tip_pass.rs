//! bsv-low loop 6 (2026-09-07) — the BLOCK-EVENT spend-confirmation pass.
//!
//! A spent-but-unconfirmed pot row can change at exactly one moment: a new
//! block. Arcade's MINED push is the PRIMARY confirmer (`/arc-ingest`,
//! measured ~4.5 min after the block on beta); the `*/15` cron is the LAST
//! backstop (up to 15 min). This route is the floor in between: the app-layer
//! forwards every tip it receives from chaintracks (`POST /internal/tip-changed`,
//! bearer `INTERNAL_TOKEN` — the same first-party webhook shape it serves) and
//! the overlay runs ONE bounded confirmation pass over the few rows that are
//! `spent = 1 AND spentConfirmed = 0`, then ships the pot-changed webhook for
//! whatever it confirmed. Cost: at most `TIP_PASS_LIMIT` rows and
//! `TIP_PASS_BUDGET` courier calls per block. A height already passed in this
//! isolate is skipped (chaintracks can announce a tip more than once).
//!
//! Money truth is untouched: a client credits on its OWN landing proof; this
//! only moves the SERVED state ("refund landed") from minutes to the tip lag.
use std::cell::Cell;

use worker::*;

/// Rows scanned per block-event pass.
pub const TIP_PASS_LIMIT: u64 = 20;
/// Courier calls per block-event pass (a fetcher of its own — never the cron's).
pub const TIP_PASS_BUDGET: u32 = 20;

thread_local! {
    static LAST_PASSED_HEIGHT: Cell<u64> = const { Cell::new(0) };
}

/// `{"height": <positive integer>}` → the height; anything else is `None`.
pub fn parse_tip_changed(raw: &[u8]) -> Option<u64> {
    serde_json::from_slice::<serde_json::Value>(raw)
        .ok()?
        .get("height")?
        .as_u64()
        .filter(|h| *h > 0)
}

/// bsv-low M19 R2: the header HASH the app-layer forwards beside the height
/// (`{"height": n, "hash": "<64 hex>"}`), lower-cased; absent or malformed
/// → `None` (an older app-layer forwards the height alone — the pass still
/// runs, the reorg detector simply has nothing to compare).
pub fn parse_tip_changed_hash(raw: &[u8]) -> Option<String> {
    let v = serde_json::from_slice::<serde_json::Value>(raw).ok()?;
    let h = v.get("hash")?.as_str()?.trim();
    if h.len() != 64 || !h.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some(h.to_ascii_lowercase())
}

/// bsv-low M19 R2 round 3 (review MED-1): the fork height the chaintracks
/// announce carries (`{"height", "hash", "reorgFrom": <h>}`) from its own
/// `handle_reorg` — the lowest height whose block the reorg replaced. `None`
/// when the announce carried none (no reorg, or an older announcer).
pub fn parse_tip_changed_reorg_from(raw: &[u8]) -> Option<u64> {
    serde_json::from_slice::<serde_json::Value>(raw)
        .ok()?
        .get("reorgFrom")?
        .as_u64()
        .filter(|h| *h > 0)
}

/// Rows a detected reorg re-verifies per block-event pass at the replaced
/// height (the sweep's cursor walk covers the rest).
pub const REORG_DEMOTE_LIMIT: u64 = 200;
/// The revalidation sweep's window: the last N heights (the reference's
/// `reorgScanDepth` default).
pub const REORG_SWEEP_DEPTH: u64 = 3;
/// Confirmed rows the sweep re-verifies per pass, PER LEG (bounded D1 reads).
/// bsv-low M19 R2 round 3 (review MED-3): raised 50 → 200 now that the
/// `(height, root)` memo collapses a whole block's header re-verifies to
/// ~one chaintracks read — the per-pass cost is the memo's distinct
/// (height, root) count (~depth), not the row count, so a bigger page keeps
/// the transactions/hop leg abreast of an 18-pair fleet (~6 proven txs/hand)
/// without adding subrequests. The D1 row read stays index-served.
pub const REORG_SWEEP_LIMIT: u64 = 200;
/// The courier budget for the sweep's MED-4 re-check of courier-confirmed
/// rows (a small ladder allowance; most passes spend none).
pub const REORG_SWEEP_BUDGET: u32 = 20;

/// `Authorization: Bearer <INTERNAL_TOKEN>` — exact, non-empty; no secret ⇒ refused.
pub fn bearer_ok(authorization: Option<&str>, secret: Option<&str>) -> bool {
    match (authorization, secret) {
        (Some(a), Some(s)) if !s.is_empty() => a.strip_prefix("Bearer ").map(str::trim) == Some(s),
        _ => false,
    }
}

/// True the FIRST time this isolate sees `height` (or a higher one); a repeat
/// or an older height is not a new block and runs no pass.
pub fn first_pass_for(height: u64) -> bool {
    LAST_PASSED_HEIGHT.with(|c| {
        if c.get() >= height {
            false
        } else {
            c.set(height);
            true
        }
    })
}

/// `POST /internal/tip-changed` — see the module doc.
pub async fn internal_tip_changed(
    mut req: Request,
    env: &Env,
    ctx: &Context,
    pot_storage: &dyn overlay_discovery::pot::storage::PotStorage,
    ops_db: Option<&D1Database>,
) -> Result<Response> {
    let authorization = req.headers().get("authorization").ok().flatten();
    let secret = env.secret("INTERNAL_TOKEN").ok().map(|s| s.to_string());
    if !bearer_ok(authorization.as_deref(), secret.as_deref()) {
        console_log!("POST /internal/tip-changed -> 401");
        return Response::error("unauthorized", 401);
    }
    let raw = req.bytes().await?;
    let Some(height) = parse_tip_changed(&raw) else {
        return Response::error("body must be {\"height\": <positive integer>}", 400);
    };
    // ── bsv-low M19 R2 (round 2): the reorg detector ──
    // The announce carries the header hash (chaintracks announces on ANY tip
    // change, height or hash, since m19-reorg-announce; the app-layer
    // forwards it); an older announcer's hash-less body falls back to a
    // header read. Recorded per height in D1 (seen across isolates); the ONE
    // reorg producer is a same-height hash change; a lower unrecorded height
    // is an OLD header (ignored, counted); a repeat is a repeat.
    let hash = match parse_tip_changed_hash(&raw) {
        Some(h) => Some(h),
        None => {
            let read = crate::chain_tracker::chaintracks_block_hash(env, height).await;
            if read.is_none() {
                console_log!("POST /internal/tip-changed height={height}: no hash in the body and none readable — no reorg detection this pass");
            }
            read
        }
    };
    let announce = match hash.as_deref() {
        Some(h) => detect_announce(pot_storage, height, h).await,
        None => None,
    };
    let tracker = crate::lookup_service_chain_tracker(env);
    let sweep_fetcher = crate::courier_fetcher(env, tracker.clone()).with_budget(REORG_SWEEP_BUDGET);
    if announce == Some(overlay_discovery::pot::reorg::TipAnnounce::Old) {
        console_log!("POST /internal/tip-changed height={height} -> 200 (an OLD header below the held tip: ignored, counted)");
        if let Some(db) = ops_db {
            crate::ops::bump_counter(db, crate::ops::COUNTER_TIP_ANNOUNCE_OLD, 1).await;
        }
        return Response::from_json(&serde_json::json!({ "ok": true, "height": height, "skipped": "old-header" }));
    }
    // The targeted `Reorg{from}` range: from EITHER producer, widened to
    // cover both. (1) The overlay's OWN same-height hash change (the
    // `classify_tip_announce` arm). (2) The `reorgFrom` the chaintracks
    // announce carries from its `handle_reorg` (review MED-1): chaintracks
    // activates a branch only on MORE work, so the common 2026-09-07 shape
    // (a sibling at equal work at H, the tip flipping only when its child
    // H+1 lands) reaches us as `{H+1, hash, reorgFrom: fork+1}` — an
    // `Extends` to the overlay, whose fast arm would otherwise miss it.
    let announced_from = parse_tip_changed_reorg_from(&raw).filter(|f| *f > 0);
    let same_height_from = match announce {
        Some(overlay_discovery::pot::reorg::TipAnnounce::Reorg { from }) => Some(from),
        _ => None,
    };
    let reorg_from = match (announced_from, same_height_from) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, b) => b,
    };
    let mut demotion = crate::reorg_sweep::ReverifyPassSummary::default();
    if let Some(from) = reorg_from {
        console_log!(
            "POST /internal/tip-changed height={height} hash={} reorgFrom(announced)={announced_from:?} reorgFrom(same-height)={same_height_from:?}: REORG — re-verifying every confirmation in {from}..={height}",
            hash.as_deref().unwrap_or("?")
        );
        // the whole reorged range, not just one height: a fork at `from`
        // orphaned every block from there to the new tip.
        demotion = crate::reorg_sweep::handle_reorg(pot_storage, tracker.as_deref(), from, height, None, REORG_DEMOTE_LIMIT).await;
        if let Some(db) = ops_db {
            crate::ops::bump_counter(db, crate::ops::COUNTER_CHAIN_REORGS_DETECTED, 1).await;
            crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_DEMOTED, (demotion.stale + demotion.demoted_blind) as u64).await;
            crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_TRACKER_FAULTS, demotion.faults as u64).await;
        }
    }
    // A repeat of an already-passed height runs nothing — unless it carried
    // a reorg, which must still sweep and re-chase.
    if !first_pass_for(height) && reorg_from.is_none() {
        console_log!("POST /internal/tip-changed height={height} -> 200 (already passed in this isolate)");
        return Response::from_json(&serde_json::json!({ "ok": true, "height": height, "skipped": "already-passed" }));
    }
    // ── bsv-low M19B-G1: Arcade's reorg EVENTS first (its orphaned-block
    // feed, corroborated against chaintracks, every row judged by its own
    // stored proof; a refuted spender re-anchored in place when the ladder
    // serves the canonical proof, demoted otherwise). Before the sweep, so a
    // demotion is re-chased in this same tick.
    let arcade = run_arcade_reorg_pass(env, pot_storage, ops_db, "tip-changed").await;
    // ── the revalidation sweep (reference: the fallback for trackers without
    // a reorg stream; here the PRIMARY path, every block): three cursor-walked
    // legs (spenders, the pots' own proofs, the engine's hop proofs). It runs
    // BEFORE the confirmation pass so a row it demotes is re-chased in this
    // same tick with a proof verified against the canonical chain.
    let tx_store = ops_db.map(crate::reorg_sweep::D1ProvenTxStore);
    let sweep = crate::reorg_sweep::reorg_revalidation_sweep(
        pot_storage,
        tx_store.as_ref(),
        tracker.as_deref(),
        Some(&sweep_fetcher),
        height,
        REORG_SWEEP_DEPTH,
        REORG_SWEEP_LIMIT,
    )
    .await;
    if let Some(db) = ops_db {
        let scanned = sweep.spenders.scanned + sweep.pot_beefs.scanned + sweep.transactions.scanned;
        crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_REVERIFIED, scanned as u64).await;
        crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_STALE_PROOFS, sweep.spenders.stale as u64).await;
        crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_DEMOTED, sweep.spenders.stale as u64).await;
        crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_STALE_POT_PROOFS, sweep.pot_beefs.stale as u64).await;
        crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_STALE_TX_PROOFS, sweep.transactions.stale as u64).await;
        let faults = sweep.spenders.faults + sweep.pot_beefs.faults + sweep.transactions.faults;
        crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_TRACKER_FAULTS, faults as u64).await;
        crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_HEALED_FROM_COURIER, sweep.spenders.stored_from_courier as u64).await;
        crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_REANCHORED, sweep.spenders.reanchored as u64).await;
    }
    let fetcher = crate::courier_fetcher(env, tracker).with_budget(TIP_PASS_BUDGET);
    // min_age 0: the block just landed; every unconfirmed spend is a candidate
    // (the push may still arrive — its CAS then finds the row confirmed, harmless)
    let s = crate::proof_fetcher::complete_spend_confirmations(pot_storage, &fetcher, TIP_PASS_LIMIT, 0).await;
    console_log!(
        "POST /internal/tip-changed height={height} -> 200 (block-event spend-confirmation: scanned={} confirmed={} \
         still_unconfirmed={} fetch_failed={} tracker_faults={} cas_missed={} cas_errors={}; \
         reorg: from={:?} demoted={} demoted_blind={} standing={} faults={}; \
         sweep: spenders {:?} scanned={} standing={} stale={} faults={} no_stored_proof={} demote_missed={} errors={} exhausted={}; \
         pot_beefs {:?} scanned={} stale={} faults={}; transactions {:?} scanned={} stale={} faults={}; state_errors={})",
        s.scanned,
        s.confirmed,
        s.still_unconfirmed,
        s.fetch_failed,
        s.tracker_faults,
        s.cas_missed,
        s.cas_errors,
        reorg_from,
        demotion.stale,
        demotion.demoted_blind,
        demotion.standing,
        demotion.faults,
        sweep.spenders_window,
        sweep.spenders.scanned,
        sweep.spenders.standing,
        sweep.spenders.stale,
        sweep.spenders.faults,
        sweep.spenders.no_stored_proof,
        sweep.spenders.demote_missed,
        sweep.spenders.errors,
        sweep.spenders.exhausted,
        sweep.pot_beefs_window,
        sweep.pot_beefs.scanned,
        sweep.pot_beefs.stale,
        sweep.pot_beefs.faults,
        sweep.transactions_window,
        sweep.transactions.scanned,
        sweep.transactions.stale,
        sweep.transactions.faults,
        sweep.state_errors
    );
    if let Some(db) = ops_db {
        crate::ops::bump_counter(db, crate::ops::COUNTER_TIP_PASS_TOTAL, 1).await;
        if s.confirmed > 0 {
            crate::ops::bump_counter(db, crate::ops::COUNTER_SPENDS_CONFIRMED, s.confirmed as u64).await;
        }
    }
    // what the pass confirmed is a pot CHANGE — the seats' events boxes hear it
    crate::pot_changes::flush(env, |fut| ctx.wait_until(fut));
    Response::from_json(&serde_json::json!({
        "ok": true,
        "height": height,
        "scanned": s.scanned,
        "confirmed": s.confirmed,
        "stillUnconfirmed": s.still_unconfirmed,
        "reorgFrom": reorg_from,
        "reorgDemoted": demotion.stale + demotion.demoted_blind,
        "reorgStanding": demotion.standing,
        "sweepScanned": sweep.spenders.scanned + sweep.pot_beefs.scanned + sweep.transactions.scanned,
        "sweepStale": sweep.spenders.stale + sweep.pot_beefs.stale + sweep.transactions.stale,
        "sweepFaults": sweep.spenders.faults + sweep.pot_beefs.faults + sweep.transactions.faults,
        "sweepWindow": sweep.spenders_window,
        "arcadeReorg": arcade_reorg_summary_json(&arcade),
    }))
}

/// bsv-low M19B-G1: ONE bounded pass of the Arcade reorg-event consumer
/// (`crate::arcade_reorg`), on the worker's own sources (the `ARCADE_URL`
/// feed, chaintracks through the binding, the D1 state row, the courier
/// ladder on its own budget), its outcome folded into the lifetime
/// counters. Runs before the routine sweep on every block-event pass, on
/// the cron, and on demand (`POST /internal/arcade-reorg`).
pub async fn run_arcade_reorg_pass(
    env: &Env,
    pot_storage: &dyn overlay_discovery::pot::storage::PotStorage,
    ops_db: Option<&D1Database>,
    origin: &str,
) -> crate::arcade_reorg::ArcadePassSummary {
    let tracker = crate::lookup_service_chain_tracker(env);
    let Some(db) = ops_db else {
        console_log!("[arcade-reorg] ({origin}) no D1 handle: no state row, no pass");
        return crate::arcade_reorg::ArcadePassSummary { stopped: Some("no state store".into()), ..Default::default() };
    };
    let Some(tracker) = tracker else {
        console_log!("[arcade-reorg] ({origin}) no header source configured: no pass");
        return crate::arcade_reorg::ArcadePassSummary { stopped: Some("no header source configured".into()), ..Default::default() };
    };
    let arcade_base = env
        .var("ARCADE_URL")
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| crate::broadcaster::ARCADE_DEFAULT_URL.to_string());
    let feed = crate::arcade_reorg::ArcadeBlockStatusFeed::new(arcade_base);
    let headers = crate::arcade_reorg::EnvHeaderSource { env, tracker: tracker.as_ref() };
    let state = crate::arcade_reorg::D1ConsumerState(db);
    let fetcher = crate::courier_fetcher(env, Some(tracker.clone())).with_budget(crate::arcade_reorg::ARCADE_LADDER_BUDGET);
    let tx_store = crate::reorg_sweep::D1ProvenTxStore(db);
    let s = crate::arcade_reorg::consume_arcade_reorg_events(
        &feed,
        &headers,
        &state,
        pot_storage,
        Some(&tx_store),
        Some(tracker.as_ref()),
        Some(&fetcher),
        crate::arcade_reorg::PassLimits::default(),
    )
    .await;
    crate::ops::bump_counter(db, crate::ops::COUNTER_ARCADE_REORG_EVENTS, s.events_finished() as u64).await;
    crate::ops::bump_counter(db, crate::ops::COUNTER_ARCADE_REORG_REANCHORED, s.reanchored() as u64).await;
    crate::ops::bump_counter(db, crate::ops::COUNTER_ARCADE_REORG_DEMOTED, s.demoted() as u64).await;
    crate::ops::bump_counter(db, crate::ops::COUNTER_ARCADE_REORG_UNCORROBORATED, s.skipped_uncorroborated as u64).await;
    crate::ops::bump_counter(db, crate::ops::COUNTER_ARCADE_REORG_FAULTS, (s.faults + s.errors) as u64).await;
    // the R2 lifetime totals count the same rows whatever the producer (the
    // sweep, the announce, the operator, or Arcade's event)
    crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_DEMOTED, s.demoted() as u64).await;
    crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_STALE_PROOFS, s.demoted() as u64).await;
    crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_STALE_POT_PROOFS, s.pot_beefs.stale as u64).await;
    crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_STALE_TX_PROOFS, s.transactions.stale as u64).await;
    crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_REANCHORED, s.reanchored() as u64).await;
    crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_TRACKER_FAULTS, (s.spenders.faults + s.pot_beefs.faults + s.transactions.faults) as u64).await;
    console_log!(
        "[arcade-reorg] ({origin}) feed_read={} rows={} malformed={} events_after_cursor={} applied={} skipped_uncorroborated={} held={} \
         spenders scanned={} standing={} reanchored={} demoted={} faults={}; pot_beefs scanned={} stale={}; transactions scanned={} stale={}; \
         faults={} errors={} idle={} cursor={:?} pending={:?} stopped={:?}",
        s.feed_read,
        s.feed_rows,
        s.feed_malformed,
        s.feed_events,
        s.applied,
        s.skipped_uncorroborated,
        s.held,
        s.spenders.scanned,
        s.spenders.standing,
        s.reanchored(),
        s.demoted(),
        s.spenders.faults,
        s.pot_beefs.scanned,
        s.pot_beefs.stale,
        s.transactions.scanned,
        s.transactions.stale,
        s.faults,
        s.errors,
        s.idle,
        s.cursor.as_ref().map(|c| (c.height, &c.hash[..16], c.orphaned_at.as_str())),
        s.pending.as_ref().map(|(k, held)| (k.height, &k.hash[..16], *held)),
        s.stopped
    );
    s
}

/// PURE: the JSON body `POST /internal/arcade-reorg` and the block-event
/// pass answer for the consumer's outcome.
pub fn arcade_reorg_summary_json(s: &crate::arcade_reorg::ArcadePassSummary) -> serde_json::Value {
    let key = |k: &overlay_discovery::pot::arcade_events::EventKey| {
        serde_json::json!({ "orphanedAt": k.orphaned_at, "height": k.height, "hash": k.hash })
    };
    serde_json::json!({
        "feedRead": s.feed_read,
        "feedRows": s.feed_rows,
        "feedMalformed": s.feed_malformed,
        "eventsAfterCursor": s.feed_events,
        "applied": s.applied,
        "skippedUncorroborated": s.skipped_uncorroborated,
        "held": s.held,
        "scanned": s.spenders.scanned + s.pot_beefs.scanned + s.transactions.scanned,
        "standing": s.spenders.standing,
        "reanchored": s.reanchored(),
        "demoted": s.demoted(),
        "stalePotProofs": s.pot_beefs.stale,
        "staleTxProofs": s.transactions.stale,
        "faults": s.faults,
        "errors": s.errors,
        "idle": s.idle,
        "stopped": s.stopped,
        "cursor": s.cursor.as_ref().map(key),
        "pending": s.pending.as_ref().map(|(k, held)| serde_json::json!({ "event": key(k), "heldPasses": held })),
    })
}

/// `POST /internal/arcade-reorg` (bearer `INTERNAL_TOKEN`): one bounded
/// pass of the Arcade reorg-event consumer on demand (the deploy check, an
/// operator catch-up). No body. The same pass the block-event route and the
/// cron run; answers its summary.
pub async fn internal_arcade_reorg(
    req: Request,
    env: &Env,
    ctx: &Context,
    pot_storage: &dyn overlay_discovery::pot::storage::PotStorage,
    ops_db: Option<&D1Database>,
) -> Result<Response> {
    let authorization = req.headers().get("authorization").ok().flatten();
    let secret = env.secret("INTERNAL_TOKEN").ok().map(|s| s.to_string());
    if !bearer_ok(authorization.as_deref(), secret.as_deref()) {
        console_log!("POST /internal/arcade-reorg -> 401");
        return Response::error("unauthorized", 401);
    }
    let s = run_arcade_reorg_pass(env, pot_storage, ops_db, "internal").await;
    // a demotion or a re-anchor is a served-state change: ship the pot-changed webhook
    crate::pot_changes::flush(env, |fut| ctx.wait_until(fut));
    let mut body = arcade_reorg_summary_json(&s);
    body["ok"] = serde_json::Value::Bool(true);
    Response::from_json(&body)
}

/// The reorg detector through the REAL producer path: record the announced
/// `(height, hash)` and classify it against what was held. `None` only on a
/// storage fault (no detection this pass).
pub async fn detect_announce(
    pot_storage: &dyn overlay_discovery::pot::storage::PotStorage,
    height: u64,
    hash: &str,
) -> Option<overlay_discovery::pot::reorg::TipAnnounce> {
    match pot_storage.record_header_seen(height, hash).await {
        Ok(seen) => Some(overlay_discovery::pot::reorg::classify_tip_announce(height, hash, &seen)),
        Err(e) => {
            crate::proof_fetcher::push_log(&format!(
                "POST /internal/tip-changed height={height}: header record failed ({e}) — no reorg detection this pass"
            ));
            None
        }
    }
}

/// `POST /internal/reorg`'s body, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReorgTrigger {
    pub from_height: u64,
    pub to_height: u64,
    pub limit: u64,
    pub after: Option<overlay_discovery::pot::reorg::RowKey>,
}

/// PURE: `{"fromHeight": n, "toHeight"?: m, "limit"?: k, "cursor"?: {"height": h, "rowid": r}}`.
/// `toHeight` defaults to `fromHeight` and may not be below it; `limit` is
/// clamped to [`REORG_DEMOTE_LIMIT`]; `cursor` is the `nextCursor` a previous
/// answer handed back. `None` for anything else.
pub fn parse_reorg_trigger(raw: &[u8]) -> Option<ReorgTrigger> {
    let v: serde_json::Value = serde_json::from_slice(raw).ok()?;
    let from_height = v.get("fromHeight")?.as_u64().filter(|h| *h > 0)?;
    let to_height = match v.get("toHeight") {
        None | Some(serde_json::Value::Null) => from_height,
        Some(t) => t.as_u64().filter(|t| *t >= from_height)?,
    };
    let limit = v
        .get("limit")
        .and_then(serde_json::Value::as_u64)
        .filter(|l| *l > 0)
        .map_or(REORG_DEMOTE_LIMIT, |l| l.min(REORG_DEMOTE_LIMIT));
    let after = match v.get("cursor") {
        None | Some(serde_json::Value::Null) => None,
        Some(c) => Some(overlay_discovery::pot::reorg::RowKey {
            height: c.get("height")?.as_u64()?,
            rowid: c.get("rowid")?.as_i64()?,
        }),
    };
    Some(ReorgTrigger { from_height, to_height, limit, after })
}

/// `POST /internal/reorg` (bearer `INTERNAL_TOKEN`): the operator's
/// EVIDENCE-DRIVEN demotion over a height window (round-2 review M1), for
/// rows a past reorg left behind (the 2026-09-07 rows at 965771 pre-date
/// this build). Every confirmed row anchored in `[fromHeight, toHeight]`
/// has its stored spender proof re-verified against chaintracks: a REFUTED
/// one is demoted (guarded) and unlatched, a proofless one is demoted blind
/// (nothing to verify; the courier arm re-judges it), a canonical one is
/// untouched and counted. Bounded per call; `drained` says the window is
/// exhausted, else pass `nextCursor` back as `cursor`. No header source:
/// 503, nothing demoted.
///
/// The heal for the event: `{"fromHeight": 965771, "toHeight": 965771}`.
pub async fn internal_reorg(
    mut req: Request,
    env: &Env,
    ctx: &Context,
    pot_storage: &dyn overlay_discovery::pot::storage::PotStorage,
    ops_db: Option<&D1Database>,
) -> Result<Response> {
    let authorization = req.headers().get("authorization").ok().flatten();
    let secret = env.secret("INTERNAL_TOKEN").ok().map(|s| s.to_string());
    if !bearer_ok(authorization.as_deref(), secret.as_deref()) {
        console_log!("POST /internal/reorg -> 401");
        return Response::error("unauthorized", 401);
    }
    let raw = req.bytes().await?;
    let Some(trigger) = parse_reorg_trigger(&raw) else {
        return Response::error(
            "body must be {\"fromHeight\": <positive integer>, \"toHeight\"?: <integer >= fromHeight>, \"limit\"?: <positive integer>, \"cursor\"?: {\"height\", \"rowid\"}}",
            400,
        );
    };
    let tracker = crate::lookup_service_chain_tracker(env);
    if tracker.is_none() {
        console_log!("POST /internal/reorg -> 503 (no header source configured; nothing demoted)");
        let resp = Response::from_json(&serde_json::json!({
            "ok": false,
            "error": "no header source configured; nothing demoted",
        }))?;
        return Ok(resp.with_status(503));
    }
    let pass = crate::reorg_sweep::handle_reorg(
        pot_storage,
        tracker.as_deref(),
        trigger.from_height,
        trigger.to_height,
        trigger.after,
        trigger.limit,
    )
    .await;
    if let Some(db) = ops_db {
        // review L2: the OPERATOR's manual heal is counted apart from
        // `chain_reorgs_detected_total`, so "zero detected on a healthy
        // stream" stays a true invariant.
        crate::ops::bump_counter(db, crate::ops::COUNTER_OPERATOR_REORG, 1).await;
        crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_DEMOTED, (pass.stale + pass.demoted_blind) as u64).await;
        crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_TRACKER_FAULTS, pass.faults as u64).await;
    }
    console_log!(
        "POST /internal/reorg {}..={} limit={} -> 200 (scanned={} standing={} demoted={} demoted_blind={} demote_missed={} faults={} errors={} drained={})",
        trigger.from_height,
        trigger.to_height,
        trigger.limit,
        pass.scanned,
        pass.standing,
        pass.stale,
        pass.demoted_blind,
        pass.demote_missed,
        pass.faults,
        pass.errors,
        pass.exhausted
    );
    // a demotion is a served-state change: ship the pot-changed webhook
    crate::pot_changes::flush(env, |fut| ctx.wait_until(fut));
    Response::from_json(&serde_json::json!({
        "ok": true,
        "fromHeight": trigger.from_height,
        "toHeight": trigger.to_height,
        "limit": trigger.limit,
        "scanned": pass.scanned,
        "standing": pass.standing,
        "demoted": pass.stale,
        "demotedBlind": pass.demoted_blind,
        "demoteMissed": pass.demote_missed,
        "faults": pass.faults,
        "errors": pass.errors,
        "drained": pass.exhausted,
        "nextCursor": if pass.exhausted { serde_json::Value::Null } else {
            pass.next_cursor.map_or(serde_json::Value::Null, |c| serde_json::json!({ "height": c.height, "rowid": c.rowid }))
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tip_changed_accepts_a_positive_height_only() {
        assert_eq!(parse_tip_changed(br#"{"height": 965702}"#), Some(965702));
        assert_eq!(parse_tip_changed(br#"{"height": 0}"#), None);
        assert_eq!(parse_tip_changed(br#"{"height": -1}"#), None);
        assert_eq!(parse_tip_changed(br#"{"height": "965702"}"#), None);
        assert_eq!(parse_tip_changed(br#"{}"#), None);
        assert_eq!(parse_tip_changed(b"nope"), None);
    }

    #[test]
    fn parse_tip_changed_hash_is_a_lowercased_64_hex_or_nothing() {
        let h = "00000000000000001DE5AA96BAA3566CE66E4941F8295CC44CC85FC75949DB4D";
        assert_eq!(
            parse_tip_changed_hash(format!(r#"{{"height": 965771, "hash": "{h}"}}"#).as_bytes()).as_deref(),
            Some(h.to_ascii_lowercase().as_str())
        );
        // an older app-layer forwards the height alone: no hash, no detection
        assert_eq!(parse_tip_changed_hash(br#"{"height": 965771}"#), None);
        assert_eq!(parse_tip_changed_hash(br#"{"height": 965771, "hash": "abc"}"#), None);
        assert_eq!(parse_tip_changed_hash(br#"{"height": 965771, "hash": 12}"#), None);
        assert_eq!(parse_tip_changed_hash(b"nope"), None);
        // the height parser is untouched by the hash's presence
        assert_eq!(parse_tip_changed(format!(r#"{{"height": 965771, "hash": "{h}"}}"#).as_bytes()), Some(965771));
    }

    #[test]
    fn bearer_is_exact_and_a_missing_secret_refuses_everything() {
        assert!(bearer_ok(Some("Bearer s3cret"), Some("s3cret")));
        assert!(bearer_ok(Some("Bearer  s3cret "), Some("s3cret")));
        assert!(!bearer_ok(Some("Bearer other"), Some("s3cret")));
        assert!(!bearer_ok(Some("s3cret"), Some("s3cret")));
        assert!(!bearer_ok(None, Some("s3cret")));
        assert!(!bearer_ok(Some("Bearer "), Some("")));
        assert!(!bearer_ok(Some("Bearer s3cret"), None));
    }

    #[test]
    fn a_height_runs_one_pass_per_isolate_and_only_forward() {
        // a fresh, high height (never used by another test)
        let h = 9_000_000 + u64::from(std::process::id() % 1000);
        assert!(first_pass_for(h), "first announcement runs the pass");
        assert!(!first_pass_for(h), "a repeat of the same tip is not a new block");
        assert!(!first_pass_for(h - 1), "an older tip is not a new block");
        assert!(first_pass_for(h + 1), "the next block runs again");
    }

    #[test]
    fn reorg_trigger_body_is_a_height_window_with_a_clamped_limit_and_a_cursor() {
        let t = |raw: &str| parse_reorg_trigger(raw.as_bytes());
        assert_eq!(
            t(r#"{"fromHeight":965771}"#),
            Some(ReorgTrigger { from_height: 965771, to_height: 965771, limit: REORG_DEMOTE_LIMIT, after: None }),
            "toHeight defaults to fromHeight: the event's heal is one height"
        );
        assert_eq!(
            t(r#"{"fromHeight":965771,"toHeight":965773,"limit":50,"cursor":{"height":965772,"rowid":40}}"#),
            Some(ReorgTrigger {
                from_height: 965771,
                to_height: 965773,
                limit: 50,
                after: Some(overlay_discovery::pot::reorg::RowKey { height: 965772, rowid: 40 }),
            })
        );
        assert_eq!(t(r#"{"fromHeight":965771,"limit":100000}"#).unwrap().limit, REORG_DEMOTE_LIMIT, "the per-call bound holds");
        assert_eq!(t(r#"{"fromHeight":965771,"limit":0}"#).unwrap().limit, REORG_DEMOTE_LIMIT);
        assert_eq!(t(r#"{"fromHeight":965771,"toHeight":null,"cursor":null}"#).unwrap().to_height, 965771);
        assert_eq!(t(r#"{"fromHeight":965771,"toHeight":965770}"#), None, "a window cannot end below its start");
        assert_eq!(t(r#"{"fromHeight":965771,"cursor":{"height":1}}"#), None, "a cursor needs both keys");
        assert_eq!(t(r#"{"fromHeight":0}"#), None);
        assert_eq!(t(r#"{"fromHeight":-1}"#), None);
        assert_eq!(t(r#"{"height":965771}"#), None);
        assert_eq!(t("nope"), None);
    }

    /// bsv-low M19B-G1: the pass summary the block-event answer and
    /// `POST /internal/arcade-reorg` carry, shaped from the summary.
    #[test]
    fn arcade_reorg_summary_json_carries_the_counts_the_cursor_and_the_pending_event() {
        use overlay_discovery::pot::arcade_events::EventKey;
        let key = EventKey { orphaned_at: "2026-09-07T22:45:22.316Z".into(), height: 965771, hash: "cd".repeat(32) };
        let mut s = crate::arcade_reorg::ArcadePassSummary { applied: 1, skipped_uncorroborated: 1, held: 0, cursor: Some(key.clone()), ..Default::default() };
        s.spenders.scanned = 3;
        s.spenders.standing = 1;
        s.spenders.stale = 1;
        s.spenders.reanchored_from_courier = 1;
        s.pot_beefs.stale = 2;
        s.pending = Some((key.clone(), 2));
        s.stopped = Some("event cd…@965771 continues next pass (a leg is not exhausted)".into());
        let v = arcade_reorg_summary_json(&s);
        assert_eq!(v["applied"], 1);
        assert_eq!(v["skippedUncorroborated"], 1);
        assert_eq!(v["scanned"], 3);
        assert_eq!(v["standing"], 1);
        assert_eq!(v["reanchored"], 1);
        assert_eq!(v["demoted"], 1);
        assert_eq!(v["stalePotProofs"], 2);
        assert_eq!(v["cursor"]["height"], 965771);
        assert_eq!(v["cursor"]["orphanedAt"], "2026-09-07T22:45:22.316Z");
        assert_eq!(v["pending"]["heldPasses"], 2);
        assert_eq!(v["pending"]["event"]["hash"], "cd".repeat(32));
        assert!(v["stopped"].as_str().unwrap().contains("continues next pass"));
        let idle = arcade_reorg_summary_json(&crate::arcade_reorg::ArcadePassSummary { idle: true, ..Default::default() });
        assert_eq!(idle["idle"], true);
        assert!(idle["cursor"].is_null() && idle["pending"].is_null() && idle["stopped"].is_null());
    }

    /// Review H3: the detector's pins run through the REAL producer path,
    /// the parsed webhook body (`{"height", "hash"}`), the per-height record
    /// and the classification, on the 2026-09-07 sequence.
    #[tokio::test]
    async fn the_detector_runs_through_the_parsed_webhook_body() {
        use overlay_discovery::pot::reorg::TipAnnounce;
        use overlay_discovery::pot::storage::MemoryPotStorage;
        const ORPHAN: &str = "0000000000000000153E10F465DBA9697E4BDE364FDF3A3224A736B019FFBFB1";
        const CANON: &str = "00000000000000001de5aa96baa3566ce66e4941f8295cc44cc85fc75949db4d";
        const NEXT: &str = "0000000000000000014d2556f2b0a0f1c1a0b9b8e7f6d5c4b3a2918070605040";
        let store = MemoryPotStorage::new();
        let feed = |raw: String| {
            let store = &store;
            async move {
                let height = parse_tip_changed(raw.as_bytes()).expect("a height");
                let hash = parse_tip_changed_hash(raw.as_bytes());
                match hash {
                    Some(h) => detect_announce(store, height, &h).await,
                    None => None,
                }
            }
        };
        // 22:39:20Z the 34 MB block at 965771 (the orphan) announced
        assert_eq!(feed(format!(r#"{{"height":965771,"hash":"{ORPHAN}"}}"#)).await, Some(TipAnnounce::Extends));
        // the canonical 965771 announced at the same height: THE reorg producer
        assert_eq!(feed(format!(r#"{{"height":965771,"hash":"{CANON}"}}"#)).await, Some(TipAnnounce::Reorg { from: 965771 }));
        // the same announce again (another isolate, a retry): a repeat
        assert_eq!(feed(format!(r#"{{"height":965771,"hash":"{CANON}"}}"#)).await, Some(TipAnnounce::Repeat));
        // the chain extends
        assert_eq!(feed(format!(r#"{{"height":965772,"hash":"{NEXT}"}}"#)).await, Some(TipAnnounce::Extends));
        // two webhook tasks landing out of order: 965770 after 965772 is an OLD header, never a reorg
        assert_eq!(feed(format!(r#"{{"height":965770,"hash":"{}"}}"#, "ab".repeat(32))).await, Some(TipAnnounce::Old));
        // an older announcer's hash-less body: nothing to compare, no detection
        assert_eq!(feed(r#"{"height":965773}"#.to_string()).await, None);
    }
}
