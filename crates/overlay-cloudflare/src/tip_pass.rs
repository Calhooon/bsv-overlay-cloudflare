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

/// Rows a detected reorg demotes per block-event pass (a deep reorg drains
/// over a few passes; the sweep below and the cron cover the rest).
pub const REORG_DEMOTE_LIMIT: u64 = 200;
/// The revalidation sweep's window: the last N heights (the reference's
/// `reorgScanDepth` default).
pub const REORG_SWEEP_DEPTH: u64 = 3;
/// Confirmed rows the sweep re-verifies per pass (bounded D1 reads).
pub const REORG_SWEEP_LIMIT: u64 = 50;

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
    // ── bsv-low M19 R2: the reorg detector (reference: Engine.handleReorg) ──
    // Record the block this pass acts on; a hash change at a held height, or
    // a lower announce whose hash we do not hold, names the height every
    // confirmation above it rests on. Durable (D1), so it sees across
    // isolates; a height-less body (an older app-layer) detects nothing.
    let hash = parse_tip_changed_hash(&raw);
    let mut reorg_from: Option<u64> = None;
    if let Some(h) = hash.as_deref() {
        match pot_storage.record_header_seen(height, h).await {
            Ok(seen) => {
                reorg_from = overlay_discovery::pot::reorg::reorg_from_height(height, h, &seen);
            }
            Err(e) => console_log!("POST /internal/tip-changed height={height}: header record failed ({e}) — no reorg detection this pass"),
        }
    }
    let mut demotion = crate::proof_fetcher::ReorgDemotionSummary::default();
    if let Some(from) = reorg_from {
        console_log!(
            "POST /internal/tip-changed height={height} hash={}: REORG detected — confirmations at or above {from} rest on a block the chain no longer holds",
            hash.as_deref().unwrap_or("?")
        );
        demotion = crate::proof_fetcher::handle_reorg(pot_storage, from, REORG_DEMOTE_LIMIT).await;
        if let Some(db) = ops_db {
            crate::ops::bump_counter(db, crate::ops::COUNTER_CHAIN_REORGS_DETECTED, 1).await;
            if demotion.demoted > 0 {
                crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_DEMOTED, demotion.demoted as u64).await;
            }
        }
    }
    // A repeat of an already-passed height runs nothing — unless it carried
    // a reorg, which must still sweep and re-chase.
    if !first_pass_for(height) && reorg_from.is_none() {
        console_log!("POST /internal/tip-changed height={height} -> 200 (already passed in this isolate)");
        return Response::from_json(&serde_json::json!({ "ok": true, "height": height, "skipped": "already-passed" }));
    }
    let tracker = crate::lookup_service_chain_tracker(env);
    // ── the revalidation sweep (reference: the fallback for trackers without
    // a reorg stream; here the PRIMARY path, every block). It runs BEFORE the
    // confirmation pass so a row it demotes is re-chased in this same tick
    // with a proof verified against the canonical chain.
    let sweep = crate::proof_fetcher::reorg_revalidation_sweep(
        pot_storage,
        tracker.as_deref(),
        height,
        REORG_SWEEP_DEPTH,
        REORG_SWEEP_LIMIT,
    )
    .await;
    if let Some(db) = ops_db {
        if sweep.scanned > 0 {
            crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_REVERIFIED, sweep.scanned as u64).await;
        }
        if sweep.stale > 0 {
            crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_STALE_PROOFS, sweep.stale as u64).await;
            crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_DEMOTED, sweep.stale as u64).await;
        }
        if sweep.faults > 0 {
            crate::ops::bump_counter(db, crate::ops::COUNTER_REORG_TRACKER_FAULTS, sweep.faults as u64).await;
        }
    }
    let fetcher = crate::courier_fetcher(env, tracker).with_budget(TIP_PASS_BUDGET);
    // min_age 0: the block just landed; every unconfirmed spend is a candidate
    // (the push may still arrive — its CAS then finds the row confirmed, harmless)
    let s = crate::proof_fetcher::complete_spend_confirmations(pot_storage, &fetcher, TIP_PASS_LIMIT, 0).await;
    console_log!(
        "POST /internal/tip-changed height={height} -> 200 (block-event spend-confirmation: scanned={} confirmed={} \
         still_unconfirmed={} fetch_failed={} tracker_faults={} cas_missed={} cas_errors={}; \
         reorg: from={:?} demoted={} unlatched={}; sweep: scanned={} standing={} stale={} faults={} no_stored_proof={} demote_missed={} errors={})",
        s.scanned,
        s.confirmed,
        s.still_unconfirmed,
        s.fetch_failed,
        s.tracker_faults,
        s.cas_missed,
        s.cas_errors,
        reorg_from,
        demotion.demoted,
        demotion.unlatched,
        sweep.scanned,
        sweep.standing,
        sweep.stale,
        sweep.faults,
        sweep.no_stored_proof,
        sweep.demote_missed,
        sweep.errors
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
        "reorgDemoted": demotion.demoted,
        "sweepScanned": sweep.scanned,
        "sweepStale": sweep.stale,
        "sweepFaults": sweep.faults,
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
}
