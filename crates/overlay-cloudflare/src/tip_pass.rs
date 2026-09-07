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
    if !first_pass_for(height) {
        console_log!("POST /internal/tip-changed height={height} -> 200 (already passed in this isolate)");
        return Response::from_json(&serde_json::json!({ "ok": true, "height": height, "skipped": "already-passed" }));
    }
    let fetcher = crate::courier_fetcher(env, crate::lookup_service_chain_tracker(env)).with_budget(TIP_PASS_BUDGET);
    // min_age 0: the block just landed; every unconfirmed spend is a candidate
    // (the push may still arrive — its CAS then finds the row confirmed, harmless)
    let s = crate::proof_fetcher::complete_spend_confirmations(pot_storage, &fetcher, TIP_PASS_LIMIT, 0).await;
    console_log!(
        "POST /internal/tip-changed height={height} -> 200 (block-event spend-confirmation: scanned={} confirmed={} \
         still_unconfirmed={} fetch_failed={} tracker_faults={} cas_missed={} cas_errors={})",
        s.scanned,
        s.confirmed,
        s.still_unconfirmed,
        s.fetch_failed,
        s.tracker_faults,
        s.cas_missed,
        s.cas_errors
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
