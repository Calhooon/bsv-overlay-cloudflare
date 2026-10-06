//! bsv-low #484 (the 2026-09-19 gate's M1, hardening): THE INVALIDATION OF THE HOP PROBE MEMOS.
//!
//! The app layer memoises the couriers' spend probes in `hop_chain_probes` (migration 150; one row per outpoint,
//! the last KNOWN answer and when it was read). A memo that recorded a CONFIRMED spend answers the owed walk for
//! two hours (`low-app-layer` `PROBE_MEMO_CONFIRMED_MAX_AGE_MS`), and `swept_home` reads the same word: a swept
//! hop's Payout row is `claimable` on it. Nothing invalidated the table: no TTL, no DELETE, and neither reorg
//! judge touched it. After a reorg orphaned a mined hop sweep the owed list said "ready to collect" until the
//! memo aged out: a wrong word and a failing press (the press needs a landing proof: no credit), never lost sats.
//!
//! Two rules, both run by the overlay (the reorg judge, on the shared `OVERLAY_DB`):
//!
//! 1. **A reorg clears every CONFIRMED memo.** The memo carries no height (the couriers' probe answers
//!    spent / spender / confirmed, never a block), and the sweep it names is usually a transaction the index does
//!    not hold (the chain rung is asked exactly BECAUSE the index says unspent), so no reorg pass can match a memo
//!    to the rows it judges. What a pass does know is that a reorg HAPPENED: it re-judged a row (a demotion, a
//!    re-anchor, a refuted proof), an announce named a reorg range, or an Arcade orphan event was applied. On that
//!    evidence every memo with `spentConfirmed = 1` is deleted (the superset of the memos at the orphaned
//!    heights) and the next owed walk re-probes them, eight per recompute. A pass that scanned and left every row
//!    standing is NOT evidence: the routine sweep runs every block, and clearing on it would unmake the memo.
//!    An unconfirmed spend and an unspent word keep their own five minutes.
//! 2. **The TTL the contract states.** No reader honours a memo older than the confirmed window, so a row past
//!    it is dead weight: every block-event pass deletes them ([`HOP_PROBE_MEMO_TTL_MS`], pinned equal to the app
//!    layer's longest window from its tests, which link this crate).
//!
//! Residual, stated: a courier that has not yet seen the reorg can answer "confirmed" again right after the
//! clear and the memo is re-written for its window. The memo's age rides the row (`facts.chainProbeAgeMs`).
use worker::wasm_bindgen::JsValue;
use worker::{console_log, D1Database};

use crate::reorg_sweep::{ProofLegSummary, ReorgSweepSummary, ReverifyPassSummary};

/// The longest window any reader passes for a memo (the app layer's `PROBE_MEMO_CONFIRMED_MAX_AGE_MS`): two hours.
pub const HOP_PROBE_MEMO_TTL_MS: i64 = 2 * 60 * 60_000;

/// Rule 1: every memo that recorded a CONFIRMED spend.
pub const HOP_PROBE_MEMO_REORG_CLEAR_SQL: &str = "DELETE FROM hop_chain_probes WHERE spentConfirmed = 1";

/// Rule 2: every memo read before the bound (bind: `now - HOP_PROBE_MEMO_TTL_MS`).
pub const HOP_PROBE_MEMO_EXPIRE_SQL: &str = "DELETE FROM hop_chain_probes WHERE probedAtMs < ?";

/// Confirmed memos a reorg pass cleared (served on `/health/invariants.arcadeReorg.probeMemosCleared` and with
/// the counters).
pub const COUNTER_HOP_PROBE_MEMOS_CLEARED: &str = "hop_probe_memos_cleared_by_reorg_total";
/// Memos the TTL sweep deleted.
pub const COUNTER_HOP_PROBE_MEMOS_EXPIRED: &str = "hop_probe_memos_expired_total";

/// PURE: the spenders leg re-judged a row (demoted it, blind or on a refuted proof, or moved its anchor).
pub fn reverify_rejudged(s: &ReverifyPassSummary) -> bool {
    s.stale + s.demoted_blind + s.reanchored + s.reanchored_from_courier > 0
}

/// PURE: a proof leg (the pots' own proofs, the engine's hop proofs) found a refuted proof.
pub fn proof_leg_rejudged(s: &ProofLegSummary) -> bool {
    s.stale > 0
}

/// PURE: the routine R2 sweep re-judged something on any of its three legs.
pub fn sweep_rejudged(s: &ReorgSweepSummary) -> bool {
    reverify_rejudged(&s.spenders) || proof_leg_rejudged(&s.pot_beefs) || proof_leg_rejudged(&s.transactions)
}

/// PURE: the D7 Arcade pass has reorg evidence: an orphan event applied (chaintracks corroborated it, whether
/// or not a row of ours sat at its height), an event released unresolved (a reorg we could not judge: the
/// conservative side), or any row re-judged on the way. Never an event chaintracks refused to corroborate, nor
/// one still held.
pub fn arcade_pass_rejudged(s: &crate::arcade_reorg::ArcadePassSummary) -> bool {
    s.applied + s.skipped_unresolved + s.released_by_operator > 0
        || reverify_rejudged(&s.spenders)
        || proof_leg_rejudged(&s.pot_beefs)
        || proof_leg_rejudged(&s.transactions)
}

async fn delete(db: &D1Database, sql: &str, binds: &[JsValue]) -> Result<u64, String> {
    let stmt = db.prepare(sql);
    let stmt = if binds.is_empty() {
        stmt
    } else {
        stmt.bind(binds).map_err(|e| e.to_string())?
    };
    let res = stmt.run().await.map_err(|e| e.to_string())?;
    Ok(res
        .meta()
        .ok()
        .flatten()
        .and_then(|m| m.changes)
        .unwrap_or(0) as u64)
}

/// Rule 1, run by a reorg pass with its outcome in hand: no evidence, no write. Logged and counted; a fault is
/// logged and costs the memo's own window, nothing else. Returns the memos cleared.
pub async fn clear_on_reorg(db: &D1Database, origin: &str, evidence: bool) -> u64 {
    if !evidence {
        return 0;
    }
    match delete(db, HOP_PROBE_MEMO_REORG_CLEAR_SQL, &[]).await {
        Ok(cleared) => {
            console_log!("[hop-probe-memos] ({origin}) reorg evidence: cleared {cleared} confirmed memo(s); the next owed walk re-probes them");
            crate::ops::bump_counter(db, COUNTER_HOP_PROBE_MEMOS_CLEARED, cleared).await;
            cleared
        }
        Err(e) => {
            console_log!("[hop-probe-memos] ({origin}) reorg clear FAILED (the confirmed memos stand for their window): {e}");
            0
        }
    }
}

/// Rule 2, on every block-event pass. Returns the memos deleted.
pub async fn expire(db: &D1Database, now_ms: i64) -> u64 {
    let bound = (now_ms - HOP_PROBE_MEMO_TTL_MS) as f64;
    match delete(db, HOP_PROBE_MEMO_EXPIRE_SQL, &[JsValue::from_f64(bound)]).await {
        Ok(expired) => {
            if expired > 0 {
                console_log!("[hop-probe-memos] ttl: deleted {expired} memo(s) older than {HOP_PROBE_MEMO_TTL_MS} ms");
                crate::ops::bump_counter(db, COUNTER_HOP_PROBE_MEMOS_EXPIRED, expired).await;
            }
            expired
        }
        Err(e) => {
            console_log!("[hop-probe-memos] ttl sweep failed: {e}");
            0
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        for sql in crate::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                let msg = e.to_string().to_ascii_lowercase();
                assert!(msg.contains("duplicate column"), "production migration failed under real SQLite: {e}\n{sql}");
            }
        }
        conn
    }

    fn plant(conn: &rusqlite::Connection, outpoint: &str, at_ms: i64, spent: bool, confirmed: Option<bool>) {
        conn.execute(
            "INSERT INTO hop_chain_probes (outpoint, probedAtMs, spent, spendingTxid, spentConfirmed) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![outpoint, at_ms, spent, spent.then(|| "e5".repeat(32)), confirmed],
        )
        .unwrap();
    }

    fn held(conn: &rusqlite::Connection) -> Vec<String> {
        let mut stmt = conn.prepare("SELECT outpoint FROM hop_chain_probes ORDER BY outpoint").unwrap();
        let rows = stmt.query_map([], |r| r.get::<_, String>(0)).unwrap();
        rows.map(|r| r.unwrap()).collect()
    }

    /// bsv-low #484, the clearing SQL over the SHIPPED schema (real SQLite): a reorg clears every memo that
    /// recorded a CONFIRMED spend (the only word a reorg can make wrong for longer than five minutes, and the
    /// one `swept_home` turns into `claimable`); an unconfirmed spend and an unspent word keep their five
    /// minutes. To red: narrow the WHERE off `spentConfirmed = 1`, or drop the DELETE.
    #[test]
    fn a_reorg_clears_every_confirmed_memo_and_leaves_the_rest_real_sqlite() {
        let conn = db();
        plant(&conn, "a.0", 1_000, true, Some(true));
        plant(&conn, "b.0", 1_000, true, Some(false));
        plant(&conn, "c.0", 1_000, true, None);
        plant(&conn, "d.0", 1_000, false, None);
        plant(&conn, "e.1", 9_000, true, Some(true));
        let cleared = conn.execute(HOP_PROBE_MEMO_REORG_CLEAR_SQL, []).unwrap();
        assert_eq!(cleared, 2, "both confirmed memos, whatever their age");
        assert_eq!(held(&conn), vec!["b.0", "c.0", "d.0"]);
        assert_eq!(conn.execute(HOP_PROBE_MEMO_REORG_CLEAR_SQL, []).unwrap(), 0, "idempotent");
    }

    /// bsv-low #484, the TTL the memo's contract states: no reader honours a memo older than the confirmed
    /// window (two hours), so the row is dead weight past it and the sweep deletes it. To red: drop the DELETE
    /// or move the bound.
    #[test]
    fn the_ttl_sweep_deletes_exactly_the_memos_no_reader_honours_real_sqlite() {
        assert_eq!(HOP_PROBE_MEMO_TTL_MS, 2 * 60 * 60_000, "the longest window a reader passes (the confirmed one)");
        let conn = db();
        let now: i64 = 1_800_000_000_000;
        plant(&conn, "old.0", now - HOP_PROBE_MEMO_TTL_MS - 1, true, Some(true));
        plant(&conn, "edge.0", now - HOP_PROBE_MEMO_TTL_MS, true, Some(true));
        plant(&conn, "young.0", now - 60_000, false, None);
        plant(&conn, "ancient.0", 5, false, None);
        let expired = conn.execute(HOP_PROBE_MEMO_EXPIRE_SQL, rusqlite::params![now - HOP_PROBE_MEMO_TTL_MS]).unwrap();
        assert_eq!(expired, 2);
        assert_eq!(held(&conn), vec!["edge.0", "young.0"]);
    }

    /// bsv-low #484: what counts as a reorg pass having RE-JUDGED something. A pass that scanned and left every
    /// row standing is not evidence (the routine sweep runs every block: clearing on it would unmake the memo);
    /// a demotion, a re-anchor or a refuted proof is; so is an Arcade orphan event applied or released
    /// unresolved (never one chaintracks refused to corroborate, nor one still held).
    #[test]
    fn only_a_pass_that_rejudged_something_is_reorg_evidence() {
        let quiet = ReverifyPassSummary { scanned: 40, standing: 40, stored_from_courier: 2, ..Default::default() };
        assert!(!reverify_rejudged(&quiet));
        for s in [
            ReverifyPassSummary { stale: 1, ..Default::default() },
            ReverifyPassSummary { demoted_blind: 1, ..Default::default() },
            ReverifyPassSummary { reanchored: 1, ..Default::default() },
            ReverifyPassSummary { reanchored_from_courier: 1, ..Default::default() },
        ] {
            assert!(reverify_rejudged(&s), "{s:?}");
        }
        assert!(!proof_leg_rejudged(&ProofLegSummary { scanned: 9, standing: 9, ..Default::default() }));
        assert!(proof_leg_rejudged(&ProofLegSummary { stale: 1, ..Default::default() }));
        let mut sweep = ReorgSweepSummary::default();
        assert!(!sweep_rejudged(&sweep));
        sweep.transactions.stale = 1;
        assert!(sweep_rejudged(&sweep), "the hop proofs' leg counts");
        let mut sweep = ReorgSweepSummary::default();
        sweep.pot_beefs.stale = 1;
        assert!(sweep_rejudged(&sweep));
        let mut sweep = ReorgSweepSummary::default();
        sweep.spenders.reanchored = 1;
        assert!(sweep_rejudged(&sweep));
        use crate::arcade_reorg::ArcadePassSummary;
        assert!(!arcade_pass_rejudged(&ArcadePassSummary { idle: true, ..Default::default() }));
        assert!(!arcade_pass_rejudged(&ArcadePassSummary { skipped_uncorroborated: 1, held: 1, tracker_lagging: 1, ..Default::default() }));
        assert!(arcade_pass_rejudged(&ArcadePassSummary { applied: 1, ..Default::default() }), "an orphan event applied, even with no row of ours at its height");
        assert!(arcade_pass_rejudged(&ArcadePassSummary { skipped_unresolved: 1, ..Default::default() }));
        assert!(arcade_pass_rejudged(&ArcadePassSummary { released_by_operator: 1, ..Default::default() }));
        let mut partial = ArcadePassSummary::default();
        partial.spenders.stale = 1;
        assert!(arcade_pass_rejudged(&partial), "a row demoted by an event still pending");
    }

    /// `/health/invariants.arcadeReorg` counts the memos cleared (the issue's acceptance): the lifetime counter
    /// rides the view, 0 before the first clear; and the invariants body is built through it.
    #[test]
    fn the_reorg_view_counts_the_memos_cleared() {
        let view = serde_json::json!({ "readable": true, "everRan": true });
        let none = crate::ops::with_probe_memos_cleared(view.clone(), &serde_json::json!({}));
        assert_eq!(none["probeMemosCleared"], 0);
        let some = crate::ops::with_probe_memos_cleared(view, &serde_json::json!({ COUNTER_HOP_PROBE_MEMOS_CLEARED: 7 }));
        assert_eq!(some["probeMemosCleared"], 7);
        assert_eq!(some["everRan"], true, "the view's own keys stand");
        let ops = include_str!("ops.rs");
        assert!(ops.contains("let arcade_reorg = with_probe_memos_cleared(arcade_reorg_view(db).await, &counters);"));
    }

    /// SOURCE PIN (bsv-low #484): BOTH reorg judges invalidate, each where it has its outcome in hand: the D7
    /// Arcade pass inside `run_arcade_reorg_pass` (so the block-event pass, the cron and the on-demand route all
    /// carry it), the R2 legs in `internal_tip_changed` (the announced reorg, the targeted demotion, the routine
    /// sweep) and `internal_reorg` (the operator's window); the TTL sweep rides every block-event pass.
    /// To red: drop any one call.
    #[test]
    fn both_reorg_judges_invalidate_and_the_ttl_rides_the_block_event_pass() {
        let code_only = |s: &str| s.lines().map(|l| l.split("//").next().unwrap_or("")).collect::<Vec<_>>().join("\n");
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let src = include_str!("tip_pass.rs");
        let src = code_only(&src[..src.find("#[cfg(test)]").unwrap_or(src.len())]);
        let body = |name: &str| -> String {
            let start = src.find(&format!("pub async fn {name}(")).unwrap_or_else(|| panic!("{name}"));
            let rest = &src[start + 1..];
            let end = rest.find("\npub async fn ").or_else(|| rest.find("\npub fn ")).unwrap_or(rest.len());
            squash(&rest[..end.min(rest.find("\npub fn ").unwrap_or(rest.len()))])
        };
        let arcade = body("run_arcade_reorg_pass");
        assert!(arcade.contains(&squash("crate::hop_probe_memos::clear_on_reorg(db, origin, crate::hop_probe_memos::arcade_pass_rejudged(&s)).await")), "the D7 pass");
        let tip = body("internal_tip_changed");
        assert!(
            tip.contains(&squash("reorg_from.is_some() || crate::hop_probe_memos::reverify_rejudged(&demotion) || crate::hop_probe_memos::sweep_rejudged(&sweep)")),
            "the R2 legs of the block-event pass"
        );
        assert!(tip.contains(&squash("crate::hop_probe_memos::expire(db, worker::Date::now().as_millis() as i64).await")), "the TTL sweep");
        let operator = body("internal_reorg");
        assert!(operator.contains(&squash("crate::hop_probe_memos::clear_on_reorg(db, \"operator\", crate::hop_probe_memos::reverify_rejudged(&pass)).await")), "the operator's window");
    }
}
