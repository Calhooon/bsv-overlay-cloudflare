//! LOW lobby-advert lifecycle maintenance (bsv-low #309).
//!
//! LOW's lobby adverts are 500-sat PushDrop tokens admitted on `tm_low` and
//! indexed by `ls_low` into `low_records`. Deletion happens ONLY via the
//! lookup service's `output_spent` hook — which fires ONLY when the closing
//! tx is SUBMITTED to `tm_low`. A close that reached the network via the
//! client's direct-ARC fallback never evicts the row (the #1 orphan
//! generator, per the bsv-low#256 incident record), and expired-but-unspent
//! rows accumulate forever (`find_open_tables` filters them at query time,
//! bsv-low#148, but nothing ever deletes them; `byGameId`/`byHost` apply NO
//! expiry filter, so the orphans keep surfacing there).
//!
//! Two bounded passes, run from the `*/15` cron AND poke-able via the
//! bearer-authed `POST /admin/advert-lifecycle` (cron completion has
//! historically been carried by the external admin poker — bsv-low#257):
//!
//! 1. [`reap_expired_adverts`] — delete TABLE rows whose `expiryHeight` is
//!    at least [`ADVERT_REAP_MARGIN_BLOCKS`] blocks below a RESOLVED chain
//!    tip. FAIL-CLOSED on the tip: a null tip reaps NOTHING (the #148
//!    lesson — the expiry filter was once a silent no-op in prod; a
//!    DESTRUCTIVE pass must refuse without evidence, the opposite fail
//!    direction of the display filter's fail-open).
//! 2. [`confirm_advert_spends`] — probe a bounded RANDOM batch of TABLE
//!    rows' advert OUTPOINTS for an on-chain spend and, on a PROVEN spend,
//!    run the SAME deletion `output_spent` runs
//!    ([`LowStorage::delete_record`]). UNKNOWN/fault → left standing (a
//!    flake never deletes).
//!
//! ## why the spend probe is WoC's outpoint-spent endpoint
//!
//! The pot spend-confirmation chaser (`complete_spend_confirmations`)
//! cannot be mirrored directly: its `ChainProofFetcher`/chaintracks path
//! answers "did KNOWN txid X mine?" — it cannot see arbitrary OUTPOINT
//! spends, and `low_records` rows record no spending txid (the closing tx
//! never came through us; that is the whole problem). The closest honest
//! alternative on the services this worker already uses (WoC + Bitails,
//! the #273 rebroadcast-backstop / courier-ladder hosts — no new provider)
//! is the `/spent-any` doctrine already shipped server-side in this repo
//! (`low-app-layer::routes::spent_any_resolve`, bsv-low#227): WoC's
//! `GET /tx/{txid}/{vout}/spent` pointer is accepted ONLY after RAW
//! VERIFICATION — the spender's raw bytes are fetched (Bitails first, WoC
//! break-glass, THIS worker's courier order), hash-checked against the
//! reported txid, and input-matched to the advert outpoint. A pointer that
//! fails verification is a provider fault → the row stands. One provider's
//! word alone never destroys a row; a raw-verified spend is chain truth
//! (the spender's bytes themselves prove the outpoint is consumed).
//!
//! WoC on a background pass: bounded at [`ADVERT_SPEND_CHECK_LIMIT`]
//! candidates × ≤3 GETs per tick — the same cold-path posture as the #273
//! backstop's WoC presence probes (the 429 doctrine bans WoC from HOT
//! paths; a 16-row cron sample is not one, and a 429/fault simply leaves
//! the row for a later tick).

use overlay_discovery::low::storage::LowStorage;

use crate::proof_fetcher::{http_get, push_log, DEFAULT_BITAILS_BASE, DEFAULT_WOC_BASE};

/// Per-tick candidate bound for [`confirm_advert_spends`] — the same figure
/// as the #273 rebroadcast backstop (each candidate costs 1 WoC spent-probe
/// GET and, only on a spent pointer, ≤2 raw-fetch GETs → ≤48 subrequests
/// worst case).
pub const ADVERT_SPEND_CHECK_LIMIT: u64 = 16;

/// Admin-route cap for `?spendLimit=` (≤64 × 3 GETs = 192 subrequests +
/// ≤64 D1 deletes — comfortably under the paid 1,000-subrequest wall with
/// the route's other ops).
pub const ADVERT_SPEND_CHECK_MAX_LIMIT: u64 = 64;

/// Per-tick candidate bound for [`reap_expired_adverts`]. D1-only (1 scan +
/// ≤50 per-row deletes through the shared `delete_record` path); at 96
/// ticks/day a multi-thousand-row backlog drains in a day.
pub const ADVERT_REAP_LIMIT: u64 = 50;

/// Admin-route cap for `?reapLimit=` (1 scan + ≤400 deletes ≈ 401 D1 ops
/// warm, ≈ 493 cold with the migration pass — under the wall).
pub const ADVERT_REAP_MAX_LIMIT: u64 = 400;

/// Blocks BELOW the tip an advert's `expiryHeight` must be before the
/// reaper deletes it: reap iff `expiryHeight <= tip - MARGIN` (~1 h at the
/// 10-min Poisson mean).
///
/// Why 6, and why a margin at all: deletion is IRREVERSIBLE while the
/// expiry filter already hides `expiryHeight <= tip` rows from the lobby —
/// so a late reap costs nothing (the row is invisible), but an early reap
/// on a WRONG tip would destroy a live advert. The tip here comes from the
/// same chaintracks source as the display filter and can skew (a stale
/// cache is bounded at 30 s, but a provider fault/reorg edge can misreport
/// by a block or two); 6 blocks is generous against any plausible tip skew
/// while still clearing an expired row within the hour.
pub const ADVERT_REAP_MARGIN_BLOCKS: u32 = 6;

// ============================================================================
// Pure decision logic (unit-tested)
// ============================================================================

/// PURE: the reap cutoff height for a (possibly unresolved) tip.
///
/// `None` = REAP NOTHING, in BOTH degenerate cases:
/// - tip unresolved (no tracker / fetch fault) — the #148 fail-CLOSED rule:
///   a destructive pass without a real tip must refuse, never guess;
/// - tip < margin (early-chain absurdity) — no honest mainnet tip is below
///   6, so a tip that small is itself evidence of a broken read.
pub fn reap_cutoff(tip: Option<u32>, margin: u32) -> Option<u32> {
    tip?.checked_sub(margin)
}

/// PURE: the per-row reap predicate against a RESOLVED cutoff. A row
/// without an `expiryHeight` is NEVER reaped (no expiry evidence — only a
/// confirmed on-chain spend may remove it).
pub fn should_reap(expiry_height: Option<u32>, cutoff: u32) -> bool {
    expiry_height.is_some_and(|e| e <= cutoff)
}

/// One WoC outpoint-spent observation, shape-validated (PURE parse below).
/// Mirrors `low-app-layer::results::SpentObservation`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpentProbe {
    /// WoC 200 with a well-formed spender txid (lowercase hex).
    Spent { spending_txid: String },
    /// WoC 4xx: "unspent or not yet indexed".
    NotSpent,
    /// Transport / 5xx / rate-limit / malformed body.
    Fault,
}

/// PURE: parse a WoC `GET /tx/{txid}/{vout}/spent` response into a
/// [`SpentProbe`]. Strict: a 200 whose body lacks a well-formed 64-hex
/// `txid` is a Fault, never a verdict.
pub fn parse_woc_spent_probe(status: u16, body: &str) -> SpentProbe {
    if (400..500).contains(&status) {
        return SpentProbe::NotSpent;
    }
    if !(200..300).contains(&status) {
        return SpentProbe::Fault;
    }
    let Ok(v) = serde_json::from_str::<serde_json::Value>(body) else {
        return SpentProbe::Fault;
    };
    let Some(txid) = v.get("txid").and_then(|t| t.as_str()) else {
        return SpentProbe::Fault;
    };
    if txid.len() != 64 || !txid.chars().all(|c| c.is_ascii_hexdigit()) {
        return SpentProbe::Fault;
    }
    SpentProbe::Spent {
        spending_txid: txid.to_ascii_lowercase(),
    }
}

/// PURE: verify a fetched spender raw — it must hash to `spender_txid` AND
/// spend `(outpoint_txid, vout)`. The one-provider-positive rule rests on
/// this (same bar as `low-app-layer::results::spender_raw_verifies`): the
/// bytes themselves prove the spend, so no second provider is needed for
/// the POSITIVE — and nothing less than the bytes is ever enough.
pub fn spender_raw_verifies(
    raw_hex: &str,
    spender_txid: &str,
    outpoint_txid: &str,
    vout: u32,
) -> bool {
    let Ok(tx) = bsv_rs::transaction::Transaction::from_hex(raw_hex.trim()) else {
        return false;
    };
    if !tx.id().eq_ignore_ascii_case(spender_txid) {
        return false;
    }
    tx.inputs.iter().any(|i| {
        i.source_output_index == vout
            && i.source_txid
                .as_deref()
                .is_some_and(|t| t.eq_ignore_ascii_case(outpoint_txid))
    })
}

/// What [`confirm_advert_spends`] does with one probed row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpendRowAction {
    /// Raw-verified spend — run the `output_spent` deletion.
    Delete,
    /// WoC answered "unspent" — the row stands (healthy advert).
    LeaveNotSpent,
    /// Fault or an unverifiable pointer — the row stands, retried on a
    /// later tick. A flake NEVER deletes.
    LeaveUnknown,
}

/// PURE: the spent-verdict → delete mapping. ONLY a probe pointer whose
/// spender raw VERIFIED deletes; everything else leaves the row standing.
pub fn spend_row_action(probe: &SpentProbe, spender_raw_ok: bool) -> SpendRowAction {
    match probe {
        SpentProbe::Spent { .. } if spender_raw_ok => SpendRowAction::Delete,
        SpentProbe::Spent { .. } => SpendRowAction::LeaveUnknown,
        SpentProbe::NotSpent => SpendRowAction::LeaveNotSpent,
        SpentProbe::Fault => SpendRowAction::LeaveUnknown,
    }
}

// ============================================================================
// Summaries
// ============================================================================

/// Tally of one advert spend-confirmation pass (logged by the cron /
/// returned by the admin route).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct AdvertSpendSummary {
    /// Table rows probed this tick.
    pub scanned: usize,
    /// Rows deleted on a raw-verified spend (the orphan class closed).
    pub deleted: usize,
    /// Rows WoC reported unspent — healthy adverts, left standing.
    pub not_spent: usize,
    /// Probe faults / unverifiable pointers — left standing, retried on a
    /// later tick (fail-safe: never a deletion on a flake).
    pub unknown: usize,
    /// Verified spends whose D1 delete failed — retried next tick.
    pub delete_failed: usize,
}

/// Tally of one expired-advert reap pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct AdvertReapSummary {
    /// Whether a real tip resolved this pass. `false` ⇒ NOTHING was reaped
    /// (fail-closed) — surfaced so a permanently-dead tracker is visible in
    /// logs instead of masquerading as "no expired adverts".
    pub tip_resolved: bool,
    /// Expired candidates surfaced this tick.
    pub scanned: usize,
    /// Rows deleted (expiry ≥ [`ADVERT_REAP_MARGIN_BLOCKS`] below the tip).
    pub reaped: usize,
    /// Candidates whose D1 delete failed — retried next tick.
    pub delete_failed: usize,
}

// ============================================================================
// The passes
// ============================================================================

/// Resolve the chain tip for the reaper, mirroring `ls_low`'s
/// `resolve_tip`: `None` on no tracker or a fetch fault — but here the
/// consumer is fail-CLOSED (reap nothing), the opposite direction of the
/// lobby filter's fail-open. Never silent either way.
pub async fn resolve_tip(
    tracker: Option<&dyn bsv_rs::transaction::ChainTracker>,
) -> Option<u32> {
    match tracker {
        Some(ct) => match ct.current_height().await {
            Ok(h) => Some(h),
            Err(e) => {
                push_log(&format!(
                    "[advert-lifecycle] chain-tip fetch failed — reaper refuses this tick \
                     (fail-closed, bsv-low#148 lesson): {e}"
                ));
                None
            }
        },
        None => {
            push_log(
                "[advert-lifecycle] no chain tracker configured — reaper refuses this tick \
                 (fail-closed)",
            );
            None
        }
    }
}

/// Reap TABLE rows whose `expiryHeight` sits at least
/// [`ADVERT_REAP_MARGIN_BLOCKS`] below a RESOLVED tip (bsv-low #309).
///
/// FAIL-CLOSED: `tip = None` reaps NOTHING — a destructive pass must never
/// act without a real tip (the #148 silent-no-op lesson, inverted for a
/// deleter). Bounded by `limit`; oldest-expiry-first, so the backlog drains
/// deterministically. Deletion goes through the SAME
/// [`LowStorage::delete_record`] the `output_spent` path uses.
pub async fn reap_expired_adverts(
    storage: &dyn LowStorage,
    tip: Option<u32>,
    limit: u64,
) -> AdvertReapSummary {
    let mut summary = AdvertReapSummary::default();

    let Some(cutoff) = reap_cutoff(tip, ADVERT_REAP_MARGIN_BLOCKS) else {
        // Logged by resolve_tip when the tip itself failed; log the
        // decision here too so a reap-nothing tick is always attributable.
        push_log("[advert-reap] no resolved tip — reaping NOTHING this tick");
        return summary;
    };
    summary.tip_resolved = true;

    let candidates = match storage.find_tables_expired_at_or_before(cutoff, limit).await {
        Ok(c) => c,
        Err(e) => {
            push_log(&format!("[advert-reap] candidate scan failed: {e}"));
            return summary;
        }
    };
    summary.scanned = candidates.len();

    for row in candidates {
        // Belt-and-braces re-check of the pure predicate — a backend that
        // ignored the cutoff must not turn the reaper into a mass delete.
        if !should_reap(row.expiry_height, cutoff) {
            continue;
        }
        match storage.delete_record(&row.txid, row.output_index).await {
            Ok(()) => {
                push_log(&format!(
                    "[advert-reap] reaped {}:{} (expiry {} <= cutoff {cutoff}, tip {})",
                    row.txid,
                    row.output_index,
                    row.expiry_height.unwrap_or(0),
                    tip.unwrap_or(0),
                ));
                summary.reaped += 1;
            }
            Err(e) => {
                push_log(&format!(
                    "[advert-reap] delete {}:{} failed (retry next tick): {e}",
                    row.txid, row.output_index
                ));
                summary.delete_failed += 1;
            }
        }
    }

    summary
}

/// Probe ONE advert outpoint's spend status and verify any reported
/// spender's raw bytes (WoC pointer → raw fetch Bitails-first / WoC
/// break-glass → hash + input match). Network glue only — every decision
/// it feeds is pure ([`parse_woc_spent_probe`], [`spender_raw_verifies`],
/// [`spend_row_action`]).
async fn probe_advert_spend(
    txid: &str,
    vout: u32,
    woc_api_key: Option<&str>,
) -> (SpentProbe, bool) {
    let hdr = woc_api_key.map(|k| ("woc-api-key", k));
    let probe = match http_get(
        &format!("{DEFAULT_WOC_BASE}/tx/{txid}/{vout}/spent"),
        hdr,
    )
    .await
    {
        Ok((status, body)) => parse_woc_spent_probe(status, &body),
        Err(_) => SpentProbe::Fault,
    };

    let SpentProbe::Spent { spending_txid } = &probe else {
        return (probe, false);
    };

    // Raw verification — Bitails hex first, WoC hex break-glass (this
    // worker's courier order: WoC never sits on the warm path).
    let mut raw_ok = false;
    for url in [
        format!("{DEFAULT_BITAILS_BASE}/download/tx/{spending_txid}/hex"),
        format!("{DEFAULT_WOC_BASE}/tx/{spending_txid}/hex"),
    ] {
        if let Ok((status, body)) = http_get(&url, hdr).await {
            if (200..300).contains(&status)
                && spender_raw_verifies(&body, spending_txid, txid, vout)
            {
                raw_ok = true;
                break;
            }
        }
    }
    (probe, raw_ok)
}

/// Confirm on-chain spends of lobby-advert outpoints and evict the spent
/// rows (bsv-low #309) — the maintenance closer of the `output_spent` gap:
/// a close broadcast via the client's direct-ARC fallback never came
/// through `/submit`, so nothing ever fired the hook and the row lingered.
///
/// Per RANDOM-sampled TABLE row (bounded by `limit`): WoC outpoint-spent
/// probe → on a pointer, fetch + verify the spender's RAW (hash-bound,
/// input-matched — the module-doc doctrine) → on a VERIFIED spend, the same
/// [`LowStorage::delete_record`] the `output_spent` path runs. NotSpent /
/// fault / unverifiable pointer → the row stands (fail-safe: an uncertain
/// read never deletes; the RANDOM sample revisits it later).
pub async fn confirm_advert_spends(
    storage: &dyn LowStorage,
    woc_api_key: Option<&str>,
    limit: u64,
) -> AdvertSpendSummary {
    let mut summary = AdvertSpendSummary::default();

    let candidates = match storage.find_tables_for_spend_check(limit).await {
        Ok(c) => c,
        Err(e) => {
            push_log(&format!("[advert-spend] candidate scan failed: {e}"));
            return summary;
        }
    };
    summary.scanned = candidates.len();

    for row in &candidates {
        let (probe, raw_ok) = probe_advert_spend(&row.txid, row.output_index, woc_api_key).await;
        match spend_row_action(&probe, raw_ok) {
            SpendRowAction::Delete => {
                let spender = match &probe {
                    SpentProbe::Spent { spending_txid } => spending_txid.as_str(),
                    _ => unreachable!("Delete only arises from a Spent probe"),
                };
                match storage.delete_record(&row.txid, row.output_index).await {
                    Ok(()) => {
                        push_log(&format!(
                            "[advert-spend] evicted {}:{} — RAW-VERIFIED spent by {spender} \
                             (the direct-ARC close class, bsv-low#309)",
                            row.txid, row.output_index
                        ));
                        summary.deleted += 1;
                    }
                    Err(e) => {
                        push_log(&format!(
                            "[advert-spend] delete {}:{} failed (retry next tick): {e}",
                            row.txid, row.output_index
                        ));
                        summary.delete_failed += 1;
                    }
                }
            }
            SpendRowAction::LeaveNotSpent => summary.not_spent += 1,
            SpendRowAction::LeaveUnknown => {
                if matches!(probe, SpentProbe::Spent { .. }) {
                    push_log(&format!(
                        "[advert-spend] {}:{} has a WoC spent pointer whose raw did NOT verify \
                         — left standing (provider fault, never a deletion)",
                        row.txid, row.output_index
                    ));
                }
                summary.unknown += 1;
            }
        }
    }

    summary
}

/// Run both lifecycle passes: the reaper FIRST (D1-only — cheaply shrinks
/// the table population so the network-bound spend probe never wastes GETs
/// on rows the reaper is about to delete), then the spend-confirmation
/// probe. Both entry points (the scheduled tick and
/// `POST /admin/advert-lifecycle`) call THIS function so the order cannot
/// drift apart — the `run_pot_maintenance` idiom.
pub async fn run_advert_lifecycle(
    storage: &dyn LowStorage,
    tip: Option<u32>,
    woc_api_key: Option<&str>,
    spend_limit: u64,
    reap_limit: u64,
) -> (AdvertReapSummary, AdvertSpendSummary) {
    let reap = reap_expired_adverts(storage, tip, reap_limit).await;
    let spend = confirm_advert_spends(storage, woc_api_key, spend_limit).await;
    (reap, spend)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use overlay_discovery::low::storage::{LowRecord, LowRecordType, MemoryLowStorage};

    // ── the reap predicate ───────────────────────────────────────────────

    /// The #148 fail-CLOSED rule: no resolved tip ⇒ no cutoff ⇒ nothing is
    /// ever reaped — a reaper must refuse without evidence, never guess.
    #[test]
    fn null_tip_yields_no_cutoff() {
        assert_eq!(reap_cutoff(None, ADVERT_REAP_MARGIN_BLOCKS), None);
        // Degenerate tip below the margin (an impossible mainnet read) also
        // refuses rather than underflowing to a bogus cutoff.
        assert_eq!(reap_cutoff(Some(3), ADVERT_REAP_MARGIN_BLOCKS), None);
    }

    #[test]
    fn cutoff_is_tip_minus_margin() {
        assert_eq!(reap_cutoff(Some(900_006), ADVERT_REAP_MARGIN_BLOCKS), Some(900_000));
        assert_eq!(reap_cutoff(Some(6), 6), Some(0));
    }

    /// The margin keeps freshly-expired rows standing: only an expiry at
    /// least MARGIN blocks below the tip reaps; a NULL expiry NEVER reaps.
    #[test]
    fn reap_predicate_margin_and_null_expiry() {
        let tip = 900_006u32;
        let cutoff = reap_cutoff(Some(tip), ADVERT_REAP_MARGIN_BLOCKS).unwrap();
        assert!(should_reap(Some(900_000), cutoff), "expiry == tip - margin reaps");
        assert!(should_reap(Some(899_999), cutoff), "older reaps");
        assert!(
            !should_reap(Some(900_001), cutoff),
            "inside the margin window (expired but < margin below tip) stands"
        );
        assert!(!should_reap(Some(tip), cutoff), "unexpired stands");
        assert!(!should_reap(None, cutoff), "NULL expiry is never reap evidence");
    }

    // ── the reap pass over the real memory backend ───────────────────────

    fn table(txid: &str, expiry: Option<u32>) -> LowRecord {
        LowRecord {
            record_type: LowRecordType::Table,
            txid: txid.into(),
            output_index: 0,
            host_identity: "02".repeat(33),
            game_id: "11".repeat(32),
            stake_sats: Some(1000),
            rules_hash: None,
            relay_url: None,
            expiry_height: expiry,
        }
    }

    /// A null tip reaps NOTHING even against an ancient expired row — the
    /// pass-level proof of the fail-closed rule, through the real storage.
    #[tokio::test]
    async fn reap_pass_refuses_on_null_tip() {
        let store = MemoryLowStorage::new();
        store.store_record(&table("ancient", Some(1))).await.unwrap();

        let s = reap_expired_adverts(&store, None, ADVERT_REAP_LIMIT).await;
        assert_eq!(
            s,
            AdvertReapSummary {
                tip_resolved: false,
                scanned: 0,
                reaped: 0,
                delete_failed: 0
            }
        );
        assert_eq!(store.record_count(), 1, "the row STANDS — no tip, no reap");
    }

    /// With a real tip: beyond-margin rows are reaped, within-margin and
    /// unexpired rows stand, and the batch is bounded.
    #[tokio::test]
    async fn reap_pass_deletes_only_beyond_the_margin() {
        let store = MemoryLowStorage::new();
        let tip = 900_006u32;
        store.store_record(&table("beyond", Some(900_000))).await.unwrap();
        store.store_record(&table("inside", Some(900_003))).await.unwrap();
        store.store_record(&table("fresh", Some(900_100))).await.unwrap();
        store.store_record(&table("no_expiry", None)).await.unwrap();

        let s = reap_expired_adverts(&store, Some(tip), ADVERT_REAP_LIMIT).await;
        assert!(s.tip_resolved);
        assert_eq!((s.scanned, s.reaped, s.delete_failed), (1, 1, 0));
        let remaining = store.find_by_game_id(&"11".repeat(32)).await.unwrap();
        let mut txids: Vec<&str> = remaining.iter().map(|r| r.txid.as_str()).collect();
        txids.sort_unstable();
        assert_eq!(
            txids,
            vec!["fresh", "inside", "no_expiry"],
            "only the beyond-margin row was reaped"
        );
    }

    #[tokio::test]
    async fn reap_pass_is_bounded_per_tick() {
        let store = MemoryLowStorage::new();
        for i in 0..5u32 {
            store
                .store_record(&table(&format!("t{i}"), Some(100 + i)))
                .await
                .unwrap();
        }
        let s = reap_expired_adverts(&store, Some(10_000), 2).await;
        assert_eq!((s.scanned, s.reaped), (2, 2), "limit bounds one pass");
        assert_eq!(store.record_count(), 3, "the rest drain on later ticks");
    }

    // ── the spent-verdict → delete mapping ───────────────────────────────

    /// ONLY a raw-verified spent pointer deletes; unknown NEVER deletes.
    #[test]
    fn only_a_verified_spend_deletes() {
        let spent = SpentProbe::Spent {
            spending_txid: "ab".repeat(32),
        };
        assert_eq!(spend_row_action(&spent, true), SpendRowAction::Delete);
        assert_eq!(
            spend_row_action(&spent, false),
            SpendRowAction::LeaveUnknown,
            "an UNVERIFIED pointer is a provider fault, never a deletion"
        );
        assert_eq!(
            spend_row_action(&SpentProbe::NotSpent, false),
            SpendRowAction::LeaveNotSpent
        );
        assert_eq!(
            spend_row_action(&SpentProbe::Fault, false),
            SpendRowAction::LeaveUnknown,
            "a flake never deletes"
        );
        // raw_ok can never rescue a non-Spent probe (defensive pairing).
        assert_eq!(
            spend_row_action(&SpentProbe::Fault, true),
            SpendRowAction::LeaveUnknown
        );
        assert_eq!(
            spend_row_action(&SpentProbe::NotSpent, true),
            SpendRowAction::LeaveNotSpent
        );
    }

    #[test]
    fn woc_spent_probe_parse_is_strict() {
        let good = format!("{{\"txid\":\"{}\",\"status\":\"confirmed\"}}", "AB".repeat(32));
        assert_eq!(
            parse_woc_spent_probe(200, &good),
            SpentProbe::Spent {
                spending_txid: "ab".repeat(32)
            },
            "well-formed pointer, lowercased"
        );
        assert_eq!(
            parse_woc_spent_probe(404, ""),
            SpentProbe::NotSpent,
            "4xx = unspent-or-unindexed"
        );
        for (status, body) in [
            (500u16, ""),                          // provider fault
            (200, "not json"),                     // malformed body
            (200, "{\"noTxid\":true}"),            // missing pointer
            (200, "{\"txid\":\"beef\"}"),          // short txid
            (200, "{\"txid\":\"zz\"}"),            // non-hex
        ] {
            assert_eq!(
                parse_woc_spent_probe(status, body),
                SpentProbe::Fault,
                "({status}, {body:?}) must be a fault, never a verdict"
            );
        }
    }

    // ── raw verification binds hash AND input ────────────────────────────

    /// Serialize a bare raw tx: one input spending (prev_txid, prev_vout),
    /// one dust output — the low-app-layer test idiom.
    fn raw_tx_spending(prev_txid_hex: &str, prev_vout: u32) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&1u32.to_le_bytes());
        v.push(1); // input count
        let mut prev = hex::decode(prev_txid_hex).unwrap();
        prev.reverse();
        v.extend_from_slice(&prev);
        v.extend_from_slice(&prev_vout.to_le_bytes());
        v.push(0); // empty unlocking script
        v.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
        v.push(1); // output count
        v.extend_from_slice(&1u64.to_le_bytes());
        v.push(1); // script len
        v.push(0x51); // OP_TRUE
        v.extend_from_slice(&0u32.to_le_bytes());
        v
    }

    #[test]
    fn spender_raw_verifies_binds_hash_and_input() {
        let advert_txid = "11".repeat(32);
        let raw = raw_tx_spending(&advert_txid, 0);
        let raw_hex = hex::encode(&raw);
        let spender = bsv_rs::transaction::Transaction::from_binary(&raw).unwrap().id();

        assert!(spender_raw_verifies(&raw_hex, &spender, &advert_txid, 0));
        assert!(
            spender_raw_verifies(&raw_hex, &spender.to_ascii_uppercase(), &advert_txid, 0),
            "case-insensitive txid comparison"
        );
        assert!(
            !spender_raw_verifies(&raw_hex, &"ff".repeat(32), &advert_txid, 0),
            "a raw that does not hash to the reported spender is refused"
        );
        assert!(
            !spender_raw_verifies(&raw_hex, &spender, &advert_txid, 1),
            "wrong vout — the raw does not spend the ADVERT outpoint"
        );
        assert!(
            !spender_raw_verifies(&raw_hex, &spender, &"22".repeat(32), 0),
            "wrong outpoint txid"
        );
        assert!(
            !spender_raw_verifies("zz-not-hex", &spender, &advert_txid, 0),
            "unparseable raw is refused"
        );
    }
}
