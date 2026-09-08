//! bsv-low M19 R2 (2026-09-08, round 2): the reorg reconcile's I/O half,
//! the reference's `Engine.handleReorg` + revalidation sweep in the
//! Workers-native shape. Every demotion here is EVIDENCE-DRIVEN (round-2
//! review H2/M1): a row is demoted only when chaintracks REFUTES the stored
//! proof that confirmed it, never on a height alone and never on a courier's
//! say-so. Every walk is height-windowed, index-served, bounded per pass
//! and carries a persisted cursor (review H3b), so a bounded pass continues
//! where the last one stopped until its window is exhausted.
//!
//! Three legs share the cursor machinery (review M4):
//! - `spenders`: `pot_records` confirmations, re-verified through the
//!   spender's stored `pot_beefs` bump; a refuted one is demoted (guarded on
//!   the pointer) and its proof unlatched;
//! - `pot_beefs`: the pots' OWN verified bumps (a JOIN mined in the
//!   orphan), by `proofHeight`; a refuted one is unlatched for the
//!   completion pass;
//! - `transactions`: the engine's stitched hop proofs, by `proofHeight`; a
//!   refuted one is un-proved (`has_proof = 0`), the engine's re-fetch cue.
//!
//! Fail-safe on every uncertain arm: a header-source fault is counted and
//! changes nothing; no header source examines nothing.

use std::collections::HashMap;

use bsv_rs::transaction::{Beef, ChainTracker, MerklePath};
use overlay_discovery::pot::reorg::{
    classify_reverify, next_sweep_window, BumpAnchor, ReverifyVerdict, RowKey, SweepState,
};
use overlay_discovery::pot::storage::PotStorage;
use overlay_engine::gasp::AncestorFetcher;

use crate::proof_fetcher::{push_log, verify_bump_detailed};

/// bsv-low M19 R2 round 3 (review MED-3): a per-PASS `(height, root)` →
/// validity memo. Every tx mined in one block computes the SAME merkle root,
/// so a window of N confirmations at a handful of heights collapses from N
/// chaintracks subrequests (one per row) to ~one per distinct (height, root)
/// — a full 3-height block-event pass drops from up to 150 reads to ~3.
/// Faults are NOT memoized (they are retried, never cached as a verdict).
#[derive(Default)]
pub struct RootMemo {
    verdicts: HashMap<(u32, String), bool>,
    /// bsv-low M19B-G1 round 2 (review MED-5): the CANONICAL merkle root
    /// chaintracks holds at a height, seeded from a header read the pass
    /// already made (the Arcade event consumer's corroboration read). One
    /// header per height, so a root at that height is valid iff it is this
    /// one: every row anchored there is judged without a further read.
    canonical: HashMap<u32, String>,
}

impl RootMemo {
    /// The row count is what the memo saves: `reads` is how many distinct
    /// (height, root) pairs actually hit chaintracks this pass.
    pub fn reads(&self) -> usize {
        self.verdicts.len()
    }

    /// Seed the canonical root at `height` from a header the pass read.
    pub fn seed_canonical(&mut self, height: u32, root: &str) {
        self.canonical.insert(height, root.to_ascii_lowercase());
    }

    /// Heights with a seeded canonical root.
    pub fn seeded(&self) -> usize {
        self.canonical.len()
    }
}

/// [`verify_bump_detailed`] through the per-pass memo (review MED-3).
pub(crate) async fn verify_bump_memoized(
    tracker: &dyn ChainTracker,
    memo: &mut RootMemo,
    bump_hex: &str,
    txid: &str,
) -> Result<bool, String> {
    let bump = match MerklePath::from_hex(bump_hex) {
        Ok(b) => b,
        Err(_) => return Ok(false),
    };
    let root = match bump.compute_root(Some(txid)) {
        Ok(r) => r,
        Err(_) => return Ok(false),
    };
    if let Some(canonical) = memo.canonical.get(&bump.block_height) {
        return Ok(canonical.eq_ignore_ascii_case(&root));
    }
    let key = (bump.block_height, root);
    if let Some(&v) = memo.verdicts.get(&key) {
        return Ok(v);
    }
    let v = tracker
        .is_valid_root_for_height(&key.1, key.0)
        .await
        .map_err(|e| format!("chaintracks read failed for {txid}@{}: {e}", key.0))?;
    memo.verdicts.insert(key, v);
    Ok(v)
}

/// The persisted walk names (one `reorg_sweep_state` row each).
pub const WALK_SPENDERS: &str = "spenders";
pub const WALK_POT_BEEFS: &str = "pot_beefs";
pub const WALK_TRANSACTIONS: &str = "transactions";

/// What a stored BEEF's OWN bump for `txid` anchors it to (height + the
/// root it computes), if the BEEF parses and carries one.
pub fn stored_bump_anchor(stored_beef: &[u8], txid: &str) -> Option<BumpAnchor> {
    let beef = Beef::from_binary(stored_beef).ok()?;
    let bump = own_bump(&beef, txid)?;
    let root = bump.compute_root(Some(txid)).ok()?.to_ascii_lowercase();
    Some(BumpAnchor { height: u64::from(bump.block_height), root })
}

/// The tx's OWN bump (its `bump_index`), NEVER `find_bump` (bsv-low M19 R2
/// round 3, review HIGH-1): `find_bump` returns the FIRST bump whose path
/// contains the txid, which after a same-height reorg is the STALE orphan
/// bump the subject was moved off. `bump_index` is the one the stitch (now
/// fixed) actually anchored the subject to.
fn own_bump<'a>(beef: &'a Beef, txid: &str) -> Option<&'a MerklePath> {
    let bi = beef.find_txid(txid).and_then(bsv_rs::transaction::BeefTx::bump_index)?;
    beef.bumps.get(bi)
}

/// The hex of a stored BEEF's OWN bump for `txid`, if any.
pub(crate) fn stored_bump_hex(stored_beef: &[u8], txid: &str) -> Option<String> {
    let beef = Beef::from_binary(stored_beef).ok()?;
    own_bump(&beef, txid).map(MerklePath::to_hex)
}

/// What one bounded pass over a window of confirmations found and did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReverifyPassSummary {
    /// Confirmed rows examined this pass.
    pub scanned: usize,
    /// Rows whose stored spender proof the header source still HOLDS.
    pub standing: usize,
    /// Rows whose stored proof the header source REFUTES: demoted (guarded)
    /// and unlatched (#429: zero on a healthy stream).
    pub stale: usize,
    /// Rows with no stored proof demoted BLIND because the caller asked
    /// (a detected reorg at their height, the operator's window): nothing
    /// to verify, the courier arm re-judges them.
    pub demoted_blind: usize,
    /// Rows with no stored proof LEFT ALONE (the routine sweep re-asked the
    /// ladder and it could not prove the row, or no courier is configured, or
    /// the row has no stored raw to stitch a proof into).
    pub no_stored_proof: usize,
    /// Rows re-anchored: STANDING (the stored bump verifies) but the row's
    /// `spentHeight` was stale — moved to the bump's height (review L1).
    pub reanchored: usize,
    /// bsv-low M19B-G1: REFUTED rows re-anchored WITHOUT a demotion: the
    /// ladder (Arcade first) answered a chaintracks-verified proof naming
    /// another block and the stored bump + the row's height were replaced
    /// in place (`ReverifyMode::reanchor_first`). The served confirmation
    /// never flickered to SEEN.
    pub reanchored_from_courier: usize,
    /// Courier-confirmed rows the routine sweep HEALED: re-asked the ladder,
    /// got an agreeing proof, stitched it into the stored BEEF so future
    /// passes verify it locally (review MED-4).
    pub stored_from_courier: usize,
    /// Distinct (height, root) chaintracks reads this pass (review MED-3): the
    /// memo hit for every other row.
    pub memo_reads: usize,
    /// bsv-low M19B-G1 round 2 (review MED-2): the ladder's budget ran out
    /// before a row that needed it; the page STOPPED there (`exhausted` is
    /// false, `next_cursor` is the last row judged) so a fresh budget
    /// continues at that row. Nothing was judged blind of the couriers.
    pub budget_exhausted: bool,
    /// Refuted rows whose demotion guard MISSED (the pointer OR the judged
    /// height moved under the pass): nothing written, re-examined next pass.
    pub demote_missed: usize,
    /// Header-source read faults: NOT a verdict, nothing changed, retried.
    pub faults: usize,
    /// Storage faults.
    pub errors: usize,
    /// The window's end was reached this pass.
    pub exhausted: bool,
    /// Where the next pass continues (`None` = from the head).
    pub next_cursor: Option<RowKey>,
}

/// How one [`reverify_window_with`] pass treats the rows it cannot verify
/// locally or finds refuted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReverifyMode {
    /// Demote rows that have no stored proof to verify (the caller has
    /// independent reason: a detected reorg at that height, an operator's
    /// window). Off: the routine sweep's courier re-check (review MED-4).
    pub demote_proofless: bool,
    /// bsv-low M19B-G1: before demoting a REFUTED row, ask the courier
    /// ladder ONCE (Arcade first) for a chaintracks-verified proof and, when
    /// one names another block, replace the stored bump and the row's
    /// height IN PLACE (`apply_pushed_proof_to_pot_stores`); demote only
    /// when no canonical proof is served. Needs a `fetcher`; without one
    /// the arm is the plain demotion. Off for every R2 caller (their pins
    /// are unchanged).
    pub reanchor_first: bool,
}

/// ONE bounded, evidence-driven pass over the confirmed rows anchored in
/// `lo..=hi`, continuing after `after`: each row's stored spender bump is
/// re-verified against the header source; a REFUTED one demotes the row
/// (guarded on the pointer) and unlatches the spender's proof; a standing
/// one is counted; a fault changes nothing. `demote_proofless` demotes
/// rows that have no stored proof to verify (the caller has independent
/// reason: a detected reorg at that height, an operator's window).
#[allow(clippy::too_many_arguments)] // the window bounds, the cursor, the two chain sources, the mode
pub async fn reverify_window(
    pot_storage: &dyn PotStorage,
    tracker: Option<&dyn ChainTracker>,
    fetcher: Option<&dyn AncestorFetcher>,
    lo: u64,
    hi: u64,
    after: Option<RowKey>,
    limit: u64,
    demote_proofless: bool,
) -> ReverifyPassSummary {
    reverify_window_with(
        pot_storage,
        tracker,
        fetcher,
        lo,
        hi,
        after,
        limit,
        ReverifyMode { demote_proofless, reanchor_first: false },
        &mut RootMemo::default(),
    )
    .await
}

/// What the ladder said when a REFUTED row asked it for a canonical proof
/// (`ReverifyMode::reanchor_first`).
enum LadderReanchor {
    /// A chaintracks-verified proof naming another block is in the pot
    /// stores: landed by this ask, or ALREADY there because another writer
    /// (an `/arc-ingest` push, another isolate) landed the same anchor
    /// between this pass's page read and its ask (round 2, review MED-1).
    Reanchored,
    /// No courier serves a proof that verifies (or the verified one wrote
    /// nothing and the store does not hold its anchor): the row's
    /// confirmation still rests on the refuted bump.
    NoCanonicalProof,
    /// The ladder's per-pass budget is spent (round 2, review MED-2): no
    /// courier was asked, so nothing is known; the page stops here.
    BudgetExhausted,
    /// A chaintracks read fault while verifying a candidate: not a verdict.
    Fault(String),
}

/// The anchor a verified pushed proof names for `txid`.
fn pushed_anchor(hex: &str, txid: &str) -> Option<BumpAnchor> {
    let mp = MerklePath::from_hex(hex).ok()?;
    let root = mp.compute_root(Some(txid)).ok()?.to_ascii_lowercase();
    Some(BumpAnchor { height: u64::from(mp.block_height), root })
}

/// bsv-low M19B-G1: the re-anchor-first arm for ONE refuted confirmed row.
/// The ladder's `verified_proof_for_detailed` verifies every candidate bump
/// against chaintracks before answering it (the `apply_pushed_proof_to_pot_stores`
/// precondition); an `Ok(None)` (unmined, unverifiable) is "no canonical
/// proof", never a demotion by itself. Round 2: a spent budget is asked of
/// the ladder BEFORE the ask (its `Ok(None)` would read as "no proof"), and
/// a verified proof that lands nothing is re-checked against the stored
/// anchor: the store already holding it means someone else landed it and
/// the row is canonical, never a demotion.
async fn reanchor_from_ladder(
    pot_storage: &dyn PotStorage,
    fetcher: &dyn AncestorFetcher,
    rec: &overlay_discovery::pot::storage::PotRecord,
    spender: &str,
) -> LadderReanchor {
    if fetcher.budget_remaining() == Some(0) {
        return LadderReanchor::BudgetExhausted;
    }
    match fetcher.verified_proof_for_detailed(spender).await {
        Ok(Some(hex)) => {
            let applied = crate::proof_fetcher::apply_pushed_proof_to_pot_stores(pot_storage, spender, &hex).await;
            if applied.landed_anything() {
                push_log(&format!(
                    "[reorg] {}:{} RE-ANCHORED IN PLACE: the ladder serves a chaintracks-verified proof for {spender} at another block (pot_beef_reanchored={} spends_reanchored={} compacted={})",
                    rec.txid, rec.output_index, applied.pot_beef_reanchored, applied.spends_reanchored, applied.pot_beef_compacted
                ));
                return LadderReanchor::Reanchored;
            }
            // nothing landed: EXACTLY the case where the store already holds
            // the pushed anchor (another writer got there first, or the
            // header source flipped between the page read and the ladder's
            // fresh read). Re-read the stored anchor before believing "no proof".
            let pushed = pushed_anchor(&hex, spender);
            let stored = match pot_storage.get_beef(spender).await {
                Ok(Some(bytes)) => stored_bump_anchor(&bytes, spender),
                Ok(None) => None,
                Err(e) => return LadderReanchor::Fault(format!("{spender} pot-beef re-read failed: {e}")),
            };
            if pushed.is_some() && pushed == stored {
                push_log(&format!(
                    "[reorg] {}:{} ALREADY RE-ANCHORED by another writer: the store holds the ladder's verified anchor for {spender} ({:?}); the row stands",
                    rec.txid, rec.output_index, stored
                ));
                return LadderReanchor::Reanchored;
            }
            push_log(&format!(
                "[reorg] {}:{} the ladder's verified proof for {spender} landed NOTHING (cas_missed={} cas_errors={}) and the store holds {:?}, not {:?}: demoting on the refuted stored bump",
                rec.txid, rec.output_index, applied.spends_cas_missed, applied.spends_cas_errors, stored, pushed
            ));
            LadderReanchor::NoCanonicalProof
        }
        Ok(None) => LadderReanchor::NoCanonicalProof,
        Err(e) => LadderReanchor::Fault(e),
    }
}

/// [`reverify_window`] with an explicit [`ReverifyMode`] and a SHARED
/// per-pass [`RootMemo`] (round 2, review MED-5: one memo across the legs,
/// the events and the sweep of a pass; `memo_reads` reports this call's
/// own reads).
#[allow(clippy::too_many_arguments)] // the window bounds, the cursor, the two chain sources, the mode, the memo
pub async fn reverify_window_with(
    pot_storage: &dyn PotStorage,
    tracker: Option<&dyn ChainTracker>,
    fetcher: Option<&dyn AncestorFetcher>,
    lo: u64,
    hi: u64,
    after: Option<RowKey>,
    limit: u64,
    mode: ReverifyMode,
    memo: &mut RootMemo,
) -> ReverifyPassSummary {
    let demote_proofless = mode.demote_proofless;
    let reads_before = memo.reads();
    let mut summary = ReverifyPassSummary { next_cursor: after, ..Default::default() };
    // No header source, no verdict: `verify_bump_detailed` answers Ok(false)
    // without a tracker, which would read as "every stored proof is refuted".
    // A pass that cannot ask the chain examines nothing (fail-safe), loudly.
    let Some(tracker) = tracker else {
        push_log("[reorg] no header source configured — nothing re-verified");
        return summary;
    };
    let rows = match pot_storage.confirmed_window_page(lo, hi, after, limit).await {
        Ok(rows) => rows,
        Err(e) => {
            summary.errors += 1;
            push_log(&format!("[reorg] window {lo}..={hi} read failed: {e}"));
            return summary;
        }
    };
    summary.scanned = rows.len();
    summary.exhausted = (rows.len() as u64) < limit;
    if let Some((key, _)) = rows.last() {
        summary.next_cursor = Some(*key);
    }
    // round 2 (review MED-2): the last row fully judged, where a budget stop
    // leaves the cursor so a fresh budget resumes at the next row
    let mut last_judged: Option<RowKey> = after;
    for (key, rec) in rows {
        let Some(spender) = rec.spending_txid.as_deref() else {
            last_judged = Some(key);
            continue;
        };
        let stored = match pot_storage.get_beef(spender).await {
            Ok(b) => b,
            Err(e) => {
                summary.errors += 1;
                push_log(&format!("[reorg] {spender} pot-beef read failed: {e}"));
                last_judged = Some(key);
                continue;
            }
        };
        let bump_hex = stored.as_deref().and_then(|bytes| stored_bump_hex(bytes, spender));
        let Some(bump_hex) = bump_hex else {
            // No stored proof to verify locally. Two modes:
            if demote_proofless {
                // the operator/reorg window: demote blind (the caller has
                // independent reason — a reorg at this height).
                match pot_storage
                    .demote_confirmed_for_spender_at(&rec.txid, rec.output_index, spender, rec.spent_height)
                    .await
                {
                    Ok(true) => {
                        summary.demoted_blind += 1;
                        push_log(&format!(
                            "[reorg] {}:{} demoted to SEEN (no stored proof to re-verify at {:?}; the courier arm re-judges it)",
                            rec.txid, rec.output_index, rec.spent_height
                        ));
                    }
                    Ok(false) => summary.demote_missed += 1,
                    Err(e) => {
                        summary.errors += 1;
                        push_log(&format!("[reorg] {}:{} demote CAS failed: {e}", rec.txid, rec.output_index));
                    }
                }
            } else {
                // the routine sweep (review MED-4): a courier-confirmed row
                // was never re-verified — unknown read as fine. Re-ask the
                // ladder ONCE: an agreeing proof HEALS the row (stitch it in
                // so the next pass verifies locally); a disagreeing one
                // DEMOTES it (its confirming block was orphaned); a fault or
                // an unprovable answer changes nothing (never demote on
                // unknown). Round 2 (review MED-2): a spent budget stops the
                // page BEFORE the ask, the cursor at the last row judged.
                if fetcher.is_some_and(|f| f.budget_remaining() == Some(0)) {
                    summary.budget_exhausted = true;
                    summary.exhausted = false;
                    summary.next_cursor = last_judged;
                    push_log(&format!(
                        "[reorg] {}:{} the ladder's budget is spent before {spender}'s re-check: the page stops here, resumed on a fresh budget",
                        rec.txid, rec.output_index
                    ));
                    break;
                }
                courier_recheck(pot_storage, fetcher, &rec, spender, stored.as_deref(), &mut summary).await;
            }
            last_judged = Some(key);
            continue;
        };
        let answer = verify_bump_memoized(tracker, memo, &bump_hex, spender).await;
        match classify_reverify(&answer) {
            ReverifyVerdict::Standing => {
                summary.standing += 1;
                // review L1: the stored bump is canonical, but the row's
                // spentHeight may be stale (confirmed from an orphan bump at
                // H1, re-proved at H2). Move it to the bump's height.
                if let Some(anchor) = stored.as_deref().and_then(|b| stored_bump_anchor(b, spender)) {
                    if rec.spent_height != Some(anchor.height) {
                        match pot_storage
                            .reanchor_confirmed_for_spender(&rec.txid, rec.output_index, spender, anchor.height)
                            .await
                        {
                            Ok(true) => {
                                summary.reanchored += 1;
                                push_log(&format!(
                                    "[reorg] {}:{} RE-ANCHORED {:?} → {} (the stored bump verifies at a different height)",
                                    rec.txid, rec.output_index, rec.spent_height, anchor.height
                                ));
                            }
                            Ok(false) => {}
                            Err(e) => {
                                summary.errors += 1;
                                push_log(&format!("[reorg] {}:{} re-anchor CAS failed: {e}", rec.txid, rec.output_index));
                            }
                        }
                    }
                }
            }
            ReverifyVerdict::NoStoredProof => summary.no_stored_proof += 1,
            ReverifyVerdict::Fault => {
                summary.faults += 1;
                push_log(&format!(
                    "[reorg] {}:{} header READ FAULT re-verifying {spender} — not a verdict: {}",
                    rec.txid,
                    rec.output_index,
                    answer.as_ref().err().map(String::as_str).unwrap_or("?")
                ));
            }
            ReverifyVerdict::Stale => {
                // bsv-low M19B-G1: a re-anchor is a REPLACEMENT, never a
                // demote-then-rechase, when the ladder already serves the
                // canonical proof (Arcade's re-anchored `/tx`, or another
                // courier). The served confirmation never flickers.
                if mode.reanchor_first {
                    if let Some(fetcher) = fetcher {
                        match reanchor_from_ladder(pot_storage, fetcher, &rec, spender).await {
                            LadderReanchor::Reanchored => {
                                summary.reanchored_from_courier += 1;
                                last_judged = Some(key);
                                continue;
                            }
                            LadderReanchor::BudgetExhausted => {
                                summary.budget_exhausted = true;
                                summary.exhausted = false;
                                summary.next_cursor = last_judged;
                                push_log(&format!(
                                    "[reorg] {}:{} the ladder's budget is spent before {spender}'s re-anchor ask: the page stops here, resumed on a fresh budget (nothing demoted blind of the couriers)",
                                    rec.txid, rec.output_index
                                ));
                                break;
                            }
                            LadderReanchor::Fault(e) => {
                                summary.faults += 1;
                                push_log(&format!(
                                    "[reorg] {}:{} header READ FAULT while the ladder re-anchored {spender}: not a verdict, nothing changed: {e}",
                                    rec.txid, rec.output_index
                                ));
                                last_judged = Some(key);
                                continue;
                            }
                            LadderReanchor::NoCanonicalProof => {}
                        }
                    }
                }
                // round 2 (review MED-1): the demotion is bound to the height
                // the refuted bump was judged at; a row another writer moved
                // meanwhile is a guard MISS, never a demotion
                match pot_storage
                    .demote_confirmed_for_spender_at(&rec.txid, rec.output_index, spender, rec.spent_height)
                    .await
                {
                    Ok(true) => {
                        summary.stale += 1;
                        push_log(&format!(
                            "[reorg] {}:{} STALE PROOF — the header source refutes {spender}'s stored bump at {:?}; demoted to SEEN",
                            rec.txid, rec.output_index, rec.spent_height
                        ));
                        if let Err(e) = pot_storage.unlatch_pot_beef_proof(spender).await {
                            summary.errors += 1;
                            push_log(&format!("[reorg] {spender} unlatch failed: {e}"));
                        }
                    }
                    Ok(false) => summary.demote_missed += 1,
                    Err(e) => {
                        summary.errors += 1;
                        push_log(&format!("[reorg] {}:{} demote CAS failed: {e}", rec.txid, rec.output_index));
                    }
                }
            }
        }
        last_judged = Some(key);
    }
    summary.memo_reads = memo.reads().saturating_sub(reads_before);
    summary
}

/// The routine sweep's MED-4 courier re-check for ONE `no_stored_proof`
/// confirmed row: re-ask the ladder once and record the outcome on
/// `summary`. Evidence-driven: only a proof at a DIFFERENT anchor demotes;
/// an agreeing proof heals (stitched into the stored BEEF, when there is
/// one with the raw); a fault or an unprovable answer changes nothing.
async fn courier_recheck(
    pot_storage: &dyn PotStorage,
    fetcher: Option<&dyn AncestorFetcher>,
    rec: &overlay_discovery::pot::storage::PotRecord,
    spender: &str,
    stored: Option<&[u8]>,
    summary: &mut ReverifyPassSummary,
) {
    let Some(fetcher) = fetcher else {
        summary.no_stored_proof += 1;
        return;
    };
    let proof_hex = match fetcher.verified_proof_for_detailed(spender).await {
        Ok(Some(hex)) => hex,
        Ok(None) => {
            // the ladder cannot prove it right now: unknown, never demote
            summary.no_stored_proof += 1;
            return;
        }
        Err(e) => {
            summary.faults += 1;
            push_log(&format!("[reorg] {spender} courier re-check faulted — not a verdict: {e}"));
            return;
        }
    };
    let proof_height = MerklePath::from_hex(&proof_hex).ok().map(|mp| u64::from(mp.block_height));
    match proof_height {
        Some(h) if rec.spent_height == Some(h) => {
            // agree: heal by stitching the proof into the stored BEEF (needs
            // the raw, which the stored proofless BEEF carries). No stored
            // BEEF ⇒ nothing to stitch, leave it.
            match stored.and_then(|bytes| crate::proof_fetcher::stitch_and_trim_pot_beef(spender, bytes, &proof_hex)) {
                Some(compacted) => match pot_storage.compact_pot_beef(spender, &compacted).await {
                    Ok(()) => {
                        summary.stored_from_courier += 1;
                        push_log(&format!(
                            "[reorg] {}:{} HEALED — the ladder's proof for {spender} at {h} agrees; stitched into the store",
                            rec.txid, rec.output_index
                        ));
                    }
                    Err(e) => {
                        summary.errors += 1;
                        push_log(&format!("[reorg] {spender} heal write failed: {e}"));
                    }
                },
                None => summary.no_stored_proof += 1,
            }
        }
        Some(h) => {
            // disagree: the confirming block was orphaned — demote + unlatch
            // (bound to the judged height, round 2 review MED-1)
            match pot_storage
                .demote_confirmed_for_spender_at(&rec.txid, rec.output_index, spender, rec.spent_height)
                .await
            {
                Ok(true) => {
                    summary.stale += 1;
                    push_log(&format!(
                        "[reorg] {}:{} STALE — the ladder proves {spender} at {h}, not the confirmed {:?}; demoted to SEEN",
                        rec.txid, rec.output_index, rec.spent_height
                    ));
                    if let Err(e) = pot_storage.unlatch_pot_beef_proof(spender).await {
                        summary.errors += 1;
                        push_log(&format!("[reorg] {spender} unlatch failed: {e}"));
                    }
                }
                Ok(false) => summary.demote_missed += 1,
                Err(e) => {
                    summary.errors += 1;
                    push_log(&format!("[reorg] {}:{} demote CAS failed: {e}", rec.txid, rec.output_index));
                }
            }
        }
        None => summary.no_stored_proof += 1,
    }
}

/// A detected reorg at a height, or the operator's window (`POST
/// /internal/reorg`): the evidence-driven pass with proofless rows demoted
/// blind (review M1: never by height alone; a canonical row is untouched
/// and counted).
pub async fn handle_reorg(
    pot_storage: &dyn PotStorage,
    tracker: Option<&dyn ChainTracker>,
    from_height: u64,
    to_height: u64,
    after: Option<RowKey>,
    limit: u64,
) -> ReverifyPassSummary {
    // The operator/reorg window demotes proofless rows blind (the caller has
    // independent reason), so no courier is needed here.
    reverify_window(pot_storage, tracker, None, from_height, to_height, after, limit, true).await
}

/// What one bounded pass over a proof store's window found and did (the
/// pot_beefs and transactions legs).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ProofLegSummary {
    pub scanned: usize,
    pub standing: usize,
    /// Refuted stitched bumps unlatched / un-proved for the completion pass.
    pub stale: usize,
    /// Rows whose bytes carry no bump for their own txid (nothing to verify).
    pub no_bump: usize,
    pub faults: usize,
    pub errors: usize,
    pub exhausted: bool,
    pub next_cursor: Option<RowKey>,
    /// Distinct (height, root) chaintracks reads this pass (review MED-3).
    pub memo_reads: usize,
}

/// The pot_beefs leg: the pots' OWN verified bumps in the window.
pub async fn reverify_pot_beefs_window(
    pot_storage: &dyn PotStorage,
    tracker: Option<&dyn ChainTracker>,
    lo: u64,
    hi: u64,
    after: Option<RowKey>,
    limit: u64,
) -> ProofLegSummary {
    reverify_pot_beefs_window_with(pot_storage, tracker, lo, hi, after, limit, &mut RootMemo::default()).await
}

/// [`reverify_pot_beefs_window`] over a SHARED per-pass memo (round 2, MED-5).
pub async fn reverify_pot_beefs_window_with(
    pot_storage: &dyn PotStorage,
    tracker: Option<&dyn ChainTracker>,
    lo: u64,
    hi: u64,
    after: Option<RowKey>,
    limit: u64,
    memo: &mut RootMemo,
) -> ProofLegSummary {
    let reads_before = memo.reads();
    let mut summary = ProofLegSummary { next_cursor: after, ..Default::default() };
    let Some(tracker) = tracker else {
        return summary;
    };
    let rows = match pot_storage.verified_pot_beefs_page(lo, hi, after, limit).await {
        Ok(rows) => rows,
        Err(e) => {
            summary.errors += 1;
            push_log(&format!("[reorg] pot_beefs window {lo}..={hi} read failed: {e}"));
            return summary;
        }
    };
    summary.scanned = rows.len();
    summary.exhausted = (rows.len() as u64) < limit;
    if let Some((key, _, _)) = rows.last() {
        summary.next_cursor = Some(*key);
    }
    for (_, txid, beef) in rows {
        let Some(bump_hex) = stored_bump_hex(&beef, &txid) else {
            summary.no_bump += 1;
            continue;
        };
        let answer = verify_bump_memoized(tracker, memo, &bump_hex, &txid).await;
        match classify_reverify(&answer) {
            ReverifyVerdict::Standing => summary.standing += 1,
            ReverifyVerdict::NoStoredProof => summary.no_bump += 1,
            ReverifyVerdict::Fault => summary.faults += 1,
            ReverifyVerdict::Stale => match pot_storage.unlatch_pot_beef_proof(&txid).await {
                Ok(()) => {
                    summary.stale += 1;
                    push_log(&format!(
                        "[reorg] pot {txid} STALE PROOF — the header source refutes its stored bump; unlatched for the completion pass"
                    ));
                }
                Err(e) => {
                    summary.errors += 1;
                    push_log(&format!("[reorg] pot {txid} unlatch failed: {e}"));
                }
            },
        }
    }
    summary.memo_reads = memo.reads().saturating_sub(reads_before);
    summary
}

/// The engine's transactions store as the sweep's third leg sees it: a
/// keyed page of PROVEN rows in a height window, and the un-prove write.
pub trait ProvenTxStore {
    fn proven_window_page(
        &self,
        lo: u64,
        hi: u64,
        after: Option<RowKey>,
        limit: u64,
    ) -> impl std::future::Future<Output = Result<Vec<(RowKey, String, Vec<u8>)>, String>>;
    fn unprove(&self, txid: &str) -> impl std::future::Future<Output = Result<(), String>>;
}

/// The transactions leg: the engine's stitched hop proofs in the window.
pub async fn reverify_transactions_window<S: ProvenTxStore>(
    store: &S,
    tracker: Option<&dyn ChainTracker>,
    lo: u64,
    hi: u64,
    after: Option<RowKey>,
    limit: u64,
) -> ProofLegSummary {
    reverify_transactions_window_with(store, tracker, lo, hi, after, limit, &mut RootMemo::default()).await
}

/// [`reverify_transactions_window`] over a SHARED per-pass memo (round 2, MED-5).
pub async fn reverify_transactions_window_with<S: ProvenTxStore>(
    store: &S,
    tracker: Option<&dyn ChainTracker>,
    lo: u64,
    hi: u64,
    after: Option<RowKey>,
    limit: u64,
    memo: &mut RootMemo,
) -> ProofLegSummary {
    let reads_before = memo.reads();
    let mut summary = ProofLegSummary { next_cursor: after, ..Default::default() };
    let Some(tracker) = tracker else {
        return summary;
    };
    let rows = match store.proven_window_page(lo, hi, after, limit).await {
        Ok(rows) => rows,
        Err(e) => {
            summary.errors += 1;
            push_log(&format!("[reorg] transactions window {lo}..={hi} read failed: {e}"));
            return summary;
        }
    };
    summary.scanned = rows.len();
    summary.exhausted = (rows.len() as u64) < limit;
    if let Some((key, _, _)) = rows.last() {
        summary.next_cursor = Some(*key);
    }
    for (_, txid, beef) in rows {
        let Some(bump_hex) = stored_bump_hex(&beef, &txid) else {
            summary.no_bump += 1;
            continue;
        };
        let answer = verify_bump_memoized(tracker, memo, &bump_hex, &txid).await;
        match classify_reverify(&answer) {
            ReverifyVerdict::Standing => summary.standing += 1,
            ReverifyVerdict::NoStoredProof => summary.no_bump += 1,
            ReverifyVerdict::Fault => summary.faults += 1,
            ReverifyVerdict::Stale => match store.unprove(&txid).await {
                Ok(()) => {
                    summary.stale += 1;
                    push_log(&format!(
                        "[reorg] tx {txid} STALE PROOF — the header source refutes its stitched bump; un-proved for the engine's completion pass"
                    ));
                }
                Err(e) => {
                    summary.errors += 1;
                    push_log(&format!("[reorg] tx {txid} un-prove failed: {e}"));
                }
            },
        }
    }
    summary.memo_reads = memo.reads().saturating_sub(reads_before);
    summary
}

/// The D1 `transactions` table as a [`ProvenTxStore`].
pub struct D1ProvenTxStore<'a>(pub &'a worker::D1Database);

impl ProvenTxStore for D1ProvenTxStore<'_> {
    async fn proven_window_page(
        &self,
        lo: u64,
        hi: u64,
        after: Option<RowKey>,
        limit: u64,
    ) -> Result<Vec<(RowKey, String, Vec<u8>)>, String> {
        #[derive(serde::Deserialize)]
        struct Row {
            #[serde(rename = "rowKey")]
            row_key: f64,
            #[serde(rename = "proofHeight", default)]
            proof_height: Option<f64>,
            txid: String,
            beef: Option<String>,
        }
        fn keyed(rows: Vec<Row>, out: &mut Vec<(RowKey, String, Vec<u8>)>) {
            for r in rows {
                let Some(beef) = r.beef.and_then(|h| hex::decode(h).ok()) else { continue };
                out.push((
                    RowKey { height: r.proof_height.unwrap_or(0.0) as u64, rowid: r.row_key as i64 },
                    r.txid,
                    beef,
                ));
            }
        }
        let mut out = Vec::new();
        if limit == 0 || lo > hi {
            return Ok(out);
        }
        let (below_hi, remaining) = match after {
            Some(a) => {
                let rows: Vec<Row> =
                    crate::d1::Query::new(crate::d1_storage::transactions_proven_same_height_sql(limit))
                        .bind(a.height as f64)
                        .bind(a.rowid as f64)
                        .fetch_all(self.0)
                        .await
                        .map_err(|e| e.to_string())?;
                keyed(rows, &mut out);
                let remaining = limit.saturating_sub(out.len() as u64);
                if remaining == 0 || a.height <= lo {
                    return Ok(out);
                }
                (a.height - 1, remaining)
            }
            None => (hi, limit),
        };
        let rows: Vec<Row> = crate::d1::Query::new(crate::d1_storage::transactions_proven_head_sql(remaining))
            .bind(lo as f64)
            .bind(below_hi as f64)
            .fetch_all(self.0)
            .await
            .map_err(|e| e.to_string())?;
        keyed(rows, &mut out);
        Ok(out)
    }

    async fn unprove(&self, txid: &str) -> Result<(), String> {
        crate::d1::Query::new(crate::d1_storage::TRANSACTION_UNPROVE_SQL)
            .bind(txid)
            .execute(self.0)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

/// What the revalidation sweep did this pass, per leg.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReorgSweepSummary {
    pub spenders: ReverifyPassSummary,
    pub pot_beefs: ProofLegSummary,
    pub transactions: ProofLegSummary,
    /// The window each leg walked, `(lo, hi)`, when it ran.
    pub spenders_window: Option<(u64, u64)>,
    pub pot_beefs_window: Option<(u64, u64)>,
    pub transactions_window: Option<(u64, u64)>,
    /// Walk-state read/write faults (the leg still ran from what it had).
    pub state_errors: usize,
}

/// The revalidation sweep, run on EVERY block-event pass (the Workers-native
/// primary path; the reference's sweep is its fallback "for chain trackers
/// without a reorg event stream"): each leg reads its persisted walk,
/// decides its window ([`next_sweep_window`]: anchored until exhausted,
/// continuous, restarting from the last `depth` heights), examines at most
/// `limit` rows, and persists where it stopped. No header source: nothing
/// examined, no state moved.
#[allow(clippy::too_many_arguments)] // two stores, the two chain sources, the window bounds
pub async fn reorg_revalidation_sweep<S: ProvenTxStore>(
    pot_storage: &dyn PotStorage,
    tx_store: Option<&S>,
    tracker: Option<&dyn ChainTracker>,
    fetcher: Option<&dyn AncestorFetcher>,
    tip: u64,
    depth: u64,
    limit: u64,
) -> ReorgSweepSummary {
    reorg_revalidation_sweep_with(pot_storage, tx_store, tracker, fetcher, tip, depth, limit, &mut RootMemo::default()).await
}

/// [`reorg_revalidation_sweep`] over a SHARED per-pass memo (round 2, review
/// MED-5): the block-event pass hands the Arcade event consumer's memo on,
/// so a height the consumer seeded or read is never asked again here.
#[allow(clippy::too_many_arguments)] // two stores, the two chain sources, the window bounds, the memo
pub async fn reorg_revalidation_sweep_with<S: ProvenTxStore>(
    pot_storage: &dyn PotStorage,
    tx_store: Option<&S>,
    tracker: Option<&dyn ChainTracker>,
    fetcher: Option<&dyn AncestorFetcher>,
    tip: u64,
    depth: u64,
    limit: u64,
    memo: &mut RootMemo,
) -> ReorgSweepSummary {
    let mut out = ReorgSweepSummary::default();
    if tracker.is_none() {
        push_log("[reorg-sweep] no header source configured — nothing re-verified, no walk moved");
        return out;
    }
    async fn plan(
        pot_storage: &dyn PotStorage,
        name: &str,
        tip: u64,
        depth: u64,
        state_errors: &mut usize,
    ) -> Option<SweepState> {
        let prev = match pot_storage.read_sweep_state(name).await {
            Ok(p) => p,
            Err(e) => {
                *state_errors += 1;
                push_log(&format!("[reorg-sweep] {name} walk state read failed ({e}); walking the newest window"));
                None
            }
        };
        next_sweep_window(prev.as_ref(), tip, depth)
    }
    async fn persist(
        pot_storage: &dyn PotStorage,
        name: &str,
        window: &SweepState,
        exhausted: bool,
        next_cursor: Option<RowKey>,
        state_errors: &mut usize,
    ) {
        let state = SweepState {
            lo: window.lo,
            hi: window.hi,
            cursor: if exhausted { None } else { next_cursor },
            exhausted,
        };
        if let Err(e) = pot_storage.write_sweep_state(name, &state).await {
            *state_errors += 1;
            push_log(&format!("[reorg-sweep] {name} walk state write failed: {e}"));
        }
    }

    if let Some(w) = plan(pot_storage, WALK_SPENDERS, tip, depth, &mut out.state_errors).await {
        out.spenders_window = Some((w.lo, w.hi));
        out.spenders = reverify_window_with(
            pot_storage,
            tracker,
            fetcher,
            w.lo,
            w.hi,
            w.cursor,
            limit,
            ReverifyMode { demote_proofless: false, reanchor_first: false },
            memo,
        )
        .await;
        persist(pot_storage, WALK_SPENDERS, &w, out.spenders.exhausted, out.spenders.next_cursor, &mut out.state_errors).await;
    }
    if let Some(w) = plan(pot_storage, WALK_POT_BEEFS, tip, depth, &mut out.state_errors).await {
        out.pot_beefs_window = Some((w.lo, w.hi));
        out.pot_beefs = reverify_pot_beefs_window_with(pot_storage, tracker, w.lo, w.hi, w.cursor, limit, memo).await;
        persist(pot_storage, WALK_POT_BEEFS, &w, out.pot_beefs.exhausted, out.pot_beefs.next_cursor, &mut out.state_errors).await;
    }
    if let Some(store) = tx_store {
        if let Some(w) = plan(pot_storage, WALK_TRANSACTIONS, tip, depth, &mut out.state_errors).await {
            out.transactions_window = Some((w.lo, w.hi));
            out.transactions = reverify_transactions_window_with(store, tracker, w.lo, w.hi, w.cursor, limit, memo).await;
            persist(pot_storage, WALK_TRANSACTIONS, &w, out.transactions.exhausted, out.transactions.next_cursor, &mut out.state_errors).await;
        }
    }
    out
}

/// What an Arcade `reorg_unmined` HINT led to.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct UnminedHintSummary {
    /// Confirmed rows whose stored proof chaintracks REFUTES: demoted, unlatched.
    pub demoted: usize,
    /// Rows whose stored proof chaintracks still HOLDS: the hint is not
    /// corroborated, nothing changed (a planted or stale marker).
    pub uncorroborated: usize,
    /// Rows with no stored proof to check: nothing changed.
    pub no_stored_proof: usize,
    pub faults: usize,
    pub errors: usize,
}

/// Arcade's `reorg_unmined` for a spender, as a HINT (review H2): the
/// `/arc-ingest` bearer is the public txid, so the marker is plantable and
/// by itself changes nothing. Each confirmed row the spender points at has
/// its stored bump re-verified against chaintracks; only a REFUTED one is
/// demoted (guarded) and unlatched.
pub async fn unmined_hint(
    pot_storage: &dyn PotStorage,
    tracker: Option<&dyn ChainTracker>,
    spender: &str,
) -> UnminedHintSummary {
    let mut summary = UnminedHintSummary::default();
    let Some(tracker) = tracker else {
        push_log(&format!("[arc-ingest] {spender} reorg_unmined: no header source configured — the hint changes nothing"));
        return summary;
    };
    let rows = match pot_storage.find_confirmed_by_spending_txid(spender).await {
        Ok(rows) => rows,
        Err(e) => {
            summary.errors += 1;
            push_log(&format!("[arc-ingest] {spender} reorg_unmined: confirmed lookup failed: {e}"));
            return summary;
        }
    };
    if rows.is_empty() {
        return summary;
    }
    let stored = match pot_storage.get_beef(spender).await {
        Ok(b) => b,
        Err(e) => {
            summary.errors += 1;
            push_log(&format!("[arc-ingest] {spender} reorg_unmined: pot-beef read failed: {e}"));
            return summary;
        }
    };
    let Some(bump_hex) = stored.as_deref().and_then(|b| stored_bump_hex(b, spender)) else {
        summary.no_stored_proof += rows.len();
        push_log(&format!("[arc-ingest] {spender} reorg_unmined: no stored proof to re-verify — the hint changes nothing"));
        return summary;
    };
    let answer = verify_bump_detailed(Some(tracker), &bump_hex, spender).await;
    match classify_reverify(&answer) {
        ReverifyVerdict::Standing => {
            summary.uncorroborated += rows.len();
            push_log(&format!(
                "[arc-ingest] {spender} reorg_unmined UNCORROBORATED — chaintracks still holds its stored bump's root; nothing changed"
            ));
        }
        ReverifyVerdict::NoStoredProof => summary.no_stored_proof += rows.len(),
        ReverifyVerdict::Fault => {
            summary.faults += 1;
            push_log(&format!(
                "[arc-ingest] {spender} reorg_unmined: header READ FAULT ({}) — not a verdict, nothing changed",
                answer.as_ref().err().map(String::as_str).unwrap_or("?")
            ));
        }
        ReverifyVerdict::Stale => {
            for rec in rows {
                match pot_storage
                    .demote_confirmed_for_spender(&rec.txid, rec.output_index, spender)
                    .await
                {
                    Ok(true) => {
                        summary.demoted += 1;
                        push_log(&format!(
                            "[arc-ingest] {}:{} demoted to SEEN — Arcade reports {spender} reorg_unmined and chaintracks refutes its stored bump",
                            rec.txid, rec.output_index
                        ));
                    }
                    Ok(false) => {}
                    Err(e) => {
                        summary.errors += 1;
                        push_log(&format!("[arc-ingest] {}:{} reorg_unmined demote failed: {e}", rec.txid, rec.output_index));
                    }
                }
            }
            if summary.demoted > 0 {
                if let Err(e) = pot_storage.unlatch_pot_beef_proof(spender).await {
                    summary.errors += 1;
                    push_log(&format!("[arc-ingest] {spender} unlatch failed: {e}"));
                }
            }
        }
    }
    summary
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;
    use crate::proof_fetcher::apply_pushed_proof_to_pot_stores;
    use crate::proof_fetcher::tests::{real_spender_raw, single_tx_bump};
    use bsv_rs::transaction::{ChainTrackerError, MockChainTracker, Transaction};
    use overlay_discovery::pot::storage::{MemoryPotStorage, PotRecord};
    use std::sync::Mutex;

    /// A tracker whose header source cannot be read (transport fault).
    struct FaultyTracker;
    #[async_trait::async_trait]
    impl ChainTracker for FaultyTracker {
        async fn is_valid_root_for_height(&self, _root: &str, _height: u32) -> Result<bool, ChainTrackerError> {
            Err(ChainTrackerError::NetworkError("starved".into()))
        }
        async fn current_height(&self) -> Result<u32, ChainTrackerError> {
            Err(ChainTrackerError::NetworkError("starved".into()))
        }
    }

    /// A tracker that holds a SET of (height, root) pairs (the mock holds one
    /// root per height; 158 single-leaf fixtures at three heights need one
    /// per row) and RECORDS every root it was asked about (a single-leaf
    /// bump's root is its txid, so the record names the rows examined).
    struct RecordingTracker {
        valid: std::collections::HashSet<(u32, String)>,
        asked: Mutex<Vec<String>>,
    }
    #[async_trait::async_trait]
    impl ChainTracker for RecordingTracker {
        async fn is_valid_root_for_height(&self, root: &str, height: u32) -> Result<bool, ChainTrackerError> {
            self.asked.lock().unwrap().push(root.to_string());
            Ok(self.valid.contains(&(height, root.to_string())))
        }
        async fn current_height(&self) -> Result<u32, ChainTrackerError> {
            Ok(965_773)
        }
    }

    /// A pot CONFIRMED at `height` by a spender whose stored pot BEEF carries
    /// a single-leaf bump at that height (root = the spender txid), latched
    /// verified with its anchor. Returns the spender txid.
    async fn confirmed_pot_with_stored_proof(store: &MemoryPotStorage, pot: &str, height: u32) -> String {
        confirmed_pot_with_proof_at(store, pot, height, true).await
    }

    /// `anchored = false` latches the spender's proof WITHOUT its
    /// `proofHeight` (a row verified before the column existed): the
    /// pot_beefs leg never pages it, so a pin about the spenders leg alone
    /// can count the header source's questions.
    async fn confirmed_pot_with_proof_at(store: &MemoryPotStorage, pot: &str, height: u32, anchored: bool) -> String {
        let raw = real_spender_raw(pot, 0);
        let spender = Transaction::from_hex(&raw).unwrap().id();
        store.store_record(&PotRecord { txid: pot.into(), output_index: 0, ..Default::default() }).await.unwrap();
        store.mark_spent(pot, 0, &spender, true, None, Some(u64::from(height)), Some(true)).await.unwrap();
        let bump_hex = single_tx_bump(&spender, height).to_hex();
        let beef = crate::proof_fetcher::assemble_spender_beef(&raw, &bump_hex, &spender).unwrap();
        store.store_beef(&spender, &beef).await.unwrap();
        store
            .mark_pot_beef_proven_at(&spender, anchored.then_some(u64::from(height)))
            .await
            .unwrap();
        spender
    }

    /// A pot confirmed by a courier answer: no stored spender BEEF at all.
    async fn confirmed_pot_without_stored_proof(store: &MemoryPotStorage, pot: &str, height: u32) -> String {
        let spender = format!("{}{}", &pot[..32], "c".repeat(32));
        store.store_record(&PotRecord { txid: pot.into(), output_index: 0, ..Default::default() }).await.unwrap();
        store.mark_spent(pot, 0, &spender, true, None, Some(u64::from(height)), Some(true)).await.unwrap();
        spender
    }

    fn pot(n: u32) -> String {
        format!("{n:064x}")
    }

    /// A courier fetcher stub: a per-spender configured
    /// `verified_proof_for_detailed` answer (review MED-4). `fetch_ancestor`
    /// is required by the trait but never reached by the sweep's re-check.
    struct CourierStub(std::collections::HashMap<String, Result<Option<String>, String>>);
    #[async_trait::async_trait(?Send)]
    impl AncestorFetcher for CourierStub {
        async fn fetch_ancestor(&self, _txid: &str) -> Result<overlay_engine::gasp::FetchedAncestor, overlay_engine::gasp::GASPError> {
            Err(overlay_engine::gasp::GASPError::NodeNotFound("stub".into()))
        }
        async fn verified_proof_for_detailed(&self, txid: &str) -> Result<Option<String>, String> {
            self.0.get(txid).cloned().unwrap_or(Ok(None))
        }
    }

    /// A pot confirmed at `height` whose stored spender BEEF is PROOFLESS
    /// (the raw only, no bump) — the courier-confirmed shape the routine
    /// sweep must re-verify (review MED-4).
    async fn confirmed_pot_with_proofless_beef(store: &MemoryPotStorage, pot: &str, height: u32) -> String {
        let raw = real_spender_raw(pot, 0);
        let spender = Transaction::from_hex(&raw).unwrap().id();
        store.store_record(&PotRecord { txid: pot.into(), output_index: 0, ..Default::default() }).await.unwrap();
        store.mark_spent(pot, 0, &spender, true, None, Some(u64::from(height)), Some(true)).await.unwrap();
        let mut beef = Beef::new();
        beef.merge_raw_tx(hex::decode(&raw).unwrap(), None);
        store.store_beef(&spender, &beef.to_binary()).await.unwrap();
        spender
    }

    /// bsv-low M19 R2 round 3 (review MED-3): the per-pass memo collapses a
    /// block's worth of re-verifies to ONE chaintracks read — the same
    /// (height, root) is asked once, a different root asks again.
    #[tokio::test]
    async fn the_root_memo_asks_chaintracks_once_per_height_root() {
        let raw = real_spender_raw(&pot(1), 0);
        let spender = Transaction::from_hex(&raw).unwrap().id();
        let bump_hex = single_tx_bump(&spender, 965_771).to_hex();
        let tracker = RecordingTracker { valid: [(965_771u32, spender.clone())].into_iter().collect(), asked: Mutex::new(Vec::new()) };
        let mut memo = RootMemo::default();
        for _ in 0..50 {
            assert_eq!(verify_bump_memoized(&tracker, &mut memo, &bump_hex, &spender).await, Ok(true));
        }
        assert_eq!(tracker.asked.lock().unwrap().len(), 1, "50 rows of one block collapse to one read");
        assert_eq!(memo.reads(), 1);
        // a different (height, root) is a second read
        let other = real_spender_raw(&pot(2), 0);
        let other_id = Transaction::from_hex(&other).unwrap().id();
        let _ = verify_bump_memoized(&tracker, &mut memo, &single_tx_bump(&other_id, 965_771).to_hex(), &other_id).await;
        assert_eq!(tracker.asked.lock().unwrap().len(), 2);
        assert_eq!(memo.reads(), 2);
    }

    /// bsv-low M19 R2 round 3 (review MED-4): the routine sweep re-asks the
    /// ladder for a courier-confirmed (no-stored-bump) row. An agreeing
    /// proof HEALS it (stitched into the store, verifiable next pass); a
    /// disagreeing proof DEMOTES it; a fault or an unprovable answer or no
    /// courier changes nothing (unknown never demotes).
    #[tokio::test]
    async fn the_routine_sweep_rechecks_courier_confirmed_rows_and_only_a_disagreement_demotes() {
        let store = MemoryPotStorage::new();
        let heal = confirmed_pot_with_proofless_beef(&store, &pot(60), 965_771).await;
        let orphaned = confirmed_pot_with_proofless_beef(&store, &pot(61), 965_771).await;
        let quiet = confirmed_pot_with_proofless_beef(&store, &pot(62), 965_771).await;
        let faulted = confirmed_pot_with_proofless_beef(&store, &pot(63), 965_771).await;
        // the ladder's per-spender answers
        let agree = single_tx_bump(&heal, 965_771).to_hex();
        let disagree = single_tx_bump(&orphaned, 965_773).to_hex();
        let fetcher = CourierStub(
            [
                (heal.clone(), Ok(Some(agree))),
                (orphaned.clone(), Ok(Some(disagree))),
                (quiet.clone(), Ok(None)),
                (faulted.clone(), Err("chaintracks starved".into())),
            ]
            .into_iter()
            .collect(),
        );
        let tracker = MockChainTracker::new(965_775);
        let s = reverify_window(&store, Some(&tracker), Some(&fetcher), 965_771, 965_773, None, 50, false).await;
        assert_eq!(
            (s.scanned, s.stored_from_courier, s.stale, s.no_stored_proof, s.faults),
            (4, 1, 1, 1, 1),
            "{s:?}"
        );
        // heal: still confirmed, and now carries a stored bump (verifiable next pass)
        assert!(store.get_spent_status(&pot(60), 0).await.unwrap().unwrap().spent_confirmed);
        assert!(store.get_beef(&heal).await.unwrap().and_then(|b| stored_bump_hex(&b, &heal)).is_some(), "healed: a bump is now stored");
        // orphaned: demoted + unlatched
        let o = store.get_spent_status(&pot(61), 0).await.unwrap().unwrap();
        assert!(o.spent && !o.spent_confirmed && o.spent_height.is_none());
        // quiet + faulted: untouched
        assert!(store.get_spent_status(&pot(62), 0).await.unwrap().unwrap().spent_confirmed);
        assert!(store.get_spent_status(&pot(63), 0).await.unwrap().unwrap().spent_confirmed);
        // no courier configured: nothing re-asked, nothing demoted
        let s2 = reverify_window(&store, Some(&tracker), None, 965_771, 965_773, None, 50, false).await;
        assert_eq!(s2.stored_from_courier, 0);
        assert!(s2.no_stored_proof >= 2, "quiet + faulted stay no_stored_proof: {s2:?}");
    }

    /// bsv-low M19 R2 round 3 (review L1): a STANDING row whose stored bump
    /// verifies at a different height than the row's `spentHeight` (confirmed
    /// from an orphan bump at H1, re-proved at H2) is re-anchored to the
    /// bump's height, not demoted.
    #[tokio::test]
    async fn a_standing_row_with_a_stale_height_is_reanchored_not_demoted() {
        let store = MemoryPotStorage::new();
        let raw = real_spender_raw(&pot(70), 0);
        let spender = Transaction::from_hex(&raw).unwrap().id();
        store.store_record(&PotRecord { txid: pot(70), output_index: 0, ..Default::default() }).await.unwrap();
        // confirmed at the STALE height 965771 …
        store.mark_spent(&pot(70), 0, &spender, true, None, Some(965_771), Some(true)).await.unwrap();
        // … but the stored, verified bump is the canonical one at 965773
        let beef = crate::proof_fetcher::assemble_spender_beef(&raw, &single_tx_bump(&spender, 965_773).to_hex(), &spender).unwrap();
        store.store_beef(&spender, &beef).await.unwrap();
        store.mark_pot_beef_proven_at(&spender, Some(965_773)).await.unwrap();
        let mut tracker = MockChainTracker::new(965_775);
        tracker.add_root(965_773, spender.clone());
        let s = reverify_window(&store, Some(&tracker), None, 965_771, 965_773, None, 50, false).await;
        assert_eq!((s.standing, s.reanchored, s.stale), (1, 1, 0), "{s:?}");
        let r = store.get_spent_status(&pot(70), 0).await.unwrap().unwrap();
        assert!(r.spent_confirmed && r.spent_height == Some(965_773), "the height moved to the bump's, the row stays confirmed");
        // a second pass finds it standing at the right height, nothing to move
        let s2 = reverify_window(&store, Some(&tracker), None, 965_771, 965_773, None, 50, false).await;
        assert_eq!((s2.standing, s2.reanchored), (1, 0));
    }

    /// bsv-low M19B-G1: the re-anchor-first arm of `reverify_window_with`.
    /// A REFUTED row asks the ladder once: a chaintracks-verified proof for
    /// another block replaces the stored bump and moves the height IN PLACE
    /// (confirmed throughout, the latch kept); `Ok(None)` demotes (the plain
    /// arm); a ladder FAULT changes nothing and counts. The R2 wrapper
    /// (`reverify_window`) never asks the ladder for a refuted row.
    #[tokio::test]
    async fn reanchor_first_replaces_a_refuted_bump_from_a_verified_courier_proof_or_demotes() {
        let store = MemoryPotStorage::new();
        let mut tracker = MockChainTracker::new(965_860);
        let reproven = confirmed_pot_with_stored_proof(&store, &pot(80), 965_771).await;
        tracker.add_root(965_773, reproven.clone());
        let unproven = confirmed_pot_with_stored_proof(&store, &pot(81), 965_771).await;
        let faulted = confirmed_pot_with_stored_proof(&store, &pot(82), 965_771).await;
        let ladder = CourierStub(
            [
                (reproven.clone(), Ok(Some(single_tx_bump(&reproven, 965_773).to_hex()))),
                (unproven.clone(), Ok(None)),
                (faulted.clone(), Err("chaintracks starved".into())),
            ]
            .into_iter()
            .collect(),
        );
        let mode = ReverifyMode { demote_proofless: false, reanchor_first: true };
        let s = reverify_window_with(&store, Some(&tracker), Some(&ladder), 965_771, 965_771, None, 50, mode, &mut RootMemo::default()).await;
        assert_eq!((s.scanned, s.reanchored_from_courier, s.stale, s.faults, s.standing), (3, 1, 1, 1, 0), "{s:?}");
        let r = store.get_spent_status(&pot(80), 0).await.unwrap().unwrap();
        assert!(r.spent_confirmed && r.spent_height == Some(965_773), "re-anchored in place: {r:?}");
        assert_eq!(stored_bump_anchor(&store.get_beef(&reproven).await.unwrap().unwrap(), &reproven), Some(BumpAnchor { height: 965_773, root: reproven.clone() }));
        assert!(store.pot_beef_proof_verified(&reproven).await.unwrap(), "the latch is kept across the replacement");
        let r = store.get_spent_status(&pot(81), 0).await.unwrap().unwrap();
        assert!(r.spent && !r.spent_confirmed, "no canonical proof served: demoted");
        assert!(!store.pot_beef_proof_verified(&unproven).await.unwrap());
        let r = store.get_spent_status(&pot(82), 0).await.unwrap().unwrap();
        assert!(r.spent_confirmed && r.spent_height == Some(965_771), "a ladder fault is not a verdict");
        assert!(store.pot_beef_proof_verified(&faulted).await.unwrap());
        // a second pass: the re-anchored row stands (its stored bump is canonical now), the faulted one is asked again
        let s2 = reverify_window_with(&store, Some(&tracker), Some(&ladder), 965_771, 965_773, None, 50, mode, &mut RootMemo::default()).await;
        assert_eq!((s2.scanned, s2.standing, s2.reanchored_from_courier, s2.faults), (2, 1, 0, 1), "{s2:?}");
        // the R2 wrapper: the same refuted row takes the plain arm, the ladder is never asked
        let store2 = MemoryPotStorage::new();
        let refuted = confirmed_pot_with_stored_proof(&store2, &pot(83), 965_771).await;
        let ladder2 = CourierStub([(refuted.clone(), Ok(Some(single_tx_bump(&refuted, 965_773).to_hex())))].into_iter().collect());
        let s3 = reverify_window(&store2, Some(&tracker), Some(&ladder2), 965_771, 965_771, None, 50, false).await;
        assert_eq!((s3.stale, s3.reanchored_from_courier), (1, 0), "the R2 arm demotes; it never re-anchors from the ladder: {s3:?}");
        assert!(!store2.get_spent_status(&pot(83), 0).await.unwrap().unwrap().spent_confirmed);
    }

    /// bsv-low M19B-G1 round 2 (review MED-2): a spent ladder budget stops
    /// the walk's page at the last row JUDGED (nothing demoted blind of the
    /// couriers, the cursor set for a fresh budget to resume), on both the
    /// re-anchor-first arm and the routine courier re-check; the pin runs
    /// through the REAL fetcher at budget 0.
    #[tokio::test]
    async fn a_spent_ladder_budget_stops_the_page_at_the_last_judged_row() {
        let store = MemoryPotStorage::new();
        let mut tracker = MockChainTracker::new(965_860);
        let refuted = confirmed_pot_with_stored_proof(&store, &pot(84), 965_771).await;
        let standing = confirmed_pot_with_stored_proof(&store, &pot(85), 965_771).await; // newest rowid: judged first
        tracker.add_root(965_771, standing.clone());
        let real = crate::proof_fetcher::ChainProofFetcher::new(Some(std::rc::Rc::new(MockChainTracker::new(965_860)))).with_budget(0);
        let mode = ReverifyMode { demote_proofless: false, reanchor_first: true };
        let s = reverify_window_with(&store, Some(&tracker), Some(&real), 965_771, 965_771, None, 50, mode, &mut RootMemo::default()).await;
        assert_eq!((s.scanned, s.standing, s.stale, s.budget_exhausted, s.exhausted), (2, 1, 0, true, false), "{s:?}");
        assert!(s.next_cursor.is_some(), "the cursor is the standing row, the last one judged: {s:?}");
        assert!(store.get_spent_status(&pot(84), 0).await.unwrap().unwrap().spent_confirmed, "the refuted row was NOT demoted blind");
        assert!(store.pot_beef_proof_verified(&refuted).await.unwrap());
        // the routine sweep's courier re-check stops the same way for a proofless row
        let store2 = MemoryPotStorage::new();
        let proofless = confirmed_pot_with_proofless_beef(&store2, &pot(86), 965_771).await;
        let s2 = reverify_window(&store2, Some(&tracker), Some(&real), 965_771, 965_773, None, 50, false).await;
        assert_eq!((s2.scanned, s2.budget_exhausted, s2.exhausted, s2.next_cursor, s2.stale), (1, true, false, None, 0), "{s2:?}");
        assert!(store2.get_spent_status(&pot(86), 0).await.unwrap().unwrap().spent_confirmed);
        let _ = proofless;
        // a fresh budget resumes at the cursor and judges the refuted row (no proof served: demoted)
        let fresh = CourierStub([(refuted.clone(), Ok(None))].into_iter().collect());
        let s3 = reverify_window_with(&store, Some(&tracker), Some(&fresh), 965_771, 965_771, s.next_cursor, 50, mode, &mut RootMemo::default()).await;
        assert_eq!((s3.scanned, s3.stale, s3.exhausted), (1, 1, true), "{s3:?}");
    }

    #[test]
    fn a_stored_bump_anchor_is_its_height_and_root() {
        let raw = real_spender_raw(&pot(1), 0);
        let spender = Transaction::from_hex(&raw).unwrap().id();
        let beef = crate::proof_fetcher::assemble_spender_beef(&raw, &single_tx_bump(&spender, 965_771).to_hex(), &spender).unwrap();
        assert_eq!(stored_bump_anchor(&beef, &spender), Some(BumpAnchor { height: 965_771, root: spender.clone() }));
        assert_eq!(stored_bump_anchor(&beef, &pot(2)), None, "another txid: no bump");
        assert_eq!(stored_bump_anchor(&[1, 2, 3], &spender), None, "garbage");
    }

    /// The 2026-09-07 class: two confirmations at 965771, one whose stored
    /// bump the header source still holds and one whose bump names the
    /// orphan's root. Only the refuted one is demoted and unlatched.
    #[tokio::test]
    async fn the_pass_demotes_the_refuted_stored_bump_and_keeps_the_standing_one() {
        let store = MemoryPotStorage::new();
        let spender_a = confirmed_pot_with_stored_proof(&store, &pot(1), 965_771).await;
        let spender_b = confirmed_pot_with_stored_proof(&store, &pot(2), 965_771).await;
        let mut tracker = MockChainTracker::new(965_773);
        tracker.add_root(965_771, spender_a.clone());
        let s = reverify_window(&store, Some(&tracker), None, 965_771, 965_773, None, 50, false).await;
        assert_eq!((s.scanned, s.standing, s.stale, s.faults, s.errors), (2, 1, 1, 0, 0), "{s:?}");
        assert!(s.exhausted);
        let a = store.get_spent_status(&pot(1), 0).await.unwrap().unwrap();
        assert!(a.spent_confirmed && a.spent_height == Some(965_771), "the standing row is untouched");
        assert!(store.pot_beef_proof_verified(&spender_a).await.unwrap());
        let b = store.get_spent_status(&pot(2), 0).await.unwrap().unwrap();
        assert!(b.spent && !b.spent_confirmed && b.spent_height.is_none(), "the refuted row is SEEN again");
        assert_eq!(b.spending_txid.as_deref(), Some(spender_b.as_str()), "the pointer stays");
        assert_eq!(b.spender_final, Some(true), "the #371 witness stays");
        assert!(!store.pot_beef_proof_verified(&spender_b).await.unwrap(), "its proof lost the latch");
        assert!(store.get_beef(&spender_b).await.unwrap().is_some_and(|b| !b.is_empty()), "the bytes stay");
        assert_eq!(store.find_unconfirmed_by_spending_txid(&spender_b).await.unwrap().len(), 1, "a chaser candidate again");
        let s2 = reverify_window(&store, Some(&tracker), None, 965_771, 965_773, None, 50, false).await;
        assert_eq!((s2.scanned, s2.standing, s2.stale), (1, 1, 0));
    }

    /// Fail-safe on every uncertain arm: a header-source FAULT counts and
    /// touches nothing; no header source at all examines nothing.
    #[tokio::test]
    async fn the_pass_never_demotes_on_a_fault_or_without_a_header_source() {
        let store = MemoryPotStorage::new();
        let spender = confirmed_pot_with_stored_proof(&store, &pot(3), 965_771).await;
        let s = reverify_window(&store, Some(&FaultyTracker), None, 965_771, 965_773, None, 50, true).await;
        assert_eq!((s.scanned, s.faults, s.stale, s.standing, s.demoted_blind), (1, 1, 0, 0, 0), "{s:?}");
        let r = store.get_spent_status(&pot(3), 0).await.unwrap().unwrap();
        assert!(r.spent_confirmed && r.spent_height == Some(965_771));
        assert!(store.pot_beef_proof_verified(&spender).await.unwrap());
        let none = reverify_window(&store, None, None, 965_771, 965_773, None, 50, true).await;
        assert_eq!(none, ReverifyPassSummary::default(), "no header source: nothing examined, nothing demoted");
        assert!(store.get_spent_status(&pot(3), 0).await.unwrap().unwrap().spent_confirmed);
        let sweep = reorg_revalidation_sweep::<MemoryTxStore>(&store, None, None, None, 965_773, 3, 50).await;
        assert_eq!(sweep, ReorgSweepSummary::default(), "the sweep moves no walk either");
        assert_eq!(store.read_sweep_state(WALK_SPENDERS).await.unwrap(), None);
    }

    /// Review M1: the operator's window demotes ONLY the refuted and the
    /// proofless rows; the canonical rows are untouched and counted; the
    /// pass is bounded and its cursor drains the window.
    #[tokio::test]
    async fn handle_reorg_demotes_only_refuted_and_proofless_rows_in_the_window() {
        let store = MemoryPotStorage::new();
        let mut tracker = MockChainTracker::new(965_775);
        let canonical = confirmed_pot_with_stored_proof(&store, &pot(10), 965_771).await;
        tracker.add_root(965_771, canonical.clone());
        let refuted = confirmed_pot_with_stored_proof(&store, &pot(11), 965_771).await;
        let proofless = confirmed_pot_without_stored_proof(&store, &pot(12), 965_771).await;
        let outside = confirmed_pot_with_stored_proof(&store, &pot(13), 965_772).await; // refuted but outside {965771}
        // bounded: two rows per call, the cursor carries the rest
        let first = handle_reorg(&store, Some(&tracker), 965_771, 965_771, None, 2).await;
        assert_eq!(first.scanned, 2, "{first:?}");
        assert!(!first.exhausted);
        let second = handle_reorg(&store, Some(&tracker), 965_771, 965_771, first.next_cursor, 2).await;
        assert!(second.exhausted, "{second:?}");
        let total = |f: fn(&ReverifyPassSummary) -> usize| f(&first) + f(&second);
        assert_eq!(total(|s| s.scanned), 3, "the three rows at 965771, once each");
        assert_eq!(total(|s| s.standing), 1, "the canonical row is counted, never demoted");
        assert_eq!(total(|s| s.stale), 1, "the refuted row is demoted");
        assert_eq!(total(|s| s.demoted_blind), 1, "the proofless row is demoted blind");
        assert!(store.get_spent_status(&pot(10), 0).await.unwrap().unwrap().spent_confirmed);
        assert!(store.pot_beef_proof_verified(&canonical).await.unwrap());
        assert!(!store.get_spent_status(&pot(11), 0).await.unwrap().unwrap().spent_confirmed);
        assert!(!store.pot_beef_proof_verified(&refuted).await.unwrap());
        let p = store.get_spent_status(&pot(12), 0).await.unwrap().unwrap();
        assert!(!p.spent_confirmed && p.spending_txid.as_deref() == Some(proofless.as_str()));
        assert!(store.get_spent_status(&pot(13), 0).await.unwrap().unwrap().spent_confirmed, "outside the window: untouched");
        assert!(store.pot_beef_proof_verified(&outside).await.unwrap());
        let third = handle_reorg(&store, Some(&tracker), 965_771, 965_771, None, 10).await;
        assert_eq!((third.scanned, third.standing, third.stale, third.demoted_blind), (1, 1, 0, 0), "drained: only the canonical row remains confirmed");
    }

    /// Review H2: Arcade's `reorg_unmined` is a HINT. Planted against a
    /// canonical row it changes nothing and is counted; against a refuted
    /// row it demotes and unlatches; a fault changes nothing.
    #[tokio::test]
    async fn a_planted_unmined_marker_changes_nothing_and_a_corroborated_one_demotes() {
        let store = MemoryPotStorage::new();
        let mut tracker = MockChainTracker::new(965_775);
        let canonical = confirmed_pot_with_stored_proof(&store, &pot(20), 965_771).await;
        tracker.add_root(965_771, canonical.clone());
        let planted = unmined_hint(&store, Some(&tracker), &canonical).await;
        assert_eq!(planted, UnminedHintSummary { uncorroborated: 1, ..Default::default() }, "{planted:?}");
        let r = store.get_spent_status(&pot(20), 0).await.unwrap().unwrap();
        assert!(r.spent_confirmed && r.spent_height == Some(965_771));
        assert!(store.pot_beef_proof_verified(&canonical).await.unwrap());
        // the same marker in a loop stays inert
        assert_eq!(unmined_hint(&store, Some(&tracker), &canonical).await.uncorroborated, 1);
        // a refuted stored proof: the hint is corroborated
        let refuted = confirmed_pot_with_stored_proof(&store, &pot(21), 965_771).await;
        let real = unmined_hint(&store, Some(&tracker), &refuted).await;
        assert_eq!(real, UnminedHintSummary { demoted: 1, ..Default::default() }, "{real:?}");
        let r = store.get_spent_status(&pot(21), 0).await.unwrap().unwrap();
        assert!(r.spent && !r.spent_confirmed && r.spent_height.is_none());
        assert!(!store.pot_beef_proof_verified(&refuted).await.unwrap());
        assert_eq!(unmined_hint(&store, Some(&tracker), &refuted).await, UnminedHintSummary::default(), "idempotent: nothing confirmed points at it now");
        // a fault and no header source change nothing
        let again = confirmed_pot_with_stored_proof(&store, &pot(22), 965_771).await;
        assert_eq!(unmined_hint(&store, Some(&FaultyTracker), &again).await, UnminedHintSummary { faults: 1, ..Default::default() });
        assert_eq!(unmined_hint(&store, None, &again).await, UnminedHintSummary::default());
        assert!(store.get_spent_status(&pot(22), 0).await.unwrap().unwrap().spent_confirmed);
        // a courier-confirmed row (no stored proof) is never demoted by a marker
        let proofless = confirmed_pot_without_stored_proof(&store, &pot(23), 965_771).await;
        assert_eq!(unmined_hint(&store, Some(&tracker), &proofless).await, UnminedHintSummary { no_stored_proof: 1, ..Default::default() });
        assert!(store.get_spent_status(&pot(23), 0).await.unwrap().unwrap().spent_confirmed);
        assert_eq!(unmined_hint(&store, Some(&tracker), &"77".repeat(32)).await, UnminedHintSummary::default(), "an unknown spender");
    }

    /// Review M2: a pushed proof at the SAME height with a different root
    /// (a tx in both competing blocks) replaces the stored, already-verified
    /// bump; the same anchor again changes nothing; a new height moves the
    /// confirmed row's height and replaces the bump.
    #[tokio::test]
    async fn a_pushed_proof_with_another_root_at_the_same_height_replaces_the_stored_bump() {
        let store = MemoryPotStorage::new();
        let spender = confirmed_pot_with_stored_proof(&store, &pot(30), 965_771).await;
        let stored = || async { stored_bump_anchor(&store.get_beef(&spender).await.unwrap().unwrap(), &spender).unwrap() };
        assert_eq!(stored().await, BumpAnchor { height: 965_771, root: spender.clone() });
        // the same anchor again: nothing moves
        let same = apply_pushed_proof_to_pot_stores(&store, &spender, &single_tx_bump(&spender, 965_771).to_hex()).await;
        assert_eq!((same.spends_reanchored, same.pot_beef_reanchored, same.spends_confirmed), (0, false, 0), "{same:?}");
        // the same height, a DIFFERENT root: a two-leaf bump whose root is not the txid
        let sibling = "cd".repeat(32);
        let two_leaf = bsv_rs::transaction::MerklePath::new(
            965_771,
            vec![vec![
                bsv_rs::transaction::MerklePathLeaf::new_txid(0, spender.clone()),
                bsv_rs::transaction::MerklePathLeaf::new(1, sibling.clone()),
            ]],
        )
        .unwrap();
        let new_root = two_leaf.compute_root(Some(&spender)).unwrap().to_ascii_lowercase();
        assert_ne!(new_root, spender);
        let replaced = apply_pushed_proof_to_pot_stores(&store, &spender, &two_leaf.to_hex()).await;
        assert!(replaced.pot_beef_reanchored, "{replaced:?}");
        assert_eq!(replaced.spends_reanchored, 0, "the row's height did not change");
        assert_eq!(stored().await, BumpAnchor { height: 965_771, root: new_root });
        assert!(store.pot_beef_proof_verified(&spender).await.unwrap(), "a verifying write keeps the latch");
        let r = store.get_spent_status(&pot(30), 0).await.unwrap().unwrap();
        assert!(r.spent_confirmed && r.spent_height == Some(965_771));
        // a new height: the row moves and the bump is replaced
        let moved = apply_pushed_proof_to_pot_stores(&store, &spender, &single_tx_bump(&spender, 965_773).to_hex()).await;
        assert_eq!((moved.spends_reanchored, moved.pot_beef_reanchored, moved.spends_cas_missed), (1, true, 0), "{moved:?}");
        assert!(moved.landed_anything(), "a re-anchor IS a landed push (never the unknown-txid arm)");
        assert_eq!(store.get_spent_status(&pot(30), 0).await.unwrap().unwrap().spent_height, Some(965_773));
        assert_eq!(stored().await, BumpAnchor { height: 965_773, root: spender.clone() });
    }

    /// (rowid, txid, beef, proofHeight, has_proof)
    type TxRow = (i64, String, Vec<u8>, Option<u64>, bool);

    /// A memory twin of the D1 transactions leg store.
    #[derive(Default)]
    struct MemoryTxStore {
        rows: Mutex<Vec<TxRow>>,
    }
    impl MemoryTxStore {
        fn insert(&self, txid: &str, beef: Vec<u8>, height: Option<u64>, proven: bool) {
            let mut rows = self.rows.lock().unwrap();
            let rowid = rows.len() as i64 + 1;
            rows.push((rowid, txid.to_string(), beef, height, proven));
        }
        fn proven(&self, txid: &str) -> bool {
            self.rows.lock().unwrap().iter().any(|r| r.1 == txid && r.4)
        }
    }
    impl ProvenTxStore for MemoryTxStore {
        async fn proven_window_page(&self, lo: u64, hi: u64, after: Option<RowKey>, limit: u64) -> Result<Vec<(RowKey, String, Vec<u8>)>, String> {
            let mut page: Vec<(RowKey, String, Vec<u8>)> = self
                .rows
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.4 && r.3.is_some_and(|h| h >= lo && h <= hi))
                .map(|r| (RowKey { height: r.3.unwrap(), rowid: r.0 }, r.1.clone(), r.2.clone()))
                .collect();
            page.sort_by_key(|(k, _, _)| std::cmp::Reverse((k.height, k.rowid)));
            if let Some(a) = after {
                page.retain(|(k, _, _)| k.height < a.height || (k.height == a.height && k.rowid < a.rowid));
            }
            page.truncate(limit as usize);
            Ok(page)
        }
        async fn unprove(&self, txid: &str) -> Result<(), String> {
            for r in self.rows.lock().unwrap().iter_mut() {
                if r.1 == txid {
                    r.4 = false;
                }
            }
            Ok(())
        }
    }

    /// Review M4: the pot_beefs and transactions legs re-verify the pots'
    /// own bumps and the stitched hop proofs in the window: a refuted one
    /// is unlatched / un-proved, a standing one kept, one outside the window
    /// never examined, a row without an anchor never paged.
    #[tokio::test]
    async fn the_proof_legs_unlatch_refuted_bumps_in_the_window_only() {
        let store = MemoryPotStorage::new();
        let mut tracker = MockChainTracker::new(965_775);
        // pots: their OWN bumps (a JOIN mined at 965771; a canonical one; one outside; one unanchored)
        let pot_beef = |n: u32, height: u32| async move {
            let raw = real_spender_raw(&pot(n), 0);
            let txid = Transaction::from_hex(&raw).unwrap().id();
            let beef = crate::proof_fetcher::assemble_spender_beef(&raw, &single_tx_bump(&txid, height).to_hex(), &txid).unwrap();
            (txid, beef)
        };
        let (join_ok, beef_ok) = pot_beef(40, 965_771).await;
        let (join_bad, beef_bad) = pot_beef(41, 965_771).await;
        let (join_out, beef_out) = pot_beef(42, 965_760).await;
        let (join_unanchored, beef_un) = pot_beef(43, 965_771).await;
        for (txid, beef, height) in [(&join_ok, &beef_ok, Some(965_771)), (&join_bad, &beef_bad, Some(965_771)), (&join_out, &beef_out, Some(965_760)), (&join_unanchored, &beef_un, None)] {
            store.store_beef(txid, beef).await.unwrap();
            store.mark_pot_beef_proven_at(txid, height).await.unwrap();
        }
        tracker.add_root(965_771, join_ok.clone());
        let leg = reverify_pot_beefs_window(&store, Some(&tracker), 965_771, 965_773, None, 50).await;
        assert_eq!((leg.scanned, leg.standing, leg.stale, leg.faults), (2, 1, 1, 0), "{leg:?}");
        assert!(store.pot_beef_proof_verified(&join_ok).await.unwrap());
        assert!(!store.pot_beef_proof_verified(&join_bad).await.unwrap(), "the orphan's JOIN lost its latch");
        assert!(store.get_beef(&join_bad).await.unwrap().is_some(), "bytes stay for the completion pass");
        assert!(store.pot_beef_proof_verified(&join_out).await.unwrap(), "outside the window");
        assert!(store.pot_beef_proof_verified(&join_unanchored).await.unwrap(), "no anchor: never paged (a stated limit)");
        // the transactions leg over the engine's store
        let txs = MemoryTxStore::default();
        let (hop_ok, hbeef_ok) = pot_beef(50, 965_772).await;
        let (hop_bad, hbeef_bad) = pot_beef(51, 965_772).await;
        let (hop_out, hbeef_out) = pot_beef(52, 965_700).await;
        txs.insert(&hop_ok, hbeef_ok, Some(965_772), true);
        txs.insert(&hop_bad, hbeef_bad, Some(965_772), true);
        txs.insert(&hop_out, hbeef_out, Some(965_700), true);
        tracker.add_root(965_772, hop_ok.clone());
        let leg = reverify_transactions_window(&txs, Some(&tracker), 965_771, 965_773, None, 50).await;
        assert_eq!((leg.scanned, leg.standing, leg.stale), (2, 1, 1), "{leg:?}");
        assert!(txs.proven(&hop_ok));
        assert!(!txs.proven(&hop_bad), "un-proved: the engine's completion pass re-fetches it");
        assert!(txs.proven(&hop_out));
        // faults change nothing on either leg
        let f1 = reverify_pot_beefs_window(&store, Some(&FaultyTracker), 965_771, 965_773, None, 50).await;
        assert_eq!((f1.scanned, f1.faults, f1.stale), (1, 1, 0));
        assert!(store.pot_beef_proof_verified(&join_ok).await.unwrap());
        let f2 = reverify_transactions_window(&txs, Some(&FaultyTracker), 965_771, 965_773, None, 50).await;
        assert_eq!((f2.scanned, f2.faults, f2.stale), (1, 1, 0));
    }

    /// Review H3b: a 158-row window at 50 rows per pass is covered in four
    /// passes with no row examined twice and none left out; the state is
    /// persisted, so a pass after a skip (any later pass) resumes at the
    /// cursor; the window stays anchored while the tip moves; when
    /// exhausted the next pass starts a new window with continuity.
    #[tokio::test]
    async fn n_passes_cover_a_158_row_window_exactly_once_and_resume_at_the_cursor() {
        let store = MemoryPotStorage::new();
        let mut spenders = Vec::new();
        for i in 0..158u32 {
            let height = 965_771 + (i % 3); // spread over 965771..=965773
            // unanchored proofs: this pin counts the SPENDERS leg's questions only
            spenders.push(confirmed_pot_with_proof_at(&store, &pot(1000 + i), height, false).await);
        }
        let mut valid = std::collections::HashSet::new();
        for (i, s) in spenders.iter().enumerate() {
            if i % 20 != 7 {
                valid.insert((965_771 + (i as u32 % 3), s.clone()));
            }
        }
        let tracker = RecordingTracker { valid, asked: Mutex::new(Vec::new()) };
        let txs = MemoryTxStore::default();
        let mut passes = 0;
        let mut scanned = 0;
        loop {
            passes += 1;
            let s = reorg_revalidation_sweep(&store, Some(&txs), Some(&tracker), None, 965_773, 3, 50).await;
            assert_eq!(s.spenders_window, Some((965_771, 965_773)), "the window stays anchored across passes");
            assert!(s.spenders.scanned <= 50);
            assert_eq!(s.pot_beefs.scanned, 0, "no anchored pot proofs seeded: the other legs ask nothing");
            assert_eq!(s.transactions.scanned, 0);
            scanned += s.spenders.scanned;
            if s.spenders.exhausted {
                break;
            }
            let st = store.read_sweep_state(WALK_SPENDERS).await.unwrap().unwrap();
            assert!(!st.exhausted && st.cursor.is_some(), "the cursor persists between passes: {st:?}");
            assert!(passes < 10, "runaway");
        }
        assert_eq!(passes, 4, "158 rows at 50 per pass: three full pages and a tail");
        assert_eq!(scanned, 158);
        let asked = tracker.asked.lock().unwrap().clone();
        let mut unique = asked.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(asked.len(), 158, "every row re-verified exactly once");
        assert_eq!(unique.len(), 158, "no row examined twice");
        let mut all = spenders.clone();
        all.sort();
        assert_eq!(unique, all, "no row left unverified");
        let refuted: usize = spenders.iter().enumerate().filter(|(i, _)| i % 20 == 7).count();
        let mut still_confirmed = 0usize;
        for i in 0..158u32 {
            if store.get_spent_status(&pot(1000 + i), 0).await.unwrap().unwrap().spent_confirmed {
                still_confirmed += 1;
            }
        }
        assert_eq!(still_confirmed, 158 - refuted, "exactly the refuted rows were demoted");
        // exhausted: the next pass at a NEW tip starts a fresh window from the newest, with continuity
        let st = store.read_sweep_state(WALK_SPENDERS).await.unwrap().unwrap();
        assert!(st.exhausted && st.cursor.is_none());
        let s = reorg_revalidation_sweep(&store, Some(&txs), Some(&tracker), None, 965_774, 3, 50).await;
        assert_eq!(s.spenders_window, Some((965_772, 965_774)));
        assert_eq!(s.spenders.scanned, 50, "restarted from the newest, bounded");
        // fell behind: an exhausted old window and a tip far ahead continue at prev.hi + 1
        store.write_sweep_state(WALK_SPENDERS, &SweepState { lo: 965_769, hi: 965_770, cursor: None, exhausted: true }).await.unwrap();
        let s = reorg_revalidation_sweep(&store, Some(&txs), Some(&tracker), None, 965_780, 3, 50).await;
        assert_eq!(s.spenders_window, Some((965_771, 965_780)), "no height skipped");
    }
}
