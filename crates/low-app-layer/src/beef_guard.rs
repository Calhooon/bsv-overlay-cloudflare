//! bsv-low M19 R2 (2026-09-08, #425): `/beef/:txid` never serves a bump the
//! current header refutes.
//!
//! ## Why
//!
//! Loop 8 met two mainnet reorgs (965771 at 22:39Z, 965772/3 at 23:14Z).
//! The overlay had stitched MINED proofs from the orphaned block into stored
//! pot BEEFs, and `/beef` kept serving those bytes for the rest of the run:
//! a wallet that ingested one refused its own later spends over it
//! (`createAction` 400, "root mismatch at height"). The overlay's block-event
//! reconcile (`tip_pass.rs`: `handle_reorg` + the revalidation sweep) is the
//! PRIMARY repair; this module is the READ-SIDE belt for the window between
//! the reorg and that pass: every bump anchored in the last
//! [`BEEF_REFUTE_DEPTH`] heights is re-checked against the header chaintracks
//! holds NOW, per read.
//!
//! ## What a refuted bump becomes
//!
//! Reference (overlay-express) serves stored bytes and repairs by demotion;
//! it has no read-side check. Ours: a refuted bump is STRIPPED and the BEEF
//! is served raw with its ancestry when the stored bytes still source every
//! unproven tx (an untrimmed store); when they do not (the overlay TRIMS the
//! ancestry once a proof verifies — `stitch_and_trim_pot_beef`), the row
//! cannot be served honestly and `/beef` answers 503 (its existing
//! "retryable fault" shape, never a 404), naming the refuted height. The
//! overlay's next pass re-anchors or demotes the row; the client retries.
//!
//! ## Bounds
//!
//! Per isolate: the present tip is cached [`PRESENT_TIP_TTL_MS`] (and
//! latched by every `tip-changed` webhook this isolate receives); a header
//! root is cached per height for [`HEADER_TTL_MS`], at most
//! [`HEADER_CACHE_MAX`] heights. A read with no recent bump costs nothing.
//! Fail direction: with NO header in hand (chaintracks unreadable, or a
//! height it does not hold yet) nothing is refuted and the stored bytes are
//! served as before, LOGGED — the read side must not turn a chaintracks
//! outage into a `/beef` outage on the money path; the overlay's sweep
//! (which counts its faults) is the path that never reads unknown as fine.

use bsv_rs::transaction::{Beef, MerklePath};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashSet};
use worker::Env;

/// Bumps anchored in the last this-many heights (tip inclusive) are
/// re-checked on every read. Twice the overlay sweep's 3 (the reference's
/// `reorgScanDepth`): the read side is the belt for the pass the sweep has
/// not run yet.
pub const BEEF_REFUTE_DEPTH: u64 = 6;
/// How long a fetched present height is trusted per isolate.
const PRESENT_TIP_TTL_MS: f64 = 30_000.0;
/// How long a fetched header root is trusted per isolate (a recent height's
/// header CAN change — that is the whole point — so it is short).
const HEADER_TTL_MS: f64 = 30_000.0;
/// Per-isolate header cache bound (heights); the lowest height is evicted.
const HEADER_CACHE_MAX: usize = 64;

thread_local! {
    /// (present height, expires-at ms).
    static PRESENT_TIP: Cell<Option<(u64, f64)>> = const { Cell::new(None) };
    /// height → (lowercase merkle root, expires-at ms).
    static HEADER_ROOTS: RefCell<BTreeMap<u64, (String, f64)>> = const { RefCell::new(BTreeMap::new()) };
}

/// PURE: is a bump at `height` inside the re-check window for `tip`? The
/// window is `tip - depth + 1 ..` with NO upper bound: a bump ABOVE the tip
/// this isolate knows means our tip is stale, and that bump is recent by
/// definition.
pub fn in_recheck_window(height: u64, tip: u64, depth: u64) -> bool {
    if depth == 0 {
        return false;
    }
    height >= tip.saturating_sub(depth - 1)
}

/// PURE: the `(bump index, height)` pairs of `beef` inside the window.
pub fn recent_bumps(beef: &Beef, tip: u64, depth: u64) -> Vec<(usize, u64)> {
    beef.bumps
        .iter()
        .enumerate()
        .map(|(i, b)| (i, u64::from(b.block_height)))
        .filter(|(_, h)| in_recheck_window(*h, tip, depth))
        .collect()
}

/// PURE: the merkle root a bump claims, from its own leaves (every leaf of
/// one bump computes the same root). `None` for a bump with no txid leaf.
pub fn claimed_root(bump: &MerklePath) -> Option<String> {
    bump.compute_root(None).ok().map(|r| r.to_ascii_lowercase())
}

/// The read-side verdict for ONE bump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BumpVerdict {
    /// The header chaintracks holds at that height carries this root.
    Standing,
    /// Chaintracks holds a DIFFERENT root at that height: the bump rests on
    /// an orphan.
    Refuted,
    /// No header in hand (unreadable, or not yet held): not a verdict.
    Unknown,
}

/// PURE: judge a claimed root against the canonical one (`None` = no header
/// in hand). Case-insensitive; never refutes on absence.
pub fn judge_bump(claimed: &str, canonical: Option<&str>) -> BumpVerdict {
    match canonical {
        None => BumpVerdict::Unknown,
        Some(c) if c.eq_ignore_ascii_case(claimed) => BumpVerdict::Standing,
        Some(_) => BumpVerdict::Refuted,
    }
}

/// PURE: strip the bumps at `refuted` (indexes into `beef.bumps`) and return
/// the re-serialized BEEF — in the input's wire format — when every tx that
/// LOST its proof is still fully sourced by raw parents in the BEEF (the
/// consumer can then verify it as an unproven tx over proven ancestry).
/// `None` when the stripped BEEF would not verify (a trimmed store), when
/// the subject is absent, or on any parse/serialize fault: the caller must
/// then refuse, never serve.
pub fn strip_bumps(beef_bytes: &[u8], subject: &str, refuted: &[usize]) -> Option<Vec<u8>> {
    let mut beef = Beef::from_binary(beef_bytes).ok()?;
    if refuted.is_empty() || beef.find_txid(subject).is_none() {
        return None;
    }
    let atomic = beef.atomic_txid.clone();
    let mut remap: Vec<Option<usize>> = Vec::with_capacity(beef.bumps.len());
    let mut kept: Vec<MerklePath> = Vec::new();
    for (i, bump) in beef.bumps.iter().enumerate() {
        if refuted.contains(&i) {
            remap.push(None);
        } else {
            remap.push(Some(kept.len()));
            kept.push(bump.clone());
        }
    }
    if kept.len() == beef.bumps.len() {
        return None; // nothing named was a bump of this BEEF
    }
    beef.bumps = kept;
    for tx in beef.txs.iter_mut() {
        if let Some(i) = tx.bump_index() {
            tx.set_bump_index(remap.get(i).copied().flatten());
        }
    }
    // Structural validity (bump/leaf consistency, dependency order; txid-only
    // entries tolerated as the format allows) …
    if !beef.is_valid(true) {
        return None;
    }
    // … AND the strict rule for the txs that are now unproven: every input
    // must be a RAW tx in this BEEF (a txid-only parent proves nothing to a
    // verifier that has never seen it).
    let raw_present: HashSet<String> = beef
        .txs
        .iter()
        .filter(|t| !t.is_txid_only())
        .map(|t| t.txid())
        .collect();
    for tx in &beef.txs {
        if tx.is_txid_only() || tx.bump_index().is_some() {
            continue;
        }
        if tx.input_txids.iter().any(|i| !raw_present.contains(i)) {
            return None;
        }
    }
    match atomic {
        Some(_) => beef.to_binary_atomic(subject).ok(),
        None => Some(beef.to_binary()),
    }
}

/// What the guard decided for a stored BEEF about to be served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Guarded {
    /// Serve these bytes (unchanged, or with refuted bumps stripped).
    Serve(Vec<u8>),
    /// A refuted bump could not be stripped honestly: refuse (503), naming
    /// the refuted height.
    Refuted { height: u64 },
}

/// Latch a present height this isolate learned from the chaintracks
/// `tip-changed` webhook (never lowers a held tip).
pub fn latch_present_tip(height: u64) {
    let now = worker::Date::now().as_millis() as f64;
    PRESENT_TIP.with(|c| {
        let held = c.get().map(|(h, _)| h).unwrap_or(0);
        c.set(Some((height.max(held), now + PRESENT_TIP_TTL_MS)));
    });
}

/// The present height: cached per isolate, else fetched through the
/// CHAINTRACKS binding; a fetch fault falls back to a stale held value.
async fn present_tip(env: &Env) -> Option<u64> {
    let now = worker::Date::now().as_millis() as f64;
    if let Some((h, exp)) = PRESENT_TIP.with(|c| c.get()) {
        if now < exp {
            return Some(h);
        }
    }
    match crate::routes::chaintracks_present_height_env(env, "beef-guard").await {
        Ok(h) => {
            PRESENT_TIP.with(|c| c.set(Some((h, now + PRESENT_TIP_TTL_MS))));
            Some(h)
        }
        Err((msg, _)) => {
            let stale = PRESENT_TIP.with(|c| c.get()).map(|(h, _)| h);
            worker::console_warn!(
                "[beef-guard] present height unreadable ({msg}); {}",
                match stale {
                    Some(h) => format!("using the stale held tip {h}"),
                    None => "no tip held — recent bumps cannot be re-checked this read".to_string(),
                }
            );
            stale
        }
    }
}

/// The lowercase merkle root chaintracks holds at `height`, cached per
/// isolate for [`HEADER_TTL_MS`]; `None` when it cannot be read.
async fn canonical_root(env: &Env, height: u64) -> Option<String> {
    let now = worker::Date::now().as_millis() as f64;
    let cached = HEADER_ROOTS.with(|m| {
        m.borrow()
            .get(&height)
            .filter(|(_, exp)| now < *exp)
            .map(|(root, _)| root.clone())
    });
    if cached.is_some() {
        return cached;
    }
    let header = crate::internal_events::chaintracks_header(env, height).await?;
    let root = header.merkle_root.to_ascii_lowercase();
    HEADER_ROOTS.with(|m| {
        let mut m = m.borrow_mut();
        m.insert(height, (root.clone(), now + HEADER_TTL_MS));
        while m.len() > HEADER_CACHE_MAX {
            let Some(&lowest) = m.keys().next() else { break };
            m.remove(&lowest);
        }
    });
    Some(root)
}

/// The read-side guard for `/beef/:txid`: re-check every bump of `bytes`
/// anchored in the last [`BEEF_REFUTE_DEPTH`] heights against chaintracks;
/// a refuted bump is stripped (served raw with ancestry) or, when the store
/// cannot source the now-unproven tx, refused.
pub async fn guard_served_beef(env: &Env, subject: &str, bytes: &[u8]) -> Guarded {
    let Ok(beef) = Beef::from_binary(bytes) else {
        return Guarded::Serve(bytes.to_vec()); // unparseable: passthrough, as compaction does
    };
    if beef.bumps.is_empty() {
        return Guarded::Serve(bytes.to_vec());
    }
    let Some(tip) = present_tip(env).await else {
        return Guarded::Serve(bytes.to_vec());
    };
    let recent = recent_bumps(&beef, tip, BEEF_REFUTE_DEPTH);
    if recent.is_empty() {
        return Guarded::Serve(bytes.to_vec());
    }
    let mut refuted: Vec<usize> = Vec::new();
    let mut refuted_height = 0u64;
    for (idx, height) in recent {
        let Some(claimed) = beef.bumps.get(idx).and_then(claimed_root) else {
            continue;
        };
        let canonical = canonical_root(env, height).await;
        match judge_bump(&claimed, canonical.as_deref()) {
            BumpVerdict::Standing => {}
            BumpVerdict::Unknown => {
                worker::console_warn!(
                    "[beef-guard] {subject}: no header in hand for {height}; its bump is served unchecked"
                );
            }
            BumpVerdict::Refuted => {
                worker::console_warn!(
                    "[beef-guard] {subject}: REFUTED bump at {height} (claimed root {claimed}, chaintracks holds {}) — a reorg; stripping",
                    canonical.as_deref().unwrap_or("?")
                );
                refuted.push(idx);
                refuted_height = refuted_height.max(height);
            }
        }
    }
    if refuted.is_empty() {
        return Guarded::Serve(bytes.to_vec());
    }
    match strip_bumps(bytes, subject, &refuted) {
        Some(stripped) => {
            worker::console_log!(
                "[beef-guard] {subject}: served raw with ancestry, {} refuted bump(s) stripped ({} → {} bytes)",
                refuted.len(),
                bytes.len(),
                stripped.len()
            );
            Guarded::Serve(stripped)
        }
        None => {
            worker::console_warn!(
                "[beef-guard] {subject}: the stored BEEF cannot source its tx without the refuted bump at {refuted_height} (trimmed store) — refusing 503 until the overlay re-anchors"
            );
            Guarded::Refuted { height: refuted_height }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bsv_rs::transaction::{MerklePathLeaf, Transaction};

    /// A real mainnet raw tx (the compaction module's fixture): the
    /// unproven grandparent whose own inputs are NOT in any BEEF here.
    const RAW_TX: &str = "0100000001c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd3704000000004847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff0200ca9a3b00000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac00286bee0000000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac00000000";

    fn child_spending(source_txid: &str, vout: u32) -> Transaction {
        let mut s = String::from("01000000");
        s.push_str("01");
        let mut prev = hex::decode(source_txid).unwrap();
        prev.reverse();
        s.push_str(&hex::encode(prev));
        s.push_str(&hex::encode(vout.to_le_bytes()));
        s.push_str("00");
        s.push_str("ffffffff");
        s.push_str("01");
        s.push_str(&hex::encode(1000u64.to_le_bytes()));
        s.push_str("0151");
        s.push_str("00000000");
        Transaction::from_hex(&s).unwrap()
    }

    /// A single-leaf bump (a one-tx block: root == txid) at `height`.
    fn single_leaf_bump(txid: &str, height: u32) -> MerklePath {
        MerklePath::new_unchecked(height, vec![vec![MerklePathLeaf::new_txid(0, txid.to_string())]]).unwrap()
    }

    /// grandparent (proven long ago, at 965_700) ← parent (bump at
    /// `height`, the one under test) ← child (the subject, unproven). A
    /// valid BEEF: every ancestry chain ends in a proven tx. `trimmed` drops
    /// the grandparent — the shape the overlay STORES once a proof verifies
    /// (`stitch_and_trim_pot_beef` trims the ancestry a bump now covers).
    fn fixture(height: u32, trimmed: bool) -> (Vec<u8>, String, String) {
        let gp = Transaction::from_hex(RAW_TX).unwrap();
        let gp_txid = gp.id();
        let parent = child_spending(&gp_txid, 0);
        let parent_txid = parent.id();
        let child = child_spending(&parent_txid, 0);
        let child_txid = child.id();
        let mut beef = Beef::new();
        let bi = beef.merge_bump(single_leaf_bump(&parent_txid, height));
        if !trimmed {
            let gi = beef.merge_bump(single_leaf_bump(&gp_txid, 965_700));
            beef.merge_raw_tx(gp.to_binary(), Some(gi));
        }
        beef.merge_raw_tx(parent.to_binary(), Some(bi));
        beef.merge_raw_tx(child.to_binary(), None);
        assert!(beef.is_valid(false), "the fixture itself verifies");
        (beef.to_binary(), child_txid, parent_txid)
    }

    #[test]
    fn the_window_is_the_last_depth_heights_with_no_upper_bound() {
        assert!(in_recheck_window(965_771, 965_776, 6));
        assert!(!in_recheck_window(965_770, 965_776, 6));
        assert!(in_recheck_window(965_776, 965_776, 6));
        assert!(in_recheck_window(965_780, 965_776, 6), "above a stale tip is recent");
        assert!(!in_recheck_window(965_776, 965_776, 0));
        assert!(in_recheck_window(1, 3, 6), "clamped at genesis");
    }

    #[test]
    fn recent_bumps_names_only_the_window() {
        let (bytes, _, _) = fixture(965_771, false);
        let beef = Beef::from_binary(&bytes).unwrap();
        assert_eq!(beef.bumps.len(), 2, "the parent's and the grandparent's");
        assert_eq!(recent_bumps(&beef, 965_776, 6), vec![(0, 965_771)], "the old one is outside");
        assert_eq!(recent_bumps(&beef, 965_777, 6), vec![]);
    }

    #[test]
    fn a_claimed_root_is_judged_against_the_canonical_one_and_never_refuted_on_absence() {
        let (bytes, _, parent) = fixture(965_771, false);
        let beef = Beef::from_binary(&bytes).unwrap();
        let claimed = claimed_root(&beef.bumps[0]).unwrap();
        assert_eq!(claimed, parent, "a one-tx block's root is the txid");
        assert_eq!(judge_bump(&claimed, Some(&parent.to_ascii_uppercase())), BumpVerdict::Standing);
        assert_eq!(judge_bump(&claimed, Some(&"ab".repeat(32))), BumpVerdict::Refuted);
        assert_eq!(judge_bump(&claimed, None), BumpVerdict::Unknown);
    }

    /// The untrimmed store: the refuted bump goes, the parent is served as an
    /// unproven tx over its raw grandparent, the subject stays, format kept.
    #[test]
    fn stripping_a_refuted_bump_serves_raw_with_ancestry_when_the_store_sources_it() {
        let (bytes, child, parent) = fixture(965_771, false);
        let out = strip_bumps(&bytes, &child, &[0]).expect("sourced: served");
        let mut re = Beef::from_binary(&out).unwrap();
        assert_eq!(re.bumps.len(), 1, "the refuted bump is gone; the grandparent's stands");
        assert_eq!(re.bumps[0].block_height, 965_700);
        assert_eq!(re.txs.len(), 3, "grandparent, parent, child all served");
        assert!(re.find_txid(&child).is_some());
        assert_eq!(re.find_txid(&parent).unwrap().bump_index(), None, "the parent is now an unproven tx over its proven parent");
        assert!(!re.is_atomic(), "plain in, plain out");
        assert!(re.is_valid(false), "a verifier can walk it");
        assert_ne!(out, bytes);
    }

    /// The overlay's TRIMMED store (ancestry dropped once the proof
    /// verified): without the bump the parent cannot be sourced, so the row
    /// cannot be served honestly — the guard must refuse, never serve.
    #[test]
    fn a_trimmed_store_cannot_be_served_without_its_refuted_bump() {
        let (bytes, child, _) = fixture(965_771, true);
        assert_eq!(strip_bumps(&bytes, &child, &[0]), None);
    }

    /// Two bumps, one refuted: the kept bump is re-indexed and the tx it
    /// proves keeps pointing at it, whichever position it held.
    #[test]
    fn a_kept_bump_is_reindexed_after_a_strip() {
        let gp = Transaction::from_hex(RAW_TX).unwrap();
        let gp_txid = gp.id();
        let parent = child_spending(&gp_txid, 0);
        let parent_txid = parent.id();
        let child = child_spending(&parent_txid, 0);
        let child_txid = child.id();
        // bump 0 proves the PARENT (the one refuted), bump 1 proves the grandparent
        let mut beef = Beef::new();
        let b_parent = beef.merge_bump(single_leaf_bump(&parent_txid, 965_771));
        let b_gp = beef.merge_bump(single_leaf_bump(&gp_txid, 965_700));
        assert_eq!((b_parent, b_gp), (0, 1));
        beef.merge_raw_tx(gp.to_binary(), Some(b_gp));
        beef.merge_raw_tx(parent.to_binary(), Some(b_parent));
        beef.merge_raw_tx(child.to_binary(), None);
        let bytes = beef.to_binary();
        let out = strip_bumps(&bytes, &child_txid, &[0]).expect("the parent is sourced by its proven grandparent");
        let mut re = Beef::from_binary(&out).unwrap();
        assert_eq!(re.bumps.len(), 1);
        assert_eq!(re.bumps[0].block_height, 965_700, "the standing bump stays");
        assert_eq!(re.find_txid(&gp_txid).unwrap().bump_index(), Some(0), "re-indexed 1 → 0");
        assert_eq!(re.find_txid(&parent_txid).unwrap().bump_index(), None);
        assert!(re.is_valid(false));
        // refuting the grandparent's bump instead: the grandparent's own
        // inputs are not here, so nothing honest can be served
        assert_eq!(strip_bumps(&bytes, &child_txid, &[1]), None);
    }

    #[test]
    fn strip_refuses_nonsense() {
        let (bytes, child, _) = fixture(965_771, false);
        assert_eq!(strip_bumps(&bytes, &child, &[]), None, "nothing to strip is not a served answer");
        assert_eq!(strip_bumps(&bytes, &child, &[7]), None, "not a bump of this BEEF");
        assert_eq!(strip_bumps(&bytes, &"cd".repeat(32), &[0]), None, "the subject must be present");
        assert_eq!(strip_bumps(&[0, 1, 2], &child, &[0]), None, "garbage");
    }
}
