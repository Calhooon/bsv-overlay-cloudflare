//! bsv-low M19 R2 (2026-09-08): the PURE half of the overlay's reorg
//! reconcile. Loop 8 met a one-block reorg at 965771 (a 34 MB block
//! orphaned by a 58-tx block) and a second reorg 35 minutes later; the
//! overlay had latched 158 `pot_records` confirmations from the orphan's
//! MINED callbacks and nothing ever revisited them. Reference behaviour is
//! overlay-express (`ReorgStream.ts` + `Engine.handleReorg` + its
//! revalidation sweep): demote proven admissions whose block was orphaned,
//! then re-verify the recent window on every (re)connect / poll.
//!
//! Everything here is a decision over facts already in hand; the I/O lives
//! in the overlay crate (`tip_pass.rs`, `proof_fetcher.rs`).

/// What the storage learned when the block-event pass recorded `(height,
/// hash)`: the hash it previously held at that height (if any) and the
/// highest height it had seen before this record.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HeaderSeen {
    /// The hash the overlay had recorded at THIS height before this call;
    /// `None` when the height was never recorded.
    pub prior_hash_at_height: Option<String>,
    /// The highest height recorded before this call (`None` on the first).
    pub max_height_before: Option<u64>,
}

/// Where a reorg starts, or `None` when the announced header extends the
/// chain the overlay already holds.
///
/// Two shapes, both from the reference's `handleReorg` input:
///
/// - the SAME height announces a DIFFERENT hash: the block at that height
///   was replaced, so every confirmation at `height` and above rests on an
///   orphan (`Some(height)`);
/// - the tip DECREASED (an announced height below the highest we hold):
///   every confirmation above the announced height rests on blocks the
///   canonical chain no longer contains (`Some(height + 1)`).
///
/// A repeat of the same `(height, hash)` and a plain new tip are not reorgs.
pub fn reorg_from_height(height: u64, hash: &str, seen: &HeaderSeen) -> Option<u64> {
    let same_hash_held = seen
        .prior_hash_at_height
        .as_deref()
        .is_some_and(|prior| prior.eq_ignore_ascii_case(hash));
    let hash_changed = seen.prior_hash_at_height.is_some() && !same_hash_held;
    // A lower height whose hash we already hold is a stale RE-ANNOUNCE of a
    // block still on our chain (chaintracks can announce a tip more than
    // once), never a reorg. A lower height with an unknown or different hash
    // is the chain getting shorter under us.
    let tip_decreased = seen.max_height_before.is_some_and(|max| max > height) && !same_hash_held;
    match (hash_changed, tip_decreased) {
        (true, _) => Some(height),
        (false, true) => Some(height + 1),
        (false, false) => None,
    }
}

/// The revalidation sweep's verdict for ONE confirmed row, from the stored
/// spender proof's re-verification against the current header source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReverifyVerdict {
    /// The stored proof's root still matches the canonical header: keep.
    Standing,
    /// The header source refutes the stored root: the row's confirmation
    /// rests on an orphan — demote back to SEEN, unlatch the proof.
    Stale,
    /// The header source could not be read (transport / a starved
    /// subrequest): NOT a chain verdict, retried next pass, counted.
    Fault,
    /// No stored proof for the spender: nothing to re-verify locally; the
    /// courier ladder decides (`Ok(None)` from it = not verifiably mined).
    NoStoredProof,
}

/// Fold a `verify_bump_detailed`-shaped answer into a sweep verdict.
/// `Ok(true)` stands, `Ok(false)` is stale, `Err` is a fault. Fail-safe: a
/// fault never demotes (a starved header read must not un-confirm a row),
/// and it never stands either (it is counted, retried).
pub fn classify_reverify(answer: &Result<bool, String>) -> ReverifyVerdict {
    match answer {
        Ok(true) => ReverifyVerdict::Standing,
        Ok(false) => ReverifyVerdict::Stale,
        Err(_) => ReverifyVerdict::Fault,
    }
}

/// The sweep's height window: the last `depth` heights ending at `tip`,
/// inclusive (`tip - depth + 1 ..= tip`), clamped at genesis. `depth = 0`
/// is an empty window (`None`).
pub fn sweep_window(tip: u64, depth: u64) -> Option<(u64, u64)> {
    if depth == 0 || tip == 0 {
        return None;
    }
    let min = tip.saturating_sub(depth - 1).max(1);
    Some((min, tip))
}

/// Arcade v2's reorg-correction markers (issue #279 there): the `extraInfo`
/// a status event carries when it is a reorg correction rather than a plain
/// transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArcadeReorgMarker {
    /// A MINED event whose txs were RE-ANCHORED from an orphaned block to
    /// the active-chain block at its height: carries a NEW block hash +
    /// merkle path. The webhook's same-status dedup makes its one exception
    /// for exactly this event.
    Reanchor,
    /// A SEEN_ON_NETWORK event whose txs were reverted out of an orphaned
    /// block they do not exist outside of: the spend is back in the mempool.
    Unmined,
}

/// Read Arcade's `extraInfo` marker, if it is one of the two reorg
/// corrections. Anything else (a reason text, `None`) is not a marker.
pub fn arcade_reorg_marker(extra_info: Option<&str>) -> Option<ArcadeReorgMarker> {
    match extra_info.map(str::trim) {
        Some("reorg_reanchor") => Some(ArcadeReorgMarker::Reanchor),
        Some("reorg_unmined") => Some(ArcadeReorgMarker::Unmined),
        _ => None,
    }
}

/// A pushed MINED proof for a spender the index already holds CONFIRMED:
/// is it a RE-ANCHOR (the proof names a different block than the stored
/// confirmation) or the same anchor again? `stored_height = None` means
/// the row was confirmed without a height on record (a pre-migration row),
/// which a pushed height simply FILLS — not a re-anchor.
pub fn is_reanchor(stored_height: Option<u64>, pushed_height: u64) -> bool {
    stored_height.is_some_and(|h| h != pushed_height)
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "00000000000000001de5aa96baa3566ce66e4941f8295cc44cc85fc75949db4d";
    const ORPHAN: &str = "0000000000000000153e10f465dba9697e4bde364fdf3a3224a736b019ffbfb1";

    #[test]
    fn a_new_tip_and_a_repeat_are_not_reorgs() {
        assert_eq!(reorg_from_height(965771, A, &HeaderSeen::default()), None);
        assert_eq!(
            reorg_from_height(
                965772,
                A,
                &HeaderSeen { prior_hash_at_height: None, max_height_before: Some(965771) }
            ),
            None
        );
        // the same (height, hash) announced twice
        assert_eq!(
            reorg_from_height(
                965771,
                A,
                &HeaderSeen { prior_hash_at_height: Some(A.to_uppercase()), max_height_before: Some(965771) }
            ),
            None,
            "a repeat of the same hash (case-insensitive) is not a reorg"
        );
    }

    #[test]
    fn a_hash_change_at_the_same_height_reorgs_from_that_height() {
        // 2026-09-07 22:39:32Z: the orphan …153e10f4 was recorded first, then
        // the canonical …1de5aa96 announced at the same height.
        let seen = HeaderSeen { prior_hash_at_height: Some(ORPHAN.into()), max_height_before: Some(965771) };
        assert_eq!(reorg_from_height(965771, A, &seen), Some(965771));
    }

    #[test]
    fn a_tip_decrease_reorgs_from_the_height_above_the_announced_one() {
        // 23:14Z: the two near-empty blocks 965772/965773 were replaced; the
        // announce came in at 965772 below a held 965773.
        let seen = HeaderSeen { prior_hash_at_height: None, max_height_before: Some(965773) };
        assert_eq!(reorg_from_height(965772, A, &seen), Some(965773));
        // a stale RE-ANNOUNCE of a lower height whose hash we already hold is
        // not a reorg (the block is still on our chain)
        let seen = HeaderSeen { prior_hash_at_height: Some(A.into()), max_height_before: Some(965773) };
        assert_eq!(reorg_from_height(965772, A, &seen), None);
        // a hash change at the announced height wins the lower start
        let seen = HeaderSeen { prior_hash_at_height: Some(ORPHAN.into()), max_height_before: Some(965773) };
        assert_eq!(reorg_from_height(965772, A, &seen), Some(965772));
    }

    #[test]
    fn reverify_folds_fail_safe() {
        assert_eq!(classify_reverify(&Ok(true)), ReverifyVerdict::Standing);
        assert_eq!(classify_reverify(&Ok(false)), ReverifyVerdict::Stale);
        assert_eq!(classify_reverify(&Err("read failed".into())), ReverifyVerdict::Fault);
    }

    #[test]
    fn the_sweep_window_is_the_last_depth_heights_inclusive() {
        assert_eq!(sweep_window(965774, 3), Some((965772, 965774)));
        assert_eq!(sweep_window(965774, 1), Some((965774, 965774)));
        assert_eq!(sweep_window(2, 6), Some((1, 2)));
        assert_eq!(sweep_window(965774, 0), None);
        assert_eq!(sweep_window(0, 3), None);
    }

    #[test]
    fn arcade_markers_are_the_two_spellings_only() {
        assert_eq!(arcade_reorg_marker(Some("reorg_reanchor")), Some(ArcadeReorgMarker::Reanchor));
        assert_eq!(arcade_reorg_marker(Some(" reorg_unmined ")), Some(ArcadeReorgMarker::Unmined));
        assert_eq!(arcade_reorg_marker(Some("UTXO_SPENT (70): x")), None);
        assert_eq!(arcade_reorg_marker(Some("")), None);
        assert_eq!(arcade_reorg_marker(None), None);
    }

    #[test]
    fn a_reanchor_is_a_different_stored_height_never_a_fill() {
        assert!(is_reanchor(Some(965771), 965773));
        assert!(!is_reanchor(Some(965773), 965773));
        assert!(!is_reanchor(None, 965773), "a height-less confirmation is filled, not re-anchored");
    }
}
