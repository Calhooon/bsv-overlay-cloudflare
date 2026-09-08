//! bsv-low M19 R2 (2026-09-08, round 2): the PURE half of the overlay's reorg
//! reconcile. Loop 8 met a one-block reorg at 965771 (a 34 MB block orphaned
//! by a 58-tx block) and a second reorg 35 minutes later; the overlay had
//! latched 158 `pot_records` confirmations from the orphan's MINED callbacks
//! and nothing ever revisited them. Reference behaviour is overlay-express
//! (`ReorgStream.ts` + `Engine.handleReorg` + its revalidation sweep) and
//! wallet-toolbox's `TaskNewHeader` (a header BELOW the held tip is an old
//! header: ignored, never a reorg).
//!
//! Everything here is a decision over facts already in hand; the I/O lives
//! in the overlay crate (`tip_pass.rs`, `reorg_sweep.rs`).

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

/// What a tip announce IS, given what the overlay already holds. The ONE
/// producer of a reorg is a same-height hash change (round-2 review H3: the
/// former "tip decreased" arm had no chain producer at all, only a benign
/// out-of-order webhook race, and would have demoted canonical rows).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TipAnnounce {
    /// A height above everything held: the chain extended. Run the pass.
    Extends,
    /// The same `(height, hash)` again (chaintracks announces a tip more
    /// than once; several isolates hear one block): nothing new.
    Repeat,
    /// A height below the highest held, never recorded: an OLD header
    /// announced late. The reference ignores it; counted, never a reorg.
    Old,
    /// The SAME height with a DIFFERENT hash: the block at that height was
    /// replaced, so every confirmation anchored at or above it rests on a
    /// block the chain no longer holds.
    Reorg {
        /// The replaced height.
        from: u64,
    },
}

/// Classify an announce against the recorded header at its height.
/// Case-insensitive on the hash.
pub fn classify_tip_announce(height: u64, hash: &str, seen: &HeaderSeen) -> TipAnnounce {
    match seen.prior_hash_at_height.as_deref() {
        Some(prior) if prior.eq_ignore_ascii_case(hash) => TipAnnounce::Repeat,
        Some(_) => TipAnnounce::Reorg { from: height },
        None if seen.max_height_before.is_some_and(|max| max > height) => TipAnnounce::Old,
        None => TipAnnounce::Extends,
    }
}

/// The revalidation sweep's verdict for ONE stored proof, from its
/// re-verification against the current header source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReverifyVerdict {
    /// The stored proof's root still matches the canonical header: keep.
    Standing,
    /// The header source refutes the stored root: the confirmation rests
    /// on an orphan. Demote back to SEEN, unlatch the proof.
    Stale,
    /// The header source could not be read (transport, a starved
    /// subrequest, an absent header): NOT a chain verdict, counted, retried.
    Fault,
    /// No stored proof: nothing to re-verify locally.
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

/// One row's position in a height-windowed walk: `(height DESC, rowid
/// DESC)` is the walk order, and this is the last row examined.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowKey {
    pub height: u64,
    pub rowid: i64,
}

/// The persisted state of ONE height-windowed walk (round-2 review H3b):
/// a pass examines at most N rows and must continue where the last one
/// stopped, across passes and across isolates, until the window is
/// exhausted; a skipped pass loses nothing. The window stays ANCHORED
/// until exhausted (the tip may move meanwhile); the next window starts
/// from the newest with continuity (see [`next_sweep_window`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepState {
    pub lo: u64,
    pub hi: u64,
    /// The last row examined; `None` = the walk has not started.
    pub cursor: Option<RowKey>,
    /// The walk reached the end of `[lo, hi]`.
    pub exhausted: bool,
}

/// Which window a pass walks at `tip`, given the persisted state.
///
/// - depth or tip 0: nothing (`None`);
/// - a walk in progress: the SAME window, unchanged (anchored);
/// - otherwise a new window ending at `tip`, starting at the last
///   `depth` heights OR at `prev.hi + 1` when the previous window ended
///   further below (CONTINUITY: a walker that fell behind never skips a
///   height; the window is wider by rows, still bounded per pass);
/// - the same tip again after exhaustion: the last `depth` heights again
///   (a restart from the newest; cheap when few rows).
pub fn next_sweep_window(prev: Option<&SweepState>, tip: u64, depth: u64) -> Option<SweepState> {
    if depth == 0 || tip == 0 {
        return None;
    }
    if let Some(p) = prev {
        if !p.exhausted {
            return Some(p.clone());
        }
    }
    let floor = tip.saturating_sub(depth - 1).max(1);
    let lo = prev.map_or(floor, |p| (p.hi + 1).min(floor)).max(1);
    Some(SweepState {
        lo,
        hi: tip.max(lo),
        cursor: None,
        exhausted: false,
    })
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
    /// A HINT only (round-2 review H2): `/arc-ingest`'s bearer is the public
    /// txid, so the marker is plantable; it triggers a re-verify of the
    /// stored proof against chaintracks and changes nothing by itself.
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

/// What a bump ANCHORS a tx to: the block height and the merkle root the
/// bump computes. Two bumps for one tx that differ in EITHER name different
/// blocks (round-2 review M2: a tx mined in both competing blocks of a
/// reorg keeps its height and changes its root).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BumpAnchor {
    pub height: u64,
    pub root: String,
}

/// A pushed MINED proof for a tx the store already holds VERIFIED: is it a
/// RE-ANCHOR (the pushed bump names a different block than the stored one,
/// by height OR by root) or the same anchor again? `stored = None` means no
/// stored bump to compare (a fill, not a re-anchor).
pub fn is_reanchor(stored: Option<&BumpAnchor>, pushed: &BumpAnchor) -> bool {
    stored.is_some_and(|s| s.height != pushed.height || !s.root.eq_ignore_ascii_case(&pushed.root))
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "00000000000000001de5aa96baa3566ce66e4941f8295cc44cc85fc75949db4d";
    const ORPHAN: &str = "0000000000000000153e10f465dba9697e4bde364fdf3a3224a736b019ffbfb1";

    fn seen(prior: Option<&str>, max: Option<u64>) -> HeaderSeen {
        HeaderSeen { prior_hash_at_height: prior.map(String::from), max_height_before: max }
    }

    #[test]
    fn a_new_tip_extends_and_the_same_header_again_repeats() {
        assert_eq!(classify_tip_announce(965771, A, &HeaderSeen::default()), TipAnnounce::Extends);
        assert_eq!(classify_tip_announce(965772, A, &seen(None, Some(965771))), TipAnnounce::Extends);
        assert_eq!(
            classify_tip_announce(965771, A, &seen(Some(&A.to_uppercase()), Some(965771))),
            TipAnnounce::Repeat,
            "the same hash again (case-insensitive) is not a reorg"
        );
    }

    #[test]
    fn a_hash_change_at_the_same_height_is_the_one_reorg_producer() {
        // 2026-09-07 22:39:32Z: the orphan …153e10f4 was recorded first, then
        // the canonical …1de5aa96 announced at the same height.
        assert_eq!(
            classify_tip_announce(965771, A, &seen(Some(ORPHAN), Some(965771))),
            TipAnnounce::Reorg { from: 965771 }
        );
        // a replaced height BELOW the held tip is still a reorg from that height
        assert_eq!(
            classify_tip_announce(965772, A, &seen(Some(ORPHAN), Some(965773))),
            TipAnnounce::Reorg { from: 965772 }
        );
    }

    #[test]
    fn a_lower_unrecorded_height_is_an_old_header_never_a_reorg() {
        // the former "tip decreased" arm: two webhook tasks landing out of
        // order announce 965772 after 965773 was held. Nothing is demoted.
        assert_eq!(classify_tip_announce(965772, A, &seen(None, Some(965773))), TipAnnounce::Old);
        assert_eq!(classify_tip_announce(1, A, &seen(None, Some(965773))), TipAnnounce::Old);
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
    fn the_next_window_continues_an_unfinished_walk_and_never_skips_a_height() {
        // fresh: the last 3 heights
        assert_eq!(
            next_sweep_window(None, 965774, 3),
            Some(SweepState { lo: 965772, hi: 965774, cursor: None, exhausted: false })
        );
        // in progress: anchored, even though the tip moved on
        let walking = SweepState { lo: 965771, hi: 965773, cursor: Some(RowKey { height: 965772, rowid: 40 }), exhausted: false };
        assert_eq!(next_sweep_window(Some(&walking), 965779, 3), Some(walking.clone()));
        // kept up: exhausted at 965773, tip 965774 → the last 3 heights again
        let done = SweepState { exhausted: true, ..walking.clone() };
        assert_eq!(
            next_sweep_window(Some(&done), 965774, 3),
            Some(SweepState { lo: 965772, hi: 965774, cursor: None, exhausted: false })
        );
        // fell behind: exhausted at 965770, tip 965774 → continue at 965771 (no height skipped)
        let behind = SweepState { lo: 965769, hi: 965770, cursor: None, exhausted: true };
        assert_eq!(
            next_sweep_window(Some(&behind), 965774, 3),
            Some(SweepState { lo: 965771, hi: 965774, cursor: None, exhausted: false })
        );
        // the same tip again after exhaustion: a restart from the newest
        let same = SweepState { lo: 965772, hi: 965774, cursor: None, exhausted: true };
        assert_eq!(
            next_sweep_window(Some(&same), 965774, 3),
            Some(SweepState { lo: 965772, hi: 965774, cursor: None, exhausted: false })
        );
        assert_eq!(next_sweep_window(None, 2, 6), Some(SweepState { lo: 1, hi: 2, cursor: None, exhausted: false }));
        assert_eq!(next_sweep_window(None, 965774, 0), None);
        assert_eq!(next_sweep_window(None, 0, 3), None);
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
    fn a_reanchor_is_a_different_height_or_a_different_root_never_a_fill() {
        let stored = BumpAnchor { height: 965771, root: "aa".repeat(32) };
        assert!(is_reanchor(Some(&stored), &BumpAnchor { height: 965773, root: "aa".repeat(32) }), "a new height");
        assert!(
            is_reanchor(Some(&stored), &BumpAnchor { height: 965771, root: "bb".repeat(32) }),
            "the SAME height with another root: a tx in both competing blocks"
        );
        assert!(!is_reanchor(Some(&stored), &BumpAnchor { height: 965771, root: "AA".repeat(32) }), "the same anchor, case-insensitive");
        assert!(!is_reanchor(None, &BumpAnchor { height: 965773, root: "aa".repeat(32) }), "no stored bump: a fill");
    }
}
