//! P0-2d (bsv-stack-lean #52): Arcade's BLOCK words and reorg markers in the
//! engine.
//!
//! Arcade's TRANSACTION words are read by bsv-tracker since bsv-low #624
//! (L8): `Verdict::from_arc_body` and `Verdict::into_hint`, hosted by the
//! overlay's door (`overlay-cloudflare` `door_words::arcade_hint`), whose
//! policy over the hint keeps the door's answers
//! (`docs/words/L8-ARCADE-VERDICT-TABLE.md`). The nine-class `ArcadeVerdict`
//! that lived here is deleted: one reading of Arcade's words, the tracker's.
//!
//! What stays: the block processing-status feed the reorg consumer reads
//! ([`arcade_block_status`], read exactly as Arcade stores it) and the reorg
//! markers in `extraInfo` ([`arcade_reorg_marker`]), a re-ask of the stored
//! proof against our headers, never a settlement change by themselves.
//!
//! Arcade's words at the pin: `[SRC] arcade@1ae1208 models/transaction.go:89-126`
//! (twelve, `AllStatuses`), reorg markers `:319-332`; block statuses
//! `models/block.go:30-42`.

pub use crate::pot::reorg::{arcade_reorg_marker, ArcadeReorgMarker};

/// What one row of Arcade's block processing-status listing says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArcadeBlockStatus {
    /// `active`: on Arcade's active chain.
    Active,
    /// `orphaned`: Arcade orphaned it. A HINT corroborated against our
    /// headers before any row anchored there is re-judged by its own proof.
    Orphaned,
    /// `parked`: on the active chain, its BUMP abandoned by Arcade's watchdog.
    Parked,
    /// Anything else: not a block status Arcade defines.
    Invalid,
}

/// Read one Arcade block status word, exactly as Arcade stores it
/// (`models/block.go:30-42`, lower case). Unlike a transaction word, a
/// differently spelled block status is no event: the consumer's pin
/// (`pot::arcade_events` tests, `the_sse_reorg_frame_and_an_unknown_status_are_not_events`)
/// holds that an `ORPHANED` row is not an orphan event.
pub fn arcade_block_status(status: &str) -> ArcadeBlockStatus {
    match status {
        "active" => ArcadeBlockStatus::Active,
        "orphaned" => ArcadeBlockStatus::Orphaned,
        "parked" => ArcadeBlockStatus::Parked,
        _ => ArcadeBlockStatus::Invalid,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_statuses() {
        assert_eq!(arcade_block_status("active"), ArcadeBlockStatus::Active);
        assert_eq!(arcade_block_status("orphaned"), ArcadeBlockStatus::Orphaned);
        assert_eq!(arcade_block_status("ORPHANED"), ArcadeBlockStatus::Invalid);
        assert_eq!(arcade_block_status(" orphaned"), ArcadeBlockStatus::Invalid);
        assert_eq!(arcade_block_status("parked"), ArcadeBlockStatus::Parked);
        assert_eq!(arcade_block_status("stale"), ArcadeBlockStatus::Invalid);
        assert_eq!(arcade_block_status(""), ArcadeBlockStatus::Invalid);
    }
}
