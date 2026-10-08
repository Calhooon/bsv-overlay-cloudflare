//! P0-2d (bsv-stack-lean #52): the ONE reading of Arcade's words in the engine.
//!
//! Every place the engine hears Arcade (the network gate's submit echo, poll,
//! probe and witness look; the `/arc-ingest` callback action and its terminal
//! record; the proof look and the live look of the proof fetcher; the block
//! processing-status feed the reorg consumer reads) calls [`arcade_verdict`]
//! or [`arcade_block_status`], so one word gets one verdict wherever it
//! arrives. The rule (`crates/overlay-discovery/vectors/arcade_status_verdicts.json`,
//! `rule`, carried byte-identical by bsv-wallet-toolbox-rs, whose
//! `arcade_verdict` has the same nine classes):
//!
//! - REJECTED fails; DOUBLE_SPEND_ATTEMPTED fails as a conflict; any word
//!   containing ORPHAN fails as an orphan view (missing parents). Each is a
//!   HINT: the engine corroborates before it refuses or evicts (#214).
//! - every other word Arcade defines is accepted as the hint it is: pending,
//!   parked, seen, in a block, mined.
//! - a word Arcade does not define, an empty word, no word: invalid, an
//!   unusable answer, never a success and never a verdict on the transaction.
//! - letter case and surrounding space never change a transaction word's
//!   verdict (a block status is read exactly as Arcade stores it).
//! - the reorg markers in `extraInfo` ([`arcade_reorg_marker`]) are a re-ask
//!   of the stored proof against our headers, never a settlement change by
//!   themselves; the word beside a marker is read like any other word.
//!
//! Arcade's words at the pin: `[SRC] arcade@1ae1208 models/transaction.go:89-126`
//! (twelve, `AllStatuses`), terminal `:128-138`, reorg markers `:319-332`;
//! block statuses `models/block.go:30-42`.

pub use crate::pot::reorg::{arcade_reorg_marker, ArcadeReorgMarker};

/// What one Arcade transaction word means. The same nine classes as the
/// toolbox's `providers::arcade::ArcadeVerdict` (bsv-wallet-toolbox-rs 0.4.0).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArcadeVerdict {
    /// RECEIVED, SENT_TO_NETWORK, ACCEPTED_BY_NETWORK: on its way; REJECTED
    /// may still follow (`models/transaction.go:227-237`).
    Pending,
    /// UNKNOWN, PENDING_RETRY: no network verdict yet; PENDING_RETRY is parked
    /// for rebroadcast (`services/propagation/propagator.go:2362-2395`).
    Parked,
    /// SEEN_ON_NETWORK, SEEN_MULTIPLE_NODES: the network holds it.
    Seen,
    /// STUMP_PROCESSING: in a block, the BUMP being built; no path yet.
    InBlock,
    /// MINED, IMMUTABLE: mined; the body may carry the path.
    Mined,
    /// REJECTED: a refusal (the ARC code rides in the body's `status`).
    Rejected,
    /// DOUBLE_SPEND_ATTEMPTED: a refusal naming a conflict.
    Conflict,
    /// Any word containing ORPHAN: Arcade holds it without its parents.
    Orphan,
    /// Not a word Arcade defines (empty included): no answer.
    Invalid,
}

impl ArcadeVerdict {
    /// The network holds it (seen, in a block or mined): the gate's bar.
    pub fn network_holds(self) -> bool {
        matches!(self, Self::Seen | Self::InBlock | Self::Mined)
    }

    /// REJECTED or DOUBLE_SPEND_ATTEMPTED: Arcade's terminal refusals.
    pub fn is_refusal(self) -> bool {
        matches!(self, Self::Rejected | Self::Conflict)
    }
}

/// Read one Arcade transaction word. Any case, any surrounding space.
pub fn arcade_verdict(tx_status: &str) -> ArcadeVerdict {
    let word = tx_status.trim().to_ascii_uppercase();
    // the orphan check first: SEEN_IN_ORPHAN_MEMPOOL contains SEEN (#267)
    if word.contains("ORPHAN") {
        return ArcadeVerdict::Orphan;
    }
    match word.as_str() {
        "RECEIVED" | "SENT_TO_NETWORK" | "ACCEPTED_BY_NETWORK" => ArcadeVerdict::Pending,
        "UNKNOWN" | "PENDING_RETRY" => ArcadeVerdict::Parked,
        "SEEN_ON_NETWORK" | "SEEN_MULTIPLE_NODES" => ArcadeVerdict::Seen,
        "STUMP_PROCESSING" => ArcadeVerdict::InBlock,
        "MINED" | "IMMUTABLE" => ArcadeVerdict::Mined,
        "REJECTED" => ArcadeVerdict::Rejected,
        "DOUBLE_SPEND_ATTEMPTED" => ArcadeVerdict::Conflict,
        _ => ArcadeVerdict::Invalid,
    }
}

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
    fn the_twelve_words_and_their_classes() {
        use ArcadeVerdict::*;
        for (word, want) in [
            ("UNKNOWN", Parked),
            ("RECEIVED", Pending),
            ("SENT_TO_NETWORK", Pending),
            ("ACCEPTED_BY_NETWORK", Pending),
            ("SEEN_ON_NETWORK", Seen),
            ("SEEN_MULTIPLE_NODES", Seen),
            ("DOUBLE_SPEND_ATTEMPTED", Conflict),
            ("REJECTED", Rejected),
            ("PENDING_RETRY", Parked),
            ("STUMP_PROCESSING", InBlock),
            ("MINED", Mined),
            ("IMMUTABLE", Mined),
        ] {
            assert_eq!(arcade_verdict(word), want, "{word}");
            assert_eq!(
                arcade_verdict(&word.to_ascii_lowercase()),
                want,
                "{word} lower"
            );
            assert_eq!(arcade_verdict(&format!(" {word}\n")), want, "{word} spaced");
        }
    }

    #[test]
    fn orphan_words_before_seen_and_undefined_words_invalid() {
        assert_eq!(
            arcade_verdict("SEEN_IN_ORPHAN_MEMPOOL"),
            ArcadeVerdict::Orphan
        );
        assert_eq!(arcade_verdict("orphaned"), ArcadeVerdict::Orphan);
        for w in [
            "",
            "  ",
            "NOT_A_STATUS",
            "MINED_IN_STALE_BLOCK",
            "STORED",
            "ANNOUNCED_TO_NETWORK",
        ] {
            assert_eq!(arcade_verdict(w), ArcadeVerdict::Invalid, "{w:?}");
        }
    }

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
