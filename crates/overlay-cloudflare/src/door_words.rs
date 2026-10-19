//! bsv-low #619 (L3, the words map's step C2, its admission slice): the door
//! hosts bsv-tracker for Arcade's words.
//!
//! Every place the door hears an Arcade word (the gated arm's synchronous
//! submit echo and its poll, the probe, the pending watch's look, the #371
//! witness, the `/arc-ingest` callback, the retire pass's live look, the proof
//! look) now carries it as the tracker's own value, a [`bsv_tracker::Hint`]
//! from source `arcade`, and the door's answer is the HOST'S POLICY over that
//! hint (`gate`, [`callback`], [`refusal`], [`live_look`], [`proof_class`]).
//! A hint writes no chain word (the tracker charter's tier rule): admitting
//! an index row on it is the host's own act, as the words map's S1 states.
//!
//! The adapter (bsv-low #624, L8). Arcade's words are read by the tracker
//! alone: [`arcade_hint`] hands the status word to bsv-tracker 0.6.0's
//! `Verdict::from_arc_body` (the vocabulary, the trim and the letter case,
//! the metadata bounds) and `Verdict::into_hint` (the hint tier).
//! `overlay_discovery::arcade_words::ArcadeVerdict` is deleted. The door
//! reads the status word ONLY, as it always has (#213: no gate reads
//! `extraInfo`); a body's `blockHeight`, `merklePath` and `competingTxs` are
//! read by their own readers (the proof look against chaintracks, the
//! conflict pass of #602), never by this adapter.
//!
//! The host's policy over the hint (#619's rule: admission is host policy
//! over hints) keeps three readings the tracker's `HintStatus` does not
//! carry, each decided in `docs/words/L8-ARCADE-VERDICT-TABLE.md`:
//! - [`arcade_speaks`]: a word the tracker knows that Arcade does not define
//!   (ARC's INVALID, UTXO_SPENT, CONFIRMED, STORED, ...) is no input, the
//!   vector's `invalid` rule (P0-2d, #52), except ARC's
//!   SEEN_IN_ORPHAN_MEMPOOL, the #267 orphan view;
//! - STUMP_PROCESSING (the tracker's `Accepted`) holds: in a block, the gate's
//!   bar;
//! - MINED / IMMUTABLE with no height (the tracker's `Unknown`, the word on
//!   `verdict.status`) holds and is the proof look's mined claim, checked
//!   against chaintracks by its caller.
//!
//! What this slice does NOT move (bsv-low #619's other halves, stopped on the
//! byte-identity bar, see the PR): the eviction still rests on #214's
//! corroborated refusal (Arcade's fatal hint, or its absence, plus both
//! indexers' absence), not on the tracker's `conflicted` / `rejected` alone;
//! the retire and absence memos still write `refused` / `retired`.

use crate::admit_fast::CallbackAction;
use crate::broadcaster::GateVerdict;
use crate::proof_fetcher::ArcadeLook;
use bsv_tracker::{Hint, HintStatus, Timestamp, Verdict};

/// The hint source the door names Arcade by (the app layer's `tracker_host`
/// names it the same).
pub const ARCADE_SOURCE: &str = "arcade";

/// The words Arcade defines (`[SRC] arcade@1ae1208 models/transaction.go:89-126`,
/// `AllStatuses`; the vector's `words`, pinned equal). The tracker's
/// vocabulary is wider (it reads ARC too); this list is the SOURCE's, the
/// host's rule of which words this source speaks, never a parse.
pub const ARCADE_WORDS: [&str; 12] = [
    "UNKNOWN",
    "RECEIVED",
    "SENT_TO_NETWORK",
    "ACCEPTED_BY_NETWORK",
    "SEEN_ON_NETWORK",
    "SEEN_MULTIPLE_NODES",
    "DOUBLE_SPEND_ATTEMPTED",
    "REJECTED",
    "PENDING_RETRY",
    "STUMP_PROCESSING",
    "MINED",
    "IMMUTABLE",
];

/// The one adapter: an Arcade transaction word as the tracker's hint. The
/// word is read by `Verdict::from_arc_body` over the word's own body and
/// reduced by `into_hint`; `None` is a body the tracker refuses (a word
/// outside its vocabulary, empty, or carrying a control character) or a word
/// Arcade does not speak ([`arcade_speaks`]), which is no input. The hint's
/// `verdict` keeps the word (trimmed, upper case, the tracker's) and
/// `extraInfo` as the reason: text attached after the word is read, so its
/// bounds never unread a word and it is never a gate input (the #213
/// stale-extraInfo trap). The pure classifiers pass `observed = 0`: the door
/// stores no hint; the served state is the app layer's.
pub fn arcade_hint(tx_status: &str, extra_info: &str, observed: Timestamp) -> Option<Hint> {
    let body = serde_json::json!({ "txStatus": tx_status }).to_string();
    let mut verdict = Verdict::from_arc_body("", body.as_bytes()).ok()?;
    verdict.reason = (!extra_info.is_empty()).then(|| extra_info.to_string());
    let hint = verdict.into_hint(ARCADE_SOURCE, observed);
    arcade_speaks(&hint).then_some(hint)
}

/// The host's rule of the source: Arcade speaks its twelve words, and the
/// orphan view is read wherever it comes from (#267: an orphan-pool residence
/// is never a network accept, and the answer is the ancestry rungs).
pub fn arcade_speaks(hint: &Hint) -> bool {
    matches!(hint.status, HintStatus::OrphanMempool)
        || word_of_hint(hint).is_some_and(|w| ARCADE_WORDS.contains(&w))
}

/// The tracker's word behind the hint (its `verdict.status`).
fn word_of_hint(hint: &Hint) -> Option<&str> {
    hint.verdict.as_ref().map(|v| v.status.as_str())
}

/// In a block, the path pending: the tracker reads STUMP_PROCESSING as
/// `Accepted` (a broadcaster still working); the gate's bar holds it.
fn in_a_block(hint: &Hint) -> bool {
    matches!(hint.status, HintStatus::Accepted) && word_of_hint(hint) == Some("STUMP_PROCESSING")
}

/// A mined word with no height: the tracker keeps it `Unknown` (no evidence of
/// a height); the host reads it as Arcade's mined claim, never a chain word.
fn mined_without_height(hint: &Hint) -> bool {
    matches!(hint.status, HintStatus::Unknown)
        && matches!(word_of_hint(hint), Some("MINED" | "IMMUTABLE"))
}

/// The host's bar: the network holds it (seen, in a block, or mined). Never
/// an orphan view (#267), never a refusal.
pub fn network_holds(hint: &Hint) -> bool {
    matches!(hint.status, HintStatus::Seen | HintStatus::Mined { .. })
        || in_a_block(hint)
        || mined_without_height(hint)
}

/// Arcade's refusal (REJECTED, DOUBLE_SPEND_ATTEMPTED): a HINT the door
/// corroborates before it refuses or evicts (#214).
pub fn refusal(hint: &Hint) -> bool {
    matches!(
        hint.status,
        HintStatus::Rejected { .. } | HintStatus::DoubleSpend { .. }
    )
}

/// The gate step's reading of one hint; no hint is `Pending` (never admits,
/// never refuses: the ladder ends at the second broadcaster).
pub(crate) fn gate(hint: Option<&Hint>) -> GateVerdict {
    match hint {
        Some(h) if matches!(h.status, HintStatus::OrphanMempool) => GateVerdict::Orphan,
        Some(h) if refusal(h) => GateVerdict::Fatal,
        Some(h) if network_holds(h) => GateVerdict::Reached,
        _ => GateVerdict::Pending,
    }
}

/// The `/arc-ingest` status-only push: a trigger, never the word itself (the
/// bearer is the public txid). A holding hint asks Arcade live before it
/// latches; a refusal or an orphan view runs the evidence check; everything
/// else does nothing.
pub fn callback(hint: Option<&Hint>) -> CallbackAction {
    match (gate(hint), hint) {
        (GateVerdict::Reached, _) => CallbackAction::LatchSeen,
        (GateVerdict::Fatal | GateVerdict::Orphan, Some(h)) => {
            CallbackAction::EvidenceCheck(word_of(h))
        }
        _ => CallbackAction::Ignore,
    }
}

/// The retire pass's live look: a refusal is `Fatal` with its word and the
/// reason text; no hint is a `Fault` (the webhook evidence and the
/// both-indexer bar decide, as behind an outage); every other hint, an
/// orphan view among them, is `Present`.
pub fn live_look(hint: Option<&Hint>, extra_info: String) -> ArcadeLook {
    match hint {
        Some(h) if refusal(h) => ArcadeLook::Fatal(word_of(h), extra_info),
        None => ArcadeLook::Fault,
        Some(_) => ArcadeLook::Present,
    }
}

/// What the proof look may do with a hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProofClass {
    /// Mined (MINED / IMMUTABLE): the body's `merklePath` is a candidate
    /// the caller checks against chaintracks.
    Mined,
    /// Seen, unmined: a fresh word stops the ladder (no proof exists yet).
    Seen,
    /// Anything else, STUMP_PROCESSING included: the couriers are asked.
    Other,
}

/// The proof look's reading of one hint.
pub fn proof_class(hint: Option<&Hint>) -> ProofClass {
    match hint {
        Some(h) if matches!(h.status, HintStatus::Mined { .. }) => ProofClass::Mined,
        Some(h) if mined_without_height(h) => ProofClass::Mined,
        Some(h) if matches!(h.status, HintStatus::Seen) => ProofClass::Seen,
        _ => ProofClass::Other,
    }
}

/// Arcade's word as the hint carries it (trimmed, upper case).
fn word_of(hint: &Hint) -> String {
    word_of_hint(hint).unwrap_or_default().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TXID: &str = "aa00000000000000000000000000000000000000000000000000000000000001";

    /// The tracker's bare hint tier against the door's answer, after L8
    /// (#624, bsv-tracker 0.6.0): every body is read by the tracker on both
    /// sides; `bare` is the gate a host with NO policy would give
    /// (`HintStatus` alone), `door` is [`gate`] over [`arcade_hint`]. The rows
    /// that still differ differ by POLICY, not vocabulary, each named with its
    /// rule (`docs/words/L8-ARCADE-VERDICT-TABLE.md`). Closed at 0.4.1 and gone
    /// from this list: STUMP_PROCESSING's word (now `Accepted`, held by the
    /// door's in-a-block rule), the spaced word, the stale ORPHAN `extraInfo`.
    #[test]
    fn the_doors_policy_differs_from_the_bare_hint_exactly_here() {
        let bodies: &[(&str, &str)] = &[
            ("UNKNOWN", r#"{"txStatus":"UNKNOWN"}"#),
            ("RECEIVED", r#"{"txStatus":"RECEIVED"}"#),
            ("SENT_TO_NETWORK", r#"{"txStatus":"SENT_TO_NETWORK"}"#),
            ("ACCEPTED_BY_NETWORK", r#"{"txStatus":"ACCEPTED_BY_NETWORK"}"#),
            ("SEEN_ON_NETWORK", r#"{"txStatus":"SEEN_ON_NETWORK"}"#),
            ("SEEN_MULTIPLE_NODES", r#"{"txStatus":"SEEN_MULTIPLE_NODES"}"#),
            ("DOUBLE_SPEND_ATTEMPTED", r#"{"txStatus":"DOUBLE_SPEND_ATTEMPTED"}"#),
            ("REJECTED", r#"{"txStatus":"REJECTED"}"#),
            ("PENDING_RETRY", r#"{"txStatus":"PENDING_RETRY"}"#),
            ("STUMP_PROCESSING", r#"{"txStatus":"STUMP_PROCESSING"}"#),
            ("MINED no height", r#"{"txStatus":"MINED"}"#),
            ("MINED", r#"{"txStatus":"MINED","blockHeight":970710}"#),
            ("IMMUTABLE no height", r#"{"txStatus":"IMMUTABLE"}"#),
            (
                "SEEN stale orphan extraInfo",
                r#"{"txStatus":"SEEN_MULTIPLE_NODES","extraInfo":"ORPHAN parents missing"}"#,
            ),
            (
                "SEEN with competitors",
                r#"{"txStatus":"SEEN_ON_NETWORK","competingTxs":["bb00000000000000000000000000000000000000000000000000000000000002"]}"#,
            ),
            (" spaced", r#"{"txStatus":" SEEN_ON_NETWORK "}"#),
            (
                "SEEN_IN_ORPHAN_MEMPOOL",
                r#"{"txStatus":"SEEN_IN_ORPHAN_MEMPOOL"}"#,
            ),
            ("INVALID", r#"{"txStatus":"INVALID"}"#),
            ("UTXO_SPENT", r#"{"txStatus":"UTXO_SPENT"}"#),
            ("CONFIRMED", r#"{"txStatus":"CONFIRMED","blockHeight":970710}"#),
        ];
        fn bare(hint: Option<&Hint>) -> GateVerdict {
            match hint.map(|h| &h.status) {
                Some(HintStatus::OrphanMempool) => GateVerdict::Orphan,
                Some(HintStatus::Rejected { .. } | HintStatus::DoubleSpend { .. }) => {
                    GateVerdict::Fatal
                }
                Some(HintStatus::Seen | HintStatus::Mined { .. }) => GateVerdict::Reached,
                _ => GateVerdict::Pending,
            }
        }
        let mut differs = Vec::new();
        for (label, body) in bodies {
            let word: serde_json::Value = serde_json::from_str(body).unwrap();
            let word = word["txStatus"].as_str().unwrap();
            let door = gate(arcade_hint(word, "", 0).as_ref());
            let tracker = Verdict::from_arc_body(TXID, body.as_bytes())
                .ok()
                .map(|v| v.into_hint(ARCADE_SOURCE, 0));
            let bare = bare(tracker.as_ref());
            if door != bare {
                differs.push(format!("{label}: door {door:?}, bare hint {bare:?}"));
            }
        }
        assert_eq!(
            differs,
            [
                // the gate's bar: in a block (D1, SEEN or better; the vector's engine_gate)
                "STUMP_PROCESSING: door Reached, bare hint Pending",
                // Arcade's mined claim with no height: the gate's bar; the
                // proof look checks the path against chaintracks
                "MINED no height: door Reached, bare hint Pending",
                "IMMUTABLE no height: door Reached, bare hint Pending",
                // the door reads the status word only (#213); a competitor is
                // the conflict pass's (#602), never a refusal by this body
                "SEEN with competitors: door Reached, bare hint Fatal",
                // ARC's words: Arcade does not define them, no input (the
                // vector's invalid rule, P0-2d #52)
                "INVALID: door Pending, bare hint Fatal",
                "UTXO_SPENT: door Pending, bare hint Fatal",
                "CONFIRMED: door Pending, bare hint Reached",
            ],
            "the tracker's vocabulary or the door's policy moved: re-read the L8 table"
        );
    }

    /// The host's list of Arcade's words is the vector's `words`, in order.
    #[test]
    fn arcade_words_are_the_vectors_twelve() {
        let v: serde_json::Value = serde_json::from_str(include_str!(
            "../../overlay-discovery/vectors/arcade_status_verdicts.json"
        ))
        .unwrap();
        let words: Vec<&str> = v["words"]
            .as_array()
            .unwrap()
            .iter()
            .map(|w| w["word"].as_str().unwrap())
            .collect();
        assert_eq!(words, ARCADE_WORDS);
    }

    /// The three inputs whose answer moved at L8 (the table's vocabulary rows):
    /// a word with a control character and two ORPHAN-bearing words no
    /// broadcaster defines are bodies the tracker refuses, so no input.
    #[test]
    fn l8_moved_a_control_character_word_and_undefined_orphan_words_to_no_input() {
        for w in [" MINED\n", "orphaned", "ORPHAN"] {
            assert!(arcade_hint(w, "", 0).is_none(), "{w:?}");
            assert_eq!(gate(None), GateVerdict::Pending);
        }
        assert_eq!(
            arcade_hint(" stump_processing ", "", 0).map(|h| h.status),
            Some(HintStatus::Accepted)
        );
        assert_eq!(
            gate(arcade_hint("SEEN_IN_ORPHAN_MEMPOOL", "", 0).as_ref()),
            GateVerdict::Orphan
        );
    }

    #[test]
    fn the_hint_carries_the_word_and_the_reason_never_a_chain_word() {
        let h = arcade_hint(" rejected ", "fee too low", 7).unwrap();
        assert_eq!(h.source, "arcade");
        assert_eq!(h.observed, 7);
        assert_eq!(
            h.status,
            HintStatus::Rejected {
                reason: "fee too low".into()
            }
        );
        assert_eq!(h.verdict.as_ref().unwrap().status, "REJECTED");
        assert!(arcade_hint("STORED", "", 0).is_none());
        assert!(arcade_hint("", "", 0).is_none());
        // the stale-extraInfo trap: the reason never moves the gate
        let healed = arcade_hint("SEEN_MULTIPLE_NODES", "ORPHAN: failed to validate", 0);
        assert_eq!(gate(healed.as_ref()), GateVerdict::Reached);
        // a mined hint stays a hint: the tracker's state moves to no chain word on it
        let mut state = bsv_tracker::State::new(TXID);
        let params = bsv_tracker::Params { age_threshold: 0 };
        struct NoHeaders;
        impl bsv_tracker::Headers for NoHeaders {
            fn header_at(
                &self,
                _: bsv_tracker::Height,
            ) -> Result<Option<bsv_tracker::Header>, bsv_tracker::HeaderError> {
                Ok(None)
            }
            fn tip_height(&self) -> Result<bsv_tracker::Height, bsv_tracker::HeaderError> {
                Ok(0)
            }
        }
        state
            .step(
                &params,
                &NoHeaders,
                bsv_tracker::Input::Hint(arcade_hint("MINED", "", 1).unwrap()),
            )
            .unwrap();
        assert!(state.word().chain().is_none(), "{:?}", state.word());
    }
}
