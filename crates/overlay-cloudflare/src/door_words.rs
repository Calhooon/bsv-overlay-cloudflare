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
//! The adapter. `overlay_discovery::arcade_words` stays the ONE reading of
//! Arcade's vocabulary and feeds [`arcade_hint`] (#624, L8): the tracker's
//! `Verdict::from_arc_body` and `into_hint` cannot replace it yet without
//! changing the door's answers, which this item does not do. K3
//! (bsv-stack-lean #82) is open at the pinned 0.4.0 (as at 0.3.0; 0.4.1
//! moves `from_arc_body`'s vocabulary and is its own re-pin): `into_hint` maps STUMP_PROCESSING and PENDING_RETRY to
//! `HintStatus::Unknown`, and a MINED or IMMUTABLE word needs a `blockHeight`
//! to be `Mined`. The other differences are pinned by name in
//! `tracker_vocabulary_differs_from_the_door_exactly_here` below.
//!
//! The K3 residue. `HintStatus` has no word for "in a block, the path
//! pending" or "parked", and the gate's bar admits STUMP_PROCESSING and a
//! height-less MINED. Such a hint is `Unknown` and carries Arcade's word in
//! its `verdict`; `k3_residue` is the one place the policy reads that word,
//! and the place L8 deletes once K3 lands.
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
use overlay_discovery::arcade_words::{arcade_verdict, ArcadeVerdict};

/// The hint source the door names Arcade by (the app layer's `tracker_host`
/// names it the same).
pub const ARCADE_SOURCE: &str = "arcade";

/// The one adapter: an Arcade transaction word as the tracker's hint, read
/// through `arcade_words` (any letter case, any surrounding space). `None`:
/// not a word Arcade defines, empty included, which is no input. The hint's
/// `verdict` keeps Arcade's word (trimmed, upper case) and `extraInfo` as
/// metadata: the reason text of a refusal, never a gate input (the #213
/// stale-extraInfo trap). The pure classifiers pass `observed = 0`: the door
/// stores no hint; the served state is the app layer's.
pub fn arcade_hint(tx_status: &str, extra_info: &str, observed: Timestamp) -> Option<Hint> {
    let word = tx_status.trim().to_ascii_uppercase();
    let status = match arcade_verdict(tx_status) {
        ArcadeVerdict::Invalid => return None,
        ArcadeVerdict::Pending => HintStatus::Accepted,
        ArcadeVerdict::Seen => HintStatus::Seen,
        ArcadeVerdict::Rejected => HintStatus::Rejected {
            reason: if extra_info.is_empty() {
                word.clone()
            } else {
                extra_info.to_string()
            },
        },
        // the competitor is named by the couriers and the conflict pass,
        // never by this word
        ArcadeVerdict::Conflict => HintStatus::DoubleSpend {
            competitors: Vec::new(),
        },
        ArcadeVerdict::Orphan => HintStatus::OrphanMempool,
        // the K3 residue: no HintStatus carries these yet
        ArcadeVerdict::Parked | ArcadeVerdict::InBlock | ArcadeVerdict::Mined => {
            HintStatus::Unknown
        }
    };
    Some(Hint {
        source: ARCADE_SOURCE.to_string(),
        status,
        observed,
        verdict: Some(Verdict {
            status: word,
            height: None,
            reason: (!extra_info.is_empty()).then(|| extra_info.to_string()),
            competitors: Vec::new(),
        }),
    })
}

/// K3 (bsv-stack-lean #82): Arcade's word behind an `Unknown` hint, the one
/// place the policy reads it. Deleted by L8 (#624) when the tracker's
/// vocabulary carries "in a block" and "parked".
fn k3_residue(hint: &Hint) -> Option<ArcadeVerdict> {
    match hint.status {
        HintStatus::Unknown => hint.verdict.as_ref().map(|v| arcade_verdict(&v.status)),
        _ => None,
    }
}

/// The host's bar: the network holds it (seen, in a block, or mined). Never
/// an orphan view (#267), never a refusal.
pub fn network_holds(hint: &Hint) -> bool {
    matches!(hint.status, HintStatus::Seen | HintStatus::Mined { .. })
        || matches!(
            k3_residue(hint),
            Some(ArcadeVerdict::InBlock | ArcadeVerdict::Mined)
        )
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
        Some(h) if matches!(k3_residue(h), Some(ArcadeVerdict::Mined)) => ProofClass::Mined,
        Some(h) if matches!(h.status, HintStatus::Seen) => ProofClass::Seen,
        _ => ProofClass::Other,
    }
}

/// Arcade's word as the hint carries it (trimmed, upper case).
fn word_of(hint: &Hint) -> String {
    hint.verdict
        .as_ref()
        .map(|v| v.status.clone())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TXID: &str = "aa00000000000000000000000000000000000000000000000000000000000001";

    /// L8's blocker, measured: each Arcade-shaped body read through the
    /// tracker's own `Verdict::from_arc_body` + `into_hint` and this host's
    /// policy, against the door's answer. The differences are exactly these;
    /// a tracker bump that closes one (K3) reds here and tells L8 to look.
    /// bsv-tracker 0.4.1 closed two (lane W8e, at 0.6.0): the status word is
    /// trimmed (" spaced") and an `ORPHAN` in `extraInfo` no longer makes an
    /// orphan ("SEEN stale orphan extraInfo"); the seven below stand.
    #[test]
    fn tracker_vocabulary_differs_from_the_door_exactly_here() {
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
            ("INVALID", r#"{"txStatus":"INVALID"}"#),
            ("UTXO_SPENT", r#"{"txStatus":"UTXO_SPENT"}"#),
            ("CONFIRMED", r#"{"txStatus":"CONFIRMED","blockHeight":970710}"#),
        ];
        let mut differs = Vec::new();
        for (label, body) in bodies {
            let word: serde_json::Value = serde_json::from_str(body).unwrap();
            let word = word["txStatus"].as_str().unwrap();
            let door = gate(arcade_hint(word, "", 0).as_ref());
            let tracker = Verdict::from_arc_body(TXID, body.as_bytes())
                .ok()
                .map(|v| v.into_hint(ARCADE_SOURCE, 0));
            let tracker = gate(tracker.as_ref().filter(|h| h.status != HintStatus::Unknown));
            if door != tracker {
                differs.push(format!("{label}: door {door:?}, tracker {tracker:?}"));
            }
        }
        assert_eq!(
            differs,
            [
                "STUMP_PROCESSING: door Reached, tracker Pending",
                "MINED no height: door Reached, tracker Pending",
                "IMMUTABLE no height: door Reached, tracker Pending",
                "SEEN with competitors: door Reached, tracker Fatal",
                "INVALID: door Pending, tracker Fatal",
                "UTXO_SPENT: door Pending, tracker Fatal",
                "CONFIRMED: door Pending, tracker Reached",
            ],
            "the tracker's vocabulary moved: re-read L8 (#624) against K3"
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
