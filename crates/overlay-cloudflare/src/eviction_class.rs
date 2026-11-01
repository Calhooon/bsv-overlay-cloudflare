//! bsv-low #1019: the eviction's refusal CLASS, recorded at the write and served beside its chain word.
//!
//! The eviction serves its chain word (`evicted.word`: `rejected`, `conflicted`, or null; #621), never the ledger's
//! reason text, so a reader of `/utxo-status` (the tower's late arm, the felt) read every Arcade terminal refusal
//! the same. The ledger now records the refusal's class (`pot_evictions.refusalClass`) in the OPEN marker's own
//! statement (`admit_fast::OPEN_EVICTION_MARKER_SQL`'s `?4`), from Arcade's `txStatus` and `extraInfo` read through
//! the tracker's hint ([`refusal_class_of`] over `door_words::arcade_hint`), and both readers serve it as
//! `evicted.class` (`overlay_discovery::pot::storage::served_eviction_value`), additive.
//!
//! The vocabulary is bounded (`overlay_discovery::pot::storage::REFUSAL_CLASSES`): `missing-parent`,
//! `double-spend`, `non-final`, `policy`, `absence`. A reason no rule below names is `None` (NULL, the key absent
//! on the wire), never a new word.
//!
//! Which arm writes which class ([`eviction_refusal_class`] over the reason each arm already writes):
//! - Arcade's live terminal word with both indexers absent (the pending watch's and the callback's evidence check,
//!   `admit_fast::evidence_verdict_of`'s Fatal arm, `{status} (arcade live {word}: {extra}; both indexers absent)`):
//!   the word and the hint's reading of `extraInfo`: DOUBLE_SPEND_ATTEMPTED or a competitor named is
//!   `double-spend`, `TX_LOCK_TIME (35)` (past #836's bound) `non-final`, a missing input or parent (or Arcade's
//!   orphan view) `missing-parent`, a fee or `TX_POLICY` refusal `policy`, anything else (a script failure) `None`;
//! - the ladder's re-refusal under an open eviction (`admit_fast::ladder_rerefusal_reason`): the same reading of
//!   the text the ladder carries (its leading Arcade word, else REJECTED);
//! - the conflict pass (`admit_fast::conflict_eviction_reason`): `double-spend`;
//! - an eviction on absence alone, a fault-filled recorded word, and the retire pass (every
//!   `overlay_discovery::pot::storage::ABSENCE_REASON_MARKS`): `absence`.
//!
//! The order is the chain word's (`eviction_chain_word`): the conflict mark, then the absence marks (so a refusal
//! whose text also carries an absence mark is `absence`, as it is no chain word), then Arcade's word.
//!
//! REACH: the class is DISPLAY and classification only. Nothing admits, refuses, presses, readmits or evicts on
//! it: no gate, no owed judgement, no readmission reads `refusalClass`. Arcade's `extraInfo` is a stranger's text
//! (the #213 trap), so a class is a reading of what Arcade said, never evidence.

use crate::door_words::{arcade_hint, lock_time_not_final};
use bsv_tracker::HintStatus;
use overlay_discovery::pot::storage::{
    ABSENCE_REASON_MARKS, CONFLICT_REASON_MARK, REFUSAL_CLASS_ABSENCE, REFUSAL_CLASS_DOUBLE_SPEND,
    REFUSAL_CLASS_MISSING_PARENT, REFUSAL_CLASS_NON_FINAL, REFUSAL_CLASS_POLICY,
};

/// The texts of a non-final refusal (Teranode's `TX_LOCK_TIME (35)`, ARC's 476 "not final"), lower case.
pub const NON_FINAL_TOKENS: [&str; 3] = ["tx_lock_time (35)", "not final", "non-final"];
/// The texts of a refusal that names a competitor or a double spend, lower case (`utxo already spent by tx` is
/// scrubbed to `utxo spent by tx` by #519's belt, so the token is `spent by tx`).
pub const DOUBLE_SPEND_TOKENS: [&str; 5] = [
    "utxo_spent (70)",
    "tx_conflicting (36)",
    "spent by tx",
    "double spend",
    "double_spend",
];
/// The texts of a missing input or parent (Arcade's `TX_MISSING_PARENT`, bitcoind's `Missing inputs`), lower case.
pub const MISSING_PARENT_TOKENS: [&str; 3] =
    ["tx_missing_parent", "missing parent", "missing input"];
/// The texts of a policy refusal (`TX_POLICY (39)`, a fee too low), lower case.
pub const POLICY_TOKENS: [&str; 5] = [
    "tx_policy",
    "insufficient-fee",
    "insufficient fee",
    "fee too low",
    "fee is too low",
];

fn names_any(text: &str, tokens: &[&str]) -> bool {
    tokens.iter().any(|t| text.contains(t))
}

/// PURE: the class of one Arcade word and its `extraInfo`, read through the tracker's hint
/// (`door_words::arcade_hint`): the hint's DoubleSpend is `double-spend`, its orphan view `missing-parent`; a
/// Rejected is read by its reason (the `extraInfo` the hint carries) in this order: non-final (#836's reader, or
/// its tokens), a competitor named, a missing parent, a policy refusal; anything else, and every word that is no
/// refusal, is `None`.
pub fn refusal_class_of(tx_status: &str, extra_info: &str) -> Option<&'static str> {
    let hint = arcade_hint(tx_status, extra_info, 0)?;
    match &hint.status {
        HintStatus::DoubleSpend { .. } => Some(REFUSAL_CLASS_DOUBLE_SPEND),
        HintStatus::OrphanMempool => Some(REFUSAL_CLASS_MISSING_PARENT),
        HintStatus::Rejected { reason } => {
            let text = reason.to_ascii_lowercase();
            if lock_time_not_final(tx_status, extra_info).is_some()
                || names_any(&text, &NON_FINAL_TOKENS)
            {
                Some(REFUSAL_CLASS_NON_FINAL)
            } else if names_any(&text, &DOUBLE_SPEND_TOKENS) {
                Some(REFUSAL_CLASS_DOUBLE_SPEND)
            } else if names_any(&text, &MISSING_PARENT_TOKENS) {
                Some(REFUSAL_CLASS_MISSING_PARENT)
            } else if names_any(&text, &POLICY_TOKENS) {
                Some(REFUSAL_CLASS_POLICY)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// The evidence check's live word inside a reason: `arcade live {word}: {extra}; both indexers absent`. The
/// producer's own mark is the FIRST `arcade live ` (the reason opens with the status the eviction rests on);
/// the extra runs to the LAST `; both indexers absent` (the text between is Arcade's, a stranger's).
fn live_word(reason: &str) -> Option<(&str, &str)> {
    const MARK: &str = "arcade live ";
    const TAIL: &str = "; both indexers absent";
    let rest = &reason[reason.find(MARK)? + MARK.len()..];
    let rest = match rest.rfind(TAIL) {
        Some(end) => &rest[..end],
        None => rest.trim_end_matches(')'),
    };
    Some(match rest.split_once(": ") {
        Some((word, extra)) => (word.trim(), extra),
        None => (rest.split(';').next().unwrap_or(rest).trim(), ""),
    })
}

/// The ladder's re-refusal (`REJECTED ({inner}; re-refused at the ladder)`): its inner text.
fn ladder_inner(reason: &str) -> Option<&str> {
    reason
        .strip_prefix("REJECTED (")?
        .strip_suffix("; re-refused at the ladder)")
}

/// The leading Arcade word of a ladder text (`{txStatus} {extraInfo}`, `Arcade {status} {detail} ...`), when it
/// is a word Arcade speaks; else none (`ARC HTTP 465: ...`, a sync refusal's bare reason).
fn leading_word(text: &str) -> Option<&str> {
    let mut tokens = text.split_whitespace();
    let first = tokens.next()?;
    let word = if first == "Arcade" {
        tokens.next()?
    } else {
        first
    };
    let word = word.trim_end_matches(':');
    arcade_hint(word, "", 0).map(|_| word)
}

/// PURE (bsv-low #1019): the class of an eviction from the ledger reason its arm writes (the module's table). The
/// one function the OPEN marker binds (`eviction_write`), so every arm, and every re-eviction that reuses a stored
/// reason, records the class of the reason it writes.
pub fn eviction_refusal_class(reason: &str) -> Option<&'static str> {
    if reason.starts_with("input ") && reason.contains(CONFLICT_REASON_MARK) {
        return Some(REFUSAL_CLASS_DOUBLE_SPEND);
    }
    // Arcade's own live word first (round 1's NOTE-1): a retire reason that carries `arcade live {word}` is the
    // network's terminal word, not an absence, however the reason opens.
    if let Some((word, extra)) = live_word(reason) {
        return refusal_class_of(word, extra);
    }
    if let Some(inner) = ladder_inner(reason) {
        return refusal_class_of(leading_word(inner).unwrap_or("REJECTED"), inner);
    }
    if ABSENCE_REASON_MARKS.iter().any(|m| reason.contains(m)) {
        return Some(REFUSAL_CLASS_ABSENCE);
    }
    None
}

#[cfg(test)]
pub(crate) mod tests;
