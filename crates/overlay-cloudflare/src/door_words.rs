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
/// reason text, unless it is Arcade's non-final word ([`lock_time_not_final`]:
/// `NotFinal`, a time, never a verdict; bsv-low #836); no hint is a `Fault`
/// (the webhook evidence and the both-indexer bar decide, as behind an
/// outage); a word below SEEN ([`holds_below_seen`]) is `Holding`, a hint that
/// promotes nothing (bsv-low #845); every other hint, an orphan view among
/// them, is `Present`.
pub fn live_look(hint: Option<&Hint>, extra_info: String) -> ArcadeLook {
    match hint {
        Some(h) if refusal(h) && not_final_hint(h, &extra_info).is_some() => {
            ArcadeLook::NotFinal(word_of(h), extra_info)
        }
        Some(h) if refusal(h) => ArcadeLook::Fatal(word_of(h), extra_info),
        None => ArcadeLook::Fault,
        Some(h) if holds_below_seen(h) || no_network_verdict(h) => ArcadeLook::Holding(word_of(h)),
        Some(_) => ArcadeLook::Present,
    }
}

/// bsv-low #845 (bsv-wallet-toolbox-rs 0.11.2, its `e28acdc`: the vector's
/// `toolbox_status` is `unknown` for these three): the words that say ARCADE
/// holds the transaction, never that the network does. They promote nothing
/// anywhere the door reads Arcade's word: the gate holds them below its bar
/// (`Pending`), the callback ignores them, and the live look is `Holding`,
/// never `Present` (the evidence check asks the indexers and refuses nothing
/// on them; the retire pass takes its absence arm).
pub const ARCADE_HOLDING_WORDS: [&str; 3] = ["RECEIVED", "SENT_TO_NETWORK", "ACCEPTED_BY_NETWORK"];

/// Arcade holds it below SEEN: one of [`ARCADE_HOLDING_WORDS`].
pub fn holds_below_seen(hint: &Hint) -> bool {
    word_of_hint(hint).is_some_and(|w| ARCADE_HOLDING_WORDS.contains(&w))
}

/// bsv-low #836 fold (the lens's L3): Arcade's two words that carry no network verdict at all, UNKNOWN and
/// PENDING_RETRY ("no network verdict ... parked for durable rebroadcast"). bsv-wallet-toolbox-rs 0.11.2's vector
/// reads both `toolbox_status: unknown`, as for [`ARCADE_HOLDING_WORDS`], and the app layer reads neither as
/// presence (`txany.rs` `ARCADE_SEEN_STATUSES`). The live look is `Holding` for them too: never presence (the
/// indexers are asked, the evidence check refuses nothing on them), the retire pass's absence arm (48 h), so a
/// subject Arcade parks for good with both indexers absent is no longer kept live by the word.
pub const ARCADE_NO_VERDICT_WORDS: [&str; 2] = ["UNKNOWN", "PENDING_RETRY"];

/// Arcade's word carries no network verdict: one of [`ARCADE_NO_VERDICT_WORDS`].
pub fn no_network_verdict(hint: &Hint) -> bool {
    word_of_hint(hint).is_some_and(|w| ARCADE_NO_VERDICT_WORDS.contains(&w))
}

/// The code Arcade's peer names a non-final transaction by (Teranode's
/// `TX_LOCK_TIME (35)`; arcade@85bfe5d `services/propagation/propagator.go:
/// 1796-1798` maps it to 476 `StatusNotFinal` with a retryable suffix).
pub const ARCADE_LOCK_TIME_CODE: &str = "TX_LOCK_TIME (35)";

/// bsv-low #836: Arcade's REJECTED that names [`ARCADE_LOCK_TIME_CODE`]. The
/// transaction is not final against Arcade's PEER's chain view, which can trail
/// the chain (loop 44: the peer at 972172 while the tip was 972185, thirteen
/// blocks behind). A non-final transaction cannot be held by any node yet, so
/// its absence proves nothing: the word is a time, never a verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotFinal {
    /// The lock height the peer read (`lock time (H)`), when the text names one.
    pub lock_height: Option<u64>,
    /// The PEER's block height (`... block height (P)`), when the text names one: a hint recorded beside the
    /// word, never a gate input (the lag is the courier's, not the chain's).
    pub peer_height: Option<u64>,
    /// Arcade's text as reason text, its already-known tokens scrubbed as #519 scrubs a refusal's.
    pub reason: String,
}

/// The heights in parentheses after the code: the first the lock height, the
/// last (when there are two or more) the peer's height.
fn not_final_hint(hint: &Hint, extra_info: &str) -> Option<NotFinal> {
    if !matches!(hint.status, HintStatus::Rejected { .. }) {
        return None;
    }
    let at = extra_info.find(ARCADE_LOCK_TIME_CODE)?;
    let tail = &extra_info[at + ARCADE_LOCK_TIME_CODE.len()..];
    let heights: Vec<u64> = tail
        .split('(')
        .skip(1)
        .filter_map(|part| part.split(')').next()?.trim().parse().ok())
        .collect();
    Some(NotFinal {
        lock_height: heights.first().copied(),
        peer_height: (heights.len() >= 2).then(|| heights[heights.len() - 1]),
        reason: crate::broadcaster::scrub_already_known_tokens(extra_info),
    })
}

/// bsv-low #836: Arcade's word and its `extraInfo` read as a non-final word.
/// `Some` only for a REJECTED (any letter case) whose text names
/// [`ARCADE_LOCK_TIME_CODE`]; DOUBLE_SPEND_ATTEMPTED and every other word are
/// `None`. This is the one place the door reads `extraInfo` for a decision
/// (#213's stale-extraInfo trap): it reads it only under a REJECTED, and it
/// only ever turns a refusal into "not now", the fail-safe direction (a stale
/// or planted lock-time text can delay an eviction, never cause one or admit).
pub fn lock_time_not_final(tx_status: &str, extra_info: &str) -> Option<NotFinal> {
    not_final_hint(&arcade_hint(tx_status, "", 0)?, extra_info)
}

/// bsv-low #836 fold (the lens's M1): the blocks past the subject's own lock
/// height that Arcade's non-final word is still honoured as "not now". Loop 44
/// measured the peer 13 blocks behind the tip; past the lock height plus this
/// lag the word is Arcade's fatal word again, judged as any other refusal (the
/// two-source path, #513's age floor, the retire pass's 1 h arm).
pub const NOT_FINAL_LAG_BLOCKS: u64 = 24;

/// The same bound for a time lock (`nLockTime >= 500,000,000`, seconds): the
/// lag's 24 blocks at 600 s and the median time past's hour behind the clock.
pub const NOT_FINAL_LAG_SECS: u64 = NOT_FINAL_LAG_BLOCKS * 600 + 3_600;

/// The first nLockTime read as a time, not a height (the consensus rule).
const LOCK_TIME_THRESHOLD: u32 = 500_000_000;

/// The subject's own lock, read from its bytes (Arcade's `finality` rule,
/// arcade@85bfe5d `finality/finality.go`): final when nLockTime is 0 or every
/// input's nSequence is `0xffffffff`; else locked to a height or a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubjectLock {
    /// Final by its bytes: no peer can call it non-final, so a lock-time word is not about these bytes.
    Final,
    /// Locked to a block height.
    Height(u32),
    /// Locked to a unix time (seconds).
    Time(u32),
}

impl SubjectLock {
    /// The lock of a parsed transaction.
    pub fn of_tx(tx: &bsv_rs::transaction::Transaction) -> Self {
        if tx.lock_time == 0 || tx.inputs.iter().all(|i| i.sequence == u32::MAX) {
            SubjectLock::Final
        } else if tx.lock_time < LOCK_TIME_THRESHOLD {
            SubjectLock::Height(tx.lock_time)
        } else {
            SubjectLock::Time(tx.lock_time)
        }
    }
    /// The lock of the raw bytes, or of the extended format (`None`: neither parses).
    pub fn of_bytes(bytes: &[u8]) -> Option<Self> {
        bsv_rs::transaction::Transaction::from_binary(bytes)
            .or_else(|_| bsv_rs::transaction::Transaction::from_ef(bytes))
            .ok()
            .map(|tx| Self::of_tx(&tx))
    }
    /// The lock of `txid`'s own entry in a stored BEEF (`None`: the BEEF does not read or carries no such body).
    pub fn of_beef(beef: &[u8], txid: &str) -> Option<Self> {
        overlay_engine::beef_limits::transaction_from_beef(
            beef,
            Some(txid),
            &overlay_engine::beef_limits::STORED_BEEF_LIMITS,
        )
        .ok()
        .map(|tx| Self::of_tx(&tx))
    }
}

/// What bounds the hold: the subject's own lock (`None`: its bytes are not held here), the chaintracks tip
/// (`None`: no tracker, or it faulted) and the clock (seconds).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LockBound {
    /// The subject's own lock, from its bytes.
    pub lock: Option<SubjectLock>,
    /// The chain's tip, from chaintracks.
    pub tip: Option<u64>,
    /// Now, unix seconds.
    pub now_secs: u64,
}

/// PURE (bsv-low #836 fold, the lens's M1): whether Arcade's non-final word is still "not now". The hold ENDS by the
/// chain: past the lock height plus [`NOT_FINAL_LAG_BLOCKS`] (`tip + 1 > lock + lag`), or past a time lock plus
/// [`NOT_FINAL_LAG_SECS`], the word is Arcade's refusal again. A subject FINAL by its bytes is never held (the word
/// cannot be about them). The lock is the subject's BYTES; only where they are not held does the text's lock height
/// bound the hold (Arcade's report of those bytes: it can end the hold, and with no height it cannot lengthen it past
/// the retire pass's 48 h absence arm). An unknown tip holds (fail-safe; the 48 h arm still ends it).
pub fn not_final_holds(nf: &NotFinal, bound: &LockBound) -> bool {
    let by_height = |lock: u64| bound.tip.is_none_or(|tip| tip < lock + NOT_FINAL_LAG_BLOCKS);
    match bound.lock {
        Some(SubjectLock::Final) => false,
        Some(SubjectLock::Height(h)) => by_height(u64::from(h)),
        Some(SubjectLock::Time(t)) => bound.now_secs <= u64::from(t) + NOT_FINAL_LAG_SECS,
        None => nf.lock_height.is_none_or(by_height),
    }
}

/// bsv-low #836 fold: [`lock_time_not_final`] under the chain's bound ([`not_final_holds`]): `Some` only while the
/// word is still "not now". Every decision reads this; [`lock_time_not_final`] alone is the text.
pub fn lock_time_not_final_under(
    tx_status: &str,
    extra_info: &str,
    bound: &LockBound,
) -> Option<NotFinal> {
    lock_time_not_final(tx_status, extra_info).filter(|nf| not_final_holds(nf, bound))
}

/// PURE (bsv-low #836 fold): the live look under the chain's bound: a `NotFinal` past it is Arcade's `Fatal` word
/// again, its text unchanged; every other look is itself.
pub fn bound_look(look: ArcadeLook, bound: &LockBound) -> ArcadeLook {
    match look {
        ArcadeLook::NotFinal(word, extra)
            if lock_time_not_final_under(&word, &extra, bound).is_none() =>
        {
            ArcadeLook::Fatal(word, extra)
        }
        look => look,
    }
}

/// Now, unix seconds (the Workers clock; std off wasm32 for the native pins).
pub fn now_secs() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        (worker::js_sys::Date::now() / 1000.0) as u64
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
}

/// The chaintracks tip (`None`: no tracker, or it faulted: the hold stands, fail-safe).
pub async fn chain_tip(tracker: Option<&dyn bsv_rs::transaction::ChainTracker>) -> Option<u64> {
    tracker?.current_height().await.ok().map(u64::from)
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

    /// bsv-low #836: the non-final word is read only under a REJECTED naming TX_LOCK_TIME (35); the heights are the
    /// text's (the lock height first, the peer's last), and the reason is scrubbed as #519 scrubs a refusal's.
    #[test]
    fn fix836_the_non_final_word_and_its_heights() {
        let loop44 = "TX_LOCK_TIME (35): lock time (972185) as block height is not less than block height (972172) \u{2014} retryable: the transaction is not final against this peer's chain view; resubmit after the locktime expires";
        let nf =
            lock_time_not_final(" rejected ", loop44).expect("a REJECTED naming TX_LOCK_TIME (35)");
        assert_eq!(
            (nf.lock_height, nf.peer_height),
            (Some(972_185), Some(972_172))
        );
        assert!(!nf.reason.contains('\u{2014}'), "{}", nf.reason);
        assert!(
            nf.reason
                .starts_with("TX_LOCK_TIME (35): lock time (972185)"),
            "{}",
            nf.reason
        );
        // one height only: the lock height, no peer's
        let one = lock_time_not_final("REJECTED", "TX_LOCK_TIME (35): lock time (972185)").unwrap();
        assert_eq!((one.lock_height, one.peer_height), (Some(972_185), None));
        // the code bare: still non-final, no heights
        let bare = lock_time_not_final("REJECTED", "TX_LOCK_TIME (35)").unwrap();
        assert_eq!((bare.lock_height, bare.peer_height), (None, None));
        // never under another word, never without the code, never a near spelling
        for (word, extra) in [
            ("DOUBLE_SPEND_ATTEMPTED", loop44),
            ("SEEN_ON_NETWORK", loop44),
            ("RECEIVED", loop44),
            ("REJECTED", "TX_INVALID (31): script failed"),
            ("REJECTED", "UTXO_SPENT (70): spent"),
            ("REJECTED", "TX_LOCK_TIME: lock time (972185)"),
            ("REJECTED", ""),
            ("NOT_A_WORD", loop44),
        ] {
            assert!(lock_time_not_final(word, extra).is_none(), "{word} {extra}");
        }
        // the gate's word is unchanged (REJECTED stays Arcade's refusal hint); only the live look moves
        assert_eq!(
            gate(arcade_hint("REJECTED", loop44, 0).as_ref()),
            GateVerdict::Fatal
        );
        assert!(matches!(
            live_look(arcade_hint("REJECTED", "", 0).as_ref(), loop44.to_string()),
            ArcadeLook::NotFinal(ref w, _) if w == "REJECTED"
        ));
        assert!(matches!(
            live_look(
                arcade_hint("REJECTED", "", 0).as_ref(),
                "TX_INVALID (31)".to_string()
            ),
            ArcadeLook::Fatal(..)
        ));
    }

    /// bsv-low #845: the three words below SEEN are `Holding` on the live look and nowhere a promotion: the gate
    /// holds them (`Pending`), the callback ignores them, the proof look asks the couriers; SEEN and STUMP_PROCESSING
    /// are still the network's hold.
    #[test]
    fn fix845_the_holding_words_promote_nothing() {
        // the #836 fold's L3: the two words that carry no network verdict read the same way
        for w in ARCADE_HOLDING_WORDS.into_iter().chain(ARCADE_NO_VERDICT_WORDS) {
            for spelled in [w.to_string(), w.to_ascii_lowercase()] {
                let h = arcade_hint(&spelled, "", 0);
                assert_eq!(gate(h.as_ref()), GateVerdict::Pending, "{spelled}");
                assert_eq!(callback(h.as_ref()), CallbackAction::Ignore, "{spelled}");
                assert_eq!(proof_class(h.as_ref()), ProofClass::Other, "{spelled}");
                assert!(!network_holds(h.as_ref().unwrap()), "{spelled}");
                assert_eq!(
                    live_look(h.as_ref(), String::new()),
                    ArcadeLook::Holding(w.to_string())
                );
            }
        }
        for w in [
            "SEEN_ON_NETWORK",
            "SEEN_MULTIPLE_NODES",
            "STUMP_PROCESSING",
            "MINED",
        ] {
            assert_eq!(
                live_look(arcade_hint(w, "", 0).as_ref(), String::new()),
                ArcadeLook::Present,
                "{w}"
            );
        }
        // the toolbox 0.11.2 vector names the three `unknown` on its status surface
        let v: serde_json::Value = serde_json::from_str(include_str!(
            "../../overlay-discovery/vectors/arcade_status_verdicts.json"
        ))
        .unwrap();
        for case in v["cases"].as_array().unwrap() {
            if case["surface"] != "status" {
                continue;
            }
            let word = case["body"]["txStatus"].as_str().unwrap_or_default();
            if ARCADE_HOLDING_WORDS.contains(&word) || ARCADE_NO_VERDICT_WORDS.contains(&word) {
                assert_eq!(case["expect"]["toolbox_status"], "unknown", "{word}");
            }
        }
    }

    /// A raw transaction: one input spending `00..01:0` with an empty unlocking script and sequence `seq`, one output
    /// of 1 sat locked by `OP_TRUE`, nLockTime `lock`.
    fn raw_tx(lock: u32, seq: u32) -> Vec<u8> {
        let mut raw = 1u32.to_le_bytes().to_vec();
        raw.push(1);
        raw.extend([1u8; 32]);
        raw.extend(0u32.to_le_bytes());
        raw.push(0);
        raw.extend(seq.to_le_bytes());
        raw.push(1);
        raw.extend(1u64.to_le_bytes());
        raw.extend([1u8, 0x51]);
        raw.extend(lock.to_le_bytes());
        raw
    }

    /// bsv-low #836 fold (the lens's M1, parts 1 and 2): the hold ENDS by the chain. The lock is the subject's own
    /// bytes (Arcade's finality rule); the word is "not now" only while `tip + 1 <= lock + NOT_FINAL_LAG_BLOCKS`
    /// (24), or a time lock plus `NOT_FINAL_LAG_SECS`; a subject final by its bytes is never held; no tip holds; only
    /// where the bytes are not held does the text's height bound it. Past the bound the look is Arcade's fatal word.
    #[test]
    fn fix836f_m1_the_hold_ends_by_the_chain() {
        assert_eq!(NOT_FINAL_LAG_BLOCKS, 24);
        assert_eq!(
            SubjectLock::of_bytes(&raw_tx(972_185, 0xffff_fffe)),
            Some(SubjectLock::Height(972_185))
        );
        assert_eq!(SubjectLock::of_bytes(&raw_tx(972_185, u32::MAX)), Some(SubjectLock::Final));
        assert_eq!(SubjectLock::of_bytes(&raw_tx(0, 0)), Some(SubjectLock::Final));
        assert_eq!(
            SubjectLock::of_bytes(&raw_tx(1_761_000_000, 0)),
            Some(SubjectLock::Time(1_761_000_000))
        );
        assert_eq!(SubjectLock::of_bytes(b"not a transaction"), None);
        let loop44 = "TX_LOCK_TIME (35): lock time (972185) as block height is not less than block height (972172)";
        let nf = lock_time_not_final("REJECTED", loop44).unwrap();
        let height = |tip: Option<u64>| LockBound {
            lock: Some(SubjectLock::Height(972_185)),
            tip,
            now_secs: 0,
        };
        // the window: the loop 44 tip, the last tip inside it, the first past it
        assert!(not_final_holds(&nf, &height(Some(972_185))));
        assert!(not_final_holds(&nf, &height(Some(972_208))));
        assert!(!not_final_holds(&nf, &height(Some(972_209))));
        // no tip: held (fail-safe; the retire pass's 48 h absence arm still ends it)
        assert!(not_final_holds(&nf, &height(None)));
        // final by its bytes: never held, whatever the tip
        let final_ = LockBound {
            lock: Some(SubjectLock::Final),
            tip: Some(972_185),
            now_secs: 0,
        };
        assert!(!not_final_holds(&nf, &final_));
        // a time lock against the clock
        let time = |now_secs| LockBound {
            lock: Some(SubjectLock::Time(1_761_000_000)),
            tip: Some(972_185),
            now_secs,
        };
        assert!(not_final_holds(&nf, &time(1_761_000_000 + NOT_FINAL_LAG_SECS)));
        assert!(!not_final_holds(&nf, &time(1_761_000_000 + NOT_FINAL_LAG_SECS + 1)));
        // the bytes not held here: the text's height bounds it; with no height in the text, held
        let bytes_absent = |tip| LockBound {
            lock: None,
            tip: Some(tip),
            now_secs: 0,
        };
        assert!(not_final_holds(&nf, &bytes_absent(972_208)));
        assert!(!not_final_holds(&nf, &bytes_absent(972_209)));
        let bare = lock_time_not_final("REJECTED", "TX_LOCK_TIME (35)").unwrap();
        assert!(not_final_holds(&bare, &bytes_absent(2_000_000)));
        // the bytes decide over the text: a text naming a far lock height over bytes locked lower ends at the bytes'
        let far = lock_time_not_final("REJECTED", "TX_LOCK_TIME (35): lock time (999999999)").unwrap();
        assert!(!not_final_holds(&far, &height(Some(972_209))));
        // the decision and the look under the bound
        assert!(lock_time_not_final_under("REJECTED", loop44, &height(Some(972_209))).is_none());
        assert!(lock_time_not_final_under("REJECTED", loop44, &height(Some(972_185))).is_some());
        let live = live_look(arcade_hint("REJECTED", "", 0).as_ref(), loop44.to_string());
        assert_eq!(
            bound_look(live.clone(), &height(Some(972_209))),
            ArcadeLook::Fatal("REJECTED".into(), loop44.into())
        );
        assert_eq!(bound_look(live.clone(), &height(Some(972_185))), live);
        assert_eq!(bound_look(ArcadeLook::Missing, &final_), ArcadeLook::Missing);
    }

    /// bsv-low #836 fold (the lens's M1, part 3): the door re-feeds a subject past its bound once an isolate.
    #[test]
    fn fix836f_m1_the_refeed_is_once_a_txid() {
        let txid = "AB".repeat(32);
        assert!(crate::broadcaster::refeed_once(&txid));
        assert!(!crate::broadcaster::refeed_once(&txid.to_ascii_lowercase()));
        assert!(crate::broadcaster::refeed_once(&"cd".repeat(32)));
    }

    /// bsv-low #836 fold (the lens's M1): every site that reads Arcade's non-final word reads it under the chain's
    /// bound: the door (probe judgement, echo, poll, the re-feed), the evidence check, the pending watch, the retire
    /// pass; and the push is the evidence check's (the lens's L1).
    #[test]
    fn fix836f_m1_every_site_reads_the_word_under_the_bound() {
        let strip = |s: &str| s.chars().filter(|c| !c.is_whitespace()).collect::<String>();
        let code = |s: &str| strip(&s[..s.find("#[cfg(test)]").unwrap_or(s.len())]);
        let broadcaster = code(include_str!("broadcaster.rs"));
        let admit = code(include_str!("admit_fast.rs"));
        let proof = code(include_str!("proof_fetcher.rs"));
        let routes = code(include_str!("routes.rs"));
        // the door's decisions: only under the bound
        for needle in [
            "ifletSome(nf)=crate::door_words::lock_time_not_final_under(arcade_status,extra_info,bound)",
            "crate::door_words::lock_time_not_final_under(arcade_status,extra_info,bound).is_some()",
            "self.not_final_now(&parsed.tx_status,&parsed.extra_info).await",
            "self.not_final_now(&resp.tx_status,&resp.extra_info).await",
            "ifletSome(fresh)=self.refeed_past_lock_bound(&probe,subject_txid,subject_ef).await",
            "self.submit_ef(&self.tx_endpoint(),subject_txid,&leg.ef,false)",
        ] {
            assert!(broadcaster.contains(needle), "broadcaster: {needle}");
        }
        assert!(!broadcaster.contains("ifletSome(nf)=crate::door_words::lock_time_not_final("));
        assert!(admit.contains("look=crate::door_words::bound_look(look,&bound);"), "the evidence check");
        assert!(
            admit.contains("last=crate::broadcaster::bound_witness_look(last,&bound);"),
            "the pending watch"
        );
        assert!(
            proof.contains("look=crate::door_words::bound_look(look,&port.lock_bound(txid).await);"),
            "the retire pass"
        );
        assert!(routes.contains("matchcrate::admit_fast::callback_action(&tx_status){"), "the push");
        assert!(!routes.contains("HoldNotFinal") && !admit.contains("HoldNotFinal"));
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
