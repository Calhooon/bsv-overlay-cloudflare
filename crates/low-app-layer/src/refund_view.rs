//! `/refund-view` — the per-identity REFUND STATUS view (bsv-low #252
//! stage 2a).
//!
//! The client's refund pipeline today lives in localStorage: the pre-signed
//! refund, its recovery height, and the "did it land" poll are all
//! client-side state a wiped profile loses. This view is the SERVER half of
//! stage 2a: for one identity, every pot it is a party to, shaped as a
//! refund-status answer — how far the height gate is, whether a
//! refund-backup marker row is indexed for the pot, and what (if anything)
//! the chain proves happened to it. DISPLAY-ONLY by decision (#252 stage-2
//! plan §4):
//! `backupMarkerPresent` is PRESENCE only — `refundRawHex` is never served here;
//! recovery paths keep their existing per-pot `lookupPotRefund`.
//!
//! ## Honesty model (mirrors `/results`' `outcome`/`outcomeSource`)
//!
//! RECONCILED 2026-08-03 (#323): the two views now share ONE confirmation
//! bar, and it is stated here so the "mirrors `/results`" claim above is
//! checkable rather than asserted. Until #323, `/results` derived a verdict
//! from `spent = 1` ALONE while this view already required a confirmed
//! spend — so the two genuinely disagreed and this comment was false. The
//! shared bar is now: a spend counts as a LANDING iff `spentConfirmed = 1`
//! OR the spender carries a chaintracks-verified proof
//! (`pot_beefs.proof_verified`, the #323 MEDIUM-4 allowance for rows the
//! migration stamped 0). A recorded-but-unconfirmed pointer is a
//! displaceable INTENT in both views, and both still SERVE it as a raw fact
//! while refusing to derive any conclusion from it.
//!
//! The `status`/`statusSource` pair never asserts what the data doesn't
//! prove:
//!
//! | facts                                                        | status       | source           |
//! |--------------------------------------------------------------|--------------|------------------|
//! | pot unspent, tip below the gate                              | `armed`      | chain (+marker)  |
//! | pot unspent, tip at/past the gate                            | `gate-open`  | chain (+marker)  |
//! | spend CONFIRMED, trusted decoded verdict = `refund`          | `landed`     | chain            |
//! | spend CONFIRMED, trusted decoded verdict = settle/tie        | `superseded` | chain            |
//! | anything incomplete (below)                                  | `unknown`    | null             |
//!
//! `unknown` is a first-class value, never a guess: a pot with no
//! `pot_records` row (spend status genuinely unknown — NEVER asserted
//! unspent), a spend that is recorded but unconfirmed (displaceable), or a
//! confirmed spend with no trusted decoded verdict (#284 columns absent, a
//! stale `verdictTxid` pointer, or an unrecognized stored string) all answer
//! `unknown`/`null`.
//!
//! `statusSource` is `"chain"` when the status derives purely from
//! `pot_records` spend/verdict facts; `"chain+marker"` when the
//! `potrefund_records` PRESENCE bit factored into the wording; `null` for
//! `unknown`. The bit is named `backupMarkerPresent` — NOT "a backup
//! exists" — because `tm_potrefund` admission is BYTE-FORMAT-ONLY: either
//! seat OR any third party can file a marker row for any pot outpoint for
//! one dust `OP_RETURN`, and the overlay never parses or verifies the
//! stored bytes. Presence means exactly "at least one marker row is
//! indexed for this outpoint", nothing more. The READER verifies
//! (doctrine): before rendering a real re-broadcastable backup, the client
//! must fetch the marker via its existing per-pot `lookupPotRefund`,
//! verify the signature, and parse the raw refund bytes itself.
//!
//! Deliberately NOT derived server-side: a `parked`/`unarmed` distinction.
//! The server cannot see tower arm state or client dormancy — that honesty
//! gap stays client-side (stage 2a client work).
//!
//! ## The durable `timeline` block (bsv-low #217 — the presence RECORD half)
//!
//! Presence and timing live in the ephemeral relay + client localStorage, so
//! there is no durable "when did this hand start / end" for history, disputes
//! or analytics. This block is the persistent complement. It is
//! **DISPLAY/AUDIT ONLY and must never become a money gate** — no status, no
//! credit, no gate, and nothing in [`derive_refund_status`] reads it. The
//! LIVE presence signal stays the relay's `lastSeenMs`.
//!
//! Every value is a STORED column served by a plain `SELECT`; the route
//! computes no timestamp of its own. What each one IS, because a consumer
//! must never have to guess (epoch Rule 21's covenant-vs-marker sort, epoch
//! Rule 13's "surface it, do not consume it"):
//!
//! | field | what it is a time OF | provenance |
//! |---|---|---|
//! | `potAdmittedAt` | THIS overlay first admitting the pot's FUNDING output (`pot_records.createdAt`, write-once — the conflict update never touches it) | server-observed, over an index entry that only exists because a real funded output was submitted |
//! | `firstPartyMarkerAt` | THIS overlay admitting the OLDEST party marker NAMING THE CALLER for this outpoint (`potparty_records.createdAt`) | server-observed, over **attacker-writable content** |
//! | `firstSpentAt` | THIS overlay first recording an ACCEPTED spend pointer (`pot_records.firstSpentAt`, write-once) | server-observed |
//! | `spentHeight` | the BLOCK the spend confirmed in | NETWORK-ANCHORED (an SPV-verified BUMP), the strongest of the four |
//!
//! **No timestamp here is client-claimed, and none may become one.** No LOW
//! marker carries a time push, so nothing an attacker writes can move any of
//! these values; what an attacker CAN do is make a marker exist. That is why
//! `firstPartyMarkerAt` is named for the marker rather than for the seat: the
//! representative row is chosen sig-valid-first (the #283 latch) then oldest,
//! but `tm_potparty` admission is byte-format-only, so on a pot with no
//! sig-valid marker the oldest row naming the caller may have been filed by
//! somebody else (the #281 F1b residual, inherited whole). If a
//! client-CLAIMED time is ever recorded, it needs its own column and its own
//! field name — never one of these.
//!
//! `seatAnchor` is the actionable bit (Rule 13's corollary: a provenance
//! label the consumer cannot act on is telemetry, not information). It names
//! which stamp a consumer should read as the hand's START:
//! `"pot-admission"` when `potAdmittedAt` exists — index-backed, the pot's
//! own funding output is in the index; `"party-marker"` when only
//! `firstPartyMarkerAt` does — marker-backed, i.e. the weaker claim, and the
//! consumer's action is to treat the start time as approximate and unattested
//! (the pot itself is a JOIN MISS, which the row already says via
//! `spent: null`); `null` when neither is known, which is not a defect to
//! hide — an un-indexed pot with no admitted marker genuinely has no start
//! time here.
//!
//! **What is NOT served, deliberately:** `pot_records.spentAt`. It is the
//! #228 backstop AGE anchor and is re-stamped by every accepted spend write
//! (the 0-conf pointer, its confirm, a reorg-displacing spender), which makes
//! it a correct age gate and a lying audit stamp — one field whose meaning
//! depends on which writer touched it last. `firstSpentAt` exists precisely
//! so the durable question has its own column.
//!
//! **Permanent NULLs, said plainly (epoch Rule 6):** `firstSpentAt` is NULL
//! for every pot whose spend was recorded before that migration, and no
//! backfill or re-latch pass can ever repair it — SQL cannot recover a time
//! nobody observed. This is not self-healing. `null` there means "no accepted
//! spend write since #217 shipped", never "unspent" (`spent`/`spentConfirmed`
//! answer that).
//!
//! **Still missing, and it is not fixable in this layer:** a seat LEAVING.
//! Every LOW overlay topic rides a transaction, and leaving a table produces
//! none — so no leave/abandon event reaches the overlay at the moment it
//! happens. `firstSpentAt` on a `refund` verdict is the closest durable
//! proxy (the hand ended in a refund, at this observed time), and it is a
//! proxy for the RESOLUTION, not for the walk-away.
//!
//! ## The gate math
//!
//! `blocksToGate = max(0, recoveryHeight - tip)` when both are known, else
//! `null`; `gatePassed = tip >= recoveryHeight` when both known, else
//! `false` (fail-safe: a gate is never reported open on missing data). The
//! served `recoveryHeight` prefers the COVENANT-COMMITTED height decoded
//! from the admitted funding lock (`pot_records.recoveryHeight`, #284 —
//! chain truth an attacker cannot file) and falls back to the caller's own
//! `potparty_records` marker height (byte-format-admitted, i.e. a HINT) only
//! when no decoded value exists (bare/legacy pots). Either source is
//! range-checked against [`LOCKTIME_THRESHOLD`] — a nonsense height serves
//! `null`, never a fake countdown.
//!
//! ## The window is the `/results` window (bsv-low #281)
//!
//! `tm_potparty` admission is byte-format-only, so the same dust-DoS that
//! erased a victim's real pot from `/results` applies verbatim here — and an
//! erased refund row is money-visibility harm (the user stops seeing a
//! recoverable pot). The SQL reuses the exact #281 shape: per-POT-OUTPOINT
//! collapse (oldest marker is the representative), pot-existence tier with
//! the newest [`REFUND_VIEW_UNKNOWN_POT_QUOTA`] unknown pots promoted, LIMIT
//! [`REFUND_VIEW_MAX_ROWS`]. The `potrefund_records` presence probe runs on
//! the ≤100 survivors only (the results_sql BLOB-join discipline — never let
//! an attacker multiply per-row work), and transfers no bytes (EXISTS).
//!
//! Accepted residuals, inherited with the shape (documented in
//! `results_sql`'s own notes; no defense here): an attacker who copies ~100
//! REAL, recently-admitted pot txids out of the public index and files one
//! marker per pot naming the victim can still fill the window; and the
//! representative (oldest) marker row owns the DISPLAY fields — `gameId`
//! and the marker height hint — so a forged marker front-run with an
//! earlier `createdAt` can own them (#281 F1b). Both are display-tier harms
//! only here: the served recovery height prefers the covenant-COMMITTED
//! column precisely because of the second residual, and no status is ever
//! derived from marker fields alone.

use serde_json::json;

use crate::results::{PotVerdict, LOCKTIME_THRESHOLD};

/// Hard bound on `/refund-view` rows per request — same cap + rationale as
/// [`crate::results::RESULTS_MAX_ROWS`] (a cap on DISTINCT POTS since the
/// window is per-pot-outpoint).
pub const REFUND_VIEW_MAX_ROWS: usize = 100;

/// The `/refund-view` cursor's ceiling (the paging round, 2026-08-21) — same
/// bound + rationale as [`crate::results::RESULTS_VIEW_AFTER_MAX`].
pub const REFUND_VIEW_AFTER_MAX: usize = 1_000_000;

/// How many of the newest pots ABSENT from `pot_records` are promoted into
/// the main tier — same reservation + rationale as
/// [`crate::results::RESULTS_UNKNOWN_POT_QUOTA`] (a fresh pot whose `tm_pot`
/// admission is in flight is exactly the pot a refund view must not hide).
pub const REFUND_VIEW_UNKNOWN_POT_QUOTA: usize = 10;

const _: () = assert!(REFUND_VIEW_UNKNOWN_POT_QUOTA < REFUND_VIEW_MAX_ROWS);

/// The single `/refund-view` SQL (ONE bind: the lowercase identity). The
/// #281 window over the caller's `potparty_records` rows LEFT-JOINed to
/// `pot_records` on the pot outpoint, then — OUTSIDE the window, on the
/// ≤[`REFUND_VIEW_MAX_ROWS`] survivors only — an EXISTS probe of
/// `potrefund_records` for `backupMarkerPresent`. No BLOB is ever touched.
/// `pot_records.recoveryHeight` is aliased `covRecoveryHeight` (the potparty
/// marker owns the bare name), mirroring `results_sql`.
///
/// # #217 timeline columns
///
/// Three stamps ride out of the window unchanged — `potCreatedAt` and
/// `markerCreatedAt` were ALREADY selected (the #281 ordering keys) and are
/// now merely PROJECTED, and `r.firstSpentAt` is threaded up beside
/// `spentHeight`. Nothing is computed: this stays one bounded query, and
/// every served time is a stored column (epoch Rule 25 — a fact computable
/// at admission is never recomputed at read).
///
/// # #375 era write-off
///
/// `written_off_before_ms` set ⇒ the innermost scan drops rows whose era
/// anchor (`COALESCE(r.createdAt, pp.createdAt)` — the pot's admission stamp
/// when indexed, else the marker's; both server-written unix seconds)
/// pre-dates the cutoff, before the dedupe/quota windows run. ONE extra bind
/// (the cutoff, after the identity). `None` ⇒ byte-identical to the
/// pre-#375 query.
pub fn refund_view_sql(written_off_before_ms: Option<i64>, after: usize) -> String {
    format!(
        "SELECT w.gameId AS gameId, w.potTxid AS potTxid, w.potVout AS potVout, \
                w.recoveryHeight AS recoveryHeight, \
                w.covRecoveryHeight AS covRecoveryHeight, \
                w.spent AS spent, w.spendingTxid AS spendingTxid, \
                w.spentConfirmed AS spentConfirmed, \
                w.verdict AS verdict, w.verdictTxid AS verdictTxid, \
                w.spentHeight AS spentHeight, \
                w.potCreatedAt AS potAdmittedAt, \
                w.markerCreatedAt AS firstPartyMarkerAt, \
                w.firstSpentAt AS firstSpentAt, \
                EXISTS(SELECT 1 FROM potrefund_records pr \
                       WHERE pr.potTxid = w.potTxid AND pr.potVout = w.potVout) \
                    AS backupMarkerPresent, \
                sb.proof_verified AS spenderProofVerified, \
                w.spenderFinal AS spenderFinal, \
                ns.txid IS NOT NULL AS spenderSeen \
         FROM (SELECT gameId, potTxid, potVout, recoveryHeight, covRecoveryHeight, \
                  spent, spendingTxid, spentConfirmed, verdict, verdictTxid, spentHeight, \
                  firstSpentAt, spenderFinal, \
                  markerCreatedAt, markerRowid, potCreatedAt, potBestSigRank, tier \
           FROM (SELECT gameId, potTxid, potVout, recoveryHeight, covRecoveryHeight, \
                    spent, spendingTxid, spentConfirmed, verdict, verdictTxid, spentHeight, \
                    firstSpentAt, spenderFinal, \
                    markerCreatedAt, markerRowid, potCreatedAt, potBestSigRank, tier, \
                    DENSE_RANK() OVER (ORDER BY potBestSigRank DESC, tier ASC, \
                                                COALESCE(potCreatedAt, markerCreatedAt) DESC, \
                                                markerCreatedAt DESC, markerRowid DESC) \
                        AS finalRank \
           FROM (SELECT gameId, potTxid, potVout, recoveryHeight, covRecoveryHeight, \
                  spent, spendingTxid, spentConfirmed, verdict, verdictTxid, spentHeight, \
                  firstSpentAt, spenderFinal, \
                  markerCreatedAt, markerRowid, potCreatedAt, potBestSigRank, \
                  CASE WHEN unknownPot = 0 OR potRank <= {quota} THEN 0 ELSE 1 END AS tier \
           FROM (SELECT gameId, potTxid, potVout, recoveryHeight, covRecoveryHeight, \
                    spent, spendingTxid, spentConfirmed, verdict, verdictTxid, spentHeight, \
                    firstSpentAt, spenderFinal, \
                    markerCreatedAt, markerRowid, potCreatedAt, unknownPot, \
                    potBestSigRank, \
                    ROW_NUMBER() OVER (PARTITION BY unknownPot \
                                       ORDER BY potBestSigRank DESC, \
                                                COALESCE(potCreatedAt, markerCreatedAt) DESC, \
                                                markerCreatedAt DESC, markerRowid DESC) AS potRank \
             FROM (SELECT pp.gameId AS gameId, pp.potTxid AS potTxid, \
                      pp.potVout AS potVout, pp.recoveryHeight AS recoveryHeight, \
                      r.recoveryHeight AS covRecoveryHeight, \
                      r.spent AS spent, r.spendingTxid AS spendingTxid, \
                      r.spentConfirmed AS spentConfirmed, \
                      r.verdict AS verdict, r.verdictTxid AS verdictTxid, \
                      r.spentHeight AS spentHeight, \
                      r.firstSpentAt AS firstSpentAt, \
                      r.spenderFinal AS spenderFinal, \
                      pp.createdAt AS markerCreatedAt, pp.markerRowid AS markerRowid, \
                      r.createdAt AS potCreatedAt, \
                      CASE WHEN r.txid IS NULL THEN 1 ELSE 0 END AS unknownPot, \
                      MAX({rank}) OVER (PARTITION BY pp.potTxid, pp.potVout) \
                          AS potBestSigRank, \
                      ROW_NUMBER() OVER (PARTITION BY pp.potTxid, pp.potVout \
                                         ORDER BY {rank} DESC, \
                                                  pp.createdAt ASC, pp.markerRowid ASC) AS rn \
               FROM {party} pp \
               LEFT JOIN pot_records r \
                      ON r.txid = pp.potTxid AND r.outputIndex = pp.potVout \
               WHERE pp.identity = ?1{era}) \
             WHERE rn = 1))) \
           WHERE finalRank > {after} AND finalRank <= {after} + {probe} \
           ORDER BY potBestSigRank DESC, tier ASC, \
                    COALESCE(potCreatedAt, markerCreatedAt) DESC, \
                    markerCreatedAt DESC, markerRowid DESC \
           LIMIT {probe}) w \
         LEFT JOIN pot_beefs sb ON w.spendingTxid IS NOT NULL \
              AND sb.txid = lower(w.spendingTxid) \
         LEFT JOIN network_seen ns ON w.spendingTxid IS NOT NULL \
              AND ns.txid = lower(w.spendingTxid) \
         ORDER BY w.potBestSigRank DESC, w.tier ASC, \
                  COALESCE(w.potCreatedAt, w.markerCreatedAt) DESC, \
                  w.markerCreatedAt DESC, w.markerRowid DESC",
        quota = REFUND_VIEW_UNKNOWN_POT_QUOTA,
        probe = REFUND_VIEW_MAX_ROWS + 1,
        after = after,
        rank = overlay_discovery::potparty::validity::sig_rank_expr("pp."),
        party = crate::logic::party_candidates_sql(),
        era = crate::logic::era_filter_sql(
            "COALESCE(r.createdAt, pp.createdAt)",
            "?2",
            written_off_before_ms
        ),
    )
}

/// One `/refund-view` joined row, host-typed (the `refund_view_sql` shape).
/// The pot-side fields are `Option` because the `pot_records` join can MISS
/// — a party marker whose pot the overlay never indexed yields NULL columns
/// (fail-safe: never asserted unspent).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RefundViewRow {
    pub game_id: String,
    pub pot_txid: String,
    pub pot_vout: u32,
    /// The caller's OWN potparty marker height — byte-format-admitted, a
    /// HINT (used only when no covenant-committed height exists).
    pub marker_recovery_height: u32,
    /// The COVENANT-COMMITTED recoveryHeight decoded from the admitted
    /// funding lock (#284) — chain truth; `None` for bare/legacy rows.
    pub cov_recovery_height: Option<u64>,
    pub spent: Option<bool>,
    pub spending_txid: Option<String>,
    pub spent_confirmed: Option<bool>,
    /// The stored #284 spend verdict — trusted ONLY via [`trusted_verdict`]
    /// (`verdict_txid` must equal `spending_txid`; a stale pointer-overwrite
    /// leftover is ignored).
    pub verdict: Option<String>,
    pub verdict_txid: Option<String>,
    /// Block height of the SPV-verified spend confirm (pointer-guarded at
    /// write time — see the overlay's `mark_spent_sql`).
    pub spent_height: Option<u64>,
    /// `potrefund_records` PRESENCE for the pot outpoint — an UNVERIFIED,
    /// byte-format-admitted bit: either seat OR any third party can file a
    /// marker row, so this is never proof a genuine backup exists (the
    /// reader verifies — module docs).
    ///
    /// #335 (bsv-low) decision — **consumed nowhere today, and kept
    /// unverified on purpose**: bsv-low's `chainReads.ts` /
    /// `refundViewDisplay.ts` ignore it; its only effect is the
    /// `chain+marker` statusSource WORDING below. Server-side verification
    /// was considered and rejected — the app layer is an index, not an
    /// authority (zanaadu invariant #3: indexers cache and serve, never
    /// validate), and the marker's identity sig is the WEAKER proof anyway:
    /// the backup raw's own 2-of-2 signatures are the network-verifiable
    /// truth, which only the spending path can prove (Rule 4). A FIRST
    /// CONSUMER of this bit inherits this warning: presence is a hint to go
    /// fetch + verify the marker (sig AND raw) — it must never stand down a
    /// safety mechanism or gate a credit by itself.
    pub backup_marker_present: bool,
    /// bsv-low#304's VERIFIED proof latch for the recorded spender
    /// (`pot_beefs.proof_verified`, joined on `spendingTxid`) — set only by
    /// the overlay's chaintracks-verifying writers. #323 MEDIUM-1: this is
    /// the SECOND accepted confirmation signal, so `/refund-view` and
    /// `/results` share one bar. `None` = no spender row joined.
    pub spender_proof_verified: Option<bool>,
    /// #371: the overlay's OWN network witness for the recorded spender
    /// (`network_seen` join) + the spender's bytes-finality latch
    /// (`pot_records.spenderFinal`) — the verdict gate's third arm. A
    /// non-final parked refund is `spender_final = Some(false)` and stays
    /// behind the merkle bar (#323 verbatim).
    pub spender_seen: Option<bool>,
    pub spender_final: Option<bool>,
    /// #217 — `pot_records.createdAt`: THIS overlay's admission of the pot's
    /// FUNDING output, unix seconds, write-once. `None` on a join miss.
    pub pot_admitted_at: Option<i64>,
    /// #217 — `potparty_records.createdAt` of the window REPRESENTATIVE:
    /// admission of the oldest (sig-valid-preferred) marker NAMING the
    /// caller for this outpoint. Marker-backed — see the module docs.
    pub first_party_marker_at: Option<i64>,
    /// #217 — `pot_records.firstSpentAt`: THIS overlay's FIRST accepted spend
    /// pointer for the pot, unix seconds, write-once. `None` = no accepted
    /// spend write since the column shipped (PERMANENT for older rows) —
    /// never "unspent".
    pub first_spent_at: Option<i64>,
}

/// Which stamp a consumer should read as the hand's START, and how strong it
/// is. The actionable half of the #217 timeline's provenance (module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeatAnchor {
    /// `potAdmittedAt` — INDEX-backed: the pot's own funding output is in
    /// `pot_records`, so a real funded output was submitted for it.
    PotAdmission,
    /// `firstPartyMarkerAt` — MARKER-backed: no `pot_records` row, so the
    /// only stamp is the admission of a byte-format-admitted marker naming
    /// the caller. Treat the start time as approximate and unattested.
    PartyMarker,
}

impl SeatAnchor {
    pub fn as_str(self) -> &'static str {
        match self {
            SeatAnchor::PotAdmission => "pot-admission",
            SeatAnchor::PartyMarker => "party-marker",
        }
    }

    /// Prefer the index-backed stamp; fall back to the marker one; `None`
    /// when neither exists (never a fabricated start time).
    pub fn choose(
        pot_admitted_at: Option<i64>,
        first_party_marker_at: Option<i64>,
    ) -> Option<Self> {
        if pot_admitted_at.is_some() {
            Some(SeatAnchor::PotAdmission)
        } else {
            first_party_marker_at.map(|_| SeatAnchor::PartyMarker)
        }
    }
}

impl RefundViewRow {
    /// The stored verdict, trusted only when it was computed FOR the row's
    /// current spend pointer (`verdictTxid == spendingTxid` — the #284
    /// stale-pointer rule `results.rs` applies before believing a stored
    /// verdict). An unrecognized stored string is `None` (never a guess).
    pub fn trusted_verdict(&self) -> Option<PotVerdict> {
        let (v, vt, st) = (
            self.verdict.as_deref()?,
            self.verdict_txid.as_deref()?,
            self.spending_txid.as_deref()?,
        );
        if !vt.eq_ignore_ascii_case(st) {
            return None;
        }
        PotVerdict::from_wire(v)
    }

    /// The recovery height this view SERVES — see the free
    /// [`served_recovery_height`] (shared with `/live-view`).
    pub fn served_recovery_height(&self) -> Option<u64> {
        served_recovery_height(self.cov_recovery_height, self.marker_recovery_height)
    }
}

/// The range check every served recovery height passes: a usable BLOCK-height
/// gate is `0 < h < LOCKTIME_THRESHOLD`. `0` means "no gate committed" and a
/// timestamp-range value is not a height at all — both answer `None` rather
/// than a fake countdown.
///
/// Extracted (bsv-low #332 follow-up) so `/refund-view`, `/live-view` and
/// `/results` share ONE predicate instead of three copies: "these must agree"
/// is durably fixed by a single shared function, not by a test (Rule 10).
pub fn valid_recovery_height(h: u64) -> Option<u64> {
    (h > 0 && h < u64::from(LOCKTIME_THRESHOLD)).then_some(h)
}

/// The recovery height a per-identity view SERVES: the covenant-committed
/// value when decoded (chain truth), else the caller's own marker value
/// (hint), each range-checked — 0 or a timestamp-range value is `None` (no
/// fake countdown). Shared by `/refund-view` and `/live-view` so the two
/// surfaces can never drift on the sourcing rule.
///
/// NOTE `/results` deliberately does NOT use this MERGED answer: it serves the
/// covenant leg and the marker leg as two DISTINCT wire fields, because a
/// client gating a money word must be able to tell "the chain committed this"
/// from "a marker claims this" (see [`crate::results::results_body`]).
pub fn served_recovery_height(
    cov_recovery_height: Option<u64>,
    marker_recovery_height: u32,
) -> Option<u64> {
    cov_recovery_height
        .and_then(valid_recovery_height)
        .or_else(|| valid_recovery_height(u64::from(marker_recovery_height)))
}

/// The `/refund-view` status enum (wire strings per the #252 stage-2 spec).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefundStatus {
    Armed,
    GateOpen,
    Landed,
    Superseded,
    Unknown,
}

impl RefundStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            RefundStatus::Armed => "armed",
            RefundStatus::GateOpen => "gate-open",
            RefundStatus::Landed => "landed",
            RefundStatus::Superseded => "superseded",
            RefundStatus::Unknown => "unknown",
        }
    }
}

/// Derive `(status, statusSource)` from the row facts — the honesty table in
/// the module docs. NEVER guesses: every incomplete-fact combination is
/// (`Unknown`, `None`).
#[allow(clippy::too_many_arguments)] // every input is one row fact feeding one honesty table
pub fn derive_refund_status(
    spent: Option<bool>,
    spent_confirmed: Option<bool>,
    trusted_verdict: Option<PotVerdict>,
    gate_passed: bool,
    backup_marker_present: bool,
    spender_proof_verified: Option<bool>,
    spender_seen: Option<bool>,
    spender_final: Option<bool>,
) -> (RefundStatus, Option<&'static str>) {
    // #323 MEDIUM-1 — ONE landing bar, shared with `assemble_results`:
    // the `spentConfirmed` flag, a chaintracks-VERIFIED spender proof, or
    // the #371 seen-and-final arm. Without the shared bar these money
    // surfaces DISAGREED on a legacy row the migration stamped 0:
    // `/results` served `refund` at a proven height while this view said
    // `unknown`. Note the #371 arm can only mark a refund SUPERSEDED
    // early (a final settle displaced the parked refund) — a landed
    // REFUND is itself non-final until its height, so `spender_final =
    // Some(false)` keeps it behind the merkle bar exactly as before.
    let confirmed_landing = crate::logic::is_confirmed_landing_with_proof(
        spent_confirmed,
        spender_proof_verified,
        spender_seen,
        spender_final,
    );
    match spent {
        // `spent = 0` is the overlay's NON-OBSERVATION of a spend on an
        // indexed pot (the admission default), not a UTXO existence check —
        // the overlay may simply not have seen the spend yet. The error
        // direction is safe: a missed spend keeps the user pursuing money
        // that already landed; it never abandons a live claim.
        Some(false) => {
            let status = if gate_passed {
                RefundStatus::GateOpen
            } else {
                RefundStatus::Armed
            };
            // The marker bit's PARTICIPATION is declared, never trusted:
            // presence is unverified byte-format admission (either seat or
            // any third party can file it — module docs), so the source
            // only says a marker row factored into the wording. The client
            // must fetch, sig-verify and parse the marker itself before
            // rendering it as a real re-broadcastable backup.
            let source = if backup_marker_present {
                "chain+marker"
            } else {
                "chain"
            };
            (status, Some(source))
        }
        Some(true) => match (confirmed_landing.then_some(true), trusted_verdict) {
            // Confirmed spend + a verdict decoded FOR that spender: the one
            // place this view asserts an outcome — pure chain truth.
            (Some(true), Some(PotVerdict::Refund)) => (RefundStatus::Landed, Some("chain")),
            (Some(true), Some(_)) => (RefundStatus::Superseded, Some("chain")),
            // Confirmed but undecoded, or recorded-but-unconfirmed (a
            // displaceable intent, not a landing): incomplete — never guess.
            _ => (RefundStatus::Unknown, None),
        },
        // No pot_records row: spend status genuinely unknown (never
        // asserted unspent → never "armed" on absence-of-evidence).
        None => (RefundStatus::Unknown, None),
    }
}

/// One `/refund-view` response entry, pre-JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefundEntry {
    pub game_id: String,
    pub pot_txid: String,
    pub pot_vout: u32,
    /// The served recovery height (see [`RefundViewRow::served_recovery_height`]).
    pub recovery_height: Option<u64>,
    /// `max(0, recoveryHeight - tip)` when both known, else `None`.
    pub blocks_to_gate: Option<u64>,
    /// `tip >= recoveryHeight` when both known, else `false` (fail-safe).
    pub gate_passed: bool,
    pub backup_marker_present: bool,
    pub spent: Option<bool>,
    pub spending_txid: Option<String>,
    pub spent_confirmed: Option<bool>,
    /// The TRUSTED decoded verdict (stale/unrecognized served as `None`).
    pub verdict: Option<PotVerdict>,
    pub spent_height: Option<u64>,
    pub status: RefundStatus,
    pub status_source: Option<&'static str>,
    /// #217 durable timeline — AUDIT ONLY. None of these four feed
    /// [`derive_refund_status`] (checked by
    /// `the_timeline_never_moves_the_status`); the money words above are
    /// derived from spend/verdict facts exactly as before.
    pub pot_admitted_at: Option<i64>,
    pub first_party_marker_at: Option<i64>,
    pub first_spent_at: Option<i64>,
    /// Which of the two start stamps to read, and how strong it is.
    pub seat_anchor: Option<SeatAnchor>,
}

/// Assemble the joined rows + chain tip into response entries. Order is
/// preserved (the SQL already returns tiered-newest-first). Pure — every
/// branch is unit-testable without D1.
pub fn assemble_refund_view(rows: Vec<RefundViewRow>, tip: Option<u64>) -> Vec<RefundEntry> {
    rows.into_iter()
        .map(|r| {
            let recovery_height = r.served_recovery_height();
            let (blocks_to_gate, gate_passed) = match (recovery_height, tip) {
                (Some(rh), Some(t)) => (Some(rh.saturating_sub(t)), t >= rh),
                _ => (None, false),
            };
            let verdict = r.trusted_verdict();
            let (status, status_source) = derive_refund_status(
                r.spent,
                r.spent_confirmed,
                verdict,
                gate_passed,
                r.backup_marker_present,
                r.spender_proof_verified,
                r.spender_seen,
                r.spender_final,
            );
            RefundEntry {
                game_id: r.game_id,
                pot_txid: r.pot_txid,
                pot_vout: r.pot_vout,
                recovery_height,
                blocks_to_gate,
                gate_passed,
                backup_marker_present: r.backup_marker_present,
                spent: r.spent,
                spending_txid: r.spending_txid,
                spent_confirmed: r.spent_confirmed,
                verdict,
                spent_height: r.spent_height,
                status,
                status_source,
                // #217 — carried through verbatim, AFTER the status is
                // derived, and never an input to it.
                pot_admitted_at: r.pot_admitted_at,
                first_party_marker_at: r.first_party_marker_at,
                first_spent_at: r.first_spent_at,
                seat_anchor: SeatAnchor::choose(r.pot_admitted_at, r.first_party_marker_at),
            }
        })
        .collect()
}

/// Assemble the `/refund-view` wire body:
/// `{"identity","tip":<height|null>,"refunds":[{gameId,potTxid,potVout,
/// recoveryHeight,blocksToGate,gatePassed,backupMarkerPresent,spent,
/// spendingTxid,spentConfirmed,verdict,spentHeight,status,statusSource,
/// timeline}]}`, with
/// `timeline = {potAdmittedAt,firstPartyMarkerAt,firstSpentAt,spentHeight,
/// seatAnchor}` — the #217 durable audit stamps, every one a stored column
/// and every one nullable. `timeline.spentHeight` is THE SAME value as the
/// row's top-level `spentHeight` (one column, read once, repeated so the
/// timeline is self-contained and its one NETWORK-ANCHORED entry sits beside
/// the three server-observed ones); the two can never disagree and a cell
/// pins that. `seatAnchor` is `"pot-admission"`, `"party-marker"` or `null`.
/// See the module docs for what each stamp is a time OF — the block is
/// AUDIT-ONLY and gates nothing.
/// `tip` mirrors `/recovery-view` (`null` on a chaintracks fault — the D1
/// facts still serve; the gate fields then degrade to `null`/`false`).
pub fn refund_view_body(
    identity: &str,
    tip: Option<u64>,
    entries: &[RefundEntry],
    truncated: bool,
    after: usize,
) -> String {
    let arr: Vec<serde_json::Value> = entries
        .iter()
        .map(|e| {
            json!({
                "gameId": e.game_id,
                "potTxid": e.pot_txid,
                "potVout": e.pot_vout,
                "recoveryHeight": e.recovery_height,
                "blocksToGate": e.blocks_to_gate,
                "gatePassed": e.gate_passed,
                "backupMarkerPresent": e.backup_marker_present,
                "spent": e.spent,
                "spendingTxid": e.spending_txid,
                "spentConfirmed": e.spent_confirmed,
                "verdict": e.verdict.map(PotVerdict::as_str),
                "spentHeight": e.spent_height,
                "status": e.status.as_str(),
                "statusSource": e.status_source,
                "timeline": {
                    "potAdmittedAt": e.pot_admitted_at,
                    "firstPartyMarkerAt": e.first_party_marker_at,
                    "firstSpentAt": e.first_spent_at,
                    "spentHeight": e.spent_height,
                    "seatAnchor": e.seat_anchor.map(SeatAnchor::as_str),
                },
            })
        })
        .collect();
    // The paging round (2026-08-21) — the /recovery-view #398 contract:
    // `truncated` is the honest incompleteness bit, `nextAfter` the cursor
    // (absent on the last page and at the ceiling). Additive.
    let next_after = if truncated && after < REFUND_VIEW_AFTER_MAX {
        Some(after + REFUND_VIEW_MAX_ROWS)
    } else {
        None
    };
    json!({
        "identity": identity,
        "tip": tip,
        "refunds": arr,
        "truncated": truncated,
        "nextAfter": next_after,
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h64(seed: u8) -> String {
        format!("{seed:02x}").repeat(32)
    }

    fn base_row() -> RefundViewRow {
        RefundViewRow {
            game_id: h64(0x11),
            pot_txid: h64(0xaa),
            pot_vout: 0,
            marker_recovery_height: 900_123,
            spender_proof_verified: None,
            spender_seen: None,
            spender_final: None,
            cov_recovery_height: None,
            spent: Some(false),
            spending_txid: None,
            spent_confirmed: Some(false),
            verdict: None,
            verdict_txid: None,
            spent_height: None,
            backup_marker_present: false,
            pot_admitted_at: None,
            first_party_marker_at: None,
            first_spent_at: None,
        }
    }

    // ── status derivation table ─────────────────────────────────────────────

    /// #323 — THIS VIEW honours a verified spender proof as a landing.
    ///
    /// Named for what it actually proves. It was previously called
    /// `..._exactly_as_in_results`, which over-claimed: it calls only
    /// `derive_refund_status` and never touches the results side, so breaking
    /// that side left this green. **An executable claim is only as strong as
    /// the surface it executes against** — nastier than a false comment,
    /// because a green cell with the right name reads as stronger evidence
    /// than prose. The AGREEMENT is now structural instead of asserted: both
    /// views call `logic::is_confirmed_landing_with_proof`, pinned by
    /// `both_money_views_call_the_shared_bar`.
    #[test]
    fn a_verified_spender_proof_is_a_landing_in_this_view() {
        // The parked intent: no flag, no proof ⇒ unknown on BOTH surfaces.
        assert_eq!(
            derive_refund_status(
                Some(true),
                Some(false),
                Some(PotVerdict::Refund),
                true,
                true,
                None,
                None,
                None
            ),
            (RefundStatus::Unknown, None),
            "a recorded-but-unconfirmed pointer is a displaceable intent"
        );
        // The legacy shape: flag 0, but a chaintracks-VERIFIED spender proof.
        assert_eq!(
            derive_refund_status(
                Some(true),
                Some(false),
                Some(PotVerdict::Refund),
                true,
                true,
                Some(true),
                None,
                None
            ),
            (RefundStatus::Landed, Some("chain")),
            "a VERIFIED spender proof is a landing even when the flag is 0"
        );
        // And it supersedes correctly for a non-refund verdict too.
        assert_eq!(
            derive_refund_status(
                Some(true),
                None,
                Some(PotVerdict::WinnerA),
                true,
                true,
                Some(true),
                None,
                None
            ),
            (RefundStatus::Superseded, Some("chain")),
        );
        // An UNVERIFIED proof latch is not a signal (never a guess).
        assert_eq!(
            derive_refund_status(
                Some(true),
                Some(false),
                Some(PotVerdict::Refund),
                true,
                true,
                Some(false),
                None,
                None
            ),
            (RefundStatus::Unknown, None),
        );
    }

    /// #371 — the SEEN-and-final arm in THIS view's terms: a network-
    /// witnessed FINAL settle marks the refund SUPERSEDED before any merkle
    /// proof; a network-witnessed NON-FINAL spender (the parked refund
    /// itself, as indexers report it) stays UNKNOWN — #323 verbatim; and a
    /// final-looking pointer with NO witness (the ungated-/submit plant,
    /// epoch Rule 21) stays UNKNOWN.
    #[test]
    fn seen_and_final_supersedes_early_but_neither_half_alone_does() {
        assert_eq!(
            derive_refund_status(
                Some(true),
                Some(false),
                Some(PotVerdict::WinnerA),
                true,
                true,
                None,
                Some(true),
                Some(true)
            ),
            (RefundStatus::Superseded, Some("chain")),
            "a SEEN final settle supersedes at the ruling-3 bar"
        );
        assert_eq!(
            derive_refund_status(
                Some(true),
                Some(false),
                Some(PotVerdict::Refund),
                true,
                true,
                None,
                Some(true),
                Some(false)
            ),
            (RefundStatus::Unknown, None),
            "a SEEN but NON-FINAL spender is the parked refund — never a landing before its height"
        );
        assert_eq!(
            derive_refund_status(
                Some(true),
                Some(false),
                Some(PotVerdict::WinnerA),
                true,
                true,
                None,
                None,
                Some(true)
            ),
            (RefundStatus::Unknown, None),
            "final bytes with no network witness is an attacker-plantable pointer — merkle bar holds"
        );
    }

    /// The SQL must actually FETCH the signal the bar reads — otherwise the
    /// widened rule is inoperative in production (the producer-level check).
    #[test]
    fn refund_view_sql_fetches_the_spender_proof_latch() {
        let sql = refund_view_sql(None, 0);
        assert!(
            sql.contains("sb.proof_verified AS spenderProofVerified"),
            "the proof latch must be SELECTed: {sql}"
        );
        assert!(
            sql.contains(
                "LEFT JOIN pot_beefs sb ON w.spendingTxid IS NOT NULL \
              AND sb.txid = lower(w.spendingTxid)"
            ),
            "the join must sit OUTSIDE the window, on survivors only: {sql}"
        );
    }

    #[test]
    fn unspent_below_gate_is_armed() {
        assert_eq!(
            derive_refund_status(
                Some(false),
                Some(false),
                None,
                false,
                false,
                None,
                None,
                None
            ),
            (RefundStatus::Armed, Some("chain"))
        );
        // A published backup upgrades the SOURCE, not the status.
        assert_eq!(
            derive_refund_status(
                Some(false),
                Some(false),
                None,
                false,
                true,
                None,
                None,
                None
            ),
            (RefundStatus::Armed, Some("chain+marker"))
        );
    }

    #[test]
    fn unspent_past_gate_is_gate_open() {
        assert_eq!(
            derive_refund_status(
                Some(false),
                Some(false),
                None,
                true,
                false,
                None,
                None,
                None
            ),
            (RefundStatus::GateOpen, Some("chain"))
        );
        assert_eq!(
            derive_refund_status(Some(false), Some(false), None, true, true, None, None, None),
            (RefundStatus::GateOpen, Some("chain+marker"))
        );
    }

    #[test]
    fn confirmed_refund_verdict_is_landed() {
        assert_eq!(
            derive_refund_status(
                Some(true),
                Some(true),
                Some(PotVerdict::Refund),
                true,
                true,
                None,
                None,
                None
            ),
            (RefundStatus::Landed, Some("chain"))
        );
    }

    #[test]
    fn confirmed_settle_verdicts_are_superseded() {
        for v in [PotVerdict::WinnerA, PotVerdict::WinnerB, PotVerdict::Tie] {
            assert_eq!(
                derive_refund_status(
                    Some(true),
                    Some(true),
                    Some(v),
                    false,
                    true,
                    None,
                    None,
                    None
                ),
                (RefundStatus::Superseded, Some("chain"))
            );
        }
    }

    #[test]
    fn incomplete_facts_are_unknown_never_guessed() {
        // No pot_records row at all.
        assert_eq!(
            derive_refund_status(None, None, None, true, true, None, None, None),
            (RefundStatus::Unknown, None)
        );
        // Spent but unconfirmed — a displaceable intent, even with a verdict.
        assert_eq!(
            derive_refund_status(
                Some(true),
                Some(false),
                Some(PotVerdict::Refund),
                true,
                true,
                None,
                None,
                None
            ),
            (RefundStatus::Unknown, None)
        );
        assert_eq!(
            derive_refund_status(Some(true), None, None, false, false, None, None, None),
            (RefundStatus::Unknown, None)
        );
        // Confirmed spend, no trusted decoded verdict.
        assert_eq!(
            derive_refund_status(Some(true), Some(true), None, false, true, None, None, None),
            (RefundStatus::Unknown, None)
        );
    }

    // ── verdict trust (the #284 stale-pointer rule) ─────────────────────────

    #[test]
    fn verdict_trusted_only_for_the_current_spend_pointer() {
        let mut r = base_row();
        r.spent = Some(true);
        r.spent_confirmed = Some(true);
        r.spending_txid = Some(h64(0xfe));
        r.verdict = Some("refund".into());
        r.verdict_txid = Some(h64(0xfe));
        assert_eq!(r.trusted_verdict(), Some(PotVerdict::Refund));

        // Stale pointer: verdict computed for a DIFFERENT spender.
        r.verdict_txid = Some(h64(0xfd));
        assert_eq!(r.trusted_verdict(), None);

        // Unrecognized stored string never becomes a verdict.
        r.verdict_txid = Some(h64(0xfe));
        r.verdict = Some("garbage".into());
        assert_eq!(r.trusted_verdict(), None);

        // Missing pieces refuse.
        r.verdict = Some("refund".into());
        r.spending_txid = None;
        assert_eq!(r.trusted_verdict(), None);
    }

    // ── served recovery height ──────────────────────────────────────────────

    #[test]
    fn covenant_height_preferred_over_marker_hint() {
        let mut r = base_row();
        r.cov_recovery_height = Some(900_200);
        assert_eq!(r.served_recovery_height(), Some(900_200));
        r.cov_recovery_height = None;
        assert_eq!(r.served_recovery_height(), Some(900_123));
    }

    #[test]
    fn nonsense_heights_serve_null() {
        let mut r = base_row();
        r.marker_recovery_height = 0;
        assert_eq!(r.served_recovery_height(), None);
        r.marker_recovery_height = LOCKTIME_THRESHOLD; // timestamp range
        assert_eq!(r.served_recovery_height(), None);
        // A junk covenant column falls back to a valid marker height.
        r.marker_recovery_height = 900_123;
        r.cov_recovery_height = Some(0);
        assert_eq!(r.served_recovery_height(), Some(900_123));
    }

    // ── gate math + assembly ────────────────────────────────────────────────

    #[test]
    fn blocks_to_gate_math() {
        let rows = vec![base_row()]; // recoveryHeight 900_123
                                     // 45 blocks out.
        let e = &assemble_refund_view(rows.clone(), Some(900_078))[0];
        assert_eq!(e.blocks_to_gate, Some(45));
        assert!(!e.gate_passed);
        assert_eq!(e.status, RefundStatus::Armed);
        // Exactly at the gate.
        let e = &assemble_refund_view(rows.clone(), Some(900_123))[0];
        assert_eq!(e.blocks_to_gate, Some(0));
        assert!(e.gate_passed);
        assert_eq!(e.status, RefundStatus::GateOpen);
        // Past the gate clamps at 0, never negative.
        let e = &assemble_refund_view(rows.clone(), Some(900_200))[0];
        assert_eq!(e.blocks_to_gate, Some(0));
        assert!(e.gate_passed);
        // No tip: gate fields degrade, status stays fail-safe armed.
        let e = &assemble_refund_view(rows, None)[0];
        assert_eq!(e.blocks_to_gate, None);
        assert!(!e.gate_passed);
        assert_eq!(e.status, RefundStatus::Armed);
    }

    #[test]
    fn gate_math_null_when_no_height_known() {
        let mut r = base_row();
        r.marker_recovery_height = 0;
        let e = &assemble_refund_view(vec![r], Some(900_500))[0];
        assert_eq!(e.recovery_height, None);
        assert_eq!(e.blocks_to_gate, None);
        assert!(!e.gate_passed);
        // Spend facts still derive a status (unspent + !gatePassed = armed).
        assert_eq!(e.status, RefundStatus::Armed);
    }

    // ── wire body ───────────────────────────────────────────────────────────

    #[test]
    fn refund_view_body_shape() {
        let mut r = base_row();
        r.spent = Some(true);
        r.spent_confirmed = Some(true);
        r.spending_txid = Some(h64(0xfe));
        r.verdict = Some("refund".into());
        r.verdict_txid = Some(h64(0xfe));
        r.spent_height = Some(900_170);
        r.backup_marker_present = true;
        let me = format!("02{}", "a1".repeat(32));
        let entries = assemble_refund_view(vec![r], Some(900_168));
        let v: serde_json::Value =
            serde_json::from_str(&refund_view_body(&me, Some(900_168), &entries, false, 0))
                .unwrap();
        assert_eq!(v["identity"], serde_json::json!(me));
        assert_eq!(v["tip"], serde_json::json!(900_168));
        let e = &v["refunds"][0];
        assert_eq!(e["gameId"], serde_json::json!(h64(0x11)));
        assert_eq!(e["potTxid"], serde_json::json!(h64(0xaa)));
        assert_eq!(e["potVout"], serde_json::json!(0));
        assert_eq!(e["recoveryHeight"], serde_json::json!(900_123));
        assert_eq!(e["blocksToGate"], serde_json::json!(0));
        assert_eq!(e["gatePassed"], serde_json::json!(true));
        assert_eq!(e["backupMarkerPresent"], serde_json::json!(true));
        assert_eq!(e["spent"], serde_json::json!(true));
        assert_eq!(e["spendingTxid"], serde_json::json!(h64(0xfe)));
        assert_eq!(e["spentConfirmed"], serde_json::json!(true));
        assert_eq!(e["verdict"], serde_json::json!("refund"));
        assert_eq!(e["spentHeight"], serde_json::json!(900_170));
        assert_eq!(e["status"], serde_json::json!("landed"));
        assert_eq!(e["statusSource"], serde_json::json!("chain"));
        // Display-only: the raw refund bytes are NEVER in this body.
        assert!(!refund_view_body(&me, None, &entries, false, 0).contains("refundRawHex"));
    }

    #[test]
    fn refund_view_body_empty_and_null_tip() {
        let v: serde_json::Value =
            serde_json::from_str(&refund_view_body("nope", None, &[], false, 0)).unwrap();
        assert_eq!(v["identity"], serde_json::json!("nope"));
        assert!(v["tip"].is_null());
        assert_eq!(v["refunds"], serde_json::json!([]));
    }

    // ── SQL structure pins ──────────────────────────────────────────────────

    #[test]
    fn refund_view_sql_shape() {
        let sql = refund_view_sql(None, 0);
        // 2026-08-29 party-candidates: ONE identity bind (`?1`), reused by
        // the subquery's two arms and the outer scan — three placeholders.
        assert_eq!(sql.matches("?1").count(), 3, "identity bind reused thrice");
        assert!(
            !sql.contains("?2"),
            "no cutoff placeholder without a cutoff"
        );
        // The paging round: the window PROBES one past the page (truncation is
        // decided by what the query returned) and the cursor bounds the page.
        let probe = REFUND_VIEW_MAX_ROWS + 1;
        assert!(sql.contains(&format!("LIMIT {probe}")));
        assert!(sql.contains("finalRank > 0 AND finalRank <= 0 + 101"));
        assert!(sql.contains("PARTITION BY pp.potTxid, pp.potVout"));
        assert!(sql.contains(&format!("potRank <= {REFUND_VIEW_UNKNOWN_POT_QUOTA}")));
        assert!(sql.contains("EXISTS(SELECT 1 FROM potrefund_records"));
        // Display-only: the refund bytes column never appears in the query.
        assert!(!sql.contains("refundRawHex"));
        // No BLOB is ever transferred.
        assert!(!sql.contains("hex("));
    }

    /// #375 — the era filter on `/refund-view`: exactly one shared fragment,
    /// at the innermost identity scan (before the dedupe/quota windows),
    /// anchored `COALESCE(r.createdAt, pp.createdAt)`; stripping it restores
    /// the `None` arm byte-for-byte, and the cutoff is exactly one extra
    /// bind.
    #[test]
    fn refund_view_sql_era_filter_shape_and_none_identity() {
        let cutoff = Some(1_754_500_000_000i64);
        let frag =
            crate::logic::era_filter_sql("COALESCE(r.createdAt, pp.createdAt)", "?2", cutoff);
        let with = refund_view_sql(cutoff, 0);
        let without = refund_view_sql(None, 0);
        assert_eq!(with.matches(&frag).count(), 1, "exactly one era fragment");
        assert_eq!(
            with.matches(&format!("WHERE pp.identity = ?1{frag})"))
                .count(),
            1,
            "the era filter rides the innermost identity scan"
        );
        // identity (`?1` ×3 across the party-candidates subquery + outer scan)
        // + cutoff (`?2` ×1) — two BINDS at the route.
        assert_eq!(with.matches("?1").count(), 3, "identity bind reused thrice");
        assert_eq!(with.matches("?2").count(), 1, "one cutoff placeholder");
        assert_eq!(
            with.replace(&frag, ""),
            without,
            "None must stay byte-identical to the pre-#375 query"
        );
    }
}
