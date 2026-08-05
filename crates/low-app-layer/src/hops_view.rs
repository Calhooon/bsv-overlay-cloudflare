//! `/hops-view` — the per-identity HOPS-IN-FLIGHT view (bsv-low #315, #252
//! stage 2b).
//!
//! The funding HOP (the staging P2PKH coin a seat pays to its own
//! `[2,'low settle']` key before the JOIN assembles the pot) previously had
//! NO identity-keyed server row: `tm_lowfund` indexes the bare outpoint and
//! `potparty_records` only fills at JOIN-assembly, so a seat that funded a
//! hop and died pre-JOIN was invisible to every per-identity view (the
//! #256 ~80.8k-sat class). Stage 2b adds the `LOW/hopparty/v1` marker,
//! which RIDES THE HOP TRANSACTION as a second output (indexed by
//! `tm_hopparty` → `hopparty_records`): the marker names its hop by VOUT
//! only and the containing tx supplies the txid, so the hop outpoint is
//! `(hopparty_records.txid, hopVout)`. This view joins those rows to the
//! `tm_lowfund`-indexed hop outpoint and answers, for one identity: which
//! hops did you mark, are they spent (JOIN / hop sweep) or still in
//! flight, and does each marker actually VERIFY.
//!
//! ## Posture (the `/results` / `/refund-view` template)
//!
//! One identity bind, one bounded D1 query, fail-safe-empty 200 on a bad
//! identity, 503 only on a D1 fault, `no-store`, tip in body, honesty
//! pairs, `unknown` first-class. Join key with `/live-view` semantics is
//! `identity` + `gameId` (each entry carries the marker's `gameId` so a
//! client can correlate a hop with a live hand).
//!
//! ## `status`/`statusSource` (the spent/unspent honesty pair)
//!
//! | facts                                                   | status    | source |
//! |---------------------------------------------------------|-----------|--------|
//! | `pot_records` row, spent = 0                            | `unspent` | chain  |
//! | row, spend recorded + confirmed (shared #323 bar)       | `spent`   | chain  |
//! | row, spend recorded but UNCONFIRMED (displaceable)      | `unknown` | null   |
//! | NO `pot_records` row (hop never indexed)                | `unknown` | null   |
//!
//! An absent `pot_records` row is `unknown`, NEVER asserted-unspent — the
//! overlay may simply never have seen the hop tx. `spent = 0` is likewise
//! the overlay's NON-observation of a spend on an indexed outpoint, not a
//! UTXO existence proof (the raw facts ride alongside either way). The
//! confirmation bar is the ONE shared bar
//! (`logic::is_confirmed_landing_with_proof` — `spentConfirmed` OR a
//! chaintracks-verified spender proof), so this view can never disagree
//! with `/refund-view`/`/results` on what counts as a landing.
//!
//! ## `markerVerified` — the read-time validity FILTER (never an authority)
//!
//! Admission is byte-format-only by doctrine, so every served row is an
//! UNVERIFIED claim until read time. Three bars, all against facts the
//! CONTAINER itself supplies:
//!
//!  1. **The hop lock.** The hop output is
//!     `P2PKH(hash160(seatSettlePubkey))`, so this re-derives that script
//!     from the marker's own pubkey and compares it to the container's
//!     output at `hopVout` (`hopparty_records.hopLockHex`, decoded ONCE at
//!     admission from the very BEEF admitted — #310/#284 decode-at-write;
//!     the script at `(txid, vout)` is committed by the txid, so this is
//!     hash-bound chain truth).
//!  2. **The hop VALUE.** That same output's satoshis must equal the
//!     marker's claimed `hopSats` (`hopSatsOnChain`). This bar is what
//!     makes the replay case below cost real money.
//!  3. **Both signatures**, with the `results.rs` recipe: `seatSig` as
//!     plain ECDSA under `seatSettlePubkey` over sha256(hopparty seatsig
//!     preimage), and `identitySig` with the BRC-42 'anyone' round-trip
//!     under the marker's identity (protocol `[1,'low potparty']` — the
//!     SAME constant `/results` verifies potparty markers under; the
//!     version tag inside the challenge separates the domains).
//!
//! Reading the container's own decoded columns — rather than joining the
//! engine's `outputs` table on the `tm_lowfund` topic, which the first
//! draft did — is both cleaner and strictly more available: the hop tx is
//! the marker's own container, so the facts are guaranteed present for
//! every admitted row, they cannot be lifecycle-evicted when the hop is
//! spent, they need no second topic to have been submitted, and they carry
//! the SATOSHIS (which an `outputs`/`pot_records` join does not).
//!
//! Three-valued, `unknown` first-class:
//!
//! - `verified`   — both signatures verify AND the container's output at
//!   `hopVout` is `P2PKH(hash160(seatSettlePubkey))` paying exactly
//!   `hopSats`;
//! - `unverified` — a signature fails, a field is malformed, the lock or
//!   the value MISMATCHES, or the container has no output at `hopVout` at
//!   all (a PROVEN absence — `containerOutputs` says how many it has —
//!   which refutes the marker rather than leaving it open);
//! - `unknown`    — the per-request verify budget ran out before this row
//!   (`verifyBudgetExhausted` in the body surfaces that case — Rule 13: an
//!   ambiguous signal is surfaced, never consumed as either verdict).
//!
//! Rows failing the filter are SERVED and labeled — filter-for-display,
//! never silently dropped, never an authority decision (the signatures
//! ride back in the body so the client re-verifies).
//!
//! ### What the container buys, and the one residual it leaves
//!
//! Because the marker rides the hop tx, `SIGHASH_ALL` makes the funding
//! wallet's signature commit the marker bytes: **the entity that funded
//! the hop provably authored the marker.** Neither digest can bind the
//! containing txid (a tx cannot embed its own txid), so what remains is a
//! PAID REPLAY, analysed at [`derive_marker_verification`].
//!
//! ## The window (the #281 shape, SUPERSET per outpoint)
//!
//! `hopparty_records.identity` is attacker-writable (byte-format
//! admission), so the SQL reuses the identity-window discipline: `limit`
//! counts HOP OUTPOINTS ([`HOPS_VIEW_MAX_OUTPOINTS`], keyed
//! `(txid, hopVout)`), a hop-existence tier against `pot_records` demotes
//! markers naming never-indexed hops
//! (with [`HOPS_VIEW_UNKNOWN_HOP_QUOTA`] promoted slots so a genuinely
//! fresh hop whose `tm_lowfund` admission is in flight is not hidden), and
//! each outpoint serves up to [`HOPS_VIEW_ROWS_PER_OUTPOINT`] OLDEST rows —
//! a SUPERSET, never a one-row collapse, because THIS view verifies at
//! read and a layer that picks "the real row" before verification hands an
//! attacker the eviction (the potparty rn=1 lesson, executed in the #281
//! gate). The window fetches ONE outpoint beyond the page so the body's
//! `truncated` bit is honest (a flood is DETECTABLE, never a
//! complete-looking partial answer).

use serde_json::json;

/// Rows served per hop OUTPOINT — the overlay window constant, imported so
/// the two crates cannot drift (Rule 16: share the constant, not the
/// convention).
pub use overlay_discovery::hopparty::storage::HOPSFOR_ROWS_PER_OUTPOINT as HOPS_VIEW_ROWS_PER_OUTPOINT;

/// Hard bound on `/hops-view` DISTINCT HOP OUTPOINTS per request — same cap
/// + rationale as [`crate::refund_view::REFUND_VIEW_MAX_ROWS`].
pub const HOPS_VIEW_MAX_OUTPOINTS: usize = 100;

/// How many of the newest hops ABSENT from `pot_records` are promoted into
/// the main tier — same reservation + rationale as
/// [`crate::refund_view::REFUND_VIEW_UNKNOWN_POT_QUOTA`] (a fresh hop whose
/// `tm_lowfund` admission is in flight is exactly the hop this view exists
/// to show).
pub const HOPS_VIEW_UNKNOWN_HOP_QUOTA: usize = 10;

/// Freshness window for promoting a hop the overlay has NOT yet indexed
/// under `tm_lowfund` (bsv-low #283a semantics, adopted at the gate's
/// MEDIUM-5): an unknown hop competes for a promoted slot only while its
/// first marker is younger than this, and slots go OLDEST-first — the one
/// order an attacker cannot jump by publishing MORE after seeing the
/// victim's funding. MUST equal the overlay's
/// `UNKNOWN_POT_PROMOTION_MAX_AGE_SECS`, which the sibling identity
/// windows use; a test asserts that equality across the crate boundary
/// (the runtime dependency does not exist, so the PIN carries it —
/// Rule 16).
pub const HOPS_VIEW_UNKNOWN_HOP_MAX_AGE_SECS: u64 = 3600;

/// Per-request VERIFY budget (rows), spent in SERVED order — which the SQL
/// makes QUALITY order: rows whose container demonstrably paid the claimed
/// value, richest first, come before everything else. The bound exists because each row can cost up to
/// two ECDSA verifications plus a BRC-42 derivation on a public,
/// unauthenticated endpoint (the #314 MEDIUM-B read-time CPU class); the
/// cheap script pre-filter (one hash160 + string compare) runs FIRST and
/// refuses mismatched-lock rows without any curve work. Budget exhaustion
/// labels the remaining rows `unknown` and raises the body's
/// `verifyBudgetExhausted` bit (surfaced, never silently consumed —
/// Rule 13). 150 covers the full honest page (≤100 outpoints × 1 honest
/// marker) with headroom.
///
/// MEASURED COST of exhausting it (gate MEDIUM-4, corrected from an
/// earlier "only a flood can exhaust it" that understated this): a single
/// transaction filing ~38 outpoints × the per-outpoint superset, or ~150
/// marker rows, spends the budget. That is cheap. Two things keep it a
/// DEGRADATION rather than an erasure: the budget is spent in the SQL's
/// quality order, so a paid honest row is verified before any dust row can
/// consume a slot (pinned by `budget_is_spent_in_quality_order`); and an
/// unverified-for-lack-of-budget row is still SERVED with both signatures,
/// so the client re-verifies locally. The bound is also PER-REQUEST, not
/// per-identity, on a public unauthenticated route — the #314 MEDIUM-B
/// class restated; closing that is #318's identity-auth + quota work, not
/// this route's.
pub const HOPS_VIEW_VERIFY_BUDGET: usize = 150;

const _: () = assert!(HOPS_VIEW_UNKNOWN_HOP_QUOTA < HOPS_VIEW_MAX_OUTPOINTS);
const _: () = assert!(HOPS_VIEW_VERIFY_BUDGET > HOPS_VIEW_MAX_OUTPOINTS);

/// The single `/hops-view` SQL. Binds: `?1` the lowercase identity, and —
/// when `scoped_to_game` — `?2` a lowercase gameId.
///
/// # Ranking: on what the chain settled, never on arrival (gate HIGH-1)
///
/// `hopparty_records.identity` is attacker-writable by design, and the hop
/// outpoint is `(container txid, hopVout)` — so ONE transaction with K dust
/// P2PKH outputs plus K markers naming `hopVout = 0..K-1` mints K distinct
/// hop outpoints, and `tm_lowfund` admits every P2PKH output of an
/// explicitly-submitted tx, putting them all in the existence tier beside
/// the victim's real hop. Ranking those by RECENCY (the first draft) let
/// ~3,600 sats in one transaction permanently outrank an 80,800-sat hop:
/// measured k=99 honest survives, **k=100 erased**.
///
/// The ranking is therefore led by a fact the attacker must PAY for, read
/// off the container itself:
///
/// ```text
/// paidTier = 0 when hopLockHex IS NOT NULL AND hopSatsOnChain = hopSats
/// ```
///
/// i.e. the container really does pay the claimed value to the claimed
/// settle key — the same predicate `markerVerified`'s bars 1+2 use.
///
/// **What actually prices the attack is the next key, `hopSatsOnChain
/// DESC`** — the value the CHAIN records for that output, never the value
/// the marker claims. Displacing an honest hop therefore requires an
/// output of at least its value per attacker outpoint.
///
/// That is a real bar against DUST, and it is weaker than it first looks
/// against a funded attacker: the value is paid to the attacker's OWN key,
/// and `hopSatsOnChain` is decoded once at admission and never re-read, so
/// a single coin CHAINED through k transactions satisfies it k times at
/// ~one hop of peak, fully recoverable, capital. See the re-priced
/// residual at [`assemble_hops_view`] — do not restate this key as a cost
/// multiplier without reading it. This was verified by injection — neutering `paidTier` alone
/// does NOT re-open the flood, because the value key already demotes dust,
/// so the ordering doc must not credit the tier with work it does not do
/// (Rule 10).
///
/// `paidTier` earns its place for a narrower, real reason: it demotes rows
/// whose container has NO output at `hopVout` (or a mismatched one)
/// DETERMINISTICALLY, instead of leaning on SQLite's convention that NULLs
/// sort last under `DESC` — an engine detail this money-visible ordering
/// should not depend on.
///
/// Ties on value break **OLDEST-first** (#283a again): a REACTIVE flood
/// necessarily arrives after the hop it targets, and `createdAt` is
/// server-stamped at admission, so an attacker can always be newer but can
/// never backdate. (Paying oneself is cheap in
/// BURN terms, which is exactly why this is a cost multiplier and not a
/// closure; the closure is verify-then-page in
/// [`assemble_hops_view`], where the identity signature — the one input an
/// attacker cannot forge — decides the served order.)
///
/// The existence tier follows, with unknown-hop promotion on the #283a
/// semantics the sibling windows use (gate MEDIUM-5): only hops whose
/// first marker is younger than
/// [`HOPS_VIEW_UNKNOWN_HOP_MAX_AGE_SECS`] compete, and slots go
/// OLDEST-first — a newest-first quota handed all 10 promoted slots to an
/// attacker's just-published ghosts, which is precisely the case where the
/// honest hop's own `tm_lowfund` admission is still in flight.
///
/// **This is a SECURITY order, not a display preference** — it decides
/// which rows survive the page bound under flood, nothing more. Clients
/// should sort the returned entries however their screen wants.
///
/// `finalRank <= MAX+1` fetches one outpoint beyond the page so the
/// `truncated` bit is honest; `scoped_to_game` narrows the window to one
/// game, which helps a truncated caller **unless the flood names that same
/// gameId** (measured: it does not help then — see the residual at
/// [`assemble_hops_view`]).
///
/// The verification facts need NO join: `hopLockHex`, `hopSatsOnChain` and
/// `containerOutputs` are typed columns the overlay decoded from the
/// marker's own container at admission (#310), so this query touches no
/// BLOB, no `outputs` row, and no second topic.
pub fn hops_view_sql(scoped_to_game: bool) -> String {
    let game_filter = if scoped_to_game {
        " AND hp.gameId = ?2"
    } else {
        ""
    };
    format!(
        "SELECT w.identity AS identity, w.gameId AS gameId, w.hopTxid AS hopTxid, \
                w.hopVout AS hopVout, w.hopSats AS hopSats, \
                w.opponentIdentity AS opponentIdentity, \
                w.seatSettlePubkey AS seatSettlePubkey, w.seatSigHex AS seatSigHex, \
                w.identitySigHex AS identitySigHex, w.markerTxid AS markerTxid, \
                w.markerVout AS markerVout, \
                w.hopLockHex AS hopLockHex, w.hopSatsOnChain AS hopSatsOnChain, \
                w.containerOutputs AS containerOutputs, \
                w.spent AS spent, w.spendingTxid AS spendingTxid, \
                w.spentConfirmed AS spentConfirmed, \
                sb.proof_verified AS spenderProofVerified \
         FROM (SELECT identity, gameId, hopTxid, hopVout, hopSats, opponentIdentity, \
                  seatSettlePubkey, seatSigHex, identitySigHex, markerTxid, markerVout, \
                  hopLockHex, hopSatsOnChain, containerOutputs, \
                  spent, spendingTxid, spentConfirmed, \
                  markerCreatedAt, markerRowid, potCreatedAt, firstMarkerAt, \
                  paidTier, tier \
           FROM (SELECT identity, gameId, hopTxid, hopVout, hopSats, opponentIdentity, \
                    seatSettlePubkey, seatSigHex, identitySigHex, markerTxid, markerVout, \
                    hopLockHex, hopSatsOnChain, containerOutputs, \
                    spent, spendingTxid, spentConfirmed, \
                    markerCreatedAt, markerRowid, potCreatedAt, firstMarkerAt, \
                    paidTier, tier, \
                    DENSE_RANK() OVER (ORDER BY paidTier ASC, tier ASC, \
                                                hopSatsOnChain DESC, \
                                                COALESCE(potCreatedAt, firstMarkerAt) ASC, \
                                                hopTxid ASC, hopVout ASC) AS finalRank \
           FROM (SELECT identity, gameId, hopTxid, hopVout, hopSats, opponentIdentity, \
                    seatSettlePubkey, seatSigHex, identitySigHex, markerTxid, markerVout, \
                    hopLockHex, hopSatsOnChain, containerOutputs, \
                    spent, spendingTxid, spentConfirmed, \
                    markerCreatedAt, markerRowid, potCreatedAt, firstMarkerAt, \
                    paidTier, unknownHop, \
                    CASE WHEN unknownHop = 0 \
                         OR (freshUnknown = 1 AND hopRank <= {quota}) \
                         THEN 0 ELSE 1 END AS tier \
             FROM (SELECT identity, gameId, hopTxid, hopVout, hopSats, opponentIdentity, \
                      seatSettlePubkey, seatSigHex, identitySigHex, markerTxid, markerVout, \
                      hopLockHex, hopSatsOnChain, containerOutputs, \
                      spent, spendingTxid, spentConfirmed, \
                      markerCreatedAt, markerRowid, potCreatedAt, firstMarkerAt, \
                      paidTier, unknownHop, freshUnknown, \
                      DENSE_RANK() OVER (PARTITION BY unknownHop, freshUnknown \
                                         ORDER BY COALESCE(firstMarkerAt, 0) ASC, \
                                                  hopTxid ASC, hopVout ASC) AS hopRank \
               FROM (SELECT hp.identity AS identity, hp.gameId AS gameId, \
                        hp.txid AS hopTxid, hp.hopVout AS hopVout, \
                        hp.hopSats AS hopSats, \
                        hp.opponentIdentity AS opponentIdentity, \
                        hp.seatSettlePubkey AS seatSettlePubkey, \
                        hp.seatSigHex AS seatSigHex, \
                        hp.identitySigHex AS identitySigHex, \
                        hp.txid AS markerTxid, hp.outputIndex AS markerVout, \
                        hp.hopLockHex AS hopLockHex, \
                        hp.hopSatsOnChain AS hopSatsOnChain, \
                        hp.containerOutputs AS containerOutputs, \
                        r.spent AS spent, r.spendingTxid AS spendingTxid, \
                        r.spentConfirmed AS spentConfirmed, \
                        hp.createdAt AS markerCreatedAt, hp.rowid AS markerRowid, \
                        r.createdAt AS potCreatedAt, \
                        MIN(hp.createdAt) OVER (PARTITION BY hp.txid, hp.hopVout) \
                            AS firstMarkerAt, \
                        CASE WHEN hp.hopLockHex IS NOT NULL \
                                  AND hp.hopSatsOnChain = hp.hopSats \
                             THEN 0 ELSE 1 END AS paidTier, \
                        CASE WHEN r.txid IS NULL THEN 1 ELSE 0 END AS unknownHop, \
                        CASE WHEN r.txid IS NULL \
                                  AND COALESCE(MIN(hp.createdAt) OVER \
                                        (PARTITION BY hp.txid, hp.hopVout), 0) \
                                      >= unixepoch() - {fresh_secs} \
                             THEN 1 ELSE 0 END AS freshUnknown, \
                        ROW_NUMBER() OVER (PARTITION BY hp.txid, hp.hopVout \
                                           ORDER BY hp.createdAt ASC, hp.rowid ASC) AS rn \
                 FROM hopparty_records hp \
                 LEFT JOIN pot_records r \
                        ON r.txid = hp.txid AND r.outputIndex = hp.hopVout \
                 WHERE hp.identity = ?1{game_filter}) \
               WHERE rn <= {per_outpoint}))) \
           WHERE finalRank <= {rank_cap} \
           ORDER BY paidTier ASC, tier ASC, hopSatsOnChain DESC, \
                    COALESCE(potCreatedAt, firstMarkerAt) ASC, \
                    hopTxid ASC, hopVout ASC, markerCreatedAt ASC, markerRowid ASC \
           LIMIT {row_cap}) w \
         LEFT JOIN pot_beefs sb ON w.spendingTxid IS NOT NULL \
              AND sb.txid = lower(w.spendingTxid) \
         ORDER BY w.paidTier ASC, w.tier ASC, w.hopSatsOnChain DESC, \
                  COALESCE(w.potCreatedAt, w.firstMarkerAt) ASC, \
                  w.hopTxid ASC, w.hopVout ASC, w.markerCreatedAt ASC, w.markerRowid ASC",
        quota = HOPS_VIEW_UNKNOWN_HOP_QUOTA,
        fresh_secs = HOPS_VIEW_UNKNOWN_HOP_MAX_AGE_SECS,
        per_outpoint = HOPS_VIEW_ROWS_PER_OUTPOINT,
        rank_cap = HOPS_VIEW_MAX_OUTPOINTS + 1,
        row_cap = (HOPS_VIEW_MAX_OUTPOINTS + 1) * HOPS_VIEW_ROWS_PER_OUTPOINT,
        game_filter = game_filter,
    )
}

/// One `/hops-view` joined row, host-typed (the `hops_view_sql` shape).
/// Pot-side fields are `Option` because the `pot_records` join can MISS —
/// a marker whose hop the overlay never indexed yields NULL columns
/// (fail-safe: never asserted unspent).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HopsViewRow {
    /// The marker's identity (== the bound caller identity by SQL).
    pub identity: String,
    pub game_id: String,
    pub hop_txid: String,
    pub hop_vout: u32,
    pub hop_sats: u64,
    pub opponent_identity: String,
    pub seat_settle_pubkey: String,
    pub seat_sig_hex: String,
    pub identity_sig_hex: String,
    /// The txid carrying the marker OP_RETURN — the SAME tx as the hop
    /// (the marker rides it), so this equals `hop_txid` by construction.
    pub marker_txid: String,
    /// The marker output's index within that tx.
    pub marker_vout: u32,
    pub spent: Option<bool>,
    pub spending_txid: Option<String>,
    pub spent_confirmed: Option<bool>,
    /// `pot_beefs.proof_verified` for the recorded spender — the second
    /// accepted confirmation signal (#323 shared bar). `None` = no row.
    pub spender_proof_verified: Option<bool>,
    /// The CONTAINER's own output at `hop_vout`: its locking script
    /// (lowercase hex), decoded at admission (#310). `None` IFF the
    /// container has no such output — a PROVEN absence (see
    /// `container_outputs`) that REFUTES the marker.
    pub hop_lock_hex: Option<String>,
    /// That output's satoshi value as the chain records it. `None` in the
    /// same absent case.
    pub hop_sats_on_chain: Option<u64>,
    /// How many outputs the container has — makes the absence above
    /// provable rather than ambiguous.
    pub container_outputs: u32,
}

/// The three-valued read-time marker verification — `unknown` first-class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerVerification {
    Verified,
    Unverified,
    Unknown,
}

impl MarkerVerification {
    pub fn as_str(self) -> &'static str {
        match self {
            MarkerVerification::Verified => "verified",
            MarkerVerification::Unverified => "unverified",
            MarkerVerification::Unknown => "unknown",
        }
    }
}

/// The P2PKH lock a hop paying `seat_settle_pubkey` MUST carry:
/// `OP_DUP OP_HASH160 <hash160(pubkey)> OP_EQUALVERIFY OP_CHECKSIG`.
/// `None` when the pubkey hex is malformed (not 33 bytes) — an unbuildable
/// expectation can never verify (fail-safe).
pub fn expected_hop_lock_hex(seat_settle_pubkey_hex: &str) -> Option<String> {
    let pk = hex::decode(seat_settle_pubkey_hex).ok()?;
    if pk.len() != 33 {
        return None;
    }
    let h = bsv_rs::primitives::hash::hash160(&pk);
    Some(format!("76a914{}88ac", hex::encode(h)))
}

/// The decoded byte shapes of a row's digest-relevant hex fields:
/// (identity, opponent, gameId, seatSettlePubkey). No hop txid — the
/// container supplies it and no digest can bind it.
type RowDigestParts = (Vec<u8>, Vec<u8>, [u8; 32], Vec<u8>);

/// Decode the row's hex fields into the byte shapes the shared digest
/// builders take. `None` on any malformed field.
fn row_digest_parts(r: &HopsViewRow) -> Option<RowDigestParts> {
    let identity = hex::decode(r.identity.to_ascii_lowercase()).ok()?;
    let opponent = hex::decode(r.opponent_identity.to_ascii_lowercase()).ok()?;
    let game_id_v = hex::decode(r.game_id.to_ascii_lowercase()).ok()?;
    let settle_pk = hex::decode(r.seat_settle_pubkey.to_ascii_lowercase()).ok()?;
    if game_id_v.len() != 32 {
        return None;
    }
    let mut game_id = [0u8; 32];
    game_id.copy_from_slice(&game_id_v);
    Some((identity, opponent, game_id, settle_pk))
}

/// CANONICAL STRICT DER (gate MEDIUM-3, Rule 4c): re-encode the parsed
/// `(r, s)` and demand byte-equality with the submitted bytes.
///
/// `Signature::from_der` is permissive — measured on a real verifying row,
/// an `r` padded with a leading `0x00` (non-minimal INTEGER) and a garbage
/// byte appended inside the SEQUENCE length both still VERIFIED, while
/// high-S was already refused. The `67..=74` admission bounds constrain
/// only LENGTH, so they pin nothing about the encoding.
///
/// Two reasons this matters even though the outpoint keying already denies
/// an attacker the Rule-4-CORRECTION saturation (each variant still costs a
/// whole container):
///
///  1. the Rule-16 golden vector pins ONE encoding while a permissive
///     verifier accepts many, so a client emitting non-canonical DER would
///     pass in production and fail the cross-repo pin — the drift would be
///     found at the worst moment;
///  2. it collapses the honest candidate set to exactly one byte string
///     per signature, which is the property Rule 4c asks for.
///
/// This can never reject an honest marker: every LOW signing path emits
/// `Signature::to_der()` output, which is by construction what this
/// re-encodes to (pinned by the golden vector, which must still verify).
fn is_canonical_der(sig_bytes: &[u8], sig: &bsv_rs::primitives::ec::Signature) -> bool {
    sig.to_der() == sig_bytes
}

/// Verify the marker's SEAT signature: plain secp256k1 ECDSA under the
/// marker's OWN `seatSettlePubkey` over sha256(hopparty seatsig preimage)
/// — the byte layout is the overlay's [`overlay_discovery::hopparty::
/// hopparty_seatsig_preimage`] (one source of truth across the crates).
/// Any malformed key/sig/field is `false` (refused).
pub fn verify_hop_seat_sig(r: &HopsViewRow) -> bool {
    let Some((identity, _, game_id, _)) = row_digest_parts(r) else {
        return false;
    };
    let Some(preimage) =
        overlay_discovery::hopparty::hopparty_seatsig_preimage(&game_id, r.hop_vout, &identity)
    else {
        return false;
    };
    let Ok(pubkey) = bsv_rs::primitives::ec::PublicKey::from_hex(&r.seat_settle_pubkey) else {
        return false;
    };
    let Ok(sig_bytes) = hex::decode(&r.seat_sig_hex) else {
        return false;
    };
    let Ok(sig) = bsv_rs::primitives::ec::Signature::from_der(&sig_bytes) else {
        return false;
    };
    if !is_canonical_der(&sig_bytes, &sig) {
        return false; // non-canonical encoding — see `is_canonical_der`
    }
    let hash = bsv_rs::primitives::hash::sha256(&preimage);
    pubkey.verify(&hash, &sig)
}

/// Verify the marker's IDENTITY signature: the claimed identity really
/// published this marker (BRC-42/43 'anyone' verification under the SAME
/// `[1,'low potparty']` protocol constant `/results` uses — the version
/// tag inside the challenge is the domain separator, keyID = gameId).
/// Without this bar an opponent's wallet could seat-sign a preimage
/// embedding the VICTIM's identity with its OWN settle key over its OWN
/// hop — the potparty F1 shape — and land a "verified" row in the victim's
/// view. Only the named identity can mint this signature.
pub fn verify_hop_identity_binding(r: &HopsViewRow) -> bool {
    let Some((identity, opponent, game_id, settle_pk)) = row_digest_parts(r) else {
        return false;
    };
    let Some(challenge) = overlay_discovery::hopparty::hopparty_identity_challenge(
        &identity, &opponent, &game_id, r.hop_vout, r.hop_sats, &settle_pk,
    ) else {
        return false;
    };
    // Same canonical-DER bar as the seat signature. `anyone_sig_verifies`
    // is shared with `/results` (whose own encoding posture is not this
    // route's to change), so the bar is applied HERE, on this route's
    // bytes, before delegating the BRC-42 verification.
    let Ok(sig_bytes) = hex::decode(&r.identity_sig_hex) else {
        return false;
    };
    let Ok(sig) = bsv_rs::primitives::ec::Signature::from_der(&sig_bytes) else {
        return false;
    };
    if !is_canonical_der(&sig_bytes, &sig) {
        return false;
    }
    crate::results::anyone_sig_verifies(
        &r.identity.to_ascii_lowercase(),
        &r.game_id.to_ascii_lowercase(),
        &challenge,
        &r.identity_sig_hex,
        crate::results::potparty_protocol(),
    )
}

/// The read-time validity FILTER for one row (module docs). Order is
/// cost-aware: the container bars (a hash160 + two compares) run FIRST, so
/// a mismatched lock or value refuses with zero curve work; the two
/// signature bars only run on rows the container bars did not already
/// refuse.
///
/// # The residual: a PAID REPLAY, and why it is not patched here
///
/// Neither digest can bind the containing txid (a tx cannot embed its own
/// txid), so the marker bytes are portable. An attacker can copy an
/// admitted marker verbatim into a transaction of their own and place an
/// output at `hopVout` that satisfies both container bars. `seatSettlePubkey`
/// is public, so they CAN construct the lock — but the value bar means the
/// replay only verifies if that output pays **`hopSats` to the victim's own
/// settle key**, i.e. the attacker must GIVE THE VICTIM the full hop amount
/// (~80,800 sats in the #256 class, plus fees) for every noise row they
/// mint, and the victim can spend it: they hold the settle key. The effect
/// is bounded, self-defeating, and fails in the honest direction — an extra
/// `verified` row in the victim's own view, never a lost or altered one.
/// The `truncated` bit stays honest under it because each replay is a
/// distinct outpoint.
///
/// **No app-layer signature is invented to close it** (Rule 4 / the epoch
/// doc's recorded mistake). The only bar that would close it must bind the
/// container's INPUTS to the identity, and that is not available: the hop
/// is funded by arbitrary BRC-42 wallet children, and proving a wallet
/// child belongs to identity X server-side needs the very derivation this
/// design exists to avoid. What the container DOES buy is unforgeable
/// authorship of the honest row (`SIGHASH_ALL` commits the marker bytes to
/// the funding signature), which is the property that matters for the
/// money path.
///
/// # Legacy limit, stated plainly
///
/// Hops created BEFORE this ships carry no marker and never will — the
/// marker must ride the hop tx, and that tx is already on chain. Those hops
/// stay invisible to `/hops-view` forever. This is the same acknowledged
/// limit #319 carries for retrofitting potparty markers; it is not a
/// regression (today they are invisible too), but it means `/hops-view` is
/// a going-forward surface, not a backfill.
pub fn derive_marker_verification(r: &HopsViewRow) -> MarkerVerification {
    let Some(expected) = expected_hop_lock_hex(&r.seat_settle_pubkey) else {
        return MarkerVerification::Unverified; // malformed pubkey
    };
    // Bar 1+2: the CONTAINER's own output at hopVout. Absence here is
    // PROVEN (the container is fully known), so it refutes rather than
    // leaving the row open.
    let (Some(lock), Some(on_chain_sats)) = (&r.hop_lock_hex, r.hop_sats_on_chain) else {
        return MarkerVerification::Unverified;
    };
    if !lock.eq_ignore_ascii_case(&expected) {
        return MarkerVerification::Unverified;
    }
    if on_chain_sats != r.hop_sats {
        // The value bar — what makes the replay above cost real money.
        return MarkerVerification::Unverified;
    }
    if !verify_hop_seat_sig(r) {
        return MarkerVerification::Unverified;
    }
    if !verify_hop_identity_binding(r) {
        return MarkerVerification::Unverified;
    }
    MarkerVerification::Verified
}

/// The `/hops-view` status enum (module-docs table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HopStatus {
    Unspent,
    Spent,
    Unknown,
}

impl HopStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            HopStatus::Unspent => "unspent",
            HopStatus::Spent => "spent",
            HopStatus::Unknown => "unknown",
        }
    }
}

/// Derive `(status, statusSource)` from the hop's chain facts — never a
/// guess: an absent `pot_records` row and an unconfirmed spend pointer are
/// both (`Unknown`, `None`). The confirmation bar is the ONE shared
/// `logic::is_confirmed_landing_with_proof` (#323).
pub fn derive_hop_status(
    spent: Option<bool>,
    spent_confirmed: Option<bool>,
    spender_proof_verified: Option<bool>,
) -> (HopStatus, Option<&'static str>) {
    match spent {
        // Non-observation of a spend on an INDEXED hop (module docs).
        Some(false) => (HopStatus::Unspent, Some("chain")),
        Some(true) => {
            if crate::logic::is_confirmed_landing_with_proof(
                spent_confirmed,
                spender_proof_verified,
            ) {
                (HopStatus::Spent, Some("chain"))
            } else {
                // Recorded-but-unconfirmed: a displaceable intent.
                (HopStatus::Unknown, None)
            }
        }
        // No pot_records row: genuinely unknown, never asserted unspent.
        None => (HopStatus::Unknown, None),
    }
}

/// One `/hops-view` response entry, pre-JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HopEntry {
    pub game_id: String,
    pub hop_txid: String,
    pub hop_vout: u32,
    pub hop_sats: u64,
    pub opponent_identity: String,
    pub seat_settle_pubkey: String,
    /// Both signatures ride back verbatim so the CLIENT re-verifies —
    /// the server's filter is display labeling, never an authority.
    pub seat_sig_hex: String,
    pub identity_sig_hex: String,
    /// The marker's containing tx = the hop tx, and the marker's own vout
    /// within it (so a client can fetch the exact OP_RETURN back).
    pub marker_txid: String,
    pub marker_vout: u32,
    pub spent: Option<bool>,
    pub spending_txid: Option<String>,
    pub spent_confirmed: Option<bool>,
    pub status: HopStatus,
    pub status_source: Option<&'static str>,
    pub marker_verified: MarkerVerification,
}

/// Assemble the joined rows into response entries + the two honesty bits:
/// `truncated` (more than [`HOPS_VIEW_MAX_OUTPOINTS`] distinct outpoints
/// survived — the SQL fetches one outpoint beyond the page on purpose) and
/// `verify_budget_exhausted` (rows past [`HOPS_VIEW_VERIFY_BUDGET`] were
/// labeled `unknown` without being checked).
///
/// # VERIFY-THEN-PAGE (gate HIGH-1's real closure)
///
/// The SQL orders candidates by what the chain settled (paid tier, then
/// value), which raises the price of crowding the candidate set. But the
/// property that actually cannot be forged is the **identity signature**:
/// an attacker can mint a marker naming the victim's identity, can pay the
/// claimed value, and can even mint a valid `seatSig` over the victim's
/// identity with their own settle key — bars 1, 2 and 3a all pass — yet
/// they can never produce bar 3b, the BRC-42 signature under the victim's
/// identity key. So once verification has run, the served page is ordered
/// **verified first**: an honest row that reached the candidate set can
/// never be pushed off the page by rows that cannot verify, however many
/// of them there are and however richly funded.
///
/// The sort is STABLE, so within each verification class the SQL's
/// deterministic chain-fact order is preserved exactly.
///
/// # Residual, RE-PRICED at the delta gate (the first pricing was wrong)
///
/// This reorders the PAGE; it does not enlarge the CANDIDATE set, and the
/// ranking still reads its top keys from an attacker-writable table. What
/// the ranking DID close, measured through the real producer chain: the
/// original ~3,600-sat dust flood is dead (dust at k=400 leaves the honest
/// row at page position 0, Verified — whether the flood is reactive or
/// pre-dated).
///
/// What survives is **reactive, single-actor, permanent, and cheap**:
///
/// | shape (k = 100 outpoints)                    | honest row | cost |
/// |----------------------------------------------|-----------|------|
/// | dust, reactive or pre-dated                  | position 0, Verified | — |
/// | paid at EXACTLY the hop value, reactive      | position 0, Verified | — |
/// | **paid at hop value + 1, REACTIVE**          | **absent**, `truncated:true` | ~3.5k sats burned |
/// | **one coin CHAINED through k txs**           | **absent**, `truncated:true` | ~5.2k burned, ~86k peak capital |
///
/// Three corrections to what an earlier revision of this comment claimed,
/// each of which mattered (Rule 10 — the most specific claim was the most
/// wrong):
///
///  1. **Pre-dating is NOT required.** Paying exactly the hop value lands
///     on the `COALESCE(potCreatedAt, firstMarkerAt) ASC` tie-break, so the
///     honest row won on AGE, not on the value key. One satoshi more skips
///     the tie-break entirely and evicts reactively. Threshold unchanged
///     at exactly k=100 (k=99 present, k=100 absent).
///  2. **The capital is ~ONE hop, not k hops.** `hopSatsOnChain` is decoded
///     once at admission and never re-read, so spending the outputs
///     afterwards does not restore the honest row: the attacker chains a
///     SINGLE coin through k transactions, each spending the previous
///     output into the next minus fee, one marker each. Measured peak
///     capital ≈ 86,000 sats (~1.06× the victim's hop) and FULLY
///     RECOVERABLE.
///  3. **The burn barely moved** — ~1.0–1.4× the pre-fix cost, not the
///     ~2,244× an earlier revision claimed. The honest summary is: burn
///     roughly unchanged, peak *recoverable* capital raised ~23×.
///
/// And the price **scales with the victim's own ante**, so a low-stakes hop
/// is proportionally cheaper to erase.
///
/// Disposition: tracked residual, not a money-loss regression — the honest
/// row is HIDDEN, never altered, `truncated: true` is always set, and the
/// signatures ride back for client re-verification. It is the same class
/// #281 documents for `partyFor` (a discovery query has no verified key
/// material to bind). **Closure direction for the issue, not this branch:**
/// the only inputs an attacker cannot supply are #318's per-identity auth +
/// quota, or promoting on the identity signature the SQL cannot currently
/// see (a `seatSigValid`-at-admission ORDERING HINT, re-verified
/// unconditionally downstream — the same shape recommended for #283).
///
/// Pinned from the UNSAFE side by
/// `a_value_plus_one_reactive_flood_is_the_documented_residual` and
/// `a_chained_single_coin_flood_is_the_documented_residual`.
pub fn assemble_hops_view(rows: Vec<HopsViewRow>) -> (Vec<HopEntry>, bool, bool) {
    // Distinct outpoints in served order; drop rows beyond the page.
    let mut outpoints: Vec<(String, u32)> = Vec::new();
    for r in &rows {
        let key = (r.hop_txid.to_ascii_lowercase(), r.hop_vout);
        if !outpoints.contains(&key) {
            outpoints.push(key);
        }
    }
    let truncated = outpoints.len() > HOPS_VIEW_MAX_OUTPOINTS;
    outpoints.truncate(HOPS_VIEW_MAX_OUTPOINTS);

    let mut verify_budget = HOPS_VIEW_VERIFY_BUDGET;
    let mut budget_exhausted = false;
    let mut entries: Vec<HopEntry> = rows
        .into_iter()
        .filter(|r| outpoints.contains(&(r.hop_txid.to_ascii_lowercase(), r.hop_vout)))
        .map(|r| {
            // The budget is spent in the SQL's QUALITY order, so a paid
            // honest row is verified before any dust row can consume a slot.
            let marker_verified = if verify_budget > 0 {
                verify_budget -= 1;
                derive_marker_verification(&r)
            } else {
                // Out of budget: "we could not look" — unknown, surfaced
                // via the body bit, never a silent unverified/verified.
                budget_exhausted = true;
                MarkerVerification::Unknown
            };
            let (status, status_source) =
                derive_hop_status(r.spent, r.spent_confirmed, r.spender_proof_verified);
            HopEntry {
                game_id: r.game_id,
                hop_txid: r.hop_txid,
                hop_vout: r.hop_vout,
                hop_sats: r.hop_sats,
                opponent_identity: r.opponent_identity,
                seat_settle_pubkey: r.seat_settle_pubkey,
                seat_sig_hex: r.seat_sig_hex,
                identity_sig_hex: r.identity_sig_hex,
                marker_txid: r.marker_txid,
                marker_vout: r.marker_vout,
                spent: r.spent,
                spending_txid: r.spending_txid,
                spent_confirmed: r.spent_confirmed,
                status,
                status_source,
                marker_verified,
            }
        })
        .collect();

    // VERIFY-THEN-PAGE: verified rows lead, then the ones we could not
    // check (budget), then the refuted. STABLE, so the chain-fact order
    // inside each class is the SQL's.
    entries.sort_by_key(|e| match e.marker_verified {
        MarkerVerification::Verified => 0u8,
        MarkerVerification::Unknown => 1,
        MarkerVerification::Unverified => 2,
    });
    (entries, truncated, budget_exhausted)
}

/// Assemble the `/hops-view` wire body:
/// `{"identity","tip":<height|null>,"truncated":<bool>,
///   "verifyBudgetExhausted":<bool>,"hops":[{gameId,hopTxid,hopVout,
///   hopSats,opponentIdentity,seatSettlePubkey,seatSigHex,identitySigHex,
///   markerTxid,spent,spendingTxid,spentConfirmed,status,statusSource,
///   markerVerified}]}`.
/// `tip` mirrors the sibling views (`null` on a chaintracks fault — the D1
/// facts still serve).
pub fn hops_view_body(
    identity: &str,
    tip: Option<u64>,
    entries: &[HopEntry],
    truncated: bool,
    verify_budget_exhausted: bool,
) -> String {
    let arr: Vec<serde_json::Value> = entries
        .iter()
        .map(|e| {
            json!({
                "gameId": e.game_id,
                "hopTxid": e.hop_txid,
                "hopVout": e.hop_vout,
                "hopSats": e.hop_sats,
                "opponentIdentity": e.opponent_identity,
                "seatSettlePubkey": e.seat_settle_pubkey,
                "seatSigHex": e.seat_sig_hex,
                "identitySigHex": e.identity_sig_hex,
                "markerTxid": e.marker_txid,
                "markerVout": e.marker_vout,
                "spent": e.spent,
                "spendingTxid": e.spending_txid,
                "spentConfirmed": e.spent_confirmed,
                "status": e.status.as_str(),
                "statusSource": e.status_source,
                "markerVerified": e.marker_verified.as_str(),
            })
        })
        .collect();
    json!({
        "identity": identity,
        "tip": tip,
        "truncated": truncated,
        "verifyBudgetExhausted": verify_budget_exhausted,
        "hops": arr,
    })
    .to_string()
}

// ============================================================================
// Tests (pure logic; the real-SQLite end-to-end cells live in
// tests/hops_view_sqlite.rs)
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use bsv_rs::wallet::{
        Counterparty, CreateSignatureArgs, GetPublicKeyArgs, ProtoWallet, Protocol, SecurityLevel,
    };

    fn h64(seed: u8) -> String {
        format!("{seed:02x}").repeat(32)
    }

    fn wallet_of(seed: u8) -> ProtoWallet {
        ProtoWallet::new(Some(
            bsv_rs::primitives::ec::PrivateKey::from_hex(&format!("{seed:064x}")).unwrap(),
        ))
    }

    /// A FULLY REAL hopparty row: genuine BRC-42 settle derivation +
    /// settle-key ECDSA over the exact seatsig preimage + genuine BRC-42
    /// 'anyone' identity signature over the exact challenge — the same
    /// crypto the client will produce.
    pub(crate) fn real_row(identity_seed: u8, hop_txid_byte: u8) -> HopsViewRow {
        // `hop_txid_byte` varies the CONTAINER (= the hop tx). It is not in
        // any digest — the container supplies it — so varying it must not
        // disturb either signature; the cells below rely on that.
        let wallet = wallet_of(identity_seed);
        let identity_hex = wallet.identity_key_hex().to_ascii_lowercase();
        let identity = hex::decode(&identity_hex).unwrap();
        let opponent_key =
            bsv_rs::primitives::ec::PrivateKey::from_hex(&format!("{:064x}", 0xb0u8)).unwrap();
        let opponent_pub = opponent_key.public_key();
        let opponent = hex::decode(opponent_pub.to_hex()).unwrap();
        let game_id = [0x11u8; 32];
        let hop_txid = [hop_txid_byte; 32];
        let hop_vout = 0u32;
        let hop_sats = 80_800u64;

        let settle_protocol = Protocol::new(SecurityLevel::Counterparty, "low settle");
        let settle_pub_hex = wallet
            .get_public_key(GetPublicKeyArgs {
                identity_key: false,
                protocol_id: Some(settle_protocol.clone()),
                key_id: Some(hex::encode(game_id)),
                counterparty: Some(Counterparty::Other(opponent_pub.clone())),
                for_self: Some(true),
            })
            .unwrap()
            .public_key
            .to_ascii_lowercase();
        let settle_pk = hex::decode(&settle_pub_hex).unwrap();

        let preimage =
            overlay_discovery::hopparty::hopparty_seatsig_preimage(&game_id, hop_vout, &identity)
                .unwrap();
        let seat_sig = wallet
            .create_signature(CreateSignatureArgs {
                data: Some(preimage),
                hash_to_directly_sign: None,
                protocol_id: settle_protocol,
                key_id: hex::encode(game_id),
                counterparty: Some(Counterparty::Other(opponent_pub)),
            })
            .unwrap()
            .signature;

        let challenge = overlay_discovery::hopparty::hopparty_identity_challenge(
            &identity, &opponent, &game_id, hop_vout, hop_sats, &settle_pk,
        )
        .unwrap();
        let identity_sig = wallet
            .create_signature(CreateSignatureArgs {
                data: Some(challenge),
                hash_to_directly_sign: None,
                protocol_id: Protocol::new(SecurityLevel::App, "low potparty"),
                key_id: hex::encode(game_id),
                counterparty: Some(Counterparty::Anyone),
            })
            .unwrap()
            .signature;

        HopsViewRow {
            identity: identity_hex,
            game_id: hex::encode(game_id),
            hop_txid: hex::encode(hop_txid),
            hop_vout,
            hop_sats,
            opponent_identity: hex::encode(&opponent),
            seat_settle_pubkey: settle_pub_hex.clone(),
            seat_sig_hex: hex::encode(seat_sig),
            identity_sig_hex: hex::encode(identity_sig),
            // The marker rides the hop tx, so markerTxid == hopTxid.
            marker_txid: hex::encode(hop_txid),
            marker_vout: 1,
            spent: Some(false),
            spending_txid: None,
            spent_confirmed: Some(false),
            spender_proof_verified: None,
            // The container's output at hopVout pays the settle key exactly
            // the claimed value — the fully VERIFYING shape by default.
            hop_lock_hex: expected_hop_lock_hex(&settle_pub_hex),
            hop_sats_on_chain: Some(hop_sats),
            container_outputs: 2,
        }
    }

    // ── markerVerified derivation matrix ─────────────────────────────────

    #[test]
    fn real_marker_in_a_matching_container_is_verified() {
        // The positive control: both real sigs + the container's own output
        // at hopVout paying the settle key exactly hopSats — every bar
        // reached and cleared.
        let r = real_row(0xa1, 0xaa);
        assert!(verify_hop_seat_sig(&r));
        assert!(verify_hop_identity_binding(&r));
        assert_eq!(derive_marker_verification(&r), MarkerVerification::Verified);
    }

    /// The container is fully known at admission, so "no output at
    /// hopVout" REFUTES the marker — it never leaves it open.
    #[test]
    fn a_container_without_that_output_refutes_the_marker() {
        let mut r = real_row(0xa1, 0xaa);
        r.hop_lock_hex = None;
        r.hop_sats_on_chain = None;
        assert_eq!(
            derive_marker_verification(&r),
            MarkerVerification::Unverified,
            "PROVEN absence refutes; it is not an unknown"
        );
    }

    #[test]
    fn mismatched_container_lock_is_unverified() {
        let mut r = real_row(0xa1, 0xaa);
        // The container's output at hopVout pays a DIFFERENT key than the
        // marker claims — refused with zero curve work.
        r.hop_lock_hex = Some(format!("76a914{}88ac", "aa".repeat(20)));
        assert_eq!(
            derive_marker_verification(&r),
            MarkerVerification::Unverified
        );
    }

    /// The VALUE bar — the one that makes the replay residual cost money.
    /// A container that pays the right key the WRONG amount is refused.
    #[test]
    fn mismatched_container_value_is_unverified() {
        for on_chain in [0u64, 1, 80_799, 80_801, u64::MAX] {
            let mut r = real_row(0xa1, 0xaa);
            r.hop_sats_on_chain = Some(on_chain);
            assert_eq!(
                derive_marker_verification(&r),
                MarkerVerification::Unverified,
                "claimed 80_800 vs on-chain {on_chain} must refuse"
            );
        }
        // Positive control: the exact value clears.
        assert_eq!(
            derive_marker_verification(&real_row(0xa1, 0xaa)),
            MarkerVerification::Verified
        );
    }

    /// The PAID REPLAY residual, executed rather than asserted: the marker
    /// bytes are portable (no digest binds the containing txid), so the
    /// SAME signatures verify inside an attacker's container — but ONLY if
    /// that container pays `hopSats` to the VICTIM's settle key. The cell
    /// pins both halves: the replay verifies (so the residual is real and
    /// documented, not imagined), and every cheaper variant refuses.
    #[test]
    fn a_replay_verifies_only_when_the_attacker_actually_pays_the_victim() {
        let honest = real_row(0xa1, 0xaa);
        // The attacker's own container — a different txid entirely.
        let mut replay = honest.clone();
        replay.hop_txid = h64(0x99);
        replay.marker_txid = h64(0x99);
        // Paying the victim's key the full amount: this DOES verify.
        assert_eq!(
            derive_marker_verification(&replay),
            MarkerVerification::Verified,
            "the documented residual: a replay that really pays the victim verifies"
        );
        // Every cheaper variant fails: paying less…
        let mut cheap = replay.clone();
        cheap.hop_sats_on_chain = Some(1);
        assert_eq!(
            derive_marker_verification(&cheap),
            MarkerVerification::Unverified
        );
        // …or paying someone else.
        let mut elsewhere = replay.clone();
        elsewhere.hop_lock_hex = Some(format!("76a914{}88ac", "77".repeat(20)));
        assert_eq!(
            derive_marker_verification(&elsewhere),
            MarkerVerification::Unverified
        );
        // …or omitting the output.
        let mut omitted = replay;
        omitted.hop_lock_hex = None;
        omitted.hop_sats_on_chain = None;
        assert_eq!(
            derive_marker_verification(&omitted),
            MarkerVerification::Unverified
        );
    }

    /// gate MEDIUM-3 — CANONICAL STRICT DER (Rule 4c). The gate measured
    /// three non-canonical encodings against a real verifying row: high-S
    /// was already refused, but a NON-MINIMAL `r` INTEGER (leading 0x00)
    /// and a trailing garbage byte inside the SEQUENCE both still
    /// VERIFIED. Each is now refused by re-encoding the parsed `(r, s)`
    /// and demanding byte-equality.
    #[test]
    fn non_canonical_der_is_refused_on_both_signature_bars() {
        let base = real_row(0xa1, 0xaa);
        // Positive control FIRST: the canonical row clears both bars, so
        // the refusals below cannot pass for the wrong reason.
        assert!(verify_hop_seat_sig(&base));
        assert!(verify_hop_identity_binding(&base));
        assert_eq!(
            derive_marker_verification(&base),
            MarkerVerification::Verified
        );

        /// Re-encode a DER signature with a NON-MINIMAL `r` (a redundant
        /// leading zero byte), keeping it parseable and the value equal.
        fn pad_r(der_hex: &str) -> String {
            let d = hex::decode(der_hex).unwrap();
            assert_eq!(d[0], 0x30, "SEQUENCE");
            let r_len = d[3] as usize;
            let mut out = vec![0x30u8, 0, 0x02, (r_len + 1) as u8, 0x00];
            out.extend_from_slice(&d[4..4 + r_len]); // r bytes
            out.extend_from_slice(&d[4 + r_len..]); // the whole s INTEGER
            let body = out.len() - 2;
            out[1] = body as u8;
            hex::encode(out)
        }
        /// Append a garbage byte INSIDE the SEQUENCE length.
        fn append_junk(der_hex: &str) -> String {
            let mut d = hex::decode(der_hex).unwrap();
            d.push(0xff);
            let body = d.len() - 2;
            d[1] = body as u8;
            hex::encode(d)
        }

        for mutate in [pad_r, append_junk] {
            // …on the SEAT signature.
            let mut r = base.clone();
            r.seat_sig_hex = mutate(&base.seat_sig_hex);
            assert_ne!(
                r.seat_sig_hex, base.seat_sig_hex,
                "the fixture really mutated"
            );
            assert!(
                !verify_hop_seat_sig(&r),
                "non-canonical seat DER must be refused: {}",
                r.seat_sig_hex
            );
            assert_eq!(
                derive_marker_verification(&r),
                MarkerVerification::Unverified
            );
            // …and on the IDENTITY signature.
            let mut r = base.clone();
            r.identity_sig_hex = mutate(&base.identity_sig_hex);
            assert!(
                !verify_hop_identity_binding(&r),
                "non-canonical identity DER must be refused: {}",
                r.identity_sig_hex
            );
            assert_eq!(
                derive_marker_verification(&r),
                MarkerVerification::Unverified
            );
        }
    }

    /// The strict bar can never reject an HONEST marker: every LOW signing
    /// path emits `Signature::to_der()`, which is by construction what the
    /// re-encode produces. Driven through the real exported builders on the
    /// FROZEN cross-repo golden vector — if the canonical rule ever
    /// disagreed with the wire contract, this goes red (Rule 16).
    #[test]
    fn the_golden_vector_signatures_are_canonical_der() {
        let script = hex::decode(overlay_discovery::hopparty::GOLDEN_HOPPARTY_HEX).unwrap();
        let m = overlay_discovery::hopparty::parse_hopparty_marker(&script).unwrap();
        for sig_bytes in [&m.seat_sig, &m.identity_sig] {
            let sig = bsv_rs::primitives::ec::Signature::from_der(sig_bytes).unwrap();
            assert!(
                is_canonical_der(sig_bytes, &sig),
                "the frozen golden vector must already be canonical DER"
            );
        }
    }

    #[test]
    fn junk_seat_sig_never_verifies() {
        let mut r = real_row(0xa1, 0xaa);
        r.seat_sig_hex = format!("3045{}", "ab".repeat(69));
        assert!(!verify_hop_seat_sig(&r));
        assert_eq!(
            derive_marker_verification(&r),
            MarkerVerification::Unverified
        );
        // Even with the container's output ABSENT, junk sigs are failed,
        // never laundered into `unknown`.
        r.hop_lock_hex = None;
        r.hop_sats_on_chain = None;
        assert_eq!(
            derive_marker_verification(&r),
            MarkerVerification::Unverified
        );
    }

    #[test]
    fn junk_or_missing_identity_sig_is_unverified() {
        // The F1 bar: a valid seatSig alone (which an opponent's wallet CAN
        // mint over the victim's identity) never verifies the row.
        let mut r = real_row(0xa1, 0xaa);
        r.identity_sig_hex = format!("3045{}", "cd".repeat(69));
        assert!(verify_hop_seat_sig(&r), "seat sig alone still valid");
        assert!(!verify_hop_identity_binding(&r));
        assert_eq!(
            derive_marker_verification(&r),
            MarkerVerification::Unverified
        );
    }

    #[test]
    fn tampered_fields_break_the_right_signature() {
        // hopSats is bound by the IDENTITY challenge (not the seat
        // preimage): tampering it must break the identity bar.
        let mut r = real_row(0xa1, 0xaa);
        r.hop_sats += 1;
        assert!(verify_hop_seat_sig(&r), "seat preimage does not bind sats");
        assert!(!verify_hop_identity_binding(&r), "identity challenge does");
        // The hop VOUT is bound by BOTH digests.
        let mut r = real_row(0xa1, 0xaa);
        r.hop_vout += 1;
        assert!(!verify_hop_seat_sig(&r));
        assert!(!verify_hop_identity_binding(&r));
        // The containing TXID is bound by NEITHER — it cannot be (a tx
        // cannot embed its own txid), which is exactly the paid-replay
        // residual pinned above. Stated here so the boundary is explicit.
        let mut r = real_row(0xa1, 0xaa);
        r.hop_txid = h64(0xdd);
        r.marker_txid = h64(0xdd);
        assert!(
            verify_hop_seat_sig(&r),
            "no digest binds the container txid"
        );
        assert!(verify_hop_identity_binding(&r));
        // The identity is bound by BOTH (a thief cannot re-pair a genuine
        // seatSig with a different identity).
        let mut r = real_row(0xa1, 0xaa);
        r.identity = format!("02{}", "e9".repeat(32));
        assert!(!verify_hop_seat_sig(&r));
        assert!(!verify_hop_identity_binding(&r));
    }

    #[test]
    fn malformed_fields_are_unverified_never_a_panic() {
        for mutate in [
            |r: &mut HopsViewRow| r.seat_settle_pubkey = "zz".into(),
            |r: &mut HopsViewRow| r.seat_settle_pubkey = "02abcd".into(),
            |r: &mut HopsViewRow| r.game_id = "11".into(),
            |r: &mut HopsViewRow| r.identity = String::new(),
            |r: &mut HopsViewRow| r.seat_sig_hex = "nothex".into(),
            |r: &mut HopsViewRow| r.identity_sig_hex = String::new(),
        ] {
            let mut r = real_row(0xa1, 0xaa);
            mutate(&mut r);
            assert_eq!(
                derive_marker_verification(&r),
                MarkerVerification::Unverified
            );
        }
    }

    /// Domain separation is load-bearing: a signature minted over the
    /// POTPARTY seatsig preimage for the same (game, outpoint, identity)
    /// must NOT verify as a hopparty seatSig (and vice versa is covered by
    /// the overlay's golden cells).
    #[test]
    fn a_potparty_seatsig_never_verifies_as_hopparty() {
        let r = real_row(0xa1, 0xaa);
        // Re-sign the POTPARTY preimage with the same settle key by
        // swapping the domain: build the potparty preimage bytes and sign.
        let wallet = wallet_of(0xa1);
        let identity = hex::decode(&r.identity).unwrap();
        let game_id: [u8; 32] = hex::decode(&r.game_id).unwrap().try_into().unwrap();
        let mut potparty_pre =
            overlay_discovery::hopparty::hopparty_seatsig_preimage(&game_id, r.hop_vout, &identity)
                .unwrap();
        // Swap the 24-byte domain prefix for potparty-v2's.
        potparty_pre[..24].copy_from_slice(b"LOW/potparty/v2/seatsig|");
        let opponent_pub =
            bsv_rs::primitives::ec::PublicKey::from_hex(&r.opponent_identity).unwrap();
        let cross_sig = wallet
            .create_signature(CreateSignatureArgs {
                data: Some(potparty_pre),
                hash_to_directly_sign: None,
                protocol_id: Protocol::new(SecurityLevel::Counterparty, "low settle"),
                key_id: r.game_id.clone(),
                counterparty: Some(Counterparty::Other(opponent_pub)),
            })
            .unwrap()
            .signature;
        let mut cross = r.clone();
        cross.seat_sig_hex = hex::encode(cross_sig);
        assert!(
            !verify_hop_seat_sig(&cross),
            "a potparty-domain seatSig must never verify over the hopparty domain"
        );
        // Positive control: the un-tampered row still verifies.
        assert!(verify_hop_seat_sig(&r));
    }

    // ── status derivation table ──────────────────────────────────────────

    #[test]
    fn hop_status_table() {
        // Indexed, unspent.
        assert_eq!(
            derive_hop_status(Some(false), Some(false), None),
            (HopStatus::Unspent, Some("chain"))
        );
        // Confirmed spend (flag).
        assert_eq!(
            derive_hop_status(Some(true), Some(true), None),
            (HopStatus::Spent, Some("chain"))
        );
        // Confirmed via the verified spender proof (the #323 second signal).
        assert_eq!(
            derive_hop_status(Some(true), Some(false), Some(true)),
            (HopStatus::Spent, Some("chain"))
        );
        // Recorded-but-unconfirmed: a displaceable intent — unknown.
        assert_eq!(
            derive_hop_status(Some(true), Some(false), None),
            (HopStatus::Unknown, None)
        );
        assert_eq!(
            derive_hop_status(Some(true), None, Some(false)),
            (HopStatus::Unknown, None)
        );
        // No pot_records row: never asserted unspent.
        assert_eq!(
            derive_hop_status(None, None, None),
            (HopStatus::Unknown, None)
        );
    }

    // ── assembly: truncation + verify budget + body ──────────────────────

    #[test]
    fn truncation_bit_is_honest_and_the_page_is_outpoint_counted() {
        // MAX+1 distinct outpoints (as the SQL's rank_cap admits): the page
        // keeps MAX and reports truncated.
        let rows: Vec<HopsViewRow> = (0..=HOPS_VIEW_MAX_OUTPOINTS)
            .map(|i| {
                let mut r = real_row(0xa1, 0xaa);
                r.hop_txid = format!("{i:064x}");
                r
            })
            .collect();
        let (entries, truncated, _) = assemble_hops_view(rows.clone());
        assert_eq!(entries.len(), HOPS_VIEW_MAX_OUTPOINTS);
        assert!(truncated, "one outpoint past the page ⇒ truncated");
        // Exactly MAX outpoints: complete.
        let (entries, truncated, _) = assemble_hops_view(rows[..HOPS_VIEW_MAX_OUTPOINTS].to_vec());
        assert_eq!(entries.len(), HOPS_VIEW_MAX_OUTPOINTS);
        assert!(!truncated);
        // Multiple rows of ONE outpoint never count as multiple outpoints.
        let (entries, truncated, _) = assemble_hops_view(vec![
            real_row(0xa1, 0xaa),
            real_row(0xa1, 0xaa),
            real_row(0xa1, 0xaa),
        ]);
        assert_eq!(entries.len(), 3, "superset rows all served");
        assert!(!truncated);
    }

    #[test]
    fn verify_budget_labels_overflow_unknown_and_surfaces_the_bit() {
        // Budget+1 rows across enough outpoints to stay on the page: the
        // overflow is labeled unknown WITHOUT being checked, and the bit is
        // raised. (Rows share outpoints so the page bound doesn't cut them.)
        let mut rows: Vec<HopsViewRow> = Vec::new();
        for i in 0..HOPS_VIEW_MAX_OUTPOINTS {
            for _ in 0..2 {
                let mut r = real_row(0xa1, 0xaa);
                r.hop_txid = format!("{i:064x}");
                // Junk sig so a CHECKED row is Unverified — distinguishing
                // checked (unverified) from unchecked (unknown).
                r.seat_sig_hex = format!("3045{}", "ab".repeat(69));
                rows.push(r);
            }
        }
        let total = rows.len();
        assert!(total > HOPS_VIEW_VERIFY_BUDGET);
        let (entries, _, exhausted) = assemble_hops_view(rows);
        assert!(exhausted, "the budget bit must be surfaced");
        // Counts, not positions: verify-then-page reorders the served list,
        // so the CLASS SIZES are what encode "checked vs not looked at".
        assert_eq!(
            entries
                .iter()
                .filter(|e| e.marker_verified == MarkerVerification::Unverified)
                .count(),
            HOPS_VIEW_VERIFY_BUDGET,
            "exactly the budget many rows were genuinely checked"
        );
        assert_eq!(
            entries
                .iter()
                .filter(|e| e.marker_verified == MarkerVerification::Unknown)
                .count(),
            total - HOPS_VIEW_VERIFY_BUDGET,
            "the remainder is unknown (we could not look), never a verdict"
        );
        // The honest page never exhausts the budget.
        let (_, _, exhausted) = assemble_hops_view(vec![real_row(0xa1, 0xaa)]);
        assert!(!exhausted);
    }

    /// VERIFY-THEN-PAGE (gate HIGH-1): a verified row always leads the
    /// served page, however many unverifiable rows precede it in the SQL's
    /// order — the identity signature is the input an attacker cannot
    /// forge, so it, not arrival order, decides what the caller sees first.
    #[test]
    fn verified_rows_lead_the_page_and_the_order_is_stable() {
        let honest = real_row(0xa1, 0xaa);
        let mut rows: Vec<HopsViewRow> = Vec::new();
        // 50 unverifiable rows FIRST in SQL order…
        for i in 0..50u32 {
            let mut junk = real_row(0xa1, 0xbb);
            junk.hop_txid = format!("{i:064x}");
            junk.seat_sig_hex = format!("3045{}", "ab".repeat(69));
            rows.push(junk);
        }
        // …then the honest one.
        rows.push(honest.clone());
        let (entries, _, _) = assemble_hops_view(rows);
        assert_eq!(
            entries[0].marker_verified,
            MarkerVerification::Verified,
            "the verified row leads regardless of SQL position"
        );
        assert_eq!(entries[0].hop_txid, honest.hop_txid);
        // STABLE within a class: the 50 junk rows keep their SQL order.
        let junk_order: Vec<&String> = entries[1..].iter().map(|e| &e.hop_txid).collect();
        let expected: Vec<String> = (0..50u32).map(|i| format!("{i:064x}")).collect();
        assert_eq!(
            junk_order,
            expected.iter().collect::<Vec<_>>(),
            "the chain-fact order inside a class must be preserved exactly"
        );
    }

    /// The budget is spent in the SQL's QUALITY order, so a paid honest row
    /// is verified before dust rows can consume slots (gate MEDIUM-4's
    /// degradation bound).
    #[test]
    fn budget_is_spent_in_quality_order() {
        // The honest row FIRST (as the SQL's paid-tier ordering puts it),
        // then budget-many dust rows.
        let mut rows = vec![real_row(0xa1, 0xaa)];
        // Share outpoints (4 rows each, the per-outpoint superset) so the
        // PAGE bound does not cut the flood before the BUDGET binds.
        for i in 0..50u32 {
            for _ in 0..HOPS_VIEW_ROWS_PER_OUTPOINT {
                let mut junk = real_row(0xa1, 0xbb);
                junk.hop_txid = format!("{i:064x}");
                junk.seat_sig_hex = format!("3045{}", "ab".repeat(69));
                rows.push(junk);
            }
        }
        assert!(rows.len() > HOPS_VIEW_VERIFY_BUDGET);
        let (entries, _, exhausted) = assemble_hops_view(rows);
        assert!(exhausted, "the flood does exhaust the budget");
        let honest = entries
            .iter()
            .find(|e| e.hop_txid == real_row(0xa1, 0xaa).hop_txid)
            .expect("the honest row is still served");
        assert_eq!(
            honest.marker_verified,
            MarkerVerification::Verified,
            "the honest row is verified FIRST — a budget flood degrades the \
             attacker's own rows to unknown, never the honest one"
        );
    }

    #[test]
    fn body_shape_and_labels() {
        let verified = real_row(0xa1, 0xaa);
        let mut junk = real_row(0xa1, 0xbb);
        junk.seat_sig_hex = format!("3045{}", "ab".repeat(69));
        junk.spent = None;
        junk.spent_confirmed = None;
        let (entries, truncated, exhausted) = assemble_hops_view(vec![verified.clone(), junk]);
        let body = hops_view_body(
            &verified.identity,
            Some(960_000),
            &entries,
            truncated,
            exhausted,
        );
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["identity"], json!(verified.identity));
        assert_eq!(v["tip"], json!(960_000));
        assert_eq!(v["truncated"], json!(false));
        assert_eq!(v["verifyBudgetExhausted"], json!(false));
        let e0 = &v["hops"][0];
        assert_eq!(e0["hopTxid"], json!(verified.hop_txid));
        assert_eq!(e0["hopVout"], json!(0));
        assert_eq!(e0["hopSats"], json!(80_800));
        assert_eq!(e0["status"], json!("unspent"));
        assert_eq!(e0["statusSource"], json!("chain"));
        assert_eq!(e0["markerVerified"], json!("verified"));
        assert_eq!(e0["seatSettlePubkey"], json!(verified.seat_settle_pubkey));
        assert_eq!(
            e0["markerTxid"],
            json!(verified.hop_txid),
            "the marker rides the hop tx"
        );
        assert_eq!(e0["markerVout"], json!(1));
        // The junk row is SERVED, labeled — never dropped.
        let e1 = &v["hops"][1];
        assert_eq!(e1["markerVerified"], json!("unverified"));
        assert_eq!(e1["status"], json!("unknown"));
        assert!(e1["statusSource"].is_null());
        // The sigs ride back for client re-verification.
        assert!(e0["seatSigHex"].is_string() && e0["identitySigHex"].is_string());
    }

    #[test]
    fn empty_body_and_null_tip() {
        let v: serde_json::Value =
            serde_json::from_str(&hops_view_body("nope", None, &[], false, false)).unwrap();
        assert_eq!(v["identity"], json!("nope"));
        assert!(v["tip"].is_null());
        assert_eq!(v["hops"], json!([]));
        assert_eq!(v["truncated"], json!(false));
    }

    // ── SQL structure pins ───────────────────────────────────────────────

    #[test]
    fn hops_view_sql_shape() {
        let sql = hops_view_sql(false);
        assert_eq!(sql.matches('?').count(), 1, "one identity bind");
        assert!(
            sql.contains("ROW_NUMBER() OVER (PARTITION BY hp.txid, hp.hopVout"),
            "the hop outpoint is (containing txid, hopVout)"
        );
        assert!(
            sql.contains(&format!("rn <= {HOPS_VIEW_ROWS_PER_OUTPOINT}")),
            "bounded SUPERSET per outpoint — never rn = 1 (verify-at-read)"
        );
        assert!(sql.contains(&format!("hopRank <= {HOPS_VIEW_UNKNOWN_HOP_QUOTA}")));
        assert!(
            sql.contains(&format!("finalRank <= {}", HOPS_VIEW_MAX_OUTPOINTS + 1)),
            "one outpoint beyond the page — the honest truncated bit"
        );
        // The verification facts are TYPED COLUMNS decoded at admission —
        // no `outputs` join, no topic dependency, no BLOB. POSITIVE counts,
        // so a rename or a dropped nesting level fails loudly rather than
        // vacuously. The numbers are the per-level occurrence counts (one
        // per SELECT level, plus the aliasing projection, plus — for
        // hopSatsOnChain — the paid-tier CASE and the two ORDER BYs);
        // re-derive them deliberately if the nesting changes, never by
        // pasting whatever the failure prints.
        assert_eq!(sql.matches("hopLockHex").count(), 9);
        assert_eq!(sql.matches("hopSatsOnChain").count(), 12);
        assert_eq!(sql.matches("containerOutputs").count(), 8);
        // The ranking leads on chain-settled facts, never on arrival.
        assert!(sql.contains("CASE WHEN hp.hopLockHex IS NOT NULL"));
        assert!(sql.contains("ORDER BY paidTier ASC, tier ASC, hopSatsOnChain DESC"));
        // #283a semantics: freshness-gated, OLDEST-first promotion.
        assert!(sql.contains(&format!(
            "unixepoch() - {HOPS_VIEW_UNKNOWN_HOP_MAX_AGE_SECS}"
        )));
        assert!(sql.contains("ORDER BY COALESCE(firstMarkerAt, 0) ASC"));
        let banned_join = ["tm_low", "fund'"].concat(); // split so it never matches itself
        assert!(
            !sql.contains(&banned_join),
            "the container supplies the hop facts — no outputs/tm_lowfund join"
        );
        // The shared confirmation bar's latch is fetched.
        assert!(sql.contains("sb.proof_verified AS spenderProofVerified"));
        // No BEEF blob is ever hauled (25-byte P2PKH scripts only).
        assert!(!sql.contains("hex(sb.beef)") && !sql.contains("hex(hb.beef)"));
    }
}
