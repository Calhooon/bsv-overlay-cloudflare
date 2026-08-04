//! `/hops-view` — the per-identity HOPS-IN-FLIGHT view (bsv-low #315, #252
//! stage 2b).
//!
//! The funding HOP (the staging P2PKH coin a seat pays to its own
//! `[2,'low settle']` key before the JOIN assembles the pot) previously had
//! NO identity-keyed server row: `tm_lowfund` indexes the bare outpoint and
//! `potparty_records` only fills at JOIN-assembly, so a seat that funded a
//! hop and died pre-JOIN was invisible to every per-identity view (the
//! #256 ~80.8k-sat class). Stage 2b adds the `LOW/hopparty/v1` marker
//! (published at hop time, indexed by `tm_hopparty` → `hopparty_records`;
//! the carrier-tx shape is the client half's decision — a tx cannot embed
//! its own txid, so the marker cannot ride the very tx whose outpoint it
//! names); this view joins those rows to the
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
//! UNVERIFIED claim until read time. Per the #315 hybrid, the marker is
//! chain-checkable: the hop output is `P2PKH(hash160(seatSettlePubkey))`,
//! so this view re-derives the P2PKH lock from the marker's own pubkey and
//! compares it against the hop output script the overlay ADMITTED (the
//! engine's `outputs` row for the `tm_lowfund` topic — hash-bound chain
//! bytes: the script at (txid, vout) is committed by the txid). On top of
//! the script bar, both signatures are verified with the `results.rs`
//! recipe: `seatSig` as plain ECDSA under `seatSettlePubkey` over
//! sha256(hopparty seatsig preimage), and `identitySig` with the BRC-42
//! 'anyone' round-trip under the marker's identity (protocol
//! `[1,'low potparty']` — the SAME constant `/results` verifies potparty
//! markers under; the version tag inside the challenge separates the
//! domains).
//!
//! Three-valued, `unknown` first-class:
//!
//! - `verified`   — both signatures verify AND the admitted hop lock
//!   equals `P2PKH(hash160(seatSettlePubkey))`;
//! - `unverified` — a signature fails, a field is malformed, or the
//!   admitted lock MISMATCHES the marker's pubkey;
//! - `unknown`    — signatures verify but no admitted hop script is
//!   available to check against (hop never indexed under `tm_lowfund`, or
//!   its engine `outputs` row was lifecycle-evicted after the spend), or
//!   the per-request verify budget ran out before this row
//!   (`verifyBudgetExhausted` in the body surfaces that case — Rule 13:
//!   an ambiguous signal is surfaced, never consumed as either verdict).
//!
//! Rows failing the filter are SERVED and labeled — filter-for-display,
//! never silently dropped, never an authority decision (the signatures
//! ride back in the body so the client re-verifies).
//!
//! ## The window (the #281 shape, SUPERSET per outpoint)
//!
//! `hopparty_records.identity` is attacker-writable (byte-format
//! admission), so the SQL reuses the identity-window discipline: `limit`
//! counts HOP OUTPOINTS ([`HOPS_VIEW_MAX_OUTPOINTS`]), a hop-existence
//! tier against `pot_records` demotes markers naming never-indexed hops
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

/// Per-request VERIFY budget (rows), spent in SERVED order — which the SQL
/// makes quality order: tier-0 (chain-indexed) hops come before every
/// never-indexed ghost. The bound exists because each row can cost up to
/// two ECDSA verifications plus a BRC-42 derivation on a public,
/// unauthenticated endpoint (the #314 MEDIUM-B read-time CPU class); the
/// cheap script pre-filter (one hash160 + string compare) runs FIRST and
/// refuses mismatched-lock rows without any curve work. Budget exhaustion
/// labels the remaining rows `unknown` and raises the body's
/// `verifyBudgetExhausted` bit (surfaced, never silently consumed —
/// Rule 13). 150 covers the full honest page (≤100 outpoints × 1 honest
/// marker) with headroom; only a flood can exhaust it.
pub const HOPS_VIEW_VERIFY_BUDGET: usize = 150;

const _: () = assert!(HOPS_VIEW_UNKNOWN_HOP_QUOTA < HOPS_VIEW_MAX_OUTPOINTS);
const _: () = assert!(HOPS_VIEW_VERIFY_BUDGET > HOPS_VIEW_MAX_OUTPOINTS);

/// The single `/hops-view` SQL (ONE bind: the lowercase identity). The
/// #281 window over the caller's `hopparty_records` rows LEFT-JOINed to
/// `pot_records` on the hop outpoint, then — OUTSIDE the window, on the
/// bounded survivors only — the spender's `pot_beefs.proof_verified` latch
/// (the shared #323 confirmation bar) and the ADMITTED hop lock script
/// (`outputs`, `tm_lowfund` topic, `hex(outputScript)` — 25 bytes for a
/// P2PKH, never a BEEF blob). `finalRank <= MAX+1` fetches one outpoint
/// beyond the page for the honest `truncated` bit; the LIMIT is a belt.
pub fn hops_view_sql() -> String {
    format!(
        "SELECT w.identity AS identity, w.gameId AS gameId, w.hopTxid AS hopTxid, \
                w.hopVout AS hopVout, w.hopSats AS hopSats, \
                w.opponentIdentity AS opponentIdentity, \
                w.seatSettlePubkey AS seatSettlePubkey, w.seatSigHex AS seatSigHex, \
                w.identitySigHex AS identitySigHex, w.markerTxid AS markerTxid, \
                w.spent AS spent, w.spendingTxid AS spendingTxid, \
                w.spentConfirmed AS spentConfirmed, \
                sb.proof_verified AS spenderProofVerified, \
                CASE WHEN o.outputScript IS NULL THEN NULL \
                     ELSE hex(o.outputScript) END AS hopLockHex \
         FROM (SELECT identity, gameId, hopTxid, hopVout, hopSats, opponentIdentity, \
                  seatSettlePubkey, seatSigHex, identitySigHex, markerTxid, \
                  spent, spendingTxid, spentConfirmed, \
                  markerCreatedAt, markerRowid, potCreatedAt, firstMarkerAt, tier \
           FROM (SELECT identity, gameId, hopTxid, hopVout, hopSats, opponentIdentity, \
                    seatSettlePubkey, seatSigHex, identitySigHex, markerTxid, \
                    spent, spendingTxid, spentConfirmed, \
                    markerCreatedAt, markerRowid, potCreatedAt, firstMarkerAt, tier, \
                    DENSE_RANK() OVER (ORDER BY tier ASC, \
                                                COALESCE(potCreatedAt, firstMarkerAt) DESC, \
                                                hopTxid ASC, hopVout ASC) AS finalRank \
           FROM (SELECT identity, gameId, hopTxid, hopVout, hopSats, opponentIdentity, \
                    seatSettlePubkey, seatSigHex, identitySigHex, markerTxid, \
                    spent, spendingTxid, spentConfirmed, \
                    markerCreatedAt, markerRowid, potCreatedAt, firstMarkerAt, unknownHop, \
                    CASE WHEN unknownHop = 0 OR hopRank <= {quota} THEN 0 ELSE 1 END AS tier \
             FROM (SELECT identity, gameId, hopTxid, hopVout, hopSats, opponentIdentity, \
                      seatSettlePubkey, seatSigHex, identitySigHex, markerTxid, \
                      spent, spendingTxid, spentConfirmed, \
                      markerCreatedAt, markerRowid, potCreatedAt, firstMarkerAt, unknownHop, \
                      DENSE_RANK() OVER (PARTITION BY unknownHop \
                                         ORDER BY COALESCE(potCreatedAt, firstMarkerAt) DESC, \
                                                  hopTxid ASC, hopVout ASC) AS hopRank \
               FROM (SELECT hp.identity AS identity, hp.gameId AS gameId, \
                        hp.hopTxid AS hopTxid, hp.hopVout AS hopVout, \
                        hp.hopSats AS hopSats, \
                        hp.opponentIdentity AS opponentIdentity, \
                        hp.seatSettlePubkey AS seatSettlePubkey, \
                        hp.seatSigHex AS seatSigHex, \
                        hp.identitySigHex AS identitySigHex, \
                        hp.txid AS markerTxid, \
                        r.spent AS spent, r.spendingTxid AS spendingTxid, \
                        r.spentConfirmed AS spentConfirmed, \
                        hp.createdAt AS markerCreatedAt, hp.rowid AS markerRowid, \
                        r.createdAt AS potCreatedAt, \
                        MIN(hp.createdAt) OVER (PARTITION BY hp.hopTxid, hp.hopVout) \
                            AS firstMarkerAt, \
                        CASE WHEN r.txid IS NULL THEN 1 ELSE 0 END AS unknownHop, \
                        ROW_NUMBER() OVER (PARTITION BY hp.hopTxid, hp.hopVout \
                                           ORDER BY hp.createdAt ASC, hp.rowid ASC) AS rn \
                 FROM hopparty_records hp \
                 LEFT JOIN pot_records r \
                        ON r.txid = hp.hopTxid AND r.outputIndex = hp.hopVout \
                 WHERE hp.identity = ?) \
               WHERE rn <= {per_outpoint}))) \
           WHERE finalRank <= {rank_cap} \
           ORDER BY tier ASC, COALESCE(potCreatedAt, firstMarkerAt) DESC, \
                    hopTxid ASC, hopVout ASC, markerCreatedAt ASC, markerRowid ASC \
           LIMIT {row_cap}) w \
         LEFT JOIN pot_beefs sb ON w.spendingTxid IS NOT NULL \
              AND sb.txid = lower(w.spendingTxid) \
         LEFT JOIN outputs o ON o.txid = w.hopTxid AND o.outputIndex = w.hopVout \
              AND o.topic = 'tm_lowfund' \
         ORDER BY w.tier ASC, COALESCE(w.potCreatedAt, w.firstMarkerAt) DESC, \
                  w.hopTxid ASC, w.hopVout ASC, w.markerCreatedAt ASC, w.markerRowid ASC",
        quota = HOPS_VIEW_UNKNOWN_HOP_QUOTA,
        per_outpoint = HOPS_VIEW_ROWS_PER_OUTPOINT,
        rank_cap = HOPS_VIEW_MAX_OUTPOINTS + 1,
        row_cap = (HOPS_VIEW_MAX_OUTPOINTS + 1) * HOPS_VIEW_ROWS_PER_OUTPOINT,
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
    /// The txid carrying the marker OP_RETURN.
    pub marker_txid: String,
    pub spent: Option<bool>,
    pub spending_txid: Option<String>,
    pub spent_confirmed: Option<bool>,
    /// `pot_beefs.proof_verified` for the recorded spender — the second
    /// accepted confirmation signal (#323 shared bar). `None` = no row.
    pub spender_proof_verified: Option<bool>,
    /// `hex(outputs.outputScript)` for the hop outpoint under the
    /// `tm_lowfund` topic — the ADMITTED (hash-bound) hop lock, when the
    /// engine still holds it. `None` ⇒ the script bar answers `unknown`.
    pub hop_lock_hex: Option<String>,
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
/// (identity, opponent, gameId, hopTxid, seatSettlePubkey).
type RowDigestParts = (Vec<u8>, Vec<u8>, [u8; 32], [u8; 32], Vec<u8>);

/// Decode the row's hex fields into the byte shapes the shared digest
/// builders take. `None` on any malformed field.
fn row_digest_parts(r: &HopsViewRow) -> Option<RowDigestParts> {
    let identity = hex::decode(r.identity.to_ascii_lowercase()).ok()?;
    let opponent = hex::decode(r.opponent_identity.to_ascii_lowercase()).ok()?;
    let game_id_v = hex::decode(r.game_id.to_ascii_lowercase()).ok()?;
    let hop_txid_v = hex::decode(r.hop_txid.to_ascii_lowercase()).ok()?;
    let settle_pk = hex::decode(r.seat_settle_pubkey.to_ascii_lowercase()).ok()?;
    if game_id_v.len() != 32 || hop_txid_v.len() != 32 {
        return None;
    }
    let mut game_id = [0u8; 32];
    game_id.copy_from_slice(&game_id_v);
    let mut hop_txid = [0u8; 32];
    hop_txid.copy_from_slice(&hop_txid_v);
    Some((identity, opponent, game_id, hop_txid, settle_pk))
}

/// Verify the marker's SEAT signature: plain secp256k1 ECDSA under the
/// marker's OWN `seatSettlePubkey` over sha256(hopparty seatsig preimage)
/// — the byte layout is the overlay's [`overlay_discovery::hopparty::
/// hopparty_seatsig_preimage`] (one source of truth across the crates).
/// Any malformed key/sig/field is `false` (refused).
pub fn verify_hop_seat_sig(r: &HopsViewRow) -> bool {
    let Some((identity, _, game_id, hop_txid, _)) = row_digest_parts(r) else {
        return false;
    };
    let Some(preimage) = overlay_discovery::hopparty::hopparty_seatsig_preimage(
        &game_id, &hop_txid, r.hop_vout, &identity,
    ) else {
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
    let Some((identity, opponent, game_id, hop_txid, settle_pk)) = row_digest_parts(r) else {
        return false;
    };
    let Some(challenge) = overlay_discovery::hopparty::hopparty_identity_challenge(
        &identity, &opponent, &game_id, &hop_txid, r.hop_vout, r.hop_sats, &settle_pk,
    ) else {
        return false;
    };
    crate::results::anyone_sig_verifies(
        &r.identity.to_ascii_lowercase(),
        &r.game_id.to_ascii_lowercase(),
        &challenge,
        &r.identity_sig_hex,
        crate::results::potparty_protocol(),
    )
}

/// The read-time validity FILTER for one row (module docs). Order is
/// cost-aware: the script bar (one hash160 + a compare) runs FIRST so a
/// mismatched lock refuses with zero curve work; the two signature bars
/// only run on rows the script bar did not already refuse.
pub fn derive_marker_verification(r: &HopsViewRow) -> MarkerVerification {
    let Some(expected) = expected_hop_lock_hex(&r.seat_settle_pubkey) else {
        return MarkerVerification::Unverified; // malformed pubkey
    };
    if let Some(lock) = &r.hop_lock_hex {
        if !lock.eq_ignore_ascii_case(&expected) {
            // The ADMITTED hop lock does not pay the marker's key —
            // definite mismatch, refused before any ECDSA.
            return MarkerVerification::Unverified;
        }
    }
    if !verify_hop_seat_sig(r) {
        return MarkerVerification::Unverified;
    }
    if !verify_hop_identity_binding(r) {
        return MarkerVerification::Unverified;
    }
    match r.hop_lock_hex {
        // Signatures AND the admitted-lock match: fully verified.
        Some(_) => MarkerVerification::Verified,
        // Signatures hold but no admitted script exists to check against —
        // `unknown`, never asserted verified (and never failed).
        None => MarkerVerification::Unknown,
    }
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
            if crate::logic::is_confirmed_landing_with_proof(spent_confirmed, spender_proof_verified)
            {
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
    pub marker_txid: String,
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
/// labeled `unknown` without being checked). Order is preserved (the SQL
/// already returns tiered-newest-outpoint-first). Pure — every branch is
/// unit-testable without D1.
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
    let entries = rows
        .into_iter()
        .filter(|r| outpoints.contains(&(r.hop_txid.to_ascii_lowercase(), r.hop_vout)))
        .map(|r| {
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
                spent: r.spent,
                spending_txid: r.spending_txid,
                spent_confirmed: r.spent_confirmed,
                status,
                status_source,
                marker_verified,
            }
        })
        .collect();
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
        Counterparty, CreateSignatureArgs, GetPublicKeyArgs, Protocol, ProtoWallet, SecurityLevel,
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

        let preimage = overlay_discovery::hopparty::hopparty_seatsig_preimage(
            &game_id, &hop_txid, hop_vout, &identity,
        )
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
            &identity, &opponent, &game_id, &hop_txid, hop_vout, hop_sats, &settle_pk,
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
            marker_txid: h64(0xc1),
            spent: Some(false),
            spending_txid: None,
            spent_confirmed: Some(false),
            spender_proof_verified: None,
            // The admitted hop lock MATCHES the settle pubkey by default.
            hop_lock_hex: expected_hop_lock_hex(&settle_pub_hex),
        }
    }

    // ── markerVerified derivation matrix ─────────────────────────────────

    #[test]
    fn real_marker_with_matching_admitted_lock_is_verified() {
        // The positive control: both real sigs + a matching admitted lock
        // reach and clear every bar.
        let r = real_row(0xa1, 0xaa);
        assert!(verify_hop_seat_sig(&r));
        assert!(verify_hop_identity_binding(&r));
        assert_eq!(derive_marker_verification(&r), MarkerVerification::Verified);
    }

    #[test]
    fn absent_admitted_lock_is_unknown_not_failed() {
        let mut r = real_row(0xa1, 0xaa);
        r.hop_lock_hex = None;
        assert_eq!(derive_marker_verification(&r), MarkerVerification::Unknown);
    }

    #[test]
    fn mismatched_admitted_lock_is_unverified() {
        let mut r = real_row(0xa1, 0xaa);
        // The chain says the hop pays a DIFFERENT key than the marker
        // claims — refused (this is the pre-filter: no ECDSA needed).
        r.hop_lock_hex = Some(format!("76a914{}88ac", "aa".repeat(20)));
        assert_eq!(
            derive_marker_verification(&r),
            MarkerVerification::Unverified
        );
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
        // Even with the admitted lock ABSENT, junk sigs are failed, never
        // laundered into `unknown`.
        r.hop_lock_hex = None;
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
        // The hop outpoint is bound by BOTH.
        let mut r = real_row(0xa1, 0xaa);
        r.hop_txid = h64(0xdd);
        assert!(!verify_hop_seat_sig(&r));
        assert!(!verify_hop_identity_binding(&r));
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
            |r: &mut HopsViewRow| r.hop_txid = "gg".repeat(32),
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
            overlay_discovery::hopparty::hopparty_seatsig_preimage(
                &game_id,
                &hex::decode(&r.hop_txid).unwrap().try_into().unwrap(),
                r.hop_vout,
                &identity,
            )
            .unwrap();
        // Swap the 24-byte domain prefix for potparty-v2's.
        potparty_pre[..24].copy_from_slice(b"LOW/potparty/v2/seatsig|");
        let opponent_pub = bsv_rs::primitives::ec::PublicKey::from_hex(&r.opponent_identity)
            .unwrap();
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
        assert_eq!(derive_hop_status(None, None, None), (HopStatus::Unknown, None));
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
        let (entries, truncated, _) =
            assemble_hops_view(rows[..HOPS_VIEW_MAX_OUTPOINTS].to_vec());
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
        // last row is labeled unknown WITHOUT being checked, and the bit is
        // raised. (Rows share outpoints so the page bound doesn't cut them.)
        let mut rows: Vec<HopsViewRow> = Vec::new();
        for i in 0..HOPS_VIEW_MAX_OUTPOINTS {
            for _ in 0..2 {
                let mut r = real_row(0xa1, 0xaa);
                r.hop_txid = format!("{i:064x}");
                // Junk sig so a CHECKED row is Unverified — distinguishing
                // checked (unverified) from unchecked (unknown).
                r.seat_sig_hex = format!("3045{}", "ab".repeat(69));
                r.hop_lock_hex = None;
                rows.push(r);
            }
        }
        assert!(rows.len() > HOPS_VIEW_VERIFY_BUDGET);
        let (entries, _, exhausted) = assemble_hops_view(rows);
        assert!(exhausted, "the budget bit must be surfaced");
        assert_eq!(
            entries[..HOPS_VIEW_VERIFY_BUDGET]
                .iter()
                .filter(|e| e.marker_verified == MarkerVerification::Unverified)
                .count(),
            HOPS_VIEW_VERIFY_BUDGET,
            "in-budget rows were genuinely checked"
        );
        assert!(
            entries[HOPS_VIEW_VERIFY_BUDGET..]
                .iter()
                .all(|e| e.marker_verified == MarkerVerification::Unknown),
            "past-budget rows are unknown (we could not look), never a verdict"
        );
        // The honest page never exhausts the budget.
        let (_, _, exhausted) = assemble_hops_view(vec![real_row(0xa1, 0xaa)]);
        assert!(!exhausted);
    }

    #[test]
    fn body_shape_and_labels() {
        let verified = real_row(0xa1, 0xaa);
        let mut junk = real_row(0xa1, 0xbb);
        junk.seat_sig_hex = format!("3045{}", "ab".repeat(69));
        junk.spent = None;
        junk.spent_confirmed = None;
        junk.hop_lock_hex = None;
        let (entries, truncated, exhausted) = assemble_hops_view(vec![verified.clone(), junk]);
        let body = hops_view_body(&verified.identity, Some(960_000), &entries, truncated, exhausted);
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
        let sql = hops_view_sql();
        assert_eq!(sql.matches('?').count(), 1, "one identity bind");
        assert!(sql.contains("ROW_NUMBER() OVER (PARTITION BY hp.hopTxid, hp.hopVout"));
        assert!(
            sql.contains(&format!("rn <= {HOPS_VIEW_ROWS_PER_OUTPOINT}")),
            "bounded SUPERSET per outpoint — never rn = 1 (verify-at-read)"
        );
        assert!(sql.contains(&format!("hopRank <= {HOPS_VIEW_UNKNOWN_HOP_QUOTA}")));
        assert!(
            sql.contains(&format!("finalRank <= {}", HOPS_VIEW_MAX_OUTPOINTS + 1)),
            "one outpoint beyond the page — the honest truncated bit"
        );
        // The admitted-lock join is topic-scoped and OUTSIDE the window.
        assert!(sql.contains("o.topic = 'tm_lowfund'"));
        // hex(NULL) is '' in SQLite (not NULL) — the CASE keeps a missed
        // join distinguishable from an empty script (unknown, never a
        // mismatch).
        assert!(sql.contains(
            "CASE WHEN o.outputScript IS NULL THEN NULL \
                     ELSE hex(o.outputScript) END AS hopLockHex"
        ));
        // The shared confirmation bar's latch is fetched.
        assert!(sql.contains("sb.proof_verified AS spenderProofVerified"));
        // No BEEF blob is ever hauled (25-byte P2PKH scripts only).
        assert!(!sql.contains("hex(sb.beef)") && !sql.contains("hex(hb.beef)"));
    }
}
