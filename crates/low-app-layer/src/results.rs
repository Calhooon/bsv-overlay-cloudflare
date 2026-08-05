//! Server-derived pot RESULTS — chain-truth settle classification (bsv-low
//! #227, campaign #219).
//!
//! ## Why the server can derive results at all — the trust model
//!
//! Since bsv-low #103 the live pot lock is the `Poc5TemplatePot` COVENANT: a
//! 2-of-3 settle-key multisig whose locking script ALSO commits the payout
//! parameters (`payPkhA/payPkhB/rakePkh/stakeA/stakeB/feeSats/
//! recoveryHeight`) and consensus-mandates that ANY spend pay one of four
//! output templates derived in-script from those params:
//!
//!   T_a      `[rake → rakePkh (omit if 0), pot − fee − rake → payPkhA]`
//!   T_b      `[rake → rakePkh (omit if 0), pot − fee − rake → payPkhB]`
//!   T_tie    `[rake' → rakePkh (omit if 0), half → payPkhA, half → payPkhB]`
//!   T_refund `[stakeA − (fee − fee/2) → payPkhA, stakeB − fee/2 → payPkhB]`
//!            (height-gated: nLockTime ≥ recoveryHeight + non-final sequence)
//!
//! with `rake = floor(pot / 100)` (bsv-low #102) and the tie's odd sat joining
//! the rake. The math here is byte-identical to the authoritative pair
//! `pot.ts::settleOutputs` ≡ `cosign.rs::mandated_outputs` and
//! `settle.rs::refund_outputs` (bsv-low `crates/low-spend/src/tower_settle.rs`
//! `template_settle_outputs` / `template_refund_outputs`).
//!
//! THEREFORE: a mined spend of a covenant pot is **co-signed by construction**
//! (the covenant only validates under two of the three settle keys) and can
//! only pay a mandated shape — so matching the spend's outputs against the
//! four templates derived from the pot's OWN committed params is a
//! chain-truth verdict of WHICH exit fired, requiring no client claim at all.
//! The committed params are read out of the funding lock itself (the exact
//! bytes the pot's money sat under), and both raw txs are HASH-VERIFIED
//! against their txids before anything is derived — a lying/garbled store row
//! degrades to `unresolved`, never to a wrong verdict.
//!
//! ## Conservatism — the leaderboard bar is verified-only
//!
//! A verdict is emitted ONLY when the classification is unambiguous:
//! - the spender must actually spend the pot outpoint (input match);
//! - the outputs must equal EXACTLY ONE template (value + script, in order);
//! - the refund template additionally requires its in-script height gate
//!   observed on the wire (`nLockTime ≥` the committed `recoveryHeight`,
//!   block-height semantics, non-final sequence) — except the known T_tie ==
//!   T_refund byte-collision (rakeless equal-stakes pot, unreachable at prod
//!   stakes), where the covenant itself waives the gate and the wire
//!   locktime/sequence picks the label (money-identical either way);
//! - a degenerate pot committing `payPkhA == payPkhB` can never distinguish
//!   winner-A from winner-B → no verdict.
//!
//! Anything else — a bare-era (pre-covenant) lock, a missing/garbled BEEF, a
//! non-matching output set — is `None` (unresolved), NEVER a guessed win.
//!
//! Bare (pre-covenant) 2-of-3 pots get ONE conservative classification:
//! the pre-signed nLockTime REFUND (2 P2PKH outputs, `nLockTime` equal to the
//! `ls_potparty` marker's `recoveryHeight`, non-final sequence, conservation
//! ≤ pot). A refund is money-neutral for the leaderboard (it never counts as
//! a win), so a hostile marker's fake `recoveryHeight` cannot mint a win —
//! at worst it mislabels an exit the legacy claim path already covers.
//! Bare-pot WINNER/TIE shapes are NOT classified (a bare 2-of-3 can pay any
//! outputs, so shape alone proves nothing) — legacy client claims keep
//! covering those games.
//!
//! ## Seat → identity (what is and is NOT derivable server-side)
//!
//! The covenant commits SETTLE keys (`[2,'low settle']`, BRC-42 derived with
//! `counterparty = opponent` — an ECDH the server cannot perform) and payout
//! P2PKH homes (BRC-29 payment derivations, `counterparty = self`). The
//! `ls_potparty` markers carry each seat's IDENTITY key but no seat letter,
//! no settle key, and no pay home. So the server CANNOT map "winner-A" to an
//! identity from indexed data alone:
//! - `tie` / `refund` are seat-symmetric → BOTH identities get the outcome
//!   (pure chain truth, no claim involved);
//! - `winner-a` / `winner-b` are exposed verbatim (a PARTICIPANT knows its
//!   own seat locally and derives won/lost client-side), and the per-identity
//!   `outcome` upgrades to `won`/`lost` when either
//!   * (#230, strongest) the caller's `LOW/potparty/v2` SEAT-BINDING marker
//!     proves which committed lock key it holds — the settle pubkey was
//!     committed in the covenant lock at FUNDING, before the outcome, and
//!     the marker's `seatSig` is BY that key, so the seat → identity map is
//!     unforgeable, un-back-datable, and needs NO countersignature
//!     (`outcomeSource = "chain+seatkey"`); or
//!   * every on-record `tm_result` claim for the game agrees on one winner
//!     among the two parties AND names the chain-classified settle —
//!     claim-corroborated chain truth, never a bare claim
//!     (`outcomeSource = "chain+claim"`).
//!
//! ## Claim signatures are VERIFIED server-side before they corroborate
//!
//! `tm_result` admits markers by BYTE FORMAT only — anyone can publish a
//! marker naming any winner/loser/settle. So before a claim participates in
//! won/lost attribution its signatures are re-verified HERE, with the exact
//! recipe the client's `result.ts::verifyResultRow` uses: BRC-42/43
//! 'anyone'-key verification (`ProtoWallet::anyone()`, protocol
//! `[1, 'low result']`, keyID = gameId) — the winner's sig under the WINNER
//! identity over the canonical result challenge, the loser's countersig
//! under the LOSER identity over the same bytes. A claim whose winner sig
//! does not verify contributes NOTHING (as if never published); a
//! present-but-unverifiable countersig degrades to "no countersig" (the
//! client's `unconfirmed` demotion). The outcome tiers stay honest:
//! `won` needs the winner's VERIFIED sig; `lost` needs the loser's (the
//! caller's own) VERIFIED countersig — so a fabricated marker naming the
//! real settle txid can never flip the reported winner when the honest side
//! never published (adversarial-review finding, 2026-07-22).

use serde_json::json;

use crate::logic::ResultMarkerRow;

// ── the covenant decoder/classifier now lives in the OVERLAY (bsv-low #284) ─
//
// The pure param decoder + spend-template classifier moved down to
// `overlay-discovery::pot::covenant` so the overlay can decode covenant
// params ONCE at admission and classify a spend verdict ONCE at
// spend-detection (both pure functions of hash-bound chain bytes). This
// re-export keeps every existing consumer — this module, `routes.rs`, the
// golden-vector tests in `tests/classifier_real_txs.rs` — compiling with
// UNCHANGED call sites. What did NOT move: `classify_pot_spend` (the
// hash-verifying orchestration), `classify_bare_refund` (depends on an
// UNVERIFIED marker hint — app-layer-only), `parse_raw_tx_verified`, and
// everything touching markers/signatures. The verifying reader stays here.
pub use overlay_discovery::pot::covenant::{
    classify_covenant, covenant_params_from_hex, extract_covenant_params, is_bare_2of3_lock,
    p2pkh_lock, CovenantParams, PotVerdict, RawInput, RawTx, LOCKTIME_THRESHOLD,
    TEMPLATE_RAKE_DIVISOR,
};

/// Parse raw tx bytes via `bsv_rs` and require the bytes HASH to
/// `expected_txid` — a garbled or substituted store row must degrade to
/// `None` (→ unresolved), never feed the classifier wrong bytes. An output
/// with no satoshi amount (impossible on a mined tx, but the type allows it)
/// also refuses.
pub fn parse_raw_tx_verified(raw: &[u8], expected_txid: &str) -> Option<RawTx> {
    let tx = bsv_rs::transaction::Transaction::from_binary(raw).ok()?;
    if !tx.id().eq_ignore_ascii_case(expected_txid) {
        return None;
    }
    RawTx::from_transaction(&tx)
}

/// Everything the classifier consumes for one pot spend. All txids lowercase
/// hex; the raws are hash-verified inside (a mismatch → unresolved).
pub struct PotSpendFacts<'a> {
    /// The pot funding txid + the pot output index within it.
    pub pot_txid: &'a str,
    pub pot_vout: u32,
    /// The funding tx raw bytes (must hash to `pot_txid`).
    pub funding_raw: &'a [u8],
    /// The recorded spender's txid + raw bytes (must hash to it).
    pub spender_txid: &'a str,
    pub spender_raw: &'a [u8],
    /// The `ls_potparty` marker's recoveryHeight — an UNVERIFIED hint, used
    /// ONLY for the bare-pot refund rule (money-neutral; see module note).
    pub marker_recovery_height: Option<u32>,
}

/// Classify one pot spend. `None` = unresolved (never a guess).
///
/// See the module docs for the full rule set + conservatism rationale.
pub fn classify_pot_spend(f: &PotSpendFacts) -> Option<PotVerdict> {
    let funding = parse_raw_tx_verified(f.funding_raw, f.pot_txid)?;
    let spender = parse_raw_tx_verified(f.spender_raw, f.spender_txid)?;

    // The recorded spender must ACTUALLY spend the pot outpoint.
    let pot_input = spender
        .inputs
        .iter()
        .find(|i| i.prev_txid.eq_ignore_ascii_case(f.pot_txid) && i.prev_vout == f.pot_vout)?;

    let (pot_sats, pot_lock) = spender_pot_prevout(&funding, f.pot_vout)?;

    if let Some(p) = extract_covenant_params(&pot_lock) {
        // The covenant asserts `ctx.utxo.value == stakeA + stakeB` in-script;
        // a funding output that disagrees is not the pot the params describe.
        if p.stake_a.checked_add(p.stake_b)? != pot_sats {
            return None;
        }
        return classify_covenant(&p, &spender, pot_input.sequence);
    }

    if is_bare_2of3_lock(&pot_lock) {
        return classify_bare_refund(
            &spender,
            pot_input.sequence,
            pot_sats,
            f.marker_recovery_height,
        );
    }

    None // unknown lock shape — never classified
}

/// The pot prevout `(satoshis, lock)` from the parsed funding tx.
fn spender_pot_prevout(funding: &RawTx, vout: u32) -> Option<(u64, Vec<u8>)> {
    let (sats, lock) = funding.outputs.get(vout as usize)?;
    Some((*sats, lock.clone()))
}

/// #284 read-time fallback for a row whose DECODED params are present but
/// whose stored verdict is stale/absent: classify the spend from the column
/// params + the hash-verified spender BEEF — no funding BLOB required. The
/// same bars as [`classify_pot_spend`], resourced:
/// - the spender raw must HASH to the recorded `spending_txid` and actually
///   spend the pot outpoint (input match — its sequence feeds the refund
///   height gate);
/// - stake conservation against the stored funding value (`stakeA + stakeB
///   == potSats` — the covenant's own in-script assertion); a missing
///   `potSats` refuses (never classify without the conservation anchor).
fn classify_from_columns(
    params: &CovenantParams,
    pot_sats: Option<u64>,
    pot_txid_lc: &str,
    pot_vout: u32,
    settle_lc: &str,
    spender_beef_hex: &str,
) -> Option<PotVerdict> {
    if params.stake_a.checked_add(params.stake_b)? != pot_sats? {
        return None; // conservation failed — the params do not describe this pot
    }
    let sb = crate::logic::decode_beef_hex(spender_beef_hex)?;
    let sraw =
        crate::logic::extract_raw_tx_hex(&sb, settle_lc).and_then(|h| hex::decode(h).ok())?;
    let spender = parse_raw_tx_verified(&sraw, settle_lc)?;
    let pot_input = spender
        .inputs
        .iter()
        .find(|i| i.prev_txid.eq_ignore_ascii_case(pot_txid_lc) && i.prev_vout == pot_vout)?;
    classify_covenant(params, &spender, pot_input.sequence)
}

/// Bare-era (pre-covenant) pots: classify ONLY the pre-signed nLockTime
/// refund (see module note — winner/tie shapes prove nothing on a bare
/// 2-of-3). Requires the potparty marker's recoveryHeight to EQUAL the wire
/// nLockTime (the pre-signed refund sets it exactly), non-final sequence,
/// exactly 2 P2PKH outputs, and conservation (outputs ≤ pot).
fn classify_bare_refund(
    spender: &RawTx,
    pot_sequence: u32,
    pot_sats: u64,
    marker_recovery_height: Option<u32>,
) -> Option<PotVerdict> {
    // The SAME usable-block-height rule every serving surface applies —
    // shared, not re-spelled. This site held an independent inline copy
    // (`h == 0 || h >= LOCKTIME_THRESHOLD`, the exact negation) in a MONEY
    // CLASSIFICATION path while the commit that extracted the predicate
    // claimed the file now shared it (Rule 10; the claim's own grep was
    // region-scoped to a file that structurally could not hold the leftover).
    let h = marker_recovery_height?;
    crate::refund_view::valid_recovery_height(u64::from(h))?;
    if spender.lock_time != h || pot_sequence == 0xffff_ffff {
        return None;
    }
    if spender.outputs.len() != 2 {
        return None;
    }
    let mut sum: u64 = 0;
    for (sats, script) in &spender.outputs {
        if !is_p2pkh(script) {
            return None;
        }
        sum = sum.checked_add(*sats)?;
    }
    if sum > pot_sats {
        return None;
    }
    Some(PotVerdict::Refund)
}

/// Standard 25-byte P2PKH lock check (same shape `tm_lowfund` recognizes).
fn is_p2pkh(s: &[u8]) -> bool {
    s.len() == 25 && s[0] == 0x76 && s[1] == 0xa9 && s[2] == 0x14 && s[23] == 0x88 && s[24] == 0xac
}

/// The mined block height of `txid` per its stored BEEF's BUMP. `None` when
/// unproven/unknown — a missing height is presented as `null`, never guessed.
///
/// TRUST WARNING (bsv-low#304): this is a STRUCTURAL read of the stored
/// bytes — it does zero SPV. A bump admitted via the ungated overlay paths
/// (historical-tx / GASP sync / peer crawl) can be attacker-fabricated, so
/// serving surfaces MUST gate this on the row's VERIFIED latch
/// (`transactions.has_proof` / `pot_beefs.proof_verified`) — use
/// [`verified_beef_block_height`].
pub fn beef_block_height(beef_bytes: &[u8], txid: &str) -> Option<u64> {
    let beef = bsv_rs::transaction::Beef::from_binary(beef_bytes).ok()?;
    let btx = beef.find_txid(&txid.to_ascii_lowercase())?;
    let bump = beef.bumps.get(btx.bump_index()?)?;
    Some(u64::from(bump.block_height))
}

/// PURE (bsv-low#304): the height a read surface may SERVE from a stored
/// BEEF — [`beef_block_height`] gated on the row's VERIFIED proof latch.
/// `proof_verified = false` answers `None` regardless of what the bytes
/// structurally claim: a fake-bumped row admitted via the ungated paths
/// answers like a bumpless row (presence/confirmation defer to the external
/// leg — slower, honest), NEVER an attacker-chosen confirmed/height. The
/// fail direction only ever WEAKENS an unverified answer; a verified row's
/// answer is unchanged.
pub fn verified_beef_block_height(
    beef_bytes: &[u8],
    txid: &str,
    proof_verified: bool,
) -> Option<u64> {
    if !proof_verified {
        return None;
    }
    beef_block_height(beef_bytes, txid)
}

// ── #230 potparty-v2 SEAT ATTRIBUTION (the countersignature-free winner map) ─
//
// A `LOW/potparty/v2` marker carries `seatSettlePubkey` — the seat's
// `[2,'low settle']` pubkey, the EXACT key the covenant lock committed as
// `pubA`/`pubB` at FUNDING time, before the outcome was known — plus
// `seatSig`, a signature BY that settle key binding {gameId, potOutpoint,
// identity}. Verifying the sig and matching the pubkey against the pot's OWN
// committed lock keys yields seat → identity: unforgeable (only the key
// holder can sign), un-back-datable (the key predates the outcome), and
// slot-exact ("both seats claim seat A" is structurally impossible — the
// bytes decide, not the claim). Joined to the chain verdict, `winner-a` +
// "identity X held pubA" ⇒ X won — with NO countersignature and no claim.

/// The frozen seatSig domain tag — MUST equal the client's
/// `potParty.ts::POTPARTY_SEATSIG_DOMAIN` byte-for-byte.
///
/// Re-exported from `overlay-discovery`, which is where the potparty
/// signature rules now live so the OVERLAY's admission-time `sigValid` latch
/// (bsv-low #283) and THIS crate's read-time bars are literally the same
/// code. Two copies of a signature rule across a crate boundary is a
/// boundary with no pin (epoch Rule 16), and a drift between them would let
/// the latch rank an honest marker last.
pub use overlay_discovery::potparty::validity::POTPARTY_SEATSIG_DOMAIN;

/// One raw v2 potparty marker row (from `potparty_records`), NOT yet
/// verified — [`verify_seat_marker`] + [`verify_identity_binding`] (both
/// enforced by [`attribute_seats`]) decide whether it attributes anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeatMarkerRow {
    /// The publishing identity (66-hex compressed pubkey).
    pub identity: String,
    /// The opponent identity named by the marker (66-hex) — part of the
    /// identity-signature challenge.
    pub opponent_identity: String,
    /// The marker's gameId (64-hex).
    pub game_id: String,
    /// The pot outpoint the marker names.
    pub pot_txid: String,
    pub pot_vout: u32,
    /// The marker's recoveryHeight — part of the identity-sig challenge.
    pub recovery_height: u32,
    /// The claimed settle pubkey (66-hex) — matched against the lock.
    pub seat_settle_pubkey: String,
    /// The settle key's DER signature over the seat-binding preimage.
    pub seat_sig_hex: String,
    /// The IDENTITY's DER signature over the v2 marker challenge
    /// (`[1,'low potparty']`, keyID = gameId, counterparty 'anyone') — the
    /// proof that `identity` itself published this marker. WITHOUT verifying
    /// it, a hostile counterparty could mint a marker naming the VICTIM's
    /// identity over its OWN settle key (its wallet happily seat-signs a
    /// preimage embedding any identity), landing the victim in BOTH slots
    /// and erasing the victim's `my_seat` — the 2026-07-28 gate's F1.
    pub identity_sig_hex: String,
}

/// The EXACT cross-repo seatSig preimage (bsv-low #230; the client's
/// `potPartySeatSigPreimage`):
///
///   `"LOW/potparty/v2/seatsig|" ‖ gameId(32) ‖ potTxid(32) ‖ potVout(4 LE)
///    ‖ identity(33)`
///
/// `seatSig` is ECDSA over a SINGLE sha256 of these bytes (the BRC-100
/// `createSignature({data})` hash). `None` when any hex field is malformed —
/// an unbuildable preimage can never verify (fail-safe).
/// Hex wrapper over the shared `overlay-discovery` preimage builder — this
/// crate holds hex strings, the shared rule holds bytes.
pub fn seatsig_preimage(
    game_id_hex: &str,
    pot_txid_hex: &str,
    pot_vout: u32,
    identity_hex: &str,
) -> Option<Vec<u8>> {
    overlay_discovery::potparty::validity::seatsig_preimage(
        &hex::decode(game_id_hex).ok()?,
        &hex::decode(pot_txid_hex).ok()?,
        pot_vout,
        &hex::decode(identity_hex).ok()?,
    )
}

/// Verify one v2 marker's SEAT signature: plain secp256k1 ECDSA under the
/// marker's OWN `seatSettlePubkey` over sha256(preimage). No BRC-42
/// derivation is needed (or possible) server-side — the settle key attests
/// its own identity binding, and lock membership is checked separately by
/// [`attribute_seats`]. Any malformed key/sig/field is `false` (refused).
/// CANONICAL STRICT DER (Rule 4c) is enforced inside the shared rule:
/// `from_der` tolerates trailing bytes, so the encoding is re-derived and
/// byte-compared. Without it an observer mints unlimited distinct "valid"
/// rows from one honest marker by padding it — which since #283 would be a
/// way to fill a window with `sigValid = 1` rows.
/// `der_padding_is_refused_canonical_strict` pins it here;
/// `der_padded_seat_signature_does_not_latch_valid` pins it at the latch.
pub fn verify_seat_marker(m: &SeatMarkerRow) -> bool {
    let (Ok(game_id), Ok(pot_txid), Ok(identity), Ok(settle_pk), Ok(seat_sig)) = (
        hex::decode(m.game_id.to_ascii_lowercase()),
        hex::decode(m.pot_txid.to_ascii_lowercase()),
        hex::decode(m.identity.to_ascii_lowercase()),
        hex::decode(m.seat_settle_pubkey.to_ascii_lowercase()),
        hex::decode(m.seat_sig_hex.to_ascii_lowercase()),
    ) else {
        return false;
    };
    overlay_discovery::potparty::validity::verify_seat_sig(
        &game_id, &pot_txid, m.pot_vout, &identity, &settle_pk, &seat_sig,
    )
}

/// The BRC-43 protocol potparty markers sign their IDENTITY challenge under
/// — `potParty.ts::POTPARTY_PROTOCOL` = `[1, 'low potparty']`.
///
/// `pub` (not `pub(crate)`) for two reasons that both reduce to Rule 16 —
/// share the constant, never the convention:
/// 1. producer parity, as for [`result_protocol`]: the `results_window_sqlite`
///    integration fixtures must MINT genuinely verifiable v2 markers with the
///    exact protocol the verifier checks, rather than re-deriving the tuple.
/// 2. bsv-low #315: hopparty identity signatures REUSE this wallet protocol id
///    by the ledgered decision-3 (the version tag inside the challenge is the
///    domain separator), so `/hops-view` verifies under the SAME constant
///    rather than a second spelling that could drift.
pub use overlay_discovery::potparty::validity::potparty_protocol;

/// The EXACT v2 IDENTITY-signature challenge — byte-identical to the
/// client's `potPartyV2Challenge`: the v2 tag + every field (incl.
/// `seatSettlePubkey`), all raw bytes, u32s little-endian. `None` when any
/// hex field is malformed (an unbuildable challenge can never verify).
pub fn potparty_v2_challenge(m: &SeatMarkerRow) -> Option<Vec<u8>> {
    overlay_discovery::potparty::validity::potparty_v2_challenge(
        &hex::decode(m.identity.to_ascii_lowercase()).ok()?,
        &hex::decode(m.opponent_identity.to_ascii_lowercase()).ok()?,
        &hex::decode(m.game_id.to_ascii_lowercase()).ok()?,
        &hex::decode(m.pot_txid.to_ascii_lowercase()).ok()?,
        m.pot_vout,
        m.recovery_height,
        &hex::decode(m.seat_settle_pubkey.to_ascii_lowercase()).ok()?,
    )
}

/// Verify the marker's IDENTITY signature: the claimed `identity` really
/// published this marker (BRC-42/43 'anyone' verification, protocol
/// `[1,'low potparty']`, keyID = gameId — the exact recipe of the client's
/// `verifyPotPartyV2Marker`). This is the F1 bar: `seatSig` alone proves
/// "SOME settle key signed this identity into the preimage" — which the
/// OPPONENT's wallet will happily do with its own key — so a marker may
/// attribute a slot only when the named identity's own signature is on it.
pub fn verify_identity_binding(m: &SeatMarkerRow) -> bool {
    let Some(challenge) = potparty_v2_challenge(m) else {
        return false;
    };
    anyone_sig_verifies(
        &m.identity.to_ascii_lowercase(),
        &m.game_id.to_ascii_lowercase(),
        &challenge,
        &m.identity_sig_hex,
        potparty_protocol(),
    )
}

/// Which identity (if provably any) holds each committed settle-key slot of
/// one pot. `None` = unattributed (no verified marker for the slot, or a
/// conflict — never a guess).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SeatAttribution {
    /// The identity that PROVED holding `pubA` (the seat-A settle key).
    pub identity_a: Option<String>,
    /// The identity that PROVED holding `pubB`.
    pub identity_b: Option<String>,
}

impl SeatAttribution {
    /// The attributed winner identity for a chain verdict, when the winning
    /// seat's slot is attributed (tie/refund/None attribute nobody).
    pub fn winner_for(&self, verdict: PotVerdict) -> Option<&str> {
        match verdict {
            PotVerdict::WinnerA => self.identity_a.as_deref(),
            PotVerdict::WinnerB => self.identity_b.as_deref(),
            PotVerdict::Tie | PotVerdict::Refund => None,
        }
    }
}

/// Fold v2 markers against the pot's COMMITTED lock keys (bsv-low #230 —
/// the risk-register bars, all refusals silent):
///
/// - a marker whose `seatSettlePubkey` is NOT `pubA`/`pubB` is REFUSED (the
///   lock is the authority; a foreign key attributes nothing);
/// - a marker whose `seatSig` does not verify is REFUSED;
/// - a marker whose IDENTITY signature does not verify is REFUSED (F1: the
///   opponent can mint a valid seatSig over the VICTIM's identity with its
///   own settle key — only the victim can mint the identity sig, so a
///   hostile marker can never occupy a slot with someone else's identity,
///   and in particular can never erase `my_seat` via the both-slots case);
/// - a marker naming a different (gameId-irrelevant) pot outpoint than the
///   one being attributed is REFUSED (the preimage binds the outpoint);
/// - each key matches only its OWN slot, so both seats claiming "seat A" is
///   impossible by construction — and CONFLICTING identities for one slot
///   (two verified markers, different identities, same key — only the key
///   holder can mint BOTH sig pairs, i.e. self-sabotage) poison that slot
///   to `None`;
/// - a degenerate lock with `pubA == pubB` attributes NOTHING (one key
///   cannot distinguish the seats);
/// - duplicate identical markers (the outpoint-replay case) are idempotent.
pub fn attribute_seats(
    params: &CovenantParams,
    pot_txid_lc: &str,
    pot_vout: u32,
    markers: &[SeatMarkerRow],
) -> SeatAttribution {
    let mut out = SeatAttribution::default();
    if params.pub_a == params.pub_b {
        return out; // degenerate lock — seats indistinguishable
    }
    let pub_a_hex = hex::encode(params.pub_a);
    let pub_b_hex = hex::encode(params.pub_b);
    // Some(None) = slot poisoned by conflict; None = untouched.
    let mut slot_a: Option<Option<String>> = None;
    let mut slot_b: Option<Option<String>> = None;
    for m in markers {
        if !m.pot_txid.eq_ignore_ascii_case(pot_txid_lc) || m.pot_vout != pot_vout {
            continue; // a marker for a different pot never attributes this one
        }
        let pk = m.seat_settle_pubkey.to_ascii_lowercase();
        let slot = if pk == pub_a_hex {
            &mut slot_a
        } else if pk == pub_b_hex {
            &mut slot_b
        } else {
            continue; // key not committed in this pot's lock — refused
        };
        if !verify_seat_marker(m) {
            continue; // seat signature does not verify — refused
        }
        if !verify_identity_binding(m) {
            continue; // identity signature does not verify — refused (F1)
        }
        let id = m.identity.to_ascii_lowercase();
        match slot {
            None => *slot = Some(Some(id)),
            Some(Some(existing)) if *existing == id => {} // idempotent replay
            Some(_) => *slot = Some(None),                // conflicting identities — poison
        }
    }
    out.identity_a = slot_a.flatten();
    out.identity_b = slot_b.flatten();
    out
}

/// A committed-lock seat slot (`pubA` = A, `pubB` = B).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeatLetter {
    A,
    B,
}

/// Does the caller PROVABLY hold a committed settle key of THIS POT OUTPOINT?
///
/// # Why the row itself proves nothing
///
/// `tm_potparty` admits markers by BYTE FORMAT only (the overlay is an index,
/// not an authority — repo doctrine). Every display field on a `/results` row
/// therefore originates in an attacker-writable marker: `gameId`,
/// `opponentIdentity`, `recoveryHeight`, and — because `results_sql` keys on
/// the byte-admitted `identity` column — the fact that the row appears for
/// this caller AT ALL. Anyone can file a marker naming a VICTIM's identity, a
/// `recoveryHeight` of their choosing, and ANY real unspent outpoint as
/// `potTxid`, for one dust `OP_RETURN`. Everything the row then reports about
/// that outpoint (`spent`, `spentConfirmed`, `at.height`, and the committed
/// params) is TRUE — of the ATTACKER's pot, which the attacker can genuinely
/// spend and confirm at will. A client that reads those fields as
/// corroboration is corroborating the attacker.
///
/// # What the chain does settle
///
/// The pot's covenant lock committed `pubA`/`pubB` — the seats' `[2,'low
/// settle']` keys — INTO THE FUNDING OUTPUT, at funding time, before any
/// outcome. A `LOW/potparty/v2` seat marker carries a `seatSig` BY one of
/// those keys over the preimage `domain ‖ gameId ‖ potTxid ‖ potVout ‖
/// identity` ([`seatsig_preimage`]), plus the identity's own signature over
/// the whole marker ([`verify_identity_binding`], the F1 bar). A marker
/// clearing BOTH bars under a key THIS pot's lock committed proves: only the
/// committed key holder could sign it, only the named identity could publish
/// it, and the key predates the outcome. That is the #332 v3 spine.
///
/// # SCOPE — exactly what `Chain` does and does not vouch for (Rule 8)
///
/// `Chain` is a statement about **(potOutpoint, identity)** and NOTHING else:
/// *the caller holds a settle key this pot's covenant lock committed.*
///
/// **This table is EXHAUSTIVE over the 17 keys `results_body` emits.** A
/// partial table headed "what Chain does and does not vouch for" invites
/// exactly the reading it exists to prevent, so any new wire key must be added
/// here — `the_existing_results_shape_is_unchanged_apart_from_the_added_keys`
/// pins the key set, so a new key cannot arrive unnoticed.
///
/// | wire key | covered by `Chain`? |
/// |---|---|
/// | `potTxid`, `potVout` | **YES** — the seatSig preimage commits the outpoint |
/// | *(the `identity` query param)* | **YES** — the F1 identity signature binds it |
/// | `covRecoveryHeight` | **YES** — decoded from that outpoint's own funding lock |
/// | `potBinding`, `potBindingSource` | *(this field)* |
/// | `spent`, `spentConfirmed`, `settleTxid`, `at` | the index's record FOR THAT OUTPOINT — honest, but the row chose the outpoint |
/// | `verdict` | chain truth ABOUT THAT OUTPOINT (covenant template match) — same caveat |
/// | `outcome`, `outcomeSource` | **YES in practice, by construction**: on a `Chain` row `my_seat` is `Some`, and `derive_outcome_with_seat` gives the seat path precedence over any claim, so a winner verdict resolves `won`/`lost` from the committed key. `tie`/`refund` are seat-symmetric chain truth. No claim can move either. |
/// | **`gameId`** | **NO** — see [`ResultEntry::game_id_binding`] |
/// | **`gameIdBinding`** | *(reports precisely that)* |
/// | **`recoveryHeight`** (the marker hint) | **NO — attacker-owned even on a `Chain` row** |
/// | **`opponentIdentity`** | **NO — attacker-owned even on a `Chain` row** |
/// | **`hand`** (and every field inside it) | **NO — can be a REAL, signature-verified showdown belonging to a DIFFERENT game** |
///
/// The three `NO` display fields are not hypothetical. `results_sql` collapses
/// each pot to the OLDEST marker naming it and that representative supplies
/// them; the gameId is public (it is in the victim's own on-chain marker), so
/// an attacker re-using it with an earlier `createdAt` owns `recoveryHeight`
/// and `opponentIdentity` on a row that still reads `Chain`
/// (`a_chain_bound_row_still_carries_attacker_display_fields`).
///
/// **`hand` is the widest of them, and it was built to confirm it**
/// (`the_hand_field_is_attacker_influenceable_on_a_chain_bound_row`).
/// `assemble_results` looks claims up by the ROW's `gameId`, and
/// `resolve_winner_hand`'s party check accepts the ROW's `opponentIdentity` —
/// both attacker-ownable. An attacker who wins the representative-row race can
/// point the row at a `gameId` under which only its OWN signed claim exists and
/// name itself as the opponent, so the row serves that claim's cards. What it
/// CANNOT do is attribute a hand to the victim (claims carry the winner's own
/// verified signature) or move `outcome` (the seat path outranks claims) — so
/// the observable result is a row reading `outcome: "won"` for the caller
/// beside a `hand` naming someone else. **Do not render `hand` as this game's
/// showdown unless `gameIdBinding == "chain"`.**
///
/// **Do not read "chain-bound" as row integrity.**
///
/// # The gameId is deliberately NOT part of this
///
/// An earlier revision gated `Chain` on `marker.gameId == row.gameId`. That
/// made the money word depend on the representative row's `gameId`, which is
/// attacker-writable — and **ONE** dust marker with a fabricated gameId and an
/// earlier `createdAt` then flipped an honest row to `Unknown` PERMANENTLY
/// (republishing cannot help: `createdAt ASC` sorts every republish later and
/// rows are never deleted). That is Rule 19 exactly — dissolving one evictable
/// input by depending on another — and Rule 6: it traded a false positive for
/// a permanent lockout. The gameId agreement now rides on the separate,
/// non-load-bearing [`ResultEntry::game_id_binding`], so no attacker-writable
/// field remains anywhere in this derivation.
///
/// Anything short of proof is [`PotBinding::Unknown`] — a FIRST-CLASS answer
/// (Rule 13), never coerced to the optimistic value.
///
/// # Residual — MEASURED, not estimated (`the_measured_denial_cost_table`)
///
/// The only remaining denial axis is the #283c per-key window cap. Measured
/// through the real producer, on an honest row:
///
/// | attack | markers needed | result |
/// |---|---|---|
/// | junk under the honest committed key ALONE | never flips (0/4/8/9/16/40 all `Chain`) | — |
/// | displace the representative row + junk under the honest key | **9** (1 displacer + 8 junk) | `Unknown`, permanent |
///
/// Junk alone never flips because [`assemble_results`] re-injects the caller's
/// OWN representative-row marker regardless of the SQL window; the attacker
/// must first displace that row, which costs the extra marker.
///
/// Two properties of that residual are EXECUTED rather than asserted here —
/// they are the whole basis on which the residual is being accepted, so they
/// are the ones that must not rot into prose (Rule 10):
///  - it does **not heal**: `residual_b_does_not_heal_when_the_victim_
///    republishes` drives five honest republishes and stays `Unknown`;
///  - displacement is a **race, not a volume attack**:
///    `displacing_the_representative_row_requires_winning_the_race_not_volume`
///    shows 500 late markers change nothing and one early marker wins, because
///    `createdAt` is server-assigned at admission and cannot be backdated.
///
/// So the true cost is "9 dust markers AND having filed one before the
/// victim's honest marker" — cheap, but not free, and not retroactive.
///
/// Pre-vs-post for that attack (Rule 6, stated plainly): **before** this work
/// the client read the marker hint, so those same markers produced a FALSE
/// "recoverable"; **after**, they produce an honest permanent `Unknown`. The
/// failure direction improved; the permanence did not, and 9 dust markers is
/// cheap. Not closed here because no window size or sort order closes it — the
/// row that must win is "the one whose `seatSigHex` VERIFIES", which SQL
/// cannot compute. The named fix is bsv-low#283's verify-on-read pass over a
/// wider window when attribution comes back empty; widening
/// [`SEAT_MARKERS_PER_KEY`] only moves the number.
///
/// Second residual: a **v1 (pre-#230) pot has no seat binding to find** and
/// answers `Unknown` forever. Note this is not a fallback to something safer —
/// the legacy path such a pot defers to is the attacker-writable
/// `recoveryHeight` hint this change exists to stop clients trusting. A v1 pot
/// therefore has NO safe money word on `/results`; the client must use its own
/// held refund plan (or `/refund-view`) for those.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PotBinding {
    /// PROVEN: a settle key committed in THIS pot's own lock signed a preimage
    /// naming this outpoint and this identity, and that identity signed the
    /// marker carrying it. Scope table above.
    Chain,
    /// NOT PROVEN — the pot is not covenant-decodable, no verifying seat
    /// marker exists under the committed keys, the slot is poisoned by
    /// conflicting markers, or the lock is degenerate. This is "we cannot
    /// prove it", NOT "it is false".
    Unknown,
}

impl PotBinding {
    pub fn as_str(self) -> &'static str {
        match self {
            PotBinding::Chain => "chain",
            PotBinding::Unknown => "unknown",
        }
    }

    /// The honesty PAIR — `(value, source)`, the shape `/results` already
    /// uses for `outcome`/`outcomeSource` and `/refund-view` for
    /// `status`/`statusSource`. The source is `None` for `Unknown`: there is
    /// no provenance to name when nothing was proven.
    pub fn pair(self) -> (&'static str, Option<&'static str>) {
        match self {
            PotBinding::Chain => ("chain", Some("chain+seatkey")),
            PotBinding::Unknown => ("unknown", None),
        }
    }

    pub fn from_proof(proven: bool) -> Self {
        if proven {
            PotBinding::Chain
        } else {
            PotBinding::Unknown
        }
    }
}

/// Does a marker that clears the same two signature bars ALSO attest THIS
/// row's `gameId`?
///
/// DECORATION ONLY — **must never gate a money word.** The row's `gameId` comes
/// from the oldest marker naming the pot, which an attacker can own for one
/// dust `OP_RETURN` (see the [`PotBinding`] scope table). A `Unknown` here
/// means "the row's gameId is not attested", which on an otherwise `Chain` row
/// usually means *someone else chose the gameId*, not that anything is wrong
/// with the pot.
///
/// It is still worth serving: the seatSig preimage commits the gameId, so when
/// this says `Chain` the client knows the row's game label came from a key the
/// lock committed — useful for picking which local game record to reconcile
/// against, and useless as a security bar. `Chain` here always implies `Chain`
/// on [`PotBinding`] (same predicate, strictly narrower marker set).
pub fn game_id_bound_seat(
    params: &CovenantParams,
    pot_txid_lc: &str,
    pot_vout: u32,
    game_id_lc: &str,
    identity_lc: &str,
    markers: &[SeatMarkerRow],
) -> Option<SeatLetter> {
    let for_this_game: Vec<SeatMarkerRow> = markers
        .iter()
        .filter(|m| m.game_id.eq_ignore_ascii_case(game_id_lc))
        .cloned()
        .collect();
    my_seat(params, pot_txid_lc, pot_vout, identity_lc, &for_this_game)
}

/// The caller's proven seat for one pot, from its OWN verified v2 marker(s)
/// against the committed lock keys — the `/results` consumer. `None` unless
/// exactly one of the two slots is attributed TO THE CALLER.
pub fn my_seat(
    params: &CovenantParams,
    pot_txid_lc: &str,
    pot_vout: u32,
    identity_lc: &str,
    markers: &[SeatMarkerRow],
) -> Option<SeatLetter> {
    let attr = attribute_seats(params, pot_txid_lc, pot_vout, markers);
    let a = attr.identity_a.as_deref() == Some(identity_lc);
    let b = attr.identity_b.as_deref() == Some(identity_lc);
    match (a, b) {
        (true, false) => Some(SeatLetter::A),
        (false, true) => Some(SeatLetter::B),
        _ => None, // unattributed, or the impossible both-slots case
    }
}

/// [`derive_outcome`] with the #230 seat attribution layered ON TOP: for a
/// winner verdict, the caller's PROVEN seat decides won/lost directly
/// (`outcomeSource = "chain+seatkey"`) — no countersignature, no claim. The
/// seat proof is strictly stronger than claim corroboration (committed
/// pre-outcome, signed by the lock key itself), so it takes precedence;
/// without one, the claim-corroboration rules apply unchanged. `lost` via
/// seat key is fair to show: the caller's own wallet published the seat
/// proof at funding, and the chain verdict is the covenant's own mandate.
pub fn derive_outcome_with_seat(
    verdict: Option<PotVerdict>,
    my_seat: Option<SeatLetter>,
    identity_lc: &str,
    opponent_lc: &str,
    settle_txid_lc: Option<&str>,
    claims: Option<&GameClaims>,
) -> (Outcome, Option<&'static str>) {
    if let (Some(v @ (PotVerdict::WinnerA | PotVerdict::WinnerB)), Some(seat)) = (verdict, my_seat)
    {
        let won = matches!(
            (v, seat),
            (PotVerdict::WinnerA, SeatLetter::A) | (PotVerdict::WinnerB, SeatLetter::B)
        );
        return (
            if won { Outcome::Won } else { Outcome::Lost },
            Some("chain+seatkey"),
        );
    }
    derive_outcome(verdict, identity_lc, opponent_lc, settle_txid_lc, claims)
}

// ── /results assembly ───────────────────────────────────────────────────────

/// One pot the identity is a party to, ready for classification: the
/// `potparty_records` facts joined to the spend pointer and both stored
/// BEEFs. The route dedupes marker rows to one entry per pot outpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResultsRow {
    /// The potparty row's OWN identity column (F8: carried explicitly so the
    /// seat path can never silently decouple from the SQL's WHERE filter).
    pub identity: String,
    pub game_id: String,
    pub pot_txid: String,
    pub pot_vout: u32,
    pub recovery_height: u32,
    pub opponent_identity: String,
    pub spent: Option<bool>,
    pub spending_txid: Option<String>,
    pub spent_confirmed: Option<bool>,
    /// `hex(pot_beefs.beef)` for the FUNDING tx (keyed by potTxid). #284:
    /// FALLBACK-ONLY — NULL when the row carries decoded params (the SQL
    /// gates the join on `pubA IS NULL`).
    pub funding_beef_hex: Option<String>,
    /// `hex(pot_beefs.beef)` for the recorded spender. #284: FALLBACK-ONLY —
    /// NULL when the stored verdict is fresh AND the proven height present.
    pub spender_beef_hex: Option<String>,
    /// #230 v2 seat-binding fields from the caller's own potparty row —
    /// `None` for a v1 row. UNVERIFIED here; `my_seat` verifies before they
    /// attribute anything.
    pub seat_settle_pubkey: Option<String>,
    pub seat_sig_hex: Option<String>,
    /// The marker's IDENTITY signature push (`sigHex`) — required by the F1
    /// identity-binding verification before a v2 row may attribute a seat.
    pub marker_sig_hex: Option<String>,
    /// #284 decoded pot_records columns — the DECODE-ONCE re-presentation of
    /// the admitted funding lock (see the overlay's `pot_records` migration
    /// notes). All `None` for a legacy un-backfilled row, in which case the
    /// BEEF fallback path below behaves exactly as pre-#284.
    pub lock_kind: Option<String>,
    pub pub_a: Option<String>,
    pub pub_b: Option<String>,
    pub pub_tower: Option<String>,
    pub pay_pkh_a: Option<String>,
    pub pay_pkh_b: Option<String>,
    pub rake_pkh: Option<String>,
    pub stake_a: Option<u64>,
    pub stake_b: Option<u64>,
    pub fee_sats: Option<u64>,
    /// The COMMITTED covenant recoveryHeight (pot_records.recoveryHeight,
    /// aliased covRecoveryHeight — distinct from the marker's own
    /// `recovery_height` field above).
    pub cov_recovery_height: Option<u64>,
    pub pot_sats: Option<u64>,
    /// The stored spend verdict — trusted ONLY when `verdict_txid` equals
    /// the row's `spending_txid` (a stale pointer-overwrite leftover is
    /// ignored and the BEEF fallback classifies instead).
    pub verdict: Option<String>,
    pub verdict_txid: Option<String>,
    /// Block height of the SPV-verified spend confirm (at.height source).
    pub spent_height: Option<u64>,
    /// bsv-low#304: the spender `pot_beefs` row's VERIFIED proof latch
    /// (`sb.proof_verified` — set only by the overlay's chaintracks-
    /// verifying writers). The spender-BEEF at.height fallback is served
    /// ONLY when this is `Some(true)`; a structural bump on an unverified
    /// row is never a height. `None` = no spender row joined.
    pub spender_proof_verified: Option<bool>,
}

impl ResultsRow {
    /// The committed [`CovenantParams`] from the row's decoded columns —
    /// strict reconstruction (`covenant_params_from_hex` validates hex +
    /// lengths); any malformed/absent field is `None` and the caller falls
    /// back to the BEEF parse. Never a panic, never a trust-shortcut.
    fn column_covenant_params(&self) -> Option<CovenantParams> {
        if self.lock_kind.as_deref() != Some("covenant") {
            return None;
        }
        covenant_params_from_hex(
            self.pub_a.as_deref()?,
            self.pub_b.as_deref()?,
            self.pub_tower.as_deref()?,
            self.pay_pkh_a.as_deref()?,
            self.pay_pkh_b.as_deref()?,
            self.rake_pkh.as_deref()?,
            self.stake_a?,
            self.stake_b?,
            self.fee_sats?,
            self.cov_recovery_height?,
        )
    }
}

/// A game's SIGNATURE-VERIFIED `tm_result` claims relevant to won/lost
/// attribution. Only markers whose WINNER signature verified under the
/// claimed winner's identity make it in at all (`verified_claim`); claims
/// remain corroboration-only — a claim can never create a result the chain
/// did not classify, and conflicting claims yield `unresolved`.
#[derive(Debug, Clone, Default)]
pub struct GameClaims {
    pub claims: Vec<ClaimFact>,
}

/// One verified claim fact (all fields lowercased). Existence of a
/// `ClaimFact` MEANS the winner's signature verified over the canonical
/// challenge; `loser_sig_verified` reports the countersig independently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimFact {
    /// Claimed winner identity — its signature VERIFIED.
    pub winner: String,
    /// Claimed loser identity (whose countersig, if any, is judged below).
    pub loser: String,
    /// The settle txid the claim names (the chain-verdict binding).
    pub settle_txid: String,
    /// True iff a loser countersig was present AND verified under `loser`.
    pub loser_sig_verified: bool,
    /// The WINNER's revealed 5 cards, CANONICAL 10-hex (sorted ascending
    /// ordinals) — `None` for a v1 (no-cards) claim. These bytes are BOUND by
    /// the (already-verified) winner signature (the v2 challenge commits the
    /// canonical cards), so a present value is the winner's true showdown hand.
    /// Only the winner's hand is ever revealed on-chain; the loser's is not.
    pub cards_hex: Option<String>,
}

// ── server-side claim signature verification ────────────────────────────────
//
// The exact recipe of the client's `result.ts` — same protocol, same keyID,
// same challenge bytes, same 'anyone' verifier. BRC-42 derivation is
// byte-identical across the Rust and TS SDKs (cross-vectored in bsv-rs;
// production-proven by the LOW lobby tokens, which the TS app signs and
// `overlay-discovery`'s Rust topic manager verifies with this same
// `ProtoWallet::anyone()` pattern).

/// The BRC-43 protocol result claims are signed under —
/// `result.ts::RESULT_PROTOCOL` = `[1, 'low result']`.
///
/// `pub` (#332) so producer-parity fixtures — the leaderboard attack cells
/// and the `leaderboard_window_sqlite` integration harness — can SIGN real
/// markers with the exact protocol the verifier checks, instead of
/// re-deriving the tuple by convention (a duplicated format string is a
/// boundary with no pin by construction).
pub fn result_protocol() -> bsv_rs::wallet::Protocol {
    bsv_rs::wallet::Protocol::new(bsv_rs::wallet::SecurityLevel::App, "low result")
}

/// Canonicalize a v2 cards push: 10 hex chars → five DISTINCT ordinals
/// 0..=51, sorted ascending, re-encoded lowercase — `result.ts::cardsToHex ∘
/// cardsFromHex`. `None` = malformed (an unverifiable claim: the sigs bind
/// the canonical cards, so we must be able to reconstruct them).
fn canonical_cards_hex(cards_hex: &str) -> Option<String> {
    let mut cards = hex::decode(cards_hex).ok()?;
    if cards.len() != 5 || cards.iter().any(|&c| c > 51) {
        return None;
    }
    cards.sort_unstable();
    if cards.windows(2).any(|w| w[0] == w[1]) {
        return None;
    }
    Some(hex::encode(cards))
}

/// The canonical signed challenge — byte-identical to
/// `result.ts::resultChallenge` (all fields lowercased; v2 binds the
/// canonical sorted cards). Inputs must already be lowercase.
///
/// `pub` (#332) for the same producer-parity reason as [`result_protocol`]:
/// fixtures that must mint REAL verifiable markers build the challenge here,
/// never from a copied format string.
pub fn result_challenge_bytes(
    game_id_lc: &str,
    winner_lc: &str,
    loser_lc: &str,
    pot_lc: &str,
    settle_lc: &str,
    cards_hex: Option<&str>,
) -> Option<Vec<u8>> {
    let base = format!(
        "gid={game_id_lc}\nwinner={winner_lc}\nloser={loser_lc}\npot={pot_lc}\nsettle={settle_lc}"
    );
    let s = match cards_hex {
        Some(ch) => {
            let cards = canonical_cards_hex(ch)?;
            format!("LOW-result\nv2\n{base}\ncards={cards}")
        }
        None => format!("LOW-result\nv1\n{base}"),
    };
    Some(s.into_bytes())
}

/// Verify one DER signature under `signer_identity_hex` over `challenge`
/// with the public 'anyone' verifier — the mirror of the client's
/// `anyoneVerifier.verifySignature({counterparty: signer, forSelf: false})`.
/// Any malformed key/sig/derivation failure is simply `false` (fail-safe:
/// an unverifiable signature never corroborates).
pub(crate) fn anyone_sig_verifies(
    signer_identity_hex: &str,
    key_id: &str,
    challenge: &[u8],
    sig_hex: &str,
    protocol: bsv_rs::wallet::Protocol,
) -> bool {
    let Ok(signer) = bsv_rs::primitives::ec::PublicKey::from_hex(signer_identity_hex) else {
        return false;
    };
    let Ok(sig) = hex::decode(sig_hex) else {
        return false;
    };
    bsv_rs::wallet::ProtoWallet::anyone()
        .verify_signature(bsv_rs::wallet::VerifySignatureArgs {
            data: Some(challenge.to_vec()),
            hash_to_directly_verify: None,
            signature: sig,
            protocol_id: protocol,
            key_id: key_id.to_string(),
            counterparty: Some(bsv_rs::wallet::Counterparty::Other(signer)),
            for_self: Some(false),
        })
        .map(|r| r.valid)
        .unwrap_or(false)
}

/// Verify one raw `result_markers_v2` row into a [`ClaimFact`], or `None`
/// when it must contribute nothing: self-paired, malformed cards, or a
/// winner signature that does not verify under the claimed winner identity.
/// A present-but-unverifiable LOSER countersig does not kill the claim — it
/// degrades to `loser_sig_verified: false` (the client's `unconfirmed`
/// demotion in `verifyResultRow`): the winner's own claim still stands,
/// only the confirmation tier is garbage.
pub fn verified_claim(m: &ResultMarkerRow) -> Option<ClaimFact> {
    let winner_lc = m.winner.to_ascii_lowercase();
    let loser_lc = m.loser.to_ascii_lowercase();
    if winner_lc == loser_lc {
        return None; // self-paired claims are invalid (client parity)
    }
    let game_lc = m.game_id.to_ascii_lowercase();
    let challenge = result_challenge_bytes(
        &game_lc,
        &winner_lc,
        &loser_lc,
        &m.pot_txid.to_ascii_lowercase(),
        &m.settle_txid.to_ascii_lowercase(),
        m.cards_hex.as_deref(),
    )?;
    if !anyone_sig_verifies(
        &winner_lc,
        &game_lc,
        &challenge,
        &m.winner_sig_hex,
        result_protocol(),
    ) {
        return None; // fabricated/garbled claim — as if never published
    }
    let loser_sig_verified = m.loser_sig_hex.as_deref().is_some_and(|s| {
        anyone_sig_verifies(&loser_lc, &game_lc, &challenge, s, result_protocol())
    });
    // The cards are re-canonicalized from the SAME field the challenge bound
    // (present ⇒ it verified as part of the winner sig above), so a Some value
    // is trustworthy. A malformed field can't reach here (the challenge would
    // have failed to reconstruct → early `None`), but re-canonicalize
    // defensively so downstream always sees canonical 10-hex or nothing.
    let cards_hex = m.cards_hex.as_deref().and_then(canonical_cards_hex);
    Some(ClaimFact {
        winner: winner_lc,
        loser: loser_lc,
        settle_txid: m.settle_txid.to_ascii_lowercase(),
        loser_sig_verified,
        cards_hex,
    })
}

/// One `/results` response entry, pre-JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultEntry {
    pub game_id: String,
    pub pot_txid: String,
    pub pot_vout: u32,
    /// The `ls_potparty` MARKER's recoveryHeight — an UNVERIFIED HINT. Kept
    /// verbatim for backward compatibility (it is the `recoveryHeight` wire
    /// field every deployed client already reads); see
    /// [`ResultEntry::cov_recovery_height`] for the chain-committed value.
    pub recovery_height: u32,
    /// The COVENANT-COMMITTED recoveryHeight of the pot outpoint this row
    /// names, decoded from the funding LOCK (`pot_records`, keyed on the
    /// OUTPOINT — a UTXO, not a claimable name) and range-checked by
    /// [`crate::refund_view::valid_recovery_height`]. `None` when the pot is
    /// not in the index, is not a covenant lock, or committed an unusable
    /// height — never the marker's value, never a guess.
    pub cov_recovery_height: Option<u64>,
    pub opponent_identity: String,
    pub settle_txid: Option<String>,
    pub spent: Option<bool>,
    pub spent_confirmed: Option<bool>,
    /// Does the caller provably hold a committed settle key of THIS POT
    /// OUTPOINT? See [`PotBinding`] for the exact scope table (notably: this
    /// does NOT vouch for `recovery_height`, `opponent_identity` or
    /// `game_id`). `Unknown` is first-class and is the answer whenever the
    /// chain does not prove it.
    pub pot_binding: PotBinding,
    /// Does a marker clearing the same bars also attest THIS row's `game_id`?
    /// DECORATION — see [`game_id_bound_seat`]; never gate money on it.
    pub game_id_binding: PotBinding,
    /// The chain-truth template classification (`winner-a`/`winner-b`/`tie`/
    /// `refund`), `None` = not classified.
    pub verdict: Option<PotVerdict>,
    /// The per-identity outcome (see [`derive_outcome`]).
    pub outcome: Outcome,
    /// How `outcome` was derived: `"chain"` (seat-symmetric verdict),
    /// `"chain+seatkey"` (winner verdict + the caller's #230 seat-binding
    /// proof — no countersignature involved), `"chain+claim"` (winner
    /// verdict + unanimous claims), `None` for `unresolved`.
    pub outcome_source: Option<&'static str>,
    /// The settle's mined block height per its BEEF BUMP, when proven.
    pub at_height: Option<u64>,
    /// The provable showdown hand (bsv-low #245): the WINNER's five cards +
    /// low-sum, or `None` when no hand is provable (refund, unrevealed settle,
    /// unresolved winner). Only the winner's hand is on-chain — the loser's is
    /// never fabricated. See [`resolve_winner_hand`].
    pub winner_hand: Option<WinnerHand>,
}

/// The per-identity outcome enum (wire strings match bsv-low #227's spec).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Won,
    Lost,
    Tie,
    Refund,
    Unresolved,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Won => "won",
            Outcome::Lost => "lost",
            Outcome::Tie => "tie",
            Outcome::Refund => "refund",
            Outcome::Unresolved => "unresolved",
        }
    }
}

/// Map a chain verdict (+ VERIFIED claims) to the identity's outcome.
///
/// - `tie` / `refund` are seat-symmetric → pure chain truth.
/// - a winner verdict upgrades ONLY when every verified on-record claim that
///   names the classified settle txid agrees on ONE winner and that winner
///   is one of the two parties (the caller or its opponent). No claims,
///   conflicting claims, or a claimed winner outside the pair →
///   `unresolved` (the chain alone cannot name the seat's identity — module
///   note).
/// - the tiers are key-honest (every `ClaimFact` already carries a VERIFIED
///   winner sig — `claims_by_game` drops the rest):
///   * `won` — the unanimous verified winner is the caller. Nobody can put
///     the caller here without the caller's own key.
///   * `lost` — the unanimous verified winner is the opponent AND some claim
///     naming this settle carries the CALLER's verified countersig
///     (`loser == identity`, `loser_sig_verified`). The caller attested the
///     loss itself; an opponent-only (or third-party-countersigned) claim
///     never shows the caller a loss.
pub fn derive_outcome(
    verdict: Option<PotVerdict>,
    identity_lc: &str,
    opponent_lc: &str,
    settle_txid_lc: Option<&str>,
    claims: Option<&GameClaims>,
) -> (Outcome, Option<&'static str>) {
    match verdict {
        Some(PotVerdict::Tie) => (Outcome::Tie, Some("chain")),
        Some(PotVerdict::Refund) => (Outcome::Refund, Some("chain")),
        Some(PotVerdict::WinnerA) | Some(PotVerdict::WinnerB) => {
            let (Some(settle), Some(gc)) = (settle_txid_lc, claims) else {
                return (Outcome::Unresolved, None);
            };
            // The verified claims naming THIS settle.
            let relevant: Vec<&ClaimFact> = gc
                .claims
                .iter()
                .filter(|c| c.settle_txid.eq_ignore_ascii_case(settle))
                .collect();
            let mut winners: Vec<&str> = relevant.iter().map(|c| c.winner.as_str()).collect();
            winners.sort_unstable();
            winners.dedup();
            match winners.as_slice() {
                [w] if w.eq_ignore_ascii_case(identity_lc) => (Outcome::Won, Some("chain+claim")),
                [w] if w.eq_ignore_ascii_case(opponent_lc) => {
                    // Lost needs the caller's OWN verified countersig.
                    if relevant
                        .iter()
                        .any(|c| c.loser.eq_ignore_ascii_case(identity_lc) && c.loser_sig_verified)
                    {
                        (Outcome::Lost, Some("chain+claim"))
                    } else {
                        (Outcome::Unresolved, None)
                    }
                }
                _ => (Outcome::Unresolved, None),
            }
        }
        None => (Outcome::Unresolved, None),
    }
}

// ── hand-score exposure (bsv-low #245) ──────────────────────────────────────
//
// Your Games wants the SHOWDOWN, not just win/lose: the winner's five cards +
// its low-sum, honestly attributed. The only cards on-chain are the WINNER's
// (a coop/enforced settle never reveals the loser's hand), carried in a
// `tm_result` marker as `cardsHex` and BOUND by the winner's signature. So the
// exposed hand rides on the SAME verified, unanimous claim that already drives
// `won`/`lost` — never a bare/forged marker, never a fabricated loser hand.

/// The provable showdown hand for a `/results` row: the WINNER's five cards +
/// its low-sum, plus whose hand it is. The loser's hand is NEVER on-chain for a
/// settle, so it is never present here (see [`ResultEntry::winner_hand`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WinnerHand {
    /// Whose five cards these are (the winning identity; for a tie, the seat
    /// whose equal-sum hand was revealed — `is_tie` flags it).
    pub identity: String,
    /// The five cards, CANONICAL 10-hex (sorted ascending ordinals).
    pub cards_hex: String,
    /// The LOW low-sum of `cards_hex` (`logic::hand_score` — Ace=1, 2..9 pip,
    /// T/J/Q/K=10). For a tie this is BOTH players' equal sum.
    pub score: u32,
    /// True when the chain verdict was a TIE (both sums equal by definition);
    /// the exposed hand is one provable side, `identity` its owner.
    pub is_tie: bool,
}

/// Parse + score a claim's `cards_hex` into a [`WinnerHand`], or `None` if the
/// cards are malformed (fail-safe: never expose an unparseable hand).
fn winner_hand_from(identity: &str, cards_hex: &str, is_tie: bool) -> Option<WinnerHand> {
    let arr = crate::logic::leaderboard_cards_from_hex(cards_hex)?;
    let canon = canonical_cards_hex(cards_hex)?;
    Some(WinnerHand {
        identity: identity.to_ascii_lowercase(),
        cards_hex: canon,
        score: crate::logic::hand_score(&arr),
        is_tie,
    })
}

/// Resolve the provable showdown hand for a row from the chain verdict + the
/// VERIFIED claims naming the classified settle. Viewer-INDEPENDENT (a per-game
/// fact both parties see identically). `None` unless a hand is genuinely
/// provable — a refund, an unrevealed (v1) settle, an unresolved/conflicting
/// winner, or a claim winner outside the two parties all yield `None` (never a
/// guess, never a fabricated loser hand).
///
/// - `winner-a`/`winner-b`: needs the SAME unanimous verified winner
///   `derive_outcome` requires (one winner among the two parties, naming this
///   settle) AND a claim by that winner carrying cards → the winner's hand.
/// - `tie`: any verified claim by a party naming this settle that carries cards
///   → that (equal-sum) hand, `is_tie = true`. Both sums are equal by the tie
///   verdict; only the one revealed side is exposed (the other isn't on-chain).
/// - `refund` / `None`: no showdown → `None`.
pub fn resolve_winner_hand(
    verdict: Option<PotVerdict>,
    identity_lc: &str,
    opponent_lc: &str,
    settle_txid_lc: Option<&str>,
    claims: Option<&GameClaims>,
) -> Option<WinnerHand> {
    let settle = settle_txid_lc?;
    let gc = claims?;
    let is_party =
        |id: &str| id.eq_ignore_ascii_case(identity_lc) || id.eq_ignore_ascii_case(opponent_lc);
    let relevant: Vec<&ClaimFact> = gc
        .claims
        .iter()
        .filter(|c| c.settle_txid.eq_ignore_ascii_case(settle))
        .collect();
    match verdict {
        Some(PotVerdict::WinnerA) | Some(PotVerdict::WinnerB) => {
            let mut winners: Vec<&str> = relevant.iter().map(|c| c.winner.as_str()).collect();
            winners.sort_unstable();
            winners.dedup();
            let [w] = winners.as_slice() else {
                return None; // no claim, or conflicting winners → unresolved
            };
            if !is_party(w) {
                return None; // a foreign claim never attributes this pot's hand
            }
            let cards = relevant
                .iter()
                .filter(|c| c.winner.eq_ignore_ascii_case(w))
                .find_map(|c| c.cards_hex.as_deref())?;
            winner_hand_from(w, cards, false)
        }
        Some(PotVerdict::Tie) => {
            let c = relevant
                .iter()
                .find(|c| is_party(&c.winner) && c.cards_hex.is_some())?;
            winner_hand_from(&c.winner, c.cards_hex.as_deref()?, true)
        }
        Some(PotVerdict::Refund) | None => None,
    }
}

/// Every pot outpoint in `rows` whose funding BEEF hash-verifies, mapped to
/// its COMMITTED covenant params (`pubA`/`pubB` — the settle keys the lock
/// itself names). This is what lets `/results` fetch seat markers the way
/// `/leaderboard` does: bound to the pot's own keys, so a forged key cannot
/// enter the result set at all (bsv-low #281 F1, the same insight that fixed
/// #230's F1). A pot with no/unparseable funding bytes is simply absent — no
/// seat query, no attribution, never a guess.
pub fn covenant_params_by_pot(
    rows: &[ResultsRow],
) -> std::collections::HashMap<(String, u32), CovenantParams> {
    let mut out = std::collections::HashMap::new();
    for r in rows {
        let key = (r.pot_txid.to_ascii_lowercase(), r.pot_vout);
        if out.contains_key(&key) {
            continue;
        }
        // #284: the decoded columns first — a strict reconstruction of the
        // params the OVERLAY decoded from the admitted funding lock at
        // admission/backfill time (`column_covenant_params` refuses any
        // malformed stored hex). When they answer, no funding BLOB was even
        // fetched (the SQL gates the join on `pubA IS NULL`).
        if let Some(p) = r.column_covenant_params() {
            out.insert(key, p);
            continue;
        }
        // Fallback (legacy un-backfilled rows): the hash-verified funding
        // bytes, exactly as pre-#284.
        if let Some(p) = beef_covenant_params(r.funding_beef_hex.as_deref(), &key.0, r.pot_vout) {
            out.insert(key, p);
        }
    }
    out
}

/// The pot's COMMITTED covenant params recovered from a stored funding BEEF —
/// the legacy (un-backfilled, `pot_records` columns absent) leg of params
/// resolution. Hash-verified funding bytes only: the raw must hash to
/// `pot_txid_lc` before its lock is read, so a garbled/substituted store row
/// degrades to `None`, never to fabricated params.
///
fn beef_covenant_params(
    funding_beef_hex: Option<&str>,
    pot_txid_lc: &str,
    pot_vout: u32,
) -> Option<CovenantParams> {
    let fb = crate::logic::decode_beef_hex(funding_beef_hex?)?;
    let fraw =
        crate::logic::extract_raw_tx_hex(&fb, pot_txid_lc).and_then(|h| hex::decode(h).ok())?;
    covenant_params_from_funding_raw(&fraw, pot_txid_lc, pot_vout)
}

/// THE single funding-raw → committed-params reader. Hash-verifies `raw`
/// against `pot_txid_lc` before reading the lock at `pot_vout`, so a
/// garbled/substituted store row degrades to `None`, never to fabricated
/// params.
///
/// `pub` and shared because there were THREE inline copies of this walk with
/// two different shapes — `covenant_params_by_pot` and `assemble_results` here
/// (via [`beef_covenant_params`]), plus `routes.rs`'s `/leaderboard`
/// classification partition, which used `f.outputs.get(vout)` instead of
/// `spender_pot_prevout`. Agreement is not a property of a side (Rule 16) and
/// a duplicated walk is a boundary with no pin by construction; the durable
/// fix is one function, not a test that calls each.
pub fn covenant_params_from_funding_raw(
    raw: &[u8],
    pot_txid_lc: &str,
    pot_vout: u32,
) -> Option<CovenantParams> {
    parse_raw_tx_verified(raw, pot_txid_lc)
        .and_then(|f| spender_pot_prevout(&f, pot_vout))
        .and_then(|(_, lock)| extract_covenant_params(&lock))
}

/// Assemble the `/results` entries: dedupe rows to one per pot outpoint
/// (newest first, as the SQL orders), classify each spent pot, and derive
/// the caller's outcome. Missing bytes anywhere degrade THAT entry to
/// `unresolved` — never an error, never a guess.
///
/// `seat_markers_by_pot` is the KEY-BOUND seat-marker fetch
/// ([`seat_markers_sql`], keyed by pot outpoint) that `routes::results`
/// performs as a SECOND query.
///
/// # Why the seat proof is no longer taken from `rows` (bsv-low #281 F1)
///
/// It used to be: `assemble_results` gathered seat markers from ALL rows
/// before dedup, and `results_sql` returned every marker the caller had. Once
/// #281 collapsed that window to one row per pot, `rows` stopped being a
/// sound seat-marker source — and NO ordering rule can fix it, because
/// #252's opportunistic backfill publishes honest v2 markers for pots whose
/// txid has been public for weeks, so an attacker can always land a forged
/// row with an EARLIER `createdAt`. Preferring the "v2-looking" row would
/// therefore have dropped the cost of erasing a tower-enforced win from ~110
/// dust markers to ONE. The fix is not a better sort: it is to fetch seat
/// markers under the pot's OWN COMMITTED KEYS, where a forged key cannot
/// enter the result set at all.
///
/// Rows still contribute their own marker when they carry one — that is
/// strictly additive (every marker is re-verified against the committed lock
/// by [`attribute_seats`]) and keeps a seat proof available if the second
/// query faults.
///
/// # `params_by_pot` is PASSED IN, never re-resolved (bsv-low #314 class)
///
/// `routes::results` already computes [`covenant_params_by_pot`] to bind the
/// seat-marker fetch. An earlier revision of this function resolved the same
/// params AGAIN from the same bytes, which measured **2.0×** CPU per row on
/// the legacy-BEEF leg (65.5 µs → 131.8 µs; `assemble_results` on one unspent
/// legacy row 605 ns → 66.2 µs). `/results` takes `identity` unauthenticated
/// and its row set is populated by attacker-writable dust markers, so a
/// 100-legacy-pot page is attacker-CONSTRUCTIBLE — that doubling was a
/// free 2× on an attacker-directed route. Taking the map as an argument makes
/// double resolution unrepresentable rather than merely absent (Rule 15).
pub fn assemble_results(
    identity_lc: &str,
    rows: Vec<ResultsRow>,
    claims_by_game: &std::collections::HashMap<String, GameClaims>,
    seat_markers_by_pot: &std::collections::HashMap<(String, u32), Vec<SeatMarkerRow>>,
    params_by_pot: &std::collections::HashMap<(String, u32), CovenantParams>,
) -> Vec<ResultEntry> {
    // Keyed by pot OUTPOINT, never by gameId: `attribute_seats` re-checks the
    // outpoint and the committed key on every marker, so a marker naming a
    // different game is harmless — whereas keying on the row's gameId would
    // let a forged row's gameId hide the genuine proof.
    let mut seat_markers: std::collections::HashMap<(String, u32), Vec<SeatMarkerRow>> =
        seat_markers_by_pot.clone();
    for r in &rows {
        if let (Some(pk), Some(seat_sig), Some(id_sig)) =
            (&r.seat_settle_pubkey, &r.seat_sig_hex, &r.marker_sig_hex)
        {
            let key = (r.pot_txid.to_ascii_lowercase(), r.pot_vout);
            // F8 (#230): the marker's identity comes from the ROW, not from
            // the query parameter — a future SQL change can't silently
            // decouple them.
            let m = SeatMarkerRow {
                identity: r.identity.to_ascii_lowercase(),
                opponent_identity: r.opponent_identity.to_ascii_lowercase(),
                game_id: r.game_id.to_ascii_lowercase(),
                pot_txid: r.pot_txid.to_ascii_lowercase(),
                pot_vout: r.pot_vout,
                recovery_height: r.recovery_height,
                seat_settle_pubkey: pk.to_ascii_lowercase(),
                seat_sig_hex: seat_sig.to_ascii_lowercase(),
                identity_sig_hex: id_sig.to_ascii_lowercase(),
            };
            let slot = seat_markers.entry(key).or_default();
            if !slot.contains(&m) {
                slot.push(m);
            }
        }
    }
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for r in rows {
        let key = (
            r.game_id.to_ascii_lowercase(),
            r.pot_txid.to_ascii_lowercase(),
            r.pot_vout,
        );
        if !seen.insert(key.clone()) {
            continue; // duplicate marker rows (garbage coexists by design)
        }
        let settle_lc = r.spending_txid.as_ref().map(|s| s.to_ascii_lowercase());
        let pot_txid_lc = r.pot_txid.to_ascii_lowercase();
        // #284: the row's decoded covenant params (strictly reconstructed —
        // malformed stored hex yields None and the BEEF fallback applies).
        let column_params = r.column_covenant_params();
        // The pot's COMMITTED params — LOOKED UP, never re-resolved (see the
        // #314-class note on this function). The caller resolved them with
        // exactly this rule (`covenant_params_by_pot`: decoded columns first,
        // else the hash-verified funding bytes) keyed by outpoint. This is
        // also why the BINDING below is answerable for an UNSPENT pot —
        // precisely the state a "recoverable" money word is rendered in.
        let row_params = params_by_pot.get(&(pot_txid_lc.clone(), r.pot_vout));
        let mut verdict = None;
        let mut at_height = None;
        let mut seat = None;
        // #323 defect 1 — the spend must be CONFIRMED before any verdict,
        // outcome, height or hand is derived from it. `spent = 1` with
        // `spentConfirmed = 0` is a RECORDED-BUT-UNCONFIRMED pointer: a
        // non-final parked tx is a displaceable INTENT, not a landing, and
        // deriving from it asserts what the chain does not prove (it served
        // a never-mined spender on 7 of 8 refunds in the 2026-08-03
        // real-wallet audit). This is the SAME bar
        // `refund_view::derive_refund_status` already applies — "recorded-
        // but-unconfirmed (a displaceable intent, not a landing):
        // incomplete — never guess". The raw pointer facts (`settleTxid`,
        // `spent`, `spentConfirmed`) still SERVE below: surface the attempt,
        // never consume it as a landing.
        // #323 MEDIUM-4 — the confirmation bar accepts EITHER the
        // `spentConfirmed` flag OR a chaintracks-VERIFIED spender proof.
        //
        // Decided explicitly rather than fail-closing on the flag alone: the
        // column was added by migration with default 0, so pre-existing rows
        // whose spend is genuinely MINED can carry `spentConfirmed = 0`.
        // Gating on the flag only would silently un-resolve real historical
        // settles — an honest-but-useless answer where a stronger proof is
        // already in hand. `spender_proof_verified` is that stronger proof:
        // it is the overlay's chaintracks-verifying writers latching a real
        // merkle proof for THIS spender (`sb.proof_verified`, joined on
        // `sb.txid = spendingTxid`), and `assemble_results` already trusts it
        // for `at.height` two hundred lines below. A parked tx that never
        // mined can never acquire one, so this widens the bar toward CHAIN
        // TRUTH, never away from it.
        let confirmed_landing = crate::logic::is_confirmed_landing_with_proof(
            r.spent_confirmed,
            r.spender_proof_verified,
        );
        if let (Some(true), true, Some(settle)) = (r.spent, confirmed_landing, settle_lc.as_deref())
        {
            // 1. The STORED verdict — trusted only when it was computed from
            //    THIS spend pointer (`verdictTxid == spendingTxid`; the
            //    overlay enforced the stake-conservation check at write
            //    time). Bare pots never have one (overlay rule), so the
            //    marker-hint refund rule below still covers them.
            if let (Some(v), Some(vt)) = (r.verdict.as_deref(), r.verdict_txid.as_deref()) {
                if vt.eq_ignore_ascii_case(settle) {
                    verdict = PotVerdict::from_wire(v);
                }
            }
            // 2. Decoded params + spender BEEF (stored verdict stale/absent
            //    but the params columns answer): classify per-request from
            //    the hash-verified spender bytes against the column params —
            //    no funding BLOB needed (the SQL didn't even fetch it).
            if verdict.is_none() {
                if let (Some(p), Some(sb_hex)) = (&column_params, &r.spender_beef_hex) {
                    verdict = classify_from_columns(
                        p,
                        r.pot_sats,
                        &pot_txid_lc,
                        r.pot_vout,
                        settle,
                        sb_hex,
                    );
                }
            }
            // 3. Full legacy fallback (un-backfilled rows — no decoded
            //    columns): both BEEFs, exactly the pre-#284 path, including
            //    the bare-pot marker-hint refund rule.
            if verdict.is_none() && column_params.is_none() {
                if let (Some(fb_hex), Some(sb_hex)) = (&r.funding_beef_hex, &r.spender_beef_hex) {
                    if let (Some(fb), Some(sb)) = (
                        crate::logic::decode_beef_hex(fb_hex),
                        crate::logic::decode_beef_hex(sb_hex),
                    ) {
                        let funding_raw = crate::logic::extract_raw_tx_hex(&fb, &pot_txid_lc)
                            .and_then(|h| hex::decode(h).ok());
                        let spender_raw = crate::logic::extract_raw_tx_hex(&sb, settle)
                            .and_then(|h| hex::decode(h).ok());
                        if let (Some(fraw), Some(sraw)) = (funding_raw, spender_raw) {
                            verdict = classify_pot_spend(&PotSpendFacts {
                                pot_txid: &pot_txid_lc,
                                pot_vout: r.pot_vout,
                                funding_raw: &fraw,
                                spender_txid: settle,
                                spender_raw: &sraw,
                                marker_recovery_height: Some(r.recovery_height),
                            });
                        }
                    }
                }
            }
            // at.height: the SPV-proven spentHeight column when present,
            // else the spender BEEF's own BUMP — served ONLY when the
            // spender row's VERIFIED latch is set (bsv-low#304: a
            // structural bump on an unverified row can be attacker-
            // fabricated; proofless/unverified BEEFs honestly yield None —
            // never a guess).
            at_height = r.spent_height;
            if at_height.is_none() {
                if let Some(sb) = r
                    .spender_beef_hex
                    .as_deref()
                    .and_then(crate::logic::decode_beef_hex)
                {
                    at_height = verified_beef_block_height(
                        &sb,
                        settle,
                        r.spender_proof_verified == Some(true),
                    );
                }
            }
            // #230: the caller's PROVEN seat, from its v2 marker(s) verified
            // against the pot's OWN committed lock — the decoded columns
            // (the overlay's admission-time decode of those same bytes)
            // first, else the hash-verified funding bytes.
            if verdict.is_some() {
                seat = row_params.and_then(|p| {
                    my_seat(
                        p,
                        &pot_txid_lc,
                        r.pot_vout,
                        identity_lc,
                        seat_markers
                            .get(&(pot_txid_lc.clone(), r.pot_vout))
                            .map(Vec::as_slice)
                            .unwrap_or(&[]),
                    )
                });
            }
        }
        let game_lc = r.game_id.to_ascii_lowercase();
        let markers_for_pot = seat_markers
            .get(&(pot_txid_lc.clone(), r.pot_vout))
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        // THE MONEY-GATING BIT — computed for EVERY row, spent or not, from
        // OUTPOINT-KEYED inputs only. `my_seat` asks "does the caller hold a
        // committed settle key of THIS OUTPOINT", which reads nothing an
        // attacker can write: the params come from the funding lock and the
        // markers are fetched under that lock's own committed keys.
        //
        // It deliberately does NOT consult `game_lc`. Gating on the row's
        // gameId made ONE dust marker (fabricated gameId, earlier `createdAt`)
        // flip an honest row to `Unknown` permanently — Rule 19: dissolving
        // one attacker-writable input by depending on another.
        let pot_binding = PotBinding::from_proof(
            row_params
                .and_then(|p| my_seat(p, &pot_txid_lc, r.pot_vout, identity_lc, markers_for_pot))
                .is_some(),
        );
        // DECORATION — the same predicate over the strictly narrower set of
        // markers that attest THIS row's gameId. Never gates money; the row's
        // gameId is attacker-ownable. `Chain` here implies `Chain` above.
        let game_id_binding = PotBinding::from_proof(
            row_params
                .and_then(|p| {
                    game_id_bound_seat(
                        p,
                        &pot_txid_lc,
                        r.pot_vout,
                        &game_lc,
                        identity_lc,
                        markers_for_pot,
                    )
                })
                .is_some(),
        );
        let opponent_lc = r.opponent_identity.to_ascii_lowercase();
        let game_claims = claims_by_game.get(&game_lc);
        let (outcome, outcome_source) = derive_outcome_with_seat(
            verdict,
            seat,
            identity_lc,
            &opponent_lc,
            settle_lc.as_deref(),
            game_claims,
        );
        let winner_hand = resolve_winner_hand(
            verdict,
            identity_lc,
            &opponent_lc,
            settle_lc.as_deref(),
            game_claims,
        );
        out.push(ResultEntry {
            game_id: game_lc,
            pot_txid: r.pot_txid.to_ascii_lowercase(),
            pot_vout: r.pot_vout,
            recovery_height: r.recovery_height,
            // The COMMITTED height, from the fully-reconstructed params first
            // (a per-request decode of the hash-verified funding lock, which
            // also validated every OTHER committed param), else the overlay's
            // admission-time decode of those same bytes. The two legs cannot
            // disagree for a decoded row — `column_covenant_params` BUILDS
            // `row_params` out of `cov_recovery_height` — so the `.or` only
            // ever covers a row whose stored params are individually present
            // but not jointly reconstructible.
            cov_recovery_height: row_params
                .map(|p| p.recovery_height)
                .or(r.cov_recovery_height)
                .and_then(crate::refund_view::valid_recovery_height),
            opponent_identity: opponent_lc,
            settle_txid: settle_lc,
            spent: r.spent,
            spent_confirmed: r.spent_confirmed,
            pot_binding,
            game_id_binding,
            verdict,
            outcome,
            outcome_source,
            at_height,
            winner_hand,
        });
    }
    out
}

/// Build the claims-by-game map from raw `result_markers_v2` rows — each
/// marker goes through `verified_claim` (real ECDSA over the reconstructed
/// challenge) and an unverifiable one is DROPPED here, so nothing downstream
/// ever sees a claim whose winner signature did not verify.
pub fn claims_by_game(
    markers: &[ResultMarkerRow],
) -> std::collections::HashMap<String, GameClaims> {
    let mut map: std::collections::HashMap<String, GameClaims> = std::collections::HashMap::new();
    for m in markers {
        if let Some(fact) = verified_claim(m) {
            map.entry(m.game_id.to_ascii_lowercase())
                .or_default()
                .claims
                .push(fact);
        }
    }
    map
}

/// Assemble the `/results` wire body:
/// `{"identity","results":[{gameId,potTxid,potVout,recoveryHeight,
/// covRecoveryHeight,opponentIdentity,settleTxid,spent,spentConfirmed,
/// potBinding,potBindingSource,verdict,outcome,outcomeSource,at,hand}]}`.
/// `at` is `{"height": <n|null>}` (block height when the settle's BEEF
/// carries a verified BUMP; time is not tracked).
/// `hand` (bsv-low #245) is the provable showdown —
/// `{winnerIdentity,winnerCardsHex,winnerScore,isTie,loserCardsOnChain,note}`
/// — or `null` when no hand is provable (refund / unrevealed / unresolved).
/// Only the winner's five cards are on-chain; the loser's is never fabricated.
///
/// # ADDITIVE fields — `covRecoveryHeight`, `potBinding`, `potBindingSource`,
/// `gameIdBinding`
///
/// Nothing above them changed name or meaning; `recoveryHeight` is still the
/// caller's MARKER hint verbatim, so a deployed client is byte-unaffected in
/// what it already reads (read-both/write-new, Rule 14).
///
/// The heights are DELIBERATELY NOT MERGED. A client gating a money word
/// ("Recoverable") must be able to distinguish *the chain committed this
/// height* from *a marker claims this height* — and merging them (as
/// `/live-view`'s `served_recovery_height` does, where the value is a
/// countdown and not a money word) would erase exactly that distinction.
///
/// ## What a client may gate a MONEY WORD on
///
/// `potBinding == "chain"` **AND** `covRecoveryHeight != null`. Both.
///
/// - **`covRecoveryHeight` alone is NOT sufficient.** It is an honest fact
///   about the outpoint the ROW NAMES, and an attacker names the outpoint. A
///   row filed by a stranger against their OWN covenant pot serves that pot's
///   real committed height.
/// - **`potBinding == "chain"` does NOT vouch for the row.** It covers
///   `(potTxid, potVout, identity)` and the chain facts about that outpoint —
///   see the scope table on [`PotBinding`]. On a `"chain"` row,
///   **`recoveryHeight` and `opponentIdentity` remain attacker-owned**, and so
///   does `gameId` unless `gameIdBinding == "chain"`. Reading "chain-bound" as
///   row integrity is the predictable next defect; it is demonstrated in the
///   suite, not hypothesised.
/// - **`gameIdBinding` must never gate money.** It is decoration (which local
///   game record to reconcile against). Gating on it reintroduces exactly the
///   1-dust-marker permanent denial that the [`PotBinding`] doc records.
///
/// ## The range check on `covRecoveryHeight` is NOT a security control
///
/// It only converts an UNUSABLE value (0, or the nLockTime timestamp range) to
/// `null` so no fake countdown is rendered — the same rule `/refund-view` and
/// `/live-view` apply. A hostile lock can commit ANY in-range value: a genuine
/// covenant pot committing `recoveryHeight: 1` serves `covRecoveryHeight: 1`,
/// and it is `potBinding` — not the range — that keeps that pot out of the
/// caller's money word.
pub fn results_body(identity: &str, entries: &[ResultEntry]) -> String {
    let arr: Vec<serde_json::Value> = entries
        .iter()
        .map(|e| {
            let (binding, binding_source) = e.pot_binding.pair();
            json!({
                "gameId": e.game_id,
                "potTxid": e.pot_txid,
                "potVout": e.pot_vout,
                // UNCHANGED: the marker's UNVERIFIED hint (attacker-writable).
                "recoveryHeight": e.recovery_height,
                // NEW: the COVENANT-COMMITTED height decoded from the pot's
                // own funding lock, or null. Never the marker's value.
                "covRecoveryHeight": e.cov_recovery_height,
                // NEW (honesty pair, same shape as outcome/outcomeSource):
                // does the caller PROVABLY hold a committed settle key of
                // this POT OUTPOINT? "chain" = yes, by a seatSig under a key
                // this pot's own lock committed plus the identity's own
                // signature; "unknown" = not proven (first-class, never
                // optimistic). Scope table on `PotBinding` — this does NOT
                // vouch for recoveryHeight / opponentIdentity / gameId.
                "potBinding": binding,
                "potBindingSource": binding_source,
                // NEW, DECORATION ONLY: does a marker clearing those same
                // bars also attest THIS row's gameId? Never gate money on
                // it — the row's gameId is attacker-ownable for one dust
                // marker, which is why it is not part of `potBinding`.
                "gameIdBinding": e.game_id_binding.as_str(),
                "opponentIdentity": e.opponent_identity,
                "settleTxid": e.settle_txid,
                "spent": e.spent,
                "spentConfirmed": e.spent_confirmed,
                "verdict": e.verdict.map(PotVerdict::as_str),
                "outcome": e.outcome.as_str(),
                "outcomeSource": e.outcome_source,
                "at": { "height": e.at_height },
                // The showdown (#245): the winner's five cards + low-sum, or
                // null when no hand is provable. `loserCardsOnChain` is always
                // false — the loser's hand is never revealed for a settle, and
                // is never fabricated here. `note` explains the caveat for the
                // client's honest "—" rendering.
                "hand": e.winner_hand.as_ref().map(|h| json!({
                    "winnerIdentity": h.identity,
                    "winnerCardsHex": h.cards_hex,
                    "winnerScore": h.score,
                    "isTie": h.is_tie,
                    "loserCardsOnChain": false,
                    "note": if h.is_tie {
                        "tie — both sums are equal; only the revealed side's five \
                         cards are on-chain (the other player's hand is not)"
                    } else {
                        "only the winner's five cards are revealed on-chain; the \
                         loser's hand is not (do not fabricate it)"
                    },
                })),
            })
        })
        .collect();
    json!({ "identity": identity, "results": arr }).to_string()
}

/// The `/results` potparty join SQL: the caller's marker rows JOINed to the
/// pot spend status plus BOTH stored BEEFs (funding keyed by potTxid,
/// spender keyed by spendingTxid). Bounded at [`RESULTS_MAX_ROWS`] (the D1
/// work + BLOB transfer bound — the >50-outpoint 503 lesson).
///
/// # The window is dust-DoS-bounded (bsv-low #281)
///
/// SHAPE THIS REPLACES: `WHERE pp.identity = ? ORDER BY pp.createdAt DESC,
/// pp.rowid DESC LIMIT 100`. `tm_potparty` admission is BYTE-FORMAT-ONLY (the
/// overlay is an index, not an authority), so ANYONE can file a marker naming
/// ANY identity for one dust `OP_RETURN`. ~110 junk rows then pushed the
/// victim's REAL pot — including a chain-proven tower-enforced WIN — entirely
/// off `/results`. Proven against real SQLite: 120 attacker rows ⇒ 100
/// returned, the victim's pot present ZERO times. The cheapest variant needs
/// no forgery: re-broadcast the victim's OWN on-chain marker bytes (a
/// different tx ⇒ a different outpoint ⇒ a different row).
///
/// Two structural bounds, both deterministic:
///
///  1. **Per-POT-OUTPOINT collapse** — `ROW_NUMBER() OVER (PARTITION BY
///     pp.potTxid, pp.potVout …) = 1`. The window counts POTS, not rows, so
///     one pot can never consume it and the replay variant dies outright. The
///     representative is the OLDEST marker for the pot (`createdAt ASC,
///     rowid ASC`): an honest seat publishes at funding, and oldest-first is
///     the only order an attacker cannot win by simply publishing later.
///     Partitioning on the full OUTPOINT (not just the txid) matches every
///     other key in the system — two genuine pots sharing a funding txid are
///     not reachable in LOW today, but collapsing them would be silent
///     erasure if they ever were.
///  2. **Existence tier with a RESERVED QUOTA** — a row whose pot outpoint is
///     absent from `pot_records` normally sorts after every row whose pot
///     exists, so markers naming INVENTED pots (free, unlimited, each its own
///     partition — the collapse alone does NOT stop them) cannot displace a
///     real one. But a strict tier silently becomes a FILTER once `LIMIT`
///     binds: with 100 indexed pots, a genuinely fresh pot whose `tm_pot`
///     admission is still in flight (or a legacy pre-pot-index escrow — see
///     `/spent-any`) fell off the answer entirely. So the newest
///     [`RESULTS_UNKNOWN_POT_QUOTA`] unknown pots are PROMOTED into the main
///     tier and compete on recency; the rest stay demoted. Ghost rows are
///     therefore bounded to a small reserved slice instead of the whole page,
///     and a real-but-unindexed pot is not erased.
///
/// Ordering within a tier is by the POT's own admission stamp
/// (`pot_records.createdAt` — an attacker cannot backdate or advance it by
/// filing markers), falling back to the marker stamp when the pot is unknown,
/// then the marker `rowid` as a total-order tiebreak. EVERY level carries an
/// explicit `ORDER BY`.
///
/// HONESTY NOTE on the OUTERMOST `ORDER BY` (after the `pot_beefs` join): it
/// is insurance, and it is the one ordering guarantee here with no
/// behavioural test. SQLite cannot reorder a `LEFT JOIN`'s left side, so
/// deleting it does not change the answer under any plan we can force — it is
/// pinned by the structural test only. Every OTHER ordering rule in this
/// query has a test that goes RED when it is removed (see
/// `tests/results_window_sqlite.rs`).
///
/// The `pot_beefs` joins run on the OUTER select — AFTER the window and the
/// `LIMIT` have pruned to at most [`RESULTS_MAX_ROWS`] rows. Inside the
/// subquery they would have been evaluated for every matching marker row, so
/// each dust replay naming the victim's real pot would have dragged the real
/// funding BEEF along with it (the >50-outpoint 503 lesson: never let an
/// attacker multiply BLOB work).
///
/// # This query no longer carries the seat proof
///
/// It selects `seatSettlePubkey` / `seatSigHex` / `sigHex` for continuity,
/// but `/results` NO LONGER derives the caller's seat from these rows: one
/// row per pot cannot serve both duties, and any ordering rule that picked
/// the "v2-looking" row was front-runnable (a forged v2 with an earlier
/// `createdAt` would have owned the slot and dropped the genuine proof —
/// making a tower-enforced win erasable for ONE dust marker instead of ~110).
/// The seat proof comes from [`seat_markers_sql`] instead, bound to the pot's
/// OWN COMMITTED KEYS, where a forged key cannot enter the result set at all.
/// See `routes::results`.
///
/// EFFECTIVE CAP: [`RESULTS_MAX_ROWS`] DISTINCT POTS per request (one row
/// each, so the BLOB-weight bound is unchanged). A player with 100 real pots
/// still sees all 100.
///
/// # Residual — stated plainly, because the improvement is modest
///
/// This does NOT make the window expensive to fill. An attacker who copies
/// 100 REAL, recently-admitted pot txids out of the very index being queried
/// — they are public — and files one marker per pot naming the victim
/// achieves total erasure at the SAME ~100-dust cost as before. The honest
/// net gain is narrower than "an admitted marker per slot": it is
/// **from "any 110 junk rows" to "110 junk rows naming real, recent pot
/// txids"**, plus the outright death of the zero-forgery replay variant and
/// of free invented-pot flooding. Closing the rest requires the marker's
/// IDENTITY SIGNATURE to be verified before the row COUNTS; the overlay
/// verifies nothing by doctrine (the READER verifies), and #230 verifies
/// before the row ATTRIBUTES, which is what keeps the VERDICT correct even
/// when the display window is stuffed.
///
/// Second residual (the re-gate's F1b): the representative row supplies the
/// DISPLAY fields — `gameId`, `opponentIdentity`, `recoveryHeight`. For a pot
/// backfilled long after its txid became public (#252), an attacker CAN file
/// a marker with an earlier `createdAt` and own those fields. What that costs
/// is the CLAIMS fallback for that pot (the claims lookup keys on `gameId`);
/// it costs nothing on the paths that decide money, because the pot outpoint,
/// the chain verdict and the SEAT PROOF all come from elsewhere — the
/// `pot_records` join and the committed-key-bound [`seat_markers_sql`] fetch.
/// Pre-#281 the `(gameId, potTxid, potVout)` dedupe kept both rows and the
/// honest one survived to the claims lookup, so this IS a real (if narrow)
/// regression on the fallback path, traded for the window bound.
pub fn results_sql() -> String {
    // The #284 decoded pot_records columns, threaded verbatim through every
    // window level (pot_records.recoveryHeight is aliased covRecoveryHeight
    // — the potparty marker owns the bare `recoveryHeight` name).
    const DECODED: &str = "lockKind, pubA, pubB, pubTower, payPkhA, payPkhB, rakePkh, \
         stakeA, stakeB, feeSats, covRecoveryHeight, potSats, \
         verdict, verdictTxid, spentHeight";
    format!(
        // L4 — BEEF join, on the ≤{rows} survivors only (never inside the
        // window, where each dust replay would drag the real BLOBs along).
        //
        // #284 FALLBACK-ONLY BLOBs: the joins are CONDITION-GATED so a row
        // whose decoded columns already answer the question transfers NO
        // BLOB at all —
        //  - fundingBeef only when the decoded params are absent
        //    (`w.pubA IS NULL`): covers legacy un-backfilled rows AND bare
        //    pots (whose per-request marker-hint classification stays);
        //  - spenderBeef only when there IS a spender AND the stored verdict
        //    cannot be trusted for it (NULL verdict / NULL-or-different
        //    verdictTxid — explicit NULL handling, since `verdictTxid <>
        //    spendingTxid` is NULL-opaque) OR the proven height is missing
        //    (the spender BEEF is the at.height fallback for spends whose
        //    confirm hasn't landed).
        //
        // bsv-low#304: `sb.proof_verified` rides the spender join — the
        // at.height fallback is served only when the overlay's verifying
        // writers latched it. DEPLOY ORDER: the column comes from the
        // overlay worker's additive migration, so the overlay deploys (and
        // runs its migrations) BEFORE this worker — the same ordering every
        // prior additive column here required.
        "SELECT w.identity AS identity, w.gameId AS gameId, w.potTxid AS potTxid, \
                w.potVout AS potVout, w.recoveryHeight AS recoveryHeight, \
                w.opponentIdentity AS opponentIdentity, \
                w.seatSettlePubkey AS seatSettlePubkey, w.seatSigHex AS seatSigHex, \
                w.sigHex AS sigHex, \
                w.spent AS spent, w.spendingTxid AS spendingTxid, \
                w.spentConfirmed AS spentConfirmed, \
                w.lockKind AS lockKind, w.pubA AS pubA, w.pubB AS pubB, \
                w.pubTower AS pubTower, w.payPkhA AS payPkhA, w.payPkhB AS payPkhB, \
                w.rakePkh AS rakePkh, w.stakeA AS stakeA, w.stakeB AS stakeB, \
                w.feeSats AS feeSats, w.covRecoveryHeight AS covRecoveryHeight, \
                w.potSats AS potSats, w.verdict AS verdict, \
                w.verdictTxid AS verdictTxid, w.spentHeight AS spentHeight, \
                hex(fb.beef) AS fundingBeef, hex(sb.beef) AS spenderBeef, \
                sb.proof_verified AS spenderProofVerified \
         FROM (SELECT identity, gameId, potTxid, potVout, recoveryHeight, \
                  opponentIdentity, seatSettlePubkey, seatSigHex, sigHex, \
                  spent, spendingTxid, spentConfirmed, {DECODED}, \
                  markerCreatedAt, markerRowid, potCreatedAt, \
                  CASE WHEN unknownPot = 0 OR potRank <= {quota} THEN 0 ELSE 1 END AS tier \
           FROM (SELECT identity, gameId, potTxid, potVout, recoveryHeight, \
                    opponentIdentity, seatSettlePubkey, seatSigHex, sigHex, \
                    spent, spendingTxid, spentConfirmed, {DECODED}, \
                    markerCreatedAt, markerRowid, potCreatedAt, unknownPot, \
                    ROW_NUMBER() OVER (PARTITION BY unknownPot \
                                       ORDER BY COALESCE(potCreatedAt, markerCreatedAt) DESC, \
                                                markerCreatedAt DESC, markerRowid DESC) AS potRank \
             FROM (SELECT pp.identity AS identity, pp.gameId AS gameId, \
                      pp.potTxid AS potTxid, pp.potVout AS potVout, \
                      pp.recoveryHeight AS recoveryHeight, \
                      pp.opponentIdentity AS opponentIdentity, \
                      pp.seatSettlePubkey AS seatSettlePubkey, \
                      pp.seatSigHex AS seatSigHex, pp.sigHex AS sigHex, \
                      r.spent AS spent, r.spendingTxid AS spendingTxid, \
                      r.spentConfirmed AS spentConfirmed, \
                      r.lockKind AS lockKind, r.pubA AS pubA, r.pubB AS pubB, \
                      r.pubTower AS pubTower, r.payPkhA AS payPkhA, \
                      r.payPkhB AS payPkhB, r.rakePkh AS rakePkh, \
                      r.stakeA AS stakeA, r.stakeB AS stakeB, \
                      r.feeSats AS feeSats, \
                      r.recoveryHeight AS covRecoveryHeight, \
                      r.potSats AS potSats, r.verdict AS verdict, \
                      r.verdictTxid AS verdictTxid, r.spentHeight AS spentHeight, \
                      pp.createdAt AS markerCreatedAt, pp.rowid AS markerRowid, \
                      r.createdAt AS potCreatedAt, \
                      CASE WHEN r.txid IS NULL THEN 1 ELSE 0 END AS unknownPot, \
                      ROW_NUMBER() OVER (PARTITION BY pp.potTxid, pp.potVout \
                                         ORDER BY pp.createdAt ASC, pp.rowid ASC) AS rn \
               FROM potparty_records pp \
               LEFT JOIN pot_records r \
                      ON r.txid = pp.potTxid AND r.outputIndex = pp.potVout \
               WHERE pp.identity = ?) \
             WHERE rn = 1) \
           ORDER BY tier ASC, COALESCE(potCreatedAt, markerCreatedAt) DESC, \
                    markerCreatedAt DESC, markerRowid DESC \
           LIMIT {rows}) w \
         LEFT JOIN pot_beefs fb ON w.pubA IS NULL AND fb.txid = lower(w.potTxid) \
         LEFT JOIN pot_beefs sb ON w.spendingTxid IS NOT NULL \
              AND (w.verdict IS NULL OR w.verdictTxid IS NULL \
                   OR w.verdictTxid <> w.spendingTxid OR w.spentHeight IS NULL) \
              AND sb.txid = lower(w.spendingTxid) \
         ORDER BY w.tier ASC, COALESCE(w.potCreatedAt, w.markerCreatedAt) DESC, \
                  w.markerCreatedAt DESC, w.markerRowid DESC",
        quota = RESULTS_UNKNOWN_POT_QUOTA,
        rows = RESULTS_MAX_ROWS,
    )
}

/// The #284 decoded-column fetch for a chunk of pot outpoints (2 binds each
/// — chunk at [`crate::logic::D1_CHUNK_OUTPOINTS`] like `batch_where_sql`):
/// the `/leaderboard` classification partition reads verdict + params as
/// PURE COLUMNS here, and only rows this query cannot answer fall back to
/// the legacy BLOB path (which stays capped). `recoveryHeight` is aliased
/// `covRecoveryHeight` for shape-parity with `results_sql`.
pub fn decoded_pots_sql(n: usize) -> String {
    debug_assert!(n >= 1);
    let clause = vec!["(txid = ? AND outputIndex = ?)"; n].join(" OR ");
    format!(
        "SELECT txid, outputIndex, spendingTxid, lockKind, pubA, pubB, pubTower, \
                payPkhA, payPkhB, rakePkh, stakeA, stakeB, feeSats, \
                recoveryHeight AS covRecoveryHeight, potSats, \
                verdict, verdictTxid, spentHeight \
         FROM pot_records WHERE {clause}"
    )
}

/// Hard bound on `/results` per request. Since bsv-low #281 the window is
/// per-POT-OUTPOINT (`results_sql`'s `PARTITION BY potTxid, potVout`), so
/// this is a cap on DISTINCT POTS — and, one row per pot, still the same
/// ≤100-row BLOB-weight bound it always was.
pub const RESULTS_MAX_ROWS: usize = 100;

/// How many of the newest pots ABSENT from `pot_records` are promoted into
/// the main `/results` tier instead of being demoted behind every indexed
/// pot (bsv-low #281 F3).
///
/// A strict existence tier silently becomes a FILTER the moment `LIMIT`
/// binds: 100 indexed pots plus one genuinely fresh pot whose `tm_pot`
/// admission is still in flight returned the 100 and dropped the fresh one —
/// exactly the pot a recovering client most needs. Reserving a slice keeps
/// that pot visible while still bounding how much of the page free,
/// invented-pot rows can ever occupy.
pub const RESULTS_UNKNOWN_POT_QUOTA: usize = 10;

const _: () = assert!(RESULTS_UNKNOWN_POT_QUOTA < RESULTS_MAX_ROWS);

/// Cap on v2 seat-marker rows fetched per COMMITTED KEY SLOT (per
/// `(potTxid, seatSettlePubkey)` partition) for the `/leaderboard`
/// attribution join. Honest traffic is exactly ONE row per slot (each seat
/// publishes its own marker under its own committed key); the headroom
/// absorbs benign duplicates (the sweep's content-idempotent republish).
///
/// # KNOWN residual — eviction at exactly this bar (bsv-low #283c)
///
/// The committed settle keys are PUBLIC on-chain, so the `IN (?,?)` key
/// binding is a PREFILTER, not a barrier: an attacker can file well-formed
/// markers UNDER the honest seat's own committed key, and exactly
/// [`SEAT_MARKERS_PER_KEY`] forged rows stamped ahead of the honest one
/// evict it from the window (threshold executed in the #281 gate: 7 junk →
/// honest survives, 8 → evicted). Fail-safe — attribution is then OMITTED,
/// never wrong (the verify pass drops every non-verifying row).
///
/// DO NOT half-fix this in SQL: no window size or sort order closes it —
/// the row that must win is "the one whose `seatSigHex` VERIFIES", which
/// SQL cannot compute. The real fixes are admission-side (rate-limit /
/// price marker admission under a given key — an owner decision against
/// the byte-format-only doctrine) or a verify-on-read pass over a
/// deliberately wider window when attribution comes back empty. Tracked in
/// bsv-low#283; widening this constant only moves the executed threshold.
///
/// This is the `/results` cap (a PER-IDENTITY view — the caller holds its own
/// key↔identity mapping, so eviction only degrades ITS OWN attribution tier,
/// with the countersigned claim path intact — see the sweep note in the #332
/// v3 module doc). The `/leaderboard` uses [`LEADERBOARD_SEAT_CANDIDATES`]
/// instead, because there the eviction was a PUBLIC concern until #332 v3
/// decoupled the win from the attribution.
pub const SEAT_MARKERS_PER_KEY: usize = 8;

/// The `/leaderboard` seat-marker candidate cap (#332 v3). WIDE, because the
/// board reads a superset per committed key and `attribute_seats` then
/// VALIDITY-FILTERS it — so a junk flood must exceed this whole cap (a dust tx
/// per row) merely to push the identity DISPLAY to the settle key, never to
/// erase the (chain-counted) win. Set well above a realistic per-key flood;
/// beyond it the identity honestly degrades to UNKNOWN (settle-key display),
/// never no-win. Cost is bounded: at most this many small rows per committed
/// key, and a flood large enough to fill it costs the attacker that many
/// on-chain dust markers.
pub const LEADERBOARD_SEAT_CANDIDATES: usize = 64;

/// Pots per `seat_markers_sql` chunk. FOUR binds per pot (potTxid, potVout,
/// pubA, pubB) — 24 × 4 = 96, under D1's 100-bound-param cap.
///
/// `potVout` became a BIND (it was a hardcoded `LEADERBOARD_POT_VOUT`) when
/// `/results` started using this query too (bsv-low #281 F1): `/results` is
/// not vout-0-only, and a seat proof must be bound to the OUTPOINT being
/// attributed. `/leaderboard` passes `LEADERBOARD_POT_VOUT`, so its behaviour
/// is unchanged.
pub const SEAT_MARKERS_CHUNK_POTS: usize = 24;

/// Binds per pot in [`seat_markers_sql`]: `(potTxid, potVout, pubA, pubB)`.
pub const SEAT_MARKERS_BINDS_PER_POT: usize = 4;

const _: () = assert!(
    SEAT_MARKERS_CHUNK_POTS * SEAT_MARKERS_BINDS_PER_POT <= crate::logic::D1_MAX_BOUND_PARAMS
);

/// The `/leaderboard` seat-marker query for a chunk of `n` pots, each bound
/// as the triple `(potTxid, pubA, pubB)` — the pot's OWN committed settle
/// keys, read from its funding lock by the caller (F2, 2026-07-28 gate;
/// corrected 2026-07-28 second gate).
///
/// THE REAL INVARIANT (an earlier revision's premise was FALSE): this query
/// must NOT rely on row ORDER to find the honest markers. The #252
/// opportunistic backfill (`potPartyRepublish.ts`) publishes v2 markers for
/// pots whose txid has been PUBLIC FOR WEEKS, so an attacker CAN land junk
/// rows with an EARLIER `createdAt` than an honest marker — and rows are
/// never deleted, so an `ORDER BY createdAt ASC` window keeps that junk
/// forever (measured: 45 junk rows + 2 honest ⇒ 40 junk, 0 honest fetched).
///
/// What actually holds regardless of junk volume or timing:
/// - a row can only enter the result set if its `seatSettlePubkey` is one of
///   the two keys THIS pot's covenant lock committed (bound per pot, from
///   the hash-verified funding bytes) — every other key is filtered out in
///   SQL, so junk under an arbitrary key is free to exist and irrelevant;
/// - the window partitions by `(potTxid, seatSettlePubkey)`, so each
///   committed key gets its OWN slot: junk piled on seat B's key can never
///   starve seat A's marker, and vice versa;
/// - [`attribute_seats`] then requires BOTH signatures (seat + identity) on
///   whatever comes back, so a fetched junk row attributes nothing.
///
/// Residual (documented, not defended in SQL): the committed pubkeys are
/// public, so a determined spammer can copy the honest seat's OWN key and
/// crowd that one slot's window. That costs a dust tx per row, is bounded
/// to failing to CREDIT one pot's attribution (never a wrong attribution —
/// the signature bars are unconditional), and leaves the countersigned
/// claim path untouched. Distinguishing junk from honest inside SQL is
/// impossible (verification needs ECDSA); the honest fix if it is ever
/// observed is admission-side rate limiting, not a bigger window.
pub fn seat_markers_sql(n: usize, cap: usize) -> String {
    debug_assert!(n >= 1);
    debug_assert!(cap >= 1);
    let per_pot =
        vec!["(potTxid = ? AND potVout = ? AND seatSettlePubkey IN (?, ?))"; n].join(" OR ");
    format!(
        "SELECT identity, opponentIdentity, gameId, potTxid, potVout, \
                recoveryHeight, seatSettlePubkey, seatSigHex, sigHex \
         FROM (SELECT identity, opponentIdentity, gameId, potTxid, potVout, \
                      recoveryHeight, seatSettlePubkey, seatSigHex, sigHex, \
                      ROW_NUMBER() OVER (PARTITION BY potTxid, potVout, seatSettlePubkey \
                                         ORDER BY createdAt ASC, rowid ASC) AS rn \
               FROM potparty_records \
               WHERE seatSettlePubkey IS NOT NULL AND ({per_pot})) \
         WHERE rn <= {cap} \
         ORDER BY potTxid ASC, potVout ASC, seatSettlePubkey ASC, rn ASC",
    )
}

/// One pot's slice of a [`seat_markers_sql`] chunk: the OUTPOINT plus the two
/// COMMITTED settle keys read from that pot's own funding lock. Exists so the
/// chunking + bind construction is testable WITHOUT a Worker — `routes.rs` has
/// no test harness, and the 2026-07-28 re-gate showed the whole delivery of
/// the committed-key fetch could be deleted silently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeatMarkerBind {
    pub pot_txid: String,
    pub pot_vout: u32,
    /// `pubA` from the covenant params, lowercase hex.
    pub pub_a_hex: String,
    /// `pubB` from the covenant params, lowercase hex.
    pub pub_b_hex: String,
}

/// The EXACT chunk list `routes::results_seat_markers` issues one
/// [`seat_markers_sql`] query per — deterministically ordered (the answer must
/// never depend on `HashMap` iteration order) and sized so every chunk stays
/// under D1's bound-parameter ceiling.
///
/// INVARIANTS the tests enforce behaviourally (not by reading this text):
///  - every pot in `params_by_pot` appears in EXACTLY ONE chunk — no pot is
///    silently dropped at a chunk boundary, which is how a whole page of seat
///    proofs could go missing without a single test failing;
///  - `chunk.len() * SEAT_MARKERS_BINDS_PER_POT <= D1_MAX_BOUND_PARAMS`, so a
///    chunk can never exceed what D1 will bind;
///  - the order is a pure function of the pot outpoints.
pub fn seat_marker_chunks(
    params_by_pot: &std::collections::HashMap<(String, u32), CovenantParams>,
) -> Vec<Vec<SeatMarkerBind>> {
    let mut pots: Vec<(&(String, u32), &CovenantParams)> = params_by_pot.iter().collect();
    pots.sort_unstable_by(|a, b| a.0.cmp(b.0));
    pots.chunks(SEAT_MARKERS_CHUNK_POTS)
        .map(|chunk| {
            chunk
                .iter()
                .map(|((pot_txid, pot_vout), p)| SeatMarkerBind {
                    pot_txid: pot_txid.clone(),
                    pot_vout: *pot_vout,
                    pub_a_hex: hex::encode(p.pub_a),
                    pub_b_hex: hex::encode(p.pub_b),
                })
                .collect()
        })
        .collect()
}

/// The claims query for a chunk of gameIds (1 bind each — chunk at
/// [`crate::logic::D1_CHUNK_OUTPOINTS`] to stay far under D1's 100-param cap).
pub fn claims_sql(n: usize) -> String {
    debug_assert!(n >= 1);
    let placeholders = vec!["?"; n].join(",");
    format!(
        "SELECT gameId, winner, loser, potTxid, settleTxid, winnerSigHex, \
         loserSigHex, cardsHex, txid, createdAt FROM result_markers_v2 \
         WHERE gameId IN ({placeholders}) ORDER BY createdAt DESC, rowid DESC LIMIT 1000"
    )
}

// ── /spent-any — server-side legacy outpoint spend reads ────────────────────
//
// (bsv-low #227 addendum.) LEGACY (pre-pot-index) escrow outpoints were never
// admitted to `pot_records`, so `/utxo-status` answers `known:false` forever
// and the browser used to fall back to direct WhatsOnChain calls — slow,
// rate-limited, CORS-hostile. `/spent-any` answers spend status for ARBITRARY
// outpoints by querying the upstream providers SERVER-SIDE, with the
// proof-source-order doctrine applied:
//
//  - POSITIVE (a spender exists): WoC's pointer alone is accepted ONLY after
//    RAW VERIFICATION — the spender's raw bytes are fetched, hash-checked
//    against the reported txid, and input-matched to the requested outpoint.
//    A pointer that fails verification is a provider fault → `known:false`.
//  - NEGATIVE (unspent): NEVER concluded from WoC alone. A second provider
//    (Bitails) must cleanly corroborate "unspent"; any fault/ambiguity on
//    either side → `known:false` (honest unknown, the caller's existing
//    fail-safe shape). NOTE: Bitails' outpoint-spent endpoint was faulting
//    (HTTP 500) at build time — until it recovers, negatives surface as
//    `known:false`, which is exactly the fail-safe the doctrine demands.
//
// Responses reuse the `/utxo-status` row shape so the client parser is
// shared. A short in-isolate cache (~15 s) bounds upstream pressure.

/// Hard cap on `/spent-any` outpoints per request (each may cost up to two
/// upstream subrequests — bound the fan-out).
pub const SPENT_ANY_MAX_OUTPOINTS: usize = 20;

/// In-isolate cache TTL for `/spent-any` entries, milliseconds.
pub const SPENT_ANY_CACHE_TTL_MS: f64 = 15_000.0;

/// One provider observation for an outpoint, already shape-validated by the
/// route glue. The pure decision logic below is what unit tests pin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpentObservation {
    /// WoC 200: a spender txid (lowercase hex) + whether WoC reports the
    /// spend confirmed.
    Spent { txid: String, confirmed: bool },
    /// WoC 404/410: "unspent or not yet indexed" — a real, if weak, answer.
    NotSpent,
    /// Transport / 5xx / rate-limit / auth / malformed body — we could not
    /// look. NEVER an answer about the outpoint.
    Fault,
}

/// Map a NON-200 WoC `/spent` status to an observation (#323 HIGH-2).
///
/// Pure so it can be pinned: the route glue previously inlined
/// `(400..500) => NotSpent`, which swallowed **429** — the single
/// most-documented outage in this repo — as "unspent". That is a fault
/// consumed as information: a genuinely spent outpoint under a WoC
/// rate-limit reported `uncorroborated-unspent` with confidence, and the
/// type's own doc four lines up already said rate-limit belongs in `Fault`.
/// Comment and code disagreed; the code was wrong.
///
/// Only ABSENCE codes are an answer. Everything else — rate-limit, auth,
/// malformed request, 5xx, transport — is "we could not look".
pub fn woc_spent_status_observation(status: u16) -> SpentObservation {
    match status {
        // The outpoint is genuinely absent from the index.
        404 | 410 => SpentObservation::NotSpent,
        // 429 rate-limit, 401/403 auth, 400 malformed, 5xx, anything else.
        _ => SpentObservation::Fault,
    }
}

/// Bitails' corroboration of an UNSPENT claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnspentCorroboration {
    /// Bitails cleanly indicated the outpoint is unspent.
    ConfirmedUnspent,
    /// Bitails faulted / answered ambiguously / contradicted.
    Unknown,
}

/// The `/spent-any` decision table (pure — unit-tested with mock upstream
/// observations). `spender_raw_ok` = the spender raw was fetched, hashed to
/// the reported txid, and input-matched to the outpoint.
pub fn decide_spent_any(
    woc: &SpentObservation,
    spender_raw_ok: bool,
    bitails_unspent: UnspentCorroboration,
) -> crate::logic::OutpointStatus {
    // Filled in by the route with the real outpoint; the decision only sets
    // the known/spent/spender fields, so use a placeholder outpoint here.
    let op = crate::logic::Outpoint {
        txid: String::new(),
        vout: 0,
    };
    match woc {
        SpentObservation::Spent { txid, confirmed } => {
            if spender_raw_ok {
                crate::logic::OutpointStatus::known(&op, true, Some(txid.clone()), *confirmed)
            } else {
                // Unverifiable pointer → honest unknown, never a bare claim.
                // #323 defect 2: say WHY. A CONFIRMED spend whose raw could
                // not be fetched (WoC 429 — a documented operational fact
                // here — or a Bitails fault) lands in this arm, and without
                // a reason it is indistinguishable from "nothing there".
                crate::logic::OutpointStatus::unknown_because(
                    &op,
                    crate::logic::SPENT_ANY_REASON_UNVERIFIED_SPENDER,
                )
            }
        }
        SpentObservation::NotSpent => match bitails_unspent {
            UnspentCorroboration::ConfirmedUnspent => {
                crate::logic::OutpointStatus::known(&op, false, None, false)
            }
            UnspentCorroboration::Unknown => crate::logic::OutpointStatus::unknown_because(
                &op,
                crate::logic::SPENT_ANY_REASON_UNCORROBORATED,
            ),
        },
        SpentObservation::Fault => crate::logic::OutpointStatus::unknown_because(
            &op,
            crate::logic::SPENT_ANY_REASON_PROVIDER_FAULT,
        ),
    }
}

/// Verify a fetched spender raw: hashes to `spender_txid` AND spends
/// `(outpoint_txid, vout)`. The one-provider-positive rule rests on this.
pub fn spender_raw_verifies(
    raw: &[u8],
    spender_txid: &str,
    outpoint_txid: &str,
    vout: u32,
) -> bool {
    match parse_raw_tx_verified(raw, spender_txid) {
        Some(tx) => tx
            .inputs
            .iter()
            .any(|i| i.prev_txid.eq_ignore_ascii_case(outpoint_txid) && i.prev_vout == vout),
        None => false,
    }
}

/// Parse a WoC `/tx/{txid}/{vout}/spent` 200 body: `{"txid": "...",
/// "status": "confirmed"|...}`. Strict: a malformed txid is a Fault.
pub fn parse_woc_spent_body(v: &serde_json::Value) -> SpentObservation {
    let Some(txid) = v.get("txid").and_then(|t| t.as_str()) else {
        return SpentObservation::Fault;
    };
    if !crate::logic::valid_txid(txid) {
        return SpentObservation::Fault;
    }
    let confirmed = v.get("status").and_then(|s| s.as_str()) == Some("confirmed");
    SpentObservation::Spent {
        txid: txid.to_ascii_lowercase(),
        confirmed,
    }
}

/// Parse a Bitails outpoint-spent response into an unspent corroboration.
/// STRICT: only an explicit, well-formed `{"spent": false}` counts as a
/// clean unspent signal; everything else (their current 500 fault, unknown
/// shapes, `spent:true` — which would CONTRADICT WoC's negative) is Unknown.
pub fn parse_bitails_unspent(status: u16, v: Option<&serde_json::Value>) -> UnspentCorroboration {
    if status == 200 {
        if let Some(val) = v {
            if val.get("spent").and_then(serde_json::Value::as_bool) == Some(false) {
                return UnspentCorroboration::ConfirmedUnspent;
            }
        }
    }
    UnspentCorroboration::Unknown
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::logic::ResultMarkerRow;

    /// `assemble_results` driven EXACTLY as `routes::results` drives it: the
    /// params map resolved by the SHIPPED `covenant_params_by_pot` over the
    /// same rows, never hand-built. A hand-built map would let a cell diverge
    /// from the only real producer (Rule 6b), which is the whole reason the
    /// map is an argument rather than a per-row re-derivation.
    fn assemble_like_the_route(
        identity_lc: &str,
        rows: Vec<ResultsRow>,
        claims: &std::collections::HashMap<String, GameClaims>,
        seat_markers: &std::collections::HashMap<(String, u32), Vec<SeatMarkerRow>>,
    ) -> Vec<ResultEntry> {
        let params_by_pot = covenant_params_by_pot(&rows);
        assemble_results(identity_lc, rows, claims, seat_markers, &params_by_pot)
    }

    fn ident(b: u8) -> String {
        format!("02{}", format!("{b:02x}").repeat(32))
    }
    fn tx(b: u8) -> String {
        format!("{b:02x}").repeat(32)
    }

    // ── outcome derivation ─────────────────────────────────────────────

    /// Post-verification claim facts (winner, loser, settle, loser_verified)
    /// — the shape `claims_by_game` emits AFTER real signature verification.
    /// The adversarial tests further below go through the REAL producer
    /// (`claims_by_game` over signed markers); these tuples unit-test the
    /// outcome table itself.
    fn claims_of(entries: &[(&str, &str, &str, bool)]) -> GameClaims {
        GameClaims {
            claims: entries
                .iter()
                .map(|(w, l, s, lv)| ClaimFact {
                    winner: w.to_string(),
                    loser: l.to_string(),
                    settle_txid: s.to_string(),
                    loser_sig_verified: *lv,
                    cards_hex: None,
                })
                .collect(),
        }
    }

    #[test]
    fn tie_and_refund_are_seat_symmetric_chain_truth() {
        let me = ident(0xaa);
        let opp = ident(0xbb);
        // No claims needed at all — pure chain truth.
        let (o, src) = derive_outcome(Some(PotVerdict::Tie), &me, &opp, None, None);
        assert_eq!((o, src), (Outcome::Tie, Some("chain")));
        let (o, src) = derive_outcome(Some(PotVerdict::Refund), &me, &opp, None, None);
        assert_eq!((o, src), (Outcome::Refund, Some("chain")));
    }

    #[test]
    fn winner_verdict_upgrades_only_on_unanimous_corroborating_claims() {
        let me = ident(0xaa);
        let opp = ident(0xbb);
        let settle = tx(0x22);

        // No claims → unresolved (the chain alone cannot name the seat's
        // identity — the module-doc seat→identity truth).
        let (o, src) = derive_outcome(Some(PotVerdict::WinnerA), &me, &opp, Some(&settle), None);
        assert_eq!((o, src), (Outcome::Unresolved, None));

        // A unanimous countersigned claim naming ME for THIS settle → won.
        let gc = claims_of(&[(&me, &opp, &settle, true)]);
        let (o, src) = derive_outcome(
            Some(PotVerdict::WinnerA),
            &me,
            &opp,
            Some(&settle),
            Some(&gc),
        );
        assert_eq!((o, src), (Outcome::Won, Some("chain+claim")));

        // The same claim from the OPPONENT's perspective → lost (its OWN
        // countersig verified).
        let (o, src) = derive_outcome(
            Some(PotVerdict::WinnerA),
            &opp,
            &me,
            Some(&settle),
            Some(&gc),
        );
        assert_eq!((o, src), (Outcome::Lost, Some("chain+claim")));

        // Conflicting claims (both parties claim the same settle) → nobody.
        let gc = claims_of(&[(&me, &opp, &settle, true), (&opp, &me, &settle, true)]);
        let (o, _) = derive_outcome(
            Some(PotVerdict::WinnerA),
            &me,
            &opp,
            Some(&settle),
            Some(&gc),
        );
        assert_eq!(o, Outcome::Unresolved);

        // A winner-sig-only claim (no verified countersig): the WINNER's tier
        // is earned (its own verified key), but the LOSER is NEVER shown a
        // loss it did not countersign.
        let gc = claims_of(&[(&me, &opp, &settle, false)]);
        let (o, src) = derive_outcome(
            Some(PotVerdict::WinnerA),
            &me,
            &opp,
            Some(&settle),
            Some(&gc),
        );
        assert_eq!((o, src), (Outcome::Won, Some("chain+claim")));
        let (o, src) = derive_outcome(
            Some(PotVerdict::WinnerA),
            &opp,
            &me,
            Some(&settle),
            Some(&gc),
        );
        assert_eq!((o, src), (Outcome::Unresolved, None));

        // A countersig by a THIRD PARTY (claim's loser is not the caller)
        // never shows the caller a loss.
        let gc = claims_of(&[(&me, &ident(0xcc), &settle, true)]);
        let (o, _) = derive_outcome(
            Some(PotVerdict::WinnerA),
            &opp,
            &me,
            Some(&settle),
            Some(&gc),
        );
        assert_eq!(o, Outcome::Unresolved);

        // A claim naming a DIFFERENT settle never corroborates this one.
        let gc = claims_of(&[(&me, &opp, &tx(0x33), true)]);
        let (o, _) = derive_outcome(
            Some(PotVerdict::WinnerA),
            &me,
            &opp,
            Some(&settle),
            Some(&gc),
        );
        assert_eq!(o, Outcome::Unresolved);

        // A claimed winner OUTSIDE the two parties → unresolved (a foreign
        // marker can't award this pot to anyone).
        let gc = claims_of(&[(&ident(0xcc), &ident(0xdd), &settle, true)]);
        let (o, _) = derive_outcome(
            Some(PotVerdict::WinnerA),
            &me,
            &opp,
            Some(&settle),
            Some(&gc),
        );
        assert_eq!(o, Outcome::Unresolved);

        // No verdict at all → unresolved even with a pretty claim (a claim
        // alone NEVER makes a server-derived result — the owner directive).
        let gc = claims_of(&[(&me, &opp, &settle, true)]);
        let (o, _) = derive_outcome(None, &me, &opp, Some(&settle), Some(&gc));
        assert_eq!(o, Outcome::Unresolved);
    }

    // ── #230 seat attribution (REAL secp256k1 keys + REAL BRC-42 identity
    //    signatures — the risk-register B1/B5 bars + the 2026-07-28 gate's
    //    F1; never a mocked verify) ────────────────────────────────────────

    /// A real settle keypair: (privkey, 66-hex compressed pubkey).
    fn real_key(seed: u8) -> (bsv_rs::primitives::ec::PrivateKey, String) {
        let k = bsv_rs::primitives::ec::PrivateKey::from_bytes(&{
            let mut b = [0u8; 32];
            b[31] = seed;
            b
        })
        .unwrap();
        let pk = k.public_key().to_hex();
        (k, pk.to_ascii_lowercase())
    }

    /// Sign the v2 IDENTITY challenge exactly as the client does
    /// (`[1,'low potparty']`, keyID = gameId, counterparty 'anyone').
    fn sign_potparty_identity(w: &ProtoWallet, game_id: &str, challenge: &[u8]) -> String {
        let sig = w
            .create_signature(CreateSignatureArgs {
                data: Some(challenge.to_vec()),
                hash_to_directly_sign: None,
                protocol_id: potparty_protocol(),
                key_id: game_id.to_string(),
                counterparty: Some(Counterparty::Anyone),
            })
            .unwrap();
        hex::encode(sig.signature)
    }

    /// A FULLY REAL v2 seat marker: `seat_sig` is a genuine ECDSA signature
    /// by `settle_key` over sha256 of the exact cross-repo preimage, and the
    /// identity signature is a genuine BRC-42 'anyone' signature by
    /// `identity_wallet` over the exact v2 challenge.
    #[allow(clippy::too_many_arguments)]
    fn real_seat_marker(
        settle_key: &bsv_rs::primitives::ec::PrivateKey,
        settle_pub_hex: &str,
        identity_wallet: &ProtoWallet,
        identity_hex: &str,
        opponent_hex: &str,
        game_id: &str,
        pot_txid: &str,
        pot_vout: u32,
    ) -> SeatMarkerRow {
        let preimage = seatsig_preimage(game_id, pot_txid, pot_vout, identity_hex).unwrap();
        let hash = bsv_rs::primitives::hash::sha256(&preimage);
        let seat_sig = settle_key.sign(&hash).unwrap();
        let mut m = SeatMarkerRow {
            identity: identity_hex.to_string(),
            opponent_identity: opponent_hex.to_string(),
            game_id: game_id.to_string(),
            pot_txid: pot_txid.to_string(),
            pot_vout,
            recovery_height: 900_000,
            seat_settle_pubkey: settle_pub_hex.to_string(),
            seat_sig_hex: hex::encode(seat_sig.to_der()),
            identity_sig_hex: String::new(),
        };
        let challenge = potparty_v2_challenge(&m).unwrap();
        m.identity_sig_hex = sign_potparty_identity(identity_wallet, game_id, &challenge);
        m
    }

    /// CovenantParams committing the two settle pubkeys (rest arbitrary).
    fn params_with_keys(pub_a_hex: &str, pub_b_hex: &str) -> CovenantParams {
        let mut pub_a = [0u8; 33];
        pub_a.copy_from_slice(&hex::decode(pub_a_hex).unwrap());
        let mut pub_b = [0u8; 33];
        pub_b.copy_from_slice(&hex::decode(pub_b_hex).unwrap());
        CovenantParams {
            pub_a,
            pub_b,
            pub_tower: [2u8; 33],
            pay_pkh_a: [0xaa; 20],
            pay_pkh_b: [0xbb; 20],
            rake_pkh: [0xcc; 20],
            stake_a: 500,
            stake_b: 500,
            fee_sats: 10,
            recovery_height: 900_000,
        }
    }

    #[test]
    fn seat_attribution_real_roundtrip_and_slot_exactness() {
        let (ka, pa) = real_key(41); // seat A settle key (committed as pubA)
        let (kb, pb) = real_key(42); // seat B settle key
        let wa = wallet_of(0x51); // seat A identity wallet
        let wb = wallet_of(0x52);
        let ida = identity_of(&wa);
        let idb = identity_of(&wb);
        let gid = tx(0x01);
        let pot = tx(0x22);
        let p = params_with_keys(&pa, &pb);

        let ma = real_seat_marker(&ka, &pa, &wa, &ida, &idb, &gid, &pot, 0);
        let mb = real_seat_marker(&kb, &pb, &wb, &idb, &ida, &gid, &pot, 0);
        assert!(verify_seat_marker(&ma), "a genuine seat sig verifies");
        assert!(
            verify_identity_binding(&ma),
            "a genuine identity sig verifies"
        );
        let attr = attribute_seats(&p, &pot, 0, &[ma.clone(), mb.clone()]);
        assert_eq!(attr.identity_a.as_deref(), Some(ida.as_str()));
        assert_eq!(attr.identity_b.as_deref(), Some(idb.as_str()));
        assert_eq!(attr.winner_for(PotVerdict::WinnerA), Some(ida.as_str()));
        assert_eq!(attr.winner_for(PotVerdict::WinnerB), Some(idb.as_str()));
        assert_eq!(attr.winner_for(PotVerdict::Tie), None);
        assert_eq!(attr.winner_for(PotVerdict::Refund), None);

        // BOTH-SEATS-CLAIM-SEAT-A (risk register): B's key can only ever
        // match its OWN lock slot — even if B "claims seat A" socially, the
        // bytes put it in slot B. And B CANNOT occupy slot A: a marker
        // pairing pubA with B's identity needs a signature by A's key over
        // B's identity, which only A can mint. Simulate the best theft
        // available (copy A's marker, swap the identity): the seat sig no
        // longer verifies and the marker is refused.
        let stolen = SeatMarkerRow {
            identity: idb.clone(),
            ..ma.clone()
        };
        assert!(!verify_seat_marker(&stolen), "preimage binds the identity");
        let attr = attribute_seats(&p, &pot, 0, &[stolen, mb.clone()]);
        assert_eq!(attr.identity_a, None, "slot A stays unattributed");
        assert_eq!(attr.identity_b.as_deref(), Some(idb.as_str()));

        // Replayed identical markers (outpoint-dup) are idempotent.
        let attr = attribute_seats(&p, &pot, 0, &[ma.clone(), ma.clone(), mb]);
        assert_eq!(attr.identity_a.as_deref(), Some(ida.as_str()));

        // My seat, from my own markers only (the /results consumer).
        assert_eq!(
            my_seat(&p, &pot, 0, &ida, std::slice::from_ref(&ma)),
            Some(SeatLetter::A)
        );
        assert_eq!(my_seat(&p, &pot, 0, &idb, &[ma]), None);
    }

    /// F1 (2026-07-28 gate, HIGH): a spiteful LOSER mints a byte-valid v2
    /// marker naming the WINNER's identity over its OWN committed settle key
    /// — its wallet happily seat-signs the preimage embedding the winner's
    /// identity, so the seatSig is GENUINE. Without the identity-binding
    /// check this landed the winner in BOTH slots, `my_seat` returned None,
    /// and the enforced win fell back to `unresolved` — the exact injustice
    /// #230 ships to fix, re-opened by dust. The forged marker must be
    /// REFUSED (the loser cannot mint the WINNER's identity signature) and
    /// the winner's attribution must survive untouched.
    #[test]
    fn f1_loser_forged_identity_marker_cannot_erase_the_winner() {
        let (ka, pa) = real_key(41); // winner (seat A)
        let (kb, pb) = real_key(42); // loser (seat B)
        let w_winner = wallet_of(0x51);
        let w_loser = wallet_of(0x52);
        let winner = identity_of(&w_winner);
        let loser = identity_of(&w_loser);
        let gid = tx(0x01);
        let pot = tx(0x22);
        let p = params_with_keys(&pa, &pb);

        // The winner's own honest marker.
        let honest = real_seat_marker(&ka, &pa, &w_winner, &winner, &loser, &gid, &pot, 0);

        // The forgery: identity = WINNER, key = the LOSER's committed pubB,
        // seatSig GENUINELY made by the loser's settle key over the preimage
        // binding the winner's identity. The best identity sig the loser can
        // attach is one by its OWN identity wallet (or garbage) — it cannot
        // sign as the winner.
        let mut forged = SeatMarkerRow {
            identity: winner.clone(),
            opponent_identity: loser.clone(),
            game_id: gid.clone(),
            pot_txid: pot.clone(),
            pot_vout: 0,
            recovery_height: 900_000,
            seat_settle_pubkey: pb.clone(),
            seat_sig_hex: {
                let preimage = seatsig_preimage(&gid, &pot, 0, &winner).unwrap();
                let hash = bsv_rs::primitives::hash::sha256(&preimage);
                hex::encode(kb.sign(&hash).unwrap().to_der())
            },
            identity_sig_hex: String::new(),
        };
        let challenge = potparty_v2_challenge(&forged).unwrap();
        forged.identity_sig_hex = sign_potparty_identity(&w_loser, &gid, &challenge);

        // The seatSig alone DOES verify (that is the whole attack)…
        assert!(verify_seat_marker(&forged), "the forged seatSig is genuine");
        // …but the identity binding refuses it: the sig is not the WINNER's.
        assert!(!verify_identity_binding(&forged));

        // Attribution: slot B stays clean of the forgery; the winner's slot
        // A survives; my_seat still answers A — the /results outcome stays
        // the correct chain+seatkey WIN.
        let markers = vec![honest.clone(), forged];
        let attr = attribute_seats(&p, &pot, 0, &markers);
        assert_eq!(attr.identity_a.as_deref(), Some(winner.as_str()));
        assert_eq!(attr.identity_b, None, "the forgery occupies nothing");
        assert_eq!(my_seat(&p, &pot, 0, &winner, &markers), Some(SeatLetter::A));
        let (o, src) = derive_outcome_with_seat(
            Some(PotVerdict::WinnerA),
            my_seat(&p, &pot, 0, &winner, &markers),
            &winner,
            &loser,
            Some(&tx(0x33)),
            None,
        );
        assert_eq!((o, src), (Outcome::Won, Some("chain+seatkey")));
    }

    #[test]
    fn seat_attribution_refusals_are_exhaustive() {
        let (ka, pa) = real_key(41);
        let (_kb, pb) = real_key(42);
        let (kf, pf) = real_key(43); // a FOREIGN key, not in the lock
        let wa = wallet_of(0x51);
        let ida = identity_of(&wa);
        let opp = ident(0xbb);
        let gid = tx(0x01);
        let pot = tx(0x22);
        let p = params_with_keys(&pa, &pb);

        // (B1) a marker whose settle pubkey is NOT in the pot's lock is
        // refused at read — even with perfectly valid signatures.
        let foreign = real_seat_marker(&kf, &pf, &wa, &ida, &opp, &gid, &pot, 0);
        assert!(verify_seat_marker(&foreign), "the sig itself is fine…");
        let attr = attribute_seats(&p, &pot, 0, &[foreign]);
        assert_eq!(attr, SeatAttribution::default(), "…but the lock refuses it");

        // A marker whose seatSig does not verify is refused (tampered sig).
        let mut bad = real_seat_marker(&ka, &pa, &wa, &ida, &opp, &gid, &pot, 0);
        let mut sig = hex::decode(&bad.seat_sig_hex).unwrap();
        let last = sig.len() - 1;
        sig[last] ^= 0x01;
        bad.seat_sig_hex = hex::encode(sig);
        assert!(!verify_seat_marker(&bad));
        assert_eq!(
            attribute_seats(&p, &pot, 0, &[bad]),
            SeatAttribution::default()
        );

        // A marker whose IDENTITY sig does not verify is refused (F1) —
        // genuine seatSig, garbage identity sig.
        let mut no_id = real_seat_marker(&ka, &pa, &wa, &ida, &opp, &gid, &pot, 0);
        no_id.identity_sig_hex = "30".repeat(35);
        assert!(verify_seat_marker(&no_id));
        assert!(!verify_identity_binding(&no_id));
        assert_eq!(
            attribute_seats(&p, &pot, 0, &[no_id]),
            SeatAttribution::default()
        );

        // …and the identity sig binds EVERY challenge field: a re-targeted
        // recoveryHeight (not covered by the seatSig preimage) also refuses.
        let mut retarget = real_seat_marker(&ka, &pa, &wa, &ida, &opp, &gid, &pot, 0);
        retarget.recovery_height += 1;
        assert!(verify_seat_marker(&retarget)); // seat preimage unaffected
        assert!(!verify_identity_binding(&retarget)); // challenge tampered
        assert_eq!(
            attribute_seats(&p, &pot, 0, &[retarget]),
            SeatAttribution::default()
        );

        // A marker for a DIFFERENT pot outpoint contributes nothing to this
        // one (and a marker for a NONEXISTENT pot simply never joins — there
        // is no params/verdict for it to attribute).
        let other_pot = real_seat_marker(&ka, &pa, &wa, &ida, &opp, &gid, &tx(0x33), 0);
        assert!(verify_seat_marker(&other_pot));
        assert_eq!(
            attribute_seats(&p, &pot, 0, std::slice::from_ref(&other_pot)),
            SeatAttribution::default()
        );
        let wrong_vout = real_seat_marker(&ka, &pa, &wa, &ida, &opp, &gid, &pot, 1);
        assert_eq!(
            attribute_seats(&p, &pot, 0, &[wrong_vout]),
            SeatAttribution::default()
        );

        // Malformed fields never panic, never verify.
        let mut junk = other_pot;
        junk.seat_settle_pubkey = "zz".repeat(33);
        assert!(!verify_seat_marker(&junk));
        assert!(seatsig_preimage("nothex", &pot, 0, &ida).is_none());
        assert!(seatsig_preimage(&gid, &pot, 0, "02aabb").is_none()); // short identity

        // CONFLICTING identities for one slot (two fully-verified markers by
        // the SAME key naming different identities — only the key holder can
        // mint both seatSigs, and each identity signs its own marker, i.e.
        // coordinated self-sabotage): the slot poisons to None.
        let wc = wallet_of(0x53);
        let idc = identity_of(&wc);
        let m1 = real_seat_marker(&ka, &pa, &wa, &ida, &opp, &gid, &pot, 0);
        let m2 = real_seat_marker(&ka, &pa, &wc, &idc, &opp, &gid, &pot, 0);
        assert!(verify_seat_marker(&m2) && verify_identity_binding(&m2));
        let attr = attribute_seats(&p, &pot, 0, &[m1, m2]);
        assert_eq!(attr.identity_a, None, "conflicting slot poisons");

        // A degenerate lock (pubA == pubB) attributes NOTHING.
        let p_degen = params_with_keys(&pa, &pa);
        let m = real_seat_marker(&ka, &pa, &wa, &ida, &opp, &gid, &pot, 0);
        assert_eq!(
            attribute_seats(&p_degen, &pot, 0, &[m]),
            SeatAttribution::default()
        );
    }

    /// The CROSS-REPO crypto round-trip: the client's FROZEN golden v2 marker
    /// (real `@bsv/sdk` ProtoWallet output, pinned in `potParty.test.ts` and
    /// in `overlay-discovery`'s parser tests) must verify under THIS crate's
    /// server-side crypto — BOTH signatures: the seatSig (plain ECDSA, single
    /// sha256) AND the identity signature (BRC-42 'anyone' derivation over
    /// the v2 challenge) — proving the preimage/challenge layouts and hash
    /// conventions match the wallet byte-for-byte.
    #[test]
    fn golden_client_v2_marker_verifies_server_side() {
        const GOLDEN_V2_HEX: &str = "006a0f4c4f572f706f7470617274792f7632210279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f817982102c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee520cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc20dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd04010000000428a00e002103d3e37fc9edbd1c225d703873b45f66368e86c633cb613252b3254ffe0b8ad5ee4630440220106a632f58753f6b9ebaf20d105874d3aed43c28dab90e8b6a8a51dbd610e1e402204c7837248995842ec551eb3c8510b5862f87bf0c54368534fd3d7c1e3b9a50fd473045022100d3ea901d46fa588cb2f20e0bb0a3c7e23f6320138efee69f9e506a8e79abbaa102207cfccbd475e5d9e789091acdfa7d81503b950ebf51da6a1ac9fec44c84553773";
        let script = hex::decode(GOLDEN_V2_HEX).unwrap();
        let m = overlay_discovery::potparty::parse_potparty_marker(&script)
            .expect("the golden v2 marker parses");
        let row = SeatMarkerRow {
            identity: hex::encode(&m.identity),
            opponent_identity: hex::encode(&m.opponent),
            game_id: hex::encode(m.game_id),
            pot_txid: hex::encode(m.pot_txid),
            pot_vout: m.pot_vout,
            recovery_height: m.recovery_height,
            seat_settle_pubkey: hex::encode(m.seat_settle_pubkey.as_ref().unwrap()),
            seat_sig_hex: hex::encode(m.seat_sig.as_ref().unwrap()),
            identity_sig_hex: hex::encode(&m.sig),
        };
        assert!(
            verify_seat_marker(&row),
            "the client's REAL wallet-derived seat signature must verify server-side"
        );
        assert!(
            verify_identity_binding(&row),
            "the client's REAL identity signature must verify over the reconstructed v2 challenge"
        );
        // Tamper the identity → BOTH bindings refuse.
        let mut swapped = row.clone();
        swapped.identity = hex::encode(&m.opponent);
        assert!(!verify_seat_marker(&swapped));
        assert!(!verify_identity_binding(&swapped));
        // Tamper a challenge-only field → the identity binding refuses.
        let mut rh = row;
        rh.recovery_height += 1;
        assert!(!verify_identity_binding(&rh));
    }

    #[test]
    fn derive_outcome_with_seat_precedence_and_fallback() {
        let me = ident(0xaa);
        let opp = ident(0xbb);
        let settle = tx(0x22);

        // Seat proof decides won/lost with NO claims at all (#276 fixed).
        let (o, src) = derive_outcome_with_seat(
            Some(PotVerdict::WinnerA),
            Some(SeatLetter::A),
            &me,
            &opp,
            Some(&settle),
            None,
        );
        assert_eq!((o, src), (Outcome::Won, Some("chain+seatkey")));
        let (o, src) = derive_outcome_with_seat(
            Some(PotVerdict::WinnerA),
            Some(SeatLetter::B),
            &me,
            &opp,
            Some(&settle),
            None,
        );
        assert_eq!((o, src), (Outcome::Lost, Some("chain+seatkey")));
        let (o, src) = derive_outcome_with_seat(
            Some(PotVerdict::WinnerB),
            Some(SeatLetter::B),
            &me,
            &opp,
            Some(&settle),
            None,
        );
        assert_eq!((o, src), (Outcome::Won, Some("chain+seatkey")));

        // The seat proof outranks a conflicting claim set (which alone would
        // yield unresolved).
        let gc = claims_of(&[(&me, &opp, &settle, true), (&opp, &me, &settle, true)]);
        let (o, src) = derive_outcome_with_seat(
            Some(PotVerdict::WinnerA),
            Some(SeatLetter::A),
            &me,
            &opp,
            Some(&settle),
            Some(&gc),
        );
        assert_eq!((o, src), (Outcome::Won, Some("chain+seatkey")));

        // No seat proof → the claim-corroboration rules verbatim.
        let gc = claims_of(&[(&me, &opp, &settle, true)]);
        let (o, src) = derive_outcome_with_seat(
            Some(PotVerdict::WinnerA),
            None,
            &me,
            &opp,
            Some(&settle),
            Some(&gc),
        );
        assert_eq!((o, src), (Outcome::Won, Some("chain+claim")));

        // A seat proof NEVER manufactures an outcome without a verdict, and
        // tie/refund stay pure chain truth (seat-symmetric).
        let (o, _) = derive_outcome_with_seat(None, Some(SeatLetter::A), &me, &opp, None, None);
        assert_eq!(o, Outcome::Unresolved);
        let (o, src) = derive_outcome_with_seat(
            Some(PotVerdict::Tie),
            Some(SeatLetter::A),
            &me,
            &opp,
            None,
            None,
        );
        assert_eq!((o, src), (Outcome::Tie, Some("chain")));
    }

    // ── assemble_results (BEEF-fed producer path) ──────────────────────

    /// Wrap a raw tx in a minimal (unproven) BEEF, as uppercase hex — the
    /// SQLite `hex()` read-back shape the route feeds in.
    fn beef_hex_of(raw: &[u8]) -> String {
        let tx = bsv_rs::transaction::Transaction::from_binary(raw).unwrap();
        let mut beef = bsv_rs::transaction::Beef::new();
        beef.merge_transaction(tx);
        hex::encode(beef.to_binary()).to_ascii_uppercase()
    }

    /// Minimal Bitcoin data push (the covenant param region uses direct
    /// pushes only — `read_param_pushes`).
    fn param_push(blob: &[u8]) -> Vec<u8> {
        let mut v = vec![blob.len() as u8];
        v.extend_from_slice(blob);
        v
    }

    /// Minimal script-number push for a non-negative u64 (LE, sign-guarded)
    /// — mirrors the client builder's `push_minimal_int`.
    fn param_push_num(n: u64) -> Vec<u8> {
        if n == 0 {
            return vec![0x00];
        }
        if n <= 16 {
            return vec![0x50 + n as u8];
        }
        let mut b = Vec::new();
        let mut v = n;
        while v > 0 {
            b.push((v & 0xff) as u8);
            v >>= 8;
        }
        if b.last().unwrap() & 0x80 != 0 {
            b.push(0x00);
        }
        param_push(&b)
    }

    /// A REAL `Poc5TemplatePot` covenant lock: the frozen template's fixed
    /// HEAD + the 10 param pushes + the fixed TAIL (the exact bytes
    /// `is_pot_covenant_script` recognizes and the classifier reads).
    fn covenant_lock(p: &CovenantParams) -> Vec<u8> {
        let t = overlay_discovery::pot::POC5_TEMPLATE_HEX;
        let head = hex::decode(&t[..t.find('<').unwrap()]).unwrap();
        let tail = hex::decode(&t[t.rfind('>').unwrap() + 1..]).unwrap();
        let mut s = head;
        s.extend(param_push(&p.pub_a));
        s.extend(param_push(&p.pub_b));
        s.extend(param_push(&p.pub_tower));
        s.extend(param_push(&p.pay_pkh_a));
        s.extend(param_push(&p.pay_pkh_b));
        s.extend(param_push(&p.rake_pkh));
        s.extend(param_push_num(p.stake_a));
        s.extend(param_push_num(p.stake_b));
        s.extend(param_push_num(p.fee_sats));
        s.extend(param_push_num(p.recovery_height));
        s.extend(tail);
        s
    }

    /// Serialize a bare raw tx: one input, the given outputs, a locktime.
    fn raw_tx(
        prev_txid_hex: &str,
        prev_vout: u32,
        sequence: u32,
        outs: &[(u64, Vec<u8>)],
        lock_time: u32,
    ) -> Vec<u8> {
        fn varint(v: &mut Vec<u8>, n: usize) {
            if n < 0xfd {
                v.push(n as u8);
            } else {
                v.push(0xfd);
                v.push((n & 0xff) as u8);
                v.push(((n >> 8) & 0xff) as u8);
            }
        }
        let mut v = Vec::new();
        v.extend_from_slice(&1u32.to_le_bytes());
        varint(&mut v, 1);
        let mut prev = hex::decode(prev_txid_hex).unwrap();
        prev.reverse();
        v.extend_from_slice(&prev);
        v.extend_from_slice(&prev_vout.to_le_bytes());
        varint(&mut v, 0);
        v.extend_from_slice(&sequence.to_le_bytes());
        varint(&mut v, outs.len());
        for (sats, script) in outs {
            v.extend_from_slice(&sats.to_le_bytes());
            varint(&mut v, script.len());
            v.extend_from_slice(script);
        }
        v.extend_from_slice(&lock_time.to_le_bytes());
        v
    }

    /// F1 END-TO-END through the REAL `/results` producer path
    /// (`assemble_results` over a genuine covenant lock): the winner's own
    /// v2 marker row plus the loser's FORGED identity=WINNER row (genuine
    /// seatSig by the loser's committed key, loser-signed identity sig) —
    /// the winner's `/results` STILL answers the correct `chain+seatkey`
    /// win. Before the F1 fix this exact input erased the win to
    /// `unresolved` via the both-slots collision.
    #[test]
    fn f1_results_end_to_end_forged_row_cannot_erase_the_chain_seatkey_win() {
        let (ka, pa) = real_key(41);
        let (kb, pb) = real_key(42);
        let w_winner = wallet_of(0x51);
        let w_loser = wallet_of(0x52);
        let winner = identity_of(&w_winner);
        let loser = identity_of(&w_loser);
        let gid = tx(0x01);
        let params = params_with_keys(&pa, &pb);

        // A REAL covenant pot: funding vout 0 = the template lock over the
        // committed params, value = stakeA + stakeB.
        let lock = covenant_lock(&params);
        let f_raw = raw_tx(&"11".repeat(32), 0, 0xffff_ffff, &[(1000, lock)], 0);
        let f_id = bsv_rs::transaction::Transaction::from_binary(&f_raw)
            .unwrap()
            .id();
        // The winner-A template spend: rake 10 → rakePkh, 980 → payPkhA.
        let outs = vec![
            (10u64, p2pkh_lock(&params.rake_pkh)),
            (980u64, p2pkh_lock(&params.pay_pkh_a)),
        ];
        let s_raw = raw_tx(&f_id, 0, 0xffff_ffff, &outs, 0);
        let s_id = bsv_rs::transaction::Transaction::from_binary(&s_raw)
            .unwrap()
            .id();

        // The winner's HONEST v2 marker fields.
        let honest = real_seat_marker(&ka, &pa, &w_winner, &winner, &loser, &gid, &f_id, 0);
        // The loser's FORGED row: identity = WINNER, its own pubB + genuine
        // seatSig over the winner-bound preimage, loser-signed identity sig.
        let mut forged = SeatMarkerRow {
            identity: winner.clone(),
            opponent_identity: loser.clone(),
            game_id: gid.clone(),
            pot_txid: f_id.clone(),
            pot_vout: 0,
            recovery_height: 900_000,
            seat_settle_pubkey: pb.clone(),
            seat_sig_hex: {
                let pre = seatsig_preimage(&gid, &f_id, 0, &winner).unwrap();
                hex::encode(
                    kb.sign(&bsv_rs::primitives::hash::sha256(&pre))
                        .unwrap()
                        .to_der(),
                )
            },
            identity_sig_hex: String::new(),
        };
        forged.identity_sig_hex =
            sign_potparty_identity(&w_loser, &gid, &potparty_v2_challenge(&forged).unwrap());

        let row_of = |m: &SeatMarkerRow| ResultsRow {
            identity: m.identity.clone(),
            game_id: gid.clone(),
            pot_txid: f_id.clone(),
            pot_vout: 0,
            recovery_height: m.recovery_height,
            opponent_identity: m.opponent_identity.clone(),
            spent: Some(true),
            spending_txid: Some(s_id.clone()),
            spent_confirmed: Some(true),
            funding_beef_hex: Some(beef_hex_of(&f_raw)),
            spender_beef_hex: Some(beef_hex_of(&s_raw)),
            seat_settle_pubkey: Some(m.seat_settle_pubkey.clone()),
            seat_sig_hex: Some(m.seat_sig_hex.clone()),
            marker_sig_hex: Some(m.identity_sig_hex.clone()),
            ..Default::default()
        };

        // NO claims at all — the loser is gone (the #276 shape).
        let rows = vec![row_of(&honest), row_of(&forged)];
        let entries = assemble_like_the_route(
            &winner,
            rows,
            &std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
        );
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].verdict, Some(PotVerdict::WinnerA));
        assert_eq!(
            entries[0].outcome,
            Outcome::Won,
            "the forged row must not erase the win"
        );
        assert_eq!(entries[0].outcome_source, Some("chain+seatkey"));

        // And from the LOSER's side its own seat proof honestly shows the
        // loss (its marker, its key, the chain's verdict).
        let loser_marker = real_seat_marker(&kb, &pb, &w_loser, &loser, &winner, &gid, &f_id, 0);
        let entries = assemble_like_the_route(
            &loser,
            vec![row_of(&loser_marker)],
            &std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
        );
        assert_eq!(entries[0].outcome, Outcome::Lost);
        assert_eq!(entries[0].outcome_source, Some("chain+seatkey"));
    }

    // ══════════════════════════════════════════════════════════════════
    // FIX A (2026-07-28 re-gate finding #3) — the HEADLINE mechanism.
    //
    // The re-gate ran 20 one-at-a-time source mutations and found the ENTIRE
    // delivery of the committed-key seat fetch into the assembler could be
    // deleted with the whole suite still green: `routes.rs` has no test
    // harness, so keying the seat map by gameId, discarding the injected map,
    // or dropping a chunk all passed silently. That is exactly the blind spot
    // #230 had. These tests close it.
    // ══════════════════════════════════════════════════════════════════

    /// (i) + (ii): the genuine seat proof arrives ONLY through the injected
    /// `seat_markers_by_pot` map, and the ROW that represents the pot carries
    /// a DIFFERENT gameId (an attacker front-ran the display row of a
    /// backfilled pot — the residual `results_sql` documents).
    ///
    /// RED for BOTH mutations the gate found:
    ///  - keying the seat map by gameId (insert or lookup): the row's gameId
    ///    no longer matches the marker's, the lookup misses, and the win
    ///    degrades to `unresolved` — the #276 injustice;
    ///  - discarding the injected map: there is no other source, same result.
    #[test]
    fn the_injected_seat_map_is_keyed_by_outpoint_and_actually_reaches_the_assembler() {
        let (ka, pa) = real_key(41);
        let (_kb, pb) = real_key(42);
        let w_winner = wallet_of(0x51);
        let w_loser = wallet_of(0x52);
        let winner = identity_of(&w_winner);
        let loser = identity_of(&w_loser);
        let gid = tx(0x01);
        let params = params_with_keys(&pa, &pb);

        let lock = covenant_lock(&params);
        let f_raw = raw_tx(&"11".repeat(32), 0, 0xffff_ffff, &[(1000, lock)], 0);
        let f_id = bsv_rs::transaction::Transaction::from_binary(&f_raw)
            .unwrap()
            .id();
        let outs = vec![
            (10u64, p2pkh_lock(&params.rake_pkh)),
            (980u64, p2pkh_lock(&params.pay_pkh_a)),
        ];
        let s_raw = raw_tx(&f_id, 0, 0xffff_ffff, &outs, 0);
        let s_id = bsv_rs::transaction::Transaction::from_binary(&s_raw)
            .unwrap()
            .id();

        // The winner's GENUINE v2 marker, over its real gameId.
        let honest = real_seat_marker(&ka, &pa, &w_winner, &winner, &loser, &gid, &f_id, 0);

        // The row that survived the per-pot window names a DIFFERENT gameId —
        // and carries NO seat columns, so `rows` cannot supply the proof.
        let other_game = tx(0x99);
        let row = ResultsRow {
            identity: winner.clone(),
            game_id: other_game.clone(),
            pot_txid: f_id.clone(),
            pot_vout: 0,
            recovery_height: 900_000,
            opponent_identity: loser.clone(),
            spent: Some(true),
            spending_txid: Some(s_id.clone()),
            spent_confirmed: Some(true),
            funding_beef_hex: Some(beef_hex_of(&f_raw)),
            spender_beef_hex: Some(beef_hex_of(&s_raw)),
            seat_settle_pubkey: None,
            seat_sig_hex: None,
            marker_sig_hex: None,
            ..Default::default()
        };

        // The fetch delivers the proof keyed by POT OUTPOINT.
        let mut injected: std::collections::HashMap<(String, u32), Vec<SeatMarkerRow>> =
            std::collections::HashMap::new();
        injected.insert((f_id.to_ascii_lowercase(), 0), vec![honest]);

        let entries = assemble_like_the_route(
            &winner,
            vec![row],
            &std::collections::HashMap::new(), // zero claims — the #276 shape
            &injected,
        );
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].verdict, Some(PotVerdict::WinnerA));
        assert_eq!(
            entries[0].outcome,
            Outcome::Won,
            "the seat proof must reach the assembler through the injected map, \
             keyed by the POT OUTPOINT — not by the row's (forgeable) gameId"
        );
        assert_eq!(entries[0].outcome_source, Some("chain+seatkey"));
    }

    /// (iii) chunk-boundary coverage: EVERY pot must land in exactly one
    /// chunk, and no chunk may exceed D1's bind ceiling. RED for "drop the
    /// final chunk" and for any change to `SEAT_MARKERS_CHUNK_POTS`.
    #[test]
    fn seat_marker_chunks_cover_every_pot_at_the_boundaries() {
        let c = SEAT_MARKERS_CHUNK_POTS;
        for n in [1, c - 1, c, c + 1, 2 * c - 1, 2 * c, 2 * c + 1] {
            let mut params_by_pot = std::collections::HashMap::new();
            for i in 0..n {
                params_by_pot.insert(
                    (format!("{:064x}", i), (i % 3) as u32),
                    params_with_keys(&real_key(41).1, &real_key(42).1),
                );
            }
            let chunks = seat_marker_chunks(&params_by_pot);
            // No pot dropped, none duplicated.
            let mut seen: Vec<(String, u32)> = chunks
                .iter()
                .flatten()
                .map(|b| (b.pot_txid.clone(), b.pot_vout))
                .collect();
            let total = seen.len();
            seen.sort();
            seen.dedup();
            assert_eq!(total, n, "n={n}: every pot queried exactly once");
            assert_eq!(seen.len(), n, "n={n}: no duplicates");
            let mut want: Vec<(String, u32)> = params_by_pot.keys().cloned().collect();
            want.sort();
            assert_eq!(seen, want, "n={n}: the queried set IS the pot set");
            // Chunk count is a function of the chunk size — pins it in BOTH
            // directions (a smaller chunk size costs subrequests, a larger one
            // blows D1's bind cap).
            assert_eq!(chunks.len(), n.div_ceil(c), "n={n}: chunk count");
            for chunk in &chunks {
                assert!(!chunk.is_empty(), "n={n}: no empty chunk");
                assert!(
                    chunk.len() * SEAT_MARKERS_BINDS_PER_POT <= crate::logic::D1_MAX_BOUND_PARAMS,
                    "n={n}: a chunk must never exceed D1's bind ceiling"
                );
                // The SQL built for this chunk binds exactly what we supply.
                assert_eq!(
                    seat_markers_sql(chunk.len(), SEAT_MARKERS_PER_KEY).matches('?').count(),
                    chunk.len() * SEAT_MARKERS_BINDS_PER_POT,
                    "n={n}: bind arity matches the chunk"
                );
            }
        }
    }

    /// The chunk SIZE itself. Changing it is a PERFORMANCE change, not a
    /// correctness one — every pot is still queried, just in more or fewer D1
    /// round-trips — so it is pinned by VALUE with its rationale rather than
    /// by a contrived behavioural test. Stated honestly because the re-gate
    /// listed `24 -> 23` among the mutations nothing caught: it is benign,
    /// and the invariants that are NOT benign are asserted alongside.
    #[test]
    fn seat_marker_chunk_size_is_pinned_with_its_rationale() {
        // NEVER exceed D1's bind ceiling — a COMPILE-time failure, so the
        // build breaks before a test can even run.
        const _: () = assert!(
            SEAT_MARKERS_CHUNK_POTS * SEAT_MARKERS_BINDS_PER_POT
                <= crate::logic::D1_MAX_BOUND_PARAMS
        );
        // 24 x 4 = 96, leaving a spare slot under the 100-parameter cap.
        assert_eq!(
            SEAT_MARKERS_CHUNK_POTS, 24,
            "deliberate value: raising it past 25 breaks binding outright; \
             lowering it multiplies D1 round-trips per /results request (the \
             >50-outpoint 503 lesson)"
        );
        // A full page must stay within a handful of subrequests.
        let mut m = std::collections::HashMap::new();
        for i in 0..RESULTS_MAX_ROWS {
            m.insert(
                (format!("{i:064x}"), 0u32),
                params_with_keys(&real_key(41).1, &real_key(42).1),
            );
        }
        assert!(
            seat_marker_chunks(&m).len() <= 5,
            "a full /results page costs at most 5 seat-marker round-trips"
        );
    }

    /// Chunking is a pure function of the pot outpoints — never of `HashMap`
    /// iteration order.
    #[test]
    fn seat_marker_chunks_are_deterministic() {
        let build = || {
            let mut m = std::collections::HashMap::new();
            for i in 0..(SEAT_MARKERS_CHUNK_POTS * 2 + 5) {
                m.insert(
                    (format!("{:064x}", i * 7919 % 1000), (i % 2) as u32),
                    params_with_keys(&real_key(41).1, &real_key(42).1),
                );
            }
            seat_marker_chunks(&m)
        };
        assert_eq!(build(), build());
        assert_eq!(build(), build(), "stable across rebuilds");
    }

    /// F2 (2026-07-28 gate): the seat-marker fetch must be bounded WITHOUT
    /// relying on row order — filtered to the pot's COMMITTED keys and
    /// windowed PER KEY SLOT. Pin the load-bearing SQL clauses + the bind
    /// arity the caller depends on.
    #[test]
    fn f2_seat_markers_sql_filters_committed_keys_and_windows_per_slot() {
        let sql = seat_markers_sql(3, SEAT_MARKERS_PER_KEY);
        assert_eq!(
            sql.matches('?').count(),
            3 * SEAT_MARKERS_BINDS_PER_POT,
            "four binds per pot: txid, vout, pubA, pubB (#281 made vout a bind \
             so `/results`, which is not vout-0-only, shares this query)"
        );
        assert_eq!(
            sql.matches("(potTxid = ? AND potVout = ? AND seatSettlePubkey IN (?, ?))")
                .count(),
            3,
            "each pot binds its OWN committed keys, at its OWN outpoint: {sql}"
        );
        assert!(
            sql.contains("ROW_NUMBER() OVER (PARTITION BY potTxid, potVout, seatSettlePubkey"),
            "PER-KEY-SLOT window — junk on one seat's key can never starve the \
             other seat's marker: {sql}"
        );
        assert!(
            sql.contains(&format!("rn <= {SEAT_MARKERS_PER_KEY}")),
            "bounded per key slot: {sql}"
        );
        assert!(sql.contains("seatSettlePubkey IS NOT NULL"));
        assert!(
            sql.contains("sigHex"),
            "the F1 identity sig must be fetched"
        );
        // No unordered global LIMIT — the per-slot window IS the bound.
        assert!(
            !sql.to_ascii_uppercase().contains("LIMIT"),
            "no raw LIMIT: {sql}"
        );
        // (The chunk size × 3 binds/pot ≤ D1's param cap is proven at COMPILE
        // time by the `const _: () = assert!(…)` beside the constant.)
    }

    /// Execute the SHIPPED `seat_markers_sql`'s semantics over a row set:
    /// the WHERE key filter, the window's PARTITION BY, and the `rn <= cap`
    /// bound are all read OUT OF THE GENERATED SQL, so reverting the query
    /// to an order-dependent shape changes this simulation too (which is
    /// what makes the scenario test below RED-verifiable rather than a
    /// hard-coded restatement of the fix).
    /// `rows` are `(marker, createdAt)` in insertion (rowid) order.
    fn simulate_seat_fetch(
        sql: &str,
        committed: (&str, &str),
        rows: &[(SeatMarkerRow, i64)],
    ) -> Vec<SeatMarkerRow> {
        let key_filtered = sql.contains("seatSettlePubkey IN (?, ?)");
        let per_key_window = sql.contains("PARTITION BY potTxid, potVout, seatSettlePubkey");
        let cap: usize = sql
            .split("rn <= ")
            .nth(1)
            .and_then(|s| s.split(|c: char| !c.is_ascii_digit()).next())
            .and_then(|s| s.parse().ok())
            .unwrap_or(usize::MAX);
        // WHERE: committed-key filter (when the SQL has one).
        let mut kept: Vec<(usize, &(SeatMarkerRow, i64))> = rows
            .iter()
            .enumerate()
            .filter(|(_, (m, _))| {
                !key_filtered
                    || m.seat_settle_pubkey.eq_ignore_ascii_case(committed.0)
                    || m.seat_settle_pubkey.eq_ignore_ascii_case(committed.1)
            })
            .collect();
        // ORDER BY createdAt ASC, rowid ASC.
        kept.sort_by(|(ia, (_, ca)), (ib, (_, cb))| ca.cmp(cb).then(ia.cmp(ib)));
        // ROW_NUMBER() partition + `rn <= cap`.
        let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        let mut out = Vec::new();
        for (_, (m, _)) in kept {
            let part = if per_key_window {
                format!("{}|{}", m.pot_txid, m.seat_settle_pubkey)
            } else {
                m.pot_txid.clone()
            };
            let n = seen.entry(part).or_insert(0);
            *n += 1;
            if *n <= cap {
                out.push(m.clone());
            }
        }
        out
    }

    /// FIX 1 (2026-07-28 second gate): the reviewer's REAL-SQLite scenario.
    ///
    /// The earlier revision argued honest markers are always the OLDEST rows
    /// for a pot ("its txid does not exist to be named before funding") — but
    /// this PR's own #252 opportunistic BACKFILL republishes v2 markers for
    /// pots whose txid has been public for WEEKS, so junk can carry an
    /// EARLIER `createdAt` than the honest markers. With the order-dependent
    /// window, 45 junk rows + 2 honest ⇒ 40 junk / 0 honest fetched and the
    /// chainProven win is silently never credited (rows are never deleted, so
    /// it stays broken forever).
    ///
    /// With the shipped query the honest markers survive REGARDLESS of junk
    /// volume or timing: junk under a non-committed key is filtered out in
    /// SQL, and each committed key owns its own window slot.
    #[test]
    fn fix1_backfilled_honest_markers_survive_older_junk() {
        let (ka, pa) = real_key(41);
        let (kb, pb) = real_key(42);
        let wa = wallet_of(0x51);
        let wb = wallet_of(0x52);
        let ida = identity_of(&wa);
        let idb = identity_of(&wb);
        let gid = tx(0x01);
        let pot = tx(0x22);
        let p = params_with_keys(&pa, &pb);

        // 45 junk rows, all with createdAt EARLIER than the honest markers
        // (the backfill window), each a byte-valid v2 shape under its own
        // throwaway key + identity — exactly what dust-cost spam looks like.
        let mut rows: Vec<(SeatMarkerRow, i64)> = Vec::new();
        for i in 0..45u8 {
            let (kj, pj) = real_key(100 + i);
            let wj = wallet_of(0x80 + i);
            let idj = identity_of(&wj);
            let junk = real_seat_marker(&kj, &pj, &wj, &idj, &ida, &gid, &pot, 0);
            rows.push((junk, 1_000 + i as i64)); // early createdAt
        }
        // The two HONEST markers, published by the backfill weeks later.
        let ma = real_seat_marker(&ka, &pa, &wa, &ida, &idb, &gid, &pot, 0);
        let mb = real_seat_marker(&kb, &pb, &wb, &idb, &ida, &gid, &pot, 0);
        rows.push((ma, 9_000));
        rows.push((mb, 9_001));

        let fetched = simulate_seat_fetch(&seat_markers_sql(1, SEAT_MARKERS_PER_KEY), (&pa, &pb), &rows);
        // The MONEY assertion first: attribution must credit both seats —
        // under the pre-fix order-dependent window this yields None/None
        // (45 older junk rows fill the pot's single window).
        let attr = attribute_seats(&p, &pot, 0, &fetched);
        assert_eq!(
            attr.identity_a.as_deref(),
            Some(ida.as_str()),
            "the backfilled honest seat-A marker must still be attributed \
             ({} rows fetched)",
            fetched.len()
        );
        assert_eq!(
            fetched.len(),
            2,
            "only the two committed-key rows survive the SQL filter"
        );
        assert_eq!(attr.identity_b.as_deref(), Some(idb.as_str()));
        assert_eq!(attr.winner_for(PotVerdict::WinnerA), Some(ida.as_str()));

        // Even junk that COPIES a committed key cannot starve the OTHER seat
        // (per-key-slot windowing) — pile SEAT_MARKERS_PER_KEY×3 copies of
        // seat A's key, all older; seat B's marker still comes back.
        let mut rows2 = rows.clone();
        for i in 0..(SEAT_MARKERS_PER_KEY as u8 * 3) {
            let wj = wallet_of(0xa0 + i);
            let idj = identity_of(&wj);
            // Note: a copied key with a foreign identity cannot produce a
            // valid seatSig — this is junk by construction, as on-chain.
            let mut junk = real_seat_marker(&ka, &pa, &wj, &idj, &ida, &gid, &pot, 0);
            junk.identity = idj;
            rows2.push((junk, 500 + i as i64));
        }
        let fetched2 = simulate_seat_fetch(&seat_markers_sql(1, SEAT_MARKERS_PER_KEY), (&pa, &pb), &rows2);
        let attr2 = attribute_seats(&p, &pot, 0, &fetched2);
        assert_eq!(
            attr2.identity_b.as_deref(),
            Some(idb.as_str()),
            "junk crowding seat A's key slot must not starve seat B"
        );
    }

    /// A tiny synthetic pot: bare-era lock is NOT used here — we build a
    /// spend-shape that stays UNRESOLVED (unknown lock), which is all the
    /// assembly plumbing needs (classification itself is pinned against the
    /// real mainnet fixtures in `tests/classifier_real_txs.rs`).
    fn fake_funding_and_spender() -> (Vec<u8>, String, Vec<u8>, String) {
        // funding: one dummy input, one 1000-sat OP_TRUE output.
        let mut f = Vec::new();
        f.extend_from_slice(&1u32.to_le_bytes());
        f.push(1);
        f.extend_from_slice(&[0x11u8; 32]);
        f.extend_from_slice(&0u32.to_le_bytes());
        f.push(0);
        f.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
        f.push(1);
        f.extend_from_slice(&1000u64.to_le_bytes());
        f.push(1);
        f.push(0x51);
        f.extend_from_slice(&0u32.to_le_bytes());
        let f_id = bsv_rs::transaction::Transaction::from_binary(&f)
            .unwrap()
            .id();
        // spender: spends funding:0.
        let mut s = Vec::new();
        s.extend_from_slice(&1u32.to_le_bytes());
        s.push(1);
        let mut prev = hex::decode(&f_id).unwrap();
        prev.reverse();
        s.extend_from_slice(&prev);
        s.extend_from_slice(&0u32.to_le_bytes());
        s.push(0);
        s.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
        s.push(1);
        s.extend_from_slice(&900u64.to_le_bytes());
        s.push(1);
        s.push(0x51);
        s.extend_from_slice(&0u32.to_le_bytes());
        let s_id = bsv_rs::transaction::Transaction::from_binary(&s)
            .unwrap()
            .id();
        (f, f_id, s, s_id)
    }

    #[test]
    fn assemble_results_dedupes_and_fail_safes() {
        let me = ident(0xaa);
        let opp = ident(0xbb);
        let (f_raw, f_id, s_raw, s_id) = fake_funding_and_spender();
        let row = ResultsRow {
            identity: me.clone(),
            game_id: tx(0x01),
            pot_txid: f_id.clone(),
            pot_vout: 0,
            recovery_height: 900_000,
            opponent_identity: opp.clone(),
            spent: Some(true),
            spending_txid: Some(s_id.clone()),
            spent_confirmed: Some(true),
            funding_beef_hex: Some(beef_hex_of(&f_raw)),
            spender_beef_hex: Some(beef_hex_of(&s_raw)),
            seat_settle_pubkey: None,
            seat_sig_hex: None,
            marker_sig_hex: None,
            ..Default::default()
        };
        // A duplicate marker row (garbage coexists by outpoint keying) and an
        // unspent pot with no bytes at all.
        let unspent = ResultsRow {
            identity: me.clone(),
            game_id: tx(0x02),
            pot_txid: tx(0x44),
            pot_vout: 0,
            recovery_height: 900_100,
            opponent_identity: opp.clone(),
            spent: None,
            spending_txid: None,
            spent_confirmed: None,
            funding_beef_hex: None,
            spender_beef_hex: None,
            seat_settle_pubkey: None,
            seat_sig_hex: None,
            marker_sig_hex: None,
            ..Default::default()
        };
        let rows = vec![row.clone(), row.clone(), unspent];
        let entries = assemble_like_the_route(
            &me,
            rows,
            &std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
        );
        assert_eq!(entries.len(), 2, "duplicate pot rows dedupe");
        // Unknown lock shape → verdict None, outcome unresolved — never a
        // guess, and the pointer facts still serve.
        assert_eq!(entries[0].verdict, None);
        assert_eq!(entries[0].outcome, Outcome::Unresolved);
        assert_eq!(entries[0].settle_txid.as_deref(), Some(s_id.as_str()));
        assert_eq!(entries[0].at_height, None, "unproven BEEF → no height");
        // The never-spent pot keeps its fail-safe nulls.
        assert_eq!(entries[1].spent, None);
        assert_eq!(entries[1].outcome, Outcome::Unresolved);
    }

    /// #323 defect 1 — a PARKED (recorded-but-unconfirmed) spender must
    /// never produce a verdict, an outcome, or a height. A non-final parked
    /// tx is a displaceable INTENT, not a landing: the same bar
    /// `refund_view::derive_refund_status` already applies ("recorded-but-
    /// unconfirmed (a displaceable intent, not a landing): incomplete —
    /// never guess"). The raw pointer facts still SERVE (surface, never
    /// consume) so the client can see what was attempted.
    #[test]
    fn a_parked_unconfirmed_spender_yields_no_verdict_and_no_outcome() {
        let (ka, pa) = real_key(41);
        let (kb, pb) = real_key(42);
        let w_winner = wallet_of(0x51);
        let w_loser = wallet_of(0x52);
        let winner = identity_of(&w_winner);
        let loser = identity_of(&w_loser);
        let gid = tx(0x01);
        let params = params_with_keys(&pa, &pb);

        let lock = covenant_lock(&params);
        let f_raw = raw_tx(&"11".repeat(32), 0, 0xffff_ffff, &[(1000, lock)], 0);
        let f_id = bsv_rs::transaction::Transaction::from_binary(&f_raw)
            .unwrap()
            .id();
        let outs = vec![
            (10u64, p2pkh_lock(&params.rake_pkh)),
            (980u64, p2pkh_lock(&params.pay_pkh_a)),
        ];
        let s_raw = raw_tx(&f_id, 0, 0xffff_ffff, &outs, 0);
        let s_id = bsv_rs::transaction::Transaction::from_binary(&s_raw)
            .unwrap()
            .id();
        let marker = real_seat_marker(&ka, &pa, &w_winner, &winner, &loser, &gid, &f_id, 0);
        let _ = &kb;

        let mk = |confirmed: Option<bool>| ResultsRow {
            identity: marker.identity.clone(),
            game_id: gid.clone(),
            pot_txid: f_id.clone(),
            pot_vout: 0,
            recovery_height: marker.recovery_height,
            opponent_identity: marker.opponent_identity.clone(),
            spent: Some(true),
            spending_txid: Some(s_id.clone()),
            spent_confirmed: confirmed,
            funding_beef_hex: Some(beef_hex_of(&f_raw)),
            spender_beef_hex: Some(beef_hex_of(&s_raw)),
            seat_settle_pubkey: Some(marker.seat_settle_pubkey.clone()),
            seat_sig_hex: Some(marker.seat_sig_hex.clone()),
            marker_sig_hex: Some(marker.identity_sig_hex.clone()),
            ..Default::default()
        };

        // CONTROL: the identical row, CONFIRMED, does resolve. Without this
        // the test could pass because the fixture never classified at all.
        let confirmed = assemble_like_the_route(
            &winner,
            vec![mk(Some(true))],
            &std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
        );
        assert_eq!(confirmed[0].verdict, Some(PotVerdict::WinnerA));
        assert_eq!(confirmed[0].outcome, Outcome::Won);

        // THE DEFECT: same bytes, spentConfirmed = 0 (a parked intent).
        // MEDIUM-4: a legacy row stamped spentConfirmed=0 by migration but
        // carrying a chaintracks-VERIFIED spender proof still resolves — the
        // stronger proof is honoured, so real historical settles are not
        // silently un-resolved.
        let mut legacy = mk(Some(false));
        legacy.spender_proof_verified = Some(true);
        let e = assemble_like_the_route(
            &winner,
            vec![legacy],
            &std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
        );
        assert_eq!(
            e[0].verdict,
            Some(PotVerdict::WinnerA),
            "a VERIFIED spender proof is a landing even when the flag is 0"
        );

        for parked in [Some(false), None] {
            let e = assemble_like_the_route(
                &winner,
                vec![mk(parked)],
                &std::collections::HashMap::new(),
                &std::collections::HashMap::new(),
            );
            assert_eq!(e.len(), 1);
            assert_eq!(
                e[0].verdict, None,
                "an unconfirmed spender must never yield a verdict"
            );
            assert_eq!(
                e[0].outcome,
                Outcome::Unresolved,
                "an unconfirmed spender must never yield an outcome"
            );
            assert_eq!(e[0].at_height, None, "no height from an unconfirmed spend");
            assert!(
                e[0].winner_hand.is_none(),
                "no provable hand from an unconfirmed spend"
            );
            // Surface, never consume: the attempted pointer still serves,
            // labelled by spentConfirmed, so the client can see the intent.
            assert_eq!(e[0].settle_txid.as_deref(), Some(s_id.as_str()));
            assert_eq!(e[0].spent, Some(true));
            assert_eq!(e[0].spent_confirmed, parked);
        }
    }

    /// #323 defect 2 — `/spent-any` must not collapse "we could not look"
    /// into the same answer as "we looked and corroborated nothing".
    ///
    /// The issue filed this as "the pot has no `pot_records` row". That
    /// diagnosis is WRONG: `/spent-any` never reads `pot_records` at all
    /// (it is a live WoC+Bitails read — see `routes::spent_any_resolve`).
    /// What actually happens is that a provider fault on the spender-raw
    /// fetch — WoC 429s are a documented operational fact for this repo —
    /// turns a CONFIRMED positive into `known:false`, indistinguishable
    /// from a genuine negative. The misdiagnosis was itself caused by
    /// `OutpointStatus::known`'s doc, which describes only the D1-backed
    /// `/utxo-status` producer.
    ///
    /// Fail-safe direction is correct and MUST NOT change (never assert an
    /// unverifiable pointer). What changes is that the REASON is surfaced,
    /// so a fault is legible instead of masquerading as information.
    #[test]
    fn spent_any_distinguishes_a_fault_from_a_corroborated_negative() {
        let spender = "cd".repeat(32);
        // 1. Upstream fault — we could not look.
        let fault = decide_spent_any(
            &SpentObservation::Fault,
            false,
            UnspentCorroboration::Unknown,
        );
        // 2. Looked, and corroborated genuinely unspent.
        let unspent = decide_spent_any(
            &SpentObservation::NotSpent,
            false,
            UnspentCorroboration::ConfirmedUnspent,
        );
        // 3. Spent per WoC, but the spender raw could not be verified.
        let unverified = decide_spent_any(
            &SpentObservation::Spent {
                txid: spender.clone(),
                confirmed: true,
            },
            false,
            UnspentCorroboration::Unknown,
        );
        // 4. Looked, WoC says unspent, Bitails did not corroborate.
        let uncorroborated = decide_spent_any(
            &SpentObservation::NotSpent,
            false,
            UnspentCorroboration::Unknown,
        );

        // The corroborated negative is real information and stays known.
        assert!(unspent.known, "a corroborated unspent is a real answer");
        assert_eq!(unspent.spent, Some(false));

        // The other three are all `known:false` — the FAIL-SAFE, unchanged.
        for s in [&fault, &unverified, &uncorroborated] {
            assert!(!s.known, "unverifiable input must stay known:false");
            assert_eq!(s.spent, None, "never assert a spend we cannot verify");
        }

        // THE DEFECT: they are mutually indistinguishable on the wire, so a
        // provider outage reads exactly like "nothing there" — which is how
        // this got filed as a coverage hole in a table this route never reads.
        let body_of = |s: &crate::logic::OutpointStatus| {
            let mut e = s.clone();
            e.txid = "ab".repeat(32);
            e.vout = 0;
            crate::logic::utxo_status_body(std::slice::from_ref(&e))
        };
        assert_ne!(
            body_of(&fault),
            body_of(&uncorroborated),
            "a provider FAULT must be distinguishable from an un-corroborated negative"
        );
        assert_ne!(
            body_of(&fault),
            body_of(&unverified),
            "a provider FAULT must be distinguishable from an unverifiable spender"
        );
    }

    /// #323 HIGH-2 — a WoC rate-limit is a FAULT, not "unspent". Consuming
    /// 429 as `NotSpent` turned the repo's most-documented outage into a
    /// confident negative, and mislabelled it `uncorroborated-unspent`.
    #[test]
    fn a_woc_rate_limit_is_a_fault_not_an_unspent_answer() {
        // Absence codes are a real answer.
        for ok in [404u16, 410] {
            assert_eq!(
                woc_spent_status_observation(ok),
                SpentObservation::NotSpent,
                "{ok} means the outpoint is genuinely absent"
            );
        }
        // Everything else means we could not look.
        for fault in [429u16, 401, 403, 400, 500, 502, 503, 504] {
            assert_eq!(
                woc_spent_status_observation(fault),
                SpentObservation::Fault,
                "{fault} is a fault, never an answer about the outpoint"
            );
        }
        // And the fault must be LEGIBLE end to end, not just internally.
        let st = decide_spent_any(
            &woc_spent_status_observation(429),
            false,
            UnspentCorroboration::Unknown,
        );
        assert!(!st.known);
        assert_eq!(
            st.reason,
            Some(crate::logic::SPENT_ANY_REASON_PROVIDER_FAULT),
            "a 429 must report provider-fault, never uncorroborated-unspent"
        );
    }

    #[test]
    fn results_body_shape() {
        let me = ident(0xaa);
        let e = ResultEntry {
            game_id: tx(0x01),
            pot_txid: tx(0x02),
            pot_vout: 0,
            recovery_height: 958_846,
            cov_recovery_height: Some(958_800),
            opponent_identity: ident(0xbb),
            settle_txid: Some(tx(0x03)),
            spent: Some(true),
            spent_confirmed: Some(true),
            pot_binding: PotBinding::Chain,
            game_id_binding: PotBinding::Chain,
            verdict: Some(PotVerdict::Refund),
            outcome: Outcome::Refund,
            outcome_source: Some("chain"),
            at_height: Some(958_900),
            winner_hand: None,
        };
        let v: serde_json::Value = serde_json::from_str(&results_body(&me, &[e])).unwrap();
        assert_eq!(v["identity"], me);
        let r = &v["results"][0];
        assert_eq!(r["gameId"], tx(0x01));
        assert_eq!(r["potTxid"], tx(0x02));
        assert_eq!(r["verdict"], "refund");
        assert_eq!(r["outcome"], "refund");
        assert_eq!(r["outcomeSource"], "chain");
        assert_eq!(r["at"]["height"], 958_900);
        assert_eq!(r["settleTxid"], tx(0x03));
        // A refund has no showdown → `hand` is JSON null (never fabricated).
        assert!(r["hand"].is_null());
        // The two heights are SEPARATE wire fields — the marker hint keeps
        // its name and value; the covenant truth gets its own.
        assert_eq!(r["recoveryHeight"], 958_846);
        assert_eq!(r["covRecoveryHeight"], 958_800);
        assert_eq!(r["potBinding"], "chain");
        assert_eq!(r["potBindingSource"], "chain+seatkey");
    }

    /// The honesty pair's UNKNOWN leg, and the absent covenant height, are
    /// serialized EXPLICITLY — `potBinding: "unknown"` with a `null` source
    /// and a `null` covenant height. Never omitted (an absent key is
    /// indistinguishable from an old server) and never optimistic.
    #[test]
    fn results_body_serializes_unknown_as_a_first_class_answer() {
        let me = ident(0xaa);
        let e = ResultEntry {
            game_id: tx(0x01),
            pot_txid: tx(0x02),
            pot_vout: 0,
            recovery_height: 1, // the attacker's hint
            cov_recovery_height: None,
            opponent_identity: ident(0xbb),
            settle_txid: None,
            spent: None,
            spent_confirmed: None,
            pot_binding: PotBinding::Unknown,
            game_id_binding: PotBinding::Unknown,
            verdict: None,
            outcome: Outcome::Unresolved,
            outcome_source: None,
            at_height: None,
            winner_hand: None,
        };
        let v: serde_json::Value = serde_json::from_str(&results_body(&me, &[e])).unwrap();
        let r = &v["results"][0];
        assert_eq!(r["recoveryHeight"], 1, "the hint serves unchanged");
        assert!(
            r["covRecoveryHeight"].is_null(),
            "no covenant height ⇒ explicit null, never the hint"
        );
        assert_eq!(r["potBinding"], "unknown");
        assert!(r["potBindingSource"].is_null());
        // Both keys are PRESENT (not omitted) — "field absent" is reserved
        // for an OLD SERVER, which is a different thing a client must handle
        // differently (see the compatibility note on `results_body`).
        let obj = r.as_object().unwrap();
        assert!(obj.contains_key("covRecoveryHeight"));
        assert!(obj.contains_key("potBinding"));
        assert!(obj.contains_key("potBindingSource"));
    }

    /// BYTE-LEVEL BACKWARD COMPATIBILITY: for one existing entry, the
    /// response differs from the pre-change shape ONLY by the three added
    /// keys. Every key a deployed client already reads keeps its name, its
    /// type and its value.
    ///
    /// The pre-change key set is written out as a LITERAL, not derived from
    /// the current body — an equality whose two sides move together is
    /// `f(x) == f(x)` in a costume (Rule 9), and deriving the "old" set from
    /// today's code is exactly that.
    #[test]
    fn the_existing_results_shape_is_unchanged_apart_from_the_added_keys() {
        const PRE_CHANGE_KEYS: [&str; 12] = [
            "gameId",
            "potTxid",
            "potVout",
            "recoveryHeight",
            "opponentIdentity",
            "settleTxid",
            "spent",
            "spentConfirmed",
            "verdict",
            "outcome",
            "outcomeSource",
            "at",
        ];
        const ADDED_KEYS: [&str; 4] = [
            "covRecoveryHeight",
            "potBinding",
            "potBindingSource",
            "gameIdBinding",
        ];
        let me = ident(0xaa);
        let e = ResultEntry {
            game_id: tx(0x01),
            pot_txid: tx(0x02),
            pot_vout: 0,
            recovery_height: 958_846,
            cov_recovery_height: Some(958_800),
            opponent_identity: ident(0xbb),
            settle_txid: Some(tx(0x03)),
            spent: Some(true),
            spent_confirmed: Some(true),
            pot_binding: PotBinding::Chain,
            game_id_binding: PotBinding::Chain,
            verdict: Some(PotVerdict::WinnerA),
            outcome: Outcome::Won,
            outcome_source: Some("chain+seatkey"),
            at_height: Some(958_900),
            winner_hand: Some(WinnerHand {
                identity: me.clone(),
                cards_hex: "000102030c".to_string(),
                score: 15,
                is_tie: false,
            }),
        };
        let v: serde_json::Value = serde_json::from_str(&results_body(&me, &[e])).unwrap();
        let obj = v["results"][0].as_object().unwrap();
        // `hand` is pre-change too but is asserted by its own cell above; it
        // is listed here only so the key-set equality is exhaustive.
        let mut got: Vec<&str> = obj.keys().map(String::as_str).collect();
        got.sort_unstable();
        let mut want: Vec<&str> = PRE_CHANGE_KEYS
            .iter()
            .copied()
            .chain(ADDED_KEYS)
            .chain(["hand"])
            .collect();
        want.sort_unstable();
        assert_eq!(
            got, want,
            "the /results entry key set may only GROW by the four added keys"
        );
        // …and the pre-change values are byte-identical to what the old
        // server emitted for this entry.
        assert_eq!(obj["gameId"], json!(tx(0x01)));
        assert_eq!(obj["potTxid"], json!(tx(0x02)));
        assert_eq!(obj["potVout"], json!(0));
        assert_eq!(obj["recoveryHeight"], json!(958_846));
        assert_eq!(obj["opponentIdentity"], json!(ident(0xbb)));
        assert_eq!(obj["settleTxid"], json!(tx(0x03)));
        assert_eq!(obj["spent"], json!(true));
        assert_eq!(obj["spentConfirmed"], json!(true));
        assert_eq!(obj["verdict"], json!("winner-a"));
        assert_eq!(obj["outcome"], json!("won"));
        assert_eq!(obj["outcomeSource"], json!("chain+seatkey"));
        assert_eq!(obj["at"], json!({ "height": 958_900 }));
        // The top-level envelope is unchanged too.
        assert_eq!(v.as_object().unwrap().len(), 2);
        assert_eq!(v["identity"], me);
    }

    /// A winner ResultEntry carrying a showdown hand serializes the full
    /// `hand` object (winner-only cards + score + the loser caveat).
    #[test]
    fn results_body_carries_the_winner_hand() {
        let me = ident(0xaa);
        let e = ResultEntry {
            game_id: tx(0x01),
            pot_txid: tx(0x02),
            pot_vout: 0,
            recovery_height: 958_846,
            cov_recovery_height: Some(958_800),
            opponent_identity: ident(0xbb),
            settle_txid: Some(tx(0x03)),
            spent: Some(true),
            spent_confirmed: Some(true),
            pot_binding: PotBinding::Chain,
            game_id_binding: PotBinding::Chain,
            verdict: Some(PotVerdict::WinnerA),
            outcome: Outcome::Won,
            outcome_source: Some("chain+claim"),
            at_height: Some(958_900),
            winner_hand: Some(WinnerHand {
                identity: me.clone(),
                cards_hex: "000102030c".to_string(), // A-2-3-4-5 wheel = 15
                score: 15,
                is_tie: false,
            }),
        };
        let v: serde_json::Value = serde_json::from_str(&results_body(&me, &[e])).unwrap();
        let h = &v["results"][0]["hand"];
        assert_eq!(h["winnerIdentity"], me);
        assert_eq!(h["winnerCardsHex"], "000102030c");
        assert_eq!(h["winnerScore"], 15);
        assert_eq!(h["isTie"], false);
        assert_eq!(h["loserCardsOnChain"], false);
        assert!(h["note"].as_str().unwrap().contains("winner"));
    }

    // ── server-side claim signature verification (adversarial) ─────────
    //
    // Real ECDSA round-trips through the REAL producer path
    // (`claims_by_game` → `derive_outcome`) — never a mocked verify, never
    // hand-fed post-verification facts (repo doctrine). The signing recipe
    // is the client's exactly: `createSignature` for counterparty 'anyone'
    // under `[1,'low result']` with keyID = gameId.

    use bsv_rs::primitives::ec::PrivateKey;
    use bsv_rs::wallet::{Counterparty, CreateSignatureArgs, ProtoWallet};

    /// Deterministic test wallet (same test-key crypto the workspace's
    /// topic-manager tests use — a pinned root private key).
    fn wallet_of(seed: u8) -> ProtoWallet {
        let key = PrivateKey::from_hex(&format!("{seed:064x}")).unwrap();
        ProtoWallet::new(Some(key))
    }

    fn identity_of(w: &ProtoWallet) -> String {
        w.identity_key_hex().to_ascii_lowercase()
    }

    /// Sign the canonical result challenge as the client does (counterparty
    /// 'anyone', keyID = gameId), returning DER hex.
    fn sign_result(w: &ProtoWallet, game_id: &str, challenge: &[u8]) -> String {
        let sig = w
            .create_signature(CreateSignatureArgs {
                data: Some(challenge.to_vec()),
                hash_to_directly_sign: None,
                protocol_id: result_protocol(),
                key_id: game_id.to_string(),
                counterparty: Some(Counterparty::Anyone),
            })
            .unwrap();
        hex::encode(sig.signature)
    }

    /// A marker row over the standard test claim shape; sigs supplied by the
    /// caller (real, forged, or absent).
    #[allow(clippy::too_many_arguments)]
    fn marker(
        game: &str,
        winner: &str,
        loser: &str,
        pot: &str,
        settle: &str,
        cards_hex: Option<&str>,
        winner_sig_hex: String,
        loser_sig_hex: Option<String>,
    ) -> ResultMarkerRow {
        ResultMarkerRow {
            game_id: game.to_string(),
            winner: winner.to_string(),
            loser: loser.to_string(),
            pot_txid: pot.to_string(),
            settle_txid: settle.to_string(),
            winner_sig_hex,
            loser_sig_hex,
            cards_hex: cards_hex.map(str::to_string),
            txid: tx(0x04),
            created_at: Some(1),
        }
    }

    /// A plausibly-shaped but FABRICATED DER sig (valid hex, garbage bytes).
    fn garbage_sig() -> String {
        format!("3045{}", "ab".repeat(69))
    }

    #[test]
    fn fabricated_sig_claim_contributes_nothing_and_never_flips_the_winner() {
        // THE finding scenario: the loser fabricates a marker naming the REAL
        // settle txid and themselves as winner, with garbage sig bytes — and
        // pads a garbage "countersig" so the old `loser_sig_hex.is_some()`
        // check would have counted it as both-signed. The honest side never
        // published. Server-side verification must drop the whole claim.
        let honest = wallet_of(0x11);
        let liar = wallet_of(0x22);
        let (h, l) = (identity_of(&honest), identity_of(&liar));
        let (game, pot, settle) = (tx(0x01), tx(0x02), tx(0x03));

        let fabricated = marker(
            &game,
            &l,
            &h,
            &pot,
            &settle,
            None,
            garbage_sig(),
            Some(garbage_sig()),
        );
        let map = claims_by_game(&[fabricated]);
        assert!(
            !map.contains_key(&game),
            "a claim with an unverifiable winner sig must contribute NOTHING"
        );
        // Outcome: unresolved for BOTH parties — the honest player is never
        // shown a fabricated loss, the liar never a fabricated win.
        for (me, opp) in [(&h, &l), (&l, &h)] {
            let (o, src) = derive_outcome(
                Some(PotVerdict::WinnerA),
                me,
                opp,
                Some(&settle),
                map.get(&game),
            );
            assert_eq!((o, src), (Outcome::Unresolved, None));
        }
    }

    #[test]
    fn real_signed_claim_upgrades_and_forged_countersig_never_shows_a_loss() {
        let winner_w = wallet_of(0x11);
        let loser_w = wallet_of(0x22);
        let (w, l) = (identity_of(&winner_w), identity_of(&loser_w));
        let (game, pot, settle) = (tx(0x01), tx(0x02), tx(0x03));
        let challenge = result_challenge_bytes(&game, &w, &l, &pot, &settle, None).unwrap();
        let w_sig = sign_result(&winner_w, &game, &challenge);
        let l_sig = sign_result(&loser_w, &game, &challenge);

        // Fully countersigned: winner → won, loser → lost (both verified).
        let map = claims_by_game(&[marker(
            &game,
            &w,
            &l,
            &pot,
            &settle,
            None,
            w_sig.clone(),
            Some(l_sig),
        )]);
        let gc = map.get(&game).unwrap();
        assert_eq!(gc.claims.len(), 1);
        assert!(gc.claims[0].loser_sig_verified);
        let (o, src) = derive_outcome(Some(PotVerdict::WinnerA), &w, &l, Some(&settle), Some(gc));
        assert_eq!((o, src), (Outcome::Won, Some("chain+claim")));
        let (o, src) = derive_outcome(Some(PotVerdict::WinnerA), &l, &w, Some(&settle), Some(gc));
        assert_eq!((o, src), (Outcome::Lost, Some("chain+claim")));

        // FORGED countersig (garbage bytes next to a REAL winner sig): the
        // claim survives at winner-sig tier (client's `unconfirmed` demotion)
        // — winner still won, but the loser is NEVER shown a loss it did not
        // itself countersign. (The old presence-only check called this
        // both-signed and reported `lost`.)
        let map = claims_by_game(&[marker(
            &game,
            &w,
            &l,
            &pot,
            &settle,
            None,
            w_sig.clone(),
            Some(garbage_sig()),
        )]);
        let gc = map.get(&game).unwrap();
        assert!(!gc.claims[0].loser_sig_verified);
        let (o, _) = derive_outcome(Some(PotVerdict::WinnerA), &w, &l, Some(&settle), Some(gc));
        assert_eq!(o, Outcome::Won);
        let (o, src) = derive_outcome(Some(PotVerdict::WinnerA), &l, &w, Some(&settle), Some(gc));
        assert_eq!((o, src), (Outcome::Unresolved, None));

        // A countersig by the WRONG key (the winner signing "the loser's"
        // countersig) never verifies under the loser identity either.
        let map = claims_by_game(&[marker(
            &game,
            &w,
            &l,
            &pot,
            &settle,
            None,
            w_sig,
            Some(sign_result(&winner_w, &game, &challenge)),
        )]);
        assert!(!map.get(&game).unwrap().claims[0].loser_sig_verified);
    }

    #[test]
    fn disagreeing_verified_claims_stay_unresolved() {
        // Both parties publish REAL self-signed claims for the same settle —
        // unanimity fails, nobody gets an outcome (verdict-only honesty).
        let a = wallet_of(0x11);
        let b = wallet_of(0x22);
        let (ia, ib) = (identity_of(&a), identity_of(&b));
        let (game, pot, settle) = (tx(0x01), tx(0x02), tx(0x03));
        let ch_a = result_challenge_bytes(&game, &ia, &ib, &pot, &settle, None).unwrap();
        let ch_b = result_challenge_bytes(&game, &ib, &ia, &pot, &settle, None).unwrap();
        let map = claims_by_game(&[
            marker(
                &game,
                &ia,
                &ib,
                &pot,
                &settle,
                None,
                sign_result(&a, &game, &ch_a),
                None,
            ),
            marker(
                &game,
                &ib,
                &ia,
                &pot,
                &settle,
                None,
                sign_result(&b, &game, &ch_b),
                None,
            ),
        ]);
        let gc = map.get(&game).unwrap();
        assert_eq!(gc.claims.len(), 2, "both real claims verify");
        for (me, opp) in [(&ia, &ib), (&ib, &ia)] {
            let (o, src) =
                derive_outcome(Some(PotVerdict::WinnerA), me, opp, Some(&settle), Some(gc));
            assert_eq!((o, src), (Outcome::Unresolved, None));
        }
    }

    #[test]
    fn sig_over_a_different_challenge_never_corroborates() {
        // A REAL signature, but the marker's fields disagree with what was
        // signed (here: a different settle txid) — the reconstructed
        // challenge differs, the sig fails, the claim contributes nothing.
        let winner_w = wallet_of(0x11);
        let loser_w = wallet_of(0x22);
        let (w, l) = (identity_of(&winner_w), identity_of(&loser_w));
        let (game, pot) = (tx(0x01), tx(0x02));
        let signed_settle = tx(0x03);
        let named_settle = tx(0x33);
        let challenge = result_challenge_bytes(&game, &w, &l, &pot, &signed_settle, None).unwrap();
        let map = claims_by_game(&[marker(
            &game,
            &w,
            &l,
            &pot,
            &named_settle,
            None,
            sign_result(&winner_w, &game, &challenge),
            None,
        )]);
        assert!(!map.contains_key(&game));
    }

    #[test]
    fn v2_cards_are_bound_by_the_signatures() {
        // v2: the sigs bind the canonical cards. A marker whose cardsHex was
        // tampered (or garbled) after signing must contribute nothing; the
        // untampered claim verifies (including a non-canonical but
        // set-identical cards encoding — client parity: both sides
        // canonicalize before challenge reconstruction).
        let winner_w = wallet_of(0x11);
        let loser_w = wallet_of(0x22);
        let (w, l) = (identity_of(&winner_w), identity_of(&loser_w));
        let (game, pot, settle) = (tx(0x01), tx(0x02), tx(0x03));
        let cards = "0001020304"; // ordinals 0..4, canonical
        let challenge = result_challenge_bytes(&game, &w, &l, &pot, &settle, Some(cards)).unwrap();
        let w_sig = sign_result(&winner_w, &game, &challenge);

        // Untampered → verifies.
        let map = claims_by_game(&[marker(
            &game,
            &w,
            &l,
            &pot,
            &settle,
            Some(cards),
            w_sig.clone(),
            None,
        )]);
        assert_eq!(map.get(&game).unwrap().claims.len(), 1);

        // Unsorted-but-identical set → same canonical challenge → verifies.
        let map = claims_by_game(&[marker(
            &game,
            &w,
            &l,
            &pot,
            &settle,
            Some("0403020100"),
            w_sig.clone(),
            None,
        )]);
        assert_eq!(map.get(&game).unwrap().claims.len(), 1);

        // Tampered hand → dropped.
        let map = claims_by_game(&[marker(
            &game,
            &w,
            &l,
            &pot,
            &settle,
            Some("0001020305"),
            w_sig.clone(),
            None,
        )]);
        assert!(!map.contains_key(&game));

        // Malformed cards (duplicate ordinal) → unverifiable → dropped.
        let map = claims_by_game(&[marker(
            &game,
            &w,
            &l,
            &pot,
            &settle,
            Some("0000010203"),
            w_sig,
            None,
        )]);
        assert!(!map.contains_key(&game));
    }

    #[test]
    fn self_paired_and_case_variant_markers_are_handled() {
        // winner === loser is invalid regardless of signatures (client
        // parity), and an upper-cased marker row still verifies (all
        // challenge fields are lowercased before reconstruction).
        let winner_w = wallet_of(0x11);
        let loser_w = wallet_of(0x22);
        let (w, l) = (identity_of(&winner_w), identity_of(&loser_w));
        let (game, pot, settle) = (tx(0x01), tx(0x02), tx(0x03));
        let ch_self = result_challenge_bytes(&game, &w, &w, &pot, &settle, None).unwrap();
        let map = claims_by_game(&[marker(
            &game,
            &w,
            &w,
            &pot,
            &settle,
            None,
            sign_result(&winner_w, &game, &ch_self),
            None,
        )]);
        assert!(!map.contains_key(&game), "self-paired claim never counts");

        let challenge = result_challenge_bytes(&game, &w, &l, &pot, &settle, None).unwrap();
        let map = claims_by_game(&[marker(
            &game.to_ascii_uppercase(),
            &w.to_ascii_uppercase(),
            &l.to_ascii_uppercase(),
            &pot.to_ascii_uppercase(),
            &settle.to_ascii_uppercase(),
            None,
            sign_result(&winner_w, &game, &challenge),
            None,
        )]);
        let gc = map.get(&game).unwrap();
        assert_eq!(gc.claims.len(), 1);
        assert_eq!(gc.claims[0].settle_txid, settle);
    }

    // ── hand-score exposure (bsv-low #245) ─────────────────────────────

    #[test]
    fn hand_score_matches_frozen_oracle_vectors() {
        // (cardsHex, expected low-sum) — cross-checked against
        // oracle/eval5_lowsum.py, spanning aces (Ace=1) and face cards
        // (T/J/Q/K=10). Ordinal = 13*suit + rank, rank 0='2'..8='T', 9='J',
        // 10='Q', 11='K', 12='A' (low-core `card_from_ordinal`).
        let vectors = [
            ("0c19263300", 6),  // A A A A 2  (min_quad_ace)
            ("000102030c", 15), // A 2 3 4 5  (ace_low_wheel)
            ("0001020304", 20), // 2 3 4 5 6  (run_two_to_six)
            ("0c08090a0b", 41), // A T J Q K  (ace_and_faces)
            ("09160a170b", 50), // J J Q Q K  (all_face_cards)
            ("0b1825320a", 50), // K K K K Q  (max_quad_king)
        ];
        for (hexs, want) in vectors {
            let cards = crate::logic::leaderboard_cards_from_hex(hexs)
                .unwrap_or_else(|| panic!("vector {hexs} must parse"));
            assert_eq!(crate::logic::hand_score(&cards), want, "sum for {hexs}");
            // The exposure helper agrees end-to-end (parse → score → canonical).
            let h = winner_hand_from("02aa", hexs, false).unwrap();
            assert_eq!(h.score, want);
        }
    }

    #[test]
    fn resolve_winner_hand_exposes_winner_only_and_is_viewer_independent() {
        // Real signed v2 claim through the REAL producer (`claims_by_game`):
        // a winner verdict + the unanimous verified winner claim surfaces the
        // WINNER's five cards + low-sum — identically for either viewer.
        let winner_w = wallet_of(0x11);
        let loser_w = wallet_of(0x22);
        let (w, l) = (identity_of(&winner_w), identity_of(&loser_w));
        let (game, pot, settle) = (tx(0x01), tx(0x02), tx(0x03));
        let cards = "000102030c"; // A-2-3-4-5 wheel = 15
        let ch = result_challenge_bytes(&game, &w, &l, &pot, &settle, Some(cards)).unwrap();
        let map = claims_by_game(&[marker(
            &game,
            &w,
            &l,
            &pot,
            &settle,
            Some(cards),
            sign_result(&winner_w, &game, &ch),
            None,
        )]);
        let gc = map.get(&game);

        let hand =
            resolve_winner_hand(Some(PotVerdict::WinnerA), &w, &l, Some(&settle), gc).unwrap();
        assert_eq!(hand.identity, w);
        assert_eq!(hand.cards_hex, "000102030c");
        assert_eq!(hand.score, 15);
        assert!(!hand.is_tie);
        // The loser sees the SAME winner hand (a per-game chain+claim fact).
        let from_loser =
            resolve_winner_hand(Some(PotVerdict::WinnerA), &l, &w, Some(&settle), gc).unwrap();
        assert_eq!(from_loser, hand);

        // A refund / no verdict never exposes a hand (no showdown).
        assert!(resolve_winner_hand(Some(PotVerdict::Refund), &w, &l, Some(&settle), gc).is_none());
        assert!(resolve_winner_hand(None, &w, &l, Some(&settle), gc).is_none());
    }

    #[test]
    fn resolve_winner_hand_null_when_cards_absent_or_winner_unresolved() {
        let winner_w = wallet_of(0x11);
        let loser_w = wallet_of(0x22);
        let (w, l) = (identity_of(&winner_w), identity_of(&loser_w));
        let (game, pot, settle) = (tx(0x01), tx(0x02), tx(0x03));

        // A v1 (no-cards) claim: the winner resolves, but no hand is on-chain
        // → None (never a fabricated hand).
        let ch = result_challenge_bytes(&game, &w, &l, &pot, &settle, None).unwrap();
        let map = claims_by_game(&[marker(
            &game,
            &w,
            &l,
            &pot,
            &settle,
            None,
            sign_result(&winner_w, &game, &ch),
            None,
        )]);
        assert!(resolve_winner_hand(
            Some(PotVerdict::WinnerA),
            &w,
            &l,
            Some(&settle),
            map.get(&game)
        )
        .is_none());

        // Both parties publish REAL claims-with-cards for the same settle:
        // winner unanimity fails → no attributable hand.
        let cw = "000102030c";
        let cl = "0001020304";
        let chw = result_challenge_bytes(&game, &w, &l, &pot, &settle, Some(cw)).unwrap();
        let chl = result_challenge_bytes(&game, &l, &w, &pot, &settle, Some(cl)).unwrap();
        let map = claims_by_game(&[
            marker(
                &game,
                &w,
                &l,
                &pot,
                &settle,
                Some(cw),
                sign_result(&winner_w, &game, &chw),
                None,
            ),
            marker(
                &game,
                &l,
                &w,
                &pot,
                &settle,
                Some(cl),
                sign_result(&loser_w, &game, &chl),
                None,
            ),
        ]);
        assert!(resolve_winner_hand(
            Some(PotVerdict::WinnerA),
            &w,
            &l,
            Some(&settle),
            map.get(&game)
        )
        .is_none());
    }

    #[test]
    fn resolve_winner_hand_tie_exposes_one_provable_equal_sum_side() {
        // A TIE verdict is seat-symmetric; only ONE hand is ever on-chain, so
        // we expose that provable (equal-sum) side, flagged `is_tie`.
        let a_w = wallet_of(0x11);
        let b_w = wallet_of(0x22);
        let (a, b) = (identity_of(&a_w), identity_of(&b_w));
        let (game, pot, settle) = (tx(0x01), tx(0x02), tx(0x03));
        let cards = "000102030c"; // 15
        let ch = result_challenge_bytes(&game, &a, &b, &pot, &settle, Some(cards)).unwrap();
        let map = claims_by_game(&[marker(
            &game,
            &a,
            &b,
            &pot,
            &settle,
            Some(cards),
            sign_result(&a_w, &game, &ch),
            None,
        )]);
        let hand =
            resolve_winner_hand(Some(PotVerdict::Tie), &a, &b, Some(&settle), map.get(&game))
                .unwrap();
        assert!(hand.is_tie);
        assert_eq!(hand.score, 15);
        assert_eq!(hand.identity, a);
    }

    // ── param-push / script-number hygiene ─────────────────────────────
    // (The pure-decoder pins — script-number minimality, bare-lock
    // exactness — MOVED with the decoder to
    // `overlay-discovery::pot::covenant` (bsv-low #284). The golden-vector
    // test `tests/classifier_real_txs.rs::real_covenant_lock_params_extract_exactly`
    // still exercises the extractor through this module's re-export.)

    #[test]
    fn results_and_claims_sql_are_bounded() {
        // The results query is single-bind and bounded (the over-50-outpoint
        // 503 lesson: bound every D1 statement).
        let sql = results_sql();
        assert_eq!(sql.matches('?').count(), 1);
        assert!(sql.contains(&format!("LIMIT {RESULTS_MAX_ROWS}")));
        // #281 F7: the BEEF joins run on the OUTER select, against the ≤100
        // survivors — never inside the window, where every dust replay naming
        // the victim's real pot would have dragged the real BLOBs along.
        // #284: both joins are additionally CONDITION-GATED to fallback-only
        // — the funding BLOB only when the decoded params are absent, the
        // spender BLOB only when the stored verdict is stale/absent for the
        // current pointer or the proven height is missing (with EXPLICIT
        // NULL handling: `verdictTxid <> spendingTxid` alone is NULL-opaque).
        assert!(
            sql.contains("LEFT JOIN pot_beefs fb ON w.pubA IS NULL AND fb.txid = lower(w.potTxid)")
        );
        assert!(sql.contains("LEFT JOIN pot_beefs sb ON w.spendingTxid IS NOT NULL"));
        assert!(sql.contains(
            "(w.verdict IS NULL OR w.verdictTxid IS NULL OR w.verdictTxid <> w.spendingTxid \
             OR w.spentHeight IS NULL)"
        ));
        assert!(sql.contains("AND sb.txid = lower(w.spendingTxid)"));
        // The behaviour these strings stand for is EXECUTED in
        // tests/results_window_sqlite.rs (decoded rows fetch no BLOB; legacy
        // rows still fetch both; a stale verdict re-opens the spender BLOB).
        // #281 structural guards. These are a BACKSTOP only — the behaviour
        // they stand for is proven by EXECUTING this SQL against real SQLite
        // over the production schema in `tests/results_window_sqlite.rs`
        // (the gate rejected string-pinning as sufficient).
        assert!(
            sql.contains("ROW_NUMBER() OVER (PARTITION BY pp.potTxid, pp.potVout"),
            "the window counts POT OUTPOINTS, not rows"
        );
        assert!(
            sql.contains("CASE WHEN r.txid IS NULL THEN 1 ELSE 0 END AS unknownPot")
                && sql.contains(&format!("potRank <= {RESULTS_UNKNOWN_POT_QUOTA}")),
            "existence tier with a RESERVED QUOTA — a strict tier becomes a \
             filter once LIMIT binds (F3)"
        );
        // An explicit ORDER BY at EVERY level (the gate flagged unordered
        // windows): the per-pot window, the pot ranking, the page, and the
        // post-join projection.
        assert_eq!(
            sql.matches("ORDER BY").count(),
            4,
            "deterministic at every level"
        );
        // The seat proof does NOT ride on this query any more (F1) — it has
        // its own committed-key-bound fetch.
        assert!(
            !sql.contains("seatSettlePubkey IS NULL"),
            "no ordering heuristic may decide which marker represents a pot — \
             a forged v2 could always be stamped earlier"
        );
        // Claims chunks bind one param per gameId.
        assert_eq!(claims_sql(3).matches('?').count(), 3);
        assert_eq!(
            claims_sql(crate::logic::D1_CHUNK_OUTPOINTS)
                .matches('?')
                .count(),
            crate::logic::D1_CHUNK_OUTPOINTS
        );
    }
}
