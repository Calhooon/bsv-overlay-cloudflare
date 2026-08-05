//! `tm_potparty` marker SIGNATURE VALIDITY — decoded ONCE at admission and
//! latched onto the row, so every downstream SQL window can ORDER BY it
//! (bsv-low #283).
//!
//! # Why this exists (and why it is NOT the overlay validating anything)
//!
//! Every `potparty_records` window in this system is a fixed-size slot over
//! attacker-writable rows allocated by an ORDERING an attacker controls:
//!
//! - the per-`(pot, committedKey)` seat window (`seat_markers_sql`, cap 8)
//!   — 8 forged rows stamped ahead of the honest marker evicted it, which
//!   erased the caller's `/results` seat attribution PERMANENTLY;
//! - the per-pot representative-row collapse (`rn = 1`, `createdAt ASC`) —
//!   ONE earlier row owned the row's `gameId` / `opponentIdentity` /
//!   `recoveryHeight`;
//! - the unknown-pot promotion quota — ghost markers took the reserved
//!   slots and demoted a genuinely fresh, still-in-flight pot.
//!
//! All three are the same defect (epoch Rule 3: *exclusivity is the bug; an
//! index is a set, not a slot*), and no window size or sort order closes any
//! of them — the row that must win is *the one whose signatures VERIFY*,
//! which SQL cannot compute. The premise "SQL cannot compute it" held only
//! because **nobody stored the answer**. This module stores it.
//!
//! The result is an ORDERING HINT and NOTHING ELSE:
//!
//! - admission is still BYTE-FORMAT-ONLY — `tm_potparty` admits exactly what
//!   it admitted before, this column changes no admission decision, and a
//!   marker whose signatures do not verify is still stored and still served;
//! - every consumer that draws a CONCLUSION from a marker
//!   (`results::attribute_seats`, the client's `lookupPotParty`) re-verifies
//!   unconditionally, so a lying or stale column can MIS-ORDER candidates and
//!   can never admit one;
//! - it is used as a SORT KEY, never as a `WHERE` clause. The one exception
//!   is the unknown-pot PROMOTION tier, which is itself an ordering (a
//!   demoted row is still served, just behind every indexed pot) — the same
//!   fail direction the freshness bound already has.
//!
//! That bound matters, because a cross-language validity bar fails toward
//! **refusing honest work** (epoch Rule 16): if this predicate ever
//! disagreed with the client's signer, an honest marker would rank 0 — and
//! ranking 0 in a window it is the only occupant of changes nothing. The
//! agreement is pinned against FROZEN artifacts emitted by the real client
//! producer (`golden_client_v1_marker_is_valid_server_side` /
//! `golden_client_v2_marker_is_valid_server_side`), never against a fixture
//! this crate signed for itself.
//!
//! # What an attacker would need to reach rank 2
//!
//! `sigValid = 1` on a v2 marker means BOTH bars cleared: the `seatSig`
//! verifies under the marker's OWN claimed `seatSettlePubkey`, and the
//! marker's identity signature verifies under its OWN claimed `identity`.
//! Combined with the two bindings the CALLER supplies at read time — the
//! `identity = ?` scope of every per-identity window, and the `IN (pubA,
//! pubB)` committed-key prefilter of `seat_markers_sql`, whose keys come
//! from the pot's own funding lock — a row can only outrank an honest one
//! if the attacker holds the victim's identity key or a settle key the
//! victim's covenant lock committed. Neither is out-stampable, out-numbered
//! or free (contrast bsv-low#347: filing a marker itself IS free — the
//! SEEN-gate is opt-in by the submitter — so any bound priced in "dust per
//! row" is wrong, and this one is not priced at all).

use crate::potparty::{PotpartyMarker, POTPARTY_TAG, POTPARTY_TAG_V2};

/// The frozen seatSig domain tag — MUST equal the client's
/// `potParty.ts::POTPARTY_SEATSIG_DOMAIN` byte-for-byte.
pub const POTPARTY_SEATSIG_DOMAIN: &[u8] = b"LOW/potparty/v2/seatsig|";

/// The D1 column this module's verdict is latched into.
pub const SIG_VALID_COLUMN: &str = "sigValid";

/// The SQL rank expression every `potparty_records` window sorts by FIRST,
/// `DESC` (higher wins). Built from [`SIG_VALID_COLUMN`] so the column name
/// exists in exactly one place (epoch Rule 16 — share the constant, never
/// the convention).
///
/// Three tiers, and the middle one is load-bearing:
///
/// | rank | `sigValid` | meaning |
/// |---|---|---|
/// | 2 | `1` | evaluated at admission, every signature the marker carries VERIFIED |
/// | 1 | `NULL` | never evaluated — a row admitted BEFORE this migration |
/// | 0 | `0` | evaluated, at least one signature did NOT verify |
///
/// A two-tier scheme (treat `NULL` as valid) would leave the pre-migration
/// window PERMANENT: a legacy junk row and a fresh honest republish would
/// tie at the top and `createdAt ASC` would hand it back to the attacker.
/// With three tiers the honest seat's next republish (`potPartyRepublish.ts`,
/// bsv-low#252 — an existing sweep, not new client work) lands at rank 2 and
/// outranks EVERY legacy row, so the residual **self-heals** instead of
/// being permanent (epoch Rule 6).
///
/// `prefix` is the table alias plus `.` (e.g. `"pp."`), or `""` when the
/// query has no alias.
pub fn sig_rank_expr(prefix: &str) -> String {
    format!(
        "CASE WHEN {prefix}{SIG_VALID_COLUMN} = 1 THEN 2 \
              WHEN {prefix}{SIG_VALID_COLUMN} IS NULL THEN 1 ELSE 0 END"
    )
}

/// The BRC-43 protocol potparty markers sign their IDENTITY challenge under
/// — the client's `potParty.ts::POTPARTY_PROTOCOL` = `[1, 'low potparty']`.
pub fn potparty_protocol() -> bsv_rs::wallet::Protocol {
    bsv_rs::wallet::Protocol::new(bsv_rs::wallet::SecurityLevel::App, "low potparty")
}

/// The v1 IDENTITY-signature challenge — byte-identical to the client's
/// `potPartyChallenge`: the v1 tag + the six bound fields, raw bytes, u32s
/// little-endian.
pub fn potparty_v1_challenge(
    identity: &[u8],
    opponent: &[u8],
    game_id: &[u8],
    pot_txid: &[u8],
    pot_vout: u32,
    recovery_height: u32,
) -> Option<Vec<u8>> {
    if identity.len() != 33 || opponent.len() != 33 || game_id.len() != 32 || pot_txid.len() != 32 {
        return None;
    }
    let mut out = Vec::with_capacity(POTPARTY_TAG.len() + 33 + 33 + 32 + 32 + 4 + 4);
    out.extend_from_slice(POTPARTY_TAG);
    out.extend_from_slice(identity);
    out.extend_from_slice(opponent);
    out.extend_from_slice(game_id);
    out.extend_from_slice(pot_txid);
    out.extend_from_slice(&pot_vout.to_le_bytes());
    out.extend_from_slice(&recovery_height.to_le_bytes());
    Some(out)
}

/// The v2 IDENTITY-signature challenge — byte-identical to the client's
/// `potPartyV2Challenge`: v1's fields plus `seatSettlePubkey`, under the v2
/// tag.
pub fn potparty_v2_challenge(
    identity: &[u8],
    opponent: &[u8],
    game_id: &[u8],
    pot_txid: &[u8],
    pot_vout: u32,
    recovery_height: u32,
    seat_settle_pubkey: &[u8],
) -> Option<Vec<u8>> {
    if identity.len() != 33
        || opponent.len() != 33
        || game_id.len() != 32
        || pot_txid.len() != 32
        || seat_settle_pubkey.len() != 33
    {
        return None;
    }
    let mut out = Vec::with_capacity(POTPARTY_TAG_V2.len() + 33 + 33 + 32 + 32 + 4 + 4 + 33);
    out.extend_from_slice(POTPARTY_TAG_V2);
    out.extend_from_slice(identity);
    out.extend_from_slice(opponent);
    out.extend_from_slice(game_id);
    out.extend_from_slice(pot_txid);
    out.extend_from_slice(&pot_vout.to_le_bytes());
    out.extend_from_slice(&recovery_height.to_le_bytes());
    out.extend_from_slice(seat_settle_pubkey);
    Some(out)
}

/// The EXACT cross-repo seatSig preimage (bsv-low #230; the client's
/// `potPartySeatSigPreimage`):
///
///   `"LOW/potparty/v2/seatsig|" ‖ gameId(32) ‖ potTxid(32) ‖ potVout(4 LE)
///    ‖ identity(33)`
///
/// `seatSig` is ECDSA over a SINGLE sha256 of these bytes (the BRC-100
/// `createSignature({data})` hash). `None` when any field is malformed — an
/// unbuildable preimage can never verify (fail-safe).
pub fn seatsig_preimage(
    game_id: &[u8],
    pot_txid: &[u8],
    pot_vout: u32,
    identity: &[u8],
) -> Option<Vec<u8>> {
    if game_id.len() != 32 || pot_txid.len() != 32 || identity.len() != 33 {
        return None;
    }
    let mut out = Vec::with_capacity(POTPARTY_SEATSIG_DOMAIN.len() + 32 + 32 + 4 + 33);
    out.extend_from_slice(POTPARTY_SEATSIG_DOMAIN);
    out.extend_from_slice(game_id);
    out.extend_from_slice(pot_txid);
    out.extend_from_slice(&pot_vout.to_le_bytes());
    out.extend_from_slice(identity);
    Some(out)
}

/// CANONICAL STRICT DER + LOW-S (epoch Rule 4c), the ONE gate both signature
/// bars go through.
///
/// `from_der` tolerates trailing bytes, so the encoding is re-derived and
/// byte-compared; `is_low_s` is asserted here rather than inherited from
/// whatever the verifier happens to do, because "the verifier enforces it"
/// is a claim about a dependency and this is the layer that must not drift.
///
/// Why this is load-bearing for #283 specifically: the latch turns "does
/// this verify" into a SORT KEY over a capped window, so any way to mint
/// distinct byte strings that still latch `1` is a cost multiplier against
/// the honest row. **Both gates executed exactly that** — 24 padded replays
/// of one honest marker saturated `seat_markers_sql`'s 8-slot window and
/// evicted the row they were replaying. The seat bar had this check from the
/// start; the IDENTITY bar did not, and was harmless only because
/// `verify_identity_binding` happened to share the leniency. A coincidence
/// is not a design.
///
/// This can never reject an honest marker: the client's wallet emits
/// canonical low-S DER, pinned end-to-end by the frozen goldens.
///
/// NOTE ON THE `is_low_s` LEG, because a reviewer will ask (Rule 1 says a
/// dominated check is deleted): today it IS dominated — `Signature::to_der()`
/// normalises to low-S, so a high-S input already fails the byte-compare. It
/// stays because the dominating rule is a DEPENDENCY's current behaviour, not
/// an on-chain one, and a silent change to `to_der` would re-open the axis
/// with nothing left watching it. `no_malleated_variant_of_an_honest_marker_
/// latches_valid` asserts the PROPERTY rather than which leg enforces it, so
/// the pin survives either arrangement.
fn canonical_der(sig: &[u8]) -> Option<bsv_rs::primitives::ec::Signature> {
    let parsed = bsv_rs::primitives::ec::Signature::from_der(sig).ok()?;
    if parsed.to_der() != sig || !parsed.is_low_s() {
        return None;
    }
    Some(parsed)
}

/// Verify a marker's SEAT signature: plain secp256k1 ECDSA under the
/// marker's OWN `seat_settle_pubkey` over sha256(preimage). Lock membership
/// is a SEPARATE check the reader does against the pot's committed keys
/// (`results::attribute_seats`) — this predicate deliberately knows nothing
/// about any lock, because admission has no pot to read one from.
///
/// Signature encoding goes through [`canonical_der`] — see there for why.
pub fn verify_seat_sig(
    game_id: &[u8],
    pot_txid: &[u8],
    pot_vout: u32,
    identity: &[u8],
    seat_settle_pubkey: &[u8],
    seat_sig: &[u8],
) -> bool {
    let Some(preimage) = seatsig_preimage(game_id, pot_txid, pot_vout, identity) else {
        return false;
    };
    let Ok(pubkey) = bsv_rs::primitives::ec::PublicKey::from_bytes(seat_settle_pubkey) else {
        return false;
    };
    let Some(sig) = canonical_der(seat_sig) else {
        return false;
    };
    let hash = bsv_rs::primitives::hash::sha256(&preimage);
    pubkey.verify(&hash, &sig)
}

/// Verify a marker's IDENTITY signature: the claimed `identity` really
/// published these bytes (BRC-42/43 'anyone' verification, protocol
/// `[1,'low potparty']`, keyID = the lowercase-hex gameId — the exact recipe
/// of the client's `verifyPotPartyMarker` / `verifyPotPartyV2Marker`).
pub fn verify_identity_sig(identity: &[u8], game_id: &[u8], challenge: &[u8], sig: &[u8]) -> bool {
    let Ok(signer) = bsv_rs::primitives::ec::PublicKey::from_bytes(identity) else {
        return false;
    };
    // Same bar as the seat signature — see `canonical_der`. Without it a
    // padded or high-S replay of ONE honest marker mints unlimited distinct
    // rows that all latch `sigValid = 1`, and 24 of them were measured
    // evicting the very row they replay.
    if canonical_der(sig).is_none() {
        return false;
    }
    bsv_rs::wallet::ProtoWallet::anyone()
        .verify_signature(bsv_rs::wallet::VerifySignatureArgs {
            data: Some(challenge.to_vec()),
            hash_to_directly_verify: None,
            signature: sig.to_vec(),
            protocol_id: potparty_protocol(),
            key_id: hex::encode(game_id),
            counterparty: Some(bsv_rs::wallet::Counterparty::Other(signer)),
            for_self: Some(false),
        })
        .map(|r| r.valid)
        .unwrap_or(false)
}

/// THE LATCH — does every signature this marker carries verify?
///
/// - v1 (`seat_settle_pubkey` / `seat_sig` absent): the identity signature
///   over the v1 challenge.
/// - v2: the identity signature over the v2 challenge **AND** the seatSig
///   under the marker's own claimed settle key.
///
/// Total: at most two ECDSA verifies plus one BRC-42 derivation per admitted
/// marker, computed once, at write time — never per read, and never per row
/// of a read (epoch Rule 21: verify at write, serve at read).
///
/// A malformed field is `false`, never a panic and never an error that could
/// fail the admission: this is a hint, and a hint must not be able to refuse
/// a marker the byte-format gate accepted.
pub fn marker_sig_valid(m: &PotpartyMarker) -> bool {
    match (m.seat_settle_pubkey.as_deref(), m.seat_sig.as_deref()) {
        (Some(settle_pk), Some(seat_sig)) => {
            let Some(challenge) = potparty_v2_challenge(
                &m.identity,
                &m.opponent,
                &m.game_id,
                &m.pot_txid,
                m.pot_vout,
                m.recovery_height,
                settle_pk,
            ) else {
                return false;
            };
            verify_identity_sig(&m.identity, &m.game_id, &challenge, &m.sig)
                && verify_seat_sig(
                    &m.game_id,
                    &m.pot_txid,
                    m.pot_vout,
                    &m.identity,
                    settle_pk,
                    seat_sig,
                )
        }
        // A HALF-v2 row cannot exist off the parser (both pushes or
        // neither), but the record type allows it — treat it as unprovable.
        (Some(_), None) | (None, Some(_)) => false,
        (None, None) => {
            let Some(challenge) = potparty_v1_challenge(
                &m.identity,
                &m.opponent,
                &m.game_id,
                &m.pot_txid,
                m.pot_vout,
                m.recovery_height,
            ) else {
                return false;
            };
            verify_identity_sig(&m.identity, &m.game_id, &challenge, &m.sig)
        }
    }
}

/// [`marker_sig_valid`] over the STORED record shape (hex strings), which is
/// what the D1 writer holds. Malformed hex is `false` (fail-safe: an
/// un-decodable row is not "verified", and rank 0 only ever costs it
/// ordering precedence).
pub fn record_sig_valid(r: &crate::potparty::storage::PotpartyRecord) -> bool {
    fn dec(s: &str) -> Option<Vec<u8>> {
        hex::decode(s.to_ascii_lowercase()).ok()
    }
    let (Some(identity), Some(opponent), Some(game_id), Some(pot_txid), Some(sig)) = (
        dec(&r.identity),
        dec(&r.opponent_identity),
        dec(&r.game_id),
        dec(&r.pot_txid),
        dec(&r.sig_hex),
    ) else {
        return false;
    };
    let (Ok(game_id), Ok(pot_txid)) = (
        <[u8; 32]>::try_from(game_id.as_slice()),
        <[u8; 32]>::try_from(pot_txid.as_slice()),
    ) else {
        return false;
    };
    let seat = match (r.seat_settle_pubkey.as_deref(), r.seat_sig_hex.as_deref()) {
        (Some(pk), Some(s)) => match (dec(pk), dec(s)) {
            (Some(pk), Some(s)) => Some((pk, s)),
            _ => return false,
        },
        (None, None) => None,
        _ => return false,
    };
    marker_sig_valid(&PotpartyMarker {
        identity,
        opponent,
        game_id,
        pot_txid,
        pot_vout: r.pot_vout,
        recovery_height: r.recovery_height,
        sig,
        seat_settle_pubkey: seat.as_ref().map(|(pk, _)| pk.clone()),
        seat_sig: seat.map(|(_, s)| s),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The CROSS-REPO crypto round-trip for v1 (epoch Rule 16: share the
    /// ARTIFACT, never the convention). This exact hex is FROZEN in the
    /// client's `potParty.test.ts` GOLDEN cell — real `@bsv/sdk`
    /// `ProtoWallet(PrivateKey(1))` output, RFC6979-deterministic. If either
    /// side's challenge layout, hashing depth or protocol tuple ever drifts,
    /// this cell goes red.
    ///
    /// This is the cell that makes the whole change SAFE to ship: a
    /// cross-language disagreement inside a validity predicate fails toward
    /// refusing HONEST work, all at once, so it has to be proven against the
    /// real producer's bytes and not against a fixture this crate signed.
    const GOLDEN_V1_HEX: &str = "006a0f4c4f572f706f7470617274792f7631210279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f817982103bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb20cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc20dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd04010000000428a00e00473045022100bf1536ef5a90073214076e91117fa59da158665edfaf3833f438b9cc7cc34fbb0220144732cb31c8d8da2e836ada3f4e69511b8f065016f11feadb30282f12666751";

    /// The client's FROZEN v2 golden (same source, `potParty.test.ts`).
    const GOLDEN_V2_HEX: &str = "006a0f4c4f572f706f7470617274792f7632210279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f817982102c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee520cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc20dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd04010000000428a00e002103d3e37fc9edbd1c225d703873b45f66368e86c633cb613252b3254ffe0b8ad5ee4630440220106a632f58753f6b9ebaf20d105874d3aed43c28dab90e8b6a8a51dbd610e1e402204c7837248995842ec551eb3c8510b5862f87bf0c54368534fd3d7c1e3b9a50fd473045022100d3ea901d46fa588cb2f20e0bb0a3c7e23f6320138efee69f9e506a8e79abbaa102207cfccbd475e5d9e789091acdfa7d81503b950ebf51da6a1ac9fec44c84553773";

    fn golden(hex_str: &str) -> PotpartyMarker {
        crate::potparty::parse_potparty_marker(&hex::decode(hex_str).unwrap())
            .expect("the frozen client golden parses")
    }

    #[test]
    fn golden_client_v1_marker_is_valid_server_side() {
        let m = golden(GOLDEN_V1_HEX);
        assert!(m.seat_settle_pubkey.is_none(), "the v1 golden is a v1 marker");
        assert!(
            marker_sig_valid(&m),
            "the client's REAL v1 identity signature must verify server-side"
        );
    }

    #[test]
    fn golden_client_v2_marker_is_valid_server_side() {
        let m = golden(GOLDEN_V2_HEX);
        assert!(m.seat_settle_pubkey.is_some(), "the v2 golden is a v2 marker");
        assert!(
            marker_sig_valid(&m),
            "the client's REAL v2 identity + seat signatures must both verify server-side"
        );
    }

    /// Every field the challenge binds is load-bearing, on BOTH versions:
    /// tamper it and the latch drops to `false`. Injected by MUTATING the
    /// golden, so nothing here is signed by this crate.
    #[test]
    fn tampering_any_bound_field_invalidates_the_latch() {
        for hex_str in [GOLDEN_V1_HEX, GOLDEN_V2_HEX] {
            let base = golden(hex_str);

            let mut swapped = base.clone();
            std::mem::swap(&mut swapped.identity, &mut swapped.opponent);
            assert!(!marker_sig_valid(&swapped), "identity/opponent swap");

            let mut vout = base.clone();
            vout.pot_vout += 1;
            assert!(!marker_sig_valid(&vout), "potVout");

            let mut rh = base.clone();
            rh.recovery_height += 1;
            assert!(!marker_sig_valid(&rh), "recoveryHeight");

            let mut pot = base.clone();
            pot.pot_txid[0] ^= 0xff;
            assert!(!marker_sig_valid(&pot), "potTxid");

            let mut game = base.clone();
            game.game_id[0] ^= 0xff;
            assert!(!marker_sig_valid(&game), "gameId");

            let mut sig = base.clone();
            let last = sig.sig.len() - 1;
            sig.sig[last] ^= 0xff;
            assert!(!marker_sig_valid(&sig), "identity sig bytes");
        }
    }

    /// The v2-only bars: the seat signature and the settle key it names.
    #[test]
    fn v2_seat_signature_is_a_separate_bar() {
        let base = golden(GOLDEN_V2_HEX);

        // Break ONLY the seatSig — the identity signature is untouched, so
        // this proves the seat bar is genuinely enforced and not shadowed.
        let mut seat = base.clone();
        let s = seat.seat_sig.as_mut().unwrap();
        let last = s.len() - 1;
        s[last] ^= 0xff;
        assert!(!marker_sig_valid(&seat), "seatSig bytes");

        // Re-pointing the settle key breaks BOTH (it is inside the v2
        // challenge AND it is the seatSig's verification key).
        let mut pk = base.clone();
        pk.seat_settle_pubkey = Some(hex::decode(
            "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
        )
        .unwrap());
        assert!(!marker_sig_valid(&pk), "seatSettlePubkey");
    }

    /// secp256k1 group order, for minting the high-S twin of a signature.
    const SECP256K1_N: [u8; 32] = [
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xfe, 0xba, 0xae, 0xdc, 0xe6, 0xaf, 0x48, 0xa0, 0x3b, 0xbf, 0xd2, 0x5e, 0x8c, 0xd0, 0x36,
        0x41, 0x41,
    ];

    /// Minimal DER INTEGER: strip leading zeros, re-add one if the high bit
    /// is set. Hand-rolled ON PURPOSE — `Signature::to_der()` silently
    /// normalises to low-S, so using it here would produce a "twin" identical
    /// to the original and the cell would assert nothing (it did, on the
    /// first run; the `assert_ne!` caught it).
    fn der_int(v: &[u8; 32]) -> Vec<u8> {
        let mut b: &[u8] = v;
        while b.len() > 1 && b[0] == 0 {
            b = &b[1..];
        }
        let mut out = Vec::new();
        if b[0] & 0x80 != 0 {
            out.push(0x00);
        }
        out.extend_from_slice(b);
        out
    }

    /// Re-encode a DER signature with `s -> N - s`. The result verifies under
    /// the SAME key over the SAME digest (ECDSA is malleable in exactly this
    /// way) but is a different byte string — the classic mint-unlimited-
    /// valid-variants primitive, and a DIFFERENT SPELLING from trailing-byte
    /// padding (epoch Rule 12a: a pin verified only against the injection it
    /// was written for pins a string, not a property).
    fn high_s_twin(der: &[u8]) -> Vec<u8> {
        let sig = bsv_rs::primitives::ec::Signature::from_der(der).expect("parses");
        let s = *sig.s();
        // 256-bit N - s, big-endian, schoolbook borrow.
        let mut hi = [0u8; 32];
        let mut borrow = 0i32;
        for i in (0..32).rev() {
            let d = SECP256K1_N[i] as i32 - s[i] as i32 - borrow;
            if d < 0 {
                hi[i] = (d + 256) as u8;
                borrow = 1;
            } else {
                hi[i] = d as u8;
                borrow = 0;
            }
        }
        let (r_b, s_b) = (der_int(sig.r()), der_int(&hi));
        let mut out = vec![0x30, (4 + r_b.len() + s_b.len()) as u8, 0x02, r_b.len() as u8];
        out.extend_from_slice(&r_b);
        out.push(0x02);
        out.push(s_b.len() as u8);
        out.extend_from_slice(&s_b);
        out
    }

    /// EPOCH RULE 4c, ON BOTH SIGNATURES AND BOTH MALLEABILITY AXES.
    ///
    /// An observer who can see one honest marker must not be able to mint
    /// distinct rows that still latch `valid` — since #283 the latch is a
    /// SORT KEY over a capped window, so unlimited `sigValid = 1` variants of
    /// one honest marker are a cost multiplier against the row they replay.
    /// Both adversarial gates executed this: **24 padded IDENTITY-signature
    /// replays saturated `seat_markers_sql`'s 8-slot window and evicted the
    /// exact honest row they were replaying**, because `verify_seat_sig`
    /// re-encoded and byte-compared while `verify_identity_sig` did not. It
    /// was harmless only because `verify_identity_binding` happened to share
    /// the leniency — a coincidence, not a design.
    ///
    /// Four legs: {seat, identity} × {trailing-byte padding, high-S twin}.
    #[test]
    fn no_malleated_variant_of_an_honest_marker_latches_valid() {
        let base = golden(GOLDEN_V2_HEX);
        assert!(marker_sig_valid(&base), "positive control: the golden latches");

        // --- SEAT signature.
        let mut padded = base.clone();
        padded.seat_sig.as_mut().unwrap().push(0x00);
        assert!(!marker_sig_valid(&padded), "seat: trailing-byte DER padding");

        let mut high_s = base.clone();
        let twin = high_s_twin(base.seat_sig.as_ref().unwrap());
        assert_ne!(&twin, base.seat_sig.as_ref().unwrap(), "the twin is different bytes");
        high_s.seat_sig = Some(twin);
        assert!(!marker_sig_valid(&high_s), "seat: high-S twin");

        // --- IDENTITY signature (the leg the gates broke).
        let mut padded_id = base.clone();
        padded_id.sig.push(0x00);
        assert!(!marker_sig_valid(&padded_id), "identity: trailing-byte DER padding");

        let mut high_s_id = base.clone();
        high_s_id.sig = high_s_twin(&base.sig);
        assert_ne!(high_s_id.sig, base.sig, "the twin is different bytes");
        assert!(!marker_sig_valid(&high_s_id), "identity: high-S twin");

        // …and the same four legs on v1, which carries only the identity sig.
        let v1 = golden(GOLDEN_V1_HEX);
        assert!(marker_sig_valid(&v1), "positive control: the v1 golden latches");
        let mut v1_padded = v1.clone();
        v1_padded.sig.push(0x00);
        assert!(!marker_sig_valid(&v1_padded), "v1 identity: padding");
        let mut v1_high_s = v1.clone();
        v1_high_s.sig = high_s_twin(&v1.sig);
        assert!(!marker_sig_valid(&v1_high_s), "v1 identity: high-S twin");
    }

    /// A v1 marker's bytes are NOT a valid v2 marker's bytes and vice versa:
    /// the tag is inside both challenges, so a version confusion cannot
    /// launder one into the other.
    #[test]
    fn the_version_tag_is_inside_the_challenge() {
        let v1 = golden(GOLDEN_V1_HEX);
        // Pretend the v1 marker were a v2 one by attaching a settle key: the
        // v2 challenge is a different byte string, so it cannot verify.
        let mut faked = v1.clone();
        faked.seat_settle_pubkey =
            Some(hex::decode("02c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5").unwrap());
        faked.seat_sig = Some(vec![0x30; 70]);
        assert!(!marker_sig_valid(&faked));
    }

    /// The rank expression is built from the column constant and orders the
    /// three tiers the way the windows depend on. Asserted on the CONSTRUCT
    /// (epoch Rule 9), and executed against real SQLite by the consumers'
    /// window tests.
    #[test]
    fn sig_rank_expr_uses_the_shared_column_and_three_tiers() {
        let e = sig_rank_expr("pp.");
        assert_eq!(
            e.matches(&format!("pp.{SIG_VALID_COLUMN}")).count(),
            2,
            "both legs reference the shared column name: {e}"
        );
        assert!(e.contains("THEN 2"), "verified tier: {e}");
        assert!(e.contains("IS NULL THEN 1"), "legacy tier: {e}");
        assert!(e.contains("ELSE 0"), "invalid tier: {e}");
        assert_eq!(sig_rank_expr("").matches(SIG_VALID_COLUMN).count(), 2);
    }

    /// `record_sig_valid` (the hex/stored shape the D1 writer actually
    /// holds) agrees with `marker_sig_valid` (the parsed shape) — the two
    /// entry points must not drift (epoch Rule 10: when a claim is about two
    /// surfaces agreeing, the cell must call BOTH).
    #[test]
    fn record_and_marker_entry_points_agree() {
        use crate::potparty::storage::PotpartyRecord;
        for hex_str in [GOLDEN_V1_HEX, GOLDEN_V2_HEX] {
            let m = golden(hex_str);
            let rec = PotpartyRecord {
                identity: hex::encode(&m.identity),
                opponent_identity: hex::encode(&m.opponent),
                game_id: hex::encode(m.game_id),
                pot_txid: hex::encode(m.pot_txid),
                pot_vout: m.pot_vout,
                recovery_height: m.recovery_height,
                sig_hex: hex::encode(&m.sig),
                seat_settle_pubkey: m.seat_settle_pubkey.as_ref().map(hex::encode),
                seat_sig_hex: m.seat_sig.as_ref().map(hex::encode),
                txid: "aa".repeat(32),
                output_index: 0,
                created_at: 0,
            };
            assert!(record_sig_valid(&rec));
            assert_eq!(record_sig_valid(&rec), marker_sig_valid(&m));

            let mut bad = rec.clone();
            bad.recovery_height += 1;
            assert!(!record_sig_valid(&bad));

            let mut junk = rec;
            junk.sig_hex = "zz".repeat(70);
            assert!(!record_sig_valid(&junk), "malformed hex is fail-safe false");
        }
    }
}
