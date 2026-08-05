//! `tm_hopparty` marker VALIDITY — decided ONCE at admission and latched
//! onto the row, so `/hops-view` is a plain read + sort (bsv-low #362).
//!
//! # Why this exists: a BUDGET on a correctness check
//!
//! `/hops-view` used to answer "does this marker verify?" at READ time, for
//! every row, on every request: two ECDSA verifications plus a BRC-42
//! derivation plus a container re-check, per row, per reader. Work that
//! large cannot be unbounded, so the code had grown a constant —
//! `HOPS_VIEW_VERIFY_BUDGET = 150` — and everything downstream of it: a
//! third `unknown` verdict meaning "I did not get to check this", a body-level
//! `verifyBudgetExhausted` bit to report it, a consumer obliged to handle
//! that bit, and a client display that counted only `verified` rows and so
//! rendered an unchecked row identically to an ABSENT one.
//!
//! Six problems, one cause: **write-time work on the read path** (epoch
//! Rule 25). The budget was the tell — nobody rations a hash-map lookup.
//!
//! The security argument is the stronger one. Under read-time verification a
//! junk row an attacker files ONCE makes every honest reader pay the ECDSA to
//! reject it, forever. That inverts who pays: attacker cost is one
//! submission, defender cost is one verification per row per reader per
//! request. Any design where rejecting an attacker's object costs the
//! defender more than creating it cost the attacker is a DoS amplifier,
//! however correct its answers. Verifying at ADMISSION puts the cost on the
//! submitter, beside the on-chain fee they already paid.
//!
//! # What this module is, and is NOT
//!
//! It mirrors [`crate::potparty::validity`] (bsv-low #283) exactly: an
//! ORDERING HINT latched in a column, and nothing else.
//!
//! - Admission stays BYTE-FORMAT-ONLY. `tm_hopparty` admits exactly what it
//!   admitted before; this column changes no admission decision; a marker
//!   whose verdict is `0` is still stored, still served, still labelled.
//! - The latched verdict is a **leading SORT KEY, never a `WHERE`**. A
//!   0-latched row is deprioritised, never hidden — hiding recreates the
//!   invisible-money class (epoch Rule 23 / bsv-low #358).
//! - Every consumer that draws a CONCLUSION from a marker re-verifies: both
//!   signatures ride back to the client verbatim in `/hops-view`'s body. So
//!   a lying or stale column can MIS-ORDER candidates and can never admit
//!   one.
//!
//! # What the latch DOES buy, which the read-time filter could not
//!
//! `/hops-view`'s window is a fixed-size page over attacker-writable rows.
//! Because verification happened AFTER paging, an attacker who could crowd
//! the candidate set evicted the honest row before anything verified it —
//! the documented residual was a reactive flood at *hop value + 1* per
//! outpoint, or one coin CHAINED through k transactions at ~1.06× the
//! victim's hop in fully RECOVERABLE capital.
//!
//! Once the verdict is a column, SQL can lead its `ORDER BY` with it, so
//! **verification happens before paging**. Reaching the top tier now requires
//! a container that really pays `hopSats` to the VICTIM'S OWN settle key —
//! money the attacker hands the victim and cannot take back, per outpoint,
//! non-chainable (the outputs are not theirs to spend). And because the bar
//! is EXACTLY `hopSats`, such a row can only ever TIE the honest row on the
//! value key and then loses the oldest-first tie-break, so the remaining
//! eviction needs a SUBMIT RACE on top of the payment.
//!
//! That repricing is MEASURED, not asserted, in `low-app-layer`'s
//! `tests/hops_view_sqlite.rs`:
//! `a_value_plus_one_reactive_flood_no_longer_evicts_the_honest_row`,
//! `a_chained_single_coin_flood_no_longer_evicts_the_honest_row`, and
//! `a_paid_replay_flood_is_the_remaining_residual` (both legs).
//!
//! # A `0` IS A VERDICT, NOT "not yet checked"
//!
//! `markerValid` is written once, by `INSERT OR IGNORE`, and re-evaluated by
//! nothing: no `UPDATE hopparty_records SET markerValid` exists in production
//! code. A TRANSIENT fault in this predicate — a `bsv-rs` DER/`to_der`
//! behaviour change, a wallet emitting a non-canonical signature during a
//! rollout, a partial deploy — therefore demotes every honest row admitted in
//! that window to rank 0 PERMANENTLY, with no self-healing path (epoch
//! Rule 6), and its victims are wiped-device users seeing a silently short
//! enumeration, i.e. the population least able to report it (Rule 14).
//!
//! That is why the re-latch pass (bsv-low#367) is scoped as a pass over
//! **EVERY row**, with the closure criterion "every row's `markerValid`
//! equals [`record_marker_valid`] recomputed at the pass's own predicate
//! version". A `WHERE markerValid IS NULL` census structurally skips exactly
//! the rows such a fault would have created.
//!
//! **Rows admitted before this change carry no verdict, and that is PERMANENT
//! absent that pass.** It does not self-heal: a hopparty marker must ride the
//! hop transaction, and that transaction is already on chain, so there is no
//! republish that could re-latch it. `/hops-view` labels those rows
//! `markerVerified: "unknown"` — which is now the ONLY producer of that word,
//! and means "this row predates the latch", never "the budget ran out".

use crate::hopparty::{hopparty_identity_challenge, hopparty_seatsig_preimage, HoppartyMarker};
use crate::potparty::validity::canonical_der;

/// The D1 column this module's verdict is latched into.
pub const MARKER_VALID_COLUMN: &str = "markerValid";

/// The SQL rank expression every `hopparty_records` window sorts by FIRST,
/// `DESC` (higher wins). Built from [`MARKER_VALID_COLUMN`] so the column
/// name exists in exactly one place (epoch Rule 16 — share the constant,
/// never the convention).
///
/// Three tiers, and the middle one is load-bearing:
///
/// | rank | `markerValid` | meaning |
/// |---|---|---|
/// | 2 | `1` | evaluated at admission; both signatures verified AND the container pays the claim |
/// | 1 | `NULL` | never evaluated — a row admitted BEFORE this migration |
/// | 0 | `0` | evaluated, and REFUTED |
///
/// A two-tier scheme (treat `NULL` as valid) would be strictly worse: a
/// legacy junk row and a latched honest row would tie at the top and the
/// remaining keys would hand the slot back to the attacker. Three tiers at
/// least let ANY latched row outrank the whole legacy tier.
///
/// `prefix` is the table alias plus `.` (e.g. `"hp."`), or `""` when the
/// query has no alias.
pub fn marker_rank_expr(prefix: &str) -> String {
    format!(
        "CASE WHEN {prefix}{MARKER_VALID_COLUMN} = 1 THEN 2 \
              WHEN {prefix}{MARKER_VALID_COLUMN} IS NULL THEN 1 ELSE 0 END"
    )
}

/// The P2PKH lock a hop paying `seat_settle_pubkey` MUST carry:
/// `OP_DUP OP_HASH160 <hash160(pubkey)> OP_EQUALVERIFY OP_CHECKSIG`,
/// lowercase hex. `None` when the pubkey is malformed (not 33 bytes) — an
/// unbuildable expectation can never verify (fail-safe).
///
/// THE one copy. `low_app_layer::hops_view` re-exports this rather than
/// keeping its own (epoch Rule 10: the durable fix for "two things must
/// agree" is deleting one of them).
pub fn expected_hop_lock_hex(seat_settle_pubkey: &[u8]) -> Option<String> {
    if seat_settle_pubkey.len() != 33 {
        return None;
    }
    let h = bsv_rs::primitives::hash::hash160(seat_settle_pubkey);
    Some(format!("76a914{}88ac", hex::encode(h)))
}

/// Verify a marker's SEAT signature: plain secp256k1 ECDSA under the
/// marker's OWN `seat_settle_pubkey` over sha256(the hopparty seatsig
/// preimage). No BRC-42 derivation is needed — the marker names the public
/// key directly, and whether that key is the RIGHT one is settled by bar 1
/// (the container's own lock pays exactly it).
///
/// Signature encoding goes through the SHARED [`canonical_der`] gate (epoch
/// Rule 4c): `from_der` tolerates trailing bytes and non-minimal INTEGERs,
/// both of which were MEASURED verifying on a real row, so the encoding is
/// re-derived and byte-compared. Without it an observer can mint unlimited
/// distinct byte strings that all latch `1` from ONE honest marker, which is
/// a cost multiplier against the row they replay.
pub fn verify_seat_sig(
    game_id: &[u8; 32],
    hop_vout: u32,
    identity: &[u8],
    seat_settle_pubkey: &[u8],
    seat_sig: &[u8],
) -> bool {
    let Some(preimage) = hopparty_seatsig_preimage(game_id, hop_vout, identity) else {
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

/// THE LATCH — does this marker verify against the container that carries
/// it?
///
/// Three bars, cheapest first, ALL required:
///
///  1. **The hop lock.** The container's own output at `hop_vout` is
///     `P2PKH(hash160(seatSettlePubkey))`. The script at `(txid, vout)` is
///     committed by the txid, so this is hash-bound chain truth, not a
///     claim. A container with NO output at `hop_vout` is a PROVEN absence
///     (`containerOutputs` says how many it has) and REFUTES the marker.
///  2. **The hop VALUE.** That same output's satoshis equal the marker's
///     claimed `hopSats`. This is the bar that makes the paid-replay
///     residual cost real money.
///  3. **Both signatures.** `seatSig` under the marker's own settle key
///     ([`verify_seat_sig`]), and `identitySig` under the marker's own
///     claimed identity via the BRC-42 'anyone' round-trip. Bar 3b is the
///     one an attacker can never forge: an opponent CAN mint a valid seatSig
///     naming the victim's identity with its own settle key (the potparty F1
///     shape), so without it a forged row would latch `1` in the victim's
///     own view.
///
/// The identity bar reuses [`crate::potparty::validity::verify_identity_sig`]
/// verbatim — hopparty deliberately signs under the SAME wallet protocol
/// (`[1,'low potparty']`, keyID = the lowercase-hex gameId) and separates
/// domains with the version tag INSIDE the challenge, so one implementation
/// serves both and there is no second recipe to drift.
///
/// Total: at most two ECDSA verifies plus one BRC-42 derivation per admitted
/// marker, computed ONCE, at write time — never per read, and never per row
/// of a read.
///
/// A malformed field is `false`, never a panic and never an error: this is a
/// hint, and a hint must not be able to refuse a marker the byte-format gate
/// accepted.
pub fn marker_valid(
    m: &HoppartyMarker,
    hop_lock_hex: Option<&str>,
    hop_sats_on_chain: Option<u64>,
) -> bool {
    // Bar 1 + 2: the CONTAINER's own facts, decoded at admission (#310).
    let Some(expected) = expected_hop_lock_hex(&m.seat_settle_pubkey) else {
        return false;
    };
    let (Some(lock), Some(on_chain)) = (hop_lock_hex, hop_sats_on_chain) else {
        return false; // PROVEN absence refutes; it never leaves the row open
    };
    if !lock.eq_ignore_ascii_case(&expected) || on_chain != m.hop_sats {
        return false;
    }
    // Bar 3a: the seat signature.
    if !verify_seat_sig(
        &m.game_id,
        m.hop_vout,
        &m.identity,
        &m.seat_settle_pubkey,
        &m.seat_sig,
    ) {
        return false;
    }
    // Bar 3b: the identity signature — the unforgeable one.
    let Some(challenge) = hopparty_identity_challenge(
        &m.identity,
        &m.opponent,
        &m.game_id,
        m.hop_vout,
        m.hop_sats,
        &m.seat_settle_pubkey,
    ) else {
        return false;
    };
    crate::potparty::validity::verify_identity_sig(
        &m.identity,
        &m.game_id,
        &challenge,
        &m.identity_sig,
    )
}

/// [`marker_valid`] over the STORED record shape (hex strings), which is
/// what the D1 writer holds. Malformed hex is `false` (fail-safe: an
/// un-decodable row is not "verified", and rank 0 only ever costs it
/// ordering precedence).
pub fn record_marker_valid(r: &crate::hopparty::storage::HoppartyRecord) -> bool {
    fn dec(s: &str) -> Option<Vec<u8>> {
        hex::decode(s.to_ascii_lowercase()).ok()
    }
    let (
        Some(identity),
        Some(opponent),
        Some(game_id),
        Some(settle_pk),
        Some(seat_sig),
        Some(id_sig),
    ) = (
        dec(&r.identity),
        dec(&r.opponent_identity),
        dec(&r.game_id),
        dec(&r.seat_settle_pubkey),
        dec(&r.seat_sig_hex),
        dec(&r.identity_sig_hex),
    )
    else {
        return false;
    };
    let Ok(game_id) = <[u8; 32]>::try_from(game_id.as_slice()) else {
        return false;
    };
    marker_valid(
        &HoppartyMarker {
            identity,
            opponent,
            game_id,
            hop_vout: r.hop_vout,
            hop_sats: r.hop_sats,
            seat_settle_pubkey: settle_pk,
            seat_sig,
            identity_sig: id_sig,
        },
        // An impossible empty string reads as absent (which REFUTES) — the
        // same belt the app-layer row mapper applies.
        r.hop_lock_hex.as_deref().filter(|s| !s.is_empty()),
        r.hop_sats_on_chain,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hopparty::storage::HoppartyRecord;
    use crate::hopparty::{parse_hopparty_marker, GOLDEN_HOPPARTY_HEX};

    /// The FROZEN cross-repo golden marker — real `@bsv/sdk` /
    /// RFC6979-deterministic signatures, pinned byte-for-byte against the
    /// client's wire contract (`hopparty/mod.rs`). Every cell below MUTATES
    /// this artifact; nothing here is signed by this crate for itself
    /// (epoch Rule 18: a fixture built from the same wrong model as the code
    /// confirms the model).
    fn golden() -> HoppartyMarker {
        parse_hopparty_marker(&hex::decode(GOLDEN_HOPPARTY_HEX).unwrap())
            .expect("the frozen golden parses")
    }

    /// The container the golden marker's own claims describe.
    fn honest_container(m: &HoppartyMarker) -> (Option<String>, Option<u64>) {
        (
            expected_hop_lock_hex(&m.seat_settle_pubkey),
            Some(m.hop_sats),
        )
    }

    fn latch(m: &HoppartyMarker) -> bool {
        let (lock, sats) = honest_container(m);
        marker_valid(m, lock.as_deref(), sats)
    }

    /// THE positive control. A cross-language validity bar fails toward
    /// REFUSING HONEST WORK (epoch Rule 16), all at once, so agreement with
    /// the real client producer has to be pinned against FROZEN artifacts
    /// rather than assumed.
    #[test]
    fn the_golden_client_marker_latches_valid_server_side() {
        assert!(
            latch(&golden()),
            "the client's REAL seat + identity signatures must both verify \
             server-side, inside the container its own claims describe"
        );
    }

    /// Every field either digest binds is load-bearing: tamper it and the
    /// latch drops.
    #[test]
    fn tampering_any_bound_field_invalidates_the_latch() {
        let base = golden();

        let mut swapped = base.clone();
        std::mem::swap(&mut swapped.identity, &mut swapped.opponent);
        assert!(!latch(&swapped), "identity/opponent swap");

        let mut game = base.clone();
        game.game_id[0] ^= 0xff;
        assert!(!latch(&game), "gameId");

        // hopVout is bound by BOTH digests.
        let mut vout = base.clone();
        vout.hop_vout += 1;
        assert!(!latch(&vout), "hopVout");

        // hopSats is bound by the identity challenge only — and the
        // container VALUE bar moves with it, so both legs are exercised.
        let mut sats = base.clone();
        sats.hop_sats += 1;
        assert!(!latch(&sats), "hopSats");

        let mut seat = base.clone();
        let last = seat.seat_sig.len() - 1;
        seat.seat_sig[last] ^= 0xff;
        assert!(!latch(&seat), "seatSig bytes");

        let mut ident = base.clone();
        let last = ident.identity_sig.len() - 1;
        ident.identity_sig[last] ^= 0xff;
        assert!(!latch(&ident), "identitySig bytes");
    }

    /// The F1 bar, isolated: a valid seatSig ALONE — which an opponent's
    /// wallet CAN mint over the victim's identity with its own settle key —
    /// never latches the row. Only the named identity can produce bar 3b.
    #[test]
    fn a_valid_seat_sig_alone_never_latches() {
        let mut m = golden();
        assert!(
            verify_seat_sig(
                &m.game_id,
                m.hop_vout,
                &m.identity,
                &m.seat_settle_pubkey,
                &m.seat_sig
            ),
            "positive control: the golden's seat signature verifies"
        );
        m.identity_sig = vec![0x30; 70];
        assert!(
            verify_seat_sig(
                &m.game_id,
                m.hop_vout,
                &m.identity,
                &m.seat_settle_pubkey,
                &m.seat_sig
            ),
            "the seat bar still passes…"
        );
        assert!(!latch(&m), "…and the row still does not latch");
    }

    /// The CONTAINER bars, which no signature can substitute for: the
    /// golden's own signatures are untouched in every leg.
    #[test]
    fn the_container_bars_are_independent_of_the_signatures() {
        let m = golden();
        let (lock, sats) = honest_container(&m);
        assert!(
            marker_valid(&m, lock.as_deref(), sats),
            "positive control first"
        );

        // No output at hopVout — a PROVEN absence, which REFUTES.
        assert!(marker_valid(&m, None, None).eq(&false), "absent output");
        assert!(!marker_valid(&m, lock.as_deref(), None), "absent value");
        assert!(!marker_valid(&m, None, sats), "absent lock");
        // An impossible empty lock string reads as absent, never as a match.
        assert!(!marker_valid(&m, Some(""), sats), "empty lock hex");
        // Paying someone else…
        let elsewhere = format!("76a914{}88ac", "77".repeat(20));
        assert!(!marker_valid(&m, Some(&elsewhere), sats), "wrong key");
        // …or the wrong amount, in both directions and at the extremes.
        for v in [0u64, 1, m.hop_sats - 1, m.hop_sats + 1, u64::MAX] {
            assert!(
                !marker_valid(&m, lock.as_deref(), Some(v)),
                "on-chain {v} vs claimed {}",
                m.hop_sats
            );
        }
        // Case-insensitivity of the lock comparison is deliberate (D1 has
        // stored both cases across this table's life).
        assert!(
            marker_valid(&m, Some(&lock.clone().unwrap().to_ascii_uppercase()), sats),
            "an UPPERCASE lock hex is the same script"
        );
    }

    /// secp256k1 group order, for minting the high-S twin of a signature.
    const SECP256K1_N: [u8; 32] = [
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xfe, 0xba, 0xae, 0xdc, 0xe6, 0xaf, 0x48, 0xa0, 0x3b, 0xbf, 0xd2, 0x5e, 0x8c, 0xd0, 0x36,
        0x41, 0x41,
    ];

    /// Minimal DER INTEGER. Hand-rolled ON PURPOSE — `Signature::to_der()`
    /// silently normalises to low-S, so using it here would produce a "twin"
    /// identical to the original and the cell would assert nothing.
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

    /// Re-encode a DER signature with `s -> N - s`: verifies under the SAME
    /// key over the SAME digest, different bytes.
    fn high_s_twin(der: &[u8]) -> Vec<u8> {
        let sig = bsv_rs::primitives::ec::Signature::from_der(der).expect("parses");
        let s = *sig.s();
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
        let mut out = vec![
            0x30,
            (4 + r_b.len() + s_b.len()) as u8,
            0x02,
            r_b.len() as u8,
        ];
        out.extend_from_slice(&r_b);
        out.push(0x02);
        out.push(s_b.len() as u8);
        out.extend_from_slice(&s_b);
        out
    }

    /// Re-encode with a NON-MINIMAL `r` INTEGER (a redundant leading zero).
    fn pad_r(der: &[u8]) -> Vec<u8> {
        assert_eq!(der[0], 0x30, "SEQUENCE");
        let r_len = der[3] as usize;
        let mut out = vec![0x30u8, 0, 0x02, (r_len + 1) as u8, 0x00];
        out.extend_from_slice(&der[4..4 + r_len]);
        out.extend_from_slice(&der[4 + r_len..]);
        let body = out.len() - 2;
        out[1] = body as u8;
        out
    }

    /// EPOCH RULE 4c, ON BOTH SIGNATURES AND THREE MALLEABILITY AXES.
    ///
    /// Since #362 the latch is a LEADING sort key, so unlimited `markerValid
    /// = 1` variants of one honest marker would be a cost multiplier against
    /// the row they replay. Three different spellings — trailing-byte
    /// padding, a non-minimal `r`, and the high-S twin — because a pin
    /// verified only against the injection it was written for pins a string,
    /// not a property (epoch Rule 12a).
    #[test]
    fn no_malleated_variant_of_the_golden_marker_latches_valid() {
        let base = golden();
        assert!(latch(&base), "positive control: the golden latches");

        for (name, mutate) in [
            (
                "trailing-byte padding",
                (|d: &[u8]| {
                    let mut v = d.to_vec();
                    v.push(0x00);
                    v
                }) as fn(&[u8]) -> Vec<u8>,
            ),
            ("non-minimal r", pad_r),
            ("high-S twin", high_s_twin),
        ] {
            let mut seat = base.clone();
            seat.seat_sig = mutate(&base.seat_sig);
            assert_ne!(seat.seat_sig, base.seat_sig, "{name}: really mutated");
            assert!(!latch(&seat), "seat: {name}");

            let mut ident = base.clone();
            ident.identity_sig = mutate(&base.identity_sig);
            assert_ne!(ident.identity_sig, base.identity_sig, "{name}: mutated");
            assert!(!latch(&ident), "identity: {name}");
        }
    }

    /// Domain separation across the two marker families. A POTPARTY-domain
    /// signature must never latch a hopparty row — the tag lives inside both
    /// preimages, and the two crates share ONE identity-verification recipe,
    /// so this is the cell that proves the shared recipe did not merge the
    /// domains along with the code.
    #[test]
    fn a_potparty_domain_signature_never_latches_a_hopparty_row() {
        let m = golden();
        // The potparty v2 seatsig preimage over the same (game, index,
        // identity) — same 24-byte layout, different domain prefix.
        let mut cross = hopparty_seatsig_preimage(&m.game_id, m.hop_vout, &m.identity).unwrap();
        cross[..24].copy_from_slice(b"LOW/potparty/v2/seatsig|");
        assert_ne!(
            cross,
            hopparty_seatsig_preimage(&m.game_id, m.hop_vout, &m.identity).unwrap(),
            "the two preimages really differ"
        );
        // The golden's seatSig is over the HOPPARTY preimage, so verifying it
        // against the potparty one must fail — asserted through the real bar.
        let pubkey = bsv_rs::primitives::ec::PublicKey::from_bytes(&m.seat_settle_pubkey).unwrap();
        let sig = canonical_der(&m.seat_sig).expect("the golden is canonical DER");
        assert!(
            !pubkey.verify(&bsv_rs::primitives::hash::sha256(&cross), &sig),
            "a hopparty seatSig must not verify over the potparty domain"
        );
        assert!(
            pubkey.verify(
                &bsv_rs::primitives::hash::sha256(
                    &hopparty_seatsig_preimage(&m.game_id, m.hop_vout, &m.identity).unwrap()
                ),
                &sig
            ),
            "positive control: it does verify over its own"
        );
    }

    /// The rank expression is built from the column constant and orders the
    /// three tiers the windows depend on. Asserted on the CONSTRUCT (epoch
    /// Rule 9); executed against real SQLite by the consumer's window tests.
    #[test]
    fn marker_rank_expr_uses_the_shared_column_and_three_tiers() {
        let e = marker_rank_expr("hp.");
        assert_eq!(
            e.matches(&format!("hp.{MARKER_VALID_COLUMN}")).count(),
            2,
            "both legs reference the shared column name: {e}"
        );
        assert!(e.contains("THEN 2"), "verified tier: {e}");
        assert!(e.contains("IS NULL THEN 1"), "legacy tier: {e}");
        assert!(e.contains("ELSE 0"), "refuted tier: {e}");
        assert_eq!(marker_rank_expr("").matches(MARKER_VALID_COLUMN).count(), 2);
    }

    fn golden_record(
        m: &HoppartyMarker,
        lock: Option<String>,
        sats: Option<u64>,
    ) -> HoppartyRecord {
        HoppartyRecord {
            identity: hex::encode(&m.identity),
            opponent_identity: hex::encode(&m.opponent),
            game_id: hex::encode(m.game_id),
            hop_vout: m.hop_vout,
            hop_sats: m.hop_sats,
            seat_settle_pubkey: hex::encode(&m.seat_settle_pubkey),
            seat_sig_hex: hex::encode(&m.seat_sig),
            identity_sig_hex: hex::encode(&m.identity_sig),
            hop_lock_hex: lock,
            hop_sats_on_chain: sats,
            container_outputs: 2,
            txid: "aa".repeat(32),
            output_index: 1,
            created_at: 0,
        }
    }

    /// `record_marker_valid` (the hex/stored shape the D1 writer holds) and
    /// `marker_valid` (the parsed shape) must not drift — when a claim is
    /// about two surfaces agreeing, the cell must call BOTH (epoch Rule 10).
    #[test]
    fn the_record_and_marker_entry_points_agree() {
        let m = golden();
        let (lock, sats) = honest_container(&m);
        let rec = golden_record(&m, lock.clone(), sats);
        assert!(record_marker_valid(&rec));
        assert_eq!(
            record_marker_valid(&rec),
            marker_valid(&m, lock.as_deref(), sats)
        );

        let mut tampered = rec.clone();
        tampered.hop_sats += 1;
        assert!(!record_marker_valid(&tampered));

        let mut absent = rec.clone();
        absent.hop_lock_hex = None;
        absent.hop_sats_on_chain = None;
        assert!(!record_marker_valid(&absent));

        for junk in [
            (|r: &mut HoppartyRecord| r.seat_sig_hex = "zz".repeat(70)) as fn(&mut HoppartyRecord),
            |r: &mut HoppartyRecord| r.identity_sig_hex = String::from("nothex"),
            |r: &mut HoppartyRecord| r.identity = String::new(),
            |r: &mut HoppartyRecord| r.game_id = String::from("11"),
            |r: &mut HoppartyRecord| r.seat_settle_pubkey = String::from("02abcd"),
        ] {
            let mut bad = rec.clone();
            junk(&mut bad);
            assert!(
                !record_marker_valid(&bad),
                "malformed input is fail-safe false"
            );
        }

        // UPPERCASE hex is the SAME record — D1 has carried both cases.
        let mut upper = rec;
        upper.identity = upper.identity.to_ascii_uppercase();
        upper.seat_sig_hex = upper.seat_sig_hex.to_ascii_uppercase();
        assert!(
            record_marker_valid(&upper),
            "hex case must not decide validity"
        );
    }
}
