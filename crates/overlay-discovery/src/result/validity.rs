//! `tm_result` claim VALIDITY — decoded ONCE at admission and latched onto
//! the row as `result_markers_v2.claimValid` (brain-cutover M1,
//! bsv-low docs/PLAN-BRAIN-CUTOVER-2026-08.md; the #283/#362 latch family).
//!
//! Before this module, every `/results` and `/leaderboard` request re-ran
//! the ECDSA on every claim row (`low-app-layer results.rs::verified_claim`)
//! and the CLIENT re-ran it again per visit — the #401 main-thread freeze.
//! The recipe here is byte-identical to both (pinned by the SIGNED goldens
//! below, which are frozen artifacts of the real client producer —
//! `result.signedGolden.test.ts`, deterministic PrivateKey(1)/(2)).
//!
//! # The verdict is a TIER, and the row is immutable so the tier is stable
//!
//! - `0` — INVALID: self-paired, malformed cards, or a winner signature that
//!   does not verify under the claimed winner identity. As if never
//!   published (the client's `'invalid'`).
//! - `1` — WINNER-VALID: the winner's signature verifies; the loser
//!   countersig is absent or does not verify (the client's `'unconfirmed'`
//!   demotion — a garbage countersig degrades the tier, never kills the
//!   claim).
//! - `2` — COUNTERSIGNED: both signatures verify (the client's
//!   `'confirmed'`).
//!
//! # The serving contract (this is where it differs from `sigValid`'s)
//!
//! `potparty::validity` doctrine says "sort key, never a WHERE" because its
//! consumers re-verify before drawing conclusions. THE POINT of this latch
//! is that they stop: the app-layer serves the tier and the client trusts
//! the view (owner ruling 2026-08-20 — the app-layer is the brain). So the
//! honest statement of the fault class is: a wrong `0` here HIDES a claim
//! from money-visible lists, exactly as a client-side verify failure hides
//! it today (#335's drop) — the failure mode is relocated, not created. What
//! bounds it: `NULL` is never a verdict (an unswept row is COMPUTED at serve
//! time, then retired by the relatch sweep), the relatch fixpoint re-derives
//! every stored tier at the current predicate version, and a `demoted`
//! count > 0 in a relatch tick is the alarm (see `relatch.rs` — the action
//! is "compare predicates against the frozen goldens", never "re-run").
//!
//! Admission is UNCHANGED: byte-format-only, `INSERT OR IGNORE`,
//! outpoint-keyed, rows never deleted — a garbage front-run still cannot
//! censor a genuine claim, it just latches `0`.

use crate::result::storage::ResultRecord;

/// BRC-42/43 protocol for result claims — the client's
/// `RESULT_PROTOCOL = [1,'low result']`.
pub fn result_protocol() -> bsv_rs::wallet::Protocol {
    bsv_rs::wallet::Protocol::new(bsv_rs::wallet::SecurityLevel::App, "low result")
}

/// Canonicalize a v2 cards push: 10 hex chars → five DISTINCT ordinals
/// 0..=51, sorted ascending, re-encoded lowercase — `result.ts::cardsToHex ∘
/// cardsFromHex`. `None` = malformed (an unverifiable claim: the sigs bind
/// the canonical cards, so we must be able to reconstruct them).
pub fn canonical_cards_hex(cards_hex: &str) -> Option<String> {
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
/// Any malformed key/sig/derivation failure is simply `false` (fail-safe: an
/// unverifiable signature never corroborates).
///
/// NB deliberately NO canonical-DER bar here (unlike
/// `potparty::validity::canonical_der`): this is recipe-PARITY with the
/// serve-time `verified_claim` it latches for. Tightening both sides to
/// strict DER is a recorded follow-up, made in one move or not at all — a
/// latch stricter than its serve-time fallback would tier the same row
/// differently depending on which arm computed it.
pub fn anyone_sig_verifies(
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

/// THE LATCH — this claim's tier (0 / 1 / 2, see the module doc), from the
/// stored record shape the D1 writer holds. Malformed anything is `0`, never
/// a panic and never an error that could fail the admission.
pub fn claim_tier(r: &ResultRecord) -> i64 {
    let winner_lc = r.winner.to_ascii_lowercase();
    let loser_lc = r.loser.to_ascii_lowercase();
    if winner_lc == loser_lc {
        return 0;
    }
    let game_lc = r.game_id.to_ascii_lowercase();
    let Some(challenge) = result_challenge_bytes(
        &game_lc,
        &winner_lc,
        &loser_lc,
        &r.pot_txid.to_ascii_lowercase(),
        &r.settle_txid.to_ascii_lowercase(),
        r.cards_hex.as_deref(),
    ) else {
        return 0;
    };
    if !anyone_sig_verifies(
        &winner_lc,
        &game_lc,
        &challenge,
        &r.winner_sig_hex,
        result_protocol(),
    ) {
        return 0;
    }
    let countersigned = r.loser_sig_hex.as_deref().is_some_and(|s| {
        anyone_sig_verifies(&loser_lc, &game_lc, &challenge, s, result_protocol())
    });
    if countersigned {
        2
    } else {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result::parse_result_marker;

    /// The SIGNED cross-repo goldens — frozen artifacts of the REAL client
    /// producer (`app/src/lib/result.signedGolden.test.ts`, deterministic
    /// PrivateKey(1) winner / PrivateKey(2) loser, RFC6979). If the client's
    /// signer or this predicate drifts, one of the two suites goes red.
    const SIGNED_V2_CONFIRMED: &str = "006a0d4c4f572f726573756c742f7632201111111111111111111111111111111111111111111111111111111111111111210279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f817982102c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5202222222222222222222222222222222222222222222222222222222222222222203333333333333333333333333333333333333333333333333333333333333333050001020304473045022100f5169865c38b686b863c6932e4f5a0460a2633140e742fbac7b3db00aaf0eaf902201a07ac5e713340128fc61356eeb65618fd16067898fad85a25f0b684e6cff846473045022100fe7f746e3d1d8e72701263a9910eb59b58cd4afbc628329c7f33eb0e4cf1697b0220779757d2f1bebd6fcf269180b53a9d307dc55aa3e1e020a937d06e0a48a5fb55";
    const SIGNED_V2_WINNER_ONLY: &str = "006a0d4c4f572f726573756c742f7632201111111111111111111111111111111111111111111111111111111111111111210279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f817982102c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5202222222222222222222222222222222222222222222222222222222222222222203333333333333333333333333333333333333333333333333333333333333333050001020304473045022100f5169865c38b686b863c6932e4f5a0460a2633140e742fbac7b3db00aaf0eaf902201a07ac5e713340128fc61356eeb65618fd16067898fad85a25f0b684e6cff84600";
    const SIGNED_V1_WINNER_ONLY: &str = "006a0d4c4f572f726573756c742f7631201111111111111111111111111111111111111111111111111111111111111111210279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f817982102c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee52022222222222222222222222222222222222222222222222222222222222222222033333333333333333333333333333333333333333333333333333333333333334730450221008362bea54f601692a5e1d7b512df9496d7dee21bf3d2ffeef0da6531376cc42a02200773dc0e7cec239378d6f9c58bfc7d8aa22bacf68f8fb47019438ed49aacf60f00";

    /// Golden hex → the stored record shape, THROUGH the real parser and the
    /// EXACT field mapping of the production writer
    /// (`lookup_service.rs::output_admitted`) — the enumeration-defense
    /// lesson: never hand-fed candidates.
    fn record(hex_str: &str) -> ResultRecord {
        let script = hex::decode(hex_str).expect("golden decodes");
        let m = parse_result_marker(&script).expect("golden parses");
        ResultRecord {
            game_id: hex::encode(m.game_id),
            winner: hex::encode(&m.winner),
            loser: hex::encode(&m.loser),
            pot_txid: hex::encode(m.pot_txid),
            settle_txid: hex::encode(m.settle_txid),
            winner_sig_hex: hex::encode(&m.winner_sig),
            loser_sig_hex: m.loser_sig.as_deref().map(hex::encode),
            cards_hex: m.cards.map(hex::encode),
            txid: "aa".repeat(32),
            output_index: 0,
            created_at: 0,
        }
    }

    #[test]
    fn golden_v2_countersigned_is_tier_2() {
        assert_eq!(claim_tier(&record(SIGNED_V2_CONFIRMED)), 2);
    }

    #[test]
    fn golden_v2_winner_only_is_tier_1() {
        assert_eq!(claim_tier(&record(SIGNED_V2_WINNER_ONLY)), 1);
    }

    #[test]
    fn golden_v1_winner_only_is_tier_1() {
        assert_eq!(claim_tier(&record(SIGNED_V1_WINNER_ONLY)), 1);
    }

    #[test]
    fn tampered_settle_breaks_the_winner_sig_and_tiers_0() {
        let mut r = record(SIGNED_V2_CONFIRMED);
        r.settle_txid = "44".repeat(32);
        assert_eq!(claim_tier(&r), 0);
    }

    #[test]
    fn garbage_countersig_degrades_to_tier_1_never_kills_the_claim() {
        let mut r = record(SIGNED_V2_CONFIRMED);
        r.loser_sig_hex = Some(format!("3044{}", "cd".repeat(68)));
        assert_eq!(claim_tier(&r), 1);
    }

    #[test]
    fn self_paired_and_malformed_cards_tier_0() {
        let mut a = record(SIGNED_V2_CONFIRMED);
        a.loser = a.winner.clone();
        assert_eq!(claim_tier(&a), 0);
        let mut b = record(SIGNED_V2_CONFIRMED);
        b.cards_hex = Some("0000000000".into()); // duplicate ordinals
        assert_eq!(claim_tier(&b), 0);
    }

    #[test]
    fn uppercase_fields_verify_identically_lowercased() {
        let mut r = record(SIGNED_V2_CONFIRMED);
        r.game_id = r.game_id.to_ascii_uppercase();
        r.winner = r.winner.to_ascii_uppercase();
        r.settle_txid = r.settle_txid.to_ascii_uppercase();
        assert_eq!(claim_tier(&r), 2);
    }
}
