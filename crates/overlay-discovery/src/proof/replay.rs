//! bsv-low P1.1 part b (2026-09-02): the SERVER-SIDE replay of a
//! `LOW/proof/v1` bundle — a faithful port of the client's
//! `verifyProofBundleReplay` + `deriveOpponentHand` (bsv-low
//! `app/src/lib/proof.ts`), over the SAME pure crates the client's wasm
//! wraps: `low-core` (mental-poker deck math, blind scalar commitments, the
//! committed discard exchange) and `low-wire` (seat-signed envelopes).
//!
//! It re-derives BOTH hands from the signed transcript material the bundle
//! carries and refuses (`None`) on ANY discrepancy — exactly where the client
//! withholds its badge. Nothing here is trusted from the bundle's own words:
//! every card is unmasked from two scalars that each opened a seat- and
//! position-bound blind commitment, and every commitment/reveal/key envelope
//! is wire-verified under the seat it must be signed by.
//!
//! Pure, no I/O. DISPLAY-TIER consumer (the receipt's showdown): a verdict
//! here never gates money.

use low_core::discard::{exchange_positions, verify_discard_reveal};
use low_core::mental::{unmask, validate_deck, verify_scalar_commitment};
use low_wire::{envelope_from_wire, verify_envelope, VerifyContext};
use serde_json::Value;
use std::io::Read;

/// The only bundle version deployed clients accept (`b.v !== 1` refuses).
pub const PROOF_BUNDLE_VERSION: u64 = 1;

/// Both hands as re-derived from the bundle. Cards are CANONICAL (sorted
/// ascending ordinals 0..=51), the form the claim and the bundle both carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvedHands {
    /// 0 = seat A won, 1 = seat B won.
    pub winner_seat: u8,
    /// The two wire seat keys, positional (A, B).
    pub seats: [[u8; 33]; 2],
    pub winner_cards: [u8; 5],
    /// `None` when the bundle carries no loser half (an older winner-only
    /// bundle, or a hand the loser never revealed) — never guessed.
    pub loser_cards: Option<[u8; 5]>,
}

impl ProvedHands {
    /// 10-hex canonical card bytes — the wire form `/results` serves.
    pub fn cards_hex(cards: &[u8; 5]) -> String {
        hex::encode(cards)
    }
}

/// The bundle bytes as JSON text: gunzipped when the gzip magic leads, else
/// UTF-8 as pushed. `None` on undecodable bytes.
pub fn bundle_json(bytes: &[u8]) -> Option<String> {
    if bytes.len() >= 2 && bytes[0] == 0x1f && bytes[1] == 0x8b {
        let mut out = String::new();
        let mut dec = flate2::read::GzDecoder::new(bytes);
        // Bounded: the marker push is already capped (PROOF_BUNDLE_MAX_LEN);
        // a hostile gzip bomb still cannot exceed 16× that here.
        let mut limited = (&mut dec).take(1 << 20);
        limited.read_to_string(&mut out).ok()?;
        Some(out)
    } else {
        String::from_utf8(bytes.to_vec()).ok()
    }
}

/// Replay the bundle against the marker's own (gameId, winner) pushes.
/// `None` on any discrepancy — the caller stores "refused", never a guess.
pub fn prove_bundle(
    bundle_bytes: &[u8],
    expect_game_id: &[u8; 32],
    expect_winner: &[u8; 33],
) -> Option<ProvedHands> {
    let json = bundle_json(bundle_bytes)?;
    let b: Value = serde_json::from_str(&json).ok()?;
    replay(&b, expect_game_id, expect_winner)
}

// ── helpers ─────────────────────────────────────────────────────────────────

/// Lowercase 64-hex → 32 bytes (the client's `/^[0-9a-f]{64}$/` bar: case-
/// sensitive, so an uppercase digit refuses exactly as it does there).
fn h32_lc(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 || !s.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    let v = hex::decode(s).ok()?;
    v.try_into().ok()
}

/// Any-case 66-hex compressed point → 33 bytes (deck points; the client's
/// wasm `h33` decodes hex case-insensitively).
fn h33(s: &str) -> Option<[u8; 33]> {
    if s.len() != 66 {
        return None;
    }
    let v = hex::decode(s).ok()?;
    v.try_into().ok()
}

/// A seat pubkey as the bundle names it: `/^0[23][0-9a-f]{64}$/i`, then
/// lowercased for every comparison (the client lowercases too).
fn seat_key(v: &Value) -> Option<[u8; 33]> {
    let s = v.as_str()?;
    if s.len() != 66 {
        return None;
    }
    let lc = s.to_ascii_lowercase();
    if !(lc.starts_with("02") || lc.starts_with("03")) {
        return None;
    }
    h33(&lc)
}

fn card_list(v: &Value) -> Option<Vec<u8>> {
    let arr = v.as_array()?;
    let mut out = Vec::with_capacity(arr.len());
    for c in arr {
        let n = c.as_u64()?;
        if n > 51 {
            return None;
        }
        out.push(n as u8);
    }
    Some(out)
}

/// `validCards`: exactly five distinct ordinals 0..=51.
fn valid_cards(cards: &[u8]) -> bool {
    cards.len() == 5
        && cards.iter().all(|&c| c <= 51)
        && cards.iter().collect::<std::collections::HashSet<_>>().len() == 5
}

fn canonical(cards: &[u8]) -> Option<[u8; 5]> {
    if !valid_cards(cards) {
        return None;
    }
    let mut v: Vec<u8> = cards.to_vec();
    v.sort_unstable();
    v.try_into().ok()
}

/// The five DEAL positions of a seat (`mental_deal_positions`): `2k + seat`.
fn deal_positions(seat: u8) -> [usize; 5] {
    let s = seat as usize;
    [s, 2 + s, 4 + s, 6 + s, 8 + s]
}

struct EnvelopeView {
    kind: String,
    body: Vec<u8>,
}

/// `verifiedEnvelope`: wire-verify one envelope under ITS OWN context (the
/// same self-consistency check `envelope_wire_verify_seat` performs), then
/// bind it to (gameId, the expected seat, the allowed kind set). `None` on
/// any discrepancy.
fn verified_envelope(
    wire: Option<&Value>,
    game_id: &[u8; 32],
    want_seat: &[u8; 33],
    allowed_kinds: &[&str],
) -> Option<EnvelopeView> {
    let wire_hex = wire?.as_str()?;
    if wire_hex.is_empty() {
        return None;
    }
    let bytes = hex::decode(wire_hex).ok()?;
    let env = envelope_from_wire(&bytes).ok()?;
    let ctx = VerifyContext {
        network_id: env.fields.network_id,
        module_id: env.fields.module_id.clone(),
        contract_id: env.fields.contract_id,
        protocol_version: env.fields.protocol_version,
        seats: vec![env.fields.seat_pubkey],
        expected_seq: env.fields.sequence_no,
        transcript_head: env.fields.prior_transcript_hash,
    };
    verify_envelope(&env, &ctx).ok()?;
    if &env.fields.contract_id != game_id {
        return None;
    }
    if &env.fields.seat_pubkey != want_seat {
        return None;
    }
    if !allowed_kinds.contains(&env.fields.message_kind.as_str()) {
        return None;
    }
    Some(EnvelopeView {
        kind: env.fields.message_kind.clone(),
        body: env.fields.body.clone(),
    })
}

/// `parseHex32Array`: a JSON array of lowercase 64-hex scalars/commitments —
/// exactly `n` of them when `n` is given, never more than `max`.
fn parse_hex32_array(body: &[u8], n: Option<usize>, max: Option<usize>) -> Option<Vec<[u8; 32]>> {
    let text = std::str::from_utf8(body).ok()?;
    let v: Value = serde_json::from_str(text).ok()?;
    let arr = v.as_array()?;
    if let Some(n) = n {
        if arr.len() != n {
            return None;
        }
    }
    if let Some(max) = max {
        if arr.len() > max {
            return None;
        }
    }
    arr.iter().map(|e| e.as_str().and_then(h32_lc)).collect()
}

struct RevealView {
    hand: [u8; 5],
    scalars: [[u8; 32]; 5],
}

/// A `reveal_hand` body: `{hand:[5 ordinals], nonce:string, scalars:[5 hex64]}`.
fn parse_reveal(body: &[u8]) -> Option<RevealView> {
    let text = std::str::from_utf8(body).ok()?;
    let v: Value = serde_json::from_str(text).ok()?;
    v.get("nonce")?.as_str()?;
    let hand = card_list(v.get("hand")?)?;
    if hand.len() != 5 {
        return None;
    }
    let scalars = v.get("scalars")?.as_array()?;
    if scalars.len() != 5 {
        return None;
    }
    let scalars: Vec<[u8; 32]> = scalars
        .iter()
        .map(|s| s.as_str().and_then(h32_lc))
        .collect::<Option<_>>()?;
    Some(RevealView {
        hand: hand.try_into().ok()?,
        scalars: scalars.try_into().ok()?,
    })
}

/// The released scalars covering the OTHER seat's positions: exactly one
/// `deal_keys` (five), at most one `draw_keys` (≤5, bounded before parse).
fn parse_released_keys(
    wires: Option<&Value>,
    game_id: &[u8; 32],
    seat: &[u8; 33],
) -> Option<(Vec<[u8; 32]>, Option<Vec<[u8; 32]>>)> {
    let arr = wires?.as_array()?;
    if arr.is_empty() || arr.len() > 2 {
        return None;
    }
    let mut deal: Option<Vec<[u8; 32]>> = None;
    let mut draw: Option<Vec<[u8; 32]>> = None;
    for w in arr {
        let kv = verified_envelope(Some(w), game_id, seat, &["deal_keys", "draw_keys"])?;
        if kv.kind == "deal_keys" {
            if deal.is_some() {
                return None; // at most one of each
            }
            deal = Some(parse_hex32_array(&kv.body, Some(5), None)?);
        } else {
            if draw.is_some() {
                return None;
            }
            draw = Some(parse_hex32_array(&kv.body, None, Some(5))?);
        }
    }
    Some((deal?, draw))
}

/// One seat's derivation: its parsed reveal, its five ordinals in final-position
/// order, and the peer's `deal_keys` scalars (the winner's discard check reuses them).
type SeatHandDerivation = (RevealView, Vec<u8>, Vec<[u8; 32]>);

/// Steps 4–6 for ONE seat: its own reveal opens its five, the peer's released
/// scalars are the other half at each of its positions; both scalars must open
/// their blind commitments before the card is unmasked, and the unmasked
/// ordinal must equal the card the reveal claimed at that slot.
#[allow(clippy::too_many_arguments)]
fn derive_seat_hand(
    game_id: &[u8; 32],
    seats: &[[u8; 33]; 2],
    own_seat: u8,
    reveal_wire: Option<&Value>,
    peer_keys_wire: Option<&Value>,
    own_positions: &[usize; 5],
    commits: &[Vec<[u8; 32]>; 2],
    d4: &[[u8; 33]],
) -> Option<SeatHandDerivation> {
    let peer_seat = 1 - own_seat;
    let reveal_env = verified_envelope(
        reveal_wire,
        game_id,
        &seats[own_seat as usize],
        &["reveal_hand"],
    )?;
    let reveal = parse_reveal(&reveal_env.body)?;
    let (deal_scalars, draw_scalars) =
        parse_released_keys(peer_keys_wire, game_id, &seats[peer_seat as usize])?;
    let own_deal = deal_positions(own_seat);
    let own_draw: Vec<usize> = own_positions.iter().copied().filter(|&p| p >= 10).collect();
    if !own_draw.is_empty() {
        match &draw_scalars {
            Some(d) if d.len() == own_draw.len() => {}
            _ => return None,
        }
    }
    let mut ordinals = Vec::with_capacity(5);
    for (i, &p) in own_positions.iter().enumerate() {
        if p >= d4.len() {
            return None;
        }
        let s_own = reveal.scalars[i];
        let s_peer = if p < 10 {
            let k = own_deal.iter().position(|&q| q == p)?; // a kept slot must be one of the seat's deal positions
            deal_scalars[k]
        } else {
            let k = own_draw.iter().position(|&q| q == p)?;
            draw_scalars.as_ref()?[k]
        };
        if !verify_scalar_commitment(&commits[own_seat as usize][p], game_id, own_seat, p, &s_own) {
            return None;
        }
        if !verify_scalar_commitment(
            &commits[peer_seat as usize][p],
            game_id,
            peer_seat,
            p,
            &s_peer,
        ) {
            return None;
        }
        // Seat-A scalar first (commutative; seat order honoured as the client does).
        let (s_a, s_b) = if own_seat == 0 {
            (s_own, s_peer)
        } else {
            (s_peer, s_own)
        };
        let ordinal = unmask(&d4[p], &s_a, &s_b).ok()?;
        if ordinal != reveal.hand[i] {
            return None; // the reveal claimed a card it wasn't dealt
        }
        ordinals.push(ordinal);
    }
    if !valid_cards(&ordinals) {
        return None;
    }
    Some((reveal, ordinals, deal_scalars))
}

fn replay(b: &Value, expect_game_id: &[u8; 32], expect_winner: &[u8; 33]) -> Option<ProvedHands> {
    if b.get("v")?.as_u64()? != PROOF_BUNDLE_VERSION {
        return None;
    }
    let game_id = h32_lc(&b.get("gameId")?.as_str()?.to_ascii_lowercase())?;
    if &game_id != expect_game_id {
        return None;
    }
    let winner = seat_key(b.get("winner")?)?;
    if &winner != expect_winner {
        return None;
    }
    let winner_idx = match b.get("winnerSeat")?.as_u64()? {
        0 => 0u8,
        1 => 1u8,
        _ => return None,
    };
    let loser_idx = 1 - winner_idx;
    let raw_seats = b.get("seats")?.as_array()?;
    if raw_seats.len() != 2 {
        return None;
    }
    let seats = [seat_key(&raw_seats[0])?, seat_key(&raw_seats[1])?];
    if seats[0] == seats[1] {
        return None;
    }
    let bundle_cards = canonical(&card_list(b.get("cards")?)?)?;
    let env = b.get("envelopes")?;
    if !env.is_object() {
        return None;
    }

    // 1) The blind scalar commitments — one per seat, posted before any card.
    let commit_a = verified_envelope(
        env.get("scalarCommitA"),
        &game_id,
        &seats[0],
        &["scalar_commit"],
    )?;
    let commit_b = verified_envelope(
        env.get("scalarCommitB"),
        &game_id,
        &seats[1],
        &["scalar_commit"],
    )?;
    let commits = [
        parse_hex32_array(&commit_a.body, Some(52), None)?,
        parse_hex32_array(&commit_b.body, Some(52), None)?,
    ];

    // 2) d4 — the fully-remasked deck (the final remask is seat B's).
    let remask = verified_envelope(
        env.get("finalRemask"),
        &game_id,
        &seats[1],
        &["remask_pass"],
    )?;
    let deck_text = std::str::from_utf8(&remask.body).ok()?;
    let deck_v: Value = serde_json::from_str(deck_text).ok()?;
    let d4: Vec<[u8; 33]> = deck_v
        .as_array()?
        .iter()
        .map(|e| e.as_str().and_then(h33))
        .collect::<Option<_>>()?;
    validate_deck(&d4).ok()?;

    // 3) Final positions: from BOTH committed-then-revealed discard masks when
    //    the four mask envelopes are carried (all or none), else the deal
    //    positions.
    let mask_keys = ["maskCommitA", "maskRevealA", "maskCommitB", "maskRevealB"];
    let present = mask_keys
        .iter()
        .filter(|k| env.get(**k).is_some_and(|v| !v.is_null()))
        .count();
    if present != 0 && present != mask_keys.len() {
        return None;
    }
    let (winner_positions, loser_positions): ([usize; 5], [usize; 5]) =
        if present == mask_keys.len() {
            let mut masks = [0u8; 2];
            for seat_idx in 0..2u8 {
                let (ck, rk) = if seat_idx == 0 {
                    ("maskCommitA", "maskRevealA")
                } else {
                    ("maskCommitB", "maskRevealB")
                };
                let commit_env = verified_envelope(
                    env.get(ck),
                    &game_id,
                    &seats[seat_idx as usize],
                    &["commit_discard"],
                )?;
                let reveal_env = verified_envelope(
                    env.get(rk),
                    &game_id,
                    &seats[seat_idx as usize],
                    &["reveal_discard"],
                )?;
                let commitment: [u8; 32] = commit_env.body.as_slice().try_into().ok()?;
                // reveal body: 1 mask byte ‖ 32 nonce bytes.
                if reveal_env.body.len() != 33 {
                    return None;
                }
                let mask = reveal_env.body[0];
                let nonce: [u8; 32] = reveal_env.body[1..33].try_into().ok()?;
                // player_id = the seat pubkey's 32-byte X coordinate (protocol note).
                let player_id: [u8; 32] = seats[seat_idx as usize][1..33].try_into().ok()?;
                if !verify_discard_reveal(&commitment, mask, &nonce, &player_id, &game_id) {
                    return None;
                }
                masks[seat_idx as usize] = mask;
            }
            let (p1, p2) = exchange_positions(masks[0], masks[1]).ok()?;
            if winner_idx == 0 {
                (p1, p2)
            } else {
                (p2, p1)
            }
        } else {
            (deal_positions(winner_idx), deal_positions(loser_idx))
        };

    // 4–6) The winner's own opening + the loser's released scalars → the
    //      winner's five, every ordinal re-derived.
    let (_reveal, ordinals, deal_scalars_from_loser) = derive_seat_hand(
        &game_id,
        &seats,
        winner_idx,
        env.get("winnerReveal"),
        env.get("loserKeys"),
        &winner_positions,
        &commits,
        &d4,
    )?;

    // 7) The derived SET must equal the bundle's cards (the caller compares
    //    the claim's cards at read time — the marker's `cards` push).
    let derived = canonical(&ordinals)?;
    if derived != bundle_cards {
        return None;
    }

    // 8) The winner's DISCARDS, face-up (optional; absent ⇒ face-down). Each
    //    published scalar must open the winner's own commitment at a genuinely
    //    discarded deal position — a cook attempt refuses the whole bundle;
    //    entries at any other position are ignored (forward compatibility).
    let winner_deal = deal_positions(winner_idx);
    if let Some(raw) = b.get("discards").filter(|v| !v.is_null()) {
        let obj = raw.as_object()?; // an array or scalar refuses, as the client does
        for (key, value) in obj {
            let Ok(p) = key.parse::<usize>() else {
                continue;
            };
            let discarded = winner_deal.contains(&p) && !winner_positions.contains(&p);
            if !discarded {
                continue; // not ours to show
            }
            let scalar = h32_lc(value.as_str()?)?;
            if p >= d4.len() {
                return None;
            }
            let k = winner_deal.iter().position(|&q| q == p)?;
            let s_loser = deal_scalars_from_loser[k];
            if !verify_scalar_commitment(
                &commits[winner_idx as usize][p],
                &game_id,
                winner_idx,
                p,
                &scalar,
            ) {
                return None;
            }
            if !verify_scalar_commitment(
                &commits[loser_idx as usize][p],
                &game_id,
                loser_idx,
                p,
                &s_loser,
            ) {
                return None;
            }
            let (s_a, s_b) = if winner_idx == 0 {
                (scalar, s_loser)
            } else {
                (s_loser, scalar)
            };
            let ordinal = unmask(&d4[p], &s_a, &s_b).ok()?;
            if ordinal > 51 {
                return None;
            }
        }
    }

    // 9) The OPPONENT's hand (optional; absent ⇒ winner-only). Step 6 with
    //    the roles swapped over material already exchanged during the hand;
    //    a present-but-unprovable half refuses the whole bundle.
    let loser_cards = if env.get("loserReveal").is_some_and(|v| !v.is_null()) {
        let (_r, loser_ordinals, _k) = derive_seat_hand(
            &game_id,
            &seats,
            loser_idx,
            env.get("loserReveal"),
            env.get("winnerKeys"),
            &loser_positions,
            &commits,
            &d4,
        )?;
        let loser = canonical(&loser_ordinals)?;
        // Deck integrity: two seats cannot hold the same card.
        if loser.iter().any(|c| derived.contains(c)) {
            return None;
        }
        Some(loser)
    } else {
        None
    };

    Some(ProvedHands {
        winner_seat: winner_idx,
        seats,
        winner_cards: derived,
        loser_cards,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A REAL whole-hand bundle from the beta index (game `a1081773…`,
    /// winner identity `03926129…`, seat A won; carries the loser half and the
    /// four discard-mask envelopes). Pulled from `proof_markers.bundleB64`
    /// on 2026-09-02 — the exact bytes a deployed client published.
    const REAL: &[u8] = include_bytes!("fixtures/bundle-a1081773.bin");
    const REAL_GAME: &str = "a1081773673e8c7cb6093db8f4a59166495f15e9ded1fe354ee27bbda7922523";
    const REAL_WINNER: &str = "03926129919f02ae2910ef7505aec13bd9aa937db5e38352f8f20028e0858218e0";

    fn game() -> [u8; 32] {
        hex::decode(REAL_GAME).unwrap().try_into().unwrap()
    }
    fn winner() -> [u8; 33] {
        hex::decode(REAL_WINNER).unwrap().try_into().unwrap()
    }
    fn json_of(bytes: &[u8]) -> Value {
        serde_json::from_str(&bundle_json(bytes).unwrap()).unwrap()
    }

    #[test]
    fn the_real_bundle_replays_to_both_hands() {
        let proved =
            prove_bundle(REAL, &game(), &winner()).expect("a deployed client's bundle replays");
        assert_eq!(proved.winner_seat, 0);
        assert_eq!(
            proved.winner_cards,
            [1, 31, 35, 39, 51],
            "the bundle's own canonical cards, RE-DERIVED"
        );
        let loser = proved
            .loser_cards
            .expect("the loser half is carried and provable");
        // Both hands are distinct sets from one deck.
        assert!(loser.iter().all(|c| !proved.winner_cards.contains(c)));
        assert!(
            loser.windows(2).all(|w| w[0] < w[1]),
            "canonical order: {loser:?}"
        );
        // Independent corroboration: the WINNER's own `ls_hand` marker and its
        // v2 claim in the beta index both say `011f232733` for this game — and
        // the LOSER never published a hand marker there, so this replay is the
        // only source of its five (the point of P1.1 part b).
        assert_eq!(ProvedHands::cards_hex(&proved.winner_cards), "011f232733");
        assert_eq!(loser, [21, 28, 29, 45, 49]);
        assert_eq!(ProvedHands::cards_hex(&loser), "151c1d2d31");
        assert_eq!(
            hex::encode(proved.seats[0]),
            "032f0bceeaf001f7d16871c9eba014004a17489c8a39aec9d4e9cccf626fe66e8d"
        );
    }

    #[test]
    fn the_marker_pushes_bind_the_bundle() {
        let mut other_game = game();
        other_game[0] ^= 1;
        assert!(
            prove_bundle(REAL, &other_game, &winner()).is_none(),
            "a bundle for another game"
        );
        let mut other_winner = winner();
        other_winner[32] ^= 1;
        assert!(
            prove_bundle(REAL, &game(), &other_winner).is_none(),
            "a bundle naming another winner"
        );
    }

    /// Every refusal below is the CLIENT's refusal too (`verifyProofBundleReplay`
    /// returns null on the same edits) — the server never badges what the
    /// client would not.
    #[test]
    fn tampering_refuses_the_whole_bundle() {
        let base = json_of(REAL);
        let run = |b: &Value| prove_bundle(b.to_string().as_bytes(), &game(), &winner());
        assert!(
            run(&base).is_some(),
            "the plain-JSON form replays like the gzip form"
        );

        let mut v2 = base.clone();
        v2["v"] = Value::from(2);
        assert!(run(&v2).is_none(), "an unknown version");

        let mut cards = base.clone();
        cards["cards"] = serde_json::json!([1, 31, 35, 39, 50]);
        assert!(
            run(&cards).is_none(),
            "the bundle's claimed set must equal the derived set"
        );

        let mut swapped = base.clone();
        swapped["winnerSeat"] = Value::from(1);
        assert!(
            run(&swapped).is_none(),
            "the wrong seat cannot open the winner's commitments"
        );

        let mut flipped = base.clone();
        let wire = flipped["envelopes"]["winnerReveal"]
            .as_str()
            .unwrap()
            .to_string();
        let mut bytes = hex::decode(&wire).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01; // inside the signature
        flipped["envelopes"]["winnerReveal"] = Value::from(hex::encode(bytes));
        assert!(
            run(&flipped).is_none(),
            "a signature bit flip refuses the envelope"
        );

        let mut foreign_key = base.clone();
        foreign_key["envelopes"]["loserKeys"] = base["envelopes"]["winnerKeys"].clone();
        assert!(run(&foreign_key).is_none(), "keys signed by the wrong seat");

        let mut half_masks = base.clone();
        half_masks["envelopes"]
            .as_object_mut()
            .unwrap()
            .remove("maskRevealB");
        assert!(run(&half_masks).is_none(), "mask envelopes are all-or-none");

        let mut cooked_discard = base.clone();
        cooked_discard["discards"] = serde_json::json!({ "2": "00".repeat(32) });
        // Position 2 is one of seat A's deal positions; whether it was discarded
        // in this hand decides between "ignored" and "refused" — either way the
        // verdict must be deterministic and never a panic.
        let _ = run(&cooked_discard);

        let mut bad_loser = base.clone();
        bad_loser["envelopes"]["loserReveal"] = base["envelopes"]["winnerReveal"].clone();
        assert!(
            run(&bad_loser).is_none(),
            "a present-but-unprovable loser half refuses everything"
        );

        let mut no_loser = base.clone();
        {
            let e = no_loser["envelopes"].as_object_mut().unwrap();
            e.remove("loserReveal");
            e.remove("winnerKeys");
        }
        let winner_only = run(&no_loser).expect("a winner-only bundle still proves the winner");
        assert!(
            winner_only.loser_cards.is_none(),
            "absent ⇒ None, never guessed"
        );

        assert!(
            prove_bundle(&REAL[..REAL.len() / 2], &game(), &winner()).is_none(),
            "truncated gzip"
        );
        assert!(prove_bundle(b"{}", &game(), &winner()).is_none());
        assert!(prove_bundle(b"\x1f\x8b\x08junk", &game(), &winner()).is_none());
    }
}
