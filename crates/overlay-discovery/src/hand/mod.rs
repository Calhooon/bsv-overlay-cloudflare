//! `tm_hand` / `ls_hand` — LOW per-seat showdown-hand markers (bsv-low #382).
//!
//! At settle observation each LOW seat publishes a tiny owner-signed
//! `OP_RETURN` marker carrying ITS OWN revealed five cards for the game —
//! the fact the felt already verified at showdown (the reveal exchange's
//! commitment-checked unmask) but which previously survived NOWHERE a
//! wiped device could reach: the on-chain result claim carries the
//! WINNER's cards only, and a cooperative TIE publishes no result claim
//! at all. With both seats publishing, any device of either identity can
//! render BOTH hands + both LOW sums in its history drill-down
//! ("we should see the scores of the game that was played", #245 → #382).
//!
//! Like `tm_collected`, this is an `OP_RETURN` data-carrier topic admitted
//! by BYTE FORMAT ONLY — the overlay performs NO server-side signature
//! check and derives NO verdict. The security lives in the CLIENT verify:
//! a device renders a hand only after the public 'anyone' ProtoWallet
//! round-trip validates the carried sig under the marker's OWN named
//! identity (`HAND_PROTOCOL = [1,'low hand']`, keyID = gameId,
//! counterparty = 'anyone', challenge binding gameId + identity + potTxid
//! and cards). A forged or junk marker never renders; a display hint only,
//! and no money path reads this index, ever.
//!
//! # Marker wire format (`LOW/hand/v1`)
//!
//! `OP_FALSE OP_RETURN` (0x00 0x6a) followed by six minimal data pushes —
//! byte-identical to the app's `handMarkerScriptHex`
//! (`app/src/lib/handMarker.ts`):
//!
//! | # | Push        | Encoding                                          |
//! |---|-------------|---------------------------------------------------|
//! | 0 | tag         | UTF-8 `LOW/hand/v1` (11 bytes)                    |
//! | 1 | gameId      | 32 bytes                                          |
//! | 2 | identityKey | 33 bytes (compressed identity pubkey)             |
//! | 3 | potTxid     | 32 bytes (binds the hand to the funded pot)       |
//! | 4 | cards       | 5 bytes — card ordinals, each 0..=51, DISTINCT    |
//! | 5 | sig         | DER ECDSA signature, 68..=74 bytes                |
//!
//! The parser validates this shape strictly (exact tag + exact push
//! lengths + exactly six pushes + the cards rule) and extracts the
//! fields. Wrong tag / wrong lengths / duplicate or out-of-range cards /
//! extra or missing pushes → `None` (not a v1 hand marker).
//!
//! # Lookup (`ls_hand`)
//!
//! Query JSON (tagged by `type`):
//!
//! ```json
//! {"type": "handsForGames", "gameIds": ["<64 hex chars>", "..."]}
//! ```
//!
//! The answer is a freeform JSON array — EVERY stored marker row for the
//! requested gameIds (no per-key slot, the result_markers_v2
//! censorship-front-run lesson: junk and genuine rows coexist and the
//! client's sig verify separates them):
//!
//! ```json
//! [{"gameId": "<hex>", "identity": "<hex>", "potTxid": "<hex>",
//!   "cardsHex": "<10 hex>", "txid": "<hex>", "outputIndex": 0,
//!   "sigHex": "<hex|null>"}]
//! ```
//!
//! (`sigHex` is null only for a row whose stored sig column was null, which
//! the admit path never writes; `txid`/`outputIndex` are the marker outpoint.)
//! An absent game simply contributes no rows — fail-safe silence: the history
//! row omits the hand rather than guessing.

pub mod lookup_service;
pub mod storage;
pub mod topic_manager;

/// The domain tag the app stamps (`HAND_TAG` in `app/src/lib/handMarker.ts`).
/// v1 = `(tag, gameId, identityKey, potTxid, cards, sig)`. 11 bytes of
/// ASCII — the byte layout is the cross-repo CONTRACT (bsv-low #382); never
/// change it without a version bump on both sides.
pub const HAND_TAG: &[u8] = b"LOW/hand/v1";
/// Number of minimal data pushes in a well-formed v1 marker.
pub const HAND_FIELD_COUNT: usize = 6;
/// gameId push length (bytes).
pub const HAND_GAME_ID_LEN: usize = 32;
/// identityKey push length (bytes) — a compressed secp256k1 pubkey.
pub const HAND_IDENTITY_KEY_LEN: usize = 33;
/// potTxid push length (bytes).
pub const HAND_POT_TXID_LEN: usize = 32;
/// cards push length (bytes) — five card ordinals.
pub const HAND_CARDS_LEN: usize = 5;
/// Minimum sig push length (bytes) — a DER ECDSA signature.
pub const HAND_SIG_MIN_LEN: usize = 68;
/// Maximum sig push length (bytes) — a DER ECDSA signature.
pub const HAND_SIG_MAX_LEN: usize = 74;

/// A decoded v1 hand marker — one seat's "these were my five showdown
/// cards" fact published on-chain.
///
/// The overlay only needs the keying fields plus the raw `sig` bytes to
/// hand back to querying clients (which verify publicly under the named
/// identity — the overlay never does).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandMarker {
    /// The 32-byte game id.
    pub game_id: [u8; 32],
    /// The publisher's compressed identity pubkey (exactly 33 bytes).
    pub identity_key: Vec<u8>,
    /// The 32-byte pot funding txid the hand is bound to.
    pub pot_txid: [u8; 32],
    /// The five card ordinals (each 0..=51, distinct, wire order).
    pub cards: [u8; 5],
    /// The DER ECDSA signature push (68..=74 bytes) — verified CLIENT-side
    /// only, under `[1,'low hand']` / keyID = gameId / counterparty 'anyone'.
    pub sig: Vec<u8>,
}

/// Walk minimal Bitcoin pushdata out of a byte slice → the pushed blobs,
/// in order, stopping at the first non-push opcode / a truncated push
/// (mirrors `collected`'s `read_pushes` verbatim, including the CHECKED
/// arithmetic — this worker runs on wasm32 where a crafted OP_PUSHDATA4
/// length would otherwise wrap the bounds guard and trap `/submit`;
/// adversarial-review MED, 2026-07-16).
fn read_pushes(bytes: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let op = bytes[i];
        i += 1; // safe: i < bytes.len()
        let len = match op {
            n if n < 0x4c => n as usize,
            0x4c => {
                if i >= bytes.len() {
                    return out;
                }
                let l = bytes[i] as usize;
                i += 1;
                l
            }
            0x4d => {
                if i.checked_add(2).is_none_or(|e| e > bytes.len()) {
                    return out;
                }
                let l = bytes[i] as usize | ((bytes[i + 1] as usize) << 8);
                i += 2;
                l
            }
            0x4e => {
                if i.checked_add(4).is_none_or(|e| e > bytes.len()) {
                    return out;
                }
                let l = (bytes[i] as usize)
                    | ((bytes[i + 1] as usize) << 8)
                    | ((bytes[i + 2] as usize) << 16)
                    | ((bytes[i + 3] as usize) << 24);
                i += 4;
                l
            }
            _ => return out, // a non-push opcode — stop
        };
        // CHECKED: `i + len` can overflow u32 on wasm32; overflow ⇒ out of bounds ⇒ stop.
        match i.checked_add(len) {
            Some(end) if end <= bytes.len() => {
                out.push(&bytes[i..end]);
                i = end;
            }
            _ => return out,
        }
    }
    out
}

/// The cards rule: exactly five ordinals, each `0..=51`, all DISTINCT —
/// the same validation the app's `validCards` and the result-v2 `cardsHex`
/// decode apply. A duplicate or out-of-range byte fails the whole push.
fn valid_cards(cards: &[u8]) -> bool {
    if cards.len() != HAND_CARDS_LEN {
        return false;
    }
    let mut seen = [false; 52];
    for &c in cards {
        if c > 51 || seen[c as usize] {
            return false;
        }
        seen[c as usize] = true;
    }
    true
}

/// Parse one output locking script as a `LOW/hand/v1` marker.
///
/// `Some(marker)` IFF the script is `OP_FALSE OP_RETURN` (0x00 0x6a)
/// followed by EXACTLY six minimal data pushes with the exact v1 shape:
/// tag == [`HAND_TAG`] (11 bytes), gameId 32 bytes, identityKey 33 bytes,
/// potTxid 32 bytes, cards 5 valid distinct ordinals, sig 68..=74 bytes.
/// Everything else — a bare `OP_RETURN`, a different tag, a wrong length,
/// bad cards, extra/missing pushes — is `None`.
pub fn parse_hand_marker(script: &[u8]) -> Option<HandMarker> {
    // OP_FALSE OP_RETURN (0x00 0x6a) — the exact prefix the app's builder
    // emits (the #382 contract pins the two-byte prefix, like #161's).
    if script.len() < 2 || script[0] != 0x00 || script[1] != 0x6a {
        return None;
    }
    let data = read_pushes(&script[2..]);

    if data.len() != HAND_FIELD_COUNT {
        return None;
    }
    let (tag, game_id_b, identity_key_b, pot_txid_b, cards_b, sig_b) =
        (data[0], data[1], data[2], data[3], data[4], data[5]);
    if tag != HAND_TAG {
        return None;
    }
    if game_id_b.len() != HAND_GAME_ID_LEN {
        return None;
    }
    if identity_key_b.len() != HAND_IDENTITY_KEY_LEN {
        return None;
    }
    if pot_txid_b.len() != HAND_POT_TXID_LEN {
        return None;
    }
    if !valid_cards(cards_b) {
        return None;
    }
    if !(HAND_SIG_MIN_LEN..=HAND_SIG_MAX_LEN).contains(&sig_b.len()) {
        return None;
    }

    let mut game_id = [0u8; 32];
    game_id.copy_from_slice(game_id_b);
    let mut pot_txid = [0u8; 32];
    pot_txid.copy_from_slice(pot_txid_b);
    let mut cards = [0u8; 5];
    cards.copy_from_slice(cards_b);
    Some(HandMarker {
        game_id,
        identity_key: identity_key_b.to_vec(),
        pot_txid,
        cards,
        sig: sig_b.to_vec(),
    })
}

/// True iff `script` is a well-formed `LOW/hand/v1` marker.
pub fn is_hand_marker_script(script: &[u8]) -> bool {
    parse_hand_marker(script).is_some()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Minimal Bitcoin pushdata for a byte blob (direct / OP_PUSHDATA1 /
    /// _2) — mirrors the app's `pushData` and collected's test helper.
    pub(crate) fn push_data(blob: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        let len = blob.len();
        if len < 0x4c {
            out.push(len as u8);
        } else if len <= 0xff {
            out.push(0x4c);
            out.push(len as u8);
        } else {
            out.push(0x4d);
            out.push((len & 0xff) as u8);
            out.push(((len >> 8) & 0xff) as u8);
        }
        out.extend_from_slice(blob);
        out
    }

    /// The app's `handMarkerScriptHex` in bytes.
    pub(crate) fn marker_script(
        game_id: &[u8; 32],
        identity_key: &[u8],
        pot_txid: &[u8; 32],
        cards: &[u8; 5],
        sig: &[u8],
    ) -> Vec<u8> {
        let mut s = vec![0x00, 0x6a]; // OP_FALSE OP_RETURN
        s.extend(push_data(HAND_TAG));
        s.extend(push_data(game_id));
        s.extend(push_data(identity_key));
        s.extend(push_data(pot_txid));
        s.extend(push_data(cards));
        s.extend(push_data(sig));
        s
    }

    /// The GOLDEN VECTOR of the #382 spec — the exact 192-byte script hex
    /// BOTH sides (the app's `handMarkerScriptHex` builder and this parser)
    /// must agree on. Inputs: tag=`LOW/hand/v1`, gameId=`11`×32,
    /// identityKey=`02`+`a1`×32, potTxid=`22`×32,
    /// cards=[0x00,0x0c,0x1a,0x27,0x33], sig=`30 45`+`ab`×69 (71 bytes).
    /// The same fixed hex is asserted in the client test suite.
    pub(crate) const GOLDEN_HAND_HEX: &str = "006a0b4c4f572f68616e642f76312011111111111111111111111111111111111111111111111111111111111111112102a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a120222222222222222222222222222222222222222222222222222222222222222205000c1a2733473045ababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab";

    pub(crate) fn golden_game_id() -> [u8; 32] {
        [0x11u8; 32]
    }
    pub(crate) fn golden_identity_key() -> Vec<u8> {
        let mut k = vec![0x02u8];
        k.extend_from_slice(&[0xa1u8; 32]);
        k
    }
    pub(crate) fn golden_pot_txid() -> [u8; 32] {
        [0x22u8; 32]
    }
    pub(crate) fn golden_cards() -> [u8; 5] {
        [0x00, 0x0c, 0x1a, 0x27, 0x33]
    }
    pub(crate) fn golden_sig() -> Vec<u8> {
        let mut s = vec![0x30u8, 0x45];
        s.extend_from_slice(&[0xabu8; 69]);
        s
    }

    // ── The golden interface vector (the cross-repo CONTRACT) ────────────

    #[test]
    fn builder_reproduces_the_golden_vector() {
        let script = marker_script(
            &golden_game_id(),
            &golden_identity_key(),
            &golden_pot_txid(),
            &golden_cards(),
            &golden_sig(),
        );
        assert_eq!(hex::encode(&script), GOLDEN_HAND_HEX);
    }

    #[test]
    fn golden_vector_parses_exactly() {
        let script = hex::decode(GOLDEN_HAND_HEX).expect("golden hex decodes");
        assert_eq!(script.len(), 192, "golden vector is exactly 192 bytes");
        let m = parse_hand_marker(&script).expect("golden vector must parse");
        assert_eq!(m.game_id, golden_game_id());
        assert_eq!(m.identity_key, golden_identity_key());
        assert_eq!(m.pot_txid, golden_pot_txid());
        assert_eq!(m.cards, golden_cards());
        assert_eq!(m.sig, golden_sig());
        assert!(is_hand_marker_script(&script));
    }

    #[test]
    fn tag_is_11_bytes() {
        assert_eq!(HAND_TAG.len(), 11);
        assert_eq!(HAND_TAG, b"LOW/hand/v1");
    }

    // ── The cards rule ───────────────────────────────────────────────────

    #[test]
    fn duplicate_card_rejected() {
        let mut cards = golden_cards();
        cards[1] = cards[0];
        let script = marker_script(
            &golden_game_id(),
            &golden_identity_key(),
            &golden_pot_txid(),
            &cards,
            &golden_sig(),
        );
        assert!(parse_hand_marker(&script).is_none());
    }

    #[test]
    fn out_of_range_card_rejected() {
        let mut cards = golden_cards();
        cards[4] = 52;
        let script = marker_script(
            &golden_game_id(),
            &golden_identity_key(),
            &golden_pot_txid(),
            &cards,
            &golden_sig(),
        );
        assert!(parse_hand_marker(&script).is_none());
    }

    // ── Shape strictness ─────────────────────────────────────────────────

    #[test]
    fn wrong_tag_rejected() {
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(b"LOW/hand/v2"));
        s.extend(push_data(&golden_game_id()));
        s.extend(push_data(&golden_identity_key()));
        s.extend(push_data(&golden_pot_txid()));
        s.extend(push_data(&golden_cards()));
        s.extend(push_data(&golden_sig()));
        assert!(parse_hand_marker(&s).is_none());
    }

    #[test]
    fn wrong_lengths_rejected() {
        // Short gameId.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(HAND_TAG));
        s.extend(push_data(&[0x11u8; 31]));
        s.extend(push_data(&golden_identity_key()));
        s.extend(push_data(&golden_pot_txid()));
        s.extend(push_data(&golden_cards()));
        s.extend(push_data(&golden_sig()));
        assert!(parse_hand_marker(&s).is_none());
        // Short identity key.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(HAND_TAG));
        s.extend(push_data(&golden_game_id()));
        s.extend(push_data(&[0x02u8; 32]));
        s.extend(push_data(&golden_pot_txid()));
        s.extend(push_data(&golden_cards()));
        s.extend(push_data(&golden_sig()));
        assert!(parse_hand_marker(&s).is_none());
        // Sig too short.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(HAND_TAG));
        s.extend(push_data(&golden_game_id()));
        s.extend(push_data(&golden_identity_key()));
        s.extend(push_data(&golden_pot_txid()));
        s.extend(push_data(&golden_cards()));
        s.extend(push_data(&[0x30u8; 67]));
        assert!(parse_hand_marker(&s).is_none());
    }

    #[test]
    fn missing_or_extra_pushes_rejected() {
        // Five pushes (no sig).
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(HAND_TAG));
        s.extend(push_data(&golden_game_id()));
        s.extend(push_data(&golden_identity_key()));
        s.extend(push_data(&golden_pot_txid()));
        s.extend(push_data(&golden_cards()));
        assert!(parse_hand_marker(&s).is_none());
        // Seven pushes (a trailer).
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(HAND_TAG));
        s.extend(push_data(&golden_game_id()));
        s.extend(push_data(&golden_identity_key()));
        s.extend(push_data(&golden_pot_txid()));
        s.extend(push_data(&golden_cards()));
        s.extend(push_data(&golden_sig()));
        s.extend(push_data(&[0x01u8]));
        assert!(parse_hand_marker(&s).is_none());
    }

    #[test]
    fn bare_op_return_prefix_rejected() {
        // The contract pins OP_FALSE OP_RETURN — a bare 0x6a is not admitted.
        let full = marker_script(
            &golden_game_id(),
            &golden_identity_key(),
            &golden_pot_txid(),
            &golden_cards(),
            &golden_sig(),
        );
        assert!(parse_hand_marker(&full[1..]).is_none());
    }

    #[test]
    fn adversarial_pushdata_len_never_panics_or_wraps() {
        // The collected-module MED, re-asserted on this parser: a crafted
        // OP_PUSHDATA4 length must stop cleanly, never wrap/trap on wasm32.
        let mut s = vec![0x00, 0x6a, 0x4e, 0xff, 0xff, 0xff, 0xff];
        s.push(0x00);
        assert!(parse_hand_marker(&s).is_none());
        let s2 = vec![0x00, 0x6a, 0x4e, 0xff, 0xff];
        assert!(parse_hand_marker(&s2).is_none());
    }
}
