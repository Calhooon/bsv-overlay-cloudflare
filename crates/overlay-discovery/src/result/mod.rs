//! `tm_result` / `ls_result` — LOW hand-result markers for the on-chain
//! leaderboard (bsv-low #38).
//!
//! When a LOW hand settles, the WINNER publishes a tiny `OP_RETURN`
//! "result" marker under this topic after its landing-proof-gated payout
//! credit. Leaderboard clients query `ls_result` (`resultsFor` /
//! `recentResults`) and count only the claims they can VERIFY: both
//! signatures are over the SAME canonical challenge, built client-side
//! and verified client-side with the 'anyone' ProtoWallet round-trip —
//! the overlay NEVER verifies signatures. The carried `potTxid` /
//! `settleTxid` let a client anchor the claim to a REAL settled pot via
//! `/pots-view`.
//!
//! Like `tm_collected`, this is an `OP_RETURN` data-carrier topic
//! admitted by BYTE FORMAT ONLY — the overlay is an INDEX, not an
//! authority, and the record surface must never lie: it carries the
//! marker's bytes back verbatim (no derived "confirmed" flag; a client
//! derives confirmation by verifying the sigs itself).
//!
//! A marker whose `loserSig` push is EMPTY is an UNCONFIRMED claim (the
//! winner's word alone); a marker carrying a DER `loserSig` is a
//! CONFIRMED claim — the loser's countersignature over the same
//! challenge. Which is which is the CLIENT's judgement after verifying;
//! the overlay only preserves the distinction (`loserSigHex: null` vs a
//! hex string).
//!
//! One structural rule beyond lengths: `winnerIdentity != loserIdentity`
//! (byte compare). A self-paired marker is rejected at parse time — it
//! would let one key sign BOTH slots and fake a "confirmed" win against
//! itself.
//!
//! # Marker wire format (`LOW/result/v1`)
//!
//! `OP_FALSE OP_RETURN` (0x00 0x6a) followed by EXACTLY EIGHT minimal
//! data pushes — byte-identical to the app's builder (the cross-repo
//! CONTRACT, bsv-low #38):
//!
//! | # | Push           | Encoding                                       |
//! |---|----------------|------------------------------------------------|
//! | 0 | tag            | UTF-8 `LOW/result/v1` (13 bytes)               |
//! | 1 | gameId         | 32 bytes                                       |
//! | 2 | winnerIdentity | 33 bytes (compressed pubkey)                   |
//! | 3 | loserIdentity  | 33 bytes (compressed pubkey)                   |
//! | 4 | potTxid        | 32 bytes                                       |
//! | 5 | settleTxid     | 32 bytes                                       |
//! | 6 | winnerSig      | DER ECDSA, 68..=74 bytes                       |
//! | 7 | loserSig       | EMPTY push (0 bytes, opcode 0x00 — unconfirmed)|
//! |   |                | OR DER ECDSA 68..=74 bytes (confirmed)         |
//!
//! # Marker wire format (`LOW/result/v2`)
//!
//! v2 adds the WINNER's five revealed cards (for the "lowest winning
//! hand" leaderboard), countersigned by the loser exactly like the rest
//! of the claim. `OP_FALSE OP_RETURN` followed by EXACTLY NINE minimal
//! data pushes — v1 with a `cards` push inserted between `settleTxid`
//! and `winnerSig`:
//!
//! | # | Push           | Encoding                                       |
//! |---|----------------|------------------------------------------------|
//! | 0 | tag            | UTF-8 `LOW/result/v2` (13 bytes)               |
//! | 1 | gameId         | 32 bytes                                       |
//! | 2 | winnerIdentity | 33 bytes (compressed pubkey)                   |
//! | 3 | loserIdentity  | 33 bytes (compressed pubkey)                   |
//! | 4 | potTxid        | 32 bytes                                       |
//! | 5 | settleTxid     | 32 bytes                                       |
//! | 6 | cards          | EXACTLY 5 bytes — card indices, each 0..=51,   |
//! |   |                | all five DISTINCT (single deck)                |
//! | 7 | winnerSig      | DER ECDSA, 68..=74 bytes                       |
//! | 8 | loserSig       | EMPTY push (unconfirmed) OR DER 68..=74        |
//!
//! The cards DISTINCTNESS rule is structural like winner != loser:
//! without it a fabricated marker could claim five copies of an ace.
//! (Genuineness still lives in the client sig verify — the loser
//! countersigns the cards along with the rest of the claim.)
//!
//! Both versions are admitted (v1 stays back-compatible — live rows
//! exist). The parser validates each shape strictly (exact tag + exact
//! push count + exact push lengths + winner != loser + the v2 cards
//! rules) and extracts the fields. Wrong tag / wrong lengths / missing
//! or extra pushes / truncated pushes / a self-paired marker /
//! out-of-range or duplicate cards → `None` (not a result marker).
//!
//! # Lookup (`ls_result`)
//!
//! Query JSON (tagged by `type`):
//!
//! ```json
//! {"type": "resultsFor", "identity": "<66 hex chars>", "limit": 50}
//! {"type": "recentResults", "limit": 50}
//! ```
//!
//! `limit` is optional (default 100, clamped to 1..=500). The answer is
//! a freeform JSON array, newest first, one entry per stored marker:
//!
//! ```json
//! [{"gameId": "<hex>", "winner": "<hex>", "loser": "<hex>",
//!   "potTxid": "<hex>", "settleTxid": "<hex>",
//!   "winnerSigHex": "<hex>", "loserSigHex": "<hex|null>",
//!   "cardsHex": "<10 hex|null>",
//!   "txid": "<hex>", "outputIndex": 0, "createdAt": 1234567890}]
//! ```
//!
//! `cardsHex` is the v2 cards push verbatim (10 lowercase hex chars);
//! `null` for rows admitted from v1 markers.
//!
//! The index keeps EVERY admitted marker, keyed by its outpoint — the
//! same `(gameId, winner)` may return multiple rows (e.g. a garbage-sig
//! front-run AND the genuine marker), and clients drop the rows whose
//! sigs fail their verify before counting.

pub mod lookup_service;
pub mod storage;
pub mod topic_manager;

/// The domain tag the app stamps. v1 = `(tag, gameId, winnerIdentity,
/// loserIdentity, potTxid, settleTxid, winnerSig, loserSig)`.
/// 13 bytes of ASCII — the byte layout is the cross-repo CONTRACT
/// (bsv-low #38); never change it without a version bump on both sides.
pub const RESULT_TAG: &[u8] = b"LOW/result/v1";
/// The v2 domain tag — v1 plus the winner's five revealed cards
/// (`cards` push between settleTxid and winnerSig). Same 13-byte ASCII
/// layout discipline as v1.
pub const RESULT_TAG_V2: &[u8] = b"LOW/result/v2";
/// Number of minimal data pushes in a well-formed v1 marker.
pub const RESULT_FIELD_COUNT: usize = 8;
/// Number of minimal data pushes in a well-formed v2 marker.
pub const RESULT_FIELD_COUNT_V2: usize = 9;
/// v2 cards push length (bytes) — the winner's five revealed cards.
pub const RESULT_CARDS_LEN: usize = 5;
/// Maximum card index (single 52-card deck, 0-based).
pub const RESULT_CARD_MAX: u8 = 51;
/// gameId push length (bytes).
pub const RESULT_GAME_ID_LEN: usize = 32;
/// winner/loser identity push length (bytes) — a compressed secp256k1
/// pubkey.
pub const RESULT_IDENTITY_KEY_LEN: usize = 33;
/// potTxid / settleTxid push length (bytes).
pub const RESULT_TXID_LEN: usize = 32;
/// Minimum sig push length (bytes) — a DER ECDSA signature.
pub const RESULT_SIG_MIN_LEN: usize = 68;
/// Maximum sig push length (bytes) — a DER ECDSA signature.
pub const RESULT_SIG_MAX_LEN: usize = 74;

/// A decoded v1 result marker — one settled hand's "winner beat loser"
/// claim published on-chain.
///
/// The overlay only needs `(game_id, winner)` to key the index plus the
/// raw bytes to hand back to querying clients (which verify the sigs
/// with the 'anyone' ProtoWallet round-trip — the overlay never does).
/// `loser_sig` is `None` when the marker's loserSig push was empty (an
/// UNCONFIRMED claim); `Some` carries the loser's countersignature (a
/// CONFIRMED claim — confirmed as judged by the verifying CLIENT).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultMarker {
    /// The 32-byte game id.
    pub game_id: [u8; 32],
    /// The winner's compressed identity pubkey (exactly 33 bytes).
    pub winner: Vec<u8>,
    /// The loser's compressed identity pubkey (exactly 33 bytes) —
    /// guaranteed != `winner` (a self-paired marker never parses).
    pub loser: Vec<u8>,
    /// The 32-byte pot funding txid the claim anchors to.
    pub pot_txid: [u8; 32],
    /// The 32-byte settle txid the claim anchors to.
    pub settle_txid: [u8; 32],
    /// The winner's DER ECDSA signature push (68..=74 bytes) — verified
    /// CLIENT-side only.
    pub winner_sig: Vec<u8>,
    /// The loser's DER ECDSA countersignature push (68..=74 bytes), or
    /// `None` when the marker's loserSig push was empty (unconfirmed).
    pub loser_sig: Option<Vec<u8>>,
    /// v2 only: the winner's five revealed cards (indices 0..=51, all
    /// distinct — validated at parse). `None` for a v1 marker. Present
    /// on BOTH confirmed and unconfirmed v2 markers (only `loser_sig`
    /// distinguishes those).
    pub cards: Option<[u8; 5]>,
}

/// Walk minimal Bitcoin pushdata out of a byte slice → the pushed blobs,
/// in order, stopping at the first non-push opcode / a truncated push
/// (mirrors the app's `readPushes` and `collected`'s `read_pushes`).
///
/// EVERY offset advance uses CHECKED arithmetic. This worker runs on wasm32
/// (`usize = u32`) with wrapping release arithmetic — an OP_PUSHDATA4 length
/// of `0xFFFFFFFF` would make a naive `i + len` WRAP past the bounds guard and
/// panic-trap the topic-manager `/submit` pass on a ~7-byte crafted script.
/// `checked_add` → `None` on overflow → we stop cleanly (a malformed marker is
/// simply skipped, never a trap). Adversarial-review MED, 2026-07-16.
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

/// Validate a v2 cards push: EXACTLY five bytes, each a card index
/// 0..=51, all five DISTINCT (single deck — without distinctness a
/// fabricated marker could claim five copies of an ace).
fn parse_cards(cards_b: &[u8]) -> Option<[u8; 5]> {
    if cards_b.len() != RESULT_CARDS_LEN {
        return None;
    }
    let mut seen = 0u64;
    for &c in cards_b {
        if c > RESULT_CARD_MAX {
            return None;
        }
        if seen & (1u64 << c) != 0 {
            return None; // duplicate card
        }
        seen |= 1u64 << c;
    }
    let mut cards = [0u8; 5];
    cards.copy_from_slice(cards_b);
    Some(cards)
}

/// Parse one output locking script as a `LOW/result/v1` or
/// `LOW/result/v2` marker.
///
/// `Some(marker)` IFF the script is `OP_FALSE OP_RETURN` (0x00 0x6a)
/// followed by EXACTLY the version's push count with its exact shape —
/// v1 (tag == [`RESULT_TAG`], 8 pushes): gameId 32 bytes, winnerIdentity
/// 33 bytes, loserIdentity 33 bytes, potTxid 32 bytes, settleTxid 32
/// bytes, winnerSig 68..=74 bytes, loserSig EMPTY (0 bytes) or 68..=74
/// bytes; v2 (tag == [`RESULT_TAG_V2`], 9 pushes): the same with a
/// `cards` push (exactly 5 bytes, each 0..=51, all distinct) between
/// settleTxid and winnerSig — AND, in both versions, winnerIdentity !=
/// loserIdentity (a self-paired marker would let one key sign both
/// slots and fake a "confirmed" win against itself). Everything else —
/// a bare `OP_RETURN`, a different tag, a wrong length, extra/missing
/// pushes, out-of-range or duplicate cards — is `None`.
///
/// Deliberately Option (not the reveal parser's three-way Result): the
/// admit decision is binary "is this an exact result byte format?" and a
/// tagged-but-malformed script is simply not admitted.
pub fn parse_result_marker(script: &[u8]) -> Option<ResultMarker> {
    // OP_FALSE OP_RETURN (0x00 0x6a) — the exact prefix the app's builder
    // emits (stricter than reveal, which also accepts a bare OP_RETURN;
    // the #38 contract pins the two-byte prefix, like #161's).
    if script.len() < 2 || script[0] != 0x00 || script[1] != 0x6a {
        return None;
    }
    let data = read_pushes(&script[2..]);
    if data.is_empty() {
        return None;
    }

    // Version dispatch on the tag push: each version pins its exact push
    // count. A v1 tag with 9 pushes / a v2 tag with 8 pushes is malformed.
    let (game_id_b, winner_b, loser_b, pot_txid_b, settle_txid_b, cards, winner_sig_b, loser_sig_b) =
        match data[0] {
            tag if tag == RESULT_TAG => {
                if data.len() != RESULT_FIELD_COUNT {
                    return None;
                }
                (
                    data[1], data[2], data[3], data[4], data[5], None, data[6], data[7],
                )
            }
            tag if tag == RESULT_TAG_V2 => {
                if data.len() != RESULT_FIELD_COUNT_V2 {
                    return None;
                }
                // cards: exactly 5 distinct indices 0..=51 — or not a marker.
                let cards = parse_cards(data[6])?;
                (
                    data[1],
                    data[2],
                    data[3],
                    data[4],
                    data[5],
                    Some(cards),
                    data[7],
                    data[8],
                )
            }
            _ => return None,
        };

    if game_id_b.len() != RESULT_GAME_ID_LEN {
        return None;
    }
    if winner_b.len() != RESULT_IDENTITY_KEY_LEN {
        return None;
    }
    if loser_b.len() != RESULT_IDENTITY_KEY_LEN {
        return None;
    }
    // A self-paired marker is rejected: one key signing both slots could
    // fake a "confirmed" win against itself.
    if winner_b == loser_b {
        return None;
    }
    if pot_txid_b.len() != RESULT_TXID_LEN {
        return None;
    }
    if settle_txid_b.len() != RESULT_TXID_LEN {
        return None;
    }
    if !(RESULT_SIG_MIN_LEN..=RESULT_SIG_MAX_LEN).contains(&winner_sig_b.len()) {
        return None;
    }
    // loserSig: an EMPTY push (unconfirmed claim) or a DER sig (confirmed).
    let loser_sig = if loser_sig_b.is_empty() {
        None
    } else if (RESULT_SIG_MIN_LEN..=RESULT_SIG_MAX_LEN).contains(&loser_sig_b.len()) {
        Some(loser_sig_b.to_vec())
    } else {
        return None;
    };

    let mut game_id = [0u8; 32];
    game_id.copy_from_slice(game_id_b);
    let mut pot_txid = [0u8; 32];
    pot_txid.copy_from_slice(pot_txid_b);
    let mut settle_txid = [0u8; 32];
    settle_txid.copy_from_slice(settle_txid_b);
    Some(ResultMarker {
        game_id,
        winner: winner_b.to_vec(),
        loser: loser_b.to_vec(),
        pot_txid,
        settle_txid,
        winner_sig: winner_sig_b.to_vec(),
        loser_sig,
        cards,
    })
}

/// True iff `script` is a well-formed `LOW/result/v1` or
/// `LOW/result/v2` marker.
pub fn is_result_marker_script(script: &[u8]) -> bool {
    parse_result_marker(script).is_some()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Minimal Bitcoin pushdata for a byte blob (direct / OP_PUSHDATA1 /
    /// _2) — mirrors the app's `pushData` and collected's test helper. An
    /// empty blob encodes as the single opcode 0x00 (OP_0 — a zero-length
    /// push), which is how an unconfirmed marker's loserSig travels.
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

    /// The app's v2 result-marker builder in bytes — v1 plus the cards
    /// push between settleTxid and winnerSig. `loser_sig = &[]` encodes
    /// the empty push (an unconfirmed claim).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn marker_script_v2(
        game_id: &[u8; 32],
        winner: &[u8],
        loser: &[u8],
        pot_txid: &[u8; 32],
        settle_txid: &[u8; 32],
        cards: &[u8],
        winner_sig: &[u8],
        loser_sig: &[u8],
    ) -> Vec<u8> {
        let mut s = vec![0x00, 0x6a]; // OP_FALSE OP_RETURN
        s.extend(push_data(RESULT_TAG_V2));
        s.extend(push_data(game_id));
        s.extend(push_data(winner));
        s.extend(push_data(loser));
        s.extend(push_data(pot_txid));
        s.extend(push_data(settle_txid));
        s.extend(push_data(cards));
        s.extend(push_data(winner_sig));
        s.extend(push_data(loser_sig));
        s
    }

    /// The app's result-marker builder in bytes. `loser_sig = &[]` encodes
    /// the empty push (an unconfirmed claim).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn marker_script(
        game_id: &[u8; 32],
        winner: &[u8],
        loser: &[u8],
        pot_txid: &[u8; 32],
        settle_txid: &[u8; 32],
        winner_sig: &[u8],
        loser_sig: &[u8],
    ) -> Vec<u8> {
        let mut s = vec![0x00, 0x6a]; // OP_FALSE OP_RETURN
        s.extend(push_data(RESULT_TAG));
        s.extend(push_data(game_id));
        s.extend(push_data(winner));
        s.extend(push_data(loser));
        s.extend(push_data(pot_txid));
        s.extend(push_data(settle_txid));
        s.extend(push_data(winner_sig));
        s.extend(push_data(loser_sig));
        s
    }

    /// The CONFIRMED golden vector from the #38 spec — the exact 326-byte
    /// script hex BOTH sides (the app's builder and this parser) must
    /// agree on. Inputs: tag=`LOW/result/v1`, gameId=`11`×32,
    /// winner=`02`+`a1`×32, loser=`03`+`b2`×32, potTxid=`22`×32,
    /// settleTxid=`33`×32, winnerSig=`30 45`+`ab`×69 (71 bytes),
    /// loserSig=`30 44`+`cd`×68 (70 bytes). The same fixed hex is
    /// asserted in the client test suite.
    pub(crate) const GOLDEN_RESULT_HEX: &str = "006a0d4c4f572f726573756c742f76312011111111111111111111111111111111111111111111111111111111111111112102a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a12103b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2202222222222222222222222222222222222222222222222222222222222222222203333333333333333333333333333333333333333333333333333333333333333473045ababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab463044cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

    /// The UNCONFIRMED golden vector — the same inputs but loserSig is the
    /// EMPTY push (the single script byte 0x00). 256 bytes. Parses with
    /// `loser_sig == None`.
    pub(crate) const GOLDEN_RESULT_UNCONFIRMED_HEX: &str = "006a0d4c4f572f726573756c742f76312011111111111111111111111111111111111111111111111111111111111111112102a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a12103b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2202222222222222222222222222222222222222222222222222222222222222222203333333333333333333333333333333333333333333333333333333333333333473045ababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab00";

    /// The CONFIRMED v2 golden vector — the v1 golden inputs plus
    /// cards = [0,1,2,3,4] (the `05 0001020304` push between settleTxid
    /// and winnerSig). 332 bytes. Computed independently on both sides of
    /// the cross-repo contract.
    pub(crate) const GOLDEN_RESULT_V2_HEX: &str = "006a0d4c4f572f726573756c742f76322011111111111111111111111111111111111111111111111111111111111111112102a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a12103b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2202222222222222222222222222222222222222222222222222222222222222222203333333333333333333333333333333333333333333333333333333333333333050001020304473045ababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab463044cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

    /// The UNCONFIRMED v2 golden vector — same but loserSig is the EMPTY
    /// push (trailing 0x00). 262 bytes. Parses with `loser_sig == None`
    /// but cards STILL present (only the countersignature is missing).
    pub(crate) const GOLDEN_RESULT_V2_UNCONFIRMED_HEX: &str = "006a0d4c4f572f726573756c742f76322011111111111111111111111111111111111111111111111111111111111111112102a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a12103b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2202222222222222222222222222222222222222222222222222222222222222222203333333333333333333333333333333333333333333333333333333333333333050001020304473045ababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab00";

    /// The golden vectors' expected fields.
    pub(crate) fn golden_game_id() -> [u8; 32] {
        [0x11u8; 32]
    }
    pub(crate) fn golden_winner() -> Vec<u8> {
        let mut k = vec![0x02u8];
        k.extend_from_slice(&[0xa1u8; 32]);
        k
    }
    pub(crate) fn golden_loser() -> Vec<u8> {
        let mut k = vec![0x03u8];
        k.extend_from_slice(&[0xb2u8; 32]);
        k
    }
    pub(crate) fn golden_pot_txid() -> [u8; 32] {
        [0x22u8; 32]
    }
    pub(crate) fn golden_settle_txid() -> [u8; 32] {
        [0x33u8; 32]
    }
    pub(crate) fn golden_winner_sig() -> Vec<u8> {
        let mut s = vec![0x30u8, 0x45];
        s.extend_from_slice(&[0xabu8; 69]);
        s
    }
    pub(crate) fn golden_loser_sig() -> Vec<u8> {
        let mut s = vec![0x30u8, 0x44];
        s.extend_from_slice(&[0xcdu8; 68]);
        s
    }
    pub(crate) fn golden_cards() -> [u8; 5] {
        [0, 1, 2, 3, 4]
    }

    /// A valid v2 marker script over the golden identities with a chosen
    /// gameId + cards + loserSig.
    pub(crate) fn golden_marker_v2(
        game_id: &[u8; 32],
        cards: &[u8],
        loser_sig: &[u8],
    ) -> Vec<u8> {
        marker_script_v2(
            game_id,
            &golden_winner(),
            &golden_loser(),
            &golden_pot_txid(),
            &golden_settle_txid(),
            cards,
            &golden_winner_sig(),
            loser_sig,
        )
    }

    /// A valid marker script over the golden identities with a chosen
    /// gameId + loserSig — the common test shorthand.
    pub(crate) fn golden_marker(game_id: &[u8; 32], loser_sig: &[u8]) -> Vec<u8> {
        marker_script(
            game_id,
            &golden_winner(),
            &golden_loser(),
            &golden_pot_txid(),
            &golden_settle_txid(),
            &golden_winner_sig(),
            loser_sig,
        )
    }

    // ── The golden interface vectors (the cross-repo CONTRACT) ───────────

    #[test]
    fn golden_vector_parses_exactly() {
        let script = hex::decode(GOLDEN_RESULT_HEX).expect("golden hex decodes");
        assert_eq!(script.len(), 326, "confirmed golden vector is exactly 326 bytes");

        let m = parse_result_marker(&script).expect("golden vector must parse");
        assert_eq!(m.game_id, golden_game_id());
        assert_eq!(m.winner, golden_winner());
        assert_eq!(m.winner.len(), 33);
        assert_eq!(m.loser, golden_loser());
        assert_eq!(m.loser.len(), 33);
        assert_eq!(m.pot_txid, golden_pot_txid());
        assert_eq!(m.settle_txid, golden_settle_txid());
        assert_eq!(m.winner_sig, golden_winner_sig());
        assert_eq!(m.winner_sig.len(), 71);
        assert_eq!(m.loser_sig.as_deref(), Some(golden_loser_sig().as_slice()));
        assert_eq!(m.loser_sig.as_ref().unwrap().len(), 70);
        assert!(is_result_marker_script(&script));
    }

    #[test]
    fn golden_unconfirmed_vector_parses_with_no_loser_sig() {
        let script =
            hex::decode(GOLDEN_RESULT_UNCONFIRMED_HEX).expect("unconfirmed golden hex decodes");
        assert_eq!(
            script.len(),
            256,
            "unconfirmed golden vector is exactly 256 bytes"
        );
        // The empty loserSig push is the single trailing opcode 0x00.
        assert_eq!(*script.last().unwrap(), 0x00);

        let m = parse_result_marker(&script).expect("unconfirmed golden vector must parse");
        assert_eq!(m.game_id, golden_game_id());
        assert_eq!(m.winner, golden_winner());
        assert_eq!(m.loser, golden_loser());
        assert_eq!(m.pot_txid, golden_pot_txid());
        assert_eq!(m.settle_txid, golden_settle_txid());
        assert_eq!(m.winner_sig, golden_winner_sig());
        assert_eq!(m.loser_sig, None, "empty loserSig push ⇒ unconfirmed");
        assert!(is_result_marker_script(&script));
    }

    #[test]
    fn builder_reproduces_the_golden_vectors() {
        // The test-side builder (mirroring the app's) must PRODUCE the exact
        // golden hexes for the golden inputs — round-trip both directions.
        let confirmed = golden_marker(&golden_game_id(), &golden_loser_sig());
        assert_eq!(hex::encode(&confirmed), GOLDEN_RESULT_HEX);

        let unconfirmed = golden_marker(&golden_game_id(), &[]);
        assert_eq!(hex::encode(&unconfirmed), GOLDEN_RESULT_UNCONFIRMED_HEX);
    }

    #[test]
    fn tag_is_13_bytes() {
        assert_eq!(RESULT_TAG.len(), 13);
        assert_eq!(RESULT_TAG, b"LOW/result/v1");
        assert_eq!(RESULT_TAG_V2.len(), 13);
        assert_eq!(RESULT_TAG_V2, b"LOW/result/v2");
    }

    // ── The v2 golden interface vectors ───────────────────────────────────

    #[test]
    fn v2_golden_vector_parses_exactly() {
        let script = hex::decode(GOLDEN_RESULT_V2_HEX).expect("v2 golden hex decodes");
        assert_eq!(script.len(), 332, "confirmed v2 golden vector is exactly 332 bytes");

        let m = parse_result_marker(&script).expect("v2 golden vector must parse");
        assert_eq!(m.game_id, golden_game_id());
        assert_eq!(m.winner, golden_winner());
        assert_eq!(m.loser, golden_loser());
        assert_eq!(m.pot_txid, golden_pot_txid());
        assert_eq!(m.settle_txid, golden_settle_txid());
        assert_eq!(m.cards, Some(golden_cards()), "v2 carries the cards");
        assert_eq!(m.winner_sig, golden_winner_sig());
        assert_eq!(m.loser_sig.as_deref(), Some(golden_loser_sig().as_slice()));
        assert!(is_result_marker_script(&script));
    }

    #[test]
    fn v2_golden_unconfirmed_vector_parses_with_cards_but_no_loser_sig() {
        let script = hex::decode(GOLDEN_RESULT_V2_UNCONFIRMED_HEX)
            .expect("v2 unconfirmed golden hex decodes");
        assert_eq!(
            script.len(),
            262,
            "unconfirmed v2 golden vector is exactly 262 bytes"
        );
        assert_eq!(*script.last().unwrap(), 0x00);

        let m = parse_result_marker(&script).expect("v2 unconfirmed golden vector must parse");
        // Unconfirmed still has cards — ONLY loserSig is empty.
        assert_eq!(m.cards, Some(golden_cards()));
        assert_eq!(m.loser_sig, None, "empty loserSig push ⇒ unconfirmed");
        assert_eq!(m.winner_sig, golden_winner_sig());
        assert!(is_result_marker_script(&script));
    }

    #[test]
    fn builder_reproduces_the_v2_golden_vectors() {
        let confirmed = golden_marker_v2(&golden_game_id(), &golden_cards(), &golden_loser_sig());
        assert_eq!(hex::encode(&confirmed), GOLDEN_RESULT_V2_HEX);

        let unconfirmed = golden_marker_v2(&golden_game_id(), &golden_cards(), &[]);
        assert_eq!(hex::encode(&unconfirmed), GOLDEN_RESULT_V2_UNCONFIRMED_HEX);
    }

    #[test]
    fn v1_goldens_still_parse_with_no_cards() {
        // Back-compat: v1 markers remain admitted unchanged (live rows
        // exist) and parse with cards == None.
        for hex_v1 in [GOLDEN_RESULT_HEX, GOLDEN_RESULT_UNCONFIRMED_HEX] {
            let m = parse_result_marker(&hex::decode(hex_v1).unwrap())
                .expect("v1 golden must still parse");
            assert_eq!(m.cards, None, "a v1 marker has no cards");
        }
    }

    // ── v2 cards rules ────────────────────────────────────────────────────

    #[test]
    fn v2_card_index_out_of_range_rejected() {
        // 51 is the highest card; 52 (and beyond) is not in the deck.
        for bad in [[0u8, 1, 2, 3, 52], [52u8, 1, 2, 3, 4], [0u8, 1, 2, 3, 255]] {
            let script = golden_marker_v2(&golden_game_id(), &bad, &golden_loser_sig());
            assert!(
                parse_result_marker(&script).is_none(),
                "cards {bad:?} must be rejected"
            );
        }
        // The boundary itself is fine.
        let script = golden_marker_v2(&golden_game_id(), &[47, 48, 49, 50, 51], &golden_loser_sig());
        assert!(parse_result_marker(&script).is_some(), "card 51 is valid");
    }

    #[test]
    fn v2_duplicate_cards_rejected() {
        // Single deck: five DISTINCT cards, or a fabricated marker could
        // claim five copies of an ace.
        for bad in [
            [0u8, 0, 0, 0, 0],
            [0u8, 1, 2, 3, 3],
            [51u8, 1, 2, 51, 4],
            [7u8, 7, 2, 3, 4],
        ] {
            let script = golden_marker_v2(&golden_game_id(), &bad, &golden_loser_sig());
            assert!(
                parse_result_marker(&script).is_none(),
                "duplicate cards {bad:?} must be rejected"
            );
        }
    }

    #[test]
    fn v2_wrong_cards_length_rejected() {
        for bad in [&[0u8, 1, 2, 3][..], &[0u8, 1, 2, 3, 4, 5][..], &[][..]] {
            let script = golden_marker_v2(&golden_game_id(), bad, &golden_loser_sig());
            assert!(
                parse_result_marker(&script).is_none(),
                "cards push of {} bytes must be rejected",
                bad.len()
            );
        }
    }

    #[test]
    fn version_and_push_count_must_agree() {
        // A v2 tag with the v1 body (8 pushes — no cards) is malformed.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(RESULT_TAG_V2));
        s.extend(push_data(&golden_game_id()));
        s.extend(push_data(&golden_winner()));
        s.extend(push_data(&golden_loser()));
        s.extend(push_data(&golden_pot_txid()));
        s.extend(push_data(&golden_settle_txid()));
        s.extend(push_data(&golden_winner_sig()));
        s.extend(push_data(&golden_loser_sig()));
        assert!(parse_result_marker(&s).is_none(), "v2 tag + 8 pushes rejected");

        // A v1 tag with the v2 body (9 pushes — a cards push) is malformed.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(RESULT_TAG));
        s.extend(push_data(&golden_game_id()));
        s.extend(push_data(&golden_winner()));
        s.extend(push_data(&golden_loser()));
        s.extend(push_data(&golden_pot_txid()));
        s.extend(push_data(&golden_settle_txid()));
        s.extend(push_data(&golden_cards()));
        s.extend(push_data(&golden_winner_sig()));
        s.extend(push_data(&golden_loser_sig()));
        assert!(parse_result_marker(&s).is_none(), "v1 tag + 9 pushes rejected");
    }

    #[test]
    fn v2_self_paired_marker_rejected() {
        // winner == loser applies to v2 exactly as to v1.
        let script = marker_script_v2(
            &golden_game_id(),
            &golden_winner(),
            &golden_winner(),
            &golden_pot_txid(),
            &golden_settle_txid(),
            &golden_cards(),
            &golden_winner_sig(),
            &golden_loser_sig(),
        );
        assert!(parse_result_marker(&script).is_none());
    }

    #[test]
    fn adversarial_pushdata_len_never_panics_or_wraps() {
        // Adversarial-review MED (inherited from collected): an OP_PUSHDATA4
        // (0x4e) with len 0xFFFFFFFF on wasm32 (usize=u32) would wrap
        // `i + len` past the bounds guard → slice panic → topic-manager
        // /submit trap. The crafted ~7-byte script must parse to None (no
        // marker), never panic. Also probe OP_PUSHDATA2 and a truncated
        // push. (parse_result_marker skips the 006a prefix; call
        // read_pushes semantics via the full parser so the guard is
        // exercised.)
        for script in [
            vec![0x00u8, 0x6a, 0x4e, 0xff, 0xff, 0xff, 0xff], // PUSHDATA4 max len, no data
            vec![0x00u8, 0x6a, 0x4d, 0xff, 0xff],             // PUSHDATA2 max len, no data
            vec![0x00u8, 0x6a, 0x4e, 0xff, 0xff, 0xff],       // PUSHDATA4 header truncated
            vec![0x00u8, 0x6a, 0x4b],                         // a 75-byte push with no data
        ] {
            assert_eq!(parse_result_marker(&script), None, "crafted script must not parse");
        }
        // Direct read_pushes probe: the trap path is the len itself.
        assert!(read_pushes(&[0x4e, 0xff, 0xff, 0xff, 0xff]).is_empty());
        assert!(read_pushes(&[0x4d, 0xff, 0xff]).is_empty());
    }

    // ── Valid markers ─────────────────────────────────────────────────────

    #[test]
    fn valid_marker_parses() {
        for sig_len in [68usize, 70, 71, 72, 74] {
            // Confirmed: both sigs at this length.
            let script = marker_script(
                &[0xABu8; 32],
                &golden_winner(),
                &golden_loser(),
                &golden_pot_txid(),
                &golden_settle_txid(),
                &vec![0x30; sig_len],
                &vec![0x30; sig_len],
            );
            let m = parse_result_marker(&script)
                .unwrap_or_else(|| panic!("sig len {sig_len} must parse"));
            assert_eq!(m.game_id, [0xABu8; 32]);
            assert_eq!(m.winner_sig.len(), sig_len);
            assert_eq!(m.loser_sig.as_ref().unwrap().len(), sig_len);

            // Unconfirmed: empty loserSig at every winnerSig length.
            let script = marker_script(
                &[0xABu8; 32],
                &golden_winner(),
                &golden_loser(),
                &golden_pot_txid(),
                &golden_settle_txid(),
                &vec![0x30; sig_len],
                &[],
            );
            let m = parse_result_marker(&script)
                .unwrap_or_else(|| panic!("unconfirmed sig len {sig_len} must parse"));
            assert_eq!(m.loser_sig, None);
        }
    }

    // ── Rejections ────────────────────────────────────────────────────────

    #[test]
    fn self_paired_marker_rejected() {
        // winner == loser (byte compare) — one key faking a "confirmed"
        // win against itself. Rejected at parse time.
        let script = marker_script(
            &golden_game_id(),
            &golden_winner(),
            &golden_winner(), // loser slot = the SAME key
            &golden_pot_txid(),
            &golden_settle_txid(),
            &golden_winner_sig(),
            &golden_loser_sig(),
        );
        assert!(parse_result_marker(&script).is_none());
        // Also in the unconfirmed shape.
        let script = marker_script(
            &golden_game_id(),
            &golden_loser(),
            &golden_loser(),
            &golden_pot_txid(),
            &golden_settle_txid(),
            &golden_winner_sig(),
            &[],
        );
        assert!(parse_result_marker(&script).is_none());
    }

    #[test]
    fn wrong_tag_rejected() {
        for bad_tag in [
            b"LOW/result/v2".as_slice(),    // wrong version
            b"LOW/reveal/v2".as_slice(),    // a different LOW topic
            b"SOMETHINGELS!".as_slice(),    // foreign 13-byte tag
            b"LOW/result/v".as_slice(),     // 12 bytes
            b"LOW/collected/v1".as_slice(), // the sibling topic's tag
        ] {
            let mut s = vec![0x00, 0x6a];
            s.extend(push_data(bad_tag));
            s.extend(push_data(&[0x11u8; 32]));
            s.extend(push_data(&golden_winner()));
            s.extend(push_data(&golden_loser()));
            s.extend(push_data(&golden_pot_txid()));
            s.extend(push_data(&golden_settle_txid()));
            s.extend(push_data(&golden_winner_sig()));
            s.extend(push_data(&golden_loser_sig()));
            assert!(
                parse_result_marker(&s).is_none(),
                "tag {bad_tag:?} must be rejected"
            );
        }
    }

    #[test]
    fn non_op_return_rejected() {
        // A standard P2PKH is not an OP_RETURN.
        let p2pkh = hex::decode("76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac").unwrap();
        assert!(parse_result_marker(&p2pkh).is_none());
        // Empty / one-byte scripts.
        assert!(parse_result_marker(&[]).is_none());
        assert!(parse_result_marker(&[0x00]).is_none());
        // A bare OP_RETURN (0x6a without OP_FALSE) is NOT the v1 prefix.
        let script = golden_marker(&golden_game_id(), &golden_loser_sig());
        assert!(parse_result_marker(&script[1..]).is_none());
    }

    #[test]
    fn short_game_id_rejected() {
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(RESULT_TAG));
        s.extend(push_data(&[0x11u8; 31])); // 31, not 32
        s.extend(push_data(&golden_winner()));
        s.extend(push_data(&golden_loser()));
        s.extend(push_data(&golden_pot_txid()));
        s.extend(push_data(&golden_settle_txid()));
        s.extend(push_data(&golden_winner_sig()));
        s.extend(push_data(&golden_loser_sig()));
        assert!(parse_result_marker(&s).is_none());
    }

    #[test]
    fn wrong_identity_key_length_rejected() {
        for len in [32usize, 34, 65] {
            // Bad winner length.
            let script = marker_script(
                &[0x11u8; 32],
                &vec![0x02u8; len],
                &golden_loser(),
                &golden_pot_txid(),
                &golden_settle_txid(),
                &golden_winner_sig(),
                &golden_loser_sig(),
            );
            assert!(
                parse_result_marker(&script).is_none(),
                "winner len {len} must be rejected"
            );
            // Bad loser length.
            let script = marker_script(
                &[0x11u8; 32],
                &golden_winner(),
                &vec![0x03u8; len],
                &golden_pot_txid(),
                &golden_settle_txid(),
                &golden_winner_sig(),
                &golden_loser_sig(),
            );
            assert!(
                parse_result_marker(&script).is_none(),
                "loser len {len} must be rejected"
            );
        }
    }

    #[test]
    fn wrong_txid_length_rejected() {
        // potTxid 31 bytes.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(RESULT_TAG));
        s.extend(push_data(&[0x11u8; 32]));
        s.extend(push_data(&golden_winner()));
        s.extend(push_data(&golden_loser()));
        s.extend(push_data(&[0x22u8; 31]));
        s.extend(push_data(&golden_settle_txid()));
        s.extend(push_data(&golden_winner_sig()));
        s.extend(push_data(&golden_loser_sig()));
        assert!(parse_result_marker(&s).is_none());
        // settleTxid 33 bytes.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(RESULT_TAG));
        s.extend(push_data(&[0x11u8; 32]));
        s.extend(push_data(&golden_winner()));
        s.extend(push_data(&golden_loser()));
        s.extend(push_data(&golden_pot_txid()));
        s.extend(push_data(&[0x33u8; 33]));
        s.extend(push_data(&golden_winner_sig()));
        s.extend(push_data(&golden_loser_sig()));
        assert!(parse_result_marker(&s).is_none());
    }

    #[test]
    fn winner_sig_length_out_of_range_rejected() {
        for len in [0usize, 1, 67, 75, 100] {
            let script = marker_script(
                &[0x11u8; 32],
                &golden_winner(),
                &golden_loser(),
                &golden_pot_txid(),
                &golden_settle_txid(),
                &vec![0x30u8; len],
                &golden_loser_sig(),
            );
            assert!(
                parse_result_marker(&script).is_none(),
                "winnerSig len {len} must be rejected"
            );
        }
    }

    #[test]
    fn loser_sig_length_out_of_range_rejected() {
        // Empty is VALID (unconfirmed); 1 / 67 / 75 / 100 are not.
        for len in [1usize, 67, 75, 100] {
            let script = marker_script(
                &[0x11u8; 32],
                &golden_winner(),
                &golden_loser(),
                &golden_pot_txid(),
                &golden_settle_txid(),
                &golden_winner_sig(),
                &vec![0x30u8; len],
            );
            assert!(
                parse_result_marker(&script).is_none(),
                "loserSig len {len} must be rejected"
            );
        }
    }

    #[test]
    fn missing_pushes_rejected() {
        // Build up push-by-push: every prefix of the eight-push shape (7 or
        // fewer pushes) must be rejected.
        let pushes: Vec<Vec<u8>> = vec![
            push_data(RESULT_TAG),
            push_data(&[0x11u8; 32]),
            push_data(&golden_winner()),
            push_data(&golden_loser()),
            push_data(&golden_pot_txid()),
            push_data(&golden_settle_txid()),
            push_data(&golden_winner_sig()),
        ];
        let mut s = vec![0x00, 0x6a];
        for p in &pushes {
            s.extend_from_slice(p);
            assert!(
                parse_result_marker(&s).is_none(),
                "a marker with fewer than 8 pushes must be rejected"
            );
        }
    }

    #[test]
    fn extra_pushes_rejected() {
        // A ninth push is not the v1 format (exactly eight pushes).
        let mut s = golden_marker(&golden_game_id(), &golden_loser_sig());
        s.extend(push_data(&[0x99u8; 4]));
        assert!(parse_result_marker(&s).is_none());
        // Also on the unconfirmed shape.
        let mut s = golden_marker(&golden_game_id(), &[]);
        s.extend(push_data(&[0x99u8; 4]));
        assert!(parse_result_marker(&s).is_none());
    }

    #[test]
    fn truncated_push_rejected() {
        // Claim a 70-byte loserSig push but truncate the bytes.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(RESULT_TAG));
        s.extend(push_data(&[0x11u8; 32]));
        s.extend(push_data(&golden_winner()));
        s.extend(push_data(&golden_loser()));
        s.extend(push_data(&golden_pot_txid()));
        s.extend(push_data(&golden_settle_txid()));
        s.extend(push_data(&golden_winner_sig()));
        s.push(0x46); // push 70…
        s.extend_from_slice(&[0xcd; 10]); // …but only 10 bytes follow
        assert!(parse_result_marker(&s).is_none());
    }
}
