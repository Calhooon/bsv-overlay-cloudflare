//! `tm_potparty` / `ls_potparty` — LOW by-identity POT PARTICIPATION index
//! (recovery architecture P1, bsv-low #188).
//!
//! A fresh, seed-only LOW client has nothing but its identity key. To
//! recover it must learn WHICH pots it is a party to. Each seat, when it
//! funds (or is funding) a pot, publishes a tiny `OP_RETURN` "potparty"
//! marker under this topic naming ITS identity, the opponent, the game,
//! the pot outpoint, and the pre-signed refund's `recoveryHeight`. A
//! recovering client queries `ls_potparty` (`partyFor`) for its identity
//! and gets back every pot it is in — enough to re-derive keys, pull the
//! funding BEEF (`ls_pot` / `/beef/:txid`), and drive the refund/settle
//! exits.
//!
//! Like `tm_result` / `tm_collected`, this is an `OP_RETURN` data-carrier
//! topic admitted by BYTE FORMAT ONLY — the overlay is an INDEX, not an
//! authority. There is NO server-side signature verification: the marker
//! is admitted on its exact byte shape (plus the structural
//! `identity != opponentIdentity` rule), and its `sig` push is carried
//! back verbatim. A client that cares about authenticity verifies the
//! signature itself; the overlay only preserves the bytes.
//!
//! One structural rule beyond lengths: `identity != opponentIdentity`
//! (byte compare). A self-paired marker is rejected at parse time — a pot
//! is between two DISTINCT seats.
//!
//! # Marker wire format (`LOW/potparty/v1`)
//!
//! `OP_FALSE OP_RETURN` (0x00 0x6a) followed by EXACTLY EIGHT minimal data
//! pushes — byte-identical to the app's builder (the cross-repo CONTRACT,
//! bsv-low #188):
//!
//! | # | Push             | Encoding                                     |
//! |---|------------------|----------------------------------------------|
//! | 0 | tag              | UTF-8 `LOW/potparty/v1` (15 bytes)           |
//! | 1 | identity         | 33 bytes (publishing seat's compressed pubkey)|
//! | 2 | opponentIdentity | 33 bytes (the other seat's compressed pubkey) |
//! | 3 | gameId           | 32 bytes                                     |
//! | 4 | potTxid          | 32 bytes                                     |
//! | 5 | potVout          | 4 bytes little-endian (u32)                  |
//! | 6 | recoveryHeight   | 4 bytes little-endian (u32)                  |
//! | 7 | sig              | DER ECDSA, 68..=74 bytes (preserved, never   |
//! |   |                  | verified by the overlay)                     |
//!
//! The parser validates the shape strictly (exact tag + exact push count +
//! exact push lengths + `identity != opponentIdentity`) and extracts the
//! fields. Wrong tag / wrong lengths / missing or extra pushes / truncated
//! pushes / a self-paired marker → `None` (not a potparty marker).
//!
//! # Lookup (`ls_potparty`)
//!
//! Query JSON (tagged by `type`):
//!
//! ```json
//! {"type": "partyFor", "identity": "<66 hex chars>", "limit": 50}
//! {"type": "byPot", "potTxid": "<64 hex chars>", "potVout": 0}
//! ```
//!
//! `partyFor` answers "which pots is this identity in?"; `byPot` answers
//! "who are the two parties to this pot outpoint?". `limit` is optional
//! (default 100, clamped to 1..=500). The answer is a freeform JSON array,
//! newest first, one entry per stored marker:
//!
//! ```json
//! [{"identity": "<hex>", "opponentIdentity": "<hex>", "gameId": "<hex>",
//!   "potTxid": "<hex>", "potVout": 0, "recoveryHeight": 800000,
//!   "sigHex": "<hex>", "txid": "<hex>", "outputIndex": 0,
//!   "createdAt": 1234567890}]
//! ```

pub mod lookup_service;
pub mod storage;
pub mod topic_manager;

/// The domain tag the app stamps. v1 = `(tag, identity, opponentIdentity,
/// gameId, potTxid, potVout, recoveryHeight, sig)`. 15 bytes of ASCII —
/// the byte layout is the cross-repo CONTRACT (bsv-low #188); never change
/// it without a version bump on both sides.
pub const POTPARTY_TAG: &[u8] = b"LOW/potparty/v1";
/// Number of minimal data pushes in a well-formed v1 marker.
pub const POTPARTY_FIELD_COUNT: usize = 8;
/// identity / opponentIdentity push length (bytes) — a compressed
/// secp256k1 pubkey.
pub const POTPARTY_IDENTITY_KEY_LEN: usize = 33;
/// gameId push length (bytes).
pub const POTPARTY_GAME_ID_LEN: usize = 32;
/// potTxid push length (bytes).
pub const POTPARTY_TXID_LEN: usize = 32;
/// potVout / recoveryHeight push length (bytes) — a little-endian u32.
pub const POTPARTY_U32_LEN: usize = 4;
/// Minimum sig push length (bytes) — a DER ECDSA signature.
pub const POTPARTY_SIG_MIN_LEN: usize = 68;
/// Maximum sig push length (bytes) — a DER ECDSA signature.
pub const POTPARTY_SIG_MAX_LEN: usize = 74;

/// A decoded v1 potparty marker — one seat's "I am a party to this pot"
/// claim, published so a seed-only client can enumerate its pots.
///
/// The overlay only needs `identity` (plus the pot outpoint) to key the
/// index and the raw bytes to hand back to querying clients (which may
/// verify the `sig` themselves — the overlay never does). `opponent` is
/// guaranteed != `identity` (a self-paired marker never parses).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PotpartyMarker {
    /// The publishing seat's compressed identity pubkey (exactly 33 bytes).
    pub identity: Vec<u8>,
    /// The opponent seat's compressed identity pubkey (exactly 33 bytes) —
    /// guaranteed != `identity` (a self-paired marker never parses).
    pub opponent: Vec<u8>,
    /// The 32-byte game id.
    pub game_id: [u8; 32],
    /// The 32-byte pot funding txid.
    pub pot_txid: [u8; 32],
    /// The pot output index within `pot_txid` (u32, little-endian on wire).
    pub pot_vout: u32,
    /// The pre-signed refund's recovery height (u32, little-endian on wire).
    pub recovery_height: u32,
    /// The seat's DER ECDSA signature push (68..=74 bytes) — carried back
    /// verbatim; the overlay NEVER verifies it.
    pub sig: Vec<u8>,
}

/// Walk minimal Bitcoin pushdata out of a byte slice → the pushed blobs,
/// in order, stopping at the first non-push opcode / a truncated push
/// (mirrors `result`'s / `collected`'s `read_pushes`).
///
/// EVERY offset advance uses CHECKED arithmetic. This worker runs on wasm32
/// (`usize = u32`) with wrapping release arithmetic — an OP_PUSHDATA4 length
/// of `0xFFFFFFFF` would make a naive `i + len` WRAP past the bounds guard and
/// panic-trap the topic-manager `/submit` pass on a ~7-byte crafted script.
/// `checked_add` → `None` on overflow → we stop cleanly (a malformed marker is
/// simply skipped, never a trap). Adversarial-review MED, inherited from
/// `result` / `collected`.
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

/// Parse one output locking script as a `LOW/potparty/v1` marker.
///
/// `Some(marker)` IFF the script is `OP_FALSE OP_RETURN` (0x00 0x6a)
/// followed by EXACTLY eight minimal data pushes with the exact shape:
/// tag == [`POTPARTY_TAG`], identity 33 bytes, opponentIdentity 33 bytes,
/// gameId 32 bytes, potTxid 32 bytes, potVout 4 bytes, recoveryHeight 4
/// bytes, sig 68..=74 bytes — AND identity != opponentIdentity (a pot is
/// between two distinct seats). Everything else — a bare `OP_RETURN`, a
/// different tag, a wrong length, extra/missing pushes, a self-paired
/// marker — is `None`.
pub fn parse_potparty_marker(script: &[u8]) -> Option<PotpartyMarker> {
    // OP_FALSE OP_RETURN (0x00 0x6a) — the exact prefix the app's builder
    // emits (the #188 contract pins the two-byte prefix, like #38's).
    if script.len() < 2 || script[0] != 0x00 || script[1] != 0x6a {
        return None;
    }
    let data = read_pushes(&script[2..]);
    if data.len() != POTPARTY_FIELD_COUNT {
        return None;
    }
    if data[0] != POTPARTY_TAG {
        return None;
    }
    let identity_b = data[1];
    let opponent_b = data[2];
    let game_id_b = data[3];
    let pot_txid_b = data[4];
    let pot_vout_b = data[5];
    let recovery_height_b = data[6];
    let sig_b = data[7];

    if identity_b.len() != POTPARTY_IDENTITY_KEY_LEN {
        return None;
    }
    if opponent_b.len() != POTPARTY_IDENTITY_KEY_LEN {
        return None;
    }
    // A pot is between two DISTINCT seats — a self-paired marker never parses.
    if identity_b == opponent_b {
        return None;
    }
    if game_id_b.len() != POTPARTY_GAME_ID_LEN {
        return None;
    }
    if pot_txid_b.len() != POTPARTY_TXID_LEN {
        return None;
    }
    if pot_vout_b.len() != POTPARTY_U32_LEN {
        return None;
    }
    if recovery_height_b.len() != POTPARTY_U32_LEN {
        return None;
    }
    if !(POTPARTY_SIG_MIN_LEN..=POTPARTY_SIG_MAX_LEN).contains(&sig_b.len()) {
        return None;
    }

    let mut game_id = [0u8; 32];
    game_id.copy_from_slice(game_id_b);
    let mut pot_txid = [0u8; 32];
    pot_txid.copy_from_slice(pot_txid_b);
    let pot_vout = u32::from_le_bytes([pot_vout_b[0], pot_vout_b[1], pot_vout_b[2], pot_vout_b[3]]);
    let recovery_height = u32::from_le_bytes([
        recovery_height_b[0],
        recovery_height_b[1],
        recovery_height_b[2],
        recovery_height_b[3],
    ]);
    Some(PotpartyMarker {
        identity: identity_b.to_vec(),
        opponent: opponent_b.to_vec(),
        game_id,
        pot_txid,
        pot_vout,
        recovery_height,
        sig: sig_b.to_vec(),
    })
}

/// True iff `script` is a well-formed `LOW/potparty/v1` marker.
pub fn is_potparty_marker_script(script: &[u8]) -> bool {
    parse_potparty_marker(script).is_some()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Minimal Bitcoin pushdata for a byte blob (direct / OP_PUSHDATA1 /
    /// _2) — mirrors `result`'s test helper.
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

    /// The app's potparty-marker builder in bytes.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn marker_script(
        identity: &[u8],
        opponent: &[u8],
        game_id: &[u8; 32],
        pot_txid: &[u8; 32],
        pot_vout: u32,
        recovery_height: u32,
        sig: &[u8],
    ) -> Vec<u8> {
        let mut s = vec![0x00, 0x6a]; // OP_FALSE OP_RETURN
        s.extend(push_data(POTPARTY_TAG));
        s.extend(push_data(identity));
        s.extend(push_data(opponent));
        s.extend(push_data(game_id));
        s.extend(push_data(pot_txid));
        s.extend(push_data(&pot_vout.to_le_bytes()));
        s.extend(push_data(&recovery_height.to_le_bytes()));
        s.extend(push_data(sig));
        s
    }

    // ── The golden fixtures ───────────────────────────────────────────────
    pub(crate) fn golden_identity() -> Vec<u8> {
        let mut k = vec![0x02u8];
        k.extend_from_slice(&[0xa1u8; 32]);
        k
    }
    pub(crate) fn golden_opponent() -> Vec<u8> {
        let mut k = vec![0x03u8];
        k.extend_from_slice(&[0xb2u8; 32]);
        k
    }
    pub(crate) fn golden_game_id() -> [u8; 32] {
        [0x11u8; 32]
    }
    pub(crate) fn golden_pot_txid() -> [u8; 32] {
        [0x22u8; 32]
    }
    pub(crate) fn golden_vout() -> u32 {
        0
    }
    pub(crate) fn golden_recovery_height() -> u32 {
        850_123
    }
    pub(crate) fn golden_sig() -> Vec<u8> {
        let mut s = vec![0x30u8, 0x45];
        s.extend_from_slice(&[0xabu8; 69]);
        s
    }

    /// A valid marker over the golden identities with chosen gameId / vout.
    pub(crate) fn golden_marker(game_id: &[u8; 32], pot_txid: &[u8; 32], vout: u32) -> Vec<u8> {
        marker_script(
            &golden_identity(),
            &golden_opponent(),
            game_id,
            pot_txid,
            vout,
            golden_recovery_height(),
            &golden_sig(),
        )
    }

    #[test]
    fn tag_is_15_bytes() {
        assert_eq!(POTPARTY_TAG.len(), 15);
        assert_eq!(POTPARTY_TAG, b"LOW/potparty/v1");
    }

    /// The FROZEN cross-repo GOLDEN vector (bsv-low #189). These are the
    /// EXACT `OP_RETURN` bytes the bsv-low CLIENT emits for a potparty marker
    /// built with `PrivateKey(1)` (so identity = the secp256k1 generator
    /// point G, RFC6979-deterministic). The client is the source of truth for
    /// the wire format; this pins the byte contract so a drift on either side
    /// is caught in CI, not on-chain. NEVER regenerate this to match a parser
    /// change — fix the parser to match the client instead.
    const GOLDEN_POTPARTY_HEX: &str = "006a0f4c4f572f706f7470617274792f7631210279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f817982103bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb20cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc20dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd04010000000428a00e00473045022100bf1536ef5a90073214076e91117fa59da158665edfaf3833f438b9cc7cc34fbb0220144732cb31c8d8da2e836ada3f4e69511b8f065016f11feadb30282f12666751";

    #[test]
    fn golden_client_vector_decodes_exactly() {
        let script = hex::decode(GOLDEN_POTPARTY_HEX).expect("golden hex decodes");
        let m = parse_potparty_marker(&script)
            .expect("the client GOLDEN vector MUST parse (wire contract, bsv-low #189)");

        // identity = compressed pubkey for PrivateKey(1) = secp256k1 G.
        assert_eq!(
            hex::encode(&m.identity),
            "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
            "identity must be G (PrivateKey(1) compressed)"
        );
        assert_eq!(m.identity.len(), 33);

        // opponent = 0x03 || 32×0xbb (33 bytes, distinct from identity).
        let mut opponent = vec![0x03u8];
        opponent.extend_from_slice(&[0xbbu8; 32]);
        assert_eq!(m.opponent, opponent);
        assert_eq!(m.opponent.len(), 33);
        assert_ne!(m.identity, m.opponent);

        // gameId = 32×0xcc, potTxid = 32×0xdd.
        assert_eq!(m.game_id, [0xccu8; 32]);
        assert_eq!(m.pot_txid, [0xddu8; 32]);

        // potVout = 1, recoveryHeight = 958504 (both little-endian on wire).
        assert_eq!(m.pot_vout, 1);
        assert_eq!(m.recovery_height, 958_504);

        // sig = the 71-byte DER ECDSA push, carried back verbatim.
        assert_eq!(m.sig.len(), 71);
        assert_eq!(
            hex::encode(&m.sig),
            "3045022100bf1536ef5a90073214076e91117fa59da158665edfaf3833f438b9cc7cc34fbb0220144732cb31c8d8da2e836ada3f4e69511b8f065016f11feadb30282f12666751"
        );

        // And the boolean gate agrees it is a well-formed marker.
        assert!(is_potparty_marker_script(&script));
    }

    #[test]
    fn valid_marker_round_trips() {
        // Build → parse → every field back, over the full sig-length range.
        for sig_len in [68usize, 70, 71, 72, 74] {
            let script = marker_script(
                &golden_identity(),
                &golden_opponent(),
                &golden_game_id(),
                &golden_pot_txid(),
                7,
                golden_recovery_height(),
                &vec![0x30u8; sig_len],
            );
            let m = parse_potparty_marker(&script)
                .unwrap_or_else(|| panic!("sig len {sig_len} must parse"));
            assert_eq!(m.identity, golden_identity());
            assert_eq!(m.identity.len(), 33);
            assert_eq!(m.opponent, golden_opponent());
            assert_eq!(m.opponent.len(), 33);
            assert_eq!(m.game_id, golden_game_id());
            assert_eq!(m.pot_txid, golden_pot_txid());
            assert_eq!(m.pot_vout, 7);
            assert_eq!(m.recovery_height, golden_recovery_height());
            assert_eq!(m.sig.len(), sig_len);
            assert!(is_potparty_marker_script(&script));
        }
    }

    #[test]
    fn le_encoding_of_vout_and_height() {
        // A distinctive value proves LITTLE-endian decode (not big-endian).
        let vout = 0x0403_0201u32; // bytes 01 02 03 04 on the wire
        let height = 0x00CE_9BEFu32; // arbitrary
        let script = marker_script(
            &golden_identity(),
            &golden_opponent(),
            &golden_game_id(),
            &golden_pot_txid(),
            vout,
            height,
            &golden_sig(),
        );
        // Locate the vout push (05 opcode context aside, assert via parse).
        let m = parse_potparty_marker(&script).unwrap();
        assert_eq!(m.pot_vout, vout);
        assert_eq!(m.recovery_height, height);
        // And confirm the raw bytes are LE: find the 4-byte push 01 02 03 04.
        assert!(
            script.windows(4).any(|w| w == [0x01, 0x02, 0x03, 0x04]),
            "vout must be stored little-endian"
        );
    }

    // ── Rejections ────────────────────────────────────────────────────────

    #[test]
    fn self_paired_marker_rejected() {
        // identity == opponent (byte compare) — a pot is between two seats.
        let script = marker_script(
            &golden_identity(),
            &golden_identity(), // opponent slot = the SAME key
            &golden_game_id(),
            &golden_pot_txid(),
            golden_vout(),
            golden_recovery_height(),
            &golden_sig(),
        );
        assert!(parse_potparty_marker(&script).is_none());
    }

    #[test]
    fn wrong_tag_rejected() {
        for bad_tag in [
            b"LOW/potparty/v2".as_slice(), // wrong version
            b"LOW/result/v1".as_slice(),   // a different LOW topic
            b"LOW/potparty/v".as_slice(),  // 14 bytes
            b"SOMETHINGELSE!!".as_slice(), // foreign 15-byte tag
        ] {
            let mut s = vec![0x00, 0x6a];
            s.extend(push_data(bad_tag));
            s.extend(push_data(&golden_identity()));
            s.extend(push_data(&golden_opponent()));
            s.extend(push_data(&golden_game_id()));
            s.extend(push_data(&golden_pot_txid()));
            s.extend(push_data(&golden_vout().to_le_bytes()));
            s.extend(push_data(&golden_recovery_height().to_le_bytes()));
            s.extend(push_data(&golden_sig()));
            assert!(
                parse_potparty_marker(&s).is_none(),
                "tag {bad_tag:?} must be rejected"
            );
        }
    }

    #[test]
    fn non_op_return_rejected() {
        // A standard P2PKH is not an OP_RETURN.
        let p2pkh = hex::decode("76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac").unwrap();
        assert!(parse_potparty_marker(&p2pkh).is_none());
        assert!(parse_potparty_marker(&[]).is_none());
        assert!(parse_potparty_marker(&[0x00]).is_none());
        // A bare OP_RETURN (0x6a without OP_FALSE) is NOT the v1 prefix.
        let script = golden_marker(&golden_game_id(), &golden_pot_txid(), golden_vout());
        assert!(parse_potparty_marker(&script[1..]).is_none());
    }

    #[test]
    fn wrong_identity_key_length_rejected() {
        for len in [32usize, 34, 65] {
            // Bad identity length.
            let script = marker_script(
                &vec![0x02u8; len],
                &golden_opponent(),
                &golden_game_id(),
                &golden_pot_txid(),
                golden_vout(),
                golden_recovery_height(),
                &golden_sig(),
            );
            assert!(
                parse_potparty_marker(&script).is_none(),
                "identity len {len} must be rejected"
            );
            // Bad opponent length.
            let script = marker_script(
                &golden_identity(),
                &vec![0x03u8; len],
                &golden_game_id(),
                &golden_pot_txid(),
                golden_vout(),
                golden_recovery_height(),
                &golden_sig(),
            );
            assert!(
                parse_potparty_marker(&script).is_none(),
                "opponent len {len} must be rejected"
            );
        }
    }

    #[test]
    fn wrong_game_id_or_txid_length_rejected() {
        // gameId 31 bytes.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(POTPARTY_TAG));
        s.extend(push_data(&golden_identity()));
        s.extend(push_data(&golden_opponent()));
        s.extend(push_data(&[0x11u8; 31]));
        s.extend(push_data(&golden_pot_txid()));
        s.extend(push_data(&golden_vout().to_le_bytes()));
        s.extend(push_data(&golden_recovery_height().to_le_bytes()));
        s.extend(push_data(&golden_sig()));
        assert!(parse_potparty_marker(&s).is_none());
        // potTxid 33 bytes.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(POTPARTY_TAG));
        s.extend(push_data(&golden_identity()));
        s.extend(push_data(&golden_opponent()));
        s.extend(push_data(&golden_game_id()));
        s.extend(push_data(&[0x22u8; 33]));
        s.extend(push_data(&golden_vout().to_le_bytes()));
        s.extend(push_data(&golden_recovery_height().to_le_bytes()));
        s.extend(push_data(&golden_sig()));
        assert!(parse_potparty_marker(&s).is_none());
    }

    #[test]
    fn wrong_u32_length_rejected() {
        // potVout as a 3-byte push.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(POTPARTY_TAG));
        s.extend(push_data(&golden_identity()));
        s.extend(push_data(&golden_opponent()));
        s.extend(push_data(&golden_game_id()));
        s.extend(push_data(&golden_pot_txid()));
        s.extend(push_data(&[0x01u8, 0x02, 0x03])); // 3 bytes, not 4
        s.extend(push_data(&golden_recovery_height().to_le_bytes()));
        s.extend(push_data(&golden_sig()));
        assert!(parse_potparty_marker(&s).is_none());
        // recoveryHeight as a 5-byte push.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(POTPARTY_TAG));
        s.extend(push_data(&golden_identity()));
        s.extend(push_data(&golden_opponent()));
        s.extend(push_data(&golden_game_id()));
        s.extend(push_data(&golden_pot_txid()));
        s.extend(push_data(&golden_vout().to_le_bytes()));
        s.extend(push_data(&[0x01u8, 0x02, 0x03, 0x04, 0x05])); // 5 bytes
        s.extend(push_data(&golden_sig()));
        assert!(parse_potparty_marker(&s).is_none());
    }

    #[test]
    fn sig_length_out_of_range_rejected() {
        for len in [0usize, 1, 67, 75, 100] {
            let script = marker_script(
                &golden_identity(),
                &golden_opponent(),
                &golden_game_id(),
                &golden_pot_txid(),
                golden_vout(),
                golden_recovery_height(),
                &vec![0x30u8; len],
            );
            assert!(
                parse_potparty_marker(&script).is_none(),
                "sig len {len} must be rejected"
            );
        }
    }

    #[test]
    fn missing_pushes_rejected() {
        // Every prefix of the eight-push shape (7 or fewer pushes) rejected.
        let pushes: Vec<Vec<u8>> = vec![
            push_data(POTPARTY_TAG),
            push_data(&golden_identity()),
            push_data(&golden_opponent()),
            push_data(&golden_game_id()),
            push_data(&golden_pot_txid()),
            push_data(&golden_vout().to_le_bytes()),
            push_data(&golden_recovery_height().to_le_bytes()),
        ];
        let mut s = vec![0x00, 0x6a];
        for p in &pushes {
            s.extend_from_slice(p);
            assert!(
                parse_potparty_marker(&s).is_none(),
                "a marker with fewer than 8 pushes must be rejected"
            );
        }
    }

    #[test]
    fn extra_pushes_rejected() {
        // A ninth push is not the v1 format (exactly eight pushes).
        let mut s = golden_marker(&golden_game_id(), &golden_pot_txid(), golden_vout());
        s.extend(push_data(&[0x99u8; 4]));
        assert!(parse_potparty_marker(&s).is_none());
    }

    #[test]
    fn truncated_push_rejected() {
        // Claim a 70-byte sig push but truncate the bytes.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(POTPARTY_TAG));
        s.extend(push_data(&golden_identity()));
        s.extend(push_data(&golden_opponent()));
        s.extend(push_data(&golden_game_id()));
        s.extend(push_data(&golden_pot_txid()));
        s.extend(push_data(&golden_vout().to_le_bytes()));
        s.extend(push_data(&golden_recovery_height().to_le_bytes()));
        s.push(0x46); // push 70…
        s.extend_from_slice(&[0xab; 10]); // …but only 10 bytes follow
        assert!(parse_potparty_marker(&s).is_none());
    }

    #[test]
    fn adversarial_pushdata_len_never_panics_or_wraps() {
        // Inherited from result/collected: an OP_PUSHDATA4 (0x4e) with len
        // 0xFFFFFFFF on wasm32 (usize=u32) would wrap `i + len` past the
        // bounds guard → slice panic → topic-manager /submit trap. The
        // crafted script must parse to None, never panic.
        for script in [
            vec![0x00u8, 0x6a, 0x4e, 0xff, 0xff, 0xff, 0xff],
            vec![0x00u8, 0x6a, 0x4d, 0xff, 0xff],
            vec![0x00u8, 0x6a, 0x4e, 0xff, 0xff, 0xff],
            vec![0x00u8, 0x6a, 0x4b],
        ] {
            assert_eq!(parse_potparty_marker(&script), None);
        }
        assert!(read_pushes(&[0x4e, 0xff, 0xff, 0xff, 0xff]).is_empty());
        assert!(read_pushes(&[0x4d, 0xff, 0xff]).is_empty());
    }
}
