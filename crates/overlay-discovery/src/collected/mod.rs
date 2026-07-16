//! `tm_collected` / `ls_collected` — LOW cross-device "already collected"
//! markers (bsv-low #161).
//!
//! When a LOW device successfully collects a credit (a landing-proof-gated
//! internalize succeeds), it publishes a tiny owner-signed `OP_RETURN`
//! "collected" marker under this topic. Other devices of the SAME identity
//! query `ls_collected` during the home/History card gather; a marker they
//! can VERIFY was signed by their own identity flips the card to
//! "collected on another device" instead of offering Collect.
//!
//! Like `tm_reveal`, this is an `OP_RETURN` data-carrier topic admitted by
//! BYTE FORMAT ONLY — the overlay performs NO server-side signature check.
//! The security lives in the CLIENT verify (which holds the wallet): a
//! device trusts a marker only after `wallet.verifySignature` validates the
//! carried sig under its OWN identity's derived key
//! (`COLLECTED_PROTOCOL = [1,'low collected']`, keyID = gameId,
//! counterparty = 'self'). A forged or foreign-identity marker is ignored
//! client-side, and the marker is a UI HINT only — it never gates a credit
//! (the fail-safe is always toward SHOWING the Collect card).
//!
//! # Marker wire format (`LOW/collected/v1`)
//!
//! `OP_FALSE OP_RETURN` (0x00 0x6a) followed by four minimal data pushes —
//! byte-identical to the app's `collectedMarkerScriptHex`
//! (`app/src/lib/stake.ts`):
//!
//! | # | Push        | Encoding                                          |
//! |---|-------------|---------------------------------------------------|
//! | 0 | tag         | UTF-8 `LOW/collected/v1` (16 bytes)               |
//! | 1 | gameId      | 32 bytes                                          |
//! | 2 | identityKey | 33 bytes (compressed identity pubkey)             |
//! | 3 | sig         | DER ECDSA signature, 68..=74 bytes                |
//!
//! The parser validates this shape strictly (exact tag + exact push
//! lengths + exactly four pushes) and extracts
//! `(gameId, identityKey, sig)`. Wrong tag / wrong lengths / extra or
//! missing pushes → `None` (not a v1 collected marker).
//!
//! # Lookup (`ls_collected`)
//!
//! Query JSON (tagged by `type`):
//!
//! ```json
//! {"type": "collectedFor", "identity": "<66 hex chars>",
//!  "gameIds": ["<64 hex chars>", "..."]}
//! ```
//!
//! The answer is a freeform, input-ordered JSON array — one entry per
//! requested gameId:
//!
//! ```json
//! [{"gameId": "<hex>", "identity": "<hex>", "txid": "<hex|null>",
//!   "sigHex": "<hex|null>", "present": true}]
//! ```
//!
//! A `(identity, gameId)` with no stored marker answers
//! `{"present": false, "txid": null, "sigHex": null}` — fail-safe: an
//! absent marker means "still offer Collect", never a hidden card.

pub mod lookup_service;
pub mod storage;
pub mod topic_manager;

/// The domain tag the app stamps (`COLLECTED_TAG` in
/// `app/src/lib/stake.ts`). v1 = `(tag, gameId, identityKey, sig)`.
/// 16 bytes of ASCII — the byte layout is the cross-repo CONTRACT
/// (bsv-low #161); never change it without a version bump on both sides.
pub const COLLECTED_TAG: &[u8] = b"LOW/collected/v1";
/// Number of minimal data pushes in a well-formed v1 marker.
pub const COLLECTED_FIELD_COUNT: usize = 4;
/// gameId push length (bytes).
pub const COLLECTED_GAME_ID_LEN: usize = 32;
/// identityKey push length (bytes) — a compressed secp256k1 pubkey.
pub const COLLECTED_IDENTITY_KEY_LEN: usize = 33;
/// Minimum sig push length (bytes) — a DER ECDSA signature.
pub const COLLECTED_SIG_MIN_LEN: usize = 68;
/// Maximum sig push length (bytes) — a DER ECDSA signature.
pub const COLLECTED_SIG_MAX_LEN: usize = 74;

/// A decoded v1 collected marker — one identity's "I collected this game's
/// credit" fact published on-chain.
///
/// The overlay only needs `(identity_key, game_id)` to key the index plus
/// the raw `sig` bytes to hand back to querying clients (which verify it
/// under their own wallet — the overlay never does).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectedMarker {
    /// The 32-byte game id.
    pub game_id: [u8; 32],
    /// The publisher's compressed identity pubkey (exactly 33 bytes).
    pub identity_key: Vec<u8>,
    /// The DER ECDSA signature push (68..=74 bytes) — verified CLIENT-side
    /// only, under `[1,'low collected']` / keyID = gameId / self.
    pub sig: Vec<u8>,
}

/// Walk minimal Bitcoin pushdata out of a byte slice → the pushed blobs,
/// in order, stopping at the first non-push opcode / a truncated push
/// (mirrors the app's `readPushes` and `reveal`'s `read_pushes`).
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
                if i.checked_add(2).map_or(true, |e| e > bytes.len()) {
                    return out;
                }
                let l = bytes[i] as usize | ((bytes[i + 1] as usize) << 8);
                i += 2;
                l
            }
            0x4e => {
                if i.checked_add(4).map_or(true, |e| e > bytes.len()) {
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

/// Parse one output locking script as a `LOW/collected/v1` marker.
///
/// `Some(marker)` IFF the script is `OP_FALSE OP_RETURN` (0x00 0x6a)
/// followed by EXACTLY four minimal data pushes with the exact v1 shape:
/// tag == [`COLLECTED_TAG`] (16 bytes), gameId 32 bytes, identityKey
/// 33 bytes, sig 68..=74 bytes. Everything else — a bare `OP_RETURN`, a
/// different tag, a wrong length, extra/missing pushes — is `None`.
///
/// Deliberately Option (not the reveal parser's three-way Result): the
/// admit decision is binary "is this the exact v1 byte format?" and a
/// tagged-but-malformed script is simply not admitted.
pub fn parse_collected_marker(script: &[u8]) -> Option<CollectedMarker> {
    // OP_FALSE OP_RETURN (0x00 0x6a) — the exact prefix the app's builder
    // emits (stricter than reveal, which also accepts a bare OP_RETURN;
    // the #161 contract pins the two-byte prefix).
    if script.len() < 2 || script[0] != 0x00 || script[1] != 0x6a {
        return None;
    }
    let data = read_pushes(&script[2..]);

    // Exactly four pushes, exact lengths, exact tag.
    if data.len() != COLLECTED_FIELD_COUNT {
        return None;
    }
    let (tag, game_id_b, identity_key_b, sig_b) = (data[0], data[1], data[2], data[3]);
    if tag != COLLECTED_TAG {
        return None;
    }
    if game_id_b.len() != COLLECTED_GAME_ID_LEN {
        return None;
    }
    if identity_key_b.len() != COLLECTED_IDENTITY_KEY_LEN {
        return None;
    }
    if !(COLLECTED_SIG_MIN_LEN..=COLLECTED_SIG_MAX_LEN).contains(&sig_b.len()) {
        return None;
    }

    let mut game_id = [0u8; 32];
    game_id.copy_from_slice(game_id_b);
    Some(CollectedMarker {
        game_id,
        identity_key: identity_key_b.to_vec(),
        sig: sig_b.to_vec(),
    })
}

/// True iff `script` is a well-formed `LOW/collected/v1` marker.
pub fn is_collected_marker_script(script: &[u8]) -> bool {
    parse_collected_marker(script).is_some()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Minimal Bitcoin pushdata for a byte blob (direct / OP_PUSHDATA1 /
    /// _2) — mirrors the app's `pushData` and reveal's test helper.
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

    /// The app's `collectedMarkerScriptHex` in bytes.
    pub(crate) fn marker_script(game_id: &[u8; 32], identity_key: &[u8], sig: &[u8]) -> Vec<u8> {
        let mut s = vec![0x00, 0x6a]; // OP_FALSE OP_RETURN
        s.extend(push_data(COLLECTED_TAG));
        s.extend(push_data(game_id));
        s.extend(push_data(identity_key));
        s.extend(push_data(sig));
        s
    }

    /// The GOLDEN VECTOR from the #161 spec — the exact 158-byte script hex
    /// BOTH sides (the app's `collectedMarkerScriptHex` builder and this
    /// parser) must agree on. Inputs: tag=`LOW/collected/v1`,
    /// gameId=`11`×32, identityKey=`02`+`a1`×32, sig=`30 45`+`ab`×69
    /// (71 bytes). The same fixed hex is asserted in the client test suite.
    pub(crate) const GOLDEN_MARKER_HEX: &str = "006a104c4f572f636f6c6c65637465642f76312011111111111111111111111111111111111111111111111111111111111111112102a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1473045ababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab";

    /// The golden vector's expected fields.
    pub(crate) fn golden_game_id() -> [u8; 32] {
        [0x11u8; 32]
    }
    pub(crate) fn golden_identity_key() -> Vec<u8> {
        let mut k = vec![0x02u8];
        k.extend_from_slice(&[0xa1u8; 32]);
        k
    }
    pub(crate) fn golden_sig() -> Vec<u8> {
        let mut s = vec![0x30u8, 0x45];
        s.extend_from_slice(&[0xabu8; 69]);
        s
    }

    // ── The golden interface vector (the cross-repo CONTRACT) ────────────

    #[test]
    fn golden_vector_parses_exactly() {
        let script = hex::decode(GOLDEN_MARKER_HEX).expect("golden hex decodes");
        assert_eq!(script.len(), 158, "golden vector is exactly 158 bytes");

        let m = parse_collected_marker(&script).expect("golden vector must parse");
        assert_eq!(m.game_id, golden_game_id());
        assert_eq!(m.identity_key, golden_identity_key());
        assert_eq!(m.identity_key.len(), 33);
        assert_eq!(m.sig, golden_sig());
        assert_eq!(m.sig.len(), 71);
        assert!(is_collected_marker_script(&script));
    }

    #[test]
    fn builder_reproduces_the_golden_vector() {
        // The test-side builder (mirroring the app's) must PRODUCE the exact
        // golden hex for the golden inputs — round-trip both directions.
        let script = marker_script(&golden_game_id(), &golden_identity_key(), &golden_sig());
        assert_eq!(hex::encode(&script), GOLDEN_MARKER_HEX);
    }

    #[test]
    fn tag_is_16_bytes() {
        assert_eq!(COLLECTED_TAG.len(), 16);
        assert_eq!(COLLECTED_TAG, b"LOW/collected/v1");
    }

    #[test]
    fn adversarial_pushdata_len_never_panics_or_wraps() {
        // Adversarial-review MED: an OP_PUSHDATA4 (0x4e) with len 0xFFFFFFFF on
        // wasm32 (usize=u32) would wrap `i + len` past the bounds guard → slice
        // panic → topic-manager /submit trap. The crafted ~7-byte script must
        // parse to None (no marker), never panic. Also probe OP_PUSHDATA2 and a
        // truncated push. (parse_collected_marker skips the 006a prefix; call
        // read_pushes semantics via the full parser so the guard is exercised.)
        for script in [
            vec![0x00u8, 0x6a, 0x4e, 0xff, 0xff, 0xff, 0xff], // PUSHDATA4 max len, no data
            vec![0x00u8, 0x6a, 0x4d, 0xff, 0xff],             // PUSHDATA2 max len, no data
            vec![0x00u8, 0x6a, 0x4e, 0xff, 0xff, 0xff],       // PUSHDATA4 header truncated
            vec![0x00u8, 0x6a, 0x4b],                         // a 75-byte push with no data
        ] {
            assert_eq!(parse_collected_marker(&script), None, "crafted script must not parse");
        }
        // Direct read_pushes probe: the trap path is the len itself.
        assert!(read_pushes(&[0x4e, 0xff, 0xff, 0xff, 0xff]).is_empty());
        assert!(read_pushes(&[0x4d, 0xff, 0xff]).is_empty());
    }

    // ── Valid markers ─────────────────────────────────────────────────────

    #[test]
    fn valid_marker_parses() {
        for sig_len in [68usize, 70, 71, 72, 74] {
            let script = marker_script(&[0xABu8; 32], &golden_identity_key(), &vec![0x30; sig_len]);
            let m = parse_collected_marker(&script)
                .unwrap_or_else(|| panic!("sig len {sig_len} must parse"));
            assert_eq!(m.game_id, [0xABu8; 32]);
            assert_eq!(m.sig.len(), sig_len);
        }
    }

    // ── Rejections ────────────────────────────────────────────────────────

    #[test]
    fn wrong_tag_rejected() {
        for bad_tag in [
            b"LOW/collected/v2".as_slice(), // wrong version
            b"LOW/reveal/v2".as_slice(),    // a different LOW topic
            b"SOMETHINGELSE!!!".as_slice(), // foreign 16-byte tag
            b"LOW/collected/v".as_slice(),  // 15 bytes
        ] {
            let mut s = vec![0x00, 0x6a];
            s.extend(push_data(bad_tag));
            s.extend(push_data(&[0x11u8; 32]));
            s.extend(push_data(&golden_identity_key()));
            s.extend(push_data(&golden_sig()));
            assert!(
                parse_collected_marker(&s).is_none(),
                "tag {bad_tag:?} must be rejected"
            );
        }
    }

    #[test]
    fn non_op_return_rejected() {
        // A standard P2PKH is not an OP_RETURN.
        let p2pkh = hex::decode("76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac").unwrap();
        assert!(parse_collected_marker(&p2pkh).is_none());
        // Empty / one-byte scripts.
        assert!(parse_collected_marker(&[]).is_none());
        assert!(parse_collected_marker(&[0x00]).is_none());
        // A bare OP_RETURN (0x6a without OP_FALSE) is NOT the v1 prefix.
        let script = marker_script(&golden_game_id(), &golden_identity_key(), &golden_sig());
        assert!(parse_collected_marker(&script[1..]).is_none());
    }

    #[test]
    fn short_game_id_rejected() {
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(COLLECTED_TAG));
        s.extend(push_data(&[0x11u8; 31])); // 31, not 32
        s.extend(push_data(&golden_identity_key()));
        s.extend(push_data(&golden_sig()));
        assert!(parse_collected_marker(&s).is_none());
    }

    #[test]
    fn wrong_identity_key_length_rejected() {
        for len in [32usize, 34, 65] {
            let script = marker_script(&[0x11u8; 32], &vec![0x02u8; len], &golden_sig());
            assert!(
                parse_collected_marker(&script).is_none(),
                "identityKey len {len} must be rejected"
            );
        }
    }

    #[test]
    fn sig_length_out_of_range_rejected() {
        for len in [0usize, 1, 67, 75, 100] {
            let script = marker_script(&[0x11u8; 32], &golden_identity_key(), &vec![0x30u8; len]);
            assert!(
                parse_collected_marker(&script).is_none(),
                "sig len {len} must be rejected"
            );
        }
    }

    #[test]
    fn missing_pushes_rejected() {
        // tag only
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(COLLECTED_TAG));
        assert!(parse_collected_marker(&s).is_none());
        // tag + gameId, no identityKey/sig
        s.extend(push_data(&[0x11u8; 32]));
        assert!(parse_collected_marker(&s).is_none());
        // tag + gameId + identityKey, no sig
        s.extend(push_data(&golden_identity_key()));
        assert!(parse_collected_marker(&s).is_none());
    }

    #[test]
    fn extra_pushes_rejected() {
        // A fifth push is not the v1 format (exactly four pushes).
        let mut s = marker_script(&golden_game_id(), &golden_identity_key(), &golden_sig());
        s.extend(push_data(&[0x99u8; 4]));
        assert!(parse_collected_marker(&s).is_none());
    }

    #[test]
    fn truncated_push_rejected() {
        // Claim a 71-byte sig push but truncate the bytes.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(COLLECTED_TAG));
        s.extend(push_data(&[0x11u8; 32]));
        s.extend(push_data(&golden_identity_key()));
        s.push(0x47); // push 71…
        s.extend_from_slice(&[0xab; 10]); // …but only 10 bytes follow
        assert!(parse_collected_marker(&s).is_none());
    }
}
