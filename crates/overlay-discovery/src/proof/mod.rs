//! `tm_proof` / `ls_proof` — LOW rung-3 transcript-proof bundle carrier
//! (bsv-low leaderboard verification ladder, rung 3:
//! `docs/DESIGN-rung3-transcript-proven-hands.md`).
//!
//! The leaderboard's verification ladder is (1) countersigned →
//! (2) covenant-anchored → (3) TRANSCRIPT-PROVEN. After a winner's
//! `LOW/result/v2` claim, it publishes a `LOW/proof/v1` marker carrying
//! the canonical JSON proof bundle — the signed envelopes (blind scalar
//! commitments, final masked deck d4, mask commits/reveals, the winner's
//! reveal and the loser's released keys) from which a claim's five cards
//! are provable from the game's cryptographic transcript itself, not
//! from "both wallets said so".
//!
//! Like `tm_reveal` / `tm_collected` / `tm_result`, this is an
//! `OP_RETURN` data-carrier topic admitted by BYTE FORMAT ONLY — the
//! overlay performs NO content validation of the bundle JSON and NO
//! signature verification. The CLIENT verifies the transcript
//! cryptography (envelope sigs, scalar-commitment openings, unmasking —
//! all wasm-exported) and the winner's identity signature over the
//! canonical challenge ('anyone' ProtoWallet round-trip); a bundle that
//! fails any check simply earns no badge (the claim stays merely
//! countersigned — never hidden, never upgraded). The overlay is an
//! INDEX: bytes in, bytes out.
//!
//! What rung 3 can NEVER prove (locked no-VDF decision, accepted
//! residual): a single person holding BOTH seats controls the joint
//! shuffle end-to-end — no transcript check defeats self-play.
//!
//! # Marker wire format (`LOW/proof/v1`)
//!
//! `OP_FALSE OP_RETURN` (0x00 0x6a) followed by EXACTLY FIVE minimal
//! data pushes — byte-identical to the app's builder (the cross-repo
//! CONTRACT):
//!
//! | # | Push           | Encoding                                       |
//! |---|----------------|------------------------------------------------|
//! | 0 | tag            | UTF-8 `LOW/proof/v1` (12 bytes)                |
//! | 1 | gameId         | 32 bytes                                       |
//! | 2 | winnerIdentity | 33 bytes (compressed pubkey)                   |
//! | 3 | sig            | DER ECDSA, 68..=74 bytes — the winner's        |
//! |   |                | identity signature over the canonical          |
//! |   |                | challenge (client-verified only)               |
//! | 4 | bundle         | the canonical JSON proof bundle bytes,         |
//! |   |                | 1..=65536 bytes (big pushes use OP_PUSHDATA2)  |
//!
//! The parser validates the shape strictly (exact tag + exact push
//! lengths for 1–3 + the bundle length range + exactly five pushes) and
//! extracts the fields. Wrong tag / wrong lengths / missing or extra
//! pushes / truncated pushes / an empty or oversized bundle → `None`.
//!
//! # Lookup (`ls_proof`)
//!
//! Query JSON (tagged by `type`):
//!
//! ```json
//! {"type": "proofsFor", "gameId": "<64 hex chars>",
//!  "winner": "<66 hex chars>", "limit": 3}
//! ```
//!
//! `limit` is optional (default 3, clamped to 1..=10 — bundles are
//! ~10–15 KB each). The answer is a freeform JSON array, newest first:
//!
//! ```json
//! [{"gameId": "<hex>", "winner": "<hex>", "sigHex": "<hex>",
//!   "bundleBase64": "<base64>", "txid": "<hex>", "outputIndex": 0,
//!   "createdAt": 1234567890}]
//! ```
//!
//! The index keeps EVERY admitted marker, keyed by its outpoint — the
//! same `(gameId, winner)` may return multiple rows (the tm_result
//! censorship lesson applies identically: first-marker-wins would let a
//! garbage bundle front-run the real proof for one OP_RETURN fee).
//! Clients verify each bundle and use the one that proves.

pub mod lookup_service;
pub mod storage;
pub mod topic_manager;

/// The domain tag the app stamps. v1 = `(tag, gameId, winnerIdentity,
/// sig, bundle)`. 12 bytes of ASCII — the byte layout is the cross-repo
/// CONTRACT; never change it without a version bump on both sides.
pub const PROOF_TAG: &[u8] = b"LOW/proof/v1";
/// Number of minimal data pushes in a well-formed v1 marker.
pub const PROOF_FIELD_COUNT: usize = 5;
/// gameId push length (bytes).
pub const PROOF_GAME_ID_LEN: usize = 32;
/// winnerIdentity push length (bytes) — a compressed secp256k1 pubkey.
pub const PROOF_IDENTITY_KEY_LEN: usize = 33;
/// Minimum sig push length (bytes) — a DER ECDSA signature.
pub const PROOF_SIG_MIN_LEN: usize = 68;
/// Maximum sig push length (bytes) — a DER ECDSA signature.
pub const PROOF_SIG_MAX_LEN: usize = 74;
/// Minimum bundle push length (bytes) — an empty bundle proves nothing.
pub const PROOF_BUNDLE_MIN_LEN: usize = 1;
/// Maximum bundle push length (bytes). Real bundles run ~10–15 KB; 64 KiB
/// is a generous format ceiling that still bounds index row size.
pub const PROOF_BUNDLE_MAX_LEN: usize = 65536;

/// A decoded v1 proof marker — one winner's transcript-proof bundle for
/// one settled hand.
///
/// The overlay only needs `(game_id, winner)` to index plus the raw
/// `sig` and `bundle` bytes to hand back verbatim to querying clients
/// (which verify the identity sig AND the transcript cryptography — the
/// overlay never does).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProofMarker {
    /// The 32-byte game id.
    pub game_id: [u8; 32],
    /// The winner's compressed identity pubkey (exactly 33 bytes).
    pub winner: Vec<u8>,
    /// The winner's DER ECDSA identity signature over the canonical
    /// challenge (68..=74 bytes) — verified CLIENT-side only.
    pub sig: Vec<u8>,
    /// The canonical JSON proof bundle bytes, verbatim
    /// (1..=65536 bytes). NOT validated here — the client verifies the
    /// transcript cryptography.
    pub bundle: Vec<u8>,
}

/// Walk minimal Bitcoin pushdata out of a byte slice → the pushed blobs,
/// in order, stopping at the first non-push opcode / a truncated push
/// (mirrors the app's `readPushes` and `result`'s `read_pushes`).
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

/// Parse one output locking script as a `LOW/proof/v1` marker.
///
/// `Some(marker)` IFF the script is `OP_FALSE OP_RETURN` (0x00 0x6a)
/// followed by EXACTLY five minimal data pushes with the exact v1 shape:
/// tag == [`PROOF_TAG`] (12 bytes), gameId 32 bytes, winnerIdentity 33
/// bytes, sig 68..=74 bytes, bundle 1..=65536 bytes. The bundle content
/// is NOT validated (byte carrier — the CLIENT verifies the transcript
/// cryptography). Everything else — a bare `OP_RETURN`, a different tag,
/// a wrong length, extra/missing pushes — is `None`.
///
/// Deliberately Option (not the reveal parser's three-way Result): the
/// admit decision is binary "is this the exact v1 byte format?" and a
/// tagged-but-malformed script is simply not admitted.
pub fn parse_proof_marker(script: &[u8]) -> Option<ProofMarker> {
    // OP_FALSE OP_RETURN (0x00 0x6a) — the exact prefix the app's builder
    // emits (the contract pins the two-byte prefix, like #161's/#38's).
    if script.len() < 2 || script[0] != 0x00 || script[1] != 0x6a {
        return None;
    }
    let data = read_pushes(&script[2..]);

    // Exactly five pushes, exact lengths, exact tag.
    if data.len() != PROOF_FIELD_COUNT {
        return None;
    }
    let (tag, game_id_b, winner_b, sig_b, bundle_b) = (data[0], data[1], data[2], data[3], data[4]);
    if tag != PROOF_TAG {
        return None;
    }
    if game_id_b.len() != PROOF_GAME_ID_LEN {
        return None;
    }
    if winner_b.len() != PROOF_IDENTITY_KEY_LEN {
        return None;
    }
    if !(PROOF_SIG_MIN_LEN..=PROOF_SIG_MAX_LEN).contains(&sig_b.len()) {
        return None;
    }
    if !(PROOF_BUNDLE_MIN_LEN..=PROOF_BUNDLE_MAX_LEN).contains(&bundle_b.len()) {
        return None;
    }

    let mut game_id = [0u8; 32];
    game_id.copy_from_slice(game_id_b);
    Some(ProofMarker {
        game_id,
        winner: winner_b.to_vec(),
        sig: sig_b.to_vec(),
        bundle: bundle_b.to_vec(),
    })
}

/// True iff `script` is a well-formed `LOW/proof/v1` marker.
pub fn is_proof_marker_script(script: &[u8]) -> bool {
    parse_proof_marker(script).is_some()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Minimal Bitcoin pushdata for a byte blob (direct / OP_PUSHDATA1 /
    /// _2 / _4) — mirrors the app's `pushData` and result's test helper.
    /// A bundle over 255 bytes emits the OP_PUSHDATA2 (0x4d) form; only
    /// the 65536-byte format ceiling itself needs OP_PUSHDATA4 (0x4e,
    /// PUSHDATA2 tops out at 65535).
    pub(crate) fn push_data(blob: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        let len = blob.len();
        if len < 0x4c {
            out.push(len as u8);
        } else if len <= 0xff {
            out.push(0x4c);
            out.push(len as u8);
        } else if len <= 0xffff {
            out.push(0x4d);
            out.push((len & 0xff) as u8);
            out.push(((len >> 8) & 0xff) as u8);
        } else {
            out.push(0x4e);
            out.push((len & 0xff) as u8);
            out.push(((len >> 8) & 0xff) as u8);
            out.push(((len >> 16) & 0xff) as u8);
            out.push(((len >> 24) & 0xff) as u8);
        }
        out.extend_from_slice(blob);
        out
    }

    /// The app's proof-marker builder in bytes.
    pub(crate) fn marker_script(
        game_id: &[u8; 32],
        winner: &[u8],
        sig: &[u8],
        bundle: &[u8],
    ) -> Vec<u8> {
        let mut s = vec![0x00, 0x6a]; // OP_FALSE OP_RETURN
        s.extend(push_data(PROOF_TAG));
        s.extend(push_data(game_id));
        s.extend(push_data(winner));
        s.extend(push_data(sig));
        s.extend(push_data(bundle));
        s
    }

    /// The GOLDEN VECTOR — the exact 174-byte script hex BOTH sides (the
    /// app's builder and this parser) must agree on. Inputs:
    /// tag=`LOW/proof/v1`, gameId=`11`×32, winner=`02`+`a1`×32,
    /// sig=`30 45`+`ab`×69 (71 bytes), bundle = the UTF-8 bytes of
    /// exactly `{"v":1,"test":true}` (19 bytes). The same fixed hex is
    /// asserted in the client test suite.
    pub(crate) const GOLDEN_PROOF_HEX: &str = "006a0c4c4f572f70726f6f662f76312011111111111111111111111111111111111111111111111111111111111111112102a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1473045ababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababababab137b2276223a312c2274657374223a747275657d";

    /// The golden vector's expected fields.
    pub(crate) fn golden_game_id() -> [u8; 32] {
        [0x11u8; 32]
    }
    pub(crate) fn golden_winner() -> Vec<u8> {
        let mut k = vec![0x02u8];
        k.extend_from_slice(&[0xa1u8; 32]);
        k
    }
    pub(crate) fn golden_sig() -> Vec<u8> {
        let mut s = vec![0x30u8, 0x45];
        s.extend_from_slice(&[0xabu8; 69]);
        s
    }
    pub(crate) fn golden_bundle() -> Vec<u8> {
        br#"{"v":1,"test":true}"#.to_vec()
    }

    /// A valid marker script over the golden identity with a chosen
    /// gameId + bundle — the common test shorthand.
    pub(crate) fn golden_marker(game_id: &[u8; 32], bundle: &[u8]) -> Vec<u8> {
        marker_script(game_id, &golden_winner(), &golden_sig(), bundle)
    }

    // ── The golden interface vector (the cross-repo CONTRACT) ────────────

    #[test]
    fn golden_vector_parses_exactly() {
        let script = hex::decode(GOLDEN_PROOF_HEX).expect("golden hex decodes");
        assert_eq!(script.len(), 174, "golden vector is exactly 174 bytes");

        let m = parse_proof_marker(&script).expect("golden vector must parse");
        assert_eq!(m.game_id, golden_game_id());
        assert_eq!(m.winner, golden_winner());
        assert_eq!(m.winner.len(), 33);
        assert_eq!(m.sig, golden_sig());
        assert_eq!(m.sig.len(), 71);
        assert_eq!(m.bundle, golden_bundle());
        assert_eq!(m.bundle.len(), 19);
        assert_eq!(m.bundle, b"{\"v\":1,\"test\":true}");
        assert!(is_proof_marker_script(&script));
    }

    #[test]
    fn builder_reproduces_the_golden_vector() {
        // The test-side builder (mirroring the app's) must PRODUCE the exact
        // golden hex for the golden inputs — round-trip both directions.
        let script = golden_marker(&golden_game_id(), &golden_bundle());
        assert_eq!(hex::encode(&script), GOLDEN_PROOF_HEX);
    }

    #[test]
    fn tag_is_12_bytes() {
        assert_eq!(PROOF_TAG.len(), 12);
        assert_eq!(PROOF_TAG, b"LOW/proof/v1");
    }

    #[test]
    fn big_bundle_pushdata2_roundtrips() {
        // A realistic transcript bundle is ~10–15 KB; the builder emits an
        // OP_PUSHDATA2 push (0x4d, little-endian length). A 20,000-byte
        // bundle must round-trip byte-for-byte.
        let bundle: Vec<u8> = (0..20_000u32).map(|i| (i % 251) as u8).collect();
        let script = golden_marker(&golden_game_id(), &bundle);
        // The bundle push header is 0x4d 0x20 0x4e (20000 LE).
        let bundle_push_at = script.len() - 20_000 - 3;
        assert_eq!(&script[bundle_push_at..bundle_push_at + 3], &[0x4d, 0x20, 0x4e]);

        let m = parse_proof_marker(&script).expect("20 KB bundle must parse");
        assert_eq!(m.bundle.len(), 20_000);
        assert_eq!(m.bundle, bundle, "bundle bytes round-trip verbatim");
    }

    #[test]
    fn bundle_boundary_lengths() {
        // 1 byte (minimum) and 65536 bytes (maximum) parse; 65537 does not.
        let m = parse_proof_marker(&golden_marker(&golden_game_id(), &[0x7b]))
            .expect("1-byte bundle parses");
        assert_eq!(m.bundle, vec![0x7b]);

        let max = vec![0x42u8; PROOF_BUNDLE_MAX_LEN];
        let m = parse_proof_marker(&golden_marker(&golden_game_id(), &max))
            .expect("65536-byte bundle parses");
        assert_eq!(m.bundle.len(), 65536);
    }

    #[test]
    fn adversarial_pushdata_len_never_panics_or_wraps() {
        // Adversarial-review MED (inherited from collected/result): an
        // OP_PUSHDATA4 (0x4e) with len 0xFFFFFFFF on wasm32 (usize=u32)
        // would wrap `i + len` past the bounds guard → slice panic →
        // topic-manager /submit trap. The crafted ~7-byte script must
        // parse to None (no marker), never panic. Also probe OP_PUSHDATA2
        // and a truncated push.
        for script in [
            vec![0x00u8, 0x6a, 0x4e, 0xff, 0xff, 0xff, 0xff], // PUSHDATA4 max len, no data
            vec![0x00u8, 0x6a, 0x4d, 0xff, 0xff],             // PUSHDATA2 max len, no data
            vec![0x00u8, 0x6a, 0x4e, 0xff, 0xff, 0xff],       // PUSHDATA4 header truncated
            vec![0x00u8, 0x6a, 0x4b],                         // a 75-byte push with no data
        ] {
            assert_eq!(parse_proof_marker(&script), None, "crafted script must not parse");
        }
        // Direct read_pushes probe: the trap path is the len itself.
        assert!(read_pushes(&[0x4e, 0xff, 0xff, 0xff, 0xff]).is_empty());
        assert!(read_pushes(&[0x4d, 0xff, 0xff]).is_empty());
        // A PUSHDATA4 with an in-range length is still a valid push form —
        // read_pushes handles 0x4e even though the builder never emits it.
        let mut s = vec![0x4e, 0x03, 0x00, 0x00, 0x00];
        s.extend_from_slice(&[0xaa, 0xbb, 0xcc]);
        let pushes = read_pushes(&s);
        assert_eq!(pushes, vec![&[0xaa, 0xbb, 0xcc][..]]);
    }

    // ── Valid markers ─────────────────────────────────────────────────────

    #[test]
    fn valid_marker_parses() {
        for sig_len in [68usize, 70, 71, 72, 74] {
            let script =
                marker_script(&[0xABu8; 32], &golden_winner(), &vec![0x30; sig_len], b"{}");
            let m = parse_proof_marker(&script)
                .unwrap_or_else(|| panic!("sig len {sig_len} must parse"));
            assert_eq!(m.game_id, [0xABu8; 32]);
            assert_eq!(m.sig.len(), sig_len);
            assert_eq!(m.bundle, b"{}");
        }
    }

    // ── Rejections ────────────────────────────────────────────────────────

    #[test]
    fn wrong_tag_rejected() {
        for bad_tag in [
            b"LOW/proof/v2".as_slice(),  // wrong version
            b"LOW/reveal/v2".as_slice(), // a different LOW topic
            b"SOMETHINGELS".as_slice(),  // foreign 12-byte tag
            b"LOW/proof/v".as_slice(),   // 11 bytes
            b"LOW/result/v1".as_slice(), // the sibling topic's 13-byte tag
        ] {
            let mut s = vec![0x00, 0x6a];
            s.extend(push_data(bad_tag));
            s.extend(push_data(&[0x11u8; 32]));
            s.extend(push_data(&golden_winner()));
            s.extend(push_data(&golden_sig()));
            s.extend(push_data(&golden_bundle()));
            assert!(
                parse_proof_marker(&s).is_none(),
                "tag {bad_tag:?} must be rejected"
            );
        }
    }

    #[test]
    fn non_op_return_rejected() {
        // A standard P2PKH is not an OP_RETURN.
        let p2pkh = hex::decode("76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac").unwrap();
        assert!(parse_proof_marker(&p2pkh).is_none());
        // Empty / one-byte scripts.
        assert!(parse_proof_marker(&[]).is_none());
        assert!(parse_proof_marker(&[0x00]).is_none());
        // A bare OP_RETURN (0x6a without OP_FALSE) is NOT the v1 prefix.
        let script = golden_marker(&golden_game_id(), &golden_bundle());
        assert!(parse_proof_marker(&script[1..]).is_none());
    }

    #[test]
    fn short_game_id_rejected() {
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(PROOF_TAG));
        s.extend(push_data(&[0x11u8; 31])); // 31, not 32
        s.extend(push_data(&golden_winner()));
        s.extend(push_data(&golden_sig()));
        s.extend(push_data(&golden_bundle()));
        assert!(parse_proof_marker(&s).is_none());
    }

    #[test]
    fn wrong_identity_key_length_rejected() {
        for len in [32usize, 34, 65] {
            let script =
                marker_script(&[0x11u8; 32], &vec![0x02u8; len], &golden_sig(), b"{}");
            assert!(
                parse_proof_marker(&script).is_none(),
                "winner len {len} must be rejected"
            );
        }
    }

    #[test]
    fn sig_length_out_of_range_rejected() {
        for len in [0usize, 1, 67, 75, 100] {
            let script =
                marker_script(&[0x11u8; 32], &golden_winner(), &vec![0x30u8; len], b"{}");
            assert!(
                parse_proof_marker(&script).is_none(),
                "sig len {len} must be rejected"
            );
        }
    }

    #[test]
    fn empty_bundle_rejected() {
        // A zero-byte bundle proves nothing and is not a v1 marker.
        let script = golden_marker(&golden_game_id(), &[]);
        assert!(parse_proof_marker(&script).is_none());
    }

    #[test]
    fn oversized_bundle_rejected() {
        // 65537 bytes exceeds the format ceiling.
        let big = vec![0x42u8; PROOF_BUNDLE_MAX_LEN + 1];
        let script = golden_marker(&golden_game_id(), &big);
        assert!(parse_proof_marker(&script).is_none());
    }

    #[test]
    fn missing_pushes_rejected() {
        // Every prefix of the five-push shape (4 or fewer pushes) must be
        // rejected.
        let pushes: Vec<Vec<u8>> = vec![
            push_data(PROOF_TAG),
            push_data(&[0x11u8; 32]),
            push_data(&golden_winner()),
            push_data(&golden_sig()),
        ];
        let mut s = vec![0x00, 0x6a];
        for p in &pushes {
            s.extend_from_slice(p);
            assert!(
                parse_proof_marker(&s).is_none(),
                "a marker with fewer than 5 pushes must be rejected"
            );
        }
    }

    #[test]
    fn extra_pushes_rejected() {
        // A sixth push is not the v1 format (exactly five pushes).
        let mut s = golden_marker(&golden_game_id(), &golden_bundle());
        s.extend(push_data(&[0x99u8; 4]));
        assert!(parse_proof_marker(&s).is_none());
    }

    #[test]
    fn truncated_push_rejected() {
        // Claim a 19-byte bundle push but truncate the bytes.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(PROOF_TAG));
        s.extend(push_data(&[0x11u8; 32]));
        s.extend(push_data(&golden_winner()));
        s.extend(push_data(&golden_sig()));
        s.push(0x13); // push 19…
        s.extend_from_slice(&[0x7b; 5]); // …but only 5 bytes follow
        assert!(parse_proof_marker(&s).is_none());
    }
}
