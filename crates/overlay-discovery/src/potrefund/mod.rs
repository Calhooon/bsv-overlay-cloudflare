//! `tm_potrefund` / `ls_potrefund` — LOW KEYLESS PRE-SIGNED REFUND BACKUP
//! (recovery defense-in-depth, bsv-low #191).
//!
//! A fully-wiped LOW device recovers via seed + the `potparty` index
//! (#188) — but the actual on-chain REFUND that brings a
//! both-players-vanished pot home still relies on the tower's dead-man's
//! switch firing the pre-signed refund. This marker is the BELT: each seat
//! publishes the pre-signed 2-of-2 refund transaction itself — public-safe
//! (non-final until `recovery_height`, and by the covenant it can only pay
//! the mandated refund homes) — stored on the overlay so ANY client can
//! re-broadcast it after `recovery_height` even if the tower failed. Public
//! data only, non-custodial.
//!
//! Like `tm_result` / `tm_collected` / `tm_potparty`, this is an
//! `OP_RETURN` data-carrier topic admitted by BYTE FORMAT ONLY — the
//! overlay is an INDEX, not an authority. There is NO server-side signature
//! verification and NO transaction validation: the marker is admitted on
//! its exact byte shape, and its `refundRawHex` + `sig` pushes are carried
//! back verbatim. A client that cares about authenticity parses and
//! verifies the refund tx itself; the overlay only preserves the bytes.
//!
//! # Marker wire format (`LOW/potrefund/v1`)
//!
//! `OP_FALSE OP_RETURN` (0x00 0x6a) followed by EXACTLY SEVEN minimal data
//! pushes — byte-identical to the app's builder (the cross-repo CONTRACT,
//! bsv-low #191):
//!
//! | # | Push          | Encoding                                          |
//! |---|---------------|---------------------------------------------------|
//! | 0 | tag           | UTF-8 `LOW/potrefund/v1` (16 bytes)               |
//! | 1 | identity      | 33 bytes (publishing seat's compressed pubkey)    |
//! | 2 | gameId        | 32 bytes                                          |
//! | 3 | potTxid       | 32 bytes                                          |
//! | 4 | potVout       | 4 bytes little-endian (u32)                       |
//! | 5 | refundRawHex  | VARIABLE — the pre-signed refund tx bytes         |
//! |   |               | (non-empty, <= 100_000; OP_PUSHDATA2/4 path)      |
//! | 6 | sig           | DER ECDSA, 68..=74 bytes (preserved, never        |
//! |   |               | verified by the overlay)                          |
//!
//! The parser validates the shape strictly (exact tag + exact push count +
//! exact fixed-field lengths + a non-empty, capped `refundRawHex` + a
//! DER-ranged `sig`) and extracts the fields. Wrong tag / wrong lengths /
//! missing or extra pushes / a truncated push / an empty or oversized
//! refund → `None` (not a potrefund marker).
//!
//! # Lookup (`ls_potrefund`)
//!
//! Query JSON (tagged by `type`):
//!
//! ```json
//! {"type": "byPot", "potTxid": "<64 hex chars>", "potVout": 0, "limit": 50}
//! {"type": "partyFor", "identity": "<66 hex chars>", "limit": 50}
//! ```
//!
//! `byPot` answers "give me the pre-signed refund(s) for this pot outpoint"
//! — the recovery question (BOTH seats may publish; every marker is
//! returned). `partyFor` answers "which pots have I published a refund
//! backup for?" (completeness). `limit` is optional (default 100, clamped
//! to 1..=500). The answer is a freeform JSON array, newest first, one
//! entry per stored marker:
//!
//! ```json
//! [{"identity": "<hex>", "gameId": "<hex>", "potTxid": "<hex>",
//!   "potVout": 0, "refundRawHex": "<hex>", "sigHex": "<hex>",
//!   "txid": "<hex>", "outputIndex": 0, "createdAt": 1234567890}]
//! ```

pub mod lookup_service;
pub mod storage;
pub mod topic_manager;

/// The domain tag the app stamps. v1 = `(tag, identity, gameId, potTxid,
/// potVout, refundRawHex, sig)`. 16 bytes of ASCII — the byte layout is the
/// cross-repo CONTRACT (bsv-low #191); never change it without a version
/// bump on both sides.
pub const POTREFUND_TAG: &[u8] = b"LOW/potrefund/v1";
/// Number of minimal data pushes in a well-formed v1 marker.
pub const POTREFUND_FIELD_COUNT: usize = 7;
/// identity push length (bytes) — a compressed secp256k1 pubkey.
pub const POTREFUND_IDENTITY_KEY_LEN: usize = 33;
/// gameId push length (bytes).
pub const POTREFUND_GAME_ID_LEN: usize = 32;
/// potTxid push length (bytes).
pub const POTREFUND_TXID_LEN: usize = 32;
/// potVout push length (bytes) — a little-endian u32.
pub const POTREFUND_U32_LEN: usize = 4;
/// Sanity cap on the refund raw-tx push length (bytes). A real pre-signed
/// refund is a few KB; the cap only keeps a pathological blob out of the
/// index. The refund push must also be NON-EMPTY.
pub const POTREFUND_REFUND_MAX_LEN: usize = 100_000;
/// Minimum sig push length (bytes) — a DER ECDSA signature.
pub const POTREFUND_SIG_MIN_LEN: usize = 68;
/// Maximum sig push length (bytes) — a DER ECDSA signature.
pub const POTREFUND_SIG_MAX_LEN: usize = 74;

/// A decoded v1 potrefund marker — one seat's pre-signed refund backup for
/// a pot, published so ANY client can re-broadcast it after the refund's
/// recovery height even if the tower failed.
///
/// The overlay only needs `identity` (plus the pot outpoint) to key the
/// index and the raw bytes to hand back to querying clients (which parse +
/// verify the `refund_raw` / `sig` themselves — the overlay never does).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PotrefundMarker {
    /// The publishing seat's compressed identity pubkey (exactly 33 bytes).
    pub identity: Vec<u8>,
    /// The 32-byte game id.
    pub game_id: [u8; 32],
    /// The 32-byte pot funding txid.
    pub pot_txid: [u8; 32],
    /// The pot output index within `pot_txid` (u32, little-endian on wire).
    pub pot_vout: u32,
    /// The pre-signed refund transaction bytes (non-empty, <= 100_000) —
    /// carried back verbatim; the overlay NEVER parses or validates it.
    pub refund_raw: Vec<u8>,
    /// The seat's DER ECDSA signature push (68..=74 bytes) — carried back
    /// verbatim; the overlay NEVER verifies it.
    pub sig: Vec<u8>,
}

/// Walk minimal Bitcoin pushdata out of a byte slice → the pushed blobs,
/// in order, stopping at the first non-push opcode / a truncated push
/// (mirrors `result`'s / `collected`'s / `potparty`'s `read_pushes`).
///
/// EVERY offset advance uses CHECKED arithmetic. This worker runs on wasm32
/// (`usize = u32`) with wrapping release arithmetic — an OP_PUSHDATA4 length
/// of `0xFFFFFFFF` would make a naive `i + len` WRAP past the bounds guard and
/// panic-trap the topic-manager `/submit` pass on a ~7-byte crafted script.
/// `checked_add` → `None` on overflow → we stop cleanly (a malformed marker is
/// simply skipped, never a trap). Adversarial-review MED, inherited from
/// `result` / `collected` / `potparty`. The variable `refundRawHex` push
/// rides the OP_PUSHDATA2 (0x4d) branch — exercised by the round-trip test.
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

/// Parse one output locking script as a `LOW/potrefund/v1` marker.
///
/// `Some(marker)` IFF the script is `OP_FALSE OP_RETURN` (0x00 0x6a)
/// followed by EXACTLY seven minimal data pushes with the exact shape:
/// tag == [`POTREFUND_TAG`], identity 33 bytes, gameId 32 bytes, potTxid 32
/// bytes, potVout 4 bytes, refundRawHex non-empty and <= 100_000 bytes, sig
/// 68..=74 bytes. Everything else — a bare `OP_RETURN`, a different tag, a
/// wrong length, extra/missing pushes, an empty or oversized refund — is
/// `None`.
pub fn parse_potrefund_marker(script: &[u8]) -> Option<PotrefundMarker> {
    // OP_FALSE OP_RETURN (0x00 0x6a) — the exact prefix the app's builder
    // emits (the #191 contract pins the two-byte prefix, like #188's).
    if script.len() < 2 || script[0] != 0x00 || script[1] != 0x6a {
        return None;
    }
    let data = read_pushes(&script[2..]);
    if data.len() != POTREFUND_FIELD_COUNT {
        return None;
    }
    if data[0] != POTREFUND_TAG {
        return None;
    }
    let identity_b = data[1];
    let game_id_b = data[2];
    let pot_txid_b = data[3];
    let pot_vout_b = data[4];
    let refund_b = data[5];
    let sig_b = data[6];

    if identity_b.len() != POTREFUND_IDENTITY_KEY_LEN {
        return None;
    }
    if game_id_b.len() != POTREFUND_GAME_ID_LEN {
        return None;
    }
    if pot_txid_b.len() != POTREFUND_TXID_LEN {
        return None;
    }
    if pot_vout_b.len() != POTREFUND_U32_LEN {
        return None;
    }
    // The refund raw-tx push must be NON-EMPTY and within the sanity cap.
    if refund_b.is_empty() || refund_b.len() > POTREFUND_REFUND_MAX_LEN {
        return None;
    }
    if !(POTREFUND_SIG_MIN_LEN..=POTREFUND_SIG_MAX_LEN).contains(&sig_b.len()) {
        return None;
    }

    let mut game_id = [0u8; 32];
    game_id.copy_from_slice(game_id_b);
    let mut pot_txid = [0u8; 32];
    pot_txid.copy_from_slice(pot_txid_b);
    let pot_vout = u32::from_le_bytes([pot_vout_b[0], pot_vout_b[1], pot_vout_b[2], pot_vout_b[3]]);
    Some(PotrefundMarker {
        identity: identity_b.to_vec(),
        game_id,
        pot_txid,
        pot_vout,
        refund_raw: refund_b.to_vec(),
        sig: sig_b.to_vec(),
    })
}

/// True iff `script` is a well-formed `LOW/potrefund/v1` marker.
pub fn is_potrefund_marker_script(script: &[u8]) -> bool {
    parse_potrefund_marker(script).is_some()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Minimal Bitcoin pushdata for a byte blob (direct / OP_PUSHDATA1 /
    /// _2) — mirrors `potparty`'s test helper. A ~1KB refund rides the
    /// OP_PUSHDATA2 (0x4d) branch.
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
            // OP_PUSHDATA4 — needed to encode the at-cap (100_000-byte)
            // refund; read_pushes handles the 0x4e branch symmetrically.
            out.push(0x4e);
            out.push((len & 0xff) as u8);
            out.push(((len >> 8) & 0xff) as u8);
            out.push(((len >> 16) & 0xff) as u8);
            out.push(((len >> 24) & 0xff) as u8);
        }
        out.extend_from_slice(blob);
        out
    }

    /// The app's potrefund-marker builder in bytes.
    pub(crate) fn marker_script(
        identity: &[u8],
        game_id: &[u8; 32],
        pot_txid: &[u8; 32],
        pot_vout: u32,
        refund_raw: &[u8],
        sig: &[u8],
    ) -> Vec<u8> {
        let mut s = vec![0x00, 0x6a]; // OP_FALSE OP_RETURN
        s.extend(push_data(POTREFUND_TAG));
        s.extend(push_data(identity));
        s.extend(push_data(game_id));
        s.extend(push_data(pot_txid));
        s.extend(push_data(&pot_vout.to_le_bytes()));
        s.extend(push_data(refund_raw));
        s.extend(push_data(sig));
        s
    }

    // ── The golden fixtures ───────────────────────────────────────────────
    pub(crate) fn golden_identity() -> Vec<u8> {
        let mut k = vec![0x02u8];
        k.extend_from_slice(&[0xa1u8; 32]);
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
    /// A ~1KB fake pre-signed refund tx — big enough to force the
    /// OP_PUSHDATA2 (0x4d) push path, distinctive bytes for round-trip.
    pub(crate) fn golden_refund() -> Vec<u8> {
        (0..1024u32).map(|i| (i & 0xff) as u8).collect()
    }
    pub(crate) fn golden_sig() -> Vec<u8> {
        let mut s = vec![0x30u8, 0x45];
        s.extend_from_slice(&[0xabu8; 69]);
        s
    }

    /// A valid marker over the golden identity with chosen gameId / vout.
    pub(crate) fn golden_marker(game_id: &[u8; 32], pot_txid: &[u8; 32], vout: u32) -> Vec<u8> {
        marker_script(
            &golden_identity(),
            game_id,
            pot_txid,
            vout,
            &golden_refund(),
            &golden_sig(),
        )
    }

    #[test]
    fn tag_is_16_bytes() {
        assert_eq!(POTREFUND_TAG.len(), 16);
        assert_eq!(POTREFUND_TAG, b"LOW/potrefund/v1");
    }

    #[test]
    fn valid_marker_round_trips() {
        // Build → parse → every field back, over the full sig-length range.
        for sig_len in [68usize, 70, 71, 72, 74] {
            let script = marker_script(
                &golden_identity(),
                &golden_game_id(),
                &golden_pot_txid(),
                7,
                &golden_refund(),
                &vec![0x30u8; sig_len],
            );
            let m = parse_potrefund_marker(&script)
                .unwrap_or_else(|| panic!("sig len {sig_len} must parse"));
            assert_eq!(m.identity, golden_identity());
            assert_eq!(m.identity.len(), 33);
            assert_eq!(m.game_id, golden_game_id());
            assert_eq!(m.pot_txid, golden_pot_txid());
            assert_eq!(m.pot_vout, 7);
            assert_eq!(m.refund_raw, golden_refund());
            assert_eq!(m.refund_raw.len(), 1024);
            assert_eq!(m.sig.len(), sig_len);
            assert!(is_potrefund_marker_script(&script));
        }
    }

    #[test]
    fn large_refund_pushdata2_round_trips() {
        // The refund push rides OP_PUSHDATA2 (0x4d). Confirm the encoder
        // emitted a 0x4d prefix somewhere and the bytes survive verbatim.
        let refund: Vec<u8> = (0..4096u32).map(|i| (i.wrapping_mul(7) & 0xff) as u8).collect();
        let script = marker_script(
            &golden_identity(),
            &golden_game_id(),
            &golden_pot_txid(),
            0,
            &refund,
            &golden_sig(),
        );
        assert!(
            script.windows(1).any(|w| w == [0x4d]),
            "a >255-byte refund must use OP_PUSHDATA2 (0x4d)"
        );
        let m = parse_potrefund_marker(&script).expect("4KB refund must parse");
        assert_eq!(m.refund_raw, refund);
        assert_eq!(m.refund_raw.len(), 4096);
    }

    #[test]
    fn le_encoding_of_vout() {
        // A distinctive value proves LITTLE-endian decode (not big-endian).
        let vout = 0x0403_0201u32; // bytes 01 02 03 04 on the wire
        let script = marker_script(
            &golden_identity(),
            &golden_game_id(),
            &golden_pot_txid(),
            vout,
            &golden_refund(),
            &golden_sig(),
        );
        let m = parse_potrefund_marker(&script).unwrap();
        assert_eq!(m.pot_vout, vout);
        assert!(
            script.windows(4).any(|w| w == [0x01, 0x02, 0x03, 0x04]),
            "vout must be stored little-endian"
        );
    }

    // ── Rejections ────────────────────────────────────────────────────────

    #[test]
    fn empty_refund_rejected() {
        // refundRawHex must be NON-EMPTY.
        let script = marker_script(
            &golden_identity(),
            &golden_game_id(),
            &golden_pot_txid(),
            golden_vout(),
            &[], // empty refund push
            &golden_sig(),
        );
        assert!(parse_potrefund_marker(&script).is_none());
    }

    #[test]
    fn oversized_refund_rejected() {
        // refundRawHex over the sanity cap (100_000) is rejected.
        let refund = vec![0x00u8; POTREFUND_REFUND_MAX_LEN + 1];
        let script = marker_script(
            &golden_identity(),
            &golden_game_id(),
            &golden_pot_txid(),
            golden_vout(),
            &refund,
            &golden_sig(),
        );
        assert!(parse_potrefund_marker(&script).is_none());
        // Exactly at the cap parses.
        let refund = vec![0x00u8; POTREFUND_REFUND_MAX_LEN];
        let script = marker_script(
            &golden_identity(),
            &golden_game_id(),
            &golden_pot_txid(),
            golden_vout(),
            &refund,
            &golden_sig(),
        );
        assert!(parse_potrefund_marker(&script).is_some());
    }

    #[test]
    fn wrong_tag_rejected() {
        for bad_tag in [
            b"LOW/potrefund/v2".as_slice(), // wrong version
            b"LOW/potparty/v1".as_slice(),  // a different LOW topic
            b"LOW/potrefund/v".as_slice(),  // 15 bytes
            b"SOMETHINGELSE!!!".as_slice(), // foreign 16-byte tag
        ] {
            let mut s = vec![0x00, 0x6a];
            s.extend(push_data(bad_tag));
            s.extend(push_data(&golden_identity()));
            s.extend(push_data(&golden_game_id()));
            s.extend(push_data(&golden_pot_txid()));
            s.extend(push_data(&golden_vout().to_le_bytes()));
            s.extend(push_data(&golden_refund()));
            s.extend(push_data(&golden_sig()));
            assert!(
                parse_potrefund_marker(&s).is_none(),
                "tag {bad_tag:?} must be rejected"
            );
        }
    }

    #[test]
    fn non_op_return_rejected() {
        // A standard P2PKH is not an OP_RETURN.
        let p2pkh = hex::decode("76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac").unwrap();
        assert!(parse_potrefund_marker(&p2pkh).is_none());
        assert!(parse_potrefund_marker(&[]).is_none());
        assert!(parse_potrefund_marker(&[0x00]).is_none());
        // A bare OP_RETURN (0x6a without OP_FALSE) is NOT the v1 prefix.
        let script = golden_marker(&golden_game_id(), &golden_pot_txid(), golden_vout());
        assert!(parse_potrefund_marker(&script[1..]).is_none());
    }

    #[test]
    fn wrong_identity_key_length_rejected() {
        for len in [32usize, 34, 65] {
            let script = marker_script(
                &vec![0x02u8; len],
                &golden_game_id(),
                &golden_pot_txid(),
                golden_vout(),
                &golden_refund(),
                &golden_sig(),
            );
            assert!(
                parse_potrefund_marker(&script).is_none(),
                "identity len {len} must be rejected"
            );
        }
    }

    #[test]
    fn wrong_game_id_or_txid_length_rejected() {
        // gameId 31 bytes.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(POTREFUND_TAG));
        s.extend(push_data(&golden_identity()));
        s.extend(push_data(&[0x11u8; 31]));
        s.extend(push_data(&golden_pot_txid()));
        s.extend(push_data(&golden_vout().to_le_bytes()));
        s.extend(push_data(&golden_refund()));
        s.extend(push_data(&golden_sig()));
        assert!(parse_potrefund_marker(&s).is_none());
        // potTxid 33 bytes.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(POTREFUND_TAG));
        s.extend(push_data(&golden_identity()));
        s.extend(push_data(&golden_game_id()));
        s.extend(push_data(&[0x22u8; 33]));
        s.extend(push_data(&golden_vout().to_le_bytes()));
        s.extend(push_data(&golden_refund()));
        s.extend(push_data(&golden_sig()));
        assert!(parse_potrefund_marker(&s).is_none());
    }

    #[test]
    fn wrong_u32_length_rejected() {
        // potVout as a 3-byte push.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(POTREFUND_TAG));
        s.extend(push_data(&golden_identity()));
        s.extend(push_data(&golden_game_id()));
        s.extend(push_data(&golden_pot_txid()));
        s.extend(push_data(&[0x01u8, 0x02, 0x03])); // 3 bytes, not 4
        s.extend(push_data(&golden_refund()));
        s.extend(push_data(&golden_sig()));
        assert!(parse_potrefund_marker(&s).is_none());
    }

    #[test]
    fn sig_length_out_of_range_rejected() {
        for len in [0usize, 1, 67, 75, 100] {
            let script = marker_script(
                &golden_identity(),
                &golden_game_id(),
                &golden_pot_txid(),
                golden_vout(),
                &golden_refund(),
                &vec![0x30u8; len],
            );
            assert!(
                parse_potrefund_marker(&script).is_none(),
                "sig len {len} must be rejected"
            );
        }
    }

    #[test]
    fn missing_pushes_rejected() {
        // Every prefix of the seven-push shape (6 or fewer pushes) rejected.
        let pushes: Vec<Vec<u8>> = vec![
            push_data(POTREFUND_TAG),
            push_data(&golden_identity()),
            push_data(&golden_game_id()),
            push_data(&golden_pot_txid()),
            push_data(&golden_vout().to_le_bytes()),
            push_data(&golden_refund()),
        ];
        let mut s = vec![0x00, 0x6a];
        for p in &pushes {
            s.extend_from_slice(p);
            assert!(
                parse_potrefund_marker(&s).is_none(),
                "a marker with fewer than 7 pushes must be rejected"
            );
        }
    }

    #[test]
    fn extra_pushes_rejected() {
        // An eighth push is not the v1 format (exactly seven pushes).
        let mut s = golden_marker(&golden_game_id(), &golden_pot_txid(), golden_vout());
        s.extend(push_data(&[0x99u8; 4]));
        assert!(parse_potrefund_marker(&s).is_none());
    }

    #[test]
    fn truncated_push_rejected() {
        // Claim a 70-byte sig push but truncate the bytes.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(POTREFUND_TAG));
        s.extend(push_data(&golden_identity()));
        s.extend(push_data(&golden_game_id()));
        s.extend(push_data(&golden_pot_txid()));
        s.extend(push_data(&golden_vout().to_le_bytes()));
        s.extend(push_data(&golden_refund()));
        s.push(0x46); // push 70…
        s.extend_from_slice(&[0xab; 10]); // …but only 10 bytes follow
        assert!(parse_potrefund_marker(&s).is_none());
    }

    #[test]
    fn adversarial_pushdata_len_never_panics_or_wraps() {
        // Inherited from result/collected/potparty: an OP_PUSHDATA4 (0x4e)
        // with len 0xFFFFFFFF on wasm32 (usize=u32) would wrap `i + len`
        // past the bounds guard → slice panic → topic-manager /submit trap.
        // The crafted script must parse to None, never panic.
        for script in [
            vec![0x00u8, 0x6a, 0x4e, 0xff, 0xff, 0xff, 0xff],
            vec![0x00u8, 0x6a, 0x4d, 0xff, 0xff],
            vec![0x00u8, 0x6a, 0x4e, 0xff, 0xff, 0xff],
            vec![0x00u8, 0x6a, 0x4b],
        ] {
            assert_eq!(parse_potrefund_marker(&script), None);
        }
        assert!(read_pushes(&[0x4e, 0xff, 0xff, 0xff, 0xff]).is_empty());
        assert!(read_pushes(&[0x4d, 0xff, 0xff]).is_empty());
    }
}
