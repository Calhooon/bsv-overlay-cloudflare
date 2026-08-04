//! `tm_hopparty` / `ls_hopparty` — LOW by-identity HOP-IN-FLIGHT marker index
//! (bsv-low #315, #252 stage 2b).
//!
//! The funding HOP (the staging P2PKH coin a seat pays to its own
//! `[2,'low settle']` key before the JOIN assembles the pot) has no
//! identity/gameId key anywhere server-side: `tm_lowfund` indexes the bare
//! P2PKH outpoint, and `potparty_records` rows are only published at
//! JOIN-assembly — so a seat that funds its hop and dies BEFORE the join
//! leaves ZERO identity-keyed server rows (the #256 ~80.8k-sat class). At
//! hop time the seat publishes this tiny signed `OP_RETURN` marker (as a
//! SECOND OUTPUT on the hop `createAction` itself — no extra spend
//! approval) naming its identity, the opponent, the game, the hop outpoint,
//! the hop value, and its settle pubkey. `/hops-view` (low-app-layer) joins
//! it to the `tm_lowfund`-indexed hop outpoint to answer "which hops of
//! mine are in flight?".
//!
//! Like `tm_potparty`, this is an `OP_RETURN` data-carrier topic admitted by
//! BYTE FORMAT ONLY — the overlay is an INDEX, not an authority. There is
//! NO server-side signature verification at admission: the marker is
//! admitted on its exact byte shape (plus the structural
//! `identity != opponentIdentity` rule), and its signature pushes are
//! carried back verbatim. Verification is a READ-time validity FILTER
//! (`low-app-layer /hops-view`) and a client-side check — never an
//! admission authority (zanaadu invariant #3: indexers cache and serve,
//! never validate; parsing succeeding is the entire gate).
//!
//! UNLIKE the #319 join-tx potparty plan, the hop marker CANNOT inherit
//! network enforcement by construction: pre-JOIN nothing on-chain has been
//! signed by the settle key (the hop tx PAYS to it, funded by arbitrary
//! wallet coins), so hops-in-flight keeps read-time signature verification
//! (ledger 2026-08-03, "#315 must state that rather than imply parity").
//! What the hybrid DOES buy: the hop output is
//! `P2PKH(hash160(seatSettlePubkey))`, so the marker's own pubkey is
//! chain-checkable at read against the admitted hop output script.
//!
//! # Marker wire format (`LOW/hopparty/v1`)
//!
//! `OP_FALSE OP_RETURN` (0x00 0x6a) followed by EXACTLY TEN minimal data
//! pushes — the cross-repo CONTRACT with the bsv-low client (bsv-low #315;
//! versioning copies the #230 potparty-v2 contract verbatim):
//!
//! | # | Push             | Encoding                                      |
//! |---|------------------|-----------------------------------------------|
//! | 0 | tag              | UTF-8 `LOW/hopparty/v1` (15 bytes)            |
//! | 1 | identity         | 33 bytes (publishing seat's compressed pubkey)|
//! | 2 | opponentIdentity | 33 bytes (the other seat's compressed pubkey) |
//! | 3 | gameId           | 32 bytes                                      |
//! | 4 | hopTxid          | 32 bytes                                      |
//! | 5 | hopVout          | 4 bytes little-endian (u32)                   |
//! | 6 | hopSats          | 8 bytes little-endian (u64)                   |
//! | 7 | seatSettlePubkey | 33 bytes (compressed `[2,'low settle']` key — |
//! |   |                  | the key the hop output pays)                  |
//! | 8 | seatSig          | DER ECDSA, 67..=74 bytes (BY the settle key)  |
//! | 9 | identitySig      | DER ECDSA, 67..=74 bytes (BY the identity)    |
//!
//! `hopSats` is 8 bytes because satoshi amounts are u64 in the transaction
//! format itself (a 4-byte u32 caps at ~42.9 BSV); the potparty precedent's
//! 4-byte pushes carry u32-domain values only (vout / block height), so the
//! minimal FAITHFUL encoding for a sats field is the tx serialization's own
//! 8-byte little-endian u64.
//!
//! Structural rule: `identity != opponentIdentity` (byte compare) — a
//! self-paired marker is junk and never parses.
//!
//! # Digests (domain-separated by the version tag — the #230 twin recipe)
//!
//! Both signatures embed the `LOW/hopparty/v1` tag string, so a hopparty
//! signature can never verify over any potparty preimage and vice versa:
//!
//! - **identitySig** (BRC-42/43, protocol `[1,'low potparty']` — the wallet
//!   protocol id is REUSED by the ledgered #315 decision-3 (no new manifest
//!   grant), the TAG is what separates the domains — keyID = gameId
//!   (lowercase), counterparty 'anyone'):
//!   [`hopparty_identity_challenge`] =
//!   `UTF-8("LOW/hopparty/v1") ‖ identity(33) ‖ opponentIdentity(33) ‖
//!    gameId(32) ‖ hopTxid(32) ‖ hopVout(4 LE) ‖ hopSats(8 LE) ‖
//!    seatSettlePubkey(33)`.
//! - **seatSig** (plain ECDSA by the settle key over a SINGLE sha256 — the
//!   BRC-100 `createSignature({data})` hash):
//!   [`hopparty_seatsig_preimage`] =
//!   `UTF-8("LOW/hopparty/v1/seatsig|") ‖ gameId(32) ‖ hopTxid(32) ‖
//!    hopVout(4 LE) ‖ identity(33)` — the exact potparty-v2 seatsig layout
//!   with the hopparty domain tag (both tags are 24 ASCII bytes, so the two
//!   preimage families share a fixed 125-byte layout and differ in the
//!   domain prefix alone).
//!
//! # Lookup (`ls_hopparty`)
//!
//! Query JSON (tagged by `type`):
//!
//! ```json
//! {"type": "hopsFor", "identity": "<66 hex chars>", "limit": 50}
//! {"type": "byHop", "hopTxid": "<64 hex chars>", "hopVout": 0}
//! ```
//!
//! `hopsFor` counts HOP OUTPOINTS (newest first) and returns a BOUNDED
//! SUPERSET per outpoint (the #281 lesson: a layer that cannot verify
//! signatures must never choose which row is real); `byHop` returns markers
//! OLDEST first. Answer entries:
//!
//! ```json
//! [{"identity": "<hex>", "opponentIdentity": "<hex>", "gameId": "<hex>",
//!   "hopTxid": "<hex>", "hopVout": 0, "hopSats": 1234567,
//!   "seatSettlePubkey": "<hex>", "seatSigHex": "<hex>",
//!   "identitySigHex": "<hex>", "txid": "<hex>", "outputIndex": 0,
//!   "createdAt": 1234567890}]
//! ```

pub mod lookup_service;
pub mod storage;
pub mod topic_manager;

/// The domain tag the app stamps — 15 bytes of ASCII, the same length as
/// the potparty tags. The byte layout is the cross-repo CONTRACT (bsv-low
/// #315); never change it without a version bump on both sides.
pub const HOPPARTY_TAG: &[u8] = b"LOW/hopparty/v1";
/// The seatSig preimage's domain-separation tag (version-tagged ASCII, 24
/// bytes — the same length as `LOW/potparty/v2/seatsig|`, so the two
/// preimage families share one fixed layout and differ only in the domain
/// prefix). MUST stay byte-identical to the client's
/// `HOPPARTY_SEATSIG_DOMAIN`.
pub const HOPPARTY_SEATSIG_DOMAIN: &[u8] = b"LOW/hopparty/v1/seatsig|";
/// Number of minimal data pushes in a well-formed hopparty marker.
pub const HOPPARTY_FIELD_COUNT: usize = 10;
/// identity / opponentIdentity / seatSettlePubkey push length (bytes) — a
/// compressed secp256k1 pubkey.
pub const HOPPARTY_IDENTITY_KEY_LEN: usize = 33;
/// gameId push length (bytes).
pub const HOPPARTY_GAME_ID_LEN: usize = 32;
/// hopTxid push length (bytes).
pub const HOPPARTY_TXID_LEN: usize = 32;
/// hopVout push length (bytes) — a little-endian u32.
pub const HOPPARTY_U32_LEN: usize = 4;
/// hopSats push length (bytes) — a little-endian u64 (sats are u64 in the
/// tx format; see the module docs for why this is not 4).
pub const HOPPARTY_U64_LEN: usize = 8;
/// Minimum sig push length (bytes) — a DER ECDSA signature. 67 (not 68) is
/// LOAD-BEARING, inherited from potparty's F5: RFC6979 is DETERMINISTIC
/// and both hopparty digests are FIXED per (game, hop, identity), so a
/// signature that fell below the floor would re-produce IDENTICALLY forever
/// and permanently wedge that marker. Same bounds as
/// `POTPARTY_SIG_MIN_LEN`/`_MAX_LEN` and mirrored in the client parser.
pub const HOPPARTY_SIG_MIN_LEN: usize = 67;
/// Maximum sig push length (bytes) — a DER ECDSA signature.
pub const HOPPARTY_SIG_MAX_LEN: usize = 74;

/// A decoded hopparty marker — one seat's "my funding hop for game G is
/// outpoint (T,v)" claim, published at hop time so `/hops-view` can
/// enumerate hops-in-flight for an identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HoppartyMarker {
    /// The publishing seat's compressed identity pubkey (exactly 33 bytes).
    pub identity: Vec<u8>,
    /// The opponent seat's compressed identity pubkey (exactly 33 bytes) —
    /// guaranteed != `identity` (a self-paired marker never parses).
    pub opponent: Vec<u8>,
    /// The 32-byte game id.
    pub game_id: [u8; 32],
    /// The 32-byte hop funding txid.
    pub hop_txid: [u8; 32],
    /// The hop output index within `hop_txid` (u32, little-endian on wire).
    pub hop_vout: u32,
    /// The hop output's satoshi value (u64, little-endian on wire).
    pub hop_sats: u64,
    /// The seat's `[2,'low settle']` pubkey (exactly 33 bytes) — the key
    /// the hop output pays (`P2PKH(hash160(seatSettlePubkey))`), which is
    /// what makes this marker chain-checkable at read.
    pub seat_settle_pubkey: Vec<u8>,
    /// The settle key's DER ECDSA signature push (67..=74 bytes) over the
    /// seatsig preimage — carried back verbatim; NEVER verified here.
    pub seat_sig: Vec<u8>,
    /// The identity's DER ECDSA signature push (67..=74 bytes) over the
    /// identity challenge — carried back verbatim; NEVER verified here.
    pub identity_sig: Vec<u8>,
}

/// The EXACT cross-repo seatSig preimage (bsv-low #315; the client's
/// hopparty twin of `potPartySeatSigPreimage`):
///
///   `"LOW/hopparty/v1/seatsig|" ‖ gameId(32) ‖ hopTxid(32) ‖ hopVout(4 LE)
///    ‖ identity(33)`
///
/// `seatSig` is ECDSA over **sha256(preimage)** (single SHA-256 — the
/// BRC-100 `createSignature({data})` hash), made by the settle PRIVATE key
/// (`[2,'low settle']`, keyID = gameId, counterparty = opponentIdentity) —
/// the key whose pubkey the hop output pays. Any verifier checks it as
/// PLAIN ECDSA under the marker's `seatSettlePubkey` — no BRC-42 needed
/// server-side. `None` when `identity` is not 33 bytes (an unbuildable
/// preimage can never verify — fail-safe).
///
/// Binding: {gameId, hopTxid:hopVout, identity}. The identity is IN the
/// preimage, so a thief cannot pair a genuine seatSig with a different
/// identity; gameId + the hop outpoint pin it to one hop. Domain
/// separation: the fixed ASCII tag makes this preimage family disjoint from
/// every potparty preimage and every money digest (BIP-143 sighashes are
/// double-SHA-256 of tx preimages; requesterSig JSON starts with `{`).
pub fn hopparty_seatsig_preimage(
    game_id: &[u8; 32],
    hop_txid: &[u8; 32],
    hop_vout: u32,
    identity: &[u8],
) -> Option<Vec<u8>> {
    if identity.len() != HOPPARTY_IDENTITY_KEY_LEN {
        return None;
    }
    let mut out = Vec::with_capacity(HOPPARTY_SEATSIG_DOMAIN.len() + 32 + 32 + 4 + 33);
    out.extend_from_slice(HOPPARTY_SEATSIG_DOMAIN);
    out.extend_from_slice(game_id);
    out.extend_from_slice(hop_txid);
    out.extend_from_slice(&hop_vout.to_le_bytes());
    out.extend_from_slice(identity);
    Some(out)
}

/// The EXACT cross-repo identitySig challenge (bsv-low #315; the hopparty
/// twin of `potPartyV2Challenge`): the version tag + every field in wire
/// order, minus the two signatures. Signed via BRC-42/43 under protocol
/// `[1,'low potparty']` (REUSED wallet protocol id — the tag bytes at the
/// front are the domain separator), keyID = gameId (lowercase hex),
/// counterparty 'anyone'. `None` when any key field has the wrong length.
pub fn hopparty_identity_challenge(
    identity: &[u8],
    opponent: &[u8],
    game_id: &[u8; 32],
    hop_txid: &[u8; 32],
    hop_vout: u32,
    hop_sats: u64,
    seat_settle_pubkey: &[u8],
) -> Option<Vec<u8>> {
    if identity.len() != HOPPARTY_IDENTITY_KEY_LEN
        || opponent.len() != HOPPARTY_IDENTITY_KEY_LEN
        || seat_settle_pubkey.len() != HOPPARTY_IDENTITY_KEY_LEN
    {
        return None;
    }
    let mut out = Vec::with_capacity(HOPPARTY_TAG.len() + 33 + 33 + 32 + 32 + 4 + 8 + 33);
    out.extend_from_slice(HOPPARTY_TAG);
    out.extend_from_slice(identity);
    out.extend_from_slice(opponent);
    out.extend_from_slice(game_id);
    out.extend_from_slice(hop_txid);
    out.extend_from_slice(&hop_vout.to_le_bytes());
    out.extend_from_slice(&hop_sats.to_le_bytes());
    out.extend_from_slice(seat_settle_pubkey);
    Some(out)
}

/// Walk minimal Bitcoin pushdata out of a byte slice → the pushed blobs, in
/// order, stopping at the first non-push opcode / truncated push — the
/// EXACT `potparty::read_pushes` logic (checked arithmetic throughout: this
/// worker runs on wasm32 where `usize = u32` and a crafted OP_PUSHDATA4
/// length would wrap a naive `i + len` past the bounds guard and panic-trap
/// the `/submit` pass — the inherited adversarial-review MED).
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
        // CHECKED: `i + len` can overflow u32 on wasm32; overflow ⇒ out of
        // bounds ⇒ stop.
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

/// Parse one output locking script as a `LOW/hopparty/v1` marker.
///
/// `Some(marker)` IFF the script is `OP_FALSE OP_RETURN` (0x00 0x6a)
/// followed by EXACTLY [`HOPPARTY_FIELD_COUNT`] pushes with the exact shape
/// (tag == [`HOPPARTY_TAG`]; identity 33; opponentIdentity 33; gameId 32;
/// hopTxid 32; hopVout 4; hopSats 8; seatSettlePubkey 33; seatSig 67..=74;
/// identitySig 67..=74) AND `identity != opponentIdentity`. Everything else
/// — a bare `OP_RETURN`, ANY other tag (both potparty tags included), a
/// wrong length, extra/missing pushes, a self-paired marker — is `None`.
/// Strict tag dispatch is what makes the tag/topic/table split
/// SERVER-ENFORCED (a hopparty marker can never leak into
/// `potparty_records` and vice versa — the ledgered #315 decision-3).
pub fn parse_hopparty_marker(script: &[u8]) -> Option<HoppartyMarker> {
    // OP_FALSE OP_RETURN (0x00 0x6a) — the exact two-byte prefix the app's
    // builder emits (the #188/#230 contract family pins it).
    if script.len() < 2 || script[0] != 0x00 || script[1] != 0x6a {
        return None;
    }
    let data = read_pushes(&script[2..]);
    if data.is_empty() || data[0] != HOPPARTY_TAG {
        return None;
    }
    if data.len() != HOPPARTY_FIELD_COUNT {
        return None;
    }
    let (identity_b, opponent_b) = (data[1], data[2]);
    let (game_id_b, hop_txid_b) = (data[3], data[4]);
    let (hop_vout_b, hop_sats_b) = (data[5], data[6]);
    let (settle_pk_b, seat_sig_b, identity_sig_b) = (data[7], data[8], data[9]);

    if identity_b.len() != HOPPARTY_IDENTITY_KEY_LEN {
        return None;
    }
    if opponent_b.len() != HOPPARTY_IDENTITY_KEY_LEN {
        return None;
    }
    // A hop belongs to a game between two DISTINCT seats — a self-paired
    // marker never parses (the structural rule; the ONLY check beyond byte
    // shape, exactly as potparty).
    if identity_b == opponent_b {
        return None;
    }
    if game_id_b.len() != HOPPARTY_GAME_ID_LEN {
        return None;
    }
    if hop_txid_b.len() != HOPPARTY_TXID_LEN {
        return None;
    }
    if hop_vout_b.len() != HOPPARTY_U32_LEN {
        return None;
    }
    if hop_sats_b.len() != HOPPARTY_U64_LEN {
        return None;
    }
    if settle_pk_b.len() != HOPPARTY_IDENTITY_KEY_LEN {
        return None;
    }
    if !(HOPPARTY_SIG_MIN_LEN..=HOPPARTY_SIG_MAX_LEN).contains(&seat_sig_b.len()) {
        return None;
    }
    if !(HOPPARTY_SIG_MIN_LEN..=HOPPARTY_SIG_MAX_LEN).contains(&identity_sig_b.len()) {
        return None;
    }

    let mut game_id = [0u8; 32];
    game_id.copy_from_slice(game_id_b);
    let mut hop_txid = [0u8; 32];
    hop_txid.copy_from_slice(hop_txid_b);
    let hop_vout = u32::from_le_bytes([hop_vout_b[0], hop_vout_b[1], hop_vout_b[2], hop_vout_b[3]]);
    let hop_sats = u64::from_le_bytes([
        hop_sats_b[0],
        hop_sats_b[1],
        hop_sats_b[2],
        hop_sats_b[3],
        hop_sats_b[4],
        hop_sats_b[5],
        hop_sats_b[6],
        hop_sats_b[7],
    ]);
    Some(HoppartyMarker {
        identity: identity_b.to_vec(),
        opponent: opponent_b.to_vec(),
        game_id,
        hop_txid,
        hop_vout,
        hop_sats,
        seat_settle_pubkey: settle_pk_b.to_vec(),
        seat_sig: seat_sig_b.to_vec(),
        identity_sig: identity_sig_b.to_vec(),
    })
}

/// True iff `script` is a well-formed `LOW/hopparty/v1` marker.
pub fn is_hopparty_marker_script(script: &[u8]) -> bool {
    parse_hopparty_marker(script).is_some()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Minimal Bitcoin pushdata for a byte blob (direct / OP_PUSHDATA1 / _2)
    /// — mirrors `potparty`'s test helper.
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

    /// The app's hopparty-marker builder in bytes.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn marker_script(
        identity: &[u8],
        opponent: &[u8],
        game_id: &[u8; 32],
        hop_txid: &[u8; 32],
        hop_vout: u32,
        hop_sats: u64,
        seat_settle_pubkey: &[u8],
        seat_sig: &[u8],
        identity_sig: &[u8],
    ) -> Vec<u8> {
        let mut s = vec![0x00, 0x6a]; // OP_FALSE OP_RETURN
        s.extend(push_data(HOPPARTY_TAG));
        s.extend(push_data(identity));
        s.extend(push_data(opponent));
        s.extend(push_data(game_id));
        s.extend(push_data(hop_txid));
        s.extend(push_data(&hop_vout.to_le_bytes()));
        s.extend(push_data(&hop_sats.to_le_bytes()));
        s.extend(push_data(seat_settle_pubkey));
        s.extend(push_data(seat_sig));
        s.extend(push_data(identity_sig));
        s
    }

    // ── The golden fixtures (structure-level; the cross-repo vector is
    //    below) ───────────────────────────────────────────────────────────
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
    pub(crate) fn golden_settle_pubkey() -> Vec<u8> {
        let mut k = vec![0x03u8];
        k.extend_from_slice(&[0xc4u8; 32]);
        k
    }
    pub(crate) fn golden_game_id() -> [u8; 32] {
        [0x11u8; 32]
    }
    pub(crate) fn golden_hop_txid() -> [u8; 32] {
        [0x22u8; 32]
    }
    pub(crate) fn golden_vout() -> u32 {
        0
    }
    pub(crate) fn golden_sats() -> u64 {
        80_800
    }
    pub(crate) fn golden_sig() -> Vec<u8> {
        let mut s = vec![0x30u8, 0x45];
        s.extend_from_slice(&[0xabu8; 69]);
        s
    }

    /// A valid marker over the golden identities with chosen gameId / hop.
    pub(crate) fn golden_marker(game_id: &[u8; 32], hop_txid: &[u8; 32], vout: u32) -> Vec<u8> {
        marker_script(
            &golden_identity(),
            &golden_opponent(),
            game_id,
            hop_txid,
            vout,
            golden_sats(),
            &golden_settle_pubkey(),
            &golden_sig(),
            &golden_sig(),
        )
    }

    #[test]
    fn tag_and_domain_shapes() {
        assert_eq!(HOPPARTY_TAG.len(), 15, "same length as the potparty tags");
        assert_eq!(HOPPARTY_TAG, b"LOW/hopparty/v1");
        // Same 24-byte length as LOW/potparty/v2/seatsig| — one fixed
        // preimage layout, two disjoint domains.
        assert_eq!(HOPPARTY_SEATSIG_DOMAIN.len(), 24);
        assert_eq!(
            HOPPARTY_SEATSIG_DOMAIN.len(),
            crate::potparty::POTPARTY_TAG.len() + b"/seatsig|".len() + 0
        );
        assert_ne!(
            HOPPARTY_SEATSIG_DOMAIN,
            b"LOW/potparty/v2/seatsig|".as_slice()
        );
    }

    // ── The FROZEN cross-repo GOLDEN vector (bsv-low #315, Rule 16) ───────
    //
    // The EXACT `OP_RETURN` bytes for a hopparty marker built with
    // `PrivateKey(1)` as the identity and `PrivateKey(2)`'s pubkey as the
    // opponent (both real curve points — BRC-42 derives against them), the
    // same fixed-key recipe as the #230 potparty-v2 vector. Every signature
    // is REAL, RFC6979-deterministic ECDSA over the real preimages —
    // derived at runtime in `golden_vector_derives_and_parses` and asserted
    // byte-identical to this pin, so the pin can never be hand-typed drift.
    // The bsv-low CLIENT pins the identical hex; a drift on either side is
    // caught in CI, not on-chain. NEVER regenerate this to match a parser
    // change — fix the parser to match the contract instead.
    //
    // Fixed inputs: gameId = 32×0xcc, hopTxid = 32×0xdd, hopVout = 1,
    // hopSats = 1_234_567 (little-endian on wire: 87 d6 12 00 00 00 00 00 —
    // a value that proves both LE order and the 8-byte width);
    // seatSettlePubkey = the REAL BRC-42 `[2,'low settle']` derivation
    // (keyID = gameId, counterparty = opponent, forSelf) from
    // PrivateKey(1)'s wallet — exactly what the client wallet derives.
    pub(crate) const GOLDEN_HOPPARTY_HEX: &str = "006a0f4c4f572f686f7070617274792f7631210279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f817982102c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee520cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc20dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd04010000000887d61200000000002103d3e37fc9edbd1c225d703873b45f66368e86c633cb613252b3254ffe0b8ad5ee473045022100d578c4df584ab2bb86a324c902de7a7b38685a0ea25f7d0a27c0d510f516738a02203d8cb847db70748335b4d682faaff769180386ff3cd451cc89b0c0f36c52d484463044022053392b77fab3e27ec719cc23b80bfe76b514dcb9f539c94f978788684d882335022014a89fc59724925d2f6c487711f3210f682784a9719bfaf2357d0d2e38c193ce";

    /// Golden-vector inputs shared by the derive test and the lookup
    /// fixture cell.
    pub(crate) fn golden_vector_inputs() -> (
        bsv_rs::wallet::ProtoWallet, // identity wallet = PrivateKey(1)
        Vec<u8>,                     // opponent pubkey = PrivateKey(2)'s
        [u8; 32],                    // gameId
        [u8; 32],                    // hopTxid
        u32,                         // hopVout
        u64,                         // hopSats
    ) {
        let identity_wallet = bsv_rs::wallet::ProtoWallet::new(Some(
            bsv_rs::primitives::ec::PrivateKey::from_hex(&format!("{:064x}", 1u8)).unwrap(),
        ));
        let opponent_key =
            bsv_rs::primitives::ec::PrivateKey::from_hex(&format!("{:064x}", 2u8)).unwrap();
        let opponent = hex::decode(opponent_key.public_key().to_hex()).unwrap();
        (
            identity_wallet,
            opponent,
            [0xccu8; 32],
            [0xddu8; 32],
            1,
            1_234_567,
        )
    }

    /// Build the golden marker script the way the CLIENT will: real BRC-42
    /// settle-key derivation, real settle-key ECDSA over the seatsig
    /// preimage, real BRC-42 'anyone' identity signature over the identity
    /// challenge (protocol `[1,'low potparty']`, keyID = gameId). All
    /// RFC6979-deterministic, so the output is stable forever.
    pub(crate) fn build_golden_marker() -> Vec<u8> {
        use bsv_rs::wallet::{
            Counterparty, CreateSignatureArgs, GetPublicKeyArgs, Protocol, SecurityLevel,
        };
        let (wallet, opponent, game_id, hop_txid, hop_vout, hop_sats) = golden_vector_inputs();
        let identity = hex::decode(wallet.identity_key_hex()).unwrap();
        let game_id_hex = hex::encode(game_id);
        let opponent_pub =
            bsv_rs::primitives::ec::PublicKey::from_bytes(&opponent).unwrap();

        // The seat's settle pubkey: [2,'low settle'], keyID = gameId,
        // counterparty = opponent, forSelf — the derivation whose pubkey the
        // hop output pays (`stake.ts::POT_SETTLE_PROTOCOL`).
        let settle_protocol = Protocol::new(SecurityLevel::Counterparty, "low settle");
        let settle_pub_hex = wallet
            .get_public_key(GetPublicKeyArgs {
                identity_key: false,
                protocol_id: Some(settle_protocol.clone()),
                key_id: Some(game_id_hex.clone()),
                counterparty: Some(Counterparty::Other(opponent_pub.clone())),
                for_self: Some(true),
            })
            .unwrap()
            .public_key;
        let settle_pub = hex::decode(&settle_pub_hex).unwrap();

        // seatSig: the settle key signs sha256(preimage) — via the SAME
        // wallet derivation (createSignature under [2,'low settle']).
        let preimage =
            hopparty_seatsig_preimage(&game_id, &hop_txid, hop_vout, &identity).unwrap();
        let seat_sig = wallet
            .create_signature(CreateSignatureArgs {
                data: Some(preimage),
                hash_to_directly_sign: None,
                protocol_id: settle_protocol,
                key_id: game_id_hex.clone(),
                counterparty: Some(Counterparty::Other(opponent_pub)),
            })
            .unwrap()
            .signature;

        // identitySig: BRC-42 'anyone' under the REUSED [1,'low potparty']
        // protocol id (the tag in the challenge is the domain separator).
        let challenge = hopparty_identity_challenge(
            &identity,
            &opponent,
            &game_id,
            &hop_txid,
            hop_vout,
            hop_sats,
            &settle_pub,
        )
        .unwrap();
        let identity_sig = wallet
            .create_signature(CreateSignatureArgs {
                data: Some(challenge),
                hash_to_directly_sign: None,
                protocol_id: Protocol::new(SecurityLevel::App, "low potparty"),
                key_id: game_id_hex,
                counterparty: Some(Counterparty::Anyone),
            })
            .unwrap()
            .signature;

        marker_script(
            &identity,
            &opponent,
            &game_id,
            &hop_txid,
            hop_vout,
            hop_sats,
            &settle_pub,
            &seat_sig,
            &identity_sig,
        )
    }

    /// Rule 16 boundary pin, writer side: the runtime-derived golden marker
    /// (real BRC-42 + RFC6979 crypto) must equal the pinned hex
    /// byte-for-byte, AND parse back to the exact fixed inputs. The client
    /// repo pins the identical hex against ITS builder — regenerate both
    /// copies together or not at all.
    #[test]
    fn golden_vector_derives_and_parses() {
        let derived = build_golden_marker();
        assert_eq!(
            hex::encode(&derived),
            GOLDEN_HOPPARTY_HEX,
            "the derived golden marker drifted from the cross-repo pin \
             (wire contract, bsv-low #315) — fix the builder/derivation, \
             never the pin"
        );

        let script = hex::decode(GOLDEN_HOPPARTY_HEX).expect("golden hex decodes");
        let m = parse_hopparty_marker(&script)
            .expect("the GOLDEN vector MUST parse (wire contract, bsv-low #315)");
        // identity = compressed pubkey for PrivateKey(1) = secp256k1 G.
        assert_eq!(
            hex::encode(&m.identity),
            "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
        );
        // opponent = PrivateKey(2)'s pubkey (a REAL point — BRC-42 derives
        // against it).
        assert_eq!(
            hex::encode(&m.opponent),
            "02c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5"
        );
        assert_eq!(m.game_id, [0xccu8; 32]);
        assert_eq!(m.hop_txid, [0xddu8; 32]);
        assert_eq!(m.hop_vout, 1);
        assert_eq!(m.hop_sats, 1_234_567, "8-byte little-endian u64");
        // The settle pubkey from the REAL [2,'low settle'] derivation of
        // PrivateKey(1) against PrivateKey(2) with keyID cc… — the same
        // derivation (same inputs) the #230 v2 golden vector pinned.
        assert_eq!(
            hex::encode(&m.seat_settle_pubkey),
            "03d3e37fc9edbd1c225d703873b45f66368e86c633cb613252b3254ffe0b8ad5ee"
        );
        // Both sigs carried verbatim, inside the DER bounds.
        assert!((67..=74).contains(&m.seat_sig.len()));
        assert!((67..=74).contains(&m.identity_sig.len()));
        assert!(is_hopparty_marker_script(&script));
    }

    /// The golden marker's signatures genuinely VERIFY over the recorded
    /// preimages (positive control for the read-time filter's recipe — the
    /// fixture is real crypto, never decorative hex).
    #[test]
    fn golden_vector_signatures_verify() {
        let script = hex::decode(GOLDEN_HOPPARTY_HEX).unwrap();
        let m = parse_hopparty_marker(&script).unwrap();

        // seatSig: plain ECDSA under seatSettlePubkey over sha256(preimage).
        let preimage =
            hopparty_seatsig_preimage(&m.game_id, &m.hop_txid, m.hop_vout, &m.identity).unwrap();
        let pubkey =
            bsv_rs::primitives::ec::PublicKey::from_bytes(&m.seat_settle_pubkey).unwrap();
        let sig = bsv_rs::primitives::ec::Signature::from_der(&m.seat_sig).unwrap();
        assert!(
            pubkey.verify(&bsv_rs::primitives::hash::sha256(&preimage), &sig),
            "golden seatSig must verify under the marker's own settle pubkey"
        );

        // identitySig: BRC-42 'anyone' verification under the identity.
        let challenge = hopparty_identity_challenge(
            &m.identity,
            &m.opponent,
            &m.game_id,
            &m.hop_txid,
            m.hop_vout,
            m.hop_sats,
            &m.seat_settle_pubkey,
        )
        .unwrap();
        let signer = bsv_rs::primitives::ec::PublicKey::from_bytes(&m.identity).unwrap();
        let valid = bsv_rs::wallet::ProtoWallet::anyone()
            .verify_signature(bsv_rs::wallet::VerifySignatureArgs {
                data: Some(challenge),
                hash_to_directly_verify: None,
                signature: m.identity_sig.clone(),
                protocol_id: bsv_rs::wallet::Protocol::new(
                    bsv_rs::wallet::SecurityLevel::App,
                    "low potparty",
                ),
                key_id: hex::encode(m.game_id),
                counterparty: Some(bsv_rs::wallet::Counterparty::Other(signer)),
                for_self: Some(false),
            })
            .map(|r| r.valid)
            .unwrap_or(false);
        assert!(valid, "golden identitySig must verify with the anyone round-trip");
    }

    // ── Round-trips ───────────────────────────────────────────────────────

    #[test]
    fn valid_marker_round_trips() {
        // Build → parse → every field back, over the full sig-length range
        // (both sig slots).
        for sig_len in [67usize, 68, 70, 71, 72, 74] {
            let script = marker_script(
                &golden_identity(),
                &golden_opponent(),
                &golden_game_id(),
                &golden_hop_txid(),
                7,
                golden_sats(),
                &golden_settle_pubkey(),
                &vec![0x30u8; sig_len],
                &vec![0x30u8; sig_len],
            );
            let m = parse_hopparty_marker(&script)
                .unwrap_or_else(|| panic!("sig len {sig_len} must parse"));
            assert_eq!(m.identity, golden_identity());
            assert_eq!(m.opponent, golden_opponent());
            assert_eq!(m.game_id, golden_game_id());
            assert_eq!(m.hop_txid, golden_hop_txid());
            assert_eq!(m.hop_vout, 7);
            assert_eq!(m.hop_sats, golden_sats());
            assert_eq!(m.seat_settle_pubkey, golden_settle_pubkey());
            assert_eq!(m.seat_sig.len(), sig_len);
            assert_eq!(m.identity_sig.len(), sig_len);
            assert!(is_hopparty_marker_script(&script));
        }
    }

    #[test]
    fn le_encoding_of_vout_and_sats() {
        // Distinctive values prove LITTLE-endian decode and the 4/8 widths.
        let vout = 0x0403_0201u32; // bytes 01 02 03 04 on the wire
        let sats = 0x0807_0605_0403_0201u64; // bytes 01..08 on the wire
        let script = marker_script(
            &golden_identity(),
            &golden_opponent(),
            &golden_game_id(),
            &golden_hop_txid(),
            vout,
            sats,
            &golden_settle_pubkey(),
            &golden_sig(),
            &golden_sig(),
        );
        let m = parse_hopparty_marker(&script).unwrap();
        assert_eq!(m.hop_vout, vout);
        assert_eq!(m.hop_sats, sats);
        // Confirm the raw wire bytes are LE: the 8-byte sats push 01..08.
        assert!(
            script
                .windows(8)
                .any(|w| w == [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]),
            "hopSats must be stored little-endian over 8 bytes"
        );
    }

    /// PARITY NOTE (matching potparty's strictness EXACTLY): the pushdata
    /// walker accepts a NON-MINIMAL push encoding (OP_PUSHDATA1 for a short
    /// blob) just as `potparty::read_pushes` does — the "minimal pushes"
    /// language in both contracts describes what the BUILDER emits, and the
    /// parser pins tag/count/lengths, not push-opcode minimality. Pinned
    /// here so a future "tighten the parser" change is a deliberate
    /// cross-repo version bump, not a silent drift from the sibling.
    #[test]
    fn non_minimal_push_parity_with_potparty() {
        // Rebuild the golden marker but encode the 32-byte gameId push with
        // OP_PUSHDATA1 (0x4c 0x20 …) instead of the minimal direct push.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(HOPPARTY_TAG));
        s.extend(push_data(&golden_identity()));
        s.extend(push_data(&golden_opponent()));
        s.push(0x4c);
        s.push(32);
        s.extend_from_slice(&golden_game_id());
        s.extend(push_data(&golden_hop_txid()));
        s.extend(push_data(&golden_vout().to_le_bytes()));
        s.extend(push_data(&golden_sats().to_le_bytes()));
        s.extend(push_data(&golden_settle_pubkey()));
        s.extend(push_data(&golden_sig()));
        s.extend(push_data(&golden_sig()));
        assert!(
            parse_hopparty_marker(&s).is_some(),
            "non-minimal pushes parse (potparty parity)"
        );
        // And potparty behaves the same on its own shape (the parity claim
        // calls BOTH surfaces — Rule 10).
        let mut p = vec![0x00, 0x6a];
        p.extend(push_data(crate::potparty::POTPARTY_TAG));
        p.extend(push_data(&golden_identity()));
        p.extend(push_data(&golden_opponent()));
        p.push(0x4c);
        p.push(32);
        p.extend_from_slice(&golden_game_id());
        p.extend(push_data(&golden_hop_txid()));
        p.extend(push_data(&golden_vout().to_le_bytes()));
        p.extend(push_data(&golden_vout().to_le_bytes()));
        p.extend(push_data(&golden_sig()));
        assert!(crate::potparty::parse_potparty_marker(&p).is_some());
    }

    // ── Rejections ────────────────────────────────────────────────────────

    #[test]
    fn self_paired_marker_rejected() {
        let script = marker_script(
            &golden_identity(),
            &golden_identity(), // opponent slot = the SAME key
            &golden_game_id(),
            &golden_hop_txid(),
            golden_vout(),
            golden_sats(),
            &golden_settle_pubkey(),
            &golden_sig(),
            &golden_sig(),
        );
        assert!(parse_hopparty_marker(&script).is_none());
    }

    /// The CROSS-LEAK refusals, BOTH directions (the ledgered #315
    /// server-enforced tag separation): a potparty tag on the hopparty
    /// 10-push shape never parses as hopparty, and the hopparty golden
    /// marker never parses as potparty. Every foreign tag refused.
    #[test]
    fn wrong_tag_rejected_including_both_potparty_tags() {
        for bad_tag in [
            b"LOW/potparty/v1".as_slice(), // the potparty v1 tag
            b"LOW/potparty/v2".as_slice(), // the potparty v2 tag (same 10-push count!)
            b"LOW/hopparty/v2".as_slice(), // a future version is NOT this parser's
            b"LOW/hopparty/v".as_slice(),  // 14 bytes
            b"SOMETHINGELSE!!".as_slice(), // foreign 15-byte tag
        ] {
            let mut s = vec![0x00, 0x6a];
            s.extend(push_data(bad_tag));
            s.extend(push_data(&golden_identity()));
            s.extend(push_data(&golden_opponent()));
            s.extend(push_data(&golden_game_id()));
            s.extend(push_data(&golden_hop_txid()));
            s.extend(push_data(&golden_vout().to_le_bytes()));
            s.extend(push_data(&golden_sats().to_le_bytes()));
            s.extend(push_data(&golden_settle_pubkey()));
            s.extend(push_data(&golden_sig()));
            s.extend(push_data(&golden_sig()));
            assert!(
                parse_hopparty_marker(&s).is_none(),
                "tag {bad_tag:?} must be rejected by the hopparty parser"
            );
        }
        // The reverse leak: a well-formed hopparty marker must be None to
        // the POTPARTY parser (its 10-push count matches v2's, so the tag
        // dispatch is the only wall — prove it holds).
        let hop = golden_marker(&golden_game_id(), &golden_hop_txid(), 0);
        assert!(
            crate::potparty::parse_potparty_marker(&hop).is_none(),
            "a hopparty marker must never parse as potparty"
        );
        // And well-formed potparty v1/v2 markers are None to THIS parser.
        let pp_v1 = crate::potparty::tests::golden_marker(&golden_game_id(), &golden_hop_txid(), 0);
        assert!(parse_hopparty_marker(&pp_v1).is_none());
        let pp_v2 =
            crate::potparty::tests::golden_marker_v2(&golden_game_id(), &golden_hop_txid(), 0);
        assert!(parse_hopparty_marker(&pp_v2).is_none());
        // Positive control for the refusal legs above: the same 10-push body
        // under the RIGHT tag parses (the gate is reached and cleared).
        assert!(parse_hopparty_marker(&hop).is_some());
    }

    #[test]
    fn non_op_return_rejected() {
        let p2pkh = hex::decode("76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac").unwrap();
        assert!(parse_hopparty_marker(&p2pkh).is_none());
        assert!(parse_hopparty_marker(&[]).is_none());
        assert!(parse_hopparty_marker(&[0x00]).is_none());
        // A bare OP_RETURN (0x6a without OP_FALSE) is NOT the prefix.
        let script = golden_marker(&golden_game_id(), &golden_hop_txid(), golden_vout());
        assert!(parse_hopparty_marker(&script[1..]).is_none());
    }

    #[test]
    fn wrong_key_lengths_rejected() {
        for len in [32usize, 34, 65] {
            // Bad identity length.
            let s = marker_script(
                &vec![0x02u8; len],
                &golden_opponent(),
                &golden_game_id(),
                &golden_hop_txid(),
                golden_vout(),
                golden_sats(),
                &golden_settle_pubkey(),
                &golden_sig(),
                &golden_sig(),
            );
            assert!(parse_hopparty_marker(&s).is_none(), "identity len {len}");
            // Bad opponent length.
            let s = marker_script(
                &golden_identity(),
                &vec![0x03u8; len],
                &golden_game_id(),
                &golden_hop_txid(),
                golden_vout(),
                golden_sats(),
                &golden_settle_pubkey(),
                &golden_sig(),
                &golden_sig(),
            );
            assert!(parse_hopparty_marker(&s).is_none(), "opponent len {len}");
            // Bad settle-pubkey length.
            let s = marker_script(
                &golden_identity(),
                &golden_opponent(),
                &golden_game_id(),
                &golden_hop_txid(),
                golden_vout(),
                golden_sats(),
                &vec![0x02u8; len],
                &golden_sig(),
                &golden_sig(),
            );
            assert!(parse_hopparty_marker(&s).is_none(), "settle pk len {len}");
        }
    }

    #[test]
    fn wrong_game_id_or_txid_length_rejected() {
        // gameId 31 bytes.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(HOPPARTY_TAG));
        s.extend(push_data(&golden_identity()));
        s.extend(push_data(&golden_opponent()));
        s.extend(push_data(&[0x11u8; 31]));
        s.extend(push_data(&golden_hop_txid()));
        s.extend(push_data(&golden_vout().to_le_bytes()));
        s.extend(push_data(&golden_sats().to_le_bytes()));
        s.extend(push_data(&golden_settle_pubkey()));
        s.extend(push_data(&golden_sig()));
        s.extend(push_data(&golden_sig()));
        assert!(parse_hopparty_marker(&s).is_none());
        // hopTxid 33 bytes.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(HOPPARTY_TAG));
        s.extend(push_data(&golden_identity()));
        s.extend(push_data(&golden_opponent()));
        s.extend(push_data(&golden_game_id()));
        s.extend(push_data(&[0x22u8; 33]));
        s.extend(push_data(&golden_vout().to_le_bytes()));
        s.extend(push_data(&golden_sats().to_le_bytes()));
        s.extend(push_data(&golden_settle_pubkey()));
        s.extend(push_data(&golden_sig()));
        s.extend(push_data(&golden_sig()));
        assert!(parse_hopparty_marker(&s).is_none());
    }

    #[test]
    fn wrong_int_widths_rejected() {
        // hopVout as 3 / 5 / 8 bytes (8 would be the SATS width — a swapped
        // field order must not parse).
        for len in [3usize, 5, 8] {
            let mut s = vec![0x00, 0x6a];
            s.extend(push_data(HOPPARTY_TAG));
            s.extend(push_data(&golden_identity()));
            s.extend(push_data(&golden_opponent()));
            s.extend(push_data(&golden_game_id()));
            s.extend(push_data(&golden_hop_txid()));
            s.extend(push_data(&vec![0x01u8; len])); // vout: wrong width
            s.extend(push_data(&golden_sats().to_le_bytes()));
            s.extend(push_data(&golden_settle_pubkey()));
            s.extend(push_data(&golden_sig()));
            s.extend(push_data(&golden_sig()));
            assert!(parse_hopparty_marker(&s).is_none(), "vout width {len}");
        }
        // hopSats as 4 bytes (the u32 width — the one most tempting drift).
        for len in [4usize, 7, 9] {
            let mut s = vec![0x00, 0x6a];
            s.extend(push_data(HOPPARTY_TAG));
            s.extend(push_data(&golden_identity()));
            s.extend(push_data(&golden_opponent()));
            s.extend(push_data(&golden_game_id()));
            s.extend(push_data(&golden_hop_txid()));
            s.extend(push_data(&golden_vout().to_le_bytes()));
            s.extend(push_data(&vec![0x01u8; len])); // sats: wrong width
            s.extend(push_data(&golden_settle_pubkey()));
            s.extend(push_data(&golden_sig()));
            s.extend(push_data(&golden_sig()));
            assert!(parse_hopparty_marker(&s).is_none(), "sats width {len}");
        }
    }

    #[test]
    fn sig_lengths_out_of_range_rejected() {
        for len in [0usize, 1, 66, 75, 100] {
            // seatSig out of range.
            let s = marker_script(
                &golden_identity(),
                &golden_opponent(),
                &golden_game_id(),
                &golden_hop_txid(),
                golden_vout(),
                golden_sats(),
                &golden_settle_pubkey(),
                &vec![0x30u8; len],
                &golden_sig(),
            );
            assert!(parse_hopparty_marker(&s).is_none(), "seatSig len {len}");
            // identitySig out of range.
            let s = marker_script(
                &golden_identity(),
                &golden_opponent(),
                &golden_game_id(),
                &golden_hop_txid(),
                golden_vout(),
                golden_sats(),
                &golden_settle_pubkey(),
                &golden_sig(),
                &vec![0x30u8; len],
            );
            assert!(parse_hopparty_marker(&s).is_none(), "identitySig len {len}");
        }
    }

    /// Push-count refusals: 8, 9 and 11 pushes (the spec'd cells). Every
    /// prefix of the ten-push shape is rejected, and an 11th push is too.
    #[test]
    fn wrong_push_counts_rejected() {
        let pushes: Vec<Vec<u8>> = vec![
            push_data(HOPPARTY_TAG),
            push_data(&golden_identity()),
            push_data(&golden_opponent()),
            push_data(&golden_game_id()),
            push_data(&golden_hop_txid()),
            push_data(&golden_vout().to_le_bytes()),
            push_data(&golden_sats().to_le_bytes()),
            push_data(&golden_settle_pubkey()),
            push_data(&golden_sig()),
        ];
        // 1..=9 pushes (incl. the 8- and 9-push cases): all rejected.
        let mut s = vec![0x00, 0x6a];
        for p in &pushes {
            s.extend_from_slice(p);
            assert!(
                parse_hopparty_marker(&s).is_none(),
                "a marker with fewer than 10 pushes must be rejected"
            );
        }
        // 11 pushes: the golden marker + one extra.
        let mut s = golden_marker(&golden_game_id(), &golden_hop_txid(), golden_vout());
        s.extend(push_data(&[0x99u8; 4]));
        assert!(parse_hopparty_marker(&s).is_none(), "an 11th push is not the format");
    }

    #[test]
    fn truncated_push_rejected() {
        // Claim a 70-byte identitySig push but truncate the bytes.
        let mut s = vec![0x00, 0x6a];
        s.extend(push_data(HOPPARTY_TAG));
        s.extend(push_data(&golden_identity()));
        s.extend(push_data(&golden_opponent()));
        s.extend(push_data(&golden_game_id()));
        s.extend(push_data(&golden_hop_txid()));
        s.extend(push_data(&golden_vout().to_le_bytes()));
        s.extend(push_data(&golden_sats().to_le_bytes()));
        s.extend(push_data(&golden_settle_pubkey()));
        s.extend(push_data(&golden_sig()));
        s.push(0x46); // push 70…
        s.extend_from_slice(&[0xab; 10]); // …but only 10 bytes follow
        assert!(parse_hopparty_marker(&s).is_none());
    }

    #[test]
    fn adversarial_pushdata_len_never_panics_or_wraps() {
        // Inherited from potparty/result/collected: an OP_PUSHDATA4 with a
        // wrapping length on wasm32 must parse to None, never panic.
        for script in [
            vec![0x00u8, 0x6a, 0x4e, 0xff, 0xff, 0xff, 0xff],
            vec![0x00u8, 0x6a, 0x4d, 0xff, 0xff],
            vec![0x00u8, 0x6a, 0x4e, 0xff, 0xff, 0xff],
            vec![0x00u8, 0x6a, 0x4b],
        ] {
            assert_eq!(parse_hopparty_marker(&script), None);
        }
        assert!(read_pushes(&[0x4e, 0xff, 0xff, 0xff, 0xff]).is_empty());
        assert!(read_pushes(&[0x4d, 0xff, 0xff]).is_empty());
    }

    // ── Digest builders ───────────────────────────────────────────────────

    #[test]
    fn seatsig_preimage_layout_and_domain_separation() {
        let p = hopparty_seatsig_preimage(&golden_game_id(), &golden_hop_txid(), 7, &golden_identity())
            .unwrap();
        assert_eq!(p.len(), 24 + 32 + 32 + 4 + 33, "fixed 125-byte layout");
        assert!(p.starts_with(HOPPARTY_SEATSIG_DOMAIN));
        assert_eq!(&p[24..56], &golden_game_id());
        assert_eq!(&p[56..88], &golden_hop_txid());
        assert_eq!(&p[88..92], &7u32.to_le_bytes());
        assert_eq!(&p[92..125], golden_identity().as_slice());
        // A malformed identity can never build a preimage (fail-safe).
        assert!(
            hopparty_seatsig_preimage(&golden_game_id(), &golden_hop_txid(), 7, &[0x02; 32])
                .is_none()
        );
        // Same layout as potparty-v2's seatsig preimage, DIFFERENT domain:
        // the two families can never collide byte-for-byte.
        assert_ne!(&p[..24], b"LOW/potparty/v2/seatsig|".as_slice());
    }

    #[test]
    fn identity_challenge_layout() {
        let c = hopparty_identity_challenge(
            &golden_identity(),
            &golden_opponent(),
            &golden_game_id(),
            &golden_hop_txid(),
            7,
            golden_sats(),
            &golden_settle_pubkey(),
        )
        .unwrap();
        assert_eq!(c.len(), 15 + 33 + 33 + 32 + 32 + 4 + 8 + 33);
        assert!(c.starts_with(HOPPARTY_TAG), "the tag IS the domain separator");
        assert_eq!(&c[15..48], golden_identity().as_slice());
        assert_eq!(&c[48..81], golden_opponent().as_slice());
        assert_eq!(&c[81..113], &golden_game_id());
        assert_eq!(&c[113..145], &golden_hop_txid());
        assert_eq!(&c[145..149], &7u32.to_le_bytes());
        assert_eq!(&c[149..157], &golden_sats().to_le_bytes());
        assert_eq!(&c[157..190], golden_settle_pubkey().as_slice());
        // Malformed key lengths can never build a challenge.
        assert!(hopparty_identity_challenge(
            &[0x02; 32],
            &golden_opponent(),
            &golden_game_id(),
            &golden_hop_txid(),
            7,
            golden_sats(),
            &golden_settle_pubkey(),
        )
        .is_none());
    }
}
