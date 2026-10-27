//! bsv-low #848 (FAST-C; #461 design C, section 3.3; the owner's ruling, record 1117): the four funding-moment
//! filings under ONE signature per key, `POST /record?kind=handfile`.
//!
//! At funding a seat filed four markers, each identity-signed and two of them settle-key-signed as well:
//! `hopparty` (the hop-in-flight marker), `potparty` v1, `potparty` v2 and `potrefund`, six wallet signatures. The
//! `handfile` family carries the four families' CANONICAL BYTES UNSIGNED (each family's own identity CHALLENGE, the
//! bytes its identity signature signs today, byte for byte the client's `potPartyChallenge`,
//! `potPartyV2Challenge`, `hopPartyChallenge` and `potRefundChallenge`) plus ONE identity signature under the
//! potparty protocol (`[1,'low potparty']`, keyID the gameId as today) and ONE settle-key signature, both over
//! `sha256(preimage)` where [`handfile_preimage`] is `"low/handfile/v1" || hopTxid || len || p0 || .. || len || p3`.
//!
//! This door:
//!   1. checks the two signatures' encoding through the one canonical-DER gate the four families use
//!      (`potparty::validity::canonical_der`): a padded or high-S twin of an honest batch is not a second batch;
//!   2. parses every payload with its FAMILY'S OWN PARSER: the payload's fields are laid out as the family's marker
//!      script with the batch's signatures in the signature pushes and run through `parse_hopparty_marker`,
//!      `parse_potparty_marker` and `parse_potrefund_marker`, then the family's own challenge builder must give the
//!      payload back byte for byte; a payload any of them refuses refuses the WHOLE batch and nothing is written;
//!   3. holds the batch to ONE seat's ONE hand: one identity (the poster), one opponent, one game, one pot
//!      outpoint, one recovery height, one settle key, or the whole batch is refused; after the consistency and
//!      poster checks, verifies the identity signature and then the settle-key signature over the batch;
//!   4. binds the refund exactly as the direct door does (`bind_refund_backup`: the pot's pre-signed spend by both
//!      committed settle keys in the template's unlock shape, `settle_signers_for_spend == Coop`; an unindexed pot
//!      files at rank 0, as today);
//!   5. reads the HOP's container facts from the index's own copy of the hop (`pot_beefs`, the bytes `tm_lowfund`
//!      admitted): the output at `hopVout` must be the P2PKH of the batch's settle key with the sats the payload
//!      names, else the batch is refused (a free row that the chain refutes is never written); a hop the index does
//!      not hold yet is 425 (the client's outbox re-files, the `hopsweep` precedent);
//!   6. EXPLODES the batch: each payload becomes a row of its family's own table exactly as a filed row, keyed by
//!      the SAME content key a direct filing of that marker gets (`filed:<sha256 of the family tag and the signed
//!      fields>[..56]`), so a direct filing and a handfile of one marker are one row (`INSERT OR IGNORE`); the
//!      hopparty row lives under the hop's txid at a handfile `outputIndex`
//!      ([`overlay_discovery::hopparty::HANDFILE_MARKER_VOUT_BASE`]) so the derived links and seats join it as a
//!      chain marker; the potparty v2 row's `sigValid` and the hopparty row's `markerValid` are written `1` only
//!      because both batch signatures verified (and, for the hop, the container facts agreed), the write-time
//!      verdict every reader of the K binding (`/owed`'s `sigValid = 1 AND seatSettlePubkey IS NOT NULL`) reads
//!      off the row, so an exploded row reads exactly as a directly filed one;
//!   7. applies EVERY cap per family before any write (per poster, game and pot: potparty's 2, potrefund's
//!      1; per poster and hop outpoint: hopparty's 1; and the day's 200 per poster and family), and the chain no-op per family (a
//!      verified chain marker already indexed for this identity, game and pot writes nothing for that family);
//!   8. stores the batch itself (`handfile_batches`, keyed `handfile:<sha256 of the preimage>`) and each exploded
//!      row's place in it (`handfile_rows`), so a reader can re-verify any one row: a served row may carry the
//!      additive `batch {key, hopTxid, payloads, sigs, index}` ([`HandfileBatchRef::json`]), the ONE wire delta;
//!      a row filed directly never carries it, so an old row's bytes are unchanged.
//!
//! Idempotent by CONTENT: the same four payloads under the same hop txid make the same digest, so a re-signed or
//! re-posted batch that changes nothing answers `filed: false, alreadyFiled: true`, with the same key and rows.
//! It marks nothing stale and recomputes nothing. A later refund-verdict promotion is a change.
//!
//! What the batch does NOT change, stated: the client's existing direct filings keep working (the door's other
//! kinds are untouched); the chain's own markers are read as before; the owed list's evidence rule reads the
//! write-time verdict columns, which this door sets from the batch's signatures; the overlay's relatch passes
//! (#355), which recompute a row's verdict from its OWN signature columns, leave a door-filed row alone (its
//! columns hold the batch's signatures; `d1_discovery`'s `relatch_if_changed` on `filed:` keys and handfile
//! vouts), else a sweep would demote every exploded row to 0.
use serde::Deserialize;

use crate::record_post::{
    content_key, potparty_content_key, potrefund_content_key, refund_spends_pot, RecordRefusal,
    RECORD_POST_MAX_SCRIPT_BYTES,
};
use overlay_discovery::hopparty::validity::expected_hop_lock_hex;
use overlay_discovery::hopparty::{
    hopparty_identity_challenge, parse_hopparty_marker, HoppartyMarker, HANDFILE_MARKER_VOUT_BASE,
    HOPPARTY_TAG,
};
use overlay_discovery::potparty::validity::{
    canonical_der, potparty_v1_challenge, potparty_v2_challenge, verify_identity_sig,
};
use overlay_discovery::potparty::{
    parse_potparty_marker, storage::PotpartyRecord, PotpartyMarker, POTPARTY_TAG, POTPARTY_TAG_V2,
};
use overlay_discovery::potrefund::{
    parse_potrefund_marker, storage::PotrefundRecord, PotrefundMarker, POTREFUND_TAG,
};

/// The digest's domain tag (the design's `"low/handfile/v1"`).
pub const HANDFILE_DOMAIN: &[u8] = b"low/handfile/v1";
/// The payloads a batch carries, in order: hopparty, potparty v1, potparty v2, potrefund.
pub const HANDFILE_PAYLOADS: usize = 4;
/// The batch key's prefix: never 64 hex, never `filed:`, so no reader mistakes it for a txid or a row key.
pub const HANDFILE_KEY_PREFIX: &str = "handfile:";
/// The family names a `handfile_rows` row carries (the table the row lives in).
pub const HANDFILE_FAMILY_HOPPARTY: &str = "hopparty";
pub const HANDFILE_FAMILY_POTPARTY: &str = "potparty";
pub const HANDFILE_FAMILY_POTREFUND: &str = "potrefund";
/// FILED hopparty rows one poster may hold per hop outpoint, across games: the honest need is one.
pub const HOPPARTY_FILED_ROWS_CAP: i64 = 1;

/// The hopparty identity challenge's fixed length: tag 15, identity 33, opponent 33, gameId 32, hopVout 4,
/// hopSats 8, settle 33.
pub const HOPPARTY_PAYLOAD_LEN: usize = 15 + 33 + 33 + 32 + 4 + 8 + 33;
/// The potparty v1 challenge's fixed length: tag 15, identity 33, opponent 33, gameId 32, potTxid 32, potVout 4,
/// recoveryHeight 4.
pub const POTPARTY_V1_PAYLOAD_LEN: usize = 15 + 33 + 33 + 32 + 32 + 4 + 4;
/// The potparty v2 challenge's fixed length: v1's plus the settle key.
pub const POTPARTY_V2_PAYLOAD_LEN: usize = POTPARTY_V1_PAYLOAD_LEN + 33;
/// The potrefund challenge's head: tag 16, identity 33, gameId 32, potTxid 32, potVout 4; the refund raw follows.
pub const POTREFUND_PAYLOAD_HEAD: usize = 16 + 33 + 32 + 32 + 4;

/// The request body of `POST /record?kind=handfile`.
#[derive(Debug, Clone, Deserialize)]
pub struct HandfileBody {
    /// The funding hop's txid (64 hex): the container the hopparty marker would have ridden.
    #[serde(rename = "hopTxid")]
    pub hop_txid: String,
    /// The four canonical payloads, hex, in order: hopparty, potparty v1, potparty v2, potrefund.
    pub payloads: Vec<String>,
    /// The identity's signature over `sha256(preimage)` under `[1,'low potparty']`, keyID the gameId (hex DER).
    #[serde(rename = "identitySigHex")]
    pub identity_sig_hex: String,
    /// The seat's settle key's plain ECDSA signature over `sha256(preimage)` (hex DER).
    #[serde(rename = "settleSigHex")]
    pub settle_sig_hex: String,
}

/// The signed bytes: the domain tag, the hop txid (its 32 bytes in display order), then each payload behind a
/// 4-byte little-endian length. The lengths make the framing explicit (the potrefund payload's tail is the refund
/// raw, of any length up to its cap).
pub fn handfile_preimage(hop_txid: &[u8; 32], payloads: &[&[u8]; HANDFILE_PAYLOADS]) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        HANDFILE_DOMAIN.len() + 32 + payloads.iter().map(|p| p.len() + 4).sum::<usize>(),
    );
    out.extend_from_slice(HANDFILE_DOMAIN);
    out.extend_from_slice(hop_txid);
    for p in payloads {
        out.extend_from_slice(&(p.len() as u32).to_le_bytes());
        out.extend_from_slice(p);
    }
    out
}

/// The batch's key: the sha256 of its preimage, hex, behind [`HANDFILE_KEY_PREFIX`]. Signatures excluded, so a
/// re-signed batch is the same batch.
pub fn handfile_key(preimage: &[u8]) -> String {
    format!(
        "{HANDFILE_KEY_PREFIX}{}",
        hex::encode(bsv_rs::primitives::hash::sha256(preimage))
    )
}

/// A canonically encoded marker script over the pushes (what every client builder emits; the door's
/// `canonical_marker_pushes` accepts exactly this).
pub fn canonical_script(pushes: &[&[u8]]) -> Vec<u8> {
    let mut out = vec![0x00, 0x6a];
    for data in pushes {
        let n = data.len();
        if n < 0x4c {
            out.push(n as u8);
        } else if n <= 0xff {
            out.push(0x4c);
            out.push(n as u8);
        } else if n <= 0xffff {
            out.push(0x4d);
            out.extend_from_slice(&(n as u16).to_le_bytes());
        } else {
            out.push(0x4e);
            out.extend_from_slice(&(n as u32).to_le_bytes());
        }
        out.extend_from_slice(data);
    }
    out
}

/// A hopparty payload (the family's identity challenge) as the marker it stands for, the batch's signatures in
/// its two signature pushes: the family's own parser judges it, then the family's challenge builder must give the
/// payload back byte for byte.
pub fn hopparty_marker_of_payload(
    payload: &[u8],
    seat_sig: &[u8],
    identity_sig: &[u8],
) -> Option<HoppartyMarker> {
    if payload.len() != HOPPARTY_PAYLOAD_LEN || !payload.starts_with(HOPPARTY_TAG) {
        return None;
    }
    let f = &payload[HOPPARTY_TAG.len()..];
    let (identity, opponent, game_id) = (&f[0..33], &f[33..66], &f[66..98]);
    let (hop_vout, hop_sats, settle) = (&f[98..102], &f[102..110], &f[110..143]);
    let script = canonical_script(&[
        HOPPARTY_TAG,
        identity,
        opponent,
        game_id,
        hop_vout,
        hop_sats,
        settle,
        seat_sig,
        identity_sig,
    ]);
    let m = parse_hopparty_marker(&script)?;
    let rebuilt = hopparty_identity_challenge(
        &m.identity,
        &m.opponent,
        &m.game_id,
        m.hop_vout,
        m.hop_sats,
        &m.seat_settle_pubkey,
    )?;
    (rebuilt == payload).then_some(m)
}

/// A potparty payload (v1 or v2 by its tag) as the marker it stands for; the batch's identity signature in `sig`
/// and, for v2, the settle signature in `seatSig`.
pub fn potparty_marker_of_payload(
    payload: &[u8],
    seat_sig: &[u8],
    identity_sig: &[u8],
) -> Option<PotpartyMarker> {
    let v2 = if payload.len() == POTPARTY_V1_PAYLOAD_LEN && payload.starts_with(POTPARTY_TAG) {
        false
    } else if payload.len() == POTPARTY_V2_PAYLOAD_LEN && payload.starts_with(POTPARTY_TAG_V2) {
        true
    } else {
        return None;
    };
    let f = &payload[POTPARTY_TAG.len()..];
    let (identity, opponent, game_id, pot_txid) = (&f[0..33], &f[33..66], &f[66..98], &f[98..130]);
    let (pot_vout, height) = (&f[130..134], &f[134..138]);
    let script = if v2 {
        canonical_script(&[
            POTPARTY_TAG_V2,
            identity,
            opponent,
            game_id,
            pot_txid,
            pot_vout,
            height,
            &f[138..171],
            seat_sig,
            identity_sig,
        ])
    } else {
        canonical_script(&[
            POTPARTY_TAG,
            identity,
            opponent,
            game_id,
            pot_txid,
            pot_vout,
            height,
            identity_sig,
        ])
    };
    let m = parse_potparty_marker(&script)?;
    let rebuilt = match &m.seat_settle_pubkey {
        Some(settle) => potparty_v2_challenge(
            &m.identity,
            &m.opponent,
            &m.game_id,
            &m.pot_txid,
            m.pot_vout,
            m.recovery_height,
            settle,
        )?,
        None => potparty_v1_challenge(
            &m.identity,
            &m.opponent,
            &m.game_id,
            &m.pot_txid,
            m.pot_vout,
            m.recovery_height,
        )?,
    };
    (rebuilt == payload).then_some(m)
}

/// A potrefund payload as the marker it stands for, the batch's identity signature in its signature push.
pub fn potrefund_marker_of_payload(payload: &[u8], identity_sig: &[u8]) -> Option<PotrefundMarker> {
    if payload.len() <= POTREFUND_PAYLOAD_HEAD || !payload.starts_with(POTREFUND_TAG) {
        return None;
    }
    let f = &payload[POTREFUND_TAG.len()..];
    let (identity, game_id, pot_txid, pot_vout) = (&f[0..33], &f[33..65], &f[65..97], &f[97..101]);
    let refund_raw = &f[101..];
    let script = canonical_script(&[
        POTREFUND_TAG,
        identity,
        game_id,
        pot_txid,
        pot_vout,
        refund_raw,
        identity_sig,
    ]);
    let m = parse_potrefund_marker(&script)?;
    let rebuilt = crate::record_post::potrefund_challenge(
        &m.identity,
        &m.game_id,
        &m.pot_txid,
        m.pot_vout,
        &m.refund_raw,
    );
    (rebuilt == payload).then_some(m)
}

/// The hopparty row's content key: the family tag and the signed fields, the HOP TXID first (the container the
/// chain's marker takes for granted), signatures excluded.
pub fn hopparty_content_key(m: &HoppartyMarker, hop_txid: &[u8; 32]) -> String {
    let vout = m.hop_vout.to_le_bytes();
    let sats = m.hop_sats.to_le_bytes();
    content_key(
        HOPPARTY_TAG,
        &[
            hop_txid,
            &m.identity,
            &m.opponent,
            &m.game_id,
            &vout,
            &sats,
            &m.seat_settle_pubkey,
        ],
    )
}

/// The `outputIndex` a handfile hopparty row takes: the base bit and the low 31 bits of the sha256 of its
/// content key, so the row is keyed by its content under the hop's txid.
pub fn handfile_hop_marker_vout(hop_content_key: &str) -> u32 {
    let h = bsv_rs::primitives::hash::sha256(hop_content_key.as_bytes());
    HANDFILE_MARKER_VOUT_BASE | (u32::from_be_bytes([h[0], h[1], h[2], h[3]]) & 0x7fff_ffff)
}

/// A verified batch: what the exploder writes.
#[derive(Debug, Clone, PartialEq)]
pub struct VerifiedHandfile {
    /// `handfile:<sha256 of the preimage>`.
    pub key: String,
    /// The poster (lowercase hex), the one identity every payload names.
    pub identity: String,
    pub game_id: String,
    pub hop_txid: String,
    /// The hopparty marker as the batch states it, the batch's signatures in its signature fields.
    pub hopparty: HoppartyMarker,
    /// The hopparty row's `outputIndex` (a handfile vout) and the content key it was derived from.
    pub hop_row_vout: u32,
    pub hop_content_key: String,
    pub potparty_v1: PotpartyRecord,
    pub potparty_v2: PotpartyRecord,
    pub potrefund: PotrefundRecord,
    pub payloads_hex: [String; HANDFILE_PAYLOADS],
    pub identity_sig_hex: String,
    pub settle_sig_hex: String,
}

fn hex32(s: &str) -> Option<[u8; 32]> {
    let b = hex::decode(s).ok()?;
    <[u8; 32]>::try_from(b.as_slice()).ok()
}

/// The door's verification (steps 1 to 3 of the module doc, and the refund's cheap spend check): everything that
/// needs no store. The caller then binds the refund to the pot and the hop to its container.
pub fn verify_handfile(
    body: &HandfileBody,
    poster_lc: &str,
) -> std::result::Result<VerifiedHandfile, RecordRefusal> {
    let hop_txid_lc = body.hop_txid.trim().to_ascii_lowercase();
    let hop_txid = hex32(&hop_txid_lc).ok_or(RecordRefusal::HandfileShape)?;
    if body.payloads.len() != HANDFILE_PAYLOADS {
        return Err(RecordRefusal::HandfileShape);
    }
    let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(HANDFILE_PAYLOADS);
    for p in &body.payloads {
        let p = p.trim();
        if p.len() > RECORD_POST_MAX_SCRIPT_BYTES * 2 {
            return Err(RecordRefusal::ScriptTooLarge);
        }
        payloads.push(hex::decode(p).map_err(|_| RecordRefusal::HandfileShape)?);
    }
    let identity_sig =
        hex::decode(body.identity_sig_hex.trim()).map_err(|_| RecordRefusal::HandfileShape)?;
    let settle_sig =
        hex::decode(body.settle_sig_hex.trim()).map_err(|_| RecordRefusal::HandfileShape)?;
    // 1. the one canonical-DER gate, before anything is parsed
    if canonical_der(&identity_sig).is_none() {
        return Err(RecordRefusal::SignatureInvalid);
    }
    if canonical_der(&settle_sig).is_none() {
        return Err(RecordRefusal::SettleSignatureInvalid);
    }
    // 2. every payload through its family's parser
    let hopparty = hopparty_marker_of_payload(&payloads[0], &settle_sig, &identity_sig)
        .ok_or(RecordRefusal::HandfilePayloadNotAMarker)?;
    let v1 = potparty_marker_of_payload(&payloads[1], &settle_sig, &identity_sig)
        .ok_or(RecordRefusal::HandfilePayloadNotAMarker)?;
    let v2 = potparty_marker_of_payload(&payloads[2], &settle_sig, &identity_sig)
        .ok_or(RecordRefusal::HandfilePayloadNotAMarker)?;
    if v1.seat_settle_pubkey.is_some() || v2.seat_settle_pubkey.is_none() {
        return Err(RecordRefusal::HandfilePayloadNotAMarker); // the slots are v1 then v2
    }
    let refund = potrefund_marker_of_payload(&payloads[3], &identity_sig)
        .ok_or(RecordRefusal::HandfilePayloadNotAMarker)?;
    // 3. one seat, one hand
    let settle = v2.seat_settle_pubkey.clone().unwrap_or_default();
    if hopparty.identity != v1.identity
        || v1.identity != v2.identity
        || v2.identity != refund.identity
        || hopparty.opponent != v1.opponent
        || v1.opponent != v2.opponent
        || hopparty.game_id != v1.game_id
        || v1.game_id != v2.game_id
        || v2.game_id != refund.game_id
        || v1.pot_txid != v2.pot_txid
        || v2.pot_txid != refund.pot_txid
        || v1.pot_vout != v2.pot_vout
        || v2.pot_vout != refund.pot_vout
        || v1.recovery_height != v2.recovery_height
        || hopparty.seat_settle_pubkey != settle
    {
        return Err(RecordRefusal::HandfileInconsistent);
    }
    let identity = hex::encode(&v1.identity);
    if identity != poster_lc {
        return Err(RecordRefusal::PosterMismatch);
    }
    // the two signatures, over the one digest
    let preimage = handfile_preimage(
        &hop_txid,
        &[&payloads[0], &payloads[1], &payloads[2], &payloads[3]],
    );
    if !verify_identity_sig(&v1.identity, &v1.game_id, &preimage, &identity_sig) {
        return Err(RecordRefusal::SignatureInvalid);
    }
    let Ok(settle_key) = bsv_rs::primitives::ec::PublicKey::from_bytes(&settle) else {
        return Err(RecordRefusal::SettleSignatureInvalid);
    };
    let Some(settle_der) = canonical_der(&settle_sig) else {
        return Err(RecordRefusal::SettleSignatureInvalid);
    };
    if !settle_key.verify(&bsv_rs::primitives::hash::sha256(&preimage), &settle_der) {
        return Err(RecordRefusal::SettleSignatureInvalid);
    }
    // the refund's cheap spend check (the direct door's step before the bind)
    if !refund_spends_pot(&refund.refund_raw, &refund.pot_txid, refund.pot_vout) {
        return Err(RecordRefusal::RefundDoesNotSpendThePot);
    }
    let hop_content_key = hopparty_content_key(&hopparty, &hop_txid);
    let hop_row_vout = handfile_hop_marker_vout(&hop_content_key);
    let identity_sig_hex = hex::encode(&identity_sig);
    let settle_sig_hex = hex::encode(&settle_sig);
    let record_of = |m: &PotpartyMarker| PotpartyRecord {
        identity: hex::encode(&m.identity),
        opponent_identity: hex::encode(&m.opponent),
        game_id: hex::encode(m.game_id),
        pot_txid: hex::encode(m.pot_txid),
        pot_vout: m.pot_vout,
        recovery_height: m.recovery_height,
        sig_hex: identity_sig_hex.clone(),
        seat_settle_pubkey: m.seat_settle_pubkey.as_ref().map(hex::encode),
        seat_sig_hex: m
            .seat_settle_pubkey
            .as_ref()
            .map(|_| settle_sig_hex.clone()),
        txid: potparty_content_key(m),
        output_index: 0,
        created_at: 0,
    };
    Ok(VerifiedHandfile {
        key: handfile_key(&preimage),
        identity: identity.clone(),
        game_id: hex::encode(v1.game_id),
        hop_txid: hop_txid_lc,
        potparty_v1: record_of(&v1),
        potparty_v2: record_of(&v2),
        potrefund: PotrefundRecord {
            identity,
            game_id: hex::encode(refund.game_id),
            pot_txid: hex::encode(refund.pot_txid),
            pot_vout: refund.pot_vout,
            refund_raw_hex: hex::encode(&refund.refund_raw),
            sig_hex: identity_sig_hex.clone(),
            txid: potrefund_content_key(&refund),
            output_index: 0,
            created_at: 0,
        },
        hopparty,
        hop_row_vout,
        hop_content_key,
        payloads_hex: [
            hex::encode(&payloads[0]),
            hex::encode(&payloads[1]),
            hex::encode(&payloads[2]),
            hex::encode(&payloads[3]),
        ],
        identity_sig_hex,
        settle_sig_hex,
    })
}

// ── the hop's container facts ──────────────────────────────────────────────────────────────────────────────────

/// The hop output as the index's copy of the hop transaction records it (#310's facts, read here from `pot_beefs`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HopFacts {
    pub lock_hex: String,
    pub sats: u64,
    pub outputs: u32,
}

/// The facts of output `hop_vout` of transaction `hop_txid` inside the stored BEEF, or `None` when the bytes do
/// not read, do not carry that transaction, or it has no such output.
pub fn hop_facts_of_beef(beef: &[u8], hop_txid: &str, hop_vout: u32) -> Option<HopFacts> {
    let parsed = overlay_engine::beef_limits::parse_beef(
        beef,
        &overlay_engine::beef_limits::APP_BEEF_LIMITS,
    )
    .ok()?;
    let tx = parsed
        .find_txid(&hop_txid.to_ascii_lowercase())
        .and_then(|t| t.tx())?;
    let out = tx.outputs.get(hop_vout as usize)?;
    Some(HopFacts {
        lock_hex: hex::encode(out.locking_script.to_binary()),
        sats: out.satoshis?,
        outputs: u32::try_from(tx.outputs.len()).ok()?,
    })
}

/// Do the container's facts agree with the batch's hop claims: the output pays the settle key (P2PKH) the sats the
/// payload names?
pub fn hop_facts_agree(m: &HoppartyMarker, facts: &HopFacts) -> bool {
    expected_hop_lock_hex(&m.seat_settle_pubkey)
        .is_some_and(|expected| expected.eq_ignore_ascii_case(&facts.lock_hex))
        && facts.sats == m.hop_sats
}

// ── the tables and statements ──────────────────────────────────────────────────────────────────────────────────

/// The batch rows (overlay migration 347; the app layer's catch-up carries it byte for byte).
pub const HANDFILE_BATCHES_CREATE: &str = "CREATE TABLE IF NOT EXISTS handfile_batches (batchKey TEXT PRIMARY KEY, identity TEXT NOT NULL, gameId TEXT NOT NULL, hopTxid TEXT NOT NULL, payloadsHex TEXT NOT NULL, identitySigHex TEXT NOT NULL, settleSigHex TEXT NOT NULL, createdAt INTEGER)";
/// Each exploded row's place in its batch (overlay migration 348).
pub const HANDFILE_ROWS_CREATE: &str = "CREATE TABLE IF NOT EXISTS handfile_rows (family TEXT NOT NULL, rowTxid TEXT NOT NULL, rowVout INTEGER NOT NULL, batchKey TEXT NOT NULL, idx INTEGER NOT NULL, PRIMARY KEY (family, rowTxid, rowVout))";

pub const HANDFILE_BATCH_FILE_SQL: &str = "INSERT OR IGNORE INTO handfile_batches \
     (batchKey, identity, gameId, hopTxid, payloadsHex, identitySigHex, settleSigHex, createdAt) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
pub const HANDFILE_ROW_FILE_SQL: &str = "INSERT OR IGNORE INTO handfile_rows \
     (family, rowTxid, rowVout, batchKey, idx) VALUES (?, ?, ?, ?, ?)";
/// The hopparty row, the overlay's own column list (`hopparty_records`), `markerValid` the door's verdict.
pub const HOPPARTY_HANDFILE_FILE_SQL: &str = "INSERT OR IGNORE INTO hopparty_records \
     (identity, opponentIdentity, gameId, hopVout, hopSats, seatSettlePubkey, seatSigHex, identitySigHex, \
      hopLockHex, hopSatsOnChain, containerOutputs, txid, outputIndex, createdAt, markerValid) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
/// The poster's FILED hopparty rows for this hop outpoint, across games, another content than this one's.
pub const HOPPARTY_FILED_ROWS_SQL: &str = "SELECT COUNT(*) AS n FROM hopparty_records \
     WHERE identity = ?1 AND txid = ?2 AND hopVout = ?3 AND outputIndex >= 2147483648 AND outputIndex <> ?4";
/// The poster's FILED hopparty rows of the day.
pub const HOPPARTY_FILED_TODAY_SQL: &str = "SELECT COUNT(*) AS n FROM hopparty_records \
     WHERE identity = ?1 AND outputIndex >= 2147483648 AND createdAt >= ?2";
/// Count the pair once, excluding both contents the batch intends to file.
pub const POTPARTY_HANDFILE_ROWS_SQL: &str = "SELECT COUNT(*) AS n FROM potparty_records \
     WHERE identity = ?1 AND gameId = ?2 AND potTxid = ?3 AND txid LIKE 'filed:%' AND txid NOT IN (?4, ?5)";
/// Project the day's count after the write: landed rows count once, only absent contents add a row.
/// An empty key is a slot skipped by the chain no-op. A landed row older than the day adds nothing.
pub const POTPARTY_HANDFILE_DAY_SQL: &str = "SELECT COUNT(*) \
     + (?3 <> '' AND NOT EXISTS (SELECT 1 FROM potparty_records WHERE txid = ?3 AND outputIndex = 0)) \
     + (?4 <> '' AND NOT EXISTS (SELECT 1 FROM potparty_records WHERE txid = ?4 AND outputIndex = 0)) AS n \
     FROM potparty_records WHERE identity = ?1 AND txid LIKE 'filed:%' AND createdAt >= ?2";
pub const POTREFUND_HANDFILE_DAY_SQL: &str = "SELECT COUNT(*) \
     + NOT EXISTS (SELECT 1 FROM potrefund_records WHERE txid = ?3 AND outputIndex = 0) AS n \
     FROM potrefund_records WHERE identity = ?1 AND txid LIKE 'filed:%' AND createdAt >= ?2";
pub const HOPPARTY_HANDFILE_DAY_SQL: &str = "SELECT COUNT(*) \
     + NOT EXISTS (SELECT 1 FROM hopparty_records WHERE txid = ?3 AND outputIndex = ?4) AS n \
     FROM hopparty_records WHERE identity = ?1 AND outputIndex >= 2147483648 AND createdAt >= ?2";
/// A CHAIN hop marker of this identity for this hop, verified: the handfile's hopparty row is then nothing to add.
pub const HOPPARTY_CHAIN_ROWS_SQL: &str = "SELECT COUNT(*) AS n FROM hopparty_records \
     WHERE txid = ?1 AND hopVout = ?2 AND identity = ?3 AND outputIndex < 2147483648 AND markerValid = 1";
/// A served row's batch, by the row's family and key.
pub const HANDFILE_BATCH_FOR_ROW_SQL: &str =
    "SELECT r.rowTxid AS rowTxid, r.rowVout AS rowVout, r.idx AS idx, \
            b.batchKey AS batchKey, b.hopTxid AS hopTxid, b.payloadsHex AS payloadsHex, \
            b.identitySigHex AS identitySigHex, b.settleSigHex AS settleSigHex \
     FROM handfile_rows r JOIN handfile_batches b ON b.batchKey = r.batchKey \
     WHERE r.family = ?1 AND r.rowTxid = ?2 AND r.rowVout = ?3";

const _: () = assert!(HANDFILE_MARKER_VOUT_BASE == 2_147_483_648);

/// The served `batch` of an exploded row (the one wire delta): one type and one JSON for every reader, the
/// discovery crate's (`overlay_discovery::handfile::HandfileBatch`), so the overlay's lookup services and the app
/// layer's views serve the same bytes.
pub use overlay_discovery::handfile::HandfileBatch as HandfileBatchRef;

/// The stored form of the payloads column: a JSON array of the four hex strings.
pub fn payloads_column(payloads_hex: &[String; HANDFILE_PAYLOADS]) -> String {
    serde_json::json!(payloads_hex).to_string()
}

pub use overlay_discovery::handfile::is_filed_key;

// ── the route ──────────────────────────────────────────────────────────────────────────────────────────────────

/// One exploded family's place in the batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandfileSlot {
    Hopparty = 0,
    PotpartyV1 = 1,
    PotpartyV2 = 2,
    Potrefund = 3,
}

impl HandfileSlot {
    pub const ALL: [HandfileSlot; HANDFILE_PAYLOADS] = [
        Self::Hopparty,
        Self::PotpartyV1,
        Self::PotpartyV2,
        Self::Potrefund,
    ];
    pub fn family(self) -> &'static str {
        match self {
            Self::Hopparty => HANDFILE_FAMILY_HOPPARTY,
            Self::PotpartyV1 | Self::PotpartyV2 => HANDFILE_FAMILY_POTPARTY,
            Self::Potrefund => HANDFILE_FAMILY_POTREFUND,
        }
    }
}

/// One family's cap reads, shared by the route and the SQLite regression cases.
pub(crate) struct HandfileCapQuery {
    pub family: &'static str,
    cap: i64,
    contents: i64,
    pub rows: (&'static str, Vec<String>),
    pub day: (&'static str, Vec<String>),
}

impl HandfileCapQuery {
    pub fn refusal(&self, other_rows: i64, day_after_write: i64) -> Option<RecordRefusal> {
        if other_rows + self.contents > self.cap {
            Some(RecordRefusal::TooManyFiled)
        } else if day_after_write > crate::record_post::RECORD_FILINGS_PER_IDENTITY_PER_DAY {
            Some(RecordRefusal::DailyCapReached)
        } else {
            None
        }
    }
}

pub(crate) fn handfile_cap_queries(
    v: &VerifiedHandfile,
    write: &[bool; HANDFILE_PAYLOADS],
    day_floor: i64,
) -> Vec<HandfileCapQuery> {
    use crate::record_post::{filed_rows_cap, RecordKind, POTREFUND_FILED_ROWS_SQL};
    let mut queries = Vec::with_capacity(3);
    let day_floor = day_floor.to_string();
    if write[HandfileSlot::Hopparty as usize] {
        queries.push(HandfileCapQuery {
            family: HANDFILE_FAMILY_HOPPARTY,
            cap: HOPPARTY_FILED_ROWS_CAP,
            contents: 1,
            rows: (
                HOPPARTY_FILED_ROWS_SQL,
                vec![
                    v.identity.clone(),
                    v.hop_txid.clone(),
                    v.hopparty.hop_vout.to_string(),
                    v.hop_row_vout.to_string(),
                ],
            ),
            day: (
                HOPPARTY_HANDFILE_DAY_SQL,
                vec![
                    v.identity.clone(),
                    day_floor.clone(),
                    v.hop_txid.clone(),
                    v.hop_row_vout.to_string(),
                ],
            ),
        });
    }
    let party_keys = [
        if write[HandfileSlot::PotpartyV1 as usize] {
            v.potparty_v1.txid.clone()
        } else {
            String::new()
        },
        if write[HandfileSlot::PotpartyV2 as usize] {
            v.potparty_v2.txid.clone()
        } else {
            String::new()
        },
    ];
    let contents = party_keys.iter().filter(|key| !key.is_empty()).count() as i64;
    if contents > 0 {
        queries.push(HandfileCapQuery {
            family: HANDFILE_FAMILY_POTPARTY,
            cap: filed_rows_cap(RecordKind::Potparty),
            contents,
            rows: (
                POTPARTY_HANDFILE_ROWS_SQL,
                vec![
                    v.identity.clone(),
                    v.game_id.clone(),
                    v.potparty_v1.pot_txid.clone(),
                    party_keys[0].clone(),
                    party_keys[1].clone(),
                ],
            ),
            day: (
                POTPARTY_HANDFILE_DAY_SQL,
                vec![
                    v.identity.clone(),
                    day_floor.clone(),
                    party_keys[0].clone(),
                    party_keys[1].clone(),
                ],
            ),
        });
    }
    if write[HandfileSlot::Potrefund as usize] {
        queries.push(HandfileCapQuery {
            family: HANDFILE_FAMILY_POTREFUND,
            cap: filed_rows_cap(RecordKind::Potrefund),
            contents: 1,
            rows: (
                POTREFUND_FILED_ROWS_SQL,
                vec![
                    v.identity.clone(),
                    v.game_id.clone(),
                    v.potrefund.pot_txid.clone(),
                    v.potrefund.txid.clone(),
                ],
            ),
            day: (
                POTREFUND_HANDFILE_DAY_SQL,
                vec![v.identity.clone(), day_floor, v.potrefund.txid.clone()],
            ),
        });
    }
    queries
}

/// The handfile's answer, retaining the same content keys on a no-op re-filing.
pub fn handfile_answer(v: &VerifiedHandfile, refund_valid: bool, wrote: bool) -> serde_json::Value {
    let mut body = serde_json::json!({
        "filed": wrote,
        "kind": "handfile",
        "key": v.key,
        "rows": {
            "hopparty": { "txid": v.hop_txid, "outputIndex": v.hop_row_vout },
            "potpartyV1": v.potparty_v1.txid,
            "potpartyV2": v.potparty_v2.txid,
            "potrefund": v.potrefund.txid,
        },
        "refundValid": i64::from(refund_valid),
    });
    if !wrote {
        body["alreadyFiled"] = serde_json::json!(true);
    }
    body
}

/// The row keys an exploded batch writes into `handfile_rows`, in slot order.
pub fn handfile_row_keys(v: &VerifiedHandfile) -> [(HandfileSlot, String, u32); HANDFILE_PAYLOADS] {
    [
        (HandfileSlot::Hopparty, v.hop_txid.clone(), v.hop_row_vout),
        (HandfileSlot::PotpartyV1, v.potparty_v1.txid.clone(), 0),
        (HandfileSlot::PotpartyV2, v.potparty_v2.txid.clone(), 0),
        (HandfileSlot::Potrefund, v.potrefund.txid.clone(), 0),
    ]
}

/// The handfile source rows and their trigger marks commit together, before any judgement.
pub(crate) async fn write_handfile(
    db: &impl crate::pot_hook::SqlPort,
    verified: &VerifiedHandfile,
    facts: &HopFacts,
    refund_valid: bool,
    write: &[bool; HANDFILE_PAYLOADS],
    now: i64,
) -> Result<bool, String> {
    use crate::record_post::{
        int as js_num, text as js, text_opt as js_opt, POTPARTY_FILE_SQL,
        POTREFUND_FILE_SQL, POTREFUND_LATCH_SQL,
    };
    let mut stmts = vec![crate::pot_hook::Stmt::new(
        HANDFILE_BATCH_FILE_SQL,
        vec![
            js(&verified.key),
            js(&verified.identity),
            js(&verified.game_id),
            js(&verified.hop_txid),
            js(&payloads_column(&verified.payloads_hex)),
            js(&verified.identity_sig_hex),
            js(&verified.settle_sig_hex),
            js_num(now),
        ],
    )];
    if write[HandfileSlot::Hopparty as usize] {
        let m = &verified.hopparty;
        stmts.push(crate::pot_hook::Stmt::new(
            HOPPARTY_HANDFILE_FILE_SQL,
            vec![
                js(&verified.identity),
                js(&hex::encode(&m.opponent)),
                js(&verified.game_id),
                js_num(i64::from(m.hop_vout)),
                js_num(m.hop_sats as i64),
                js(&hex::encode(&m.seat_settle_pubkey)),
                js(&verified.settle_sig_hex),
                js(&verified.identity_sig_hex),
                js(&facts.lock_hex),
                js_num(facts.sats as i64),
                js_num(i64::from(facts.outputs)),
                js(&verified.hop_txid),
                js_num(i64::from(verified.hop_row_vout)),
                js_num(now),
                js_num(1),
            ],
        ));
    }
    for (slot, r) in [
        (HandfileSlot::PotpartyV1, &verified.potparty_v1),
        (HandfileSlot::PotpartyV2, &verified.potparty_v2),
    ] {
        if !write[slot as usize] {
            continue;
        }
        stmts.push(crate::pot_hook::Stmt::new(
            POTPARTY_FILE_SQL,
            vec![
                js(&r.identity),
                js(&r.opponent_identity),
                js(&r.game_id),
                js(&r.pot_txid),
                js_num(i64::from(r.pot_vout)),
                js_num(i64::from(r.recovery_height)),
                js(&r.sig_hex),
                js_opt(r.seat_settle_pubkey.as_deref()),
                js_opt(r.seat_sig_hex.as_deref()),
                js(&r.txid),
                js_num(0),
                js_num(now),
                js_num(1),
            ],
        ));
    }
    if write[HandfileSlot::Potrefund as usize] {
        let r = &verified.potrefund;
        stmts.push(crate::pot_hook::Stmt::new(
            POTREFUND_FILE_SQL,
            vec![
                js(&r.identity),
                js(&r.game_id),
                js(&r.pot_txid),
                js_num(i64::from(r.pot_vout)),
                js(&r.refund_raw_hex),
                js(&r.sig_hex),
                js(&r.txid),
                js_num(0),
                js_num(now),
                js_num(i64::from(refund_valid)),
            ],
        ));
        if refund_valid {
            stmts.push(crate::pot_hook::Stmt::new(
                POTREFUND_LATCH_SQL,
                vec![js(&r.txid)],
            ));
        }
    }
    for (slot, row_txid, row_vout) in handfile_row_keys(verified) {
        if !write[slot as usize] {
            continue;
        }
        stmts.push(crate::pot_hook::Stmt::new(
            HANDFILE_ROW_FILE_SQL,
            vec![
                js(slot.family()),
                js(&row_txid),
                js_num(i64::from(row_vout)),
                js(&verified.key),
                js_num(slot as i64),
            ],
        ));
    }

    let changes = crate::record_post::owed_write::write_filings(db, stmts).await?;
    Ok(changes.iter().any(|n| crate::record_post::insert_wrote_a_row(n.map(|n| n as usize))))
}

/// The door's handfile arm, after the kind and the raw body are read (`record_post` dispatches here).
pub async fn handfile_post(
    ctx: &worker::RouteContext<crate::auth::AuthState>,
    raw: &[u8],
    identity: &str,
    anonymous: bool,
) -> worker::Result<worker::Response> {
    use crate::d1_ledger::Counted;
    use crate::record_post::{
        bind_refund_backup, chain_rows_query, js, js_num, note_cap_refused, note_filed,
        read_pot_context, refuse, RecordKind, VerifiedRecord, RECORD_DAY_SECS,
    };
    let kind = RecordKind::Handfile;
    let body: HandfileBody = match serde_json::from_slice(raw) {
        Ok(b) => b,
        Err(e) => {
            crate::record_post::note_bad_body(kind);
            return crate::routes::json_error(&format!("body is not a handfile: {e}"), 400);
        }
    };
    let verified = match verify_handfile(&body, identity) {
        Ok(v) => v,
        Err(r) => return refuse(Some(kind), r),
    };
    let db = ctx.env.d1("OVERLAY_DB")?;
    // 4. the refund bound to the pot, as the direct door binds it
    let pot = read_pot_context(
        &db,
        &verified.potrefund.pot_txid,
        verified.potrefund.pot_vout,
    )
    .await?;
    let refund_valid = match bind_refund_backup(&verified.potrefund, pot.as_ref()) {
        Ok(v) => v,
        Err(r) => return refuse(Some(kind), r),
    };
    // 5. the hop's container facts, from the index's own copy of the hop
    let beef_hex = db
        .prepare(crate::owed::JOIN_POT_BEEF_SQL)
        .bind(&[js(&verified.hop_txid)])?
        .counted_first::<PotBeefD1>()
        .await?
        .and_then(|r| r.beef);
    let facts = beef_hex
        .and_then(|h| hex::decode(h).ok())
        .and_then(|b| hop_facts_of_beef(&b, &verified.hop_txid, verified.hopparty.hop_vout));
    let Some(facts) = facts else {
        return refuse(Some(kind), RecordRefusal::HopNotIndexed);
    };
    if !hop_facts_agree(&verified.hopparty, &facts) {
        return refuse(Some(kind), RecordRefusal::HopMarkerRefuted);
    }
    let now = (worker::Date::now().as_millis() / 1000) as i64;
    // 7. the chain no-op and the caps, per family, before any write
    let v1 = VerifiedRecord::Potparty(verified.potparty_v1.clone(), true);
    let v2 = VerifiedRecord::Potparty(verified.potparty_v2.clone(), true);
    let mut write = [true; HANDFILE_PAYLOADS];
    let hop_chain = crate::record_post::count(
        &db,
        HOPPARTY_CHAIN_ROWS_SQL,
        &[
            js(&verified.hop_txid),
            js_num(i64::from(verified.hopparty.hop_vout)),
            js(&verified.identity),
        ],
    )
    .await?;
    write[HandfileSlot::Hopparty as usize] = hop_chain == 0;
    // bsv-low #798: the refund's chain no-op is the direct door's, row by row (a junk-signed chain row naming this
    // identity is never a no-op; an own row is latched before the batch)
    let port = crate::routes::D1Sql::of(&ctx.env)?;
    let refund_chain =
        crate::record_post::potrefund_chain_noop(&port, &verified.potrefund, pot.as_ref())
            .await
            .map_err(worker::Error::RustError)?;
    write[HandfileSlot::Potrefund as usize] = refund_chain.own == 0;
    for (slot, v) in [
        (HandfileSlot::PotpartyV1, &v1),
        (HandfileSlot::PotpartyV2, &v2),
    ] {
        if let Some((sql, binds)) = chain_rows_query(v) {
            let n = crate::record_post::count(
                &db,
                sql,
                &binds.iter().map(|s| js(s)).collect::<Vec<_>>(),
            )
            .await?;
            write[slot as usize] = n == 0;
        }
    }
    // Count each family once. For potparty both active keys are excluded, then the pair's two
    // contents are reserved together. A landed v1 or v2 is never counted again as pending.
    // The day query counts existing rows once and adds only contents absent from the table.
    for query in handfile_cap_queries(&verified, &write, now - RECORD_DAY_SECS) {
        let other_rows = crate::record_post::count(
            &db,
            query.rows.0,
            &query.rows.1.iter().map(|s| js(s)).collect::<Vec<_>>(),
        )
        .await?;
        let day_after_write = crate::record_post::count(
            &db,
            query.day.0,
            &query.day.1.iter().map(|s| js(s)).collect::<Vec<_>>(),
        )
        .await?;
        if let Some(r) = query.refusal(other_rows, day_after_write) {
            note_cap_refused(r, kind);
            return crate::record_post::refuse_with_family(Some(kind), r, Some(query.family));
        }
    }
    let wrote = write_handfile(&port, &verified, &facts, refund_valid, &write, now)
        .await
        .map_err(worker::Error::RustError)?;
    // bsv-low #911 (D2-ACTOR-A): the batch's owed events before the answer, never a judge here (the consumer's):
    // its hop marker, its two party rows and its refund, each the pair its row's trigger marks.
    if crate::owed_events::configured(&ctx.env).await {
        crate::owed_events_publish::note(
            "hop",
            &overlay_discovery::owed_event::outpoint_key(&verified.hop_txid, verified.hopparty.hop_vout),
        );
        crate::owed_events_publish::note("party", &verified.potparty_v1.txid);
        crate::owed_events_publish::note("party", &verified.potparty_v2.txid);
        crate::record_post::owed_write::note_events(&VerifiedRecord::Potrefund(
            verified.potrefund.clone(),
            refund_valid,
        ));
        crate::record_post::note_enqueue(crate::owed_events::publish_noted(&ctx.env).await);
    } else {
        crate::record_post::detached::schedule(
            ctx,
            vec![VerifiedRecord::Potrefund(verified.potrefund.clone(), refund_valid)],
            "handfile",
        );
    }
    if !wrote {
        crate::record_post::note_already_filed(kind);
        return crate::routes::json_response(
            handfile_answer(&verified, refund_valid, false).to_string(),
            200,
        );
    }
    note_filed(kind, anonymous);
    crate::routes::json_response(
        handfile_answer(&verified, refund_valid, true).to_string(),
        200,
    )
}

#[derive(Deserialize)]
struct PotBeefD1 {
    #[serde(default)]
    beef: Option<String>,
}

/// One `handfile_rows` join row as D1 returns it.
#[derive(Deserialize)]
pub struct HandfileBatchRowD1 {
    #[serde(rename = "rowTxid")]
    pub row_txid: String,
    #[serde(rename = "rowVout")]
    pub row_vout: f64,
    pub idx: f64,
    #[serde(rename = "batchKey")]
    pub batch_key: String,
    #[serde(rename = "hopTxid")]
    pub hop_txid: String,
    #[serde(rename = "payloadsHex")]
    pub payloads_hex: String,
    #[serde(rename = "identitySigHex")]
    pub identity_sig_hex: String,
    #[serde(rename = "settleSigHex")]
    pub settle_sig_hex: String,
}

impl HandfileBatchRowD1 {
    pub fn into_ref(self) -> Option<HandfileBatchRef> {
        HandfileBatchRef::of_stored(
            &self.batch_key,
            &self.hop_txid,
            &self.payloads_hex,
            &self.identity_sig_hex,
            &self.settle_sig_hex,
            self.idx as u32,
        )
    }
}

/// The batch a served row of `family` at (`row_txid`, `row_vout`) belongs to, or `None` (a directly filed or chain
/// row; a read fault reads as none: the row is served without its batch, never withheld).
pub async fn batch_for_row(
    db: &worker::D1Database,
    family: &str,
    row_txid: &str,
    row_vout: u32,
) -> Option<HandfileBatchRef> {
    use crate::d1_ledger::Counted;
    db.prepare(HANDFILE_BATCH_FOR_ROW_SQL)
        .bind(&[
            family.into(),
            row_txid.into(),
            worker::wasm_bindgen::JsValue::from_f64(f64::from(row_vout)),
        ])
        .ok()?
        .counted_first::<HandfileBatchRowD1>()
        .await
        .ok()
        .flatten()
        .and_then(HandfileBatchRowD1::into_ref)
}

/// The batches one view read asks for at most: one statement per exploded row, never per chain row.
pub const HANDFILE_BATCH_READS_PER_VIEW: usize = 16;

/// `/hops-view`: attach the batch of each handfile marker (a handfile vout), at most
/// [`HANDFILE_BATCH_READS_PER_VIEW`] reads; a chain marker is left as it was.
pub async fn attach_hop_batches(
    db: &worker::D1Database,
    entries: &mut [crate::hops_view::HopEntry],
) {
    let mut reads = 0usize;
    for e in entries.iter_mut() {
        if !overlay_discovery::hopparty::is_handfile_marker_vout(e.marker_vout) {
            continue;
        }
        if reads >= HANDFILE_BATCH_READS_PER_VIEW {
            break;
        }
        reads += 1;
        e.batch = batch_for_row(db, HANDFILE_FAMILY_HOPPARTY, &e.marker_txid, e.marker_vout).await;
    }
}

/// `/refund-backups`: attach the batch of each exploded (`filed:`) row, at most
/// [`HANDFILE_BATCH_READS_PER_VIEW`] reads; a chain row is left as it was.
pub async fn attach_refund_batches(
    db: &worker::D1Database,
    backups: &mut [crate::refund_backups::PotBackups],
) {
    let mut reads = 0usize;
    for p in backups.iter_mut() {
        for r in p.rows.iter_mut() {
            if !is_filed_key(&r.txid) {
                continue;
            }
            if reads >= HANDFILE_BATCH_READS_PER_VIEW {
                return;
            }
            reads += 1;
            r.batch = batch_for_row(db, HANDFILE_FAMILY_POTREFUND, &r.txid, r.output_index).await;
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
pub(crate) mod tests {
    use super::*;
    use crate::record_post::tests::{identity, script, sign, wallet};
    use crate::record_post::{RecordKind, VerifiedRecord};
    use bsv_rs::wallet::ProtoWallet;
    use overlay_discovery::potparty::validity::potparty_protocol;

    pub(crate) const HOP_TXID: [u8; 32] = [0x77u8; 32];
    pub(crate) const HOP_VOUT: u32 = 0;
    pub(crate) const HOP_SATS: u64 = 80_800;

    fn settle_key(scalar: u8) -> bsv_rs::primitives::ec::PrivateKey {
        let mut b = [0u8; 32];
        b[31] = scalar;
        bsv_rs::primitives::ec::PrivateKey::from_bytes(&b).unwrap()
    }

    /// One seat's honest batch: the four challenges the client computes today, the refund spending the pot.
    pub(crate) struct Hand {
        pub w: ProtoWallet,
        pub settle: bsv_rs::primitives::ec::PrivateKey,
        pub opponent: Vec<u8>,
        pub game_id: [u8; 32],
        pub pot_txid: [u8; 32],
        pub pot_vout: u32,
        pub recovery_height: u32,
        pub refund_raw: Vec<u8>,
        pub hop_txid: [u8; 32],
    }

    #[tokio::test]
    async fn owed908_handfile_sources_and_marks_are_one_transaction() {
        let db = crate::pot_hook::tests::SqliteSql::migrated();
        let hand = Hand::new(15);
        let verified = verify_handfile(&hand.body(), &hand.poster()).unwrap();
        let facts = HopFacts {
            lock_hex: expected_hop_lock_hex(&hand.settle_pub()).unwrap(),
            sats: HOP_SATS,
            outputs: 2,
        };
        db.conn
            .execute(
                "INSERT INTO pot_records(txid,outputIndex,spent,createdAt) VALUES(?1,0,0,1)",
                [hex::encode(HOP_TXID)],
            )
            .unwrap();
        db.conn.execute_batch("DELETE FROM owed_dirty; CREATE TRIGGER fail_owed BEFORE INSERT ON owed_rows BEGIN SELECT RAISE(ABORT,'owed row fault'); END;").unwrap();
        // A judge fault cannot affect the source batch: no judge runs until after the answer.
        // The marks remain durable for the detached job or belt alarm.
        assert!(
            write_handfile(&db, &verified, &facts, false, &[true; 4], 2_000)
                .await
                .unwrap(),
            "the sources landed"
        );
        let count = |table: &str| -> i64 {
            db.conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
                .unwrap()
        };
        for table in [
            "handfile_batches",
            "handfile_rows",
            "potparty_records",
            "potrefund_records",
            "hopparty_records",
        ] {
            assert!(count(table) > 0, "the batch's sources landed: {table}");
        }
        assert_eq!(count("owed_rows"), 0);
        assert!(count("owed_dirty") > 0, "the marks stand for the drain");
        assert_eq!(db.counter(crate::owed_judge::COUNTER_FILING_FAULTS), 0, "the door never invokes a judge");
        db.conn.execute_batch("DROP TRIGGER fail_owed").unwrap();
        crate::owed_judge::drain_dirty(&db, 64).await.unwrap();
        let rows: i64 = db
            .conn
            .query_row("SELECT COUNT(*) FROM owed_rows", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            rows, 2,
            "the new pot and hop are judged from the pending markers"
        );
        let marks: i64 = db
            .conn
            .query_row("SELECT COUNT(*) FROM owed_dirty", [], |r| r.get(0))
            .unwrap();
        assert_eq!(marks, 0, "owned marks are acknowledged atomically");
        // A chain-covered hop is not this batch's source. A mark raised during hydration must survive.
        struct ReMark<'a> {
            db: &'a crate::pot_hook::tests::SqliteSql,
            id: String,
            game: String,
            hop: String,
        }
        impl crate::pot_hook::SqlPort for ReMark<'_> {
            async fn run_batch(
                &self,
                stmts: Vec<crate::pot_hook::Stmt>,
            ) -> Result<Vec<crate::pot_hook::StmtOut>, String> {
                if stmts
                    .first()
                    .is_some_and(|s| s.sql == HANDFILE_BATCH_FILE_SQL)
                {
                    self.db.conn.execute("INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES(?1,?2,'hop',?3,1)",rusqlite::params![self.id,self.game,self.hop]).unwrap();
                }
                crate::pot_hook::SqlPort::run_batch(self.db, stmts).await
            }
            async fn counters(&self, _: Vec<(String, u64)>) {}
            fn now_ms(&self) -> u64 {
                self.db.now.get()
            }
            fn log(&self, _: &str) {}
        }
        impl crate::owed_judge::JudgeDb for ReMark<'_> {
            fn tip(&self) -> Option<u64> {
                Some(900_000)
            }
            async fn word_views(
                &self,
                rows: &std::collections::BTreeMap<String, crate::tracker_host::stored::Row>,
            ) -> Result<std::collections::BTreeMap<String, serde_json::Value>, String> {
                Ok(rows.iter().map(|(k, v)| (k.clone(), v.view())).collect())
            }
            async fn canonical_root(&self, _: u64) -> Option<String> {
                None
            }
            async fn push(&self, _: &str, _: serde_json::Value) {}
        }
        let racing = ReMark {
            db: &db,
            id: verified.identity.clone(),
            game: verified.game_id.clone(),
            hop: format!("{}:{}", verified.hop_txid, verified.hopparty.hop_vout),
        };
        assert!(
            !write_handfile(&racing, &verified, &facts, false, &[false; 4], 2_001)
                .await
                .unwrap()
        );
        let remaining: i64 = db
            .conn
            .query_row("SELECT COUNT(*) FROM owed_dirty", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            remaining, 1,
            "a no-op handfile cannot acknowledge another writer's new hop mark"
        );
    }

    impl Hand {
        pub(crate) fn new(seed: u8) -> Self {
            Self {
                w: wallet(seed),
                settle: settle_key(seed.wrapping_add(40)),
                opponent: identity(&wallet(seed.wrapping_add(1))),
                game_id: [seed.wrapping_add(0x10); 32],
                pot_txid: crate::record_post::tests::POT_TXID,
                pot_vout: 0,
                recovery_height: 900_000,
                refund_raw: crate::record_post::tests::presigned_refund(),
                hop_txid: HOP_TXID,
            }
        }
        pub(crate) fn identity(&self) -> Vec<u8> {
            identity(&self.w)
        }
        pub(crate) fn settle_pub(&self) -> Vec<u8> {
            self.settle.public_key().to_compressed().to_vec()
        }
        pub(crate) fn payloads(&self) -> [Vec<u8>; 4] {
            let id = self.identity();
            let settle = self.settle_pub();
            [
                hopparty_identity_challenge(
                    &id,
                    &self.opponent,
                    &self.game_id,
                    HOP_VOUT,
                    HOP_SATS,
                    &settle,
                )
                .unwrap(),
                potparty_v1_challenge(
                    &id,
                    &self.opponent,
                    &self.game_id,
                    &self.pot_txid,
                    self.pot_vout,
                    self.recovery_height,
                )
                .unwrap(),
                potparty_v2_challenge(
                    &id,
                    &self.opponent,
                    &self.game_id,
                    &self.pot_txid,
                    self.pot_vout,
                    self.recovery_height,
                    &settle,
                )
                .unwrap(),
                crate::record_post::potrefund_challenge(
                    &id,
                    &self.game_id,
                    &self.pot_txid,
                    self.pot_vout,
                    &self.refund_raw,
                ),
            ]
        }
        pub(crate) fn preimage_of(&self, payloads: &[Vec<u8>; 4]) -> Vec<u8> {
            handfile_preimage(
                &self.hop_txid,
                &[&payloads[0], &payloads[1], &payloads[2], &payloads[3]],
            )
        }
        pub(crate) fn body_of(&self, payloads: &[Vec<u8>; 4]) -> HandfileBody {
            let preimage = self.preimage_of(payloads);
            let identity_sig = sign(
                &self.w,
                potparty_protocol(),
                &hex::encode(self.game_id),
                &preimage,
            );
            let digest = bsv_rs::primitives::hash::sha256(&preimage);
            let settle_sig = self.settle.sign(&digest).unwrap().to_der();
            HandfileBody {
                hop_txid: hex::encode(self.hop_txid),
                payloads: payloads.iter().map(hex::encode).collect(),
                identity_sig_hex: hex::encode(identity_sig),
                settle_sig_hex: hex::encode(settle_sig),
            }
        }
        pub(crate) fn body(&self) -> HandfileBody {
            self.body_of(&self.payloads())
        }
        pub(crate) fn poster(&self) -> String {
            hex::encode(self.identity())
        }
        /// The same marker filed DIRECTLY (the v2 potparty marker, signed per row as today).
        pub(crate) fn direct_v2_script(&self) -> Vec<u8> {
            use overlay_discovery::potparty::validity::seatsig_preimage;
            let id = self.identity();
            let settle = self.settle_pub();
            let challenge = potparty_v2_challenge(
                &id,
                &self.opponent,
                &self.game_id,
                &self.pot_txid,
                self.pot_vout,
                self.recovery_height,
                &settle,
            )
            .unwrap();
            let id_sig = sign(
                &self.w,
                potparty_protocol(),
                &hex::encode(self.game_id),
                &challenge,
            );
            let pre = seatsig_preimage(&self.game_id, &self.pot_txid, self.pot_vout, &id).unwrap();
            let seat_sig = self
                .settle
                .sign(&bsv_rs::primitives::hash::sha256(&pre))
                .unwrap()
                .to_der();
            script(&[
                POTPARTY_TAG_V2,
                &id,
                &self.opponent,
                &self.game_id,
                &self.pot_txid,
                &self.pot_vout.to_le_bytes(),
                &self.recovery_height.to_le_bytes(),
                &settle,
                &seat_sig,
                &id_sig,
            ])
        }
    }

    fn flip_last(hex_sig: &str) -> String {
        let mut b = hex::decode(hex_sig).unwrap();
        let last = b.len() - 1;
        b[last] ^= 0x01;
        hex::encode(b)
    }

    #[test]
    fn fastc_a_an_honest_batch_verifies_and_explodes_into_the_four_rows() {
        let hand = Hand::new(7);
        let v = verify_handfile(&hand.body(), &hand.poster()).expect("verifies");
        assert!(v.key.starts_with(HANDFILE_KEY_PREFIX) && v.key.len() == 9 + 64);
        assert_eq!(v.identity, hand.poster());
        assert_eq!(v.hop_txid, hex::encode(HOP_TXID));
        assert!(v.potparty_v1.seat_settle_pubkey.is_none());
        assert_eq!(
            v.potparty_v2.seat_settle_pubkey.as_deref(),
            Some(hex::encode(hand.settle_pub()).as_str())
        );
        assert!(
            v.potparty_v1.txid.starts_with("filed:") && v.potparty_v2.txid.starts_with("filed:")
        );
        assert_ne!(v.potparty_v1.txid, v.potparty_v2.txid);
        assert!(v.potrefund.txid.starts_with("filed:"));
        assert!(overlay_discovery::hopparty::is_handfile_marker_vout(
            v.hop_row_vout
        ));
        // the exploded v2 row is keyed exactly as a direct filing of the same marker
        let direct = crate::record_post::verify_record_post(
            RecordKind::Potparty,
            &hand.direct_v2_script(),
            &hand.poster(),
        )
        .expect("the direct filing verifies");
        let VerifiedRecord::Potparty(direct, true) = direct else {
            panic!("a v2 potparty filing")
        };
        assert_eq!(
            direct.txid, v.potparty_v2.txid,
            "one marker, one content key"
        );
        // the row's signature columns hold the batch's two signatures
        assert_eq!(v.potparty_v2.sig_hex, v.identity_sig_hex);
        assert_eq!(
            v.potparty_v2.seat_sig_hex.as_deref(),
            Some(v.settle_sig_hex.as_str())
        );
        assert_eq!(v.hopparty.seat_sig, hex::decode(&v.settle_sig_hex).unwrap());
        assert_eq!(
            v.hopparty.identity_sig,
            hex::decode(&v.identity_sig_hex).unwrap()
        );
        assert_eq!(v.potrefund.sig_hex, v.identity_sig_hex);
    }

    #[test]
    fn fastc_b_a_bad_identity_signature_is_refused() {
        let hand = Hand::new(8);
        let mut body = hand.body();
        body.identity_sig_hex = flip_last(&body.identity_sig_hex);
        assert_eq!(
            verify_handfile(&body, &hand.poster()).unwrap_err(),
            RecordRefusal::SignatureInvalid
        );
        // another identity's signature over the same digest: the poster's identity is in every payload
        let other = Hand::new(9);
        let mut stolen = hand.body();
        stolen.identity_sig_hex = other.body_of(&hand.payloads()).identity_sig_hex;
        assert_eq!(
            verify_handfile(&stolen, &hand.poster()).unwrap_err(),
            RecordRefusal::SignatureInvalid
        );
        // the direct families' signature is not the batch's: a per-marker signature does not verify here
        let mut per_row = hand.body();
        let challenge = hand.payloads()[2].clone();
        per_row.identity_sig_hex = hex::encode(sign(
            &hand.w,
            potparty_protocol(),
            &hex::encode(hand.game_id),
            &challenge,
        ));
        assert_eq!(
            verify_handfile(&per_row, &hand.poster()).unwrap_err(),
            RecordRefusal::SignatureInvalid
        );
    }

    #[test]
    fn fastc_c_a_bad_settle_key_signature_is_refused() {
        let hand = Hand::new(10);
        let mut body = hand.body();
        body.settle_sig_hex = flip_last(&body.settle_sig_hex);
        assert_eq!(
            verify_handfile(&body, &hand.poster()).unwrap_err(),
            RecordRefusal::SettleSignatureInvalid
        );
        // the OPPONENT's settle key signing the batch: the v2 payload names the seat's own key
        let mut other = Hand::new(10);
        other.settle = settle_key(99);
        let mut swapped = hand.body();
        swapped.settle_sig_hex = other.body_of(&hand.payloads()).settle_sig_hex;
        assert_eq!(
            verify_handfile(&swapped, &hand.poster()).unwrap_err(),
            RecordRefusal::SettleSignatureInvalid
        );
        // a high-S twin of the honest settle signature is not canonical
        let honest = hex::decode(&hand.body().settle_sig_hex).unwrap();
        let mut padded = honest.clone();
        padded.push(0x00);
        let mut p = hand.body();
        p.settle_sig_hex = hex::encode(padded);
        assert_eq!(
            verify_handfile(&p, &hand.poster()).unwrap_err(),
            RecordRefusal::SettleSignatureInvalid
        );
    }

    #[test]
    fn fastc_d_a_payload_its_family_refuses_refuses_the_whole_batch() {
        let hand = Hand::new(11);
        // the hopparty payload self-paired (the family's structural rule)
        let mut p = hand.payloads();
        let id = hand.identity();
        p[0][15 + 33..15 + 66].copy_from_slice(&id);
        assert_eq!(
            verify_handfile(&hand.body_of(&p), &hand.poster()).unwrap_err(),
            RecordRefusal::HandfilePayloadNotAMarker
        );
        // a v1 payload one byte short
        let mut p = hand.payloads();
        p[1].pop();
        assert_eq!(
            verify_handfile(&hand.body_of(&p), &hand.poster()).unwrap_err(),
            RecordRefusal::HandfilePayloadNotAMarker
        );
        // the v2 slot holding a v1 payload
        let mut p = hand.payloads();
        p[2] = p[1].clone();
        assert_eq!(
            verify_handfile(&hand.body_of(&p), &hand.poster()).unwrap_err(),
            RecordRefusal::HandfilePayloadNotAMarker
        );
        // a refund payload with no refund raw
        let mut p = hand.payloads();
        p[3].truncate(POTREFUND_PAYLOAD_HEAD);
        assert_eq!(
            verify_handfile(&hand.body_of(&p), &hand.poster()).unwrap_err(),
            RecordRefusal::HandfilePayloadNotAMarker
        );
        // a refund raw that does not spend the pot
        let mut p = hand.payloads();
        let junk = crate::record_post::tests::junk_refund_raw(&[0x99; 32], 0);
        p[3].truncate(POTREFUND_PAYLOAD_HEAD);
        p[3].extend_from_slice(&junk);
        assert_eq!(
            verify_handfile(&hand.body_of(&p), &hand.poster()).unwrap_err(),
            RecordRefusal::RefundDoesNotSpendThePot
        );
        // the four payloads naming two pots
        let mut p = hand.payloads();
        p[1][15 + 98] ^= 0xff;
        assert_eq!(
            verify_handfile(&hand.body_of(&p), &hand.poster()).unwrap_err(),
            RecordRefusal::HandfileInconsistent
        );
        // three payloads
        let mut b = hand.body();
        b.payloads.pop();
        assert_eq!(
            verify_handfile(&b, &hand.poster()).unwrap_err(),
            RecordRefusal::HandfileShape
        );
        // a stranger posting the seat's batch
        assert_eq!(
            verify_handfile(&hand.body(), &Hand::new(12).poster()).unwrap_err(),
            RecordRefusal::PosterMismatch
        );
    }

    #[test]
    fn fastc_e_the_digest_binds_the_hop_txid_and_the_order_and_is_the_batch_key() {
        let hand = Hand::new(13);
        let v = verify_handfile(&hand.body(), &hand.poster()).unwrap();
        let mut other_hop = hand.body();
        other_hop.hop_txid = hex::encode([0x78u8; 32]);
        assert_eq!(
            verify_handfile(&other_hop, &hand.poster()).unwrap_err(),
            RecordRefusal::SignatureInvalid,
            "the hop txid is signed"
        );
        // re-signed with fresh signatures (a second wallet session): the same key
        let again = verify_handfile(&hand.body(), &hand.poster()).unwrap();
        assert_eq!(again.key, v.key);
        assert_eq!(again.potparty_v2.txid, v.potparty_v2.txid);
        assert_eq!(again.hop_row_vout, v.hop_row_vout);
        // the preimage's framing
        let p = hand.payloads();
        let pre = hand.preimage_of(&p);
        assert!(pre.starts_with(HANDFILE_DOMAIN));
        assert_eq!(&pre[15..47], &HOP_TXID);
        assert_eq!(&pre[47..51], &(p[0].len() as u32).to_le_bytes());
        assert_eq!(v.key, handfile_key(&pre));
    }

    #[test]
    fn fastc_f_the_hop_facts_are_read_from_the_stored_hop_and_must_agree() {
        let hand = Hand::new(14);
        let v = verify_handfile(&hand.body(), &hand.poster()).unwrap();
        let settle = hand.settle_pub();
        let lock = hex::decode(expected_hop_lock_hex(&settle).unwrap()).unwrap();
        // a hop tx paying the settle key HOP_SATS at vout 0, as a BEEF the index would hold
        let mut tx = bsv_rs::transaction::Transaction::new();
        tx.add_input(bsv_rs::transaction::TransactionInput {
            source_txid: Some(hex::encode([0x11u8; 32])),
            source_output_index: 0,
            unlocking_script: Some(bsv_rs::script::UnlockingScript::from_hex("00").unwrap()),
            sequence: 0xffff_ffff,
            ..Default::default()
        })
        .unwrap();
        tx.add_output(bsv_rs::transaction::TransactionOutput {
            satoshis: Some(HOP_SATS),
            locking_script: bsv_rs::script::LockingScript::from_binary(&lock).unwrap(),
            change: false,
        })
        .unwrap();
        tx.add_output(bsv_rs::transaction::TransactionOutput {
            satoshis: Some(1),
            locking_script: bsv_rs::script::LockingScript::from_hex("51").unwrap(),
            change: false,
        })
        .unwrap();
        let txid = tx.id();
        let mut beef = bsv_rs::transaction::Beef::new();
        beef.merge_transaction(tx);
        let bytes = beef.to_binary();
        let facts = hop_facts_of_beef(&bytes, &txid, 0).expect("the hop reads");
        assert_eq!(
            facts,
            HopFacts {
                lock_hex: hex::encode(&lock),
                sats: HOP_SATS,
                outputs: 2
            }
        );
        assert!(hop_facts_agree(&v.hopparty, &facts));
        assert!(
            hop_facts_of_beef(&bytes, &txid, 2).is_none(),
            "no such output"
        );
        assert!(hop_facts_of_beef(&bytes, &hex::encode([0x12u8; 32]), 0).is_none());
        let mut other_sats = facts.clone();
        other_sats.sats += 1;
        assert!(!hop_facts_agree(&v.hopparty, &other_sats));
        let mut other_lock = facts.clone();
        other_lock.lock_hex = "51".into();
        assert!(!hop_facts_agree(&v.hopparty, &other_lock));
    }

    /// The exploder's writes through the SHIPPED statements under real SQLite: the four rows land exactly as filed
    /// rows, idempotent by content, and the owed list's K-binding rule reads the exploded v2 row.
    #[test]
    fn fastc_g_the_shipped_statements_explode_the_batch_idempotently_and_the_owed_rule_reads_it() {
        let conn = crate::record_post::tests::w25c_db();
        let hand = Hand::new(15);
        let v = verify_handfile(&hand.body(), &hand.poster()).unwrap();
        let facts = HopFacts {
            lock_hex: expected_hop_lock_hex(&hand.settle_pub()).unwrap(),
            sats: HOP_SATS,
            outputs: 2,
        };
        for _ in 0..2 {
            explode_under_sqlite(&conn, &v, &facts, false);
        }
        let count = |sql: &str| -> i64 { conn.query_row(sql, [], |r| r.get(0)).unwrap() };
        assert_eq!(count("SELECT COUNT(*) FROM handfile_batches"), 1);
        assert_eq!(count("SELECT COUNT(*) FROM handfile_rows"), 4);
        assert_eq!(count("SELECT COUNT(*) FROM potparty_records"), 2);
        assert_eq!(count("SELECT COUNT(*) FROM potrefund_records"), 1);
        assert_eq!(count("SELECT COUNT(*) FROM hopparty_records"), 1);
        // the v2 row: sigValid = 1, the settle key named, a filed key
        let (sig_valid, settle, txid): (i64, String, String) = conn
            .query_row(
                "SELECT sigValid, seatSettlePubkey, txid FROM potparty_records WHERE seatSettlePubkey IS NOT NULL",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            (sig_valid, settle.as_str(), txid.as_str()),
            (
                1,
                hex::encode(hand.settle_pub()).as_str(),
                v.potparty_v2.txid.as_str()
            )
        );
        // the hop row: under the hop's txid at a handfile vout, markerValid 1, the container's facts
        let (hop_txid, vout, mv, lock, sats): (String, i64, i64, String, i64) = conn
            .query_row(
                "SELECT txid, outputIndex, markerValid, hopLockHex, hopSatsOnChain FROM hopparty_records",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .unwrap();
        assert_eq!(hop_txid, hex::encode(HOP_TXID));
        assert_eq!(vout, i64::from(v.hop_row_vout));
        assert_eq!(
            (mv, lock.as_str(), sats),
            (1, facts.lock_hex.as_str(), HOP_SATS as i64)
        );
        // the owed judge's party read (`owed_judge::PARTIES_SQL`, the K binding of bsv-low #610: a verified seat
        // with its settle key) reads the exploded v2 row
        let mut st = conn.prepare(crate::owed_judge::PARTIES_SQL).unwrap();
        let parties: Vec<(String, Option<i64>, Option<String>)> = st
            .query_map(
                rusqlite::params![
                    hand.poster(),
                    serde_json::json!([hex::encode(hand.game_id)]).to_string()
                ],
                |r| {
                    Ok((
                        r.get("potTxid")?,
                        r.get("sigValid")?,
                        r.get("seatSettlePubkey")?,
                    ))
                },
            )
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert!(
            parties.iter().any(|(pot, valid, key)| pot
                .eq_ignore_ascii_case(&hex::encode(hand.pot_txid))
                && *valid == Some(1)
                && key.is_some()),
            "the judge reads the exploded v2 row, verified with its settle key: {parties:?}"
        );
        // the direct door's cap counts the exploded rows: a third potparty content is refused
        let (rows_sql, binds, _, _) = crate::record_post::cap_queries(&VerifiedRecord::Potparty(
            {
                let mut r = v.potparty_v1.clone();
                r.txid = "filed:another".into();
                r
            },
            true,
        ));
        let n: i64 = conn
            .query_row(rows_sql, rusqlite::params_from_iter(binds.iter()), |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(n, 2);
        assert_eq!(
            crate::record_post::cap_refusal(RecordKind::Potparty, n, 0),
            Some(RecordRefusal::TooManyFiled)
        );
        // the hopparty cap: another content for the same hop is refused, the same content is not counted
        let other: i64 = conn
            .query_row(
                HOPPARTY_FILED_ROWS_SQL,
                rusqlite::params![
                    hand.poster(),
                    hex::encode(HOP_TXID),
                    HOP_VOUT,
                    v.hop_row_vout + 1
                ],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(other, 1);
        assert_eq!(
            crate::record_post::cap_refusal_at(HOPPARTY_FILED_ROWS_CAP, other, 0),
            Some(RecordRefusal::TooManyFiled)
        );
        let same: i64 = conn
            .query_row(
                HOPPARTY_FILED_ROWS_SQL,
                rusqlite::params![
                    hand.poster(),
                    hex::encode(HOP_TXID),
                    HOP_VOUT,
                    v.hop_row_vout
                ],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(same, 0);
        let today: i64 = conn
            .query_row(
                HOPPARTY_FILED_TODAY_SQL,
                rusqlite::params![hand.poster(), 0i64],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(today, 1);
        // the chain no-op: a chain hop marker of this identity, verified, makes the hopparty slot nothing to add
        let chain: i64 = conn
            .query_row(
                HOPPARTY_CHAIN_ROWS_SQL,
                rusqlite::params![hex::encode(HOP_TXID), HOP_VOUT, hand.poster()],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(chain, 0, "a handfile row is never a chain row");
        // the served batch of each exploded row
        for (slot, txid, vout) in handfile_row_keys(&v) {
            let b: HandfileBatchRef = conn
                .query_row(
                    HANDFILE_BATCH_FOR_ROW_SQL,
                    rusqlite::params![slot.family(), txid, vout],
                    |r| {
                        Ok(HandfileBatchRef::of_stored(
                            &r.get::<_, String>(3)?,
                            &r.get::<_, String>(4)?,
                            &r.get::<_, String>(5)?,
                            &r.get::<_, String>(6)?,
                            &r.get::<_, String>(7)?,
                            r.get::<_, i64>(2)? as u32,
                        )
                        .unwrap())
                    },
                )
                .unwrap();
            assert_eq!(b.key, v.key);
            assert_eq!(b.index, slot as u32);
            assert_eq!(b.payloads, v.payloads_hex.to_vec());
            let j = b.json();
            assert_eq!(j["sigs"]["identity"], v.identity_sig_hex);
            assert_eq!(j["sigs"]["settle"], v.settle_sig_hex);
            assert_eq!(j["hopTxid"], hex::encode(HOP_TXID));
        }
        // a directly filed row has no batch
        assert!(conn
            .query_row(
                HANDFILE_BATCH_FOR_ROW_SQL,
                rusqlite::params![HANDFILE_FAMILY_POTPARTY, "filed:direct", 0],
                |r| r.get::<_, String>(3),
            )
            .is_err());
        // the answer is the same both times (idempotent by content)
        assert_eq!(
            handfile_answer(&v, false, true),
            handfile_answer(
                &verify_handfile(&hand.body(), &hand.poster()).unwrap(),
                false,
                true,
            )
        );
    }

    /// The exploder's statements as the route binds them (rusqlite in place of D1).
    pub(crate) fn explode_under_sqlite(
        conn: &rusqlite::Connection,
        v: &VerifiedHandfile,
        facts: &HopFacts,
        refund_valid: bool,
    ) -> u64 {
        let before = conn.total_changes();
        let now = 1_000i64;
        conn.execute(
            HANDFILE_BATCH_FILE_SQL,
            rusqlite::params![
                v.key,
                v.identity,
                v.game_id,
                v.hop_txid,
                payloads_column(&v.payloads_hex),
                v.identity_sig_hex,
                v.settle_sig_hex,
                now
            ],
        )
        .unwrap();
        let m = &v.hopparty;
        conn.execute(
            HOPPARTY_HANDFILE_FILE_SQL,
            rusqlite::params![
                v.identity,
                hex::encode(&m.opponent),
                v.game_id,
                m.hop_vout,
                m.hop_sats as i64,
                hex::encode(&m.seat_settle_pubkey),
                v.settle_sig_hex,
                v.identity_sig_hex,
                facts.lock_hex,
                facts.sats as i64,
                facts.outputs,
                v.hop_txid,
                v.hop_row_vout,
                now,
                1
            ],
        )
        .unwrap();
        for r in [&v.potparty_v1, &v.potparty_v2] {
            conn.execute(
                crate::record_post::POTPARTY_FILE_SQL,
                rusqlite::params![
                    r.identity,
                    r.opponent_identity,
                    r.game_id,
                    r.pot_txid,
                    r.pot_vout,
                    r.recovery_height,
                    r.sig_hex,
                    r.seat_settle_pubkey,
                    r.seat_sig_hex,
                    r.txid,
                    0,
                    now,
                    1
                ],
            )
            .unwrap();
        }
        let r = &v.potrefund;
        conn.execute(
            crate::record_post::POTREFUND_FILE_SQL,
            rusqlite::params![
                r.identity,
                r.game_id,
                r.pot_txid,
                r.pot_vout,
                r.refund_raw_hex,
                r.sig_hex,
                r.txid,
                0,
                now,
                i64::from(refund_valid)
            ],
        )
        .unwrap();
        if refund_valid {
            conn.execute(crate::record_post::POTREFUND_LATCH_SQL, [&r.txid])
                .unwrap();
        }
        for (slot, txid, vout) in handfile_row_keys(v) {
            conn.execute(
                HANDFILE_ROW_FILE_SQL,
                rusqlite::params![slot.family(), txid, vout, v.key, slot as i64],
            )
            .unwrap();
        }
        conn.total_changes() - before
    }

    /// An old row's bytes: a directly filed v2 row is served without `batch`, byte for byte as before, and the
    /// hopparty marker's chain rows keep their vout below the handfile base.
    #[test]
    fn fastc_h_an_old_rows_answer_is_unchanged_and_a_handfile_vout_is_above_every_chain_vout() {
        let r = HandfileBatchRef {
            key: "handfile:ab".into(),
            hop_txid: "77".repeat(32),
            payloads: vec!["00".into(), "01".into(), "02".into(), "03".into()],
            identity_sig_hex: "30".into(),
            settle_sig_hex: "31".into(),
            index: 2,
        };
        let j = r.json();
        assert_eq!(
            j,
            serde_json::json!({"key": "handfile:ab", "hopTxid": "77".repeat(32), "payloads": ["00","01","02","03"], "sigs": {"identity": "30", "settle": "31"}, "index": 2})
        );
        assert!(HandfileBatchRef::of_stored("k", "h", "[\"00\"]", "a", "b", 0).is_none());
        assert!(!overlay_discovery::hopparty::is_handfile_marker_vout(0));
        assert!(!overlay_discovery::hopparty::is_handfile_marker_vout(
            0x7fff_ffff
        ));
        assert!(overlay_discovery::hopparty::is_handfile_marker_vout(
            handfile_hop_marker_vout("filed:x")
        ));
        assert!(!is_filed_key("ab".repeat(32).as_str()) && is_filed_key("filed:ab"));
    }
}
