//! bsv-low M18-2 (B), the beta window's W-E (2026-09-10): `POST /record?kind=`
//! — every index marker LOW used to BUY on chain is FILED here instead.
//!
//! The four bought families (`potparty` v1+v2, `potrefund`, `result`,
//! `collected`) were "admitted by BYTE FORMAT ONLY, the security lives in the
//! client verify" (the crate docs): the chain was the DELIVERY mechanism, and
//! the delivery cost a fee and a wallet prompt per marker. Under the owner's
//! ruling ("we ARE the app-layer"; proof-in-DB is the pattern, `proof_post.rs`)
//! the client posts the SAME locking-script bytes it would have put in the
//! marker output, and this route:
//!
//!   1. accepts ONLY the CANONICAL encoding of those bytes
//!      ([`canonical_marker_pushes`]: `OP_FALSE OP_RETURN`, then minimally
//!      encoded pushes and nothing else — exactly what every client builder
//!      emits) and parses them with the family's own parser (`parse_*_marker`)
//!      — one grammar, the on-chain one;
//!   2. VERIFIES the family's signatures with the same pure validity code the
//!      overlay applies at admission where it has one (`potparty::validity::
//!      record_sig_valid`, `result::validity::claim_tier`) and, for the two
//!      families the overlay never verified (`potrefund`, `collected`), with
//!      the client's own challenge bytes re-derived here — every signature
//!      through the ONE canonical-DER gate (`potparty::validity::canonical_der`,
//!      low-S, byte-exact re-encoding) — STRICTER than the chain path on
//!      purpose: a filed row costs nothing to plant, so a row the identity did
//!      not sign is refused (the D2 lesson), and a padded or high-S replay of
//!      an honest marker is not a second marker;
//!   3. for a REFUND BACKUP, binds the bytes to the POT they name: when the
//!      index holds the pot with its committed params, the raw must be the
//!      pot's PRE-SIGNED spend in the covenant template's EXACT unlock shape —
//!      `[sigA ‖ 0x41, sigB ‖ 0x41, the BIP-143 preimage]` over the rebuilt
//!      lock and the funded value, the two signatures canonical DER verifying
//!      against the committed settle keys IN ORDER, the preimage byte-equal to
//!      the one this input hashes — height-gated at the committed recovery
//!      height and non-final (the 2-of-2 the seats exchanged at funding), and
//!      `pot::settle_signers_for_spend` = `Coop` as the belt. The chain path
//!      checked only that input 0 named the pot, which is exactly the client's
//!      own selector, so a free junk row was indistinguishable from the honest
//!      backup to every reader (the gate's HIGH-1); a malleated variant of the
//!      real refund (the counterparty holds both signatures) is refused by the
//!      shape, never latched as rank 1 (the delta-verify's MED-2). A pot the
//!      index does not hold yet files at RANK 0 (`refundValid = 0`: the belt
//!      survives an unindexed pot, as before) and the response says so; the
//!      client's ladder re-files until the row latches rank 1 (the same
//!      content: the write is a no-op, the verdict an UPDATE) — never a 425,
//!      which cancelled the funding kick's ladder (the delta-verify's MED-1);
//!   4. compares the poster to the marker: the resolved identity must be the
//!      marker's identity (`potparty`/`potrefund`/`collected`) or one of the
//!      two seats (`result`). Truth in advertising (the gate's MED-1): `/record`
//!      is not an `IDENTITY_ROUTE`, so an ANONYMOUS caller's poster is the
//!      `?identity=` claim itself — the bar that holds for a stranger is the
//!      SIGNATURE (step 2) and the caps (step 6), not this comparison; a
//!      VERIFIED session must match it. Anonymous filings are counted per
//!      family on `/health` (`record.anonymousFiledByKind`);
//!   5. writes the family's OWN records table with the SAME column list the
//!      overlay's lookup service writes (`potparty_records`,
//!      `potrefund_records` + its `refundValid` verdict, `result_markers_v2` +
//!      its `lb_marker_rows` row, `collected_markers_v2`), keyed by a synthetic
//!      outpoint (vout 0) whose txid is the marker's CONTENT key
//!      (`filed:<sha256 of the family tag + the signed fields, signatures
//!      EXCLUDED>[..56]`): the same marker re-signed with a fresh nonce, or
//!      re-posted, files the same row (`INSERT OR IGNORE`) — idempotent by
//!      CONTENT, not by bytes (the gate's HIGH-2: a byte key let one honest
//!      marker mint unlimited rows). Every reader — the identity views,
//!      `/refund-backups`, `/results`, the overlay's own lookup services, the
//!      client's verifiers — serves a filed row exactly as an admitted one,
//!      with no chain anchor to check;
//!   6. CAPS what one identity can file: per `(poster, family, game, pot)` the
//!      honest need ([`filed_rows_cap`]: one row per family, two for
//!      `potparty` (v1 + v2 share a table) and `result` (the claim, then its
//!      countersigned upgrade)) — a distinct content past the cap is `409`;
//!      and per `(poster, family)` a day's budget
//!      ([`RECORD_FILINGS_PER_IDENTITY_PER_DAY`], `429`). Both count FILED rows
//!      only (`txid LIKE 'filed:%'`), never chain admissions. And a marker the
//!      CHAIN already holds for this identity, game, pot and version is a
//!      no-op 200 (`alreadyIndexed`): nothing to add, nothing written, nothing
//!      counted — so a stranger replaying a victim's public chain markers
//!      (every field of them is served by the lookup services) can never
//!      burn that identity's budget (the delta-verify's HIGH-1); a replay of a
//!      filed row is the `INSERT OR IGNORE` no-op it always was.
//!
//! Sizes are refused BEFORE any decode (`RECORD_POST_MAX_BODY_BYTES` on the
//! raw body, the hex length, then the script) — the gate's MED-3.
//!
//! Trust: a lying store can WITHHOLD a row, never forge one (every row is
//! identity-signed and client-verified, as today). Not a ts-stack
//! divergence: lookup services, their storage and the app-layer are the
//! reference's explicitly pluggable surfaces. RESIDUAL, named: identities are
//! free, so a stranger with many identities can still file many rows naming
//! a victim's pot under its OWN identities — one per identity per family per
//! pot, none of them rank-1 on the refund window, none of them a committed
//! seat, none of them a chain win; the per-IP rate rule at the edge is the
//! operator's step at the promotion (the same residual as `/cases`).
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Deserialize;
use worker::{Request, Response, Result, RouteContext};

use crate::auth::{AuthState, CallerAuth};
use overlay_discovery::collected::{
    parse_collected_marker, storage::CollectedRecord, CollectedMarker, COLLECTED_TAG,
};
use overlay_discovery::pot::covenant::CovenantParams;
use overlay_discovery::pot::{settle_signers_for_spend, SettleSigners};
use overlay_discovery::potparty::validity::canonical_der;
use overlay_discovery::potparty::{
    parse_potparty_marker, storage::PotpartyRecord, PotpartyMarker, POTPARTY_TAG,
    POTPARTY_TAG_V2,
};
use overlay_discovery::potrefund::{
    parse_potrefund_marker, storage::PotrefundRecord, PotrefundMarker, POTREFUND_TAG,
};
use overlay_discovery::result::{
    parse_result_marker, storage::ResultRecord, ResultMarker, RESULT_TAG, RESULT_TAG_V2,
};

/// The largest script a post may carry: the refund backup's own cap
/// (`POTREFUND_REFUND_MAX_LEN`, 100 KB of raw tx) plus its fixed fields.
pub const RECORD_POST_MAX_SCRIPT_BYTES: usize =
    overlay_discovery::potrefund::POTREFUND_REFUND_MAX_LEN + 512;

/// The largest request BODY read at all: the script as hex plus the JSON
/// frame. Checked on the raw bytes before the JSON parse, then on the hex
/// string before the decode, then on the script (the gate's MED-3: the old
/// order materialised ~60 MB of a 40 MB body before refusing it).
pub const RECORD_POST_MAX_BODY_BYTES: usize = RECORD_POST_MAX_SCRIPT_BYTES * 2 + 64;
const _: () = assert!(RECORD_POST_MAX_BODY_BYTES > RECORD_POST_MAX_SCRIPT_BYTES * 2);

/// One identity's filing budget per family per day (the honest need is a
/// handful per hand; this bounds a stranger's free growth of the tables
/// under ONE identity — the gate's MED-4).
pub const RECORD_FILINGS_PER_IDENTITY_PER_DAY: i64 = 200;
/// The day the budget is counted over, in the tables' unix SECONDS.
pub const RECORD_DAY_SECS: i64 = 86_400;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordKind {
    Potparty,
    Potrefund,
    Result,
    Collected,
}

impl RecordKind {
    pub const ALL: [RecordKind; 4] = [
        RecordKind::Potparty,
        RecordKind::Potrefund,
        RecordKind::Result,
        RecordKind::Collected,
    ];
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "potparty" => Some(Self::Potparty),
            "potrefund" => Some(Self::Potrefund),
            "result" => Some(Self::Result),
            "collected" => Some(Self::Collected),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Potparty => "potparty",
            Self::Potrefund => "potrefund",
            Self::Result => "result",
            Self::Collected => "collected",
        }
    }
    fn index(self) -> usize {
        match self {
            Self::Potparty => 0,
            Self::Potrefund => 1,
            Self::Result => 2,
            Self::Collected => 3,
        }
    }
}

/// FILED rows one poster may hold per `(family, game, pot)` — the honest
/// need, never more: `potparty` v1 + v2 share `potparty_records`; a `result`
/// is the winner's claim and, at most, its countersigned upgrade (a second
/// content); `potrefund` and `collected` are one row each. A re-file of the
/// SAME content is never counted against this (the key is excluded).
pub const fn filed_rows_cap(kind: RecordKind) -> i64 {
    match kind {
        RecordKind::Potparty | RecordKind::Result => 2,
        RecordKind::Potrefund | RecordKind::Collected => 1,
    }
}

#[derive(Debug, Deserialize)]
pub struct RecordPostBody {
    /// The marker's OP_RETURN locking script, hex — byte-for-byte what the
    /// marker output would have carried on chain.
    #[serde(rename = "scriptHex")]
    pub script_hex: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordRefusal {
    BadKind,
    BadScriptHex,
    /// The request body (or its hex) is larger than any marker can be —
    /// refused before any decode.
    BodyTooLarge,
    ScriptTooLarge,
    /// The bytes are not the canonical marker encoding (a non-minimal push,
    /// bytes after the last field, a missing prefix).
    NotCanonical,
    NotAMarker,
    /// The poster is not the marker's identity (or, for a result, neither seat).
    PosterMismatch,
    /// The marker's own signature does not verify under its identity (or is
    /// not canonical low-S DER).
    SignatureInvalid,
    /// A refund backup whose raw tx does not spend the pot outpoint it names.
    RefundDoesNotSpendThePot,
    /// A refund backup whose raw is not the pot's pre-signed spend by BOTH
    /// committed settle keys in the template's exact unlock shape,
    /// height-gated and non-final.
    RefundNotThePresignedSpend,
    /// The poster already holds its rows for this family, game and pot.
    TooManyFiled,
    /// The poster's filing budget for the day is spent.
    DailyCapReached,
}

impl RecordRefusal {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BadKind => "kind must be one of potparty | potrefund | result | collected",
            Self::BadScriptHex => "scriptHex must be hex",
            Self::BodyTooLarge => "request body too large for a marker",
            Self::ScriptTooLarge => "script too large",
            Self::NotCanonical => {
                "the script must be the canonical marker encoding (OP_FALSE OP_RETURN, minimal pushes, nothing after the last field)"
            }
            Self::NotAMarker => "the script is not a marker of this kind",
            Self::PosterMismatch => {
                "the poster must be the marker's identity (a result: one of its two seats)"
            }
            Self::SignatureInvalid => {
                "the marker's signature does not verify under its identity (canonical low-S DER required)"
            }
            Self::RefundDoesNotSpendThePot => {
                "the refund backup's raw tx does not spend the pot outpoint it names"
            }
            Self::RefundNotThePresignedSpend => {
                "the refund backup is not the pot's pre-signed spend by both committed settle keys in the template's unlock shape (height-gated, non-final)"
            }
            Self::TooManyFiled => {
                "this identity already holds its filed rows for this family, game and pot"
            }
            Self::DailyCapReached => "this identity's filing budget for the day is spent",
        }
    }
    pub fn status(self) -> u16 {
        match self {
            Self::BadKind | Self::BadScriptHex | Self::NotCanonical | Self::NotAMarker => 400,
            Self::BodyTooLarge | Self::ScriptTooLarge => 413,
            Self::PosterMismatch => 403,
            Self::TooManyFiled => 409,
            Self::SignatureInvalid
            | Self::RefundDoesNotSpendThePot
            | Self::RefundNotThePresignedSpend => 422,
            Self::DailyCapReached => 429,
        }
    }
}

// ── canonical bytes ─────────────────────────────────────────────────────────

/// The pushes of a CANONICALLY encoded marker script, or `None`.
///
/// Canonical = exactly what every client builder emits (`pushData` in
/// `potParty.ts` / `result.ts` / `stake.ts` / `hopParty.ts`): the two-byte
/// `OP_FALSE OP_RETURN` prefix, then data pushes only, each with the shortest
/// length encoding (`0x00` for empty data, a direct opcode below 76 bytes,
/// `PUSHDATA1` to 255, `PUSHDATA2` to 65535, `PUSHDATA4` above), and NOTHING
/// after the last push. The families' parsers (`read_pushes`) accept every
/// push encoding and stop silently at the first non-push byte, which is
/// right for a chain reader and wrong for a free writer: under them one
/// marker had unboundedly many byte spellings (the gate's HIGH-2). A marker
/// that is not canonical is refused, never normalised — the client never
/// sends one, so a non-canonical post is by construction not the client's.
pub fn canonical_marker_pushes(script: &[u8]) -> Option<Vec<&[u8]>> {
    if script.len() < 2 || script[0] != 0x00 || script[1] != 0x6a {
        return None;
    }
    let mut out = Vec::new();
    let mut i = 2usize;
    while i < script.len() {
        let op = script[i];
        i += 1;
        let (len, header) = match op {
            n if n < 0x4c => (n as usize, 0usize),
            0x4c => {
                let l = *script.get(i)? as usize;
                if l < 0x4c {
                    return None; // a direct push would have been shorter
                }
                (l, 1)
            }
            0x4d => {
                let b = script.get(i..i + 2)?;
                let l = b[0] as usize | ((b[1] as usize) << 8);
                if l <= 0xff {
                    return None;
                }
                (l, 2)
            }
            0x4e => {
                let b = script.get(i..i + 4)?;
                let l = b[0] as usize
                    | ((b[1] as usize) << 8)
                    | ((b[2] as usize) << 16)
                    | ((b[3] as usize) << 24);
                if l <= 0xffff {
                    return None;
                }
                (l, 4)
            }
            _ => return None, // a non-push opcode: never canonical
        };
        i += header;
        let end = i.checked_add(len)?;
        if end > script.len() {
            return None;
        }
        out.push(&script[i..end]);
        i = end;
    }
    Some(out)
}

// ── the content key ─────────────────────────────────────────────────────────

/// The synthetic outpoint a filed row is keyed by: `filed:` + the first 56
/// hex chars of the sha256 over the family tag and the marker's SIGNED
/// FIELDS, signatures EXCLUDED (each field length-prefixed, so no two field
/// lists collide). Never 64 hex, so no reader can mistake it for a chain
/// txid. The same marker — however it was pushed, whatever nonce signed it —
/// files the same key; a different game, pot, seat or claim a different one.
fn content_key(tag: &[u8], fields: &[&[u8]]) -> String {
    let mut pre = Vec::with_capacity(16 + tag.len() + fields.iter().map(|f| f.len() + 4).sum::<usize>());
    pre.extend_from_slice(b"LOW/filed/v1\n");
    pre.extend_from_slice(&(tag.len() as u32).to_le_bytes());
    pre.extend_from_slice(tag);
    for f in fields {
        pre.extend_from_slice(&(f.len() as u32).to_le_bytes());
        pre.extend_from_slice(f);
    }
    let h = hex::encode(bsv_rs::primitives::hash::sha256(&pre));
    format!("filed:{}", &h[..56])
}

/// A potparty marker's key: v1 = identity ‖ opponent ‖ game ‖ pot ‖ vout ‖
/// height; v2 additionally the seat settle pubkey (a v2 row is a different
/// row from the v1 one — the reader's `decideV2Step` needs both).
pub fn potparty_content_key(m: &PotpartyMarker) -> String {
    let vout = m.pot_vout.to_le_bytes();
    let height = m.recovery_height.to_le_bytes();
    let mut fields: Vec<&[u8]> = vec![
        &m.identity,
        &m.opponent,
        &m.game_id,
        &m.pot_txid,
        &vout,
        &height,
    ];
    let tag = match &m.seat_settle_pubkey {
        Some(pk) => {
            fields.push(pk);
            POTPARTY_TAG_V2
        }
        None => POTPARTY_TAG,
    };
    content_key(tag, &fields)
}

/// A refund backup's key: identity ‖ game ‖ pot ‖ vout ‖ the refund bytes.
pub fn potrefund_content_key(m: &PotrefundMarker) -> String {
    let vout = m.pot_vout.to_le_bytes();
    content_key(
        POTREFUND_TAG,
        &[&m.identity, &m.game_id, &m.pot_txid, &vout, &m.refund_raw],
    )
}

/// A result's key: game ‖ winner ‖ loser ‖ pot ‖ settle ‖ (v2: cards) ‖
/// whether it is countersigned (the tier-2 upgrade is a second content).
pub fn result_content_key(m: &ResultMarker) -> String {
    let countersigned = [u8::from(m.loser_sig.is_some())];
    let mut fields: Vec<&[u8]> = vec![
        &m.game_id,
        &m.winner,
        &m.loser,
        &m.pot_txid,
        &m.settle_txid,
    ];
    let tag = match &m.cards {
        Some(c) => {
            fields.push(c);
            RESULT_TAG_V2
        }
        None => RESULT_TAG,
    };
    fields.push(&countersigned);
    content_key(tag, &fields)
}

/// A collected marker's key: game ‖ identity.
pub fn collected_content_key(m: &CollectedMarker) -> String {
    content_key(COLLECTED_TAG, &[&m.game_id, &m.identity_key])
}

// ── the challenges the client signs ─────────────────────────────────────────

pub fn potrefund_protocol() -> bsv_rs::wallet::Protocol {
    bsv_rs::wallet::Protocol::new(bsv_rs::wallet::SecurityLevel::App, "low potrefund")
}

pub fn collected_protocol() -> bsv_rs::wallet::Protocol {
    bsv_rs::wallet::Protocol::new(bsv_rs::wallet::SecurityLevel::App, "low collected")
}

/// The client's `potRefundChallenge`: TAG ‖ identity ‖ gameId ‖ potTxid ‖
/// vout (LE u32) ‖ the raw refund bytes. Signed under `[1,'low potrefund']`,
/// keyID = gameId (lowercase hex), counterparty anyone.
pub fn potrefund_challenge(
    identity: &[u8],
    game_id: &[u8; 32],
    pot_txid: &[u8; 32],
    pot_vout: u32,
    refund_raw: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(POTREFUND_TAG.len() + 33 + 32 + 32 + 4 + refund_raw.len());
    out.extend_from_slice(POTREFUND_TAG);
    out.extend_from_slice(identity);
    out.extend_from_slice(game_id);
    out.extend_from_slice(pot_txid);
    out.extend_from_slice(&pot_vout.to_le_bytes());
    out.extend_from_slice(refund_raw);
    out
}

/// The client's `collectedChallenge`: `LOW-collected\nv1\ngid=<gid>\nid=<identity>`
/// (both lowercase hex), signed under `[1,'low collected']`, keyID = gameId,
/// counterparty anyone.
pub fn collected_challenge(game_id_lc: &str, identity_lc: &str) -> Vec<u8> {
    format!("LOW-collected\nv1\ngid={game_id_lc}\nid={identity_lc}").into_bytes()
}

/// `anyone_sig_verifies` behind the canonical-DER gate — the three families
/// whose serve-time verifier has no such bar get it at the FILING, where a
/// row is free (the gate's HIGH-2; `canonical_der`'s own doc has the why).
fn canonical_anyone_sig_verifies(
    signer_identity_hex: &str,
    key_id: &str,
    challenge: &[u8],
    sig: &[u8],
    protocol: bsv_rs::wallet::Protocol,
) -> bool {
    canonical_der(sig).is_some()
        && overlay_discovery::result::validity::anyone_sig_verifies(
            signer_identity_hex,
            key_id,
            challenge,
            &hex::encode(sig),
            protocol,
        )
}

// ── the refund backup's pot bind ────────────────────────────────────────────

/// A refund backup binds the pot it names: its raw tx's first input spends
/// that outpoint. The client's own selector (`selectRefundBackupRaw`) is this
/// same predicate — which is exactly why it is NOT the bar (see
/// [`refund_is_the_presigned_spend`]); it stays as the cheap first refusal.
pub fn refund_spends_pot(refund_raw: &[u8], pot_txid: &[u8; 32], pot_vout: u32) -> bool {
    let Ok(tx) = bsv_rs::transaction::Transaction::from_binary(refund_raw) else {
        return false;
    };
    let Some(input) = tx.inputs.first() else {
        return false;
    };
    let want = hex::encode(pot_txid);
    input
        .source_txid
        .as_deref()
        .is_some_and(|t| t.eq_ignore_ascii_case(&want))
        && input.source_output_index == pot_vout
}

/// The pot a refund backup names, as the index holds it: the covenant params
/// decoded at admission (`pot_records`, `paramsDecoded = 1`) and the funded
/// value. Read by the route from `decoded_pots_sql` after the marker's
/// signature verified; a pot the index does not hold files at rank 0
/// ([`bind_refund_backup`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PotContext {
    pub params: CovenantParams,
    pub pot_sats: u64,
}

/// The data pushes of an unlocking script (any push encoding; nothing but
/// pushes; the whole script consumed), or `None`.
fn unlock_pushes(script: &[u8]) -> Option<Vec<&[u8]>> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < script.len() {
        let op = script[i];
        i += 1;
        let (len, header) = match op {
            n if n < 0x4c => (n as usize, 0usize),
            0x4c => (*script.get(i)? as usize, 1),
            0x4d => {
                let b = script.get(i..i + 2)?;
                (b[0] as usize | ((b[1] as usize) << 8), 2)
            }
            0x4e => {
                let b = script.get(i..i + 4)?;
                (
                    b[0] as usize
                        | ((b[1] as usize) << 8)
                        | ((b[2] as usize) << 16)
                        | ((b[3] as usize) << 24),
                    4,
                )
            }
            _ => return None,
        };
        i += header;
        let end = i.checked_add(len)?;
        if end > script.len() {
            return None;
        }
        out.push(&script[i..end]);
        i = end;
    }
    Some(out)
}

/// The sighash-type byte every LOW pot spend signs with.
const SIGHASH_ALL_FORKID_BYTE: u8 = (bsv_rs::primitives::bsv::sighash::SIGHASH_ALL
    | bsv_rs::primitives::bsv::sighash::SIGHASH_FORKID) as u8;

/// THE bar on a refund backup (the gate's HIGH-1 + the delta-verify's MED-2):
/// the raw must be the pot's PRE-SIGNED spend in the covenant template's
/// EXACT unlock shape — `low_spend::template_pot_unlock`'s three pushes
/// `[sigA ‖ 0x41, sigB ‖ 0x41, preimage]`: the two signatures canonical DER,
/// verifying against the committed settle keys IN LOCK ORDER over the
/// BIP-143 digest of the rebuilt covenant lock and the funded value, the
/// preimage byte-equal to the one this input hashes — height-gated at the
/// committed recovery height and non-final (a cooperative SETTLE also
/// carries both seats' signatures but is final; a tower-parked sibling
/// carries the tower's key). `settle_signers_for_spend` (the missing_j
/// discriminator, the one SSOT for "who signed this spend") is kept as the
/// belt: it is deliberately malleation-TOLERANT (it scans every push), which
/// is exactly why it cannot be the bar here — the counterparty holds both
/// signatures after the 2-of-2 exchange and could re-encode the unlock into
/// a variant the network would refuse; the shape check pins the bytes to the
/// one spend the seats built. A stranger cannot produce any of this (it
/// needs a key it does not hold). Nothing here is broadcast: the client still
/// judges the bytes against the pot's lock before it ever pushes them.
pub fn refund_is_the_presigned_spend(
    ctx: &PotContext,
    refund_raw: &[u8],
    pot_txid: &[u8; 32],
    pot_vout: u32,
) -> std::result::Result<(), RecordRefusal> {
    use bsv_rs::primitives::bsv::sighash::{
        build_sighash_preimage, compute_sighash_for_signing, parse_transaction, SighashParams,
        SIGHASH_ALL, SIGHASH_FORKID,
    };
    if !refund_spends_pot(refund_raw, pot_txid, pot_vout) {
        return Err(RecordRefusal::RefundDoesNotSpendThePot);
    }
    let Ok(tx) = bsv_rs::transaction::Transaction::from_binary(refund_raw) else {
        return Err(RecordRefusal::RefundDoesNotSpendThePot);
    };
    let Some(input) = tx.inputs.first() else {
        return Err(RecordRefusal::RefundDoesNotSpendThePot);
    };
    if u64::from(tx.lock_time) < ctx.params.recovery_height {
        return Err(RecordRefusal::RefundNotThePresignedSpend);
    }
    if input.sequence == 0xffff_ffff {
        return Err(RecordRefusal::RefundNotThePresignedSpend);
    }
    // The template's unlock shape, byte for byte.
    let Ok(parsed) = parse_transaction(refund_raw) else {
        return Err(RecordRefusal::RefundNotThePresignedSpend);
    };
    let Some(raw_input) = parsed.inputs.first() else {
        return Err(RecordRefusal::RefundNotThePresignedSpend);
    };
    let Some(pushes) = unlock_pushes(&raw_input.script) else {
        return Err(RecordRefusal::RefundNotThePresignedSpend);
    };
    if pushes.len() != 3 {
        return Err(RecordRefusal::RefundNotThePresignedSpend);
    }
    if ctx.params.stake_a.checked_add(ctx.params.stake_b) != Some(ctx.pot_sats) {
        return Err(RecordRefusal::RefundNotThePresignedSpend); // not the pot the params describe
    }
    let lock = overlay_discovery::pot::covenant_lock_of(&ctx.params);
    let params = SighashParams {
        version: parsed.version,
        inputs: &parsed.inputs,
        outputs: &parsed.outputs,
        locktime: parsed.locktime,
        input_index: 0,
        subscript: &lock,
        satoshis: ctx.pot_sats,
        scope: SIGHASH_ALL | SIGHASH_FORKID,
    };
    if pushes[2] != build_sighash_preimage(&params).as_slice() {
        return Err(RecordRefusal::RefundNotThePresignedSpend);
    }
    let digest = compute_sighash_for_signing(&params);
    for (push, key) in [(pushes[0], &ctx.params.pub_a), (pushes[1], &ctx.params.pub_b)] {
        let Some((&sighash_byte, der)) = push.split_last() else {
            return Err(RecordRefusal::RefundNotThePresignedSpend);
        };
        if sighash_byte != SIGHASH_ALL_FORKID_BYTE {
            return Err(RecordRefusal::RefundNotThePresignedSpend);
        }
        let Some(sig) = canonical_der(der) else {
            return Err(RecordRefusal::RefundNotThePresignedSpend);
        };
        let Ok(pk) = bsv_rs::primitives::ec::PublicKey::from_bytes(key) else {
            return Err(RecordRefusal::RefundNotThePresignedSpend);
        };
        if !sig.verify(&digest, &pk) {
            return Err(RecordRefusal::RefundNotThePresignedSpend);
        }
    }
    // The belt: the SSOT classifier agrees it is the seats' spend.
    match settle_signers_for_spend(&ctx.params, ctx.pot_sats, refund_raw, 0) {
        Some(SettleSigners::Coop) => Ok(()),
        _ => Err(RecordRefusal::RefundNotThePresignedSpend),
    }
}

/// The refund backup's verdict for the row: `Ok(true)` = the index holds the
/// pot and the raw is its pre-signed spend (rank 1); `Ok(false)` = the index
/// does not hold the pot yet (rank 0 — the belt survives an unindexed pot;
/// the client re-files until the row latches); `Err` = the index holds the
/// pot and the raw is NOT its refund (refused, never written).
pub fn bind_refund_backup(
    record: &PotrefundRecord,
    pot: Option<&PotContext>,
) -> std::result::Result<bool, RecordRefusal> {
    let Some(ctx) = pot else {
        return Ok(false);
    };
    let (Ok(raw), Ok(pot_txid)) = (
        hex::decode(&record.refund_raw_hex),
        hex::decode(&record.pot_txid),
    ) else {
        return Err(RecordRefusal::RefundNotThePresignedSpend);
    };
    let Ok(pot_txid) = <[u8; 32]>::try_from(pot_txid.as_slice()) else {
        return Err(RecordRefusal::RefundNotThePresignedSpend);
    };
    refund_is_the_presigned_spend(ctx, &raw, &pot_txid, record.pot_vout).map(|()| true)
}

// ── the verifier ────────────────────────────────────────────────────────────

/// What a verified post writes — the family's own record, keyed by the
/// content key, with the validity the overlay would compute at admission
/// carried as the VERDICT that computed it (never a literal at the bind —
/// the gate's LOW-2).
#[derive(Debug, Clone, PartialEq)]
pub enum VerifiedRecord {
    /// The record and `record_sig_valid`'s verdict (always true here — a
    /// false one is refused — carried so the write binds the verdict).
    Potparty(PotpartyRecord, bool),
    /// The record and [`bind_refund_backup`]'s verdict — `false` until the
    /// route binds it (the pot is read AFTER the signature, never before: a
    /// free `pot_records` read per anonymous POST was the delta-verify's
    /// MED-3); `true` = rank 1.
    Potrefund(PotrefundRecord, bool),
    /// The record and its `claim_tier` (1 = the winner's claim, 2 = countersigned).
    Result(ResultRecord, i64),
    Collected(CollectedRecord),
}

impl VerifiedRecord {
    pub fn key(&self) -> &str {
        match self {
            Self::Potparty(r, _) => &r.txid,
            Self::Potrefund(r, _) => &r.txid,
            Self::Result(r, _) => &r.txid,
            Self::Collected(r) => &r.txid,
        }
    }
    pub fn kind(&self) -> RecordKind {
        match self {
            Self::Potparty(..) => RecordKind::Potparty,
            Self::Potrefund(..) => RecordKind::Potrefund,
            Self::Result(..) => RecordKind::Result,
            Self::Collected(_) => RecordKind::Collected,
        }
    }
}

/// The pure verifier: the bytes, the kind and the resolved poster in; the
/// record to write out, or the refusal. A refund backup comes out UNBOUND
/// (`Potrefund(_, false)`); the route binds it with [`bind_refund_backup`]
/// after the signature has been checked.
pub fn verify_record_post(
    kind: RecordKind,
    script: &[u8],
    poster_lc: &str,
) -> std::result::Result<VerifiedRecord, RecordRefusal> {
    if script.len() > RECORD_POST_MAX_SCRIPT_BYTES {
        return Err(RecordRefusal::ScriptTooLarge);
    }
    if canonical_marker_pushes(script).is_none() {
        return Err(RecordRefusal::NotCanonical);
    }
    match kind {
        RecordKind::Potparty => {
            let m = parse_potparty_marker(script).ok_or(RecordRefusal::NotAMarker)?;
            let record = PotpartyRecord {
                identity: hex::encode(&m.identity),
                opponent_identity: hex::encode(&m.opponent),
                game_id: hex::encode(m.game_id),
                pot_txid: hex::encode(m.pot_txid),
                pot_vout: m.pot_vout,
                recovery_height: m.recovery_height,
                sig_hex: hex::encode(&m.sig),
                seat_settle_pubkey: m.seat_settle_pubkey.as_ref().map(hex::encode),
                seat_sig_hex: m.seat_sig.as_ref().map(hex::encode),
                txid: potparty_content_key(&m),
                output_index: 0,
                created_at: 0,
            };
            if record.identity != poster_lc {
                return Err(RecordRefusal::PosterMismatch);
            }
            // `record_sig_valid` verifies the identity signature (and the v2
            // seat signature) through the canonical-DER gate already.
            let sig_valid = overlay_discovery::potparty::validity::record_sig_valid(&record);
            if !sig_valid {
                return Err(RecordRefusal::SignatureInvalid);
            }
            Ok(VerifiedRecord::Potparty(record, sig_valid))
        }
        RecordKind::Potrefund => {
            let m = parse_potrefund_marker(script).ok_or(RecordRefusal::NotAMarker)?;
            let identity = hex::encode(&m.identity);
            if identity != poster_lc {
                return Err(RecordRefusal::PosterMismatch);
            }
            let game_id = hex::encode(m.game_id);
            let challenge = potrefund_challenge(
                &m.identity,
                &m.game_id,
                &m.pot_txid,
                m.pot_vout,
                &m.refund_raw,
            );
            if !canonical_anyone_sig_verifies(
                &identity,
                &game_id,
                &challenge,
                &m.sig,
                potrefund_protocol(),
            ) {
                return Err(RecordRefusal::SignatureInvalid);
            }
            if !refund_spends_pot(&m.refund_raw, &m.pot_txid, m.pot_vout) {
                return Err(RecordRefusal::RefundDoesNotSpendThePot);
            }
            Ok(VerifiedRecord::Potrefund(
                PotrefundRecord {
                    identity,
                    game_id,
                    pot_txid: hex::encode(m.pot_txid),
                    pot_vout: m.pot_vout,
                    refund_raw_hex: hex::encode(&m.refund_raw),
                    sig_hex: hex::encode(&m.sig),
                    txid: potrefund_content_key(&m),
                    output_index: 0,
                    created_at: 0,
                },
                false,
            ))
        }
        RecordKind::Result => {
            let m = parse_result_marker(script).ok_or(RecordRefusal::NotAMarker)?;
            // The canonical-DER gate on BOTH signatures before the tier is
            // computed (`claim_tier` deliberately has none — its serve-time
            // parity; the filing is where a row is free).
            if canonical_der(&m.winner_sig).is_none() {
                return Err(RecordRefusal::SignatureInvalid);
            }
            if m.loser_sig.as_deref().is_some_and(|s| canonical_der(s).is_none()) {
                return Err(RecordRefusal::SignatureInvalid);
            }
            let record = ResultRecord {
                game_id: hex::encode(m.game_id),
                winner: hex::encode(&m.winner),
                loser: hex::encode(&m.loser),
                pot_txid: hex::encode(m.pot_txid),
                settle_txid: hex::encode(m.settle_txid),
                winner_sig_hex: hex::encode(&m.winner_sig),
                loser_sig_hex: m.loser_sig.as_deref().map(hex::encode),
                cards_hex: m.cards.map(hex::encode),
                txid: result_content_key(&m),
                output_index: 0,
                created_at: 0,
            };
            if record.winner != poster_lc && record.loser != poster_lc {
                return Err(RecordRefusal::PosterMismatch);
            }
            let tier = overlay_discovery::result::validity::claim_tier(&record);
            if tier == 0 {
                return Err(RecordRefusal::SignatureInvalid);
            }
            // A countersignature that is present but does not verify is a
            // tier-1 row under `claim_tier`; at the filing it is refused —
            // the content key says "countersigned", so the row must be.
            if record.loser_sig_hex.is_some() && tier != 2 {
                return Err(RecordRefusal::SignatureInvalid);
            }
            Ok(VerifiedRecord::Result(record, tier))
        }
        RecordKind::Collected => {
            let m = parse_collected_marker(script).ok_or(RecordRefusal::NotAMarker)?;
            let identity = hex::encode(&m.identity_key);
            if identity != poster_lc {
                return Err(RecordRefusal::PosterMismatch);
            }
            let game_id = hex::encode(m.game_id);
            let challenge = collected_challenge(&game_id, &identity);
            if !canonical_anyone_sig_verifies(
                &identity,
                &game_id,
                &challenge,
                &m.sig,
                collected_protocol(),
            ) {
                return Err(RecordRefusal::SignatureInvalid);
            }
            Ok(VerifiedRecord::Collected(CollectedRecord {
                identity,
                game_id,
                txid: collected_content_key(&m),
                output_index: 0,
                sig_hex: Some(hex::encode(&m.sig)),
            }))
        }
    }
}

// ── The writes: the SAME column lists the overlay's lookup services use ──────
// (prepared against the production schema in `tests/sql_prepares_sqlite.rs`).

pub const POTPARTY_FILE_SQL: &str = "INSERT OR IGNORE INTO potparty_records \
     (identity, opponentIdentity, gameId, potTxid, potVout, \
      recoveryHeight, sigHex, seatSettlePubkey, seatSigHex, \
      txid, outputIndex, createdAt, sigValid) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

/// The overlay's column list plus `refundValid` — the committed-key verdict
/// this route computed (migration 147; a chain-admitted row is NULL).
pub const POTREFUND_FILE_SQL: &str = "INSERT OR IGNORE INTO potrefund_records \
     (identity, gameId, potTxid, potVout, refundRawHex, \
      sigHex, txid, outputIndex, createdAt, refundValid) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

pub const RESULT_FILE_SQL: &str = "INSERT OR IGNORE INTO result_markers_v2 \
     (gameId, winner, loser, potTxid, settleTxid, winnerSigHex, \
      loserSigHex, cardsHex, txid, outputIndex, createdAt, claimValid) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

pub const COLLECTED_FILE_SQL: &str = "INSERT OR IGNORE INTO collected_markers_v2 \
     (identity, gameId, txid, outputIndex, sigHex, createdAt) \
     VALUES (?, ?, ?, ?, ?, ?)";

/// The leaderboard row the overlay writes beside every result marker —
/// byte-for-byte `result_write::lb_row_insert_sql()` (the overlay crate is a
/// dev-dependency only; `tests/sql_prepares_sqlite.rs` pins the equality and
/// prepares it against the production schema).
pub const LB_ROW_FILE_SQL: &str = "INSERT OR IGNORE INTO lb_marker_rows \
             (txid, outputIndex, gameId, winner, loser, potTxid, settleTxid, \
              winnerSigHex, loserSigHex, cardsHex, createdAt, claimValid, rn, \
              potCreatedAt, potFirstMarkerAt, orderAt, unknownPot) \
             SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
                    (SELECT COUNT(*) + 1 FROM lb_marker_rows WHERE potTxid = ?), \
                    (SELECT MIN(createdAt) FROM pot_records WHERE txid = ?), \
                    COALESCE((SELECT MIN(potFirstMarkerAt) FROM lb_marker_rows WHERE potTxid = ?), ?), \
                    COALESCE((SELECT MIN(createdAt) FROM pot_records WHERE txid = ?), \
                             (SELECT MIN(potFirstMarkerAt) FROM lb_marker_rows WHERE potTxid = ?), ?), \
                    CASE WHEN EXISTS (SELECT 1 FROM pot_records WHERE txid = ?) THEN 0 ELSE 1 END \
             WHERE (SELECT COUNT(*) FROM lb_marker_rows WHERE potTxid = ?) < ?";

pub fn lb_row_file_sql() -> &'static str {
    LB_ROW_FILE_SQL
}

// ── The caps (FILED rows only — `txid LIKE 'filed:%'` — never chain rows) ──
// BINDS: the per-pot counts take (poster, gameId, potTxid, the content key
// being filed — excluded, so a re-file of the same content never counts);
// `collected_markers_v2` has no pot column, so (poster, gameId, key). The
// day counts take (poster, the day's floor in unix seconds).

pub const POTPARTY_FILED_ROWS_SQL: &str = "SELECT COUNT(*) AS n FROM potparty_records \
     WHERE identity = ?1 AND gameId = ?2 AND potTxid = ?3 AND txid LIKE 'filed:%' AND txid <> ?4";
pub const POTREFUND_FILED_ROWS_SQL: &str = "SELECT COUNT(*) AS n FROM potrefund_records \
     WHERE identity = ?1 AND gameId = ?2 AND potTxid = ?3 AND txid LIKE 'filed:%' AND txid <> ?4";
pub const RESULT_FILED_ROWS_SQL: &str = "SELECT COUNT(*) AS n FROM result_markers_v2 \
     WHERE winner = ?1 AND gameId = ?2 AND potTxid = ?3 AND txid LIKE 'filed:%' AND txid <> ?4";
pub const COLLECTED_FILED_ROWS_SQL: &str = "SELECT COUNT(*) AS n FROM collected_markers_v2 \
     WHERE identity = ?1 AND gameId = ?2 AND txid LIKE 'filed:%' AND txid <> ?3";

/// The late latch: a refund backup filed at rank 0 (the pot unindexed then)
/// re-filed once the index holds the pot — the INSERT is the no-op it always
/// is, the verdict is written. Never demotes (a 1 stays 1).
pub const POTREFUND_LATCH_SQL: &str = "UPDATE potrefund_records SET refundValid = 1 \
     WHERE txid = ?1 AND COALESCE(refundValid, 0) = 0";

// The chain no-op (the delta-verify's HIGH-1): a marker the CHAIN already
// holds for this identity, game, pot and VERSION. BINDS: (poster, gameId,
// potTxid, the version flag) — potparty: `seatSettlePubkey IS NULL` (1 = v1);
// result: `loserSigHex IS NULL` (1 = the plain claim); potrefund: (poster,
// gameId, potTxid); collected: (poster, gameId).
pub const POTPARTY_CHAIN_ROWS_SQL: &str = "SELECT COUNT(*) AS n FROM potparty_records \
     WHERE identity = ?1 AND gameId = ?2 AND potTxid = ?3 AND txid NOT LIKE 'filed:%' \
       AND (seatSettlePubkey IS NULL) = ?4";
pub const POTREFUND_CHAIN_ROWS_SQL: &str = "SELECT COUNT(*) AS n FROM potrefund_records \
     WHERE identity = ?1 AND gameId = ?2 AND potTxid = ?3 AND txid NOT LIKE 'filed:%'";
pub const RESULT_CHAIN_ROWS_SQL: &str = "SELECT COUNT(*) AS n FROM result_markers_v2 \
     WHERE winner = ?1 AND gameId = ?2 AND potTxid = ?3 AND txid NOT LIKE 'filed:%' \
       AND (loserSigHex IS NULL) = ?4";
pub const COLLECTED_CHAIN_ROWS_SQL: &str = "SELECT COUNT(*) AS n FROM collected_markers_v2 \
     WHERE identity = ?1 AND gameId = ?2 AND txid NOT LIKE 'filed:%'";

/// The chain no-op statement and its binds for a verified record (a pure
/// value, for the same reason as [`cap_queries`]).
pub fn chain_rows_query(v: &VerifiedRecord) -> (&'static str, Vec<String>) {
    match v {
        VerifiedRecord::Potparty(r, _) => (
            POTPARTY_CHAIN_ROWS_SQL,
            vec![
                r.identity.clone(),
                r.game_id.clone(),
                r.pot_txid.clone(),
                if r.seat_settle_pubkey.is_none() { "1" } else { "0" }.to_string(),
            ],
        ),
        VerifiedRecord::Potrefund(r, _) => (
            POTREFUND_CHAIN_ROWS_SQL,
            vec![r.identity.clone(), r.game_id.clone(), r.pot_txid.clone()],
        ),
        VerifiedRecord::Result(r, _) => (
            RESULT_CHAIN_ROWS_SQL,
            vec![
                r.winner.clone(),
                r.game_id.clone(),
                r.pot_txid.clone(),
                if r.loser_sig_hex.is_none() { "1" } else { "0" }.to_string(),
            ],
        ),
        VerifiedRecord::Collected(r) => (
            COLLECTED_CHAIN_ROWS_SQL,
            vec![r.identity.clone(), r.game_id.clone()],
        ),
    }
}

pub const POTPARTY_FILED_TODAY_SQL: &str = "SELECT COUNT(*) AS n FROM potparty_records \
     WHERE identity = ?1 AND txid LIKE 'filed:%' AND createdAt >= ?2";
pub const POTREFUND_FILED_TODAY_SQL: &str = "SELECT COUNT(*) AS n FROM potrefund_records \
     WHERE identity = ?1 AND txid LIKE 'filed:%' AND createdAt >= ?2";
pub const RESULT_FILED_TODAY_SQL: &str = "SELECT COUNT(*) AS n FROM result_markers_v2 \
     WHERE winner = ?1 AND txid LIKE 'filed:%' AND createdAt >= ?2";
pub const COLLECTED_FILED_TODAY_SQL: &str = "SELECT COUNT(*) AS n FROM collected_markers_v2 \
     WHERE identity = ?1 AND txid LIKE 'filed:%' AND createdAt >= ?2";

/// The two cap statements and their binds for a verified record, as a pure
/// value (so a native test can see what the route binds — the same
/// unreachability lesson as the overlay's `by_pot_query`).
pub fn cap_queries(v: &VerifiedRecord) -> (&'static str, Vec<String>, &'static str, String) {
    match v {
        VerifiedRecord::Potparty(r, _) => (
            POTPARTY_FILED_ROWS_SQL,
            vec![r.identity.clone(), r.game_id.clone(), r.pot_txid.clone(), r.txid.clone()],
            POTPARTY_FILED_TODAY_SQL,
            r.identity.clone(),
        ),
        VerifiedRecord::Potrefund(r, _) => (
            POTREFUND_FILED_ROWS_SQL,
            vec![r.identity.clone(), r.game_id.clone(), r.pot_txid.clone(), r.txid.clone()],
            POTREFUND_FILED_TODAY_SQL,
            r.identity.clone(),
        ),
        VerifiedRecord::Result(r, _) => (
            RESULT_FILED_ROWS_SQL,
            vec![r.winner.clone(), r.game_id.clone(), r.pot_txid.clone(), r.txid.clone()],
            RESULT_FILED_TODAY_SQL,
            r.winner.clone(),
        ),
        VerifiedRecord::Collected(r) => (
            COLLECTED_FILED_ROWS_SQL,
            vec![r.identity.clone(), r.game_id.clone(), r.txid.clone()],
            COLLECTED_FILED_TODAY_SQL,
            r.identity.clone(),
        ),
    }
}

/// The pure cap decision from the two counts.
pub fn cap_refusal(kind: RecordKind, rows_for_pot: i64, rows_today: i64) -> Option<RecordRefusal> {
    if rows_for_pot >= filed_rows_cap(kind) {
        return Some(RecordRefusal::TooManyFiled);
    }
    if rows_today >= RECORD_FILINGS_PER_IDENTITY_PER_DAY {
        return Some(RecordRefusal::DailyCapReached);
    }
    None
}

// ── counters (per isolate; a soak/monitoring surface on /health, Rule 13) ──

static FILED_BY_KIND: [AtomicU64; 4] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];
static REFUSED_BY_KIND: [AtomicU64; 4] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];
static TOO_MANY_FILED_BY_KIND: [AtomicU64; 4] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];
static DAILY_CAP_BY_KIND: [AtomicU64; 4] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];
static ALREADY_INDEXED_BY_KIND: [AtomicU64; 4] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];
/// Refund backups written at rank 0 (the pot not indexed at filing time).
static REFUNDS_FILED_UNBOUND: AtomicU64 = AtomicU64::new(0);
/// Rank-0 refund rows latched to rank 1 by a later re-file.
static REFUNDS_LATCHED_LATER: AtomicU64 = AtomicU64::new(0);
static ANONYMOUS_FILED_BY_KIND: [AtomicU64; 4] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];

fn bump(c: &[AtomicU64; 4], kind: RecordKind) {
    c[kind.index()].fetch_add(1, Ordering::Relaxed);
}

fn by_kind(c: &[AtomicU64; 4]) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    for k in RecordKind::ALL {
        m.insert(
            k.as_str().to_string(),
            serde_json::json!(c[k.index()].load(Ordering::Relaxed)),
        );
    }
    serde_json::Value::Object(m)
}

/// The `/health` surface of this route: filings, refusals and cap hits per
/// family, and how many filings came from an ANONYMOUS caller (the poster
/// was the `?identity=` claim — see the module doc, step 4).
pub fn record_health_json() -> serde_json::Value {
    serde_json::json!({
        "countersScope": "isolate",
        "filedByKind": by_kind(&FILED_BY_KIND),
        "refusedByKind": by_kind(&REFUSED_BY_KIND),
        "tooManyFiledByKind": by_kind(&TOO_MANY_FILED_BY_KIND),
        "dailyCapByKind": by_kind(&DAILY_CAP_BY_KIND),
        "alreadyIndexedByKind": by_kind(&ALREADY_INDEXED_BY_KIND),
        "anonymousFiledByKind": by_kind(&ANONYMOUS_FILED_BY_KIND),
        "refundsFiledUnbound": REFUNDS_FILED_UNBOUND.load(Ordering::Relaxed),
        "refundsLatchedLater": REFUNDS_LATCHED_LATER.load(Ordering::Relaxed),
        "filedRowsCap": {
            "potparty": filed_rows_cap(RecordKind::Potparty),
            "potrefund": filed_rows_cap(RecordKind::Potrefund),
            "result": filed_rows_cap(RecordKind::Result),
            "collected": filed_rows_cap(RecordKind::Collected),
        },
        "filingsPerIdentityPerDay": RECORD_FILINGS_PER_IDENTITY_PER_DAY,
    })
}

// ── the route ───────────────────────────────────────────────────────────────

fn js(s: &str) -> worker::wasm_bindgen::JsValue {
    s.into()
}
fn js_opt(s: Option<&str>) -> worker::wasm_bindgen::JsValue {
    s.map_or(worker::wasm_bindgen::JsValue::NULL, |v| v.into())
}
fn js_num(n: i64) -> worker::wasm_bindgen::JsValue {
    (n as f64).into()
}

#[derive(Deserialize)]
struct CountRowD1 {
    #[serde(default)]
    n: Option<f64>,
}

/// The decoded-param columns of `pot_records` for ONE outpoint
/// (`decoded_pots_sql(1)`), as D1 returns them.
#[derive(Deserialize)]
struct PotRowD1 {
    #[serde(rename = "lockKind", default)]
    lock_kind: Option<String>,
    #[serde(rename = "pubA", default)]
    pub_a: Option<String>,
    #[serde(rename = "pubB", default)]
    pub_b: Option<String>,
    #[serde(rename = "pubTower", default)]
    pub_tower: Option<String>,
    #[serde(rename = "payPkhA", default)]
    pay_pkh_a: Option<String>,
    #[serde(rename = "payPkhB", default)]
    pay_pkh_b: Option<String>,
    #[serde(rename = "rakePkh", default)]
    rake_pkh: Option<String>,
    #[serde(rename = "stakeA", default)]
    stake_a: Option<f64>,
    #[serde(rename = "stakeB", default)]
    stake_b: Option<f64>,
    #[serde(rename = "feeSats", default)]
    fee_sats: Option<f64>,
    #[serde(rename = "covRecoveryHeight", default)]
    cov_recovery_height: Option<f64>,
    #[serde(rename = "potSats", default)]
    pot_sats: Option<f64>,
}

impl PotRowD1 {
    fn context(&self) -> Option<PotContext> {
        if self.lock_kind.as_deref() != Some("covenant") {
            return None;
        }
        let params = overlay_discovery::pot::covenant::covenant_params_from_hex(
            self.pub_a.as_deref()?,
            self.pub_b.as_deref()?,
            self.pub_tower.as_deref()?,
            self.pay_pkh_a.as_deref()?,
            self.pay_pkh_b.as_deref()?,
            self.rake_pkh.as_deref()?,
            self.stake_a? as u64,
            self.stake_b? as u64,
            self.fee_sats? as u64,
            self.cov_recovery_height? as u64,
        )?;
        Some(PotContext {
            params,
            pot_sats: self.pot_sats? as u64,
        })
    }
}

async fn read_pot_context(
    db: &worker::D1Database,
    pot_txid_lc: &str,
    pot_vout: u32,
) -> Result<Option<PotContext>> {
    let row = db
        .prepare(crate::results::decoded_pots_sql(1))
        .bind(&[js(pot_txid_lc), js_num(i64::from(pot_vout))])?
        .first::<PotRowD1>(None)
        .await?;
    Ok(row.and_then(|r| r.context()))
}

async fn count(db: &worker::D1Database, sql: &str, binds: &[worker::wasm_bindgen::JsValue]) -> Result<i64> {
    let row = db.prepare(sql).bind(binds)?.first::<CountRowD1>(None).await?;
    Ok(row.and_then(|r| r.n).map_or(0, |n| n as i64))
}

fn refuse(kind: Option<RecordKind>, r: RecordRefusal) -> Result<Response> {
    if let Some(k) = kind {
        bump(&REFUSED_BY_KIND, k);
    }
    crate::routes::json_error(r.as_str(), r.status())
}

/// `POST /record?kind=<family>&identity=<poster>` with `{ "scriptHex": … }`.
pub async fn record_post(mut req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    let identity = match crate::routes::view_identity(&req, &ctx) {
        crate::routes::ViewIdentity::Identity(id) => id.to_ascii_lowercase(),
        crate::routes::ViewIdentity::Refuse(resp) => return resp,
    };
    let anonymous = matches!(ctx.data.caller, CallerAuth::Anonymous);
    let url = match req.url() {
        Ok(u) => u,
        Err(_) => return crate::routes::json_error("unreadable request url", 400),
    };
    let kind = url
        .query_pairs()
        .find(|(k, _)| k == "kind")
        .and_then(|(_, v)| RecordKind::parse(&v));
    let Some(kind) = kind else {
        return refuse(None, RecordRefusal::BadKind);
    };
    let raw: Vec<u8> = match ctx.data.body.clone() {
        Some(b) => b,
        None => match req.bytes().await {
            Ok(b) => b,
            Err(e) => return crate::routes::json_error(&format!("body unreadable: {e}"), 400),
        },
    };
    // Sizes BEFORE any decode (MED-3): the body, the hex, then the script.
    if raw.len() > RECORD_POST_MAX_BODY_BYTES {
        return refuse(Some(kind), RecordRefusal::BodyTooLarge);
    }
    let body: RecordPostBody = match serde_json::from_slice(&raw) {
        Ok(b) => b,
        Err(e) => {
            bump(&REFUSED_BY_KIND, kind);
            return crate::routes::json_error(&format!("body is not a record post: {e}"), 400);
        }
    };
    let script_hex = body.script_hex.trim();
    if script_hex.len() > RECORD_POST_MAX_SCRIPT_BYTES * 2 {
        return refuse(Some(kind), RecordRefusal::ScriptTooLarge);
    }
    let script = match hex::decode(script_hex) {
        Ok(s) => s,
        Err(_) => return refuse(Some(kind), RecordRefusal::BadScriptHex),
    };
    let db = ctx.env.d1("OVERLAY_DB")?;
    let mut verified = match verify_record_post(kind, &script, &identity) {
        Ok(v) => v,
        Err(r) => return refuse(Some(kind), r),
    };
    // A refund backup's pot, as the index holds it — read only AFTER the
    // marker's own signature verified (no free read for a stranger): the
    // bind is rank 1 (the pre-signed spend), rank 0 (the pot not indexed
    // yet), or a refusal.
    if let VerifiedRecord::Potrefund(r, refund_valid) = &mut verified {
        let pot = read_pot_context(&db, &r.pot_txid, r.pot_vout).await?;
        *refund_valid = match bind_refund_backup(r, pot.as_ref()) {
            Ok(v) => v,
            Err(refusal) => return refuse(Some(kind), refusal),
        };
    }
    // The overlay stamps every one of these tables in unix SECONDS.
    let now = (worker::Date::now().as_millis() / 1000) as i64;
    // The chain no-op (step 6): the chain already holds this marker for this
    // identity — nothing to add, nothing written, nothing counted.
    let (chain_sql, chain_binds) = chain_rows_query(&verified);
    let chain_rows = count(
        &db,
        chain_sql,
        &chain_binds.iter().map(|s| js(s)).collect::<Vec<_>>(),
    )
    .await?;
    if chain_rows > 0 {
        bump(&ALREADY_INDEXED_BY_KIND, kind);
        return crate::routes::json_response(
            serde_json::json!({
                "filed": true,
                "kind": kind.as_str(),
                "key": verified.key(),
                "alreadyIndexed": true,
            })
            .to_string(),
            200,
        );
    }
    // The caps (step 6): the poster's rows for this (family, game, pot) —
    // this content excluded — and its day.
    let (rows_sql, rows_binds, day_sql, day_identity) = cap_queries(&verified);
    let rows_for_pot = count(
        &db,
        rows_sql,
        &rows_binds.iter().map(|s| js(s)).collect::<Vec<_>>(),
    )
    .await?;
    let rows_today = count(
        &db,
        day_sql,
        &[js(&day_identity), js_num(now - RECORD_DAY_SECS)],
    )
    .await?;
    if let Some(r) = cap_refusal(kind, rows_for_pot, rows_today) {
        bump(
            if r == RecordRefusal::TooManyFiled { &TOO_MANY_FILED_BY_KIND } else { &DAILY_CAP_BY_KIND },
            kind,
        );
        return refuse(Some(kind), r);
    }
    match &verified {
        VerifiedRecord::Potparty(r, sig_valid) => {
            db.prepare(POTPARTY_FILE_SQL)
                .bind(&[
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
                    js_num(i64::from(*sig_valid)),
                ])?
                .run()
                .await?;
        }
        VerifiedRecord::Potrefund(r, refund_valid) => {
            db.prepare(POTREFUND_FILE_SQL)
                .bind(&[
                    js(&r.identity),
                    js(&r.game_id),
                    js(&r.pot_txid),
                    js_num(i64::from(r.pot_vout)),
                    js(&r.refund_raw_hex),
                    js(&r.sig_hex),
                    js(&r.txid),
                    js_num(0),
                    js_num(now),
                    js_num(i64::from(*refund_valid)),
                ])?
                .run()
                .await?;
            if *refund_valid {
                // The late latch: a row filed at rank 0 earlier (the same
                // content — the INSERT above was ignored) takes its verdict.
                let latched = db
                    .prepare(POTREFUND_LATCH_SQL)
                    .bind(&[js(&r.txid)])?
                    .run()
                    .await?;
                if latched.meta().ok().flatten().and_then(|m| m.changes).unwrap_or(0) > 0 {
                    REFUNDS_LATCHED_LATER.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                REFUNDS_FILED_UNBOUND.fetch_add(1, Ordering::Relaxed);
            }
        }
        VerifiedRecord::Result(r, tier) => {
            db.prepare(RESULT_FILE_SQL)
                .bind(&[
                    js(&r.game_id),
                    js(&r.winner),
                    js(&r.loser),
                    js(&r.pot_txid),
                    js(&r.settle_txid),
                    js(&r.winner_sig_hex),
                    js_opt(r.loser_sig_hex.as_deref()),
                    js_opt(r.cards_hex.as_deref()),
                    js(&r.txid),
                    js_num(0),
                    js_num(now),
                    js_num(*tier),
                ])?
                .run()
                .await?;
            let pot = r.pot_txid.as_str();
            let per_pot = overlay_discovery::result::storage::RESULT_ROWS_PER_POT as i64;
            db.prepare(lb_row_file_sql())
                .bind(&[
                    js(&r.txid),
                    js_num(0),
                    js(&r.game_id),
                    js(&r.winner),
                    js(&r.loser),
                    js(pot),
                    js(&r.settle_txid),
                    js(&r.winner_sig_hex),
                    js_opt(r.loser_sig_hex.as_deref()),
                    js_opt(r.cards_hex.as_deref()),
                    js_num(now),
                    js_num(*tier),
                    js(pot),
                    js(pot),
                    js(pot),
                    js_num(now),
                    js(pot),
                    js(pot),
                    js_num(now),
                    js(pot),
                    js(pot),
                    js_num(per_pot),
                ])?
                .run()
                .await?;
        }
        VerifiedRecord::Collected(r) => {
            db.prepare(COLLECTED_FILE_SQL)
                .bind(&[
                    js(&r.identity),
                    js(&r.game_id),
                    js(&r.txid),
                    js_num(0),
                    js_opt(r.sig_hex.as_deref()),
                    js_num(now),
                ])?
                .run()
                .await?;
        }
    }
    bump(&FILED_BY_KIND, kind);
    if anonymous {
        bump(&ANONYMOUS_FILED_BY_KIND, kind);
    }
    let mut body = serde_json::json!({ "filed": true, "kind": kind.as_str(), "key": verified.key() });
    if let VerifiedRecord::Potrefund(_, refund_valid) = &verified {
        body["refundValid"] = serde_json::json!(i64::from(*refund_valid));
    }
    crate::routes::json_response(body.to_string(), 200)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use bsv_rs::primitives::bsv::sighash::{
        build_sighash_preimage, compute_sighash_for_signing, parse_transaction, SighashParams,
        SIGHASH_ALL, SIGHASH_FORKID,
    };
    use bsv_rs::primitives::PrivateKey;
    use bsv_rs::wallet::{Counterparty, CreateSignatureArgs, GetPublicKeyArgs, ProtoWallet};

    fn wallet(seed: u8) -> ProtoWallet {
        ProtoWallet::new(Some(
            PrivateKey::from_hex(&hex::encode([seed; 32])).unwrap(),
        ))
    }
    fn identity(w: &ProtoWallet) -> Vec<u8> {
        hex::decode(
            w.get_public_key(GetPublicKeyArgs {
                identity_key: true,
                protocol_id: None,
                key_id: None,
                counterparty: None,
                for_self: None,
            })
            .unwrap()
            .public_key,
        )
        .unwrap()
    }
    fn sign(
        w: &ProtoWallet,
        protocol: bsv_rs::wallet::Protocol,
        key_id: &str,
        data: &[u8],
    ) -> Vec<u8> {
        w.create_signature(CreateSignatureArgs {
            data: Some(data.to_vec()),
            hash_to_directly_sign: None,
            protocol_id: protocol,
            key_id: key_id.to_string(),
            counterparty: Some(Counterparty::Anyone),
        })
        .unwrap()
        .signature
    }
    /// The client's `pushData` rule — the canonical encoding.
    fn push(out: &mut Vec<u8>, data: &[u8]) {
        let n = data.len();
        if n < 0x4c {
            out.push(n as u8);
        } else if n <= 0xff {
            out.push(0x4c);
            out.push(n as u8);
        } else {
            out.push(0x4d);
            out.extend_from_slice(&(n as u16).to_le_bytes());
        }
        out.extend_from_slice(data);
    }
    fn script(pushes: &[&[u8]]) -> Vec<u8> {
        let mut out = vec![0x00, 0x6a];
        for p in pushes {
            push(&mut out, p);
        }
        out
    }
    /// A one-input tx spending `pot:vout` — the OLD test's junk shape (input 0
    /// names the pot and nothing else is true of it).
    fn junk_refund_raw(pot_txid: &[u8; 32], vout: u32) -> Vec<u8> {
        let mut tx = bsv_rs::transaction::Transaction::new();
        tx.add_input(bsv_rs::transaction::TransactionInput {
            source_txid: Some(hex::encode(pot_txid)),
            source_output_index: vout,
            unlocking_script: Some(bsv_rs::script::UnlockingScript::from_hex("00").unwrap()),
            sequence: 0,
            ..Default::default()
        })
        .unwrap();
        tx.add_output(bsv_rs::transaction::TransactionOutput {
            satoshis: Some(1_000),
            locking_script: bsv_rs::script::LockingScript::from_hex("51").unwrap(),
            change: false,
        })
        .unwrap();
        tx.to_binary()
    }

    // ── a real covenant pot and its pre-signed refund (the HIGH-1 fixtures) ──
    // A symmetric pot txid ([0x55; 32]) so the raw's internal byte order and
    // the marker's display order agree without a reversal in the fixture.
    const POT_TXID: [u8; 32] = [0x55u8; 32];
    const POT_SATS: u64 = 1000;
    fn skey(scalar: u8) -> bsv_rs::primitives::ec::PrivateKey {
        let mut b = [0u8; 32];
        b[31] = scalar;
        bsv_rs::primitives::ec::PrivateKey::from_bytes(&b).expect("nonzero scalar")
    }
    fn pot_keys() -> [bsv_rs::primitives::ec::PrivateKey; 3] {
        [skey(1), skey(2), skey(3)]
    }
    fn pot_params() -> CovenantParams {
        let keys = pot_keys();
        CovenantParams {
            pub_a: keys[0].public_key().to_compressed(),
            pub_b: keys[1].public_key().to_compressed(),
            pub_tower: keys[2].public_key().to_compressed(),
            pay_pkh_a: [0x11; 20],
            pay_pkh_b: [0x22; 20],
            rake_pkh: [0x33; 20],
            stake_a: 500,
            stake_b: 500,
            fee_sats: 6,
            recovery_height: 900_000,
        }
    }
    fn pot_ctx() -> PotContext {
        PotContext {
            params: pot_params(),
            pot_sats: POT_SATS,
        }
    }
    /// A raw spend of `prev:0` with the given unlock, sequence and locktime.
    fn spend_raw(prev: &[u8; 32], unlock: &[u8], sequence: u32, lock_time: u32) -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend_from_slice(&1i32.to_le_bytes());
        raw.push(1);
        raw.extend_from_slice(prev);
        raw.extend_from_slice(&0u32.to_le_bytes());
        // The script length as a varint: the template unlock (two signatures
        // + the BIP-143 preimage) is past the 1-byte form.
        if unlock.len() < 0xfd {
            raw.push(unlock.len() as u8);
        } else {
            assert!(unlock.len() <= 0xffff);
            raw.push(0xfd);
            raw.extend_from_slice(&(unlock.len() as u16).to_le_bytes());
        }
        raw.extend_from_slice(unlock);
        raw.extend_from_slice(&sequence.to_le_bytes());
        raw.push(1);
        raw.extend_from_slice(&994u64.to_le_bytes());
        raw.push(3);
        raw.extend_from_slice(&[0x76, 0xa9, 0x88]);
        raw.extend_from_slice(&lock_time.to_le_bytes());
        raw
    }
    /// The digest and the BIP-143 preimage of input 0 of `skeleton` over the
    /// pot's rebuilt covenant lock — what the seats sign and what the
    /// template's third push carries.
    fn digest_and_preimage(skeleton: &[u8]) -> ([u8; 32], Vec<u8>) {
        let parsed = parse_transaction(skeleton).expect("parses");
        let lock = overlay_discovery::pot::covenant_lock_of(&pot_params());
        let params = SighashParams {
            version: parsed.version,
            inputs: &parsed.inputs,
            outputs: &parsed.outputs,
            locktime: parsed.locktime,
            input_index: 0,
            subscript: &lock,
            satoshis: POT_SATS,
            scope: SIGHASH_ALL | SIGHASH_FORKID,
        };
        (compute_sighash_for_signing(&params), build_sighash_preimage(&params))
    }
    fn wire_sig(sk: &bsv_rs::primitives::ec::PrivateKey, digest: &[u8; 32]) -> Vec<u8> {
        let mut push = sk.sign(digest).expect("signs").to_der();
        push.push((SIGHASH_ALL | SIGHASH_FORKID) as u8);
        push
    }
    fn push_of(out: &mut Vec<u8>, data: &[u8]) {
        if data.len() < 0x4c {
            out.push(data.len() as u8);
        } else if data.len() <= 0xff {
            out.push(0x4c);
            out.push(data.len() as u8);
        } else {
            out.push(0x4d);
            out.extend_from_slice(&(data.len() as u16).to_le_bytes());
        }
        out.extend_from_slice(data);
    }
    /// The pot's spend in the covenant TEMPLATE's unlock shape
    /// (`low_spend::template_pot_unlock`: `[sig1 ‖ 0x41, sig2 ‖ 0x41,
    /// preimage]`), signed by `first` and `second`, with the given sequence
    /// and locktime — the pre-signed refund when (A, B, non-final, ≥ the
    /// recovery height). `extra` appends pushes after the preimage (a
    /// malleated variant the classifier tolerates and the shape bar must
    /// not).
    fn template_spend(
        first: &bsv_rs::primitives::ec::PrivateKey,
        second: &bsv_rs::primitives::ec::PrivateKey,
        sequence: u32,
        lock_time: u32,
        extra: &[&[u8]],
    ) -> Vec<u8> {
        let skeleton = spend_raw(&POT_TXID, &[], sequence, lock_time);
        let (digest, preimage) = digest_and_preimage(&skeleton);
        let mut unlock = Vec::new();
        push_of(&mut unlock, &wire_sig(first, &digest));
        push_of(&mut unlock, &wire_sig(second, &digest));
        push_of(&mut unlock, &preimage);
        for e in extra {
            push_of(&mut unlock, e);
        }
        spend_raw(&POT_TXID, &unlock, sequence, lock_time)
    }
    /// The BARE 2-of-3 shape (`[OP_0, sigA, sigB]`): both seats' valid
    /// signatures, not the covenant template's unlock.
    fn bare_spend(sequence: u32, lock_time: u32) -> Vec<u8> {
        let k = pot_keys();
        let skeleton = spend_raw(&POT_TXID, &[], sequence, lock_time);
        let (digest, _) = digest_and_preimage(&skeleton);
        let mut unlock = vec![0x00];
        push_of(&mut unlock, &wire_sig(&k[0], &digest));
        push_of(&mut unlock, &wire_sig(&k[1], &digest));
        spend_raw(&POT_TXID, &unlock, sequence, lock_time)
    }
    fn spend_signed_by(
        first: &bsv_rs::primitives::ec::PrivateKey,
        second: &bsv_rs::primitives::ec::PrivateKey,
        sequence: u32,
        lock_time: u32,
    ) -> Vec<u8> {
        template_spend(first, second, sequence, lock_time, &[])
    }
    fn presigned_refund() -> Vec<u8> {
        let k = pot_keys();
        spend_signed_by(&k[0], &k[1], 0xffff_fffe, 900_000)
    }
    /// The route's order: verify the marker, then bind its refund to the pot.
    fn verify_and_bind(
        script: &[u8],
        poster: &str,
        pot: Option<&PotContext>,
    ) -> std::result::Result<(PotrefundRecord, bool), RecordRefusal> {
        match verify_record_post(RecordKind::Potrefund, script, poster)? {
            VerifiedRecord::Potrefund(r, _) => {
                let v = bind_refund_backup(&r, pot)?;
                Ok((r, v))
            }
            _ => panic!("potrefund"),
        }
    }
    /// A signed potrefund marker for `raw` under wallet `w`.
    fn potrefund_script(w: &ProtoWallet, gid: &[u8; 32], raw: &[u8]) -> Vec<u8> {
        let id = identity(w);
        let sig = sign(
            w,
            potrefund_protocol(),
            &hex::encode(gid),
            &potrefund_challenge(&id, gid, &POT_TXID, 0, raw),
        );
        script(&[
            b"LOW/potrefund/v1",
            &id,
            gid,
            &POT_TXID,
            &0u32.to_le_bytes(),
            raw,
            &sig,
        ])
    }

    #[test]
    fn the_content_key_is_never_a_txid_and_ignores_the_signature_bytes() {
        let m = PotpartyMarker {
            identity: vec![0x02; 33],
            opponent: vec![0x03; 33],
            game_id: [0x11; 32],
            pot_txid: [0x22; 32],
            pot_vout: 0,
            recovery_height: 900_000,
            sig: vec![0xaa; 70],
            seat_settle_pubkey: None,
            seat_sig: None,
        };
        let k = potparty_content_key(&m);
        assert!(k.starts_with("filed:"));
        assert_eq!(k.len(), 62);
        assert_ne!(k.len(), 64);
        // The SAME marker under another signature (a fresh nonce) is the same row.
        let resigned = PotpartyMarker {
            sig: vec![0xbb; 71],
            ..m.clone()
        };
        assert_eq!(potparty_content_key(&resigned), k);
        // A v2 of the same marker is its own row (the reader wants both).
        let v2 = PotpartyMarker {
            seat_settle_pubkey: Some(vec![0x02; 33]),
            seat_sig: Some(vec![0xcc; 70]),
            ..m.clone()
        };
        assert_ne!(potparty_content_key(&v2), k);
        // Another game, another row.
        let other = PotpartyMarker {
            game_id: [0x12; 32],
            ..m.clone()
        };
        assert_ne!(potparty_content_key(&other), k);
        // A result's countersigned upgrade is a second content; the
        // signature bytes themselves are not.
        let r = ResultMarker {
            game_id: [0x11; 32],
            winner: vec![0x02; 33],
            loser: vec![0x03; 33],
            pot_txid: [0x22; 32],
            settle_txid: [0x33; 32],
            winner_sig: vec![0xaa; 70],
            loser_sig: None,
            cards: None,
        };
        let rk = result_content_key(&r);
        assert_eq!(
            result_content_key(&ResultMarker {
                winner_sig: vec![0xab; 70],
                ..r.clone()
            }),
            rk
        );
        assert_ne!(
            result_content_key(&ResultMarker {
                loser_sig: Some(vec![0xcd; 70]),
                ..r.clone()
            }),
            rk
        );
    }

    #[test]
    fn only_the_canonical_encoding_is_accepted_so_one_marker_has_one_key() {
        let w = wallet(3);
        let id = identity(&w);
        let id_lc = hex::encode(&id);
        let gid = [0x44u8; 32];
        let sig = sign(
            &w,
            collected_protocol(),
            &hex::encode(gid),
            &collected_challenge(&hex::encode(gid), &id_lc),
        );
        let canonical = script(&[b"LOW/collected/v1", &gid, &id, &sig]);
        let key = match verify_record_post(RecordKind::Collected, &canonical, &id_lc).unwrap()
        {
            VerifiedRecord::Collected(r) => r.txid,
            _ => panic!("collected"),
        };
        assert_eq!(
            key,
            collected_content_key(&parse_collected_marker(&canonical).unwrap())
        );
        // PUSHDATA1 spelling of a short field: the parser reads it, the
        // filing refuses it.
        let mut pd1 = vec![0x00, 0x6a, 0x4c, 0x10];
        pd1.extend_from_slice(b"LOW/collected/v1");
        push(&mut pd1, &gid);
        push(&mut pd1, &id);
        push(&mut pd1, &sig);
        assert!(parse_collected_marker(&pd1).is_some());
        assert_eq!(
            verify_record_post(RecordKind::Collected, &pd1, &id_lc).unwrap_err(),
            RecordRefusal::NotCanonical
        );
        // PUSHDATA2 spelling: the same.
        let mut pd2 = vec![0x00, 0x6a, 0x4d, 0x10, 0x00];
        pd2.extend_from_slice(b"LOW/collected/v1");
        push(&mut pd2, &gid);
        push(&mut pd2, &id);
        push(&mut pd2, &sig);
        assert!(parse_collected_marker(&pd2).is_some());
        assert_eq!(
            verify_record_post(RecordKind::Collected, &pd2, &id_lc).unwrap_err(),
            RecordRefusal::NotCanonical
        );
        // Bytes after the last field (an opcode, then junk): the parser
        // stops silently, the filing refuses.
        let mut trailing = canonical.clone();
        trailing.extend_from_slice(&[0x6a, 0xde, 0xad, 0xbe, 0xef]);
        assert!(parse_collected_marker(&trailing).is_some());
        assert_eq!(
            verify_record_post(RecordKind::Collected, &trailing, &id_lc).unwrap_err(),
            RecordRefusal::NotCanonical
        );
        // A padded DER (a redundant leading zero in r): not canonical, so
        // not a signature — the same marker cannot file under a second key.
        let mut padded = sig.clone();
        let rlen = padded[3] as usize;
        assert_eq!(padded[2], 0x02);
        if padded[4] < 0x80 {
            padded.insert(4, 0x00);
            padded[3] = (rlen + 1) as u8;
            padded[1] += 1;
        } else {
            // r already carries a leading zero: strip it to make the
            // non-canonical variant instead.
            padded.remove(4);
            padded[3] = (rlen - 1) as u8;
            padded[1] -= 1;
        }
        let padded_script = script(&[b"LOW/collected/v1", &gid, &id, &padded]);
        assert_eq!(
            verify_record_post(RecordKind::Collected, &padded_script, &id_lc).unwrap_err(),
            RecordRefusal::SignatureInvalid
        );
        assert_eq!(canonical_marker_pushes(&[0x6a, 0x00]), None);
        assert_eq!(canonical_marker_pushes(&[0x00, 0x6a]).unwrap().len(), 0);
        assert_eq!(
            canonical_marker_pushes(&[0x00, 0x6a, 0x00, 0x01, 0xff]).unwrap(),
            vec![&[][..], &[0xff][..]]
        );
    }

    #[test]
    fn the_two_challenges_are_the_clients_bytes() {
        let gid = [0x11u8; 32];
        let pot = [0x22u8; 32];
        let id = [0x02u8; 33];
        let c = potrefund_challenge(&id, &gid, &pot, 7, &[0xaa, 0xbb]);
        let mut want = b"LOW/potrefund/v1".to_vec();
        want.extend_from_slice(&id);
        want.extend_from_slice(&gid);
        want.extend_from_slice(&pot);
        want.extend_from_slice(&7u32.to_le_bytes());
        want.extend_from_slice(&[0xaa, 0xbb]);
        assert_eq!(c, want);
        assert_eq!(
            collected_challenge(&"ab".repeat(32), &"03".repeat(33)),
            format!(
                "LOW-collected\nv1\ngid={}\nid={}",
                "ab".repeat(32),
                "03".repeat(33)
            )
            .into_bytes()
        );
    }

    #[test]
    fn a_collected_marker_files_under_its_signer_and_refuses_a_stranger_or_a_forged_signature() {
        let w = wallet(1);
        let id = identity(&w);
        let id_lc = hex::encode(&id);
        let gid = [0x33u8; 32];
        let gid_lc = hex::encode(gid);
        let sig = sign(
            &w,
            collected_protocol(),
            &gid_lc,
            &collected_challenge(&gid_lc, &id_lc),
        );
        let s = script(&[b"LOW/collected/v1", &gid, &id, &sig]);
        let v = verify_record_post(RecordKind::Collected, &s, &id_lc).unwrap();
        match &v {
            VerifiedRecord::Collected(r) => {
                assert_eq!(r.identity, id_lc);
                assert_eq!(r.game_id, gid_lc);
                assert_eq!(
                    r.txid,
                    collected_content_key(&parse_collected_marker(&s).unwrap())
                );
                assert_eq!(r.output_index, 0);
            }
            _ => panic!("collected"),
        }
        // A stranger posting the signer's marker: refused (the poster must
        // be the identity).
        assert_eq!(
            verify_record_post(
                RecordKind::Collected,
                &s,
                &hex::encode(identity(&wallet(2))))
            .unwrap_err(),
            RecordRefusal::PosterMismatch
        );
        // A forged signature: refused.
        let mut bad = sig.clone();
        bad[10] ^= 0x01;
        let s2 = script(&[b"LOW/collected/v1", &gid, &id, &bad]);
        assert_eq!(
            verify_record_post(RecordKind::Collected, &s2, &id_lc).unwrap_err(),
            RecordRefusal::SignatureInvalid
        );
        // The wrong family for the bytes: not a marker of that kind.
        assert_eq!(
            verify_record_post(RecordKind::Potparty, &s, &id_lc).unwrap_err(),
            RecordRefusal::NotAMarker
        );
    }

    #[test]
    fn a_refund_backup_files_only_as_the_pots_presigned_spend_by_both_committed_seats() {
        let w = wallet(3);
        let id_lc = hex::encode(identity(&w));
        let gid = [0x44u8; 32];
        let ctx = pot_ctx();
        // The genuine pre-signed refund: the template's unlock shape, both
        // seats, height-gated, non-final → rank 1.
        let raw = presigned_refund();
        let s = potrefund_script(&w, &gid, &raw);
        let (r, refund_valid) = verify_and_bind(&s, &id_lc, Some(&ctx)).unwrap();
        assert!(refund_valid);
        assert_eq!(r.pot_txid, hex::encode(POT_TXID));
        assert_eq!(r.refund_raw_hex, hex::encode(&raw));
        assert_eq!(
            r.txid,
            potrefund_content_key(&parse_potrefund_marker(&s).unwrap())
        );
        // No pot in the index yet: FILED at rank 0, never a refusal (the
        // belt survives an unindexed pot; the client re-files until it
        // latches).
        let (r0, unbound) = verify_and_bind(&s, &id_lc, None).unwrap();
        assert!(!unbound);
        assert_eq!(r0.txid, r.txid);
        // The BARE 2-of-3 shape carries both seats' valid signatures — the
        // classifier says Coop — and is NOT the template's unlock: refused.
        let bare = bare_spend(0xffff_fffe, 900_000);
        assert_eq!(
            settle_signers_for_spend(&ctx.params, POT_SATS, &bare, 0),
            Some(SettleSigners::Coop)
        );
        assert_eq!(
            verify_and_bind(&potrefund_script(&w, &gid, &bare), &id_lc, Some(&ctx)).unwrap_err(),
            RecordRefusal::RefundNotThePresignedSpend
        );
        // A MALLEATED variant of the real refund (an extra push after the
        // preimage — the counterparty holds both signatures): the classifier
        // still says Coop; the shape bar refuses it, so it is never rank 1.
        let k = pot_keys();
        let malleated = template_spend(&k[0], &k[1], 0xffff_fffe, 900_000, &[&[0xde, 0xad]]);
        assert_eq!(
            settle_signers_for_spend(&ctx.params, POT_SATS, &malleated, 0),
            Some(SettleSigners::Coop)
        );
        assert_eq!(
            verify_and_bind(&potrefund_script(&w, &gid, &malleated), &id_lc, Some(&ctx))
                .unwrap_err(),
            RecordRefusal::RefundNotThePresignedSpend
        );
        // The seats' signatures in the WRONG order: refused (lock order).
        let swapped = template_spend(&k[1], &k[0], 0xffff_fffe, 900_000, &[]);
        assert_eq!(
            verify_and_bind(&potrefund_script(&w, &gid, &swapped), &id_lc, Some(&ctx)).unwrap_err(),
            RecordRefusal::RefundNotThePresignedSpend
        );
        // The OLD bar (input 0 names the pot) under a valid marker signature:
        // refused — a junk raw is not the pre-signed spend. Without the pot it
        // files at rank 0 (the junk never leads: rank 0 sorts after every
        // rank-1 row and the client's own validity gate judges the bytes).
        let junk = junk_refund_raw(&POT_TXID, 0);
        assert!(refund_spends_pot(&junk, &POT_TXID, 0));
        assert_eq!(
            verify_and_bind(&potrefund_script(&w, &gid, &junk), &id_lc, Some(&ctx))
            .unwrap_err(),
            RecordRefusal::RefundNotThePresignedSpend
        );
        assert!(!verify_and_bind(&potrefund_script(&w, &gid, &junk), &id_lc, None).unwrap().1);
        // A spend one seat signed with the TOWER (a parked sibling / an
        // enforced settle): not the seats' refund.
        for (a, b) in [(&k[0], &k[2]), (&k[1], &k[2])] {
            let r = spend_signed_by(a, b, 0xffff_fffe, 900_000);
            assert_eq!(
                verify_and_bind(&potrefund_script(&w, &gid, &r), &id_lc, Some(&ctx))
                .unwrap_err(),
                RecordRefusal::RefundNotThePresignedSpend
            );
        }
        // Both seats, but FINAL (a cooperative settle's shape): refused.
        let settle = spend_signed_by(&k[0], &k[1], 0xffff_ffff, 0);
        assert_eq!(
            verify_and_bind(&potrefund_script(&w, &gid, &settle), &id_lc, Some(&ctx))
            .unwrap_err(),
            RecordRefusal::RefundNotThePresignedSpend
        );
        // Both seats, non-final, but below the committed recovery height.
        let early = spend_signed_by(&k[0], &k[1], 0xffff_fffe, 899_999);
        assert_eq!(
            verify_and_bind(&potrefund_script(&w, &gid, &early), &id_lc, Some(&ctx))
            .unwrap_err(),
            RecordRefusal::RefundNotThePresignedSpend
        );
        // A stranger's keys over the same shape: refused.
        let stranger = spend_signed_by(&skey(7), &skey(8), 0xffff_fffe, 900_000);
        assert_eq!(
            verify_and_bind(&potrefund_script(&w, &gid, &stranger), &id_lc, Some(&ctx))
            .unwrap_err(),
            RecordRefusal::RefundNotThePresignedSpend
        );
        // A raw spending ANOTHER outpoint under a valid signature: refused
        // before the pot is even consulted.
        let other = junk_refund_raw(&[0x66u8; 32], 0);
        let id = identity(&w);
        let sig2 = sign(
            &w,
            potrefund_protocol(),
            &hex::encode(gid),
            &potrefund_challenge(&id, &gid, &POT_TXID, 0, &other),
        );
        let s2 = script(&[
            b"LOW/potrefund/v1",
            &id,
            &gid,
            &POT_TXID,
            &0u32.to_le_bytes(),
            &other,
            &sig2,
        ]);
        assert_eq!(
            verify_record_post(RecordKind::Potrefund, &s2, &id_lc).unwrap_err(),
            RecordRefusal::RefundDoesNotSpendThePot
        );
        // The signature over a different vout: refused.
        let sig = sign(
            &w,
            potrefund_protocol(),
            &hex::encode(gid),
            &potrefund_challenge(&id, &gid, &POT_TXID, 0, &raw),
        );
        let s3 = script(&[
            b"LOW/potrefund/v1",
            &id,
            &gid,
            &POT_TXID,
            &1u32.to_le_bytes(),
            &raw,
            &sig,
        ]);
        assert_eq!(
            verify_and_bind(&s3, &id_lc, Some(&ctx)).unwrap_err(),
            RecordRefusal::SignatureInvalid
        );
        // The pot context is the pure product of the decoded columns.
        assert!(PotRowD1 {
            lock_kind: Some("bare".into()),
            pub_a: None,
            pub_b: None,
            pub_tower: None,
            pay_pkh_a: None,
            pay_pkh_b: None,
            rake_pkh: None,
            stake_a: None,
            stake_b: None,
            fee_sats: None,
            cov_recovery_height: None,
            pot_sats: None,
        }
        .context()
        .is_none());
        let p = pot_params();
        let row = PotRowD1 {
            lock_kind: Some("covenant".into()),
            pub_a: Some(hex::encode(p.pub_a)),
            pub_b: Some(hex::encode(p.pub_b)),
            pub_tower: Some(hex::encode(p.pub_tower)),
            pay_pkh_a: Some(hex::encode(p.pay_pkh_a)),
            pay_pkh_b: Some(hex::encode(p.pay_pkh_b)),
            rake_pkh: Some(hex::encode(p.rake_pkh)),
            stake_a: Some(500.0),
            stake_b: Some(500.0),
            fee_sats: Some(6.0),
            cov_recovery_height: Some(900_000.0),
            pot_sats: Some(1000.0),
        };
        assert_eq!(row.context(), Some(ctx));
    }

    #[test]
    fn a_result_files_for_either_seat_with_the_winners_signature_and_carries_its_tier() {
        let winner = wallet(5);
        let loser = wallet(6);
        let (wid, lid) = (identity(&winner), identity(&loser));
        let (wid_lc, lid_lc) = (hex::encode(&wid), hex::encode(&lid));
        let gid = [0x77u8; 32];
        let pot = [0x88u8; 32];
        let settle = [0x99u8; 32];
        let gid_lc = hex::encode(gid);
        let challenge = overlay_discovery::result::validity::result_challenge_bytes(
            &gid_lc,
            &wid_lc,
            &lid_lc,
            &hex::encode(pot),
            &hex::encode(settle),
            None,
        )
        .unwrap();
        let proto = overlay_discovery::result::validity::result_protocol();
        let wsig = sign(&winner, proto.clone(), &gid_lc, &challenge);
        let s = script(&[
            b"LOW/result/v1",
            &gid,
            &wid,
            &lid,
            &pot,
            &settle,
            &wsig,
            &[],
        ]);
        // Either seat may post it.
        for poster in [&wid_lc, &lid_lc] {
            match verify_record_post(RecordKind::Result, &s, poster).unwrap() {
                VerifiedRecord::Result(r, tier) => {
                    assert_eq!(tier, 1, "the winner's claim alone is tier 1");
                    assert_eq!(r.winner, wid_lc);
                    assert!(r.loser_sig_hex.is_none());
                    assert_eq!(r.txid, result_content_key(&parse_result_marker(&s).unwrap()));
                }
                _ => panic!("result"),
            }
        }
        // Countersigned: tier 2, a second content (the upgrade).
        let lsig = sign(&loser, proto, &gid_lc, &challenge);
        let s2 = script(&[
            b"LOW/result/v1",
            &gid,
            &wid,
            &lid,
            &pot,
            &settle,
            &wsig,
            &lsig,
        ]);
        let key1 = result_content_key(&parse_result_marker(&s).unwrap());
        match verify_record_post(RecordKind::Result, &s2, &wid_lc).unwrap() {
            VerifiedRecord::Result(r, tier) => {
                assert_eq!(tier, 2);
                assert_ne!(r.txid, key1);
            }
            _ => panic!("result"),
        }
        // A countersignature that does not verify is NOT a tier-1 row here:
        // the key says countersigned, so the row must be.
        let mut badl = lsig.clone();
        badl[12] ^= 0x01;
        let s4 = script(&[b"LOW/result/v1", &gid, &wid, &lid, &pot, &settle, &wsig, &badl]);
        assert_eq!(
            verify_record_post(RecordKind::Result, &s4, &wid_lc).unwrap_err(),
            RecordRefusal::SignatureInvalid
        );
        // A third party is refused; a forged winner signature is refused.
        assert_eq!(
            verify_record_post(
                RecordKind::Result,
                &s,
                &hex::encode(identity(&wallet(9))))
            .unwrap_err(),
            RecordRefusal::PosterMismatch
        );
        let mut bad = wsig.clone();
        bad[9] ^= 0x01;
        let s3 = script(&[b"LOW/result/v1", &gid, &wid, &lid, &pot, &settle, &bad, &[]]);
        assert_eq!(
            verify_record_post(RecordKind::Result, &s3, &wid_lc).unwrap_err(),
            RecordRefusal::SignatureInvalid
        );
    }

    #[test]
    fn a_potparty_marker_files_only_with_a_valid_identity_signature() {
        let w = wallet(7);
        let id = identity(&w);
        let id_lc = hex::encode(&id);
        let opp = identity(&wallet(8));
        let gid = [0xaau8; 32];
        let pot = [0xbbu8; 32];
        let challenge = overlay_discovery::potparty::validity::potparty_v1_challenge(
            &id, &opp, &gid, &pot, 0, 900_000,
        )
        .unwrap();
        let sig = sign(
            &w,
            overlay_discovery::potparty::validity::potparty_protocol(),
            &hex::encode(gid),
            &challenge,
        );
        let s = script(&[
            b"LOW/potparty/v1",
            &id,
            &opp,
            &gid,
            &pot,
            &0u32.to_le_bytes(),
            &900_000u32.to_le_bytes(),
            &sig,
        ]);
        match verify_record_post(RecordKind::Potparty, &s, &id_lc).unwrap() {
            VerifiedRecord::Potparty(r, sig_valid) => {
                assert!(sig_valid);
                assert_eq!(r.identity, id_lc);
                assert_eq!(r.recovery_height, 900_000);
                assert_eq!(
                    r.txid,
                    potparty_content_key(&parse_potparty_marker(&s).unwrap())
                );
            }
            _ => panic!("potparty"),
        }
        let mut bad = sig.clone();
        bad[11] ^= 0x01;
        let s2 = script(&[
            b"LOW/potparty/v1",
            &id,
            &opp,
            &gid,
            &pot,
            &0u32.to_le_bytes(),
            &900_000u32.to_le_bytes(),
            &bad,
        ]);
        assert_eq!(
            verify_record_post(RecordKind::Potparty, &s2, &id_lc).unwrap_err(),
            RecordRefusal::SignatureInvalid
        );
        assert_eq!(
            verify_record_post(RecordKind::Potparty, &s, &hex::encode(&opp)).unwrap_err(),
            RecordRefusal::PosterMismatch
        );
        let big = vec![0u8; RECORD_POST_MAX_SCRIPT_BYTES + 1];
        assert_eq!(
            verify_record_post(RecordKind::Potparty, &big, &id_lc).unwrap_err(),
            RecordRefusal::ScriptTooLarge
        );
        assert_eq!(RecordKind::parse("Result"), Some(RecordKind::Result));
        assert_eq!(RecordKind::parse("hopparty"), None);
    }

    #[test]
    fn the_caps_are_the_honest_need_and_never_count_the_same_content() {
        assert_eq!(filed_rows_cap(RecordKind::Potparty), 2);
        assert_eq!(filed_rows_cap(RecordKind::Result), 2);
        assert_eq!(filed_rows_cap(RecordKind::Potrefund), 1);
        assert_eq!(filed_rows_cap(RecordKind::Collected), 1);
        assert_eq!(cap_refusal(RecordKind::Potrefund, 0, 0), None);
        assert_eq!(
            cap_refusal(RecordKind::Potrefund, 1, 0),
            Some(RecordRefusal::TooManyFiled)
        );
        assert_eq!(cap_refusal(RecordKind::Potparty, 1, 0), None);
        assert_eq!(
            cap_refusal(RecordKind::Potparty, 2, 0),
            Some(RecordRefusal::TooManyFiled)
        );
        assert_eq!(
            cap_refusal(RecordKind::Collected, 0, RECORD_FILINGS_PER_IDENTITY_PER_DAY),
            Some(RecordRefusal::DailyCapReached)
        );
        // The binds: the poster, the game, the pot and the content key being
        // filed (excluded from the count), then the poster for the day.
        let r = CollectedRecord {
            identity: "02aa".into(),
            game_id: "11".repeat(32),
            txid: "filed:x".into(),
            output_index: 0,
            sig_hex: None,
        };
        let (rows_sql, binds, day_sql, day_id) = cap_queries(&VerifiedRecord::Collected(r));
        assert_eq!(rows_sql, COLLECTED_FILED_ROWS_SQL);
        assert_eq!(binds, vec!["02aa".to_string(), "11".repeat(32), "filed:x".to_string()]);
        assert_eq!(day_sql, COLLECTED_FILED_TODAY_SQL);
        assert_eq!(day_id, "02aa");
        // The chain no-op binds: potparty carries its VERSION (v1 = 1), a
        // result whether it is the plain claim (1) or the countersigned one.
        let (csql, cbinds) = chain_rows_query(&VerifiedRecord::Potparty(
            PotpartyRecord {
                identity: "02aa".into(),
                opponent_identity: "02bb".into(),
                game_id: "11".repeat(32),
                pot_txid: "22".repeat(32),
                pot_vout: 0,
                recovery_height: 1,
                sig_hex: "30".into(),
                seat_settle_pubkey: Some("02cc".into()),
                seat_sig_hex: Some("30".into()),
                txid: "filed:y".into(),
                output_index: 0,
                created_at: 0,
            },
            true,
        ));
        assert_eq!(csql, POTPARTY_CHAIN_ROWS_SQL);
        assert_eq!(cbinds, vec!["02aa".to_string(), "11".repeat(32), "22".repeat(32), "0".to_string()]);
        for sql in [
            POTPARTY_CHAIN_ROWS_SQL,
            POTREFUND_CHAIN_ROWS_SQL,
            RESULT_CHAIN_ROWS_SQL,
            COLLECTED_CHAIN_ROWS_SQL,
        ] {
            assert!(sql.contains("txid NOT LIKE 'filed:%'"), "chain rows only: {sql}");
        }
        assert!(POTREFUND_LATCH_SQL.contains("COALESCE(refundValid, 0) = 0"));
        for sql in [
            POTPARTY_FILED_ROWS_SQL,
            POTREFUND_FILED_ROWS_SQL,
            RESULT_FILED_ROWS_SQL,
            COLLECTED_FILED_ROWS_SQL,
            POTPARTY_FILED_TODAY_SQL,
            POTREFUND_FILED_TODAY_SQL,
            RESULT_FILED_TODAY_SQL,
            COLLECTED_FILED_TODAY_SQL,
        ] {
            assert!(sql.contains("txid LIKE 'filed:%'"), "filed rows only: {sql}");
        }
        assert_eq!(RecordRefusal::TooManyFiled.status(), 409);
        assert_eq!(RecordRefusal::DailyCapReached.status(), 429);
        assert_eq!(RecordRefusal::RefundNotThePresignedSpend.status(), 422);
        assert_eq!(RecordRefusal::BodyTooLarge.status(), 413);
        assert_eq!(RecordRefusal::NotCanonical.status(), 400);
        let h = record_health_json();
        assert_eq!(h["filedRowsCap"]["potrefund"], 1);
        assert_eq!(h["filingsPerIdentityPerDay"], RECORD_FILINGS_PER_IDENTITY_PER_DAY);
        assert!(h["anonymousFiledByKind"]["result"].is_u64());
    }
}
