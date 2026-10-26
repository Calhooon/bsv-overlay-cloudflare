//! bsv-low M18-2 (B), the beta window's W-E (2026-09-10): `POST /record?kind=`
//! — every index marker LOW used to BUY on chain is FILED here instead.
//!
//! bsv-low #469 (2026-09-19, the owed list's decision 3): a FIFTH family that
//! was never bought, `hopsweep` — the seat's pre-signed sweep of its own
//! funding hop, filed so a stranded hop is claimable from any device. Its
//! grammar, its bar (the sweep's own input signature under the hop's lock, the
//! hop's identity-signed marker as the poster binding; no marker signature)
//! and its table live in `hopsweep.rs`; this door runs the same ladder for it.
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
//!      countersigned upgrade), and for `collected` the v1 marker and,
//!      COUNTED APART, [`HELD_FILINGS_PER_GAME`] held filings (bsv-low #492,
//!      below: one per payout transaction of the game; each shape counts its
//!      own rows only ([`filed_rows_cap_of`]), so held filings can never
//!      refuse the v1 marker nor the marker a held filing; five rows in all,
//!      under the 8 rows per pair the overlay's `ls_collected` serves)); a
//!      distinct content past the cap is `409`;
//!      and per `(poster, family)` a day's budget
//!      ([`RECORD_FILINGS_PER_IDENTITY_PER_DAY`], `429`). Both count FILED rows
//!      only (`txid LIKE 'filed:%'`), never chain admissions. And a marker the
//!      CHAIN already holds for this identity, game, pot and version, and
//!      that VERIFIES as the identity's own (bsv-low #564: potparty's
//!      `sigValid` 1, result's `claimValid` at its shape's tier, collected's
//!      signature re-checked here; a chain row is admitted by byte format
//!      only, so its presence alone is no-one's word), is a
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
pub(crate) mod owed_write;

use crate::d1_ledger::Counted;
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
    parse_potparty_marker, storage::PotpartyRecord, PotpartyMarker, POTPARTY_TAG, POTPARTY_TAG_V2,
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

/// The largest request BODY read at all: the script as hex plus 2 KiB for
/// the handfile's other three payloads, two signatures, hop txid and JSON frame.
/// Shared by the direct and handfile doors. Checked before the JSON parse, then on the hex
/// string before the decode, then on the script (the gate's MED-3: the old
/// order materialised ~60 MB of a 40 MB body before refusing it).
pub const RECORD_POST_MAX_BODY_BYTES: usize = RECORD_POST_MAX_SCRIPT_BYTES * 2 + 2_048;
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
    /// bsv-low #469: the seat's pre-signed hop sweep (`hopsweep.rs`); never bought on chain.
    Hopsweep,
    /// bsv-low #848 (FAST-C): the four funding-moment filings under one signature per key (`handfile.rs`).
    Handfile,
}

/// The families, as the counters are sized.
pub const RECORD_KINDS: usize = 6;

impl RecordKind {
    pub const ALL: [RecordKind; RECORD_KINDS] = [
        RecordKind::Potparty,
        RecordKind::Potrefund,
        RecordKind::Result,
        RecordKind::Collected,
        RecordKind::Hopsweep,
        RecordKind::Handfile,
    ];
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "potparty" => Some(Self::Potparty),
            "potrefund" => Some(Self::Potrefund),
            "result" => Some(Self::Result),
            "collected" => Some(Self::Collected),
            "hopsweep" => Some(Self::Hopsweep),
            "handfile" => Some(Self::Handfile),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Potparty => "potparty",
            Self::Potrefund => "potrefund",
            Self::Result => "result",
            Self::Collected => "collected",
            Self::Hopsweep => "hopsweep",
            Self::Handfile => "handfile",
        }
    }
    fn index(self) -> usize {
        match self {
            Self::Potparty => 0,
            Self::Potrefund => 1,
            Self::Result => 2,
            Self::Collected => 3,
            Self::Hopsweep => 4,
            Self::Handfile => 5,
        }
    }
}

/// FILED rows one poster may hold per `(family, game, pot)` — the honest
/// need, never more: `potparty` v1 + v2 share `potparty_records`; a `result`
/// is the winner's claim and, at most, its countersigned upgrade (a second
/// content); `potrefund` and `collected` are one row each; a `hopsweep` is the
/// funding device's pre-sign and, at most, a later re-sign (a pre-sign that
/// timed out, a fresh device's one-signature press — bsv-low #469). A re-file
/// of the SAME content is never counted against this (the key is excluded).
pub const fn filed_rows_cap(kind: RecordKind) -> i64 {
    match kind {
        RecordKind::Potparty | RecordKind::Result | RecordKind::Hopsweep => 2,
        // `collected` is the v1 marker's cap: one per game. The held filings of the same door share the table and
        // are counted APART ([`filed_rows_cap_of`], [`HELD_FILED_ROWS_SQL`]).
        RecordKind::Potrefund | RecordKind::Collected => 1,
        // bsv-low #848: a handfile is capped PER FAMILY it explodes into (potparty's 2, potrefund's 1, and its own
        // hopparty row's `HOPPARTY_FILED_ROWS_CAP`, 1 per hop); this is the hop row's.
        RecordKind::Handfile => crate::handfile::HOPPARTY_FILED_ROWS_CAP,
    }
}

/// bsv-low #492: held filings (`LOW/collected/v2`) one identity may hold per game: one per payout transaction the
/// game can pay it (the pot's settle or refund, a hop's sweep, a re-funded pot's). A game with more payout
/// transactions than this keeps the rows past the cap served (the pre-#492 state of those rows, never a wrong row).
pub const HELD_FILINGS_PER_GAME: i64 = 4;

/// The per-game cap of ONE verified record (the B3 lens fold L1): a held filing counts held rows only, up to
/// [`HELD_FILINGS_PER_GAME`]; every other record its family's [`filed_rows_cap`]. The v1 `collected` marker and
/// the held filings share a table and a door, never a count: five held rows cannot refuse the v1 marker, and the
/// marker takes no held slot.
pub const fn filed_rows_cap_of(v: &VerifiedRecord) -> i64 {
    match v {
        VerifiedRecord::Held(..) => HELD_FILINGS_PER_GAME,
        VerifiedRecord::Potparty(..) => filed_rows_cap(RecordKind::Potparty),
        VerifiedRecord::Potrefund(..) => filed_rows_cap(RecordKind::Potrefund),
        VerifiedRecord::Result(..) => filed_rows_cap(RecordKind::Result),
        VerifiedRecord::Collected(_) => filed_rows_cap(RecordKind::Collected),
        VerifiedRecord::Hopsweep(..) => filed_rows_cap(RecordKind::Hopsweep),
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
    /// bsv-low #469: the index holds no hop marker for this outpoint and poster yet (425: the client's outbox re-files).
    HopNotIndexed,
    /// bsv-low #469: the hop's marker does not verify under the poster yet (425: the latch, or the replay, decides later).
    HopMarkerUnverified,
    /// bsv-low #469: the sweep's first input does not spend the hop the marker names.
    SweepDoesNotSpendTheHop,
    /// bsv-low #469: the sweep is not the seat's spend of the hop (`hopsweep::bind_hop_sweep`).
    SweepNotTheSeatsSpend,
    /// bsv-low #848: the handfile body is not a 64-hex hop txid, four hex payloads and two hex signatures.
    HandfileShape,
    /// bsv-low #848: a handfile payload is not its family's canonical bytes (the family's own parser refuses it).
    HandfilePayloadNotAMarker,
    /// bsv-low #848: the four payloads do not name one seat's one hand (identity, opponent, game, pot, height,
    /// settle key).
    HandfileInconsistent,
    /// bsv-low #848: the handfile's settle-key signature does not verify under the batch's settle key.
    SettleSignatureInvalid,
    /// bsv-low #848: the hop the index holds does not pay the batch's settle key the sats the payload names.
    HopMarkerRefuted,
    /// bsv-low #663: the hop sweep names another game than the hop's own verified marker row.
    SweepGameMismatch,
}

impl From<crate::hopsweep::SweepRefusal> for RecordRefusal {
    fn from(r: crate::hopsweep::SweepRefusal) -> Self {
        use crate::hopsweep::SweepRefusal as S;
        match r {
            S::HopNotIndexed => Self::HopNotIndexed,
            S::HopMarkerUnverified => Self::HopMarkerUnverified,
            S::SweepDoesNotSpendTheHop => Self::SweepDoesNotSpendTheHop,
            S::SweepNotTheSeatsSpend => Self::SweepNotTheSeatsSpend,
            S::SweepGameMismatch => Self::SweepGameMismatch,
        }
    }
}

impl RecordRefusal {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BadKind => {
                "kind must be one of potparty | potrefund | result | collected | hopsweep | handfile"
            }
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
            Self::HopNotIndexed => "the index holds no hop marker for this outpoint and identity yet (file again once the hop is admitted)",
            Self::HopMarkerUnverified => "the hop's marker does not verify under this identity yet",
            Self::SweepDoesNotSpendTheHop => "the hop sweep's first input does not spend the hop it names",
            Self::SweepNotTheSeatsSpend => {
                "the hop sweep is not the seat's spend of the hop (the unlock must be the seat's canonical signature over the hop's own lock and value, then the seat's settle key)"
            }
            Self::SweepGameMismatch => {
                "the hop sweep names another game than the hop's own marker (the filing's gameId must be the hop's)"
            }
            Self::HandfileShape => {
                "a handfile is {hopTxid: 64 hex, payloads: [4 hex], identitySigHex, settleSigHex}"
            }
            Self::HandfilePayloadNotAMarker => {
                "a handfile payload is not its family's canonical bytes (hopparty, potparty v1, potparty v2, potrefund, in that order)"
            }
            Self::HandfileInconsistent => {
                "the handfile's payloads must name one seat's one hand: one identity, opponent, game, pot outpoint, recovery height and settle key"
            }
            Self::SettleSignatureInvalid => {
                "the handfile's settle-key signature does not verify under the batch's settle key (canonical low-S DER required)"
            }
            Self::HopMarkerRefuted => {
                "the hop the index holds does not pay the batch's settle key the sats the payload names"
            }
        }
    }
    pub fn status(self) -> u16 {
        match self {
            Self::BadKind
            | Self::BadScriptHex
            | Self::NotCanonical
            | Self::NotAMarker
            | Self::HandfileShape
            | Self::HandfilePayloadNotAMarker => 400,
            Self::BodyTooLarge | Self::ScriptTooLarge => 413,
            Self::PosterMismatch => 403,
            Self::TooManyFiled => 409,
            Self::SignatureInvalid
            | Self::RefundDoesNotSpendThePot
            | Self::RefundNotThePresignedSpend
            | Self::SweepDoesNotSpendTheHop
            | Self::SweepNotTheSeatsSpend
            | Self::SweepGameMismatch
            | Self::HandfileInconsistent
            | Self::SettleSignatureInvalid
            | Self::HopMarkerRefuted => 422,
            Self::HopNotIndexed | Self::HopMarkerUnverified => 425,
            Self::DailyCapReached => 429,
        }
    }
    /// bsv-low #628: the refusal's class in its durable name, `applayer_record_refused_<class>_total`.
    pub fn class(self) -> &'static str {
        match self {
            Self::BadKind => "bad_kind",
            Self::BadScriptHex => "bad_script_hex",
            Self::BodyTooLarge => "body_too_large",
            Self::ScriptTooLarge => "script_too_large",
            Self::NotCanonical => "not_canonical",
            Self::NotAMarker => "not_a_marker",
            Self::PosterMismatch => "poster_mismatch",
            Self::SignatureInvalid => "signature_invalid",
            Self::RefundDoesNotSpendThePot => "refund_does_not_spend_the_pot",
            Self::RefundNotThePresignedSpend => "refund_not_the_presigned_spend",
            Self::TooManyFiled => "too_many_filed",
            Self::DailyCapReached => "daily_cap_reached",
            Self::HopNotIndexed => "hop_not_indexed",
            Self::HopMarkerUnverified => "hop_marker_unverified",
            Self::SweepDoesNotSpendTheHop => "sweep_does_not_spend_the_hop",
            Self::SweepNotTheSeatsSpend => "sweep_not_the_seats_spend",
            Self::SweepGameMismatch => "sweep_game_mismatch",
            Self::HandfileShape => "handfile_shape",
            Self::HandfilePayloadNotAMarker => "handfile_payload_not_a_marker",
            Self::HandfileInconsistent => "handfile_inconsistent",
            Self::SettleSignatureInvalid => "settle_signature_invalid",
            Self::HopMarkerRefuted => "hop_marker_refuted",
        }
    }
    /// Every refusal, in declaration order (the durable names' list).
    pub const ALL: [RecordRefusal; 22] = [
        Self::BadKind,
        Self::BadScriptHex,
        Self::BodyTooLarge,
        Self::ScriptTooLarge,
        Self::NotCanonical,
        Self::NotAMarker,
        Self::PosterMismatch,
        Self::SignatureInvalid,
        Self::RefundDoesNotSpendThePot,
        Self::RefundNotThePresignedSpend,
        Self::TooManyFiled,
        Self::DailyCapReached,
        Self::HopNotIndexed,
        Self::HopMarkerUnverified,
        Self::SweepDoesNotSpendTheHop,
        Self::SweepNotTheSeatsSpend,
        Self::HandfileShape,
        Self::HandfilePayloadNotAMarker,
        Self::HandfileInconsistent,
        Self::SettleSignatureInvalid,
        Self::HopMarkerRefuted,
        Self::SweepGameMismatch,
    ];
}

/// bsv-low #628: the class of a body that is not a record post (the door's one refusal outside [`RecordRefusal`]).
pub const REFUSED_BAD_BODY_CLASS: &str = "bad_body";

/// bsv-low #628 (the observability bar, issue 4): mirror one door refusal durably, its class beside the total,
/// on the overlay's `ops_counters` through the courier census's batched flush. `refusedByKind` on `/health.record`
/// stays the isolate sample.
pub fn mirror_refused(class: &str) {
    crate::courier::note_presence("applayer_record_refused_total");
    crate::courier::note_presence(&format!("applayer_record_refused_{class}_total"));
}

/// Every record name this worker mirrors (the overlay seeds each at 0).
pub fn record_mirrored_counter_names() -> Vec<String> {
    let mut names = vec!["applayer_record_refused_total".to_string()];
    names.extend(
        RecordRefusal::ALL
            .iter()
            .map(|r| r.class())
            .chain([REFUSED_BAD_BODY_CLASS])
            .map(|c| format!("applayer_record_refused_{c}_total")),
    );
    names
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
pub(crate) fn content_key(tag: &[u8], fields: &[&[u8]]) -> String {
    let mut pre =
        Vec::with_capacity(16 + tag.len() + fields.iter().map(|f| f.len() + 4).sum::<usize>());
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
    let mut fields: Vec<&[u8]> = vec![&m.game_id, &m.winner, &m.loser, &m.pot_txid, &m.settle_txid];
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

// ── bsv-low #492 / #512: the HELD filing (`LOW/collected/v2`) ───────────────
//
// The v1 marker names (identity, game): "I collected this game's credit". It cannot say WHICH payout when a game
// pays the identity twice (the pot's payout and a swept hop's, a re-funded game's two pots: the owed list's N10),
// so there a filed marker retired nothing and the rows were served claimable for good. The held filing names the
// PAYING TRANSACTION: "my wallet holds the payout of game G that transaction T pays". It is filed by the device
// whose wallet ledger already holds T (the live credit of a hand whose `collected` filing a closing page dropped,
// a press the wallet answered as already known), with no credit and no press, and the owed list retires exactly
// the payout row T pays (`owed::OwedInputs::held_verified`).
//
// FILED ONLY: the chain's topic manager admits the v1 tag alone (`parse_collected_marker`), so every stored row of
// this shape passed this door's signature bar. The table is the v1 marker's plus ONE column, `payTxid` (overlay
// migration 163; the B3 lens fold M1): this door, the only writer of the column, writes T there in the same INSERT
// as the row, after the signature over (game, identity, T) verified. The owed walk retires by matching the column
// (`owed::CollectedFold`), and re-derives the row's own content key from (game, identity, T) first, so a row can
// only ever retire the transaction its key was filed for; no stored signature is checked again at the read.
//
// ── THE CLIENT CONTRACT (the device's half; bsv-low #492 / #512, the B3 lens fold M2) ──
//
// 1. THE HOLD RULE. A held filing is IRREVERSIBLE: rows are never deleted and there is no un-file door, so a
//    filed row is never offered to a press again. File it ONLY when the wallet OWNS THE PAYING OUTPUT: the
//    outpoint `facts.payTxid`:`facts.payVout` of the served row (spent or unspent), by the wallet's own OUTPUT
//    record, or by an internalize of that output that answered (a fresh credit or a merge). NEVER because the
//    wallet merely KNOWS the txid: a wallet lists a transaction it built or co-signed (a sweep its own client
//    composed, a settle it signed) without holding the home output, and a filing on that word cuts the player's
//    own drift net for good. `payVout` is the output to the identity's committed home in `payTxid` (`payPkh`);
//    it is `null` when the server could not name exactly one this pass: then the device finds its home outputs
//    in the credit BEEF itself (as the press does) and must own EVERY one, or it does not file. The signed
//    fields name the TRANSACTION; the ownership check is the device's duty and nothing here can make it.
//    SPENT STILL COUNTS (the B3 delta lens D-L4): a home output the wallet's own next hop already spent is
//    owned all the same, by the wallet's record of that OUTPUT, spent or unspent. A wallet that lists
//    spendable outputs only and cannot answer for the outpoint itself: the device does not file; it never
//    falls back to the txid.
//    NO SINGLE HOME: `payPkh: null` (no proven seat or no decoded keys on a pot, a hop whose game has no
//    known home and whose filed sweep does not carry exactly one P2PKH output) comes with `payVout: null` on
//    every pass, and nothing is filed from the row alone: the device finds its home outputs in the credit BEEF
//    and must own every one, as above. A hop whose game has no known home (no pot row: its JOIN never
//    assembled) and whose filed sweep, hashing to `payTxid`, carries EXACTLY ONE P2PKH output is served that
//    output: `payVout` its index and `payPkh` its own pkh, read from the sweep, never a committed home (bsv-low
//    L25). The hold rule above is unchanged: the device files only when its wallet owns that outpoint.
//    `payPkhSource` says which: `"committed-home"` or `"filed-sweep"` (absent with `payPkh: null`).
// 2. THE DOOR. `POST /record?kind=collected&identity=<identity hex>`, body `{ "scriptHex": "<the script>" }`.
//    The script: `OP_FALSE OP_RETURN`, then five minimal pushes: `LOW/collected/v2`, gameId (32), identityKey
//    (33), payTxid (32, the byte order of the txid's hex as `facts.payTxid` prints it), sig. The signature: over
//    `LOW-collected\nv2\ngid=<gid>\nid=<identity>\ntxid=<payTxid>` (lowercase hex), protocol
//    `[1,'low collected']`, keyID = gameId, counterparty anyone, low-S DER. The answers:
//      200 `{ filed: true, kind, key, heldTxid }`: written; the row retires on the next read. Memo it, done.
//      200 `{ filed: false, alreadyFiled: true, kind, key, heldTxid }`: this content was already on file;
//          nothing was written, staled or pushed. Memo it, done.
//      400 (the script is not this marker, not canonical, not hex), 403 (the `identity` is not the marker's),
//          413 (too large), 422 (the signature does not verify under the identity): a client bug; drop the
//          filing, never retry it unchanged. The body's `error` says which.
//      409: four held filings already stand for this (identity, game); stop for this game, for good. It
//          never binds an honest player: a game id is ONE hand (a rematch mints a new id, the client's
//          `sha256("<gameId>:rematch")`), and one hand pays an identity by one to three transactions (the
//          pot's spend, a hop's sweep, a re-funded pot).
//      429: the identity's day (200 `collected` rows in 24 h, v1 and held together) is spent; stop every
//          `collected` filing until tomorrow. A re-file of content ALREADY on file answers 429 too, not
//          `alreadyFiled`, once the day is spent (the caps run before the insert): the memo the first 200
//          wrote stands and nothing is queued again, and a 429 never WRITES that memo (it does not say the
//          content is on file).
//      5xx, a timeout, no answer: not an answer; the outbox keeps it and re-files.
//    BEFORE THE DOOR, the auth seam's own answers (the B3 delta lens D-N3), none of them this door's word on
//    the filing:
//      401 `ERR_SESSION_REFUSED` on a LANED call (the session lane refused the call's session or MAC): the
//          lane is a fast path; the same call goes again by the reference BRC-104 path and only that
//          answer is judged.
//      401 with no session under `AUTH_ENFORCE`: authenticate (the BRC-103/104 handshake), then file.
//      403 naming `authenticatedIdentity` and `queryIdentity` (`routes::view_identity`): the session is not
//          the `identity` of the query; a client bug, drop the filing. 400 for a body that is not JSON: same.
// 3. ONCE. One filing per (game, payTxid), remembered in a device memo keyed by both, never one per read of
//    `/owed`. A row still served after its filing answered 200 is NOT a reason to file again (the read may
//    simply be older than the write; a replaced spend has another `payTxid`, which is another filing). A
//    re-file repairs nothing (the B3 delta lens D-L3): if the write's stale mark was missed, the list heals
//    at the read's age rule (15 minutes, `owed::OWED_RECOMPUTE_ANY_AFTER_MS`), and a re-file answers `alreadyFiled` and
//    marks nothing. A device that sees a row it already filed for WAITS for the age rule, never re-files.
// 4. ONE SHAPE. After a FRESH credit (the wallet's internalize answered and the balance rose) file the held
//    filing ALONE: it is exact, and it is one row of the day budget. The v1 marker is for nothing new.
// 5. NOTHING TO FILE for a row with `payTxid: null` or `claimable: false`: the wallet cannot hold what has not
//    landed, and a row that names no transaction names nothing to sign.
// 6. THE OUTCOME LINE SURVIVES THE ROW. A press that learned "already in your wallet" keeps saying so after
//    its filing retired the row (bsv-low #515's retired block); the row's absence is not an error to render.
// 7. THE OUTBOX. The filing rides the record outbox (bsv-low #492 remedy 1), so a closing page does not drop
//    it; the load-time quiet filing is the repair for one that was dropped before the outbox existed.
// 8. NO COUNT PER WINDOW. The server checks no stored signature at the read, so every filed row retires its
//    payout on every recompute, however many an identity holds (the 256-per-recompute cut is gone). The only
//    limits are the two at this door: four per game, 200 a day.

/// The held filing's tag. Five pushes: tag, gameId (32), identityKey (33), payTxid (32, the byte order of the
/// txid's hex as every LOW surface prints it, like a marker's `potTxid`), sig (canonical DER).
pub const HELD_TAG: &[u8] = b"LOW/collected/v2";

/// A decoded held filing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldMarker {
    pub game_id: [u8; 32],
    pub identity_key: Vec<u8>,
    pub pay_txid: [u8; 32],
    pub sig: Vec<u8>,
}

/// Parse a held filing: the canonical encoding only (this family has no chain reader), exact tag, exact lengths.
pub fn parse_held_marker(script: &[u8]) -> Option<HeldMarker> {
    use overlay_discovery::collected::{
        COLLECTED_GAME_ID_LEN, COLLECTED_IDENTITY_KEY_LEN, COLLECTED_SIG_MAX_LEN,
        COLLECTED_SIG_MIN_LEN,
    };
    let pushes = canonical_marker_pushes(script)?;
    let [tag, game_id, identity_key, pay_txid, sig] = pushes.as_slice() else {
        return None;
    };
    if *tag != HELD_TAG
        || game_id.len() != COLLECTED_GAME_ID_LEN
        || identity_key.len() != COLLECTED_IDENTITY_KEY_LEN
        || pay_txid.len() != 32
        || !(COLLECTED_SIG_MIN_LEN..=COLLECTED_SIG_MAX_LEN).contains(&sig.len())
    {
        return None;
    }
    Some(HeldMarker {
        game_id: (*game_id).try_into().ok()?,
        identity_key: identity_key.to_vec(),
        pay_txid: (*pay_txid).try_into().ok()?,
        sig: sig.to_vec(),
    })
}

/// A held filing's key: game ‖ identity ‖ the paying txid (one row per payout transaction).
pub fn held_content_key(m: &HeldMarker) -> String {
    content_key(HELD_TAG, &[&m.game_id, &m.identity_key, &m.pay_txid])
}

/// PURE (the B3 lens fold M1): is `row_key` the content key this door files a held filing of (game, identity,
/// paying txid) under? The owed walk's gate on a stored row's `payTxid` column: only a row whose own key was
/// derived from that very txid retires its payout, so the column can never name a transaction the row was not
/// filed for, and a row keyed by a chain outpoint (which this door never writes) names nothing at all. One
/// sha256, no signature. All hex, any case; anything undecodable or of the wrong length is `false`.
pub fn is_held_row_key(row_key: &str, game_id: &str, identity: &str, pay_txid: &str) -> bool {
    let (Ok(game), Ok(id), Ok(txid)) = (
        hex::decode(game_id),
        hex::decode(identity),
        hex::decode(pay_txid),
    ) else {
        return false;
    };
    let (Ok(game), Ok(txid)) = (
        <[u8; 32]>::try_from(game.as_slice()),
        <[u8; 32]>::try_from(txid.as_slice()),
    ) else {
        return false;
    };
    if id.len() != overlay_discovery::collected::COLLECTED_IDENTITY_KEY_LEN {
        return false;
    }
    row_key
        == held_content_key(&HeldMarker {
            game_id: game,
            identity_key: id,
            pay_txid: txid,
            sig: Vec::new(),
        })
}

/// The client's held challenge: `LOW-collected\nv2\ngid=<gid>\nid=<identity>\ntxid=<payTxid>` (all lowercase
/// hex), signed under `[1,'low collected']`, keyID = gameId, counterparty anyone (the v1 marker's protocol and
/// key; the `v2` line and the `txid=` line keep the two challenges apart, so neither signature is the other's).
pub fn held_challenge(game_id_lc: &str, identity_lc: &str, pay_txid_lc: &str) -> Vec<u8> {
    format!("LOW-collected\nv2\ngid={game_id_lc}\nid={identity_lc}\ntxid={pay_txid_lc}")
        .into_bytes()
}

/// bsv-low #492: does this signature verify UNDER the named identity as a held filing of `pay_txid_lc` for the
/// game (the replay the filing door runs before it writes the row and its `payTxid`)? The owed walk does not call
/// it (the B3 lens fold M1: the door's check is the only one); a reader of `ls_collected` rows can.
pub fn held_sig_verifies(
    identity_lc: &str,
    game_id_lc: &str,
    pay_txid_lc: &str,
    sig_hex: &str,
) -> bool {
    let Ok(sig) = hex::decode(sig_hex) else {
        return false;
    };
    canonical_anyone_sig_verifies(
        identity_lc,
        game_id_lc,
        &held_challenge(game_id_lc, identity_lc, pay_txid_lc),
        &sig,
        collected_protocol(),
    )
}

/// `anyone_sig_verifies` behind the canonical-DER gate — the three families
/// whose serve-time verifier has no such bar get it at the FILING, where a
/// row is free (the gate's HIGH-2; `canonical_der`'s own doc has the why).
/// bsv-low #469 (the gate's HIGH-1): does this `collected_markers_v2` row's signature verify UNDER the named identity
/// (the same replay the filing door runs)? Only such a row retires a payout from the owed list; a row that merely
/// exists is byte-format admitted and proves nothing.
pub(crate) fn collected_sig_verifies(identity_lc: &str, game_id_lc: &str, sig_hex: &str) -> bool {
    let Ok(sig) = hex::decode(sig_hex) else {
        return false;
    };
    canonical_anyone_sig_verifies(
        identity_lc,
        game_id_lc,
        &collected_challenge(game_id_lc, identity_lc),
        &sig,
        collected_protocol(),
    )
}

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
pub(crate) fn unlock_pushes(script: &[u8]) -> Option<Vec<&[u8]>> {
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
    for (push, key) in [
        (pushes[0], &ctx.params.pub_a),
        (pushes[1], &ctx.params.pub_b),
    ] {
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
    /// bsv-low #492: a held filing (`LOW/collected/v2`), the row it writes to the v1 marker's table and the paying
    /// txid its signature names (lowercase hex; written to the row's `payTxid` column).
    Held(CollectedRecord, String),
    /// bsv-low #469: the parsed sweep filing, UNBOUND (the route reads the hop
    /// row and binds with `hopsweep::bind_hop_sweep` after the parse); only a
    /// bound one is written.
    Hopsweep(
        crate::hopsweep::HopsweepMarker,
        crate::hopsweep::HopsweepRecord,
    ),
}

impl VerifiedRecord {
    pub fn key(&self) -> &str {
        match self {
            Self::Potparty(r, _) => &r.txid,
            Self::Potrefund(r, _) => &r.txid,
            Self::Result(r, _) => &r.txid,
            Self::Collected(r) | Self::Held(r, _) => &r.txid,
            Self::Hopsweep(_, r) => &r.txid,
        }
    }
    pub fn kind(&self) -> RecordKind {
        match self {
            Self::Potparty(..) => RecordKind::Potparty,
            Self::Potrefund(..) => RecordKind::Potrefund,
            Self::Result(..) => RecordKind::Result,
            Self::Collected(_) | Self::Held(..) => RecordKind::Collected,
            Self::Hopsweep(..) => RecordKind::Hopsweep,
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
            if m.loser_sig
                .as_deref()
                .is_some_and(|s| canonical_der(s).is_none())
            {
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
            // bsv-low #492: the held filing (v2) comes through the v1 marker's door; the tags tell them apart
            if let Some(m) = parse_held_marker(script) {
                let identity = hex::encode(&m.identity_key);
                if identity != poster_lc {
                    return Err(RecordRefusal::PosterMismatch);
                }
                let game_id = hex::encode(m.game_id);
                let pay_txid = hex::encode(m.pay_txid);
                if !canonical_anyone_sig_verifies(
                    &identity,
                    &game_id,
                    &held_challenge(&game_id, &identity, &pay_txid),
                    &m.sig,
                    collected_protocol(),
                ) {
                    return Err(RecordRefusal::SignatureInvalid);
                }
                return Ok(VerifiedRecord::Held(
                    CollectedRecord {
                        identity,
                        game_id,
                        txid: held_content_key(&m),
                        output_index: 0,
                        sig_hex: Some(hex::encode(&m.sig)),
                        // bsv-low #492/#512 (loop 25 I5): the held filing's own
                        // paying txid, so the overlay's `ls_collected` reader
                        // serves it and a client can rebuild `heldChallenge`.
                        // The write binds it from the `Held` variant's String
                        // (`COLLECTED_FILE_SQL`), unchanged.
                        pay_txid: Some(pay_txid.clone()),
                    },
                    pay_txid,
                ));
            }
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
                // The v1 marker signs `collectedChallenge` (no txid): no payout
                // named, so the row's `payTxid` is NULL.
                pay_txid: None,
            }))
        }
        // bsv-low #848: a handfile is a batch body, not a script; the door dispatches it before this walk.
        RecordKind::Handfile => Err(RecordRefusal::BadKind),
        RecordKind::Hopsweep => {
            let m =
                crate::hopsweep::parse_hopsweep_marker(script).ok_or(RecordRefusal::NotAMarker)?;
            if hex::encode(&m.identity) != poster_lc {
                return Err(RecordRefusal::PosterMismatch);
            }
            // the cheap first refusal; the bar (the seat's signature over the hop's lock) needs the hop row
            if !crate::hopsweep::sweep_spends_hop(&m.sweep_raw, &m.hop_txid, m.hop_vout) {
                return Err(RecordRefusal::SweepDoesNotSpendTheHop);
            }
            let record =
                crate::hopsweep::record_of(&m).ok_or(RecordRefusal::SweepDoesNotSpendTheHop)?;
            Ok(VerifiedRecord::Hopsweep(m, record))
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

/// The `collected` door's ONE insert. `payTxid` (the last bind) is the paying transaction a held filing names
/// (bsv-low #492, the B3 lens fold M1) and NULL for the v1 marker; this door is the column's only writer, and it
/// writes it only after the signature over that txid verified. Rows are never updated and never deleted.
pub const COLLECTED_FILE_SQL: &str = "INSERT OR IGNORE INTO collected_markers_v2 \
     (identity, gameId, txid, outputIndex, sigHex, createdAt, payTxid) \
     VALUES (?, ?, ?, ?, ?, ?, ?)";

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
// `collected` counts its two shapes APART (the B3 lens fold L1): the v1 marker's rows name no transaction
// (`payTxid IS NULL`), a held filing's do. Same binds for both.
pub const COLLECTED_FILED_ROWS_SQL: &str = "SELECT COUNT(*) AS n FROM collected_markers_v2 \
     WHERE identity = ?1 AND gameId = ?2 AND txid LIKE 'filed:%' AND payTxid IS NULL AND txid <> ?3";
pub const HELD_FILED_ROWS_SQL: &str = "SELECT COUNT(*) AS n FROM collected_markers_v2 \
     WHERE identity = ?1 AND gameId = ?2 AND txid LIKE 'filed:%' AND payTxid IS NOT NULL AND txid <> ?3";

/// The late latch: a refund backup filed at rank 0 (the pot unindexed then)
/// re-filed once the index holds the pot — the INSERT is the no-op it always
/// is, the verdict is written. Never demotes (a 1 stays 1).
pub const POTREFUND_LATCH_SQL: &str = "UPDATE potrefund_records SET refundValid = 1 \
     WHERE txid = ?1 AND COALESCE(refundValid, 0) = 0";

// The chain no-op (the delta-verify's HIGH-1): a marker the CHAIN already
// holds for this identity, game, pot and VERSION, AND that verifies as the
// identity's own (bsv-low #564/#565). BINDS: (poster, gameId, potTxid, the
// version flag) — potparty: `seatSettlePubkey IS NULL` (1 = v1); result:
// `loserSigHex IS NULL` (1 = the plain claim); potrefund: (poster, gameId,
// potTxid); collected: (poster, gameId).
//
// #565: the flag is bound as the TEXT '1'/'0' (every bind of this door is a JS
// string, and D1 binds a JS string as TEXT), and `(x IS NULL)` is an INTEGER
// with no affinity, so SQLite compared INTEGER 1 with TEXT '1' unconverted:
// never equal, the no-op never fired. `CAST(?4 AS INTEGER)` compares integer
// with integer whatever the bind's type.
//
// #564: a chain row is admitted by BYTE FORMAT only, so its presence is no
// proof it is the poster's: anyone can buy a marker naming another identity.
// A chain row counts only when it verifies for the identity it names: potparty
// by the overlay's own `sigValid` latch (1; NULL, not yet latched, and 0 do not
// count), result by its `claimValid` tier at its own shape (1 for the plain
// claim, 2 for the countersigned), collected by the door re-running the filed
// path's signature check over each chain row's `sigHex`
// ([`own_collected_chain_rows`], at most `COLLECTED_CHAIN_SIGS_READ` rows: past
// them the filing proceeds, never a no-op on rows not checked), and potrefund
// (bsv-low #798) by the filed path's WHOLE bar over each chain row of the
// filing's pot outpoint: the marker signature under the identity AND the raw
// bound to the pot as its pre-signed spend by both committed settle keys
// ([`potrefund_chain_row_is_own`]; with the pot not indexed no chain row is
// valid, so a rank-0 filing writes), each such row latched `refundValid = 1`
// before the no-op answers. A row that does not verify is no no-op: the filing
// goes on to the caps and writes its own. Hopsweep rows have no chain writer.
pub const POTPARTY_CHAIN_ROWS_SQL: &str = "SELECT COUNT(*) AS n FROM potparty_records \
     WHERE identity = ?1 AND gameId = ?2 AND potTxid = ?3 AND txid NOT LIKE 'filed:%' \
       AND (seatSettlePubkey IS NULL) = CAST(?4 AS INTEGER) AND sigValid = 1";
/// The potrefund chain rows themselves, not a count (bsv-low #798): the door verifies each
/// ([`potrefund_chain_row_is_own`]). BINDS: (poster, gameId, potTxid, potVout).
pub const POTREFUND_CHAIN_ROWS_SQL: &str =
    "SELECT txid, outputIndex, refundRawHex, sigHex FROM potrefund_records \
     WHERE identity = ?1 AND gameId = ?2 AND potTxid = ?3 AND potVout = CAST(?4 AS INTEGER) \
       AND txid NOT LIKE 'filed:%' LIMIT 16";
/// The `LIMIT` of [`POTREFUND_CHAIN_ROWS_SQL`]: the most chain rows one filing verifies (past them the filing
/// proceeds, never a no-op on rows not checked).
pub const POTREFUND_CHAIN_ROWS_READ: usize = 16;
/// bsv-low #798: a potrefund CHAIN row the door verified as the poster's own pre-signed refund takes the verdict a
/// filed row takes ([`POTREFUND_LATCH_SQL`]'s), keyed by its own outpoint, before the no-op answers. Never demotes.
pub const POTREFUND_CHAIN_LATCH_SQL: &str = "UPDATE potrefund_records SET refundValid = 1 \
     WHERE txid = ?1 AND outputIndex = CAST(?2 AS INTEGER) AND COALESCE(refundValid, 0) = 0";
pub const RESULT_CHAIN_ROWS_SQL: &str = "SELECT COUNT(*) AS n FROM result_markers_v2 \
     WHERE winner = ?1 AND gameId = ?2 AND potTxid = ?3 AND txid NOT LIKE 'filed:%' \
       AND (loserSigHex IS NULL) = CAST(?4 AS INTEGER) \
       AND claimValid >= CASE WHEN loserSigHex IS NULL THEN 1 ELSE 2 END";
/// The collected chain rows' signatures, not a count (#564): the door verifies each.
pub const COLLECTED_CHAIN_ROWS_SQL: &str = "SELECT sigHex FROM collected_markers_v2 \
     WHERE identity = ?1 AND gameId = ?2 AND txid NOT LIKE 'filed:%' AND sigHex IS NOT NULL \
     LIMIT 16";
/// The `LIMIT` of [`COLLECTED_CHAIN_ROWS_SQL`]: the most chain rows one filing verifies (16 ECDSA verifies).
pub const COLLECTED_CHAIN_SIGS_READ: usize = 16;

/// #564: how many of the collected chain rows read for (identity, game) carry a signature that verifies for that
/// identity: the filed path's own check ([`collected_sig_verifies`]). Only those make the filing a no-op.
pub fn own_collected_chain_rows(
    identity_lc: &str,
    game_id_lc: &str,
    sig_hexes: &[Option<String>],
) -> i64 {
    sig_hexes
        .iter()
        .take(COLLECTED_CHAIN_SIGS_READ)
        .flatten()
        .filter(|sig| collected_sig_verifies(identity_lc, game_id_lc, &sig.to_ascii_lowercase()))
        .count() as i64
}

/// One potrefund chain row as [`POTREFUND_CHAIN_ROWS_SQL`] reads it.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PotrefundChainRow {
    pub txid: String,
    #[serde(rename = "outputIndex")]
    pub output_index: f64,
    #[serde(rename = "refundRawHex", default)]
    pub refund_raw_hex: Option<String>,
    #[serde(rename = "sigHex", default)]
    pub sig_hex: Option<String>,
}

/// bsv-low #798: is this chain row (of the filing's identity, game and pot outpoint) VALID for the filing's own
/// binding? The filed path's whole bar over the row's stored bytes: its marker signature verifies under the
/// identity (canonical DER, over `potrefund_challenge` of the row's refund raw) AND its raw is the pot's pre-signed
/// spend by both committed settle keys ([`bind_refund_backup`] = rank 1). With the pot not indexed nothing is rank
/// 1, so no chain row is valid and the filing writes its own (rank 0). A presence-only row (`tm_potrefund` admits by
/// byte format) is never valid.
pub fn potrefund_chain_row_is_own(
    filing: &PotrefundRecord,
    row: &PotrefundChainRow,
    pot: Option<&PotContext>,
) -> bool {
    let (Some(raw_hex), Some(sig_hex)) = (&row.refund_raw_hex, &row.sig_hex) else {
        return false;
    };
    let raw_hex = raw_hex.to_ascii_lowercase();
    let (Ok(raw), Ok(sig), Ok(id), Ok(gid), Ok(pot_txid)) = (
        hex::decode(&raw_hex),
        hex::decode(sig_hex),
        hex::decode(&filing.identity),
        hex::decode(&filing.game_id),
        hex::decode(&filing.pot_txid),
    ) else {
        return false;
    };
    let (Ok(gid), Ok(pot_txid)) = (
        <[u8; 32]>::try_from(gid.as_slice()),
        <[u8; 32]>::try_from(pot_txid.as_slice()),
    ) else {
        return false;
    };
    let challenge = potrefund_challenge(&id, &gid, &pot_txid, filing.pot_vout, &raw);
    if !canonical_anyone_sig_verifies(
        &filing.identity,
        &filing.game_id,
        &challenge,
        &sig,
        potrefund_protocol(),
    ) {
        return false;
    }
    let as_filed = PotrefundRecord {
        refund_raw_hex: raw_hex,
        ..filing.clone()
    };
    bind_refund_backup(&as_filed, pot) == Ok(true)
}

/// bsv-low #798: the chain rows read for a refund filing split into the poster's own (their outpoints, to latch) and
/// how many did not verify (counted). At most [`POTREFUND_CHAIN_ROWS_READ`] rows are judged.
pub fn own_potrefund_chain_rows(
    filing: &PotrefundRecord,
    rows: &[PotrefundChainRow],
    pot: Option<&PotContext>,
) -> (Vec<(String, u32)>, u64) {
    let mut own = Vec::new();
    let mut unverified = 0u64;
    for row in rows.iter().take(POTREFUND_CHAIN_ROWS_READ) {
        if potrefund_chain_row_is_own(filing, row, pot) {
            own.push((row.txid.clone(), row.output_index.max(0.0) as u32));
        } else {
            unverified += 1;
        }
    }
    (own, unverified)
}

/// bsv-low #798: what the potrefund chain no-op found: how many own rows (`> 0` = the no-op) and whether its latch
/// moved one (`refundValid` 0 / NULL to 1, or a latch whose count the platform did not answer): a latch that moved a
/// row changes what is owed (the MONEY-DOOR lens's LOW-1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChainNoop {
    pub own: i64,
    pub latched: bool,
}

/// bsv-low #798: the potrefund chain no-op through the port, shared by the direct door and the handfile arm. Reads
/// the chain rows of the filing's (identity, game, pot outpoint), judges each by the filed path's bar, LATCHES every
/// own row `refundValid = 1` and judges both seats in one batch; answers how many own rows there are and whether
/// the latch moved one. The batch runs before the caller answers.
pub(crate) async fn potrefund_chain_noop<P: crate::owed_judge::JudgeDb>(
    port: &P,
    filing: &PotrefundRecord,
    pot: Option<&PotContext>,
) -> std::result::Result<ChainNoop, String> {
    let (sql, binds) = chain_rows_query(&VerifiedRecord::Potrefund(filing.clone(), false))
        .expect("a refund backup has a chain no-op");
    let out = port_one(port, stmt(sql, binds.iter().map(|s| text(s)).collect())).await?;
    let rows: Vec<PotrefundChainRow> = out
        .rows
        .into_iter()
        .filter_map(|r| serde_json::from_value(r).ok())
        .collect();
    let (own, unverified) = own_potrefund_chain_rows(filing, &rows, pot);
    if unverified > 0 {
        POTREFUND_CHAIN_ROWS_UNVERIFIED.fetch_add(unverified, Ordering::Relaxed);
    }
    let mut latched = false;
    if !own.is_empty() {
        let writes = own
            .iter()
            .map(|(txid, vout)| {
                stmt(
                    POTREFUND_CHAIN_LATCH_SQL,
                    vec![text(txid), int(i64::from(*vout))],
                )
            })
            .collect();
        let marks: Vec<_> = own
            .iter()
            .map(|(txid, _)| crate::owed_judge::FilingMark {
                source: "refund".into(),
                source_key: txid.clone(),
            })
            .collect();
        let verified = VerifiedRecord::Potrefund(filing.clone(), true);
        let report = owed_write::judge_filings(
            port,
            &[verified],
            &[],
            (port.now_ms() / 1000) as i64,
            writes,
            &marks,
        )
        .await?;
        latched = report
            .source_changes
            .iter()
            .any(|n| insert_wrote_a_row(n.map(|n| n as usize)));
    }
    Ok(ChainNoop {
        own: own.len() as i64,
        latched,
    })
}

/// The chain no-op statement and its binds for a verified record (a pure
/// value, for the same reason as [`cap_queries`]). `None` for a held filing
/// (bsv-low #492): the chain never holds one, and the v1 marker a chain row
/// for its game would be is another marker, never a reason to drop it.
pub fn chain_rows_query(v: &VerifiedRecord) -> Option<(&'static str, Vec<String>)> {
    Some(match v {
        VerifiedRecord::Held(..) => return None,
        VerifiedRecord::Potparty(r, _) => (
            POTPARTY_CHAIN_ROWS_SQL,
            vec![
                r.identity.clone(),
                r.game_id.clone(),
                r.pot_txid.clone(),
                if r.seat_settle_pubkey.is_none() {
                    "1"
                } else {
                    "0"
                }
                .to_string(),
            ],
        ),
        VerifiedRecord::Potrefund(r, _) => (
            POTREFUND_CHAIN_ROWS_SQL,
            vec![
                r.identity.clone(),
                r.game_id.clone(),
                r.pot_txid.clone(),
                r.pot_vout.to_string(),
            ],
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
        VerifiedRecord::Hopsweep(_, r) => (
            crate::hopsweep::HOPSWEEP_CHAIN_ROWS_SQL,
            vec![r.identity.clone(), r.game_id.clone(), r.hop_txid.clone()],
        ),
    })
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
            vec![
                r.identity.clone(),
                r.game_id.clone(),
                r.pot_txid.clone(),
                r.txid.clone(),
            ],
            POTPARTY_FILED_TODAY_SQL,
            r.identity.clone(),
        ),
        VerifiedRecord::Potrefund(r, _) => (
            POTREFUND_FILED_ROWS_SQL,
            vec![
                r.identity.clone(),
                r.game_id.clone(),
                r.pot_txid.clone(),
                r.txid.clone(),
            ],
            POTREFUND_FILED_TODAY_SQL,
            r.identity.clone(),
        ),
        VerifiedRecord::Result(r, _) => (
            RESULT_FILED_ROWS_SQL,
            vec![
                r.winner.clone(),
                r.game_id.clone(),
                r.pot_txid.clone(),
                r.txid.clone(),
            ],
            RESULT_FILED_TODAY_SQL,
            r.winner.clone(),
        ),
        VerifiedRecord::Collected(r) => (
            COLLECTED_FILED_ROWS_SQL,
            vec![r.identity.clone(), r.game_id.clone(), r.txid.clone()],
            COLLECTED_FILED_TODAY_SQL,
            r.identity.clone(),
        ),
        // the held rows of the game only (L1); the day is the family's, both shapes together
        VerifiedRecord::Held(r, _) => (
            HELD_FILED_ROWS_SQL,
            vec![r.identity.clone(), r.game_id.clone(), r.txid.clone()],
            COLLECTED_FILED_TODAY_SQL,
            r.identity.clone(),
        ),
        VerifiedRecord::Hopsweep(_, r) => (
            crate::hopsweep::HOPSWEEP_FILED_ROWS_SQL,
            vec![
                r.identity.clone(),
                r.game_id.clone(),
                r.hop_txid.clone(),
                r.txid.clone(),
            ],
            crate::hopsweep::HOPSWEEP_FILED_TODAY_SQL,
            r.identity.clone(),
        ),
    }
}

/// The pure cap decision from the two counts, for a family's own cap.
pub fn cap_refusal(kind: RecordKind, rows_for_pot: i64, rows_today: i64) -> Option<RecordRefusal> {
    cap_refusal_at(filed_rows_cap(kind), rows_for_pot, rows_today)
}

/// The pure cap decision for ONE verified record (the route's call): its own per-game cap
/// ([`filed_rows_cap_of`]) over the count [`cap_queries`] names for it.
pub fn cap_refusal_for(
    v: &VerifiedRecord,
    rows_for_pot: i64,
    rows_today: i64,
) -> Option<RecordRefusal> {
    cap_refusal_at(filed_rows_cap_of(v), rows_for_pot, rows_today)
}

pub(crate) fn cap_refusal_at(
    cap: i64,
    rows_for_pot: i64,
    rows_today: i64,
) -> Option<RecordRefusal> {
    if rows_for_pot >= cap {
        return Some(RecordRefusal::TooManyFiled);
    }
    if rows_today >= RECORD_FILINGS_PER_IDENTITY_PER_DAY {
        return Some(RecordRefusal::DailyCapReached);
    }
    None
}

/// PURE (the B3 lens fold L2): did the `collected` door's `INSERT OR IGNORE` write a row? `changes` is D1's count
/// for the statement: 0 = the content key was already on file (a re-file). An unreadable count is read as WRITTEN
/// (the stale mark and the push run, as before the fold): a missed recompute would be the worse error.
pub fn insert_wrote_a_row(changes: Option<usize>) -> bool {
    changes != Some(0)
}

/// The `collected` row of one content key, re-read (the MONEY-DOOR lens's LOW-2).
pub const COLLECTED_ROW_STAMP_SQL: &str =
    "SELECT createdAt FROM collected_markers_v2 WHERE txid = ?1 AND outputIndex = 0";

/// The MONEY-DOOR lens's LOW-2: the `collected` insert answered with NO change count. The row is re-read by its
/// key: stamped by THIS request (`createdAt` = `now`) is a write; stamped earlier is the re-file (nothing written).
/// A row not found, or a stamp that does not read, is read as written: a missed recompute is the worse error. A
/// re-file inside the same second as its first filing reads as a write (one extra stale mark).
pub(crate) async fn collected_written_now<P: crate::pot_hook::SqlPort>(
    port: &P,
    content_key: &str,
    now: i64,
) -> std::result::Result<bool, String> {
    let out = port_one(port, stmt(COLLECTED_ROW_STAMP_SQL, vec![text(content_key)])).await?;
    Ok(match out.rows.first().and_then(|r| r.get("createdAt")) {
        Some(stamp) => stamp.as_f64().is_none_or(|t| t as i64 == now),
        None => true,
    })
}

/// PURE (the B3 lens fold L2): the door's answer to a re-file that wrote nothing. 200 with the EXISTING key (the
/// shipped client keys on `key` and drops its outbox row), `filed: false` (this request wrote nothing) and
/// `alreadyFiled: true`; a held filing names its txid as on a first filing.
pub fn already_filed_body(v: &VerifiedRecord) -> serde_json::Value {
    let mut body = serde_json::json!({ "filed": false, "kind": v.kind().as_str(), "key": v.key(), "alreadyFiled": true });
    if let VerifiedRecord::Held(_, pay_txid) = v {
        body["heldTxid"] = serde_json::json!(pay_txid);
    }
    body
}

// ── counters (per isolate; a soak/monitoring surface on /health, Rule 13) ──

static FILED_BY_KIND: [AtomicU64; RECORD_KINDS] = [const { AtomicU64::new(0) }; RECORD_KINDS];
static REFUSED_BY_KIND: [AtomicU64; RECORD_KINDS] = [const { AtomicU64::new(0) }; RECORD_KINDS];
static TOO_MANY_FILED_BY_KIND: [AtomicU64; RECORD_KINDS] =
    [const { AtomicU64::new(0) }; RECORD_KINDS];
static DAILY_CAP_BY_KIND: [AtomicU64; RECORD_KINDS] = [const { AtomicU64::new(0) }; RECORD_KINDS];
static ALREADY_INDEXED_BY_KIND: [AtomicU64; RECORD_KINDS] =
    [const { AtomicU64::new(0) }; RECORD_KINDS];
/// Re-files of a `collected` content already on file (the B3 lens fold L2): nothing written, staled or pushed.
static ALREADY_FILED_BY_KIND: [AtomicU64; RECORD_KINDS] =
    [const { AtomicU64::new(0) }; RECORD_KINDS];
/// Refund backups written at rank 0 (the pot not indexed at filing time).
static REFUNDS_FILED_UNBOUND: AtomicU64 = AtomicU64::new(0);
/// Rank-0 refund rows latched to rank 1 by a later re-file.
static REFUNDS_LATCHED_LATER: AtomicU64 = AtomicU64::new(0);
/// bsv-low #798: potrefund chain rows a refund filing read that did not verify as the poster's own (never a no-op).
static POTREFUND_CHAIN_ROWS_UNVERIFIED: AtomicU64 = AtomicU64::new(0);
static ANONYMOUS_FILED_BY_KIND: [AtomicU64; RECORD_KINDS] =
    [const { AtomicU64::new(0) }; RECORD_KINDS];

fn bump(c: &[AtomicU64; RECORD_KINDS], kind: RecordKind) {
    c[kind.index()].fetch_add(1, Ordering::Relaxed);
}

/// bsv-low #848: the handfile arm's counters, the door's own (`handfile.rs` runs the same ledger).
pub(crate) fn note_filed(kind: RecordKind, anonymous: bool) {
    bump(&FILED_BY_KIND, kind);
    if anonymous {
        bump(&ANONYMOUS_FILED_BY_KIND, kind);
    }
}
pub(crate) fn note_already_filed(kind: RecordKind) {
    bump(&ALREADY_FILED_BY_KIND, kind);
}
pub(crate) fn note_cap_refused(r: RecordRefusal, kind: RecordKind) {
    bump(
        if r == RecordRefusal::TooManyFiled {
            &TOO_MANY_FILED_BY_KIND
        } else {
            &DAILY_CAP_BY_KIND
        },
        kind,
    );
}
pub(crate) fn note_bad_body(kind: RecordKind) {
    bump(&REFUSED_BY_KIND, kind);
    mirror_refused(REFUSED_BAD_BODY_CLASS);
}

fn by_kind(c: &[AtomicU64; RECORD_KINDS]) -> serde_json::Value {
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
        "alreadyFiledByKind": by_kind(&ALREADY_FILED_BY_KIND),
        "anonymousFiledByKind": by_kind(&ANONYMOUS_FILED_BY_KIND),
        "refundsFiledUnbound": REFUNDS_FILED_UNBOUND.load(Ordering::Relaxed),
        "refundsLatchedLater": REFUNDS_LATCHED_LATER.load(Ordering::Relaxed),
        "potrefundChainRowsUnverified": POTREFUND_CHAIN_ROWS_UNVERIFIED.load(Ordering::Relaxed),
        "filedRowsCap": {
            "potparty": filed_rows_cap(RecordKind::Potparty),
            "potrefund": filed_rows_cap(RecordKind::Potrefund),
            "result": filed_rows_cap(RecordKind::Result),
            "collected": filed_rows_cap(RecordKind::Collected),
            "collectedHeld": HELD_FILINGS_PER_GAME,
            "hopsweep": filed_rows_cap(RecordKind::Hopsweep),
            "handfileHopparty": filed_rows_cap(RecordKind::Handfile),
        },
        "filingsPerIdentityPerDay": RECORD_FILINGS_PER_IDENTITY_PER_DAY,
    })
}

// ── the route ───────────────────────────────────────────────────────────────

pub(crate) fn js(s: &str) -> worker::wasm_bindgen::JsValue {
    s.into()
}
pub(crate) fn js_num(n: i64) -> worker::wasm_bindgen::JsValue {
    (n as f64).into()
}

/// One collected chain row's signature (`COLLECTED_CHAIN_ROWS_SQL`).
#[derive(Deserialize)]
struct SigRowD1 {
    #[serde(rename = "sigHex", default)]
    sig_hex: Option<String>,
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

pub(crate) async fn read_pot_context(
    db: &worker::D1Database,
    pot_txid_lc: &str,
    pot_vout: u32,
) -> Result<Option<PotContext>> {
    let row = db
        .prepare(crate::results::decoded_pots_sql(1))
        .bind(&[js(pot_txid_lc), js_num(i64::from(pot_vout))])?
        .counted_first::<PotRowD1>()
        .await?;
    Ok(row.and_then(|r| r.context()))
}

/// The `hopparty_records` row of ONE hop outpoint for the poster (bsv-low #469; `hopsweep::HOP_CONTEXT_SQL`).
#[derive(Deserialize)]
struct HopRowD1 {
    identity: String,
    #[serde(rename = "opponentIdentity")]
    opponent_identity: String,
    #[serde(rename = "gameId")]
    game_id: String,
    #[serde(rename = "hopVout")]
    hop_vout: f64,
    #[serde(rename = "hopSats")]
    hop_sats: f64,
    #[serde(rename = "seatSettlePubkey")]
    seat_settle_pubkey: String,
    #[serde(rename = "identitySigHex")]
    identity_sig_hex: String,
    #[serde(rename = "hopLockHex", default)]
    hop_lock_hex: Option<String>,
    #[serde(rename = "hopSatsOnChain", default)]
    hop_sats_on_chain: Option<f64>,
    #[serde(rename = "markerValid", default)]
    marker_valid: Option<f64>,
}

impl HopRowD1 {
    fn context(self) -> crate::hopsweep::HopContext {
        crate::hopsweep::HopContext {
            identity: self.identity.to_ascii_lowercase(),
            opponent_identity: self.opponent_identity.to_ascii_lowercase(),
            game_id: self.game_id.to_ascii_lowercase(),
            hop_vout: self.hop_vout as u32,
            hop_sats: self.hop_sats as u64,
            seat_settle_pubkey: self.seat_settle_pubkey.to_ascii_lowercase(),
            identity_sig_hex: self.identity_sig_hex.to_ascii_lowercase(),
            hop_lock_hex: self.hop_lock_hex.map(|l| l.to_ascii_lowercase()),
            hop_sats_on_chain: self.hop_sats_on_chain.map(|v| v as u64),
            marker_valid: self.marker_valid.map(|v| v >= 1.0),
        }
    }
}

pub(crate) async fn count(
    db: &worker::D1Database,
    sql: &str,
    binds: &[worker::wasm_bindgen::JsValue],
) -> Result<i64> {
    let row = db
        .prepare(sql)
        .bind(binds)?
        .counted_first::<CountRowD1>()
        .await?;
    Ok(row.and_then(|r| r.n).map_or(0, |n| n as i64))
}

pub(crate) fn refuse(kind: Option<RecordKind>, r: RecordRefusal) -> Result<Response> {
    refuse_with_family(kind, r, None)
}

pub(crate) fn record_refusal_body(r: RecordRefusal, family: Option<&str>) -> serde_json::Value {
    let mut body = serde_json::json!({ "error": r.as_str() });
    if r == RecordRefusal::SweepGameMismatch {
        body["code"] = serde_json::json!(r.class());
    }
    if let Some(family) = family {
        body["family"] = serde_json::json!(family);
    }
    body
}

pub(crate) fn refuse_with_family(
    kind: Option<RecordKind>,
    r: RecordRefusal,
    family: Option<&str>,
) -> Result<Response> {
    let (status, body) = record_refusal_reply(kind, r, family);
    crate::routes::json_response(body.to_string(), status)
}

fn record_refusal_reply(
    kind: Option<RecordKind>,
    r: RecordRefusal,
    family: Option<&str>,
) -> (u16, serde_json::Value) {
    if let Some(k) = kind {
        bump(&REFUSED_BY_KIND, k);
    }
    mirror_refused(r.class());
    (r.status(), record_refusal_body(r, family))
}

// ── the door's core, over the SQL port (the route's D1, the pins' real SQLite) ──

/// What the door's core answered for one verified filing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DoorOutcome {
    /// Refused (the bind, the hop, a cap): nothing written.
    Refused(RecordRefusal),
    /// The chain no-op: nothing written, nothing counted. `latched`: the potrefund no-op's latch moved its own chain
    /// row to `refundValid = 1` (the MONEY-DOOR lens's LOW-1: that moves what is owed).
    AlreadyIndexed { latched: bool },
    /// The family's write ran; `row_written` is false only when the `collected` door's insert wrote nothing.
    Written { row_written: bool },
}

/// The chain no-op's answer: 200, the filing's key, `alreadyIndexed`.
pub(crate) fn already_indexed_body(v: &VerifiedRecord) -> serde_json::Value {
    serde_json::json!({
        "filed": true,
        "kind": v.kind().as_str(),
        "key": v.key(),
        "alreadyIndexed": true,
    })
}

pub(crate) fn text(s: &str) -> crate::pot_hook::Bind {
    crate::pot_hook::Bind::Text(s.to_string())
}
pub(crate) fn int(n: i64) -> crate::pot_hook::Bind {
    crate::pot_hook::Bind::Int(n)
}
pub(crate) fn text_opt(s: Option<&str>) -> crate::pot_hook::Bind {
    s.map_or(crate::pot_hook::Bind::Null, text)
}
fn stmt(sql: &str, binds: Vec<crate::pot_hook::Bind>) -> crate::pot_hook::Stmt {
    crate::pot_hook::Stmt {
        sql: sql.to_string(),
        binds,
    }
}

/// One statement through the port: its rows and its changes.
pub(crate) async fn port_one<P: crate::pot_hook::SqlPort>(
    port: &P,
    st: crate::pot_hook::Stmt,
) -> std::result::Result<crate::pot_hook::StmtOut, String> {
    port.run_batch(vec![st])
        .await?
        .pop()
        .ok_or_else(|| "the batch answered no statement".to_string())
}

/// A `COUNT(*) AS n` through the port.
pub(crate) async fn port_count<P: crate::pot_hook::SqlPort>(
    port: &P,
    sql: &str,
    binds: Vec<crate::pot_hook::Bind>,
) -> std::result::Result<i64, String> {
    let out = port_one(port, stmt(sql, binds)).await?;
    Ok(out
        .rows
        .first()
        .and_then(|r| r.get("n"))
        .and_then(|n| n.as_f64())
        .unwrap_or(0.0) as i64)
}

/// The pot a refund backup names, as the index holds it (`decoded_pots_sql(1)`), through the port.
pub(crate) async fn port_pot_context<P: crate::pot_hook::SqlPort>(
    port: &P,
    pot_txid_lc: &str,
    pot_vout: u32,
) -> std::result::Result<Option<PotContext>, String> {
    let out = port_one(
        port,
        stmt(
            &crate::results::decoded_pots_sql(1),
            vec![text(pot_txid_lc), int(i64::from(pot_vout))],
        ),
    )
    .await?;
    Ok(out
        .rows
        .into_iter()
        .next()
        .and_then(|r| serde_json::from_value::<PotRowD1>(r).ok())
        .and_then(|r| r.context()))
}

/// The hop row of ONE hop outpoint for the poster (`hopsweep::HOP_CONTEXT_SQL`), through the port.
async fn port_hop_context<P: crate::pot_hook::SqlPort>(
    port: &P,
    hop_txid_lc: &str,
    hop_vout: u32,
    poster_lc: &str,
) -> std::result::Result<Option<crate::hopsweep::HopContext>, String> {
    let out = port_one(
        port,
        stmt(
            crate::hopsweep::HOP_CONTEXT_SQL,
            vec![text(hop_txid_lc), int(i64::from(hop_vout)), text(poster_lc)],
        ),
    )
    .await?;
    Ok(out
        .rows
        .into_iter()
        .next()
        .and_then(|r| serde_json::from_value::<HopRowD1>(r).ok())
        .map(HopRowD1::context))
}

/// THE DOOR'S CORE, after the marker verified (`verify_record_post`): a refund backup bound to its pot, a hop
/// sweep bound to its hop, the chain no-op, the caps, the family's write and the refund's late latch, in that
/// order. The route runs it over D1 (`routes::D1Sql`); the pins run it as shipped over real SQLite.
pub(crate) async fn file_verified<P: crate::owed_judge::JudgeDb>(
    port: &P,
    verified: &mut VerifiedRecord,
    identity: &str,
    now: i64,
) -> std::result::Result<DoorOutcome, String> {
    // A refund backup's pot, as the index holds it — read only AFTER the
    // marker's own signature verified (no free read for a stranger): the
    // bind is rank 1 (the pre-signed spend), rank 0 (the pot not indexed
    // yet), or a refusal.
    let mut pot = None;
    if let VerifiedRecord::Potrefund(r, refund_valid) = verified {
        pot = port_pot_context(port, &r.pot_txid, r.pot_vout).await?;
        *refund_valid = match bind_refund_backup(r, pot.as_ref()) {
            Ok(v) => v,
            Err(refusal) => return Ok(DoorOutcome::Refused(refusal)),
        };
    }
    // bsv-low #469: a hop sweep's hop, as the index holds it for THIS poster — read only after the marker parsed and
    // the poster matched (no free read of a hop a stranger cannot name); the bind is the seat's own signature over
    // the hop's lock (`hopsweep::bind_hop_sweep`); an unindexed hop is 425 (the client's outbox re-files).
    if let VerifiedRecord::Hopsweep(m, r) = &*verified {
        let hop = port_hop_context(port, &r.hop_txid, r.hop_vout, identity).await?;
        if let Err(refusal) = crate::hopsweep::bind_hop_sweep(m, hop.as_ref()) {
            return Ok(DoorOutcome::Refused(refusal.into()));
        }
    }
    let verified = &*verified;
    let mut chain_latched = false;
    // The chain no-op (step 6): the chain already holds this marker for this
    // identity — nothing to add, nothing written, nothing counted.
    // A chain row counts only when it verifies as the poster's own (#564).
    let chain_rows = match (chain_rows_query(verified), verified) {
        (Some(_), VerifiedRecord::Potrefund(r, _)) => {
            let noop = potrefund_chain_noop(port, r, pot.as_ref()).await?;
            chain_latched = noop.latched;
            noop.own
        }
        (Some((chain_sql, chain_binds)), VerifiedRecord::Collected(r)) => {
            let out = port_one(
                port,
                stmt(chain_sql, chain_binds.iter().map(|s| text(s)).collect()),
            )
            .await?;
            let sigs = out
                .rows
                .into_iter()
                .map(|row| {
                    serde_json::from_value::<SigRowD1>(row)
                        .ok()
                        .and_then(|r| r.sig_hex)
                })
                .collect::<Vec<_>>();
            own_collected_chain_rows(&r.identity, &r.game_id, &sigs)
        }
        (Some((chain_sql, chain_binds)), _) => {
            port_count(
                port,
                chain_sql,
                chain_binds.iter().map(|s| text(s)).collect(),
            )
            .await?
        }
        (None, _) => 0,
    };
    if chain_rows > 0 {
        return Ok(DoorOutcome::AlreadyIndexed {
            latched: chain_latched,
        });
    }
    // The caps (step 6): the poster's rows for this (family, game, pot) —
    // this content excluded — and its day.
    let (rows_sql, rows_binds, day_sql, day_identity) = cap_queries(verified);
    let rows_for_pot =
        port_count(port, rows_sql, rows_binds.iter().map(|s| text(s)).collect()).await?;
    let rows_today = port_count(
        port,
        day_sql,
        vec![text(&day_identity), int(now - RECORD_DAY_SECS)],
    )
    .await?;
    if let Some(r) = cap_refusal_for(verified, rows_for_pot, rows_today) {
        return Ok(DoorOutcome::Refused(r));
    }
    let writes = owed_write::statements(verified, now);
    let report = owed_write::judge_filings(
        port,
        std::slice::from_ref(verified),
        &[],
        now,
        writes,
        &[owed_write::mark(verified)],
    )
    .await?;
    let mut row_written = true;
    match verified {
        VerifiedRecord::Collected(r) | VerifiedRecord::Held(r, _) => {
            row_written = match report.source_changes.first().copied().flatten() {
                Some(n) => insert_wrote_a_row(Some(n as usize)),
                None => collected_written_now(port, &r.txid, now).await?,
            };
        }
        VerifiedRecord::Potrefund(_, valid) => {
            if *valid && report.source_changes.get(1).copied().flatten().unwrap_or(0) > 0 {
                REFUNDS_LATCHED_LATER.fetch_add(1, Ordering::Relaxed);
            } else if !valid {
                REFUNDS_FILED_UNBOUND.fetch_add(1, Ordering::Relaxed);
            }
        }
        _ => {}
    }
    Ok(DoorOutcome::Written { row_written })
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
    if kind == RecordKind::Handfile {
        // bsv-low #848: the batch arm, its own body shape, the same ledger, caps and tables.
        return crate::handfile::handfile_post(&ctx, &raw, &identity, anonymous).await;
    }
    let body: RecordPostBody = match serde_json::from_slice(&raw) {
        Ok(b) => b,
        Err(e) => {
            bump(&REFUSED_BY_KIND, kind);
            mirror_refused(REFUSED_BAD_BODY_CLASS);
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
    let mut verified = match verify_record_post(kind, &script, &identity) {
        Ok(v) => v,
        Err(r) => return refuse(Some(kind), r),
    };
    // The overlay stamps every one of these tables in unix SECONDS.
    let now = (worker::Date::now().as_millis() / 1000) as i64;
    let tip = crate::routes::chaintracks_present_height_env(&ctx.env, "owed")
        .await
        .ok();
    let port = crate::owed_judge::D1Judge::new(
        &ctx.env,
        tip,
        crate::routes::written_off_before_ms_env(&ctx.env),
    )?;
    let outcome = file_verified(&port, &mut verified, &identity, now)
        .await
        .map_err(worker::Error::RustError)?;
    let row_written = match outcome {
        DoorOutcome::Refused(r) => {
            if matches!(
                r,
                RecordRefusal::TooManyFiled | RecordRefusal::DailyCapReached
            ) {
                bump(
                    if r == RecordRefusal::TooManyFiled {
                        &TOO_MANY_FILED_BY_KIND
                    } else {
                        &DAILY_CAP_BY_KIND
                    },
                    kind,
                );
            }
            return refuse(Some(kind), r);
        }
        DoorOutcome::AlreadyIndexed { .. } => {
            bump(&ALREADY_INDEXED_BY_KIND, kind);
            return crate::routes::json_response(already_indexed_body(&verified).to_string(), 200);
        }
        DoorOutcome::Written { row_written } => row_written,
    };
    if !row_written {
        bump(&ALREADY_FILED_BY_KIND, kind);
        return crate::routes::json_response(already_filed_body(&verified).to_string(), 200);
    }

    note_filed(kind, anonymous);
    let mut body =
        serde_json::json!({ "filed": true, "kind": kind.as_str(), "key": verified.key() });
    if let VerifiedRecord::Potrefund(_, refund_valid) = &verified {
        body["refundValid"] = serde_json::json!(i64::from(*refund_valid));
    }
    if let VerifiedRecord::Held(_, pay_txid) = &verified {
        body["heldTxid"] = serde_json::json!(pay_txid);
    }
    crate::routes::json_response(body.to_string(), 200)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
pub(crate) mod tests {
    use super::*;

    mod budget_fleet {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/d1_fleet.rs"
        ));
    }

    #[tokio::test]
    #[ignore = "exports the production filing plan for the local D1 budget gate"]
    async fn owed893_export_filing_budget() {
        use crate::pot_hook::Bind;
        let path = std::env::var("OWED_FILING_BUDGET_OUT").expect("fixture destination");
        let db = crate::pot_hook::tests::SqliteSql::migrated();
        let victim = wallet(0x41);
        let id = hex::encode(identity(&victim));
        let game = [0x7a; 32];
        md_index_the_pot(&db);
        md_seat(&db, &id, &game);
        let partner = hex::encode(identity(&wallet(0x42)));
        db.conn.execute("INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt,sigValid) SELECT ?1,identity,gameId,potTxid,potVout,recoveryHeight,sigHex,'filed:other-seat',0,createdAt,sigValid FROM potparty_records WHERE identity=?2",rusqlite::params![partner,id]).unwrap();
        let schema: Vec<String> = db.conn.prepare("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY rowid").unwrap().query_map([],|r|r.get(0)).unwrap().map(Result::unwrap).collect();
        let mut seed = Vec::new();
        for table in ["pot_records", "potparty_records"] {
            let cols: Vec<String> = db
                .conn
                .prepare(&format!("PRAGMA table_info({table})"))
                .unwrap()
                .query_map([], |r| r.get(1))
                .unwrap()
                .map(Result::unwrap)
                .collect();
            let values = cols
                .iter()
                .map(|c| format!("quote(\"{c}\")"))
                .collect::<Vec<_>>()
                .join(" || ',' || ");
            let sql =
                format!("SELECT 'INSERT INTO {table} VALUES (' || {values} || ')' FROM {table}");
            seed.extend(
                db.conn
                    .prepare(&sql)
                    .unwrap()
                    .query_map([], |r| r.get::<_, String>(0))
                    .unwrap()
                    .map(Result::unwrap),
            );
        }
        for sql in budget_fleet::rows(2_000)
            .into_iter()
            .chain(budget_fleet::owed_rows(2_000))
        {
            budget_fleet::execute_row(&db.conn, &sql);
            seed.push(sql);
        }
        db.conn.execute_batch("DELETE FROM owed_dirty").unwrap();
        seed.push("DELETE FROM owed_dirty".into());
        db.recorded.borrow_mut().clear();
        let marker = potrefund_script(&victim, &game, &presigned_refund());
        let mut filing = verify_record_post(RecordKind::Potrefund, &marker, &id).unwrap();
        file_verified(&db, &mut filing, &id, 2_000).await.unwrap();
        let batches: Vec<_> = db.recorded.borrow().iter().map(|batch|batch.iter().map(|s|serde_json::json!({"sql":s.sql,"binds":s.binds.iter().map(|b|match b {Bind::Text(t)=>serde_json::json!(t),Bind::Int(n)=>serde_json::json!(n),Bind::Null=>serde_json::Value::Null}).collect::<Vec<_>>() })).collect::<Vec<_>>()).collect();
        let expected_rows: i64 = db.conn.query_row("SELECT COUNT(*) FROM owed_rows", [], |r| r.get(0)).unwrap();
        assert_eq!(expected_rows, 2);
        std::fs::write(
            path,
            serde_json::to_vec(
                &serde_json::json!({"schema":schema,"seed":seed,"batches":batches,"identity":id,"expectedRows":expected_rows}),
            )
            .unwrap(),
        )
        .unwrap();
    }

    /// The D2-OWED lens's MEDIUM-2 (this pin inverted): a judge that cannot run never refuses the filing. The filing
    /// lands ALONE with its trigger marks for the drain, the fault is counted, and the drain judges both seats later.
    #[tokio::test]
    async fn owed893_app_filing_atomic_on_row_fault() {
        let db = crate::pot_hook::tests::SqliteSql::migrated();
        let victim = wallet(0x41);
        let id = hex::encode(identity(&victim));
        let game = [0x7a; 32];
        md_index_the_pot(&db);
        md_seat(&db, &id, &game);
        db.conn.execute("INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt,sigValid) SELECT ?1,identity,gameId,potTxid,potVout,recoveryHeight,sigHex,'filed:other-seat',0,createdAt,sigValid FROM potparty_records WHERE identity=?2",rusqlite::params![hex::encode(identity(&wallet(0x42))),id]).unwrap();
        db.conn.execute_batch("DELETE FROM owed_dirty; CREATE TEMP TABLE observed_marks(identity TEXT); CREATE TEMP TRIGGER observe_dirty AFTER INSERT ON owed_dirty BEGIN INSERT INTO observed_marks(identity) VALUES (NEW.identity); END; CREATE TRIGGER fail_owed BEFORE INSERT ON owed_rows BEGIN SELECT RAISE(ABORT,'owed row fault'); END;").unwrap();
        let script = potrefund_script(&victim, &game, &presigned_refund());
        let mut filing = verify_record_post(RecordKind::Potrefund, &script, &id).unwrap();
        let result = file_verified(&db, &mut filing, &id, 2_000).await;
        assert!(
            matches!(result, Ok(DoorOutcome::Written { row_written: true })),
            "a judge fault lands the filing alone: {result:?}"
        );
        let count = |table: &str| -> i64 {
            db.conn
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
                .unwrap()
        };
        assert_eq!(count("potrefund_records"), 1, "the filing landed");
        assert_eq!(count("owed_rows"), 0, "nothing judged");
        assert_eq!(count("observed_marks"), 2, "both seats marked in the source transaction");
        assert_eq!(count("owed_dirty"), 2, "the marks stand for the drain");
        assert_eq!(db.counter(crate::owed_judge::COUNTER_FILING_FAULTS), 1);
        db.conn.execute_batch("DROP TRIGGER fail_owed;").unwrap();
        crate::owed_judge::drain_dirty(&db, 64).await.unwrap();
        assert_eq!(count("owed_rows"), 2, "the drain judged both seats");
        assert!(count("owed_row_words") > 0);
        assert_eq!(count("owed_dirty"), 0);
    }

    /// The D2-OWED lens's MEDIUM-2: no chain tip is no refusal of a filing (TIP-877's rule, as main judges): the filing
    /// is written and judged with its tip-dependent claims withheld, `probeTip` NULL for the next tip to re-mark.
    #[tokio::test]
    async fn d2fold_medium2_a_tipless_filing_is_written_and_judged() {
        let db = crate::pot_hook::tests::SqliteSql::migrated();
        let victim = wallet(0x41);
        let id = hex::encode(identity(&victim));
        let game = [0x7a; 32];
        md_index_the_pot(&db);
        md_seat(&db, &id, &game);
        db.conn.execute_batch("DELETE FROM owed_dirty;").unwrap();
        // an earlier judgement at a tip, then chaintracks goes dark
        crate::owed_judge::judge_subjects(&db, &id, &[hex::encode(game)], "test")
            .await
            .unwrap();
        db.tip.set(None);
        let script = potrefund_script(&victim, &game, &presigned_refund());
        let mut filing = verify_record_post(RecordKind::Potrefund, &script, &id).unwrap();
        let result = file_verified(&db, &mut filing, &id, 2_000).await;
        assert!(
            matches!(result, Ok(DoorOutcome::Written { row_written: true })),
            "{result:?}"
        );
        assert_eq!(db.counter(crate::owed_judge::COUNTER_FILING_FAULTS), 0, "judged, not faulted");
        let (claimable, probe): (Option<i64>, Option<i64>) = db
            .conn
            .query_row(
                "SELECT claimable,probeTip FROM owed_rows WHERE identity=?1",
                [&id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(probe, None, "judged with no tip");
        assert_ne!(claimable, Some(1), "no tip-dependent press");
    }

    /// The D2-OWED lens's MEDIUM-2: a refund filing's subjects are the filer FIRST, then the pot's verified seats.
    /// Byte-format party rows naming the pot (anyone's, sorting before the filer) are no subject: they can neither
    /// push the filer's judgement past the bound nor fault the victim's filing with their hydration.
    #[tokio::test]
    async fn d2fold_medium2_b_refund_subjects_are_the_filer_then_verified_seats() {
        let db = crate::pot_hook::tests::SqliteSql::migrated();
        let victim = wallet(0x41);
        let id = hex::encode(identity(&victim));
        let game = [0x7a; 32];
        md_index_the_pot(&db);
        md_seat(&db, &id, &game);
        for n in 0..20 {
            db.conn.execute("INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt,sigValid) VALUES (?1,'00',?2,?3,0,900000,'00',?4,0,1,0)", rusqlite::params![format!("00{n:064x}"), hex::encode([n as u8; 32]), hex::encode(POT_TXID), format!("planted:{n}")]).unwrap();
        }
        let subjects =
            crate::owed_judge::refund_subjects(&db, &hex::encode(POT_TXID), 0, &id, &hex::encode(game))
                .await
                .unwrap();
        assert_eq!(subjects[0].identity, id, "the filer first");
        assert_eq!(subjects.len(), 1, "no planted subject: {subjects:?}");
        db.conn.execute_batch("DELETE FROM owed_dirty; CREATE TRIGGER fail_planted BEFORE INSERT ON owed_rows WHEN NEW.identity LIKE '00%' BEGIN SELECT RAISE(ABORT,'planted'); END;").unwrap();
        let script = potrefund_script(&victim, &game, &presigned_refund());
        let mut filing = verify_record_post(RecordKind::Potrefund, &script, &id).unwrap();
        file_verified(&db, &mut filing, &id, 2_000).await.unwrap();
        assert_eq!(db.counter(crate::owed_judge::COUNTER_FILING_FAULTS), 0);
        let n: i64 = db
            .conn
            .query_row("SELECT COUNT(*) FROM owed_rows WHERE identity=?1", [&id], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1, "the filer's own subject judged in its filing batch");
    }

    #[test]
    fn owed893_writers_use_the_judge() {
        let runtime = include_str!("tracker_host/runtime.rs");
        let held = runtime
            .split("async fn event_held(")
            .nth(1)
            .unwrap()
            .split("pub async fn ingest(")
            .next()
            .unwrap();
        assert!(
            held.contains("judge_word_write"),
            "the word hook must judge its own marked subjects"
        );
        let tip = include_str!("internal_events.rs")
            .split("pub async fn tip_changed(")
            .nth(1)
            .unwrap()
            .split("#[cfg(test)]")
            .next()
            .unwrap();
        assert!(
            tip.contains("mark_tip"),
            "the tip must use the indexed gate marks"
        );
        assert!(!tip.contains("OWED_STALE_ON_TIP"));
    }
    use bsv_rs::primitives::bsv::sighash::{
        build_sighash_preimage, compute_sighash_for_signing, parse_transaction, SighashParams,
        SIGHASH_ALL, SIGHASH_FORKID,
    };
    use bsv_rs::primitives::PrivateKey;
    use bsv_rs::wallet::{Counterparty, CreateSignatureArgs, GetPublicKeyArgs, ProtoWallet};

    pub(crate) fn wallet(seed: u8) -> ProtoWallet {
        ProtoWallet::new(Some(
            PrivateKey::from_hex(&hex::encode([seed; 32])).unwrap(),
        ))
    }
    pub(crate) fn identity(w: &ProtoWallet) -> Vec<u8> {
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
    pub(crate) fn sign(
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
    pub(crate) fn script(pushes: &[&[u8]]) -> Vec<u8> {
        let mut out = vec![0x00, 0x6a];
        for p in pushes {
            push(&mut out, p);
        }
        out
    }
    /// A one-input tx spending `pot:vout` — the OLD test's junk shape (input 0
    /// names the pot and nothing else is true of it).
    pub(crate) fn junk_refund_raw(pot_txid: &[u8; 32], vout: u32) -> Vec<u8> {
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
    pub(crate) const POT_TXID: [u8; 32] = [0x55u8; 32];
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
    pub(crate) fn pot_ctx() -> PotContext {
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
        (
            compute_sighash_for_signing(&params),
            build_sighash_preimage(&params),
        )
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
    pub(crate) fn presigned_refund() -> Vec<u8> {
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
    pub(crate) fn potrefund_script(w: &ProtoWallet, gid: &[u8; 32], raw: &[u8]) -> Vec<u8> {
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
    fn the_hop_sweep_family_walks_the_same_door_the_kind_the_poster_and_the_cheap_spend_check() {
        // bsv-low #469 decision 3: the fifth family through the one ladder (the bar itself is `hopsweep::bind_hop_sweep`)
        assert_eq!(RecordKind::parse("hopsweep"), Some(RecordKind::Hopsweep));
        assert_eq!(RecordKind::Hopsweep.as_str(), "hopsweep");
        assert_eq!(RecordKind::ALL.len(), RECORD_KINDS);
        assert_eq!(filed_rows_cap(RecordKind::Hopsweep), 2);
        let w = wallet(0x31);
        let id = identity(&w);
        let hop = [0x51u8; 32];
        let raw = junk_refund_raw(&hop, 0); // input 0 spends hop:0 — the cheap check passes; the bind judges the bytes
        let s = script(&[
            b"LOW/hopsweep/v1",
            &id,
            &[0x11; 32],
            &hop,
            &0u32.to_le_bytes(),
            &raw,
        ]);
        match verify_record_post(RecordKind::Hopsweep, &s, &hex::encode(&id)) {
            Ok(VerifiedRecord::Hopsweep(m, r)) => {
                assert_eq!(m.hop_vout, 0);
                assert_eq!(r.hop_txid, hex::encode(hop));
                assert!(r.txid.starts_with("filed:"));
                assert_eq!(r.txid, crate::hopsweep::hopsweep_content_key(&m));
                assert_eq!(r.sweep_raw_hex, hex::encode(&raw));
            }
            other => panic!("expected a parsed sweep filing, got {other:?}"),
        }
        assert_eq!(
            verify_record_post(
                RecordKind::Hopsweep,
                &s,
                &hex::encode(identity(&wallet(0x32)))
            )
            .unwrap_err(),
            RecordRefusal::PosterMismatch
        );
        let other_vout = script(&[
            b"LOW/hopsweep/v1",
            &id,
            &[0x11; 32],
            &hop,
            &1u32.to_le_bytes(),
            &raw,
        ]);
        assert_eq!(
            verify_record_post(RecordKind::Hopsweep, &other_vout, &hex::encode(&id)).unwrap_err(),
            RecordRefusal::SweepDoesNotSpendTheHop
        );
        let five = script(&[b"LOW/hopsweep/v1", &id, &[0x11; 32], &hop, &raw]);
        assert_eq!(
            verify_record_post(RecordKind::Hopsweep, &five, &hex::encode(&id)).unwrap_err(),
            RecordRefusal::NotAMarker
        );
        // the ladder's shapes name the hop
        let v = verify_record_post(RecordKind::Hopsweep, &s, &hex::encode(&id)).unwrap();
        let (rows_sql, binds, day_sql, day_id) = cap_queries(&v);
        assert!(rows_sql.contains("hopsweep_records") && day_sql.contains("hopsweep_records"));
        assert_eq!(
            (binds.len(), binds[2].as_str(), day_id.as_str()),
            (4, hex::encode(hop).as_str(), hex::encode(&id).as_str())
        );
        let (chain_sql, chain_binds) = chain_rows_query(&v).unwrap();
        assert!(chain_sql.contains("hopsweep_records") && chain_binds.len() == 3);
        assert_eq!(
            cap_refusal(RecordKind::Hopsweep, 2, 0),
            Some(RecordRefusal::TooManyFiled)
        );
        assert_eq!(cap_refusal(RecordKind::Hopsweep, 1, 0), None);
        // the statuses: 425 for the two not-yet classes (the client's outbox re-files), 422 for the two judged ones
        assert_eq!(RecordRefusal::HopNotIndexed.status(), 425);
        assert_eq!(RecordRefusal::HopMarkerUnverified.status(), 425);
        assert_eq!(RecordRefusal::SweepDoesNotSpendTheHop.status(), 422);
        assert_eq!(RecordRefusal::SweepNotTheSeatsSpend.status(), 422);
        assert_eq!(record_health_json()["filedRowsCap"]["hopsweep"], 2);
        assert!(record_health_json()["filedByKind"]["hopsweep"].is_number());
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
        let key = match verify_record_post(RecordKind::Collected, &canonical, &id_lc).unwrap() {
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

    /// bsv-low #492 / #512: the held filing names the PAYING TRANSACTION, verifies under its identity at the door,
    /// is one row per (game, identity, txid) whatever the signature bytes, and is never the v1 marker (nor the v1
    /// marker it): the two challenges differ, so a signature over one retires nothing as the other.
    #[test]
    fn a_held_filing_names_its_paying_txid_and_files_under_its_signer_only() {
        let w = wallet(11);
        let id = identity(&w);
        let id_lc = hex::encode(&id);
        let gid = [0x55u8; 32];
        let gid_lc = hex::encode(gid);
        let pay = [0x66u8; 32];
        let pay_lc = hex::encode(pay);
        assert_eq!(
            held_challenge(&gid_lc, &id_lc, &pay_lc),
            format!("LOW-collected\nv2\ngid={gid_lc}\nid={id_lc}\ntxid={pay_lc}").into_bytes()
        );
        let sig = sign(
            &w,
            collected_protocol(),
            &gid_lc,
            &held_challenge(&gid_lc, &id_lc, &pay_lc),
        );
        let s = script(&[HELD_TAG, &gid, &id, &pay, &sig]);
        let v = verify_record_post(RecordKind::Collected, &s, &id_lc).unwrap();
        let VerifiedRecord::Held(r, named) = &v else {
            panic!("a held filing")
        };
        assert_eq!(named, &pay_lc);
        assert_eq!(
            (r.identity.as_str(), r.game_id.as_str()),
            (id_lc.as_str(), gid_lc.as_str())
        );
        assert_eq!(r.txid, held_content_key(&parse_held_marker(&s).unwrap()));
        assert!(r.txid.starts_with("filed:"));
        assert_eq!(v.kind(), RecordKind::Collected);
        // the chain never holds one: a chain v1 marker of the game must not swallow it as `alreadyIndexed`
        assert!(chain_rows_query(&v).is_none());
        // the caps (the B3 lens fold L1): the HELD rows of the (identity, game) window, counted apart from the v1
        // marker's, up to HELD_FILINGS_PER_GAME; the day is the family's, both shapes together
        let (rows_sql, binds, day_sql, _) = cap_queries(&v);
        assert_eq!(
            (rows_sql, day_sql),
            (HELD_FILED_ROWS_SQL, COLLECTED_FILED_TODAY_SQL)
        );
        assert!(
            HELD_FILED_ROWS_SQL.contains("payTxid IS NOT NULL")
                && COLLECTED_FILED_ROWS_SQL.contains("payTxid IS NULL")
        );
        assert_eq!(binds, vec![id_lc.clone(), gid_lc.clone(), r.txid.clone()]);
        assert_eq!(filed_rows_cap_of(&v), HELD_FILINGS_PER_GAME);
        assert_eq!(cap_refusal_for(&v, HELD_FILINGS_PER_GAME - 1, 0), None);
        assert_eq!(
            cap_refusal_for(&v, HELD_FILINGS_PER_GAME, 0),
            Some(RecordRefusal::TooManyFiled)
        );
        assert_eq!(
            cap_refusal_for(&v, 0, RECORD_FILINGS_PER_IDENTITY_PER_DAY),
            Some(RecordRefusal::DailyCapReached)
        );
        // the row's key is the content key of (game, identity, txid), and of nothing else (the owed walk's gate, M1)
        assert!(is_held_row_key(&r.txid, &gid_lc, &id_lc, &pay_lc));
        assert!(is_held_row_key(
            &r.txid,
            &gid_lc.to_ascii_uppercase(),
            &id_lc.to_ascii_uppercase(),
            &pay_lc.to_ascii_uppercase()
        ));
        assert!(!is_held_row_key(&r.txid, &gid_lc, &id_lc, &"67".repeat(32)));
        assert!(!is_held_row_key(&r.txid, &"56".repeat(32), &id_lc, &pay_lc));
        assert!(!is_held_row_key(
            &r.txid,
            &gid_lc,
            &hex::encode(identity(&wallet(12))),
            &pay_lc
        ));
        assert!(
            !is_held_row_key(&"ee".repeat(32), &gid_lc, &id_lc, &pay_lc),
            "a chain outpoint is no held key"
        );
        assert!(!is_held_row_key("filed:", &gid_lc, &id_lc, &pay_lc));
        assert!(
            !is_held_row_key(&r.txid, &gid_lc, &id_lc, "zz")
                && !is_held_row_key(&r.txid, &gid_lc[2..], &id_lc, &pay_lc)
        );
        // the stored signature verifies as a held filing of THAT txid only, and never as the v1 marker
        let sig_hex = r.sig_hex.clone().unwrap();
        assert!(held_sig_verifies(&id_lc, &gid_lc, &pay_lc, &sig_hex));
        assert!(!held_sig_verifies(
            &id_lc,
            &gid_lc,
            &"67".repeat(32),
            &sig_hex
        ));
        assert!(!held_sig_verifies(
            &id_lc,
            &"56".repeat(32),
            &pay_lc,
            &sig_hex
        ));
        assert!(!collected_sig_verifies(&id_lc, &gid_lc, &sig_hex));
        // and the v1 marker's signature is no held filing of anything
        let v1_sig = sign(
            &w,
            collected_protocol(),
            &gid_lc,
            &collected_challenge(&gid_lc, &id_lc),
        );
        assert!(!held_sig_verifies(
            &id_lc,
            &gid_lc,
            &pay_lc,
            &hex::encode(&v1_sig)
        ));
        assert_eq!(
            verify_record_post(
                RecordKind::Collected,
                &script(&[HELD_TAG, &gid, &id, &pay, &v1_sig]),
                &id_lc
            )
            .unwrap_err(),
            RecordRefusal::SignatureInvalid
        );
        // a re-signed copy is the same row; another txid is another row; another game another
        let again = sign(
            &w,
            collected_protocol(),
            &gid_lc,
            &held_challenge(&gid_lc, &id_lc, &pay_lc),
        );
        let m = parse_held_marker(&s).unwrap();
        assert_eq!(
            held_content_key(&HeldMarker {
                sig: again,
                ..m.clone()
            }),
            r.txid
        );
        assert_ne!(
            held_content_key(&HeldMarker {
                pay_txid: [0x67; 32],
                ..m.clone()
            }),
            r.txid
        );
        assert_ne!(
            held_content_key(&HeldMarker {
                game_id: [0x56; 32],
                ..m.clone()
            }),
            r.txid
        );
        // a stranger posting it, a signature over another txid, a stranger's signature: refused
        let stranger = wallet(12);
        let stranger_lc = hex::encode(identity(&stranger));
        assert_eq!(
            verify_record_post(RecordKind::Collected, &s, &stranger_lc).unwrap_err(),
            RecordRefusal::PosterMismatch
        );
        let other = [0x67u8; 32];
        assert_eq!(
            verify_record_post(
                RecordKind::Collected,
                &script(&[HELD_TAG, &gid, &id, &other, &sig]),
                &id_lc
            )
            .unwrap_err(),
            RecordRefusal::SignatureInvalid
        );
        let forged = sign(
            &stranger,
            collected_protocol(),
            &gid_lc,
            &held_challenge(&gid_lc, &id_lc, &pay_lc),
        );
        assert_eq!(
            verify_record_post(
                RecordKind::Collected,
                &script(&[HELD_TAG, &gid, &id, &pay, &forged]),
                &id_lc
            )
            .unwrap_err(),
            RecordRefusal::SignatureInvalid
        );
        // the shape is exact: a short txid, a sixth push, the v1 tag over five pushes are not markers of this kind
        for bad in [
            script(&[HELD_TAG, &gid, &id, &pay[..31], &sig]),
            script(&[HELD_TAG, &gid, &id, &pay, &sig, &[1u8]]),
            script(&[b"LOW/collected/v1", &gid, &id, &pay, &sig]),
        ] {
            assert!(parse_held_marker(&bad).is_none());
            assert_eq!(
                verify_record_post(RecordKind::Collected, &bad, &id_lc).unwrap_err(),
                RecordRefusal::NotAMarker
            );
        }
        // no other door takes it
        assert_eq!(
            verify_record_post(RecordKind::Potrefund, &s, &id_lc).unwrap_err(),
            RecordRefusal::NotAMarker
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
                &hex::encode(identity(&wallet(2)))
            )
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
            verify_and_bind(&potrefund_script(&w, &gid, &junk), &id_lc, Some(&ctx)).unwrap_err(),
            RecordRefusal::RefundNotThePresignedSpend
        );
        assert!(
            !verify_and_bind(&potrefund_script(&w, &gid, &junk), &id_lc, None)
                .unwrap()
                .1
        );
        // A spend one seat signed with the TOWER (a parked sibling / an
        // enforced settle): not the seats' refund.
        for (a, b) in [(&k[0], &k[2]), (&k[1], &k[2])] {
            let r = spend_signed_by(a, b, 0xffff_fffe, 900_000);
            assert_eq!(
                verify_and_bind(&potrefund_script(&w, &gid, &r), &id_lc, Some(&ctx)).unwrap_err(),
                RecordRefusal::RefundNotThePresignedSpend
            );
        }
        // Both seats, but FINAL (a cooperative settle's shape): refused.
        let settle = spend_signed_by(&k[0], &k[1], 0xffff_ffff, 0);
        assert_eq!(
            verify_and_bind(&potrefund_script(&w, &gid, &settle), &id_lc, Some(&ctx)).unwrap_err(),
            RecordRefusal::RefundNotThePresignedSpend
        );
        // Both seats, non-final, but below the committed recovery height.
        let early = spend_signed_by(&k[0], &k[1], 0xffff_fffe, 899_999);
        assert_eq!(
            verify_and_bind(&potrefund_script(&w, &gid, &early), &id_lc, Some(&ctx)).unwrap_err(),
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
                    assert_eq!(
                        r.txid,
                        result_content_key(&parse_result_marker(&s).unwrap())
                    );
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
        let s4 = script(&[
            b"LOW/result/v1",
            &gid,
            &wid,
            &lid,
            &pot,
            &settle,
            &wsig,
            &badl,
        ]);
        assert_eq!(
            verify_record_post(RecordKind::Result, &s4, &wid_lc).unwrap_err(),
            RecordRefusal::SignatureInvalid
        );
        // A third party is refused; a forged winner signature is refused.
        assert_eq!(
            verify_record_post(RecordKind::Result, &s, &hex::encode(identity(&wallet(9))))
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
        assert_eq!(
            filed_rows_cap(RecordKind::Collected),
            1,
            "the v1 marker's; the held filings are counted apart"
        );
        assert_eq!(HELD_FILINGS_PER_GAME, 4);
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
            cap_refusal(
                RecordKind::Collected,
                0,
                RECORD_FILINGS_PER_IDENTITY_PER_DAY
            ),
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
            pay_txid: None,
        };
        let (rows_sql, binds, day_sql, day_id) = cap_queries(&VerifiedRecord::Collected(r));
        assert_eq!(rows_sql, COLLECTED_FILED_ROWS_SQL);
        assert_eq!(
            binds,
            vec!["02aa".to_string(), "11".repeat(32), "filed:x".to_string()]
        );
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
        ))
        .unwrap();
        assert_eq!(csql, POTPARTY_CHAIN_ROWS_SQL);
        assert_eq!(
            cbinds,
            vec![
                "02aa".to_string(),
                "11".repeat(32),
                "22".repeat(32),
                "0".to_string()
            ]
        );
        for sql in [
            POTPARTY_CHAIN_ROWS_SQL,
            POTREFUND_CHAIN_ROWS_SQL,
            RESULT_CHAIN_ROWS_SQL,
            COLLECTED_CHAIN_ROWS_SQL,
        ] {
            assert!(
                sql.contains("txid NOT LIKE 'filed:%'"),
                "chain rows only: {sql}"
            );
        }
        assert!(POTREFUND_LATCH_SQL.contains("COALESCE(refundValid, 0) = 0"));
        for sql in [
            POTPARTY_FILED_ROWS_SQL,
            POTREFUND_FILED_ROWS_SQL,
            RESULT_FILED_ROWS_SQL,
            COLLECTED_FILED_ROWS_SQL,
            HELD_FILED_ROWS_SQL,
            POTPARTY_FILED_TODAY_SQL,
            POTREFUND_FILED_TODAY_SQL,
            RESULT_FILED_TODAY_SQL,
            COLLECTED_FILED_TODAY_SQL,
        ] {
            assert!(
                sql.contains("txid LIKE 'filed:%'"),
                "filed rows only: {sql}"
            );
        }
        assert_eq!(RecordRefusal::TooManyFiled.status(), 409);
        assert_eq!(RecordRefusal::DailyCapReached.status(), 429);
        assert_eq!(RecordRefusal::RefundNotThePresignedSpend.status(), 422);
        assert_eq!(RecordRefusal::BodyTooLarge.status(), 413);
        assert_eq!(RecordRefusal::NotCanonical.status(), 400);
        let h = record_health_json();
        assert_eq!(h["filedRowsCap"]["potrefund"], 1);
        assert_eq!(
            (
                &h["filedRowsCap"]["collected"],
                &h["filedRowsCap"]["collectedHeld"]
            ),
            (
                &serde_json::json!(1),
                &serde_json::json!(HELD_FILINGS_PER_GAME)
            )
        );
        assert!(h["alreadyFiledByKind"]["collected"].is_u64());
        assert_eq!(
            h["filingsPerIdentityPerDay"],
            RECORD_FILINGS_PER_IDENTITY_PER_DAY
        );
        assert!(h["anonymousFiledByKind"]["result"].is_u64());
    }
    /// The B3 delta lens D-N1: `collected_markers_v2.payTxid` has ONE writer, the door's insert, and that insert
    /// runs behind the signature check. Read over the production source of both workers (each file up to its
    /// test module, comment lines dropped): one INSERT into the table names the column, it is
    /// [`COLLECTED_FILE_SQL`], it is prepared once, after `verify_record_post`, and no statement updates,
    /// replaces or deletes a row of the table. A statement whose table name is built at run time is out of
    /// this pin's sight; none exists today.
    #[test]
    fn i492_dn1_the_door_is_the_only_writer_of_pay_txid() {
        fn sources(dir: &std::path::Path, out: &mut Vec<(String, String)>) {
            for e in std::fs::read_dir(dir).unwrap() {
                let path = e.unwrap().path();
                if path.is_dir() {
                    sources(&path, out);
                } else if path.extension().is_some_and(|x| x == "rs") {
                    let text = std::fs::read_to_string(&path).unwrap();
                    let production = text
                        .split("\nmod tests {")
                        .next()
                        .unwrap()
                        .split("\npub(crate) mod tests {")
                        .next()
                        .unwrap();
                    let code: Vec<&str> = production
                        .lines()
                        .filter(|l| !l.trim_start().starts_with("//"))
                        .collect();
                    // one line of text: a statement split over source lines (a `\` continuation) reads whole
                    let flat = code
                        .join(" ")
                        .replace('\\', " ")
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" ");
                    out.push((
                        path.file_name().unwrap().to_string_lossy().into_owned(),
                        flat,
                    ));
                }
            }
        }
        let here = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut files = Vec::new();
        sources(&here.join("src"), &mut files);
        sources(&here.join("../overlay-cloudflare/src"), &mut files);
        assert!(
            files.len() > 40,
            "both workers' sources were read: {}",
            files.len()
        );
        let table = "collected_markers_v2";
        let (mut inserts, mut naming_pay_txid) = (0usize, Vec::new());
        for (file, flat) in &files {
            let upper = flat.to_ascii_uppercase();
            for verb in [
                "UPDATE ",
                "DELETE FROM ",
                "REPLACE INTO ",
                "INSERT OR REPLACE INTO ",
            ] {
                assert!(
                    !upper.contains(&format!("{verb}{}", table.to_ascii_uppercase())),
                    "{file}: `{verb}{table}`: rows are never rewritten"
                );
            }
            let into = format!("INTO {table}");
            for (at, _) in flat.match_indices(&into) {
                inserts += 1;
                // the statement: up to the end of its string literal
                let stmt = flat[at..].split('"').next().unwrap();
                if stmt.contains("payTxid") {
                    naming_pay_txid.push(file.clone());
                }
            }
        }
        assert!(inserts >= 3, "the door's insert, the overlay's `store_record` and the carry were all seen: {inserts}");
        assert_eq!(
            naming_pay_txid,
            vec!["record_post.rs".to_string()],
            "one insert names the column, the door's"
        );
        assert!(
            COLLECTED_FILE_SQL.contains("INTO collected_markers_v2")
                && COLLECTED_FILE_SQL.contains("payTxid")
        );
        let door = &files.iter().find(|(f, _)| f == "record_post.rs").unwrap().1;
        assert_eq!(
            door.matches("COLLECTED_FILE_SQL").count(),
            1,
            "one SQL definition"
        );
        let builder = &files.iter().find(|(f, _)| f == "owed_write.rs").unwrap().1;
        assert_eq!(
            builder.matches("COLLECTED_FILE_SQL").count(),
            1,
            "one source statement builder"
        );
        assert!(door.contains("owed_write::judge_filings"));
        let route = door.find("pub async fn record_post(").expect("the door");
        let verify = door[route..]
            .find("verify_record_post(kind, &script, &identity)")
            .expect("the door verifies")
            + route;
        let call = door[route..]
            .find("file_verified(&port, &mut verified, &identity, now)")
            .expect("the route calls the core")
            + route;
        assert!(
            route < verify && verify < call,
            "the signature check runs before the core and its one insert"
        );
        assert_eq!(
            door.matches("file_verified(&port").count(),
            1,
            "the route is the core's only caller"
        );
        assert_eq!(
            door.matches("pub async fn ").count(),
            1,
            "the route is the file's only async door, so the insert is inside it"
        );
    }

    // ── W25C: the chain no-op (bsv-low #564, #565), over the SHIPPED statements under real SQLite ──

    /// A fresh SQLite carrying the production schema (the overlay's migrations, as `tests/sql_prepares_sqlite.rs`).
    pub(crate) fn w25c_db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        for sql in bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                assert!(
                    e.to_string()
                        .to_ascii_lowercase()
                        .contains("duplicate column"),
                    "{e}\n{sql}"
                );
            }
        }
        conn
    }

    /// The door's step 6 as the route runs it: the shipped statement with the shipped binds, each bound as TEXT (a
    /// JS string binds as TEXT on D1, a `&str` as TEXT under rusqlite), then the collected rows' signature check.
    fn w25c_door_chain_rows(conn: &rusqlite::Connection, v: &VerifiedRecord) -> i64 {
        let (sql, binds) = chain_rows_query(v).unwrap();
        let mut stmt = conn.prepare(sql).unwrap();
        match v {
            VerifiedRecord::Collected(r) => {
                let sigs: Vec<Option<String>> = stmt
                    .query_map(rusqlite::params_from_iter(binds.iter()), |row| row.get(0))
                    .unwrap()
                    .map(|x| x.unwrap())
                    .collect();
                own_collected_chain_rows(&r.identity, &r.game_id, &sigs)
            }
            _ => stmt
                .query_row(rusqlite::params_from_iter(binds.iter()), |row| row.get(0))
                .unwrap(),
        }
    }

    /// The door's write when step 6 answers no no-op (the caps are under: one row each here).
    fn w25c_file(conn: &rusqlite::Connection, v: &VerifiedRecord) {
        match v {
            VerifiedRecord::Potparty(r, valid) => {
                conn.execute(
                    POTPARTY_FILE_SQL,
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
                        1_000,
                        i64::from(*valid)
                    ],
                )
                .unwrap();
            }
            VerifiedRecord::Result(r, tier) => {
                conn.execute(
                    RESULT_FILE_SQL,
                    rusqlite::params![
                        r.game_id,
                        r.winner,
                        r.loser,
                        r.pot_txid,
                        r.settle_txid,
                        r.winner_sig_hex,
                        r.loser_sig_hex,
                        r.cards_hex,
                        r.txid,
                        0,
                        1_000,
                        tier
                    ],
                )
                .unwrap();
            }
            VerifiedRecord::Collected(r) => {
                conn.execute(
                    COLLECTED_FILE_SQL,
                    rusqlite::params![
                        r.identity, r.game_id, r.txid, 0, r.sig_hex, 1_000, r.pay_txid
                    ],
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
    }

    /// One filing through step 6: `true` = the no-op `alreadyIndexed`, `false` = the filing wrote.
    fn w25c_filing(conn: &rusqlite::Connection, v: &VerifiedRecord) -> bool {
        if w25c_door_chain_rows(conn, v) > 0 {
            return true;
        }
        w25c_file(conn, v);
        false
    }

    fn w25c_count(conn: &rusqlite::Connection, sql: &str) -> i64 {
        conn.query_row(sql, [], |r| r.get(0)).unwrap()
    }

    fn w25c_potparty(seat_settle: Option<&str>, txid: &str) -> PotpartyRecord {
        PotpartyRecord {
            identity: "02aa".into(),
            opponent_identity: "02bb".into(),
            game_id: "11".repeat(32),
            pot_txid: "22".repeat(32),
            pot_vout: 0,
            recovery_height: 900_000,
            sig_hex: "3044".into(),
            seat_settle_pubkey: seat_settle.map(Into::into),
            seat_sig_hex: seat_settle.map(|_| "3045".into()),
            txid: txid.into(),
            output_index: 0,
            created_at: 0,
        }
    }

    fn w25c_result(loser_sig: Option<&str>, txid: &str) -> ResultRecord {
        ResultRecord {
            game_id: "33".repeat(32),
            winner: "02aa".into(),
            loser: "02bb".into(),
            pot_txid: "44".repeat(32),
            settle_txid: "55".repeat(32),
            winner_sig_hex: "3044".into(),
            loser_sig_hex: loser_sig.map(Into::into),
            cards_hex: None,
            txid: txid.into(),
            output_index: 0,
            created_at: 0,
        }
    }

    /// #565: a potparty or result marker the CHAIN holds (latched valid by the overlay), filed again through the
    /// door, is the no-op `alreadyIndexed`; before the fix the version flag bound as TEXT '1' never equalled the
    /// INTEGER `(x IS NULL)`, so the filing WROTE a second (filed) row beside the chain row.
    #[test]
    fn w25c_565_a_chain_marker_filed_again_is_the_no_op_under_real_sqlite() {
        let conn = w25c_db();
        // potparty v1: the chain row, latched sigValid = 1 (the overlay's admission latch)
        let chain = VerifiedRecord::Potparty(w25c_potparty(None, &"cc".repeat(32)), true);
        w25c_file(&conn, &chain);
        let filed = VerifiedRecord::Potparty(w25c_potparty(None, "filed:pp1"), true);
        assert_eq!(
            w25c_door_chain_rows(&conn, &filed),
            1,
            "the v1 chain row is found"
        );
        assert!(w25c_filing(&conn, &filed), "the second filing is the no-op");
        assert_eq!(
            w25c_count(&conn, "SELECT COUNT(*) FROM potparty_records"),
            1,
            "no filed row beside the chain row"
        );
        // the version still parts them: a v2 filing over a v1 chain row is not a no-op
        let filed_v2 = VerifiedRecord::Potparty(w25c_potparty(Some("02cc"), "filed:pp2"), true);
        assert!(
            !w25c_filing(&conn, &filed_v2),
            "a v2 filing over a v1 chain row writes"
        );
        // ... and a v2 chain row makes a v2 filing the no-op
        let conn = w25c_db();
        w25c_file(
            &conn,
            &VerifiedRecord::Potparty(w25c_potparty(Some("02cc"), &"cd".repeat(32)), true),
        );
        assert!(w25c_filing(&conn, &filed_v2), "v2 over v2: the no-op");
        assert!(!w25c_filing(&conn, &filed), "v1 over v2: writes");

        // result, the plain claim: a chain row of tier 1
        let conn = w25c_db();
        w25c_file(
            &conn,
            &VerifiedRecord::Result(w25c_result(None, &"dd".repeat(32)), 1),
        );
        let filed = VerifiedRecord::Result(w25c_result(None, "filed:r1"), 1);
        assert!(
            w25c_filing(&conn, &filed),
            "the plain claim filed again is the no-op"
        );
        assert_eq!(
            w25c_count(&conn, "SELECT COUNT(*) FROM result_markers_v2"),
            1
        );
        // the countersigned claim over a countersigned chain row of tier 2
        let conn = w25c_db();
        w25c_file(
            &conn,
            &VerifiedRecord::Result(w25c_result(Some("3045"), &"de".repeat(32)), 2),
        );
        let filed2 = VerifiedRecord::Result(w25c_result(Some("3045"), "filed:r2"), 2);
        assert!(
            w25c_filing(&conn, &filed2),
            "countersigned filed again: the no-op"
        );
        assert!(
            !w25c_filing(&conn, &filed),
            "a plain claim over a countersigned chain row writes"
        );
    }

    /// #564 and #565's own planted case: a chain row that does NOT verify for the identity it names (a stranger
    /// bought it) is no no-op; the honest filing writes. The identity's own verified chain row still is the no-op.
    #[test]
    fn w25c_564_a_planted_chain_row_never_answers_already_indexed() {
        // collected v1: the victim's real marker vs a stranger's chain row naming the victim
        let victim = wallet(21);
        let id = identity(&victim);
        let id_lc = hex::encode(&id);
        let gid = [0x5au8; 32];
        let gid_lc = hex::encode(gid);
        let challenge = collected_challenge(&gid_lc, &id_lc);
        let own_sig = sign(&victim, collected_protocol(), &gid_lc, &challenge);
        let stranger_sig = sign(&wallet(22), collected_protocol(), &gid_lc, &challenge);
        let filing = verify_record_post(
            RecordKind::Collected,
            &script(&[b"LOW/collected/v1", &gid, &id, &own_sig]),
            &id_lc,
        )
        .unwrap();
        let chain_row = |txid: &str, sig: &[u8]| {
            VerifiedRecord::Collected(CollectedRecord {
                identity: id_lc.clone(),
                game_id: gid_lc.clone(),
                txid: txid.into(),
                output_index: 0,
                sig_hex: Some(hex::encode(sig)),
                pay_txid: None,
            })
        };
        let conn = w25c_db();
        w25c_file(&conn, &chain_row(&"e1".repeat(32), &stranger_sig)); // a foreign key's signature
        w25c_file(&conn, &chain_row(&"e2".repeat(32), &[0x30, 0x44, 0x01])); // junk
        assert_eq!(
            w25c_door_chain_rows(&conn, &filing),
            0,
            "no planted row is the victim's own"
        );
        assert!(
            !w25c_filing(&conn, &filing),
            "the honest v1 collected filing writes"
        );
        assert_eq!(
            w25c_count(
                &conn,
                "SELECT COUNT(*) FROM collected_markers_v2 WHERE txid LIKE 'filed:%'"
            ),
            1,
            "the victim's filed row is written beside the planted ones"
        );
        // the identity's own chain row: the no-op (upper-case hex too, as a chain row may be stored)
        let conn = w25c_db();
        w25c_file(&conn, &chain_row(&"e1".repeat(32), &stranger_sig));
        w25c_file(&conn, &chain_row(&"e3".repeat(32), &own_sig));
        assert!(
            w25c_filing(&conn, &filing),
            "the identity's own chain row: alreadyIndexed"
        );
        assert_eq!(
            own_collected_chain_rows(
                &id_lc,
                &gid_lc,
                &[Some(hex::encode(&own_sig).to_ascii_uppercase())]
            ),
            1
        );
        // past the read bound the filing proceeds: never a no-op on rows not checked
        let junk = vec![Some(hex::encode(&stranger_sig)); COLLECTED_CHAIN_SIGS_READ];
        let mut past = junk.clone();
        past.push(Some(hex::encode(&own_sig)));
        assert_eq!(own_collected_chain_rows(&id_lc, &gid_lc, &past), 0);
        assert!(COLLECTED_CHAIN_ROWS_SQL.ends_with(&format!("LIMIT {COLLECTED_CHAIN_SIGS_READ}")));

        // potparty: a chain row the overlay latched invalid (0) or has not latched (NULL) is no no-op
        for latch in [Some(0i64), None] {
            let conn = w25c_db();
            let planted = w25c_potparty(None, &"f1".repeat(32));
            conn.execute(
                POTPARTY_FILE_SQL,
                rusqlite::params![
                    planted.identity,
                    planted.opponent_identity,
                    planted.game_id,
                    planted.pot_txid,
                    0,
                    900_000,
                    "3044",
                    Option::<String>::None,
                    Option::<String>::None,
                    planted.txid,
                    0,
                    1_000,
                    latch
                ],
            )
            .unwrap();
            let filed = VerifiedRecord::Potparty(w25c_potparty(None, "filed:pp"), true);
            assert!(
                !w25c_filing(&conn, &filed),
                "sigValid {latch:?}: the honest filing writes"
            );
        }
        // result: a tier-0 chain row is no no-op, and a countersigned-shaped chain row whose countersig did not
        // verify (tier 1) does not swallow the honest countersigned filing
        let conn = w25c_db();
        w25c_file(
            &conn,
            &VerifiedRecord::Result(w25c_result(None, &"f2".repeat(32)), 0),
        );
        assert!(!w25c_filing(
            &conn,
            &VerifiedRecord::Result(w25c_result(None, "filed:r"), 1)
        ));
        let conn = w25c_db();
        w25c_file(
            &conn,
            &VerifiedRecord::Result(w25c_result(Some("3045"), &"f3".repeat(32)), 1),
        );
        assert!(!w25c_filing(
            &conn,
            &VerifiedRecord::Result(w25c_result(Some("3045"), "filed:r2"), 2)
        ));
    }

    // ── MONEY-DOOR (bsv-low #798, #663): real filings through the SHIPPED door core over real SQLite ──

    /// The overlay's own chain writer for `potrefund_records` (`d1_discovery.rs`, `store_record`), verbatim.
    const OVERLAY_POTREFUND_STORE_SQL: &str = "INSERT OR IGNORE INTO potrefund_records \
             (identity, gameId, potTxid, potVout, refundRawHex, \
              sigHex, txid, outputIndex, createdAt) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    /// A statement's words, whether read from a literal or from its Rust source (line continuations dropped).
    fn md_flat(s: &str) -> String {
        s.replace('\\', " ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// The fixture's covenant pot, held by the index as the overlay decodes it (`paramsDecoded = 1`).
    pub(crate) fn md_index_the_pot(db: &crate::pot_hook::tests::SqliteSql) {
        let p = pot_params();
        db.conn
            .execute(
                "INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind, paramsDecoded, pubA, pubB, \
                 pubTower, payPkhA, payPkhB, rakePkh, stakeA, stakeB, feeSats, recoveryHeight, potSats) \
                 VALUES (?1, 0, 0, 1, 'covenant', 1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                rusqlite::params![
                    hex::encode(POT_TXID),
                    hex::encode(p.pub_a),
                    hex::encode(p.pub_b),
                    hex::encode(p.pub_tower),
                    hex::encode(p.pay_pkh_a),
                    hex::encode(p.pay_pkh_b),
                    hex::encode(p.rake_pkh),
                    p.stake_a as i64,
                    p.stake_b as i64,
                    p.fee_sats as i64,
                    p.recovery_height as i64,
                    POT_SATS as i64
                ],
            )
            .unwrap();
    }

    /// The victim's seat on the pot (the party window `/refund-backups` is driven from).
    pub(crate) fn md_seat(db: &crate::pot_hook::tests::SqliteSql, id_lc: &str, gid: &[u8; 32]) {
        db.conn
            .execute(
                POTPARTY_FILE_SQL,
                rusqlite::params![
                    id_lc,
                    "02".to_string() + &"bb".repeat(32),
                    hex::encode(gid),
                    hex::encode(POT_TXID),
                    0,
                    900_000,
                    "3044",
                    Option::<String>::None,
                    Option::<String>::None,
                    "filed:seat",
                    0,
                    1_000,
                    1
                ],
            )
            .unwrap();
    }

    /// A chain marker admitted the way the overlay admits one: `tm_potrefund`'s byte-format check, then the
    /// overlay's own `store_record` statement, at `created_at`.
    fn md_chain_admit(
        db: &crate::pot_hook::tests::SqliteSql,
        script: &[u8],
        txid: &str,
        created_at: i64,
    ) {
        let out = bsv_rs::transaction::TransactionOutput {
            satoshis: Some(0),
            locking_script: bsv_rs::script::LockingScript::from_binary(script).unwrap(),
            change: false,
        };
        assert!(
            overlay_discovery::potrefund::topic_manager::PotrefundTopicManager::validate_potrefund_output(&out),
            "tm_potrefund admits the marker by its bytes"
        );
        let m = parse_potrefund_marker(script).unwrap();
        db.conn
            .execute(
                OVERLAY_POTREFUND_STORE_SQL,
                rusqlite::params![
                    hex::encode(&m.identity),
                    hex::encode(m.game_id),
                    hex::encode(m.pot_txid),
                    m.pot_vout,
                    hex::encode(&m.refund_raw),
                    hex::encode(&m.sig),
                    txid,
                    0,
                    created_at
                ],
            )
            .unwrap();
    }

    /// A stranger's `LOW/potrefund`-shaped marker naming the victim's identity, game and pot: a junk signature
    /// (DER-ranged, never the victim's) over a junk refund raw.
    fn md_junk_marker(victim_id: &[u8], gid: &[u8; 32]) -> Vec<u8> {
        let mut junk_sig = vec![0x30, 0x44, 0x02, 0x20];
        junk_sig.extend_from_slice(&[0x11; 32]);
        junk_sig.extend_from_slice(&[0x02, 0x20]);
        junk_sig.extend_from_slice(&[0x22; 32]);
        script(&[
            b"LOW/potrefund/v1",
            victim_id,
            gid,
            &POT_TXID,
            &0u32.to_le_bytes(),
            &junk_refund_raw(&POT_TXID, 0),
            &junk_sig,
        ])
    }

    async fn md_file(
        db: &crate::pot_hook::tests::SqliteSql,
        kind: RecordKind,
        script: &[u8],
        poster: &str,
    ) -> (DoorOutcome, VerifiedRecord) {
        let mut v = verify_record_post(kind, script, poster).expect("the marker verifies");
        let out = file_verified(db, &mut v, poster, 2_000).await.unwrap();
        (out, v)
    }

    fn md_refund_rows(db: &crate::pot_hook::tests::SqliteSql) -> Vec<(String, Option<i64>)> {
        let mut q = db
            .conn
            .prepare("SELECT txid, refundValid FROM potrefund_records ORDER BY rowid")
            .unwrap();
        q.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|x| x.unwrap())
            .collect()
    }

    /// `/refund-backups`' shipped read for the identity: (txid, refundValid) in its served order.
    fn md_served(
        db: &crate::pot_hook::tests::SqliteSql,
        id_lc: &str,
    ) -> Vec<(String, Option<i64>)> {
        let mut q = db
            .conn
            .prepare(&crate::refund_backups::refund_backups_sql(None))
            .unwrap();
        q.query_map([id_lc], |r| {
            Ok((
                r.get::<_, String>("txid")?,
                r.get::<_, Option<i64>>("refundValid")?,
            ))
        })
        .unwrap()
        .map(|x| x.unwrap())
        .collect()
    }

    fn md_unverified_seen() -> u64 {
        record_health_json()["potrefundChainRowsUnverified"]
            .as_u64()
            .unwrap_or(0)
    }

    /// #798: a stranger's junk-signed `LOW/potrefund` chain marker naming the victim never makes the victim's own
    /// VALID refund backup a no-op: the valid filing is written, latched `refundValid = 1` and served first by
    /// `/refund-backups`; the planted row is counted.
    #[tokio::test(flavor = "current_thread")]
    async fn money_door_798_a_junk_chain_marker_never_swallows_the_victims_valid_backup() {
        assert!(
            md_flat(include_str!("../../overlay-cloudflare/src/d1_discovery.rs"))
                .contains(&md_flat(OVERLAY_POTREFUND_STORE_SQL)),
            "the planted row is written by the overlay's own chain statement"
        );
        let db = crate::pot_hook::tests::SqliteSql::migrated();
        let victim = wallet(0x41);
        let id = identity(&victim);
        let id_lc = hex::encode(&id);
        let gid = [0x7au8; 32];
        md_index_the_pot(&db);
        md_seat(&db, &id_lc, &gid);
        let planted_txid = "a1".repeat(32);
        md_chain_admit(&db, &md_junk_marker(&id, &gid), &planted_txid, 5_000);
        let seen_before = md_unverified_seen();
        let filing = potrefund_script(&victim, &gid, &presigned_refund());
        let (out, v) = md_file(&db, RecordKind::Potrefund, &filing, &id_lc).await;
        assert_eq!(
            (out, matches!(v, VerifiedRecord::Potrefund(_, true))),
            (DoorOutcome::Written { row_written: true }, true),
            "the victim's valid backup is written at rank 1, never the chain no-op"
        );
        let rows = md_refund_rows(&db);
        assert_eq!(
            rows,
            vec![(planted_txid.clone(), None), (v.key().to_string(), Some(1))],
            "the filed row latched refundValid = 1 beside the planted row"
        );
        let served = md_served(&db, &id_lc);
        assert_eq!(
            served.first(),
            Some(&(v.key().to_string(), Some(1))),
            "/refund-backups serves the victim's valid backup first: {served:?}"
        );
        assert!(
            md_unverified_seen() > seen_before,
            "the planted row is counted"
        );
        // the cap counts honest need only: a second valid content of the same (poster, game, pot) is refused,
        // the planted chain row never counted
        let k = pot_keys();
        let other = spend_signed_by(&k[0], &k[1], 0xffff_fffd, 900_000);
        let (out, _) = md_file(
            &db,
            RecordKind::Potrefund,
            &potrefund_script(&victim, &gid, &other),
            &id_lc,
        )
        .await;
        assert_eq!(out, DoorOutcome::Refused(RecordRefusal::TooManyFiled));
    }

    /// #798's controls: the identity's own VALID chain row is still the no-op (and is latched before the answer),
    /// a second filing of the same content is the idempotent write, a valid-signed chain row whose raw is not the
    /// pot's pre-signed spend is no no-op, and a rank-0 filing latches when the pot lands.
    #[tokio::test(flavor = "current_thread")]
    async fn money_door_798_b_a_valid_existing_row_is_still_the_no_op() {
        let victim = wallet(0x42);
        let id = identity(&victim);
        let id_lc = hex::encode(&id);
        let gid = [0x7bu8; 32];
        let filing = potrefund_script(&victim, &gid, &presigned_refund());

        // the identity's own chain marker (the same bytes, bought on chain), the pot indexed
        let db = crate::pot_hook::tests::SqliteSql::migrated();
        md_index_the_pot(&db);
        md_seat(&db, &id_lc, &gid);
        let own_txid = "b1".repeat(32);
        md_chain_admit(&db, &md_junk_marker(&id, &gid), &"b0".repeat(32), 6_000);
        md_chain_admit(&db, &filing, &own_txid, 1_500);
        let (out, _) = md_file(&db, RecordKind::Potrefund, &filing, &id_lc).await;
        assert_eq!(
            out,
            DoorOutcome::AlreadyIndexed { latched: true },
            "the valid chain row is the no-op"
        );
        assert_eq!(
            md_refund_rows(&db),
            vec![("b0".repeat(32), None), (own_txid.clone(), Some(1))],
            "no filed row beside it; the verified chain row is latched before the answer"
        );
        assert_eq!(
            md_served(&db, &id_lc).first(),
            Some(&(own_txid.clone(), Some(1))),
            "the latched chain row is served first"
        );

        // the same content filed twice: one row, rank 1 (idempotent by content)
        let db = crate::pot_hook::tests::SqliteSql::migrated();
        md_index_the_pot(&db);
        let (first, v) = md_file(&db, RecordKind::Potrefund, &filing, &id_lc).await;
        let (second, _) = md_file(&db, RecordKind::Potrefund, &filing, &id_lc).await;
        assert_eq!(first, DoorOutcome::Written { row_written: true });
        assert_eq!(second, DoorOutcome::Written { row_written: true });
        assert_eq!(md_refund_rows(&db), vec![(v.key().to_string(), Some(1))]);

        // a chain row the victim signed whose raw is NOT the pot's pre-signed spend: no no-op
        let db = crate::pot_hook::tests::SqliteSql::migrated();
        md_index_the_pot(&db);
        md_chain_admit(
            &db,
            &potrefund_script(&victim, &gid, &junk_refund_raw(&POT_TXID, 0)),
            &"c1".repeat(32),
            1_500,
        );
        let (out, v) = md_file(&db, RecordKind::Potrefund, &filing, &id_lc).await;
        assert_eq!(out, DoorOutcome::Written { row_written: true });
        assert_eq!(
            md_refund_rows(&db),
            vec![("c1".repeat(32), None), (v.key().to_string(), Some(1))]
        );

        // the pot not indexed: the filing is rank 0 (a junk row beside it no no-op either), then latched by the
        // re-file once the pot lands
        let db = crate::pot_hook::tests::SqliteSql::migrated();
        md_chain_admit(&db, &md_junk_marker(&id, &gid), &"d1".repeat(32), 1_500);
        let (out, v) = md_file(&db, RecordKind::Potrefund, &filing, &id_lc).await;
        assert_eq!(out, DoorOutcome::Written { row_written: true });
        assert_eq!(
            md_refund_rows(&db),
            vec![("d1".repeat(32), None), (v.key().to_string(), Some(0))]
        );
        md_index_the_pot(&db);
        let (out, _) = md_file(&db, RecordKind::Potrefund, &filing, &id_lc).await;
        assert_eq!(out, DoorOutcome::Written { row_written: true });
        assert_eq!(
            md_refund_rows(&db),
            vec![("d1".repeat(32), None), (v.key().to_string(), Some(1))]
        );
        // the handfile arm takes the same chain decision (no presence-only count of its own)
        let handfile: String = include_str!("handfile.rs").split_whitespace().collect();
        assert!(handfile.contains(
            "crate::record_post::potrefund_chain_noop(&port,&verified.potrefund,pot.as_ref())"
        ));
        assert!(!handfile.contains("(HandfileSlot::Potrefund,&refund)"));
    }

    /// The hop's marker row as the index holds it (the shipped hopparty write), `markerValid` as given.
    pub(crate) fn md_index_the_hop(
        db: &crate::pot_hook::tests::SqliteSql,
        hop: &crate::hopsweep::HopContext,
        marker_valid: Option<i64>,
    ) {
        db.conn
            .execute(
                crate::handfile::HOPPARTY_HANDFILE_FILE_SQL,
                rusqlite::params![
                    hop.identity,
                    hop.opponent_identity,
                    hop.game_id,
                    hop.hop_vout,
                    hop.hop_sats as i64,
                    hop.seat_settle_pubkey,
                    "3044",
                    hop.identity_sig_hex,
                    hop.hop_lock_hex,
                    hop.hop_sats_on_chain.map(|v| v as i64),
                    2,
                    hex::encode(crate::hopsweep::tests::HOP),
                    1,
                    1_000,
                    marker_valid
                ],
            )
            .unwrap();
    }

    pub(crate) fn md_sweep_script(id: &[u8], gid: &[u8; 32], raw: &[u8]) -> Vec<u8> {
        crate::hopsweep::tests::script(&[
            crate::hopsweep::HOPSWEEP_TAG,
            id,
            gid,
            &crate::hopsweep::tests::HOP,
            &0u32.to_le_bytes(),
            raw,
        ])
    }

    /// #663: a hop sweep filing's gameId is compared to the hop's own row in EVERY arm (the overlay's latched
    /// `markerValid` and the replay alike): a verified sweep filed under another game is refused by name, never
    /// written (it was written, so the per-(poster, game, hop) cap of 2 was bought again under every game id).
    #[tokio::test(flavor = "current_thread")]
    async fn money_door_663_a_sweep_filed_under_another_game_is_refused_by_name() {
        use crate::hopsweep::tests as hs;
        let w = hs::wallet(0x43);
        let id = hs::identity(&w);
        let id_lc = hex::encode(&id);
        let seat = hs::skey(12);
        let raw = hs::sweep_raw(&seat, &seat, 0, hs::HOP_SATS, &hs::DEST);
        // three of the seat's sweeps of the one hop, to three homes (three contents)
        let (mut dest2, mut dest3) = (hs::DEST, hs::DEST);
        dest2[3] = 0x12;
        dest3[3] = 0x13;
        let other = hs::sweep_raw(&seat, &seat, 0, hs::HOP_SATS, &dest2);
        let third = hs::sweep_raw(&seat, &seat, 0, hs::HOP_SATS, &dest3);
        let wrong_game = [0x99u8; 32];
        for latch in [Some(1i64), None] {
            let db = crate::pot_hook::tests::SqliteSql::migrated();
            md_index_the_hop(&db, &hs::hop_ctx(&w, &seat, true), latch);
            let (out, _) = md_file(
                &db,
                RecordKind::Hopsweep,
                &md_sweep_script(&id, &wrong_game, &raw),
                &id_lc,
            )
            .await;
            assert_eq!(
                format!("{out:?}"),
                "Refused(SweepGameMismatch)",
                "markerValid {latch:?}: the wrong game is refused by name"
            );
            let n: i64 = db
                .conn
                .query_row("SELECT COUNT(*) FROM hopsweep_records", [], |r| r.get(0))
                .unwrap();
            assert_eq!(n, 0, "markerValid {latch:?}: nothing written");
            // the honest control: the hop's own game files, twice (its cap), and the third is the cap
            for (k, r) in [&raw, &other].into_iter().enumerate() {
                let (out, _) = md_file(
                    &db,
                    RecordKind::Hopsweep,
                    &md_sweep_script(&id, &hs::GID, r),
                    &id_lc,
                )
                .await;
                assert_eq!(
                    out,
                    DoorOutcome::Written { row_written: true },
                    "{latch:?} {k}"
                );
            }
            let (out, _) = md_file(
                &db,
                RecordKind::Hopsweep,
                &md_sweep_script(&id, &hs::GID, &third),
                &id_lc,
            )
            .await;
            assert_eq!(out, DoorOutcome::Refused(RecordRefusal::TooManyFiled));
            // ... and no other game id buys the cap again
            let (out, _) = md_file(
                &db,
                RecordKind::Hopsweep,
                &md_sweep_script(&id, &wrong_game, &third),
                &id_lc,
            )
            .await;
            assert_eq!(
                format!("{out:?}"),
                "Refused(SweepGameMismatch)",
                "markerValid {latch:?}: past the cap under another game"
            );
        }
    }

    /// The signed sweep goes through the same parser, hop lookup, binding and reply preparation as the route.
    /// A latched hop still refuses another game's filing, with a stable code and the durable counter delta.
    #[tokio::test(flavor = "current_thread")]
    async fn d2_663_wrong_game_signed_filing_has_a_counted_422_code() {
        use crate::hopsweep::tests as hs;
        let wallet = hs::wallet(0x45);
        let identity = hs::identity(&wallet);
        let poster = hex::encode(&identity);
        let seat = hs::skey(14);
        let raw = hs::sweep_raw(&seat, &seat, 0, hs::HOP_SATS, &hs::DEST);
        let start_ms = 1_000_000_000_000.0;
        crate::courier::take_pending_if_due(start_ms);
        for (i, latch) in [Some(1), Some(0), None].into_iter().enumerate() {
            let db = crate::pot_hook::tests::SqliteSql::migrated();
            md_index_the_hop(&db, &hs::hop_ctx(&wallet, &seat, true), latch);
            let (outcome, _) = md_file(
                &db,
                RecordKind::Hopsweep,
                &md_sweep_script(&identity, &[0x99; 32], &raw),
                &poster,
            )
            .await;
            let DoorOutcome::Refused(refusal) = outcome else {
                panic!("wrong-game filing was accepted: {outcome:?}");
            };
            let (status, body) = record_refusal_reply(Some(RecordKind::Hopsweep), refusal, None);
            let counters: std::collections::BTreeMap<_, _> =
                crate::courier::take_pending_if_due(start_ms + (i + 1) as f64 * 60_001.0)
                    .into_iter()
                    .collect();
            assert_eq!(counters.get("applayer_record_refused_total"), Some(&1));
            assert_eq!(
                counters.get("applayer_record_refused_sweep_game_mismatch_total"),
                Some(&1)
            );
            assert_eq!(status, 422);
            assert_eq!(body["code"], "sweep_game_mismatch", "latch {latch:?}");
            assert_eq!(body["error"], RecordRefusal::SweepGameMismatch.as_str());
            let rows: i64 = db
                .conn
                .query_row("SELECT COUNT(*) FROM hopsweep_records", [], |r| r.get(0))
                .unwrap();
            assert_eq!(rows, 0);
            let (own_game, _) = md_file(
                &db,
                RecordKind::Hopsweep,
                &md_sweep_script(&identity, &hs::GID, &raw),
                &poster,
            )
            .await;
            assert_eq!(own_game, DoorOutcome::Written { row_written: true });
        }
    }

    /// A wrong signature must have the same refusal under either game, across every latch state.
    #[tokio::test(flavor = "current_thread")]
    async fn d2_note3_wrong_signature_never_answers_the_game_comparison() {
        use crate::hopsweep::tests as hs;
        let wallet = hs::wallet(0x46);
        let identity = hs::identity(&wallet);
        let poster = hex::encode(&identity);
        let seat = hs::skey(14);
        let forged = hs::sweep_raw(&seat, &hs::skey(15), 0, hs::HOP_SATS, &hs::DEST);
        for latch in [Some(1), Some(0), None] {
            for game in [hs::GID, [0x99; 32]] {
                let db = crate::pot_hook::tests::SqliteSql::migrated();
                md_index_the_hop(&db, &hs::hop_ctx(&wallet, &seat, true), latch);
                let (outcome, _) = md_file(
                    &db,
                    RecordKind::Hopsweep,
                    &md_sweep_script(&identity, &game, &forged),
                    &poster,
                )
                .await;
                assert_eq!(
                    outcome,
                    DoorOutcome::Refused(RecordRefusal::SweepNotTheSeatsSpend),
                    "latch {latch:?}, game {}",
                    hex::encode(game)
                );
                let (status, body) = record_refusal_reply(
                    Some(RecordKind::Hopsweep),
                    RecordRefusal::SweepNotTheSeatsSpend,
                    None,
                );
                assert_eq!(status, 422);
                assert_eq!(body["error"], RecordRefusal::SweepNotTheSeatsSpend.as_str());
                assert!(
                    body.get("code").is_none(),
                    "the game refusal leaked through"
                );
                let rows: i64 = db
                    .conn
                    .query_row("SELECT COUNT(*) FROM hopsweep_records", [], |r| r.get(0))
                    .unwrap();
                assert_eq!(rows, 0);
            }
        }
    }

    // ── MONEY-DOOR-LENS: the lens's negative cases (not part of PR 169) ──

    /// The lens: a VALID row first, then a stranger's junk chain row at the same (identity, game, pot): the valid
    /// row is never demoted, the victim's re-file stays idempotent, and the valid row leads `/refund-backups`.
    #[tokio::test(flavor = "current_thread")]
    async fn lens_798_a_valid_row_then_junk_never_demotes() {
        let victim = wallet(0x51);
        let id = identity(&victim);
        let id_lc = hex::encode(&id);
        let gid = [0x5au8; 32];
        let filing = potrefund_script(&victim, &gid, &presigned_refund());
        // (1) a FILED rank-1 row, then junk planted after it, then the victim's re-file
        let db = crate::pot_hook::tests::SqliteSql::migrated();
        md_index_the_pot(&db);
        md_seat(&db, &id_lc, &gid);
        let (out, v) = md_file(&db, RecordKind::Potrefund, &filing, &id_lc).await;
        assert_eq!(out, DoorOutcome::Written { row_written: true });
        md_chain_admit(&db, &md_junk_marker(&id, &gid), &"e1".repeat(32), 9_000);
        let (out, _) = md_file(&db, RecordKind::Potrefund, &filing, &id_lc).await;
        assert_eq!(
            out,
            DoorOutcome::Written { row_written: true },
            "re-file: the idempotent write"
        );
        assert_eq!(
            md_refund_rows(&db),
            vec![(v.key().to_string(), Some(1)), ("e1".repeat(32), None)],
            "the filed row keeps refundValid = 1; one filed row"
        );
        assert_eq!(
            md_served(&db, &id_lc).first(),
            Some(&(v.key().to_string(), Some(1)))
        );
        // (2) the victim's own valid CHAIN row latched, then junk planted, then a re-file: still the no-op, latch kept
        let db = crate::pot_hook::tests::SqliteSql::migrated();
        md_index_the_pot(&db);
        md_seat(&db, &id_lc, &gid);
        md_chain_admit(&db, &filing, &"f1".repeat(32), 1_500);
        let (out, _) = md_file(&db, RecordKind::Potrefund, &filing, &id_lc).await;
        assert_eq!(out, DoorOutcome::AlreadyIndexed { latched: true });
        md_chain_admit(&db, &md_junk_marker(&id, &gid), &"f2".repeat(32), 9_000);
        let (out, _) = md_file(&db, RecordKind::Potrefund, &filing, &id_lc).await;
        assert_eq!(
            out,
            DoorOutcome::AlreadyIndexed { latched: false },
            "the own row is still the no-op over the junk"
        );
        assert_eq!(
            md_refund_rows(&db),
            vec![("f1".repeat(32), Some(1)), ("f2".repeat(32), None)]
        );
        assert_eq!(
            md_served(&db, &id_lc).first(),
            Some(&("f1".repeat(32), Some(1)))
        );
    }

    /// The lens: the forger cannot reach the door as the victim (poster mismatch, junk signature), and a stranger's
    /// OWN-identity filing of a junk raw naming the victim's indexed pot is refused at the bind.
    #[tokio::test(flavor = "current_thread")]
    async fn lens_798_b_the_forger_cannot_file_at_the_door() {
        let victim = wallet(0x52);
        let id = identity(&victim);
        let id_lc = hex::encode(&id);
        let gid = [0x5bu8; 32];
        let stranger = wallet(0x53);
        let s_lc = hex::encode(identity(&stranger));
        assert_eq!(
            verify_record_post(RecordKind::Potrefund, &md_junk_marker(&id, &gid), &id_lc).err(),
            Some(RecordRefusal::SignatureInvalid),
            "a junk-signed marker naming the victim is refused at the door"
        );
        assert_eq!(
            verify_record_post(
                RecordKind::Potrefund,
                &potrefund_script(&victim, &gid, &presigned_refund()),
                &s_lc
            )
            .err(),
            Some(RecordRefusal::PosterMismatch)
        );
        let db = crate::pot_hook::tests::SqliteSql::migrated();
        md_index_the_pot(&db);
        let junk = potrefund_script(&stranger, &gid, &junk_refund_raw(&POT_TXID, 0));
        let (out, _) = md_file(&db, RecordKind::Potrefund, &junk, &s_lc).await;
        assert!(
            matches!(out, DoorOutcome::Refused(_)),
            "a stranger's junk raw on an indexed pot is refused: {out:?}"
        );
        assert!(md_refund_rows(&db).is_empty());
    }

    /// The lens: the read bound. Twenty junk chain rows naming the victim never spend the victim's cap and never
    /// make the filing a no-op; past the 16 rows read, an own chain row may go unseen, and the filing then WRITES
    /// its own rank-1 row (the safe direction), never a junk no-op.
    #[tokio::test(flavor = "current_thread")]
    async fn lens_798_c_junk_flood_spends_no_cap_and_is_never_the_noop() {
        let victim = wallet(0x54);
        let id = identity(&victim);
        let id_lc = hex::encode(&id);
        let gid = [0x5cu8; 32];
        let filing = potrefund_script(&victim, &gid, &presigned_refund());
        let db = crate::pot_hook::tests::SqliteSql::migrated();
        md_index_the_pot(&db);
        md_seat(&db, &id_lc, &gid);
        for i in 0..20u8 {
            md_chain_admit(
                &db,
                &md_junk_marker(&id, &gid),
                &format!("{:02x}", i).repeat(32),
                100 + i64::from(i),
            );
        }
        // the victim's own valid chain row, the 21st by rowid
        md_chain_admit(&db, &filing, &"fe".repeat(32), 50);
        let (out, v) = md_file(&db, RecordKind::Potrefund, &filing, &id_lc).await;
        let latched: Vec<_> = md_refund_rows(&db)
            .into_iter()
            .filter(|(_, r)| *r == Some(1))
            .collect();
        match out {
            DoorOutcome::AlreadyIndexed { .. } => {
                assert_eq!(latched, vec![("fe".repeat(32), Some(1))])
            }
            DoorOutcome::Written { row_written: true } => {
                assert_eq!(
                    latched,
                    vec![(v.key().to_string(), Some(1))],
                    "the filing's own rank-1 row"
                )
            }
            other => panic!("never a refusal under a junk flood: {other:?}"),
        }
        eprintln!("lens_798_c: past 16 junk rows the door answered {out:?}");
        assert_eq!(
            md_served(&db, &id_lc).first().map(|r| r.1),
            Some(Some(1)),
            "a valid row leads"
        );
        // the junk rows spent no cap: a DIFFERENT valid content is refused only by the victim's own filed row, if any
        let k = pot_keys();
        let other = spend_signed_by(&k[0], &k[1], 0xffff_fffd, 900_000);
        let (out2, _) = md_file(
            &db,
            RecordKind::Potrefund,
            &potrefund_script(&victim, &gid, &other),
            &id_lc,
        )
        .await;
        if matches!(out, DoorOutcome::AlreadyIndexed { .. }) {
            assert_eq!(
                out2,
                DoorOutcome::Written { row_written: true },
                "no filed row yet: the cap is untouched"
            );
        } else {
            assert_eq!(
                out2,
                DoorOutcome::Refused(RecordRefusal::TooManyFiled),
                "only the victim's own filed row counts"
            );
        }
    }

    /// The lens: #663's hop row choice. A stranger-shaped hopparty row of the victim's hop under ANOTHER game: when
    /// the victim's own row is latched it leads and the honest sweep files; when neither is latched the older row
    /// is read and the filing is 425 (as on the base); with no hop row the filing is 425 (unchanged).
    #[tokio::test(flavor = "current_thread")]
    async fn lens_663_b_the_hop_row_read_and_the_absent_hop() {
        use crate::hopsweep::tests as hs;
        let w = hs::wallet(0x55);
        let id = hs::identity(&w);
        let id_lc = hex::encode(&id);
        let seat = hs::skey(13);
        let raw = hs::sweep_raw(&seat, &seat, 0, hs::HOP_SATS, &hs::DEST);
        let honest = md_sweep_script(&id, &hs::GID, &raw);
        // no hop row
        let db = crate::pot_hook::tests::SqliteSql::migrated();
        let (out, _) = md_file(&db, RecordKind::Hopsweep, &honest, &id_lc).await;
        assert_eq!(format!("{out:?}"), "Refused(HopNotIndexed)");
        let plant = |db: &crate::pot_hook::tests::SqliteSql, valid: Option<i64>| {
            let h = hs::hop_ctx(&w, &seat, true);
            db.conn
                .execute(
                    crate::handfile::HOPPARTY_HANDFILE_FILE_SQL,
                    rusqlite::params![
                        h.identity,
                        h.opponent_identity,
                        hex::encode([0x98u8; 32]),
                        h.hop_vout,
                        h.hop_sats as i64,
                        h.seat_settle_pubkey,
                        "3044",
                        "3044",
                        h.hop_lock_hex,
                        h.hop_sats_on_chain.map(|v| v as i64),
                        2,
                        hex::encode(hs::HOP),
                        7,
                        500,
                        valid
                    ],
                )
                .unwrap();
        };
        for (own, junk, want) in [
            (Some(1i64), None, "Written { row_written: true }"),
            (Some(1), Some(0), "Written { row_written: true }"),
            (None, Some(0), "Refused(HopMarkerUnverified)"),
            (None, None, "Refused(HopMarkerUnverified)"),
        ] {
            let db = crate::pot_hook::tests::SqliteSql::migrated();
            md_index_the_hop(&db, &hs::hop_ctx(&w, &seat, true), own);
            plant(&db, junk);
            let (out, _) = md_file(&db, RecordKind::Hopsweep, &honest, &id_lc).await;
            assert_eq!(format!("{out:?}"), want, "own {own:?} junk {junk:?}");
            // the wrong game is never written in any row state
            let (out, _) = md_file(
                &db,
                RecordKind::Hopsweep,
                &md_sweep_script(&id, &[0x98u8; 32], &raw),
                &id_lc,
            )
            .await;
            assert!(
                matches!(out, DoorOutcome::Refused(_)),
                "own {own:?} junk {junk:?}: {out:?}"
            );
        }
    }

    // ── MONEY-DOOR-FOLD: the lens's LOW-1 and LOW-2 ──

    fn md_owed_state(db: &crate::pot_hook::tests::SqliteSql, id_lc: &str, stale: i64) {
        db.conn
            .execute(
                "INSERT OR REPLACE INTO owed_state (identity, computedAtMs, tip, rows, stale) VALUES (?1, 1, 900000, 1, ?2)",
                rusqlite::params![id_lc, stale],
            )
            .unwrap();
    }

    fn md_stale(db: &crate::pot_hook::tests::SqliteSql, id_lc: &str) -> i64 {
        db.conn
            .query_row(
                "SELECT stale FROM owed_state WHERE identity = ?1",
                [id_lc],
                |r| r.get(0),
            )
            .unwrap()
    }

    /// LOW-1 (direct arm): the identity's own valid chain row is the no-op, and its latch moves it to
    /// `refundValid = 1`, which the owed list reads as the pot's valid refund: the pot's parties and the filer are
    /// marked stale as on a written row. A repeat whose latch moves nothing marks nothing; a non-party is never marked.
    #[tokio::test(flavor = "current_thread")]
    async fn money_door_fold_l1_a_latching_no_op_judges_the_pot() {
        let victim = wallet(0x61);
        let id = identity(&victim);
        let id_lc = hex::encode(&id);
        let gid = [0x6au8; 32];
        let filing = potrefund_script(&victim, &gid, &presigned_refund());
        let db = crate::pot_hook::tests::SqliteSql::migrated();
        md_index_the_pot(&db);
        md_seat(&db, &id_lc, &gid);
        let bystander = "03".to_string() + &"cc".repeat(32);
        md_owed_state(&db, &id_lc, 0);
        md_owed_state(&db, &bystander, 0);
        md_chain_admit(&db, &filing, &"a7".repeat(32), 1_500);
        let (out, _v) = md_file(&db, RecordKind::Potrefund, &filing, &id_lc).await;
        assert_eq!(out, DoorOutcome::AlreadyIndexed { latched: true });

        assert_eq!(
            (md_stale(&db, &id_lc), md_stale(&db, &bystander)),
            (0, 0),
            "the latching no-op judges the pot, never a non-party"
        );
        // the repeat: the row is already latched, nothing moves, nothing is marked
        md_owed_state(&db, &id_lc, 0);
        let (out, _v) = md_file(&db, RecordKind::Potrefund, &filing, &id_lc).await;
        assert_eq!(out, DoorOutcome::AlreadyIndexed { latched: false });

        assert_eq!(md_stale(&db, &id_lc), 0, "a quiet no-op marks nothing");
        // a written row still marks (the control)
        let db = crate::pot_hook::tests::SqliteSql::migrated();
        md_index_the_pot(&db);
        md_seat(&db, &id_lc, &gid);
        md_owed_state(&db, &id_lc, 0);
        let (out, _v) = md_file(&db, RecordKind::Potrefund, &filing, &id_lc).await;
        assert_eq!(out, DoorOutcome::Written { row_written: true });

        assert_eq!(md_stale(&db, &id_lc), 0);
        let n: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM owed_rows WHERE identity=?1",
                [&id_lc],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1, "the filing judged its row before returning");
    }

    #[test]
    fn money_door_fold_l1_b_handfile_judges_in_its_batch() {
        let source = include_str!("handfile.rs");
        let body = source
            .split("pub(crate) async fn judge_handfile(")
            .nth(1)
            .unwrap()
            .split("/// The door's handfile arm")
            .next()
            .unwrap();
        assert!(body.contains("owed_write::judge_filings"));
        assert!(!source.contains("note_filing_and_recompute"));
    }

    /// A port whose answers carry NO change count for the statements it names (D1's `meta.changes` is optional).
    impl crate::owed_judge::JudgeDb for NoCount<'_> {
        fn tip(&self) -> Option<u64> {
            Some(900_000)
        }
        async fn word_views(
            &self,
            rows: &std::collections::BTreeMap<String, crate::tracker_host::stored::Row>,
        ) -> std::result::Result<std::collections::BTreeMap<String, serde_json::Value>, String>
        {
            Ok(rows.iter().map(|(k, v)| (k.clone(), v.view())).collect())
        }
        async fn canonical_root(&self, _: u64) -> Option<String> {
            None
        }
        async fn push(&self, _: &str, _: serde_json::Value) {}
    }
    struct NoCount<'a>(&'a crate::pot_hook::tests::SqliteSql, &'static str);
    impl crate::pot_hook::SqlPort for NoCount<'_> {
        async fn run_batch(
            &self,
            stmts: Vec<crate::pot_hook::Stmt>,
        ) -> std::result::Result<Vec<crate::pot_hook::StmtOut>, String> {
            let named: Vec<bool> = stmts.iter().map(|s| s.sql == self.1).collect();
            let mut outs = self.0.run_batch(stmts).await?;
            for (o, n) in outs.iter_mut().zip(named) {
                if n {
                    o.changes = None;
                }
            }
            Ok(outs)
        }
        async fn counters(&self, d: Vec<(String, u64)>) {
            self.0.counters(d).await
        }
        fn now_ms(&self) -> u64 {
            self.0.now_ms()
        }
        fn log(&self, l: &str) {
            self.0.log(l)
        }
    }

    /// LOW-2: an answer with no change count is never read as zero rows written. The `collected` door re-reads its
    /// row by key: written by this request (its stamp is this request's) is a write, marked; on file before is the
    /// re-file. The chain latch with no count is read as moved (it marks: the safe direction).
    #[tokio::test(flavor = "current_thread")]
    async fn money_door_fold_l2_an_absent_change_count_is_never_zero_rows() {
        let w = wallet(0x62);
        let id = identity(&w);
        let id_lc = hex::encode(&id);
        let gid = [0x6bu8; 32];
        let sig = sign(
            &w,
            collected_protocol(),
            &hex::encode(gid),
            &collected_challenge(&hex::encode(gid), &id_lc),
        );
        let marker = script(&[b"LOW/collected/v1", &gid, &id, &sig]);
        let db = crate::pot_hook::tests::SqliteSql::migrated();
        let port = NoCount(&db, COLLECTED_FILE_SQL);
        let mut v = verify_record_post(RecordKind::Collected, &marker, &id_lc).unwrap();
        let first = file_verified(&port, &mut v, &id_lc, 2_000).await.unwrap();
        assert_eq!(
            first,
            DoorOutcome::Written { row_written: true },
            "a first filing whose answer carries no count is a write"
        );
        md_owed_state(&db, &id_lc, 0);
        assert_eq!(
            md_stale(&db, &id_lc),
            0,
            "the judgement needs no stale read"
        );
        let mut v = verify_record_post(RecordKind::Collected, &marker, &id_lc).unwrap();
        let again = file_verified(&port, &mut v, &id_lc, 2_500).await.unwrap();
        assert_eq!(
            again,
            DoorOutcome::Written { row_written: false },
            "a re-file with no count: the row re-read by key was on file before"
        );
        // the potrefund chain latch with no count
        let victim = wallet(0x63);
        let rid = identity(&victim);
        let rid_lc = hex::encode(&rid);
        let filing = potrefund_script(&victim, &gid, &presigned_refund());
        let db = crate::pot_hook::tests::SqliteSql::migrated();
        md_index_the_pot(&db);
        md_seat(&db, &rid_lc, &gid);
        md_chain_admit(&db, &filing, &"a8".repeat(32), 1_500);
        let port = NoCount(&db, POTREFUND_CHAIN_LATCH_SQL);
        let mut v = verify_record_post(RecordKind::Potrefund, &filing, &rid_lc).unwrap();
        assert_eq!(
            file_verified(&port, &mut v, &rid_lc, 2_000).await.unwrap(),
            DoorOutcome::AlreadyIndexed { latched: true }
        );
        // the worker's port carries the count as D1 answers it, never defaulting an absent one to 0
        let routes: String = include_str!("routes.rs").split_whitespace().collect();
        assert!(routes
            .contains("letchanges=r.meta().ok().flatten().and_then(|m|m.changes).map(|n|nasu64);"));
    }
}
