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
//!   1. parses them with the family's own parser (`parse_*_marker`) — one
//!      grammar, the on-chain one;
//!   2. VERIFIES the family's signatures with the same pure validity code the
//!      overlay applies at admission where it has one (`potparty::validity::
//!      record_sig_valid`, `result::validity::claim_tier`) and, for the two
//!      families the overlay never verified (`potrefund`, `collected`), with
//!      the client's own challenge bytes re-derived here — STRICTER than the
//!      chain path on purpose: a filed row costs nothing to plant, so a row
//!      the identity did not sign is refused (the D2 lesson);
//!   3. binds the POSTER to the marker: the resolved identity must be the
//!      marker's identity (`potparty`/`potrefund`/`collected`) or one of the
//!      two seats (`result`) — anyone may deliver what the signer signed, as
//!      anyone may broadcast a marker, and a VERIFIED session must match;
//!   4. writes the family's OWN records table with the SAME column list the
//!      overlay's lookup service writes (`potparty_records`,
//!      `potrefund_records`, `result_markers_v2` + its `lb_marker_rows` row,
//!      `collected_markers_v2`), keyed by a synthetic outpoint
//!      (`filed_key(script)` = `filed:<sha256 of the bytes>`, vout 0), so
//!      EVERY reader — the identity views, `/refund-backups`, `/results`, the
//!      overlay's own lookup services, the client's verifiers — serves a filed
//!      row exactly as an admitted one, with no chain anchor to check.
//!
//! Trust: a lying store can WITHHOLD a row, never forge one (every row is
//! identity-signed and client-verified, as today). Not a ts-stack
//! divergence: lookup services, their storage and the app-layer are the
//! reference's explicitly pluggable surfaces. Idempotent: the same bytes file
//! the same key (`INSERT OR IGNORE`), so a retry is free.
use serde::Deserialize;
use worker::{Request, Response, Result, RouteContext};

use crate::auth::AuthState;
use overlay_discovery::collected::{parse_collected_marker, storage::CollectedRecord};
use overlay_discovery::potparty::{parse_potparty_marker, storage::PotpartyRecord};
use overlay_discovery::potrefund::{
    parse_potrefund_marker, storage::PotrefundRecord, POTREFUND_TAG,
};
use overlay_discovery::result::{parse_result_marker, storage::ResultRecord};

/// The largest script a post may carry: the refund backup's own cap
/// (`POTREFUND_REFUND_MAX_LEN`, 100 KB of raw tx) plus its fixed fields.
pub const RECORD_POST_MAX_SCRIPT_BYTES: usize =
    overlay_discovery::potrefund::POTREFUND_REFUND_MAX_LEN + 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordKind {
    Potparty,
    Potrefund,
    Result,
    Collected,
}

impl RecordKind {
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
    ScriptTooLarge,
    NotAMarker,
    /// The poster is not the marker's identity (or, for a result, neither seat).
    PosterMismatch,
    /// The marker's own signature does not verify under its identity.
    SignatureInvalid,
    /// A refund backup whose raw tx does not spend the pot outpoint it names.
    RefundDoesNotSpendThePot,
}

impl RecordRefusal {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BadKind => "kind must be one of potparty | potrefund | result | collected",
            Self::BadScriptHex => "scriptHex must be hex",
            Self::ScriptTooLarge => "script too large",
            Self::NotAMarker => "the script is not a marker of this kind",
            Self::PosterMismatch => {
                "the poster must be the marker's identity (a result: one of its two seats)"
            }
            Self::SignatureInvalid => "the marker's signature does not verify under its identity",
            Self::RefundDoesNotSpendThePot => {
                "the refund backup's raw tx does not spend the pot outpoint it names"
            }
        }
    }
    pub fn status(self) -> u16 {
        match self {
            Self::BadKind | Self::BadScriptHex | Self::NotAMarker => 400,
            Self::ScriptTooLarge => 413,
            Self::PosterMismatch => 403,
            Self::SignatureInvalid | Self::RefundDoesNotSpendThePot => 422,
        }
    }
}

/// The synthetic outpoint a filed row is keyed by: `filed:` + the sha256 of
/// the script bytes (56 hex chars). Never 64 hex, so no reader can mistake it
/// for a chain txid; the same bytes always file the same key.
pub fn filed_key(script: &[u8]) -> String {
    let h = hex::encode(bsv_rs::primitives::hash::sha256(script));
    format!("filed:{}", &h[..56])
}

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

/// A refund backup binds the pot it names: its raw tx's first input spends
/// that outpoint. Anything else is refused (the covenant makes any other
/// shape unspendable anyway; this is the belt the issue names).
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

/// What a verified post writes — the family's own record, keyed by the filed
/// outpoint, with the validity the overlay would compute at admission.
#[derive(Debug, Clone, PartialEq)]
pub enum VerifiedRecord {
    Potparty(PotpartyRecord),
    Potrefund(PotrefundRecord),
    /// The record and its `claim_tier` (1 = the winner's claim, 2 = countersigned).
    Result(ResultRecord, i64),
    Collected(CollectedRecord),
}

impl VerifiedRecord {
    pub fn key(&self) -> &str {
        match self {
            Self::Potparty(r) => &r.txid,
            Self::Potrefund(r) => &r.txid,
            Self::Result(r, _) => &r.txid,
            Self::Collected(r) => &r.txid,
        }
    }
}

/// The pure verifier: the bytes, the kind and the resolved poster in; the
/// record to write out, or the refusal.
pub fn verify_record_post(
    kind: RecordKind,
    script: &[u8],
    poster_lc: &str,
) -> std::result::Result<VerifiedRecord, RecordRefusal> {
    if script.len() > RECORD_POST_MAX_SCRIPT_BYTES {
        return Err(RecordRefusal::ScriptTooLarge);
    }
    let key = filed_key(script);
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
                txid: key,
                output_index: 0,
                created_at: 0,
            };
            if record.identity != poster_lc {
                return Err(RecordRefusal::PosterMismatch);
            }
            if !overlay_discovery::potparty::validity::record_sig_valid(&record) {
                return Err(RecordRefusal::SignatureInvalid);
            }
            Ok(VerifiedRecord::Potparty(record))
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
            if !overlay_discovery::result::validity::anyone_sig_verifies(
                &identity,
                &game_id,
                &challenge,
                &hex::encode(&m.sig),
                potrefund_protocol(),
            ) {
                return Err(RecordRefusal::SignatureInvalid);
            }
            if !refund_spends_pot(&m.refund_raw, &m.pot_txid, m.pot_vout) {
                return Err(RecordRefusal::RefundDoesNotSpendThePot);
            }
            Ok(VerifiedRecord::Potrefund(PotrefundRecord {
                identity,
                game_id,
                pot_txid: hex::encode(m.pot_txid),
                pot_vout: m.pot_vout,
                refund_raw_hex: hex::encode(&m.refund_raw),
                sig_hex: hex::encode(&m.sig),
                txid: key,
                output_index: 0,
                created_at: 0,
            }))
        }
        RecordKind::Result => {
            let m = parse_result_marker(script).ok_or(RecordRefusal::NotAMarker)?;
            let record = ResultRecord {
                game_id: hex::encode(m.game_id),
                winner: hex::encode(&m.winner),
                loser: hex::encode(&m.loser),
                pot_txid: hex::encode(m.pot_txid),
                settle_txid: hex::encode(m.settle_txid),
                winner_sig_hex: hex::encode(&m.winner_sig),
                loser_sig_hex: m.loser_sig.as_deref().map(hex::encode),
                cards_hex: m.cards.map(hex::encode),
                txid: key,
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
            if !overlay_discovery::result::validity::anyone_sig_verifies(
                &identity,
                &game_id,
                &challenge,
                &hex::encode(&m.sig),
                collected_protocol(),
            ) {
                return Err(RecordRefusal::SignatureInvalid);
            }
            Ok(VerifiedRecord::Collected(CollectedRecord {
                identity,
                game_id,
                txid: key,
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

pub const POTREFUND_FILE_SQL: &str = "INSERT OR IGNORE INTO potrefund_records \
     (identity, gameId, potTxid, potVout, refundRawHex, \
      sigHex, txid, outputIndex, createdAt) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

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

fn js(s: &str) -> worker::wasm_bindgen::JsValue {
    s.into()
}
fn js_opt(s: Option<&str>) -> worker::wasm_bindgen::JsValue {
    s.map_or(worker::wasm_bindgen::JsValue::NULL, |v| v.into())
}
fn js_num(n: i64) -> worker::wasm_bindgen::JsValue {
    (n as f64).into()
}

/// `POST /record?kind=<family>&identity=<poster>` with `{ "scriptHex": … }`.
pub async fn record_post(mut req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    let identity = match crate::routes::view_identity(&req, &ctx) {
        crate::routes::ViewIdentity::Identity(id) => id.to_ascii_lowercase(),
        crate::routes::ViewIdentity::Refuse(resp) => return resp,
    };
    let url = match req.url() {
        Ok(u) => u,
        Err(_) => return crate::routes::json_error("unreadable request url", 400),
    };
    let kind = url
        .query_pairs()
        .find(|(k, _)| k == "kind")
        .and_then(|(_, v)| RecordKind::parse(&v));
    let Some(kind) = kind else {
        return crate::routes::json_error(
            RecordRefusal::BadKind.as_str(),
            RecordRefusal::BadKind.status(),
        );
    };
    let raw: Vec<u8> = match ctx.data.body.clone() {
        Some(b) => b,
        None => match req.bytes().await {
            Ok(b) => b,
            Err(e) => return crate::routes::json_error(&format!("body unreadable: {e}"), 400),
        },
    };
    let body: RecordPostBody = match serde_json::from_slice(&raw) {
        Ok(b) => b,
        Err(e) => {
            return crate::routes::json_error(&format!("body is not a record post: {e}"), 400)
        }
    };
    let script = match hex::decode(body.script_hex.trim()) {
        Ok(s) => s,
        Err(_) => {
            return crate::routes::json_error(
                RecordRefusal::BadScriptHex.as_str(),
                RecordRefusal::BadScriptHex.status(),
            )
        }
    };
    let verified = match verify_record_post(kind, &script, &identity) {
        Ok(v) => v,
        Err(r) => return crate::routes::json_error(r.as_str(), r.status()),
    };
    let db = ctx.env.d1("OVERLAY_DB")?;
    // The overlay stamps every one of these tables in unix SECONDS.
    let now = (worker::Date::now().as_millis() / 1000) as i64;
    match &verified {
        VerifiedRecord::Potparty(r) => {
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
                    js_num(1),
                ])?
                .run()
                .await?;
        }
        VerifiedRecord::Potrefund(r) => {
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
                ])?
                .run()
                .await?;
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
    crate::routes::json_response(
        serde_json::json!({ "filed": true, "kind": kind.as_str(), "key": verified.key() })
            .to_string(),
        200,
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
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
    /// A one-input tx spending `pot:vout` (the refund backup's shape).
    fn refund_raw(pot_txid: &[u8; 32], vout: u32) -> Vec<u8> {
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

    #[test]
    fn filed_key_is_never_a_txid_and_the_same_bytes_file_the_same_key() {
        let k = filed_key(b"abc");
        assert!(k.starts_with("filed:"));
        assert_eq!(k.len(), 62);
        assert_ne!(k.len(), 64);
        assert_eq!(k, filed_key(b"abc"));
        assert_ne!(k, filed_key(b"abd"));
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
                assert_eq!(r.txid, filed_key(&s));
                assert_eq!(r.output_index, 0);
            }
            _ => panic!("collected"),
        }
        // A stranger posting the signer's marker: the poster binding refuses.
        assert_eq!(
            verify_record_post(
                RecordKind::Collected,
                &s,
                &hex::encode(identity(&wallet(2)))
            )
            .unwrap_err(),
            RecordRefusal::PosterMismatch
        );
        // The signer's identity with a forged signature: refused.
        let mut bad = sig.clone();
        bad[10] ^= 0x01;
        let s2 = script(&[b"LOW/collected/v1", &gid, &id, &bad]);
        assert_eq!(
            verify_record_post(RecordKind::Collected, &s2, &id_lc).unwrap_err(),
            RecordRefusal::SignatureInvalid
        );
        // The wrong kind for these bytes.
        assert_eq!(
            verify_record_post(RecordKind::Potparty, &s, &id_lc).unwrap_err(),
            RecordRefusal::NotAMarker
        );
    }

    #[test]
    fn a_refund_backup_files_when_signed_by_its_identity_and_spending_its_pot() {
        let w = wallet(3);
        let id = identity(&w);
        let id_lc = hex::encode(&id);
        let gid = [0x44u8; 32];
        let pot = [0x55u8; 32];
        let raw = refund_raw(&pot, 0);
        let gid_lc = hex::encode(gid);
        let sig = sign(
            &w,
            potrefund_protocol(),
            &gid_lc,
            &potrefund_challenge(&id, &gid, &pot, 0, &raw),
        );
        let s = script(&[
            b"LOW/potrefund/v1",
            &id,
            &gid,
            &pot,
            &0u32.to_le_bytes(),
            &raw,
            &sig,
        ]);
        let v = verify_record_post(RecordKind::Potrefund, &s, &id_lc).unwrap();
        match &v {
            VerifiedRecord::Potrefund(r) => {
                assert_eq!(r.pot_txid, hex::encode(pot));
                assert_eq!(r.refund_raw_hex, hex::encode(&raw));
                assert_eq!(r.txid, filed_key(&s));
            }
            _ => panic!("potrefund"),
        }
        // A raw spending ANOTHER outpoint under a valid signature: refused.
        let other = refund_raw(&[0x66u8; 32], 0);
        let sig2 = sign(
            &w,
            potrefund_protocol(),
            &gid_lc,
            &potrefund_challenge(&id, &gid, &pot, 0, &other),
        );
        let s2 = script(&[
            b"LOW/potrefund/v1",
            &id,
            &gid,
            &pot,
            &0u32.to_le_bytes(),
            &other,
            &sig2,
        ]);
        assert_eq!(
            verify_record_post(RecordKind::Potrefund, &s2, &id_lc).unwrap_err(),
            RecordRefusal::RefundDoesNotSpendThePot
        );
        // The signature over a different vout: refused.
        let s3 = script(&[
            b"LOW/potrefund/v1",
            &id,
            &gid,
            &pot,
            &1u32.to_le_bytes(),
            &raw,
            &sig,
        ]);
        assert_eq!(
            verify_record_post(RecordKind::Potrefund, &s3, &id_lc).unwrap_err(),
            RecordRefusal::SignatureInvalid
        );
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
                }
                _ => panic!("result"),
            }
        }
        // Countersigned: tier 2.
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
        match verify_record_post(RecordKind::Result, &s2, &wid_lc).unwrap() {
            VerifiedRecord::Result(_, tier) => assert_eq!(tier, 2),
            _ => panic!("result"),
        }
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
            VerifiedRecord::Potparty(r) => {
                assert_eq!(r.identity, id_lc);
                assert_eq!(r.recovery_height, 900_000);
                assert_eq!(r.txid, filed_key(&s));
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
}
