//! bsv-low P1.1 PROOF-IN-DB (owner GO, 2026-09-02): `POST /proof`.
//!
//! The winner's `LOW/proof/v1` transcript bundle used to be spent on chain
//! (≈11 KB, ≈1,200 sats, one wallet prompt) — and skipped by the client's
//! economy gate on every small-ante hand, and never published for a tie.
//! Under the owner's ruling ("we ARE the app-layer") the same bundle is now
//! POSTED here, bound by the SAME identity signature the on-chain marker
//! carries (`proofChallenge` = `LOW-proof\nv1\ngid=…\nwinner=…\nbundle=<sha256>`
//! signed under `[1,'low proof']`, keyID = gameId, counterparty `anyone`),
//! REPLAYED on write (`overlay_discovery::proof::replay` — both hands
//! re-derived from the signed transcript, never the bundle's words) and
//! stored. `/results` serves the hands from it exactly as from an on-chain
//! bundle; the loser never publishes anything after a hand.
//!
//! Trust: a lying store can WITHHOLD a bundle, never forge one — the bytes are
//! poster-signed and the replay is the only source of the cards. Anyone may
//! post a bundle the poster signed (as anyone may broadcast a marker); a
//! VERIFIED session must be the poster (the seam refuses a mismatch).
//! Ties: the revealed side posts with itself as `winner` (the bundle's own
//! `winnerSeat`); `winner` here means POSTER.
use base64::Engine as _;
use serde::Deserialize;
use worker::{Request, Response, Result, RouteContext};

use crate::auth::AuthState;

/// The client's `PROOF_PROTOCOL = [1,'low proof']`.
pub fn proof_protocol() -> bsv_rs::wallet::Protocol {
    bsv_rs::wallet::Protocol::new(bsv_rs::wallet::SecurityLevel::App, "low proof")
}

/// The canonical signed bytes — byte-identical to the client's
/// `proofChallenge(gameId, winner, bundleSha256Hex)` (all lowercase).
pub fn proof_challenge(game_id_lc: &str, winner_lc: &str, bundle_sha256_hex_lc: &str) -> Vec<u8> {
    format!("LOW-proof\nv1\ngid={game_id_lc}\nwinner={winner_lc}\nbundle={bundle_sha256_hex_lc}")
        .into_bytes()
}

/// The largest bundle a post may carry — the on-chain marker's own cap.
pub const PROOF_POST_MAX_BUNDLE_BYTES: usize = overlay_discovery::proof::PROOF_BUNDLE_MAX_LEN;

#[derive(Debug, Deserialize)]
pub struct ProofPostBody {
    #[serde(rename = "gameId")]
    pub game_id: String,
    /// The POSTER's identity key (66-hex) — the bundle's `winner`.
    pub winner: String,
    /// The bundle bytes (gzip or plain JSON) as base64 — what the signature binds.
    #[serde(rename = "bundleB64")]
    pub bundle_b64: String,
    /// DER ECDSA signature over `proof_challenge`, hex.
    #[serde(rename = "sigHex")]
    pub sig_hex: String,
}

/// What a verified post writes (the same columns the on-chain admission fills).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedProofPost {
    pub game_id: String,
    pub winner: String,
    pub sig_hex: String,
    pub bundle: Vec<u8>,
    pub bundle_valid: bool,
    pub winner_seat: Option<u8>,
    pub seat_a: Option<String>,
    pub seat_b: Option<String>,
    pub winner_cards_hex: Option<String>,
    pub loser_cards_hex: Option<String>,
}

/// Why a post is refused — each is a 4xx with this word in the body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProofPostRefusal {
    BadGameId,
    BadWinner,
    BadBase64,
    BundleTooLarge,
    BundleEmpty,
    BadSignatureHex,
    SignatureInvalid,
    BundleRefused,
}

impl ProofPostRefusal {
    pub fn status(self) -> u16 {
        match self {
            ProofPostRefusal::BundleTooLarge => 413,
            ProofPostRefusal::SignatureInvalid | ProofPostRefusal::BundleRefused => 422,
            _ => 400,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            ProofPostRefusal::BadGameId => "gameId must be 64 hex chars",
            ProofPostRefusal::BadWinner => "winner must be a 66-hex compressed identity key",
            ProofPostRefusal::BadBase64 => "bundleB64 is not valid base64",
            ProofPostRefusal::BundleTooLarge => "bundle exceeds the 65,536-byte cap",
            ProofPostRefusal::BundleEmpty => "bundle is empty",
            ProofPostRefusal::BadSignatureHex => {
                "sigHex is not a DER signature (68..=74 bytes, hex)"
            }
            ProofPostRefusal::SignatureInvalid => {
                "signature does not verify under the poster's identity over the proof challenge"
            }
            ProofPostRefusal::BundleRefused => {
                "the bundle does not replay (transcript proof refused)"
            }
        }
    }
}

fn is_hex_len(s: &str, n: usize) -> bool {
    s.len() == n && s.bytes().all(|c| c.is_ascii_hexdigit())
}

/// PURE: shape → signature → replay. The signature check runs BEFORE the
/// (expensive) replay so a stranger cannot spend our CPU on unsigned bytes.
pub fn verify_proof_post(
    body: &ProofPostBody,
) -> std::result::Result<VerifiedProofPost, ProofPostRefusal> {
    let game_id = body.game_id.trim().to_ascii_lowercase();
    if !is_hex_len(&game_id, 64) {
        return Err(ProofPostRefusal::BadGameId);
    }
    let winner = body.winner.trim().to_ascii_lowercase();
    if !is_hex_len(&winner, 66) || !(winner.starts_with("02") || winner.starts_with("03")) {
        return Err(ProofPostRefusal::BadWinner);
    }
    let bundle = base64::engine::general_purpose::STANDARD
        .decode(body.bundle_b64.trim())
        .map_err(|_| ProofPostRefusal::BadBase64)?;
    if bundle.is_empty() {
        return Err(ProofPostRefusal::BundleEmpty);
    }
    if bundle.len() > PROOF_POST_MAX_BUNDLE_BYTES {
        return Err(ProofPostRefusal::BundleTooLarge);
    }
    let sig_hex = body.sig_hex.trim().to_ascii_lowercase();
    let sig_len = sig_hex.len() / 2;
    if !sig_hex.len().is_multiple_of(2)
        || !sig_hex.bytes().all(|c| c.is_ascii_hexdigit())
        || !(68..=74).contains(&sig_len)
    {
        return Err(ProofPostRefusal::BadSignatureHex);
    }
    let bundle_sha = hex::encode(bsv_rs::primitives::hash::sha256(&bundle));
    let challenge = proof_challenge(&game_id, &winner, &bundle_sha);
    if !overlay_discovery::result::validity::anyone_sig_verifies(
        &winner,
        &game_id,
        &challenge,
        &sig_hex,
        proof_protocol(),
    ) {
        return Err(ProofPostRefusal::SignatureInvalid);
    }
    let game: [u8; 32] = hex::decode(&game_id)
        .map_err(|_| ProofPostRefusal::BadGameId)?
        .try_into()
        .map_err(|_| ProofPostRefusal::BadGameId)?;
    let winner_key: [u8; 33] = hex::decode(&winner)
        .map_err(|_| ProofPostRefusal::BadWinner)?
        .try_into()
        .map_err(|_| ProofPostRefusal::BadWinner)?;
    let proved = overlay_discovery::proof::replay::prove_bundle(&bundle, &game, &winner_key)
        .ok_or(ProofPostRefusal::BundleRefused)?;
    let cards = overlay_discovery::proof::replay::ProvedHands::cards_hex;
    Ok(VerifiedProofPost {
        game_id,
        winner,
        sig_hex,
        bundle,
        bundle_valid: true,
        winner_seat: Some(proved.winner_seat),
        seat_a: Some(hex::encode(proved.seats[0])),
        seat_b: Some(hex::encode(proved.seats[1])),
        winner_cards_hex: Some(cards(&proved.winner_cards)),
        loser_cards_hex: proved.loser_cards.map(|c| cards(&c)),
    })
}

/// The write: one row per (game, poster); a later post by the same poster
/// REPLACES (a newer signed bundle supersedes). 11 binds. Pub for the
/// REAL-SQLite harness.
pub const PROOF_POST_WRITE_SQL: &str = "INSERT OR REPLACE INTO proof_posts \
     (gameId, winner, sigHex, bundle, createdAt, bundleValid, winnerSeat, seatA, seatB, winnerCardsHex, loserCardsHex) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

/// `/results`: the posted, replay-valid bundles for a chunk of gameIds (1 bind
/// each) — the same columns `proof_hands_sql` serves for on-chain ones.
pub fn proof_posts_hands_sql(n: usize) -> String {
    debug_assert!(n >= 1);
    let placeholders = vec!["?"; n].join(", ");
    format!(
        "SELECT gameId, winner, winnerCardsHex, loserCardsHex FROM proof_posts \
         WHERE bundleValid = 1 AND gameId IN ({placeholders}) ORDER BY gameId ASC, createdAt DESC"
    )
}

/// `POST /proof?identity=<poster>` — the seam resolves the caller exactly as
/// the identity views do (a VERIFIED session must be the poster; anonymous
/// lenient posts are bound by the signature alone). Answers the replay verdict.
pub async fn proof_post(mut req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    let identity = match crate::routes::view_identity(&req, &ctx) {
        crate::routes::ViewIdentity::Identity(id) => id,
        crate::routes::ViewIdentity::Refuse(resp) => return resp,
    };
    let body: ProofPostBody = match req.json().await {
        Ok(b) => b,
        Err(e) => return crate::routes::json_error(&format!("body is not a proof post: {e}"), 400),
    };
    if !identity.eq_ignore_ascii_case(body.winner.trim()) {
        return crate::routes::json_error("the poster must be the resolved identity (?identity= and the session must name the bundle's winner)", 403);
    }
    let verified = match verify_proof_post(&body) {
        Ok(v) => v,
        Err(r) => return crate::routes::json_error(r.as_str(), r.status()),
    };
    let db = ctx.env.d1("OVERLAY_DB")?;
    let now = worker::Date::now().as_millis() / 1000;
    db.prepare(PROOF_POST_WRITE_SQL)
        .bind(&[
            verified.game_id.as_str().into(),
            verified.winner.as_str().into(),
            verified.sig_hex.as_str().into(),
            worker::wasm_bindgen::JsValue::from(worker::js_sys::Uint8Array::from(
                verified.bundle.as_slice(),
            )),
            (now as f64).into(),
            1f64.into(),
            verified
                .winner_seat
                .map_or(worker::wasm_bindgen::JsValue::NULL, |v| (v as f64).into()),
            verified
                .seat_a
                .as_deref()
                .map_or(worker::wasm_bindgen::JsValue::NULL, |v| v.into()),
            verified
                .seat_b
                .as_deref()
                .map_or(worker::wasm_bindgen::JsValue::NULL, |v| v.into()),
            verified
                .winner_cards_hex
                .as_deref()
                .map_or(worker::wasm_bindgen::JsValue::NULL, |v| v.into()),
            verified
                .loser_cards_hex
                .as_deref()
                .map_or(worker::wasm_bindgen::JsValue::NULL, |v| v.into()),
        ])?
        .run()
        .await?;
    crate::routes::json_response(
        serde_json::json!({
            "ok": true,
            "gameId": verified.game_id,
            "winner": verified.winner,
            "bundleValid": true,
            "winnerCardsHex": verified.winner_cards_hex,
            "loserCardsHex": verified.loser_cards_hex,
        })
        .to_string(),
        200,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bsv_rs::primitives::ec::PrivateKey;
    use bsv_rs::wallet::{Counterparty, CreateSignatureArgs, ProtoWallet};

    const REAL: &[u8] =
        include_bytes!("../../overlay-discovery/src/proof/fixtures/bundle-a1081773.bin");
    const REAL_GAME: &str = "a1081773673e8c7cb6093db8f4a59166495f15e9ded1fe354ee27bbda7922523";

    fn wallet_of(seed: u8) -> ProtoWallet {
        ProtoWallet::new(Some(PrivateKey::from_hex(&format!("{seed:064x}")).unwrap()))
    }
    fn sign(w: &ProtoWallet, game_id: &str, challenge: &[u8]) -> String {
        let sig = w
            .create_signature(CreateSignatureArgs {
                data: Some(challenge.to_vec()),
                hash_to_directly_sign: None,
                protocol_id: proof_protocol(),
                key_id: game_id.to_string(),
                counterparty: Some(Counterparty::Anyone),
            })
            .unwrap();
        hex::encode(sig.signature)
    }
    fn post(w: &ProtoWallet, game_id: &str, bundle: &[u8]) -> ProofPostBody {
        let winner = w.identity_key_hex().to_ascii_lowercase();
        let sha = hex::encode(bsv_rs::primitives::hash::sha256(bundle));
        let sig_hex = sign(w, game_id, &proof_challenge(game_id, &winner, &sha));
        ProofPostBody {
            game_id: game_id.to_string(),
            winner,
            bundle_b64: base64::engine::general_purpose::STANDARD.encode(bundle),
            sig_hex,
        }
    }

    #[test]
    fn the_challenge_is_byte_identical_to_the_clients() {
        let c = proof_challenge(
            "aa".repeat(32).as_str(),
            &format!("02{}", "bb".repeat(32)),
            &"cc".repeat(32),
        );
        assert_eq!(
            String::from_utf8(c).unwrap(),
            format!(
                "LOW-proof\nv1\ngid={}\nwinner=02{}\nbundle={}",
                "aa".repeat(32),
                "bb".repeat(32),
                "cc".repeat(32)
            )
        );
    }

    /// The signature is checked BEFORE the replay: a fresh key signs the REAL
    /// bundle correctly (the sig passes) and the replay then refuses it (that
    /// key is not the bundle's winner) — so a valid signature alone never
    /// stores a bundle it does not prove, and an invalid one never reaches the
    /// replay at all.
    #[test]
    fn signature_binds_then_the_replay_decides() {
        let w = wallet_of(0x21);
        let ok = post(&w, REAL_GAME, REAL);
        assert_eq!(
            verify_proof_post(&ok).unwrap_err(),
            ProofPostRefusal::BundleRefused,
            "signed by a stranger to the bundle"
        );

        let mut wrong_game = post(&w, REAL_GAME, REAL);
        wrong_game.game_id = "bb".repeat(32);
        assert_eq!(
            verify_proof_post(&wrong_game).unwrap_err(),
            ProofPostRefusal::SignatureInvalid
        );

        let mut flipped = post(&w, REAL_GAME, REAL);
        let mut b = base64::engine::general_purpose::STANDARD
            .decode(&flipped.bundle_b64)
            .unwrap();
        b[10] ^= 1;
        flipped.bundle_b64 = base64::engine::general_purpose::STANDARD.encode(&b);
        assert_eq!(
            verify_proof_post(&flipped).unwrap_err(),
            ProofPostRefusal::SignatureInvalid,
            "the sig binds the exact bytes"
        );

        let other = wallet_of(0x22);
        let mut stolen = post(&w, REAL_GAME, REAL);
        stolen.winner = other.identity_key_hex().to_ascii_lowercase();
        assert_eq!(
            verify_proof_post(&stolen).unwrap_err(),
            ProofPostRefusal::SignatureInvalid,
            "a signature is not transferable"
        );
    }

    #[test]
    fn shape_refusals_are_named_and_cheap() {
        let w = wallet_of(0x23);
        let mut b = post(&w, REAL_GAME, b"{}");
        b.game_id = "zz".into();
        assert_eq!(
            verify_proof_post(&b).unwrap_err(),
            ProofPostRefusal::BadGameId
        );
        let mut b = post(&w, REAL_GAME, b"{}");
        b.winner = "04".repeat(33);
        assert_eq!(
            verify_proof_post(&b).unwrap_err(),
            ProofPostRefusal::BadWinner
        );
        let mut b = post(&w, REAL_GAME, b"{}");
        b.bundle_b64 = "@@@".into();
        assert_eq!(
            verify_proof_post(&b).unwrap_err(),
            ProofPostRefusal::BadBase64
        );
        let b = post(&w, REAL_GAME, &vec![0u8; PROOF_POST_MAX_BUNDLE_BYTES + 1]);
        assert_eq!(
            verify_proof_post(&b).unwrap_err(),
            ProofPostRefusal::BundleTooLarge
        );
        let mut b = post(&w, REAL_GAME, b"{}");
        b.sig_hex = "30".into();
        assert_eq!(
            verify_proof_post(&b).unwrap_err(),
            ProofPostRefusal::BadSignatureHex
        );
        assert_eq!(ProofPostRefusal::BundleTooLarge.status(), 413);
        assert_eq!(ProofPostRefusal::SignatureInvalid.status(), 422);
        assert_eq!(ProofPostRefusal::BadGameId.status(), 400);
    }

    #[test]
    fn the_sql_shapes_are_pinned() {
        assert_eq!(PROOF_POST_WRITE_SQL.matches('?').count(), 11);
        assert!(proof_posts_hands_sql(3).contains("gameId IN (?, ?, ?)"));
        assert!(proof_posts_hands_sql(1).contains("bundleValid = 1"));
    }
}
