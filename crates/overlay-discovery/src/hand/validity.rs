//! `tm_hand` marker SIGNATURE VALIDITY — decoded ONCE at admission and
//! latched onto the row as `hand_markers.rowValid` (brain-cutover M1,
//! bsv-low docs/PLAN-BRAIN-CUTOVER-2026-08.md; the #283/#362 latch family —
//! see `result::validity` for the serving contract this family carries and
//! `potparty::validity` for the original doctrine).
//!
//! Before this module hand rows were verified NOWHERE server-side: the
//! client's `verifyHandRow` ran per row, per visit, on the main thread (a
//! #401 freeze contributor on Your games), and the app-layer never touched
//! `hand_markers` at all. The latch is what lets `/results` join hands in
//! M2 with zero read-time crypto.
//!
//! Recipe = the client's `verifyHandRow`, byte for byte: BRC-42/43 'anyone'
//! verification under the row's OWN claimed identity, protocol
//! `[1,'low hand']`, keyID = the lowercase gameId, over the challenge
//! `LOW-hand\nv1\ngid=…\nid=…\npot=…\ncards=…`. NB the challenge binds
//! `cardsHex` VERBATIM (lowercased, NOT canonical-sorted) — a deliberate
//! divergence from the result challenge, preserved on both sides and pinned
//! by the frozen golden (`handMarker.signedGolden.test.ts`).

use crate::hand::storage::HandRecord;
use crate::result::validity::anyone_sig_verifies;

/// BRC-42/43 protocol for hand markers — the client's
/// `HAND_PROTOCOL = [1,'low hand']`.
pub fn hand_protocol() -> bsv_rs::wallet::Protocol {
    bsv_rs::wallet::Protocol::new(bsv_rs::wallet::SecurityLevel::App, "low hand")
}

/// The canonical signed challenge — byte-identical to
/// `handMarker.ts::handChallenge` (all fields lowercased; cards verbatim).
pub fn hand_challenge_bytes(
    game_id_lc: &str,
    identity_lc: &str,
    pot_lc: &str,
    cards_hex_lc: &str,
) -> Vec<u8> {
    format!("LOW-hand\nv1\ngid={game_id_lc}\nid={identity_lc}\npot={pot_lc}\ncards={cards_hex_lc}")
        .into_bytes()
}

/// THE LATCH — does this row's signature verify under its own claimed
/// identity? A missing sig or malformed anything is `false`, never a panic
/// and never an error that could fail the admission (the client's
/// `verifyHandRow` returns exactly `false` for a `null` sig).
pub fn row_valid(r: &HandRecord) -> bool {
    let Some(sig_hex) = r.sig_hex.as_deref() else {
        return false;
    };
    let game_lc = r.game_id.to_ascii_lowercase();
    let identity_lc = r.identity.to_ascii_lowercase();
    let challenge = hand_challenge_bytes(
        &game_lc,
        &identity_lc,
        &r.pot_txid.to_ascii_lowercase(),
        &r.cards_hex.to_ascii_lowercase(),
    );
    anyone_sig_verifies(&identity_lc, &game_lc, &challenge, sig_hex, hand_protocol())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The SIGNED cross-repo golden — a frozen artifact of the REAL client
    /// producer (`app/src/lib/handMarker.signedGolden.test.ts`,
    /// deterministic PrivateKey(1), RFC6979).
    const SIGNED_HAND: &str = "006a0b4c4f572f68616e642f7631201111111111111111111111111111111111111111111111111111111111111111210279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f8179820222222222222222222222222222222222222222222222222222222222222222205000102030446304402200d1a1461fc2ae2190466e3cfc6f1a042d8ccf379af035e5b7d8281a835a84fc9022034fc67ce95220b0ad2eb172899cfbceac10bee61da2816e3cc98cf237eb15b42";

    /// Golden hex → the stored record shape, THROUGH the real parser and the
    /// EXACT field mapping of the production writer
    /// (`lookup_service.rs::output_admitted`).
    fn record() -> HandRecord {
        let script = hex::decode(SIGNED_HAND).expect("golden decodes");
        let m = crate::hand::parse_hand_marker(&script).expect("golden parses");
        HandRecord {
            game_id: hex::encode(m.game_id),
            identity: hex::encode(&m.identity_key),
            pot_txid: hex::encode(m.pot_txid),
            cards_hex: hex::encode(m.cards),
            txid: "aa".repeat(32),
            output_index: 0,
            sig_hex: Some(hex::encode(&m.sig)),
        }
    }

    #[test]
    fn golden_client_hand_marker_is_valid_server_side() {
        assert!(row_valid(&record()));
    }

    #[test]
    fn tampered_pot_binding_is_invalid() {
        let mut r = record();
        r.pot_txid = "44".repeat(32);
        assert!(!row_valid(&r));
    }

    #[test]
    fn missing_sig_is_invalid_never_a_panic() {
        let mut r = record();
        r.sig_hex = None;
        assert!(!row_valid(&r));
    }

    #[test]
    fn uppercase_fields_verify_identically_lowercased() {
        let mut r = record();
        r.game_id = r.game_id.to_ascii_uppercase();
        r.identity = r.identity.to_ascii_uppercase();
        r.cards_hex = r.cards_hex.to_ascii_uppercase();
        assert!(row_valid(&r));
    }
}
