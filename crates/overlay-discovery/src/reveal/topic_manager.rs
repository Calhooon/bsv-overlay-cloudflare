//! REVEAL Topic Manager — validates LOW break-glass reveal artifacts.
//!
//! See [`super`] for the full on-wire format. A single record type is
//! admitted: the `LOW/reveal/v2` `OP_RETURN` data carrier (6 pushes).
//! The parser is a self-contained port of the tower's
//! `break_glass::parse_reveal_artifact` (the overlay can't depend on the
//! watchtower crate; a small parser keyed to the same byte format is
//! correct) — same field layout, same strict length checks.
//!
//! Unlike `tm_low`, there is NO signature: a reveal is an unsigned public
//! fact anyone may publish. We validate the byte format only and extract
//! `(gameId, seat)`; the tower adjudicates genuineness downstream.

use async_trait::async_trait;
use bsv_rs::transaction::Transaction;
use overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use overlay_engine::types::{AdmittanceInstructions, ServiceMetadata, SubmitMode};
use tracing::{debug, warn};

/// The domain tag the app stamps (`REVEAL_ARTIFACT_TAG` in
/// `app/src/lib/stake.ts` / the tower's `break_glass.rs`). v2 =
/// self-contained (carries the peer scalars too).
pub const REVEAL_ARTIFACT_TAG: &[u8] = b"LOW/reveal/v2";
/// Number of minimal data pushes in a well-formed v2 artifact.
pub const REVEAL_FIELD_COUNT: usize = 6;
/// gameId push length (bytes).
pub const REVEAL_GAME_ID_LEN: usize = 32;
/// positions push length (bytes) — the seat's 5 final deck positions.
pub const REVEAL_POSITIONS_LEN: usize = 5;
/// own/peer scalar-bundle push length (bytes) — 5 × 32-byte scalars.
pub const REVEAL_SCALARS_LEN: usize = 160;

/// A decoded reveal artifact — one seat's opening published on-chain.
///
/// Mirror of `low_watchtower::break_glass::RevealArtifact` (kept
/// self-contained; the overlay only needs `game_id` + `seat` to key the
/// index, but the full opening is parsed so the format is validated
/// end-to-end and future consumers can reuse it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevealArtifact {
    /// The 32-byte game id.
    pub game_id: [u8; 32],
    /// The revealing seat: 0 = A, 1 = B.
    pub seat: u8,
    /// The 5 deck positions the seat's final hand occupies.
    pub positions: [u8; 5],
    /// The seat's 5 per-position OWN remask scalars (32 bytes each).
    pub scalars: [[u8; 32]; 5],
    /// The PEER (claimant) scalars for the same 5 positions.
    pub peer_scalars: [[u8; 32]; 5],
}

/// REVEAL Topic Manager — identifies admissible LOW reveal artifacts.
pub struct RevealTopicManager;

impl RevealTopicManager {
    /// Create a new REVEAL Topic Manager.
    pub fn new() -> Self {
        Self
    }
}

impl Default for RevealTopicManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait(?Send)]
impl TopicManager for RevealTopicManager {
    async fn identify_admissible_outputs(
        &self,
        beef: &[u8],
        _previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        _mode: SubmitMode,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        let mut outputs_to_admit = Vec::new();

        let tx = match Transaction::from_beef(beef, None) {
            Ok(tx) => tx,
            Err(e) => {
                return Err(TopicManagerError::InvalidBeef(e.to_string()));
            }
        };

        for (i, output) in tx.outputs.iter().enumerate() {
            match Self::validate_reveal_output(output) {
                Ok(true) => {
                    debug!("REVEAL: admitted output {i}");
                    outputs_to_admit.push(i as u32);
                }
                Ok(false) => {
                    // Not a reveal artifact (beacon P2PKH, change, …) — skip.
                }
                Err(e) => {
                    // A reveal-tagged OP_RETURN that is malformed — skip with
                    // reason so the index can't be spammed with junk.
                    debug!("REVEAL: output {i} skipped: {e}");
                }
            }
        }

        if outputs_to_admit.is_empty() {
            warn!("REVEAL: no outputs admitted");
        }

        Ok(AdmittanceInstructions {
            outputs_to_admit,
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/reveal_topic.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "REVEAL Topic Manager".to_string(),
            description: Some(
                "Indexes LOW break-glass reveal artifacts (LOW/reveal/v2 OP_RETURN) \
                 so the watchtower can look up reveals by (gameId, seat)."
                    .to_string(),
            ),
            ..Default::default()
        }
    }
}

impl RevealTopicManager {
    /// Validate a single output as a LOW reveal artifact.
    ///
    /// Returns `Ok(true)` if the output is a well-formed `LOW/reveal/v2`
    /// artifact, `Ok(false)` if it isn't a reveal output at all (not an
    /// `OP_RETURN`, or a different tag — the common case for beacon /
    /// change outputs), `Err` if it is reveal-TAGGED but malformed.
    pub fn validate_reveal_output(
        output: &bsv_rs::transaction::TransactionOutput,
    ) -> Result<bool, String> {
        let script = output.locking_script.to_binary();
        match parse_reveal_artifact_script(&script)? {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }
}

/// Walk minimal Bitcoin pushdata out of a byte slice → the pushed blobs,
/// in order, stopping at the first non-push opcode / a truncated push
/// (mirrors the app's `readPushes` and the tower's `read_pushes`).
fn read_pushes(bytes: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let op = bytes[i];
        i += 1;
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
                if i + 2 > bytes.len() {
                    return out;
                }
                let l = bytes[i] as usize | ((bytes[i + 1] as usize) << 8);
                i += 2;
                l
            }
            0x4e => {
                if i + 4 > bytes.len() {
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
        if i + len > bytes.len() {
            return out;
        }
        out.push(&bytes[i..i + len]);
        i += len;
    }
    out
}

/// Parse one output locking script for a `LOW/reveal/v2` artifact.
///
/// Three outcomes, so the topic manager can distinguish "not ours"
/// (skip silently) from "ours but malformed" (skip with reason):
///
/// - `Ok(None)`  — not an `OP_RETURN`, or an `OP_RETURN` whose first push
///   is not the `LOW/reveal/v2` tag (a beacon / change / foreign token).
/// - `Err(_)`    — reveal-TAGGED but the fields don't validate.
/// - `Ok(Some)`  — a well-formed reveal artifact.
pub fn parse_reveal_artifact_script(script: &[u8]) -> Result<Option<RevealArtifact>, String> {
    // OP_FALSE OP_RETURN (0x00 0x6a) or a bare OP_RETURN (0x6a).
    let rest = if script.len() >= 2 && script[0] == 0x00 && script[1] == 0x6a {
        &script[2..]
    } else if !script.is_empty() && script[0] == 0x6a {
        &script[1..]
    } else {
        return Ok(None); // not an OP_RETURN → not a reveal output
    };

    let data = read_pushes(rest);
    // First push must be our tag, else it's some other OP_RETURN.
    if data.is_empty() || data[0] != REVEAL_ARTIFACT_TAG {
        return Ok(None);
    }

    // From here the output is reveal-TAGGED — any shape error is a hard
    // reject (returned as Err) so junk can't enter the index.
    if data.len() < REVEAL_FIELD_COUNT {
        return Err(format!(
            "reveal: expected {REVEAL_FIELD_COUNT} data pushes, got {}",
            data.len()
        ));
    }
    let (game_id_b, seat_b, pos_b, scalars_b, peer_scalars_b) =
        (data[1], data[2], data[3], data[4], data[5]);

    if game_id_b.len() != REVEAL_GAME_ID_LEN {
        return Err(format!(
            "reveal: gameId must be {REVEAL_GAME_ID_LEN} bytes, got {}",
            game_id_b.len()
        ));
    }
    if seat_b.len() != 1 || (seat_b[0] != 0 && seat_b[0] != 1) {
        return Err("reveal: seat must be a single byte 0x00 or 0x01".into());
    }
    if pos_b.len() != REVEAL_POSITIONS_LEN {
        return Err(format!(
            "reveal: positions must be {REVEAL_POSITIONS_LEN} bytes, got {}",
            pos_b.len()
        ));
    }
    if scalars_b.len() != REVEAL_SCALARS_LEN {
        return Err(format!(
            "reveal: own scalars must be {REVEAL_SCALARS_LEN} bytes, got {}",
            scalars_b.len()
        ));
    }
    if peer_scalars_b.len() != REVEAL_SCALARS_LEN {
        return Err(format!(
            "reveal: peer scalars must be {REVEAL_SCALARS_LEN} bytes, got {}",
            peer_scalars_b.len()
        ));
    }

    let mut game_id = [0u8; 32];
    game_id.copy_from_slice(game_id_b);
    let mut positions = [0u8; 5];
    positions.copy_from_slice(pos_b);
    let split160 = |src: &[u8]| -> [[u8; 32]; 5] {
        let mut out = [[0u8; 32]; 5];
        for (k, s) in out.iter_mut().enumerate() {
            s.copy_from_slice(&src[k * 32..k * 32 + 32]);
        }
        out
    };

    Ok(Some(RevealArtifact {
        game_id,
        seat: seat_b[0],
        positions,
        scalars: split160(scalars_b),
        peer_scalars: split160(peer_scalars_b),
    }))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::TransactionOutput;

    /// Minimal Bitcoin pushdata for a byte blob (direct / OP_PUSHDATA1 /
    /// _2) — mirrors the app's `pushData` and the tower's test helper.
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

    /// The app's `revealArtifactScriptHex` in bytes (v2: own + peer scalars).
    pub(crate) fn artifact_script(
        game_id: &[u8; 32],
        seat: u8,
        positions: &[u8; 5],
        scalars: &[[u8; 32]; 5],
        peer_scalars: &[[u8; 32]; 5],
    ) -> Vec<u8> {
        let flat = |scs: &[[u8; 32]; 5]| -> Vec<u8> {
            let mut v = Vec::with_capacity(160);
            for sc in scs {
                v.extend_from_slice(sc);
            }
            v
        };
        let mut s = vec![0x00, 0x6a]; // OP_FALSE OP_RETURN
        s.extend(push_data(REVEAL_ARTIFACT_TAG));
        s.extend(push_data(game_id));
        s.extend(push_data(&[seat]));
        s.extend(push_data(positions));
        s.extend(push_data(&flat(scalars)));
        s.extend(push_data(&flat(peer_scalars)));
        s
    }

    /// A valid reveal `TransactionOutput` (0-sat OP_RETURN artifact).
    pub(crate) fn make_reveal_output(
        game_id: &[u8; 32],
        seat: u8,
        positions: &[u8; 5],
        scalars: &[[u8; 32]; 5],
        peer_scalars: &[[u8; 32]; 5],
    ) -> TransactionOutput {
        let script = artifact_script(game_id, seat, positions, scalars, peer_scalars);
        TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&script).unwrap(),
            change: false,
        }
    }

    /// A canonical valid artifact output for the shared test fixtures.
    fn valid_output() -> TransactionOutput {
        make_reveal_output(
            &[0x33u8; 32],
            1,
            &[0u8, 2, 4, 6, 8],
            &[[0x11u8; 32], [0x22; 32], [0x33; 32], [0x44; 32], [0x55; 32]],
            &[
                [0x66u8; 32],
                [0x77; 32],
                [0x88; 32],
                [0x99; 32],
                [0xaau8; 32],
            ],
        )
    }

    // ── Valid artifacts ──────────────────────────────────────────────────

    #[test]
    fn valid_reveal_admitted() {
        assert!(RevealTopicManager::validate_reveal_output(&valid_output()).unwrap());
    }

    #[test]
    fn bare_op_return_prefix_accepted() {
        // Same artifact but with a BARE OP_RETURN (0x6a) rather than
        // OP_FALSE OP_RETURN (0x00 0x6a).
        let mut full = artifact_script(
            &[0x01u8; 32],
            0,
            &[1u8, 2, 3, 4, 5],
            &[[0x01u8; 32]; 5],
            &[[0x02u8; 32]; 5],
        );
        // strip the leading OP_FALSE (0x00)
        assert_eq!(full[0], 0x00);
        full.remove(0);
        let output = TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&full).unwrap(),
            change: false,
        };
        assert!(RevealTopicManager::validate_reveal_output(&output).unwrap());
    }

    #[test]
    fn parses_game_id_and_seat() {
        let a = parse_reveal_artifact_script(&artifact_script(
            &[0xABu8; 32],
            1,
            &[9u8, 8, 7, 6, 5],
            &[[0x01u8; 32]; 5],
            &[[0x02u8; 32]; 5],
        ))
        .unwrap()
        .unwrap();
        assert_eq!(a.game_id, [0xABu8; 32]);
        assert_eq!(a.seat, 1);
        assert_eq!(a.positions, [9u8, 8, 7, 6, 5]);
    }

    // ── Not-a-reveal (skip silently, Ok(false)) ──────────────────────────

    #[test]
    fn p2pkh_beacon_not_admitted() {
        // A standard P2PKH (the reveal's beacon output) is not an OP_RETURN.
        let output = TransactionOutput {
            satoshis: Some(1000),
            locking_script: LockingScript::from_hex(
                "76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac",
            )
            .unwrap(),
            change: false,
        };
        assert!(!RevealTopicManager::validate_reveal_output(&output).unwrap());
    }

    #[test]
    fn foreign_op_return_not_admitted() {
        // An OP_RETURN carrying a different protocol tag — not ours.
        let mut s = vec![0x00, 0x6au8];
        s.extend(push_data(b"SOMETHINGELSE"));
        s.extend(push_data(&[0x11u8; 32]));
        let output = TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&s).unwrap(),
            change: false,
        };
        assert!(!RevealTopicManager::validate_reveal_output(&output).unwrap());
    }

    #[test]
    fn wrong_tag_version_not_admitted() {
        let mut s = vec![0x00, 0x6au8];
        s.extend(push_data(b"LOW/reveal/v1")); // superseded version
        s.extend(push_data(&[0x11u8; 32]));
        s.extend(push_data(&[0u8]));
        s.extend(push_data(&[0u8; 5]));
        s.extend(push_data(&[0u8; 160]));
        let output = TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&s).unwrap(),
            change: false,
        };
        assert!(!RevealTopicManager::validate_reveal_output(&output).unwrap());
    }

    #[test]
    fn empty_script_not_admitted() {
        let output = TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&[]).unwrap(),
            change: false,
        };
        assert!(!RevealTopicManager::validate_reveal_output(&output).unwrap());
    }

    // ── Reveal-tagged but malformed (hard reject, Err) ───────────────────

    fn tagged_with_pushes(pushes: &[Vec<u8>]) -> TransactionOutput {
        let mut s = vec![0x00, 0x6au8];
        for p in pushes {
            s.extend(push_data(p));
        }
        TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&s).unwrap(),
            change: false,
        }
    }

    #[test]
    fn too_few_pushes_rejected() {
        let out = tagged_with_pushes(&[
            REVEAL_ARTIFACT_TAG.to_vec(),
            vec![0x11; 32],
            vec![0x01],
            // missing positions / scalars / peer scalars
        ]);
        assert!(RevealTopicManager::validate_reveal_output(&out).is_err());
    }

    #[test]
    fn short_game_id_rejected() {
        let out = tagged_with_pushes(&[
            REVEAL_ARTIFACT_TAG.to_vec(),
            vec![0x11; 31], // 31, not 32
            vec![0x01],
            vec![0u8; 5],
            vec![0u8; 160],
            vec![0u8; 160],
        ]);
        assert!(RevealTopicManager::validate_reveal_output(&out).is_err());
    }

    #[test]
    fn bad_seat_rejected() {
        let out = tagged_with_pushes(&[
            REVEAL_ARTIFACT_TAG.to_vec(),
            vec![0x11; 32],
            vec![0x02], // not 0 or 1
            vec![0u8; 5],
            vec![0u8; 160],
            vec![0u8; 160],
        ]);
        assert!(RevealTopicManager::validate_reveal_output(&out).is_err());
    }

    #[test]
    fn short_positions_rejected() {
        let out = tagged_with_pushes(&[
            REVEAL_ARTIFACT_TAG.to_vec(),
            vec![0x11; 32],
            vec![0x01],
            vec![0u8; 4], // not 5
            vec![0u8; 160],
            vec![0u8; 160],
        ]);
        assert!(RevealTopicManager::validate_reveal_output(&out).is_err());
    }

    #[test]
    fn short_scalars_rejected() {
        let out = tagged_with_pushes(&[
            REVEAL_ARTIFACT_TAG.to_vec(),
            vec![0x11; 32],
            vec![0x01],
            vec![0u8; 5],
            vec![0u8; 128], // not 160
            vec![0u8; 160],
        ]);
        assert!(RevealTopicManager::validate_reveal_output(&out).is_err());
    }

    #[test]
    fn short_peer_scalars_rejected() {
        let out = tagged_with_pushes(&[
            REVEAL_ARTIFACT_TAG.to_vec(),
            vec![0x11; 32],
            vec![0x01],
            vec![0u8; 5],
            vec![0u8; 160],
            vec![0u8; 96], // not 160
        ]);
        assert!(RevealTopicManager::validate_reveal_output(&out).is_err());
    }

    // ── Golden mainnet fixture ───────────────────────────────────────────

    /// The captured REAL mainnet break-glass reveal tx (built by the fixed
    /// `stake.ts::publishOnChainReveal` path, WoC-accepted) —
    /// txid `a0e644db698f510db0d1e50b9fec7a2d72ce328a8a1b51dfea90e6ce6cbf4c24`.
    /// Same golden vector the tower's `break_glass.rs` locks. Output 0 is the
    /// `LOW/reveal/v2` OP_RETURN artifact; output 1 the beacon P2PKH.
    pub(crate) const GOLDEN_REVEAL_RAW: &str = "010000000143ee8ac505e1a71b5ed7352bc2700bf361e1fd776da5578b159f67c4f433a0c1020000006a47304402205c5d9ef2e31742172c3ea9e5eedf144941601532fb3ff5a2dd1990fbc79456f302202ac923630b21685f1466c910ccc41e73e16c7434894074f62b4903199f83e7714121023a4122cb1b8fb58c8ee35b1230c72cd482c5097d1273f7d4b889bed70a3116e4ffffffff030000000000000000fd7d01006a0d4c4f572f72657665616c2f76322066a950e5e22cb232210497896a73a65b7d95be6e5a55c0baf05cc8b69e4ffd1001010500020406084ca059abae85d00fcd811b2b71cdd8450f60be08059ba8b60028d7cbcaa5b0fd527843ec19d5d91cac341fa72f314be4da1a1de92f4c583872ed9fd9572c6e82f7691b38ab993898aaa3db185f2cfb52942edd1eaf6bba10605e49817df5faec1189773d537079785d77fac4806063e8944957ba7bb01d6c13ec5b85362c4272392ddf21303b88019f18eda04478e05ab3ecce0a51a4b75e0bd80a4726e9c5cafdd04ca03491ec22bc6e6c3560b162d922935883e910373a4bb0c555c62193a5f344a2a3692ed91dc189e53afa050292d9c7bd725d57a884843264ba0274184d0ff98a3c7a9003e825cc1ecdfd7f731412a59c2fe005200886b58b5f47715270a9853abd0336af544cf0b568301b237ed85fec0f3ca1a5b15bb81daf8b5696a78462d4edb35d645d096a6b56c0a048a1c9f2bcbfd8d62966d928aeca962bc24401985832e8030000000000001976a914ec48fcae21b11476443fc695aa3b2bc574121ac088ac05140000000000001976a914ec48fcae21b11476443fc695aa3b2bc574121ac088ac00000000";

    /// The gameId encoded in the golden reveal artifact.
    pub(crate) const GOLDEN_GAME_ID_HEX: &str =
        "66a950e5e22cb232210497896a73a65b7d95be6e5a55c0baf05cc8b69e4ffd10";
    /// The seat encoded in the golden reveal artifact (B).
    pub(crate) const GOLDEN_SEAT: u8 = 1;

    #[test]
    fn golden_mainnet_reveal_output0_admitted() {
        let tx = Transaction::from_hex(GOLDEN_REVEAL_RAW).expect("mainnet reveal parses");

        // Output 0 = the LOW/reveal/v2 OP_RETURN artifact → admitted, and
        // it carries the expected (gameId, seat).
        assert!(
            RevealTopicManager::validate_reveal_output(&tx.outputs[0]).unwrap(),
            "output 0 must be an admissible reveal artifact"
        );
        let artifact = parse_reveal_artifact_script(&tx.outputs[0].locking_script.to_binary())
            .unwrap()
            .unwrap();
        assert_eq!(hex::encode(artifact.game_id), GOLDEN_GAME_ID_HEX);
        assert_eq!(artifact.seat, GOLDEN_SEAT);
        assert_eq!(artifact.positions, [0u8, 2, 4, 6, 8]);

        // The other outputs (beacon + change P2PKH) are NOT reveal artifacts.
        for o in &tx.outputs[1..] {
            assert!(
                !RevealTopicManager::validate_reveal_output(o).unwrap(),
                "beacon / change P2PKH must not be admitted"
            );
        }
    }

    // ── Whole-transaction admission via BEEF ─────────────────────────────

    #[tokio::test]
    async fn identify_admissible_outputs_over_beef() {
        use bsv_rs::transaction::{Transaction as Tx, TransactionInput};

        let reveal = valid_output();
        // A beacon-style P2PKH in the middle — must be skipped.
        let p2pkh = TransactionOutput {
            satoshis: Some(546),
            locking_script: LockingScript::from_hex(
                "76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac",
            )
            .unwrap(),
            change: false,
        };

        let mut tx = Tx::new();
        tx.add_input(TransactionInput::new("00".repeat(32), 0))
            .unwrap();
        tx.add_output(p2pkh).unwrap();
        tx.add_output(reveal).unwrap();
        let beef = tx.to_beef(true).expect("BEEF serialization");

        let mgr = RevealTopicManager::new();
        let instructions = mgr
            .identify_admissible_outputs(&beef, &[], None, SubmitMode::HistoricalTxNoSpv)
            .await
            .unwrap();
        // Only the reveal artifact (index 1) is admitted.
        assert_eq!(instructions.outputs_to_admit, vec![1]);
    }

    #[tokio::test]
    async fn topic_manager_trait_works() {
        let mgr = RevealTopicManager::new();
        let meta = mgr.get_metadata().await;
        assert_eq!(meta.name, "REVEAL Topic Manager");
        assert!(!mgr.get_documentation().await.is_empty());
    }
}
