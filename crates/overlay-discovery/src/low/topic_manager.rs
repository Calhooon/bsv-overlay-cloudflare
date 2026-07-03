//! LOW Topic Manager — validates LOW poker lobby PushDrop outputs.
//!
//! See [`super`] for the full on-wire format. Two record types are
//! admitted: TABLE_OPEN (`LOW.table.v1`, 8 fields) and GAME_UTXO
//! (`LOW.gameutxo.v1`, 6 fields). Field checks mirror
//! `ship::topic_manager` — silently skip outputs that aren't LOW
//! tokens, hard-reject malformed LOW tokens, and require the same
//! BRC-42 signature/locking-key linkage SHIP uses.

use async_trait::async_trait;
use bsv_rs::script::templates::PushDrop;
use bsv_rs::transaction::Transaction;
use overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use overlay_engine::types::{AdmittanceInstructions, ServiceMetadata, SubmitMode};
use tracing::{debug, warn};

use crate::validation::is_token_signature_correctly_linked;

/// Protocol tag for TABLE_OPEN records (PushDrop field\[0\]).
pub const LOW_TABLE_TAG: &[u8] = b"LOW.table.v1";
/// Protocol tag for GAME_UTXO pointer records (PushDrop field\[0\]).
pub const LOW_GAMEUTXO_TAG: &[u8] = b"LOW.gameutxo.v1";
/// Field count for a TABLE_OPEN token (tag..signature inclusive).
pub const LOW_TABLE_FIELD_COUNT: usize = 8;
/// Field count for a GAME_UTXO token (tag..signature inclusive).
pub const LOW_GAMEUTXO_FIELD_COUNT: usize = 6;
/// Upper bound on the relay URL field, in bytes.
pub const LOW_MAX_RELAY_URL_BYTES: usize = 512;
/// Short protocol name passed to `is_token_signature_correctly_linked`
/// (maps to BRC-43 protocol `[2, "low poker lobby"]`, key ID `"1"`).
pub const LOW_PROTOCOL_NAME: &str = "LOW";

/// LOW Topic Manager — identifies admissible LOW poker lobby outputs.
pub struct LowTopicManager;

impl LowTopicManager {
    /// Create a new LOW Topic Manager.
    pub fn new() -> Self {
        Self
    }
}

impl Default for LowTopicManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait(?Send)]
impl TopicManager for LowTopicManager {
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
            match Self::validate_low_output(output) {
                Ok(true) => {
                    debug!("LOW: admitted output {i}");
                    outputs_to_admit.push(i as u32);
                }
                Ok(false) => {
                    // Not a LOW output — skip silently (common for change etc.)
                }
                Err(e) => {
                    // Malformed LOW token or non-PushDrop — skip with reason
                    debug!("LOW: output {i} skipped: {e}");
                }
            }
        }

        if outputs_to_admit.is_empty() {
            warn!("LOW: no outputs admitted");
        }

        Ok(AdmittanceInstructions {
            outputs_to_admit,
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/low_topic.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "LOW Topic Manager".to_string(),
            description: Some(
                "Manages LOW poker lobby tokens: open-table announcements and \
                 live game-UTXO pointers."
                    .to_string(),
            ),
            ..Default::default()
        }
    }
}

impl LowTopicManager {
    /// Validate a single output as a LOW lobby token (either record type).
    ///
    /// Returns `Ok(true)` if valid, `Ok(false)` if not a LOW output (or
    /// signature/linkage fails — mirrors SHIP), `Err` if a LOW-tagged
    /// token is malformed.
    pub fn validate_low_output(
        output: &bsv_rs::transaction::TransactionOutput,
    ) -> Result<bool, String> {
        // Decode PushDrop
        let pushdrop =
            PushDrop::decode(&output.locking_script).map_err(|e| format!("not a PushDrop: {e}"))?;

        if pushdrop.fields.is_empty() {
            return Ok(false);
        }

        match pushdrop.fields[0].as_slice() {
            tag if tag == LOW_TABLE_TAG => Self::validate_table_open(&pushdrop),
            tag if tag == LOW_GAMEUTXO_TAG => Self::validate_game_utxo(&pushdrop),
            // Different protocol entirely — not our token.
            _ => Ok(false),
        }
    }

    /// Validate the shared prefix (identity key + gameId) and signature
    /// linkage for either record type.
    ///
    /// `fields` is the full field list including the trailing signature.
    fn validate_common(
        pushdrop: &PushDrop,
        record_name: &str,
    ) -> Result<bool, String> {
        // Field[1]: host identity key (33-byte compressed pubkey)
        if pushdrop.fields[1].len() != 33 {
            return Err(format!("{record_name}: identity key must be 33 bytes"));
        }

        // Field[2]: gameId (32 bytes)
        if pushdrop.fields[2].len() != 32 {
            return Err(format!("{record_name}: gameId must be 32 bytes"));
        }

        // Last field: signature — verify via BRC-42 key derivation + ECDSA
        let sig = &pushdrop.fields[pushdrop.fields.len() - 1];
        if sig.is_empty() {
            return Err(format!("{record_name}: empty signature"));
        }

        // Verify the ECDSA signature links the identity key to the locking
        // key (same procedure as SHIP; protocol "LOW" → "low poker lobby").
        match is_token_signature_correctly_linked(
            &pushdrop.locking_public_key,
            &pushdrop.fields[1],
            &pushdrop.fields,
            LOW_PROTOCOL_NAME,
        ) {
            Ok(true) => Ok(true),
            Ok(false) => {
                warn!("{record_name}: output skipped: signature/key linkage failed");
                Ok(false)
            }
            Err(e) => {
                warn!("{record_name}: output skipped: signature verification error: {e}");
                Ok(false)
            }
        }
    }

    /// Validate a TABLE_OPEN token (tag already matched).
    fn validate_table_open(pushdrop: &PushDrop) -> Result<bool, String> {
        if pushdrop.fields.len() != LOW_TABLE_FIELD_COUNT {
            return Err(format!(
                "TABLE_OPEN requires {LOW_TABLE_FIELD_COUNT} fields, got {}",
                pushdrop.fields.len()
            ));
        }

        // Field[3]: stake satoshis (8-byte LE u64)
        if pushdrop.fields[3].len() != 8 {
            return Err("TABLE_OPEN: stake sats must be 8 bytes (LE u64)".into());
        }

        // Field[4]: rules hash (32 bytes)
        if pushdrop.fields[4].len() != 32 {
            return Err("TABLE_OPEN: rules hash must be 32 bytes".into());
        }

        // Field[5]: relay URL — bounded UTF-8, https:// or wss:// only
        let relay_bytes = &pushdrop.fields[5];
        if relay_bytes.is_empty() || relay_bytes.len() > LOW_MAX_RELAY_URL_BYTES {
            return Err(format!(
                "TABLE_OPEN: relay URL must be 1..={LOW_MAX_RELAY_URL_BYTES} bytes"
            ));
        }
        let relay = std::str::from_utf8(relay_bytes)
            .map_err(|e| format!("TABLE_OPEN: relay URL not valid UTF-8: {e}"))?;
        if !(relay.starts_with("https://") || relay.starts_with("wss://")) {
            return Err(format!(
                "TABLE_OPEN: relay URL must start with https:// or wss://, got: {relay}"
            ));
        }

        // Field[6]: expiry block height (4-byte LE u32)
        if pushdrop.fields[6].len() != 4 {
            return Err("TABLE_OPEN: expiry height must be 4 bytes (LE u32)".into());
        }

        Self::validate_common(pushdrop, "TABLE_OPEN")
    }

    /// Validate a GAME_UTXO pointer token (tag already matched).
    fn validate_game_utxo(pushdrop: &PushDrop) -> Result<bool, String> {
        if pushdrop.fields.len() != LOW_GAMEUTXO_FIELD_COUNT {
            return Err(format!(
                "GAME_UTXO requires {LOW_GAMEUTXO_FIELD_COUNT} fields, got {}",
                pushdrop.fields.len()
            ));
        }

        // Field[3]: pot txid (32 bytes)
        if pushdrop.fields[3].len() != 32 {
            return Err("GAME_UTXO: pot txid must be 32 bytes".into());
        }

        // Field[4]: pot vout (4-byte LE u32)
        if pushdrop.fields[4].len() != 4 {
            return Err("GAME_UTXO: pot vout must be 4 bytes (LE u32)".into());
        }

        Self::validate_common(pushdrop, "GAME_UTXO")
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use bsv_rs::primitives::ec::{PrivateKey, PublicKey};
    use bsv_rs::script::templates::PushDrop as PushDropTemplate;
    use bsv_rs::transaction::TransactionOutput;
    use bsv_rs::wallet::{
        Counterparty, CreateSignatureArgs, GetPublicKeyArgs, ProtoWallet, Protocol, SecurityLevel,
    };

    /// The BRC-43 protocol used for LOW token signing (matches the "LOW"
    /// arm in `validation::is_token_signature_correctly_linked`).
    fn low_protocol() -> Protocol {
        Protocol::new(SecurityLevel::Counterparty, "low poker lobby")
    }

    /// Sign `data_fields` with the host key and return the full PushDrop
    /// output using the correctly BRC-42-linked locking key.
    /// Same technique as SHIP's `make_signed_ship_output`.
    pub(crate) fn make_signed_low_output(
        signer_key: &PrivateKey,
        data_fields: Vec<Vec<u8>>,
    ) -> TransactionOutput {
        let signer_wallet = ProtoWallet::new(Some(signer_key.clone()));

        // Concatenate data fields for signing
        let data: Vec<u8> = data_fields.iter().flat_map(|f| f.iter().copied()).collect();

        // Sign with counterparty = 'anyone' (matches SHIP)
        let sig_result = signer_wallet
            .create_signature(CreateSignatureArgs {
                data: Some(data),
                hash_to_directly_sign: None,
                protocol_id: low_protocol(),
                key_id: "1".to_string(),
                counterparty: Some(Counterparty::Anyone),
            })
            .unwrap();

        // Derive the locking key: signer's own derived key (for_self = true)
        let locking_key_hex = signer_wallet
            .get_public_key(GetPublicKeyArgs {
                identity_key: false,
                protocol_id: Some(low_protocol()),
                key_id: Some("1".to_string()),
                counterparty: Some(Counterparty::Anyone),
                for_self: Some(true),
            })
            .unwrap()
            .public_key;
        let locking_key = PublicKey::from_hex(&locking_key_hex).unwrap();

        let mut all_fields = data_fields;
        all_fields.push(sig_result.signature);

        let pushdrop = PushDropTemplate::new(locking_key, all_fields);
        TransactionOutput {
            satoshis: Some(1),
            locking_script: pushdrop.lock(),
            change: false,
        }
    }

    /// Data fields (unsigned) for a valid TABLE_OPEN.
    pub(crate) fn table_open_data_fields(
        signer_key: &PrivateKey,
        stake_sats: u64,
        relay_url: &str,
        expiry_height: u32,
    ) -> Vec<Vec<u8>> {
        let identity = ProtoWallet::new(Some(signer_key.clone())).identity_key_hex();
        vec![
            LOW_TABLE_TAG.to_vec(),
            hex::decode(identity).unwrap(),
            [0x11u8; 32].to_vec(),                    // gameId
            stake_sats.to_le_bytes().to_vec(),        // stake sats (8B LE)
            [0x22u8; 32].to_vec(),                    // rules hash
            relay_url.as_bytes().to_vec(),            // relay URL
            expiry_height.to_le_bytes().to_vec(),     // expiry height (4B LE)
        ]
    }

    /// Data fields (unsigned) for a valid GAME_UTXO pointer.
    pub(crate) fn game_utxo_data_fields(signer_key: &PrivateKey, pot_vout: u32) -> Vec<Vec<u8>> {
        let identity = ProtoWallet::new(Some(signer_key.clone())).identity_key_hex();
        vec![
            LOW_GAMEUTXO_TAG.to_vec(),
            hex::decode(identity).unwrap(),
            [0x11u8; 32].to_vec(),                // gameId
            [0x33u8; 32].to_vec(),                // pot txid
            pot_vout.to_le_bytes().to_vec(),      // pot vout (4B LE)
        ]
    }

    /// Build a raw (garbage-signed) PushDrop output with a random locking
    /// key. Used for tests that exercise field-level validation before the
    /// signature check.
    fn make_raw_low_output(fields: Vec<Vec<u8>>) -> TransactionOutput {
        let locking_key = PublicKey::from_private_key(&PrivateKey::random());
        let pushdrop = PushDropTemplate::new(locking_key, fields);
        TransactionOutput {
            satoshis: Some(1),
            locking_script: pushdrop.lock(),
            change: false,
        }
    }

    fn dummy_identity_key() -> Vec<u8> {
        PublicKey::from_private_key(&PrivateKey::random())
            .to_compressed()
            .to_vec()
    }

    // ── Valid tokens ─────────────────────────────────────────────────────

    #[test]
    fn valid_table_open_admitted() {
        let signer = PrivateKey::random();
        let fields = table_open_data_fields(&signer, 1000, "https://low-relay.example.com", 900000);
        let output = make_signed_low_output(&signer, fields);
        let result = LowTopicManager::validate_low_output(&output);
        assert!(result.is_ok(), "expected Ok, got: {result:?}");
        assert!(result.unwrap(), "valid TABLE_OPEN must be admitted");
    }

    #[test]
    fn valid_game_utxo_admitted() {
        let signer = PrivateKey::random();
        let fields = game_utxo_data_fields(&signer, 1);
        let output = make_signed_low_output(&signer, fields);
        let result = LowTopicManager::validate_low_output(&output);
        assert!(result.is_ok(), "expected Ok, got: {result:?}");
        assert!(result.unwrap(), "valid GAME_UTXO must be admitted");
    }

    #[test]
    fn wss_relay_url_accepted() {
        let signer = PrivateKey::random();
        let fields = table_open_data_fields(&signer, 500, "wss://relay.example.com", 900000);
        let output = make_signed_low_output(&signer, fields);
        assert!(LowTopicManager::validate_low_output(&output).unwrap());
    }

    // ── Wrong tag ────────────────────────────────────────────────────────

    #[test]
    fn wrong_tag_not_admitted() {
        let signer = PrivateKey::random();
        let mut fields = table_open_data_fields(&signer, 1000, "https://r.example.com", 900000);
        fields[0] = b"LOW.table.v2".to_vec(); // unknown version
        let output = make_signed_low_output(&signer, fields);
        // Unknown tag → not our token → Ok(false), no error
        assert!(!LowTopicManager::validate_low_output(&output).unwrap());
    }

    #[test]
    fn ship_token_not_admitted() {
        let output = make_raw_low_output(vec![
            b"SHIP".to_vec(),
            dummy_identity_key(),
            b"https://example.com".to_vec(),
            b"tm_test".to_vec(),
            vec![0x30],
        ]);
        assert!(!LowTopicManager::validate_low_output(&output).unwrap());
    }

    // ── Truncated / malformed fields ─────────────────────────────────────

    #[test]
    fn table_open_wrong_field_count_rejected() {
        let signer = PrivateKey::random();
        let mut fields = table_open_data_fields(&signer, 1000, "https://r.example.com", 900000);
        fields.pop(); // drop expiry — now 6 data fields + sig = 7 total
        let output = make_signed_low_output(&signer, fields);
        assert!(LowTopicManager::validate_low_output(&output).is_err());
    }

    #[test]
    fn table_open_short_identity_key_rejected() {
        let signer = PrivateKey::random();
        let mut fields = table_open_data_fields(&signer, 1000, "https://r.example.com", 900000);
        fields[1] = vec![0x02, 0x03]; // 2 bytes, not 33
        let output = make_signed_low_output(&signer, fields);
        assert!(LowTopicManager::validate_low_output(&output).is_err());
    }

    #[test]
    fn table_open_short_game_id_rejected() {
        let signer = PrivateKey::random();
        let mut fields = table_open_data_fields(&signer, 1000, "https://r.example.com", 900000);
        fields[2] = vec![0x11; 31]; // 31 bytes, not 32
        let output = make_signed_low_output(&signer, fields);
        assert!(LowTopicManager::validate_low_output(&output).is_err());
    }

    #[test]
    fn table_open_short_stake_rejected() {
        let signer = PrivateKey::random();
        let mut fields = table_open_data_fields(&signer, 1000, "https://r.example.com", 900000);
        fields[3] = 1000u32.to_le_bytes().to_vec(); // 4 bytes, not 8
        let output = make_signed_low_output(&signer, fields);
        assert!(LowTopicManager::validate_low_output(&output).is_err());
    }

    #[test]
    fn table_open_short_rules_hash_rejected() {
        let signer = PrivateKey::random();
        let mut fields = table_open_data_fields(&signer, 1000, "https://r.example.com", 900000);
        fields[4] = vec![0x22; 16];
        let output = make_signed_low_output(&signer, fields);
        assert!(LowTopicManager::validate_low_output(&output).is_err());
    }

    #[test]
    fn table_open_http_relay_url_rejected() {
        let signer = PrivateKey::random();
        let fields = table_open_data_fields(&signer, 1000, "http://r.example.com", 900000);
        let output = make_signed_low_output(&signer, fields);
        assert!(LowTopicManager::validate_low_output(&output).is_err());
    }

    #[test]
    fn table_open_oversize_relay_url_rejected() {
        let signer = PrivateKey::random();
        let long_url = format!("https://{}.example.com", "a".repeat(LOW_MAX_RELAY_URL_BYTES));
        let fields = table_open_data_fields(&signer, 1000, &long_url, 900000);
        let output = make_signed_low_output(&signer, fields);
        assert!(LowTopicManager::validate_low_output(&output).is_err());
    }

    #[test]
    fn table_open_short_expiry_rejected() {
        let signer = PrivateKey::random();
        let mut fields = table_open_data_fields(&signer, 1000, "https://r.example.com", 900000);
        fields[6] = vec![0x01, 0x02]; // 2 bytes, not 4
        let output = make_signed_low_output(&signer, fields);
        assert!(LowTopicManager::validate_low_output(&output).is_err());
    }

    #[test]
    fn game_utxo_wrong_field_count_rejected() {
        let signer = PrivateKey::random();
        let mut fields = game_utxo_data_fields(&signer, 0);
        fields.pop(); // drop pot vout — 4 data fields + sig = 5 total
        let output = make_signed_low_output(&signer, fields);
        assert!(LowTopicManager::validate_low_output(&output).is_err());
    }

    #[test]
    fn game_utxo_short_pot_txid_rejected() {
        let signer = PrivateKey::random();
        let mut fields = game_utxo_data_fields(&signer, 0);
        fields[3] = vec![0x33; 20];
        let output = make_signed_low_output(&signer, fields);
        assert!(LowTopicManager::validate_low_output(&output).is_err());
    }

    #[test]
    fn game_utxo_short_pot_vout_rejected() {
        let signer = PrivateKey::random();
        let mut fields = game_utxo_data_fields(&signer, 0);
        fields[4] = vec![0x00]; // 1 byte, not 4
        let output = make_signed_low_output(&signer, fields);
        assert!(LowTopicManager::validate_low_output(&output).is_err());
    }

    #[test]
    fn non_pushdrop_output_rejected() {
        let output = TransactionOutput {
            satoshis: Some(1000),
            locking_script: bsv_rs::script::Script::from_hex(
                "76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac",
            )
            .unwrap()
            .into(),
            change: false,
        };
        assert!(LowTopicManager::validate_low_output(&output).is_err());
    }

    // ── Bad signature / wrong linkage ────────────────────────────────────

    #[test]
    fn garbage_signature_not_admitted() {
        let signer = PrivateKey::random();
        let mut fields = table_open_data_fields(&signer, 1000, "https://r.example.com", 900000);
        fields.push(vec![0x30, 0x44, 0x02, 0x20]); // garbage DER-like bytes
        let output = make_raw_low_output(fields);
        // Bad signature → skipped (Ok(false)), not error — mirrors SHIP
        assert!(!LowTopicManager::validate_low_output(&output).unwrap());
    }

    #[test]
    fn empty_signature_not_admitted() {
        let signer = PrivateKey::random();
        let mut fields = table_open_data_fields(&signer, 1000, "https://r.example.com", 900000);
        fields.push(vec![]); // empty signature field
        let output = make_raw_low_output(fields);
        // Depending on how the PushDrop template encodes an empty push this
        // is either a hard Err (empty sig) or Ok(false) (linkage failure) —
        // both mean NOT admitted, which is the invariant that matters.
        assert!(
            !LowTopicManager::validate_low_output(&output).unwrap_or(false),
            "empty signature must not be admitted"
        );
    }

    #[test]
    fn mismatched_identity_key_not_admitted() {
        // Signer signs with their key but claims to be an impostor in field[1]
        let signer = PrivateKey::random();
        let impostor = PrivateKey::random();
        let impostor_id = PublicKey::from_private_key(&impostor)
            .to_compressed()
            .to_vec();

        let mut fields = table_open_data_fields(&signer, 1000, "https://r.example.com", 900000);
        fields[1] = impostor_id;
        let output = make_signed_low_output(&signer, fields);
        // Signature made by signer, identity claims impostor → linkage fails
        assert!(!LowTopicManager::validate_low_output(&output).unwrap());
    }

    #[test]
    fn wrong_locking_key_not_admitted() {
        // Correctly signed fields but locked to a random key (wrong BRC-42
        // linkage): rebuild the PushDrop with a different locking key.
        let signer = PrivateKey::random();
        let fields = table_open_data_fields(&signer, 1000, "https://r.example.com", 900000);
        let good = make_signed_low_output(&signer, fields);
        let decoded = PushDrop::decode(&good.locking_script).unwrap();

        let wrong_key = PublicKey::from_private_key(&PrivateKey::random());
        let relocked = PushDropTemplate::new(wrong_key, decoded.fields);
        let output = TransactionOutput {
            satoshis: Some(1),
            locking_script: relocked.lock(),
            change: false,
        };
        assert!(!LowTopicManager::validate_low_output(&output).unwrap());
    }

    #[test]
    fn tampered_stake_not_admitted() {
        // Sign a 1000-sat table, then bump the stake field post-signature.
        let signer = PrivateKey::random();
        let fields = table_open_data_fields(&signer, 1000, "https://r.example.com", 900000);
        let good = make_signed_low_output(&signer, fields);
        let mut decoded = PushDrop::decode(&good.locking_script).unwrap();
        decoded.fields[3] = 1_000_000u64.to_le_bytes().to_vec();

        let relocked = PushDropTemplate::new(decoded.locking_public_key, decoded.fields);
        let output = TransactionOutput {
            satoshis: Some(1),
            locking_script: relocked.lock(),
            change: false,
        };
        assert!(!LowTopicManager::validate_low_output(&output).unwrap());
    }

    // ── Whole-transaction admission via BEEF ─────────────────────────────

    #[tokio::test]
    async fn identify_admissible_outputs_over_beef() {
        use bsv_rs::transaction::{Transaction, TransactionInput};

        let signer = PrivateKey::random();
        let table = make_signed_low_output(
            &signer,
            table_open_data_fields(&signer, 2500, "https://r.example.com", 910000),
        );
        let pointer = make_signed_low_output(&signer, game_utxo_data_fields(&signer, 0));
        // A non-LOW output in the middle — must be skipped.
        let p2pkh = TransactionOutput {
            satoshis: Some(546),
            locking_script: bsv_rs::script::Script::from_hex(
                "76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac",
            )
            .unwrap()
            .into(),
            change: false,
        };

        let mut tx = Transaction::new();
        tx.add_input(TransactionInput::new("00".repeat(32), 0))
            .unwrap();
        tx.add_output(table).unwrap();
        tx.add_output(p2pkh).unwrap();
        tx.add_output(pointer).unwrap();
        let beef = tx.to_beef(true).expect("BEEF serialization");

        let mgr = LowTopicManager::new();
        let instructions = mgr
            .identify_admissible_outputs(&beef, &[], None, SubmitMode::HistoricalTxNoSpv)
            .await
            .unwrap();
        assert_eq!(instructions.outputs_to_admit, vec![0, 2]);
    }

    #[tokio::test]
    async fn topic_manager_trait_works() {
        let mgr = LowTopicManager::new();
        let meta = mgr.get_metadata().await;
        assert_eq!(meta.name, "LOW Topic Manager");

        let docs = mgr.get_documentation().await;
        assert!(!docs.is_empty());
    }
}
