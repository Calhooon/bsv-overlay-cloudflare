//! SHIP Topic Manager -- validates SHIP advertisement PushDrop outputs.
//!
//! SHIP (Service Host Interconnect Protocol) advertisements are PushDrop outputs
//! with exactly 5 fields:
//! 1. Protocol identifier: "SHIP"
//! 2. Identity key: 33-byte compressed secp256k1 pubkey
//! 3. Advertised URI: must pass `is_advertisable_uri()`
//! 4. Topic name: must start with "tm_" and pass BRC-87 validation
//! 5. ECDSA signature: linking identity key to locking key
//!
//! Ported from `~/bsv/overlay-discovery-services/src/SHIP/SHIPTopicManager.ts`.

use async_trait::async_trait;
use bsv_rs::script::templates::PushDrop;
use bsv_rs::transaction::Transaction;
use overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use overlay_engine::types::{AdmittanceInstructions, ServiceMetadata, SubmitMode};
use tracing::{debug, warn};

use crate::validation::{
    is_advertisable_uri, is_token_signature_correctly_linked, is_valid_topic_or_service_name,
};

/// SHIP Topic Manager -- identifies admissible SHIP advertisement outputs.
pub struct SHIPTopicManager;

impl SHIPTopicManager {
    /// Create a new SHIP Topic Manager.
    pub fn new() -> Self {
        Self
    }
}

impl Default for SHIPTopicManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait(?Send)]
impl TopicManager for SHIPTopicManager {
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
            match Self::validate_ship_output(output) {
                Ok(true) => {
                    debug!("SHIP: admitted output {i}");
                    outputs_to_admit.push(i as u32);
                }
                Ok(false) => {
                    // Not a SHIP output -- skip silently (common for non-SHIP outputs)
                }
                Err(e) => {
                    // Malformed PushDrop -- skip silently (common for non-PushDrop outputs)
                    debug!("SHIP: output {i} skipped: {e}");
                }
            }
        }

        if outputs_to_admit.is_empty() {
            warn!("SHIP: no outputs admitted");
        }

        Ok(AdmittanceInstructions {
            outputs_to_admit,
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/ship_topic.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "SHIP Topic Manager".to_string(),
            description: Some("Manages SHIP tokens for service host interconnect.".to_string()),
            ..Default::default()
        }
    }
}

impl SHIPTopicManager {
    /// Validate a single output as a SHIP advertisement.
    /// Returns Ok(true) if valid SHIP, Ok(false) if not a SHIP output, Err if malformed.
    fn validate_ship_output(
        output: &bsv_rs::transaction::TransactionOutput,
    ) -> Result<bool, String> {
        // Decode PushDrop
        let pushdrop =
            PushDrop::decode(&output.locking_script).map_err(|e| format!("not a PushDrop: {e}"))?;

        if pushdrop.fields.len() != 5 {
            return Ok(false);
        }

        let protocol = String::from_utf8_lossy(&pushdrop.fields[0]);
        if protocol != "SHIP" {
            return Ok(false);
        }

        // Field[1]: identity key (33-byte compressed pubkey)
        // Just check length -- the full verification happens in signature check
        if pushdrop.fields[1].len() != 33 {
            return Err("identity key must be 33 bytes".into());
        }

        // Field[2]: advertised URI
        let uri = String::from_utf8_lossy(&pushdrop.fields[2]);
        if !is_advertisable_uri(&uri) {
            return Err(format!("invalid URI: {uri}"));
        }

        // Field[3]: topic name -- must start with "tm_" and pass BRC-87
        let topic = String::from_utf8_lossy(&pushdrop.fields[3]);
        if !is_valid_topic_or_service_name(&topic) {
            return Err(format!("invalid topic name: {topic}"));
        }
        if !topic.starts_with("tm_") {
            return Err(format!("SHIP requires tm_ prefix, got: {topic}"));
        }

        // Field[4]: signature -- verify via BRC-42 key derivation + ECDSA
        if pushdrop.fields[4].is_empty() {
            return Err("empty signature".into());
        }

        // Verify the ECDSA signature links the identity key to the locking key
        match is_token_signature_correctly_linked(
            &pushdrop.locking_public_key,
            &pushdrop.fields[1],
            &pushdrop.fields,
            "SHIP",
        ) {
            Ok(true) => Ok(true),
            Ok(false) => {
                warn!("output skipped: signature/key linkage failed");
                Ok(false)
            }
            Err(e) => {
                warn!("output skipped: signature verification error: {e}");
                Ok(false)
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use bsv_rs::primitives::ec::{PrivateKey, PublicKey};
    use bsv_rs::script::templates::PushDrop as PushDropTemplate;
    use bsv_rs::transaction::TransactionOutput;
    use bsv_rs::wallet::{
        Counterparty, CreateSignatureArgs, GetPublicKeyArgs, ProtoWallet, Protocol, SecurityLevel,
    };

    /// Build a properly signed SHIP PushDrop output.
    ///
    /// Uses the given private key to derive identity, sign the fields,
    /// and derive the correct locking key via BRC-42 key derivation.
    fn make_signed_ship_output(
        signer_key: &PrivateKey,
        uri: &str,
        topic: &str,
    ) -> TransactionOutput {
        let signer_wallet = ProtoWallet::new(Some(signer_key.clone()));
        let identity_key_hex = signer_wallet.identity_key_hex();
        let identity_key_bytes = hex::decode(&identity_key_hex).unwrap();

        let data_fields = vec![
            b"SHIP".to_vec(),
            identity_key_bytes.clone(),
            uri.as_bytes().to_vec(),
            topic.as_bytes().to_vec(),
        ];

        // Concatenate data fields for signing
        let data: Vec<u8> = data_fields.iter().flat_map(|f| f.iter().copied()).collect();

        let protocol_id = Protocol::new(SecurityLevel::Counterparty, "service host interconnect");

        // Sign with counterparty = 'anyone' (matches TS)
        let sig_result = signer_wallet
            .create_signature(CreateSignatureArgs {
                data: Some(data),
                hash_to_directly_sign: None,
                protocol_id: protocol_id.clone(),
                key_id: "1".to_string(),
                counterparty: Some(Counterparty::Anyone),
            })
            .unwrap();

        // Derive the locking key: signer's own derived key (for_self = true)
        let locking_key_hex = signer_wallet
            .get_public_key(GetPublicKeyArgs {
                identity_key: false,
                protocol_id: Some(protocol_id),
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

    /// Build a raw (unsigned) PushDrop output with a random locking key.
    /// Used for tests that check field-level validation before signature check.
    fn make_raw_ship_output(
        protocol: &str,
        identity_key: &[u8],
        uri: &str,
        topic: &str,
        signature: &[u8],
    ) -> TransactionOutput {
        let locking_key = PublicKey::from_private_key(&PrivateKey::random());
        let fields = vec![
            protocol.as_bytes().to_vec(),
            identity_key.to_vec(),
            uri.as_bytes().to_vec(),
            topic.as_bytes().to_vec(),
            signature.to_vec(),
        ];
        let pushdrop = PushDropTemplate::new(locking_key, fields);
        TransactionOutput {
            satoshis: Some(1),
            locking_script: pushdrop.lock(),
            change: false,
        }
    }

    fn dummy_identity_key() -> Vec<u8> {
        let pk = PublicKey::from_private_key(&PrivateKey::random());
        pk.to_compressed().to_vec()
    }

    #[test]
    fn validate_valid_ship_output() {
        let signer = PrivateKey::random();
        let output = make_signed_ship_output(&signer, "https://example.com", "tm_test");
        let result = SHIPTopicManager::validate_ship_output(&output);
        assert!(result.is_ok(), "expected Ok, got: {result:?}");
        assert!(result.unwrap());
    }

    #[test]
    fn reject_bad_signature() {
        // Properly structured but with a random (invalid) signature
        let output = make_raw_ship_output(
            "SHIP",
            &dummy_identity_key(),
            "https://example.com",
            "tm_test",
            &[0x30, 0x44, 0x02, 0x20], // garbage DER-like bytes
        );
        let result = SHIPTopicManager::validate_ship_output(&output);
        // Bad signature -> output is skipped (Ok(false)), not error
        assert!(!result.unwrap(), "bad signature should be rejected");
    }

    #[test]
    fn reject_mismatched_identity_key() {
        // Signer signs with their key but we swap identity_key field to someone else
        let signer = PrivateKey::random();
        let signer_wallet = ProtoWallet::new(Some(signer.clone()));
        let impostor = PrivateKey::random();
        let impostor_id = PublicKey::from_private_key(&impostor)
            .to_compressed()
            .to_vec();

        let protocol_id = Protocol::new(SecurityLevel::Counterparty, "service host interconnect");

        let data_fields = vec![
            b"SHIP".to_vec(),
            impostor_id.clone(), // claim to be the impostor
            b"https://example.com".to_vec(),
            b"tm_test".to_vec(),
        ];
        let data: Vec<u8> = data_fields.iter().flat_map(|f| f.iter().copied()).collect();

        let sig_result = signer_wallet
            .create_signature(CreateSignatureArgs {
                data: Some(data),
                hash_to_directly_sign: None,
                protocol_id: protocol_id.clone(),
                key_id: "1".to_string(),
                counterparty: Some(Counterparty::Anyone),
            })
            .unwrap();

        let locking_key_hex = signer_wallet
            .get_public_key(GetPublicKeyArgs {
                identity_key: false,
                protocol_id: Some(protocol_id),
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
        let output = TransactionOutput {
            satoshis: Some(1),
            locking_script: pushdrop.lock(),
            change: false,
        };

        let result = SHIPTopicManager::validate_ship_output(&output);
        // Mismatched identity -> output is skipped (Ok(false)), not error
        assert!(
            !result.unwrap(),
            "mismatched identity key should be rejected"
        );
    }

    #[test]
    fn reject_wrong_protocol() {
        let output = make_raw_ship_output(
            "SLAP", // wrong protocol
            &dummy_identity_key(),
            "https://example.com",
            "tm_test",
            &[0x30],
        );
        let result = SHIPTopicManager::validate_ship_output(&output).unwrap();
        assert!(!result); // Returns false, not error
    }

    #[test]
    fn reject_invalid_uri() {
        let output = make_raw_ship_output(
            "SHIP",
            &dummy_identity_key(),
            "http://localhost", // invalid
            "tm_test",
            &[0x30],
        );
        let result = SHIPTopicManager::validate_ship_output(&output);
        assert!(result.is_err());
    }

    #[test]
    fn reject_invalid_topic_name() {
        let output = make_raw_ship_output(
            "SHIP",
            &dummy_identity_key(),
            "https://example.com",
            "ls_test", // wrong prefix for SHIP
            &[0x30],
        );
        let result = SHIPTopicManager::validate_ship_output(&output);
        assert!(result.is_err());
    }

    #[test]
    fn reject_non_pushdrop_output() {
        let output = TransactionOutput {
            satoshis: Some(1000),
            locking_script: bsv_rs::script::Script::from_hex(
                "76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac",
            )
            .unwrap()
            .into(),
            change: false,
        };
        let result = SHIPTopicManager::validate_ship_output(&output);
        assert!(result.is_err());
    }

    #[test]
    fn reject_short_identity_key() {
        let output = make_raw_ship_output(
            "SHIP",
            &[0x02, 0x03], // too short (need 33 bytes)
            "https://example.com",
            "tm_test",
            &[0x30],
        );
        let result = SHIPTopicManager::validate_ship_output(&output);
        assert!(result.is_err());
    }

    #[test]
    fn reject_wrong_field_count() {
        let locking_key = PublicKey::from_private_key(&PrivateKey::random());
        let fields = vec![
            b"SHIP".to_vec(),
            dummy_identity_key(),
            b"https://example.com".to_vec(),
        ];
        let pushdrop = PushDropTemplate::new(locking_key, fields);
        let output = TransactionOutput {
            satoshis: Some(1),
            locking_script: pushdrop.lock(),
            change: false,
        };
        let result = SHIPTopicManager::validate_ship_output(&output).unwrap();
        assert!(!result); // wrong count returns false
    }

    #[tokio::test]
    async fn topic_manager_trait_works() {
        let mgr = SHIPTopicManager::new();
        let meta = mgr.get_metadata().await;
        assert_eq!(meta.name, "SHIP Topic Manager");

        let docs = mgr.get_documentation().await;
        assert!(!docs.is_empty());
    }
}
