//! SLAP Topic Manager -- validates SLAP advertisement PushDrop outputs.
//!
//! SLAP (Service Lookup Availability Protocol) advertisements use the same
//! 5-field PushDrop format as SHIP, but with:
//! - Field[0] = "SLAP" (not "SHIP")
//! - Field[3] must start with "ls_" (not "tm_")
//!
//! Ported from `~/bsv/overlay-discovery-services/src/SLAP/SLAPTopicManager.ts`.

use async_trait::async_trait;
use bsv_rs::script::templates::PushDrop;
use bsv_rs::transaction::Transaction;
use overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use overlay_engine::types::{AdmittanceInstructions, ServiceMetadata, SubmitMode};
use tracing::{debug, warn};

use crate::validation::{
    is_advertisable_uri, is_token_signature_correctly_linked, is_valid_topic_or_service_name,
};

/// SLAP Topic Manager -- identifies admissible SLAP advertisement outputs.
pub struct SLAPTopicManager;

impl SLAPTopicManager {
    /// Create a new SLAP Topic Manager.
    pub fn new() -> Self {
        Self
    }
}

impl Default for SLAPTopicManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait(?Send)]
impl TopicManager for SLAPTopicManager {
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
            match Self::validate_slap_output(output) {
                Ok(true) => {
                    debug!("SLAP: admitted output {i}");
                    outputs_to_admit.push(i as u32);
                }
                Ok(false) => {}
                Err(e) => {
                    debug!("SLAP: output {i} skipped: {e}");
                }
            }
        }

        if outputs_to_admit.is_empty() {
            warn!("SLAP: no outputs admitted");
        }

        Ok(AdmittanceInstructions {
            outputs_to_admit,
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/slap_topic.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "SLAP Topic Manager".to_string(),
            description: Some("Manages SLAP tokens for service lookup availability.".to_string()),
            ..Default::default()
        }
    }
}

impl SLAPTopicManager {
    /// Validate a single output as a SLAP advertisement.
    fn validate_slap_output(
        output: &bsv_rs::transaction::TransactionOutput,
    ) -> Result<bool, String> {
        let pushdrop =
            PushDrop::decode(&output.locking_script).map_err(|e| format!("not a PushDrop: {e}"))?;

        if pushdrop.fields.len() != 5 {
            return Ok(false);
        }

        let protocol = String::from_utf8_lossy(&pushdrop.fields[0]);
        if protocol != "SLAP" {
            return Ok(false);
        }

        if pushdrop.fields[1].len() != 33 {
            return Err("identity key must be 33 bytes".into());
        }

        let uri = String::from_utf8_lossy(&pushdrop.fields[2]);
        if !is_advertisable_uri(&uri) {
            return Err(format!("invalid URI: {uri}"));
        }

        let service = String::from_utf8_lossy(&pushdrop.fields[3]);
        if !is_valid_topic_or_service_name(&service) {
            return Err(format!("invalid service name: {service}"));
        }
        if !service.starts_with("ls_") {
            return Err(format!("SLAP requires ls_ prefix, got: {service}"));
        }

        if pushdrop.fields[4].is_empty() {
            return Err("empty signature".into());
        }

        // Verify the ECDSA signature links the identity key to the locking key
        match is_token_signature_correctly_linked(
            &pushdrop.locking_public_key,
            &pushdrop.fields[1],
            &pushdrop.fields,
            "SLAP",
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

    /// Build a properly signed SLAP PushDrop output.
    fn make_signed_slap_output(
        signer_key: &PrivateKey,
        uri: &str,
        service: &str,
    ) -> TransactionOutput {
        let signer_wallet = ProtoWallet::new(Some(signer_key.clone()));
        let identity_key_hex = signer_wallet.identity_key_hex();
        let identity_key_bytes = hex::decode(&identity_key_hex).unwrap();

        let data_fields = vec![
            b"SLAP".to_vec(),
            identity_key_bytes.clone(),
            uri.as_bytes().to_vec(),
            service.as_bytes().to_vec(),
        ];

        let data: Vec<u8> = data_fields.iter().flat_map(|f| f.iter().copied()).collect();

        let protocol_id = Protocol::new(SecurityLevel::Counterparty, "service lookup availability");

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
        TransactionOutput {
            satoshis: Some(1),
            locking_script: pushdrop.lock(),
            change: false,
        }
    }

    /// Build a raw (unsigned) PushDrop output with a random locking key.
    fn make_raw_slap_output(
        protocol: &str,
        identity_key: &[u8],
        uri: &str,
        service: &str,
        signature: &[u8],
    ) -> TransactionOutput {
        let locking_key = PublicKey::from_private_key(&PrivateKey::random());
        let fields = vec![
            protocol.as_bytes().to_vec(),
            identity_key.to_vec(),
            uri.as_bytes().to_vec(),
            service.as_bytes().to_vec(),
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
        PublicKey::from_private_key(&PrivateKey::random())
            .to_compressed()
            .to_vec()
    }

    #[test]
    fn validate_valid_slap_output() {
        let signer = PrivateKey::random();
        let output = make_signed_slap_output(&signer, "https://example.com", "ls_test");
        let result = SLAPTopicManager::validate_slap_output(&output);
        assert!(result.is_ok(), "expected Ok, got: {result:?}");
        assert!(result.unwrap());
    }

    #[test]
    fn reject_bad_signature() {
        let output = make_raw_slap_output(
            "SLAP",
            &dummy_identity_key(),
            "https://example.com",
            "ls_test",
            &[0x30, 0x44, 0x02, 0x20],
        );
        let result = SLAPTopicManager::validate_slap_output(&output);
        // Bad signature -> output is skipped (Ok(false)), not error
        assert!(!result.unwrap(), "bad signature should be rejected");
    }

    #[test]
    fn reject_mismatched_identity_key() {
        let signer = PrivateKey::random();
        let signer_wallet = ProtoWallet::new(Some(signer.clone()));
        let impostor = PrivateKey::random();
        let impostor_id = PublicKey::from_private_key(&impostor)
            .to_compressed()
            .to_vec();

        let protocol_id = Protocol::new(SecurityLevel::Counterparty, "service lookup availability");

        let data_fields = vec![
            b"SLAP".to_vec(),
            impostor_id.clone(),
            b"https://example.com".to_vec(),
            b"ls_test".to_vec(),
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

        let result = SLAPTopicManager::validate_slap_output(&output);
        // Mismatched identity -> output is skipped (Ok(false)), not error
        assert!(
            !result.unwrap(),
            "mismatched identity key should be rejected"
        );
    }

    #[test]
    fn reject_wrong_protocol() {
        let output = make_raw_slap_output(
            "SHIP",
            &dummy_identity_key(),
            "https://example.com",
            "ls_test",
            &[0x30],
        );
        let result = SLAPTopicManager::validate_slap_output(&output).unwrap();
        assert!(!result);
    }

    #[test]
    fn reject_invalid_uri() {
        let output = make_raw_slap_output(
            "SLAP",
            &dummy_identity_key(),
            "http://localhost",
            "ls_test",
            &[0x30],
        );
        let result = SLAPTopicManager::validate_slap_output(&output);
        assert!(result.is_err());
    }

    #[test]
    fn reject_wrong_prefix() {
        let output = make_raw_slap_output(
            "SLAP",
            &dummy_identity_key(),
            "https://example.com",
            "tm_test", // wrong prefix for SLAP
            &[0x30],
        );
        let result = SLAPTopicManager::validate_slap_output(&output);
        assert!(result.is_err());
    }

    #[test]
    fn reject_short_identity_key() {
        let output = make_raw_slap_output(
            "SLAP",
            &[0x02, 0x03],
            "https://example.com",
            "ls_test",
            &[0x30],
        );
        let result = SLAPTopicManager::validate_slap_output(&output);
        assert!(result.is_err());
    }

    #[test]
    fn reject_wrong_field_count() {
        let locking_key = PublicKey::from_private_key(&PrivateKey::random());
        let fields = vec![b"SLAP".to_vec(), dummy_identity_key()];
        let pushdrop = PushDropTemplate::new(locking_key, fields);
        let output = TransactionOutput {
            satoshis: Some(1),
            locking_script: pushdrop.lock(),
            change: false,
        };
        let result = SLAPTopicManager::validate_slap_output(&output).unwrap();
        assert!(!result);
    }

    #[tokio::test]
    async fn topic_manager_trait_works() {
        let mgr = SLAPTopicManager::new();
        let meta = mgr.get_metadata().await;
        assert_eq!(meta.name, "SLAP Topic Manager");

        let docs = mgr.get_documentation().await;
        assert!(!docs.is_empty());
    }
}
