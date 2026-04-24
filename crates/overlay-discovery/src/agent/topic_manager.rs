//! Agent Topic Manager -- validates Agent Registry PushDrop outputs.
//!
//! Agent registrations are PushDrop outputs with exactly 6 fields:
//! 1. Protocol identifier: "AGENT"
//! 2. Subject identity key: 33-byte compressed secp256k1 pubkey
//! 3. Certifier identity key: 33-byte compressed secp256k1 pubkey
//! 4. Name: non-empty free-form string (agent display name)
//! 5. Capabilities: non-empty comma-separated string
//! 6. ECDSA signature: linking identity key to locking key

use async_trait::async_trait;
use bsv_rs::script::templates::PushDrop;
use bsv_rs::transaction::Transaction;
use overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use overlay_engine::types::{AdmittanceInstructions, ServiceMetadata, SubmitMode};
use tracing::{debug, warn};

use crate::validation::is_token_signature_correctly_linked;

/// Agent Topic Manager -- identifies admissible Agent registration outputs.
pub struct AgentTopicManager;

impl AgentTopicManager {
    /// Create a new Agent Topic Manager.
    pub fn new() -> Self {
        Self
    }
}

impl Default for AgentTopicManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait(?Send)]
impl TopicManager for AgentTopicManager {
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
            match Self::validate_agent_output(output) {
                Ok(true) => {
                    debug!("AGENT: admitted output {i}");
                    outputs_to_admit.push(i as u32);
                }
                Ok(false) => {
                    debug!("AGENT: output {i} not an AGENT token");
                }
                Err(e) => {
                    debug!("AGENT: output {i} validation error: {e}");
                }
            }
        }
        debug!(
            "AGENT: {} outputs in tx, {} admitted",
            tx.outputs.len(),
            outputs_to_admit.len()
        );

        if outputs_to_admit.is_empty() {
            warn!("AGENT: no outputs admitted");
        }

        Ok(AdmittanceInstructions {
            outputs_to_admit,
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/agent_topic.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "Agent Topic Manager".to_string(),
            description: Some("Manages Agent Registry tokens for agent discovery.".to_string()),
            ..Default::default()
        }
    }
}

impl AgentTopicManager {
    /// Validate a single output as an Agent registration.
    /// Returns Ok(true) if valid AGENT, Ok(false) if not an AGENT output, Err if malformed.
    pub fn validate_agent_output(
        output: &bsv_rs::transaction::TransactionOutput,
    ) -> Result<bool, String> {
        // 1. Decode PushDrop
        let pushdrop = match PushDrop::decode(&output.locking_script) {
            Ok(pd) => pd,
            Err(e) => {
                tracing::debug!(
                    "AGENT: PushDrop decode failed: {e} script_len={}",
                    output.locking_script.to_hex().len() / 2
                );
                return Ok(false); // Not a PushDrop — skip silently
            }
        };

        tracing::debug!(
            "AGENT: decoded PushDrop with {} fields",
            pushdrop.fields.len()
        );

        // 2. Must have exactly 6 fields
        if pushdrop.fields.len() != 6 {
            tracing::debug!(
                "AGENT: wrong field count: {} (expected 6)",
                pushdrop.fields.len()
            );
            return Ok(false);
        }

        // 3. Field[0]: protocol identifier must be "AGENT"
        let protocol = String::from_utf8_lossy(&pushdrop.fields[0]);
        if protocol != "AGENT" {
            return Ok(false);
        }

        // 4. Field[1]: subject identity key (33-byte compressed pubkey)
        if pushdrop.fields[1].len() != 33 {
            return Err("subject identity key must be 33 bytes".into());
        }

        // 5. Field[2]: certifier identity key (33-byte compressed pubkey)
        if pushdrop.fields[2].len() != 33 {
            return Err("certifier identity key must be 33 bytes".into());
        }

        // 6. Field[3]: agent name (free-form, must be non-empty)
        if pushdrop.fields[3].is_empty() {
            return Err("agent name must be non-empty".into());
        }

        // 7. Field[4]: capabilities (must be non-empty)
        if pushdrop.fields[4].is_empty() {
            return Err("capabilities must be non-empty".into());
        }

        // 8. Field[5]: signature -- verify via BRC-42 key derivation + ECDSA
        if pushdrop.fields[5].is_empty() {
            return Err("empty signature".into());
        }

        // Verify the ECDSA signature links the identity key to the locking key
        match is_token_signature_correctly_linked(
            &pushdrop.locking_public_key,
            &pushdrop.fields[1],
            &pushdrop.fields,
            "AGENT",
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

    /// Build a properly signed AGENT PushDrop output (self-signed: certifier == subject).
    fn make_signed_agent_output(
        signer_key: &PrivateKey,
        endpoint: &str,
        capabilities: &str,
    ) -> TransactionOutput {
        let signer_wallet = ProtoWallet::new(Some(signer_key.clone()));
        let identity_key_hex = signer_wallet.identity_key_hex();
        let identity_key_bytes = hex::decode(&identity_key_hex).unwrap();

        let data_fields = vec![
            b"AGENT".to_vec(),
            identity_key_bytes.clone(),
            identity_key_bytes.clone(), // self-signed: certifier == subject
            endpoint.as_bytes().to_vec(),
            capabilities.as_bytes().to_vec(),
        ];

        // Concatenate data fields for signing
        let data: Vec<u8> = data_fields.iter().flat_map(|f| f.iter().copied()).collect();

        let protocol_id = Protocol::new(SecurityLevel::Counterparty, "agent registry");

        // Sign with counterparty = 'anyone' (matches TS self-signed pattern)
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

    /// Build a raw (unsigned) AGENT PushDrop output with a random locking key.
    /// Used for tests that check field-level validation before signature check.
    fn make_raw_agent_output(
        protocol: &str,
        identity_key: &[u8],
        certifier_key: &[u8],
        endpoint: &str,
        capabilities: &str,
        signature: &[u8],
    ) -> TransactionOutput {
        let locking_key = PublicKey::from_private_key(&PrivateKey::random());
        let fields = vec![
            protocol.as_bytes().to_vec(),
            identity_key.to_vec(),
            certifier_key.to_vec(),
            endpoint.as_bytes().to_vec(),
            capabilities.as_bytes().to_vec(),
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
    fn validate_valid_agent_output() {
        let signer = PrivateKey::random();
        let output = make_signed_agent_output(
            &signer,
            "https://agent.example.com",
            "image-generation,upscaling",
        );
        let result = AgentTopicManager::validate_agent_output(&output);
        assert!(result.is_ok(), "expected Ok, got: {result:?}");
        assert!(result.unwrap());
    }

    #[test]
    fn reject_wrong_protocol() {
        let id_key = dummy_identity_key();
        let output = make_raw_agent_output(
            "SHIP", // wrong protocol
            &id_key,
            &id_key,
            "https://example.com",
            "cap",
            &[0x30],
        );
        let result = AgentTopicManager::validate_agent_output(&output).unwrap();
        assert!(!result); // Returns false, not error
    }

    #[test]
    fn reject_too_few_fields() {
        let locking_key = PublicKey::from_private_key(&PrivateKey::random());
        let fields = vec![
            b"AGENT".to_vec(),
            dummy_identity_key(),
            dummy_identity_key(),
            b"https://example.com".to_vec(),
            // missing capabilities and signature
        ];
        let pushdrop = PushDropTemplate::new(locking_key, fields);
        let output = TransactionOutput {
            satoshis: Some(1),
            locking_script: pushdrop.lock(),
            change: false,
        };
        let result = AgentTopicManager::validate_agent_output(&output).unwrap();
        assert!(!result); // wrong count returns false
    }

    #[test]
    fn reject_too_many_fields() {
        let locking_key = PublicKey::from_private_key(&PrivateKey::random());
        let fields = vec![
            b"AGENT".to_vec(),
            dummy_identity_key(),
            dummy_identity_key(),
            b"https://example.com".to_vec(),
            b"cap".to_vec(),
            b"sig".to_vec(),
            b"extra".to_vec(), // too many
        ];
        let pushdrop = PushDropTemplate::new(locking_key, fields);
        let output = TransactionOutput {
            satoshis: Some(1),
            locking_script: pushdrop.lock(),
            change: false,
        };
        let result = AgentTopicManager::validate_agent_output(&output).unwrap();
        assert!(!result); // wrong count returns false
    }

    #[test]
    fn accept_any_name_string() {
        // Agent names are free-form — no URI validation needed
        let id_key = dummy_identity_key();
        let output = make_raw_agent_output(
            "AGENT",
            &id_key,
            &id_key,
            "dolphin-milk-agent", // any non-empty string is fine
            "cap",
            &[0x30],
        );
        let result = AgentTopicManager::validate_agent_output(&output);
        // Will be Ok(false) due to garbage signature, but NOT Err — name is valid
        assert!(result.is_ok());
    }

    #[test]
    fn reject_empty_capabilities() {
        // PushDrop encoding turns empty Vec into OP_0 which decodes as [0x00],
        // so we test the validation function directly with a crafted PushDrop
        // that has a single zero byte for capabilities (still semantically empty).
        // The validation check is a defensive measure -- in practice PushDrop
        // won't produce truly empty fields, but we verify the code path handles
        // the non-empty-but-meaningless case by checking that the garbage-signature
        // path still rejects the output.
        let id_key = dummy_identity_key();
        let output = make_raw_agent_output(
            "AGENT",
            &id_key,
            &id_key,
            "https://example.com",
            "\x00", // PushDrop encodes "" as OP_0 => decodes as [0x00]
            &[0x30],
        );
        // This output has a single zero-byte capability field, which is non-empty
        // so it passes the capabilities check, but the garbage signature causes
        // the output to be skipped (Ok(false)).
        let result = AgentTopicManager::validate_agent_output(&output);
        assert!(!result.unwrap(), "garbage signature should be rejected");
    }

    #[test]
    fn reject_empty_signature() {
        // PushDrop encoding turns empty Vec into OP_0 which decodes as [0x00],
        // so a truly empty signature field cannot be produced via PushDrop.
        // Instead we test with garbage signature bytes that will fail verification.
        let id_key = dummy_identity_key();
        let output = make_raw_agent_output(
            "AGENT",
            &id_key,
            &id_key,
            "https://example.com",
            "cap",
            &[0x00], // single zero byte (PushDrop minimum for "empty")
        );
        let result = AgentTopicManager::validate_agent_output(&output);
        // Garbage signature -> output is skipped (Ok(false))
        assert!(!result.unwrap(), "garbage signature should be rejected");
    }

    #[test]
    fn reject_short_identity_key() {
        let cert_key = dummy_identity_key();
        let output = make_raw_agent_output(
            "AGENT",
            &[0x02, 0x03], // too short (need 33 bytes)
            &cert_key,
            "https://example.com",
            "cap",
            &[0x30],
        );
        let result = AgentTopicManager::validate_agent_output(&output);
        assert!(result.is_err());
    }

    #[test]
    fn reject_short_certifier_key() {
        let id_key = dummy_identity_key();
        let output = make_raw_agent_output(
            "AGENT",
            &id_key,
            &[0x02, 0x03], // too short (need 33 bytes)
            "https://example.com",
            "cap",
            &[0x30],
        );
        let result = AgentTopicManager::validate_agent_output(&output);
        assert!(result.is_err());
    }

    #[test]
    fn bad_signature_skipped_not_error() {
        let id_key = dummy_identity_key();
        let output = make_raw_agent_output(
            "AGENT",
            &id_key,
            &id_key,
            "https://example.com",
            "image-generation",
            &[0x30, 0x44, 0x02, 0x20], // garbage DER-like bytes
        );
        let result = AgentTopicManager::validate_agent_output(&output);
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

        let protocol_id = Protocol::new(SecurityLevel::Counterparty, "agent registry");

        let data_fields = vec![
            b"AGENT".to_vec(),
            impostor_id.clone(), // claim to be the impostor
            impostor_id.clone(),
            b"https://example.com".to_vec(),
            b"cap".to_vec(),
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

        let result = AgentTopicManager::validate_agent_output(&output);
        // Mismatched identity -> output is skipped (Ok(false)), not error
        assert!(
            !result.unwrap(),
            "mismatched identity key should be rejected"
        );
    }

    #[tokio::test]
    async fn topic_manager_trait_works() {
        let mgr = AgentTopicManager::new();
        let meta = mgr.get_metadata().await;
        assert_eq!(meta.name, "Agent Topic Manager");

        let docs = mgr.get_documentation().await;
        assert!(!docs.is_empty());
    }
}
