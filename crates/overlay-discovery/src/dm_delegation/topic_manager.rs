//! `tm_dm_delegation` topic manager — validates dolphin-milk delegation
//! revocation PushDrop outputs.
//!
//! See [`super`] for the full design rationale and on-wire format.

use async_trait::async_trait;
use bsv_rs::script::templates::PushDrop;
use bsv_rs::transaction::Transaction;
use overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use overlay_engine::types::{AdmittanceInstructions, ServiceMetadata, SubmitMode};
use tracing::{debug, warn};

/// The fixed protocol marker stored in PushDrop field 0 by Phase 2's
/// `delegate_task` tool. See `rust-bsv-worm/src/tools/delegation_tools.rs`.
pub const DM_DELEGATION_MARKER: &[u8] = b"delegation_revocation";

/// Topic manager for dolphin-milk delegation revocation UTXOs.
///
/// Admits outputs that match the 3-field PushDrop format described in
/// [`super`]. Outputs that don't match are silently skipped (returned as
/// "not admitted") so unrelated transactions submitted under this topic
/// don't error out — they just contribute nothing.
pub struct DmDelegationTopicManager;

impl DmDelegationTopicManager {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DmDelegationTopicManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait(?Send)]
impl TopicManager for DmDelegationTopicManager {
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
            Err(e) => return Err(TopicManagerError::InvalidBeef(e.to_string())),
        };

        for (i, output) in tx.outputs.iter().enumerate() {
            match Self::validate_output(output) {
                Ok(true) => {
                    debug!("DM_DELEGATION: admitted output {i}");
                    outputs_to_admit.push(i as u32);
                }
                Ok(false) => {
                    debug!("DM_DELEGATION: output {i} not a delegation revocation token");
                }
                Err(e) => {
                    debug!("DM_DELEGATION: output {i} validation error: {e}");
                }
            }
        }

        debug!(
            "DM_DELEGATION: {} outputs in tx, {} admitted",
            tx.outputs.len(),
            outputs_to_admit.len()
        );

        if outputs_to_admit.is_empty() {
            warn!("DM_DELEGATION: no outputs admitted");
        }

        Ok(AdmittanceInstructions {
            outputs_to_admit,
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/dm_delegation_topic.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "Dolphin Milk Delegation Revocation Topic Manager".to_string(),
            description: Some(
                "Indexes 1-sat PushDrop UTXOs that anchor dolphin-milk \
                 macaroon-style delegation cert revocation status."
                    .to_string(),
            ),
            ..Default::default()
        }
    }
}

impl DmDelegationTopicManager {
    /// Validate a single output as a delegation revocation PushDrop.
    ///
    /// Returns `Ok(true)` if the output is a valid revocation token,
    /// `Ok(false)` if it's a different kind of output (silently skipped),
    /// `Err` only on hard malformations that indicate a buggy submitter.
    pub fn validate_output(
        output: &bsv_rs::transaction::TransactionOutput,
    ) -> Result<bool, String> {
        // 1. Must be a PushDrop. Non-PushDrop outputs are silently skipped.
        let pushdrop = match PushDrop::decode(&output.locking_script) {
            Ok(pd) => pd,
            Err(_) => return Ok(false),
        };

        // 2. Must have exactly 3 fields (matches Phase 2's `delegate_task`).
        //    Wrong field count → not our token, silently skip.
        if pushdrop.fields.len() != 3 {
            return Ok(false);
        }

        // 3. Field[0] must be the literal marker bytes.
        if pushdrop.fields[0].as_slice() != DM_DELEGATION_MARKER {
            return Ok(false);
        }

        // 4. Field[1] must be a JSON object with the expected envelope shape.
        //    We do NOT verify the cert signature here — that's the job of the
        //    cert verifier in dolphin-milk's runner. We just sanity-check that
        //    the field looks structurally like a cert envelope so junk doesn't
        //    pollute the index.
        let json_bytes = &pushdrop.fields[1];
        let parsed: serde_json::Value = match serde_json::from_slice(json_bytes) {
            Ok(v) => v,
            Err(e) => return Err(format!("field[1] not valid JSON: {e}")),
        };
        let obj = match parsed.as_object() {
            Some(o) => o,
            None => return Err("field[1] is not a JSON object".to_string()),
        };

        // The Phase 2 envelope contains: type, serial_number, subject,
        // certifier, purpose_hash, issued_at, expires_at. We require the
        // identifying fields (type, serial_number, certifier) and treat
        // missing optional fields as not-our-token rather than malformed.
        let envelope_type = match obj.get("type").and_then(|v| v.as_str()) {
            Some(t) => t,
            None => return Ok(false),
        };
        if envelope_type != "DelegationRevocation" {
            return Ok(false);
        }
        if obj.get("serial_number").and_then(|v| v.as_str()).is_none() {
            return Err("envelope missing serial_number".to_string());
        }
        if obj.get("certifier").and_then(|v| v.as_str()).is_none() {
            return Err("envelope missing certifier".to_string());
        }

        // 5. Field[2] must be a Unix timestamp string. We don't enforce
        //    a specific value range — old tokens are still valid history.
        let ts_str = std::str::from_utf8(&pushdrop.fields[2])
            .map_err(|e| format!("field[2] not valid utf-8: {e}"))?;
        if ts_str.parse::<i64>().is_err() {
            return Err(format!("field[2] not a unix timestamp: {ts_str}"));
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bsv_rs::primitives::ec::{PrivateKey, PublicKey};
    use bsv_rs::script::templates::PushDrop as PushDropTemplate;
    use bsv_rs::transaction::TransactionOutput;
    use serde_json::json;

    fn make_locking_key() -> PublicKey {
        PublicKey::from_private_key(&PrivateKey::random())
    }

    fn make_envelope_json(serial: &str, certifier: &str) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "type": "DelegationRevocation",
            "serial_number": serial,
            "subject": "02".repeat(33),
            "certifier": certifier,
            "purpose_hash": format!("sha256:{}", "ab".repeat(32)),
            "issued_at": "2026-04-12T17:00:00+00:00",
            "expires_at": "2026-04-12T17:10:00+00:00",
        }))
        .unwrap()
    }

    fn make_token_output(fields: Vec<Vec<u8>>) -> TransactionOutput {
        let pushdrop = PushDropTemplate::new(make_locking_key(), fields);
        TransactionOutput {
            satoshis: Some(1),
            locking_script: pushdrop.lock(),
            change: false,
        }
    }

    #[test]
    fn validate_well_formed_revocation_output() {
        let envelope = make_envelope_json("delegation-aabb-1234-1700000000000", &"03".repeat(33));
        let output = make_token_output(vec![
            DM_DELEGATION_MARKER.to_vec(),
            envelope,
            b"1700000000".to_vec(),
        ]);
        let result = DmDelegationTopicManager::validate_output(&output).unwrap();
        assert!(result, "well-formed revocation output should be admitted");
    }

    #[test]
    fn reject_wrong_marker() {
        let envelope = make_envelope_json("delegation-aabb-1234-1700000000000", &"03".repeat(33));
        let output = make_token_output(vec![
            b"agent_authorization".to_vec(),
            envelope,
            b"1700000000".to_vec(),
        ]);
        // Different marker → not our token, silently skipped (Ok(false), not Err)
        assert!(!DmDelegationTopicManager::validate_output(&output).unwrap());
    }

    #[test]
    fn reject_wrong_field_count_too_few() {
        let envelope = make_envelope_json("delegation-aabb-1234-1700000000000", &"03".repeat(33));
        let output = make_token_output(vec![DM_DELEGATION_MARKER.to_vec(), envelope]);
        assert!(!DmDelegationTopicManager::validate_output(&output).unwrap());
    }

    #[test]
    fn reject_wrong_field_count_too_many() {
        let envelope = make_envelope_json("delegation-aabb-1234-1700000000000", &"03".repeat(33));
        let output = make_token_output(vec![
            DM_DELEGATION_MARKER.to_vec(),
            envelope,
            b"1700000000".to_vec(),
            b"extra".to_vec(),
        ]);
        assert!(!DmDelegationTopicManager::validate_output(&output).unwrap());
    }

    #[test]
    fn reject_envelope_with_wrong_type() {
        let envelope = serde_json::to_vec(&json!({
            "type": "SomethingElse",
            "serial_number": "x",
            "certifier": "03".repeat(33),
        }))
        .unwrap();
        let output = make_token_output(vec![
            DM_DELEGATION_MARKER.to_vec(),
            envelope,
            b"1700000000".to_vec(),
        ]);
        assert!(!DmDelegationTopicManager::validate_output(&output).unwrap());
    }

    #[test]
    fn reject_envelope_missing_serial() {
        let envelope = serde_json::to_vec(&json!({
            "type": "DelegationRevocation",
            "certifier": "03".repeat(33),
        }))
        .unwrap();
        let output = make_token_output(vec![
            DM_DELEGATION_MARKER.to_vec(),
            envelope,
            b"1700000000".to_vec(),
        ]);
        let err = DmDelegationTopicManager::validate_output(&output).unwrap_err();
        assert!(err.contains("serial_number"));
    }

    #[test]
    fn reject_envelope_missing_certifier() {
        let envelope = serde_json::to_vec(&json!({
            "type": "DelegationRevocation",
            "serial_number": "x",
        }))
        .unwrap();
        let output = make_token_output(vec![
            DM_DELEGATION_MARKER.to_vec(),
            envelope,
            b"1700000000".to_vec(),
        ]);
        let err = DmDelegationTopicManager::validate_output(&output).unwrap_err();
        assert!(err.contains("certifier"));
    }

    #[test]
    fn reject_non_json_envelope() {
        let output = make_token_output(vec![
            DM_DELEGATION_MARKER.to_vec(),
            b"not json at all".to_vec(),
            b"1700000000".to_vec(),
        ]);
        let err = DmDelegationTopicManager::validate_output(&output).unwrap_err();
        assert!(err.contains("JSON"));
    }

    #[test]
    fn reject_non_object_envelope() {
        let envelope = serde_json::to_vec(&json!(["arrays", "not", "objects"])).unwrap();
        let output = make_token_output(vec![
            DM_DELEGATION_MARKER.to_vec(),
            envelope,
            b"1700000000".to_vec(),
        ]);
        let err = DmDelegationTopicManager::validate_output(&output).unwrap_err();
        assert!(err.contains("not a JSON object"));
    }

    #[test]
    fn reject_non_unix_timestamp_field() {
        let envelope = make_envelope_json("delegation-aabb-1234-1700000000000", &"03".repeat(33));
        let output = make_token_output(vec![
            DM_DELEGATION_MARKER.to_vec(),
            envelope,
            b"not-a-number".to_vec(),
        ]);
        let err = DmDelegationTopicManager::validate_output(&output).unwrap_err();
        assert!(err.contains("not a unix timestamp"));
    }

    #[test]
    fn non_pushdrop_output_silently_skipped() {
        // Bare OP_CHECKSIG byte — not a PushDrop, should return Ok(false)
        let script = bsv_rs::script::Script::from_binary(&[0xACu8]).unwrap();
        let output = TransactionOutput {
            satoshis: Some(1),
            locking_script: script.into(),
            change: false,
        };
        assert!(!DmDelegationTopicManager::validate_output(&output).unwrap());
    }

    #[tokio::test]
    async fn topic_manager_metadata() {
        let mgr = DmDelegationTopicManager::new();
        let meta = mgr.get_metadata().await;
        assert!(meta.name.contains("Delegation Revocation"));
    }
}
