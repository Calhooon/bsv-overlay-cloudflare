//! TopicManager trait — decides which outputs from a transaction to admit into a topic.
//!
//! Each topic in the overlay has a TopicManager that validates transactions
//! and returns admittance instructions. The Engine calls this for every
//! submitted transaction, per topic.
//!
//! Ported from `~/bsv/overlay-services/src/TopicManager.ts`.

use async_trait::async_trait;

use crate::types::{AdmittanceInstructions, Outpoint, ServiceMetadata, SubmitMode};

/// Validates transactions and decides which outputs to admit into a topic.
///
/// Implementations are topic-specific — e.g., SHIPTopicManager validates SHIP
/// advertisement PushDrop scripts, while a custom topic manager might validate
/// token transfers.
#[async_trait(?Send)]
pub trait TopicManager {
    /// Examine a transaction (in BEEF format) and return instructions indicating
    /// which outputs should be admitted into this topic.
    ///
    /// # Arguments
    /// - `beef` — The transaction in BEEF format (includes ancestor proofs).
    /// - `previous_coins` — BEEF data for previously-admitted outputs that this
    ///   transaction spends (inputs from the same topic). May be empty if no
    ///   inputs come from this topic.
    /// - `off_chain_values` — Optional off-chain context data associated with
    ///   the transaction (not on-chain).
    /// - `mode` — Submission mode: CurrentTx (broadcast + SPV), HistoricalTx
    ///   (no broadcast), HistoricalTxNoSpv (GASP sync).
    ///
    /// # Returns
    /// `AdmittanceInstructions` with:
    /// - `outputs_to_admit`: indices of outputs to keep in this topic
    /// - `coins_to_retain`: indices of previous coins to keep (not garbage-collect)
    async fn identify_admissible_outputs(
        &self,
        beef: &[u8],
        previous_coins: &[u8],
        off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
    ) -> Result<AdmittanceInstructions, TopicManagerError>;

    /// Identify which inputs are needed to validate this transaction for GASP sync.
    ///
    /// Called by the GASP storage layer to determine which ancestor transactions
    /// need to be fetched from the remote peer.
    ///
    /// Default: returns empty (no additional inputs needed beyond what BEEF provides).
    async fn identify_needed_inputs(
        &self,
        _beef: &[u8],
        _off_chain_values: Option<&[u8]>,
    ) -> Result<Vec<Outpoint>, TopicManagerError> {
        Ok(Vec::new())
    }

    /// Return Markdown documentation for this topic manager.
    async fn get_documentation(&self) -> String;

    /// Return metadata identifying this topic manager.
    async fn get_metadata(&self) -> ServiceMetadata;
}

/// Errors from TopicManager operations.
#[derive(Debug, thiserror::Error)]
pub enum TopicManagerError {
    /// The transaction BEEF could not be parsed.
    #[error("invalid BEEF: {0}")]
    InvalidBeef(String),

    /// The transaction has no admissible outputs for this topic.
    #[error("no admissible outputs: {0}")]
    NoAdmissibleOutputs(String),

    /// A script in the transaction is malformed.
    #[error("invalid script: {0}")]
    InvalidScript(String),

    /// Signature verification failed.
    #[error("signature verification failed: {0}")]
    SignatureError(String),

    /// Generic error.
    #[error("{0}")]
    Other(String),
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// A trivial TopicManager that admits all outputs.
    struct AdmitAllManager;

    #[async_trait(?Send)]
    impl TopicManager for AdmitAllManager {
        async fn identify_admissible_outputs(
            &self,
            _beef: &[u8],
            _previous_coins: &[u8],
            _off_chain_values: Option<&[u8]>,
            _mode: SubmitMode,
        ) -> Result<AdmittanceInstructions, TopicManagerError> {
            Ok(AdmittanceInstructions {
                outputs_to_admit: vec![0, 1, 2],
                coins_to_retain: vec![],
                coins_removed: None,
            })
        }

        async fn get_documentation(&self) -> String {
            "Admits everything.".to_string()
        }

        async fn get_metadata(&self) -> ServiceMetadata {
            ServiceMetadata {
                name: "admit-all".to_string(),
                description: Some("Test manager that admits all outputs".to_string()),
                ..Default::default()
            }
        }
    }

    /// A TopicManager that rejects everything.
    struct RejectAllManager;

    #[async_trait(?Send)]
    impl TopicManager for RejectAllManager {
        async fn identify_admissible_outputs(
            &self,
            _beef: &[u8],
            _previous_coins: &[u8],
            _off_chain_values: Option<&[u8]>,
            _mode: SubmitMode,
        ) -> Result<AdmittanceInstructions, TopicManagerError> {
            Ok(AdmittanceInstructions::default())
        }

        async fn get_documentation(&self) -> String {
            "Rejects everything.".to_string()
        }

        async fn get_metadata(&self) -> ServiceMetadata {
            ServiceMetadata {
                name: "reject-all".to_string(),
                ..Default::default()
            }
        }
    }

    #[tokio::test]
    async fn test_admit_all_manager() {
        let mgr = AdmitAllManager;
        let result = mgr
            .identify_admissible_outputs(&[], &[], None, SubmitMode::CurrentTx)
            .await
            .unwrap();
        assert_eq!(result.outputs_to_admit, vec![0, 1, 2]);
        assert!(result.has_activity());
    }

    #[tokio::test]
    async fn test_reject_all_manager() {
        let mgr = RejectAllManager;
        let result = mgr
            .identify_admissible_outputs(&[], &[], None, SubmitMode::CurrentTx)
            .await
            .unwrap();
        assert!(result.outputs_to_admit.is_empty());
        assert!(!result.has_activity());
    }

    #[tokio::test]
    async fn test_default_identify_needed_inputs() {
        let mgr = AdmitAllManager;
        let inputs = mgr.identify_needed_inputs(&[], None).await.unwrap();
        assert!(inputs.is_empty());
    }

    #[tokio::test]
    async fn test_manager_metadata() {
        let mgr = AdmitAllManager;
        let meta = mgr.get_metadata().await;
        assert_eq!(meta.name, "admit-all");
        assert!(meta.description.is_some());

        let docs = mgr.get_documentation().await;
        assert!(!docs.is_empty());
    }

    #[tokio::test]
    async fn test_topic_manager_is_object_safe() {
        // Verify the trait can be used as dyn TopicManager (required for Engine)
        let mgr: Box<dyn TopicManager> = Box::new(AdmitAllManager);
        let result = mgr
            .identify_admissible_outputs(&[], &[], None, SubmitMode::HistoricalTx)
            .await
            .unwrap();
        assert_eq!(result.outputs_to_admit.len(), 3);
    }
}
