//! TopicManager trait — decides which outputs from a transaction to admit into a topic.
//!
//! Each topic in the overlay has a TopicManager that validates transactions
//! and returns admittance instructions. The Engine calls this for every
//! submitted transaction, per topic.
//!
//! Ported from `~/bsv/overlay-services/src/TopicManager.ts`.

use async_trait::async_trait;
use bsv_rs::transaction::Transaction;

use crate::types::{
    AdmittanceInstructions, Outpoint, ServiceMetadata, SubmitMode, TopicAdmittanceContext,
};

/// Validates transactions and decides which outputs to admit into a topic.
///
/// Implementations are topic-specific — e.g., SHIPTopicManager validates SHIP
/// advertisement PushDrop scripts, while a custom topic manager might validate
/// token transfers.
#[async_trait(?Send)]
pub trait TopicManager {
    /// Examine a transaction and return instructions indicating which
    /// outputs should be admitted into this topic.
    ///
    /// # Arguments
    /// - `tx` — The submitted transaction, parsed ONCE by the engine from
    ///   the BEEF (`Transaction::from_beef(beef, None)`) and shared across
    ///   every topic manager on the submit. Before bsv-low #289 each manager
    ///   re-parsed the same bytes independently (T+1 parses per `/submit`).
    ///   Admission stays byte-format-only: managers still validate the
    ///   OUTPUT SCRIPTS themselves; only the redundant container parse
    ///   moved.
    /// - `previous_coins` — BEEF data for previously-admitted outputs that this
    ///   transaction spends (inputs from the same topic). May be empty if no
    ///   inputs come from this topic.
    /// - `off_chain_values` — Optional off-chain context data associated with
    ///   the transaction (not on-chain).
    /// - `mode` — Submission mode: CurrentTx (broadcast + SPV), HistoricalTx
    ///   (no broadcast), HistoricalTxNoSpv (GASP sync).
    /// - `context`: the reference's fifth argument (`context?:
    ///   TopicAdmittanceContext`, `dryRun`). `context.dry_run` is
    ///   `true` when the engine only asks what WOULD be admitted and admits
    ///   nothing on this call: the GASP walk asks it of every proven node a
    ///   peer sends, and the anchor check replays a peer's whole graph that
    ///   way before it is finalized. A dry run must leave NO durable trace:
    ///   no storage write, no head advance, no counter an operator reads as
    ///   an admission. The answer must still be the one a real call would
    ///   give. A manager that writes on admission MUST read this flag; a
    ///   manager that holds no state may ignore it. `mode` is not a
    ///   substitute: the Cloudflare queue replays real submissions under
    ///   `HistoricalTx`, the same mode both dry runs use, so only this flag
    ///   tells them apart. Every submit passes `dry_run: false`.
    ///
    /// # Returns
    /// `AdmittanceInstructions` with:
    /// - `outputs_to_admit`: indices of outputs to keep in this topic
    /// - `coins_to_retain`: indices of previous coins to keep (not garbage-collect)
    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        previous_coins: &[u8],
        off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
        context: &TopicAdmittanceContext,
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

    /// Whether this manager's admission READS the transaction's off-chain
    /// values (lane E1D's delta fold, L1). The door lands a carried
    /// predecessor first with NO off-chain values (the submit carries only
    /// the successor's), and the predecessor's own submit is a dupe after
    /// that, so values its manager would have read are lost for good: the
    /// engine lands nothing first into a topic whose manager answers `true`
    /// (the successor waits for the predecessor's own submit). Default
    /// `true`, so a manager that does not say is never judged without its
    /// values; one that never reads them answers `false`.
    fn reads_off_chain_values(&self) -> bool {
        true
    }

    /// Release provisional state this manager made while identifying
    /// admissible outputs, for an admission the network did not take (bsv-low
    /// #558): the reference's `abortAdmissibleOutputs(beef, outputsToAbort)`
    /// (`TopicManager.ts`, ts-stack `f999e0c1a`), called by its
    /// `Engine.abortProvisionalAdmissions` (`Engine.ts:685-700`) when a
    /// `current-tx` broadcast throws after `identifyAdmissibleOutputs` ran.
    /// `outputs_to_abort` are the indices the manager admitted; `beef` is
    /// the submitted BEEF.
    ///
    /// Who calls it here: [`abort_provisional_admissions`], through
    /// [`crate::engine::Engine::abort_provisional_admissions`] or a host that
    /// keeps its own managers. The engine's own broadcast is never fatal
    /// (the reference's default `throwOnBroadcastFailure = false`, under
    /// which the reference does not abort either), so `Engine::submit` never
    /// calls it; a host whose door judges the network's word AFTER the write
    /// (the Cloudflare worker's eviction, register row D1) calls it there.
    ///
    /// DEFAULT: a no-op, so every manager that keeps its validation
    /// read-only (all sixteen of this workspace's) is unchanged. An `Err` is
    /// logged and dropped, as the reference catches and logs it. A manager
    /// may be asked again for the same outputs (a host's eviction that
    /// faulted half way and ran again): it must be idempotent.
    async fn abort_admissible_outputs(
        &self,
        _beef: &[u8],
        _outputs_to_abort: &[u32],
    ) -> Result<(), TopicManagerError> {
        Ok(())
    }

    /// Whether this manager implements [`Self::abort_admissible_outputs`]:
    /// the Rust form of the reference's OPTIONAL member
    /// (`abortAdmissibleOutputs?`, which its engine skips when `undefined`).
    /// DEFAULT `false`. It is a COST hint for a host that must READ to build
    /// the call (the worker's eviction reads the admitted rows and the BEEF
    /// only when some manager answers `true`); the engine's own
    /// [`abort_provisional_admissions`] asks every manager whatever this
    /// answers, as it costs nothing there. A manager that overrides the abort
    /// answers `true` here too.
    fn aborts_admissions(&self) -> bool {
        false
    }

    /// Optional advert expiry for a sync root, in Unix seconds. This does
    /// not change submission admission or the treatment of ancestry.
    fn sync_expiry(&self, _script: &bsv_rs::script::LockingScript) -> Option<u64> {
        None
    }

    /// Return Markdown documentation for this topic manager.
    async fn get_documentation(&self) -> String;

    /// Return metadata identifying this topic manager.
    async fn get_metadata(&self) -> ServiceMetadata;
}

/// The reference's `abortProvisionalAdmissions` (`Engine.ts:685-700` at
/// ts-stack `f999e0c1a`), one spelling for the engine and for a host that
/// keeps its own managers (bsv-low #558): each `(topic, outputs)` of
/// `admissions` whose outputs are not empty and whose topic has a manager in
/// `manager_of` is told [`TopicManager::abort_admissible_outputs`] once, in
/// order; an `Err` is logged and the rest are still told. The reference
/// skips a dupe and a failed topic; a caller passes only the topics whose
/// outputs it admitted. Answers how many managers were told.
pub async fn abort_provisional_admissions<'m>(
    manager_of: impl Fn(&str) -> Option<&'m dyn TopicManager>,
    beef: &[u8],
    admissions: &[(String, Vec<u32>)],
) -> u32 {
    let mut told = 0;
    for (topic, outputs) in admissions {
        if outputs.is_empty() {
            continue;
        }
        let Some(manager) = manager_of(topic) else {
            continue;
        };
        told += 1;
        if let Err(e) = manager.abort_admissible_outputs(beef, outputs).await {
            tracing::error!(
                "Error aborting provisional topic admission: topic={topic} outputs={outputs:?} error={e} (bsv-low #558)"
            );
        }
    }
    told
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
            _tx: &Transaction,
            _previous_coins: &[u8],
            _off_chain_values: Option<&[u8]>,
            _mode: SubmitMode,
            _context: &TopicAdmittanceContext,
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
            _tx: &Transaction,
            _previous_coins: &[u8],
            _off_chain_values: Option<&[u8]>,
            _mode: SubmitMode,
            _context: &TopicAdmittanceContext,
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
            .identify_admissible_outputs(
                &Transaction::new(),
                &[],
                None,
                SubmitMode::CurrentTx,
                &TopicAdmittanceContext::default(),
            )
            .await
            .unwrap();
        assert_eq!(result.outputs_to_admit, vec![0, 1, 2]);
        assert!(result.has_activity());
    }

    #[tokio::test]
    async fn test_reject_all_manager() {
        let mgr = RejectAllManager;
        let result = mgr
            .identify_admissible_outputs(
                &Transaction::new(),
                &[],
                None,
                SubmitMode::CurrentTx,
                &TopicAdmittanceContext::default(),
            )
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
            .identify_admissible_outputs(
                &Transaction::new(),
                &[],
                None,
                SubmitMode::HistoricalTx,
                &TopicAdmittanceContext::default(),
            )
            .await
            .unwrap();
        assert_eq!(result.outputs_to_admit.len(), 3);
    }

    /// bsv-low #558: the abort's DEFAULT is a no-op that answers `Ok`, and a
    /// manager that does not implement it says so (`aborts_admissions`
    /// false), so every manager written before the member compiles and
    /// behaves unchanged. The workspace's sixteen are audited by the
    /// worker's `i558_every_workspace_manager_keeps_the_default`.
    #[tokio::test]
    async fn i558_the_abort_defaults_to_a_no_op() {
        for mgr in [
            Box::new(AdmitAllManager) as Box<dyn TopicManager>,
            Box::new(RejectAllManager),
        ] {
            assert!(mgr.abort_admissible_outputs(&[], &[0, 1]).await.is_ok());
            assert!(!mgr.aborts_admissions());
        }
    }
}
