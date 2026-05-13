//! LookupService trait — indexes admitted outputs and answers lookup queries.
//!
//! Each lookup service in the overlay is notified when outputs are admitted or
//! spent, and responds to queries from clients. The Engine calls lifecycle hooks
//! during submit() and delegates query handling to lookup().
//!
//! Ported from `~/bsv/overlay-services/src/LookupService.ts`.

use async_trait::async_trait;

use crate::types::{
    AdmissionMode, LookupQuestion, LookupResult, OutputAdmittedByTopic, OutputSpent,
    ServiceMetadata, SpendNotificationMode,
};

#[cfg(test)]
use crate::types::UTXOReference;

/// Indexes overlay outputs and answers lookup queries.
///
/// Implementations handle specific types of data — e.g., SHIPLookupService
/// indexes SHIP advertisements and answers "which nodes host topic X?".
///
/// The Engine calls lifecycle hooks (`output_admitted_by_topic`, `output_spent`,
/// etc.) during transaction processing, and `lookup()` when clients query.
#[async_trait(?Send)]
pub trait LookupService {
    /// How the Engine should deliver data when an output is admitted.
    /// - `LockingScript`: provides txid, outputIndex, topic, satoshis, locking script
    /// - `WholeTx`: provides the entire Atomic BEEF
    fn admission_mode(&self) -> AdmissionMode;

    /// How the Engine should notify when an output is spent.
    /// - `None`: no notification
    /// - `Txid`: just the spending txid
    /// - `Script`: spending txid + unlocking script + input details
    /// - `WholeTx`: entire spending transaction as Atomic BEEF
    fn spend_notification_mode(&self) -> SpendNotificationMode;

    // ========================================================================
    // Lifecycle hooks (called by Engine during submit)
    // ========================================================================

    /// Called when a Topic Manager admits a new UTXO.
    ///
    /// The lookup service should index this output so it can be found via `lookup()`.
    /// Payload shape depends on `admission_mode()`.
    async fn output_admitted_by_topic(
        &self,
        payload: &OutputAdmittedByTopic,
    ) -> Result<(), LookupServiceError>;

    /// Called when a previously-admitted UTXO is spent.
    ///
    /// Payload shape depends on `spend_notification_mode()`.
    /// Default: no-op (for services with SpendNotificationMode::None).
    async fn output_spent(&self, _payload: &OutputSpent) -> Result<(), LookupServiceError> {
        Ok(())
    }

    /// Called when an output is permanently evicted from the overlay.
    ///
    /// The lookup service MUST remove this output from all indices.
    /// After eviction, the output MUST NOT appear in any future lookup answers.
    async fn output_evicted(&self, txid: &str, output_index: u32)
        -> Result<(), LookupServiceError>;

    /// Called when historical retention of an output is no longer needed.
    ///
    /// Default: no-op.
    async fn output_no_longer_retained_in_history(
        &self,
        _txid: &str,
        _output_index: u32,
        _topic: &str,
    ) -> Result<(), LookupServiceError> {
        Ok(())
    }

    // ========================================================================
    // Query API
    // ========================================================================

    /// Answer a lookup query.
    ///
    /// Returns a [`LookupResult`] in one of two shapes:
    ///
    /// - `LookupResult::OutputList(refs)` — common case. The Engine
    ///   hydrates each `UTXOReference` with the BEEF from storage and
    ///   assembles a `LookupAnswer::OutputList`. This is the cheap path:
    ///   services don't need direct access to BEEF storage.
    ///
    /// - `LookupResult::Answer(answer)` — escape hatch. Services that
    ///   return aggregate stats (`Freeform`) or formula chains
    ///   (`Formula`) emit the full answer themselves; the Engine passes
    ///   it through verbatim.
    ///
    /// Existing impls that returned `Vec<UTXOReference>` migrate
    /// mechanically by wrapping in `LookupResult::OutputList(refs)` (or
    /// using the `From<Vec<UTXOReference>>` impl).
    async fn lookup(&self, question: &LookupQuestion) -> Result<LookupResult, LookupServiceError>;

    // ========================================================================
    // Documentation
    // ========================================================================

    /// Return Markdown documentation for this lookup service.
    async fn get_documentation(&self) -> String;

    /// Return metadata identifying this lookup service.
    async fn get_metadata(&self) -> ServiceMetadata;
}

/// Errors from LookupService operations.
#[derive(Debug, thiserror::Error)]
pub enum LookupServiceError {
    /// The query is malformed or missing required fields.
    #[error("invalid query: {0}")]
    InvalidQuery(String),

    /// Storage operation failed.
    #[error("storage error: {0}")]
    StorageError(String),

    /// The service does not support this query type.
    #[error("unsupported query: {0}")]
    Unsupported(String),

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

    /// Mock lookup service that stores references in memory.
    struct MockLookupService {
        records: std::sync::Mutex<Vec<UTXOReference>>,
    }

    impl MockLookupService {
        fn new() -> Self {
            Self {
                records: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn record_count(&self) -> usize {
            self.records.lock().unwrap().len()
        }
    }

    #[async_trait(?Send)]
    impl LookupService for MockLookupService {
        fn admission_mode(&self) -> AdmissionMode {
            AdmissionMode::LockingScript
        }

        fn spend_notification_mode(&self) -> SpendNotificationMode {
            SpendNotificationMode::None
        }

        async fn output_admitted_by_topic(
            &self,
            payload: &OutputAdmittedByTopic,
        ) -> Result<(), LookupServiceError> {
            let (txid, output_index) = match payload {
                OutputAdmittedByTopic::LockingScript {
                    txid, output_index, ..
                } => (txid.clone(), *output_index),
                OutputAdmittedByTopic::WholeTx { output_index, .. } => {
                    ("whole-tx".to_string(), *output_index)
                }
            };
            self.records
                .lock()
                .unwrap()
                .push(UTXOReference { txid, output_index });
            Ok(())
        }

        async fn output_evicted(
            &self,
            txid: &str,
            output_index: u32,
        ) -> Result<(), LookupServiceError> {
            self.records
                .lock()
                .unwrap()
                .retain(|r| !(r.txid == txid && r.output_index == output_index));
            Ok(())
        }

        async fn lookup(
            &self,
            _question: &LookupQuestion,
        ) -> Result<LookupResult, LookupServiceError> {
            Ok(LookupResult::OutputList(
                self.records.lock().unwrap().clone(),
            ))
        }

        async fn get_documentation(&self) -> String {
            "Mock lookup service for testing.".to_string()
        }

        async fn get_metadata(&self) -> ServiceMetadata {
            ServiceMetadata {
                name: "mock-lookup".to_string(),
                description: Some("Test lookup service".to_string()),
                ..Default::default()
            }
        }
    }

    #[tokio::test]
    async fn test_output_admitted_and_lookup() {
        let svc = MockLookupService::new();

        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "abc".to_string(),
            output_index: 0,
            topic: "tm_test".to_string(),
            satoshis: 1000,
            locking_script: vec![0x76],
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(svc.record_count(), 1);

        let question = LookupQuestion::new("ls_test", serde_json::json!({}));
        let result = svc.lookup(&question).await.unwrap();
        let LookupResult::OutputList(refs) = result else {
            panic!("expected OutputList");
        };
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].txid, "abc");
        assert_eq!(refs[0].output_index, 0);
    }

    #[tokio::test]
    async fn test_output_evicted() {
        let svc = MockLookupService::new();

        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "abc".to_string(),
            output_index: 0,
            topic: "tm_test".to_string(),
            satoshis: 1000,
            locking_script: vec![],
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(svc.record_count(), 1);

        svc.output_evicted("abc", 0).await.unwrap();
        assert_eq!(svc.record_count(), 0);

        // Lookup should return empty
        let question = LookupQuestion::new("ls_test", serde_json::json!({}));
        let result = svc.lookup(&question).await.unwrap();
        let LookupResult::OutputList(refs) = result else {
            panic!("expected OutputList");
        };
        assert!(refs.is_empty());
    }

    #[tokio::test]
    async fn test_default_output_spent_is_noop() {
        let svc = MockLookupService::new();
        let payload = OutputSpent::None {
            txid: "abc".to_string(),
            output_index: 0,
            topic: "tm_test".to_string(),
        };
        // Default impl should not error
        svc.output_spent(&payload).await.unwrap();
    }

    #[tokio::test]
    async fn test_default_output_no_longer_retained() {
        let svc = MockLookupService::new();
        svc.output_no_longer_retained_in_history("abc", 0, "tm_test")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_lookup_service_is_object_safe() {
        let svc: Box<dyn LookupService> = Box::new(MockLookupService::new());
        assert_eq!(svc.admission_mode(), AdmissionMode::LockingScript);
        assert_eq!(svc.spend_notification_mode(), SpendNotificationMode::None);

        let meta = svc.get_metadata().await;
        assert_eq!(meta.name, "mock-lookup");
    }

    #[tokio::test]
    async fn test_multiple_admissions_and_selective_eviction() {
        let svc = MockLookupService::new();

        for i in 0..5 {
            let payload = OutputAdmittedByTopic::LockingScript {
                txid: format!("tx{i}"),
                output_index: 0,
                topic: "tm_test".to_string(),
                satoshis: 1000,
                locking_script: vec![],
                off_chain_values: None,
            };
            svc.output_admitted_by_topic(&payload).await.unwrap();
        }
        assert_eq!(svc.record_count(), 5);

        // Evict only tx2
        svc.output_evicted("tx2", 0).await.unwrap();
        assert_eq!(svc.record_count(), 4);

        let result = svc
            .lookup(&LookupQuestion::new("ls_test", serde_json::json!({})))
            .await
            .unwrap();
        let LookupResult::OutputList(refs) = result else {
            panic!("expected OutputList");
        };
        assert!(!refs.iter().any(|r| r.txid == "tx2"));
    }
}
