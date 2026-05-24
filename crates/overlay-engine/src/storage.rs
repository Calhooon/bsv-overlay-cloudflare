//! Storage trait for the Overlay Services Engine.
//!
//! Defines the async storage interface that backends (D1, SQLite, in-memory) must implement.
//! The Engine never talks to a database directly — everything goes through this trait.
//!
//! Ported from `~/bsv/overlay-services/src/storage/Storage.ts`.
//! Reference implementation: `~/bsv/overlay-services/src/storage/knex/KnexStorage.ts`.

use async_trait::async_trait;

use crate::types::{AppliedTransaction, Outpoint, Output};

/// A stored transaction's txid plus its serialized BEEF bytes.
///
/// Returned by [`Storage::find_transactions_for_proof_check`] so the engine can
/// parse the BEEF and decide whether the target tx still needs a merkle proof.
#[derive(Debug, Clone)]
pub struct TransactionBeef {
    /// The transaction id (hex, lowercase).
    pub txid: String,
    /// The serialized BEEF bytes for this transaction.
    pub beef: Vec<u8>,
}

/// Overlay Services storage backend.
///
/// All methods are async. Uses `?Send` futures for wasm32 compatibility
/// (Cloudflare Workers D1).
#[async_trait(?Send)]
pub trait Storage {
    // ========================================================================
    // Write operations
    // ========================================================================

    /// Insert a new output into storage.
    ///
    /// If an output with the same (txid, outputIndex, topic) already exists, this is a no-op.
    /// If `output.beef` is Some, also upsert the BEEF into the transactions table
    /// (deduplicated by txid).
    async fn insert_output(&self, output: &Output) -> Result<(), StorageError>;

    /// Delete an output from storage.
    ///
    /// After deletion, if no other outputs reference the same txid, also delete
    /// the transaction's BEEF from the transactions table.
    async fn delete_output(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
    ) -> Result<(), StorageError>;

    /// Mark an output as spent.
    async fn mark_utxo_as_spent(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
    ) -> Result<(), StorageError>;

    /// Update the `consumed_by` list on an existing output.
    async fn update_consumed_by(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
        consumed_by: &[Outpoint],
    ) -> Result<(), StorageError>;

    /// Update the BEEF data for a transaction (used when merkle proofs arrive).
    async fn update_transaction_beef(&self, txid: &str, beef: &[u8]) -> Result<(), StorageError>;

    /// Update the block height on an output (when it gets mined).
    ///
    /// Optional — backends that don't track block height can provide a no-op default.
    async fn update_output_block_height(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
        block_height: u32,
    ) -> Result<(), StorageError> {
        let _ = (txid, output_index, topic, block_height);
        Ok(())
    }

    /// Record that a transaction has been applied to a topic (deduplication).
    async fn insert_applied_transaction(&self, tx: &AppliedTransaction)
        -> Result<(), StorageError>;

    /// Check if a transaction has already been applied to a topic.
    async fn does_applied_transaction_exist(
        &self,
        tx: &AppliedTransaction,
    ) -> Result<bool, StorageError>;

    // ========================================================================
    // Read operations
    // ========================================================================

    /// Find a single output by txid + outputIndex, with optional topic and spent filters.
    ///
    /// If `include_beef` is true, load the BEEF from the transactions table and
    /// attach it to the returned Output.
    async fn find_output(
        &self,
        txid: &str,
        output_index: u32,
        topic: Option<&str>,
        spent: Option<bool>,
        include_beef: bool,
    ) -> Result<Option<Output>, StorageError>;

    /// Batch-find outputs by outpoints. More efficient than individual find_output calls.
    ///
    /// Default implementation falls back to individual lookups.
    async fn find_outputs_by_outpoints(
        &self,
        outpoints: &[Outpoint],
        include_beef: bool,
    ) -> Result<Vec<Output>, StorageError> {
        let mut results = Vec::with_capacity(outpoints.len());
        for op in outpoints {
            if let Some(output) = self
                .find_output(&op.txid, op.output_index, None, None, include_beef)
                .await?
            {
                results.push(output);
            }
        }
        Ok(results)
    }

    /// Find all outputs for a given transaction.
    async fn find_outputs_for_transaction(
        &self,
        txid: &str,
        include_beef: bool,
    ) -> Result<Vec<Output>, StorageError>;

    /// Find unspent outputs for a topic, ordered by score ascending.
    ///
    /// - `since`: minimum score threshold (exclusive of scores below this)
    /// - `limit`: maximum number of results
    async fn find_utxos_for_topic(
        &self,
        topic: &str,
        since: Option<f64>,
        limit: Option<u64>,
        include_beef: bool,
    ) -> Result<Vec<Output>, StorageError>;

    /// Return a bounded page of *proofless* stored transactions (txid + BEEF
    /// bytes) for proof-completion scanning.
    ///
    /// Backends MUST return only transactions whose own merkle proof is still
    /// missing — i.e. the historical/recent backlog the
    /// [`Engine::complete_missing_proofs`](crate::Engine::complete_missing_proofs)
    /// cron is meant to clear. The reference D1 backend keeps a `has_proof`
    /// flag (overlay migration 0010), set on every BEEF write, and answers this
    /// with `WHERE has_proof = 0 LIMIT {limit}` — so every proofless tx is
    /// eventually reached (no "newest N rows only" starvation) and proven rows
    /// are never re-fetched. The engine still defensively re-parses each
    /// returned BEEF and skips any that turn out already-proven, so an
    /// over-inclusive backend is merely less efficient, not incorrect.
    ///
    /// Backends that cannot enumerate transactions (or have nothing to
    /// complete) may return an empty `Vec` via this default, in which case
    /// proof completion is a no-op.
    ///
    /// `limit` bounds the returned page (and therefore the per-tick WoC fetch /
    /// CPU budget).
    async fn find_transactions_for_proof_check(
        &self,
        limit: u64,
    ) -> Result<Vec<TransactionBeef>, StorageError> {
        let _ = limit;
        Ok(Vec::new())
    }

    // ========================================================================
    // GASP sync state
    // ========================================================================

    /// Update the last interaction score for a host+topic pair (upsert).
    async fn update_last_interaction(
        &self,
        host: &str,
        topic: &str,
        since: u64,
    ) -> Result<(), StorageError>;

    /// Get the last interaction score for a host+topic pair. Returns 0 if not found.
    async fn get_last_interaction(&self, host: &str, topic: &str) -> Result<u64, StorageError>;
}

// ============================================================================
// Error type
// ============================================================================

/// Storage operation errors.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("duplicate: {0}")]
    Duplicate(String),

    #[error("database error: {0}")]
    Database(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("{0}")]
    Other(String),
}

// ============================================================================
// In-memory storage (for tests and local dev)
// ============================================================================

#[cfg(any(test, feature = "memory-storage"))]
pub mod memory {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// In-memory Storage implementation for testing and local development.
    ///
    /// NOT suitable for production — no persistence, no concurrency beyond Mutex.
    #[derive(Debug, Default)]
    pub struct MemoryStorage {
        /// outputs keyed by (txid, outputIndex, topic)
        outputs: Mutex<HashMap<(String, u32, String), Output>>,
        /// BEEF data keyed by txid
        transactions: Mutex<HashMap<String, Vec<u8>>>,
        /// applied transactions keyed by (txid, topic)
        applied: Mutex<HashMap<(String, String), bool>>,
        /// GASP sync state keyed by (host, topic)
        sync_state: Mutex<HashMap<(String, String), u64>>,
    }

    impl MemoryStorage {
        pub fn new() -> Self {
            Self::default()
        }

        /// Count total outputs (for testing assertions).
        pub fn output_count(&self) -> usize {
            self.outputs.lock().unwrap().len()
        }

        /// Count total transactions (for testing assertions).
        pub fn transaction_count(&self) -> usize {
            self.transactions.lock().unwrap().len()
        }
    }

    #[async_trait(?Send)]
    impl Storage for MemoryStorage {
        async fn insert_output(&self, output: &Output) -> Result<(), StorageError> {
            let key = (
                output.txid.clone(),
                output.output_index,
                output.topic.clone(),
            );
            let mut outputs = self.outputs.lock().unwrap();

            // No-op if already exists (match KnexStorage behavior)
            if outputs.contains_key(&key) {
                // Still upsert BEEF if provided
                if let Some(ref beef) = output.beef {
                    self.transactions
                        .lock()
                        .unwrap()
                        .entry(output.txid.clone())
                        .or_insert_with(|| beef.clone());
                }
                return Ok(());
            }

            // Store output without BEEF (BEEF goes in transactions table)
            let mut stored = output.clone();
            stored.beef = None;
            outputs.insert(key, stored);

            // Upsert BEEF into transactions table
            if let Some(ref beef) = output.beef {
                self.transactions
                    .lock()
                    .unwrap()
                    .entry(output.txid.clone())
                    .or_insert_with(|| beef.clone());
            }

            Ok(())
        }

        async fn delete_output(
            &self,
            txid: &str,
            output_index: u32,
            topic: &str,
        ) -> Result<(), StorageError> {
            let key = (txid.to_string(), output_index, topic.to_string());
            let mut outputs = self.outputs.lock().unwrap();
            outputs.remove(&key);

            // If no more outputs reference this txid, remove the BEEF
            let has_remaining = outputs.keys().any(|(t, _, _)| t == txid);
            if !has_remaining {
                self.transactions.lock().unwrap().remove(txid);
            }

            Ok(())
        }

        async fn mark_utxo_as_spent(
            &self,
            txid: &str,
            output_index: u32,
            topic: &str,
        ) -> Result<(), StorageError> {
            let key = (txid.to_string(), output_index, topic.to_string());
            if let Some(output) = self.outputs.lock().unwrap().get_mut(&key) {
                output.spent = true;
            }
            Ok(())
        }

        async fn update_consumed_by(
            &self,
            txid: &str,
            output_index: u32,
            topic: &str,
            consumed_by: &[Outpoint],
        ) -> Result<(), StorageError> {
            let key = (txid.to_string(), output_index, topic.to_string());
            if let Some(output) = self.outputs.lock().unwrap().get_mut(&key) {
                output.consumed_by = consumed_by.to_vec();
            }
            Ok(())
        }

        async fn update_transaction_beef(
            &self,
            txid: &str,
            beef: &[u8],
        ) -> Result<(), StorageError> {
            self.transactions
                .lock()
                .unwrap()
                .insert(txid.to_string(), beef.to_vec());
            Ok(())
        }

        async fn update_output_block_height(
            &self,
            txid: &str,
            output_index: u32,
            topic: &str,
            block_height: u32,
        ) -> Result<(), StorageError> {
            let key = (txid.to_string(), output_index, topic.to_string());
            if let Some(output) = self.outputs.lock().unwrap().get_mut(&key) {
                output.block_height = Some(block_height);
            }
            Ok(())
        }

        async fn insert_applied_transaction(
            &self,
            tx: &AppliedTransaction,
        ) -> Result<(), StorageError> {
            self.applied
                .lock()
                .unwrap()
                .insert((tx.txid.clone(), tx.topic.clone()), true);
            Ok(())
        }

        async fn does_applied_transaction_exist(
            &self,
            tx: &AppliedTransaction,
        ) -> Result<bool, StorageError> {
            Ok(self
                .applied
                .lock()
                .unwrap()
                .contains_key(&(tx.txid.clone(), tx.topic.clone())))
        }

        async fn find_output(
            &self,
            txid: &str,
            output_index: u32,
            topic: Option<&str>,
            spent: Option<bool>,
            include_beef: bool,
        ) -> Result<Option<Output>, StorageError> {
            let outputs = self.outputs.lock().unwrap();

            // If topic is specified, do a direct lookup
            if let Some(topic) = topic {
                let key = (txid.to_string(), output_index, topic.to_string());
                if let Some(output) = outputs.get(&key) {
                    if let Some(s) = spent {
                        if output.spent != s {
                            return Ok(None);
                        }
                    }
                    let mut result = output.clone();
                    if include_beef {
                        result.beef = self.transactions.lock().unwrap().get(txid).cloned();
                    }
                    return Ok(Some(result));
                }
                return Ok(None);
            }

            // No topic — find first matching (txid, outputIndex)
            for ((t, oi, _), output) in outputs.iter() {
                if t == txid && *oi == output_index {
                    if let Some(s) = spent {
                        if output.spent != s {
                            continue;
                        }
                    }
                    let mut result = output.clone();
                    if include_beef {
                        result.beef = self.transactions.lock().unwrap().get(txid).cloned();
                    }
                    return Ok(Some(result));
                }
            }
            Ok(None)
        }

        async fn find_outputs_for_transaction(
            &self,
            txid: &str,
            include_beef: bool,
        ) -> Result<Vec<Output>, StorageError> {
            let outputs = self.outputs.lock().unwrap();
            let beef = if include_beef {
                self.transactions.lock().unwrap().get(txid).cloned()
            } else {
                None
            };

            let results: Vec<Output> = outputs
                .iter()
                .filter(|((t, _, _), _)| t == txid)
                .map(|(_, output)| {
                    let mut o = output.clone();
                    if include_beef {
                        o.beef.clone_from(&beef);
                    }
                    o
                })
                .collect();

            Ok(results)
        }

        async fn find_utxos_for_topic(
            &self,
            topic: &str,
            since: Option<f64>,
            limit: Option<u64>,
            include_beef: bool,
        ) -> Result<Vec<Output>, StorageError> {
            let outputs = self.outputs.lock().unwrap();
            let transactions = self.transactions.lock().unwrap();

            let mut results: Vec<Output> = outputs
                .iter()
                .filter(|((_, _, t), o)| {
                    t == topic && !o.spent && since.is_none_or(|s| o.score.unwrap_or(0.0) >= s)
                })
                .map(|(_, output)| {
                    let mut o = output.clone();
                    if include_beef {
                        o.beef = transactions.get(&o.txid).cloned();
                    }
                    o
                })
                .collect();

            // Sort by score ascending (matching KnexStorage behavior)
            results.sort_by(|a, b| {
                a.score
                    .unwrap_or(0.0)
                    .partial_cmp(&b.score.unwrap_or(0.0))
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            if let Some(limit) = limit {
                results.truncate(limit as usize);
            }

            Ok(results)
        }

        async fn find_transactions_for_proof_check(
            &self,
            limit: u64,
        ) -> Result<Vec<TransactionBeef>, StorageError> {
            // Models the real D1 `WHERE has_proof = 0 LIMIT {limit}` query
            // (overlay migration 0010): return only txs whose own proof is
            // still missing. Proven rows are filtered out *before* the limit
            // is applied, so a single proofless tx is always reachable no
            // matter how many proven txs sit alongside it — which is exactly
            // the historical-backlog reach the cron must guarantee.
            let transactions = self.transactions.lock().unwrap();
            let results = transactions
                .iter()
                .filter(|(txid, beef)| {
                    // Keep only txs that do NOT yet carry their own proof.
                    bsv_rs::transaction::Beef::from_binary(beef)
                        .ok()
                        .and_then(|b| {
                            b.find_txid(txid)
                                .map(bsv_rs::transaction::BeefTx::has_proof)
                        })
                        != Some(true)
                })
                .take(limit as usize)
                .map(|(txid, beef)| TransactionBeef {
                    txid: txid.clone(),
                    beef: beef.clone(),
                })
                .collect();
            Ok(results)
        }

        async fn update_last_interaction(
            &self,
            host: &str,
            topic: &str,
            since: u64,
        ) -> Result<(), StorageError> {
            self.sync_state
                .lock()
                .unwrap()
                .insert((host.to_string(), topic.to_string()), since);
            Ok(())
        }

        async fn get_last_interaction(&self, host: &str, topic: &str) -> Result<u64, StorageError> {
            Ok(*self
                .sync_state
                .lock()
                .unwrap()
                .get(&(host.to_string(), topic.to_string()))
                .unwrap_or(&0))
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::memory::MemoryStorage;
    use super::*;
    use crate::types::AppliedTransaction;

    fn make_output(txid: &str, index: u32, topic: &str, score: f64) -> Output {
        Output {
            txid: txid.to_string(),
            output_index: index,
            output_script: vec![0x76, 0xa9],
            satoshis: 1000,
            topic: topic.to_string(),
            spent: false,
            outputs_consumed: vec![],
            consumed_by: vec![],
            beef: Some(vec![0xBE, 0xEF]),
            block_height: None,
            score: Some(score),
        }
    }

    #[tokio::test]
    async fn test_insert_and_find_output() {
        let store = MemoryStorage::new();
        let output = make_output("abc", 0, "tm_test", 1.0);
        store.insert_output(&output).await.unwrap();

        // Find without BEEF
        let found = store
            .find_output("abc", 0, Some("tm_test"), None, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.txid, "abc");
        assert_eq!(found.output_index, 0);
        assert!(found.beef.is_none());

        // Find with BEEF
        let found = store
            .find_output("abc", 0, Some("tm_test"), None, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.beef.unwrap(), vec![0xBE, 0xEF]);
    }

    #[tokio::test]
    async fn test_insert_duplicate_is_noop() {
        let store = MemoryStorage::new();
        let output = make_output("abc", 0, "tm_test", 1.0);
        store.insert_output(&output).await.unwrap();
        store.insert_output(&output).await.unwrap();
        assert_eq!(store.output_count(), 1);
    }

    #[tokio::test]
    async fn test_delete_output_cleans_up_beef() {
        let store = MemoryStorage::new();
        let output = make_output("abc", 0, "tm_test", 1.0);
        store.insert_output(&output).await.unwrap();
        assert_eq!(store.transaction_count(), 1);

        store.delete_output("abc", 0, "tm_test").await.unwrap();
        assert_eq!(store.output_count(), 0);
        assert_eq!(store.transaction_count(), 0); // BEEF cleaned up
    }

    #[tokio::test]
    async fn test_delete_output_keeps_beef_if_other_outputs_exist() {
        let store = MemoryStorage::new();
        store
            .insert_output(&make_output("abc", 0, "tm_test", 1.0))
            .await
            .unwrap();
        store
            .insert_output(&make_output("abc", 1, "tm_test", 2.0))
            .await
            .unwrap();
        assert_eq!(store.output_count(), 2);
        assert_eq!(store.transaction_count(), 1);

        store.delete_output("abc", 0, "tm_test").await.unwrap();
        assert_eq!(store.output_count(), 1);
        assert_eq!(store.transaction_count(), 1); // BEEF kept for output 1
    }

    #[tokio::test]
    async fn test_mark_utxo_as_spent() {
        let store = MemoryStorage::new();
        store
            .insert_output(&make_output("abc", 0, "tm_test", 1.0))
            .await
            .unwrap();

        store.mark_utxo_as_spent("abc", 0, "tm_test").await.unwrap();

        let found = store
            .find_output("abc", 0, Some("tm_test"), None, false)
            .await
            .unwrap()
            .unwrap();
        assert!(found.spent);
    }

    #[tokio::test]
    async fn test_find_output_with_spent_filter() {
        let store = MemoryStorage::new();
        store
            .insert_output(&make_output("abc", 0, "tm_test", 1.0))
            .await
            .unwrap();

        // Not spent — should find with spent=false, not with spent=true
        assert!(store
            .find_output("abc", 0, Some("tm_test"), Some(false), false)
            .await
            .unwrap()
            .is_some());
        assert!(store
            .find_output("abc", 0, Some("tm_test"), Some(true), false)
            .await
            .unwrap()
            .is_none());

        store.mark_utxo_as_spent("abc", 0, "tm_test").await.unwrap();

        // Now spent — reversed
        assert!(store
            .find_output("abc", 0, Some("tm_test"), Some(true), false)
            .await
            .unwrap()
            .is_some());
        assert!(store
            .find_output("abc", 0, Some("tm_test"), Some(false), false)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_update_consumed_by() {
        let store = MemoryStorage::new();
        store
            .insert_output(&make_output("abc", 0, "tm_test", 1.0))
            .await
            .unwrap();

        let consumed = vec![Outpoint::new("def", 0), Outpoint::new("ghi", 1)];
        store
            .update_consumed_by("abc", 0, "tm_test", &consumed)
            .await
            .unwrap();

        let found = store
            .find_output("abc", 0, Some("tm_test"), None, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.consumed_by.len(), 2);
        assert_eq!(found.consumed_by[0].txid, "def");
        assert_eq!(found.consumed_by[1].output_index, 1);
    }

    #[tokio::test]
    async fn test_update_transaction_beef() {
        let store = MemoryStorage::new();
        store
            .insert_output(&make_output("abc", 0, "tm_test", 1.0))
            .await
            .unwrap();

        store
            .update_transaction_beef("abc", &[0xDE, 0xAD])
            .await
            .unwrap();

        let found = store
            .find_output("abc", 0, Some("tm_test"), None, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.beef.unwrap(), vec![0xDE, 0xAD]);
    }

    #[tokio::test]
    async fn test_update_output_block_height() {
        let store = MemoryStorage::new();
        store
            .insert_output(&make_output("abc", 0, "tm_test", 1.0))
            .await
            .unwrap();

        store
            .update_output_block_height("abc", 0, "tm_test", 850_000)
            .await
            .unwrap();

        let found = store
            .find_output("abc", 0, Some("tm_test"), None, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.block_height, Some(850_000));
    }

    #[tokio::test]
    async fn test_find_outputs_for_transaction() {
        let store = MemoryStorage::new();
        store
            .insert_output(&make_output("abc", 0, "tm_test", 1.0))
            .await
            .unwrap();
        store
            .insert_output(&make_output("abc", 1, "tm_test", 2.0))
            .await
            .unwrap();
        store
            .insert_output(&make_output("other", 0, "tm_test", 3.0))
            .await
            .unwrap();

        let results = store
            .find_outputs_for_transaction("abc", false)
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_find_utxos_for_topic() {
        let store = MemoryStorage::new();
        store
            .insert_output(&make_output("a", 0, "tm_test", 1.0))
            .await
            .unwrap();
        store
            .insert_output(&make_output("b", 0, "tm_test", 2.0))
            .await
            .unwrap();
        store
            .insert_output(&make_output("c", 0, "tm_test", 3.0))
            .await
            .unwrap();
        store
            .insert_output(&make_output("d", 0, "tm_other", 4.0))
            .await
            .unwrap();

        // Mark one as spent
        store.mark_utxo_as_spent("b", 0, "tm_test").await.unwrap();

        // All unspent for tm_test
        let results = store
            .find_utxos_for_topic("tm_test", None, None, false)
            .await
            .unwrap();
        assert_eq!(results.len(), 2); // a and c (b is spent)

        // With since filter
        let results = store
            .find_utxos_for_topic("tm_test", Some(2.0), None, false)
            .await
            .unwrap();
        assert_eq!(results.len(), 1); // only c (score 3.0 >= 2.0, a is 1.0)

        // With limit
        let results = store
            .find_utxos_for_topic("tm_test", None, Some(1), false)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "a"); // lowest score first
    }

    #[tokio::test]
    async fn test_find_utxos_for_topic_sorted_by_score() {
        let store = MemoryStorage::new();
        store
            .insert_output(&make_output("c", 0, "tm_test", 30.0))
            .await
            .unwrap();
        store
            .insert_output(&make_output("a", 0, "tm_test", 10.0))
            .await
            .unwrap();
        store
            .insert_output(&make_output("b", 0, "tm_test", 20.0))
            .await
            .unwrap();

        let results = store
            .find_utxos_for_topic("tm_test", None, None, false)
            .await
            .unwrap();
        assert_eq!(results[0].txid, "a");
        assert_eq!(results[1].txid, "b");
        assert_eq!(results[2].txid, "c");
    }

    #[tokio::test]
    async fn test_applied_transaction() {
        let store = MemoryStorage::new();
        let tx = AppliedTransaction {
            txid: "abc".to_string(),
            topic: "tm_test".to_string(),
        };

        assert!(!store.does_applied_transaction_exist(&tx).await.unwrap());
        store.insert_applied_transaction(&tx).await.unwrap();
        assert!(store.does_applied_transaction_exist(&tx).await.unwrap());

        // Different topic — should not exist
        let tx2 = AppliedTransaction {
            txid: "abc".to_string(),
            topic: "tm_other".to_string(),
        };
        assert!(!store.does_applied_transaction_exist(&tx2).await.unwrap());
    }

    #[tokio::test]
    async fn test_sync_state() {
        let store = MemoryStorage::new();

        assert_eq!(
            store
                .get_last_interaction("host1", "tm_test")
                .await
                .unwrap(),
            0
        );

        store
            .update_last_interaction("host1", "tm_test", 100)
            .await
            .unwrap();
        assert_eq!(
            store
                .get_last_interaction("host1", "tm_test")
                .await
                .unwrap(),
            100
        );

        // Update (upsert)
        store
            .update_last_interaction("host1", "tm_test", 200)
            .await
            .unwrap();
        assert_eq!(
            store
                .get_last_interaction("host1", "tm_test")
                .await
                .unwrap(),
            200
        );

        // Different host — independent
        assert_eq!(
            store
                .get_last_interaction("host2", "tm_test")
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn test_find_outputs_by_outpoints_batch() {
        let store = MemoryStorage::new();
        store
            .insert_output(&make_output("a", 0, "tm_test", 1.0))
            .await
            .unwrap();
        store
            .insert_output(&make_output("b", 0, "tm_test", 2.0))
            .await
            .unwrap();
        store
            .insert_output(&make_output("c", 0, "tm_test", 3.0))
            .await
            .unwrap();

        let outpoints = vec![
            Outpoint::new("a", 0),
            Outpoint::new("c", 0),
            Outpoint::new("missing", 0),
        ];
        let results = store
            .find_outputs_by_outpoints(&outpoints, false)
            .await
            .unwrap();
        assert_eq!(results.len(), 2); // a and c found, missing skipped
    }

    #[tokio::test]
    async fn test_find_output_without_topic() {
        let store = MemoryStorage::new();
        store
            .insert_output(&make_output("abc", 0, "tm_test", 1.0))
            .await
            .unwrap();

        // Find without specifying topic
        let found = store
            .find_output("abc", 0, None, None, false)
            .await
            .unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().topic, "tm_test");
    }
}
