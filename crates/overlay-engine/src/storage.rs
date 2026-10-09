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

    /// Mark a transaction as already carrying its own merkle proof, WITHOUT
    /// rewriting its BEEF.
    ///
    /// This is the lightweight flag-flip used by
    /// [`Engine::complete_missing_proofs`](crate::Engine::complete_missing_proofs)
    /// when a scanned candidate turns out to ALREADY be proven (its stored BEEF
    /// proves the target tx) yet its backend `has_proof` flag is still `0` —
    /// e.g. a GASP-synced tx that arrived with a proof, written before overlay
    /// migration 0010 added the flag (the migration defaulted every existing
    /// row to `0`). Such rows are returned by
    /// [`Storage::find_transactions_for_proof_check`] every tick and skipped by
    /// the engine, clogging the candidate window and starving genuinely
    /// proofless rows behind them. Flipping the flag here drops them out.
    ///
    /// The reference D1 backend runs
    /// `UPDATE transactions SET has_proof = 1 WHERE txid = ?`. Backends that do
    /// not track a proof flag may use this no-op default. Idempotent.
    async fn mark_transaction_proven(&self, txid: &str) -> Result<(), StorageError> {
        let _ = txid;
        Ok(())
    }

    /// [`Self::mark_transaction_proven`] that ALSO records the block height
    /// the just-verified bump anchors the tx to (`proofHeight`, bsv-low M19
    /// R2 round 3 review MED-2), so the revalidation sweep's transactions
    /// leg can window the row. `None` records no anchor. Default: the plain
    /// latch (a backend without the column loses only the windowing, never
    /// correctness).
    async fn mark_transaction_proven_at(
        &self,
        txid: &str,
        height: Option<u64>,
    ) -> Result<(), StorageError> {
        let _ = height;
        self.mark_transaction_proven(txid).await
    }

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

    /// bsv-low PLAN-PRE-LOOP4 §H4 (2026-09-06): forget a (txid, topic) applied
    /// record so a re-submit of those bytes is re-validated instead of
    /// deduplicated away. The caller proves the row is PHANTOM (no stored
    /// output of that txid on that topic) before asking — see
    /// `Engine::forget_phantom_applied`.
    async fn delete_applied_transaction(&self, tx: &AppliedTransaction)
        -> Result<(), StorageError>;

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
    ///
    /// `min_age_secs` is the PUSH-PRIMARY BACKSTOP gate (bsv-low #228 /
    /// arcade#259): rows stored less than `min_age_secs` ago are EXCLUDED —
    /// their proof is expected to arrive via the Arcade MINED webhook
    /// (`/arc-ingest`) at push speed, so polling them is wasted budget. Rules:
    /// - `0` disables the gate (poll everything — today's behaviour).
    /// - a row whose age is UNKNOWN (pre-migration `NULL` timestamp) MUST be
    ///   treated as OLD, i.e. eligible — the fail-safe direction is to poll
    ///   MORE, never to starve a row of its backstop.
    /// The reference D1 backend answers with
    /// `AND (created_at IS NULL OR created_at <= unixepoch() - min_age_secs)`.
    async fn find_transactions_for_proof_check(
        &self,
        limit: u64,
        min_age_secs: u64,
    ) -> Result<Vec<TransactionBeef>, StorageError> {
        let _ = (limit, min_age_secs);
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

    // ========================================================================
    // GASP peer health (bsv-low#302 — dead-peer quarantine)
    // ========================================================================

    // The three peer-health methods are REQUIRED (bsv-low #555, the
    // delta-2 fold's D2-L2): with defaults a wrapper that did not forward
    // one compiled and silently turned the quarantine or the yieldless bound
    // off. A backend that keeps no peer health answers as the old defaults
    // did (`Ok(())`, `PeerSyncHealth::default()`, `PeerYieldStreak::default()`):
    // fail-safe, every peer keeps being attempted. `host` is the peer's
    // NORMALIZED ORIGIN (`crate::gasp::peer_origin`), not its URL: the engine
    // passes it (the delta-2 fold's D2-M2).

    /// Record the outcome of ONE GASP sync attempt with `host` for `topic`
    /// (bsv-low#302). `success = true` resets the consecutive-failure count
    /// to 0 (full re-admission); `false` increments it. The backend stamps
    /// its own clock for the attempt time — the engine never supplies wall
    /// time. Quarantine-SKIPPED peers are NOT recorded (a skip is not an
    /// attempt; the last-attempt age must keep growing so the re-probe
    /// window opens).
    async fn record_peer_sync_outcome(
        &self,
        host: &str,
        topic: &str,
        success: bool,
    ) -> Result<(), StorageError>;

    /// Current sync health of the (host, topic) pairing (bsv-low#302). The
    /// backend answers with its consecutive-failure count and the AGE of
    /// the last attempt (relative seconds, so the engine needs no clock of
    /// its own). Pristine: never attempted, never quarantined.
    async fn get_peer_sync_health(
        &self,
        host: &str,
        topic: &str,
    ) -> Result<PeerSyncHealth, StorageError>;

    /// Record whether ONE GASP sync with `host` for `topic` YIELDED (it
    /// finalized a graph or moved the cursor) or not, and answer the
    /// yieldless STREAK it now holds (bsv-low #555, the delta fold's D-M2 and
    /// the delta-2 fold's D2-M1; `crate::gasp::yieldless_sync_failed`):
    /// - `true` ends the streak: count 0, no start; answers that.
    /// - `false` adds one to the streak and answers its count and the
    ///   seconds since its FIRST yieldless sync, on the backend's own clock
    ///   (0 on the sync that starts it). A streak whose LAST yieldless sync
    ///   is more than `crate::gasp::PEER_YIELDLESS_DECAY_SECS` old starts
    ///   again (count 1, age 0): the decay.
    ///
    /// The engine calls it only with a per-graph budget, only for a sync
    /// the peer served work, and before [`Self::record_peer_sync_outcome`].
    async fn record_peer_sync_yield(
        &self,
        host: &str,
        topic: &str,
        yielded: bool,
    ) -> Result<PeerYieldStreak, StorageError>;

    // ========================================================================
    // Deferred GASP graphs (bsv-low #555)
    // ========================================================================

    /// Save (REPLACE) the record of one deferred GASP graph, keyed by
    /// (`record.peer`, `record.topic`, `record.outpoint`). One row per graph:
    /// a later deferral of the same graph overwrites it, never appends.
    /// `AtCeiling`: refused at a bound of the storage's own (the worker's
    /// global ceiling), counted `too_many`; the walk then goes on under the
    /// per-peer budget alone, as before #555.
    ///
    /// The four deferred-graph methods are REQUIRED (the lens fold's M2,
    /// the house style of `TopicManager::identify_admissible_outputs`): with
    /// a default, a storage or WRAPPER that did not forward them compiled,
    /// and `Engine::set_graph_budget` over it saved nothing, walked every
    /// graph past the call budget from its root on every tick and never
    /// admitted it (`store_fault`), a hard cap on graph size worse than no
    /// budget. A wrapper forwards all four to the storage it wraps:
    ///
    /// ```ignore
    /// async fn put_deferred_graph(&self, record: &DeferredGraph)
    ///     -> Result<DeferredGraphSave, StorageError> {
    ///     self.inner.put_deferred_graph(record).await
    /// }
    /// // and the same for find_deferred_graphs, get_deferred_graph and
    /// // delete_deferred_graph.
    /// ```
    ///
    /// A backend that keeps no records answers none from
    /// `find_deferred_graphs` and `get_deferred_graph` and an `Err` from
    /// this one: every graph past the budget then fails its UTXO, so it
    /// must not be given a per-graph budget.
    async fn put_deferred_graph(
        &self,
        record: &crate::gasp::DeferredGraph,
    ) -> Result<crate::gasp::DeferredGraphSave, StorageError>;

    /// The KEYS of the records of (`host`, `topic`), lowest score first. A
    /// sync reads these up front and each record only when its UTXO is
    /// served (the lens fold's L3).
    async fn find_deferred_graphs(
        &self,
        host: &str,
        topic: &str,
    ) -> Result<Vec<crate::gasp::DeferredGraphKey>, StorageError>;

    /// The record of the graph of (`host`, `topic`) rooted at `outpoint`.
    async fn get_deferred_graph(
        &self,
        host: &str,
        topic: &str,
        outpoint: &str,
    ) -> Result<Option<crate::gasp::DeferredGraph>, StorageError>;

    /// Delete the record of the graph rooted at `outpoint`.
    async fn delete_deferred_graph(
        &self,
        host: &str,
        topic: &str,
        outpoint: &str,
    ) -> Result<(), StorageError>;
}

/// A (host, topic)'s yieldless streak (bsv-low #555, the delta-2 fold's
/// D2-M1) — the input to [`crate::gasp::yieldless_sync_failed`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PeerYieldStreak {
    /// Consecutive yieldless syncs (0 after a yield).
    pub yieldless_syncs: u64,
    /// Seconds since the streak's first yieldless sync. `None` = no streak,
    /// or a backend with no clock: never past the time bound.
    pub secs_since_first: Option<u64>,
}

/// Durable per-(host, topic) GASP sync health (bsv-low#302) — the input to
/// the pure quarantine rule [`crate::gasp::peer_sync_quarantined`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PeerSyncHealth {
    /// Consecutive sync attempts that ALL failed (timeout counts as a
    /// failure). Reset to 0 by any successful sync.
    pub consecutive_failures: u64,
    /// Seconds since the last recorded sync ATTEMPT (success or failure).
    /// `None` = never attempted / no health tracked — never quarantined.
    pub secs_since_last_attempt: Option<u64>,
}

// ============================================================================
// Rc delegation
// ============================================================================

/// Delegating impl so an `Rc<T: Storage>` is itself a `Storage` — lets a
/// caller keep a shared handle to the concrete backend (e.g. a test advancing
/// [`memory::MemoryStorage::advance_clock`]) while the engine owns a
/// `Box<dyn Storage>` over the same instance. Pure delegation, no semantics.
#[async_trait(?Send)]
impl<T: Storage + ?Sized> Storage for std::rc::Rc<T> {
    async fn insert_output(&self, output: &Output) -> Result<(), StorageError> {
        (**self).insert_output(output).await
    }
    async fn delete_output(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
    ) -> Result<(), StorageError> {
        (**self).delete_output(txid, output_index, topic).await
    }
    async fn mark_utxo_as_spent(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
    ) -> Result<(), StorageError> {
        (**self).mark_utxo_as_spent(txid, output_index, topic).await
    }
    async fn update_consumed_by(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
        consumed_by: &[Outpoint],
    ) -> Result<(), StorageError> {
        (**self)
            .update_consumed_by(txid, output_index, topic, consumed_by)
            .await
    }
    async fn update_transaction_beef(&self, txid: &str, beef: &[u8]) -> Result<(), StorageError> {
        (**self).update_transaction_beef(txid, beef).await
    }
    async fn mark_transaction_proven(&self, txid: &str) -> Result<(), StorageError> {
        (**self).mark_transaction_proven(txid).await
    }
    async fn mark_transaction_proven_at(
        &self,
        txid: &str,
        height: Option<u64>,
    ) -> Result<(), StorageError> {
        // review LOW-4: forward the anchored latch too — without this the
        // Rc blanket hits the trait default (drops the height, re-introducing
        // MED-2) for any caller routing through an `Rc<dyn Storage>`.
        (**self).mark_transaction_proven_at(txid, height).await
    }
    async fn update_output_block_height(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
        block_height: u32,
    ) -> Result<(), StorageError> {
        (**self)
            .update_output_block_height(txid, output_index, topic, block_height)
            .await
    }
    async fn insert_applied_transaction(
        &self,
        tx: &AppliedTransaction,
    ) -> Result<(), StorageError> {
        (**self).insert_applied_transaction(tx).await
    }
    async fn does_applied_transaction_exist(
        &self,
        tx: &AppliedTransaction,
    ) -> Result<bool, StorageError> {
        (**self).does_applied_transaction_exist(tx).await
    }
    async fn delete_applied_transaction(
        &self,
        tx: &AppliedTransaction,
    ) -> Result<(), StorageError> {
        (**self).delete_applied_transaction(tx).await
    }
    async fn find_output(
        &self,
        txid: &str,
        output_index: u32,
        topic: Option<&str>,
        spent: Option<bool>,
        include_beef: bool,
    ) -> Result<Option<Output>, StorageError> {
        (**self)
            .find_output(txid, output_index, topic, spent, include_beef)
            .await
    }
    async fn find_outputs_for_transaction(
        &self,
        txid: &str,
        include_beef: bool,
    ) -> Result<Vec<Output>, StorageError> {
        (**self)
            .find_outputs_for_transaction(txid, include_beef)
            .await
    }
    async fn find_utxos_for_topic(
        &self,
        topic: &str,
        since: Option<f64>,
        limit: Option<u64>,
        include_beef: bool,
    ) -> Result<Vec<Output>, StorageError> {
        (**self)
            .find_utxos_for_topic(topic, since, limit, include_beef)
            .await
    }
    async fn find_transactions_for_proof_check(
        &self,
        limit: u64,
        min_age_secs: u64,
    ) -> Result<Vec<TransactionBeef>, StorageError> {
        (**self)
            .find_transactions_for_proof_check(limit, min_age_secs)
            .await
    }
    async fn update_last_interaction(
        &self,
        host: &str,
        topic: &str,
        since: u64,
    ) -> Result<(), StorageError> {
        (**self).update_last_interaction(host, topic, since).await
    }
    async fn get_last_interaction(&self, host: &str, topic: &str) -> Result<u64, StorageError> {
        (**self).get_last_interaction(host, topic).await
    }
    async fn record_peer_sync_outcome(
        &self,
        host: &str,
        topic: &str,
        success: bool,
    ) -> Result<(), StorageError> {
        (**self)
            .record_peer_sync_outcome(host, topic, success)
            .await
    }
    async fn get_peer_sync_health(
        &self,
        host: &str,
        topic: &str,
    ) -> Result<PeerSyncHealth, StorageError> {
        (**self).get_peer_sync_health(host, topic).await
    }
    async fn record_peer_sync_yield(
        &self,
        host: &str,
        topic: &str,
        yielded: bool,
    ) -> Result<PeerYieldStreak, StorageError> {
        (**self).record_peer_sync_yield(host, topic, yielded).await
    }
    async fn put_deferred_graph(
        &self,
        record: &crate::gasp::DeferredGraph,
    ) -> Result<crate::gasp::DeferredGraphSave, StorageError> {
        (**self).put_deferred_graph(record).await
    }
    async fn find_deferred_graphs(
        &self,
        host: &str,
        topic: &str,
    ) -> Result<Vec<crate::gasp::DeferredGraphKey>, StorageError> {
        (**self).find_deferred_graphs(host, topic).await
    }
    async fn get_deferred_graph(
        &self,
        host: &str,
        topic: &str,
        outpoint: &str,
    ) -> Result<Option<crate::gasp::DeferredGraph>, StorageError> {
        (**self).get_deferred_graph(host, topic, outpoint).await
    }
    async fn delete_deferred_graph(
        &self,
        host: &str,
        topic: &str,
        outpoint: &str,
    ) -> Result<(), StorageError> {
        (**self).delete_deferred_graph(host, topic, outpoint).await
    }
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
        /// txids whose `has_proof` flag is set. Models the real D1
        /// `transactions.has_proof` column (overlay migration 0010), which is
        /// the source of truth for `find_transactions_for_proof_check` — NOT a
        /// live re-parse of the BEEF. A txid is absent here (flag = 0) until
        /// something explicitly proves it, so a BEEF that already carries a
        /// proof but is not in this set is still returned as a candidate — the
        /// exact window-clog the cron must clear via `mark_transaction_proven`.
        proven: Mutex<std::collections::HashSet<String>>,
        /// applied transactions keyed by (txid, topic)
        applied: Mutex<HashMap<(String, String), bool>>,
        /// GASP sync state keyed by (host, topic)
        sync_state: Mutex<HashMap<(String, String), u64>>,
        /// Deterministic logical clock (seconds) for the push-primary backstop
        /// age gate — models the D1 backend's `unixepoch()`. Tests advance it
        /// with [`MemoryStorage::advance_clock`]; no wall clock is ever read
        /// (wasm-safe, deterministic).
        clock_secs: Mutex<u64>,
        /// First-store stamp (clock seconds) per txid — models the D1
        /// `transactions.created_at` column. A txid ABSENT here has UNKNOWN
        /// age and is treated as OLD (eligible) — the fail-safe direction.
        created_at: Mutex<HashMap<String, u64>>,
        /// GASP peer health keyed by (host, topic) —
        /// `(consecutive_failures, last_attempt_at_clock_secs)`. Models the
        /// D1 `gasp_peer_health` table (bsv-low#302); the logical clock
        /// above stands in for `unixepoch()`.
        peer_health: Mutex<HashMap<(String, String), (u64, u64)>>,
        /// review LOW-4: records the height an `_at` latch carried, so a pin
        /// can prove the `Rc` blanket forwards `mark_transaction_proven_at`
        /// (the trait default would drop the height).
        proven_at: Mutex<HashMap<String, Option<u64>>>,
        /// Deferred GASP graphs (bsv-low #555) keyed by (host, topic,
        /// outpoint): models the D1 `gasp_deferred_graphs` table.
        deferred_graphs: Mutex<HashMap<(String, String, String), crate::gasp::DeferredGraph>>,
        /// How many times a deferred graph was WRITTEN (a pin counts the
        /// storage writes of a deferral).
        deferred_graph_writes: Mutex<u64>,
        /// How many single records were READ (bsv-low #555, the lens
        /// fold's L3).
        deferred_graph_reads: Mutex<u64>,
        /// Test knob: the key read of the deferred graphs faults.
        deferred_graph_keys_fault: Mutex<bool>,
        /// Test knob: a NEW record past this many held is refused
        /// `AtCeiling` (the worker's global ceiling).
        deferred_graph_ceiling: Mutex<Option<usize>>,
        /// Test knob: every save of a deferred graph faults.
        deferred_graph_put_fault: Mutex<bool>,
        /// How many saves of a deferred graph were ASKED (written, refused
        /// or faulted).
        deferred_graph_put_attempts: Mutex<u64>,
        /// The yieldless streak keyed by (host, topic): `(count,
        /// first_at, last_at)` on the logical clock. Models the D1
        /// `gasp_peer_health` columns `yieldless_syncs`, `first_yieldless_at`
        /// and `last_yieldless_at` (bsv-low #555, the delta fold's D-M2 and
        /// the delta-2 fold's D2-M1).
        peer_yieldless: Mutex<HashMap<(String, String), YieldStreakRow>>,
    }

    /// `(count, first_at, last_at)` of a yieldless streak, on the logical
    /// clock.
    type YieldStreakRow = (u64, u64, u64);

    impl MemoryStorage {
        pub fn new() -> Self {
            Self::default()
        }

        /// The anchor height a `mark_transaction_proven_at` recorded for
        /// `txid` (review LOW-4): `None` = never called `_at`; `Some(h)` =
        /// called with height `h`.
        pub fn proven_at_height(&self, txid: &str) -> Option<Option<u64>> {
            self.proven_at.lock().unwrap().get(txid).copied()
        }

        /// Advance the deterministic logical clock by `secs` (test hook for
        /// the push-primary backstop age gate — see
        /// [`Storage::find_transactions_for_proof_check`]).
        pub fn advance_clock(&self, secs: u64) {
            *self.clock_secs.lock().unwrap() += secs;
        }

        /// Stamp `txid`'s first-store time at the current clock if absent
        /// (models `COALESCE(existing, unixepoch())` in the D1 backend).
        fn stamp_created(&self, txid: &str) {
            let now = *self.clock_secs.lock().unwrap();
            self.created_at
                .lock()
                .unwrap()
                .entry(txid.to_string())
                .or_insert(now);
        }

        /// Whether `txid` clears the backstop age gate: unknown age is OLD
        /// (eligible, fail-safe); otherwise `clock - created >= min_age_secs`.
        fn age_gate_open(&self, txid: &str, min_age_secs: u64) -> bool {
            if min_age_secs == 0 {
                return true;
            }
            let now = *self.clock_secs.lock().unwrap();
            match self.created_at.lock().unwrap().get(txid) {
                None => true, // unknown age → treated old → eligible
                Some(created) => now.saturating_sub(*created) >= min_age_secs,
            }
        }

        /// Every deferred GASP graph held, any peer or topic (bsv-low #555).
        pub fn deferred_graphs(&self) -> Vec<crate::gasp::DeferredGraph> {
            let mut all: Vec<_> = self
                .deferred_graphs
                .lock()
                .unwrap()
                .values()
                .cloned()
                .collect();
            all.sort_by(|a, b| (a.score, &a.outpoint).cmp(&(b.score, &b.outpoint)));
            all
        }

        /// How many deferred-graph writes were made (bsv-low #555).
        pub fn deferred_graph_writes(&self) -> u64 {
            *self.deferred_graph_writes.lock().unwrap()
        }

        /// How many single records were read (bsv-low #555).
        pub fn deferred_graph_reads(&self) -> u64 {
            *self.deferred_graph_reads.lock().unwrap()
        }

        /// Make the key read of the deferred graphs fault, or not (a test
        /// knob, bsv-low #555).
        pub fn set_deferred_graph_keys_fault(&self, fault: bool) {
            *self.deferred_graph_keys_fault.lock().unwrap() = fault;
        }

        /// Refuse a NEW record `AtCeiling` once this many are held, as the
        /// worker's global ceiling does (a test knob, bsv-low #555).
        pub fn set_deferred_graph_ceiling(&self, ceiling: Option<usize>) {
            *self.deferred_graph_ceiling.lock().unwrap() = ceiling;
        }

        /// Make every save of a deferred graph fault, or not (a test knob,
        /// bsv-low #555's delta fold, D-L2).
        pub fn set_deferred_graph_put_fault(&self, fault: bool) {
            *self.deferred_graph_put_fault.lock().unwrap() = fault;
        }

        /// How many saves of a deferred graph were asked, whatever their
        /// answer (bsv-low #555's delta fold, D-L1).
        pub fn deferred_graph_put_attempts(&self) -> u64 {
            *self.deferred_graph_put_attempts.lock().unwrap()
        }

        /// The consecutive yieldless syncs held for (host, topic)
        /// (bsv-low #555's delta fold, D-M2). `host` may be a peer URL: it
        /// is read under its [`crate::gasp::peer_origin`], the key the
        /// engine writes (the delta-2 fold's D2-M2).
        pub fn peer_yieldless_syncs(&self, host: &str, topic: &str) -> u64 {
            self.peer_yieldless
                .lock()
                .unwrap()
                .get(&(crate::gasp::peer_origin(host), topic.to_string()))
                .map_or(0, |(count, _, _)| *count)
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
                self.stamp_created(&output.txid);
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
                self.proven.lock().unwrap().remove(txid);
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
            // Model the D1 backend: keep the proof flag accurate on every BEEF
            // write by inspecting whether the new BEEF proves the target tx.
            // (The proof-completion stitch calls back here with a proven BEEF,
            // which is how a row legitimately flips proofless → proven.)
            let has_proof = bsv_rs::transaction::Beef::from_binary(beef)
                .ok()
                .and_then(|b| {
                    b.find_txid(txid)
                        .map(bsv_rs::transaction::BeefTx::has_proof)
                })
                .unwrap_or(false);
            if has_proof {
                self.proven.lock().unwrap().insert(txid.to_string());
            }
            self.transactions
                .lock()
                .unwrap()
                .insert(txid.to_string(), beef.to_vec());
            // Preserve-or-stamp: a rewrite keeps the ORIGINAL first-store
            // time (models the D1 COALESCE), so the backstop age stays real.
            self.stamp_created(txid);
            Ok(())
        }

        async fn mark_transaction_proven(&self, txid: &str) -> Result<(), StorageError> {
            // Lightweight flag-flip (no BEEF rewrite), idempotent. Mirrors the
            // D1 `UPDATE transactions SET has_proof = 1 WHERE txid = ?`.
            self.proven.lock().unwrap().insert(txid.to_string());
            Ok(())
        }

        async fn mark_transaction_proven_at(
            &self,
            txid: &str,
            height: Option<u64>,
        ) -> Result<(), StorageError> {
            // Models the D1 `MARK_TX_PROVEN_AT_SQL`: flip has_proof AND record
            // the anchor (review MED-2/LOW-4). Recorded so the Rc-forward pin
            // can observe the height reaching the inner store.
            self.proven.lock().unwrap().insert(txid.to_string());
            self.proven_at
                .lock()
                .unwrap()
                .insert(txid.to_string(), height);
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

        async fn delete_applied_transaction(
            &self,
            tx: &AppliedTransaction,
        ) -> Result<(), StorageError> {
            self.applied
                .lock()
                .unwrap()
                .remove(&(tx.txid.clone(), tx.topic.clone()));
            Ok(())
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
            min_age_secs: u64,
        ) -> Result<Vec<TransactionBeef>, StorageError> {
            // Models the real D1 `WHERE has_proof = 0 LIMIT {limit}` query
            // (overlay migration 0010): the candidate set is driven by the
            // `has_proof` FLAG, not a live re-parse of the BEEF. A row whose
            // BEEF already carries a proof but whose flag is still 0 (e.g. a
            // GASP-synced proof written before the migration, which defaulted
            // every existing row to 0) is therefore STILL returned here — it
            // only drops out once `mark_transaction_proven` flips the flag.
            // This reproduces the window-clog the engine must clear.
            //
            // The push-primary backstop age gate (#228) excludes rows younger
            // than `min_age_secs` (their proof is expected via /arc-ingest);
            // unknown-age rows stay eligible (fail-safe — trait doc).
            let candidates: Vec<TransactionBeef> = {
                let transactions = self.transactions.lock().unwrap();
                let proven = self.proven.lock().unwrap();
                transactions
                    .iter()
                    .filter(|(txid, _)| !proven.contains(*txid))
                    .map(|(txid, beef)| TransactionBeef {
                        txid: txid.clone(),
                        beef: beef.clone(),
                    })
                    .collect()
            };
            Ok(candidates
                .into_iter()
                .filter(|c| self.age_gate_open(&c.txid, min_age_secs))
                .take(limit as usize)
                .collect())
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

        async fn record_peer_sync_outcome(
            &self,
            host: &str,
            topic: &str,
            success: bool,
        ) -> Result<(), StorageError> {
            let now = *self.clock_secs.lock().unwrap();
            let mut health = self.peer_health.lock().unwrap();
            let entry = health
                .entry((host.to_string(), topic.to_string()))
                .or_insert((0, now));
            entry.0 = if success { 0 } else { entry.0 + 1 };
            entry.1 = now;
            Ok(())
        }

        async fn get_peer_sync_health(
            &self,
            host: &str,
            topic: &str,
        ) -> Result<PeerSyncHealth, StorageError> {
            let now = *self.clock_secs.lock().unwrap();
            Ok(self
                .peer_health
                .lock()
                .unwrap()
                .get(&(host.to_string(), topic.to_string()))
                .map(|(fails, attempt_at)| PeerSyncHealth {
                    consecutive_failures: *fails,
                    secs_since_last_attempt: Some(now.saturating_sub(*attempt_at)),
                })
                .unwrap_or_default())
        }

        async fn record_peer_sync_yield(
            &self,
            host: &str,
            topic: &str,
            yielded: bool,
        ) -> Result<PeerYieldStreak, StorageError> {
            let now = *self.clock_secs.lock().unwrap();
            let mut held = self.peer_yieldless.lock().unwrap();
            let key = (host.to_string(), topic.to_string());
            if yielded {
                held.remove(&key);
                return Ok(PeerYieldStreak::default());
            }
            let streak = held.entry(key).or_insert((0, now, now));
            if streak.0 == 0
                || now.saturating_sub(streak.2) > crate::gasp::PEER_YIELDLESS_DECAY_SECS
            {
                *streak = (0, now, now);
            }
            streak.0 += 1;
            streak.2 = now;
            Ok(PeerYieldStreak {
                yieldless_syncs: streak.0,
                secs_since_first: Some(now.saturating_sub(streak.1)),
            })
        }

        async fn put_deferred_graph(
            &self,
            record: &crate::gasp::DeferredGraph,
        ) -> Result<crate::gasp::DeferredGraphSave, StorageError> {
            let key = (
                record.peer.clone(),
                record.topic.clone(),
                record.outpoint.clone(),
            );
            *self.deferred_graph_put_attempts.lock().unwrap() += 1;
            if *self.deferred_graph_put_fault.lock().unwrap() {
                return Err(StorageError::Database("deferred graph save faulted".into()));
            }
            if let Some(ceiling) = *self.deferred_graph_ceiling.lock().unwrap() {
                let held = self.deferred_graphs.lock().unwrap();
                if !held.contains_key(&key) && held.len() >= ceiling {
                    return Ok(crate::gasp::DeferredGraphSave::AtCeiling);
                }
            }
            *self.deferred_graph_writes.lock().unwrap() += 1;
            self.deferred_graphs.lock().unwrap().insert(
                (
                    record.peer.clone(),
                    record.topic.clone(),
                    record.outpoint.clone(),
                ),
                record.clone(),
            );
            Ok(crate::gasp::DeferredGraphSave::Saved)
        }

        async fn find_deferred_graphs(
            &self,
            host: &str,
            topic: &str,
        ) -> Result<Vec<crate::gasp::DeferredGraphKey>, StorageError> {
            if *self.deferred_graph_keys_fault.lock().unwrap() {
                return Err(StorageError::Database("deferred graph keys: fault".into()));
            }
            Ok(self
                .deferred_graphs()
                .into_iter()
                .filter(|r| r.peer == host && r.topic == topic)
                .map(|r| crate::gasp::DeferredGraphKey {
                    outpoint: r.outpoint,
                    score: r.score,
                })
                .collect())
        }

        async fn get_deferred_graph(
            &self,
            host: &str,
            topic: &str,
            outpoint: &str,
        ) -> Result<Option<crate::gasp::DeferredGraph>, StorageError> {
            *self.deferred_graph_reads.lock().unwrap() += 1;
            Ok(self
                .deferred_graphs
                .lock()
                .unwrap()
                .get(&(host.to_string(), topic.to_string(), outpoint.to_string()))
                .cloned())
        }

        async fn delete_deferred_graph(
            &self,
            host: &str,
            topic: &str,
            outpoint: &str,
        ) -> Result<(), StorageError> {
            self.deferred_graphs.lock().unwrap().remove(&(
                host.to_string(),
                topic.to_string(),
                outpoint.to_string(),
            ));
            Ok(())
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

    /// review LOW-4: the `Rc<T: Storage>` blanket must FORWARD
    /// `mark_transaction_proven_at`, not fall through to the trait default
    /// (which drops the height, re-introducing MED-2). Call it through an
    /// `Rc<dyn Storage>` and assert the height reached the inner store.
    #[tokio::test]
    async fn the_rc_blanket_forwards_the_anchored_proven_latch() {
        let store = std::rc::Rc::new(MemoryStorage::new());
        let via_dyn: std::rc::Rc<dyn Storage> = store.clone();
        via_dyn
            .mark_transaction_proven_at("tx", Some(965_772))
            .await
            .unwrap();
        assert_eq!(
            store.proven_at_height("tx"),
            Some(Some(965_772)),
            "the Rc blanket forwarded the height to the inner store"
        );
        // a height-less call still forwards (records None, not "never called")
        via_dyn
            .mark_transaction_proven_at("tx2", None)
            .await
            .unwrap();
        assert_eq!(store.proven_at_height("tx2"), Some(None));
        assert_eq!(
            store.proven_at_height("never"),
            None,
            "a txid never latched is absent"
        );
    }

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
