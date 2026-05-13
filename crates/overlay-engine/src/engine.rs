//! Overlay Services Engine — the core orchestrator.
//!
//! Receives transactions (submit), answers queries (lookup), manages advertisements,
//! and coordinates GASP sync. All storage, topic management, and lookup service
//! operations go through trait interfaces.
//!
//! Ported from `~/bsv/overlay-services/src/Engine.ts` (1,337 lines).

use bsv_rs::transaction::Transaction;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tracing::{error, info, warn};

use crate::advertiser::{Advertiser, AdvertiserError};
use crate::broadcaster::{ArcBroadcaster, Broadcaster};
use crate::lookup_service::{LookupService, LookupServiceError};
use crate::storage::{Storage, StorageError};
use crate::topic_manager::{TopicManager, TopicManagerError};
use crate::types::*;

/// Controls how much spend history to include in lookup responses.
///
/// Maps to the TS `historySelector` parameter which can be a number
/// (depth limit) or an async function (per-output decider).
#[derive(Debug, Clone)]
pub enum HistorySelector {
    /// Include up to N levels of ancestor spend history.
    Depth(u32),
}

/// Configuration for constructing the Engine.
pub struct EngineConfig {
    /// URL where this engine is hosted. Required for advertisement sync.
    pub hosting_url: Option<String>,
    /// Known SHIP tracker URLs for bootstrapping.
    pub ship_trackers: Vec<String>,
    /// Known SLAP tracker URLs for bootstrapping.
    pub slap_trackers: Vec<String>,
    /// Configuration for GASP topic synchronization.
    pub sync_configuration: SyncConfiguration,
    /// Whether to suppress default SHIP/SLAP sync advertisements.
    pub suppress_default_sync_advertisements: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            hosting_url: None,
            ship_trackers: Vec::new(),
            slap_trackers: Vec::new(),
            sync_configuration: HashMap::new(),
            suppress_default_sync_advertisements: true,
        }
    }
}

/// The Overlay Services Engine.
///
/// Orchestrates topic managers, lookup services, storage, and advertisements.
/// Does NOT include an HTTP server — that's the job of overlay-cloudflare.
pub struct Engine {
    managers: HashMap<String, Box<dyn TopicManager>>,
    lookup_services: HashMap<String, Box<dyn LookupService>>,
    storage: Box<dyn Storage>,
    advertiser: Option<Box<dyn Advertiser>>,
    broadcaster: Option<Box<dyn Broadcaster>>,
    arc_broadcaster: Option<Box<dyn ArcBroadcaster>>,
    chain_tracker: Option<Box<dyn bsv_rs::transaction::ChainTracker>>,
    gasp_remote_factory: Option<Box<dyn crate::gasp::GASPRemoteFactory>>,
    config: EngineConfig,
}

/// Internal result from Phase 1 + 2 validation.
///
/// Carries per-topic admittance decisions, previous coins, and the parsed
/// transaction so that Phase 3 (mutations) can proceed without re-parsing.
struct TopicValidation {
    topic: String,
    is_dupe: bool,
    /// Input indices that spend previously-admitted outputs from this topic.
    previous_coins: Vec<u32>,
    /// The previous outputs found in storage (parallel to previous_coins).
    previous_outputs: Vec<Output>,
    admittance: AdmittanceInstructions,
    failed: bool,
}

impl Engine {
    /// Create a new Overlay Services Engine.
    pub fn new(
        managers: HashMap<String, Box<dyn TopicManager>>,
        lookup_services: HashMap<String, Box<dyn LookupService>>,
        storage: Box<dyn Storage>,
        advertiser: Option<Box<dyn Advertiser>>,
        config: EngineConfig,
    ) -> Self {
        Self::with_chain_tracker(
            managers,
            lookup_services,
            storage,
            advertiser,
            None,
            None,
            config,
        )
    }

    /// Create a new Engine with an optional ChainTracker for SPV verification.
    pub fn with_chain_tracker(
        managers: HashMap<String, Box<dyn TopicManager>>,
        lookup_services: HashMap<String, Box<dyn LookupService>>,
        storage: Box<dyn Storage>,
        advertiser: Option<Box<dyn Advertiser>>,
        broadcaster: Option<Box<dyn Broadcaster>>,
        chain_tracker: Option<Box<dyn bsv_rs::transaction::ChainTracker>>,
        config: EngineConfig,
    ) -> Self {
        Self::with_all(
            managers,
            lookup_services,
            storage,
            advertiser,
            broadcaster,
            None,
            chain_tracker,
            config,
        )
    }

    /// Create a new Engine with all optional components.
    #[allow(clippy::too_many_arguments)]
    pub fn with_all(
        managers: HashMap<String, Box<dyn TopicManager>>,
        lookup_services: HashMap<String, Box<dyn LookupService>>,
        storage: Box<dyn Storage>,
        advertiser: Option<Box<dyn Advertiser>>,
        broadcaster: Option<Box<dyn Broadcaster>>,
        arc_broadcaster: Option<Box<dyn ArcBroadcaster>>,
        chain_tracker: Option<Box<dyn bsv_rs::transaction::ChainTracker>>,
        mut config: EngineConfig,
    ) -> Self {
        // Build default sync configuration: SHIP for all topics except tm_ship/tm_slap
        // which get their trackers merged with provided shipTrackers/slapTrackers.
        for manager_name in managers.keys() {
            if manager_name == "tm_ship" {
                if matches!(
                    config.sync_configuration.get(manager_name),
                    Some(SyncTarget::Disabled)
                ) {
                    continue;
                }
                let mut combined: HashSet<String> = HashSet::new();
                if let Some(SyncTarget::Peers(peers)) = config.sync_configuration.get(manager_name)
                {
                    combined.extend(peers.iter().cloned());
                }
                combined.extend(config.ship_trackers.iter().cloned());
                if !combined.is_empty() {
                    config.sync_configuration.insert(
                        manager_name.clone(),
                        SyncTarget::Peers(combined.into_iter().collect()),
                    );
                }
            } else if manager_name == "tm_slap" {
                if matches!(
                    config.sync_configuration.get(manager_name),
                    Some(SyncTarget::Disabled)
                ) {
                    continue;
                }
                let mut combined: HashSet<String> = HashSet::new();
                if let Some(SyncTarget::Peers(peers)) = config.sync_configuration.get(manager_name)
                {
                    combined.extend(peers.iter().cloned());
                }
                combined.extend(config.slap_trackers.iter().cloned());
                if !combined.is_empty() {
                    config.sync_configuration.insert(
                        manager_name.clone(),
                        SyncTarget::Peers(combined.into_iter().collect()),
                    );
                }
            } else if !config.sync_configuration.contains_key(manager_name) {
                config
                    .sync_configuration
                    .insert(manager_name.clone(), SyncTarget::Ship);
            }
        }

        Self {
            managers,
            lookup_services,
            storage,
            advertiser,
            broadcaster,
            arc_broadcaster,
            chain_tracker,
            gasp_remote_factory: None,
            config,
        }
    }

    /// Set the ARC broadcaster for network broadcast to miners.
    ///
    /// When set, the Engine broadcasts transactions to ARC during Phase 2
    /// of `submit()` for `CurrentTx` submissions.
    pub fn set_arc_broadcaster(&mut self, arc: Box<dyn ArcBroadcaster>) {
        self.arc_broadcaster = Some(arc);
    }

    /// Set the GASP remote factory for peer communication during sync.
    ///
    /// Platform-specific crates (like overlay-cloudflare) provide an implementation
    /// that creates HTTP-based remotes using their native fetch API.
    pub fn set_gasp_remote_factory(&mut self, factory: Box<dyn crate::gasp::GASPRemoteFactory>) {
        self.gasp_remote_factory = Some(factory);
    }

    // ========================================================================
    // Submit — 3-phase pipeline
    // ========================================================================

    /// Validate a tagged BEEF without applying mutations.
    ///
    /// Runs Phase 1 (topic validation) and Phase 2 (broadcast) but NOT Phase 3
    /// (storage mutations). Returns the Steak (admittance decisions) that
    /// `submit()` would return.
    ///
    /// Used by the onSteakReady pattern to return results to clients before
    /// mutations are applied via a queue consumer or `ctx.wait_until()`.
    pub async fn submit_validate_only(
        &self,
        tagged_beef: &TaggedBEEF,
        mode: SubmitMode,
    ) -> Result<Steak, EngineError> {
        let (_validations, steak, _tx, _txid) = self.run_validation(tagged_beef, mode).await?;
        Ok(steak)
    }

    /// Submit a transaction for processing by the overlay.
    ///
    /// Three phases:
    /// 1. **VALIDATE** — check topics, dedup, find previous coins, call topic managers
    /// 2. **BROADCAST** — broadcast to network (if mode is CurrentTx)
    /// 3. **MUTATE** — mark spent, delete stale, insert new outputs, notify lookup services
    ///
    /// Returns a STEAK mapping each topic to its admittance instructions.
    pub async fn submit(
        &self,
        tagged_beef: &TaggedBEEF,
        mode: SubmitMode,
    ) -> Result<Steak, EngineError> {
        let (validations, mut steak, tx, txid) = self.run_validation(tagged_beef, mode).await?;

        // =================================================================
        // PHASE 3: MUTATE STORAGE
        // =================================================================
        for v in &validations {
            if v.is_dupe || v.failed {
                continue;
            }

            let topic = &v.topic;
            let admittance = &v.admittance;

            // ── Mark previous outputs as spent + notify lookup services ──
            for (prev_idx, prev_output) in v.previous_outputs.iter().enumerate() {
                if let Err(e) = self
                    .storage
                    .mark_utxo_as_spent(&prev_output.txid, prev_output.output_index, topic)
                    .await
                {
                    error!("Error marking UTXO as spent: {e}");
                    continue;
                }

                // The input index within tx.inputs that spends this previous output.
                let spending_input_idx = v.previous_coins[prev_idx] as usize;

                // Notify all lookup services about the spent output
                for ls in self.lookup_services.values() {
                    let payload = match ls.spend_notification_mode() {
                        SpendNotificationMode::None => OutputSpent::None {
                            txid: prev_output.txid.clone(),
                            output_index: prev_output.output_index,
                            topic: topic.clone(),
                        },
                        SpendNotificationMode::Txid => OutputSpent::Txid {
                            txid: prev_output.txid.clone(),
                            output_index: prev_output.output_index,
                            topic: topic.clone(),
                            spending_txid: txid.clone(),
                        },
                        SpendNotificationMode::Script => {
                            // Extract unlocking script and sequence from the spending input
                            let input = &tx.inputs[spending_input_idx];
                            let unlocking_script = input
                                .unlocking_script
                                .as_ref()
                                .map(bsv_rs::UnlockingScript::to_binary)
                                .unwrap_or_default();
                            OutputSpent::Script {
                                txid: prev_output.txid.clone(),
                                output_index: prev_output.output_index,
                                topic: topic.clone(),
                                spending_txid: txid.clone(),
                                input_index: spending_input_idx as u32,
                                unlocking_script,
                                sequence_number: input.sequence,
                                off_chain_values: tagged_beef.off_chain_values.clone(),
                            }
                        }
                        SpendNotificationMode::WholeTx => OutputSpent::WholeTx {
                            txid: prev_output.txid.clone(),
                            output_index: prev_output.output_index,
                            topic: topic.clone(),
                            spending_atomic_beef: tagged_beef.beef.clone(),
                            off_chain_values: tagged_beef.off_chain_values.clone(),
                        },
                    };
                    if let Err(e) = ls.output_spent(&payload).await {
                        error!("Error notifying lookup service of spent output: {e}");
                    }
                }
            }

            // ── Handle stale vs retained previous coins ──
            let mut outputs_consumed: Vec<Outpoint> = Vec::new();
            let mut stale_coins: Vec<&Output> = Vec::new();

            for (coin_idx, prev_output) in v.previous_outputs.iter().enumerate() {
                let input_index = v.previous_coins[coin_idx];
                if admittance.coins_to_retain.contains(&input_index) {
                    // Retained: track as consumed (will update consumedBy later)
                    outputs_consumed
                        .push(Outpoint::new(&prev_output.txid, prev_output.output_index));
                } else {
                    // Not retained: mark as stale for deletion
                    stale_coins.push(prev_output);
                }
            }

            // Delete stale outputs recursively
            for stale in &stale_coins {
                if let Ok(Some(stale_output)) = self
                    .storage
                    .find_output(&stale.txid, stale.output_index, Some(topic), None, false)
                    .await
                {
                    let _ = self.delete_utxo_deep(&stale_output).await;
                }
            }

            // Update STEAK with removed coins
            if let Some(steak_entry) = steak.get_mut(topic) {
                steak_entry.coins_removed = Some(
                    stale_coins
                        .iter()
                        .enumerate()
                        .filter_map(|(i, _)| {
                            // Map back to input indices
                            v.previous_coins.get(i).copied()
                        })
                        .collect(),
                );
            }

            // ── Insert admitted outputs ──
            let mut new_utxos: Vec<Outpoint> = Vec::new();

            for &output_index in &admittance.outputs_to_admit {
                let (script_bytes, sats) =
                    if let Some(tx_output) = tx.outputs.get(output_index as usize) {
                        (
                            tx_output.locking_script.to_binary(),
                            tx_output.get_satoshis(),
                        )
                    } else {
                        (Vec::new(), 0)
                    };

                let output = Output {
                    txid: txid.clone(),
                    output_index,
                    output_script: script_bytes,
                    satoshis: sats,
                    topic: topic.clone(),
                    spent: false,
                    outputs_consumed: outputs_consumed.clone(),
                    consumed_by: Vec::new(),
                    beef: Some(tagged_beef.beef.clone()),
                    block_height: None,
                    score: Some(current_timestamp_ms()),
                };

                if let Err(e) = self.storage.insert_output(&output).await {
                    error!("Error inserting output for topic {topic}: {e}");
                }

                new_utxos.push(Outpoint::new(&txid, output_index));

                // Notify lookup services
                for ls in self.lookup_services.values() {
                    let (ls_script, ls_sats) =
                        if let Some(tx_out) = tx.outputs.get(output_index as usize) {
                            (tx_out.locking_script.to_binary(), tx_out.get_satoshis())
                        } else {
                            (Vec::new(), 0)
                        };

                    let payload = match ls.admission_mode() {
                        AdmissionMode::LockingScript => OutputAdmittedByTopic::LockingScript {
                            txid: txid.clone(),
                            output_index,
                            topic: topic.clone(),
                            satoshis: ls_sats,
                            locking_script: ls_script,
                            off_chain_values: tagged_beef.off_chain_values.clone(),
                        },
                        AdmissionMode::WholeTx => OutputAdmittedByTopic::WholeTx {
                            atomic_beef: tagged_beef.beef.clone(),
                            output_index,
                            topic: topic.clone(),
                            off_chain_values: tagged_beef.off_chain_values.clone(),
                        },
                    };

                    if let Err(e) = ls.output_admitted_by_topic(&payload).await {
                        error!("Error notifying lookup service: {e}");
                    }
                }
            }

            // ── Update consumedBy on retained previous outputs ──
            for consumed_outpoint in &outputs_consumed {
                if let Ok(Some(consumed_output)) = self
                    .storage
                    .find_output(
                        &consumed_outpoint.txid,
                        consumed_outpoint.output_index,
                        Some(topic),
                        None,
                        false,
                    )
                    .await
                {
                    let mut new_consumed_by = consumed_output.consumed_by.clone();
                    for new_utxo in &new_utxos {
                        if !new_consumed_by.iter().any(|c| {
                            c.txid == new_utxo.txid && c.output_index == new_utxo.output_index
                        }) {
                            new_consumed_by.push(new_utxo.clone());
                        }
                    }
                    if let Err(e) = self
                        .storage
                        .update_consumed_by(
                            &consumed_outpoint.txid,
                            consumed_outpoint.output_index,
                            topic,
                            &new_consumed_by,
                        )
                        .await
                    {
                        error!("Error updating consumedBy: {e}");
                    }
                }
            }

            // Record applied transaction
            let tx_record = AppliedTransaction {
                txid: txid.clone(),
                topic: topic.clone(),
            };
            if let Err(e) = self.storage.insert_applied_transaction(&tx_record).await {
                error!("Error inserting applied transaction for topic {topic}: {e}");
            }
        }

        Ok(steak)
    }

    /// Run Phase 1 (topic validation) and Phase 2 (broadcast) without mutating storage.
    ///
    /// Returns (validations, steak, parsed_tx, txid) so the caller can either
    /// stop (validate-only) or proceed with Phase 3 (mutations).
    async fn run_validation(
        &self,
        tagged_beef: &TaggedBEEF,
        mode: SubmitMode,
    ) -> Result<(Vec<TopicValidation>, Steak, Transaction, String), EngineError> {
        // Validate all topics are supported
        for topic in &tagged_beef.topics {
            if !self.managers.contains_key(topic) {
                return Err(EngineError::UnsupportedTopic(topic.clone()));
            }
        }

        // Parse transaction from BEEF
        let tx = Transaction::from_beef(&tagged_beef.beef, None)
            .map_err(|e| EngineError::BeefParseError(e.to_string()))?;
        let txid = tx.id();

        // SPV verification via BEEF merkle proof validation (skip for HistoricalTxNoSpv)
        if mode != SubmitMode::HistoricalTxNoSpv {
            if let Some(ref chain_tracker) = self.chain_tracker {
                use bsv_rs::transaction::Beef;
                let mut beef = Beef::from_binary(&tagged_beef.beef)
                    .map_err(|e| EngineError::SpvError(format!("BEEF parse error: {e}")))?;
                let validation = beef.verify_valid(false);
                if !validation.valid {
                    return Err(EngineError::SpvError(
                        "BEEF internal proof validation failed".into(),
                    ));
                }
                for (height, root) in &validation.roots {
                    match chain_tracker.is_valid_root_for_height(root, *height).await {
                        Ok(true) => {}
                        Ok(false) => {
                            return Err(EngineError::SpvError(format!(
                                "Merkle root {root} invalid for block height {height}"
                            )));
                        }
                        Err(e) => {
                            return Err(EngineError::SpvError(format!(
                                "Chain tracker error at height {height}: {e}"
                            )));
                        }
                    }
                }
            }
        }

        let mut steak = Steak::new();

        // =============================================================
        // PHASE 1: VALIDATE (read-only)
        // =============================================================
        let mut validations = Vec::new();

        for topic in &tagged_beef.topics {
            let tx_record = AppliedTransaction {
                txid: txid.clone(),
                topic: topic.clone(),
            };
            let is_dupe = self
                .storage
                .does_applied_transaction_exist(&tx_record)
                .await
                .unwrap_or(false);

            if is_dupe {
                validations.push(TopicValidation {
                    topic: topic.clone(),
                    is_dupe: true,
                    previous_coins: vec![],
                    previous_outputs: vec![],
                    admittance: AdmittanceInstructions::default(),
                    failed: false,
                });
                continue;
            }

            let mut previous_coins: Vec<u32> = Vec::new();
            let mut previous_outputs: Vec<Output> = Vec::new();

            for (input_idx, input) in tx.inputs.iter().enumerate() {
                let source_txid = input.get_source_txid().unwrap_or_default();
                if source_txid.is_empty() {
                    continue;
                }
                if let Ok(Some(prev_output)) = self
                    .storage
                    .find_output(
                        &source_txid,
                        input.source_output_index,
                        Some(topic),
                        None,
                        false,
                    )
                    .await
                {
                    previous_coins.push(input_idx as u32);
                    previous_outputs.push(prev_output);
                }
            }

            let manager = &self.managers[topic];
            match manager
                .identify_admissible_outputs(
                    &tagged_beef.beef,
                    &previous_coins
                        .iter()
                        .flat_map(|i| i.to_le_bytes())
                        .collect::<Vec<u8>>(),
                    tagged_beef.off_chain_values.as_deref(),
                    mode,
                )
                .await
            {
                Ok(admittance) => {
                    validations.push(TopicValidation {
                        topic: topic.clone(),
                        is_dupe: false,
                        previous_coins,
                        previous_outputs,
                        admittance,
                        failed: false,
                    });
                }
                Err(e) => {
                    error!("Error validating topic {topic} during submit: {e}");
                    validations.push(TopicValidation {
                        topic: topic.clone(),
                        is_dupe: false,
                        previous_coins: vec![],
                        previous_outputs: vec![],
                        admittance: AdmittanceInstructions::default(),
                        failed: true,
                    });
                }
            }
        }

        // Build preliminary STEAK
        for v in &validations {
            steak.insert(v.topic.clone(), v.admittance.clone());
        }

        // =============================================================
        // PHASE 2: BROADCAST / SHIP propagation (before mutations)
        // =============================================================
        if mode == SubmitMode::CurrentTx {
            // ── ARC network broadcast ──────────────────────────────────
            // Broadcast to miners via ARC, matching the TS Engine pattern.
            // Skip if the tx already has a merkle path (already mined).
            if let Some(ref arc) = self.arc_broadcaster {
                if tx.merkle_path.is_none() {
                    let raw_tx_hex = tx.to_hex();
                    match arc.broadcast(&raw_tx_hex).await {
                        Ok(arc_txid) => {
                            info!("ARC broadcast succeeded: txid={arc_txid}");
                        }
                        Err(e) => {
                            error!("ARC broadcast failed (non-fatal): {e}");
                        }
                    }
                } else {
                    info!("Skipping ARC broadcast — tx already has merkle proof");
                }
            }

            // ── SHIP peer propagation ──────────────────────────────────
            if let Some(ref broadcaster) = self.broadcaster {
                let relevant_topics: Vec<String> = validations
                    .iter()
                    .filter(|v| {
                        !v.is_dupe && !v.failed && !v.admittance.outputs_to_admit.is_empty()
                    })
                    .map(|v| v.topic.clone())
                    .collect();

                if !relevant_topics.is_empty() {
                    if let Some(ship_ls) = self.lookup_services.get("ls_ship") {
                        for topic in &relevant_topics {
                            let question = LookupQuestion::new(
                                "ls_ship",
                                serde_json::json!({ "topics": [topic] }),
                            );
                            match ship_ls.lookup(&question).await {
                                Ok(LookupResult::OutputList(refs)) => {
                                    for reference in &refs {
                                        if let Ok(Some(output)) = self
                                            .storage
                                            .find_output(
                                                &reference.txid,
                                                reference.output_index,
                                                None,
                                                None,
                                                false,
                                            )
                                            .await
                                        {
                                            if let Some(domain) =
                                                parse_ship_domain_from_script(&output.output_script)
                                            {
                                                if let Some(ref our_url) = self.config.hosting_url {
                                                    if domain.trim_end_matches('/')
                                                        == our_url.trim_end_matches('/')
                                                    {
                                                        continue;
                                                    }
                                                }
                                                if let Err(e) = broadcaster
                                                    .broadcast_to_host(&domain, tagged_beef)
                                                    .await
                                                {
                                                    error!(
                                                        "SHIP propagation to {domain} failed for topic {topic}: {e}"
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                                Ok(LookupResult::Answer(_)) => {
                                    // SHIP only ever returns OutputList; an Answer here means
                                    // a misconfigured replacement service. Skip rather than
                                    // attempt to extract refs from a freeform/formula payload.
                                    error!(
                                        "SHIP lookup for topic {topic} returned a pre-formed \
                                         LookupAnswer; expected OutputList. Skipping fanout."
                                    );
                                }
                                Err(e) => {
                                    error!("SHIP lookup for topic {topic} failed: {e}");
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok((validations, steak, tx, txid))
    }

    // ========================================================================
    // Lookup
    // ========================================================================

    /// Answer a lookup query.
    ///
    /// Delegates to the appropriate LookupService, then hydrates results with BEEF.
    ///
    /// When `history_selector` is provided, each output is hydrated with its
    /// ancestor spend chain (via `get_utxo_history`) before building the response.
    /// This matches the TS Engine behavior when a history selector is configured.
    pub async fn lookup(
        &self,
        question: &LookupQuestion,
        history_selector: Option<HistorySelector>,
    ) -> Result<LookupAnswer, EngineError> {
        let service = self
            .lookup_services
            .get(&question.service)
            .ok_or_else(|| EngineError::LookupServiceNotFound(question.service.clone()))?;

        let result = service
            .lookup(question)
            .await
            .map_err(|e| EngineError::LookupFailed(e.to_string()))?;

        // Two paths per LookupResult:
        // - OutputList(refs): the LS yields outpoints; we hydrate each with
        //   BEEF (and optional ancestor chain via history_selector) and
        //   assemble LookupAnswer::OutputList.
        // - Answer(answer): the LS already produced the full answer
        //   (Freeform/Formula); pass through verbatim. The Engine does NOT
        //   apply history-selector hydration to a pre-formed Answer — the
        //   LS is presumed to have embedded whatever ancestry it wants.
        let refs = match result {
            LookupResult::OutputList(refs) => refs,
            LookupResult::Answer(answer) => return Ok(answer),
        };

        // Hydrate each result with BEEF from storage
        let mut outputs = Vec::new();
        for reference in &refs {
            if let Ok(Some(output)) = self
                .storage
                .find_output(&reference.txid, reference.output_index, None, None, true)
                .await
            {
                // If history selector provided, hydrate ancestor chain
                let final_output = if history_selector.is_some() {
                    match self
                        .get_utxo_history(&output, history_selector.clone())
                        .await
                    {
                        Ok(Some(hydrated)) => hydrated,
                        _ => output,
                    }
                } else {
                    output
                };

                if let Some(beef) = final_output.beef {
                    outputs.push(OutputListItem {
                        beef,
                        output_index: final_output.output_index,
                        context: None,
                    });
                }
            }
        }

        Ok(LookupAnswer::OutputList { outputs })
    }

    // ========================================================================
    // Metadata
    // ========================================================================

    // ========================================================================
    // UTXO History
    // ========================================================================

    /// Traverse and return the history of a UTXO.
    ///
    /// If no `history_selector` is provided, returns the output as-is.
    /// If a depth selector is provided, includes up to N levels of ancestor
    /// transactions by embedding them as source_transactions in the BEEF.
    pub async fn get_utxo_history(
        &self,
        output: &Output,
        history_selector: Option<HistorySelector>,
    ) -> Result<Option<Output>, EngineError> {
        let Some(selector) = history_selector else {
            return Ok(Some(output.clone()));
        };

        // Verify BEEF exists before attempting hydration
        if output.beef.is_none() {
            return Err(EngineError::Other(
                "Output must have associated transaction BEEF!".into(),
            ));
        }

        match self.hydrate_utxo_history(output, &selector, 0).await {
            Ok(Some(hydrated_output)) => Ok(Some(hydrated_output)),
            Ok(None) => Ok(None),
            Err(e) => Err(EngineError::Other(format!(
                "Error retrieving UTXO history: {e}"
            ))),
        }
    }

    /// Recursive UTXO history hydration.
    fn hydrate_utxo_history<'a>(
        &'a self,
        output: &'a Output,
        selector: &'a HistorySelector,
        current_depth: u32,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<Output>, EngineError>> + 'a>,
    > {
        Box::pin(async move {
            let beef_data = output.beef.as_ref().ok_or_else(|| {
                EngineError::Other("Output must have associated transaction BEEF!".into())
            })?;

            // Check if we should traverse at this depth
            let should_traverse = match selector {
                HistorySelector::Depth(max_depth) => current_depth <= *max_depth,
            };

            if !should_traverse {
                return Ok(None);
            }

            // Parse BEEF to get the transaction
            let mut tx = Transaction::from_beef(beef_data, None)
                .map_err(|e| EngineError::BeefParseError(e.to_string()))?;

            // For each consumed output, recursively hydrate and embed as source transaction
            for consumed in &output.outputs_consumed {
                if let Ok(Some(child_output)) = self
                    .storage
                    .find_output(&consumed.txid, consumed.output_index, None, None, true)
                    .await
                {
                    if let Ok(Some(hydrated_child)) = self
                        .hydrate_utxo_history(&child_output, selector, current_depth + 1)
                        .await
                    {
                        // Try to embed the child's transaction as a source transaction
                        // on the appropriate input
                        if let Some(ref child_beef) = hydrated_child.beef {
                            if let Ok(child_tx) = Transaction::from_beef(child_beef, None) {
                                // Find the input that references this consumed output
                                for input in &mut tx.inputs {
                                    let source_txid = input.get_source_txid().unwrap_or_default();
                                    if source_txid == consumed.txid
                                        && input.source_output_index == consumed.output_index
                                    {
                                        input.source_transaction = Some(Box::new(child_tx.clone()));
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Serialize back to BEEF with enriched ancestors
            let enriched_beef = tx.to_beef(true).map_err(|e| {
                EngineError::Other(format!("Failed to serialize enriched BEEF: {e}"))
            })?;

            Ok(Some(Output {
                beef: Some(enriched_beef),
                ..output.clone()
            }))
        })
    }

    // ========================================================================
    // Metadata
    // ========================================================================

    pub async fn list_topic_managers(&self) -> HashMap<String, ServiceMetadata> {
        let mut result = HashMap::new();
        for (name, manager) in &self.managers {
            let meta = manager.get_metadata().await;
            result.insert(name.clone(), meta);
        }
        result
    }

    /// List all registered lookup services with their metadata.
    pub async fn list_lookup_service_providers(&self) -> HashMap<String, ServiceMetadata> {
        let mut result = HashMap::new();
        for (name, service) in &self.lookup_services {
            let meta = service.get_metadata().await;
            result.insert(name.clone(), meta);
        }
        result
    }

    /// Get documentation for a specific topic manager.
    pub async fn get_documentation_for_topic_manager(&self, name: &str) -> String {
        match self.managers.get(name) {
            Some(manager) => manager.get_documentation().await,
            None => "No documentation found!".to_string(),
        }
    }

    /// Get documentation for a specific lookup service.
    pub async fn get_documentation_for_lookup_service(&self, name: &str) -> String {
        match self.lookup_services.get(name) {
            Some(service) => service.get_documentation().await,
            None => "No documentation found!".to_string(),
        }
    }

    // ========================================================================
    // GASP endpoints
    // ========================================================================

    /// Respond to a GASP initial sync request.
    ///
    /// Returns UTXOs for the given topic since the requested score.
    pub async fn provide_foreign_sync_response(
        &self,
        request: &GASPInitialRequest,
        topic: &str,
    ) -> Result<GASPInitialResponse, EngineError> {
        let outputs = self
            .storage
            .find_utxos_for_topic(topic, Some(request.since as f64), request.limit, false)
            .await
            .map_err(|e| EngineError::StorageError(e.to_string()))?;

        Ok(GASPInitialResponse {
            utxo_list: outputs
                .iter()
                .map(|o| GASPOutput {
                    txid: o.txid.clone(),
                    output_index: o.output_index,
                    score: o.score.unwrap_or(0.0),
                })
                .collect(),
            since: request.since,
        })
    }

    /// Provide a GASPNode for a specific transaction within a graph.
    ///
    /// Searches the BEEF tree of the root output (identified by graphID) for
    /// the requested txid. If not found in the BEEF tree, recurses through
    /// outputsConsumed in storage.
    pub async fn provide_foreign_gasp_node(
        &self,
        graph_id: &str,
        txid: &str,
        output_index: u32,
    ) -> Result<GASPNode, EngineError> {
        let root_outpoint = Outpoint::from_graph_id(graph_id)
            .ok_or_else(|| EngineError::Other(format!("Invalid graphID: {graph_id}")))?;

        let root_output = self
            .storage
            .find_output(
                &root_outpoint.txid,
                root_outpoint.output_index,
                None,
                None,
                true,
            )
            .await
            .map_err(|e| EngineError::StorageError(e.to_string()))?
            .ok_or(EngineError::NodeNotFound)?;

        self.hydrate_gasp_node(&root_output, graph_id, txid, output_index)
            .await
    }

    /// Recursively search a BEEF tree for a specific txid and return a GASPNode.
    fn hydrate_gasp_node<'a>(
        &'a self,
        output: &'a Output,
        graph_id: &'a str,
        txid: &'a str,
        output_index: u32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GASPNode, EngineError>> + 'a>>
    {
        Box::pin(async move {
            let beef_data = output.beef.as_ref().ok_or(EngineError::NodeNotFound)?;

            let root_tx = Transaction::from_beef(beef_data, None)
                .map_err(|e| EngineError::BeefParseError(e.to_string()))?;

            // Search the transaction tree for the requested txid
            if let Some(node) = Self::search_tx_tree(&root_tx, graph_id, txid, output_index) {
                return Ok(node);
            }

            // Fallback: try parsing BEEF with the target txid directly
            // (some BEEF structures have the tx in the list but not linked as source_transaction)
            if let Some(node) = Self::search_beef_for_txid(beef_data, graph_id, txid, output_index)
            {
                return Ok(node);
            }

            // Not found in BEEF tree — recurse through outputsConsumed in storage
            for consumed in &output.outputs_consumed {
                if let Ok(Some(consumed_output)) = self
                    .storage
                    .find_output(&consumed.txid, consumed.output_index, None, None, true)
                    .await
                {
                    if let Ok(node) = self
                        .hydrate_gasp_node(&consumed_output, graph_id, txid, output_index)
                        .await
                    {
                        return Ok(node);
                    }
                }
            }

            Err(EngineError::Other(
                "Unable to find output associated with your request!".into(),
            ))
        })
    }

    /// Search a Transaction tree (via sourceTransaction links) for a specific txid.
    fn search_tx_tree(
        tx: &Transaction,
        graph_id: &str,
        target_txid: &str,
        output_index: u32,
    ) -> Option<GASPNode> {
        let current_txid = tx.id();

        if current_txid == target_txid {
            let mut node = GASPNode {
                graph_id: graph_id.to_string(),
                raw_tx: tx.to_hex(),
                output_index,
                proof: None,
                tx_metadata: None,
                output_metadata: None,
                inputs: None,
            };
            if let Some(ref merkle_path) = tx.merkle_path {
                node.proof = Some(merkle_path.to_hex());
            }
            return Some(node);
        }

        // Recurse into inputs via source_transaction
        for input in &tx.inputs {
            if let Some(ref source_tx) = input.source_transaction {
                if let Some(node) =
                    Self::search_tx_tree(source_tx, graph_id, target_txid, output_index)
                {
                    return Some(node);
                }
            }
        }

        None
    }

    /// Alternative search: try parsing the BEEF with the target txid directly.
    /// Some BEEF structures may not link source_transaction on inputs but
    /// still contain the transaction in the BEEF's transaction list.
    fn search_beef_for_txid(
        beef_data: &[u8],
        graph_id: &str,
        target_txid: &str,
        output_index: u32,
    ) -> Option<GASPNode> {
        // Try parsing BEEF with the specific txid
        if let Ok(tx) = Transaction::from_beef(beef_data, Some(target_txid)) {
            if tx.id() == target_txid {
                let mut node = GASPNode {
                    graph_id: graph_id.to_string(),
                    raw_tx: tx.to_hex(),
                    output_index,
                    proof: None,
                    tx_metadata: None,
                    output_metadata: None,
                    inputs: None,
                };
                if let Some(ref merkle_path) = tx.merkle_path {
                    node.proof = Some(merkle_path.to_hex());
                }
                return Some(node);
            }
        }
        None
    }

    // ========================================================================
    // Merkle proof handling
    // ========================================================================

    /// Handle a new merkle proof for a transaction.
    ///
    /// When a transaction gets mined, ARC calls back with the merkle proof.
    /// This method:
    /// 1. Finds all outputs for the txid in storage
    /// 2. Parses the BEEF, updates the merkle path in the transaction tree
    /// 3. Serializes updated BEEF back to storage
    /// 4. Recursively updates the consumedBy chain (outputs that spent this one)
    /// 5. Updates blockHeight on the outputs
    pub async fn handle_new_merkle_proof(
        &self,
        txid: &str,
        proof_hex: &str,
        block_height: Option<u32>,
    ) -> Result<(), EngineError> {
        let outputs = self
            .storage
            .find_outputs_for_transaction(txid, true)
            .await
            .map_err(|e| EngineError::StorageError(e.to_string()))?;

        if outputs.is_empty() {
            return Err(EngineError::Other(
                "Could not find matching transaction outputs for proof ingest!".into(),
            ));
        }

        // Parse merkle proof if provided
        let proof = if proof_hex.is_empty() {
            None
        } else {
            Some(
                bsv_rs::transaction::MerklePath::from_hex(proof_hex)
                    .map_err(|e| EngineError::Other(format!("Invalid merkle proof hex: {e}")))?,
            )
        };

        for output in &outputs {
            // Update BEEF with merkle proof if we have both BEEF and proof
            if let (Some(ref beef_data), Some(ref proof)) = (&output.beef, &proof) {
                if let Ok(mut tx) = Transaction::from_beef(beef_data, None) {
                    // Set merkle path on the transaction (or its ancestors)
                    Self::update_input_proofs(&mut tx, txid, proof);

                    // Serialize updated BEEF back to storage
                    if let Ok(new_beef) = tx.to_beef(true) {
                        let _ = self
                            .storage
                            .update_transaction_beef(&output.txid, &new_beef)
                            .await;
                    }
                }
            }

            // Update block height
            if let Some(height) = block_height {
                let _ = self
                    .storage
                    .update_output_block_height(
                        &output.txid,
                        output.output_index,
                        &output.topic,
                        height,
                    )
                    .await;
            }

            // Recursively update consumedBy chain
            for consuming in &output.consumed_by {
                if let Ok(consumed_outputs) = self
                    .storage
                    .find_outputs_for_transaction(&consuming.txid, true)
                    .await
                {
                    for consumed_output in &consumed_outputs {
                        // Update BEEF for consuming outputs (they reference this tx as an ancestor)
                        if let (Some(ref beef_data), Some(ref proof)) =
                            (&consumed_output.beef, &proof)
                        {
                            if let Ok(mut consumed_tx) = Transaction::from_beef(beef_data, None) {
                                Self::update_input_proofs(&mut consumed_tx, txid, proof);
                                if let Ok(new_beef) = consumed_tx.to_beef(true) {
                                    let _ = self
                                        .storage
                                        .update_transaction_beef(&consumed_output.txid, &new_beef)
                                        .await;
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Recursively update merkle paths in a transaction's input tree.
    ///
    /// If the transaction's id matches txid, set its merkle_path.
    /// Otherwise, recurse into sourceTransactions of each input.
    fn update_input_proofs(
        tx: &mut Transaction,
        txid: &str,
        proof: &bsv_rs::transaction::MerklePath,
    ) {
        if tx.merkle_path.is_some() {
            // Already has a proof — update it (handles reorgs)
            tx.merkle_path = Some(proof.clone());
            return;
        }
        if tx.id() == txid {
            tx.merkle_path = Some(proof.clone());
        } else {
            for input in &mut tx.inputs {
                if let Some(ref mut source_tx) = input.source_transaction {
                    Self::update_input_proofs(source_tx, txid, proof);
                }
            }
        }
    }

    // ========================================================================
    // Advertisement sync
    // ========================================================================

    /// Sync SHIP/SLAP advertisements with configured managers and services.
    ///
    /// Creates missing advertisements and revokes stale ones.
    pub async fn sync_advertisements(&self) -> Result<(), EngineError> {
        let Some(advertiser) = &self.advertiser else {
            return Ok(()); // No advertiser configured
        };

        let hosting_url = match &self.config.hosting_url {
            Some(url) if !url.is_empty() => url.clone(),
            _ => return Ok(()), // No hosting URL
        };

        // Get configured topics and services
        let mut configured_topics: Vec<String> = self.managers.keys().cloned().collect();
        let mut configured_services: Vec<String> = self.lookup_services.keys().cloned().collect();

        if self.config.suppress_default_sync_advertisements {
            configured_topics.retain(|t| t != "tm_ship" && t != "tm_slap");
            configured_services.retain(|s| s != "ls_ship" && s != "ls_slap");
        }

        // Fetch current advertisements
        let current_ship = advertiser
            .find_all_advertisements(Protocol::Ship)
            .await
            .unwrap_or_default();
        let current_slap = advertiser
            .find_all_advertisements(Protocol::Slap)
            .await
            .unwrap_or_default();

        // Determine what to create
        let ships_to_create: Vec<AdvertisementData> = configured_topics
            .iter()
            .filter(|topic| {
                !current_ship
                    .iter()
                    .any(|a| a.topic_or_service == **topic && a.domain == hosting_url)
            })
            .map(|topic| AdvertisementData {
                protocol: Protocol::Ship,
                topic_or_service_name: topic.clone(),
            })
            .collect();

        let slaps_to_create: Vec<AdvertisementData> = configured_services
            .iter()
            .filter(|service| {
                !current_slap
                    .iter()
                    .any(|a| a.topic_or_service == **service && a.domain == hosting_url)
            })
            .map(|service| AdvertisementData {
                protocol: Protocol::Slap,
                topic_or_service_name: service.clone(),
            })
            .collect();

        // Determine what to revoke
        let ships_to_revoke: Vec<Advertisement> = current_ship
            .into_iter()
            .filter(|a| !configured_topics.contains(&a.topic_or_service))
            .collect();

        let slaps_to_revoke: Vec<Advertisement> = current_slap
            .into_iter()
            .filter(|a| !configured_services.contains(&a.topic_or_service))
            .collect();

        // Create new advertisements
        let mut all_to_create = ships_to_create;
        all_to_create.extend(slaps_to_create);
        if !all_to_create.is_empty() {
            match advertiser.create_advertisements(&all_to_create).await {
                Ok(tagged_beef) => {
                    if let Err(e) = self.submit(&tagged_beef, SubmitMode::CurrentTx).await {
                        error!("Failed to submit new advertisements: {e}");
                    }
                }
                Err(e) => error!("Failed to create advertisements: {e}"),
            }
        }

        // Revoke stale advertisements
        let mut all_to_revoke = ships_to_revoke;
        all_to_revoke.extend(slaps_to_revoke);
        if !all_to_revoke.is_empty() {
            match advertiser.revoke_advertisements(&all_to_revoke).await {
                Ok(tagged_beef) => {
                    if let Err(e) = self.submit(&tagged_beef, SubmitMode::CurrentTx).await {
                        error!("Failed to submit revocation: {e}");
                    }
                }
                Err(e) => error!("Failed to revoke advertisements: {e}"),
            }
        }

        Ok(())
    }

    // ========================================================================
    // Deep deletion
    // ========================================================================

    /// Recursively delete a UTXO and all stale consumed inputs.
    ///
    /// Only deletes if the output has no remaining consumers (consumedBy is empty).
    /// Then recurses into outputsConsumed, removing the deleted output from their
    /// consumedBy lists and deleting them if they become unreferenced.
    #[allow(dead_code)] // Used by submit() once BEEF parsing is wired up
    fn delete_utxo_deep<'a>(
        &'a self,
        output: &'a Output,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), EngineError>> + 'a>> {
        Box::pin(async move {
            // Only delete if nothing consumes this output
            if output.consumed_by.is_empty() {
                self.storage
                    .delete_output(&output.txid, output.output_index, &output.topic)
                    .await
                    .map_err(|e| EngineError::StorageError(e.to_string()))?;

                // Notify lookup services
                for ls in self.lookup_services.values() {
                    let _ = ls
                        .output_no_longer_retained_in_history(
                            &output.txid,
                            output.output_index,
                            &output.topic,
                        )
                        .await;
                }
            }

            // Recurse into consumed outputs
            for consumed in &output.outputs_consumed {
                if let Ok(Some(mut stale_output)) = self
                    .storage
                    .find_output(
                        &consumed.txid,
                        consumed.output_index,
                        Some(&output.topic),
                        None,
                        false,
                    )
                    .await
                {
                    // Remove the deleted output from consumedBy
                    stale_output.consumed_by.retain(|c| {
                        !(c.txid == output.txid && c.output_index == output.output_index)
                    });

                    self.storage
                        .update_consumed_by(
                            &consumed.txid,
                            consumed.output_index,
                            &output.topic,
                            &stale_output.consumed_by,
                        )
                        .await
                        .map_err(|e| EngineError::StorageError(e.to_string()))?;

                    // Recurse
                    let _ = self.delete_utxo_deep(&stale_output).await;
                }
            }

            Ok(())
        }) // Box::pin
    }

    // ========================================================================
    // Eviction
    // ========================================================================

    /// Evict a specific outpoint from the overlay.
    ///
    /// 1. Deletes the output from storage for the given topic.
    /// 2. Notifies all lookup services via `output_evicted()`.
    ///
    /// If no topic is specified, finds all outputs for the txid and evicts the
    /// matching outputIndex across every topic.
    ///
    /// Matches TS OverlayExpress `/admin/evictOutpoint` (lines 974-1004).
    pub async fn evict_output(
        &self,
        txid: &str,
        output_index: u32,
        topic: Option<&str>,
    ) -> Result<(), EngineError> {
        if let Some(topic) = topic {
            // Delete from storage
            self.storage
                .delete_output(txid, output_index, topic)
                .await
                .map_err(|e| EngineError::StorageError(e.to_string()))?;

            // Notify all lookup services
            for ls in self.lookup_services.values() {
                let _ = ls.output_evicted(txid, output_index).await;
            }
        } else {
            // No topic specified — find all outputs for this txid and evict matching ones
            let outputs = self
                .storage
                .find_outputs_for_transaction(txid, false)
                .await
                .map_err(|e| EngineError::StorageError(e.to_string()))?;

            for output in &outputs {
                if output.output_index == output_index {
                    self.storage
                        .delete_output(txid, output_index, &output.topic)
                        .await
                        .map_err(|e| EngineError::StorageError(e.to_string()))?;
                }
            }

            // Notify all lookup services once
            for ls in self.lookup_services.values() {
                let _ = ls.output_evicted(txid, output_index).await;
            }
        }

        Ok(())
    }

    // ========================================================================
    // GASP sync
    // ========================================================================

    /// Start GASP synchronization with peers for all configured topics.
    ///
    /// For each topic in `sync_configuration`:
    /// - `SyncTarget::Ship` — discovers peers via local `ls_ship` lookup, parses
    ///   SHIP advertisement scripts to extract domain URLs.
    /// - `SyncTarget::Peers(urls)` — uses the provided peer URLs directly.
    /// - `SyncTarget::Disabled` — skips the topic.
    ///
    /// Filters out our own `hosting_url` to avoid self-sync.
    ///
    /// Returns a `GASPSyncResult` summarizing discovered peers per topic.
    ///
    /// Discovers peers for each configured topic and runs GASP sync with each.
    ///
    /// When a `GASPRemoteFactory` is set (via `set_gasp_remote_factory`), this
    /// method creates `OverlayGASPStorage` + `GASPRemote` instances per
    /// (topic, peer) pair and runs `GASPSync::sync()` to exchange UTXOs.
    ///
    /// Without a factory, peer discovery still runs but no actual sync occurs
    /// (backwards-compatible with the previous discovery-only behavior).
    pub async fn start_gasp_sync(&self) -> Result<GASPSyncResult, EngineError> {
        use crate::gasp::{GASPSync, DEFAULT_GASP_SYNC_LIMIT};
        use crate::gasp_overlay::OverlayGASPStorage;

        if self.config.sync_configuration.is_empty() {
            info!("[GASP SYNC] No sync configuration — nothing to sync");
            return Ok(GASPSyncResult {
                topics_synced: HashMap::new(),
            });
        }

        let mut topics_synced: HashMap<String, TopicSyncResult> = HashMap::new();

        for (topic, target) in &self.config.sync_configuration {
            let (peers, sync_type) = match target {
                SyncTarget::Disabled => {
                    info!("[GASP SYNC] Topic {topic} is disabled — skipping");
                    continue;
                }
                SyncTarget::Ship => {
                    info!("[GASP SYNC] Topic {topic} configured for SHIP discovery");
                    let peers = self.discover_ship_peers(topic).await;
                    info!(
                        "[GASP SYNC] Discovered {} peer(s) for topic {topic}",
                        peers.len()
                    );
                    (peers, "ship".to_string())
                }
                SyncTarget::Peers(peer_urls) => {
                    info!(
                        "[GASP SYNC] Topic {topic} configured with {} hardcoded peer(s)",
                        peer_urls.len()
                    );
                    let peers: Vec<String> = peer_urls
                        .iter()
                        .filter(|url| {
                            if let Some(ref our_url) = self.config.hosting_url {
                                url.trim_end_matches('/') != our_url.trim_end_matches('/')
                            } else {
                                true
                            }
                        })
                        .cloned()
                        .collect();

                    info!(
                        "[GASP SYNC] {} peer(s) for topic {topic} after self-filtering",
                        peers.len()
                    );
                    (peers, "peers".to_string())
                }
            };

            let mut errors = Vec::new();

            // If we have a remote factory, actually run GASP sync
            if let Some(ref factory) = self.gasp_remote_factory {
                for peer_url in &peers {
                    info!("[GASP SYNC] Syncing topic {topic} with peer {peer_url}");

                    // Get last interaction score for this (peer, topic) pair
                    let last_interaction = self
                        .storage
                        .get_last_interaction(peer_url, topic)
                        .await
                        .unwrap_or(0);

                    // Create shared sink for finalized graphs
                    let sink = crate::gasp_overlay::new_finalized_graph_sink();

                    // Create storage adapter and remote
                    let gasp_storage =
                        OverlayGASPStorage::new(self.storage.as_ref(), topic, sink.clone());
                    let gasp_remote = factory.create_remote(peer_url, topic);

                    let log_prefix = format!("[GASP {topic} <-> {peer_url}]");
                    let mut sync = GASPSync::new(
                        Box::new(gasp_storage),
                        gasp_remote,
                        last_interaction,
                        &log_prefix,
                        false, // bidirectional
                    );

                    match sync.sync(Some(DEFAULT_GASP_SYNC_LIMIT)).await {
                        Ok(()) => {
                            // Submit finalized graphs to the Engine
                            let finalized: Vec<_> = sink
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .drain(..)
                                .collect();

                            for graph in &finalized {
                                for beef_bytes in &graph.beefs {
                                    let tagged = TaggedBEEF::new(
                                        beef_bytes.clone(),
                                        vec![graph.topic.clone()],
                                    );
                                    if let Err(e) =
                                        self.submit(&tagged, SubmitMode::HistoricalTxNoSpv).await
                                    {
                                        warn!(
                                            "[GASP SYNC] Failed to submit BEEF for topic {}: {e}",
                                            graph.topic
                                        );
                                    }
                                }
                            }

                            if !finalized.is_empty() {
                                info!(
                                    "[GASP SYNC] Submitted {} finalized graph(s) for {topic} from {peer_url}",
                                    finalized.len()
                                );
                            }

                            // Update last interaction score
                            if let Err(e) = self
                                .storage
                                .update_last_interaction(peer_url, topic, sync.last_interaction)
                                .await
                            {
                                warn!(
                                    "[GASP SYNC] Failed to update last_interaction for {peer_url}/{topic}: {e}"
                                );
                            }
                            info!(
                                "[GASP SYNC] Sync with {peer_url} for {topic} completed (last_interaction={})",
                                sync.last_interaction
                            );
                        }
                        Err(e) => {
                            let msg = format!("{peer_url}: {e}");
                            warn!("[GASP SYNC] Sync failed: {msg}");
                            errors.push(msg);
                        }
                    }
                }
            }

            topics_synced.insert(
                topic.clone(),
                TopicSyncResult {
                    peers,
                    sync_type,
                    errors,
                },
            );
        }

        info!(
            "[GASP SYNC] Peer discovery complete for {} topic(s)",
            topics_synced.len()
        );

        Ok(GASPSyncResult { topics_synced })
    }

    /// Discover peer overlay nodes for a topic via SHIP lookup.
    ///
    /// Queries the local `ls_ship` lookup service for SHIP advertisement records
    /// matching the given topic, then parses each record's PushDrop locking script
    /// to extract the advertised domain URL. Filters out our own hosting URL.
    async fn discover_ship_peers(&self, topic: &str) -> Vec<String> {
        let Some(ship_ls) = self.lookup_services.get("ls_ship") else {
            warn!("[GASP SYNC] No ls_ship lookup service registered — cannot discover peers");
            return Vec::new();
        };

        let question = LookupQuestion::new("ls_ship", serde_json::json!({ "topics": [topic] }));

        let refs = match ship_ls.lookup(&question).await {
            Ok(LookupResult::OutputList(refs)) => refs,
            Ok(LookupResult::Answer(_)) => {
                error!(
                    "[GASP SYNC] SHIP lookup for topic {topic} returned a pre-formed \
                     LookupAnswer; expected OutputList. Skipping."
                );
                return Vec::new();
            }
            Err(e) => {
                error!("[GASP SYNC] SHIP lookup for topic {topic} failed: {e}");
                return Vec::new();
            }
        };

        let mut domains = HashSet::new();
        for reference in &refs {
            if let Ok(Some(output)) = self
                .storage
                .find_output(&reference.txid, reference.output_index, None, None, false)
                .await
            {
                if let Some(domain) = parse_ship_domain_from_script(&output.output_script) {
                    if let Some(ref our_url) = self.config.hosting_url {
                        if domain.trim_end_matches('/') == our_url.trim_end_matches('/') {
                            continue;
                        }
                    }
                    domains.insert(domain);
                }
            }
        }

        let mut peers: Vec<String> = domains.into_iter().collect();
        peers.sort();
        peers
    }

    // ========================================================================
    // Helpers
    // ========================================================================

    /// Get a reference to the storage backend.
    pub fn storage(&self) -> &dyn Storage {
        self.storage.as_ref()
    }

    /// Get the engine configuration.
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }
}

/// Result of a GASP sync operation.
///
/// Returned by `Engine::start_gasp_sync()` to summarize what happened.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GASPSyncResult {
    /// Per-topic sync results.
    pub topics_synced: HashMap<String, TopicSyncResult>,
}

/// Sync result for a single topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicSyncResult {
    /// Peer URLs discovered or configured for this topic.
    pub peers: Vec<String>,
    /// How peers were determined: "ship" or "peers".
    pub sync_type: String,
    /// Any errors encountered during sync (peer URL -> error message).
    pub errors: Vec<String>,
}

/// Get current time in milliseconds (for output scores).
///
/// Uses `js_sys::Date::now()` on wasm32 (Cloudflare Workers) since
/// `std::time::SystemTime` is not available on that platform.
fn current_timestamp_ms() -> f64 {
    #[cfg(target_arch = "wasm32")]
    {
        js_sys::Date::now()
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0.0, |d| d.as_millis() as f64)
    }
}

/// Extract the domain field from a SHIP PushDrop locking script.
///
/// SHIP PushDrop format: field[0] = "SHIP", field[1] = identity_key,
/// field[2] = domain (UTF-8), field[3] = topic.
/// Returns `None` if the script cannot be parsed or is not a SHIP advertisement.
fn parse_ship_domain_from_script(output_script: &[u8]) -> Option<String> {
    use bsv_rs::script::templates::PushDrop;

    let script = bsv_rs::script::Script::from_binary(output_script).ok()?;
    let pushdrop = PushDrop::decode(&script.into()).ok()?;

    if pushdrop.fields.len() < 3 {
        return None;
    }

    let protocol = String::from_utf8_lossy(&pushdrop.fields[0]);
    if protocol != "SHIP" {
        return None;
    }

    let domain = String::from_utf8_lossy(&pushdrop.fields[2]).to_string();
    if domain.is_empty() {
        return None;
    }

    Some(domain)
}

// ============================================================================
// Error type
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("This server does not support this topic: {0}")]
    UnsupportedTopic(String),

    #[error("Lookup service not found for provider: {0}")]
    LookupServiceNotFound(String),

    /// GASP: `/requestForeignGASPNode` asked for a graph/txid we don't
    /// have locally. Matches mainline's 400-class "not found" response.
    #[error("No matching output found!")]
    NodeNotFound,

    #[error("lookup failed: {0}")]
    LookupFailed(String),

    #[error("storage error: {0}")]
    StorageError(String),

    #[error("broadcast error: {0}")]
    BroadcastError(String),

    #[error("SPV verification failed: {0}")]
    SpvError(String),

    #[error("BEEF parsing failed: {0}")]
    BeefParseError(String),

    #[error("{0}")]
    Other(String),
}

impl From<StorageError> for EngineError {
    fn from(e: StorageError) -> Self {
        EngineError::StorageError(e.to_string())
    }
}

impl From<TopicManagerError> for EngineError {
    fn from(e: TopicManagerError) -> Self {
        EngineError::Other(e.to_string())
    }
}

impl From<LookupServiceError> for EngineError {
    fn from(e: LookupServiceError) -> Self {
        EngineError::LookupFailed(e.to_string())
    }
}

impl From<AdvertiserError> for EngineError {
    fn from(e: AdvertiserError) -> Self {
        EngineError::Other(e.to_string())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lookup_service::LookupService as LookupServiceTrait;
    use crate::storage::memory::MemoryStorage;
    use crate::topic_manager::TopicManager as TopicManagerTrait;
    use async_trait::async_trait;
    use std::sync::Mutex;

    // ── Mock TopicManager ──────────────────────────────────────────────

    struct MockTopicManager {
        admit_indices: Vec<u32>,
    }

    impl MockTopicManager {
        fn admitting(indices: Vec<u32>) -> Self {
            Self {
                admit_indices: indices,
            }
        }
    }

    #[async_trait(?Send)]
    impl TopicManagerTrait for MockTopicManager {
        async fn identify_admissible_outputs(
            &self,
            _beef: &[u8],
            _previous_coins: &[u8],
            _off_chain_values: Option<&[u8]>,
            _mode: SubmitMode,
        ) -> Result<AdmittanceInstructions, TopicManagerError> {
            Ok(AdmittanceInstructions {
                outputs_to_admit: self.admit_indices.clone(),
                coins_to_retain: vec![],
                coins_removed: None,
            })
        }

        async fn get_documentation(&self) -> String {
            "Mock topic manager".to_string()
        }

        async fn get_metadata(&self) -> ServiceMetadata {
            ServiceMetadata {
                name: "mock-tm".to_string(),
                description: Some("Mock for testing".to_string()),
                ..Default::default()
            }
        }
    }

    // ── Mock LookupService ─────────────────────────────────────────────

    struct MockLookupService {
        records: Mutex<Vec<UTXOReference>>,
    }

    impl MockLookupService {
        fn new() -> Self {
            Self {
                records: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait(?Send)]
    impl LookupServiceTrait for MockLookupService {
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
            let (txid, oi) = match payload {
                OutputAdmittedByTopic::LockingScript {
                    txid, output_index, ..
                } => (txid.clone(), *output_index),
                OutputAdmittedByTopic::WholeTx { output_index, .. } => {
                    ("whole".into(), *output_index)
                }
            };
            self.records.lock().unwrap().push(UTXOReference {
                txid,
                output_index: oi,
            });
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
            "Mock lookup service".to_string()
        }

        async fn get_metadata(&self) -> ServiceMetadata {
            ServiceMetadata {
                name: "mock-ls".to_string(),
                ..Default::default()
            }
        }
    }

    // ── Test BEEF data ──────────────────────────────────────────────────

    /// Real BRC-62 BEEF from TS overlay-services test suite.
    const TEST_BEEF_HEX: &str = "0100beef01fe636d0c0007021400fe507c0c7aa754cef1f7889d5fd395cf1f785dd7de98eed895dbedfe4e5bc70d1502ac4e164f5bc16746bb0868404292ac8318bbac3800e4aad13a014da427adce3e010b00bc4ff395efd11719b277694cface5aa50d085a0bb81f613f70313acd28cf4557010400574b2d9142b8d28b61d88e3b2c3f44d858411356b49a28a4643b6d1a6a092a5201030051a05fc84d531b5d250c23f4f886f6812f9fe3f402d61607f977b4ecd2701c19010000fd781529d58fc2523cf396a7f25440b409857e7e221766c57214b1d38c7b481f01010062f542f45ea3660f86c013ced80534cb5fd4c19d66c56e7e8c5d4bf2d40acc5e010100b121e91836fd7cd5102b654e9f72f3cf6fdbfd0b161c53a9c54b12c841126331020100000001cd4e4cac3c7b56920d1e7655e7e260d31f29d9a388d04910f1bbd72304a79029010000006b483045022100e75279a205a547c445719420aa3138bf14743e3f42618e5f86a19bde14bb95f7022064777d34776b05d816daf1699493fcdf2ef5a5ab1ad710d9c97bfb5b8f7cef3641210263e2dee22b1ddc5e11f6fab8bcd2378bdd19580d640501ea956ec0e786f93e76ffffffff013e660000000000001976a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac0000000001000100000001ac4e164f5bc16746bb0868404292ac8318bbac3800e4aad13a014da427adce3e000000006a47304402203a61a2e931612b4bda08d541cfb980885173b8dcf64a3471238ae7abcd368d6402204cbf24f04b9aa2256d8901f0ed97866603d2be8324c2bfb7a37bf8fc90edd5b441210263e2dee22b1ddc5e11f6fab8bcd2378bdd19580d640501ea956ec0e786f93e76ffffffff013c660000000000001976a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac0000000000";
    const TEST_TXID: &str = "157428aee67d11123203735e4c540fa1bdab3b36d5882c6f8c5ff79f07d20d1c";

    fn decode_hex(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    fn test_beef() -> Vec<u8> {
        decode_hex(TEST_BEEF_HEX)
    }

    fn test_tagged_beef(topics: Vec<&str>) -> TaggedBEEF {
        TaggedBEEF::new(
            test_beef(),
            topics.into_iter().map(str::to_string).collect(),
        )
    }

    // ── Helpers ────────────────────────────────────────────────────────

    fn make_engine(admit_indices: Vec<u32>) -> Engine {
        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_test".to_string(),
            Box::new(MockTopicManager::admitting(admit_indices)),
        );

        let mut lookup_services: HashMap<String, Box<dyn LookupServiceTrait>> = HashMap::new();
        lookup_services.insert("ls_test".to_string(), Box::new(MockLookupService::new()));

        let storage = Box::new(MemoryStorage::new());

        Engine::new(
            managers,
            lookup_services,
            storage,
            None,
            EngineConfig::default(),
        )
    }

    // ── Tests ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_submit_unsupported_topic_errors() {
        let engine = make_engine(vec![0]);
        let beef = test_tagged_beef(vec!["tm_nonexistent"]);

        let result = engine.submit(&beef, SubmitMode::CurrentTx).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            EngineError::UnsupportedTopic(_)
        ));
    }

    #[tokio::test]
    async fn test_submit_admits_outputs() {
        let engine = make_engine(vec![0, 1]);
        let beef = test_tagged_beef(vec!["tm_test"]);

        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        // STEAK should have tm_test with outputs 0 and 1 admitted
        let instructions = steak.get("tm_test").unwrap();
        assert_eq!(instructions.outputs_to_admit, vec![0, 1]);

        // Outputs should be in storage with real data
        let txid = TEST_TXID;
        let found = engine
            .storage()
            .find_output(txid, 0, Some("tm_test"), None, true)
            .await
            .unwrap()
            .expect("Output 0 should be in storage");

        assert_eq!(found.txid, TEST_TXID);
        assert_eq!(found.satoshis, 26172); // Real value from BRC62 BEEF
        assert!(
            !found.output_script.is_empty(),
            "Script should be populated"
        );
        assert!(found.beef.is_some(), "BEEF should be stored");
        assert!(!found.spent);
    }

    #[tokio::test]
    async fn test_submit_duplicate_is_skipped() {
        let engine = make_engine(vec![0]);
        let beef = test_tagged_beef(vec!["tm_test"]);

        // First submit
        let steak1 = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();
        assert_eq!(steak1["tm_test"].outputs_to_admit, vec![0]);

        // Second submit — should be a dupe, no new outputs admitted
        let steak2 = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();
        assert!(steak2["tm_test"].outputs_to_admit.is_empty());
    }

    #[tokio::test]
    async fn test_submit_notifies_lookup_service() {
        let engine = make_engine(vec![0]);
        let beef = test_tagged_beef(vec!["tm_test"]);

        engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        // Lookup service should have the output
        let question = LookupQuestion::new("ls_test", serde_json::json!({}));
        let answer = engine.lookup(&question, None).await.unwrap();

        match answer {
            LookupAnswer::OutputList { outputs } => {
                assert_eq!(outputs.len(), 1);
            }
            _ => panic!("Expected OutputList"),
        }
    }

    #[tokio::test]
    async fn test_lookup_unknown_service_errors() {
        let engine = make_engine(vec![]);
        let question = LookupQuestion::new("ls_nonexistent", serde_json::json!({}));

        let result = engine.lookup(&question, None).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            EngineError::LookupServiceNotFound(_)
        ));
    }

    #[tokio::test]
    async fn test_list_topic_managers() {
        let engine = make_engine(vec![]);
        let managers = engine.list_topic_managers().await;

        assert_eq!(managers.len(), 1);
        assert!(managers.contains_key("tm_test"));
        assert_eq!(managers["tm_test"].name, "mock-tm");
    }

    #[tokio::test]
    async fn test_list_lookup_service_providers() {
        let engine = make_engine(vec![]);
        let services = engine.list_lookup_service_providers().await;

        assert_eq!(services.len(), 1);
        assert!(services.contains_key("ls_test"));
        assert_eq!(services["ls_test"].name, "mock-ls");
    }

    #[tokio::test]
    async fn test_get_documentation() {
        let engine = make_engine(vec![]);

        let tm_docs = engine.get_documentation_for_topic_manager("tm_test").await;
        assert_eq!(tm_docs, "Mock topic manager");

        let ls_docs = engine.get_documentation_for_lookup_service("ls_test").await;
        assert_eq!(ls_docs, "Mock lookup service");

        let missing = engine
            .get_documentation_for_topic_manager("tm_missing")
            .await;
        assert_eq!(missing, "No documentation found!");
    }

    #[tokio::test]
    async fn test_provide_foreign_sync_response() {
        let engine = make_engine(vec![0]);

        // Submit data
        let beef = test_tagged_beef(vec!["tm_test"]);
        engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        // Query GASP sync
        let request = GASPInitialRequest {
            version: 1,
            since: 0,
            limit: Some(100),
        };
        let response = engine
            .provide_foreign_sync_response(&request, "tm_test")
            .await
            .unwrap();

        // One tx admitted with output index 0
        assert_eq!(response.utxo_list.len(), 1);
        assert_eq!(response.utxo_list[0].txid, TEST_TXID);
        assert_eq!(response.since, 0);
    }

    #[tokio::test]
    async fn test_delete_utxo_deep_simple() {
        let engine = make_engine(vec![0]);
        let beef = test_tagged_beef(vec!["tm_test"]);

        engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        let txid = TEST_TXID;
        let output = engine
            .storage()
            .find_output(txid, 0, Some("tm_test"), None, false)
            .await
            .unwrap()
            .unwrap();

        // Delete it
        engine.delete_utxo_deep(&output).await.unwrap();

        // Should be gone
        let found = engine
            .storage()
            .find_output(txid, 0, Some("tm_test"), None, false)
            .await
            .unwrap();
        assert!(found.is_none());
    }

    #[tokio::test]
    async fn test_sync_config_defaults_to_ship() {
        let engine = make_engine(vec![]);
        let sync = &engine.config.sync_configuration;

        // tm_test should default to SHIP sync
        assert!(sync.contains_key("tm_test"));
        assert!(matches!(sync["tm_test"], SyncTarget::Ship));
    }

    #[tokio::test]
    async fn test_multiple_topics_in_single_submit() {
        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_alpha".to_string(),
            Box::new(MockTopicManager::admitting(vec![0])),
        );
        managers.insert(
            "tm_beta".to_string(),
            Box::new(MockTopicManager::admitting(vec![1])),
        );

        let engine = Engine::new(
            managers,
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig::default(),
        );

        let beef = test_tagged_beef(vec!["tm_alpha", "tm_beta"]);

        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        assert_eq!(steak["tm_alpha"].outputs_to_admit, vec![0]);
        assert_eq!(steak["tm_beta"].outputs_to_admit, vec![1]);
    }

    #[tokio::test]
    async fn test_evict_output_with_topic() {
        let engine = make_engine(vec![0]);
        let beef = test_tagged_beef(vec!["tm_test"]);
        engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        // Output should exist
        let found = engine
            .storage()
            .find_output(TEST_TXID, 0, Some("tm_test"), None, false)
            .await
            .unwrap();
        assert!(found.is_some(), "Output should exist before eviction");

        // Evict with topic
        engine
            .evict_output(TEST_TXID, 0, Some("tm_test"))
            .await
            .unwrap();

        // Output should be gone from storage
        let found = engine
            .storage()
            .find_output(TEST_TXID, 0, Some("tm_test"), None, false)
            .await
            .unwrap();
        assert!(found.is_none(), "Output should be gone after eviction");

        // Lookup service should also have been notified (output evicted)
        let question = LookupQuestion::new("ls_test", serde_json::json!({}));
        let answer = engine.lookup(&question, None).await.unwrap();
        match answer {
            LookupAnswer::OutputList { outputs } => {
                assert!(
                    outputs.is_empty(),
                    "Lookup should return empty after eviction"
                );
            }
            _ => panic!("Expected OutputList"),
        }
    }

    #[tokio::test]
    async fn test_evict_output_without_topic() {
        let engine = make_engine(vec![0]);
        let beef = test_tagged_beef(vec!["tm_test"]);
        engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        // Evict without topic — should find and remove across all topics
        engine.evict_output(TEST_TXID, 0, None).await.unwrap();

        let found = engine
            .storage()
            .find_output(TEST_TXID, 0, Some("tm_test"), None, false)
            .await
            .unwrap();
        assert!(
            found.is_none(),
            "Output should be gone after topic-less eviction"
        );
    }

    #[tokio::test]
    async fn test_evict_nonexistent_output_is_ok() {
        let engine = make_engine(vec![]);

        // Evicting something that doesn't exist should not error
        let result = engine
            .evict_output("nonexistent_txid", 99, Some("tm_test"))
            .await;
        assert!(result.is_ok(), "Evicting nonexistent output should succeed");
    }

    // ── Mock Broadcaster ──────────────────────────────────────────────

    use crate::broadcaster::Broadcaster;
    use std::sync::Arc;

    type BroadcastCallLog = Arc<Mutex<Vec<(String, Vec<String>)>>>;

    struct MockBroadcaster {
        calls: BroadcastCallLog,
    }

    impl MockBroadcaster {
        fn new() -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        #[allow(dead_code, reason = "kept for future test extension")]
        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }

        #[allow(dead_code, reason = "kept for future test extension")]
        fn calls(&self) -> Vec<(String, Vec<String>)> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait(?Send)]
    impl Broadcaster for MockBroadcaster {
        async fn broadcast_to_host(
            &self,
            host_url: &str,
            tagged_beef: &TaggedBEEF,
        ) -> Result<(), String> {
            self.calls
                .lock()
                .unwrap()
                .push((host_url.to_string(), tagged_beef.topics.clone()));
            Ok(())
        }
    }

    // ── Mock SHIP LookupService ─────────────────────────────────────

    /// A mock "ls_ship" lookup service that returns pre-configured UTXOReferences
    /// when queried. Used to test SHIP propagation without depending on
    /// overlay-discovery in the engine crate's unit tests.
    struct MockSHIPLookupService {
        /// Maps topic -> list of (txid, output_index) references
        records: Mutex<HashMap<String, Vec<UTXOReference>>>,
    }

    impl MockSHIPLookupService {
        fn with_records(records: HashMap<String, Vec<UTXOReference>>) -> Self {
            Self {
                records: Mutex::new(records),
            }
        }
    }

    #[async_trait(?Send)]
    impl LookupServiceTrait for MockSHIPLookupService {
        fn admission_mode(&self) -> AdmissionMode {
            AdmissionMode::LockingScript
        }

        fn spend_notification_mode(&self) -> SpendNotificationMode {
            SpendNotificationMode::None
        }

        async fn output_admitted_by_topic(
            &self,
            _payload: &OutputAdmittedByTopic,
        ) -> Result<(), LookupServiceError> {
            Ok(())
        }

        async fn output_evicted(
            &self,
            _txid: &str,
            _output_index: u32,
        ) -> Result<(), LookupServiceError> {
            Ok(())
        }

        async fn lookup(
            &self,
            question: &LookupQuestion,
        ) -> Result<LookupResult, LookupServiceError> {
            // Parse the topics from the query
            if let Some(topics) = question.query.get("topics").and_then(|v| v.as_array()) {
                let records = self.records.lock().unwrap();
                let mut results = Vec::new();
                for topic_val in topics {
                    if let Some(topic) = topic_val.as_str() {
                        if let Some(refs) = records.get(topic) {
                            results.extend(refs.iter().cloned());
                        }
                    }
                }
                return Ok(LookupResult::OutputList(results));
            }
            Ok(LookupResult::OutputList(vec![]))
        }

        async fn get_documentation(&self) -> String {
            "Mock SHIP lookup service".to_string()
        }

        async fn get_metadata(&self) -> ServiceMetadata {
            ServiceMetadata {
                name: "mock-ls-ship".to_string(),
                ..Default::default()
            }
        }
    }

    /// Build a minimal PushDrop locking script for a SHIP advertisement.
    fn build_ship_pushdrop_script(domain: &str, topic: &str) -> Vec<u8> {
        use bsv_rs::script::templates::PushDrop;
        use bsv_rs::PublicKey;

        let fields = vec![
            b"SHIP".to_vec(),
            vec![0x02; 33], // fake compressed pubkey bytes for identity_key field
            domain.as_bytes().to_vec(),
            topic.as_bytes().to_vec(),
        ];

        // PushDrop requires a real compressed public key for the locking script
        // Use a well-known test key (generator point G)
        let pubkey = PublicKey::from_hex(
            "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
        )
        .expect("valid test pubkey");

        let pd = PushDrop::new(pubkey, fields);
        pd.lock().to_binary()
    }

    /// Create engine with a broadcaster and a mock SHIP lookup service.
    /// The `ship_domains` parameter provides (domain, topic) pairs to pre-populate.
    async fn make_engine_with_broadcaster(
        admit_indices: Vec<u32>,
        broadcaster: MockBroadcaster,
        ship_domains: Vec<(&str, &str)>,
        hosting_url: Option<&str>,
    ) -> (Engine, Arc<Mutex<Vec<(String, Vec<String>)>>>) {
        let calls = broadcaster.calls.clone();

        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_test".to_string(),
            Box::new(MockTopicManager::admitting(admit_indices)),
        );

        let storage = Box::new(MemoryStorage::new());

        // Build SHIP records: map each topic to its UTXO references and
        // store outputs in storage with PushDrop scripts so the engine can
        // parse the domain when broadcasting.
        let mut ship_records: HashMap<String, Vec<UTXOReference>> = HashMap::new();

        for (domain, topic) in &ship_domains {
            // Deterministic fake txid per domain+topic
            let fake_txid = format!("{:064x}", {
                let mut h: u64 = 0;
                for b in domain.bytes().chain(topic.bytes()) {
                    h = h.wrapping_mul(31).wrapping_add(u64::from(b));
                }
                h
            });

            // Add to SHIP records for the mock lookup service
            ship_records
                .entry(topic.to_string())
                .or_default()
                .push(UTXOReference {
                    txid: fake_txid.clone(),
                    output_index: 0,
                });

            // Build PushDrop SHIP script and store as an output in main storage
            let script = build_ship_pushdrop_script(domain, topic);

            let output = Output {
                txid: fake_txid,
                output_index: 0,
                output_script: script,
                satoshis: 1,
                topic: "tm_ship".to_string(),
                spent: false,
                outputs_consumed: vec![],
                consumed_by: vec![],
                beef: None,
                block_height: None,
                score: None,
            };

            storage.insert_output(&output).await.unwrap();
        }

        let mut lookup_services: HashMap<String, Box<dyn LookupServiceTrait>> = HashMap::new();
        lookup_services.insert("ls_test".to_string(), Box::new(MockLookupService::new()));
        lookup_services.insert(
            "ls_ship".to_string(),
            Box::new(MockSHIPLookupService::with_records(ship_records)),
        );

        let config = EngineConfig {
            hosting_url: hosting_url.map(str::to_string),
            ..Default::default()
        };

        let engine = Engine::with_chain_tracker(
            managers,
            lookup_services,
            storage,
            None,
            Some(Box::new(broadcaster)),
            None,
            config,
        );

        (engine, calls)
    }

    // ── Broadcaster Tests ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_broadcaster_called_on_current_tx_with_ship_peers() {
        let broadcaster = MockBroadcaster::new();
        let (engine, calls) = make_engine_with_broadcaster(
            vec![0],
            broadcaster,
            vec![("https://peer1.example.com", "tm_test")],
            Some("https://self.example.com"),
        )
        .await;

        let beef = test_tagged_beef(vec!["tm_test"]);
        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        // Submit should succeed
        assert!(!steak["tm_test"].outputs_to_admit.is_empty());

        // Broadcaster should have been called for peer1
        let recorded = calls.lock().unwrap();
        assert_eq!(
            recorded.len(),
            1,
            "Broadcaster should be called once for the one SHIP peer"
        );
        assert_eq!(recorded[0].0, "https://peer1.example.com");
    }

    #[tokio::test]
    async fn test_broadcaster_not_called_on_historical_tx() {
        let broadcaster = MockBroadcaster::new();
        let (engine, calls) = make_engine_with_broadcaster(
            vec![0],
            broadcaster,
            vec![("https://peer1.example.com", "tm_test")],
            None,
        )
        .await;

        let beef = test_tagged_beef(vec!["tm_test"]);
        let steak = engine
            .submit(&beef, SubmitMode::HistoricalTx)
            .await
            .unwrap();

        assert!(!steak["tm_test"].outputs_to_admit.is_empty());

        // Broadcaster should NOT have been called
        assert_eq!(
            calls.lock().unwrap().len(),
            0,
            "Broadcaster should not be called for historical TX"
        );
    }

    #[tokio::test]
    async fn test_broadcaster_not_called_on_historical_tx_no_spv() {
        let broadcaster = MockBroadcaster::new();
        let (engine, calls) = make_engine_with_broadcaster(
            vec![0],
            broadcaster,
            vec![("https://peer1.example.com", "tm_test")],
            None,
        )
        .await;

        let beef = test_tagged_beef(vec!["tm_test"]);
        let steak = engine
            .submit(&beef, SubmitMode::HistoricalTxNoSpv)
            .await
            .unwrap();

        assert!(!steak["tm_test"].outputs_to_admit.is_empty());

        assert_eq!(
            calls.lock().unwrap().len(),
            0,
            "Broadcaster should not be called for historical-tx-no-spv"
        );
    }

    #[tokio::test]
    async fn test_broadcaster_skips_self_hosting_url() {
        let broadcaster = MockBroadcaster::new();
        let (engine, calls) = make_engine_with_broadcaster(
            vec![0],
            broadcaster,
            vec![
                ("https://self.example.com", "tm_test"),
                ("https://peer2.example.com", "tm_test"),
            ],
            Some("https://self.example.com"),
        )
        .await;

        let beef = test_tagged_beef(vec!["tm_test"]);
        engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        // Should only broadcast to peer2, not to self
        let recorded = calls.lock().unwrap();
        assert_eq!(
            recorded.len(),
            1,
            "Should broadcast to peer2 only, skipping self"
        );
        assert_eq!(recorded[0].0, "https://peer2.example.com");
    }

    #[tokio::test]
    async fn test_broadcaster_not_called_when_no_outputs_admitted() {
        let broadcaster = MockBroadcaster::new();
        // Topic manager admits nothing
        let (engine, calls) = make_engine_with_broadcaster(
            vec![],
            broadcaster,
            vec![("https://peer1.example.com", "tm_test")],
            None,
        )
        .await;

        let beef = test_tagged_beef(vec!["tm_test"]);
        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        assert!(steak["tm_test"].outputs_to_admit.is_empty());

        // No outputs admitted -> no broadcast
        assert_eq!(
            calls.lock().unwrap().len(),
            0,
            "Broadcaster should not be called when no outputs are admitted"
        );
    }

    #[tokio::test]
    async fn test_broadcaster_not_called_when_no_ship_peers() {
        let broadcaster = MockBroadcaster::new();
        // No SHIP peers registered
        let (engine, calls) = make_engine_with_broadcaster(
            vec![0],
            broadcaster,
            vec![], // no SHIP peers
            None,
        )
        .await;

        let beef = test_tagged_beef(vec!["tm_test"]);
        engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        assert_eq!(
            calls.lock().unwrap().len(),
            0,
            "Broadcaster should not be called when no SHIP peers exist"
        );
    }

    // ── Configurable SpendNotification mock ─────────────────────────────

    /// A mock lookup service whose SpendNotificationMode can be configured.
    /// Uses a shared Arc<Mutex<Vec<OutputSpent>>> so the caller can inspect
    /// captured payloads after submit().
    struct SpendModeLookupService {
        mode: SpendNotificationMode,
        records: Mutex<Vec<UTXOReference>>,
        spent_payloads: Arc<Mutex<Vec<OutputSpent>>>,
    }

    impl SpendModeLookupService {
        fn with_mode(mode: SpendNotificationMode, capture: Arc<Mutex<Vec<OutputSpent>>>) -> Self {
            Self {
                mode,
                records: Mutex::new(Vec::new()),
                spent_payloads: capture,
            }
        }
    }

    #[async_trait(?Send)]
    impl LookupServiceTrait for SpendModeLookupService {
        fn admission_mode(&self) -> AdmissionMode {
            AdmissionMode::LockingScript
        }

        fn spend_notification_mode(&self) -> SpendNotificationMode {
            self.mode
        }

        async fn output_admitted_by_topic(
            &self,
            payload: &OutputAdmittedByTopic,
        ) -> Result<(), LookupServiceError> {
            let (txid, oi) = match payload {
                OutputAdmittedByTopic::LockingScript {
                    txid, output_index, ..
                } => (txid.clone(), *output_index),
                OutputAdmittedByTopic::WholeTx { output_index, .. } => {
                    ("whole".into(), *output_index)
                }
            };
            self.records.lock().unwrap().push(UTXOReference {
                txid,
                output_index: oi,
            });
            Ok(())
        }

        async fn output_spent(&self, payload: &OutputSpent) -> Result<(), LookupServiceError> {
            self.spent_payloads.lock().unwrap().push(payload.clone());
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
            "SpendMode mock lookup service".to_string()
        }

        async fn get_metadata(&self) -> ServiceMetadata {
            ServiceMetadata {
                name: "spend-mode-ls".to_string(),
                ..Default::default()
            }
        }
    }

    /// TXID of the input's source transaction in TEST_BEEF_HEX (the "previous coin").
    const PREVIOUS_TXID: &str = "3ecead27a44d013ad1aae40038acbb1883ac9242406808bb4667c15b4f164eac";

    /// Build an engine with a pre-populated previous output and a SpendModeLookupService.
    /// Returns the engine and the shared capture vec for inspecting OutputSpent payloads.
    async fn make_spend_mode_engine(
        mode: SpendNotificationMode,
    ) -> (Engine, Arc<Mutex<Vec<OutputSpent>>>) {
        let storage = MemoryStorage::new();

        // Pre-populate storage with the previous output that TEST_BEEF_HEX's input spends.
        let prev_output = Output {
            txid: PREVIOUS_TXID.to_string(),
            output_index: 0,
            output_script: vec![0x76, 0xa9],
            satoshis: 26174,
            topic: "tm_test".to_string(),
            spent: false,
            outputs_consumed: vec![],
            consumed_by: vec![],
            beef: Some(test_beef()),
            block_height: None,
            score: Some(1000.0),
        };
        storage.insert_output(&prev_output).await.unwrap();

        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_test".to_string(),
            Box::new(MockTopicManager::admitting(vec![0])),
        );

        let capture: Arc<Mutex<Vec<OutputSpent>>> = Arc::new(Mutex::new(Vec::new()));
        let ls = SpendModeLookupService::with_mode(mode, capture.clone());
        let mut lookup_services: HashMap<String, Box<dyn LookupServiceTrait>> = HashMap::new();
        lookup_services.insert("ls_test".to_string(), Box::new(ls));

        let engine = Engine::new(
            managers,
            lookup_services,
            Box::new(storage),
            None,
            EngineConfig::default(),
        );

        (engine, capture)
    }

    #[tokio::test]
    async fn test_spend_notification_mode_script() {
        let (engine, capture) = make_spend_mode_engine(SpendNotificationMode::Script).await;

        let beef = test_tagged_beef(vec!["tm_test"]);
        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        // The new output should be admitted
        assert_eq!(steak["tm_test"].outputs_to_admit, vec![0]);

        // The lookup service should have received a Script spend notification
        let payloads = capture.lock().unwrap().clone();
        assert_eq!(payloads.len(), 1, "Expected exactly one spend notification");

        match &payloads[0] {
            OutputSpent::Script {
                txid,
                output_index,
                topic,
                spending_txid,
                input_index,
                unlocking_script,
                sequence_number,
                ..
            } => {
                assert_eq!(txid, PREVIOUS_TXID);
                assert_eq!(*output_index, 0);
                assert_eq!(topic, "tm_test");
                assert_eq!(spending_txid, TEST_TXID);
                // The BEEF has a single input at index 0 that spends PREVIOUS_TXID:0
                assert_eq!(*input_index, 0);
                // The unlocking script should be non-empty (it's a P2PKH scriptSig)
                assert!(
                    !unlocking_script.is_empty(),
                    "Unlocking script should be non-empty"
                );
                // Standard final sequence number
                assert_eq!(*sequence_number, 0xffff_ffff);
            }
            other => panic!("Expected OutputSpent::Script, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_spend_notification_mode_whole_tx() {
        let (engine, capture) = make_spend_mode_engine(SpendNotificationMode::WholeTx).await;

        let beef = test_tagged_beef(vec!["tm_test"]);
        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        assert_eq!(steak["tm_test"].outputs_to_admit, vec![0]);

        let payloads = capture.lock().unwrap().clone();
        assert_eq!(payloads.len(), 1, "Expected exactly one spend notification");

        match &payloads[0] {
            OutputSpent::WholeTx {
                txid,
                output_index,
                topic,
                spending_atomic_beef,
                ..
            } => {
                assert_eq!(txid, PREVIOUS_TXID);
                assert_eq!(*output_index, 0);
                assert_eq!(topic, "tm_test");
                // The spending BEEF should be the entire BEEF we submitted
                assert_eq!(spending_atomic_beef, &test_beef());
            }
            other => panic!("Expected OutputSpent::WholeTx, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_spend_notification_mode_txid() {
        let (engine, capture) = make_spend_mode_engine(SpendNotificationMode::Txid).await;

        let beef = test_tagged_beef(vec!["tm_test"]);
        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        assert_eq!(steak["tm_test"].outputs_to_admit, vec![0]);

        let payloads = capture.lock().unwrap().clone();
        assert_eq!(payloads.len(), 1, "Expected exactly one spend notification");

        match &payloads[0] {
            OutputSpent::Txid {
                txid,
                output_index,
                topic,
                spending_txid,
            } => {
                assert_eq!(txid, PREVIOUS_TXID);
                assert_eq!(*output_index, 0);
                assert_eq!(topic, "tm_test");
                assert_eq!(spending_txid, TEST_TXID);
            }
            other => panic!("Expected OutputSpent::Txid, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_spend_notification_mode_none() {
        let (engine, capture) = make_spend_mode_engine(SpendNotificationMode::None).await;

        let beef = test_tagged_beef(vec!["tm_test"]);
        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        assert_eq!(steak["tm_test"].outputs_to_admit, vec![0]);

        let payloads = capture.lock().unwrap().clone();
        assert_eq!(payloads.len(), 1, "Expected exactly one spend notification");

        match &payloads[0] {
            OutputSpent::None {
                txid,
                output_index,
                topic,
            } => {
                assert_eq!(txid, PREVIOUS_TXID);
                assert_eq!(*output_index, 0);
                assert_eq!(topic, "tm_test");
            }
            other => panic!("Expected OutputSpent::None, got: {other:?}"),
        }
    }

    // ── GASP Sync Tests ───────────────────────────────────────────────

    #[tokio::test]
    async fn test_start_gasp_sync_empty_config() {
        let engine = Engine::new(
            HashMap::new(),
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig {
                sync_configuration: HashMap::new(),
                ..Default::default()
            },
        );

        let result = engine.start_gasp_sync().await.unwrap();
        assert!(
            result.topics_synced.is_empty(),
            "No topics should be synced with empty config"
        );
    }

    #[tokio::test]
    async fn test_start_gasp_sync_disabled_topic_skipped() {
        let mut sync_config: SyncConfiguration = HashMap::new();
        sync_config.insert("tm_test".to_string(), SyncTarget::Disabled);

        let engine = Engine::new(
            HashMap::new(),
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig {
                sync_configuration: sync_config,
                ..Default::default()
            },
        );

        let result = engine.start_gasp_sync().await.unwrap();
        assert!(
            result.topics_synced.is_empty(),
            "Disabled topics should not appear in results"
        );
    }

    #[tokio::test]
    async fn test_start_gasp_sync_peers_config() {
        let mut sync_config: SyncConfiguration = HashMap::new();
        sync_config.insert(
            "tm_test".to_string(),
            SyncTarget::Peers(vec![
                "https://peer1.example.com".to_string(),
                "https://peer2.example.com".to_string(),
            ]),
        );

        let engine = Engine::new(
            HashMap::new(),
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig {
                sync_configuration: sync_config,
                ..Default::default()
            },
        );

        let result = engine.start_gasp_sync().await.unwrap();
        assert_eq!(result.topics_synced.len(), 1);

        let topic_result = &result.topics_synced["tm_test"];
        assert_eq!(topic_result.peers.len(), 2);
        assert_eq!(topic_result.sync_type, "peers");
        assert!(topic_result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_start_gasp_sync_peers_filters_self() {
        let mut sync_config: SyncConfiguration = HashMap::new();
        sync_config.insert(
            "tm_test".to_string(),
            SyncTarget::Peers(vec![
                "https://self.example.com".to_string(),
                "https://peer2.example.com".to_string(),
            ]),
        );

        let engine = Engine::new(
            HashMap::new(),
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig {
                hosting_url: Some("https://self.example.com".to_string()),
                sync_configuration: sync_config,
                ..Default::default()
            },
        );

        let result = engine.start_gasp_sync().await.unwrap();
        let topic_result = &result.topics_synced["tm_test"];
        assert_eq!(
            topic_result.peers.len(),
            1,
            "Self URL should be filtered out"
        );
        assert_eq!(topic_result.peers[0], "https://peer2.example.com");
    }

    #[tokio::test]
    async fn test_start_gasp_sync_ship_discovers_peers() {
        let broadcaster = MockBroadcaster::new();
        let (engine, _calls) = make_engine_with_broadcaster(
            vec![0],
            broadcaster,
            vec![
                ("https://peer1.example.com", "tm_test"),
                ("https://peer2.example.com", "tm_test"),
            ],
            Some("https://self.example.com"),
        )
        .await;

        let result = engine.start_gasp_sync().await.unwrap();
        assert!(
            result.topics_synced.contains_key("tm_test"),
            "tm_test should be in sync results"
        );

        let topic_result = &result.topics_synced["tm_test"];
        assert_eq!(topic_result.sync_type, "ship");
        assert_eq!(
            topic_result.peers.len(),
            2,
            "Should discover both SHIP peers"
        );
        assert!(topic_result
            .peers
            .contains(&"https://peer1.example.com".to_string()));
        assert!(topic_result
            .peers
            .contains(&"https://peer2.example.com".to_string()));
    }

    #[tokio::test]
    async fn test_start_gasp_sync_ship_filters_self() {
        let broadcaster = MockBroadcaster::new();
        let (engine, _calls) = make_engine_with_broadcaster(
            vec![0],
            broadcaster,
            vec![
                ("https://self.example.com", "tm_test"),
                ("https://peer2.example.com", "tm_test"),
            ],
            Some("https://self.example.com"),
        )
        .await;

        let result = engine.start_gasp_sync().await.unwrap();
        let topic_result = &result.topics_synced["tm_test"];
        assert_eq!(
            topic_result.peers.len(),
            1,
            "Self URL should be filtered out from SHIP discovery"
        );
        assert_eq!(topic_result.peers[0], "https://peer2.example.com");
    }

    #[tokio::test]
    async fn test_start_gasp_sync_ship_no_ls_ship_returns_empty() {
        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_custom".to_string(),
            Box::new(MockTopicManager::admitting(vec![])),
        );

        let mut sync_config: SyncConfiguration = HashMap::new();
        sync_config.insert("tm_custom".to_string(), SyncTarget::Ship);

        let engine = Engine::new(
            managers,
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig {
                sync_configuration: sync_config,
                ..Default::default()
            },
        );

        let result = engine.start_gasp_sync().await.unwrap();
        let topic_result = &result.topics_synced["tm_custom"];
        assert!(
            topic_result.peers.is_empty(),
            "Without ls_ship, SHIP discovery should return no peers"
        );
    }

    #[tokio::test]
    async fn test_start_gasp_sync_mixed_config() {
        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_peers".to_string(),
            Box::new(MockTopicManager::admitting(vec![])),
        );
        managers.insert(
            "tm_disabled".to_string(),
            Box::new(MockTopicManager::admitting(vec![])),
        );
        managers.insert(
            "tm_ship_topic".to_string(),
            Box::new(MockTopicManager::admitting(vec![])),
        );

        let mut sync_config: SyncConfiguration = HashMap::new();
        sync_config.insert(
            "tm_peers".to_string(),
            SyncTarget::Peers(vec!["https://peer.com".to_string()]),
        );
        sync_config.insert("tm_disabled".to_string(), SyncTarget::Disabled);
        sync_config.insert("tm_ship_topic".to_string(), SyncTarget::Ship);

        let engine = Engine::new(
            managers,
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig {
                sync_configuration: sync_config,
                ..Default::default()
            },
        );

        let result = engine.start_gasp_sync().await.unwrap();
        assert_eq!(result.topics_synced.len(), 2);
        assert!(result.topics_synced.contains_key("tm_peers"));
        assert!(result.topics_synced.contains_key("tm_ship_topic"));
        assert!(!result.topics_synced.contains_key("tm_disabled"));

        assert_eq!(result.topics_synced["tm_peers"].peers.len(), 1);
        assert_eq!(result.topics_synced["tm_peers"].sync_type, "peers");
        assert_eq!(result.topics_synced["tm_ship_topic"].sync_type, "ship");
    }

    #[tokio::test]
    async fn test_gasp_sync_result_serialization() {
        let mut topics_synced = HashMap::new();
        topics_synced.insert(
            "tm_test".to_string(),
            super::TopicSyncResult {
                peers: vec!["https://peer.com".to_string()],
                sync_type: "ship".to_string(),
                errors: vec![],
            },
        );

        let result = super::GASPSyncResult { topics_synced };
        let json = serde_json::to_string(&result).unwrap();
        let back: super::GASPSyncResult = serde_json::from_str(&json).unwrap();

        assert_eq!(back.topics_synced.len(), 1);
        assert_eq!(back.topics_synced["tm_test"].peers.len(), 1);
        assert_eq!(back.topics_synced["tm_test"].sync_type, "ship");
    }

    // ── GASP Sync With Factory Tests ─────────────────────────────────

    /// Mock GASPRemote for factory tests
    struct MockSyncRemote {
        utxos: Vec<crate::types::GASPOutput>,
    }

    #[async_trait(?Send)]
    impl crate::gasp::GASPRemote for MockSyncRemote {
        async fn get_initial_response(
            &self,
            request: &crate::types::GASPInitialRequest,
        ) -> Result<crate::types::GASPInitialResponse, crate::gasp::GASPError> {
            let utxos: Vec<crate::types::GASPOutput> = self
                .utxos
                .iter()
                .filter(|u| u.score as u64 >= request.since)
                .cloned()
                .collect();
            Ok(crate::types::GASPInitialResponse {
                utxo_list: utxos,
                since: request.since,
            })
        }
        async fn get_initial_reply(
            &self,
            _: &crate::types::GASPInitialResponse,
        ) -> Result<crate::types::GASPInitialReply, crate::gasp::GASPError> {
            Ok(crate::types::GASPInitialReply {
                utxo_list: Vec::new(),
            })
        }
        async fn request_node(
            &self,
            graph_id: &str,
            txid: &str,
            output_index: u32,
            _: bool,
        ) -> Result<crate::types::GASPNode, crate::gasp::GASPError> {
            Ok(crate::types::GASPNode {
                graph_id: graph_id.to_string(),
                raw_tx: format!("rawtx_{txid}"),
                output_index,
                proof: None,
                tx_metadata: None,
                output_metadata: None,
                inputs: None,
            })
        }
        async fn submit_node(
            &self,
            _: &crate::types::GASPNode,
        ) -> Result<Option<crate::types::GASPNodeResponse>, crate::gasp::GASPError> {
            Ok(None)
        }
    }

    /// Mock factory that creates MockSyncRemote instances
    struct MockGASPRemoteFactory;

    impl crate::gasp::GASPRemoteFactory for MockGASPRemoteFactory {
        fn create_remote(&self, _peer_url: &str, _topic: &str) -> Box<dyn crate::gasp::GASPRemote> {
            Box::new(MockSyncRemote {
                utxos: vec![
                    crate::types::GASPOutput {
                        txid: "remote_tx1".to_string(),
                        output_index: 0,
                        score: 100.0,
                    },
                    crate::types::GASPOutput {
                        txid: "remote_tx2".to_string(),
                        output_index: 0,
                        score: 200.0,
                    },
                ],
            })
        }
    }

    #[tokio::test]
    async fn test_start_gasp_sync_with_factory_runs_sync() {
        let mut sync_config: SyncConfiguration = HashMap::new();
        sync_config.insert(
            "tm_test".to_string(),
            SyncTarget::Peers(vec!["https://peer1.example.com".to_string()]),
        );

        let mut engine = Engine::new(
            HashMap::new(),
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig {
                sync_configuration: sync_config,
                ..Default::default()
            },
        );

        engine.set_gasp_remote_factory(Box::new(MockGASPRemoteFactory));

        let result = engine.start_gasp_sync().await.unwrap();
        assert_eq!(result.topics_synced.len(), 1);

        let topic_result = &result.topics_synced["tm_test"];
        assert_eq!(topic_result.peers.len(), 1);
        assert!(
            topic_result.errors.is_empty(),
            "Sync should succeed with mock remote"
        );

        // Verify last_interaction was persisted
        let last = engine
            .storage()
            .get_last_interaction("https://peer1.example.com", "tm_test")
            .await
            .unwrap();
        assert_eq!(
            last, 200,
            "last_interaction should be updated to highest score"
        );
    }

    #[tokio::test]
    async fn test_start_gasp_sync_with_factory_handles_error() {
        struct FailingFactory;

        impl crate::gasp::GASPRemoteFactory for FailingFactory {
            fn create_remote(
                &self,
                _peer_url: &str,
                _topic: &str,
            ) -> Box<dyn crate::gasp::GASPRemote> {
                struct FailingRemote;
                #[async_trait(?Send)]
                impl crate::gasp::GASPRemote for FailingRemote {
                    async fn get_initial_response(
                        &self,
                        _: &crate::types::GASPInitialRequest,
                    ) -> Result<crate::types::GASPInitialResponse, crate::gasp::GASPError>
                    {
                        Err(crate::gasp::GASPError::RemoteError(
                            "connection refused".into(),
                        ))
                    }
                    async fn get_initial_reply(
                        &self,
                        _: &crate::types::GASPInitialResponse,
                    ) -> Result<crate::types::GASPInitialReply, crate::gasp::GASPError>
                    {
                        unreachable!()
                    }
                    async fn request_node(
                        &self,
                        _: &str,
                        _: &str,
                        _: u32,
                        _: bool,
                    ) -> Result<crate::types::GASPNode, crate::gasp::GASPError>
                    {
                        unreachable!()
                    }
                    async fn submit_node(
                        &self,
                        _: &crate::types::GASPNode,
                    ) -> Result<Option<crate::types::GASPNodeResponse>, crate::gasp::GASPError>
                    {
                        unreachable!()
                    }
                }
                Box::new(FailingRemote)
            }
        }

        let mut sync_config: SyncConfiguration = HashMap::new();
        sync_config.insert(
            "tm_test".to_string(),
            SyncTarget::Peers(vec!["https://bad-peer.example.com".to_string()]),
        );

        let mut engine = Engine::new(
            HashMap::new(),
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig {
                sync_configuration: sync_config,
                ..Default::default()
            },
        );

        engine.set_gasp_remote_factory(Box::new(FailingFactory));

        let result = engine.start_gasp_sync().await.unwrap();
        assert_eq!(result.topics_synced.len(), 1);

        let topic_result = &result.topics_synced["tm_test"];
        assert_eq!(
            topic_result.errors.len(),
            1,
            "Should have one error for the failing peer"
        );
        assert!(topic_result.errors[0].contains("connection refused"));
    }

    #[tokio::test]
    async fn test_start_gasp_sync_without_factory_still_discovers_peers() {
        let mut sync_config: SyncConfiguration = HashMap::new();
        sync_config.insert(
            "tm_test".to_string(),
            SyncTarget::Peers(vec!["https://peer.example.com".to_string()]),
        );

        // No factory set — should still work (just peer discovery, no sync)
        let engine = Engine::new(
            HashMap::new(),
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig {
                sync_configuration: sync_config,
                ..Default::default()
            },
        );

        let result = engine.start_gasp_sync().await.unwrap();
        assert_eq!(result.topics_synced.len(), 1);

        let topic_result = &result.topics_synced["tm_test"];
        assert_eq!(topic_result.peers.len(), 1);
        assert!(topic_result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_start_gasp_sync_with_factory_multiple_peers() {
        let mut sync_config: SyncConfiguration = HashMap::new();
        sync_config.insert(
            "tm_test".to_string(),
            SyncTarget::Peers(vec![
                "https://peer1.example.com".to_string(),
                "https://peer2.example.com".to_string(),
            ]),
        );

        let mut engine = Engine::new(
            HashMap::new(),
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig {
                sync_configuration: sync_config,
                ..Default::default()
            },
        );

        engine.set_gasp_remote_factory(Box::new(MockGASPRemoteFactory));

        let result = engine.start_gasp_sync().await.unwrap();
        let topic_result = &result.topics_synced["tm_test"];
        assert_eq!(topic_result.peers.len(), 2);
        assert!(topic_result.errors.is_empty());

        // Both peers should have their last_interaction updated
        let last1 = engine
            .storage()
            .get_last_interaction("https://peer1.example.com", "tm_test")
            .await
            .unwrap();
        let last2 = engine
            .storage()
            .get_last_interaction("https://peer2.example.com", "tm_test")
            .await
            .unwrap();
        assert_eq!(last1, 200);
        assert_eq!(last2, 200);
    }

    // ── Mock ARC Broadcaster ─────────────────────────────────────────

    use crate::broadcaster::ArcBroadcaster;

    struct MockArcBroadcaster {
        calls: Arc<Mutex<Vec<String>>>,
        should_fail: bool,
    }

    impl MockArcBroadcaster {
        fn new() -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                should_fail: false,
            }
        }

        fn failing() -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                should_fail: true,
            }
        }
    }

    #[async_trait(?Send)]
    impl ArcBroadcaster for MockArcBroadcaster {
        async fn broadcast(&self, raw_tx_hex: &str) -> Result<String, String> {
            self.calls.lock().unwrap().push(raw_tx_hex.to_string());
            if self.should_fail {
                Err("mock ARC failure".to_string())
            } else {
                Ok("mock_txid_from_arc".to_string())
            }
        }
    }

    /// Create engine with an ARC broadcaster (and optionally a SHIP broadcaster).
    fn make_engine_with_arc(
        admit_indices: Vec<u32>,
        arc: MockArcBroadcaster,
    ) -> (Engine, Arc<Mutex<Vec<String>>>) {
        let arc_calls = arc.calls.clone();

        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_test".to_string(),
            Box::new(MockTopicManager::admitting(admit_indices)),
        );

        let mut lookup_services: HashMap<String, Box<dyn LookupServiceTrait>> = HashMap::new();
        lookup_services.insert("ls_test".to_string(), Box::new(MockLookupService::new()));

        let storage = Box::new(MemoryStorage::new());

        let engine = Engine::with_all(
            managers,
            lookup_services,
            storage,
            None,
            None,
            Some(Box::new(arc)),
            None,
            EngineConfig::default(),
        );

        (engine, arc_calls)
    }

    // ── ARC Broadcaster Tests ────────────────────────────────────────

    #[tokio::test]
    async fn test_arc_broadcaster_called_on_current_tx() {
        let arc = MockArcBroadcaster::new();
        let (engine, arc_calls) = make_engine_with_arc(vec![0], arc);

        let beef = test_tagged_beef(vec!["tm_test"]);
        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        assert!(!steak["tm_test"].outputs_to_admit.is_empty());

        let calls = arc_calls.lock().unwrap();
        assert_eq!(
            calls.len(),
            1,
            "ARC broadcaster should be called once for CurrentTx"
        );
        assert!(
            !calls[0].is_empty(),
            "ARC should receive non-empty raw tx hex"
        );
    }

    #[tokio::test]
    async fn test_arc_broadcaster_not_called_on_historical_tx() {
        let arc = MockArcBroadcaster::new();
        let (engine, arc_calls) = make_engine_with_arc(vec![0], arc);

        let beef = test_tagged_beef(vec!["tm_test"]);
        engine
            .submit(&beef, SubmitMode::HistoricalTx)
            .await
            .unwrap();

        assert_eq!(
            arc_calls.lock().unwrap().len(),
            0,
            "ARC broadcaster should NOT be called for HistoricalTx"
        );
    }

    #[tokio::test]
    async fn test_arc_broadcaster_not_called_on_historical_tx_no_spv() {
        let arc = MockArcBroadcaster::new();
        let (engine, arc_calls) = make_engine_with_arc(vec![0], arc);

        let beef = test_tagged_beef(vec!["tm_test"]);
        engine
            .submit(&beef, SubmitMode::HistoricalTxNoSpv)
            .await
            .unwrap();

        assert_eq!(
            arc_calls.lock().unwrap().len(),
            0,
            "ARC broadcaster should NOT be called for HistoricalTxNoSpv"
        );
    }

    #[tokio::test]
    async fn test_arc_broadcast_failure_does_not_fail_submit() {
        let arc = MockArcBroadcaster::failing();
        let (engine, arc_calls) = make_engine_with_arc(vec![0], arc);

        let beef = test_tagged_beef(vec!["tm_test"]);
        // submit should succeed even when ARC fails
        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        assert!(!steak["tm_test"].outputs_to_admit.is_empty());

        // ARC was called (and failed), but submit still succeeded
        assert_eq!(
            arc_calls.lock().unwrap().len(),
            1,
            "ARC broadcaster should be attempted even if it will fail"
        );
    }

    #[tokio::test]
    async fn test_arc_broadcaster_not_present_still_works() {
        // Engine without ARC broadcaster — should work fine
        let engine = make_engine(vec![0]);

        let beef = test_tagged_beef(vec!["tm_test"]);
        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        assert!(!steak["tm_test"].outputs_to_admit.is_empty());
    }

    // ── Sync advertisements URL validation ─────────────────────────────

    /// Mock Advertiser that tracks created advertisements via shared state.
    struct TrackingAdvertiser {
        created: Arc<Mutex<Vec<AdvertisementData>>>,
        existing_ship: Vec<Advertisement>,
        existing_slap: Vec<Advertisement>,
    }

    impl TrackingAdvertiser {
        fn new() -> (Self, Arc<Mutex<Vec<AdvertisementData>>>) {
            let created = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    created: created.clone(),
                    existing_ship: vec![],
                    existing_slap: vec![],
                },
                created,
            )
        }
    }

    use crate::advertiser::{Advertiser, AdvertiserError};

    #[async_trait(?Send)]
    impl Advertiser for TrackingAdvertiser {
        async fn create_advertisements(
            &self,
            ads: &[AdvertisementData],
        ) -> Result<TaggedBEEF, AdvertiserError> {
            self.created.lock().unwrap().extend(ads.iter().cloned());
            Ok(TaggedBEEF::new(test_beef(), vec!["tm_test".to_string()]))
        }

        async fn find_all_advertisements(
            &self,
            protocol: Protocol,
        ) -> Result<Vec<Advertisement>, AdvertiserError> {
            match protocol {
                Protocol::Ship => Ok(self.existing_ship.clone()),
                Protocol::Slap => Ok(self.existing_slap.clone()),
            }
        }

        async fn revoke_advertisements(
            &self,
            _ads: &[Advertisement],
        ) -> Result<TaggedBEEF, AdvertiserError> {
            Ok(TaggedBEEF::new(test_beef(), vec!["tm_test".to_string()]))
        }

        fn parse_advertisement(&self, _script: &[u8]) -> Option<Advertisement> {
            None
        }
    }

    /// TS: "Sync advertisements URL validation"
    /// sync_advertisements should be a no-op when hosting_url is empty string,
    /// and should skip suppressed topics (tm_ship/tm_slap) when configured.
    #[tokio::test]
    async fn test_sync_advertisements_url_validation() {
        // Case 1: Empty hosting URL — should return early without creating ads
        let (adv, created) = TrackingAdvertiser::new();

        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_test".to_string(),
            Box::new(MockTopicManager::admitting(vec![0])),
        );

        let engine = Engine::new(
            managers,
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            Some(Box::new(adv)),
            EngineConfig {
                hosting_url: Some(String::new()), // empty string — invalid
                ..Default::default()
            },
        );

        engine.sync_advertisements().await.unwrap();
        assert!(
            created.lock().unwrap().is_empty(),
            "Empty hosting URL should skip advertisement creation"
        );

        // Case 2: Valid hosting URL — should create SHIP advertisements for non-suppressed topics
        let (adv2, created2) = TrackingAdvertiser::new();

        let mut managers2: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers2.insert(
            "tm_test".to_string(),
            Box::new(MockTopicManager::admitting(vec![0])),
        );
        // Also register tm_ship to verify it gets suppressed
        managers2.insert(
            "tm_ship".to_string(),
            Box::new(MockTopicManager::admitting(vec![])),
        );

        let engine2 = Engine::new(
            managers2,
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            Some(Box::new(adv2)),
            EngineConfig {
                hosting_url: Some("https://valid.example.com".to_string()),
                suppress_default_sync_advertisements: true,
                ..Default::default()
            },
        );

        engine2.sync_advertisements().await.unwrap();
        let ads = created2.lock().unwrap();
        // tm_ship should be suppressed, only tm_test should get an advertisement
        assert!(
            ads.iter().all(|a| a.topic_or_service_name != "tm_ship"),
            "tm_ship should be suppressed when suppress_default_sync_advertisements is true"
        );
        assert!(
            ads.iter().any(|a| a.topic_or_service_name == "tm_test"),
            "tm_test should get a SHIP advertisement"
        );
    }
}
