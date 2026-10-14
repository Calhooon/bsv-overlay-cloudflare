//! The log path shares the existing bounded walk and broadcast code. Judgement
//! and mutation below fail immediately; rollback belongs to the log executor.
use super::*;
use crate::admission::*;
use std::collections::BTreeMap;

impl Engine {
    /// Gather network verdicts before opening a log unit. The source performs
    /// the host's acceptance gates and content-addressed sidecar puts here.
    pub async fn walk(
        &self,
        tagged_beef: &TaggedBEEF,
        mode: SubmitMode,
        policy: WalkBreachPolicy,
        source: &dyn EvidenceSource,
    ) -> Result<Walked, WalkAnswer> {
        let (tx, txid, could_not_run) = self
            .walk_transaction(tagged_beef, mode, None, policy)
            .await?;
        let mut beef = beef_limits::parse_beef(&tagged_beef.beef, &beef_limits::ENGINE_BEEF_LIMITS)
            .map_err(|e| EngineError::BeefParseError(e.to_string()))?;
        let cover = walk_cover(&beef, &txid);
        let outcome = if mode == SubmitMode::HistoricalTxNoSpv || could_not_run.is_some() {
            WalkOutcome::HistoricalTxNoSpv
        } else {
            WalkOutcome::Spv
        };
        let mut carried = Vec::new();
        let mut outcomes = BTreeMap::from([(txid.clone(), outcome)]);
        // BEEF order is not necessarily topological. Explicit postorder keeps
        // each predecessor before its spender without recursive parsing.
        let mut order = Vec::new();
        let mut seen = HashSet::new();
        let mut stack = vec![(txid.clone(), false)];
        while let Some((id, visited)) = stack.pop() {
            if visited {
                order.push(id);
                continue;
            }
            if !seen.insert(id.clone()) {
                continue;
            }
            let Some(body) = beef.find_txid(&id) else {
                continue;
            };
            if body.has_proof() && id != txid {
                continue;
            }
            stack.push((id, true));
            if let Some(body) = body.tx() {
                for input in &body.inputs {
                    if let Ok(parent) = input.get_source_txid() {
                        stack.push((parent, false));
                    }
                }
            }
        }
        let mut reads = 0;
        for id in order.iter().filter(|id| *id != &txid) {
            let mut topics = Vec::new();
            for topic in &tagged_beef.topics {
                if !self.lands_without_values(topic) {
                    continue;
                }
                reads += 1;
                if reads > PREDECESSOR_READS_PER_SUBMIT {
                    return Err(WalkAnswer::EvidenceUnobtainable {
                        site: "predecessor_not_landed".into(),
                        reason: "predecessor read allowance exhausted".into(),
                    });
                }
                let applied = self
                    .storage
                    .does_applied_transaction_exist(&AppliedTransaction {
                        txid: id.clone(),
                        topic: topic.clone(),
                    })
                    .await
                    .map_err(|e| WalkAnswer::EvidenceUnobtainable {
                        site: "predecessor_read".into(),
                        reason: e.to_string(),
                    })?;
                if !applied {
                    topics.push(topic.clone());
                }
            }
            if topics.is_empty() {
                continue;
            }
            let bytes = beef
                .to_binary_atomic(id)
                .map_err(|e| EngineError::BeefParseError(e.to_string()))?;
            let body = TaggedBEEF::new(bytes, topics.clone());
            let trusted = (could_not_run.is_none()).then_some(&cover);
            let (_, _, stop) = self.walk_transaction(&body, mode, trusted, policy).await?;
            let walk = if mode == SubmitMode::HistoricalTxNoSpv || stop.is_some() {
                WalkOutcome::HistoricalTxNoSpv
            } else {
                WalkOutcome::Spv
            };
            outcomes.insert(id.clone(), walk);
            carried.push(Carried {
                txid: id.clone(),
                topics,
                walk,
            });
        }
        let bodies: Vec<_> = carried
            .iter()
            .filter_map(|c| {
                beef.find_txid(&c.txid)
                    .and_then(bsv_rs::transaction::BeefTx::tx)
            })
            .collect();
        let mut evidence = source.gather(&tx, &bodies, &tagged_beef.topics).await?;
        // The host supplies acceptance evidence, never the engine's walk word.
        for (id, walk) in outcomes {
            evidence
                .tx
                .entry(id)
                .and_modify(|e| e.walk = walk)
                .or_insert(TxEvidence {
                    walk,
                    network: None,
                    proof: None,
                });
        }
        if mode == SubmitMode::CurrentTx {
            let dry = self.storage.admission_dry_run(&evidence).map_err(|e| {
                WalkAnswer::EvidenceUnobtainable {
                    site: "dry_judgement".into(),
                    reason: e.to_string(),
                }
            })?;
            let mut validations = Vec::new();
            for topic in &tagged_beef.topics {
                validations.push(
                    self.validate_topic(
                        &tx,
                        &txid,
                        topic,
                        tagged_beef.off_chain_values.as_deref(),
                        mode,
                        &TopicAdmittanceContext::DRY_RUN,
                        None,
                    )
                    .await,
                );
            }
            if let Some(reason) = dry.failure() {
                return Err(WalkAnswer::EvidenceUnobtainable {
                    site: "dry_judgement".into(),
                    reason,
                });
            }
            drop(dry);
            self.broadcast_validations(tagged_beef, mode, &tx, &validations)
                .await;
        }
        // Account evidence separately; the existing e592 walk ledger and all
        // its limits remain unchanged. Sidecar bytes themselves never cross.
        let charge = evidence
            .tx
            .iter()
            .map(|(id, e)| id.len() + serde_json::to_vec(e).expect("evidence serializes").len())
            .sum::<usize>()
            + evidence
                .sidecars
                .values()
                .map(|e| {
                    32 + serde_json::to_vec(e)
                        .expect("sidecar verdict serializes")
                        .len()
                })
                .sum::<usize>();
        Ok(Walked {
            txid,
            tagged_beef: tagged_beef.clone(),
            mode,
            policy,
            walk: outcome,
            carried,
            evidence,
            charge: charge as u64,
            could_not_run,
        })
    }

    /// Judge and mutate inside the caller's open log transaction. No network
    /// operation occurs here. The executor MUST poll once and roll back on
    /// Pending or Err, and catch swallowed failed storage statements.
    pub async fn admit(&self, walked: &Walked, log: &dyn LogTxn) -> Result<Admitted, AdmitRefusal> {
        if self.storage.admission_log_id().is_none()
            || self.storage.admission_log_id() != log.admission_log_id()
            || log.seq() == 0
        {
            return Err(AdmitRefusal::transient(
                "admission_scope",
                "engine and log must share the open transaction",
            ));
        }
        let tx = beef_limits::transaction_from_beef(
            &walked.tagged_beef.beef,
            Some(&walked.txid),
            &beef_limits::ENGINE_BEEF_LIMITS,
        )
        .map_err(|e| AdmitRefusal::transient("walked_payload", e))?;
        let txid = &walked.txid;
        let mut beef =
            beef_limits::parse_beef(&walked.tagged_beef.beef, &beef_limits::ENGINE_BEEF_LIMITS)
                .map_err(|e| AdmitRefusal::transient("walked_payload", e))?;
        let named = beef
            .to_binary_atomic(txid)
            .map_err(|e| AdmitRefusal::transient("walked_payload", e))?;
        let context = TopicAdmittanceContext {
            dry_run: false,
            admission_seq: Some(log.seq()),
        };
        let mut report = MutationReport {
            walk_could_not_run: walked.could_not_run.clone(),
            ..MutationReport::default()
        };
        let mut steak = Steak::new();
        let mut validations = Vec::new();
        for topic in &walked.tagged_beef.topics {
            let record = AppliedTransaction {
                txid: txid.clone(),
                topic: topic.clone(),
            };
            if self
                .storage
                .does_applied_transaction_exist(&record)
                .await
                .map_err(|e| AdmitRefusal::transient("does_applied_transaction_exist", e))?
            {
                steak.insert(topic.clone(), AdmittanceInstructions::default());
                report.deduped_topics.push(topic.clone());
                continue;
            }
            if !log.evidence().tx.contains_key(txid) {
                return Err(AdmitRefusal::not_now(
                    "evidence_missing",
                    format!("{txid} on {topic}"),
                ));
            }
            let Some(_) = self.managers.get(topic) else {
                return Err(AdmitRefusal::transient(
                    "topic",
                    format!("unsupported topic {topic}"),
                ));
            };
            let mut previous = Vec::new();
            let mut outputs = Vec::new();
            for (i, input) in tx.inputs.iter().enumerate() {
                let source = input.get_source_txid().unwrap_or_default();
                if let Some(output) = self
                    .storage
                    .find_output(
                        &source,
                        input.source_output_index,
                        Some(topic),
                        Some(false),
                        false,
                    )
                    .await
                    .map_err(|e| AdmitRefusal::transient("find_output", e))?
                {
                    previous.push(i as u32);
                    outputs.push(output);
                }
            }
            log.at(&format!(
                "topic_manager.{topic}.identify_admissible_outputs"
            ));
            let admittance = self
                .judge(
                    &tx,
                    topic,
                    &previous,
                    walked.tagged_beef.off_chain_values.as_deref(),
                    walked.mode,
                    &context,
                    None,
                )
                .await
                .map_err(|e| manager_refusal(topic, e))?;
            if previous.is_empty() && !tx.inputs.is_empty() {
                log.at(&format!("topic_manager.{topic}.predecessor"));
                let needed = self.managers[topic]
                    .identify_needed_inputs(&named, walked.tagged_beef.off_chain_values.as_deref())
                    .await
                    .map_err(|e| AdmitRefusal::transient("identify_needed_inputs", e))?;
                let mut named_inputs = Some(
                    needed
                        .into_iter()
                        .map(|o| format!("{}.{}", o.txid, o.output_index))
                        .collect(),
                );
                let mut reads = 0;
                if let Some(wait) = self
                    .unlanded_predecessor(
                        &tx,
                        txid,
                        &named,
                        topic,
                        walked.mode,
                        !admittance.outputs_to_admit.is_empty(),
                        &mut named_inputs,
                        &mut reads,
                        None,
                    )
                    .await
                {
                    let predecessor = wait
                        .carried
                        .filter(|id| {
                            self.lands_without_values(topic)
                                && walked
                                    .carried
                                    .iter()
                                    .any(|c| &c.txid == id && c.topics.contains(topic))
                        })
                        .map(|id| (id, topic.clone()));
                    return Err(AdmitRefusal::NotNow {
                        site: "predecessor_not_landed".into(),
                        reason: wait.why,
                        notes: log.notes(),
                        predecessor,
                    });
                }
            }
            steak.insert(topic.clone(), admittance.clone());
            validations.push((topic, previous, outputs, admittance));
        }
        // All topics judge before any engine mutation, as on the ordinary path.
        for (topic, previous, outputs, admittance) in validations {
            let mut consumed = Vec::new();
            let mut stale = Vec::new();
            for (i, output) in outputs.iter().enumerate() {
                if admittance.coins_to_retain.contains(&previous[i]) {
                    consumed.push(Outpoint::new(&output.txid, output.output_index));
                } else {
                    stale.push(output);
                }
            }
            if let Some(entry) = steak.get_mut(topic) {
                entry.coins_removed = Some(
                    stale
                        .iter()
                        .enumerate()
                        .filter_map(|(i, _)| previous.get(i).copied())
                        .collect(),
                );
            }
            for (i, output) in outputs.iter().enumerate() {
                self.storage
                    .mark_utxo_as_spent(&output.txid, output.output_index, topic)
                    .await
                    .map_err(|e| AdmitRefusal::transient("mark_utxo_as_spent", e))?;
                for (name, service) in &self.lookup_services {
                    log.at(&format!("lookup_service.{name}.output_spent"));
                    let payload = match service.spend_notification_mode() {
                        SpendNotificationMode::None => OutputSpent::None {
                            txid: output.txid.clone(),
                            output_index: output.output_index,
                            topic: topic.clone(),
                        },
                        SpendNotificationMode::Txid => OutputSpent::Txid {
                            txid: output.txid.clone(),
                            output_index: output.output_index,
                            topic: topic.clone(),
                            spending_txid: txid.clone(),
                        },
                        SpendNotificationMode::Script => {
                            let input = &tx.inputs[previous[i] as usize];
                            OutputSpent::Script {
                                txid: output.txid.clone(),
                                output_index: output.output_index,
                                topic: topic.clone(),
                                spending_txid: txid.clone(),
                                input_index: previous[i],
                                unlocking_script: input
                                    .unlocking_script
                                    .as_ref()
                                    .map(bsv_rs::UnlockingScript::to_binary)
                                    .unwrap_or_default(),
                                sequence_number: input.sequence,
                                off_chain_values: walked.tagged_beef.off_chain_values.clone(),
                            }
                        }
                        SpendNotificationMode::WholeTx => OutputSpent::WholeTx {
                            txid: output.txid.clone(),
                            output_index: output.output_index,
                            topic: topic.clone(),
                            spending_atomic_beef: named.clone(),
                            off_chain_values: walked.tagged_beef.off_chain_values.clone(),
                        },
                    };
                    service
                        .output_spent(&payload)
                        .await
                        .map_err(|e| lookup_refusal(topic, "lookup_service.output_spent", e))?;
                }
            }
            let mut new_utxos = Vec::new();
            for &vout in &admittance.outputs_to_admit {
                let (script, sats) = tx.outputs.get(vout as usize).map_or_else(
                    || (Vec::new(), 0),
                    |o| (o.locking_script.to_binary(), o.get_satoshis()),
                );
                let output = Output {
                    txid: txid.clone(),
                    output_index: vout,
                    output_script: script.clone(),
                    satoshis: sats,
                    topic: topic.clone(),
                    spent: false,
                    outputs_consumed: consumed.clone(),
                    consumed_by: Vec::new(),
                    beef: Some(walked.tagged_beef.beef.clone()),
                    block_height: None,
                    score: Some(log.now_ms() as f64),
                };
                self.storage
                    .insert_output(&output)
                    .await
                    .map_err(|e| AdmitRefusal::transient("insert_output", e))?;
                new_utxos.push(Outpoint::new(txid, vout));
                for (name, service) in &self.lookup_services {
                    log.at(&format!("lookup_service.{name}.output_admitted_by_topic"));
                    let payload = match service.admission_mode() {
                        AdmissionMode::LockingScript => OutputAdmittedByTopic::LockingScript {
                            txid: txid.clone(),
                            output_index: vout,
                            topic: topic.clone(),
                            satoshis: sats,
                            locking_script: script.clone(),
                            off_chain_values: walked.tagged_beef.off_chain_values.clone(),
                        },
                        AdmissionMode::WholeTx => OutputAdmittedByTopic::WholeTx {
                            atomic_beef: named.clone(),
                            output_index: vout,
                            topic: topic.clone(),
                            off_chain_values: walked.tagged_beef.off_chain_values.clone(),
                        },
                    };
                    service
                        .output_admitted_by_topic(&payload)
                        .await
                        .map_err(|e| {
                            lookup_refusal(topic, "lookup_service.output_admitted_by_topic", e)
                        })?;
                }
            }
            for output in stale {
                self.delete_admitted_history(output, log).await?;
            }
            for outpoint in consumed {
                let Some(output) = self
                    .storage
                    .find_output(
                        &outpoint.txid,
                        outpoint.output_index,
                        Some(topic),
                        None,
                        false,
                    )
                    .await
                    .map_err(|e| AdmitRefusal::transient("find_output", e))?
                else {
                    continue;
                };
                let mut by = output.consumed_by;
                for new in &new_utxos {
                    if !by.contains(new) {
                        by.push(new.clone());
                    }
                }
                self.storage
                    .update_consumed_by(&outpoint.txid, outpoint.output_index, topic, &by)
                    .await
                    .map_err(|e| AdmitRefusal::transient("update_consumed_by", e))?;
            }
            self.storage
                .insert_applied_transaction(&AppliedTransaction {
                    txid: txid.clone(),
                    topic: topic.clone(),
                })
                .await
                .map_err(|e| AdmitRefusal::transient("insert_applied_transaction", e))?;
            report.applied_topics.push(topic.clone());
        }
        Ok(Admitted {
            steak,
            report,
            seq: log.seq(),
            notes: log.notes(),
        })
    }

    async fn delete_admitted_history(
        &self,
        output: &Output,
        log: &dyn LogTxn,
    ) -> Result<(), AdmitRefusal> {
        let mut stack = vec![output.clone()];
        let mut seen = HashSet::new();
        while let Some(output) = stack.pop() {
            if !seen.insert((
                output.txid.clone(),
                output.output_index,
                output.topic.clone(),
            )) {
                continue;
            }
            if output.consumed_by.is_empty() {
                self.storage
                    .delete_output(&output.txid, output.output_index, &output.topic)
                    .await
                    .map_err(|e| AdmitRefusal::transient("delete_output", e))?;
                for (name, service) in &self.lookup_services {
                    log.at(&format!(
                        "lookup_service.{name}.output_no_longer_retained_in_history"
                    ));
                    service
                        .output_no_longer_retained_in_history(
                            &output.txid,
                            output.output_index,
                            &output.topic,
                        )
                        .await
                        .map_err(|e| {
                            lookup_refusal(
                                &output.topic,
                                "lookup_service.output_no_longer_retained_in_history",
                                e,
                            )
                        })?;
                }
            }
            for consumed in &output.outputs_consumed {
                if let Some(mut parent) = self
                    .storage
                    .find_output(
                        &consumed.txid,
                        consumed.output_index,
                        Some(&output.topic),
                        None,
                        false,
                    )
                    .await
                    .map_err(|e| AdmitRefusal::transient("find_output", e))?
                {
                    parent
                        .consumed_by
                        .retain(|c| c.txid != output.txid || c.output_index != output.output_index);
                    self.storage
                        .update_consumed_by(
                            &parent.txid,
                            parent.output_index,
                            &parent.topic,
                            &parent.consumed_by,
                        )
                        .await
                        .map_err(|e| AdmitRefusal::transient("update_consumed_by", e))?;
                    stack.push(parent);
                }
            }
        }
        Ok(())
    }
}

fn lookup_refusal(topic: &str, site: &str, error: LookupServiceError) -> AdmitRefusal {
    match error {
        LookupServiceError::Admission(LookupRefusal::Deterministic(reason)) => {
            AdmitRefusal::Deterministic {
                topic: topic.into(),
                site: site.into(),
                reason,
                notes: vec![],
            }
        }
        // Unknown legacy errors are retryable, never string-classified.
        other => AdmitRefusal::transient(site, other),
    }
}

fn manager_refusal(topic: &str, error: TopicManagerError) -> AdmitRefusal {
    match error {
        TopicManagerError::Admission(error) => error,
        TopicManagerError::Other(reason) => {
            AdmitRefusal::transient("topic_manager.identify_admissible_outputs", reason)
        }
        other => AdmitRefusal::Deterministic {
            topic: topic.into(),
            site: "topic_manager.identify_admissible_outputs".into(),
            reason: other.to_string(),
            notes: vec![],
        },
    }
}
