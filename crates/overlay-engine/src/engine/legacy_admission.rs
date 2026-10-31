//! One-release compatibility for backends that have not opted into atomic
//! admission. This path preserves the repository's existing fault invariants.
//! It makes no atomicity claim and is reported as atomic=false on health.
//! The keeper must resolve its retirement against the no-undo requirement.
use super::*;
impl Engine {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn submit_legacy_counted_inner(
        &self,
        tagged_beef: &TaggedBEEF,
        mode: SubmitMode,
        bound: Option<&CallBound>,
        finalize: bool,
        walked: Option<&HashSet<String>>,
        question_reads: &mut usize,
        policy: WalkBreachPolicy,
    ) -> Result<Counted, EngineError> {
        // A submit is a real admission, never a dry run; a carried
        // predecessor's is judged dry until its topic is known not to wait.
        let carried = walked.is_some();
        let context = if carried {
            TopicAdmittanceContext::DRY_RUN
        } else {
            TopicAdmittanceContext::default()
        };
        let (mut validations, mut steak, tx, txid, could_not_run, _memory) = self
            .run_validation(tagged_beef, mode, &context, bound, walked, policy)
            .await?;
        let mut report = MutationReport {
            walk_could_not_run: could_not_run,
            ..MutationReport::default()
        };
        // The body every LOOKUP SERVICE receives NAMES the subject (BRC-95
        // atomic prefix over the WHOLE submitted body : bsv-rs's
        // `to_binary_atomic` sorts, names, never prunes). Lookup services
        // re-parse it with `from_beef(_, None)`; on a plain incomplete body
        // that pick is the wire-last ANCESTOR, so the pot index's admit hook
        // shape-checked a hop and wrote no record while the engine had
        // admitted the pot (loop-2 fleet, 2026-09-05; the live proof after
        // the engine's own subject fix). A body that cannot be re-serialized
        // (never, for a parsed subject) is handed through as submitted.
        let subject_named_beef: Vec<u8> =
            beef_limits::parse_beef(&tagged_beef.beef, &beef_limits::ENGINE_BEEF_LIMITS)
                .ok()
                .and_then(|mut b| b.to_binary_atomic(&txid).ok())
                .unwrap_or_else(|| tagged_beef.beef.clone());

        // The successor rule, asked of every topic that found no coin before
        // anything of this submit is written ([`Engine::successors_waiting`]):
        // the topics that must wait, each with why.
        let mut blocked: HashMap<String, String> = HashMap::new();
        // The landing trusts the subject's walk only if it RAN (the E592
        // lens fold, M1): after a breach nothing it reaches was walked.
        let landing = Landing {
            policy,
            subject_walk_ran: report.walk_could_not_run.is_none(),
        };
        let waits = self
            .successors_waiting(
                &tx,
                &txid,
                &subject_named_beef,
                &mut validations,
                &mut steak,
                tagged_beef.off_chain_values.as_deref(),
                mode,
                bound,
                finalize,
                !carried,
                &mut blocked,
                question_reads,
                &mut report.landed_predecessors,
                landing,
                &mut report.landed_walks_could_not_run,
                &context,
            )
            .await;
        // A carried predecessor's topic that does not wait is judged again,
        // for real: what it admits is this call's word, over the same coins.
        if carried {
            for v in &mut validations {
                if v.is_dupe || v.failed || v.read_fault.is_some() || waits.contains_key(&v.topic) {
                    continue;
                }
                match self
                    .judge(
                        &tx,
                        &v.topic,
                        &v.previous_coins,
                        tagged_beef.off_chain_values.as_deref(),
                        mode,
                        &TopicAdmittanceContext::default(),
                        bound,
                    )
                    .await
                {
                    Ok(admittance) => v.admittance = admittance,
                    Err(e) => {
                        error!("Error validating topic {} during submit: {e}", v.topic);
                        v.failed = true;
                        v.previous_coins.clear();
                        v.previous_outputs.clear();
                        v.admittance = AdmittanceInstructions::default();
                    }
                }
                steak.insert(v.topic.clone(), v.admittance.clone());
            }
        }

        // =================================================================
        // PHASE 3: MUTATE STORAGE
        // =================================================================
        for v in &validations {
            if v.is_dupe {
                report.deduped_topics.push(v.topic.clone());
                continue;
            }
            // A failed topic comes first, a read fault of it included: a
            // previous-coin read that faulted and a manager that then ERRED
            // is a failed topic with a durable report, no fault and no
            // replay. Parity: the reference fails the topic and never
            // replays. Nothing was written, so the store is as it was.
            if v.failed {
                continue;
            }

            let topic = &v.topic;
            let admittance = &v.admittance;
            let faults_before = report.faults.len();

            // A validation read that faulted: the manager judged without a
            // coin the store may hold, so NOTHING is written on that
            // judgement (the lens fold of 2026-10-07). Writing its outputs
            // would leave them beside the unseen coin, for a successor to
            // spend before the replay deletes it.
            if let Some((read, err)) = &v.read_fault {
                report.fault(topic, read, err.clone());
                self.legacy_hold_unapplied(
                    &txid,
                    topic,
                    "a validation read faulted, nothing written",
                );
                continue;
            }

            // bsv-low #559 and lane E1D: a topic that found no coin while a
            // predecessor's landing is unknown is "not now", recorded
            // nowhere ([`Engine::successors_waiting`]).
            if let Some(not_now) = waits.get(topic) {
                report.fault(topic, "predecessor_not_landed", not_now.clone());
                self.legacy_hold_unapplied(
                    &txid,
                    topic,
                    &format!("found no coin and its predecessor {not_now}"),
                );
                continue;
            }

            // -- Handle stale vs retained previous coins --
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

            // -- Mark previous outputs as spent + notify lookup services --
            // A mark that faults does not skip the notification (the
            // reference skips it): the coin is spent whatever the row says,
            // a stale one is deleted below and its lookup services must
            // know. The mark of a RETAINED coin is part of what landed: that
            // row stays, and left unspent it would be listed as a UTXO
            // beside the output that consumed it.
            let mut retained_mark_faulted = false;
            for (prev_idx, prev_output) in v.previous_outputs.iter().enumerate() {
                if let Err(e) = stored(
                    bound,
                    self.storage.mark_utxo_as_spent(
                        &prev_output.txid,
                        prev_output.output_index,
                        topic,
                    ),
                )
                .await
                {
                    error!("Error marking UTXO as spent: {e}");
                    report.fault(topic, "mark_utxo_as_spent", e.to_string());
                    retained_mark_faulted |= admittance
                        .coins_to_retain
                        .contains(&v.previous_coins[prev_idx]);
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
                            spending_atomic_beef: subject_named_beef.clone(),
                            off_chain_values: tagged_beef.off_chain_values.clone(),
                        },
                    };
                    if let Err(e) = hooked(bound, ls.output_spent(&payload)).await {
                        error!("Error notifying lookup service of spent output: {e}");
                        report.fault(topic, "lookup_service.output_spent", e);
                    }
                }
            }
            if retained_mark_faulted {
                self.legacy_hold_unapplied(
                    &txid,
                    topic,
                    "the spent mark of a retained coin faulted, nothing inserted",
                );
                continue;
            }

            // -- Insert admitted outputs --
            // The inserts stop at the first one that faults: what follows is
            // the undo, not the rest of the transaction.
            let mut new_utxos: Vec<Outpoint> = Vec::new();
            let mut attempted: Vec<u32> = Vec::new();
            let mut insert_faulted = false;

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

                attempted.push(output_index);
                if let Err(e) = stored(bound, self.storage.insert_output(&output)).await {
                    error!("Error inserting output for topic {topic}: {e}");
                    report.fault(topic, "insert_output", e.to_string());
                    insert_faulted = true;
                    break;
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
                            atomic_beef: subject_named_beef.clone(),
                            output_index,
                            topic: topic.clone(),
                            off_chain_values: tagged_beef.off_chain_values.clone(),
                        },
                    };

                    if let Err(e) = hooked(bound, ls.output_admitted_by_topic(&payload)).await {
                        error!("Error notifying lookup service: {e}");
                        report.fault(topic, "lookup_service.output_admitted_by_topic", e);
                    }
                }
            }

            // An insert faulted: the transaction did not land, so NOTHING of
            // it stays (the lens fold of 2026-10-07, F1). The previous coins
            // are kept (#559: with the delete first, one faulted insert left
            // a non-retaining head chain with NO head) and the outputs that
            // were inserted are taken out again, the faulted one included (a
            // backend's insert can fault after its row landed). Left in,
            // they could be spent and deleted by a successor before the
            // replay, and the replay, which still finds the previous coin,
            // would insert them a second time: a spent head back among the
            // UTXOs for good.
            if insert_faulted {
                if let Some(bound) = bound {
                    bound.renew();
                }
                self.legacy_undo_inserts(&txid, topic, &attempted, bound, &mut report)
                    .await;
                self.legacy_hold_unapplied(
                    &txid,
                    topic,
                    &format!(
                        "an insert faulted: {} previous coin(s) kept, nothing of it held",
                        stale_coins.len()
                    ),
                );
                continue;
            }

            // -- Delete stale outputs recursively --
            // AFTER the inserts, and once every one of them landed (bsv-low
            // #559 and the lens fold of 2026-10-07, F1). The reference
            // deletes first (`applyTopicStorageMutation`: `removeStaleOutputs`,
            // then `admitOutput`), catches a throw per topic and never
            // replays; here a fault is survived and replayed, so the order
            // is ours, an addition. The delete does NOT wait for a clean
            // report (c9921ee did, and one faulted mark or notification
            // after the insert then left the stale coin beside the new
            // output for good: two unspent heads): a fault of the mark, of
            // a notification, deletes as the reference does.
            //
            // Before a stale coin is deleted its transaction is recorded as
            // applied in the topic (the delta fold of 2026-10-07, H2; an
            // addition, one idempotent write per deleted coin). A held coin
            // proves the inserts of its transaction landed, but that
            // transaction may have no applied row (a fault after its
            // inserts withheld it, or its insert landed after its timeout),
            // and then its replay is judged again: a manager that admits it
            // with NO previous coin (the opener of a chain) had it inserted
            // a second time, unspent, beside the output that spent it, for
            // good. With the row its replay is a dupe. A record that faults
            // leaves its coin undeleted: that is a delete fault, below.
            let due_before_the_delete = bound.is_some_and(CallBound::is_due);
            let mut delete_faulted = false;
            let mut delete_started = false;
            for stale in &stale_coins {
                let spent_coin_tx = AppliedTransaction {
                    txid: stale.txid.clone(),
                    topic: topic.clone(),
                };
                if let Err(e) = stored(
                    bound,
                    self.storage.insert_applied_transaction(&spent_coin_tx),
                )
                .await
                {
                    error!(
                        "Error recording {} as applied before its coin is deleted from topic {topic}: {e}",
                        stale.txid
                    );
                    report.fault(topic, "record_spent_coin_applied", e.to_string());
                    delete_faulted = true;
                    continue;
                }
                self.not_landed
                    .borrow_mut()
                    .remove(&(stale.txid.clone(), topic.clone()));
                match stored(
                    bound,
                    self.storage.find_output(
                        &stale.txid,
                        stale.output_index,
                        Some(topic),
                        None,
                        false,
                    ),
                )
                .await
                {
                    Ok(Some(stale_output)) => {
                        delete_started = true;
                        if let Err(e) = self.delete_utxo_deep(&stale_output, bound).await {
                            error!("Error deleting stale output for topic {topic}: {e}");
                            report.fault(topic, "delete_utxo_deep", e.to_string());
                            delete_faulted = true;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        error!("Error reading stale output for topic {topic}: {e}");
                        report.fault(topic, "find_output", e.to_string());
                        delete_faulted = true;
                    }
                }
            }

            // A fault here, and what it leaves. The store is put back as it
            // was found (the inserts undone, the replay does the whole
            // transaction) only when NO delete was started: the fault is of
            // H2's record or of the read before the delete, the call
            // answered, and EVERY stale coin is read back still held.
            //
            // Once a delete was STARTED nothing is undone, whether it
            // answered or not (the delta fold of 2026-10-07, H1, for a call
            // that did not answer; the second delta fold, M1, for one that
            // answered an error). A statement can land after its caller was
            // told it failed, as after its caller stopped waiting: the coin
            // is read back held now and is deleted a moment later, and the
            // undo then left the chain with no head, for good, for a manager
            // that needs the previous coin. So the outputs stay, the stale
            // coin stays (marked spent) until its delete lands or is done
            // again, and there is no applied row. Since H2 every order from
            // there converges. The replay first, the delete never landed:
            // the coin is found, the manager admits again, the inserts are
            // no-ops, the delete is done and the row written. The replay
            // first, the delete landed late: no previous coin, so a manager
            // that needs one admits nothing and the transaction is recorded
            // with its outputs held. A successor first: it recorded this
            // transaction as applied (H2) and the replay is a dupe; the
            // stale coin's spent row is then left behind, no UTXO.
            //
            // A call that did not ANSWER before any delete (the record, the
            // read) undoes nothing either: it is not known what it did. A
            // deadline that was already due BEFORE the first of these calls
            // started nothing here, so the coins are certainly held and the
            // store is put back.
            if delete_faulted {
                let unanswered = !due_before_the_delete && bound.is_some_and(CallBound::is_due);
                if let Some(bound) = bound {
                    bound.renew();
                }
                let mut all_held = !unanswered && !delete_started;
                for stale in &stale_coins {
                    if !all_held {
                        break;
                    }
                    let read = stored(
                        bound,
                        self.storage.find_output(
                            &stale.txid,
                            stale.output_index,
                            Some(topic),
                            None,
                            false,
                        ),
                    )
                    .await;
                    if !matches!(read, Ok(Some(_))) {
                        all_held = false;
                        break;
                    }
                }
                if all_held {
                    self.legacy_undo_inserts(&txid, topic, &attempted, bound, &mut report)
                        .await;
                }
                self.legacy_hold_unapplied(
                    &txid,
                    topic,
                    if all_held {
                        "no delete of a stale coin was started: the coins are held, its outputs taken out"
                    } else if unanswered {
                        "the delete of a stale coin did not answer: nothing undone, its outputs stay"
                    } else if delete_started {
                        "the delete of a stale coin faulted: nothing undone, its outputs stay"
                    } else {
                        "no delete was started and a stale coin was not read back held: nothing undone, its outputs stay"
                    },
                );
                continue;
            }

            // -- Update consumedBy on retained previous outputs --
            for consumed_outpoint in &outputs_consumed {
                let consumed_output = match stored(
                    bound,
                    self.storage.find_output(
                        &consumed_outpoint.txid,
                        consumed_outpoint.output_index,
                        Some(topic),
                        None,
                        false,
                    ),
                )
                .await
                {
                    Ok(Some(o)) => o,
                    Ok(None) => continue,
                    Err(e) => {
                        error!("Error reading consumed output for topic {topic}: {e}");
                        report.fault(topic, "find_output", e.to_string());
                        continue;
                    }
                };
                {
                    let mut new_consumed_by = consumed_output.consumed_by.clone();
                    for new_utxo in &new_utxos {
                        if !new_consumed_by.iter().any(|c| {
                            c.txid == new_utxo.txid && c.output_index == new_utxo.output_index
                        }) {
                            new_consumed_by.push(new_utxo.clone());
                        }
                    }
                    if let Err(e) = stored(
                        bound,
                        self.storage.update_consumed_by(
                            &consumed_outpoint.txid,
                            consumed_outpoint.output_index,
                            topic,
                            &new_consumed_by,
                        ),
                    )
                    .await
                    {
                        error!("Error updating consumedBy: {e}");
                        report.fault(topic, "update_consumed_by", e.to_string());
                    }
                }
            }

            // Record applied transaction : ONLY when every write above
            // landed. A faulted topic stays unrecorded so the Phase-1 dedup
            // cannot turn its replay into a no-op (S2).
            if report.faults.len() > faults_before {
                self.legacy_hold_unapplied(
                    &txid,
                    topic,
                    &format!(
                        "{} mutation fault(s); a replay of these bytes re-applies",
                        report.faults.len() - faults_before
                    ),
                );
                continue;
            }
            let tx_record = AppliedTransaction {
                txid: txid.clone(),
                topic: topic.clone(),
            };
            match stored(bound, self.storage.insert_applied_transaction(&tx_record)).await {
                Ok(()) => {
                    report.applied_topics.push(topic.clone());
                    self.not_landed
                        .borrow_mut()
                        .remove(&(txid.clone(), topic.clone()));
                }
                Err(e) => {
                    error!("Error inserting applied transaction for topic {topic}: {e}");
                    report.fault(topic, "insert_applied_transaction", e.to_string());
                    self.not_landed
                        .borrow_mut()
                        .insert((txid.clone(), topic.clone()));
                }
            }
        }

        Ok((steak, report, blocked))
    }

    /// A topic of a submit that is NOT recorded as applied: logged, and
    /// remembered for the successor rule (`not_landed`).
    fn legacy_hold_unapplied(&self, txid: &str, topic: &str, why: &str) {
        warn!("topic {topic}: {txid}: {why}; applied_transactions NOT recorded (bsv-low #559)");
        self.not_landed
            .borrow_mut()
            .insert((txid.to_string(), topic.to_string()));
    }

    /// Take out again the outputs a faulted submit inserted (bsv-low #559,
    /// the lens fold of 2026-10-07, F1), so that nothing of a transaction
    /// that did not land is held. Only a row nobody has touched since is
    /// deleted: unspent and consumed by nothing. A plain delete, not the
    /// deep one: the row's retained inputs were not pointed at it yet. A
    /// fault here is reported (`undo_insert_output`) and the row stays: the
    /// limit of an undo made of separate calls.
    async fn legacy_undo_inserts(
        &self,
        txid: &str,
        topic: &str,
        output_indices: &[u32],
        bound: Option<&CallBound>,
        report: &mut MutationReport,
    ) {
        for &output_index in output_indices {
            let row = stored(
                bound,
                self.storage
                    .find_output(txid, output_index, Some(topic), None, false),
            )
            .await;
            let undone = match row {
                Ok(Some(row)) if !row.spent && row.consumed_by.is_empty() => {
                    stored(bound, self.storage.delete_output(txid, output_index, topic)).await
                }
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            };
            if let Err(e) = undone {
                error!("Error taking {txid}.{output_index} out of topic {topic} again: {e}");
                report.fault(topic, "undo_insert_output", e.to_string());
            }
        }
    }
}
