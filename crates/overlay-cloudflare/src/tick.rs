//! The maintenance sequence shared by the cron and the alarm.
//!
//! The step order is pinned by beef_blob_sweep's source tests and the tick
//! sequence test below. Source-specific coordination stays outside the steps.

use super::*;

/// Leave a minute of the platform's fifteen-minute event window for cleanup.
pub(crate) const TICK_BUDGET_MS: i64 = 840_000;
pub(crate) const TICK_LEASE_MARGIN_MS: i64 = 60_000;
pub(crate) const TICK_LEASE_MS: i64 = TICK_BUDGET_MS + TICK_LEASE_MARGIN_MS;

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TickSource {
    Cron,
    Alarm,
}

impl TickSource {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Cron => "cron",
            Self::Alarm => "alarm",
        }
    }
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TickReport {
    pub source: TickSource,
    pub at_ms: i64,
    pub outcome: &'static str,
    pub error: Option<String>,
}

pub(crate) fn now_ms() -> i64 {
    js_sys::Date::now() as i64
}

fn budget_at(deadline: i64, now: i64, limit: u64) -> u64 {
    (deadline.saturating_sub(now).max(0) as u64).min(limit)
}

pub(crate) async fn run_tick(env: &Env, source: TickSource) -> TickReport {
    let mut report = TickReport {
        source,
        at_ms: now_ms(),
        outcome: "failed",
        error: None,
    };
    // Two independent random mantissas distinguish invocations in one ms,
    // including overlapping invocations of the same trigger.
    let holder = format!(
        "{}:{}:{:x}:{:x}",
        source.as_str(),
        report.at_ms,
        js_sys::Math::random().to_bits(),
        js_sys::Math::random().to_bits()
    );
    let db = match env.d1("OVERLAY_DB") {
        Ok(db) => db,
        Err(e) => {
            report.error = Some(e.to_string());
            worker::console_log!("tick {} failed: D1 binding: {e}", source.as_str());
            return report;
        }
    };
    let lease = match crate::tick_ops::take(&db, &holder, report.at_ms).await {
        Ok(lease) => lease,
        Err(e) => {
            worker::console_log!("tick {} failed: lease: {e}", source.as_str());
            report.error = Some(e);
            return report;
        }
    };
    if lease.holder != holder {
        worker::console_log!(
            "tick skipped: leased by {} until {}",
            lease.holder,
            lease.until_ms
        );
        report.outcome = "skipped";
        crate::tick_ops::record_attempt(&db, &holder, source, report.at_ms, true).await;
        return report;
    }

    let deadline = lease.until_ms as i64 - TICK_LEASE_MARGIN_MS;
    let setup = race_or_deadline(
        async {
            ensure_overlay_migrations(&db).await?;
            crate::tick_ops::record_attempt(&db, &holder, source, report.at_ms, false).await;
            crate::tick_ops::record_heartbeat(&db, &report).await;
            Ok::<_, String>(())
        },
        crate::broadcaster::sleep_ms(budget_at(deadline, now_ms(), TICK_BUDGET_MS as u64)),
    )
    .await;
    let result = match setup {
        Some(Ok(())) => run_steps(env, deadline).await,
        Some(Err(e)) => Err(e),
        None => Err("tick setup exceeded its budget".to_string()),
    };
    // Both triggers drain the same notifications before releasing the lease.
    // Preserve their own bounded delivery policy, including after a timeout.
    let mut notifications = Vec::new();
    crate::pot_changes::flush(env, |fut| notifications.push(fut));
    crate::lobby_changes::flush(env, |fut| notifications.push(fut));
    let _ = race_or_deadline(
        async {
            for notification in notifications {
                notification.await;
            }
        },
        crate::broadcaster::sleep_ms(budget_at(lease.until_ms as i64, now_ms(), 30_000)),
    )
    .await;
    report.outcome = if result.is_ok() {
        "completed"
    } else {
        "failed"
    };
    report.error = result.err();
    if let Err(e) = crate::tick_ops::release(&db, &holder).await {
        worker::console_log!("tick lease release failed; expiry remains in force: {e}");
        report.outcome = "failed";
        report.error = Some(e);
    }
    worker::console_log!(
        "tick {} {}: {:?}",
        source.as_str(),
        report.outcome,
        report.error
    );
    report
}

async fn run_steps(env: &Env, deadline: i64) -> Result<(), String> {
    if now_ms() >= deadline {
        return Err("tick exceeded its budget before the maintenance steps".to_string());
    }
    worker::console_log!("Scheduled event triggered");

    // Initialize D1 and run migrations
    let db = match env.d1("OVERLAY_DB") {
        Ok(db) => Rc::new(db),
        Err(e) => {
            worker::console_log!("Scheduled: D1 binding error: {}", e);
            return Err(e.to_string());
        }
    };

    if let Err(e) = ensure_overlay_migrations(&db).await {
        worker::console_log!("Scheduled: Migration error: {}", e);
        return Err(e.to_string());
    }

    // Build shared storage + engine
    let ship_storage: Rc<dyn SHIPStorage> = Rc::new(D1SHIPStorage::new(db.clone()));
    let slap_storage: Rc<dyn SLAPStorage> = Rc::new(D1SLAPStorage::new(db.clone()));
    let agent_storage: Rc<dyn AgentStorage> = Rc::new(D1AgentStorage::new(db.clone()));
    let dm_delegation_storage: Rc<dyn DmDelegationStorage> =
        Rc::new(D1DmDelegationStorage::new(db.clone()));
    let uhrp_storage: Rc<dyn UHRPStorage> = Rc::new(D1UHRPStorage::new(db.clone()));
    let low_storage: Rc<dyn LowStorage> = Rc::new(D1LowStorage::new(db.clone()));
    let reveal_storage: Rc<dyn RevealStorage> = Rc::new(D1RevealStorage::new(db.clone()));
    let pot_storage: Rc<dyn PotStorage> = Rc::new(D1PotStorage::new(db.clone()));
    let collected_storage: Rc<dyn CollectedStorage> = Rc::new(D1CollectedStorage::new(db.clone()));
    let hand_storage: Rc<dyn HandStorage> = Rc::new(D1HandStorage::new(db.clone()));
    let result_storage: Rc<dyn ResultStorage> = Rc::new(D1ResultStorage::new(db.clone()));
    let proof_storage: Rc<dyn ProofStorage> = Rc::new(D1ProofStorage::new(db.clone()));
    let potparty_storage: Rc<dyn PotpartyStorage> = Rc::new(D1PotpartyStorage::new(db.clone()));
    let potrefund_storage: Rc<dyn PotrefundStorage> = Rc::new(D1PotrefundStorage::new(db.clone()));
    let hopparty_storage: Rc<dyn HoppartyStorage> = Rc::new(D1HoppartyStorage::new(db.clone()));
    // Keep a DB handle for the observability writes (#192/#193, P4) ; the
    // engine build below consumes `db`.
    let ops_db = db.clone();
    let mut engine = build_engine_with_storage(
        db,
        env,
        ship_storage.clone(),
        slap_storage.clone(),
        agent_storage.clone(),
        dm_delegation_storage.clone(),
        uhrp_storage.clone(),
        low_storage.clone(),
        reveal_storage.clone(),
        pot_storage.clone(),
        collected_storage.clone(),
        hand_storage.clone(),
        result_storage.clone(),
        proof_storage.clone(),
        potparty_storage.clone(),
        potrefund_storage.clone(),
        hopparty_storage.clone(),
    );

    // The outer deadline uses the same write gate as GASP. A submit in
    // progress finishes its error handling; each of its remaining storage
    // calls and hooks receives only the remaining tick budget.
    engine.set_finalize_submit_budget(
        Rc::new(move |ms| {
            let remaining = budget_at(deadline, now_ms(), ms);
            Box::pin(async move {
                if remaining > 0 {
                    crate::broadcaster::sleep_ms(remaining).await;
                }
            }) as overlay_engine::engine::SleepFuture
        }),
        GASP_FINALIZE_SUBMIT_BUDGET_MS,
    );
    let result = overlay_engine::gasp::race_or_deadline_guarded(async {

    crate::proof_window::run_pass(&engine, &ops_db).await;

    // Give stored proof completion its own early slot. Held GASP graphs are
    // not admitted transactions: their roots use hydrate_root during sync.
    // A timeout stamps no success heartbeat; the next tick retries the rows.
    let completion_fetcher = transaction_proof_fetcher(env);
    let backstop_age = crate::proof_fetcher::PUSH_BACKSTOP_MIN_AGE_SECS;
    let (tx_completed, tx_fetch_failed) = match race_or_deadline(
        engine.complete_missing_proofs_with(&completion_fetcher, TRANSACTION_PROOF_COMPLETION_LIMIT, backstop_age),
        crate::broadcaster::sleep_ms(TRANSACTION_PROOF_COMPLETION_BUDGET_MS),
    )
    .await
    {
        Some(Ok(summary)) => {
            worker::console_log!("Scheduled: transaction proof completion scanned={} completed={} unconfirmed={} stitch_failed={}",
                summary.scanned, summary.completed, summary.still_unconfirmed, summary.stitch_failed);
            crate::ops::record_completion_tick(
                &ops_db,
                summary.completed as u64,
                summary.fetch_failed as u64,
                0,
                0,
            )
            .await;
            (summary.completed as u64, summary.fetch_failed as u64)
        }
        Some(Err(e)) => {
            worker::console_log!("Scheduled: transaction proof completion failed: {e}");
            (0, 0)
        }
        None => {
            worker::console_log!("Scheduled: transaction proof completion exceeded {TRANSACTION_PROOF_COMPLETION_BUDGET_MS} ms; retry next tick");
            (0, 0)
        }
    };

    // Sync advertisements (if advertiser + hosting URL are configured).
    // Publishes any new SHIP/SLAP ads on-chain so peers can discover us.
    match engine.sync_advertisements().await {
        Ok(report) if report.effective() => {
            worker::console_log!("Scheduled: Ad sync ok: {report:?}");
        }
        Ok(report) => {
            // bsv-low #320 defect 3a/M1 ; surface, never swallow (incl. the
            // zero-admit refusal shape).
            worker::console_log!("Scheduled: Ad sync completed WITH FAILURES: {report:?}");
        }
        Err(e) => {
            worker::console_log!("Scheduled: Ad sync error: {}", e);
        }
    }

    // GASP sync with discovered peers. For each topic configured in
    // `sync_configuration`, start_gasp_sync discovers peers (via SHIP
    // lookup of other overlays carrying the topic) and exchanges UTXOs
    // ; we pull their records into our D1 and, symmetrically, they pull
    // ours. This is how a UHRP advert published on bsvb.tech ends up
    // queryable on rust-overlay and vice versa.
    //
    // If no `sync_configuration` is set in EngineConfig (the current
    // default), GASP sync is a near-no-op: `start_gasp_sync` iterates
    // configured topics only. That's fine ; calling it keeps the wire
    // connected so adding topic peers later Just Works.
    // bsv-low #585 (door 3's fold): one bounded pass of the orphan sweep over
    // the queue's R2 objects (`beef_blob_sweep.rs`: at most 200 listed and 50
    // deleted, the cursor at rest in D1). Before the GASP step, so a tick
    // that step holds to its belt has swept. A dropped pass saved no cursor
    // and is made again. `run_pass` is the bounded pass the operator's lever
    // (`POST /internal/beef-blob-sweep`) runs too (the d3 fold-4).
    crate::beef_blob_sweep::run_pass(env, &ops_db, "tick").await;
    // bsv-low #555 (the lens fold's M3): rows of deferred graphs nothing
    // will resume (a peer that never finishes a sync again) are swept initial.
    crate::gasp_deferred::sweep_stale(&ops_db).await;
    // Engine 0.1.8: the remembered expired roots past their keep (hourly,
    // past their row bound too).
    crate::gasp_deferred::sweep_expired_outpoints(&ops_db, now_ms() as f64).await;
    // NL-6c: deferred EF jobs whose run ended without settling go back to the
    // queue; settled ones past their keep are swept with their bytes.
    crate::ef_deferred::redrive(env, &ops_db).await;
    // N5 (LOW's E585 land2 lens): one bounded pass over the deferred EF jobs'
    // R2 objects (`ef-deferred/`), counted for the health figure; an object no
    // job row names, past the keep, is deleted (L1 (b)).
    crate::beef_blob_sweep::run_ef_deferred_pass(env, &ops_db).await;
    // bsv-low #649 M1: the pot and hop notes a background flush withheld (too little of its `waitUntil` window
    // left) are parked in D1; every tick delivers the oldest of them and deletes only what the app layer accepted.
    // D3-H1, D4-L1: this `*/15` tick is the observed trigger, and `/admin/complete-proofs` (the authenticated admin
    // path) runs the same drain when invoked. An eligible fresh parked note within the next 128 rows of its set
    // reaches the app layer within one scheduled tick, about 16 minutes in the normal worst case, plus the app
    // layer's own delivery. The authenticated admin maintenance call can also drain it when invoked.
    crate::change_park::drain_parked(env, &ops_db, crate::change_park::DrainTrigger::Tick).await;
    // The order (bsv-low #585 land2): the sweep's pass (its own 30 s race),
    // the stale graphs, the EF jobs' hand-back and keep sweep (no race of its
    // own: two D1 reads, up to 20 quiet jobs re-sent and touched, up to 50
    // settled jobs deleted with their bytes; the jobs RUN on the queue
    // consumer), the EF jobs' objects' pass (its own 30 s race, N5), then
    // the GASP step under its 240 s belt, which bounds that step alone.
    // #257: BOUNDED ; an unbounded GASP sync (dead SHIP-discovered peers, no
    // per-fetch timeout) hung every cron to the 15-min kill since deploy day;
    // see the budget consts. A timeout drops the sync (cursors persist only
    // per completed peer ; idempotent redo next tick) and the tick moves on.
    // The race is the GUARDED one over the engine's finalize gate (bsv-low
    // #552, the lens fold's HIGH-1): this belt can fall due while a peer's
    // graph is being submitted, and a submit dropped between its writes
    // leaves a head chain with no head. It waits for the transaction being
    // written and drops the sync at the boundary.
    match overlay_engine::gasp::race_or_deadline_guarded(
        engine.start_gasp_sync(),
        crate::broadcaster::sleep_ms(GASP_SYNC_BUDGET_MS),
        engine.finalize_submit_gate(),
    )
    .await
    {
        None => worker::console_log!(
            "Scheduled: GASP sync EXCEEDED its {GASP_SYNC_BUDGET_MS} ms budget ; dropped (bsv-low#257); continuing the tick"
        ),
        Some(Ok(r)) => {
            let total_peers: usize = r.topics_synced.values().map(|t| t.peers.len()).sum();
            let total_errors: usize = r.topics_synced.values().map(|t| t.errors.len()).sum();
            // D8 decoy rule: manager-named inputs a peer answered it does
            // not hold, pruned from their graphs. Counted apart from errors,
            // per distinct outpoint per peer: one decoy seen through two
            // peers reads 2.
            let total_pruned: u64 = r.topics_synced.values().map(|t| t.pruned_inputs).sum();
            // bsv-low #551: graphs the anchor check refused and discarded
            // whole (a root that does not verify, or one its topic manager
            // does not admit at the end of the replay). Not errors.
            let total_discarded: u64 = r
                .topics_synced
                .values()
                .map(|t| t.discarded_graphs)
                .sum();
            // bsv-low #555: graphs deferred past their per-graph budget (their
            // walk kept), resumed from a record, converged, and records
            // dropped (by reason in the counters and the per-topic line).
            let sum = |f: fn(&overlay_engine::engine::TopicSyncResult) -> u64| -> u64 {
                r.topics_synced.values().map(f).sum()
            };
            worker::console_log!(
                "Scheduled: GASP sync: topics={} peers={} errors={} pruned_inputs={} discarded_graphs={} anchor_walk_held_graphs={} deferred_graphs={} resumed_graphs={} converged_graphs={} dropped_graphs={} stalled_graphs={} held_back_graphs={} unanchorable_graphs={} unanchorable_cleared={} strikes_refused={} backed_off_graphs={} waiting_graphs={} idle_counted={}",
                r.topics_synced.len(),
                total_peers,
                total_errors,
                total_pruned,
                total_discarded,
                sum(|t| t.anchor_walk_held_graphs),
                sum(|t| t.deferred_graphs),
                sum(|t| t.resumed_graphs),
                sum(|t| t.converged_graphs),
                sum(|t| t.dropped_graphs.len() as u64),
                sum(|t| t.stalled_graphs),
                sum(|t| t.held_back_graphs),
                sum(|t| t.unanchorable_graphs),
                sum(|t| t.unanchorable_cleared),
                // Engine 0.1.10: a strike not written is a fact on the line,
                // and the held records the idle backoff sent away or kept.
                sum(|t| t.strikes_refused),
                sum(|t| t.backed_off_graphs),
                sum(|t| t.waiting_graphs),
                // Engine 0.1.11: the held records whose resume counted an
                // idle pass, so a captain reads the idle rule firing.
                sum(|t| t.idle_counted)
            );
            crate::gasp_deferred::record_counters(&ops_db, &r.topics_synced).await;
            // bsv-low #552: what the tick got done under the per-peer
            // budget. Graphs are submitted as they finalize, so a peer
            // dropped at its deadline still counts the ones it finished;
            // deadline_dropped_graphs are the ones in flight (nothing of them
            // admitted, walked again next tick); cursor_moves the persisted
            // cursors that advanced (past completed UTXOs only).
            let total_finalized: u64 = r
                .topics_synced
                .values()
                .map(|t| t.finalized_graphs)
                .sum();
            let total_deadline_dropped: u64 = r
                .topics_synced
                .values()
                .map(|t| t.deadline_dropped_graphs)
                .sum();
            let total_cursor_moves: usize =
                r.topics_synced.values().map(|t| t.cursor_moves.len()).sum();
            worker::console_log!(
                "Scheduled: GASP sync finalized_graphs={} deadline_dropped_graphs={} cursor_moves={}",
                total_finalized,
                total_deadline_dropped,
                total_cursor_moves
            );
            for (topic, res) in &r.topics_synced {
                // Engine 0.1.9: every root a peer's sync dropped, once a
                // run, with its fault class; `cursor_held_by` names only what
                // is neither finished nor held, so the two together say why
                // the cursor stopped or moved.
                worker::console_log!(
                    "Scheduled GASP topic={} gaspRootsProven={} gaspRootsUnproven={} cursor_held_by={:?} faulted={:?}",
                    topic,
                    res.roots_proven,
                    res.roots_unproven,
                    res.cursor_held_by,
                    faulted_summary(&res.faulted_roots)
                );
                if !res.errors.is_empty()
                    || res.pruned_inputs > 0
                    || res.discarded_graphs > 0
                    || res.anchor_walk_held_graphs > 0
                    || res.finalized_graphs > 0
                    || res.deadline_dropped_graphs > 0
                    || !res.cursor_moves.is_empty()
                    || res.deferred_graphs > 0
                    || res.resumed_graphs > 0
                    || !res.dropped_graphs.is_empty()
                    || res.stalled_graphs > 0
                    || res.held_back_graphs > 0
                {
                    let cursors: Vec<String> = res
                        .cursor_moves
                        .iter()
                        .map(|m| format!("{} {}->{}", m.peer, m.from, m.to))
                        .collect();
                    let dropped: Vec<String> = res
                        .dropped_graphs
                        .iter()
                        .map(|d| format!("{} {}", d.outpoint, d.reason))
                        .collect();
                    worker::console_log!(
                        "  Scheduled GASP topic={} sync_type={} pruned_inputs={} discarded_graphs={} anchor_walk_held_graphs={} finalized_graphs={} deadline_dropped_graphs={} deferred_graphs={} resumed_graphs={} converged_graphs={} stalled_graphs={} held_back_graphs={} dropped={:?} cursors={:?} errors={:?}",
                        topic,
                        res.sync_type,
                        res.pruned_inputs,
                        res.discarded_graphs,
                        res.anchor_walk_held_graphs,
                        res.finalized_graphs,
                        res.deadline_dropped_graphs,
                        res.deferred_graphs,
                        res.resumed_graphs,
                        res.converged_graphs,
                        res.stalled_graphs,
                        res.held_back_graphs,
                        dropped,
                        cursors,
                        res.errors
                    );
                }
            }
        }
        Some(Err(e)) => worker::console_log!("Scheduled: GASP sync error: {}", e),
    }

    // Peer crawl: bridge for non-GASP peers (bsvb today). `/lookup` +
    // `/submit` instead of `/requestSyncResponse`. Engine's tm_X
    // is_dupe check makes this idempotent ; crawling the same peer
    // twice in a row costs compute but admits nothing new.
    let peers = non_gasp_peers();
    // #257: bounded like GASP sync ; crawl submits are dupe-checked, so a
    // dropped crawl re-does nothing next tick.
    // E592 NOTE-3: each walk that could not run is counted inside the crawl, as its submit returns, so a crawl
    // dropped at its budget keeps the counts of what it finished.
    let crawl_db = env.d1("OVERLAY_DB").ok();
    match race_or_deadline(
        peer_crawler::crawl_peers(&engine, &peers, "cron", crawl_db.as_ref()),
        crate::broadcaster::sleep_ms(PEER_CRAWL_BUDGET_MS),
    )
    .await
    {
        None => worker::console_log!(
            "Scheduled: peer-crawl EXCEEDED its {PEER_CRAWL_BUDGET_MS} ms budget ; dropped (bsv-low#257); continuing the tick"
        ),
        Some(crawl_result) => {
            let total_attempted: usize = crawl_result.attempted.values().sum();
            let total_admitted: usize = crawl_result.admitted_by.values().sum();
            worker::console_log!(
                "Scheduled: peer-crawl ; peers={} attempted={} admitted={} peer_errors={}",
                peers.len(),
                total_attempted,
                total_admitted,
                crawl_result.peer_errors.len(),
            );
            for (k, errs) in &crawl_result.errors {
                if !errs.is_empty() {
                    worker::console_log!(
                        "  Scheduled peer-crawl {k}: {} submit-errors (initial: {})",
                        errs.len(),
                        errs.iter().next().map(String::as_str).unwrap_or("")
                    );
                }
            }
            for (k, e) in &crawl_result.peer_errors {
                if !crate::peer_crawler::is_lookup_400(e) {
                    worker::console_log!("  Scheduled peer-crawl {k}: lookup failed: {e}");
                }
            }
        }
    }

    // BEEF proof completion continues with the LOW maintenance passes;
    // transaction completion already ran in its bounded early slot.
    // 2+3. LOW pot-store maintenance, ORDER LOAD-BEARING (bsv-low#304 gate
    //    M-5): the #186 spend-confirmation chaser (the small, money-relevant
    //    CREDIT ANCHOR) runs BEFORE the pot-beef proof/bulk-drain pass so a
    //    subrequest-wall starvation can never queue the anchor behind the
    //    drain. The order is encoded ONCE in `run_pot_maintenance` (shared
    //    with /admin/complete-proofs). Each pass keeps its own fetcher (own
    //    budget cell). The chaser upgrades a 0-conf pot spend to
    //    spentConfirmed = 1 ONLY once the SPENDING tx's bump verifies against
    //    chaintracks ; fail-closed, never downgrades. The pot-beef pass
    //    fetches → verifies → stitches → trims → compacts each
    //    not-yet-verified pot BEEF; its candidate page is
    //    POT_PROOF_PASS_LIMIT (drain + op math at the const), courier
    //    traffic independently bounded by its fetcher budget.
    // bsv-low M19B-G1: the Arcade reorg-event consumer's cron leg (the
    // block-event pass is its fast path; this is the drought fallback and
    // the initial catch-up), BEFORE the confirmation chaser so a row it
    // demotes is re-chased in this same tick. Bounded on its own budget.
    let _arcade = crate::tip_pass::run_arcade_reorg_pass(
        env,
        pot_storage.as_ref(),
        Some(&ops_db),
        "cron",
        crate::arcade_reorg::PassLimits::cron(),
        &mut crate::reorg_sweep::RootMemo::default(),
        false,
    )
    .await;
    let spend_fetcher = courier_fetcher(env, lookup_service_chain_tracker(env)).with_budget(20);
    let pot_tracker = lookup_service_chain_tracker(env);
    let pot_fetcher = courier_fetcher(env, pot_tracker);
    let (spend_summary, pot_summary) = crate::proof_fetcher::run_pot_maintenance(
        pot_storage.as_ref(),
        &spend_fetcher,
        20,
        &pot_fetcher,
        crate::proof_fetcher::POT_PROOF_PASS_LIMIT,
        backstop_age,
    )
    .await;
    worker::console_log!(
        "Scheduled: spend-confirmation (pot_records) ; scanned={} confirmed={} \
         still_unconfirmed={} fetch_failed={} tracker_faults={} cas_missed={} cas_errors={} \
         displaced={} displace_attempts={} displace_faults={}",
        spend_summary.scanned,
        spend_summary.confirmed,
        spend_summary.still_unconfirmed,
        spend_summary.fetch_failed,
        spend_summary.tracker_faults,
        spend_summary.cas_missed,
        spend_summary.cas_errors,
        spend_summary.displaced,
        spend_summary.displace_attempts,
        spend_summary.displace_faults,
    );
    // bsv-low W4 (2026-09-04): spends the index never saw ; bounded, oldest
    // initial, on a fetcher of its OWN (the initial tick on beta shared the
    // confirmation pass's 20-call budget and faulted 11 of 20 candidates on
    // "budget exhausted"): up to 4 courier calls per candidate, never a call
    // taken from the money-relevant confirmation pass.
    let discovery_fetcher = courier_fetcher(env, lookup_service_chain_tracker(env))
        .with_budget(crate::proof_fetcher::MISSING_SPEND_FETCH_BUDGET);
    let missing = crate::proof_fetcher::discover_missing_spends(
        pot_storage.as_ref(),
        &discovery_fetcher,
        crate::proof_fetcher::MISSING_SPEND_PASS_LIMIT,
        crate::proof_fetcher::MISSING_SPEND_MIN_AGE_SECS,
    )
    .await;
    worker::console_log!(
        "Scheduled: missing-spend discovery (pot_records) ; scanned={} discovered={} by_script={} no_hint={} unbound={} faults={} write_errors={} hop_rows={} new_era={} couriers={:?}",
        missing.scanned,
        missing.discovered,
        missing.by_script,
        missing.no_hint,
        missing.unbound,
        missing.faults,
        missing.write_errors,
        missing.hop_rows,
        missing.discovered_new_era,
        discovery_fetcher.rung_stats()
    );
    // 2026-09-04: each rung's ok/fault/skipped this tick → the lifetime
    // `courier_<rung>_*_total` counters (`/health/invariants`, the monitor).
    crate::ops::record_courier_tallies(&ops_db, &discovery_fetcher.rung_stats()).await;
    // 2026-09-04: the pass's lifetime counters ride `/health/invariants`
    // (`counters.missing_spend_*`) so the backlog's drain, the couriers'
    // faults and ; the paged one ; NEW-era discoveries are observable without
    // a laptop tail. Additive upserts; a never-run counter reads 0 there.
    crate::ops::record_missing_spend_pass(
        &ops_db,
        missing.scanned as u64,
        missing.discovered as u64,
        missing.faults as u64,
        missing.discovered_new_era as u64,
    )
    .await;
    worker::console_log!(
        "Scheduled: proof-completion (pot_beefs) ; scanned={} completed={} already_proven={} \
         still_unconfirmed={} fetch_failed={} stitch_failed={}",
        pot_summary.scanned,
        pot_summary.completed,
        pot_summary.already_proven,
        pot_summary.still_unconfirmed,
        pot_summary.fetch_failed,
        pot_summary.stitch_failed,
    );

    // 4. LOW `pot_records` decoded-params lazy backfill (#284): decode the
    //    covenant params / lock kind / verdict for pre-migration rows from
    //    the durable pot_beefs bytes. No courier, no tracker ; pure re-reads
    //    of our own admitted bytes; a row with a missing funding BEEF stays
    //    a candidate (bounded per tick, RANDOM-sampled).
    let backfill_summary = crate::proof_fetcher::backfill_decoded_params(
        pot_storage.as_ref(),
        crate::proof_fetcher::PARAMS_BACKFILL_LIMIT,
    )
    .await;
    worker::console_log!(
        "Scheduled: params-backfill (pot_records) ; scanned={} decoded={} verdicts={} \
         missing_beef={}",
        backfill_summary.scanned,
        backfill_summary.decoded,
        backfill_summary.verdicts,
        backfill_summary.missing_beef,
    );

    // 4a. bsv-low #406: settleSigners historic backfill ; attach WHO SIGNED
    //     to rows classified before #406 shipped, from the durable spender
    //     bytes. Pure local re-reads + ECDSA verifies; bounded, RANDOM-
    //     sampled; converges to zero candidates and stays there.
    let signers_summary = crate::proof_fetcher::backfill_settle_signers(
        pot_storage.as_ref(),
        crate::proof_fetcher::SETTLE_SIGNERS_BACKFILL_LIMIT,
    )
    .await;
    worker::console_log!(
        "Scheduled: signers-backfill (pot_records) ; scanned={} latched={} unresolved={} \
         missing_beef={}",
        signers_summary.scanned,
        signers_summary.latched,
        signers_summary.unresolved,
        signers_summary.missing_beef,
    );

    // 4a'. bsv-low #468: spender PAYOUTS historic backfill ; measure what
    //      each pre-#468 spend paid to the committed homes, from the durable
    //      spender bytes. Local re-reads only; bounded; converges to zero
    //      candidates (History's amount column reads the served number).
    let payouts_summary = crate::proof_fetcher::backfill_spender_payouts(
        pot_storage.as_ref(),
        crate::proof_fetcher::SPENDER_PAYOUTS_BACKFILL_LIMIT,
    )
    .await;
    worker::console_log!(
        "Scheduled: payouts-backfill (pot_records) ; scanned={} measured={} missing_beef={}",
        payouts_summary.scanned,
        payouts_summary.measured,
        payouts_summary.missing_beef,
    );

    // 4b. The RE-LATCH fixpoint over the two admission-latched verdict columns
    //     (bsv-low #355 potparty.sigValid + #367 hopparty.markerValid). Pure
    //     re-reads of our own rows ; no courier, no tracker, no BEEF parse ;
    //     and a bounded page per table per tick. This is the ONLY repair path
    //     either column has: a hopparty marker rides a transaction already on
    //     chain, so no republish can ever re-latch it, and a row a transient
    //     predicate fault latched 0 sorts below even the legacy tier forever.
    //     `changed`/`demoted` in the log lines are the regression detector.
    for summary in
        crate::relatch::run_relatch(ops_db.clone(), crate::relatch::RELATCH_PAGE_LIMIT).await
    {
        crate::relatch::log_relatch_summary(&summary);
    }

    // 4b. Terminal-retire classifier (INCIDENT D1-CALLBACK-FLOOD 2026-09-01).
    //    Runs BEFORE the rebroadcast backstop on purpose: a row it retires
    //    this tick (corroborated network-dead ; Arcade terminal verdict or
    //    48h+ absence, PLUS both indexers' definitive 404) must not be
    //    re-presented by the very next step. Bounded: ≤8 candidates per
    //    store, ≤3 courier GETs each; every uncertainty keeps (fail-safe).
    let arcade_base = env
        .var("ARCADE_URL")
        .ok()
        .map(|v| v.to_string())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| crate::broadcaster::ARCADE_DEFAULT_URL.to_string());
    // #602 fold (MEDIUM-2): the conflict pass's GETs ride the WoC key when one is installed.
    let woc_key = env.secret("WOC_API_KEY").ok().map(|k| k.to_string());
    let retire =
        crate::proof_fetcher::run_retire_pass(
        &ops_db,
        &arcade_base,
        woc_key.as_deref(),
        crate::proof_fetcher::ProofSources::from_env(env),
    )
    .await;
    worker::console_log!(
        "Scheduled: retire-pass ; scanned={} retired={} kept_present={} kept_uncertain={} conflict: scanned={} evicted={} faults={}",
        retire.scanned,
        retire.retired,
        retire.kept_present,
        retire.kept_uncertain,
        retire.conflict.scanned,
        retire.conflict.evicted,
        retire.conflict.faults,
    );

    // 5. Admitted-but-network-absent rebroadcast backstop (bsv-low #273,
    //    #267 item c). The passes above only help txs the network HOLDS; an
    //    admitted tx the network never accepted (the #267 incident class)
    //    never self-heals ; this pass presence-probes old proofless rows
    //    (Bitails + WoC) and rebroadcasts the stored BEEF in ancestry order
    //    when BOTH indexers definitively 404 it. Runs LAST, own bounds
    //    (16 candidates / 48 POSTs, 30min–14d candidacy bracket ; gate
    //    LOW-1), so it can never starve proof completion. Since the incident
    //    fix the attempts are RECORDED AND CAPPED (`run_rebroadcast_backstop`
    //    ; 3 lifetime attempts, spacing 1h/6h, dead-letter log at the cap).
    let tx_storage = D1Storage::new(ops_db.clone());
    let taal_key = env.secret("TAAL_API_KEY").ok().map(|s| s.to_string());
    let rb = crate::proof_fetcher::run_rebroadcast_backstop(
        &tx_storage,
        taal_key.as_deref(),
        None,
        crate::proof_fetcher::ProofSources::from_env(env),
    )
        .await;
    worker::console_log!(
        "Scheduled: rebroadcast-backstop (transactions) ; scanned={} present={} \
         inconclusive={} rebroadcast={} failed={} budget_skipped={} attempted={}",
        rb.scanned,
        rb.present,
        rb.inconclusive,
        rb.rebroadcast,
        rb.rebroadcast_failed,
        rb.budget_skipped,
        rb.attempted.len(),
    );

    // 6. LOW advert lifecycle (bsv-low#309): reap expired-but-unspent lobby
    //    adverts (fail-CLOSED ; a null tip reaps NOTHING) + spend-confirm a
    //    bounded advert-outpoint sample so a close that bypassed /submit
    //    (the direct-ARC fallback ; the #256 orphan generator) still leaves
    //    ls_low. Raced like the other network-bound steps; both passes are
    //    idempotent, so a dropped tick just retries. Also poke-able via
    //    POST /admin/advert-lifecycle (the cron-poker doctrine).
    let advert_tracker = lookup_service_chain_tracker(env);
    match race_or_deadline(
        async {
            let tip = crate::advert_lifecycle::resolve_tip(advert_tracker.as_deref()).await;
            crate::advert_lifecycle::run_advert_lifecycle(
                low_storage.as_ref(),
                tip,
                None,
                crate::advert_lifecycle::ADVERT_SPEND_CHECK_LIMIT,
                crate::advert_lifecycle::ADVERT_REAP_LIMIT,
                crate::proof_fetcher::ProofSources::from_env(env),
            )
            .await
        },
        crate::broadcaster::sleep_ms(ADVERT_LIFECYCLE_BUDGET_MS),
    )
    .await
    {
        None => worker::console_log!(
            "Scheduled: advert-lifecycle EXCEEDED its {ADVERT_LIFECYCLE_BUDGET_MS} ms budget ; dropped; continuing the tick"
        ),
        Some((reap, spend)) => {
            worker::console_log!(
                "Scheduled: advert-lifecycle (low_records) ; tip_resolved={} reap_scanned={} \
                 reaped={} reap_delete_failed={} spend_scanned={} spend_deleted={} \
                 not_spent={} unknown={} spend_delete_failed={}",
                reap.tip_resolved,
                reap.scanned,
                reap.reaped,
                reap.delete_failed,
                spend.scanned,
                spend.deleted,
                spend.not_spent,
                spend.unknown,
                spend.delete_failed,
            );
        }
    }

    // Add later LOW maintenance results without counting the early pass twice.
    let proofs_completed = tx_completed + pot_summary.completed as u64;
    let fetch_failed = tx_fetch_failed + pot_summary.fetch_failed as u64;
    let pot_beefs_compacted = pot_summary.completed as u64;
    // A displacement latches spentConfirmed the same as a confirm ; the ops
    // heartbeat counts both or displaced rows go invisible (2026-08-18).
    let spends_confirmed = (spend_summary.confirmed + spend_summary.displaced) as u64;
    crate::ops::record_completion_results(
        &ops_db,
        pot_summary.completed as u64,
        pot_summary.fetch_failed as u64,
        pot_beefs_compacted,
        spends_confirmed,
    )
    .await;
    let flagged = crate::ops::refresh_proofless_watch(&ops_db).await;
    worker::console_log!(
        "Scheduled: ops ; proofs_completed+={proofs_completed} fetch_failed+={fetch_failed} \
         pot_beefs_compacted+={pot_beefs_compacted} spends_confirmed+={spends_confirmed} \
         proofless_over_24h={flagged}"
    );

    // Run janitor health checks. #257: bounded ; the janitor serially
    // health-checks every SHIP/SLAP domain (including dead ephemeral hosts),
    // so once GASP sync stopped eating the whole budget this step would be
    // the next unbounded candidate. A dropped janitor just retries evictions
    // next tick.
    let janitor_config = overlay_engine::health_checker::JanitorConfig::default();
    let checker = WorkerHealthChecker;
    let hosting_url = env.var("HOSTING_URL").ok().map(|v| v.to_string());
    match race_or_deadline(
        janitor::run_janitor(
            ship_storage.as_ref(),
            slap_storage.as_ref(),
            &checker,
            &janitor_config,
            hosting_url.as_deref(),
        ),
        crate::broadcaster::sleep_ms(JANITOR_BUDGET_MS),
    )
    .await
    {
        None => worker::console_log!(
            "Scheduled: janitor EXCEEDED its {JANITOR_BUDGET_MS} ms budget ; dropped (bsv-low#257); continuing"
        ),
        Some(Ok(result)) => {
            worker::console_log!(
                "Scheduled: Janitor completed ; SHIP: {}, SLAP: {}, evicted: {}, healthy: {}, unhealthy: {}",
                result.ship_records_checked,
                result.slap_records_checked,
                result.records_evicted,
                result.domains_healthy,
                result.domains_unhealthy,
            );
        }
        Some(Err(e)) => {
            worker::console_log!("Scheduled: Janitor error: {}", e);
        }
    }

    worker::console_log!("Scheduled tasks completed");
    }, crate::broadcaster::sleep_ms(budget_at(deadline, now_ms(), TICK_BUDGET_MS as u64)),
    engine.finalize_submit_gate()).await;
    result.ok_or_else(|| "tick exceeded its total budget".to_string())
}

/// PURE: one item per root a sync dropped (engine 0.1.9), for the tick's
/// line: `<outpoint> <class> runs=<n>`, ` unanchorable` once the cursor
/// passes it, the reason its strike was refused (engine 0.1.10: ` no_room`
/// or ` store_fault`), then the peer.
pub(crate) fn faulted_summary(roots: &[overlay_engine::gasp::FaultedRoot]) -> Vec<String> {
    roots
        .iter()
        .map(|f| {
            let passed = if f.unanchorable { " unanchorable" } else { "" };
            let refused = if f.refused.is_empty() {
                String::new()
            } else {
                format!(" {}", f.refused)
            };
            format!(
                "{} {} runs={}{}{} {}",
                f.outpoint, f.class, f.faults, passed, refused, f.peer
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Engine 0.1.9: the tick's per-topic line names each dropped root with
    /// its class, its faulted runs and whether the cursor passes it.
    #[test]
    fn unanchorable_019_the_tick_line_names_each_dropped_root() {
        let root =
            |outpoint: &str, class: &str, faults, unanchorable| overlay_engine::gasp::FaultedRoot {
                peer: "https://peer.example".into(),
                outpoint: outpoint.into(),
                class: class.into(),
                faults,
                unanchorable,
                error: "no rung served".into(),
                refused: String::new(),
            };
        assert_eq!(
            faulted_summary(&[
                root("aa.0", "courier", 3, true),
                root("bb.1", "peer", 0, false),
            ]),
            vec![
                "aa.0 courier runs=3 unanchorable https://peer.example".to_string(),
                "bb.1 peer runs=0 https://peer.example".to_string(),
            ]
        );
        let src = include_str!("tick.rs");
        assert!(src.contains("cursor_held_by={:?} faulted={:?}"));
        assert!(src.contains("faulted_summary(&res.faulted_roots)"));
    }

    /// Engine 0.1.10: a root whose strike was refused carries why on the
    /// tick's per-topic line (`no_room`, `store_fault`), and the summary
    /// line counts the refused strikes and the idle backoff's records.
    #[test]
    fn room_0110_the_tick_names_a_refused_strike() {
        let root = |refused: &str| overlay_engine::gasp::FaultedRoot {
            peer: "https://peer.example".into(),
            outpoint: "aa.0".into(),
            class: "courier".into(),
            faults: 0,
            unanchorable: false,
            error: "no rung served".into(),
            refused: refused.into(),
        };
        assert_eq!(
            faulted_summary(&[
                root(overlay_engine::gasp::strike_refused::NO_ROOM),
                root(overlay_engine::gasp::strike_refused::STORE_FAULT),
                root(""),
            ]),
            vec![
                "aa.0 courier runs=0 no_room https://peer.example".to_string(),
                "aa.0 courier runs=0 store_fault https://peer.example".to_string(),
                "aa.0 courier runs=0 https://peer.example".to_string(),
            ]
        );
        let src = include_str!("tick.rs");
        assert!(src.contains(
            "unanchorable_cleared={} strikes_refused={} backed_off_graphs={} waiting_graphs={}"
        ));
        assert!(src.contains("sum(|t| t.strikes_refused)"));
    }

    /// Engine 0.1.11: the summary line counts the held records whose resume
    /// counted an idle pass (`idle_counted`), after the idle backoff's two.
    #[test]
    fn obligations_0111_the_tick_counts_idle_resumes() {
        let src = include_str!("tick.rs");
        assert!(src.contains("backed_off_graphs={} waiting_graphs={} idle_counted={}\","));
        assert!(src.contains("sum(|t| t.idle_counted)"));
    }

    #[test]
    fn total_budget_reserves_the_lease_margin_and_caps_each_write() {
        assert_eq!(TICK_LEASE_MS, TICK_BUDGET_MS + TICK_LEASE_MARGIN_MS);
        assert_eq!(budget_at(100, 0, 30), 30);
        assert_eq!(budget_at(100, 90, 30), 10);
        assert_eq!(budget_at(100, 100, 30), 0);
        assert_eq!(budget_at(100, 110, 30), 0);
    }

    #[test]
    fn maintenance_steps_keep_the_base_sequence() {
        let src = include_str!("tick.rs");
        let steps = src
            .split("async fn run_steps(")
            .nth(1)
            .unwrap()
            .split("#[cfg(test)]")
            .next()
            .unwrap();
        let mut after = 0;
        for step in [
            "ensure_overlay_migrations(&db)",
            "build_engine_with_storage(",
            "engine.complete_missing_proofs_with(",
            "engine.sync_advertisements()",
            "crate::beef_blob_sweep::run_pass(",
            "crate::gasp_deferred::sweep_stale(",
            "crate::ef_deferred::redrive(",
            "crate::beef_blob_sweep::run_ef_deferred_pass(",
            "engine.start_gasp_sync()",
            "peer_crawler::crawl_peers(",
            "crate::tip_pass::run_arcade_reorg_pass(",
            "crate::proof_fetcher::run_pot_maintenance(",
            "crate::proof_fetcher::discover_missing_spends(",
            "crate::proof_fetcher::backfill_decoded_params(",
            "crate::proof_fetcher::backfill_settle_signers(",
            "crate::proof_fetcher::backfill_spender_payouts(",
            "crate::relatch::run_relatch(",
            "crate::proof_fetcher::run_retire_pass(",
            "crate::proof_fetcher::run_rebroadcast_backstop(",
            "crate::advert_lifecycle::run_advert_lifecycle(",
            "crate::ops::record_completion_results(",
            "crate::ops::refresh_proofless_watch(",
            "janitor::run_janitor(",
        ] {
            after += steps[after..]
                .find(step)
                .unwrap_or_else(|| panic!("step missing or reordered: {step}"))
                + step.len();
        }
    }
}
