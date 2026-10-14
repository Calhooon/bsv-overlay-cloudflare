async fn minutes_unanchorable() -> UnanchorableFixture {
    let fixture = unanchorable_fixture();
    for _ in 0..crate::gasp::UNANCHORABLE_AFTER_FAULTED_RUNS {
        fixture.engine.start_gasp_sync().await.unwrap();
    }
    fixture
}

async fn minutes_record(f: &UnanchorableFixture) -> crate::gasp::DeferredGraph {
    f.store
        .get_deferred_graph("https://budget.example", "tm_test", &f.root)
        .await
        .unwrap()
        .unwrap()
}

async fn minutes_exhaust(f: &UnanchorableFixture) {
    for _ in 0..crate::gasp::UNANCHORABLE_RETRIES {
        let mut record = minutes_record(f).await;
        record.retry_at = 0;
        f.store.put_deferred_graph(&record).await.unwrap();
        f.engine.start_gasp_sync().await.unwrap();
    }
}

#[tokio::test]
async fn minutes_0113_dead_after_four_retries_and_no_more_requests() {
    let f = minutes_unanchorable().await;
    let before = f.asked.get();
    minutes_exhaust(&f).await;
    assert_eq!(
        f.asked.get() - before,
        u64::from(crate::gasp::UNANCHORABLE_RETRIES)
    );
    let record = minutes_record(&f).await;
    assert_eq!(record.reason, "dead:courier");
    assert!(record.cursor_passable);
    let before = f.remote.asked.borrow().len();
    for _ in 0..3 {
        let result = f.engine.start_gasp_sync().await.unwrap();
        assert!(result.topics_synced["tm_test"].faulted_roots.is_empty());
    }
    assert_eq!(f.remote.asked.borrow().len(), before);
}

#[tokio::test]
async fn minutes_0113_new_score_revives_a_dead_root() {
    let mut f = minutes_unanchorable().await;
    minutes_exhaust(&f).await;
    assert_eq!(minutes_record(&f).await.reason, "dead:courier");
    f.serve.set(true);
    std::rc::Rc::make_mut(&mut f.remote.outputs)[0].score = 40.0;
    f.engine.set_gasp_remote_factory(Box::new(f.remote.clone()));
    let result = f.engine.start_gasp_sync().await.unwrap();
    assert_eq!(result.topics_synced["tm_test"].finalized_graphs, 1);
    assert!(f
        .store
        .get_deferred_graph("https://budget.example", "tm_test", &f.root)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn minutes_0113_expiry_sweeps_unanchorable_without_fetching() {
    let f = minutes_unanchorable().await;
    let mut record = minutes_record(&f).await;
    record.expiry = Some(1);
    f.store.put_deferred_graph(&record).await.unwrap();
    let before = f.remote.asked.borrow().len();
    f.engine.start_gasp_sync().await.unwrap();
    assert_eq!(f.remote.asked.borrow().len(), before);
    assert!(f
        .store
        .get_deferred_graph("https://budget.example", "tm_test", &f.root)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn minutes_0113_deadline_retries_also_exhaust_and_name_dead_once() {
    let mut f = minutes_unanchorable().await;
    f.remote.node_cost_ms = 40_000;
    f.engine.set_gasp_remote_factory(Box::new(f.remote.clone()));
    for round in 1..=crate::gasp::UNANCHORABLE_RETRIES {
        let mut record = minutes_record(&f).await;
        record.retry_at = 0;
        f.store.put_deferred_graph(&record).await.unwrap();
        let result = f.engine.start_gasp_sync().await.unwrap();
        assert_eq!(minutes_record(&f).await.unanchorable_retries, round);
        let topic = &result.topics_synced["tm_test"];
        assert_eq!(
            topic.dead_graphs,
            u64::from(round == crate::gasp::UNANCHORABLE_RETRIES)
        );
        if topic.dead_graphs > 0 {
            assert_eq!(topic.faulted_roots.len(), 1);
            assert_eq!(topic.faulted_roots[0].class, "dead:courier");
        }
    }
    assert_eq!(minutes_record(&f).await.reason, "dead:courier");
}

#[tokio::test]
async fn minutes_0113_dead_expiry_and_legacy_memo_remove_only_expired_records() {
    for memo in [true, false] {
        let f = minutes_unanchorable().await;
        minutes_exhaust(&f).await;
        let mut record = minutes_record(&f).await;
        if memo {
            f.store
                .remember_expired_outpoint("tm_test", &f.root, 1)
                .await
                .unwrap();
        } else {
            record.expiry = Some(1);
            f.store.put_deferred_graph(&record).await.unwrap();
        }
        let before = f.remote.asked.borrow().len();
        let result = f.engine.start_gasp_sync().await.unwrap();
        assert_eq!(result.topics_synced["tm_test"].expired_swept, 1);
        assert_eq!(f.remote.asked.borrow().len(), before);
        assert!(f
            .store
            .get_deferred_graph("https://budget.example", "tm_test", &f.root)
            .await
            .unwrap()
            .is_none());
    }
}

fn minutes_backfill_request(budget: u64) -> crate::backfill::SyncRequest {
    crate::backfill::SyncRequest::parse(
        &serde_json::json!({
            "topics": ["tm_test"], "peers": ["https://budget.example"],
            "until": "tip", "budget_ms": budget
        })
        .to_string(),
    )
    .unwrap()
}

fn minutes_clock(engine: &mut Engine, remote: &BudgetPageRemote) -> crate::meter::LoadMeter {
    let elapsed = remote.elapsed.clone();
    let sleep: SleepFactory = std::rc::Rc::new(move |ms| {
        let elapsed = elapsed.clone();
        let until = elapsed.get() + ms;
        Box::pin(std::future::poll_fn(move |_| {
            if elapsed.get() >= until {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        }))
    });
    engine.set_peer_sync_budget(sleep.clone(), 30_000);
    engine.set_graph_budget(sleep, 100, 15_000);
    let elapsed = remote.elapsed.clone();
    crate::meter::LoadMeter::with_clock(std::rc::Rc::new(move || elapsed.get() as f64))
}

#[tokio::test]
async fn minutes_0113_backfill_three_pages_in_three_slices_with_speed() {
    let (mut engine, mut remote) = budget_page_engine(6);
    remote.clamp = Some(3);
    remote.node_cost_ms = 10;
    let meter = minutes_clock(&mut engine, &remote);
    engine.set_gasp_remote_factory(Box::new(remote.clone()));
    let result = engine
        .start_gasp_backfill_metered(&minutes_backfill_request(1000), &meter)
        .await
        .unwrap();
    assert!(result.reached_tip);
    assert!(result.continuation.is_none());
    assert_eq!(result.slices, 3);
    assert_eq!(result.sync.topics_synced["tm_test"].finalized_graphs, 6);
    assert_eq!(result.sync.cost.outputs_walked, 6);
    assert_eq!(result.sync.cost.ms, 60);
    assert_eq!(result.sync.cost.ms_per_1000_outputs, Some(10_000));
    let topic = &result.sync.topics_synced["tm_test"];
    let peer = topic.peer_results["https://budget.example"].cost;
    assert_eq!(
        (peer.ms, peer.outputs_walked, peer.ms_per_1000_outputs),
        (
            topic.cost.ms,
            topic.cost.outputs_walked,
            topic.cost.ms_per_1000_outputs
        )
    );
    assert_eq!(
        result.sync.cost.peer_requests,
        (remote.requests.borrow().len() + remote.asked.borrow().len()) as u64
    );
}

#[tokio::test]
async fn minutes_0113_backfill_budget_continues_from_durable_cursor() {
    let (mut engine, mut remote) = budget_page_engine(6);
    remote.clamp = Some(3);
    remote.node_cost_ms = 10;
    let meter = minutes_clock(&mut engine, &remote);
    engine.set_gasp_remote_factory(Box::new(remote.clone()));
    let partial = engine
        .start_gasp_backfill_metered(&minutes_backfill_request(25), &meter)
        .await
        .unwrap();
    assert!(!partial.reached_tip);
    assert_eq!(partial.stop_reason, "wall_budget");
    assert!(partial.sync.topics_synced["tm_test"].finalized_graphs < 6);
    let token = partial.continuation.unwrap();
    let held_cursor = engine
        .storage
        .get_last_interaction("https://budget.example", "tm_test")
        .await
        .unwrap();
    assert_eq!(
        crate::backfill::Continuation::decode(&token)
            .unwrap()
            .cursor,
        held_cursor
    );
    let mut request = minutes_backfill_request(1000);
    request.continuation = Some(token);
    let meter = minutes_clock(&mut engine, &remote);
    let resumed = engine
        .start_gasp_backfill_metered(&request, &meter)
        .await
        .unwrap();
    assert!(resumed.reached_tip);
    assert_eq!(
        partial.sync.topics_synced["tm_test"].finalized_graphs
            + resumed.sync.topics_synced["tm_test"].finalized_graphs,
        6
    );
}

#[tokio::test]
async fn minutes_0113_continuation_cannot_advance_storage() {
    let (mut engine, remote) = budget_page_engine(6);
    let meter = minutes_clock(&mut engine, &remote);
    engine.set_gasp_remote_factory(Box::new(remote.clone()));
    let mut request = minutes_backfill_request(1000);
    request.continuation = Some(
        crate::backfill::Continuation::new("tm_test", "https://budget.example", 1000).encode(),
    );
    assert!(engine
        .start_gasp_backfill_metered(&request, &meter)
        .await
        .is_err());
    assert!(remote.requests.borrow().is_empty());
}

#[test]
fn minutes_0113_request_shape_and_speed_arithmetic() {
    for body in ["{}", r#"{"topics":["tm_test"]}"#, r#"{"peers":[]}"#] {
        assert!(crate::backfill::SyncRequest::parse(body).is_ok());
    }
    for body in [
        r#"{"until":"tip"}"#,
        r#"{"budget_ms":1}"#,
        r#"{"until":"tip","topics":["a","b"],"peers":["p"],"budget_ms":100}"#,
        r#"{"until":"tip","topics":["a"],"peers":["p"],"budget_ms":0}"#,
        r#"{"until":"tip","topics":["a"],"peers":["p"],"budget_ms":1.5}"#,
        r#"{"until":"later"}"#,
        r#"{"unexpected":1}"#,
    ] {
        assert!(crate::backfill::SyncRequest::parse(body).is_err(), "{body}");
    }
    let mut request = minutes_backfill_request(100);
    request.continuation =
        Some(crate::backfill::Continuation::new("another", "https://budget.example", 0).encode());
    assert!(request.validate().is_err());
    let mut cost = crate::meter::SyncCost {
        ms: 250,
        outputs_walked: 4,
        ..Default::default()
    };
    cost.refresh_speed();
    assert_eq!(cost.ms_per_1000_outputs, Some(62_500));
    cost.add(crate::meter::SyncCost {
        ms: 50,
        outputs_walked: 6,
        ..Default::default()
    });
    assert_eq!(cost.ms_per_1000_outputs, Some(30_000));
    assert_eq!(crate::meter::SyncCost::default().ms_per_1000_outputs, None);
}

#[tokio::test]
async fn minutes_0113_absence_memo_spans_backfill_slices() {
    let mut f = unanchorable_fixture();
    let old = f.root.split('.').next().unwrap();
    let child = Transaction::from_hex(&f.remote.nodes[old].raw_tx).unwrap();
    let source = child.inputs[0].source_txid.as_ref().unwrap();
    let mut outputs = Vec::new();
    let mut nodes = HashMap::new();
    for n in 1..=3 {
        let mut tx = child_of(source, 0);
        tx.lock_time = n;
        let txid = tx.id();
        outputs.push(crate::types::GASPOutput {
            txid: txid.clone(),
            output_index: 0,
            score: f64::from(n * 10),
        });
        nodes.insert(
            txid,
            crate::types::GASPNode {
                graph_id: String::new(),
                raw_tx: tx.to_hex(),
                output_index: 0,
                proof: None,
                tx_metadata: None,
                output_metadata: None,
                inputs: None,
            },
        );
    }
    f.remote.outputs = std::rc::Rc::new(outputs);
    f.remote.nodes = std::rc::Rc::new(nodes);
    f.remote.clamp = Some(2);
    let calls = std::rc::Rc::new(Cell::new(0));
    f.engine
        .set_gasp_ancestor_fetcher(std::rc::Rc::new(LoadCourier {
            calls: calls.clone(),
            bounded: false,
        }));
    let meter = minutes_clock(&mut f.engine, &f.remote);
    f.engine.set_gasp_remote_factory(Box::new(f.remote));
    let result = f
        .engine
        .start_gasp_backfill_metered(&minutes_backfill_request(1000), &meter)
        .await
        .unwrap();
    assert!(result.reached_tip, "{result:?}");
    assert_eq!(calls.get(), 1);
    assert_eq!(result.sync.cost.courier_not_found_memo_hits, 2);
    assert_eq!(result.sync.topics_synced["tm_test"].unanchorable_graphs, 3);
    assert_eq!(result.sync.topics_synced["tm_test"].finalized_graphs, 0);
}

#[tokio::test]
async fn minutes_0113_request_allowance_yields_a_continuation() {
    let (mut engine, mut remote) = budget_page_engine(200);
    remote.node_cost_ms = 0;
    let meter = minutes_clock(&mut engine, &remote);
    engine.set_gasp_remote_factory(Box::new(remote.clone()));
    let result = engine
        .start_gasp_backfill_metered(&minutes_backfill_request(1000), &meter)
        .await
        .unwrap();
    assert!(!result.reached_tip);
    assert_eq!(result.stop_reason, "request_allowance");
    assert!(result.continuation.is_some());
    let done = result.sync.topics_synced["tm_test"].finalized_graphs;
    assert!(done > 0 && done < 200);
    let cursor = crate::backfill::Continuation::decode(result.continuation.as_ref().unwrap())
        .unwrap()
        .cursor;
    assert_eq!(cursor, done * 10);
}

#[tokio::test]
async fn minutes_0113_clamped_boundary_does_not_claim_tip() {
    let (mut engine, mut remote) = budget_page_engine(2);
    remote.clamp = Some(1);
    remote.node_cost_ms = 10;
    let meter = minutes_clock(&mut engine, &remote);
    engine.set_gasp_remote_factory(Box::new(remote.clone()));
    let result = engine
        .start_gasp_backfill_metered(&minutes_backfill_request(1000), &meter)
        .await
        .unwrap();
    assert!(!result.reached_tip);
    assert_eq!(result.stop_reason, "no_progress");
    assert_eq!(result.sync.topics_synced["tm_test"].finalized_graphs, 1);
    assert_eq!(
        crate::backfill::Continuation::decode(result.continuation.as_ref().unwrap())
            .unwrap()
            .cursor,
        10
    );
    assert_eq!(remote.requests.borrow().last().unwrap().since, 11);
}

#[tokio::test]
async fn minutes_0113_sql_allowance_counts_attempts_without_trait_calls() {
    let meter = crate::meter::LoadMeter::default();
    let control = crate::backfill::Control {
        meter: meter.clone(),
        budget_ms: u64::MAX,
    };
    meter
        .scope(async {
            crate::meter::d1_queries(crate::backfill::BACKFILL_D1_QUERIES - 1);
            assert_eq!(control.stop_reason(), None);
            crate::meter::d1_queries(1);
            assert_eq!(control.stop_reason(), Some("request_allowance"));
        })
        .await;
    assert_eq!(meter.snapshot().storage_reads, 0);
    assert_eq!(meter.snapshot().storage_writes, 0);
}

struct MinutesExpiry(MockTopicManager, u64);

#[async_trait(?Send)]
impl TopicManagerTrait for MinutesExpiry {
    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        previous_coins: &[u8],
        off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
        context: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        self.0
            .identify_admissible_outputs(tx, previous_coins, off_chain_values, mode, context)
            .await
    }
    async fn get_documentation(&self) -> String {
        self.0.get_documentation().await
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        self.0.get_metadata().await
    }
    fn sync_expiry(&self, _: &bsv_rs::script::LockingScript) -> Option<u64> {
        Some(self.1)
    }
}

#[tokio::test]
async fn minutes_0113_fault_retains_the_topic_managers_expiry() {
    let mut f = unanchorable_fixture();
    let expiry = (current_timestamp_ms() / 1000.0) as u64 + 86_400;
    f.engine.managers.insert(
        "tm_test".into(),
        Box::new(MinutesExpiry(MockTopicManager::admitting(vec![0]), expiry)),
    );
    for _ in 0..crate::gasp::UNANCHORABLE_AFTER_FAULTED_RUNS {
        f.engine.start_gasp_sync().await.unwrap();
    }
    let record = minutes_record(&f).await;
    assert!(record.is_unanchorable());
    assert_eq!(record.expiry, Some(expiry));
}

#[tokio::test]
async fn minutes_0113_one_reservation_per_retry_slot() {
    let f = minutes_unanchorable().await;
    let mut record = minutes_record(&f).await;
    record.retry_at = 0;
    f.store.put_deferred_graph(&record).await.unwrap();
    record.unanchorable_retries = 1;
    record.retry_at =
        (current_timestamp_ms() / 1000.0) as u64 + crate::gasp::UNANCHORABLE_RETRY_SECS;
    let a = f.store.reserve_unanchorable_retry(&record).await.unwrap();
    let b = f.store.reserve_unanchorable_retry(&record).await.unwrap();
    assert_eq!(
        u8::from(a) + u8::from(b),
        1,
        "two callers read the same prior count"
    );
}
