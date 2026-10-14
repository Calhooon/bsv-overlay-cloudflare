fn memo_page(count: u32) -> (Engine, BudgetPageRemote, std::rc::Rc<MemoryStorage>) {
    let (mut engine, mut remote) = budget_page_engine(count);
    remote.node_cost_ms = 0;
    for node in std::rc::Rc::make_mut(&mut remote.nodes).values_mut() {
        node.proof = None;
    }
    engine.set_gasp_ancestor_fetcher(std::rc::Rc::new(LoadCourier {
        calls: std::rc::Rc::new(Cell::new(0)),
        bounded: false,
    }));
    let store = std::rc::Rc::new(MemoryStorage::new());
    engine.storage = Box::new(store.clone());
    virtual_peer_budget(&mut engine, &remote);
    engine.set_graph_budget(
        std::rc::Rc::new(|_| Box::pin(std::future::pending())),
        100,
        15_000,
    );
    engine.set_gasp_remote_factory(Box::new(remote.clone()));
    (engine, remote, store)
}

#[tokio::test]
async fn memo_0114_forty_absent_roots_pass_in_one_run() {
    let (engine, remote, store) = memo_page(40);
    let result = engine.start_gasp_sync().await.unwrap();
    let topic = &result.topics_synced["tm_test"];
    assert_eq!(topic.strikes_refused, 0, "{topic:?}");
    assert_eq!(topic.unanchorable_graphs, 40);
    assert_eq!(
        store
            .get_last_interaction("https://budget.example", "tm_test")
            .await
            .unwrap(),
        400
    );
    assert_eq!(store.fault_memos().len(), 40);
    assert!(store.deferred_graphs().is_empty());
    let requests = remote.asked.borrow().len();
    engine.start_gasp_sync().await.unwrap();
    assert_eq!(
        remote.asked.borrow().len(),
        requests,
        "remembered absence spends no node request"
    );
}

#[tokio::test]
async fn memo_0114_walk_admission_does_not_wait_for_a_push() {
    let f = unanchorable_fixture();
    f.serve.set(true);
    f.engine.start_gasp_sync().await.unwrap();
    let root = f.root.trim_end_matches(".0");
    let candidates = f
        .store
        .find_transactions_for_proof_check(100, 1800)
        .await
        .unwrap();
    assert!(
        candidates.iter().any(|c| c.txid == root),
        "a walk-admitted row has no callback coming"
    );
    let courier = StubFetcher {
        proof: Some(single_leaf_bump_hex(root, 901)),
        calls: Cell::new(0),
    };
    let result = f
        .engine
        .complete_missing_proofs_with(&courier, 100, 1800)
        .await
        .unwrap();
    assert!(result.early_gasp > 0);
    assert_eq!(result.completed, 1);
    assert_eq!(courier.calls.get(), 1);
    let stored = f
        .store
        .find_output(root, 0, Some("tm_test"), None, true)
        .await
        .unwrap()
        .unwrap();
    assert!(
        crate::beef_limits::own_proof(stored.beef.as_ref().unwrap(), root)
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn memo_0114_folds_legacy_facts_once_and_keeps_partial_graphs() {
    let (engine, remote, store) = memo_page(40);
    let later = (current_timestamp_ms() / 1000.0) as u64 + 3600;
    for output in remote.outputs.iter() {
        let mut r = bare_record(0, output.score as u64, "unanchorable:not_found", true);
        r.outpoint = format!("{}.0", output.txid);
        r.faults = 1;
        r.retry_at = later;
        r.unanchorable_retries = 2;
        store.seed_legacy_fault(r);
    }
    let result = engine.start_gasp_sync().await.unwrap();
    assert_eq!(result.topics_synced["tm_test"].memo_folded, 40);
    assert!(remote.asked.borrow().is_empty());
    assert_eq!(store.fault_memos().len(), 40);
    assert!(store
        .fault_memos()
        .iter()
        .all(|r| r.retries == 2 && r.next_at == later));
    assert!(store.deferred_graphs().is_empty());
    let result = engine.start_gasp_sync().await.unwrap();
    assert_eq!(result.topics_synced["tm_test"].memo_folded, 0);
    assert!(remote.asked.borrow().is_empty());
}

#[tokio::test]
async fn memo_0114_bound_evicts_old_facts_without_refusing_strikes() {
    let store = MemoryStorage::new();
    let limit = crate::fault_memo::FAULT_MEMO_MAX_ROWS;
    for n in 0..limit + 2 {
        let mut r = bare_record(n as u32, n as u64, "unanchorable:not_found", true);
        r.faults = 1;
        assert_eq!(
            store.put_deferred_graph(&r).await.unwrap(),
            crate::gasp::DeferredGraphSave::Saved
        );
    }
    assert_eq!(store.fault_memos().len(), limit);
    assert!(store.deferred_graphs().is_empty());
    let old = bare_record(0, 0, "", false);
    assert!(store
        .get_deferred_graph(&old.peer, &old.topic, &old.outpoint)
        .await
        .unwrap()
        .is_none());
    let due = store
        .read_fault_memos(&old.peer, &old.topic, None)
        .await
        .unwrap();
    assert_eq!(due.records.len(), crate::fault_memo::FAULT_MEMO_DUE_LIMIT);
}

#[tokio::test]
async fn memo_0114_full_partial_store_does_not_limit_fault_facts() {
    let (engine, _, store) = memo_page(40);
    store.set_deferred_graph_ceiling(Some(0));
    let result = engine.start_gasp_sync().await.unwrap();
    assert_eq!(result.topics_synced["tm_test"].unanchorable_graphs, 40);
    assert_eq!(result.topics_synced["tm_test"].strikes_refused, 0);
}
