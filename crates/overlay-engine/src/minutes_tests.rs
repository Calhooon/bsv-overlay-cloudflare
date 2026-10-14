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
