// bsv-low #590: the per-graph TIME limb is charged per RESUME, and a node in
// flight finishes (the finish-the-node rule). Included in `engine::tests`.

/// `graphs` chains, each `depth` unproven links over a proven bottom, every
/// request `cost_ms` of the remote's virtual clock. The UTXO of chain `g` is
/// its top, score `(g + 1) * 10`. Answers each chain's txids, top first.
fn i590_chain_engine(
    graphs: u32,
    depth: u32,
    cost_ms: u64,
) -> (Engine, BudgetPageRemote, Vec<Vec<String>>) {
    let (mut engine, mut remote) = budget_page_engine(0);
    remote.node_cost_ms = cost_ms;
    let mut tracker = bsv_rs::transaction::MockChainTracker::new(1000);
    let mut outputs = Vec::new();
    let mut nodes = HashMap::new();
    let mut chains = Vec::new();
    for g in 0..graphs {
        let bottom = child_of(&format!("{:064x}", 0x590_0000 + g), 0);
        tracker.add_root(100 + g, bottom.id());
        let mut chain = vec![bottom];
        for _ in 0..depth {
            let next = child_of(&chain.last().unwrap().id(), 0);
            chain.push(next);
        }
        for (i, tx) in chain.iter().enumerate() {
            nodes.insert(
                tx.id(),
                crate::types::GASPNode {
                    graph_id: String::new(),
                    raw_tx: tx.to_hex(),
                    output_index: 0,
                    proof: (i == 0).then(|| single_leaf_bump_hex(&tx.id(), 100 + g)),
                    tx_metadata: None,
                    output_metadata: None,
                    inputs: None,
                },
            );
        }
        outputs.push(crate::types::GASPOutput {
            txid: chain.last().unwrap().id(),
            output_index: 0,
            score: f64::from((g + 1) * 10),
        });
        chains.push(chain.iter().rev().map(Transaction::id).collect());
    }
    engine.chain_tracker = Some(Box::new(tracker));
    remote.outputs = std::rc::Rc::new(outputs);
    remote.nodes = std::rc::Rc::new(nodes);
    (engine, remote, chains)
}

/// A sleep factory over the remote's virtual clock.
fn i590_virtual_sleep(remote: &BudgetPageRemote) -> SleepFactory {
    let clock = remote.elapsed.clone();
    std::rc::Rc::new(move |ms| {
        let clock = clock.clone();
        let due = clock.get() + ms;
        Box::pin(std::future::poll_fn(move |_| {
            if clock.get() >= due {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        }))
    })
}

async fn i590_record_nodes(engine: &Engine, outpoint: &str) -> Option<usize> {
    engine
        .storage()
        .get_deferred_graph("https://budget.example", "tm_test", outpoint)
        .await
        .unwrap()
        .map(|record| record.nodes.len())
}

/// A graph of 7 nodes (six unproven links over a proven bottom), every
/// request 2,000 ms, a slice of 5,000 ms. A step is started only inside the
/// slice and a started one FINISHES: a fresh pass makes requests at 0, 2 and
/// 4 s (3 nodes), a resume re-asks its unproven root (the #555 re-ask, one
/// call) then walks two more. So the record holds 3 nodes, then 5, and the
/// third pass lands the graph: 3 passes, each link fetched ONCE, the root
/// once a pass. RED on the base (65027163), where the slice raced the node
/// in flight: the request started at 4 s was cut at 5 s and fetched again
/// the next pass (the record held 2 nodes after the first pass).
#[tokio::test]
async fn i590_a_a_deep_graph_lands_in_ceil_passes_and_no_link_is_fetched_twice() {
    let (mut engine, remote, chains) = i590_chain_engine(1, 6, 2_000);
    let chain = &chains[0];
    let root = format!("{}.0", chain[0]);
    engine.set_graph_budget(i590_virtual_sleep(&remote), 100, 5_000);
    engine.set_gasp_remote_factory(Box::new(remote.clone()));
    let mut held = Vec::new();
    let mut passes = 0;
    while engine
        .storage
        .find_output(&chain[0], 0, Some("tm_test"), None, true)
        .await
        .unwrap()
        .is_none()
    {
        passes += 1;
        assert!(passes <= 6, "never landed: {held:?}");
        engine.start_gasp_sync().await.unwrap();
        held.push(i590_record_nodes(&engine, &root).await);
    }
    assert_eq!(
        held,
        vec![Some(3), Some(5), None],
        "nodes held after each pass"
    );
    assert_eq!(passes, 3);
    let asked = remote.asked.borrow();
    for txid in &chain[1..] {
        assert_eq!(
            asked.iter().filter(|t| *t == txid).count(),
            1,
            "a link fetched once: {asked:?}"
        );
    }
    assert_eq!(asked.iter().filter(|t| **t == chain[0]).count(), 3);
}

/// The finish-the-node rule on its own: a node whose request starts inside
/// the slice completes and is kept, even when it ends past the slice; the
/// next is not started. Slice 5,000 ms, requests at 0, 2 and 4 s, the third
/// ending at 6 s: the pass keeps 3 nodes and asks nothing at 6 s. A request
/// that outlasts a whole slice of its own from its start is still cut
/// (`time`): the stall rule (`proofs_deadline_stalls_become_durable_after_three_runs`).
#[tokio::test]
async fn i590_b_a_node_started_inside_the_slice_completes() {
    let (mut engine, remote, chains) = i590_chain_engine(1, 6, 2_000);
    let root = format!("{}.0", chains[0][0]);
    engine.set_graph_budget(i590_virtual_sleep(&remote), 100, 5_000);
    engine.set_gasp_remote_factory(Box::new(remote.clone()));
    let result = engine.start_gasp_sync().await.unwrap();
    let topic = &result.topics_synced["tm_test"];
    assert_eq!(topic.deferred_graphs, 1, "{topic:?}");
    assert_eq!(
        remote.asked.borrow().len(),
        3,
        "nothing started past the slice"
    );
    assert_eq!(
        remote.elapsed.get(),
        6_000,
        "the third request ended past it"
    );
    assert_eq!(i590_record_nodes(&engine, &root).await, Some(3));
}

/// The time limb per RESUME: two deferred graphs resumed in one sync each
/// walk a whole slice of their own (with no resumes' share configured, no
/// time bound is shared). Pass 1 defers both with 3 nodes; pass 2 resumes
/// both, each re-asking its root and appending 2 more, none held back. RED
/// on the base, where every resume of a sync shared ONE deadline made at
/// the first resume (the lens fold's M1): the second graph found it spent
/// and was held back (Zanaadu's #377: deferred graphs resumed one or two a
/// tick).
#[tokio::test]
async fn i590_c_each_resume_of_a_sync_walks_its_own_slice() {
    let (mut engine, remote, chains) = i590_chain_engine(2, 6, 2_000);
    let roots: Vec<String> = chains.iter().map(|c| format!("{}.0", c[0])).collect();
    engine.set_graph_budget(i590_virtual_sleep(&remote), 100, 5_000);
    engine.set_gasp_remote_factory(Box::new(remote.clone()));
    let first = engine.start_gasp_sync().await.unwrap();
    assert_eq!(first.topics_synced["tm_test"].deferred_graphs, 2);
    let second = engine.start_gasp_sync().await.unwrap();
    let topic = &second.topics_synced["tm_test"];
    assert_eq!(topic.resumed_graphs, 2, "{topic:?}");
    assert_eq!(topic.held_back_graphs, 0, "{topic:?}");
    for root in &roots {
        assert_eq!(i590_record_nodes(&engine, root).await, Some(5), "{root}");
    }
}
