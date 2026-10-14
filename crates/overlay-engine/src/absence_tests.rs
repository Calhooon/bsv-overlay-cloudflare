// Engine 0.1.15 (bsv-overlay-cloudflare #56): an absence is the peers' word,
// never an explorer's. The witnesses run with the production shape: a
// courier installed with no rung configured (`PROOF_SOURCES` unset), which
// answers every raw fetch "exhausted" with no request.

/// The courier of a deployment with no rung configured: every raw source is
/// disabled, so `fetch_raw_hex` answers `NodeNotFound` without a request
/// (`proof_fetcher.rs`, the rung loop with every source skipped).
struct NoRungCourier {
    calls: std::rc::Rc<Cell<u64>>,
}

#[async_trait(?Send)]
impl AncestorFetcher for NoRungCourier {
    async fn fetch_ancestor(&self, txid: &str) -> Result<FetchedAncestor, GASPError> {
        self.calls.set(self.calls.get() + 1);
        Err(GASPError::NodeNotFound(format!(
            "no raw tx for {txid} (bitails + woc + bananablocks exhausted)"
        )))
    }
    async fn verified_proof_for(&self, _: &str) -> Option<String> {
        None
    }
}

/// The advertising peer: lists the page, answers `NodeNotFound` for the
/// withheld root until told to serve it, or a transport fault when told to.
#[derive(Clone)]
struct AbsencePeer {
    inner: BudgetPageRemote,
    withheld: std::rc::Rc<(String, crate::types::GASPNode)>,
    serve: std::rc::Rc<Cell<bool>>,
    fault: std::rc::Rc<Cell<bool>>,
}

#[async_trait(?Send)]
impl crate::gasp::GASPRemote for AbsencePeer {
    async fn get_initial_response(
        &self,
        request: &crate::types::GASPInitialRequest,
    ) -> Result<crate::types::GASPInitialResponse, GASPError> {
        self.inner.get_initial_response(request).await
    }

    async fn get_initial_reply(
        &self,
        response: &crate::types::GASPInitialResponse,
    ) -> Result<crate::types::GASPInitialReply, GASPError> {
        self.inner.get_initial_reply(response).await
    }

    async fn request_node(
        &self,
        graph_id: &str,
        txid: &str,
        output_index: u32,
        metadata: bool,
    ) -> Result<crate::types::GASPNode, GASPError> {
        if txid == self.withheld.0 {
            self.inner.asked.borrow_mut().push(txid.to_string());
            if self.fault.get() {
                return Err(GASPError::RemoteError("HTTP 503".into()));
            }
            if self.serve.get() {
                let mut node = self.withheld.1.clone();
                node.graph_id = graph_id.to_string();
                return Ok(node);
            }
            return Err(GASPError::NodeNotFound(format!("{txid}.0")));
        }
        self.inner
            .request_node(graph_id, txid, output_index, metadata)
            .await
    }

    async fn submit_node(
        &self,
        node: &crate::types::GASPNode,
    ) -> Result<Option<crate::types::GASPNodeResponse>, GASPError> {
        self.inner.submit_node(node).await
    }
}

impl crate::gasp::GASPRemoteFactory for AbsencePeer {
    fn create_remote(&self, _: &str, _: &str) -> Box<dyn crate::gasp::GASPRemote> {
        Box::new(self.clone())
    }
}

struct AbsenceFixture {
    engine: Engine,
    peer: AbsencePeer,
    store: std::rc::Rc<MemoryStorage>,
    root: String,
    courier_calls: std::rc::Rc<Cell<u64>>,
}

const ABSENCE_HOST: &str = "https://budget.example";

/// A page of three proven roots (scores 10, 20, 30) whose lowest the peer
/// lists and answers it does not hold; no rung configured.
fn absence_fixture() -> AbsenceFixture {
    let (mut engine, mut remote) = budget_page_engine(3);
    remote.node_cost_ms = 0;
    let txid = remote.outputs[0].txid.clone();
    let node = std::rc::Rc::make_mut(&mut remote.nodes)
        .remove(&txid)
        .unwrap();
    let courier_calls = std::rc::Rc::new(Cell::new(0));
    engine.set_gasp_ancestor_fetcher(std::rc::Rc::new(NoRungCourier {
        calls: courier_calls.clone(),
    }));
    let store = std::rc::Rc::new(MemoryStorage::new());
    engine.storage = Box::new(store.clone());
    virtual_peer_budget(&mut engine, &remote);
    engine.set_graph_budget(
        std::rc::Rc::new(|_| Box::pin(std::future::pending())),
        100,
        60_000,
    );
    let peer = AbsencePeer {
        inner: remote,
        withheld: std::rc::Rc::new((txid.clone(), node)),
        serve: std::rc::Rc::new(Cell::new(false)),
        fault: std::rc::Rc::new(Cell::new(false)),
    };
    engine.set_gasp_remote_factory(Box::new(peer.clone()));
    AbsenceFixture {
        engine,
        peer,
        store,
        root: format!("{txid}.0"),
        courier_calls,
    }
}

/// The gap between two counted absences, passed by hand: the record's next
/// count is due now.
async fn absence_gap_passes(store: &MemoryStorage, root: &str) {
    let mut record = store
        .get_deferred_graph(ABSENCE_HOST, "tm_test", root)
        .await
        .unwrap()
        .unwrap();
    record.retry_at = 0;
    store.put_deferred_graph(&record).await.unwrap();
}

fn root_asks(peer: &AbsencePeer) -> usize {
    peer.inner
        .asked
        .borrow()
        .iter()
        .filter(|t| **t == peer.withheld.0)
        .count()
}

/// Branch A of the corpus draft: the peer answers it does not hold the root
/// on three runs with a gap; with no rung configured the record is struck
/// `unanchorable:not_found` and the cursor passes it. A run inside the gap
/// asks nothing and counts nothing. On 0.1.14 the class was `not_served`.
#[tokio::test]
async fn absence_0115_the_peers_word_on_three_runs_strikes_not_found_with_no_rung() {
    let f = absence_fixture();
    let runs = crate::gasp::UNANCHORABLE_AFTER_FAULTED_RUNS;
    for run in 1..=runs {
        if run > 1 {
            // Inside the gap: no request, no count, the cursor holds.
            let asks = root_asks(&f.peer);
            let result = f.engine.start_gasp_sync().await.unwrap();
            let topic = &result.topics_synced["tm_test"];
            assert_eq!(root_asks(&f.peer), asks, "run {run}: inside the gap");
            let record = f
                .store
                .get_deferred_graph(ABSENCE_HOST, "tm_test", &f.root)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(record.faults, run - 1, "no count inside the gap");
            assert_eq!(topic.cursor_held_by, vec![f.root.clone()]);
            assert!(result.cost.peer_absent_memo_hits >= 1, "{:?}", result.cost);
            absence_gap_passes(&f.store, &f.root).await;
        }
        let result = f.engine.start_gasp_sync().await.unwrap();
        let topic = &result.topics_synced["tm_test"];
        let named = topic
            .faulted_roots
            .iter()
            .find(|r| r.outpoint == f.root)
            .unwrap_or_else(|| panic!("run {run}: {topic:?}"));
        assert_eq!(named.class, crate::gasp::fault_class::NOT_FOUND, "run {run}");
        assert_eq!(named.faults, run, "run {run}");
        let record = f
            .store
            .get_deferred_graph(ABSENCE_HOST, "tm_test", &f.root)
            .await
            .unwrap()
            .unwrap();
        if run < runs {
            assert_eq!(record.reason, "retry:fault:not_found", "run {run}");
            assert!(!record.cursor_passable);
            assert!(
                record.retry_at > (current_timestamp_ms() / 1000.0) as u64,
                "the gap before the next count"
            );
            assert_eq!(topic.cursor_held_by, vec![f.root.clone()]);
            assert_eq!(topic.absent_struck, 0);
        } else {
            assert_eq!(record.reason, "unanchorable:not_found");
            assert!(record.cursor_passable);
            assert_eq!(topic.unanchorable_graphs, 1);
            assert_eq!(topic.absent_struck, 1);
            assert!(topic.cursor_held_by.is_empty(), "{topic:?}");
        }
    }
    assert_eq!(
        f.store
            .get_last_interaction(ABSENCE_HOST, "tm_test")
            .await
            .unwrap(),
        30,
        "the cursor passed"
    );
    assert_eq!(f.courier_calls.get(), 0, "no rung decided anything");
    // Struck and passed: below the cursor and inside its backoff, it is
    // neither on the page nor due, so the run costs no request for it.
    let asks = root_asks(&f.peer);
    f.engine.start_gasp_sync().await.unwrap();
    assert_eq!(root_asks(&f.peer), asks);
}

/// Branch B: two absences and one transport fault give no strike; a 503 is
/// the moment's and counts nothing.
#[tokio::test]
async fn absence_0115_two_absences_and_a_transport_fault_give_no_strike() {
    let f = absence_fixture();
    f.engine.start_gasp_sync().await.unwrap();
    absence_gap_passes(&f.store, &f.root).await;
    f.engine.start_gasp_sync().await.unwrap();
    absence_gap_passes(&f.store, &f.root).await;
    f.peer.fault.set(true);
    let result = f.engine.start_gasp_sync().await.unwrap();
    let topic = &result.topics_synced["tm_test"];
    let record = f
        .store
        .get_deferred_graph(ABSENCE_HOST, "tm_test", &f.root)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (record.reason.as_str(), record.faults),
        ("retry:fault:not_found", 2)
    );
    assert!(!record.cursor_passable);
    assert_eq!(topic.unanchorable_graphs, 0);
    assert_eq!(topic.absent_struck, 0);
    assert_eq!(topic.cursor_held_by, vec![f.root.clone()]);
    assert_eq!(
        f.store
            .get_last_interaction(ABSENCE_HOST, "tm_test")
            .await
            .unwrap(),
        0,
        "the cursor holds below the root"
    );
}

/// A root the peer serves on the third run is admitted with its own proof
/// and its record cleared: the absence was never a verdict on the chain.
#[tokio::test]
async fn absence_0115_a_root_the_peer_serves_on_the_third_run_is_admitted_with_its_proof() {
    let f = absence_fixture();
    f.engine.start_gasp_sync().await.unwrap();
    absence_gap_passes(&f.store, &f.root).await;
    f.engine.start_gasp_sync().await.unwrap();
    absence_gap_passes(&f.store, &f.root).await;
    f.peer.serve.set(true);
    let result = f.engine.start_gasp_sync().await.unwrap();
    let topic = &result.topics_synced["tm_test"];
    assert_eq!(topic.finalized_graphs, 1, "{topic:?}");
    assert_eq!(topic.unanchorable_cleared, 1);
    assert!(topic.faulted_roots.is_empty());
    assert!(f
        .store
        .get_deferred_graph(ABSENCE_HOST, "tm_test", &f.root)
        .await
        .unwrap()
        .is_none());
    let (txid, _) = crate::gasp::parse_outpoint(&f.root).unwrap();
    let stored = f
        .store
        .find_output(&txid, 0, Some("tm_test"), None, true)
        .await
        .unwrap()
        .expect("admitted");
    assert!(
        crate::beef_limits::own_proof(stored.beef.as_ref().unwrap(), &txid)
            .unwrap()
            .is_some(),
        "admitted with its own proof"
    );
}

/// The rungs configured and answering absent on one run count one run, not
/// a strike: on 0.1.12 to 0.1.14 the rungs' `DefinitiveNotFound` struck
/// `unanchorable:not_found` at once. The peer, asked too, does not hold the
/// parent either: still one count, never two in a run.
#[tokio::test]
async fn absence_0115_rungs_answering_absent_count_one_run_not_a_strike() {
    let f = unanchorable_fixture();
    // The peer's own word on the parent: it does not hold it.
    f.remote.faults_for.borrow_mut().clear();
    let mut engine = f.engine;
    engine.set_gasp_ancestor_fetcher(std::rc::Rc::new(LoadCourier {
        calls: std::rc::Rc::new(Cell::new(0)),
        bounded: false,
    }));
    let result = engine.start_gasp_sync().await.unwrap();
    let topic = &result.topics_synced["tm_test"];
    let record = engine
        .storage
        .get_deferred_graph(ABSENCE_HOST, "tm_test", &f.root)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (record.reason.as_str(), record.faults),
        ("retry:fault:not_found", 1)
    );
    assert!(!record.cursor_passable);
    assert_eq!(topic.unanchorable_graphs, 0);
    assert_eq!(topic.cursor_held_by, vec![f.root.clone()]);
}

/// The peer serves the root unproven and answers it does not hold the
/// parent the root needs: nothing can anchor. With no rung configured the
/// courier cannot serve the parent, so the peer is asked (one request) and
/// its word counts the run; three with a gap strike `unanchorable:not_found`.
/// On 0.1.14 the peer was never asked and the class was `courier`.
#[tokio::test]
async fn absence_0115_an_input_the_peer_does_not_hold_under_an_unproven_root_counts() {
    let f = unanchorable_fixture();
    // The peer's own word on the parent: it does not hold it.
    f.remote.faults_for.borrow_mut().clear();
    let mut engine = f.engine;
    let calls = std::rc::Rc::new(Cell::new(0));
    engine.set_gasp_ancestor_fetcher(std::rc::Rc::new(NoRungCourier {
        calls: calls.clone(),
    }));
    let runs = crate::gasp::UNANCHORABLE_AFTER_FAULTED_RUNS;
    for run in 1..=runs {
        let before = f.remote.asked.borrow().len();
        let result = engine.start_gasp_sync().await.unwrap();
        let topic = &result.topics_synced["tm_test"];
        let named = topic
            .faulted_roots
            .iter()
            .find(|r| r.outpoint == f.root)
            .unwrap_or_else(|| panic!("run {run}: {topic:?}"));
        assert_eq!(named.class, crate::gasp::fault_class::NOT_FOUND, "run {run}");
        assert_eq!(named.faults, run);
        assert!(
            f.remote.asked.borrow().len() > before + 1,
            "the root and its parent asked of the peer"
        );
        if run < runs {
            absence_gap_passes(&f.store, &f.root).await;
        } else {
            assert!(named.unanchorable);
            assert_eq!(topic.absent_struck, 1);
        }
    }
    let record = f
        .store
        .get_deferred_graph(ABSENCE_HOST, "tm_test", &f.root)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.reason, "unanchorable:not_found");
}

/// `child_of` with its one output's value named.
fn child_of_value(source_txid: &str, vout: u32, value: u64) -> bsv_rs::transaction::Transaction {
    let mut sx = String::from("0100000001");
    let mut prev = hex::decode(source_txid).unwrap();
    prev.reverse();
    sx.push_str(&hex::encode(prev));
    sx.push_str(&hex::encode(vout.to_le_bytes()));
    sx.push_str("00ffffffff01");
    sx.push_str(&hex::encode(value.to_le_bytes()));
    sx.push_str("015100000000");
    bsv_rs::transaction::Transaction::from_hex(&sx).unwrap()
}

/// Two roots of one sync that need the same parent the peer does not hold:
/// the parent is asked once, the second need is the sync's memo.
#[tokio::test]
async fn absence_0115_one_absent_parent_is_asked_once_a_sync() {
    let (mut engine, mut remote) = budget_page_engine(2);
    remote.node_cost_ms = 0;
    let parent = child_of(&format!("{:064x}", 0xcd), 0);
    let mut tracker = bsv_rs::transaction::MockChainTracker::new(1000);
    let nodes = std::rc::Rc::make_mut(&mut remote.nodes);
    nodes.clear();
    let outputs = std::rc::Rc::make_mut(&mut remote.outputs);
    for (n, out) in outputs.iter_mut().enumerate() {
        // Two children spending the one parent output, distinct by value.
        let child = child_of_value(&parent.id(), 0, 1000 + n as u64);
        out.txid = child.id();
        tracker.add_root(1, child.id());
        nodes.insert(
            child.id(),
            crate::types::GASPNode {
                graph_id: String::new(),
                raw_tx: child.to_hex(),
                output_index: 0,
                proof: None,
                tx_metadata: None,
                output_metadata: None,
                inputs: None,
            },
        );
    }
    engine.chain_tracker = Some(Box::new(tracker));
    engine.set_gasp_ancestor_fetcher(std::rc::Rc::new(NoRungCourier {
        calls: std::rc::Rc::new(Cell::new(0)),
    }));
    let store = std::rc::Rc::new(MemoryStorage::new());
    engine.storage = Box::new(store.clone());
    virtual_peer_budget(&mut engine, &remote);
    engine.set_graph_budget(
        std::rc::Rc::new(|_| Box::pin(std::future::pending())),
        100,
        60_000,
    );
    engine.set_gasp_remote_factory(Box::new(remote.clone()));
    let result = engine.start_gasp_sync().await.unwrap();
    let parent_asks = remote
        .asked
        .borrow()
        .iter()
        .filter(|t| **t == parent.id())
        .count();
    assert_eq!(parent_asks, 1, "{:?}", remote.asked.borrow());
    assert_eq!(result.cost.peer_absent_memo_hits, 1, "{:?}", result.cost);
    let topic = &result.topics_synced["tm_test"];
    assert_eq!(
        topic
            .faulted_roots
            .iter()
            .filter(|r| r.class == crate::gasp::fault_class::NOT_FOUND)
            .count(),
        2,
        "{topic:?}"
    );
}
