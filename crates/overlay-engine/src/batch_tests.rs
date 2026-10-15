// Engine 0.2.1 (bsv-overlay-cloudflare #58): a batched node request between
// peers that both answer it. The witnesses wrap the mock peers of the 0.1.x
// tests in a batch the way the Worker's route answers one: each outpoint as
// its single request answers it, packed by the route's own packer.

#[derive(Clone, Copy, PartialEq, Eq)]
enum BatchMode {
    /// The peer has no batch request: the reference shape.
    Single,
    /// The peer answers the batch as the route does.
    Answer,
    /// The batch route answers 404: a peer that never adopted it.
    Missing,
    /// The platform answers the batch route before the peer (0.2.0's class).
    Route,
    /// The peer answers more bytes than the budget allows.
    Oversize,
}

#[derive(Clone)]
struct BatchPeer<R: Clone> {
    inner: R,
    mode: std::rc::Rc<Cell<BatchMode>>,
    /// The answer's byte budget the peer packs to.
    budget: usize,
    /// The outpoints named by each batch body sent.
    bodies: std::rc::Rc<std::cell::RefCell<Vec<usize>>>,
    /// Single node requests sent.
    singles: std::rc::Rc<Cell<u64>>,
}

impl<R: Clone> BatchPeer<R> {
    fn new(inner: R, mode: BatchMode) -> Self {
        Self {
            inner,
            mode: std::rc::Rc::new(Cell::new(mode)),
            budget: crate::node_batch::NODE_BATCH_MAX_BYTES,
            bodies: std::rc::Rc::default(),
            singles: std::rc::Rc::default(),
        }
    }
}

#[async_trait(?Send)]
impl<R: crate::gasp::GASPRemote + Clone> crate::gasp::GASPRemote for BatchPeer<R> {
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
        self.singles.set(self.singles.get() + 1);
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

    fn batches(&self) -> bool {
        self.mode.get() != BatchMode::Single
    }

    async fn request_nodes(
        &self,
        outpoints: &[crate::node_batch::BatchOutpoint],
    ) -> Result<crate::node_batch::NodeBatch, GASPError> {
        use crate::node_batch::{refusal, NodeBatch, NodeBatchEntry, Packer};
        self.bodies.borrow_mut().push(outpoints.len());
        match self.mode.get() {
            BatchMode::Single => unreachable!("a single-only peer is never asked for a batch"),
            BatchMode::Missing => {
                return Err(GASPError::RemoteError(
                    "Peer https://budget.example/requestForeignGASPNodes returned HTTP 404: Not Found"
                        .into(),
                ))
            }
            BatchMode::Route => {
                return Err(GASPError::RouteFault {
                    class: crate::route_fault::class::CF_1042.to_string(),
                    detail: "answered by the platform HTTP 404: error code: 1042".into(),
                })
            }
            BatchMode::Answer | BatchMode::Oversize => {}
        }
        let mut packer = Packer::new(self.budget);
        for outpoint in outpoints {
            let answer = self
                .inner
                .request_node(&outpoint.key(), &outpoint.txid, outpoint.vout, true)
                .await;
            let (node, refused) = match answer {
                Ok(node) => (Some(node), None),
                Err(GASPError::NodeNotFound(_)) => (None, Some(refusal::NOT_FOUND)),
                Err(_) => (None, Some(refusal::ERROR)),
            };
            if !packer.push(NodeBatchEntry {
                txid: outpoint.txid.clone(),
                vout: outpoint.vout,
                node,
                refused: refused.map(str::to_string),
            }) {
                break;
            }
        }
        let bytes = serde_json::to_string(&packer.answer).unwrap().len();
        let bytes = if self.mode.get() == BatchMode::Oversize {
            crate::node_batch::NODE_BATCH_MAX_BYTES + 1
        } else {
            bytes
        };
        Ok(NodeBatch {
            answer: packer.answer,
            bytes,
        })
    }
}

impl<R: crate::gasp::GASPRemote + Clone + 'static> crate::gasp::GASPRemoteFactory
    for BatchPeer<R>
{
    fn create_remote(&self, _: &str, _: &str) -> Box<dyn crate::gasp::GASPRemote> {
        Box::new(self.clone())
    }
}

/// An engine over `count` proven roots one per page row, its peer wrapped
/// in a batch of `mode`; the storage shared for health reads.
fn batch_engine(
    count: u32,
    mode: BatchMode,
) -> (
    Engine,
    BatchPeer<BudgetPageRemote>,
    std::rc::Rc<MemoryStorage>,
) {
    let (mut engine, mut remote) = budget_page_engine(count);
    remote.node_cost_ms = 0;
    let store = std::rc::Rc::new(MemoryStorage::new());
    engine.storage = Box::new(store.clone());
    virtual_peer_budget(&mut engine, &remote);
    let peer = BatchPeer::new(remote, mode);
    engine.set_gasp_remote_factory(Box::new(peer.clone()));
    (engine, peer, store)
}

/// The rows each page of a run served that no page before it had, as the
/// mock serves them (all rows from `since`, unless it clamps): a page
/// repeats its boundary row (the responder serves `score >= since`).
fn fresh_per_page(remote: &BudgetPageRemote) -> Vec<usize> {
    let mut seen = std::collections::HashSet::new();
    remote
        .requests
        .borrow()
        .iter()
        .map(|request| {
            remote
                .outputs
                .iter()
                .filter(|u| u.score as u64 >= request.since)
                .take(remote.clamp.map_or(usize::MAX, |cap| {
                    cap.min(request.limit.unwrap_or(u64::MAX) as usize)
                }))
                .filter(|u| seen.insert(u.txid.clone()))
                .count()
        })
        .collect()
}

async fn finalized(engine: &Engine, remote: &BudgetPageRemote) -> usize {
    let mut held = 0;
    for output in remote.outputs.iter() {
        if engine
            .storage
            .find_output(&output.txid, 0, Some("tm_test"), None, true)
            .await
            .unwrap()
            .is_some()
        {
            held += 1;
        }
    }
    held
}

/// A thousand-output slice against a peer that answers the batch costs the
/// bodies the budget predicts (the new rows of each page over the count cap;
/// this mock serves the whole slice as one page) and finalizes every node,
/// each with its own proof; no single request.
#[tokio::test]
async fn batch_021_a_thousand_outputs_cost_the_bodies_the_budget_predicts() {
    let (engine, peer, _) = batch_engine(1000, BatchMode::Answer);
    let result = engine.start_gasp_sync().await.unwrap();
    let topic = &result.topics_synced["tm_test"];
    assert_eq!(topic.finalized_graphs, 1000, "{topic:?}");
    assert_eq!(finalized(&engine, &peer.inner).await, 1000);
    let pages = peer.inner.requests.borrow().len() as u64;
    let predicted: u64 = fresh_per_page(&peer.inner)
        .iter()
        .map(|fresh| fresh.div_ceil(crate::node_batch::NODE_BATCH_MAX_OUTPOINTS) as u64)
        .sum();
    assert_eq!(predicted, 10, "1,000 new rows, 100 a body");
    assert_eq!(peer.bodies.borrow().len() as u64, predicted);
    assert_eq!(peer.singles.get(), 0);
    assert_eq!(result.cost.batch_requests, predicted);
    assert_eq!(result.cost.batch_nodes, 1000);
    assert_eq!(result.cost.peer_requests, pages + predicted);
    assert_eq!(result.cost.outputs_walked, 1000);
    eprintln!("batch_021 thousand, batched: {:?}", result.cost);
}

/// The same slice against a peer with no batch costs one request per output,
/// as before 0.2.1.
#[tokio::test]
async fn batch_021_a_single_only_peer_costs_one_request_per_output() {
    let (engine, peer, _) = batch_engine(1000, BatchMode::Single);
    let result = engine.start_gasp_sync().await.unwrap();
    assert_eq!(result.topics_synced["tm_test"].finalized_graphs, 1000);
    let pages = peer.inner.requests.borrow().len() as u64;
    assert!(peer.bodies.borrow().is_empty());
    assert_eq!(peer.singles.get(), 1000);
    assert_eq!(result.cost.batch_requests, 0);
    assert_eq!(result.cost.batch_nodes, 0);
    assert_eq!(result.cost.peer_requests, pages + 1000);
    eprintln!("batch_021 thousand, single: {:?}", result.cost);
}

/// A budget that holds two nodes: each body answers what fits, the rest is
/// named again in the next body, and nothing goes to the single route.
#[tokio::test]
async fn batch_021_what_the_budget_left_out_is_asked_in_the_next_body() {
    let (mut engine, mut peer, _) = batch_engine(5, BatchMode::Answer);
    let one = {
        let node = peer.inner.nodes.values().next().unwrap().clone();
        let entry = crate::node_batch::NodeBatchEntry {
            txid: peer.inner.outputs[0].txid.clone(),
            vout: 0,
            node: Some(crate::types::GASPNode {
                graph_id: format!("{}.0", peer.inner.outputs[0].txid),
                ..node
            }),
            refused: None,
        };
        serde_json::to_string(&entry).unwrap().len()
    };
    peer.budget = 12 + 2 * one + 1;
    engine.set_gasp_remote_factory(Box::new(peer.clone()));
    let result = engine.start_gasp_sync().await.unwrap();
    assert_eq!(result.topics_synced["tm_test"].finalized_graphs, 5);
    assert_eq!(*peer.bodies.borrow(), vec![5, 3, 1]);
    assert_eq!(peer.singles.get(), 0);
    assert_eq!(result.cost.batch_nodes, 5);
}

/// A peer whose batch route answers 404 is asked one node at a time for the
/// rest of the run, after one batch body; the memo is the run's, so the next
/// run asks for the batch again (a peer that gains the route is found).
#[tokio::test]
async fn batch_021_a_404_makes_the_peer_single_only_for_the_run() {
    let (mut engine, peer, store) = batch_engine(500, BatchMode::Missing);
    // The opening run's peer lists 250 rows of the 500.
    let mut opening = peer.clone();
    std::rc::Rc::make_mut(&mut opening.inner.outputs).truncate(250);
    engine.set_gasp_remote_factory(Box::new(opening));
    let result = engine.start_gasp_sync().await.unwrap();
    let topic = &result.topics_synced["tm_test"];
    assert_eq!(topic.finalized_graphs, 250);
    assert!(topic.errors.is_empty(), "{:?}", topic.errors);
    assert_eq!(peer.bodies.borrow().len(), 1, "one batch body a run");
    assert_eq!(peer.singles.get(), 250);
    assert_eq!(result.cost.batch_requests, 1);
    let health = store
        .get_peer_sync_health(&crate::gasp::peer_origin(ABSENCE_HOST), "tm_test")
        .await
        .unwrap();
    assert_eq!(health.consecutive_failures, 0);
    // The next run: the peer has gained the route and lists 250 more rows.
    peer.mode.set(BatchMode::Answer);
    engine.set_gasp_remote_factory(Box::new(peer.clone()));
    let result = engine.start_gasp_sync().await.unwrap();
    assert_eq!(result.topics_synced["tm_test"].finalized_graphs, 250);
    assert!(peer.bodies.borrow().len() > 1, "asked for the batch again");
    assert_eq!(peer.singles.get(), 250, "and no single request");
    assert_eq!(
        result.cost.batch_requests as usize,
        peer.bodies.borrow().len() - 1
    );
    assert_eq!(finalized(&engine, &peer.inner).await, 500);
}

/// A route fault on the batch route counts nothing against the peer (0.2.0's
/// rule): no error, no route fault named for the sync, the health rows as a
/// served sync leaves them, and the run proceeds one node at a time.
#[tokio::test]
async fn batch_021_a_route_fault_on_the_batch_counts_nothing_against_the_peer() {
    let (engine, peer, store) = batch_engine(3, BatchMode::Route);
    let host = crate::gasp::peer_origin(ABSENCE_HOST);
    let result = engine.start_gasp_sync().await.unwrap();
    let topic = &result.topics_synced["tm_test"];
    assert!(topic.errors.is_empty(), "{:?}", topic.errors);
    assert!(topic.route_faults.is_empty(), "the batch's fault is not the sync's");
    assert!(topic.peers_skipped.is_empty());
    assert_eq!(topic.finalized_graphs, 3);
    assert_eq!(peer.bodies.borrow().len(), 1, "one batch body, a route fault");
    assert_eq!(result.cost.batch_requests, 1);
    assert_eq!(peer.singles.get(), 3, "the run went on one node at a time");
    let health = store.get_peer_sync_health(&host, "tm_test").await.unwrap();
    assert_eq!(health.consecutive_failures, 0);
    assert!(!crate::gasp::peer_sync_quarantined(&health));
    assert_eq!(finalized(&engine, &peer.inner).await, 3);
}

/// An answer over the byte budget is refused: no node of it is used, and the
/// slice falls back to single requests for that peer.
#[tokio::test]
async fn batch_021_an_answer_over_the_byte_budget_is_refused() {
    let (engine, peer, _) = batch_engine(150, BatchMode::Oversize);
    let result = engine.start_gasp_sync().await.unwrap();
    assert_eq!(result.topics_synced["tm_test"].finalized_graphs, 150);
    assert_eq!(peer.bodies.borrow().len(), 1);
    assert_eq!(result.cost.batch_requests, 1);
    assert_eq!(result.cost.batch_nodes, 0, "nothing of the refused answer");
    assert_eq!(peer.singles.get(), 150);
}

/// A batch answer with one `not_found` entry counts one absence run for that
/// outpoint (the 0.1.15 record, `retry:fault:not_found`, one fault) and
/// nothing for the others, which land with their proofs; no single request.
#[tokio::test]
async fn batch_021_one_not_found_entry_counts_one_absence_and_nothing_else() {
    let f = absence_fixture();
    let peer = BatchPeer::new(f.peer.clone(), BatchMode::Answer);
    let mut engine = f.engine;
    engine.set_gasp_remote_factory(Box::new(peer.clone()));
    let result = engine.start_gasp_sync().await.unwrap();
    let topic = &result.topics_synced["tm_test"];
    assert_eq!(peer.bodies.borrow().len(), 1);
    assert_eq!(peer.singles.get(), 0);
    assert_eq!(result.cost.batch_nodes, 2);
    assert_eq!(topic.finalized_graphs, 2);
    let named: Vec<_> = topic
        .faulted_roots
        .iter()
        .map(|r| (r.outpoint.clone(), r.class.clone(), r.faults))
        .collect();
    assert_eq!(
        named,
        vec![(f.root.clone(), crate::gasp::fault_class::NOT_FOUND.to_string(), 1)]
    );
    let record = f
        .store
        .get_deferred_graph(ABSENCE_HOST, "tm_test", &f.root)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((record.reason.as_str(), record.faults), ("retry:fault:not_found", 1));
    for output in f.peer.inner.outputs.iter().skip(1) {
        let outpoint = format!("{}.0", output.txid);
        assert!(f
            .store
            .get_deferred_graph(ABSENCE_HOST, "tm_test", &outpoint)
            .await
            .unwrap()
            .is_none());
    }
    assert_eq!(f.courier_calls.get(), 0);
}

/// The route's provider: a held output's node as the single route serves
/// it, `not_found` for one not held, a repeat answered once, and a request
/// over the cap refused before any read.
#[tokio::test]
async fn batch_021_the_provider_answers_each_outpoint_as_the_single_route() {
    use crate::node_batch::{refusal, BatchOutpoint, NODE_BATCH_MAX_OUTPOINTS};
    let (engine, peer, _) = batch_engine(2, BatchMode::Single);
    engine.start_gasp_sync().await.unwrap();
    let held = &peer.inner.outputs[0].txid;
    let single = engine
        .provide_foreign_gasp_node(&format!("{held}.0"), held, 0)
        .await
        .unwrap();
    let ask = |txid: &str| BatchOutpoint {
        txid: txid.to_string(),
        vout: 0,
    };
    let answer = engine
        .provide_foreign_gasp_nodes(&[ask(held), ask(&"ee".repeat(32)), ask(held)])
        .await
        .unwrap();
    assert_eq!(answer.nodes.len(), 2);
    let node = answer.nodes[0].node.as_ref().unwrap();
    assert_eq!(
        serde_json::to_value(node).unwrap(),
        serde_json::to_value(&single).unwrap()
    );
    assert!(node.proof.is_some(), "the proof as the single route serves it");
    assert_eq!(answer.nodes[1].refused.as_deref(), Some(refusal::NOT_FOUND));
    let over = vec![ask(held); NODE_BATCH_MAX_OUTPOINTS + 1];
    assert!(matches!(
        engine.provide_foreign_gasp_nodes(&over).await,
        Err(EngineError::InvalidQuery(m)) if m.contains("100")
    ));
}
