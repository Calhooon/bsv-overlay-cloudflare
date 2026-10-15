// Engine 0.2.2 (the owner's word of 2026-10-15 on #58's finding): the
// sync's storage per page. The witnesses put the engine's own metered
// storage back in front of the mock (the 0.1.x and 0.2.1 witnesses swap it
// for the bare `MemoryStorage`, so they never open a page) over a storage
// that names every trait call the engine makes and can refuse a batch.

/// A storage that names every trait call it is asked, forwards it to the
/// memory store, and refuses the next `fail_batches` batches whole, as D1
/// rolls a failed batch back.
#[derive(Clone)]
struct CountingStorage {
    inner: std::rc::Rc<MemoryStorage>,
    log: std::rc::Rc<std::cell::RefCell<Vec<&'static str>>>,
    fail_batches: std::rc::Rc<Cell<u32>>,
}
impl CountingStorage {
    fn note(&self, name: &'static str) {
        self.log.borrow_mut().push(name);
    }
}
#[async_trait(?Send)]
impl Storage for CountingStorage {
    async fn insert_output_with_origin(
        &self,
        output: &Output,
        gasp: bool,
    ) -> Result<(), StorageError> {
        self.note("insert_output_with_origin");
        self.inner.insert_output_with_origin(output, gasp).await
    }
    async fn read_fault_memos(
        &self,
        host: &str,
        topic: &str,
        outpoints: Option<&[String]>,
    ) -> Result<crate::fault_memo::FaultMemoBatch, StorageError> {
        self.note("read_fault_memos");
        self.inner.read_fault_memos(host, topic, outpoints).await
    }

    async fn get_peer_cadence(
        &self,
        host: &str,
        topic: &str,
    ) -> Result<crate::sync_load::PeerCadence, StorageError> {
        self.note("get_peer_cadence");
        self.inner.get_peer_cadence(host, topic).await
    }
    async fn set_peer_cadence(
        &self,
        host: &str,
        topic: &str,
        state: &crate::sync_load::PeerCadence,
    ) -> Result<(), StorageError> {
        self.note("set_peer_cadence");
        self.inner.set_peer_cadence(host, topic, state).await
    }
    async fn insert_output(&self, output: &Output) -> Result<(), StorageError> {
        self.note("insert_output");
        self.inner.insert_output(output).await
    }
    async fn delete_output(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
    ) -> Result<(), StorageError> {
        self.note("delete_output");
        self.inner.delete_output(txid, output_index, topic).await
    }
    async fn mark_utxo_as_spent(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
    ) -> Result<(), StorageError> {
        self.note("mark_utxo_as_spent");
        self.inner.mark_utxo_as_spent(txid, output_index, topic).await
    }
    async fn update_consumed_by(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
        consumed_by: &[Outpoint],
    ) -> Result<(), StorageError> {
        self.note("update_consumed_by");
        self.inner
            .update_consumed_by(txid, output_index, topic, consumed_by)
            .await
    }
    async fn update_transaction_beef(&self, txid: &str, beef: &[u8]) -> Result<(), StorageError> {
        self.note("update_transaction_beef");
        self.inner.update_transaction_beef(txid, beef).await
    }
    async fn mark_transaction_proven(&self, txid: &str) -> Result<(), StorageError> {
        self.note("mark_transaction_proven");
        self.inner.mark_transaction_proven(txid).await
    }
    async fn mark_transaction_proven_at(
        &self,
        txid: &str,
        height: Option<u64>,
    ) -> Result<(), StorageError> {
        self.note("mark_transaction_proven_at");

        self.inner.mark_transaction_proven_at(txid, height).await
    }
    async fn update_output_block_height(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
        block_height: u32,
    ) -> Result<(), StorageError> {
        self.note("update_output_block_height");
        self.inner
            .update_output_block_height(txid, output_index, topic, block_height)
            .await
    }
    async fn insert_applied_transaction(
        &self,
        tx: &AppliedTransaction,
    ) -> Result<(), StorageError> {
        self.note("insert_applied_transaction");
        self.inner.insert_applied_transaction(tx).await
    }
    async fn does_applied_transaction_exist(
        &self,
        tx: &AppliedTransaction,
    ) -> Result<bool, StorageError> {
        self.note("does_applied_transaction_exist");
        self.inner.does_applied_transaction_exist(tx).await
    }
    async fn delete_applied_transaction(
        &self,
        tx: &AppliedTransaction,
    ) -> Result<(), StorageError> {
        self.note("delete_applied_transaction");
        self.inner.delete_applied_transaction(tx).await
    }
    async fn find_output(
        &self,
        txid: &str,
        output_index: u32,
        topic: Option<&str>,
        spent: Option<bool>,
        include_beef: bool,
    ) -> Result<Option<Output>, StorageError> {
        self.note("find_output");
        self.inner
            .find_output(txid, output_index, topic, spent, include_beef)
            .await
    }
    async fn find_served_outputs(
        &self,
        outpoints: &[Outpoint],
    ) -> Result<Vec<Output>, StorageError> {
        self.note("find_served_outputs");
        self.inner.find_served_outputs(outpoints).await
    }

    async fn find_outputs_for_transaction(
        &self,
        txid: &str,
        include_beef: bool,
    ) -> Result<Vec<Output>, StorageError> {
        self.note("find_outputs_for_transaction");
        self.inner
            .find_outputs_for_transaction(txid, include_beef)
            .await
    }
    async fn find_utxos_for_topic(
        &self,
        topic: &str,
        since: Option<f64>,
        limit: Option<u64>,
        include_beef: bool,
    ) -> Result<Vec<Output>, StorageError> {
        self.note("find_utxos_for_topic");
        self.inner
            .find_utxos_for_topic(topic, since, limit, include_beef)
            .await
    }
    async fn find_transactions_for_proof_check(
        &self,
        limit: u64,
        min_age_secs: u64,
    ) -> Result<Vec<crate::storage::TransactionBeef>, StorageError> {
        self.note("find_transactions_for_proof_check");
        self.inner
            .find_transactions_for_proof_check(limit, min_age_secs)
            .await
    }
    async fn update_last_interaction(
        &self,
        host: &str,
        topic: &str,
        since: u64,
    ) -> Result<(), StorageError> {
        self.note("update_last_interaction");
        self.inner.update_last_interaction(host, topic, since).await
    }
    async fn get_last_interaction(&self, host: &str, topic: &str) -> Result<u64, StorageError> {
        self.note("get_last_interaction");
        self.inner.get_last_interaction(host, topic).await
    }
    async fn record_peer_sync_outcome(
        &self,
        host: &str,
        topic: &str,
        success: bool,
    ) -> Result<(), StorageError> {
        self.note("record_peer_sync_outcome");
        self.inner.record_peer_sync_outcome(host, topic, success).await
    }
    async fn get_peer_sync_health(
        &self,
        host: &str,
        topic: &str,
    ) -> Result<crate::storage::PeerSyncHealth, StorageError> {
        self.note("get_peer_sync_health");
        self.inner.get_peer_sync_health(host, topic).await
    }
    async fn record_peer_sync_yield(
        &self,
        host: &str,
        topic: &str,
        yielded: bool,
    ) -> Result<crate::storage::PeerYieldStreak, StorageError> {
        self.note("record_peer_sync_yield");
        self.inner.record_peer_sync_yield(host, topic, yielded).await
    }
    async fn put_deferred_graph(
        &self,
        record: &crate::gasp::DeferredGraph,
    ) -> Result<crate::gasp::DeferredGraphSave, StorageError> {
        self.note("put_deferred_graph");
        self.inner.put_deferred_graph(record).await
    }
    async fn reserve_unanchorable_retry(
        &self,
        record: &crate::gasp::DeferredGraph,
    ) -> Result<bool, StorageError> {
        self.note("reserve_unanchorable_retry");
        self.inner.reserve_unanchorable_retry(record).await
    }
    async fn find_deferred_graphs(
        &self,
        host: &str,
        topic: &str,
    ) -> Result<Vec<crate::gasp::DeferredGraphKey>, StorageError> {
        self.note("find_deferred_graphs");
        self.inner.find_deferred_graphs(host, topic).await
    }
    async fn get_deferred_graph(
        &self,
        host: &str,
        topic: &str,
        outpoint: &str,
    ) -> Result<Option<crate::gasp::DeferredGraph>, StorageError> {
        self.note("get_deferred_graph");
        self.inner.get_deferred_graph(host, topic, outpoint).await
    }
    async fn delete_deferred_graph(
        &self,
        host: &str,
        topic: &str,
        outpoint: &str,
    ) -> Result<(), StorageError> {
        self.note("delete_deferred_graph");
        self.inner.delete_deferred_graph(host, topic, outpoint).await
    }
    async fn remember_expired_outpoint(
        &self,
        topic: &str,
        outpoint: &str,
        expiry: u64,
    ) -> Result<(), StorageError> {
        self.note("remember_expired_outpoint");
        self.inner
            .remember_expired_outpoint(topic, outpoint, expiry)
            .await
    }
    async fn find_expired_outpoints(
        &self,
        topic: &str,
        outpoints: &[String],
    ) -> Result<Vec<String>, StorageError> {
        self.note("find_expired_outpoints");
        self.inner.find_expired_outpoints(topic, outpoints).await
    }
    async fn write_batch(
        &self,
        writes: &[crate::page_batch::PageWrite],
    ) -> Result<u64, StorageError> {
        self.note("write_batch");
        if self.fail_batches.get() > 0 {
            self.fail_batches.set(self.fail_batches.get() - 1);
            return Err(StorageError::Database(
                "a statement of the batch failed; the batch rolled back".into(),
            ));
        }
        self.inner.write_batch(writes).await
    }
    async fn find_topic_outputs(
        &self,
        topic: &str,
        outpoints: &[(String, u32)],
    ) -> Result<Vec<Output>, StorageError> {
        self.note("find_topic_outputs");
        self.inner.find_topic_outputs(topic, outpoints).await
    }
    async fn find_applied_transactions(
        &self,
        topic: &str,
        txids: &[String],
    ) -> Result<Vec<String>, StorageError> {
        self.note("find_applied_transactions");
        self.inner.find_applied_transactions(topic, txids).await
    }
}

impl CountingStorage {
    fn count(&self, name: &str) -> usize {
        self.log.borrow().iter().filter(|n| **n == name).count()
    }
}

/// Put the engine's metered storage back in front of a counting store.
fn counted(engine: &mut Engine) -> CountingStorage {
    let counting = CountingStorage {
        inner: std::rc::Rc::new(MemoryStorage::new()),
        log: std::rc::Rc::default(),
        fail_batches: std::rc::Rc::default(),
    };
    engine.storage = Box::new(crate::load_storage::MeteredStorage(Box::new(
        counting.clone(),
    )));
    counting
}

/// The production shape: a peer budget and a graph budget.
fn page_engine(
    count: u32,
    mode: BatchMode,
) -> (Engine, BatchPeer<BudgetPageRemote>, CountingStorage) {
    let (mut engine, peer, _) = batch_engine(count, mode);
    engine.set_graph_budget(
        std::rc::Rc::new(|_| Box::pin(std::future::pending())),
        100,
        15_000,
    );
    let counting = counted(&mut engine);
    (engine, peer, counting)
}

async fn held(store: &CountingStorage, remote: &BudgetPageRemote) -> usize {
    let mut held = 0;
    for output in remote.outputs.iter() {
        if store
            .inner
            .find_output(&output.txid, 0, Some("tm_test"), None, false)
            .await
            .unwrap()
            .is_some()
        {
            held += 1;
        }
    }
    held
}

/// The page's storage at 0.2.1, per output (`docs/p0` of the lane, the
/// probe at `c0a48f1`): the anchor check's and the submit's reads of the
/// root's input, the dedup read, the output with its transaction, the
/// applied row. Five operations, so a page of 100 was 500.
const PAGE_021_OPERATIONS_PER_OUTPUT: u64 = 5;

/// A page of 100 against a peer that answers the batch: the admissions'
/// reads are two (the page's applied state, the page's held inputs), the
/// writes one batch of 200 statements (each output and its applied row),
/// and nothing of them is asked one output at a time.
#[tokio::test]
async fn page_022_a_page_of_100_costs_the_predicted_statements_and_batches() {
    let (engine, peer, store) = page_engine(100, BatchMode::Answer);
    let result = engine.start_gasp_sync().await.unwrap();
    assert_eq!(result.topics_synced["tm_test"].finalized_graphs, 100);
    assert_eq!(held(&store, &peer.inner).await, 100);
    assert_eq!(result.cost.storage_batches, 1);
    assert_eq!(result.cost.storage_statements, 200);
    for (name, count) in [
        ("write_batch", 1),
        ("find_applied_transactions", 1),
        ("find_topic_outputs", 1),
        ("find_output", 0),
        ("does_applied_transaction_exist", 0),
        ("insert_output_with_origin", 0),
        ("insert_applied_transaction", 0),
    ] {
        assert_eq!(store.count(name), count, "{name}");
    }
    // The sync's whole cost: its own reads and writes (17, as at 0.2.1),
    // the page's two reads and one batch, the list and the body.
    let operations = result.cost.calls_out + result.cost.storage_reads + result.cost.storage_writes;
    assert_eq!(operations, 17 + 2 + 1 + 3);
    assert!(operations < 100 * PAGE_021_OPERATIONS_PER_OUTPUT);
    eprintln!("page_022 a page of 100, batched: {:?}", result.cost);
}

/// A single-only peer's page: its nodes come one at a time, so each root's
/// held input is one read (asked once, where 0.2.1 asked it twice); the
/// applied state is one read for the page and the writes one batch.
#[tokio::test]
async fn page_022_a_single_only_peers_page_is_written_in_one_batch() {
    let (engine, peer, store) = page_engine(100, BatchMode::Single);
    let result = engine.start_gasp_sync().await.unwrap();
    assert_eq!(result.topics_synced["tm_test"].finalized_graphs, 100);
    assert_eq!(held(&store, &peer.inner).await, 100);
    assert_eq!(peer.singles.get(), 100);
    assert_eq!(result.cost.storage_batches, 1);
    assert_eq!(result.cost.storage_statements, 200);
    for (name, count) in [
        ("write_batch", 1),
        ("find_applied_transactions", 1),
        ("find_topic_outputs", 0),
        ("find_output", 100),
        ("does_applied_transaction_exist", 0),
        ("insert_output_with_origin", 0),
    ] {
        assert_eq!(store.count(name), count, "{name}");
    }
}

/// The batch fails: nothing of the page is admitted, the sync names the
/// failure, the persisted cursor stays where it was, and the next run is
/// served the page from the same cursor and admits all of it.
#[tokio::test]
async fn page_022_a_failed_batch_admits_nothing_and_the_next_run_resumes_at_the_cursor() {
    let (engine, peer, store) = page_engine(100, BatchMode::Answer);
    store.fail_batches.set(1);
    let result = engine.start_gasp_sync().await.unwrap();
    let topic = &result.topics_synced["tm_test"];
    assert_eq!(held(&store, &peer.inner).await, 0, "nothing of the page");
    assert!(
        topic
            .errors
            .iter()
            .any(|e| e.contains("the page's storage batch failed")),
        "{:?}",
        topic.errors
    );
    // Handed to submit, as at 0.2.1: the count of graphs submitted, landed
    // or not (the gasp_topic_manager witness `fold_medium1`).
    assert_eq!(topic.finalized_graphs, 100);
    assert_eq!(result.cost.storage_batches, 0);
    let cursor = store
        .inner
        .get_last_interaction(ABSENCE_HOST, "tm_test")
        .await
        .unwrap();
    assert_eq!(cursor, 0, "the cursor where it was");
    let first_since = peer.inner.requests.borrow()[0].since;
    let asked = peer.inner.requests.borrow().len();

    let result = engine.start_gasp_sync().await.unwrap();
    assert_eq!(peer.inner.requests.borrow()[asked].since, first_since);
    assert_eq!(result.topics_synced["tm_test"].finalized_graphs, 100);
    assert_eq!(held(&store, &peer.inner).await, 100);
    assert_eq!(result.cost.storage_batches, 1);
    assert!(
        store
            .inner
            .get_last_interaction(ABSENCE_HOST, "tm_test")
            .await
            .unwrap()
            > cursor
    );
}

/// A page whose middle entry the peer answers `not_found` in its batch:
/// one absence run is counted for it (a record `retry:fault:not_found`, one
/// fault), the other two are admitted. The absence's record is a write the
/// page does not queue, so the root admitted ahead of it is written then
/// (a batch of its own), then the rest at the page's end: the store sees
/// the writes in the order one at a time would have made them.
#[tokio::test]
async fn page_022_a_page_with_an_absent_entry_counts_one_absence_run() {
    let (mut engine, mut remote) = budget_page_engine(3);
    remote.node_cost_ms = 0;
    let txid = remote.outputs[1].txid.clone();
    let node = std::rc::Rc::make_mut(&mut remote.nodes)
        .remove(&txid)
        .unwrap();
    engine.set_gasp_ancestor_fetcher(std::rc::Rc::new(NoRungCourier {
        calls: std::rc::Rc::default(),
    }));
    virtual_peer_budget(&mut engine, &remote);
    engine.set_graph_budget(
        std::rc::Rc::new(|_| Box::pin(std::future::pending())),
        100,
        60_000,
    );
    let absent = AbsencePeer {
        inner: remote,
        withheld: std::rc::Rc::new((txid.clone(), node)),
        serve: std::rc::Rc::new(Cell::new(false)),
        fault: std::rc::Rc::new(Cell::new(false)),
    };
    let peer = BatchPeer::new(absent.clone(), BatchMode::Answer);
    engine.set_gasp_remote_factory(Box::new(peer.clone()));
    let store = counted(&mut engine);
    let result = engine.start_gasp_sync().await.unwrap();
    let topic = &result.topics_synced["tm_test"];
    let root = format!("{txid}.0");
    assert_eq!(topic.finalized_graphs, 2);
    assert_eq!(topic.absent_struck, 0);
    let named: Vec<_> = topic
        .faulted_roots
        .iter()
        .map(|r| (r.outpoint.clone(), r.class.clone(), r.faults))
        .collect();
    assert_eq!(
        named,
        vec![(root.clone(), crate::gasp::fault_class::NOT_FOUND.to_string(), 1)]
    );
    let record = store
        .inner
        .get_deferred_graph(ABSENCE_HOST, "tm_test", &root)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (record.reason.as_str(), record.faults),
        ("retry:fault:not_found", 1)
    );
    assert_eq!(held(&store, &absent.inner).await, 2);
    assert_eq!(result.cost.storage_batches, 2);
    assert_eq!(result.cost.storage_statements, 4);
    let log = store.log.borrow();
    let batches: Vec<_> = log
        .iter()
        .enumerate()
        .filter(|(_, n)| **n == "write_batch")
        .map(|(i, _)| i)
        .collect();
    let record_at = log.iter().position(|n| *n == "put_deferred_graph").unwrap();
    assert!(batches[0] < record_at && record_at < batches[1], "{log:?}");
}

/// The allowance walks pages, not outputs: production's measured walk
/// (1,339 outputs, `docs/p0/own-peer-sync-2026-10-14.md`) from a peer that
/// answers the batch, its list served 100 a page, is ONE backfill call
/// under the unchanged allowance of 500 operations (0.2.1's storage was 5
/// operations an output, about 85 outputs a call). Each page costs its
/// list, its body, its two reads, its batch and its sync's own bookkeeping.
#[tokio::test]
async fn page_022_the_measured_walk_is_one_call_under_the_allowance() {
    let (mut engine, mut remote) = budget_page_engine(1339);
    remote.node_cost_ms = 0;
    remote.clamp = Some(crate::gasp::ENGINE_GASP_SYNC_PAGE_LIMIT as usize);
    let meter = minutes_clock(&mut engine, &remote);
    let peer = BatchPeer::new(remote.clone(), BatchMode::Answer);
    engine.set_gasp_remote_factory(Box::new(peer.clone()));
    let store = counted(&mut engine);
    let result = engine
        .start_gasp_backfill_metered(&minutes_backfill_request(180_000), &meter)
        .await
        .unwrap();
    let cost = result.sync.cost;
    eprintln!(
        "page_022 the measured walk: slices {} stop {} cost {cost:?}",
        result.slices, result.stop_reason
    );
    assert!(result.reached_tip, "{}", result.stop_reason);
    assert!(result.continuation.is_none());
    assert_eq!(
        result.sync.topics_synced["tm_test"].finalized_graphs,
        1339
    );
    assert_eq!(held(&store, &remote).await, 1339);
    let operations = cost.calls_out + cost.storage_reads + cost.storage_writes;
    assert!(
        operations < crate::backfill::BACKFILL_OPERATIONS,
        "{operations}"
    );
    // A backfill slice is one page and one whole peer sync (its health,
    // cadence, cursor, memo and known-output reads and writes, 17 at most in
    // the probe of a page of 100): with the page's own list, body, two reads
    // and batch, about 22 a slice of 100 outputs, against 0.2.1's 500 and
    // more for the page's admissions alone.
    assert_eq!(cost.storage_batches, result.slices, "one batch a page");
    assert_eq!(result.slices, 1339_u64.div_ceil(99), "100 a page, the boundary row repeated");
    assert_eq!(cost.storage_statements, 2 * 1339);
    assert!(
        operations <= 22 * result.slices,
        "{operations} for {} slices",
        result.slices
    );
}
