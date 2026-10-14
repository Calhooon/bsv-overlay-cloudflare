//! GASP's shadow under `admit_graph` (bsv-low #657, the design's R7.7 item 5;
//! 3.3a "The GASP gap"; test 29's "not shadowed" rows).
//!
//! The engine's own GASP finalize submits had no observer: Zanaadu's census
//! marks the HTTP corpus's four GASP chaos records "not shadowed". Under
//! `AdmissionMode::Shadow` every finalized body gets one census row (A's walk
//! handed the served path's evidence, `judge_dry` beside the served
//! judgement, the body's unit through `admit_graph`, on a fresh snapshot) and
//! every graph one more (A's `admit_graph` over the whole graph, its rows
//! against the rows the served submits left), and nothing served moves.

#[path = "support/gasp_shadow_world.rs"]
mod world;

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use async_trait::async_trait;
use bsv_overlay_engine::admission::WalkOutcome;
use bsv_overlay_engine::engine::{Engine, EngineConfig};
use bsv_overlay_engine::gasp_shadow::{
    AdmissionMode, DeltaClass, GaspCensusRow, GaspShadow, MemoryGaspShadow, ShadowBudget,
    ShadowKind, ShadowScope, ShadowSnapshot,
};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_overlay_engine::storage::Storage;
use bsv_overlay_engine::types::GASPNode;
use bsv_rs::transaction::Transaction;
use world::*;

/// One scenario of proven transactions a peer serves over some ticks.
struct Scenario {
    txs: Vec<Transaction>,
    nodes: Vec<GASPNode>,
    /// (index into `txs`, vout, score)
    listed: Vec<(usize, u32, f64)>,
    tracker: Option<KnownRoots>,
    fails_at_finalize: Option<String>,
    budgeted: bool,
    /// Before each tick: what the peer does in it.
    ticks: Vec<fn(&ChaosRemote, &[Transaction])>,
}

impl Scenario {
    fn honest(txs: Vec<Transaction>, listed: Vec<(usize, u32, f64)>) -> Self {
        let nodes = txs
            .iter()
            .enumerate()
            .map(|(i, tx)| node(tx, honest_proof(&tx.id(), 100 + i as u32)))
            .collect();
        Self {
            txs,
            nodes,
            listed,
            tracker: None,
            fails_at_finalize: None,
            budgeted: false,
            ticks: vec![|_, _| {}],
        }
    }
}

/// What a run served, and what the shadow recorded beside it.
struct Run {
    /// Every peer call, in order.
    peer: Vec<String>,
    /// Each tick's sync result.
    results: Vec<String>,
    /// The served store's rows of every transaction, and the cursor.
    rows: BTreeMap<String, String>,
    census: Vec<GaspCensusRow>,
    snapshots: usize,
}

impl Run {
    fn served(&self) -> (&Vec<String>, &Vec<String>, &BTreeMap<String, String>) {
        (&self.peer, &self.results, &self.rows)
    }
    fn of(&self, kind: ShadowKind) -> Vec<&GaspCensusRow> {
        self.census.iter().filter(|r| r.kind == kind).collect()
    }
}

/// The shadow's engine: the same manager rule over the snapshot's copy, and
/// no tracker, broadcaster, fetcher or peer factory.
fn shadow_host(served: &Rc<MemoryStorage>, fails_at_finalize: Option<String>) -> MemoryGaspShadow {
    MemoryGaspShadow::new(served.clone(), move |store| {
        Engine::new(
            managers(fails_at_finalize.clone()),
            HashMap::new(),
            Box::new(store),
            None,
            EngineConfig::default(),
        )
    })
}

async fn run(
    scenario: &Scenario,
    mode: Option<AdmissionMode>,
    budget: Option<ShadowBudget>,
    clock: Option<fn() -> u64>,
) -> Run {
    let events = Events::default();
    let listed: Vec<_> = scenario
        .listed
        .iter()
        .map(|(i, vout, score)| (&scenario.txs[*i], *vout, *score))
        .collect();
    let remote = ChaosRemote::new(&scenario.nodes, &listed, &events);
    let store = Rc::new(MemoryStorage::new());
    let mut engine = served_engine(
        Box::new(store.clone()),
        &remote,
        scenario.tracker.clone(),
        scenario.fails_at_finalize.clone(),
    );
    if scenario.budgeted {
        engine.set_peer_sync_budget(never_due(), 30_000);
    }
    let host = mode.map(|mode| {
        let mut host = shadow_host(&store, scenario.fails_at_finalize.clone());
        if let Some(clock) = clock {
            host = host.with_clock(clock);
        }
        let host = Rc::new(host);
        engine.set_gasp_shadow(mode, host.clone());
        host
    });
    if let Some(budget) = budget {
        engine.set_gasp_shadow_budget(budget);
    }
    let mut results = Vec::new();
    for setup in &scenario.ticks {
        setup(&remote, &scenario.txs);
        let result = engine.start_gasp_sync().await.unwrap();
        let mut topics: Vec<_> = result.topics_synced.iter().collect();
        topics.sort_by_key(|(topic, _)| topic.as_str());
        results.push(serde_json::to_string(&topics).unwrap());
    }
    let txs: Vec<_> = scenario.txs.iter().collect();
    let mut rows = served_rows(store.as_ref(), &txs).await;
    rows.insert(
        "cursor".into(),
        store
            .get_last_interaction(PEER, TOPIC)
            .await
            .unwrap()
            .to_string(),
    );
    let peer = events.borrow().clone();
    Run {
        peer,
        results,
        rows,
        census: host.as_ref().map_or_else(Vec::new, |h| h.census()),
        snapshots: host.as_ref().map_or(0, |h| h.snapshots()),
    }
}

/// The off run and the shadow run of one scenario: the served side equal.
async fn beside(scenario: &Scenario) -> Run {
    let off = run(scenario, None, None, None).await;
    let shadow = run(scenario, Some(AdmissionMode::Shadow), None, None).await;
    assert_eq!(
        off.served(),
        shadow.served(),
        "the shadow changed nothing served: the peer's calls, the sync results, the rows and the cursor"
    );
    shadow
}

fn print_census(name: &str, run: &Run) {
    for row in &run.census {
        println!(
            "{name}: {:?} {} pos={} served={} shadow={} delta={:?} rows={} bytes={}",
            row.kind,
            &row.txid[..12],
            row.position,
            row.served.status,
            row.shadow.status,
            row.delta,
            row.snapshot_rows,
            row.snapshot_bytes,
        );
    }
}

/// The recorded world's transactions, in its own order (`world::recording`).
fn recorded_world() -> Scenario {
    let parent = genesis(1, 2);
    let left = child(&parent, 0, 1);
    let right = child(&parent, 1, 1);
    let lone = genesis(2, 1);
    let blip = genesis(3, 1);
    let low = genesis(4, 1);
    let high = child(&low, 0, 1);
    let mut scenario = Scenario::honest(
        vec![parent, left, right, lone, blip, low, high],
        vec![
            (1, 0, 1.0),
            (2, 0, 2.0),
            (3, 0, 3.0),
            (4, 0, 4.0),
            (6, 0, 5.0),
        ],
    );
    scenario.ticks = vec![
        |remote, txs| {
            remote.faults.borrow_mut().insert(txs[4].id());
        },
        |remote, _| remote.faults.borrow_mut().clear(),
    ];
    scenario
}

// ============================================================================
// Witness 1: one census row per body, the shadow's answer beside the served.
// ============================================================================

#[tokio::test]
async fn w1_each_finalized_body_has_one_census_row_with_the_shadows_answer_beside_the_served() {
    for budgeted in [false, true] {
        let mut scenario = recorded_world();
        scenario.budgeted = budgeted;
        let shadow = beside(&scenario).await;
        print_census(&format!("W1 budgeted={budgeted}"), &shadow);
        let id = |i: usize| scenario.txs[i].id();
        // Tick 0: [parent, left], [parent, right] (the parent a duplicate
        // body: both graphs are walked before either is submitted),
        // [lone], [low, high]; the blip held. Tick 1: [blip]. Under a
        // per-peer budget each graph is submitted as it finalizes, so the
        // parent is held when the second graph is walked and is stripped.
        let mut expected = vec![(id(0), 0), (id(1), 1)];
        if budgeted {
            expected.push((id(2), 0));
        } else {
            expected.extend([(id(0), 0), (id(2), 1)]);
        }
        expected.extend([(id(3), 0), (id(5), 0), (id(6), 1), (id(4), 0)]);
        let bodies: Vec<_> = shadow
            .of(ShadowKind::Body)
            .iter()
            .map(|r| (r.txid.clone(), r.position))
            .collect();
        assert_eq!(
            bodies, expected,
            "the census holds one row per finalized body, in the served order"
        );
        for row in shadow.of(ShadowKind::Body) {
            assert_eq!(row.peer, PEER);
            assert_eq!(row.topic, TOPIC);
            assert_eq!(row.served.status, 200, "{row:?}");
            assert_ne!(row.shadow.status, 0, "the shadow judged it: {row:?}");
            assert_eq!(row.delta, DeltaClass::Same, "{row:?}");
            assert!(row.served_judgement.is_some() && row.shadow_judgement.is_some());
            assert_eq!(
                row.served_judgement, row.shadow_judgement,
                "judge_dry beside the served judgement"
            );
            // The served finalize gathers no evidence; the shadow's walk
            // carried only its own verdict and asked nobody else.
            assert!(row.served_evidence.tx.is_empty());
            let evidence = &row.shadow_evidence.tx[&row.txid];
            assert_eq!(evidence.walk, WalkOutcome::HistoricalTxNoSpv);
            assert!(evidence.network.is_none() && evidence.proof.is_none());
        }
        if !budgeted {
            // The duplicate body: the served dedup and A's dedup, one word.
            let dupe = &shadow.of(ShadowKind::Body)[2];
            assert_eq!(dupe.served, dupe.shadow);
            assert!(String::from_utf8_lossy(&dupe.served.bytes).contains("outputsToAdmit\":[]"));
        }
        // One snapshot per body and one per graph, and no other.
        assert_eq!(shadow.snapshots, expected.len() + 5);
    }
}

// ============================================================================
// Witness 2: the finalize hop's one materialization, compared.
// ============================================================================

#[tokio::test]
async fn w2_the_finalize_materialization_is_compared_to_the_served_materialization() {
    let scenario = recorded_world();
    let shadow = beside(&scenario).await;
    let graphs = shadow.of(ShadowKind::Materialization);
    print_census("W2", &shadow);
    let id = |i: usize| scenario.txs[i].id();
    assert_eq!(
        graphs
            .iter()
            .map(|r| (r.txid.clone(), r.position))
            .collect::<Vec<_>>(),
        vec![(id(1), 2), (id(2), 2), (id(3), 1), (id(6), 2), (id(4), 1)],
        "one materialization row per finalized graph, named by its root"
    );
    for row in &graphs {
        assert_eq!(row.served, row.shadow, "{row:?}");
        assert_eq!(row.delta, DeltaClass::Same);
        let word: serde_json::Value = serde_json::from_slice(&row.served.bytes).unwrap();
        assert!(word["rows"]
            .as_object()
            .is_some_and(|rows| !rows.is_empty()));
        assert!(word["stopped"].is_null());
    }

    // A manager that errs at the finalize on the middle link: the served
    // finalize stops there (its graph's cursor waits); A's unit commits the
    // topic's empty judgement (F5, C7) and its graph goes on to the next
    // link, which waits for its unlanded predecessor. Each body is the same
    // word; the graph is not.
    let g = genesis(7, 1);
    let c = child(&g, 0, 1);
    let d = child(&c, 0, 1);
    let mut scenario = Scenario::honest(vec![g, c, d], vec![(2, 0, 1.0)]);
    scenario.fails_at_finalize = Some(scenario.txs[1].id());
    let shadow = beside(&scenario).await;
    print_census("W2 delta", &shadow);
    let graph = shadow.of(ShadowKind::Materialization);
    assert_eq!(graph.len(), 1);
    assert_eq!(
        graph[0].delta,
        DeltaClass::Materialization,
        "{:?}",
        graph[0]
    );
    let served: serde_json::Value = serde_json::from_slice(&graph[0].served.bytes).unwrap();
    let ours: serde_json::Value = serde_json::from_slice(&graph[0].shadow.bytes).unwrap();
    assert_eq!(served["landed"], serde_json::json!([scenario.txs[0].id()]));
    assert_eq!(served["stopped"], serde_json::json!(scenario.txs[1].id()));
    assert_eq!(
        ours["landed"],
        serde_json::json!([scenario.txs[0].id(), scenario.txs[1].id()])
    );
    assert_eq!(ours["stopped"], serde_json::json!(scenario.txs[2].id()));
    assert_eq!(
        served["rows"], ours["rows"],
        "the rows agree; the landing does not"
    );
    let bodies = shadow.of(ShadowKind::Body);
    assert_eq!(bodies.len(), 2, "the served finalize submitted two bodies");
    assert!(bodies.iter().all(|r| r.delta == DeltaClass::Same));
}

/// The w2 chain `g <- c <- d`, the manager erring on `c` at the finalize.
fn erring_chain() -> Scenario {
    let g = genesis(7, 1);
    let c = child(&g, 0, 1);
    let d = child(&c, 0, 1);
    let mut scenario = Scenario::honest(vec![g, c, d], vec![(2, 0, 1.0)]);
    scenario.fails_at_finalize = Some(scenario.txs[1].id());
    scenario
}

fn json(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes)
        .unwrap_or_else(|e| panic!("not JSON ({e}): {}", String::from_utf8_lossy(bytes)))
}

/// The shadow lens's MEDIUM 2: a manager error at the finalize is seen in the
/// census. The erring body's row carries `applied_topics`, `deduped_topics`
/// and the served not-landed reason in its words' bytes and names the manager
/// and its error on both sides; the graph row carries the stopping body's
/// whole word, and its status is 200 only when nothing stopped.
#[tokio::test]
async fn w2b_a_manager_error_at_the_finalize_shows_in_the_census() {
    let scenario = erring_chain();
    let shadow = beside(&scenario).await;
    print_census("W2b", &shadow);
    let c = scenario.txs[1].id();
    let manager = format!("topic_manager.{TOPIC}");
    let error = "the finalize refuses this body";

    let row = shadow
        .of(ShadowKind::Body)
        .into_iter()
        .find(|r| r.txid == c)
        .expect("the erring body has its row");
    let served = json(&row.served.bytes);
    let ours = json(&row.shadow.bytes);
    for (side, word) in [("served", &served), ("shadow", &ours)] {
        assert_eq!(
            word["applied_topics"],
            serde_json::json!([]),
            "{side}: {word}"
        );
        assert_eq!(
            word["deduped_topics"],
            serde_json::json!([]),
            "{side}: {word}"
        );
        let errors = word["manager_errors"].to_string();
        assert!(
            errors.contains(&manager) && errors.contains(error),
            "{side} names the manager and its error: {word}"
        );
    }
    assert_eq!(
        served["not_landed"],
        serde_json::json!("the topic manager failed on it"),
        "the served word says why the finalize stopped: {served}"
    );
    assert_eq!(
        ours["not_landed"],
        serde_json::Value::Null,
        "A committed the unit: {ours}"
    );
    assert_ne!(
        row.delta,
        DeltaClass::Same,
        "the served finalize stopped at this body and A went on: {row:?}"
    );

    let graph = &shadow.of(ShadowKind::Materialization)[0];
    let served = json(&graph.served.bytes);
    assert_ne!(
        graph.served.status, 200,
        "the served graph stopped: {graph:?}"
    );
    assert_eq!(served["stopped"]["txid"], serde_json::json!(c), "{served}");
    assert_eq!(
        json(served["stopped"]["bytes"].as_str().unwrap().as_bytes())["not_landed"],
        serde_json::json!("the topic manager failed on it"),
        "the graph word carries the stopping body's whole word: {served}"
    );
    assert_eq!(served["stopped"]["status"], serde_json::json!(200));
    assert!(
        served["manager_errors"].to_string().contains(error),
        "{served}"
    );
}

// ============================================================================
// Witness 3: the four GASP chaos records of the HTTP corpus
// (`parity-harness/corpus/xanaverse/chaos/0{1..4}_*.json`, Zanaadu), each a
// census row and never a served change.
// ============================================================================

fn assert_shadowed(run: &Run, bodies: usize, graphs: usize) {
    assert_eq!(
        (
            run.of(ShadowKind::Body).len(),
            run.of(ShadowKind::Materialization).len()
        ),
        (bodies, graphs),
        "one census row per finalized body and one per graph"
    );
    assert!(run
        .census
        .iter()
        .all(|r| r.delta != DeltaClass::NotShadowed));
}

/// 01_downtime_recovery (the re-sent range): the peer is down for a tick and
/// re-serves the whole range on the next, a duplicate body among it.
#[tokio::test]
async fn w3_chaos_01_downtime_recovery() {
    let parent = genesis(11, 2);
    let a = child(&parent, 0, 1);
    let b = child(&parent, 1, 1);
    let mut scenario = Scenario::honest(
        vec![parent, a, b, genesis(12, 1), genesis(13, 1)],
        vec![(1, 0, 1.0), (2, 0, 2.0), (3, 0, 3.0), (4, 0, 4.0)],
    );
    scenario.ticks = vec![|remote, _| remote.down.set(true), |remote, _| {
        remote.down.set(false)
    }];
    let shadow = beside(&scenario).await;
    print_census("01_downtime_recovery", &shadow);
    assert!(shadow.results[0].contains("peer down"), "tick 1 fails safe");
    assert_shadowed(&shadow, 6, 4);
    assert_eq!(shadow.census[0].txid, scenario.txs[0].id());
}

/// 02_partition_heal (the partial listing): the high-score half is withheld
/// for a tick, then served.
#[tokio::test]
async fn w3_chaos_02_partition_heal() {
    let txs: Vec<_> = (0..6).map(|n| genesis(20 + n, 1)).collect();
    let listed = (0..6).map(|i| (i, 0, (i + 1) as f64)).collect();
    let mut scenario = Scenario::honest(txs, listed);
    scenario.ticks = vec![
        |remote, _| remote.withhold_from.set(Some(4.0)),
        |remote, _| remote.withhold_from.set(None),
    ];
    let shadow = beside(&scenario).await;
    print_census("02_partition_heal", &shadow);
    assert_shadowed(&shadow, 6, 6);
    let order: Vec<_> = shadow
        .of(ShadowKind::Body)
        .iter()
        .map(|r| r.txid.clone())
        .collect();
    assert_eq!(
        order,
        scenario.txs.iter().map(Transaction::id).collect::<Vec<_>>(),
        "the visible half on the split tick, the rest on the heal"
    );
}

/// 03_chain_fork: the canonical branch admits; the orphaned tip, whose proof
/// no tracker knows, is discarded by the anchor check and never finalized,
/// so it is never shadowed either.
#[tokio::test]
async fn w3_chaos_03_chain_fork() {
    let txs: Vec<_> = (0..4).map(|n| genesis(30 + n, 1)).collect();
    let mut scenario = Scenario::honest(
        txs,
        vec![(0, 0, 10.0), (1, 0, 20.0), (2, 0, 25.0), (3, 0, 30.0)],
    );
    scenario.tracker = Some(KnownRoots::of(&scenario.nodes));
    let orphan = scenario.txs[2].id();
    scenario.nodes[2].proof = Some(fabricated_proof(&orphan, 102));
    let shadow = beside(&scenario).await;
    print_census("03_chain_fork", &shadow);
    assert!(shadow.results[0].contains("\"discarded_graphs\":1"));
    assert_shadowed(&shadow, 3, 3);
    assert!(shadow.census.iter().all(|r| r.txid != orphan));
}

/// 04_cursor_no_skip_on_transient (the cursor skip): the middle UTXO's node
/// blips on the first tick; the cursor stays below it and the next tick
/// admits it.
#[tokio::test]
async fn w3_chaos_04_cursor_no_skip_on_transient() {
    let txs: Vec<_> = (0..3).map(|n| genesis(40 + n, 1)).collect();
    let mut scenario = Scenario::honest(txs, vec![(0, 0, 10.0), (1, 0, 20.0), (2, 0, 30.0)]);
    scenario.ticks = vec![
        |remote, txs| {
            remote.faults.borrow_mut().insert(txs[1].id());
        },
        |remote, _| remote.faults.borrow_mut().clear(),
    ];
    let shadow = beside(&scenario).await;
    print_census("04_cursor_no_skip_on_transient", &shadow);
    assert!(shadow.rows["cursor"].parse::<u64>().unwrap() >= 20);
    assert_shadowed(&shadow, 3, 3);
    let order: Vec<_> = shadow
        .of(ShadowKind::Body)
        .iter()
        .map(|r| r.txid.clone())
        .collect();
    assert_eq!(
        order,
        [0, 2, 1].map(|i| scenario.txs[i].id()).to_vec(),
        "the blipped UTXO is shadowed on the tick that admits it"
    );
}

// ============================================================================
// Off is A1's byte for byte; the budget's row; the mode's words.
// ============================================================================

/// A host that must never be asked.
struct Untouchable;
#[async_trait(?Send)]
impl GaspShadow for Untouchable {
    async fn snapshot(
        &self,
        _: &ShadowScope<'_>,
        _: &ShadowBudget,
    ) -> Result<ShadowSnapshot, String> {
        panic!("off took a snapshot")
    }
    fn now_ms(&self) -> u64 {
        panic!("off read the shadow's clock")
    }
    async fn record(&self, _: &GaspCensusRow) -> Result<(), String> {
        panic!("off recorded a census row")
    }
}

#[test]
fn w4_off_with_a_host_set_is_a1s_recording_timing_included() {
    let (transcript, polls) = recording(&|engine, _| {
        engine.set_gasp_shadow(AdmissionMode::Off, Rc::new(Untouchable));
    });
    assert_eq!(digest(&transcript), OFF_A1_DIGEST);
    assert_eq!(polls, OFF_A1_POLLS);
}

#[tokio::test]
async fn w5_past_the_budget_the_row_says_not_shadowed_budget_and_nothing_served_moves() {
    let scenario = recorded_world();
    let off = run(&scenario, None, None, None).await;
    // Rows: every snapshot of a store that holds anything is past 0 rows.
    let rows = run(
        &scenario,
        Some(AdmissionMode::Shadow),
        Some(ShadowBudget {
            rows: 0,
            ..ShadowBudget::default()
        }),
        None,
    )
    .await;
    assert_eq!(off.served(), rows.served());
    assert_eq!(rows.census.len(), 13);
    for row in &rows.census {
        let word = String::from_utf8_lossy(&row.shadow.bytes).to_string();
        if row.snapshot_rows > 0 {
            assert_eq!(row.delta, DeltaClass::NotShadowed);
            assert_eq!(row.shadow.status, 0);
            assert!(word.starts_with("not shadowed: budget"), "{word}");
        }
    }
    assert!(rows
        .census
        .iter()
        .any(|r| r.delta == DeltaClass::NotShadowed));

    // Time: a clock that runs 10 s a read is past 2 s on every row.
    thread_local!(static NOW: Cell<u64> = const { Cell::new(0) });
    let time = run(
        &scenario,
        Some(AdmissionMode::Shadow),
        None,
        Some(|| {
            NOW.with(|n| {
                n.set(n.get() + 10_000);
                n.get()
            })
        }),
    )
    .await;
    assert_eq!(off.served(), time.served());
    assert_eq!(time.census.len(), 13);
    for row in &time.census {
        let word = String::from_utf8_lossy(&row.shadow.bytes).to_string();
        assert!(word.starts_with("not shadowed: budget (time"), "{word}");
    }
}

#[test]
fn w6_the_mode_is_the_workers_word() {
    assert_eq!(AdmissionMode::parse(None), Ok(AdmissionMode::Off));
    assert_eq!(AdmissionMode::parse(Some("off")), Ok(AdmissionMode::Off));
    assert_eq!(
        AdmissionMode::parse(Some("shadow")),
        Ok(AdmissionMode::Shadow)
    );
    assert!(AdmissionMode::parse(Some("on"))
        .unwrap_err()
        .contains("is not enabled"));
}
