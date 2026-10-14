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

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::rc::{Rc, Weak};

use async_trait::async_trait;
use bsv_overlay_engine::admission::{
    AdmitRefusal, UnitRunner, WalkOutcome, GATE_PARENT_ERROR_SITE, INLINE_BEEF_MAX_BYTES,
};
use bsv_overlay_engine::engine::{Engine, EngineConfig};
use bsv_overlay_engine::gasp_shadow::{
    AdmissionMode, DeltaClass, GaspCensusRow, GaspShadow, MemoryGaspShadow, ShadowBudget,
    ShadowKind, ShadowScope, ShadowSnapshot, SHADOW_SNAPSHOTS_HELD,
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
    /// The shadow's host cannot resolve an R2 BEEF (its read faults), so a
    /// body past the inline bound is refused at its walk.
    r2_faults: bool,
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
            r2_faults: false,
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
    /// The most snapshots alive at once (taken and not yet compared).
    peak_held: usize,
}

impl Run {
    fn served(&self) -> (&Vec<String>, &Vec<String>, &BTreeMap<String, String>) {
        (&self.peer, &self.results, &self.rows)
    }
    fn of(&self, kind: ShadowKind) -> Vec<&GaspCensusRow> {
        self.census.iter().filter(|r| r.kind == kind).collect()
    }
}

/// The reference host, counting the snapshots alive (each one's store is
/// dropped once its comparison ran) and their peak.
struct Measured {
    inner: MemoryGaspShadow,
    live: RefCell<Vec<Weak<dyn UnitRunner>>>,
    peak: Cell<usize>,
    r2_faults: bool,
}

impl Measured {
    fn new(inner: MemoryGaspShadow) -> Self {
        Self {
            inner,
            live: RefCell::default(),
            peak: Cell::default(),
            r2_faults: false,
        }
    }
}

#[async_trait(?Send)]
impl GaspShadow for Measured {
    async fn snapshot(
        &self,
        scope: &ShadowScope<'_>,
        budget: &ShadowBudget,
    ) -> Result<ShadowSnapshot, String> {
        let mut snapshot = self.inner.snapshot(scope, budget).await?;
        if self.r2_faults {
            snapshot.resolve = Box::new(|_, _| {
                Err(AdmitRefusal::transient(
                    "r2_resolve",
                    "the host's read of the R2 BEEF faulted",
                ))
            });
        }
        let mut live = self.live.borrow_mut();
        live.retain(|w| w.strong_count() > 0);
        live.push(Rc::downgrade(&snapshot.runner));
        self.peak.set(self.peak.get().max(live.len()));
        Ok(snapshot)
    }
    fn now_ms(&self) -> u64 {
        self.inner.now_ms()
    }
    async fn record(&self, row: &GaspCensusRow) -> Result<(), String> {
        self.inner.record(row).await
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
        let mut host = Measured::new(host);
        host.r2_faults = scenario.r2_faults;
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
        census: host.as_ref().map_or_else(Vec::new, |h| h.inner.census()),
        snapshots: host.as_ref().map_or(0, |h| h.inner.snapshots()),
        peak_held: host.as_ref().map_or(0, |h| h.peak.get()),
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
        // One snapshot per body, the graph's shared with its first body (the
        // shadow lens's NOTE 6: one state, one copy), and no other.
        assert_eq!(shadow.snapshots, expected.len(), "one snapshot per body");
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
        assert_eq!(row.served.status, 200, "nothing stopped");
    }

    // A manager that errs at the finalize on the middle link: the served
    // finalize stops there (its graph's cursor waits); under the owner's D44
    // (`dependency gate`) A refuses the erring unit `not now` and its spender
    // waits on it. Both land `g` alone: the rows agree, the words do not.
    let scenario = erring_chain();
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
    assert_eq!(
        served["stopped"]["txid"],
        serde_json::json!(scenario.txs[1].id())
    );
    assert_eq!(ours["landed"], serde_json::json!([scenario.txs[0].id()]));
    assert_eq!(
        ours["stopped"]["txid"],
        serde_json::json!(scenario.txs[1].id()),
        "A's first held body is the erring one"
    );
    assert_eq!(ours["stopped"]["status"], serde_json::json!(503));
    assert_eq!(
        served["rows"], ours["rows"],
        "the rows agree; the words do not"
    );
    let bodies = shadow.of(ShadowKind::Body);
    assert_eq!(bodies.len(), 2, "the served finalize submitted two bodies");
    assert_eq!(
        bodies.iter().map(|r| r.delta).collect::<Vec<_>>(),
        [DeltaClass::Same, DeltaClass::Words],
        "the erring body's word says the served finalize stopped there"
    );
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
        let errors = word["manager_errors"].to_string();
        assert!(
            errors.contains(&manager) && errors.contains(error),
            "{side} names the manager and its error: {word}"
        );
    }
    assert_eq!(served["applied_topics"], serde_json::json!([]), "{served}");
    assert_eq!(served["deduped_topics"], serde_json::json!([]), "{served}");
    assert_eq!(
        served["not_landed"],
        serde_json::json!("the topic manager failed on it"),
        "the served word says why the finalize stopped: {served}"
    );
    assert_eq!(
        row.shadow.status, 503,
        "D44: A refuses the erring unit not now: {ours}"
    );
    assert!(
        ours["refusal"]
            .as_str()
            .is_some_and(|r| r.starts_with(GATE_PARENT_ERROR_SITE)),
        "the refusal is the gate's: {ours}"
    );
    assert_ne!(
        row.delta,
        DeltaClass::Same,
        "the served finalize counted the body not landed, A refused it: {row:?}"
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

/// A scenario whose served finalize order is a `HashMap`'s (a fan-in's
/// siblings): the shadow run beside an `off` run that took the same order
/// (the same rows), tried up to 64 times; the peer's calls compared as a
/// multiset (their order is the same `HashMap`'s).
async fn beside_unordered(scenario: &Scenario) -> Run {
    let shadow = run(scenario, Some(AdmissionMode::Shadow), None, None).await;
    let sorted = |run: &Run| {
        let mut calls = run.peer.clone();
        calls.sort();
        calls
    };
    for _ in 0..64 {
        let off = run(scenario, None, None, None).await;
        if off.rows == shadow.rows {
            assert_eq!(
                (sorted(&off), &off.results),
                (sorted(&shadow), &shadow.results),
                "the shadow changed nothing served"
            );
            return shadow;
        }
    }
    panic!("no off run took the shadow run's served order in 64 tries");
}

/// `r` spends `a.0` and `b.0` (all proven); the manager errs at the finalize
/// on the parent `erring` (0: `a`, 1: `b`).
fn erring_fan_in(erring: usize) -> Scenario {
    let a = genesis(50, 1);
    let b = genesis(51, 1);
    let r = spends(&[(&a, 0), (&b, 0)], 1);
    let mut scenario = Scenario::honest(vec![a, b, r], vec![(2, 0, 1.0)]);
    scenario.fails_at_finalize = Some(scenario.txs[erring].id());
    scenario
}

/// The shadow lens's MEDIUM 3: the two materialization deltas apart. D1 (the
/// erring chain): the stop point differs and the rows agree, `materialization`.
/// D2 (a fan-in, the manager erring on either parent), under the owner's D44
/// (`dependency gate`): A lands the sibling and holds the erring parent and
/// the root; the served finalize stops at the erring parent. When the erring
/// parent comes first the served path lands nothing after it and A lands the
/// sibling: the rows differ, `rows_differ`. When it comes second both land
/// the sibling alone: the rows agree, `materialization`.
#[tokio::test]
async fn w2c_the_census_names_a_stop_delta_and_a_rows_delta_apart() {
    let class = |row: &GaspCensusRow| serde_json::to_value(row.delta).unwrap();
    let chain = beside(&erring_chain()).await;
    let graph = &chain.of(ShadowKind::Materialization)[0];
    assert_eq!(
        class(graph),
        serde_json::json!("materialization"),
        "D1: {graph:?}"
    );
    let served = json(&graph.served.bytes);
    let ours = json(&graph.shadow.bytes);
    assert_eq!(served["rows"], ours["rows"], "D1: the rows agree");

    for erring in [0, 1] {
        let scenario = erring_fan_in(erring);
        let shadow = beside_unordered(&scenario).await;
        print_census(&format!("W2c fan-in erring={erring}"), &shadow);
        let [bad, good, r] = [erring, 1 - erring, 2].map(|i| scenario.txs[i].id());
        let graph = &shadow.of(ShadowKind::Materialization)[0];
        let served = json(&graph.served.bytes);
        let ours = json(&graph.shadow.bytes);
        assert_eq!(
            ours["landed"],
            serde_json::json!([good]),
            "A lands the sibling alone: {ours}"
        );
        for (tx, held) in [(&bad, "false"), (&good, "true"), (&r, "false")] {
            assert_eq!(
                ours["rows"][format!("applied {tx}")],
                serde_json::json!(held),
                "{ours}"
            );
        }
        assert_eq!(
            served["rows"][format!("applied {r}")],
            serde_json::json!("false"),
            "the served finalize never lands the root: {served}"
        );
        let erring_first = served["landed"] == serde_json::json!([]);
        if erring_first {
            assert_ne!(served["rows"], ours["rows"], "D2: the rows differ");
            assert_eq!(
                class(graph),
                serde_json::json!("rows_differ"),
                "D2 erring={erring}: served {served}, shadow {ours}"
            );
        } else {
            assert_eq!(
                served["rows"], ours["rows"],
                "the sibling first: the rows agree"
            );
            assert_eq!(
                class(graph),
                serde_json::json!("materialization"),
                "erring={erring}, the sibling first: served {served}, shadow {ours}"
            );
        }
    }
}

/// The owner's D44 in the census: on the fan-in with the erring parent first
/// (the served finalize lands nothing after the error; A lands the sibling),
/// the graph row stays `rows_differ` against today's path and carries the
/// gate's reason per body it held, in graph order: the erring parent
/// `parent_error`, the root `{"waiting_on": [the erring parent]}`. The erring
/// parent's own body row carries its own.
#[tokio::test]
async fn w8_d44_the_census_row_carries_the_gates_reason_per_body() {
    let scenario = erring_fan_in(0);
    let [a, _, r] = [0, 1, 2].map(|i| scenario.txs[i].id());
    for _ in 0..64 {
        let shadow = beside_unordered(&scenario).await;
        let graph = &shadow.of(ShadowKind::Materialization)[0];
        if json(&graph.served.bytes)["landed"] != serde_json::json!([]) {
            continue; // the sibling came first: not the witness's order
        }
        print_census("W8 D44", &shadow);
        let row = serde_json::to_value(graph).unwrap();
        println!("W8 D44 graph row gate: {}", row["gate"]);
        assert_eq!(row["delta"], serde_json::json!("rows_differ"), "{row}");
        assert_eq!(
            row["gate"],
            serde_json::json!([
                {"txid": a, "reason": "parent_error"},
                {"txid": r, "reason": {"waiting_on": [a]}},
            ]),
            "the graph row names the gate's reason per body it held"
        );
        let body = shadow
            .of(ShadowKind::Body)
            .into_iter()
            .find(|row| row.txid == a)
            .expect("the erring parent's body row");
        let body = serde_json::to_value(body).unwrap();
        assert_eq!(
            body["gate"],
            serde_json::json!([{"txid": a, "reason": "parent_error"}]),
            "{body}"
        );
        return;
    }
    panic!("no run took the erring parent first in 64 tries");
}

/// The gate lens's LOW 2: a body whose WALK is refused is handed to the gate
/// as a held parent, so the graph is not stopped by position there. A fan-in
/// whose parent `a` carries an output past the inline bound (its BEEF rides
/// R2) under a host whose R2 read faults: `a`'s walk is refused, the sibling
/// `b` is still judged and lands, and the root waits on `a`, in either
/// sibling order (run until `a` came first, `b` after it). The served path lands all three (it reads no R2): the rows
/// differ.
#[tokio::test]
async fn w10_d44_a_refused_walk_holds_its_descendants_and_its_siblings_land() {
    let mut a = genesis(70, 1);
    let mut data = vec![0x00, 0x6a, 0x4e];
    data.extend(u32::try_from(INLINE_BEEF_MAX_BYTES).unwrap().to_le_bytes());
    data.resize(data.len() + INLINE_BEEF_MAX_BYTES, 0xab);
    a.outputs.push(bsv_rs::transaction::TransactionOutput::new(
        1,
        bsv_rs::script::LockingScript::from_binary(&data).unwrap(),
    ));
    let b = genesis(71, 1);
    let r = spends(&[(&a, 0), (&b, 0)], 1);
    let mut scenario = Scenario::honest(vec![a, b, r], vec![(2, 0, 1.0)]);
    scenario.r2_faults = true;
    let [a, b, r] = [0, 1, 2].map(|i| scenario.txs[i].id());
    let off = run(&scenario, None, None, None).await;
    // A frozen clock: the shadow's time budget is not this witness's (a
    // 1.9 MB body is walked in seconds by a debug build).
    for _ in 0..16 {
        let shadow = run(&scenario, Some(AdmissionMode::Shadow), None, Some(|| 0)).await;
        assert_eq!(
            (&off.rows, &off.results),
            (&shadow.rows, &shadow.results),
            "the shadow changed nothing served (every body lands in either order)"
        );
        let graph = &shadow.of(ShadowKind::Materialization)[0];
        let first = shadow.of(ShadowKind::Body)[0].txid.clone();
        let ours = json(&graph.shadow.bytes);
        let gate = serde_json::to_value(&graph.gate).unwrap();
        println!(
            "W10 {} first: landed {} stopped {} gate {gate}",
            &first[..8],
            ours["landed"],
            ours["stopped"]["txid"],
        );
        assert_eq!(
            (
                ours["landed"].clone(),
                ours["stopped"]["txid"].clone(),
                gate
            ),
            (
                serde_json::json!([b]),
                serde_json::json!(a),
                serde_json::json!([{"txid": r, "reason": {"waiting_on": [a]}}]),
            ),
            "the sibling lands, the refused walk stops the graph, the root waits on it"
        );
        for (tx, held) in [(&a, "false"), (&b, "true"), (&r, "false")] {
            assert_eq!(
                ours["rows"][format!("applied {tx}")],
                serde_json::json!(held),
                "{ours}"
            );
        }
        assert_eq!(
            serde_json::to_value(graph.delta).unwrap(),
            serde_json::json!("rows_differ")
        );
        if first == a {
            return; // the refused walk first: the sibling after it was judged
        }
    }
    panic!("16 runs never handed the refused walk first");
}

/// The census of every world above whose finalize order is our node's
/// (`compute_ordered_beefs`, ancestors first), with the fan-in in the order
/// w8 takes (the erring parent first), frozen on `8ed47d8` (the gate's tip,
/// before the lens's corrections): the sort of the gate's bodies (LOW 1), a
/// walk refusal handed to the gate (LOW 2) and the NOTEs leave every row as
/// it was. Only the wall clock (`shadow_ms`) is left out.
const ORDERED_CENSUS_DIGEST: &str =
    "38044a236d2293d8f93942871c3dcaafd4e53cf4c6d13258802c19fbe6c29f2d";

#[tokio::test]
async fn w9_the_census_of_every_ordered_world_is_the_gates_tips() {
    let mut lines = Vec::new();
    let mut push = |name: &str, run: &Run| {
        for row in &run.census {
            let mut row = row.clone();
            row.shadow_ms = 0;
            lines.push(format!("{name} {}", serde_json::to_string(&row).unwrap()));
        }
    };
    for budgeted in [false, true] {
        let mut scenario = recorded_world();
        scenario.budgeted = budgeted;
        push(
            &format!("recorded budgeted={budgeted}"),
            &beside(&scenario).await,
        );
    }
    push("erring chain", &beside(&erring_chain()).await);
    for (name, scenario) in [
        ("chaos 01", chaos_01()),
        ("chaos 02", chaos_02()),
        ("chaos 03", chaos_03()),
        ("chaos 04", chaos_04()),
    ] {
        push(name, &beside(&scenario).await);
    }
    let fan_in = 'found: {
        for _ in 0..64 {
            let shadow = beside_unordered(&erring_fan_in(0)).await;
            let graph = &shadow.of(ShadowKind::Materialization)[0];
            if json(&graph.served.bytes)["landed"] == serde_json::json!([]) {
                break 'found shadow;
            }
        }
        panic!("no run took the erring parent first in 64 tries");
    };
    push("fan-in erring first", &fan_in);
    let digest = digest(&lines);
    println!("W9 ORDERED CENSUS: {} rows, digest {digest}", lines.len());
    assert_eq!(digest, ORDERED_CENSUS_DIGEST, "the census moved");
}

// ============================================================================
// Witness 3: the four GASP chaos records of the HTTP corpus
// (`parity-harness/corpus/xanaverse/chaos/0{1..4}_*.json`, Zanaadu), each a
// census row and never a served change.
// ============================================================================

fn assert_shadowed(run: &Run, bodies: usize, graphs: usize) {
    // The shadow lens's LOW 4: the snapshots a sync holds at once, measured.
    println!(
        "snapshots: {} taken, {} held at the peak",
        run.snapshots, run.peak_held
    );
    assert!(run.peak_held <= SHADOW_SNAPSHOTS_HELD);
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
fn chaos_01() -> Scenario {
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
    scenario
}

#[tokio::test]
async fn w3_chaos_01_downtime_recovery() {
    let scenario = chaos_01();
    let shadow = beside(&scenario).await;
    print_census("01_downtime_recovery", &shadow);
    assert!(shadow.results[0].contains("peer down"), "tick 1 fails safe");
    assert_shadowed(&shadow, 6, 4);
    assert_eq!(shadow.census[0].txid, scenario.txs[0].id());
}

/// 02_partition_heal (the partial listing): the high-score half is withheld
/// for a tick, then served.
fn chaos_02() -> Scenario {
    let txs: Vec<_> = (0..6).map(|n| genesis(20 + n, 1)).collect();
    let listed = (0..6).map(|i| (i, 0, (i + 1) as f64)).collect();
    let mut scenario = Scenario::honest(txs, listed);
    scenario.ticks = vec![
        |remote, _| remote.withhold_from.set(Some(4.0)),
        |remote, _| remote.withhold_from.set(None),
    ];
    scenario
}

#[tokio::test]
async fn w3_chaos_02_partition_heal() {
    let scenario = chaos_02();
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
fn chaos_03() -> Scenario {
    let txs: Vec<_> = (0..4).map(|n| genesis(30 + n, 1)).collect();
    let mut scenario = Scenario::honest(
        txs,
        vec![(0, 0, 10.0), (1, 0, 20.0), (2, 0, 25.0), (3, 0, 30.0)],
    );
    scenario.tracker = Some(KnownRoots::of(&scenario.nodes));
    let orphan = scenario.txs[2].id();
    scenario.nodes[2].proof = Some(fabricated_proof(&orphan, 102));
    scenario
}

#[tokio::test]
async fn w3_chaos_03_chain_fork() {
    let scenario = chaos_03();
    let orphan = scenario.txs[2].id();
    let shadow = beside(&scenario).await;
    print_census("03_chain_fork", &shadow);
    assert!(shadow.results[0].contains("\"discarded_graphs\":1"));
    assert_shadowed(&shadow, 3, 3);
    assert!(shadow.census.iter().all(|r| r.txid != orphan));
}

/// 04_cursor_no_skip_on_transient (the cursor skip): the middle UTXO's node
/// blips on the first tick; the cursor stays below it and the next tick
/// admits it.
fn chaos_04() -> Scenario {
    let txs: Vec<_> = (0..3).map(|n| genesis(40 + n, 1)).collect();
    let mut scenario = Scenario::honest(txs, vec![(0, 0, 10.0), (1, 0, 20.0), (2, 0, 30.0)]);
    scenario.ticks = vec![
        |remote, txs| {
            remote.faults.borrow_mut().insert(txs[1].id());
        },
        |remote, _| remote.faults.borrow_mut().clear(),
    ];
    scenario
}

#[tokio::test]
async fn w3_chaos_04_cursor_no_skip_on_transient() {
    let scenario = chaos_04();
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
    let (transcript, polls) = recording(&|engine, _, _| {
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
    // The memory host honours the budget it is handed: past it no copy is
    // taken, and the row carries the engine's words. Only a snapshot of the
    // empty store (the first graph's) is judged.
    for row in &rows.census {
        let word = String::from_utf8_lossy(&row.shadow.bytes).to_string();
        if row.delta == DeltaClass::NotShadowed {
            assert_eq!(row.shadow.status, 0);
            assert!(word.starts_with("not shadowed: budget (reads: "), "{word}");
            assert_eq!(row.snapshot_rows, 0, "nothing was copied");
        } else {
            assert_eq!(row.snapshot_rows, 0, "{word}");
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

/// A reference host that writes a line into the recording's transcript for
/// each census row it records.
struct Logged {
    inner: MemoryGaspShadow,
    events: Events,
}

#[async_trait(?Send)]
impl GaspShadow for Logged {
    async fn snapshot(
        &self,
        scope: &ShadowScope<'_>,
        budget: &ShadowBudget,
    ) -> Result<ShadowSnapshot, String> {
        self.inner.snapshot(scope, budget).await
    }
    fn now_ms(&self) -> u64 {
        self.inner.now_ms()
    }
    async fn record(&self, row: &GaspCensusRow) -> Result<(), String> {
        self.events
            .borrow_mut()
            .push(format!("census {:?} {}", row.kind, row.txid));
        self.inner.record(row).await
    }
}

/// The shadow's recording: the recorded world under `shadow`, its transcript
/// carrying a line per census row.
fn shadow_recording() -> (Vec<String>, Vec<u64>) {
    recording(&|engine, store, events| {
        engine.set_gasp_shadow(
            AdmissionMode::Shadow,
            Rc::new(Logged {
                inner: shadow_host(store, None),
                events: events.clone(),
            }),
        );
    })
}

/// The shadow lens's LOW 4: the drain runs once a sync, after every served
/// write of it (the peers' syncs, their cursors and their health rows), so
/// the shadow's work delays no peer, topic or served write behind it.
#[test]
fn w5c_the_drain_runs_after_every_served_write_of_the_sync() {
    let (transcript, _) = shadow_recording();
    let mut tick = String::new();
    let mut census_at = None;
    let mut rows = 0;
    for (i, line) in transcript.iter().enumerate() {
        if line.starts_with("tick ") {
            tick = line.clone();
            census_at = None;
        } else if line.starts_with("census ") {
            rows += 1;
            census_at.get_or_insert(i);
        } else if line.starts_with("store.") || line.starts_with("remote.") {
            if let Some(at) = census_at {
                panic!(
                    "{tick}: a served call after the census began (line {at}): {line}\n{}",
                    transcript[at.saturating_sub(2)..=i].join("\n")
                );
            }
        }
    }
    assert!(rows > 0, "the census was written");
}

/// The shadow lens's LOW 4: the snapshots one sync holds at once are bounded
/// (`ShadowBudget::snapshots`); past it no snapshot is taken and the row says
/// `not shadowed: budget (snapshots ...)`; nothing served moves.
#[tokio::test]
async fn w5d_the_snapshots_a_sync_holds_are_bounded() {
    let scenario = recorded_world();
    let off = run(&scenario, None, None, None).await;
    let unbounded = run(&scenario, Some(AdmissionMode::Shadow), None, None).await;
    println!(
        "recorded world: {} snapshots taken, {} held at the peak (allowance {SHADOW_SNAPSHOTS_HELD})",
        unbounded.snapshots, unbounded.peak_held
    );
    assert!(unbounded.peak_held > 2 && unbounded.peak_held <= SHADOW_SNAPSHOTS_HELD);
    let bounded = run(
        &scenario,
        Some(AdmissionMode::Shadow),
        Some(ShadowBudget {
            snapshots: 2,
            ..ShadowBudget::default()
        }),
        None,
    )
    .await;
    assert_eq!(off.served(), bounded.served());
    assert!(bounded.peak_held <= 2, "held {}", bounded.peak_held);
    assert_eq!(bounded.census.len(), unbounded.census.len(), "a row each");
    let over: Vec<_> = bounded
        .census
        .iter()
        .filter(|r| {
            String::from_utf8_lossy(&r.shadow.bytes).starts_with("not shadowed: budget (snapshots")
        })
        .collect();
    assert!(!over.is_empty(), "the limb was reached");
    assert!(over.iter().all(|r| r.delta == DeltaClass::NotShadowed));
}

/// The shadow lens's NOTE 7 as a pin: the shadow makes no outbound call. On
/// the recorded world the peer is asked exactly what `off` asks (29 calls, the
/// same lines in the same order); the served store is read 48 times more (33
/// `find_output`, 15 `applied?`: `materialized_rows` of the served side, per
/// graph after its last body) and written exactly as under `off`; `off`'s
/// transcript is the shadow's with those reads and the census taken out.
#[test]
fn w7_the_shadow_makes_no_outbound_call_and_reads_the_served_store_48_times_more() {
    let (off, _) = recording(&|_, _, _| {});
    let (shadow, _) = shadow_recording();
    let of = |lines: &[String], prefix: &str| -> Vec<String> {
        lines
            .iter()
            .filter(|l| l.starts_with(prefix))
            .cloned()
            .collect()
    };
    let (off_peer, shadow_peer) = (of(&off, "remote."), of(&shadow, "remote."));
    println!(
        "peer calls: off {}, shadow {}; store calls: off {}, shadow {}",
        off_peer.len(),
        shadow_peer.len(),
        of(&off, "store.").len(),
        of(&shadow, "store.").len()
    );
    assert_eq!(off_peer.len(), 29);
    assert_eq!(off_peer, shadow_peer, "no peer call added");
    let extra = |op: &str| of(&shadow, op).len() - of(&off, op).len();
    assert_eq!(
        of(&shadow, "store.").len() - of(&off, "store.").len(),
        48,
        "the served-store reads the shadow adds"
    );
    assert_eq!(
        (extra("store.find_output "), extra("store.applied? ")),
        (33, 15)
    );
    // Take out exactly those reads and the census: what is left is `off`.
    let mut rest = shadow
        .iter()
        .filter(|l| !l.starts_with("census "))
        .peekable();
    let mut taken = 0;
    for line in &off {
        while rest.peek().is_some_and(|l| *l != line) {
            let read = rest.next().unwrap();
            assert!(
                read.starts_with("store.find_output ") || read.starts_with("store.applied? "),
                "only reads are added: {read}"
            );
            taken += 1;
        }
        assert_eq!(rest.next(), Some(line), "off is a subsequence of shadow");
    }
    taken += rest.count();
    assert_eq!(taken, 48);
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
