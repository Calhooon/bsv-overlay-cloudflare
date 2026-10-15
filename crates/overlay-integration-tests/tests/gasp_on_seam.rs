//! The engine's host seam for the GASP finalize under `on` (bsv-low #657, the
//! a17 lane: zanaadu-v2 `atomic-admission-a17-gasp-seam`, item 2 blocked at
//! the engine). Under `AdmissionMode::On` with a `GaspFinalizeHost` set, the
//! engine walks each finalized graph's bodies and runs the dependency gate
//! (D44) over them, handing each body it does not hold to the host, one at a
//! time, ancestors first, with the gate's reason for every body: the host
//! runs each as one unit of its own log (here the memory log over the served
//! store), and the engine's own finalize submit does not run. A graph with a
//! held body keeps the cursor below its UTXO, so the peer's next sync re-sends
//! the waiting part.

#[path = "support/gasp_shadow_world.rs"]
mod world;

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use async_trait::async_trait;
use bsv_overlay_engine::admission::{
    gasp_unit, AdmitRefusal, Committed, GateReason, GraphOutcome, UnitKind, UnitOutput, UnitRunner,
    GATE_PARENT_ERROR_SITE, GATE_WAITING_SITE,
};
use bsv_overlay_engine::engine::{Engine, EngineConfig};
use bsv_overlay_engine::gasp_shadow::{
    AdmissionMode, AdmissionModeRefused, FinalizeBody, FinalizeGraph, GaspCensusRow,
    GaspFinalizeHost, GaspShadow, ShadowBudget, ShadowScope, ShadowSnapshot,
};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_overlay_engine::storage::Storage;
use bsv_overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use bsv_overlay_engine::types::*;
use bsv_rs::transaction::{ChainTracker, Transaction};
use world::*;

/// The sync engine's manager: the world's, counting every call that is not a
/// dry run (a finalize submit of the engine's own; the anchor replay and the
/// walk's needed inputs are dry or ask nothing). Under `on` there are none.
struct Counted {
    inner: AdmitAll,
    submits: Rc<RefCell<Vec<String>>>,
}

#[async_trait(?Send)]
impl TopicManager for Counted {
    fn reads_off_chain_values(&self) -> bool {
        false
    }
    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        previous_coins: &[u8],
        off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
        context: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        if !context.dry_run {
            self.submits.borrow_mut().push(tx.id());
        }
        self.inner
            .identify_admissible_outputs(tx, previous_coins, off_chain_values, mode, context)
            .await
    }
    async fn identify_needed_inputs(
        &self,
        beef: &[u8],
        off_chain_values: Option<&[u8]>,
    ) -> Result<Vec<Outpoint>, TopicManagerError> {
        self.inner
            .identify_needed_inputs(beef, off_chain_values)
            .await
    }
    async fn get_documentation(&self) -> String {
        String::new()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata::default()
    }
}

/// The sync engine over `store`, its manager counting its own submits.
fn sync_engine(
    store: &Rc<MemoryStorage>,
    remote: &ChaosRemote,
    submits: &Rc<RefCell<Vec<String>>>,
) -> Engine {
    let mut engine = Engine::with_chain_tracker(
        HashMap::from([(
            TOPIC.to_string(),
            Box::new(Counted {
                inner: AdmitAll {
                    fails_at_finalize: None,
                },
                submits: submits.clone(),
            }) as Box<dyn TopicManager>,
        )]),
        HashMap::new(),
        Box::new(store.clone()),
        None,
        None,
        None::<Box<dyn ChainTracker>>,
        EngineConfig {
            sync_configuration: HashMap::from([(
                TOPIC.to_string(),
                SyncTarget::Peers(vec![PEER.to_string()]),
            )]),
            ..Default::default()
        },
    );
    engine.set_gasp_remote_factory(Box::new(remote.clone()));
    engine
}

/// One body's word as the host's sink hears it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Heard {
    txid: String,
    word: String,
    gate: Option<GateReason>,
}

/// A graph's outcome as the host hears it: its root, how many bodies landed,
/// and each body held with the gate's reason.
type GraphHeard = (String, usize, Vec<(String, Option<GateReason>)>);

fn word_of(answer: &Result<Committed, AdmitRefusal>) -> String {
    match answer {
        Ok(Committed {
            output: UnitOutput::Admitted(admitted),
            ..
        }) if admitted.dedup => "landed (dedup)".into(),
        Ok(_) => "landed".into(),
        Err(AdmitRefusal::NotNow { site, .. }) => format!("not now at {site}"),
        Err(other) => format!("refused: {other}"),
    }
}

/// The host: a sink of the gate's words and the memory log over the served
/// store, whose manager errs at the finalize on `fails` (the log's engine).
#[derive(Default)]
struct SinkHost {
    store: Rc<MemoryStorage>,
    fails: RefCell<Option<String>>,
    /// Bodies the host's unit refuses with this word (run, nothing written).
    refuses: RefCell<HashMap<String, AdmitRefusal>>,
    heard: RefCell<Vec<Heard>>,
    /// Each body the host ran as a unit, in order.
    ran: RefCell<Vec<String>>,
    /// Each graph's outcome: (root, landed seqs, held (txid, gate)).
    graphs: RefCell<Vec<GraphHeard>>,
}

impl SinkHost {
    fn new(store: &Rc<MemoryStorage>) -> Self {
        Self {
            store: store.clone(),
            ..Default::default()
        }
    }
    fn take(&self) -> Vec<Heard> {
        std::mem::take(&mut *self.heard.borrow_mut())
    }
}

#[async_trait(?Send)]
impl GaspFinalizeHost for SinkHost {
    async fn admit(
        &self,
        body: &FinalizeBody<'_>,
        resolved: Option<Vec<u8>>,
    ) -> Result<Committed, AdmitRefusal> {
        self.ran.borrow_mut().push(body.walked.txid.clone());
        if let Some(refusal) = self.refuses.borrow().get(&body.walked.txid) {
            return Err(refusal.clone());
        }
        if let Some(bytes) = resolved {
            self.store.cache_admission_beef(&body.walked.beef, bytes)?;
        }
        let engine = Engine::new(
            managers(self.fails.borrow().clone()),
            HashMap::new(),
            Box::new(self.store.clone()),
            None,
            EngineConfig::default(),
        );
        self.store.run_unit(
            UnitKind::Admit,
            &body.walked.evidence,
            Box::pin(gasp_unit(&engine, body.walked)),
        )
    }
    fn answered(
        &self,
        body: &FinalizeBody<'_>,
        answer: &Result<Committed, AdmitRefusal>,
        gate: Option<&GateReason>,
    ) {
        self.heard.borrow_mut().push(Heard {
            txid: body.walked.txid.clone(),
            word: word_of(answer),
            gate: gate.cloned(),
        });
    }
    async fn graph_done(&self, graph: &FinalizeGraph<'_>, outcome: &GraphOutcome) -> bool {
        let (landed, held) = match outcome {
            GraphOutcome::AllLanded { seqs } => (seqs.len(), vec![]),
            GraphOutcome::Held { landed, held } => (
                landed.len(),
                held.iter()
                    .map(|h| (h.txid.clone(), h.gate.clone()))
                    .collect(),
            ),
        };
        self.graphs
            .borrow_mut()
            .push((graph.root.to_string(), landed, held));
        matches!(outcome, GraphOutcome::AllLanded { .. })
    }
}

/// [`SinkHost`] with the engine's default `graph_done` (the cursor's rule).
struct DefaultDone(Rc<SinkHost>);

#[async_trait(?Send)]
impl GaspFinalizeHost for DefaultDone {
    async fn admit(
        &self,
        body: &FinalizeBody<'_>,
        resolved: Option<Vec<u8>>,
    ) -> Result<Committed, AdmitRefusal> {
        self.0.admit(body, resolved).await
    }
    fn answered(
        &self,
        body: &FinalizeBody<'_>,
        answer: &Result<Committed, AdmitRefusal>,
        gate: Option<&GateReason>,
    ) {
        self.0.answered(body, answer, gate);
    }
}

/// `r` spends `a.0` and `b.0`, all proven; the peer lists `r.0`.
fn fan_in() -> (Vec<Transaction>, Vec<GASPNode>) {
    let a = genesis(50, 1);
    let b = genesis(51, 1);
    let r = spends(&[(&a, 0), (&b, 0)], 1);
    let txs = vec![a, b, r];
    let nodes = txs
        .iter()
        .enumerate()
        .map(|(i, tx)| node(tx, honest_proof(&tx.id(), 100 + i as u32)))
        .collect();
    (txs, nodes)
}

/// What one run gave: the peer's calls, the sync results, the rows, the
/// cursor (at the end, and after each tick), the engine's own submits and,
/// per tick, the host's words.
struct Ran {
    peer: Vec<String>,
    results: Vec<String>,
    rows: BTreeMap<String, String>,
    cursors: Vec<u64>,
    submits: Vec<String>,
    heard: Vec<Vec<Heard>>,
    ran: Vec<String>,
    graphs: Vec<GraphHeard>,
}

/// The independent `x` the peer lists ABOVE the fan-in's root (score 2; the
/// root `r` at 1), so a cursor moved past a held root is seen: the peer
/// lists `score >= since`, and a cursor at 2 no longer lists `r`.
fn beside_x() -> Transaction {
    genesis(52, 1)
}

/// Each tick: the log's manager errs at the finalize on that txid (`None`:
/// healthy). `mode` `None`: no host is set and the mode stays `off`.
async fn run(ticks: &[Option<String>], mode: Option<AdmissionMode>, budgeted: bool) -> Ran {
    run_in(false, ticks, mode, budgeted).await
}

/// [`run`], with [`beside_x`] listed above the root when `beside`.
async fn run_in(
    beside: bool,
    ticks: &[Option<String>],
    mode: Option<AdmissionMode>,
    budgeted: bool,
) -> Ran {
    run_with(beside, &[], ticks, mode, budgeted).await
}

/// [`run_in`], the host's unit refusing on tick `i` the bodies `refuses[i]`
/// names with their words, and the host's `graph_done` the engine's default
/// when `refuses` is not empty.
async fn run_with(
    beside: bool,
    refuses: &[Vec<(String, AdmitRefusal)>],
    ticks: &[Option<String>],
    mode: Option<AdmissionMode>,
    budgeted: bool,
) -> Ran {
    let (mut txs, mut nodes) = fan_in();
    if beside {
        let x = beside_x();
        nodes.push(node(&x, honest_proof(&x.id(), 103)));
        txs.push(x);
    }
    let events = Events::default();
    let mut listed = vec![(&txs[2], 0, 1.0)];
    if beside {
        listed.push((&txs[3], 0, 2.0));
    }
    let remote = ChaosRemote::new(&nodes, &listed, &events);
    let store = Rc::new(MemoryStorage::new());
    let submits = Rc::default();
    let mut engine = sync_engine(&store, &remote, &submits);
    if budgeted {
        engine.set_peer_sync_budget(never_due(), 30_000);
    }
    let host = Rc::new(SinkHost::new(&store));
    if let Some(mode) = mode {
        if refuses.is_empty() {
            engine.set_gasp_finalize_host(host.clone());
        } else {
            engine.set_gasp_finalize_host(Rc::new(DefaultDone(host.clone())));
        }
        engine.set_admission_mode(mode).unwrap();
    }
    let mut results = Vec::new();
    let mut heard = Vec::new();
    let mut cursors = Vec::new();
    for (tick, fails) in ticks.iter().enumerate() {
        *host.fails.borrow_mut() = fails.clone();
        *host.refuses.borrow_mut() = refuses
            .get(tick)
            .map(|r| r.iter().cloned().collect())
            .unwrap_or_default();
        let result = engine.start_gasp_sync().await.unwrap();
        let mut topics: Vec<_> = result.topics_synced.iter().collect();
        topics.sort_by_key(|(topic, _)| topic.as_str());
        results.push(serde_json::to_string(&topics).unwrap());
        heard.push(host.take());
        cursors.push(store.get_last_interaction(PEER, TOPIC).await.unwrap());
    }
    let all: Vec<_> = txs.iter().collect();
    let mut rows = served_rows(store.as_ref(), &all).await;
    rows.insert(
        "cursor".into(),
        store
            .get_last_interaction(PEER, TOPIC)
            .await
            .unwrap()
            .to_string(),
    );
    let peer = events.borrow().clone();
    let submits = submits.borrow().clone();
    let ran = host.ran.borrow().clone();
    let graphs = host.graphs.borrow().clone();
    Ran {
        peer,
        results,
        rows,
        cursors,
        submits,
        heard,
        ran,
        graphs,
    }
}

fn sorted(calls: &[String]) -> Vec<String> {
    let mut calls = calls.to_vec();
    calls.sort();
    calls
}

fn print_heard(name: &str, ran: &Ran) {
    for (tick, heard) in ran.heard.iter().enumerate() {
        for h in heard {
            println!(
                "{name} tick {tick}: {} {} gate={}",
                &h.txid[..12],
                h.word,
                serde_json::to_string(&h.gate).unwrap()
            );
        }
    }
    for (root, landed, held) in &ran.graphs {
        println!(
            "{name} graph {}: landed {landed}, held {}",
            &root[..12],
            serde_json::to_string(held).unwrap()
        );
    }
}

/// Item 1. The fan-in, the log's manager erring at the finalize on `a`, the
/// independent `x` listed above its root: the host's sink hears the bodies
/// parents first (`a` and `b` in the peer's order, `r` last) with the gate's
/// reasons: `b` lands; `a` `not now`, `parent_error`; `r` `not now`,
/// `waiting_on [a]` (the erring parent and the root held, neither run past
/// `a`'s unit); `x` lands. The engine submits nothing of its own. The cursor
/// stays below `r` (the host's word) after the erring tick, though `x` above
/// it landed (the seam lens's L1: a cursor moved past `r` no longer lists
/// it, so the re-send below proves the cursor held). The next sync, the
/// manager healed, re-sends the waiting part: `a` and `r` land, the cursor
/// passes both UTXOs, and the rows are a first-time graph's (the same as the
/// engine's own `off` finalize of a healthy fan-in). With and without a
/// per-peer budget (the hook's path).
#[tokio::test]
async fn s1_the_host_hears_the_fan_in_parents_first_with_the_gates_reasons_and_the_resync_lands() {
    let (txs, _) = fan_in();
    let [a, b, r] = [0, 1, 2].map(|i| txs[i].id());
    let x = beside_x().id();
    for budgeted in [false, true] {
        let on = run_in(
            true,
            &[Some(a.clone()), None],
            Some(AdmissionMode::On),
            budgeted,
        )
        .await;
        print_heard(&format!("S1 budgeted={budgeted}"), &on);
        println!("S1 budgeted={budgeted} cursors {:?}", on.cursors);
        assert_eq!(
            on.submits,
            Vec::<String>::new(),
            "the engine's own finalize did not submit"
        );
        let first: Vec<_> = on.heard[0].iter().filter(|h| h.txid != x).collect();
        assert_eq!(
            first.len(),
            3,
            "the host heard every body of the graph: {first:?}"
        );
        assert_eq!(first[2].txid, r, "parents first: the root last");
        let by: HashMap<_, _> = on.heard[0]
            .iter()
            .map(|h| (h.txid.clone(), h.clone()))
            .collect();
        assert_eq!(
            by[&b],
            Heard {
                txid: b.clone(),
                word: "landed".into(),
                gate: None
            }
        );
        assert_eq!(
            by[&a],
            Heard {
                txid: a.clone(),
                word: format!("not now at {GATE_PARENT_ERROR_SITE}"),
                gate: Some(GateReason::ParentError)
            }
        );
        assert_eq!(
            by[&r],
            Heard {
                txid: r.clone(),
                word: format!("not now at {GATE_WAITING_SITE}"),
                gate: Some(GateReason::WaitingOn(vec![a.clone()]))
            }
        );
        assert_eq!(
            by[&x],
            Heard {
                txid: x.clone(),
                word: "landed".into(),
                gate: None
            },
            "the independent UTXO above the root lands"
        );
        assert_eq!(
            on.graphs[0],
            (
                r.clone(),
                1,
                vec![
                    (a.clone(), Some(GateReason::ParentError)),
                    (r.clone(), Some(GateReason::WaitingOn(vec![a.clone()])))
                ]
            ),
            "the erring parent and the root held"
        );
        assert_eq!(on.graphs[1], (x.clone(), 1, vec![]));
        assert!(
            !on.ran.iter().take(2).any(|t| t == &r),
            "the root never ran on the erring tick: {:?}",
            on.ran
        );
        assert_eq!(
            on.cursors[0], 0,
            "after the erring tick the cursor stays below the held root (score 1), though x (score 2) landed"
        );
        // The re-sync: the waiting part, re-sent, lands.
        assert_eq!(
            on.heard[1],
            vec![
                Heard {
                    txid: a.clone(),
                    word: "landed".into(),
                    gate: None
                },
                Heard {
                    txid: r.clone(),
                    word: "landed".into(),
                    gate: None
                },
            ],
            "the re-sync lands the waiting part"
        );
        assert_eq!(on.graphs[2], (r.clone(), 2, vec![]));
        assert_eq!(on.cursors[1], 2, "the re-sync passes both UTXOs");
        let off = run_in(true, &[None], None, budgeted).await;
        assert_eq!(off.cursors, vec![2], "off: the cursor passes both");
        assert_eq!(
            on.rows, off.rows,
            "after the re-sync the rows (and the cursor) are a first-time graph's"
        );
    }
}

/// Item 1, the engine's own path unchanged: a healthy fan-in under `on` gives
/// the `off` run's peer calls, sync results, rows and cursor, with the host
/// running every body and the engine none; under `off` with a host set the
/// host is never asked.
#[tokio::test]
async fn s1b_a_healthy_graph_under_on_is_the_off_runs_and_off_never_asks_the_host() {
    for budgeted in [false, true] {
        let off = run(&[None], None, budgeted).await;
        let on = run(&[None], Some(AdmissionMode::On), budgeted).await;
        assert_eq!(off.submits.len(), 3, "off: the engine submits each body");
        assert_eq!(on.submits, Vec::<String>::new());
        assert_eq!(on.ran.len(), 3, "on: the host runs each body");
        assert_eq!(
            (sorted(&on.peer), &on.results, &on.rows),
            (sorted(&off.peer), &off.results, &off.rows),
            "the peer's calls (a multiset: the siblings' order is a HashMap's), the sync results, the rows and the cursor"
        );
        let off_with_host = run(&[None], Some(AdmissionMode::Off), budgeted).await;
        assert_eq!(
            off_with_host.ran,
            Vec::<String>::new(),
            "off never asks the host"
        );
        assert_eq!(
            (
                sorted(&off_with_host.peer),
                &off_with_host.results,
                &off_with_host.rows,
                sorted(&off_with_host.submits)
            ),
            (
                sorted(&off.peer),
                &off.results,
                &off.rows,
                sorted(&off.submits)
            ),
        );
    }
}

/// A deterministic refusal of the host's unit (a terminal word).
fn terminal(txid: &str) -> AdmitRefusal {
    AdmitRefusal::Deterministic {
        topic: TOPIC.into(),
        site: "the host's unit".into(),
        reason: format!("{} is refused for good", &txid[..12]),
        notes: vec![],
        refused_parent: None,
    }
}

/// The seam lens's L3, the cursor's rule under `on` with the default
/// `graph_done`: a graph whose every held body is held by a TERMINAL word
/// (`Lookup`, `Deterministic`: its own, or a parent's the gate passed on)
/// lets the cursor pass it once every other body is landed; a body held `not
/// now` (or transient) holds the cursor below the graph, so the peer's next
/// sync re-sends it. The fan-in with `x` listed above its root: the host
/// refuses `a` for good, `b` lands, `r` is refused with `a`'s word, `x` lands,
/// and the cursor passes both UTXOs (2) on the first tick; the next tick
/// re-sends nothing. With `b` refused `not now` beside it, the cursor stays
/// below the root (0) and the next tick re-sends the graph.
#[tokio::test]
async fn s3_a_terminal_refusal_lets_the_cursor_pass_its_graph_and_not_now_holds_it() {
    let (txs, _) = fan_in();
    let [a, b, r] = [0, 1, 2].map(|i| txs[i].id());
    let x = beside_x().id();
    for budgeted in [false, true] {
        let only = vec![
            vec![(a.clone(), terminal(&a))],
            vec![(a.clone(), terminal(&a))],
        ];
        let on = run_with(
            true,
            &only,
            &[None, None],
            Some(AdmissionMode::On),
            budgeted,
        )
        .await;
        print_heard(&format!("S3 terminal budgeted={budgeted}"), &on);
        println!("S3 terminal budgeted={budgeted} cursors {:?}", on.cursors);
        let words: HashMap<_, _> = on.heard[0]
            .iter()
            .map(|h| (h.txid.clone(), (h.word.clone(), h.gate.clone())))
            .collect();
        assert!(
            words[&a].0.starts_with("refused: the host's unit"),
            "{words:?}"
        );
        assert_eq!(words[&b].0, "landed");
        assert_eq!(
            words[&r],
            (
                format!(
                    "refused: parent {a} was refused: the host's unit: {} is refused for good",
                    &a[..12]
                ),
                Some(GateReason::WaitingOn(vec![a.clone()]))
            ),
            "the root is refused with the parent's terminal word"
        );
        assert_eq!(words[&x].0, "landed");
        assert_eq!(
            on.cursors,
            vec![2, 2],
            "a graph held by terminal words alone lets the cursor pass it"
        );
        assert_eq!(
            on.heard[1],
            Vec::<Heard>::new(),
            "the next tick re-sends nothing"
        );

        let mixed = vec![
            vec![
                (a.clone(), terminal(&a)),
                (b.clone(), AdmitRefusal::not_now("the host's unit", "later")),
            ],
            vec![(a.clone(), terminal(&a))],
        ];
        let on = run_with(
            true,
            &mixed,
            &[None, None],
            Some(AdmissionMode::On),
            budgeted,
        )
        .await;
        print_heard(&format!("S3 mixed budgeted={budgeted}"), &on);
        println!("S3 mixed budgeted={budgeted} cursors {:?}", on.cursors);
        assert_eq!(
            on.cursors,
            vec![0, 2],
            "a body held not now holds the cursor below its graph; once it lands the graph passes"
        );
        let resent: Vec<_> = on.heard[1].iter().map(|h| h.txid.clone()).collect();
        assert!(
            resent.contains(&b),
            "the next tick re-sends the graph and b lands: {:?}",
            on.heard[1]
        );
        assert!(on.heard[1]
            .iter()
            .any(|h| h.txid == b && h.word == "landed"));
    }
}

/// A host recording each body's place in its graph as the engine hands it.
struct Positions {
    inner: Rc<SinkHost>,
    /// (txid, position, the BEEF at that position) per `admit` and per
    /// `answered`, in order.
    admitted: RefCell<Vec<(String, usize, Vec<u8>)>>,
    answered: RefCell<Vec<(String, usize, Vec<u8>)>>,
    /// Each graph's BEEFs as the engine handed them.
    beefs: RefCell<Vec<Vec<Vec<u8>>>>,
}

#[async_trait(?Send)]
impl GaspFinalizeHost for Positions {
    async fn admit(
        &self,
        body: &FinalizeBody<'_>,
        resolved: Option<Vec<u8>>,
    ) -> Result<Committed, AdmitRefusal> {
        self.admitted.borrow_mut().push((
            body.walked.txid.clone(),
            body.position,
            body.beef().to_vec(),
        ));
        self.inner.admit(body, resolved).await
    }
    fn answered(
        &self,
        body: &FinalizeBody<'_>,
        answer: &Result<Committed, AdmitRefusal>,
        gate: Option<&GateReason>,
    ) {
        self.answered.borrow_mut().push((
            body.walked.txid.clone(),
            body.position,
            body.beef().to_vec(),
        ));
        self.inner.answered(body, answer, gate);
    }
    async fn graph_done(&self, graph: &FinalizeGraph<'_>, outcome: &GraphOutcome) -> bool {
        self.beefs.borrow_mut().push(graph.beefs.to_vec());
        self.inner.graph_done(graph, outcome).await
    }
}

/// The seam lens's L4. A GASP graph can carry one txid twice: a transaction
/// is a node once per output the graph spends of it, and the finalize hands
/// on one BEEF per node (the reference's `computeOrderedBEEFsForGraph` drops
/// no repeat either: "The assembly and the two limbs" in CLAUDE.md). Here `c`
/// spends `p.0` and `p.1`, so `p` is two nodes and two BEEFs of the graph.
/// Each occurrence is handed to the host at ITS OWN place in the graph, with
/// the BEEF at that place: every position of the graph once, never the first
/// copy's place for both.
#[tokio::test]
async fn s4_a_txid_twice_in_one_graph_is_handed_at_each_place() {
    let p = genesis(53, 2);
    let c = spends(&[(&p, 0), (&p, 1)], 1);
    let nodes = vec![
        node(&p, honest_proof(&p.id(), 110)),
        node(&c, honest_proof(&c.id(), 111)),
    ];
    for budgeted in [false, true] {
        let events = Events::default();
        let remote = ChaosRemote::new(&nodes, &[(&c, 0, 1.0)], &events);
        let store = Rc::new(MemoryStorage::new());
        let submits = Rc::default();
        let mut engine = sync_engine(&store, &remote, &submits);
        if budgeted {
            engine.set_peer_sync_budget(never_due(), 30_000);
        }
        let host = Rc::new(Positions {
            inner: Rc::new(SinkHost::new(&store)),
            admitted: RefCell::default(),
            answered: RefCell::default(),
            beefs: RefCell::default(),
        });
        engine.set_gasp_finalize_host(host.clone());
        engine.set_admission_mode(AdmissionMode::On).unwrap();
        engine.start_gasp_sync().await.unwrap();
        let beefs = host.beefs.borrow().clone();
        let answered = host.answered.borrow().clone();
        let places: Vec<_> = answered
            .iter()
            .map(|(txid, at, _)| (txid[..12].to_string(), *at))
            .collect();
        println!(
            "S4 budgeted={budgeted}: the graph's bodies {}, answered at {places:?}",
            beefs.first().map_or(0, Vec::len)
        );
        assert_eq!(beefs.len(), 1, "one graph");
        let graph = &beefs[0];
        let p_twice = answered.iter().filter(|(txid, ..)| *txid == p.id()).count();
        assert_eq!(p_twice, 2, "the graph carries p twice: {places:?}");
        let mut at: Vec<usize> = answered.iter().map(|(_, at, _)| *at).collect();
        at.sort_unstable();
        assert_eq!(
            at,
            (0..graph.len()).collect::<Vec<_>>(),
            "every place of the graph handed once: {places:?}"
        );
        for (txid, at, beef) in answered.iter().chain(host.admitted.borrow().iter()) {
            assert_eq!(beef, &graph[*at], "the BEEF at its own place");
            assert_eq!(
                Transaction::from_beef(beef, None).unwrap().id(),
                *txid,
                "the BEEF at {at} is its body's"
            );
        }
        assert_eq!(
            host.inner
                .heard
                .borrow()
                .iter()
                .map(|h| h.word.clone())
                .collect::<Vec<_>>(),
            ["landed", "landed (dedup)", "landed"],
            "p lands once and its second place dedups"
        );
    }
}

/// A shadow host that is never to be asked.
struct Unasked;

#[async_trait(?Send)]
impl GaspShadow for Unasked {
    async fn snapshot(
        &self,
        _: &ShadowScope<'_>,
        _: &ShadowBudget,
    ) -> Result<ShadowSnapshot, String> {
        panic!("the shadow host is not asked here")
    }
    fn now_ms(&self) -> u64 {
        0
    }
    async fn record(&self, _: &GaspCensusRow) -> Result<(), String> {
        panic!("the shadow host is not asked here")
    }
}

/// Item 2. `on` is the engine's word now, and the engine accepts it only
/// where the finalize seam is set: without a `GaspFinalizeHost`, through
/// either setter, it is refused with the typed word and the mode is left as
/// it was; `off` and `shadow` need no seam; with the seam set `on` is taken.
#[test]
fn s2_on_is_accepted_where_the_seam_is_set_and_refused_with_the_typed_word_where_not() {
    let store = Rc::new(MemoryStorage::new());
    let mut engine = Engine::new(
        managers(None),
        HashMap::new(),
        Box::new(store.clone()),
        None,
        EngineConfig::default(),
    );
    let refused = engine.set_admission_mode(AdmissionMode::On);
    println!("S2 on without the seam: {refused:?}");
    assert_eq!(
        refused,
        Err(AdmissionModeRefused::NoFinalizeHost),
        "on without the seam is refused"
    );
    assert_eq!(
        engine.admission_mode(),
        AdmissionMode::Off,
        "the mode is left as it was"
    );
    assert_eq!(
        refused.unwrap_err().to_string(),
        "ADMISSION_MODE=on is refused: no GASP finalize host is set (Engine::set_gasp_finalize_host)"
    );
    assert_eq!(
        engine.set_gasp_shadow(AdmissionMode::On, Rc::new(Unasked)),
        Err(AdmissionModeRefused::NoFinalizeHost),
        "nor through the shadow's setter"
    );
    assert_eq!(engine.admission_mode(), AdmissionMode::Off);
    assert_eq!(engine.set_admission_mode(AdmissionMode::Shadow), Ok(()));
    assert_eq!(engine.set_admission_mode(AdmissionMode::Off), Ok(()));
    assert_eq!(
        AdmissionMode::parse(Some("on")),
        Ok(AdmissionMode::On),
        "on is the engine's word"
    );
    engine.set_gasp_finalize_host(Rc::new(SinkHost::new(&store)));
    assert_eq!(engine.set_admission_mode(AdmissionMode::On), Ok(()));
    assert_eq!(engine.admission_mode(), AdmissionMode::On);
    assert_eq!(
        engine.set_gasp_shadow(AdmissionMode::On, Rc::new(Unasked)),
        Ok(())
    );
}
