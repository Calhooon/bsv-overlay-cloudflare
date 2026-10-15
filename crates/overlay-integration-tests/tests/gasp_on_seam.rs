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
    AdmissionMode, FinalizeBody, FinalizeGraph, GaspFinalizeHost,
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
    heard: RefCell<Vec<Heard>>,
    /// Each body the host ran as a unit, in order.
    ran: RefCell<Vec<String>>,
    /// Each graph's outcome: (root, landed seqs, held (txid, gate)).
    graphs: RefCell<Vec<(String, usize, Vec<(String, Option<GateReason>)>)>>,
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
/// cursor, the engine's own submits and, per tick, the host's words.
struct Ran {
    peer: Vec<String>,
    results: Vec<String>,
    rows: BTreeMap<String, String>,
    submits: Vec<String>,
    heard: Vec<Vec<Heard>>,
    ran: Vec<String>,
    graphs: Vec<(String, usize, Vec<(String, Option<GateReason>)>)>,
}

/// Each tick: the log's manager errs at the finalize on that txid (`None`:
/// healthy). `mode` `None`: no host is set and the mode stays `off`.
async fn run(ticks: &[Option<String>], mode: Option<AdmissionMode>, budgeted: bool) -> Ran {
    let (txs, nodes) = fan_in();
    let events = Events::default();
    let remote = ChaosRemote::new(&nodes, &[(&txs[2], 0, 1.0)], &events);
    let store = Rc::new(MemoryStorage::new());
    let submits = Rc::default();
    let mut engine = sync_engine(&store, &remote, &submits);
    if budgeted {
        engine.set_peer_sync_budget(never_due(), 30_000);
    }
    let host = Rc::new(SinkHost::new(&store));
    if let Some(mode) = mode {
        engine.set_gasp_finalize_host(host.clone());
        engine.set_admission_mode(mode).unwrap();
    }
    let mut results = Vec::new();
    let mut heard = Vec::new();
    for fails in ticks {
        *host.fails.borrow_mut() = fails.clone();
        let result = engine.start_gasp_sync().await.unwrap();
        let mut topics: Vec<_> = result.topics_synced.iter().collect();
        topics.sort_by_key(|(topic, _)| topic.as_str());
        results.push(serde_json::to_string(&topics).unwrap());
        heard.push(host.take());
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
        submits,
        heard,
        ran,
        graphs,
    }
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

/// Item 1. The fan-in, the log's manager erring at the finalize on `a`: the
/// host's sink hears the bodies parents first (`a` and `b` in the peer's
/// order, `r` last) with the gate's reasons: `b` lands; `a` `not now`,
/// `parent_error`; `r` `not now`, `waiting_on [a]` (the erring parent and the
/// root held, neither run past `a`'s unit). The engine submits nothing of its
/// own. The cursor stays below `r` (the host's word). The next sync, the
/// manager healed, re-sends the waiting part: `a` and `r` land, and the rows
/// are a first-time graph's (the same as the engine's own `off` finalize of a
/// healthy fan-in). With and without a per-peer budget (the hook's path).
#[tokio::test]
async fn s1_the_host_hears_the_fan_in_parents_first_with_the_gates_reasons_and_the_resync_lands() {
    let (txs, _) = fan_in();
    let [a, b, r] = [0, 1, 2].map(|i| txs[i].id());
    for budgeted in [false, true] {
        let on = run(&[Some(a.clone()), None], Some(AdmissionMode::On), budgeted).await;
        print_heard(&format!("S1 budgeted={budgeted}"), &on);
        assert_eq!(
            on.submits,
            Vec::<String>::new(),
            "the engine's own finalize did not submit"
        );
        let first = &on.heard[0];
        assert_eq!(
            first
                .iter()
                .map(|h| h.txid.clone())
                .collect::<Vec<_>>()
                .len(),
            3,
            "the host heard every body of the graph: {first:?}"
        );
        assert_eq!(first[2].txid, r, "parents first: the root last");
        let by: HashMap<_, _> = first.iter().map(|h| (h.txid.clone(), h.clone())).collect();
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
        assert!(
            !on.ran.iter().take(2).any(|t| t == &r),
            "the root never ran on the erring tick: {:?}",
            on.ran
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
        assert_eq!(on.graphs[1], (r.clone(), 2, vec![]));
        let off = run(&[None], None, budgeted).await;
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
            (&on.peer, &on.results, &on.rows),
            (&off.peer, &off.results, &off.rows),
            "the peer's calls, the sync results, the rows and the cursor"
        );
        let off_with_host = run(&[None], Some(AdmissionMode::Off), budgeted).await;
        assert_eq!(
            off_with_host.ran,
            Vec::<String>::new(),
            "off never asks the host"
        );
        assert_eq!(
            (
                &off_with_host.peer,
                &off_with_host.results,
                &off_with_host.rows,
                &off_with_host.submits
            ),
            (&off.peer, &off.results, &off.rows, &off.submits),
        );
    }
}
