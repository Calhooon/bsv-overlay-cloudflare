//! The owner's D44 (zanaadu-v2 `progress/decisions/2026-10-14-owner-rulings.md`,
//! the GASP fan-in delta, `dependency gate`): under A a GASP body lands when
//! every parent it spends has landed (in this graph or before); a manager
//! error at the finalize refuses that body `not now` (never terminal) and every
//! descendant of it in the graph waits with the same word; independent
//! siblings land; the peer's next sync re-sends the waiting part and it lands
//! with the same judgement as a first-time body; a lookup refusal stays
//! terminal and unit-wide (D34), its descendants refused with the same typed
//! word.
//!
//! Each witness runs A's `admit_graph` directly, as the `on` path and the
//! shadow's judgement both do, over a `MemoryStorage` runner and the shadow
//! world's proven transactions and stateless manager.

#[path = "support/gasp_shadow_world.rs"]
mod world;

use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use async_trait::async_trait;
use bsv_overlay_engine::admission::{
    admit_graph_observed, AdmitRefusal, Committed, GateReason, NoEvidence, UnitOutput, Walked,
};
use bsv_overlay_engine::engine::{Engine, EngineConfig, WalkBreachPolicy};
use bsv_overlay_engine::lookup_service::{LookupService, LookupServiceError};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_overlay_engine::types::*;
use bsv_rs::transaction::{MerklePath, Transaction};
use world::*;

/// A body's answer in its graph, by kind.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Answer {
    Landed,
    NotNow,
    Lookup,
    Other(String),
}

/// What one `admit_graph` gave: each body's answer and, for a body that
/// landed, its STEAK (sorted).
#[derive(Debug, Default)]
struct Judged {
    answers: BTreeMap<String, Answer>,
    refusals: BTreeMap<String, AdmitRefusal>,
    steaks: BTreeMap<String, String>,
    /// Why the dependency gate held a body (absent: its own answer).
    gates: BTreeMap<String, GateReason>,
}

impl Judged {
    fn of(&self, tx: &Transaction) -> Option<Answer> {
        self.answers.get(&tx.id()).cloned()
    }
    fn gate(&self, tx: &Transaction) -> Option<GateReason> {
        self.gates.get(&tx.id()).cloned()
    }
}

/// A body the finalize hands on: the transaction with its proof.
fn proven_beef(tx: &Transaction, height: u32) -> Vec<u8> {
    let mut tx = tx.clone();
    tx.merkle_path = Some(MerklePath::from_hex(&honest_proof(&tx.id(), height)).unwrap());
    tx.to_beef(false).unwrap()
}

/// Refuses (deterministically, before any apply) every output of `txid`.
struct RefusesOutputsOf(String);

#[async_trait(?Send)]
impl LookupService for RefusesOutputsOf {
    fn reads_off_chain_values(&self, _: &str) -> bool {
        false
    }
    fn admission_mode(&self) -> bsv_overlay_engine::types::AdmissionMode {
        bsv_overlay_engine::types::AdmissionMode::LockingScript
    }
    fn spend_notification_mode(&self) -> SpendNotificationMode {
        SpendNotificationMode::None
    }
    fn check_admitted(&self, payload: &OutputAdmittedByTopic) -> Result<(), LookupServiceError> {
        match payload {
            OutputAdmittedByTopic::LockingScript { txid, .. } if *txid == self.0 => {
                Err(bsv_overlay_engine::admission::LookupRefusal::Deterministic(
                    "the index refuses this body".into(),
                )
                .into())
            }
            _ => Ok(()),
        }
    }
    async fn output_admitted_by_topic(
        &self,
        _: &OutputAdmittedByTopic,
    ) -> Result<(), LookupServiceError> {
        Ok(())
    }
    async fn output_evicted(&self, _: &str, _: u32) -> Result<(), LookupServiceError> {
        Ok(())
    }
    async fn lookup(&self, _: &LookupQuestion) -> Result<LookupResult, LookupServiceError> {
        Ok(LookupResult::OutputList(vec![]))
    }
    async fn get_documentation(&self) -> String {
        String::new()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata::default()
    }
}

/// An engine over `store`: the world's manager (erring at the finalize on
/// `fails`), and a lookup service refusing `refuses`'s outputs.
fn engine(store: &Rc<MemoryStorage>, fails: Option<String>, refuses: Option<String>) -> Engine {
    let lookups: HashMap<String, Box<dyn LookupService>> = refuses
        .map(|txid| {
            HashMap::from([(
                "ls_refuses".to_string(),
                Box::new(RefusesOutputsOf(txid)) as Box<dyn LookupService>,
            )])
        })
        .unwrap_or_default();
    Engine::new(
        managers(fails),
        lookups,
        Box::new(store.clone()),
        None,
        EngineConfig::default(),
    )
}

/// Each body walked as the shadow walks a finalize body
/// (`historical-tx-no-spv`, no network), in graph order.
async fn walk(engine: &Engine, graph: &[(&Transaction, u32)]) -> Vec<Walked> {
    let mut walked = Vec::new();
    for (tx, height) in graph {
        let tagged = TaggedBEEF::new(proven_beef(tx, *height), vec![TOPIC.to_string()]);
        walked.push(
            engine
                .walk_with_mode(
                    &tagged,
                    SubmitMode::HistoricalTxNoSpv,
                    WalkBreachPolicy::NotNow,
                    &NoEvidence,
                )
                .await
                .unwrap(),
        );
    }
    walked
}

/// A's `admit_graph` over `graph`, each body's answer as it is given.
async fn admit(
    engine: &Engine,
    store: &Rc<MemoryStorage>,
    graph: &[(&Transaction, u32)],
) -> Judged {
    let walked = walk(engine, graph).await;
    let mut judged = Judged::default();
    admit_graph_observed(
        engine,
        store.as_ref(),
        &walked,
        &mut |body, answer, gate| {
            if let Some(gate) = gate {
                judged.gates.insert(body.txid.clone(), gate.clone());
            }
            record(&mut judged, body, answer);
        },
    );
    judged
}

fn record(judged: &mut Judged, body: &Walked, answer: &Result<Committed, AdmitRefusal>) {
    let kind = match answer {
        Ok(Committed {
            output: UnitOutput::Admitted(admitted),
            ..
        }) => {
            let steak: BTreeMap<_, _> = admitted.steak.iter().collect();
            judged
                .steaks
                .insert(body.txid.clone(), serde_json::to_string(&steak).unwrap());
            Answer::Landed
        }
        Ok(_) => Answer::Other("done".into()),
        Err(refusal) => {
            judged.refusals.insert(body.txid.clone(), refusal.clone());
            match refusal {
                AdmitRefusal::NotNow { .. } => Answer::NotNow,
                AdmitRefusal::Lookup { .. } => Answer::Lookup,
                other => Answer::Other(format!("{other:?}")),
            }
        }
    };
    judged.answers.insert(body.txid.clone(), kind);
}

async fn rows(store: &Rc<MemoryStorage>, txs: &[&Transaction]) -> BTreeMap<String, String> {
    served_rows(store.as_ref(), txs).await
}

fn applied(rows: &BTreeMap<String, String>, tx: &Transaction) -> String {
    rows[&format!("applied {}", tx.id())].clone()
}

/// `r` spends `a.0` and `b.0`; all proven.
fn fan_in() -> [Transaction; 3] {
    let a = genesis(60, 1);
    let b = genesis(61, 1);
    let r = spends(&[(&a, 0), (&b, 0)], 1);
    [a, b, r]
}

// ============================================================================
// The fan-in: the good parent lands, the erring parent and the root wait, in
// either batch order.
// ============================================================================

#[tokio::test]
async fn d44_fan_in_the_good_parent_lands_the_erring_parent_and_the_root_wait() {
    let [a, b, r] = fan_in();
    let mut seen = Vec::new();
    for order in [[&a, &b], [&b, &a]] {
        let store = Rc::new(MemoryStorage::new());
        let engine = engine(&store, Some(a.id()), None);
        let judged = admit(
            &engine,
            &store,
            &[(order[0], 100), (order[1], 101), (&r, 102)],
        )
        .await;
        println!(
            "D44 fan-in, {} first: {:?}",
            &order[0].id()[..8],
            judged.answers
        );
        assert_eq!(
            judged.of(&b),
            Some(Answer::Landed),
            "the sibling lands: {judged:?}"
        );
        assert_eq!(
            judged.of(&a),
            Some(Answer::NotNow),
            "the erring parent waits, not now (never terminal): {judged:?}"
        );
        assert_eq!(
            judged.of(&r),
            Some(Answer::NotNow),
            "the root waits with the same word: {judged:?}"
        );
        assert_eq!(judged.gate(&a), Some(GateReason::ParentError));
        assert_eq!(judged.gate(&r), Some(GateReason::WaitingOn(vec![a.id()])));
        assert_eq!(judged.gate(&b), None);
        let rows = rows(&store, &[&a, &b, &r]).await;
        assert_eq!(applied(&rows, &b), "true");
        assert_eq!(applied(&rows, &a), "false", "the erring unit wrote nothing");
        assert_eq!(applied(&rows, &r), "false");
        seen.push(rows);
    }
    assert_eq!(seen[0], seen[1], "the batch order does not move a row");
}

// ============================================================================
// The chain: the error at body 2, body 1 lands, 2 and 3 wait.
// ============================================================================

#[tokio::test]
async fn d44_chain_the_error_at_body_2_body_1_lands_2_and_3_wait() {
    let g = genesis(62, 1);
    let c = child(&g, 0, 1);
    let d = child(&c, 0, 1);
    let store = Rc::new(MemoryStorage::new());
    let engine = engine(&store, Some(c.id()), None);
    let judged = admit(&engine, &store, &[(&g, 100), (&c, 101), (&d, 102)]).await;
    println!("D44 chain: {:?}", judged.answers);
    assert_eq!(judged.of(&g), Some(Answer::Landed), "{judged:?}");
    assert_eq!(judged.of(&c), Some(Answer::NotNow), "{judged:?}");
    assert_eq!(judged.of(&d), Some(Answer::NotNow), "{judged:?}");
    assert_eq!(judged.gate(&c), Some(GateReason::ParentError));
    assert_eq!(judged.gate(&d), Some(GateReason::WaitingOn(vec![c.id()])));
    let rows = rows(&store, &[&g, &c, &d]).await;
    assert_eq!(applied(&rows, &g), "true");
    assert_eq!(applied(&rows, &c), "false");
    assert_eq!(applied(&rows, &d), "false");
    assert!(
        rows[&format!("output {}.0", g.id())].starts_with("spent=false"),
        "the erring body spent nothing: {rows:?}"
    );
}

// ============================================================================
// The re-sync: the peer's next sync re-sends the waiting part, and it lands
// with the same judgement as a first-time body (the digests equal).
// ============================================================================

#[tokio::test]
async fn d44_the_waiting_part_re_sent_lands_as_a_first_time_body() {
    let [a, b, r] = fan_in();
    let txs = [&a, &b, &r];

    // A first-time graph, no manager error: every body lands at once.
    let first = Rc::new(MemoryStorage::new());
    let healthy = engine(&first, None, None);
    let once = admit(&healthy, &first, &[(&a, 100), (&b, 101), (&r, 102)]).await;
    assert!(
        once.answers.values().all(|a| *a == Answer::Landed),
        "{once:?}"
    );
    let once_rows = rows(&first, &txs).await;

    // The erring tick: the sibling lands, the rest waits.
    let store = Rc::new(MemoryStorage::new());
    let erring = engine(&store, Some(a.id()), None);
    let tick = admit(&erring, &store, &[(&a, 100), (&b, 101), (&r, 102)]).await;
    assert_eq!(tick.of(&b), Some(Answer::Landed), "{tick:?}");
    assert_eq!(tick.of(&a), Some(Answer::NotNow), "{tick:?}");
    assert_eq!(tick.of(&r), Some(Answer::NotNow), "{tick:?}");

    // The next sync: the manager answers; the peer re-sends the waiting part
    // (the held sibling is stripped by the walk, as GASP strips held inputs).
    let healed = engine(&store, None, None);
    let again = admit(&healed, &store, &[(&a, 100), (&r, 102)]).await;
    println!("D44 re-sync: {:?}", again.answers);
    assert_eq!(again.of(&a), Some(Answer::Landed), "{again:?}");
    assert_eq!(again.of(&r), Some(Answer::Landed), "{again:?}");

    let digest_of = |rows: &BTreeMap<String, String>, judged: &[&Judged]| {
        let mut lines: Vec<String> = rows.iter().map(|(k, v)| format!("row {k} {v}")).collect();
        for tx in [&a, &r] {
            let steak = judged
                .iter()
                .find_map(|j| j.steaks.get(&tx.id()))
                .cloned()
                .unwrap_or_default();
            lines.push(format!("steak {} {steak}", tx.id()));
        }
        digest(&lines)
    };
    let once_digest = digest_of(&once_rows, &[&once]);
    let resync_digest = digest_of(&rows(&store, &txs).await, &[&tick, &again]);
    println!("D44 re-sync digest {resync_digest}, first-time {once_digest}");
    assert_eq!(
        resync_digest, once_digest,
        "the waiting part lands with the same judgement and the same rows as a first-time body"
    );
}

// ============================================================================
// A lookup refusal: terminal and unit-wide (D34); its descendants refused
// with the same typed word, never `not now`.
// ============================================================================

#[tokio::test]
async fn d44_a_lookup_refusal_is_terminal_and_its_descendants_take_its_word() {
    let g = genesis(63, 1);
    let c = child(&g, 0, 1);
    let d = child(&c, 0, 1);
    let store = Rc::new(MemoryStorage::new());
    let engine = engine(&store, None, Some(c.id()));
    let judged = admit(&engine, &store, &[(&g, 100), (&c, 101), (&d, 102)]).await;
    println!("D44 lookup: {:?}", judged.answers);
    assert_eq!(judged.of(&g), Some(Answer::Landed), "{judged:?}");
    assert_eq!(judged.of(&c), Some(Answer::Lookup), "{judged:?}");
    assert_eq!(
        judged.of(&d),
        Some(Answer::Lookup),
        "the descendant is refused with the same typed word, not `not now`: {judged:?}"
    );
    assert_eq!(
        judged.refusals.get(&d.id()),
        judged.refusals.get(&c.id()),
        "the same word"
    );
    assert_eq!(judged.gate(&c), None, "its own refusal");
    assert_eq!(judged.gate(&d), Some(GateReason::WaitingOn(vec![c.id()])));
    let rows = rows(&store, &[&g, &c, &d]).await;
    assert_eq!(
        applied(&rows, &c),
        "false",
        "unit-wide: nothing of it written"
    );
    assert!(rows[&format!("output {}.0", g.id())].starts_with("spent=false"));
}

// ============================================================================
// The lens's LOW 1: the gate does not depend on the batch's order. A child
// handed before its parent gets the in-order answer, gate and rows (GASP's
// own batch is ancestors first already; another caller's need not be).
// ============================================================================

/// Each body's answer, gate and STEAK, and the rows, of one `admit_graph`.
async fn judged_in(
    fails: Option<String>,
    graph: &[(&Transaction, u32)],
    txs: &[&Transaction],
) -> (String, BTreeMap<String, String>) {
    let store = Rc::new(MemoryStorage::new());
    let engine = engine(&store, fails, None);
    let judged = admit(&engine, &store, graph).await;
    let word = format!(
        "answers {:?} gates {:?} steaks {:?}",
        judged.answers, judged.gates, judged.steaks
    );
    (word, rows(&store, txs).await)
}

#[tokio::test]
async fn d44_l1_a_child_handed_before_its_parent_gets_the_in_order_answer() {
    let g = genesis(64, 1);
    let c = child(&g, 0, 1);
    let d = child(&c, 0, 1);
    let chain = [&g, &c, &d];
    let in_order = judged_in(None, &[(&g, 100), (&c, 101), (&d, 102)], &chain).await;
    let reversed = judged_in(None, &[(&d, 102), (&c, 101), (&g, 100)], &chain).await;
    println!("D44 L1 chain in order: {}", in_order.0);
    println!("D44 L1 chain reversed: {}", reversed.0);
    assert!(in_order.0.contains("Landed") && !in_order.0.contains("NotNow"));
    assert_eq!(
        reversed, in_order,
        "a reversed chain: the same words and rows"
    );

    let [a, b, r] = fan_in();
    let txs = [&a, &b, &r];
    for fails in [None, Some(a.id())] {
        let parents_first =
            judged_in(fails.clone(), &[(&a, 100), (&b, 101), (&r, 102)], &txs).await;
        let root_first = judged_in(fails.clone(), &[(&r, 102), (&a, 100), (&b, 101)], &txs).await;
        println!(
            "D44 L1 fan-in fails={}: root first {}",
            fails.is_some(),
            root_first.0
        );
        assert_eq!(
            root_first,
            parents_first,
            "a root-first fan-in (the manager erring on a: {}): the same words and rows",
            fails.is_some()
        );
    }
}
