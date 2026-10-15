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

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use async_trait::async_trait;
use bsv_overlay_engine::admission::{
    admit_graph_hosted, admit_graph_observed, gasp_unit, AdmitRefusal, BeefCarry, Committed,
    GateReason, GraphHost, GraphOutcome, NoEvidence, UnitKind, UnitOutput, UnitRunner, Walked,
};
use bsv_overlay_engine::engine::{Engine, EngineConfig, WalkBreachPolicy};
use bsv_overlay_engine::lookup_service::{LookupService, LookupServiceError};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_overlay_engine::storage::Storage;
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
        &[],
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

/// The store's rows of `txs` (`served_rows`, which names a stored BEEF by its
/// length), and each stored BEEF by its BYTES (their SHA-256; the gate lens's
/// NOTE 2).
async fn rows(store: &Rc<MemoryStorage>, txs: &[&Transaction]) -> BTreeMap<String, String> {
    let mut rows = served_rows(store.as_ref(), txs).await;
    for tx in txs {
        for vout in 0..tx.outputs.len() as u32 {
            let beef = store
                .find_output(&tx.id(), vout, Some(TOPIC), None, true)
                .await
                .unwrap()
                .and_then(|o| o.beef)
                .map_or_else(
                    || "none".to_string(),
                    |b| hex::encode(bsv_rs::primitives::hash::sha256(&b)),
                );
            rows.insert(format!("beef {}.{vout}", tx.id()), beef);
        }
    }
    rows
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
    // Each body at its own height whatever the order: the same bytes (the
    // rows name each stored BEEF by its bytes).
    for order in [[(&a, 100), (&b, 101)], [(&b, 101), (&a, 100)]] {
        let store = Rc::new(MemoryStorage::new());
        let engine = engine(&store, Some(a.id()), None);
        let judged = admit(&engine, &store, &[order[0], order[1], (&r, 102)]).await;
        println!(
            "D44 fan-in, {} first: {:?}",
            &order[0].0.id()[..8],
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
    let mut named = judged.refusals[&c.id()].clone();
    if let AdmitRefusal::Lookup { refused_parent, .. } = &mut named {
        *refused_parent = Some(c.id().into());
    }
    assert_eq!(
        judged.refusals.get(&d.id()),
        Some(&named),
        "the same word, naming the refused parent (the gate lens's NOTE 1)"
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

// ============================================================================
// The gate lens's NOTEs: the descendant of a terminally refused parent names
// that parent in its word (NOTE 1); the lens's scratch witnesses kept as pins
// (NOTE 2, NOTE 6).
// ============================================================================

/// The word of a body the gate held behind a terminal refusal names the body
/// whose own refusal it is, through every generation: `g <- c <- d <- e`, the
/// index refusing `c`'s outputs.
#[tokio::test]
async fn d44_n1_a_terminal_refusals_descendants_name_the_refused_parent() {
    let g = genesis(65, 1);
    let c = child(&g, 0, 1);
    let d = child(&c, 0, 1);
    let e = child(&d, 0, 1);
    let store = Rc::new(MemoryStorage::new());
    let engine = engine(&store, None, Some(c.id()));
    let judged = admit(
        &engine,
        &store,
        &[(&g, 100), (&c, 101), (&d, 102), (&e, 103)],
    )
    .await;
    let word = |tx: &Transaction| judged.refusals[&tx.id()].to_string();
    println!("D44 N1 c: {}", word(&c));
    println!("D44 N1 d: {}", word(&d));
    println!("D44 N1 e: {}", word(&e));
    for tx in [&d, &e] {
        assert_eq!(judged.of(tx), Some(Answer::Lookup), "{judged:?}");
        assert!(
            word(tx).starts_with(&format!("parent {} was refused: ", c.id())),
            "the descendant's word names the refused parent: {}",
            word(tx)
        );
        assert!(word(tx).ends_with(&word(&c)), "and carries its word whole");
    }
    assert!(
        !word(&c).contains("was refused"),
        "c's own word: {}",
        word(&c)
    );
}

/// The lens's scratch fan-in: `a`'s manager errs, the index refuses `b`'s
/// outputs. `a` waits `not now` (`parent_error`), `b` is refused with its own
/// word, and the root takes the TERMINAL word (`b`'s: it can never land)
/// waiting on both, naming `b`.
#[tokio::test]
async fn d44_n6_a_fan_in_whose_parents_meet_a_manager_error_and_a_lookup_refusal() {
    let [a, b, r] = fan_in();
    let store = Rc::new(MemoryStorage::new());
    let engine = engine(&store, Some(a.id()), Some(b.id()));
    let judged = admit(&engine, &store, &[(&a, 100), (&b, 101), (&r, 102)]).await;
    println!(
        "D44 N6 fan-in: {:?} gates {:?}",
        judged.answers, judged.gates
    );
    assert_eq!(judged.of(&a), Some(Answer::NotNow));
    assert_eq!(judged.gate(&a), Some(GateReason::ParentError));
    assert_eq!(judged.of(&b), Some(Answer::Lookup));
    assert_eq!(judged.gate(&b), None, "its own refusal");
    assert_eq!(
        judged.of(&r),
        Some(Answer::Lookup),
        "terminal wins: {judged:?}"
    );
    assert_eq!(
        judged.gate(&r),
        Some(GateReason::WaitingOn(vec![a.id(), b.id()]))
    );
    assert!(judged.refusals[&r.id()]
        .to_string()
        .starts_with(&format!("parent {} was refused: ", b.id())));
    let rows = rows(&store, &[&a, &b, &r]).await;
    for tx in [&a, &b, &r] {
        assert_eq!(applied(&rows, tx), "false", "nothing landed: {rows:?}");
    }
}

/// The lens's scratch graph whose every body waits: `g <- c`, the manager
/// erring on the genesis. Nothing lands; `g` is `parent_error`, `c` waits on
/// it; nothing is written.
#[tokio::test]
async fn d44_n6_a_graph_whose_every_body_waits_lands_nothing_and_says_why() {
    let g = genesis(66, 1);
    let c = child(&g, 0, 1);
    let store = Rc::new(MemoryStorage::new());
    let engine = engine(&store, Some(g.id()), None);
    let judged = admit(&engine, &store, &[(&g, 100), (&c, 101)]).await;
    println!(
        "D44 N6 all waiting: {:?} gates {:?}",
        judged.answers, judged.gates
    );
    assert_eq!(judged.of(&g), Some(Answer::NotNow));
    assert_eq!(judged.of(&c), Some(Answer::NotNow));
    assert_eq!(judged.gate(&g), Some(GateReason::ParentError));
    assert_eq!(judged.gate(&c), Some(GateReason::WaitingOn(vec![g.id()])));
    let rows = rows(&store, &[&g, &c]).await;
    assert!(
        rows.iter()
            .all(|(k, v)| !k.starts_with("applied") || v == "false"),
        "{rows:?}"
    );
    assert!(
        rows.iter()
            .all(|(k, v)| !k.starts_with("output") || v == "absent"),
        "{rows:?}"
    );
}

/// The lens's scratch re-sync under ANOTHER proof: the waiting parent `a` is
/// re-sent at height 105 (it was 100). `a` and `r` land with the STEAKs of a
/// first-time graph, and the store's rows, its stored BEEFs by their bytes
/// included, are those of a first-time graph handed those very bytes.
#[tokio::test]
async fn d44_n2_a_waiting_parent_re_sent_under_another_proof_lands_as_a_first_time_body() {
    let [a, b, r] = fan_in();
    let txs = [&a, &b, &r];
    let first = Rc::new(MemoryStorage::new());
    let once = admit(
        &engine(&first, None, None),
        &first,
        &[(&a, 105), (&b, 101), (&r, 102)],
    )
    .await;
    assert!(
        once.answers.values().all(|a| *a == Answer::Landed),
        "{once:?}"
    );

    let store = Rc::new(MemoryStorage::new());
    let tick = admit(
        &engine(&store, Some(a.id()), None),
        &store,
        &[(&a, 100), (&b, 101), (&r, 102)],
    )
    .await;
    assert_eq!(tick.of(&a), Some(Answer::NotNow), "{tick:?}");
    let again = admit(&engine(&store, None, None), &store, &[(&a, 105), (&r, 102)]).await;
    println!("D44 N2 re-sync under another proof: {:?}", again.answers);
    for tx in [&a, &r] {
        assert_eq!(again.of(tx), Some(Answer::Landed), "{again:?}");
        assert_eq!(
            again.steaks.get(&tx.id()),
            once.steaks.get(&tx.id()),
            "the same judgement"
        );
    }
    assert_eq!(
        rows(&store, &txs).await,
        rows(&first, &txs).await,
        "the same rows, the stored BEEFs byte for byte"
    );
}

/// The gate lens's NOTE 3: the gate reads each body's parents from the walk's
/// own parse (`Walked::spends`), not a second parse of its BEEF. The walk
/// carries the fan-in root's two parents; and a sibling whose carried spends
/// are made to name the erring parent is held on it, which only a gate
/// reading the walk's word can do (its BEEF spends nothing of the graph).
#[tokio::test]
async fn d44_n3_the_gate_reads_the_walks_spends() {
    let [a, b, r] = fan_in();
    let store = Rc::new(MemoryStorage::new());
    let engine = engine(&store, Some(a.id()), None);
    let mut walked = walk(&engine, &[(&a, 100), (&b, 101), (&r, 102)]).await;
    assert_eq!(
        walked[2].spends,
        Some(vec![a.id(), b.id()]),
        "the walk's word"
    );
    assert_eq!(
        walked[1].spends,
        Some(vec!["00".repeat(32)]),
        "b's genesis input"
    );
    walked[1].spends = Some(vec![a.id()]);
    let mut gates = BTreeMap::new();
    admit_graph_observed(
        &engine,
        store.as_ref(),
        &walked,
        &[],
        &mut |body, _, gate| {
            if let Some(gate) = gate {
                gates.insert(body.txid.clone(), gate.clone());
            }
        },
    );
    println!("D44 N3 gates: {gates:?}");
    assert_eq!(
        gates.get(&b.id()),
        Some(&GateReason::WaitingOn(vec![a.id()])),
        "the gate took the walk's spends"
    );
}

// ============================================================================
// The a17 seam, item 3: the gate resolves each carried body through the
// host's resolver just before its own unit, one body at a time.
// ============================================================================

/// The host of the a9 lane's shape: the bodies' original BEEFs in its object
/// store ("R2", by key), each read by `resolve`, handed into the memory log
/// for that body's unit and released when the unit returns. It counts the
/// resolved bytes it holds and their peak, and names each step.
struct R2Host<'a> {
    engine: &'a Engine,
    store: Rc<MemoryStorage>,
    objects: HashMap<String, Vec<u8>>,
    held: Cell<usize>,
    peak: Cell<usize>,
    steps: RefCell<Vec<String>>,
}

#[async_trait(?Send)]
impl GraphHost for R2Host<'_> {
    async fn resolve(&self, body: &Walked) -> Result<Vec<u8>, AdmitRefusal> {
        let BeefCarry::R2 { key, .. } = &body.beef else {
            panic!("only an R2 carry is resolved");
        };
        let bytes = self.objects[key].clone();
        self.held.set(self.held.get() + bytes.len());
        self.peak.set(self.peak.get().max(self.held.get()));
        self.steps
            .borrow_mut()
            .push(format!("resolve {}", &body.txid[..8]));
        Ok(bytes)
    }
    async fn run(
        &self,
        body: &Walked,
        resolved: Option<Vec<u8>>,
    ) -> Result<Committed, AdmitRefusal> {
        self.steps
            .borrow_mut()
            .push(format!("run {}", &body.txid[..8]));
        let len = resolved.as_ref().map_or(0, Vec::len);
        if let Some(bytes) = resolved {
            self.store.cache_admission_beef(&body.beef, bytes)?;
        }
        let answer = self.store.run_unit(
            UnitKind::Admit,
            &body.evidence,
            Box::pin(gasp_unit(self.engine, body)),
        );
        if let BeefCarry::R2 { key, .. } = &body.beef {
            self.store.release_admission_beef(key);
        }
        self.held.set(self.held.get() - len);
        answer
    }
}

/// A transaction carrying `bytes` of data in a second output, its BEEF past
/// the inline bound (an R2 carry).
fn heavy(mut tx: Transaction, bytes: usize) -> Transaction {
    let mut data = vec![0x00, 0x6a, 0x4e];
    data.extend(u32::try_from(bytes).unwrap().to_le_bytes());
    data.resize(data.len() + bytes, 0xcd);
    tx.outputs.push(bsv_rs::transaction::TransactionOutput::new(
        1,
        bsv_rs::script::LockingScript::from_binary(&data).unwrap(),
    ));
    tx
}

/// Item 3. The chain `g <- c <- d`, each body's BEEF 4 MiB (three R2
/// carries), through `admit_graph_hosted` over a host whose resolver reads its
/// object store: each body is resolved just before its own unit and released
/// after it, so the host holds at most ONE body's bytes (the a9 lane's rule;
/// the a17 record: a synchronous gate's caller must resolve them all first);
/// a body the gate holds is never read (the erring `g`: `c` and `d` wait).
#[tokio::test]
async fn d44_s3_the_gate_resolves_each_carried_body_one_at_a_time() {
    const MIB4: usize = 4 << 20;
    let g = heavy(genesis(80, 1), MIB4);
    let c = heavy(child(&g, 0, 1), MIB4);
    let d = heavy(child(&c, 0, 1), MIB4);
    let graph = [(&g, 100), (&c, 101), (&d, 102)];
    let objects: HashMap<String, Vec<u8>> = graph
        .iter()
        .map(|(tx, height)| {
            let beef = proven_beef(tx, *height);
            (hex::encode(bsv_rs::primitives::hash::sha256(&beef)), beef)
        })
        .collect();
    let largest = objects.values().map(Vec::len).max().unwrap();
    let total: usize = objects.values().map(Vec::len).sum();
    for fails in [None, Some(g.id())] {
        let store = Rc::new(MemoryStorage::new());
        let engine = engine(&store, fails.clone(), None);
        let walked = walk(&engine, &graph).await;
        assert!(
            walked
                .iter()
                .all(|w| matches!(w.beef, BeefCarry::R2 { .. })),
            "every body rides R2"
        );
        let host = R2Host {
            engine: &engine,
            store: store.clone(),
            objects: objects.clone(),
            held: Cell::new(0),
            peak: Cell::new(0),
            steps: RefCell::default(),
        };
        let outcome = admit_graph_hosted(&host, &walked, &[]).await;
        let steps = host.steps.borrow().clone();
        println!(
            "S3 fails={}: peak held {} B (one body {largest} B, the graph {total} B); steps {steps:?}",
            fails.is_some(),
            host.peak.get()
        );
        assert!(
            host.peak.get() <= largest,
            "at most one body's bytes held at once: peak {} B, one body {largest} B, the graph {total} B",
            host.peak.get()
        );
        assert_eq!(host.held.get(), 0, "every resolved body released");
        let [g8, c8, d8] = [&g, &c, &d].map(|tx| tx.id()[..8].to_string());
        if fails.is_none() {
            assert!(matches!(outcome, GraphOutcome::AllLanded { ref seqs } if seqs.len() == 3));
            assert_eq!(
                steps,
                [
                    format!("resolve {g8}"),
                    format!("run {g8}"),
                    format!("resolve {c8}"),
                    format!("run {c8}"),
                    format!("resolve {d8}"),
                    format!("run {d8}"),
                ]
            );
        } else {
            let GraphOutcome::Held { landed, held } = outcome else {
                panic!("the erring genesis holds the chain");
            };
            assert!(landed.is_empty());
            assert_eq!(
                held.iter().map(|h| h.gate.clone()).collect::<Vec<_>>(),
                [
                    Some(GateReason::ParentError),
                    Some(GateReason::WaitingOn(vec![g.id()])),
                    Some(GateReason::WaitingOn(vec![c.id()])),
                ]
            );
            assert_eq!(
                steps,
                [format!("resolve {g8}"), format!("run {g8}")],
                "a body the gate holds is never read"
            );
        }
    }
}
