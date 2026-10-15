//! The walk/log boundary. Network evidence belongs to a submit, never an isolate.
use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use bsv_rs::transaction::Transaction;
use serde::{Deserialize, Serialize};

use crate::engine::{AdmissionRefusal, MutationReport, WalkBreachPolicy, WalkCouldNotRun};
use crate::storage::{Storage, StorageError};
use crate::types::{Steak, SubmitMode, TaggedBEEF};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalkOutcome {
    Spv,
    HistoricalTxNoSpv,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NetworkSource {
    Arc,
    Broadcaster { name: String, at_ms: i64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NetworkVerdict {
    Accepted { by: NetworkSource, status: String },
    Refused { status: String },
    Unavailable { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProofVerdict {
    Holds { height: u32, merkle_root: [u8; 32] },
    Fails { refusal: String },
    Unavailable { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SidecarVerdict {
    Stored { key: String },
    NotStored { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxEvidence {
    pub walk: WalkOutcome,
    pub network: Option<NetworkVerdict>,
    pub proof: Option<ProofVerdict>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmitEvidence {
    pub tx: BTreeMap<String, TxEvidence>,
    #[serde(with = "sidecar_map")]
    pub sidecars: BTreeMap<[u8; 32], SidecarVerdict>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefusalNote {
    pub kind: String,
    pub reason: String,
}
pub type RefusalNotes = Vec<(String, RefusalNote)>;

/// Local staged payload. Hosts may transport the bytes inline or by a verified
/// content-addressed pointer; resolving that pointer must precede the unit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Walked {
    pub txid: String,
    pub topics: Vec<String>,
    pub beef: BeefCarry,
    pub off_chain_values: Option<Vec<u8>>,
    pub mode: SubmitMode,
    pub policy: WalkBreachPolicy,
    pub walk: WalkOutcome,
    pub carried: Vec<String>,
    pub evidence: AdmitEvidence,
    pub charge: u64,
    pub could_not_run: Option<WalkCouldNotRun>,
    /// Shared by every topic and carried landing attempt for this submit.
    #[serde(skip)]
    pub predecessor_reads: std::rc::Rc<std::cell::Cell<usize>>,
    /// The txids the subject's inputs spend, from the walk's own parse, read
    /// by the dependency gate (the gate lens's NOTE 3: one parse per body).
    /// `None` (a body off the wire, or built by hand): the gate parses the
    /// BEEF itself.
    #[serde(skip)]
    pub spends: Option<Vec<String>>,
}

#[async_trait(?Send)]
pub trait EvidenceSource {
    async fn gather(
        &self,
        subject: &Transaction,
        bodies: &[&Transaction],
        topics: &[String],
    ) -> AdmitEvidence;
}

/// For managers needing no host network evidence. The engine supplies each
/// body's walk verdict; a host with acceptance gates supplies the other fields.
pub struct NoEvidence;
#[async_trait(?Send)]
impl EvidenceSource for NoEvidence {
    async fn gather(&self, _: &Transaction, _: &[&Transaction], _: &[String]) -> AdmitEvidence {
        AdmitEvidence::default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdmitRefusal {
    /// Pure lookup checks refused before any topic apply. The original typed
    /// service errors are retained, just as in `MutationReport::refused`.
    #[error(
        "{}lookup preflight refused: {refused:?}",
        parent_prefix(refused_parent.as_deref())
    )]
    Lookup {
        refused: Vec<AdmissionRefusal>,
        notes: RefusalNotes,
        /// Set by the dependency gate (D44) on a GASP body it held behind a
        /// parent of its graph refused with this word: the txid of the body
        /// whose own refusal it is (the gate lens's NOTE 1). `None`: the
        /// body's own. Boxed: the enum keeps its size.
        refused_parent: Option<Box<str>>,
    },
    #[error("{}{site}: {reason}", parent_prefix(refused_parent.as_deref()))]
    Deterministic {
        topic: String,
        site: String,
        reason: String,
        notes: RefusalNotes,
        /// As [`AdmitRefusal::Lookup`]'s.
        refused_parent: Option<Box<str>>,
    },
    #[error("{site}: {reason}")]
    Transient {
        site: String,
        reason: String,
        notes: RefusalNotes,
    },
    #[error("{site}: {reason}")]
    NotNow {
        site: String,
        reason: String,
        notes: RefusalNotes,
        predecessor: Option<(String, String)>,
    },
}
/// A held descendant's word names the refused parent first.
fn parent_prefix(refused_parent: Option<&str>) -> String {
    refused_parent.map_or_else(String::new, |txid| format!("parent {txid} was refused: "))
}

impl AdmitRefusal {
    #[allow(
        clippy::needless_pass_by_value,
        reason = "accepts owned errors at map_err call sites"
    )]
    pub fn transient(site: impl Into<String>, reason: impl ToString) -> Self {
        Self::Transient {
            site: site.into(),
            reason: reason.to_string(),
            notes: vec![],
        }
    }
    pub fn not_now(site: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::NotNow {
            site: site.into(),
            reason: reason.into(),
            notes: vec![],
            predecessor: None,
        }
    }
    pub fn with_notes(mut self, value: RefusalNotes) -> Self {
        match &mut self {
            Self::Lookup { notes, .. }
            | Self::Deterministic { notes, .. }
            | Self::Transient { notes, .. }
            | Self::NotNow { notes, .. } => *notes = value,
        }
        self
    }
    pub fn notes(&self) -> &RefusalNotes {
        match self {
            Self::Lookup { notes, .. }
            | Self::Deterministic { notes, .. }
            | Self::Transient { notes, .. }
            | Self::NotNow { notes, .. } => notes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LookupRefusal {
    #[error("{0}")]
    Deterministic(String),
    #[error("{0}")]
    Transient(String),
}

#[derive(Debug, Clone)]
pub struct Admitted {
    pub steak: Steak,
    pub report: MutationReport,
    pub dedup: bool,
    pub seq: u64,
    pub notes: RefusalNotes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxEvent {
    pub txid: String,
    pub topic: String,
    pub output_index: u32,
    pub event_type: String,
    pub envelope: Vec<u8>,
}

/// One open transaction. Implementations must record every failed statement,
/// including errors swallowed by a caller, and refuse commit if one failed.
/// Managers are built over this scope. No thread-local state is involved.
pub trait LogTxn: Storage {
    fn sql(&self) -> &dyn SqlExecutor;
    fn after_commit(&self, intent: AfterCommit);
    fn defer(&self, site: &str, reason: String);
    fn deferral(&self) -> Option<AdmitRefusal>;
    fn seq(&self) -> u64;
    fn now_ms(&self) -> i64;
    fn evidence(&self) -> Arc<AdmitEvidence>;
    fn note(&self, topic: &str, note: RefusalNote);
    fn notes(&self) -> RefusalNotes;
    fn emit(&self, event: OutboxEvent);
    /// Name the active hook so a Pending refusal identifies its owner.
    fn at(&self, site: &str);
    /// Keep helper faults visible even when a legacy read helper returns a
    /// conservative fallback. A unit with a recorded failure cannot commit.
    fn fail(&self, reason: String);
    /// A nested dry judgement shares reads and the unit sequence but refuses
    /// every attempted write. Dropping the guard restores the prior mode.
    fn read_only(&self) -> Box<dyn DryRunScope + '_>;
}

/// Transport uncertainty is distinct from a rollback refusal. A lost answer
/// may conceal a committed unit and must be resolved by replay.
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("{0}")]
    Refused(#[from] AdmitRefusal),
    #[error("admission answer lost; replay required")]
    AnswerLost,
}

mod sidecar_map {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        value: &BTreeMap<[u8; 32], SidecarVerdict>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value.iter().collect::<Vec<_>>().serialize(serializer)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<[u8; 32], SidecarVerdict>, D::Error> {
        Ok(
            Vec::<([u8; 32], SidecarVerdict)>::deserialize(deserializer)?
                .into_iter()
                .collect(),
        )
    }
}

/// A read-only judgement scope. A swallowed attempted write still fails it.
pub trait DryRunScope {
    fn failure(&self) -> Option<String>;
}

/// Opens one unit and polls its body once. A lost answer escapes as a transport
/// failure, never a rollback refusal. Native memory transport uses `LogError`
/// as an unwind payload for that arm; Worker bindings abort the instance.
pub trait UnitRunner {
    fn run_unit<'a>(
        &'a self,
        kind: UnitKind,
        evidence: &'a AdmitEvidence,
        unit: LocalBoxFuture<'a, Result<UnitOutput, AdmitRefusal>>,
    ) -> Result<Committed, AdmitRefusal>;
}
#[derive(Debug, Clone, Copy)]
pub enum UnitKind {
    Admit,
    Proof,
    Evict,
    Unevict,
    Readmit,
    Gc,
    Renotify,
}
#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "frozen UnitOutput carries Admitted by value"
)]
pub enum UnitOutput {
    Admitted(Admitted),
    Done,
}
#[derive(Debug)]
pub struct Committed {
    pub seq: u64,
    pub output: UnitOutput,
    pub notes: RefusalNotes,
}
#[derive(Debug)]
pub enum GraphOutcome {
    AllLanded {
        seqs: Vec<u64>,
    },
    /// The owner's D44 (`dependency gate`): every body whose parents landed
    /// was judged as its own unit and these did not land; the rest did.
    Held {
        landed: Vec<u64>,
        held: Vec<HeldBody>,
    },
}

/// A body of a GASP graph that did not land, its refusal, and why the
/// dependency gate held it (`None`: its own refusal, met by its own unit).
#[derive(Debug, Clone)]
pub struct HeldBody {
    pub txid: String,
    pub refusal: AdmitRefusal,
    pub gate: Option<GateReason>,
}

/// Why the dependency gate (D44) held a body of a GASP graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateReason {
    /// Its own topic manager answered `Err` at the finalize: the unit is
    /// refused `not now` (never terminal; nothing of it is written), and every
    /// descendant of it in the graph waits on it.
    ParentError,
    /// A parent of this graph it spends (named, in the order of its inputs)
    /// did not land in this graph: refused without a unit, with that parent's
    /// word (a terminal lookup or deterministic refusal is the same typed
    /// word; anything else is `not now`).
    WaitingOn(Vec<String>),
}

/// The site of the gate's refusals.
pub const GATE_PARENT_ERROR_SITE: &str = "gasp_finalize.topic_manager";
pub const GATE_WAITING_SITE: &str = "gasp_finalize.dependency_gate";

/// A body of a GASP graph its caller could not walk, and the walk's refusal
/// (the gate lens's LOW 2). It is handed to the gate as a held parent: its
/// descendants in the graph wait on it (its word when terminal, else `not
/// now`) and its independent siblings are still judged. A body the caller
/// drops instead (neither walked nor named here) is a parent the gate cannot
/// see: its descendants are judged as if it were outside the graph.
#[derive(Debug, Clone)]
pub struct Unwalked {
    pub txid: String,
    pub refusal: AdmitRefusal,
}

/// The host of a GASP graph's units (bsv-low #657, the a17 seam): the
/// dependency gate run by [`admit_graph_hosted`] asks it, one body at a time
/// and in the gate's order (ancestors first), to resolve that body's carried
/// bytes and to run it as one unit of the host's own log, and tells it each
/// body's answer with the gate's reason. The gate holds no body's resolved
/// bytes past that body's unit, so the host holds one body's at a time.
#[async_trait(?Send)]
pub trait GraphHost {
    /// An R2-carried body's original BEEF (`BeefCarry::R2`: the object under
    /// its key, of its length), read just before that body's own unit. Never
    /// asked for an inline body, nor for a body the gate holds. A refusal is
    /// the body's own: it opens no unit and its descendants wait on it.
    async fn resolve(&self, body: &Walked) -> Result<Vec<u8>, AdmitRefusal>;
    /// Run `body` as ONE unit of the host's log, inside the host's FIFO turn,
    /// its resolved bytes handed in (`None` for an inline body) and released
    /// when it returns. The unit's body is [`gasp_unit`]: the gate's rule 3
    /// (a topic manager's `Err` refuses the unit `not now`) lives in it, and
    /// the gate reads that refusal's site as `GateReason::ParentError`.
    async fn run(
        &self,
        body: &Walked,
        resolved: Option<Vec<u8>>,
    ) -> Result<Committed, AdmitRefusal>;
    /// Each body's answer as the gate gives it, in the gate's order, with the
    /// gate's reason (`None`: the body's own answer, from its unit).
    fn answered(
        &self,
        _body: &Walked,
        _answer: &Result<Committed, AdmitRefusal>,
        _gate: Option<&GateReason>,
    ) {
    }
}

/// One GASP body's unit (the dependency gate's rule 3, D44): `engine.admit`,
/// then a topic of the body neither applied nor deduped (the topic manager
/// answered `Err` at the finalize: F5's empty judgement) refuses the whole
/// unit `not now` at [`GATE_PARENT_ERROR_SITE`], so it rolls back and its
/// descendants wait. A host's [`GraphHost::run`] runs this inside its unit.
pub async fn gasp_unit(
    engine: &crate::engine::Engine,
    walked: &Walked,
) -> Result<UnitOutput, AdmitRefusal> {
    let admitted = engine.admit(walked).await?;
    let failed: Vec<&str> = walked
        .topics
        .iter()
        .filter(|t| {
            !admitted.report.applied_topics.contains(t)
                && !admitted.report.deduped_topics.contains(t)
        })
        .map(String::as_str)
        .collect();
    if !failed.is_empty() {
        return Err(AdmitRefusal::NotNow {
            site: GATE_PARENT_ERROR_SITE.into(),
            reason: format!(
                "the topic manager failed on it ({}): its descendants wait",
                failed.join(", ")
            ),
            notes: admitted.notes,
            predecessor: None,
        });
    }
    Ok(UnitOutput::Admitted(admitted))
}

/// GASP finalize: each body lands independently, ancestors first, under the
/// dependency gate (D44).
pub fn admit_graph(
    engine: &crate::engine::Engine,
    runner: &dyn UnitRunner,
    graph: &[Walked],
) -> GraphOutcome {
    admit_graph_observed(engine, runner, graph, &[], &mut |_, _, _| {})
}

/// Handed each body of a graph, its unit's answer and the gate's reason.
pub type GraphObserver<'a> =
    dyn FnMut(&Walked, &Result<Committed, AdmitRefusal>, Option<&GateReason>) + 'a;

/// The txids a body's inputs spend, its own excluded: the walk's own parse
/// when the body carries it ([`Walked::spends`]), else the body's BEEF parsed.
fn spent_txids(walked: &Walked) -> Vec<String> {
    if let Some(spends) = &walked.spends {
        return spends.clone();
    }
    let tx = match &walked.beef {
        BeefCarry::Inline(bytes) => crate::beef_limits::transaction_from_beef(
            bytes,
            Some(&walked.txid),
            &crate::beef_limits::ENGINE_BEEF_LIMITS,
        )
        .ok(),
        BeefCarry::R2 { raw_txs, .. } => raw_txs
            .iter()
            .filter_map(|raw| Transaction::from_binary(raw).ok())
            .find(|tx| tx.id() == walked.txid),
    };
    tx.map(|tx| spends_of(&tx, &walked.txid))
        .unwrap_or_default()
}

/// The txids `tx`'s inputs spend, in input order, once each, `txid` excluded.
pub(crate) fn spends_of(tx: &Transaction, txid: &str) -> Vec<String> {
    let mut spent: Vec<String> = Vec::new();
    for input in &tx.inputs {
        if let Ok(source) = input.get_source_txid() {
            if source != txid && !spent.contains(&source) {
                spent.push(source);
            }
        }
    }
    spent
}

/// The graph's bodies, each after every body of the graph it spends: Kahn's
/// order, the graph's own the tie-break, so an ancestors-first batch (GASP's
/// `compute_ordered_beefs`) is its own order. A cycle of txids cannot exist
/// (a txid commits to its inputs'); were one met, its bodies follow in graph
/// order.
fn ancestors_first(graph: &[Walked], spends: &[Vec<String>]) -> Vec<usize> {
    let mut by_txid: std::collections::HashMap<&str, Vec<usize>> = std::collections::HashMap::new();
    for (i, walked) in graph.iter().enumerate() {
        by_txid.entry(walked.txid.as_str()).or_default().push(i);
    }
    let mut waiting = vec![0usize; graph.len()];
    let mut children: Vec<Vec<usize>> = vec![Vec::new(); graph.len()];
    for (child, spent) in spends.iter().enumerate() {
        for parent in spent
            .iter()
            .filter_map(|p| by_txid.get(p.as_str()))
            .flatten()
        {
            waiting[child] += 1;
            children[*parent].push(child);
        }
    }
    let mut ready: std::collections::BTreeSet<usize> =
        (0..graph.len()).filter(|i| waiting[*i] == 0).collect();
    let mut order = Vec::with_capacity(graph.len());
    while let Some(next) = ready.pop_first() {
        order.push(next);
        for child in &children[next] {
            waiting[*child] -= 1;
            if waiting[*child] == 0 {
                ready.insert(*child);
            }
        }
    }
    if order.len() < graph.len() {
        let met: std::collections::HashSet<usize> = order.iter().copied().collect();
        order.extend((0..graph.len()).filter(|i| !met.contains(i)));
    }
    order
}

/// [`admit_graph`], handing each body's answer to `observe` as it is given,
/// with the gate's reason when the gate held it (the GASP shadow words each
/// body; bsv-low #657, R7.7 item 5).
///
/// The dependency gate (the owner's D44). The bodies are judged ancestors
/// first ([`ancestors_first`]: the batch's order does not move a word or a
/// row; the lens's LOW 1), so every parent of the graph a body spends was
/// judged before it. The bodies the caller could not walk (`unwalked`) are
/// held before any unit, each its walk's refusal (`HeldBody::gate` `None`),
/// and are parents like any other. For every body:
/// 1. A parent it spends that is a body of this graph and did not land in
///    this graph (refused, held or unwalked) holds it without a unit: `WaitingOn`, the
///    parent's typed word when that is terminal (`Lookup`, `Deterministic`:
///    D34's unit-wide refusal reaches its descendants), else `not now`.
/// 2. A parent outside the graph is not the gate's: it is the walk's (GASP
///    strips held inputs and ends at proven nodes) and the unit's own
///    predecessor question's. A parent of the graph the store already holds
///    is judged as its own unit and dedups: landed "before".
/// 3. A topic manager that answers `Err` at the finalize (a topic of the unit
///    neither applied nor deduped: F5's empty judgement) refuses the unit
///    `not now`, `ParentError`: it rolls back whole and its descendants wait.
/// 4. Any other refusal is the body's own; its descendants wait on it.
/// 5. An independent sibling is judged and lands as its own unit. A body that
///    waited lands on the peer's next sync with the same judgement as any
///    first-time body (nothing of it was written).
pub fn admit_graph_observed(
    engine: &crate::engine::Engine,
    runner: &dyn UnitRunner,
    graph: &[Walked],
    unwalked: &[Unwalked],
    observe: &mut GraphObserver<'_>,
) -> GraphOutcome {
    let mut gate = Gate::new(graph, unwalked);
    while let Some((walked, held)) = gate.next() {
        let (answer, reason) = match held {
            None => {
                let answer = runner.run_unit(
                    UnitKind::Admit,
                    &walked.evidence,
                    Box::pin(gasp_unit(engine, walked)),
                );
                let reason = parent_error(&answer);
                (answer, reason)
            }
            Some((refusal, reason)) => (Err(refusal), Some(reason)),
        };
        observe(walked, &answer, reason.as_ref());
        gate.answer(walked, answer, reason);
    }
    gate.outcome()
}

/// [`admit_graph_observed`] with the units run by a host (the a17 seam):
/// the same dependency gate, in the same order, each body the gate does not
/// hold run as one unit of the host's log through [`GraphHost::run`], each
/// answer handed to [`GraphHost::answered`]. An R2-carried body's bytes are
/// resolved through [`GraphHost::resolve`] just before its own unit and moved
/// into it, one body at a time (never the graph's bytes at once; a held body
/// is never read); a resolve refused is that body's own refusal (its
/// descendants wait on it).
pub async fn admit_graph_hosted(
    host: &dyn GraphHost,
    graph: &[Walked],
    unwalked: &[Unwalked],
) -> GraphOutcome {
    let mut gate = Gate::new(graph, unwalked);
    while let Some((walked, held)) = gate.next() {
        let (answer, reason) = match held {
            None => {
                // This body's bytes alone, read just before its own unit and
                // moved into it: released when `run` returns, before the next
                // body is read (the a9 lane's one-body bound).
                let resolved = match &walked.beef {
                    BeefCarry::R2 { .. } => host.resolve(walked).await.map(Some),
                    BeefCarry::Inline(_) => Ok(None),
                };
                let answer = match resolved {
                    Ok(bytes) => host.run(walked, bytes).await,
                    Err(refusal) => Err(refusal),
                };
                let reason = parent_error(&answer);
                (answer, reason)
            }
            Some((refusal, reason)) => (Err(refusal), Some(reason)),
        };
        host.answered(walked, &answer, reason.as_ref());
        gate.answer(walked, answer, reason);
    }
    gate.outcome()
}

/// The gate's reason for a unit's own refusal: `ParentError` when its topic
/// manager erred ([`gasp_unit`]'s refusal at [`GATE_PARENT_ERROR_SITE`]).
fn parent_error(answer: &Result<Committed, AdmitRefusal>) -> Option<GateReason> {
    matches!(answer, Err(AdmitRefusal::NotNow { site, .. }) if site == GATE_PARENT_ERROR_SITE)
        .then_some(GateReason::ParentError)
}

/// The dependency gate (D44) over one graph, stepped one body at a time, so
/// the synchronous [`admit_graph_observed`] and the host's
/// [`admit_graph_hosted`] run the one rule: [`Gate::next`] names the next
/// body in the order [`ancestors_first`] gives and, when a parent of it in
/// the graph did not land, the refusal and reason it is held with (it opens
/// no unit); [`Gate::answer`] records the body's answer.
struct Gate<'g> {
    graph: &'g [Walked],
    spends: Vec<Vec<String>>,
    order: std::vec::IntoIter<usize>,
    seqs: Vec<u64>,
    landed: std::collections::HashSet<String>,
    refused: BTreeMap<String, AdmitRefusal>,
    held: Vec<HeldBody>,
}

impl<'g> Gate<'g> {
    fn new(graph: &'g [Walked], unwalked: &[Unwalked]) -> Self {
        let bodies: std::collections::HashSet<&str> = graph
            .iter()
            .map(|w| w.txid.as_str())
            .chain(unwalked.iter().map(|u| u.txid.as_str()))
            .collect();
        let spends: Vec<Vec<String>> = graph
            .iter()
            .map(|walked| {
                spent_txids(walked)
                    .into_iter()
                    .filter(|p| bodies.contains(p.as_str()))
                    .collect()
            })
            .collect();
        let mut refused = BTreeMap::new();
        let mut held = Vec::new();
        for body in unwalked {
            refused.insert(body.txid.clone(), body.refusal.clone());
            held.push(HeldBody {
                txid: body.txid.clone(),
                refusal: body.refusal.clone(),
                gate: None,
            });
        }
        let order = ancestors_first(graph, &spends).into_iter();
        Self {
            graph,
            spends,
            order,
            seqs: Vec::new(),
            landed: std::collections::HashSet::new(),
            refused,
            held,
        }
    }

    /// The next body, and the gate's refusal and reason when it is held: a
    /// parent it spends that is a body of this graph and did not land in
    /// this graph (refused, held or unwalked) holds it, with the parent's
    /// typed word when that is terminal (`Lookup`, `Deterministic`, naming
    /// the parent: D34's unit-wide refusal reaches its descendants), else
    /// `not now`.
    fn next(&mut self) -> Option<(&'g Walked, Option<(AdmitRefusal, GateReason)>)> {
        let at = self.order.next()?;
        let walked = &self.graph[at];
        let waiting: Vec<String> = self.spends[at]
            .iter()
            .filter(|p| !self.landed.contains(*p) && self.refused.contains_key(*p))
            .cloned()
            .collect();
        if waiting.is_empty() {
            return Some((walked, None));
        }
        let terminal = waiting.iter().find_map(|p| {
            let mut refusal = self.refused[p].clone();
            match &mut refusal {
                AdmitRefusal::Lookup { refused_parent, .. }
                | AdmitRefusal::Deterministic { refused_parent, .. } => {
                    refused_parent.get_or_insert_with(|| p.as_str().into());
                    Some(refusal)
                }
                _ => None,
            }
        });
        let refusal = terminal.unwrap_or_else(|| {
            AdmitRefusal::not_now(
                GATE_WAITING_SITE,
                format!("waiting on {}", waiting.join(", ")),
            )
        });
        Some((walked, Some((refusal, GateReason::WaitingOn(waiting)))))
    }

    /// Record `walked`'s answer and the gate's reason.
    fn answer(
        &mut self,
        walked: &Walked,
        answer: Result<Committed, AdmitRefusal>,
        gate: Option<GateReason>,
    ) {
        match answer {
            Ok(committed) => {
                self.seqs.push(committed.seq);
                self.refused.remove(&walked.txid);
                self.landed.insert(walked.txid.clone());
            }
            Err(refusal) => {
                if !self.landed.contains(&walked.txid) {
                    self.refused.insert(walked.txid.clone(), refusal.clone());
                }
                self.held.push(HeldBody {
                    txid: walked.txid.clone(),
                    refusal,
                    gate,
                });
            }
        }
    }

    fn outcome(self) -> GraphOutcome {
        if self.held.is_empty() {
            GraphOutcome::AllLanded { seqs: self.seqs }
        } else {
            GraphOutcome::Held {
                landed: self.seqs,
                held: self.held,
            }
        }
    }
}

pub type LocalBoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + 'a>>;

pub const INLINE_BEEF_MAX_BYTES: usize = 1_900_000;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BeefCarry {
    Inline(Vec<u8>),
    R2 {
        key: String,
        sha256: [u8; 32],
        len: u64,
        raw_txs: Vec<Vec<u8>>,
    },
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AfterCommit {
    R2Put { key: String, bytes_from: BeefCarry },
}
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}
#[derive(Debug, Clone, PartialEq)]
pub struct SqlStmt {
    pub sql: String,
    pub binds: Vec<SqlValue>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlRun {
    pub changes: u64,
    pub last_row_id: Option<i64>,
}
pub type SqlRow = Vec<(String, SqlValue)>;
#[async_trait(?Send)]
pub trait SqlExecutor {
    async fn run(&self, sql: &str, binds: &[SqlValue]) -> Result<SqlRun, StorageError>;
    async fn all(&self, sql: &str, binds: &[SqlValue]) -> Result<Vec<SqlRow>, StorageError>;
    /// Atomic as a whole, and with the containing unit.
    async fn batch(&self, stmts: &[SqlStmt]) -> Result<Vec<SqlRun>, StorageError>;
}

impl BeefCarry {
    /// Check a host-resolved object's identity and the raw bodies sent on the hop.
    pub(crate) fn verify_resolved(&self, bytes: &[u8]) -> Result<(), AdmitRefusal> {
        let Self::R2 {
            sha256,
            len,
            raw_txs,
            ..
        } = self
        else {
            return Err(AdmitRefusal::transient(
                "walked_payload",
                "expected R2 descriptor",
            ));
        };
        if bytes.len() as u64 != *len || bsv_rs::primitives::hash::sha256(bytes) != *sha256 {
            return Err(AdmitRefusal::transient(
                "walked_payload",
                "R2 BEEF hash or length mismatch",
            ));
        }
        let beef = crate::beef_limits::parse_beef(bytes, &crate::beef_limits::ENGINE_BEEF_LIMITS)
            .map_err(|e| AdmitRefusal::transient("walked_payload", e))?;
        let mut original: Vec<_> = beef
            .txs
            .iter()
            .filter_map(|body| body.tx().map(Transaction::to_binary))
            .collect();
        let mut carried: Vec<_> = raw_txs.iter().collect();
        original.sort_unstable();
        carried.sort_unstable();
        if original.iter().collect::<Vec<_>>() != carried {
            return Err(AdmitRefusal::transient(
                "walked_payload",
                "R2 BEEF raw transactions mismatch",
            ));
        }
        Ok(())
    }

    /// Hosts persist the original large BEEF under this content-addressed key
    /// before sending the hop. The log uses raw transactions for judgement;
    /// the pointer remains the home of the original proof-bearing BEEF.
    pub(crate) fn from_beef(bytes: &[u8], beef: &bsv_rs::transaction::Beef, _txid: &str) -> Self {
        if bytes.len() <= INLINE_BEEF_MAX_BYTES {
            return Self::Inline(bytes.to_vec());
        }
        let sha256 = bsv_rs::primitives::hash::sha256(bytes);
        use std::fmt::Write;
        let key = sha256
            .iter()
            .fold(String::with_capacity(64), |mut key, byte| {
                write!(key, "{byte:02x}").expect("writing to String cannot fail");
                key
            });
        let raw_txs = beef
            .txs
            .iter()
            .filter_map(|body| body.tx().map(Transaction::to_binary))
            .collect();
        Self::R2 {
            key,
            sha256,
            len: bytes.len() as u64,
            raw_txs,
        }
    }
}
impl Walked {
    /// Resolve the original verified BEEF, never a proof-stripped reconstruction.
    pub(crate) fn tagged_beef(&self, storage: &dyn Storage) -> Result<TaggedBEEF, AdmitRefusal> {
        let bytes = match &self.beef {
            BeefCarry::Inline(bytes) => bytes.clone(),
            BeefCarry::R2 { key, .. } => {
                let bytes = storage.admission_beef(key).ok_or_else(|| {
                    AdmitRefusal::transient(
                        "walked_payload",
                        "R2 BEEF not resolved before admission",
                    )
                })?;
                self.beef.verify_resolved(&bytes)?;
                bytes.to_vec()
            }
        };
        let mut tagged = TaggedBEEF::new(bytes, self.topics.clone());
        tagged.off_chain_values.clone_from(&self.off_chain_values);
        Ok(tagged)
    }
}
