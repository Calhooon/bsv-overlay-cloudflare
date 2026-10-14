//! The engine's side of GASP's shadow ([`crate::gasp_shadow`]): what the
//! finalize takes inside the served path (a snapshot before each served write,
//! the served word, the served rows after a graph), and what runs after the
//! peer's sync, outside its race (the walks, `judge_dry`, the units through
//! `admit_graph`, the comparison and the census row).
use super::*;
use crate::admission::*;
use crate::gasp_shadow::AdmissionMode;
use crate::gasp_shadow::*;
use std::collections::BTreeMap;
use std::rc::Rc;

/// A snapshot the served path took for one comparison, and the host's
/// milliseconds spent taking it (the shadow's time, never the served one's).
pub(crate) struct Prepared {
    snapshot: Result<ShadowSnapshot, String>,
    ms: u64,
    /// Counts the snapshot among those held until it is dropped.
    _held: Option<Held>,
}

/// One snapshot held: the engine's count, less one when it is dropped.
pub(crate) struct Held(Rc<std::cell::Cell<usize>>);

impl Held {
    fn new(count: &Rc<std::cell::Cell<usize>>) -> Self {
        count.set(count.get() + 1);
        Self(count.clone())
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        self.0.set(self.0.get().saturating_sub(1));
    }
}

/// What the served finalize answered for one body.
pub(crate) struct BodyServed {
    txid: String,
    position: u32,
    beef: Vec<u8>,
    served: Word,
    served_judgement: Option<Judgement>,
}

/// A body's comparison on its own snapshot (every body but a graph's first).
pub(crate) struct BodyJob {
    peer: String,
    topic: String,
    graph: String,
    prepared: Prepared,
    body: BodyServed,
}

/// A graph's comparison. Its snapshot, taken just before the graph's first
/// served submit, is also that body's: one state, one copy (the shadow lens's
/// NOTE 6). `served` is set after the graph's last served body; a graph a
/// deadline dropped mid-way has none and gives its first body's row alone.
pub(crate) struct GraphJob {
    peer: String,
    topic: String,
    graph: String,
    beefs: Vec<Vec<u8>>,
    prepared: Prepared,
    first: Option<BodyServed>,
    served: Option<Word>,
}

/// One graph's shadow while the served finalize submits it: its job's place
/// in the queue, and what the served path landed and where it stopped.
pub(crate) struct FinalizeShadow {
    host: Rc<dyn GaspShadow>,
    peer: String,
    topic: String,
    root: String,
    slot: usize,
    landed: Vec<String>,
    stopped: Option<(String, Word)>,
    /// The topic managers' errors the served submits met.
    manager_errors: Vec<String>,
    /// The last served body's word.
    last: Option<Word>,
}

impl FinalizeShadow {
    /// The served body landed.
    pub(crate) fn landed(&mut self, beef: &[u8]) {
        self.landed.push(body_txid(beef));
    }

    /// The served finalize stopped at this body.
    pub(crate) fn stopped(&mut self, beef: &[u8]) {
        if let Some(word) = self.last.take() {
            self.stopped = Some((body_txid(beef), word));
        }
    }
}

/// One comparison waiting for the end of its peer's sync.
pub(crate) enum ShadowJob {
    Body(Box<BodyJob>),
    Graph(Box<GraphJob>),
}

/// The topic managers' errors one capture keeps.
const MANAGER_ERRORS_KEPT: usize = 64;

/// The served finalize's evidence for a GASP body: none. It does not walk
/// (`historical-tx-no-spv`, the anchor check already ran) and asks no network,
/// so the shadow's walk is handed exactly that and asks nobody either.
struct ServedEvidence(AdmitEvidence);

#[async_trait::async_trait(?Send)]
impl EvidenceSource for ServedEvidence {
    async fn gather(&self, _: &Transaction, _: &[&Transaction], _: &[String]) -> AdmitEvidence {
        self.0.clone()
    }
}

/// The subject of one finalize BEEF, as the submit's walk names it.
pub(crate) fn body_txid(beef: &[u8]) -> String {
    beef_limits::parse_beef(beef, &beef_limits::ENGINE_BEEF_LIMITS)
        .ok()
        .and_then(|mut parsed| crate::subject::subject_txid_of(&mut parsed))
        .and_then(|subject| {
            beef_limits::transaction_from_beef(
                beef,
                Some(&subject),
                &beef_limits::ENGINE_BEEF_LIMITS,
            )
            .ok()
        })
        .map_or_else(
            || {
                let hash = bsv_rs::primitives::hash::sha256(beef);
                format!("unparsed:{}", hex_of(&hash))
            },
            |tx| tx.id(),
        )
}

fn hex_of(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut out, b| {
        write!(out, "{b:02x}").expect("writing to String cannot fail");
        out
    })
}

/// The rows a graph's bodies reach under `topic`: each body's applied row,
/// each of its outputs and each coin it spends (spent, consumed, consumers,
/// satoshis, script, block height, the stored BEEF's hash), never a score (the
/// served path's is a wall clock). The same reads of the served store and of
/// the shadow's.
async fn materialized_rows(
    store: &dyn Storage,
    topic: &str,
    beefs: &[Vec<u8>],
) -> BTreeMap<String, String> {
    let mut rows = BTreeMap::new();
    let row = |output: Option<Output>| {
        output.map_or_else(
            || "absent".to_string(),
            |o| {
                let mut consumed: Vec<_> = o
                    .outputs_consumed
                    .iter()
                    .map(|c| format!("{}.{}", c.txid, c.output_index))
                    .collect();
                let mut by: Vec<_> = o
                    .consumed_by
                    .iter()
                    .map(|c| format!("{}.{}", c.txid, c.output_index))
                    .collect();
                consumed.sort();
                by.sort();
                format!(
                    "spent={} sats={} script={} consumed={consumed:?} by={by:?} height={:?} beef={}",
                    o.spent,
                    o.satoshis,
                    hex_of(&o.output_script),
                    o.block_height,
                    o.beef
                        .map_or_else(String::new, |b| hex_of(&bsv_rs::primitives::hash::sha256(&b)))
                )
            },
        )
    };
    let fault = |e: StorageError| format!("read fault: {e}");
    for beef in beefs {
        let txid = body_txid(beef);
        let Ok(tx) =
            beef_limits::transaction_from_beef(beef, Some(&txid), &beef_limits::ENGINE_BEEF_LIMITS)
        else {
            rows.insert(format!("body {txid}"), "unparsed".into());
            continue;
        };
        let applied = store
            .does_applied_transaction_exist(&AppliedTransaction {
                txid: txid.clone(),
                topic: topic.into(),
            })
            .await
            .map_or_else(fault, |held| held.to_string());
        rows.insert(format!("applied {txid}"), applied);
        for vout in 0..tx.outputs.len() as u32 {
            let found = store
                .find_output(&txid, vout, Some(topic), None, true)
                .await
                .map_or_else(fault, row);
            rows.insert(format!("output {txid}.{vout}"), found);
        }
        for input in &tx.inputs {
            let Ok(source) = input.get_source_txid() else {
                continue;
            };
            let found = store
                .find_output(&source, input.source_output_index, Some(topic), None, true)
                .await
                .map_or_else(fault, row);
            rows.insert(
                format!("output {source}.{}", input.source_output_index),
                found,
            );
        }
    }
    rows
}

/// A graph's materialization as one word: what landed, where it stopped (the
/// stopping body's whole word: its txid, status and bytes), the topic
/// managers' errors its bodies met, and the rows. Its status is 200 only when
/// nothing stopped; else the stopping body's, and 503 when that body's own
/// word is 200 (a durable report whose topic did not land, a manager that
/// failed: the finalize's "not now", its graph held below the cursor; the
/// shadow lens's MEDIUM 2).
fn materialization_word(
    landed: &[String],
    stopped: Option<&(String, Word)>,
    manager_errors: &[String],
    rows: &BTreeMap<String, String>,
) -> Word {
    Word {
        status: stopped.map_or(
            200,
            |(_, word)| {
                if word.status == 200 {
                    503
                } else {
                    word.status
                }
            },
        ),
        bytes: serde_json::json!({
            "landed": landed,
            "stopped": stopped.map(|(txid, word)| serde_json::json!({
                "txid": txid,
                "status": word.status,
                "bytes": String::from_utf8_lossy(&word.bytes),
            })),
            "manager_errors": manager_errors,
            "rows": rows,
        })
        .to_string()
        .into_bytes(),
        landed_topics: vec![],
    }
}

/// A finalize body's walk refused, as the gate holds it (the gate lens's LOW
/// 2), by the class its word's status names: a 400 is the walk's final word
/// (terminal: its descendants take it), a 503 "not now", anything else a
/// fault (`Transient`: its descendants wait `not now`).
fn walk_refusal(topic: &str, error: &EngineError) -> AdmitRefusal {
    match engine_error_status(error) {
        400 => AdmitRefusal::Deterministic {
            topic: topic.to_string(),
            site: "gasp_finalize.walk".into(),
            reason: error.to_string(),
            notes: vec![],
            refused_parent: None,
        },
        503 => AdmitRefusal::not_now("gasp_finalize.walk", error.to_string()),
        _ => AdmitRefusal::transient("gasp_finalize.walk", error),
    }
}

/// A snapshot that was not taken: the budget's words as they are, any other
/// failure as the host's setup.
fn not_snapshotted(error: &str) -> Word {
    if error.starts_with("not shadowed") {
        Word::not_shadowed(error)
    } else {
        Word::not_shadowed(format!("shadow setup: {error}"))
    }
}

/// What a verdict decided, never who said it or when (the slice's).
fn verdict_class(evidence: &TxEvidence) -> (Option<String>, Option<String>) {
    let network = evidence.network.as_ref().map(|verdict| match verdict {
        NetworkVerdict::Accepted { .. } => "accepted".to_string(),
        NetworkVerdict::Refused { status } => format!("refused {status}"),
        NetworkVerdict::Unavailable { .. } => "unavailable".to_string(),
    });
    let proof = evidence.proof.as_ref().map(|verdict| match verdict {
        ProofVerdict::Holds {
            height,
            merkle_root,
        } => format!("holds {height} {}", hex_of(merkle_root)),
        ProofVerdict::Fails { refusal } => format!("fails {refusal}"),
        ProofVerdict::Unavailable { .. } => "unavailable".to_string(),
    });
    (network, proof)
}

/// Only the txids the served path observed are compared; an empty served
/// record (the finalize's) is never a disagreement.
fn evidence_differs(served: &AdmitEvidence, shadow: &AdmitEvidence) -> bool {
    served.tx.iter().any(|(txid, verdict)| {
        let (network, proof) = verdict_class(verdict);
        shadow.tx.get(txid).map(verdict_class).is_none_or(|(n, p)| {
            (network.is_some() && network != n) || (proof.is_some() && proof != p)
        })
    })
}

/// The `rows` of a materialization word.
fn materialized_rows_of(word: &Word) -> Option<serde_json::Value> {
    serde_json::from_slice::<serde_json::Value>(&word.bytes)
        .ok()
        .and_then(|mut value| value.get_mut("rows").map(serde_json::Value::take))
}

fn classify(row: &GaspCensusRow) -> DeltaClass {
    if row.shadow.status == 0 {
        return DeltaClass::NotShadowed;
    }
    if row.kind == ShadowKind::Materialization {
        return if row.served == row.shadow {
            DeltaClass::Same
        } else if materialized_rows_of(&row.served) == materialized_rows_of(&row.shadow) {
            DeltaClass::Materialization
        } else {
            DeltaClass::RowsDiffer
        };
    }
    if row.shadow.status == 422
        && !row.served.landed_topics.is_empty()
        && row.shadow.landed_topics.is_empty()
    {
        DeltaClass::UnitRefusal
    } else if evidence_differs(&row.served_evidence, &row.shadow_evidence) {
        DeltaClass::Evidence
    } else if row.served != row.shadow || row.served_judgement != row.shadow_judgement {
        DeltaClass::Words
    } else {
        DeltaClass::Same
    }
}

/// The shadow's own time for one comparison: the snapshot's, then its work.
struct Spent<'a> {
    host: &'a dyn GaspShadow,
    before: u64,
    start: u64,
    budget: ShadowBudget,
}

impl Spent<'_> {
    fn ms(&self) -> u64 {
        self.before
            .saturating_add(self.host.now_ms().saturating_sub(self.start))
    }
    fn over(&self) -> Option<String> {
        let ms = self.ms();
        (ms > self.budget.ms).then(|| {
            format!(
                "not shadowed: budget (time: {ms} ms; allowance {} ms)",
                self.budget.ms
            )
        })
    }
}

impl Engine {
    /// The host, when the engine's GASP shadow runs (`Shadow` and a host set).
    pub(crate) fn gasp_shadow_host(&self) -> Option<Rc<dyn GaspShadow>> {
        if self.admission_mode == AdmissionMode::Shadow {
            self.gasp_shadow.clone()
        } else {
            None
        }
    }

    /// The snapshot for one comparison, taken in the served path BEFORE the
    /// served write it is compared with.
    async fn shadow_prepare(
        &self,
        host: &dyn GaspShadow,
        peer: &str,
        topic: &str,
        beefs: &[Vec<u8>],
    ) -> Prepared {
        let held = self.shadow_held.get();
        if held >= self.shadow_budget.snapshots {
            return Prepared {
                snapshot: Err(format!(
                    "not shadowed: budget (snapshots: {held} held; allowance {})",
                    self.shadow_budget.snapshots
                )),
                ms: 0,
                _held: None,
            };
        }
        let start = host.now_ms();
        let snapshot = host
            .snapshot(&ShadowScope { peer, topic, beefs }, &self.shadow_budget)
            .await;
        Prepared {
            _held: snapshot.is_ok().then(|| Held::new(&self.shadow_held)),
            snapshot,
            ms: host.now_ms().saturating_sub(start),
        }
    }

    /// A graph's shadow, begun before its first served submit: the graph's
    /// snapshot (its first body's too), its job queued in the graph's place.
    pub(crate) async fn shadow_graph_begin(
        &self,
        host: Rc<dyn GaspShadow>,
        peer: &str,
        graph: &crate::gasp_overlay::FinalizedGraph,
    ) -> FinalizeShadow {
        let prepared = self
            .shadow_prepare(host.as_ref(), peer, &graph.topic, &graph.beefs)
            .await;
        let root = graph
            .beefs
            .last()
            .map_or_else(String::new, |b| body_txid(b));
        let mut jobs = self.shadow_jobs.borrow_mut();
        jobs.push(ShadowJob::Graph(Box::new(GraphJob {
            peer: peer.into(),
            topic: graph.topic.clone(),
            graph: root.clone(),
            beefs: graph.beefs.clone(),
            prepared,
            first: None,
            served: None,
        })));
        FinalizeShadow {
            host,
            peer: peer.into(),
            topic: graph.topic.clone(),
            root,
            slot: jobs.len() - 1,
            landed: Vec::new(),
            stopped: None,
            manager_errors: Vec::new(),
            last: None,
        }
    }

    /// The graph's job, in its place.
    fn graph_job<R>(
        &self,
        shadow: &FinalizeShadow,
        f: impl FnOnce(&mut GraphJob) -> R,
    ) -> Option<R> {
        match self.shadow_jobs.borrow_mut().get_mut(shadow.slot) {
            Some(ShadowJob::Graph(job)) => Some(f(job)),
            _ => None,
        }
    }

    /// One body's snapshot, taken before its served submit (`body` is that
    /// one BEEF; none at position 0, whose state is the graph's snapshot);
    /// the managers' errors of that submit are captured from here.
    pub(crate) async fn shadow_body_begin(
        &self,
        shadow: &FinalizeShadow,
        position: usize,
        body: &[Vec<u8>],
    ) -> Option<Prepared> {
        let prepared = if position == 0 {
            None
        } else {
            Some(
                self.shadow_prepare(shadow.host.as_ref(), &shadow.peer, &shadow.topic, body)
                    .await,
            )
        };
        self.capture_manager_errors();
        prepared
    }

    /// From now on, keep each topic manager's error (`judge`'s `Err`).
    pub(crate) fn capture_manager_errors(&self) {
        *self.manager_errors.borrow_mut() = Some(Vec::new());
    }

    /// The errors kept since the capture began; the capture goes on.
    pub(crate) fn take_manager_errors(&self) -> Vec<String> {
        self.manager_errors
            .borrow_mut()
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default()
    }

    /// The errors kept since the capture began; the capture ends.
    fn end_manager_errors(&self) -> Vec<String> {
        self.manager_errors.borrow_mut().take().unwrap_or_default()
    }

    /// A topic manager answered `Err` (from `judge`): kept while captured,
    /// at most [`MANAGER_ERRORS_KEPT`] (a capture a dropped finalize left on
    /// is ended by the next body or the drain).
    pub(crate) fn note_manager_error(&self, topic: &str, error: &TopicManagerError) {
        if let Some(errors) = self.manager_errors.borrow_mut().as_mut() {
            if errors.len() < MANAGER_ERRORS_KEPT {
                errors.push(format!("topic_manager.{topic}: {error}"));
            }
        }
    }

    /// Queue one body's comparison: its snapshot, and the served answer.
    pub(crate) fn shadow_body_served(
        &self,
        shadow: &mut FinalizeShadow,
        prepared: Option<Prepared>,
        position: usize,
        beef: &[u8],
        served: &Result<(Steak, MutationReport), EngineError>,
        not_landed: Option<&str>,
    ) {
        let errors = self.end_manager_errors();
        let word = Word::served(served, not_landed, &errors);
        shadow.manager_errors.extend(errors);
        shadow.last = Some(word.clone());
        let body = BodyServed {
            txid: body_txid(beef),
            position: u32::try_from(position).unwrap_or(u32::MAX),
            beef: beef.to_vec(),
            served: word,
            served_judgement: served.as_ref().ok().map(|(steak, _)| judgement_of(steak)),
        };
        match prepared {
            Some(prepared) => {
                self.shadow_jobs
                    .borrow_mut()
                    .push(ShadowJob::Body(Box::new(BodyJob {
                        peer: shadow.peer.clone(),
                        topic: shadow.topic.clone(),
                        graph: shadow.root.clone(),
                        prepared,
                        body,
                    })));
            }
            None => {
                self.graph_job(shadow, |job| job.first = Some(body));
            }
        }
    }

    /// Complete one graph's comparison: the rows the served submits left,
    /// read now (before a later graph moves them).
    pub(crate) async fn shadow_graph_served(&self, shadow: FinalizeShadow, beefs: &[Vec<u8>]) {
        let rows = materialized_rows(self.storage.as_ref(), &shadow.topic, beefs).await;
        let word = materialization_word(
            &shadow.landed,
            shadow.stopped.as_ref(),
            &shadow.manager_errors,
            &rows,
        );
        self.graph_job(&shadow, |job| job.served = Some(word));
    }

    /// Run every queued comparison and hand its row to the host. After the
    /// peer's sync, outside its race. A record that fails is logged; nothing
    /// served depends on it.
    pub(crate) async fn drain_gasp_shadow(&self) {
        let Some(host) = self.gasp_shadow_host() else {
            return;
        };
        self.end_manager_errors();
        let jobs = std::mem::take(&mut *self.shadow_jobs.borrow_mut());
        // A graph's job comes before its later bodies' (it holds the first
        // body's snapshot); its row is recorded after them.
        let mut graph_row = None;
        for job in jobs {
            match job {
                ShadowJob::Body(job) => {
                    let row = self.shadow_body(host.as_ref(), *job).await;
                    Self::record_row(host.as_ref(), &row).await;
                }
                ShadowJob::Graph(job) => {
                    if let Some(row) = graph_row.take() {
                        Self::record_row(host.as_ref(), &row).await;
                    }
                    let (first, graph) = self.shadow_graph(host.as_ref(), *job).await;
                    if let Some(row) = first {
                        Self::record_row(host.as_ref(), &row).await;
                    }
                    graph_row = graph;
                }
            }
        }
        if let Some(row) = graph_row {
            Self::record_row(host.as_ref(), &row).await;
        }
    }

    async fn record_row(host: &dyn GaspShadow, row: &GaspCensusRow) {
        if let Err(e) = host.record(row).await {
            warn!(
                "[GASP SHADOW] census row for {} ({:?}) not recorded: {e}",
                row.txid, row.kind
            );
        }
    }

    /// A's walk of one finalize body on the snapshot: the served mode
    /// (`historical-tx-no-spv`), handed the served evidence; a large BEEF's
    /// original bytes handed to the snapshot before its unit.
    async fn shadow_walk(
        snapshot: &ShadowSnapshot,
        beef: &[u8],
        topic: &str,
        served: &AdmitEvidence,
    ) -> Result<Walked, (Word, AdmitRefusal)> {
        let tagged = TaggedBEEF::new(beef.to_vec(), vec![topic.to_string()]);
        let walked = snapshot
            .engine
            .walk_with_mode(
                &tagged,
                SubmitMode::HistoricalTxNoSpv,
                WalkBreachPolicy::NotNow,
                &ServedEvidence(served.clone()),
            )
            .await
            .map_err(|e| (Word::engine_error(&e), walk_refusal(topic, &e)))?;
        if matches!(walked.beef, BeefCarry::R2 { .. }) {
            (snapshot.resolve)(&walked.beef, beef.to_vec()).map_err(|r| (Word::refusal(&r), r))?;
        }
        Ok(walked)
    }

    fn over_reads(&self, snapshot: &ShadowSnapshot) -> Option<String> {
        self.shadow_budget.over_reads(snapshot.rows, snapshot.bytes)
    }

    async fn shadow_body(&self, host: &dyn GaspShadow, job: BodyJob) -> GaspCensusRow {
        let spent = Spent {
            host,
            before: job.prepared.ms,
            start: host.now_ms(),
            budget: self.shadow_budget,
        };
        let mut row = body_row(job.peer, job.topic, job.graph, job.body.served_parts());
        let beef = job.body.beef;
        row.shadow = 'judged: {
            let snapshot = match job.prepared.snapshot {
                Ok(snapshot) => snapshot,
                Err(e) => break 'judged not_snapshotted(&e),
            };
            row.snapshot_rows = snapshot.rows;
            row.snapshot_bytes = snapshot.bytes;
            if let Some(why) = self.over_reads(&snapshot).or_else(|| spent.over()) {
                break 'judged Word::not_shadowed(why);
            }
            let walked =
                match Self::shadow_walk(&snapshot, &beef, &row.topic, &row.served_evidence).await {
                    Ok(walked) => walked,
                    Err((word, _)) => break 'judged word,
                };
            row.shadow_evidence = walked.evidence.clone();
            let judged = snapshot.engine.judge_dry(&walked).await;
            row.shadow_judgement = judged.ok().map(|admitted| judgement_of(&admitted.steak));
            let mut word = Word::not_shadowed("no unit ran");
            let seqs = &mut row.shadow_seqs;
            let gates = &mut row.gate;
            let engine = &snapshot.engine;
            engine.capture_manager_errors();
            admit_graph_observed(
                engine,
                snapshot.runner.as_ref(),
                std::slice::from_ref(&walked),
                &[],
                &mut |body, answer, gate| {
                    if let Ok(committed) = answer {
                        seqs.push(committed.seq);
                    }
                    gates.extend(gate.map(|reason| GateEntry {
                        txid: body.txid.clone(),
                        reason: reason.clone(),
                    }));
                    word = Word::admission(answer, &engine.take_manager_errors());
                },
            );
            engine.end_manager_errors();
            match spent.over() {
                Some(why) => Word::not_shadowed(why),
                None => word,
            }
        };
        row.shadow_ms = spent.ms();
        row.delta = classify(&row);
        row
    }

    /// A graph's comparison on its snapshot, and its first body's: every body
    /// walked first (the Worker's side; only the first when the graph has no
    /// served word), the first body's `judge_dry` on the untouched snapshot,
    /// then the graph in one hop (each body its own unit under the dependency
    /// gate, D44), the first unit's answer the first body's.
    async fn shadow_graph(
        &self,
        host: &dyn GaspShadow,
        job: GraphJob,
    ) -> (Option<GaspCensusRow>, Option<GaspCensusRow>) {
        if job.first.is_none() && job.served.is_none() {
            return (None, None);
        }
        let spent = Spent {
            host,
            before: job.prepared.ms,
            start: host.now_ms(),
            budget: self.shadow_budget,
        };
        let mut first = job.first.map(|body| {
            body_row(
                job.peer.clone(),
                job.topic.clone(),
                job.graph.clone(),
                body.served_parts(),
            )
        });
        let mut graph = job.served.map(|served| GaspCensusRow {
            kind: ShadowKind::Materialization,
            peer: job.peer.clone(),
            topic: job.topic.clone(),
            txid: job.graph.clone(),
            graph: job.graph.clone(),
            position: u32::try_from(job.beefs.len()).unwrap_or(u32::MAX),
            served,
            shadow: Word::not_shadowed("not judged"),
            delta: DeltaClass::NotShadowed,
            served_judgement: None,
            shadow_judgement: None,
            served_evidence: AdmitEvidence::default(),
            shadow_evidence: AdmitEvidence::default(),
            shadow_seqs: vec![],
            gate: vec![],
            snapshot_rows: 0,
            snapshot_bytes: 0,
            shadow_ms: 0,
        });
        let judged: Result<Word, Word> = 'judged: {
            let snapshot = match job.prepared.snapshot {
                Ok(snapshot) => snapshot,
                Err(e) => break 'judged Err(not_snapshotted(&e)),
            };
            for row in first.iter_mut().chain(graph.iter_mut()) {
                row.snapshot_rows = snapshot.rows;
                row.snapshot_bytes = snapshot.bytes;
            }
            if let Some(why) = self.over_reads(&snapshot).or_else(|| spent.over()) {
                break 'judged Err(Word::not_shadowed(why));
            }
            let bodies = if graph.is_some() {
                &job.beefs[..]
            } else {
                &job.beefs[..job.beefs.len().min(1)]
            };
            // A body whose walk is refused is handed to the gate as a held
            // parent and the walk goes on (the gate lens's LOW 2): its
            // descendants wait on it and its siblings are still judged.
            let mut walked = Vec::new();
            let mut unwalked = Vec::new();
            let mut walk_words = Vec::new();
            let mut position = BTreeMap::new();
            let mut evidence = AdmitEvidence::default();
            for (at, beef) in bodies.iter().enumerate() {
                match Self::shadow_walk(&snapshot, beef, &job.topic, &AdmitEvidence::default())
                    .await
                {
                    Ok(body) => {
                        evidence.tx.extend(body.evidence.tx.clone());
                        position.entry(body.txid.clone()).or_insert(at);
                        walked.push(body);
                    }
                    Err((word, refusal)) => {
                        let txid = body_txid(beef);
                        position.entry(txid.clone()).or_insert(at);
                        walk_words.push((at, txid.clone(), word));
                        unwalked.push(Unwalked { txid, refusal });
                    }
                }
            }
            // The first body's own answer: its walk's refusal, or its unit's.
            let first_txid = walked
                .first()
                .filter(|body| position.get(&body.txid) == Some(&0))
                .map(|body| body.txid.clone());
            if let Some(row) = first.as_mut() {
                match (&first_txid, walk_words.first()) {
                    (Some(_), _) => {
                        let body = &walked[0];
                        row.shadow_evidence = body.evidence.clone();
                        let judged = snapshot.engine.judge_dry(body).await;
                        row.shadow_judgement =
                            judged.ok().map(|admitted| judgement_of(&admitted.steak));
                    }
                    (None, Some((0, _, word))) => row.shadow = word.clone(),
                    (None, _) => {}
                }
            }
            if let Some(row) = graph.as_mut() {
                row.shadow_evidence = evidence;
            }
            let mut landed = Vec::new();
            let mut refused = None;
            let mut errors = Vec::new();
            let mut first_answer = None;
            let mut seqs = Vec::new();
            let mut gates = Vec::new();
            let engine = &snapshot.engine;
            engine.capture_manager_errors();
            admit_graph_observed(
                engine,
                snapshot.runner.as_ref(),
                &walked,
                &unwalked,
                &mut |body, answer, gate| {
                    let met = engine.take_manager_errors();
                    let gate = gate.map(|reason| GateEntry {
                        txid: body.txid.clone(),
                        reason: reason.clone(),
                    });
                    if first_answer.is_none() && first_txid.as_ref() == Some(&body.txid) {
                        first_answer = Some((
                            Word::admission(answer, &met),
                            answer.as_ref().ok().map(|committed| committed.seq),
                            gate.clone(),
                        ));
                    }
                    gates.extend(gate);
                    errors.extend(met);
                    match answer {
                        Ok(committed) => {
                            seqs.push(committed.seq);
                            landed.push(body.txid.clone());
                        }
                        // The first body held (in graph order) is where the
                        // graph stopped landing whole; the gate judged the
                        // rest (D44).
                        Err(refusal) => {
                            let at = position.get(&body.txid).copied().unwrap_or(usize::MAX);
                            if refused.as_ref().is_none_or(|(first, _, _)| at < *first) {
                                refused = Some((at, body.txid.clone(), Word::refusal(refusal)));
                            }
                        }
                    }
                },
            );
            engine.end_manager_errors();
            if let (Some(row), Some((word, seq, gate))) = (first.as_mut(), first_answer) {
                row.shadow = word;
                row.shadow_seqs.extend(seq);
                row.gate.extend(gate);
            }
            let Some(row) = graph.as_mut() else {
                break 'judged Ok(Word::not_shadowed("no graph"));
            };
            row.shadow_seqs = seqs;
            row.gate = gates;
            let stopped = refused
                .into_iter()
                .chain(walk_words.into_iter().take(1))
                .min_by_key(|(at, _, _)| *at)
                .map(|(_, txid, word)| (txid, word));
            let rows = materialized_rows(snapshot.engine.storage(), &job.topic, &job.beefs).await;
            Ok(materialization_word(
                &landed,
                stopped.as_ref(),
                &errors,
                &rows,
            ))
        };
        let over = spent.over();
        if let Some(row) = first.as_mut() {
            match (&judged, &over) {
                (Err(word), _) => row.shadow = word.clone(),
                (_, Some(why)) => row.shadow = Word::not_shadowed(why),
                _ => {}
            }
            row.shadow_ms = spent.ms();
            row.delta = classify(row);
        }
        if let Some(row) = graph.as_mut() {
            row.shadow = match (judged, over) {
                (Err(word), _) => word,
                (_, Some(why)) => Word::not_shadowed(why),
                (Ok(word), None) => word,
            };
            row.shadow_ms = spent.ms();
            row.delta = classify(row);
        }
        (first, graph)
    }
}

/// The served side of a body's row: its parts.
struct ServedParts {
    txid: String,
    position: u32,
    served: Word,
    served_judgement: Option<Judgement>,
}

impl BodyServed {
    fn served_parts(&self) -> ServedParts {
        ServedParts {
            txid: self.txid.clone(),
            position: self.position,
            served: self.served.clone(),
            served_judgement: self.served_judgement.clone(),
        }
    }
}

/// A body's row before the shadow judged it.
fn body_row(peer: String, topic: String, graph: String, body: ServedParts) -> GaspCensusRow {
    GaspCensusRow {
        kind: ShadowKind::Body,
        peer,
        topic,
        graph,
        txid: body.txid,
        position: body.position,
        served: body.served,
        shadow: Word::not_shadowed("not judged"),
        delta: DeltaClass::NotShadowed,
        served_judgement: body.served_judgement,
        shadow_judgement: None,
        served_evidence: AdmitEvidence::default(),
        shadow_evidence: AdmitEvidence::default(),
        shadow_seqs: vec![],
        gate: vec![],
        snapshot_rows: 0,
        snapshot_bytes: 0,
        shadow_ms: 0,
    }
}
