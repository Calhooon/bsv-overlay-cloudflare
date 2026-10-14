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

pub(crate) struct BodyJob {
    peer: String,
    topic: String,
    graph: String,
    txid: String,
    position: u32,
    beef: Vec<u8>,
    prepared: Prepared,
    served: Word,
    served_judgement: Option<Judgement>,
}

pub(crate) struct GraphJob {
    peer: String,
    topic: String,
    graph: String,
    beefs: Vec<Vec<u8>>,
    prepared: Prepared,
    served: Word,
}

/// One graph's shadow while the served finalize submits it: its snapshot,
/// and what the served path landed and where it stopped.
pub(crate) struct FinalizeShadow {
    host: Rc<dyn GaspShadow>,
    peer: String,
    topic: String,
    root: String,
    prepared: Prepared,
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
    /// snapshot.
    pub(crate) async fn shadow_graph_begin(
        &self,
        host: Rc<dyn GaspShadow>,
        peer: &str,
        graph: &crate::gasp_overlay::FinalizedGraph,
    ) -> FinalizeShadow {
        let prepared = self
            .shadow_prepare(host.as_ref(), peer, &graph.topic, &graph.beefs)
            .await;
        FinalizeShadow {
            root: graph
                .beefs
                .last()
                .map_or_else(String::new, |b| body_txid(b)),
            host,
            peer: peer.into(),
            topic: graph.topic.clone(),
            prepared,
            landed: Vec::new(),
            stopped: None,
            manager_errors: Vec::new(),
            last: None,
        }
    }

    /// One body's snapshot, taken before its served submit (`body` is that
    /// one BEEF); the managers' errors of that submit are captured from here.
    pub(crate) async fn shadow_body_begin(
        &self,
        shadow: &FinalizeShadow,
        body: &[Vec<u8>],
    ) -> Prepared {
        let prepared = self
            .shadow_prepare(shadow.host.as_ref(), &shadow.peer, &shadow.topic, body)
            .await;
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
        let Some(prepared) = prepared else {
            return;
        };
        self.shadow_jobs
            .borrow_mut()
            .push(ShadowJob::Body(Box::new(BodyJob {
                peer: shadow.peer.clone(),
                topic: shadow.topic.clone(),
                graph: shadow.root.clone(),
                txid: body_txid(beef),
                position: u32::try_from(position).unwrap_or(u32::MAX),
                beef: beef.to_vec(),
                prepared,
                served: word,
                served_judgement: served.as_ref().ok().map(|(steak, _)| judgement_of(steak)),
            })));
    }

    /// Queue one graph's comparison: its snapshot, and the rows the served
    /// submits left, read now (before a later graph moves them).
    pub(crate) async fn shadow_graph_served(&self, shadow: FinalizeShadow, beefs: &[Vec<u8>]) {
        let rows = materialized_rows(self.storage.as_ref(), &shadow.topic, beefs).await;
        self.shadow_jobs
            .borrow_mut()
            .push(ShadowJob::Graph(Box::new(GraphJob {
                served: materialization_word(
                    &shadow.landed,
                    shadow.stopped.as_ref(),
                    &shadow.manager_errors,
                    &rows,
                ),
                peer: shadow.peer,
                topic: shadow.topic,
                graph: shadow.root,
                beefs: beefs.to_vec(),
                prepared: shadow.prepared,
            })));
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
        for job in jobs {
            let row = match job {
                ShadowJob::Body(job) => self.shadow_body(host.as_ref(), *job).await,
                ShadowJob::Graph(job) => self.shadow_graph(host.as_ref(), *job).await,
            };
            if let Err(e) = host.record(&row).await {
                warn!(
                    "[GASP SHADOW] census row for {} ({:?}) not recorded: {e}",
                    row.txid, row.kind
                );
            }
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
    ) -> Result<Walked, Word> {
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
            .map_err(|e| Word::engine_error(&e))?;
        if matches!(walked.beef, BeefCarry::R2 { .. }) {
            (snapshot.resolve)(&walked.beef, beef.to_vec()).map_err(|r| Word::refusal(&r))?;
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
        let mut row = GaspCensusRow {
            kind: ShadowKind::Body,
            peer: job.peer,
            topic: job.topic,
            graph: job.graph,
            txid: job.txid,
            position: job.position,
            served: job.served,
            shadow: Word::not_shadowed("not judged"),
            delta: DeltaClass::NotShadowed,
            served_judgement: job.served_judgement,
            shadow_judgement: None,
            served_evidence: AdmitEvidence::default(),
            shadow_evidence: AdmitEvidence::default(),
            shadow_seqs: vec![],
            snapshot_rows: 0,
            snapshot_bytes: 0,
            shadow_ms: 0,
        };
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
                match Self::shadow_walk(&snapshot, &job.beef, &row.topic, &row.served_evidence)
                    .await
                {
                    Ok(walked) => walked,
                    Err(word) => break 'judged word,
                };
            row.shadow_evidence = walked.evidence.clone();
            let judged = snapshot.engine.judge_dry(&walked).await;
            row.shadow_judgement = judged.ok().map(|admitted| judgement_of(&admitted.steak));
            let mut word = Word::not_shadowed("no unit ran");
            let seqs = &mut row.shadow_seqs;
            let engine = &snapshot.engine;
            engine.capture_manager_errors();
            admit_graph_observed(
                engine,
                snapshot.runner.as_ref(),
                std::slice::from_ref(&walked),
                &mut |_, answer| {
                    if let Ok(committed) = answer {
                        seqs.push(committed.seq);
                    }
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

    async fn shadow_graph(&self, host: &dyn GaspShadow, job: GraphJob) -> GaspCensusRow {
        let spent = Spent {
            host,
            before: job.prepared.ms,
            start: host.now_ms(),
            budget: self.shadow_budget,
        };
        let mut row = GaspCensusRow {
            kind: ShadowKind::Materialization,
            peer: job.peer,
            topic: job.topic,
            txid: job.graph.clone(),
            graph: job.graph,
            position: u32::try_from(job.beefs.len()).unwrap_or(u32::MAX),
            served: job.served,
            shadow: Word::not_shadowed("not judged"),
            delta: DeltaClass::NotShadowed,
            served_judgement: None,
            shadow_judgement: None,
            served_evidence: AdmitEvidence::default(),
            shadow_evidence: AdmitEvidence::default(),
            shadow_seqs: vec![],
            snapshot_rows: 0,
            snapshot_bytes: 0,
            shadow_ms: 0,
        };
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
            // Every body walked first (the Worker's side), then the graph in
            // one hop: each body its own unit, stopping at the first that
            // does not land.
            let mut walked = Vec::new();
            let mut stopped = None;
            for beef in &job.beefs {
                match Self::shadow_walk(&snapshot, beef, &row.topic, &row.served_evidence).await {
                    Ok(body) => {
                        row.shadow_evidence.tx.extend(body.evidence.tx.clone());
                        walked.push(body);
                    }
                    Err(word) => {
                        stopped = Some((body_txid(beef), word));
                        break;
                    }
                }
            }
            let mut landed = Vec::new();
            let mut refused = None;
            let seqs = &mut row.shadow_seqs;
            let engine = &snapshot.engine;
            engine.capture_manager_errors();
            admit_graph_observed(
                engine,
                snapshot.runner.as_ref(),
                &walked,
                &mut |body, answer| match answer {
                    Ok(committed) => {
                        seqs.push(committed.seq);
                        landed.push(body.txid.clone());
                    }
                    Err(refusal) => refused = Some((body.txid.clone(), Word::refusal(refusal))),
                },
            );
            let errors = engine.end_manager_errors();
            let stopped = refused.or(stopped);
            let rows = materialized_rows(snapshot.engine.storage(), &row.topic, &job.beefs).await;
            match spent.over() {
                Some(why) => Word::not_shadowed(why),
                None => materialization_word(&landed, stopped.as_ref(), &errors, &rows),
            }
        };
        row.shadow_ms = spent.ms();
        row.delta = classify(&row);
        row
    }
}
