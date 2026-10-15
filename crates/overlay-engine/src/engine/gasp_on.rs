//! The engine's side of the GASP finalize under `on` (bsv-low #657, the a17
//! seam; [`crate::gasp_shadow::GaspFinalizeHost`]): each finalized graph's
//! bodies walked here (`historical-tx-no-spv`, no network: the anchor check
//! already ran), then the dependency gate over them
//! ([`crate::admission::admit_graph_hosted`]) handing each body it does not
//! hold to the host, which runs it as one unit of its own log. The engine
//! writes nothing of a graph; the cursor follows the host's word.
use super::gasp_shadow::{body_txid, walk_refusal};
use super::*;
use crate::admission::*;
use crate::gasp_shadow::{AdmissionMode, FinalizeBody, FinalizeGraph, GaspFinalizeHost};
use std::rc::Rc;

/// One graph's [`GraphHost`]: the gate's calls, each with the body's place
/// in its graph, handed to the engine's finalize host.
struct OnGraph<'a> {
    engine: &'a Engine,
    host: &'a dyn GaspFinalizeHost,
    graph: &'a FinalizeGraph<'a>,
    positions: HashMap<String, usize>,
}

impl OnGraph<'_> {
    fn body<'b>(&'b self, walked: &'b Walked) -> FinalizeBody<'b> {
        FinalizeBody {
            graph: self.graph,
            position: self.positions.get(&walked.txid).copied().unwrap_or(0),
            walked,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl GraphHost for OnGraph<'_> {
    async fn resolve(&self, body: &Walked) -> Result<Vec<u8>, AdmitRefusal> {
        self.host.resolve(&self.body(body)).await
    }

    async fn run(
        &self,
        body: &Walked,
        resolved: Option<Vec<u8>>,
    ) -> Result<Committed, AdmitRefusal> {
        // The served finalize's rule: a due deadline stops the graph between
        // two bodies, never inside one's unit (one write section each).
        self.engine.finalize_gate.stop_if_due().await;
        let _writing = self.engine.finalize_gate.write_section();
        self.host.admit(&self.body(body), resolved).await
    }

    fn answered(
        &self,
        body: &Walked,
        answer: &Result<Committed, AdmitRefusal>,
        gate: Option<&GateReason>,
    ) {
        self.host.answered(&self.body(body), answer, gate);
    }
}

impl Engine {
    /// The finalize host, when the GASP finalize is the host's (`On`).
    pub(crate) fn gasp_finalize_host(&self) -> Option<Rc<dyn GaspFinalizeHost>> {
        if self.admission_mode == AdmissionMode::On {
            self.gasp_finalize.clone()
        } else {
            None
        }
    }

    /// [`Engine::submit_finalized_graphs`] under `On`: drain `sink`, and for
    /// each graph walk its bodies, run the dependency gate over them through
    /// `host` and ask the host whether the cursor may pass it. Returns
    /// whether it may pass every graph (`false` keeps the cursor below, as a
    /// served graph that did not land does).
    pub(super) async fn finalize_on(
        &self,
        host: Rc<dyn GaspFinalizeHost>,
        sink: &crate::gasp_overlay::FinalizedGraphSink,
        peer_url: &str,
        topic: &str,
        submitted: &std::cell::Cell<u64>,
    ) -> bool {
        let finalized: Vec<_> = sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain(..)
            .collect();
        let mut all_landed = true;
        for graph in &finalized {
            self.finalize_gate.stop_if_due().await;
            submitted.set(submitted.get() + 1);
            let root = graph
                .beefs
                .last()
                .map_or_else(String::new, |b| body_txid(b));
            // A body whose walk is refused is handed to the gate as a held
            // parent (the gate lens's LOW 2): its descendants wait on it and
            // its siblings are still judged.
            let mut walked = Vec::new();
            let mut unwalked = Vec::new();
            let mut positions = HashMap::new();
            for (at, beef) in graph.beefs.iter().enumerate() {
                let tagged = TaggedBEEF::new(beef.clone(), vec![graph.topic.clone()]);
                match self
                    .walk_with_mode(
                        &tagged,
                        SubmitMode::HistoricalTxNoSpv,
                        WalkBreachPolicy::NotNow,
                        &NoEvidence,
                    )
                    .await
                {
                    Ok(body) => {
                        positions.entry(body.txid.clone()).or_insert(at);
                        walked.push(body);
                    }
                    Err(e) => {
                        let txid = body_txid(beef);
                        positions.entry(txid.clone()).or_insert(at);
                        unwalked.push(Unwalked {
                            txid,
                            refusal: walk_refusal(&graph.topic, &e),
                        });
                    }
                }
            }
            let done = FinalizeGraph {
                peer: peer_url,
                topic: &graph.topic,
                root: &root,
                beefs: &graph.beefs,
            };
            let on = OnGraph {
                engine: self,
                host: host.as_ref(),
                graph: &done,
                positions,
            };
            let outcome = admit_graph_hosted(&on, &walked, &unwalked).await;
            if !host.graph_done(&done, &outcome).await {
                warn!(
                    "[GASP SYNC] a finalized graph for topic {} (root {root}) did not land whole in the host's log; the cursor stays below it",
                    graph.topic
                );
                all_landed = false;
            }
        }
        if !finalized.is_empty() {
            info!(
                "[GASP SYNC] Handed {} finalized graph(s) for {topic} from {peer_url} to the host's log",
                finalized.len()
            );
        }
        all_landed
    }
}
