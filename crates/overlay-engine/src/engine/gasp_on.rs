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
///
/// The graph's bytes live in the sink alone (`graph.beefs`; the seam lens's
/// M1): the gate is handed the walked bodies BARE (an inline body's BEEF and
/// an R2 body's raw transactions dropped, [`bare`]), and the one body the gate
/// is on is made whole again from the sink ([`whole`]) for the host's calls
/// and released after its answer, so the engine holds the sink and one body.
struct OnGraph<'a> {
    engine: &'a Engine,
    host: &'a dyn GaspFinalizeHost,
    graph: &'a FinalizeGraph<'a>,
    /// The bodies the gate is handed (bare), and each one's place in the
    /// sink: per OCCURRENCE (the seam lens's L4: a graph carries a txid once
    /// per node, so a transaction two of whose outputs it spends is two
    /// bodies), never by txid.
    walked: &'a [Walked],
    positions: Vec<usize>,
    /// The body the gate is on, made whole, by its place in the sink.
    current: std::cell::RefCell<Option<(usize, Walked)>>,
}

impl OnGraph<'_> {
    /// `walked`'s place in the sink: the gate hands each body by reference
    /// into the list it was given, so the body is found by identity.
    fn position(&self, walked: &Walked) -> usize {
        let at = self
            .walked
            .iter()
            .position(|w| std::ptr::eq(w, walked))
            .expect("the gate hands a body of the list it was given");
        self.positions[at]
    }

    fn body<'b>(&'b self, position: usize, walked: &'b Walked) -> FinalizeBody<'b> {
        FinalizeBody {
            graph: self.graph,
            position,
            walked,
        }
    }

    /// `bare`'s body made whole from the sink: the one held since the gate's
    /// last call for it, else made now.
    fn take(&self, bare: &Walked) -> (usize, Walked) {
        let position = self.position(bare);
        match self.current.borrow_mut().take() {
            Some((at, held)) if at == position => (position, held),
            _ => (position, whole(bare, &self.graph.beefs[position])),
        }
    }

    /// Keep the body the gate is on for its next call.
    fn keep(&self, position: usize, walked: Walked) {
        *self.current.borrow_mut() = Some((position, walked));
    }
}

/// `walked` without its bytes: an inline body's BEEF and an R2 body's raw
/// transactions dropped (the sink holds them), every other field kept.
fn bare(mut walked: Walked) -> Walked {
    walked.beef = match walked.beef {
        BeefCarry::Inline(_) => BeefCarry::Inline(Vec::new()),
        BeefCarry::R2 {
            key, sha256, len, ..
        } => BeefCarry::R2 {
            key,
            sha256,
            len,
            raw_txs: Vec::new(),
        },
    };
    walked
}

/// A [`bare`] body made whole again from its BEEF in the sink: the carry the
/// walk made of these bytes (`BeefCarry::from_beef`, the walk's own rule).
fn whole(bare: &Walked, beef: &[u8]) -> Walked {
    let mut walked = bare.clone();
    walked.beef = match &bare.beef {
        BeefCarry::Inline(_) => BeefCarry::Inline(beef.to_vec()),
        BeefCarry::R2 { .. } => {
            match beef_limits::parse_beef(beef, &beef_limits::ENGINE_BEEF_LIMITS) {
                Ok(parsed) => BeefCarry::from_beef(beef, &parsed, &bare.txid),
                // The walk parsed these very bytes: unreachable, and the host's
                // verification of an R2 carry refuses an empty one.
                Err(_) => bare.beef.clone(),
            }
        }
    };
    walked
}

#[async_trait::async_trait(?Send)]
impl GraphHost for OnGraph<'_> {
    async fn resolve(&self, body: &Walked) -> Result<Vec<u8>, AdmitRefusal> {
        let (position, walked) = self.take(body);
        let resolved = self.host.resolve(&self.body(position, &walked)).await;
        self.keep(position, walked);
        resolved
    }

    async fn run(
        &self,
        body: &Walked,
        resolved: Option<Vec<u8>>,
    ) -> Result<Committed, AdmitRefusal> {
        let (position, walked) = self.take(body);
        // The served finalize's rule: a due deadline stops the graph between
        // two bodies, never inside one's unit (one write section each).
        self.engine.finalize_gate.stop_if_due().await;
        let _writing = self.engine.finalize_gate.write_section();
        let answer = self
            .host
            .admit(&self.body(position, &walked), resolved)
            .await;
        self.keep(position, walked);
        answer
    }

    fn answered(
        &self,
        body: &Walked,
        answer: &Result<Committed, AdmitRefusal>,
        gate: Option<&GateReason>,
    ) {
        // The body's last call: released when it returns.
        let (position, walked) = self.take(body);
        self.host
            .answered(&self.body(position, &walked), answer, gate);
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
            let mut positions = Vec::new();
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
                        positions.push(at);
                        walked.push(bare(body));
                    }
                    Err(e) => {
                        let txid = body_txid(beef);
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
                walked: &walked,
                positions,
                current: std::cell::RefCell::default(),
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
