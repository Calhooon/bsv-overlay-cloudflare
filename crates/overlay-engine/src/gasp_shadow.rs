//! GASP's shadow under [`crate::admission::admit_graph`] (bsv-low #657, the
//! design's R7.7 item 5 and 3.3a "The GASP gap").
//!
//! The engine's own GASP finalize submits (`submit_finalized_graphs`, every
//! body of a peer's graph under `historical-tx-no-spv`) have no Worker
//! observer, so the Worker's shadow (Zanaadu's `shadow_admission`) leaves them
//! "not shadowed". This module is their shadow, the engine's: with
//! [`AdmissionMode::Shadow`] and a [`GaspShadow`] host set
//! ([`crate::engine::Engine::set_gasp_shadow`]), today's finalize serves as it
//! always has, and A runs beside it over a fresh isolated snapshot of the
//! store and applies nothing served:
//!
//! - **per body**: a snapshot taken just before the served submit; A's walk
//!   of the body handed the evidence the served path gathered (none beyond the
//!   walk: the finalize does not walk and asks no network; the anchor check
//!   already ran), `judge_dry` beside the served judgement, and the body's own
//!   unit through `admit_graph`; one census row ([`ShadowKind::Body`]).
//! - **per graph**: a snapshot taken before the graph's first served submit;
//!   every body walked, then `admit_graph` over the whole graph (each body its
//!   own unit, stopping at the first that does not land), and the rows the
//!   graph touched (its "one materialization") compared with the rows the
//!   served submits left; one census row ([`ShadowKind::Materialization`]).
//!
//! Only the snapshots and the served rows are read inside the served path
//! (before and after each served write); the walks, the judgements, the units
//! and the census writes run after the peer's sync, outside its per-peer race,
//! so the shadow's own time never moves a deadline. `Off` (the default) runs
//! none of it: no snapshot, no read, no extra await.
use std::collections::BTreeMap;
use std::rc::Rc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::admission::{AdmitEvidence, AdmitRefusal, BeefCarry, Committed, UnitOutput, UnitRunner};
use crate::engine::{Engine, EngineError, MutationReport};
use crate::types::Steak;

/// The engine's word for `ADMISSION_MODE`, as the Worker's (`off` default,
/// `shadow` accepted, `on` refused until the log runner exists).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionMode {
    #[default]
    Off,
    Shadow,
}

impl AdmissionMode {
    pub fn parse(value: Option<&str>) -> Result<Self, String> {
        match value {
            None | Some("off") => Ok(Self::Off),
            Some("shadow") => Ok(Self::Shadow),
            Some("on") => Err(
                "ADMISSION_MODE=on is not enabled: the admission log runner is a later slice"
                    .into(),
            ),
            Some(other) => Err(format!("unsupported ADMISSION_MODE: {other}")),
        }
    }
}

/// Rows one shadow snapshot may read (the slice's `SHADOW_READ_ALLOWANCE`).
pub const SHADOW_READ_ALLOWANCE: usize = 4096;
/// Bytes one shadow snapshot may read (the slice's `SHADOW_READ_BYTES`).
pub const SHADOW_READ_BYTES: usize = 16 << 20;
/// Milliseconds one shadow comparison may take, its snapshot and its own work
/// together, never the served submit's (the slice's `SHADOW_TIME_BUDGET_MS`).
pub const SHADOW_TIME_BUDGET_MS: u64 = 2_000;

/// One comparison's allowance. Past any limb the row is `not shadowed: budget`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShadowBudget {
    pub rows: usize,
    pub bytes: usize,
    pub ms: u64,
}

impl Default for ShadowBudget {
    fn default() -> Self {
        Self {
            rows: SHADOW_READ_ALLOWANCE,
            bytes: SHADOW_READ_BYTES,
            ms: SHADOW_TIME_BUDGET_MS,
        }
    }
}

/// What a snapshot is for: a host that scopes its read (the Worker's scoped
/// SQLite) reads the rows these bodies reach under this topic.
pub struct ShadowScope<'a> {
    pub peer: &'a str,
    pub topic: &'a str,
    /// One body for a body's comparison; the whole graph for its materialization.
    pub beefs: &'a [Vec<u8>],
}

/// A disposable engine over an isolated copy of the served store, as it stood
/// when the snapshot was taken. Its storage is the runner's (each unit runs
/// inside it); it holds no tracker, broadcaster, fetcher or peer factory, so
/// nothing it does reaches the network.
pub struct ShadowSnapshot {
    pub engine: Engine,
    pub runner: Rc<dyn UnitRunner>,
    /// Hands a resolved R2 BEEF to the snapshot's store before its unit.
    pub resolve: Box<dyn Fn(&BeefCarry, Vec<u8>) -> Result<(), AdmitRefusal>>,
    pub rows: usize,
    pub bytes: usize,
}

/// The host of the engine's GASP shadow: the snapshot, the clock and the
/// census. A failed snapshot is a `not shadowed` row; a failed record is
/// logged; neither changes anything served.
#[async_trait(?Send)]
pub trait GaspShadow {
    async fn snapshot(
        &self,
        scope: &ShadowScope<'_>,
        budget: &ShadowBudget,
    ) -> Result<ShadowSnapshot, String>;
    fn now_ms(&self) -> u64;
    async fn record(&self, row: &GaspCensusRow) -> Result<(), String>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShadowKind {
    Body,
    Materialization,
}

/// The slice's delta classes, plus the materialization's own. `UnitRefusal`
/// is the owner's `unit-wide` first delta (D34): A refused the unit where the
/// served path landed a topic, the intended change, counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeltaClass {
    Same,
    NotShadowed,
    UnitRefusal,
    Evidence,
    Words,
    Materialization,
}

/// An answer as a status and its bytes (the slice's `Word`). Status 0 is no
/// answer: the shadow did not judge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Word {
    pub status: u16,
    pub bytes: Vec<u8>,
    pub landed_topics: Vec<String>,
}

/// One topic's judgement: what the manager admitted and retained, never what
/// an apply did with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Judged {
    pub outputs_to_admit: Vec<u32>,
    pub coins_to_retain: Vec<u32>,
}
pub type Judgement = BTreeMap<String, Judged>;

/// One comparison row of the census.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GaspCensusRow {
    pub kind: ShadowKind,
    pub peer: String,
    pub topic: String,
    /// The graph's root: its last body's txid.
    pub graph: String,
    /// The body's txid; a materialization row names the graph's root.
    pub txid: String,
    /// The body's place in its graph; a materialization row, the body count.
    pub position: u32,
    pub served: Word,
    pub shadow: Word,
    pub delta: DeltaClass,
    /// The served manager's judgement, and `judge_dry`'s beside it (`None`:
    /// that side answered before any judgement).
    pub served_judgement: Option<Judgement>,
    pub shadow_judgement: Option<Judgement>,
    /// What the served path observed (the finalize gathers none) and what the
    /// shadow's walk carried.
    pub served_evidence: AdmitEvidence,
    pub shadow_evidence: AdmitEvidence,
    /// The seq of each unit the shadow committed on its snapshot.
    pub shadow_seqs: Vec<u64>,
    pub snapshot_rows: usize,
    pub snapshot_bytes: usize,
    pub shadow_ms: u64,
}

impl Word {
    /// Today's finalize answer (the slice's `Word::legacy`), its STEAK in
    /// topic order.
    pub fn served(result: &Result<(Steak, MutationReport), EngineError>) -> Self {
        match result {
            Ok((steak, report)) => {
                let mut landed_topics: Vec<_> = steak
                    .iter()
                    .filter(|(topic, word)| {
                        !word.outputs_to_admit.is_empty()
                            && !report.refused.iter().any(|r| &r.topic == *topic)
                            && !report.faults.iter().any(|f| &f.topic == *topic)
                    })
                    .map(|(topic, _)| topic.clone())
                    .collect();
                landed_topics.sort();
                let status = if report.is_durable() {
                    200
                } else if report.faults.is_empty() {
                    422
                } else if report.refused.is_empty()
                    && report
                        .faults
                        .iter()
                        .all(|f| f.site == "predecessor_not_landed")
                {
                    503
                } else {
                    500
                };
                let steak: BTreeMap<_, _> = steak.iter().collect();
                Self {
                    status,
                    bytes: serde_json::json!({
                        "steak": steak,
                        "refused": report.refused.iter().map(|r| format!("{}/{}/{}: {}", r.topic, r.service, r.site, r.error)).collect::<Vec<_>>(),
                        "faults": report.faults.iter().map(|f| format!("{}/{}: {}", f.topic, f.site, f.error)).collect::<Vec<_>>(),
                    })
                    .to_string()
                    .into_bytes(),
                    landed_topics,
                }
            }
            Err(error) => Self::engine_error(error),
        }
    }

    pub fn engine_error(error: &EngineError) -> Self {
        Self {
            status: engine_error_status(error),
            bytes: error.to_string().into_bytes(),
            landed_topics: vec![],
        }
    }

    /// A's unit, as the `on` runner's door will word it.
    pub fn admission(result: &Result<Committed, AdmitRefusal>) -> Self {
        match result {
            Ok(Committed {
                output: UnitOutput::Admitted(admitted),
                ..
            }) => Self::served(&Ok((admitted.steak.clone(), admitted.report.clone()))),
            Ok(Committed {
                output: UnitOutput::Done,
                ..
            }) => Self {
                status: 200,
                bytes: b"done".to_vec(),
                landed_topics: vec![],
            },
            Err(refusal) => Self::refusal(refusal),
        }
    }

    pub fn refusal(refusal: &AdmitRefusal) -> Self {
        Self {
            status: match refusal {
                AdmitRefusal::Lookup { .. } | AdmitRefusal::Deterministic { .. } => 422,
                AdmitRefusal::NotNow { .. } => 503,
                AdmitRefusal::Transient { .. } => 500,
            },
            bytes: refusal.to_string().into_bytes(),
            landed_topics: vec![],
        }
    }

    /// The shadow did not judge this one (status 0).
    pub fn not_shadowed(reason: impl std::fmt::Display) -> Self {
        let reason = reason.to_string();
        Self {
            status: 0,
            bytes: if reason.starts_with("not shadowed") {
                reason
            } else {
                format!("not shadowed: {reason}")
            }
            .into_bytes(),
            landed_topics: vec![],
        }
    }
}

/// The Worker door's mapping (`routes::engine_error_status`), so a census row
/// reads as the door would answer.
pub fn engine_error_status(error: &EngineError) -> u16 {
    match error {
        EngineError::LookupFailed(_) | EngineError::StorageError(_) | EngineError::Other(_) => 500,
        EngineError::BroadcastError(_) => 502,
        EngineError::WalkCouldNotRun(_) => 503,
        _ => 400,
    }
}

/// The judgement fields of a STEAK.
pub fn judgement_of(steak: &Steak) -> Judgement {
    steak
        .iter()
        .map(|(topic, word)| {
            (
                topic.clone(),
                Judged {
                    outputs_to_admit: word.outputs_to_admit.clone(),
                    coins_to_retain: word.coins_to_retain.clone(),
                },
            )
        })
        .collect()
}

/// The reference host over [`crate::storage::memory::MemoryStorage`]: each
/// snapshot a whole copy of the served store (so its rows are the store's),
/// each shadow engine built fresh by `build` over that copy, the census kept
/// in memory.
#[cfg(feature = "memory-storage")]
pub struct MemoryGaspShadow {
    served: Rc<crate::storage::memory::MemoryStorage>,
    #[allow(clippy::type_complexity)]
    build: Box<dyn Fn(Rc<crate::storage::memory::MemoryStorage>) -> Engine>,
    clock: Rc<dyn Fn() -> u64>,
    census: std::cell::RefCell<Vec<GaspCensusRow>>,
    snapshots: std::cell::Cell<usize>,
}

#[cfg(feature = "memory-storage")]
impl MemoryGaspShadow {
    pub fn new(
        served: Rc<crate::storage::memory::MemoryStorage>,
        build: impl Fn(Rc<crate::storage::memory::MemoryStorage>) -> Engine + 'static,
    ) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        let clock: Rc<dyn Fn() -> u64> = {
            let start = std::time::Instant::now();
            Rc::new(move || u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX))
        };
        #[cfg(target_arch = "wasm32")]
        let clock: Rc<dyn Fn() -> u64> = Rc::new(|| 0);
        Self {
            served,
            build: Box::new(build),
            clock,
            census: std::cell::RefCell::default(),
            snapshots: std::cell::Cell::default(),
        }
    }

    /// Replace the clock (unix-like milliseconds), for a budget's pin.
    #[must_use]
    pub fn with_clock(mut self, clock: impl Fn() -> u64 + 'static) -> Self {
        self.clock = Rc::new(clock);
        self
    }

    pub fn census(&self) -> Vec<GaspCensusRow> {
        self.census.borrow().clone()
    }

    /// How many snapshots were taken.
    pub fn snapshots(&self) -> usize {
        self.snapshots.get()
    }
}

#[cfg(feature = "memory-storage")]
#[async_trait(?Send)]
impl GaspShadow for MemoryGaspShadow {
    async fn snapshot(
        &self,
        _scope: &ShadowScope<'_>,
        _budget: &ShadowBudget,
    ) -> Result<ShadowSnapshot, String> {
        self.snapshots.set(self.snapshots.get() + 1);
        let store = Rc::new(self.served.shadow_copy());
        let (rows, bytes) = store.shadow_size();
        let engine = (self.build)(store.clone());
        let resolver = store.clone();
        Ok(ShadowSnapshot {
            engine,
            runner: store,
            resolve: Box::new(move |carry, bytes| resolver.cache_admission_beef(carry, bytes)),
            rows,
            bytes,
        })
    }
    fn now_ms(&self) -> u64 {
        (self.clock)()
    }
    async fn record(&self, row: &GaspCensusRow) -> Result<(), String> {
        self.census.borrow_mut().push(row.clone());
        Ok(())
    }
}
