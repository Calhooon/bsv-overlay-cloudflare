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
    #[error("lookup preflight refused: {refused:?}")]
    Lookup {
        refused: Vec<AdmissionRefusal>,
        notes: RefusalNotes,
    },
    #[error("{site}: {reason}")]
    Deterministic {
        topic: String,
        site: String,
        reason: String,
        notes: RefusalNotes,
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
    StoppedAt {
        txid: String,
        refusal: AdmitRefusal,
        landed: Vec<u64>,
    },
}

/// GASP finalize: each body lands independently, in graph order.
pub fn admit_graph(
    engine: &crate::engine::Engine,
    runner: &dyn UnitRunner,
    graph: &[Walked],
) -> GraphOutcome {
    let mut seqs = Vec::new();
    for walked in graph {
        match runner.run_unit(
            UnitKind::Admit,
            &walked.evidence,
            Box::pin(async { engine.admit(walked).await.map(UnitOutput::Admitted) }),
        ) {
            Ok(committed) => seqs.push(committed.seq),
            Err(refusal) => {
                return GraphOutcome::StoppedAt {
                    txid: walked.txid.clone(),
                    refusal,
                    landed: seqs,
                }
            }
        }
    }
    GraphOutcome::AllLanded { seqs }
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
