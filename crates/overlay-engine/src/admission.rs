//! The walk/log boundary. Network evidence belongs to a submit, never an isolate.
use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use bsv_rs::transaction::Transaction;
use serde::{Deserialize, Serialize};

use crate::engine::{EngineError, MutationReport, WalkBreachPolicy, WalkCouldNotRun};
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

/// An independently walked predecessor. Its bytes remain in the one BEEF.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Carried {
    pub txid: String,
    pub topics: Vec<String>,
    pub walk: WalkOutcome,
}

/// Local staged payload. Hosts may transport the bytes inline or by a verified
/// content-addressed pointer; resolving that pointer must precede the unit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Walked {
    pub txid: String,
    pub tagged_beef: TaggedBEEF,
    pub mode: SubmitMode,
    pub policy: WalkBreachPolicy,
    pub walk: WalkOutcome,
    pub carried: Vec<Carried>,
    pub evidence: AdmitEvidence,
    pub charge: u64,
    pub could_not_run: Option<WalkCouldNotRun>,
}

#[derive(Debug, thiserror::Error)]
pub enum WalkAnswer {
    #[error("{0}")]
    WalkCouldNotRun(#[from] EngineError),
    #[error("{site}: {reason}")]
    EvidenceUnobtainable { site: String, reason: String },
}

#[async_trait(?Send)]
pub trait EvidenceSource {
    async fn gather(
        &self,
        subject: &Transaction,
        bodies: &[&Transaction],
        topics: &[String],
    ) -> Result<AdmitEvidence, WalkAnswer>;
}

/// For managers needing no host network evidence. The engine supplies each
/// body's walk verdict; a host with acceptance gates supplies the other fields.
pub struct NoEvidence;
#[async_trait(?Send)]
impl EvidenceSource for NoEvidence {
    async fn gather(
        &self,
        _: &Transaction,
        _: &[&Transaction],
        _: &[String],
    ) -> Result<AdmitEvidence, WalkAnswer> {
        Ok(AdmitEvidence::default())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdmitRefusal {
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
            Self::Deterministic { notes, .. }
            | Self::Transient { notes, .. }
            | Self::NotNow { notes, .. } => *notes = value,
        }
        self
    }
    pub fn notes(&self) -> &RefusalNotes {
        match self {
            Self::Deterministic { notes, .. }
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
    fn seq(&self) -> u64;
    fn now_ms(&self) -> i64;
    fn evidence(&self) -> Arc<AdmitEvidence>;
    fn note(&self, topic: &str, note: RefusalNote);
    fn notes(&self) -> RefusalNotes;
    fn emit(&self, event: OutboxEvent) -> Result<(), StorageError>;
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
