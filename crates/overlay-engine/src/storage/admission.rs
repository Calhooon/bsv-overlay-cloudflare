//! Admission contract. All names in this module are proposals for the keeper.
use super::{Storage, StorageError};
use crate::engine::{CallBound, MutationReport};
use crate::types::{AppliedTransaction, Outpoint, Output};
use async_trait::async_trait;
use std::any::{Any, TypeId};
use std::collections::HashMap;

/// Proposed identifier, local to one admission, in staging order.
pub type GuardId = usize;
/// Proposed memory-only, typed record address.
#[derive(Hash, PartialEq, Eq)]
pub struct RecordKey {
    pub type_id: TypeId,
    pub key: String,
}
/// Proposed backend write with the bytes held by its binds, before copying to JS.
pub struct StagedWrite {
    pub write: Box<dyn Any>,
    pub bytes: u64,
}
impl StagedWrite {
    pub fn new<T: 'static>(write: T, bytes: u64) -> Self {
        Self {
            write: Box::new(write),
            bytes,
        }
    }
}
/// Proposed durable app-layer event. Never sent while staging.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OutboxEvent {
    pub event_type: String,
    pub txid: String,
    pub envelope: Vec<u8>,
}
/// Proposed commit precondition. The backend evaluates it before its own writes.
#[derive(Clone)]
pub enum Guard {
    OutputHeldUnspent {
        txid: String,
        output_index: u32,
        topic: String,
    },
    Backend {
        check: std::rc::Rc<dyn Any>,
        owner: String,
        bytes: u64,
    },
}
/// Proposed guard verdict. A lost head race is terminal for these bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreconditionVerdict {
    Replay,
    Contest { kind: String, reason: String },
    NotNow { reason: String },
}
/// Proposed successful decision. A journal is not landed until closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Committed {
    Batch,
    Journaled { journal: String },
}
/// Proposed four commit errors. Only Unanswered permits a late landing.
#[derive(Debug, thiserror::Error)]
pub enum CommitError {
    #[error("admission guard {guard} lost: {verdict:?}")]
    Precondition {
        guard: GuardId,
        verdict: PreconditionVerdict,
    },
    #[error("admission already applied")]
    Dedup,
    #[error("admission commit unanswered")]
    Unanswered,
    #[error("{0}")]
    Storage(#[from] StorageError),
}
/// Proposed one-submit write unit. Staging never performs external effects.
/// Implementations must atomically check guards and publish the batch, or its
/// journal decision. Dedup must not publish any of this unit's writes.
#[async_trait(?Send)]
pub trait AdmissionUnit {
    fn is_atomic(&self) -> bool;
    fn insert_output(&mut self, output: &Output);
    fn mark_spent(&mut self, txid: &str, output_index: u32, topic: &str);
    fn delete_output(&mut self, txid: &str, output_index: u32, topic: &str);
    fn update_consumed_by(&mut self, txid: &str, output_index: u32, topic: &str, by: &[Outpoint]);
    fn record_applied(&mut self, tx: &AppliedTransaction);
    fn require(&mut self, guard: Guard) -> GuardId;
    /// Retained checks allow the engine to ask the owner after a failed commit.
    fn guards(&self) -> Vec<(GuardId, Guard)>;
    /// Replace a topic manager's uncommitted staging on a fresh judgement.
    fn restart_topic(&mut self, topic: &str);
    fn end_topic(&mut self);
    fn stage(&mut self, write: StagedWrite);
    fn emit(&mut self, event: OutboxEvent);
    /// `bytes` includes heap allocations owned by the record. Any alone cannot
    /// measure them; size_of_val measures only the header of a Vec or String.
    fn put_record(&mut self, key: RecordKey, value: Box<dyn Any>, bytes: u64);
    fn record(&self, key: &RecordKey) -> Option<&dyn Any>;
    async fn find_output(
        &self,
        txid: &str,
        output_index: u32,
        topic: Option<&str>,
    ) -> Result<Option<Output>, StorageError>;
    async fn guard_holds(&self, guard: GuardId) -> Result<bool, StorageError>;
    /// Sum of bind bytes and their copies, records and envelopes. Each BEEF
    /// is held once by txid, irrespective of the number of admitted outputs.
    fn charge(&self) -> u64;
    /// Remaining memory after the walk. Cross it through the journal, never refusal.
    fn set_memory_allowance(&mut self, bytes: u64);
    async fn commit(self: Box<Self>, bound: Option<&CallBound>) -> Result<Committed, CommitError>;
}
impl dyn AdmissionUnit + '_ {
    /// Proposed typed record channel. Caller declares the record's owned bytes.
    pub fn stage_record<T: 'static>(&mut self, key: &str, value: T, bytes: u64) {
        self.put_record(
            RecordKey {
                type_id: TypeId::of::<T>(),
                key: key.into(),
            },
            Box::new(value),
            bytes,
        );
    }
    pub fn read_staged<T: 'static>(&self, key: &str) -> Option<&T> {
        self.record(&RecordKey {
            type_id: TypeId::of::<T>(),
            key: key.into(),
        })?
        .downcast_ref()
    }
}
/// Proposed route hook for writes belonging to the admission, such as its ledger.
pub trait SubmitStage {
    fn stage(&self, judged: &MutationReport, unit: &mut dyn AdmissionUnit);
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) enum EngineWrite {
    Insert(Output),
    Mark(String, u32, String),
    Delete(String, u32, String),
    Consumed(String, u32, String, Vec<Outpoint>),
    Applied(AppliedTransaction),
}
type StagedRecord = (Box<dyn Any>, u64, Option<String>);

#[derive(Default)]
pub(super) struct Staging {
    pub writes: Vec<EngineWrite>,
    pub beefs: HashMap<String, Vec<u8>>,
    pub guards: Vec<Guard>,
    pub backend: Vec<StagedWrite>,
    pub events: Vec<OutboxEvent>,
    records: HashMap<RecordKey, StagedRecord>,
    write_owners: Vec<Option<String>>,
    pub(super) guard_owners: Vec<Option<String>>,
    pub(super) backend_owners: Vec<Option<String>>,
    pub(super) event_owners: Vec<Option<String>>,
    pub guard_ids: Vec<GuardId>,
    pub(super) next_guard: GuardId,
    pub(super) owner: Option<String>,
    owner_binds: HashMap<String, u64>,
    binds: u64,
    pub memory_allowance: Option<u64>,
}
impl Staging {
    fn add_binds(&mut self, bytes: u64) {
        self.binds += bytes;
        if let Some(owner) = &self.owner {
            *self.owner_binds.entry(owner.clone()).or_default() += bytes;
        }
    }
    pub(super) fn restart_topic(&mut self, topic: &str) {
        fn retain<T>(items: &mut Vec<T>, owners: &mut Vec<Option<String>>, topic: &str) {
            let mut index = 0;
            items.retain(|_| {
                let keep = owners[index].as_deref() != Some(topic);
                index += 1;
                keep
            });
            owners.retain(|owner| owner.as_deref() != Some(topic));
        }
        retain(&mut self.writes, &mut self.write_owners, topic);
        let mut old_owners = self.guard_owners.clone();
        retain(&mut self.guard_ids, &mut old_owners, topic);
        retain(&mut self.guards, &mut self.guard_owners, topic);
        retain(&mut self.backend, &mut self.backend_owners, topic);
        retain(&mut self.events, &mut self.event_owners, topic);
        self.records
            .retain(|_, (_, _, owner)| owner.as_deref() != Some(topic));
        self.binds -= self.owner_binds.remove(topic).unwrap_or(0);
        self.owner = Some(topic.into());
    }

    pub fn insert(&mut self, output: &Output) {
        let mut row = output.clone();
        if let Some(beef) = row.beef.take() {
            if !self.beefs.contains_key(&row.txid) {
                self.binds += beef.len() as u64;
                self.beefs.insert(row.txid.clone(), beef);
            }
        }
        let row_binds = row.txid.len() as u64
            + row.topic.len() as u64
            + row.output_script.len() as u64
            + 4
            + 8
            + 1;
        let history_binds = row
            .outputs_consumed
            .iter()
            .chain(&row.consumed_by)
            .map(|o| o.txid.len() as u64 + 4)
            .sum::<u64>();
        self.add_binds(row_binds + history_binds);
        self.write_owners.push(self.owner.clone());
        self.writes.push(EngineWrite::Insert(row));
    }
    pub fn push(&mut self, write: EngineWrite) {
        let bytes = match &write {
            EngineWrite::Insert(_) => unreachable!(),
            EngineWrite::Mark(t, _, p) | EngineWrite::Delete(t, _, p) => {
                (t.len() + p.len() + 4) as u64
            }
            EngineWrite::Consumed(t, _, p, by) => {
                (t.len() + p.len() + 4) as u64
                    + by.iter().map(|o| o.txid.len() as u64 + 4).sum::<u64>()
            }
            EngineWrite::Applied(tx) => (tx.txid.len() + tx.topic.len()) as u64,
        };
        self.add_binds(bytes);
        self.write_owners.push(self.owner.clone());
        self.writes.push(write);
    }
    pub fn put(&mut self, key: RecordKey, value: Box<dyn Any>, bytes: u64) {
        self.records.insert(key, (value, bytes, self.owner.clone()));
    }
    pub fn get(&self, key: &RecordKey) -> Option<&dyn Any> {
        self.records.get(key).map(|(value, _, _)| value.as_ref())
    }
    pub fn charge(&self) -> u64 {
        2 * (self.binds
            + self
                .guards
                .iter()
                .map(|g| match g {
                    Guard::OutputHeldUnspent { txid, topic, .. } => {
                        (txid.len() + topic.len() + 4) as u64
                    }
                    Guard::Backend { bytes, .. } => *bytes,
                })
                .sum::<u64>()
            + self.backend.iter().map(|w| w.bytes).sum::<u64>()
            + self
                .events
                .iter()
                .map(|e| (e.event_type.len() + e.txid.len() + e.envelope.len()) as u64)
                .sum::<u64>())
            + self
                .records
                .values()
                .map(|(_, bytes, _)| bytes)
                .sum::<u64>()
    }
    pub fn overlay(
        &self,
        mut output: Option<Output>,
        txid: &str,
        index: u32,
        topic: Option<&str>,
    ) -> Option<Output> {
        for write in &self.writes {
            let matches =
                |t: &str, i: u32, p: &str| t == txid && i == index && topic.is_none_or(|v| v == p);
            match write {
                EngineWrite::Insert(o)
                    if matches(&o.txid, o.output_index, &o.topic) && output.is_none() =>
                {
                    output = Some(o.clone());
                }
                EngineWrite::Delete(t, i, p) if matches(t, *i, p) => output = None,
                EngineWrite::Mark(t, i, p) if matches(t, *i, p) => {
                    if let Some(o) = &mut output {
                        o.spent = true;
                    }
                }
                EngineWrite::Consumed(t, i, p, by) if matches(t, *i, p) => {
                    if let Some(o) = &mut output {
                        o.consumed_by.clone_from(by);
                    }
                }
                _ => {}
            }
        }
        if let Some(o) = &mut output {
            if let Some(beef) = self.beefs.get(txid) {
                o.beef = Some(beef.clone());
            }
        }
        output
    }
}

/// Compatibility unit for a backend which has not implemented atomic admission.
/// This is explicitly non-atomic. Opaque writes, guards and events are rejected
/// before any write; they can never be silently dropped by an old backend.
pub(super) struct CompatibilityUnit<'a, T: Storage + ?Sized> {
    storage: &'a T,
    staging: Staging,
}
impl<'a, T: Storage + ?Sized> CompatibilityUnit<'a, T> {
    pub fn new(storage: &'a T) -> Self {
        Self {
            storage,
            staging: Staging::default(),
        }
    }
}

macro_rules! staging_methods {
    () => {
        fn insert_output(&mut self, output: &Output) {
            self.staging.insert(output);
        }
        fn mark_spent(&mut self, txid: &str, output_index: u32, topic: &str) {
            self.staging
                .push(EngineWrite::Mark(txid.into(), output_index, topic.into()));
        }
        fn delete_output(&mut self, txid: &str, output_index: u32, topic: &str) {
            self.staging
                .push(EngineWrite::Delete(txid.into(), output_index, topic.into()));
        }
        fn update_consumed_by(
            &mut self,
            txid: &str,
            output_index: u32,
            topic: &str,
            by: &[Outpoint],
        ) {
            self.staging.push(EngineWrite::Consumed(
                txid.into(),
                output_index,
                topic.into(),
                by.to_vec(),
            ));
        }
        fn record_applied(&mut self, tx: &AppliedTransaction) {
            self.staging.push(EngineWrite::Applied(tx.clone()));
        }
        fn guards(&self) -> Vec<(GuardId, Guard)> {
            self.staging
                .guard_ids
                .iter()
                .copied()
                .zip(self.staging.guards.iter().cloned())
                .collect()
        }
        fn restart_topic(&mut self, topic: &str) {
            self.staging.restart_topic(topic);
        }
        fn end_topic(&mut self) {
            self.staging.owner = None;
        }
        fn require(&mut self, guard: Guard) -> GuardId {
            let id = self.staging.next_guard;
            self.staging.next_guard += 1;
            self.staging.guard_ids.push(id);
            self.staging.guard_owners.push(self.staging.owner.clone());
            self.staging.guards.push(guard);
            id
        }
        fn stage(&mut self, write: StagedWrite) {
            self.staging.backend_owners.push(self.staging.owner.clone());
            self.staging.backend.push(write);
        }
        fn emit(&mut self, event: OutboxEvent) {
            self.staging.event_owners.push(self.staging.owner.clone());
            self.staging.events.push(event);
        }
        fn put_record(&mut self, key: RecordKey, value: Box<dyn Any>, bytes: u64) {
            self.staging.put(key, value, bytes);
        }
        fn record(&self, key: &RecordKey) -> Option<&dyn Any> {
            self.staging.get(key)
        }
        fn set_memory_allowance(&mut self, bytes: u64) {
            self.staging.memory_allowance = Some(bytes);
        }
        fn charge(&self) -> u64 {
            self.staging.charge()
        }
    };
}
#[cfg(any(test, feature = "memory-storage"))]
pub(super) use staging_methods;

#[async_trait(?Send)]
impl<T: Storage + ?Sized> AdmissionUnit for CompatibilityUnit<'_, T> {
    fn is_atomic(&self) -> bool {
        false
    }
    staging_methods!();
    async fn find_output(
        &self,
        txid: &str,
        index: u32,
        topic: Option<&str>,
    ) -> Result<Option<Output>, StorageError> {
        let stored = self
            .storage
            .find_output(txid, index, topic, None, true)
            .await?;
        Ok(self.staging.overlay(stored, txid, index, topic))
    }
    async fn guard_holds(&self, _: GuardId) -> Result<bool, StorageError> {
        Err(StorageError::Other(
            "backend has no admission guards".into(),
        ))
    }
    async fn commit(self: Box<Self>, bound: Option<&CallBound>) -> Result<Committed, CommitError> {
        if !self.staging.guards.is_empty()
            || !self.staging.backend.is_empty()
            || !self.staging.events.is_empty()
        {
            return Err(StorageError::Other(
                "backend has no atomic admission implementation".into(),
            )
            .into());
        }
        let apply = async {
            for write in self.staging.writes {
                match write {
                    EngineWrite::Insert(mut o) => {
                        o.beef = self.staging.beefs.get(&o.txid).cloned();
                        self.storage.insert_output(&o).await?;
                    }
                    EngineWrite::Mark(t, i, p) => {
                        self.storage.mark_utxo_as_spent(&t, i, &p).await?;
                    }
                    EngineWrite::Delete(t, i, p) => self.storage.delete_output(&t, i, &p).await?,
                    EngineWrite::Consumed(t, i, p, by) => {
                        self.storage.update_consumed_by(&t, i, &p, &by).await?;
                    }
                    EngineWrite::Applied(tx) => {
                        self.storage.insert_applied_transaction(&tx).await?;
                    }
                }
            }
            Ok::<_, StorageError>(Committed::Batch)
        };
        match bound {
            Some(bound) => bound
                .call(apply)
                .await
                .ok_or(CommitError::Unanswered)?
                .map_err(Into::into),
            None => apply.await.map_err(Into::into),
        }
    }
}
