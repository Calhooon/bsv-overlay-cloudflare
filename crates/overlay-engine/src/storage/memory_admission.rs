//! Native atomic admission and deterministic fault seams. No D1 allowance is implied.
use super::admission::{staging_methods, EngineWrite, Staging};
use super::memory::MemoryStorage;
use super::*;
use std::any::Any;
use std::collections::{BTreeMap, HashMap};

/// Proposed native backend write. Index services use these in engine fixtures.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum MemoryWrite {
    Put {
        table: String,
        key: String,
        value: Vec<u8>,
    },
    Increment {
        table: String,
        key: String,
    },
}
/// Proposed native compare-and-set guard, evaluated against committed index rows.
#[derive(Debug, Clone)]
pub struct MemoryGuard {
    pub table: String,
    pub key: String,
    pub expected: Option<Vec<u8>>,
    pub lost: PreconditionVerdict,
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MemoryOutboxRow {
    pub event: OutboxEvent,
    pub attempts: u64,
    pub last_error: Option<String>,
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum Operation {
    Engine(EngineWrite),
    Backend(MemoryWrite),
    Event(OutboxEvent),
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Journal {
    operations: Vec<Operation>,
    beefs: HashMap<String, Vec<u8>>,
    next: usize,
    txids: Vec<String>,
}
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub(super) struct AdmissionRows {
    backend: BTreeMap<(String, String), Vec<u8>>,
    outbox: BTreeMap<u64, MemoryOutboxRow>,
    journals: BTreeMap<String, Journal>,
    sequence: u64,
}
/// Proposed fault/crash seam. Counts guards, writes, index rows, events, applied rows.
#[derive(Debug, Default)]
pub(super) struct AdmissionSeams {
    fault: Option<usize>,
    pub statement_bound: Option<usize>,
    pub decision_only: bool,
    pub unanswered_after_commit: bool,
}

impl MemoryStorage {
    pub fn set_admission_fault(&self, fault: Option<usize>) {
        self.admission_seams.lock().unwrap().fault = fault;
    }
    pub fn set_admission_statement_bound(&self, bound: Option<usize>) {
        self.admission_seams.lock().unwrap().statement_bound = bound;
    }
    pub fn set_admission_decision_only(&self, enabled: bool) {
        self.admission_seams.lock().unwrap().decision_only = enabled;
    }
    pub fn set_admission_unanswered_after_commit(&self, enabled: bool) {
        self.admission_seams.lock().unwrap().unanswered_after_commit = enabled;
    }
    pub fn admission_index(&self, table: &str, key: &str) -> Option<Vec<u8>> {
        self.admission_rows
            .lock()
            .unwrap()
            .backend
            .get(&(table.into(), key.into()))
            .cloned()
    }
    pub fn admission_outbox(&self) -> Vec<(u64, MemoryOutboxRow)> {
        self.admission_rows
            .lock()
            .unwrap()
            .outbox
            .iter()
            .map(|(id, row)| (*id, row.clone()))
            .collect()
    }
    /// Native drain seam: a refused send is retained with its attempt count.
    /// An external crash after send and before acknowledgement may redeliver.
    pub fn drain_admission_outbox(&self, mut send: impl FnMut(&OutboxEvent) -> Result<(), String>) {
        for (id, row) in self.admission_outbox() {
            let result = send(&row.event);
            let mut rows = self.admission_rows.lock().unwrap();
            match result {
                Ok(()) => {
                    rows.outbox.remove(&id);
                }
                Err(e) => {
                    if let Some(row) = rows.outbox.get_mut(&id) {
                        row.attempts += 1;
                        row.last_error = Some(e);
                    }
                }
            }
        }
    }
    pub fn has_admission_journal(&self, txid: &str) -> bool {
        self.admission_rows
            .lock()
            .unwrap()
            .journals
            .values()
            .any(|j| j.txids.iter().any(|t| t == txid))
    }
    /// Canonical snapshot of every table an admission can mutate. Existing
    /// witness snapshots additionally cover sync, proof-height and peer tables.
    pub fn admission_snapshot(&self) -> Vec<u8> {
        let outputs = self.outputs.lock().unwrap();
        let transactions = self.transactions.lock().unwrap();
        let proven = self.proven.lock().unwrap();
        let applied = self.applied.lock().unwrap();
        let created = self.created_at.lock().unwrap();
        let rows = self.admission_rows.lock().unwrap();
        fn sorted<T: serde::Serialize>(items: impl Iterator<Item = T>) -> Vec<Vec<u8>> {
            let mut rows: Vec<_> = items.map(|x| serde_json::to_vec(&x).unwrap()).collect();
            rows.sort();
            rows
        }
        serde_json::to_vec(&(
            sorted(outputs.iter()),
            sorted(transactions.iter()),
            sorted(proven.iter()),
            sorted(applied.iter()),
            sorted(created.iter()),
            sorted(rows.backend.iter()),
            sorted(rows.outbox.iter()),
            sorted(rows.journals.iter()),
            rows.sequence,
        ))
        .unwrap()
    }
    /// Native journal replay. The cursor and chunk publish under the same locks,
    /// so two drainers can never apply a non-idempotent index write twice.
    pub fn drain_admission_journals(&self, chunk: usize) -> Result<(), StorageError> {
        if chunk == 0 {
            return Err(StorageError::Other("zero journal drain allowance".into()));
        }
        loop {
            let now = *self.clock_secs.lock().unwrap();
            let mut outputs = self.outputs.lock().unwrap();
            let mut transactions = self.transactions.lock().unwrap();
            let mut proven = self.proven.lock().unwrap();
            let mut applied = self.applied.lock().unwrap();
            let mut created = self.created_at.lock().unwrap();
            let mut rows = self.admission_rows.lock().unwrap();
            let Some((id, journal)) = rows
                .journals
                .first_key_value()
                .map(|(id, j)| (id.clone(), j.clone()))
            else {
                return Ok(());
            };
            let end = journal
                .operations
                .len()
                .min(journal.next.saturating_add(chunk));
            for operation in &journal.operations[journal.next..end] {
                apply(
                    operation,
                    &journal.beefs,
                    now,
                    &mut outputs,
                    &mut transactions,
                    &mut proven,
                    &mut applied,
                    &mut created,
                    &mut rows,
                );
            }
            if end == journal.operations.len() {
                rows.journals.remove(&id);
            } else {
                rows.journals.get_mut(&id).unwrap().next = end;
            }
        }
    }
}

pub(super) struct MemoryAdmission<'a> {
    storage: &'a MemoryStorage,
    staging: Staging,
}
impl<'a> MemoryAdmission<'a> {
    pub fn new(storage: &'a MemoryStorage) -> Self {
        Self {
            storage,
            staging: Staging::default(),
        }
    }
}
fn guard_holds(
    guard: &Guard,
    outputs: &HashMap<(String, u32, String), Output>,
    rows: &AdmissionRows,
) -> Result<bool, StorageError> {
    match guard {
        Guard::OutputHeldUnspent {
            txid,
            output_index,
            topic,
        } => Ok(outputs
            .get(&(txid.clone(), *output_index, topic.clone()))
            .is_some_and(|o| !o.spent)),
        Guard::Backend { check, .. } => {
            let check = check.downcast_ref::<MemoryGuard>().ok_or_else(|| {
                StorageError::Other("unsupported MemoryStorage admission guard".into())
            })?;
            Ok(rows.backend.get(&(check.table.clone(), check.key.clone()))
                == check.expected.as_ref())
        }
    }
}

#[async_trait(?Send)]
impl AdmissionUnit for MemoryAdmission<'_> {
    fn is_atomic(&self) -> bool {
        true
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
    async fn guard_holds(&self, guard: GuardId) -> Result<bool, StorageError> {
        let check = self
            .staging
            .guard_ids
            .iter()
            .position(|id| *id == guard)
            .and_then(|index| self.staging.guards.get(index))
            .ok_or_else(|| StorageError::Other("unknown admission guard".into()))?;
        guard_holds(
            check,
            &self.storage.outputs.lock().unwrap(),
            &self.storage.admission_rows.lock().unwrap(),
        )
    }
    async fn commit(
        self: Box<Self>,
        bound: Option<&crate::engine::CallBound>,
    ) -> Result<Committed, CommitError> {
        if bound.is_some_and(crate::engine::CallBound::is_due) {
            return Err(CommitError::Unanswered);
        }
        let now = *self.storage.clock_secs.lock().unwrap();
        let mut outputs = self.storage.outputs.lock().unwrap();
        let mut transactions = self.storage.transactions.lock().unwrap();
        let mut proven = self.storage.proven.lock().unwrap();
        let mut applied = self.storage.applied.lock().unwrap();
        let mut created = self.storage.created_at.lock().unwrap();
        let mut rows = self.storage.admission_rows.lock().unwrap();
        let seams = self.storage.admission_seams.lock().unwrap();
        // Dedup wins over a spent-coin guard for concurrently submitted twins.
        let mut new_applied = std::collections::HashSet::new();
        for write in &self.staging.writes {
            if let EngineWrite::Applied(tx) = write {
                let key = (tx.txid.clone(), tx.topic.clone());
                if applied.contains_key(&key) || !new_applied.insert(key) {
                    return Err(CommitError::Dedup);
                }
            }
        }
        for (id, guard) in self.staging.guards.iter().enumerate() {
            if !guard_holds(guard, &outputs, &rows)? {
                let verdict = match guard {
                    Guard::OutputHeldUnspent { .. } => PreconditionVerdict::Contest {
                        kind: "contest".into(),
                        reason: "previous output no longer held and unspent".into(),
                    },
                    Guard::Backend { check, .. } => {
                        check.downcast_ref::<MemoryGuard>().unwrap().lost.clone()
                    }
                };
                return Err(CommitError::Precondition {
                    guard: self.staging.guard_ids[id],
                    verdict,
                });
            }
        }
        let over_memory = self
            .staging
            .memory_allowance
            .is_some_and(|allowance| self.staging.charge() > allowance);
        let mut operations = Vec::new();
        let mut applied_ops = Vec::new();
        for write in self.staging.writes {
            if matches!(write, EngineWrite::Applied(_)) {
                applied_ops.push(Operation::Engine(write));
            } else {
                operations.push(Operation::Engine(write));
            }
        }
        for write in self.staging.backend {
            operations.push(Operation::Backend(
                *write.write.downcast::<MemoryWrite>().map_err(|_| {
                    StorageError::Other("unsupported MemoryStorage staged write".into())
                })?,
            ));
        }
        operations.extend(self.staging.events.into_iter().map(Operation::Event));
        operations.extend(applied_ops);
        let count = self.staging.guards.len() + operations.len();
        let journaled = over_memory || seams.statement_bound.is_some_and(|b| count > b);
        // Fault injection models execution on a private transaction image. No
        // table is published until every statement, including the last, passed.
        let mut next_outputs = outputs.clone();
        let mut next_transactions = transactions.clone();
        let mut next_proven = proven.clone();
        let mut next_applied = applied.clone();
        let mut next_created = created.clone();
        let mut next_rows = rows.clone();
        let mut remainder = Vec::new();
        for id in 0..self.staging.guards.len() {
            if seams.fault == Some(id) {
                return Err(
                    StorageError::Other(format!("injected admission statement {id}")).into(),
                );
            }
        }
        for (index, operation) in operations.into_iter().enumerate() {
            let id = self.staging.guards.len() + index;
            if seams.fault == Some(id) {
                return Err(
                    StorageError::Other(format!("injected admission statement {id}")).into(),
                );
            }
            if journaled
                && !matches!(
                    operation,
                    Operation::Engine(EngineWrite::Mark(..) | EngineWrite::Applied(_))
                )
            {
                remainder.push(operation);
            } else {
                apply(
                    &operation,
                    &self.staging.beefs,
                    now,
                    &mut next_outputs,
                    &mut next_transactions,
                    &mut next_proven,
                    &mut next_applied,
                    &mut next_created,
                    &mut next_rows,
                );
            }
        }
        let result = if journaled {
            next_rows.sequence += 1;
            let id = format!("admission-{}", next_rows.sequence);
            next_rows.journals.insert(
                id.clone(),
                Journal {
                    operations: remainder,
                    beefs: self.staging.beefs,
                    next: 0,
                    txids: new_applied.into_iter().map(|(t, _)| t).collect(),
                },
            );
            Committed::Journaled { journal: id }
        } else {
            Committed::Batch
        };
        *outputs = next_outputs;
        *transactions = next_transactions;
        *proven = next_proven;
        *applied = next_applied;
        *created = next_created;
        *rows = next_rows;
        if seams.unanswered_after_commit {
            Err(CommitError::Unanswered)
        } else {
            Ok(result)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn apply(
    operation: &Operation,
    beefs: &HashMap<String, Vec<u8>>,
    now: u64,
    outputs: &mut HashMap<(String, u32, String), Output>,
    transactions: &mut HashMap<String, Vec<u8>>,
    proven: &mut std::collections::HashSet<String>,
    applied: &mut HashMap<(String, String), bool>,
    created: &mut HashMap<String, u64>,
    rows: &mut AdmissionRows,
) {
    match operation {
        Operation::Engine(EngineWrite::Insert(o)) => {
            outputs
                .entry((o.txid.clone(), o.output_index, o.topic.clone()))
                .or_insert_with(|| o.clone());
            if let Some(beef) = beefs.get(&o.txid) {
                transactions
                    .entry(o.txid.clone())
                    .or_insert_with(|| beef.clone());
                created.entry(o.txid.clone()).or_insert(now);
            }
        }
        Operation::Engine(EngineWrite::Mark(t, i, p)) => {
            if let Some(o) = outputs.get_mut(&(t.clone(), *i, p.clone())) {
                o.spent = true;
            }
        }
        Operation::Engine(EngineWrite::Delete(t, i, p)) => {
            outputs.remove(&(t.clone(), *i, p.clone()));
            if !outputs.keys().any(|(tx, _, _)| tx == t) {
                transactions.remove(t);
                proven.remove(t);
            }
        }
        Operation::Engine(EngineWrite::Consumed(t, i, p, by)) => {
            if let Some(o) = outputs.get_mut(&(t.clone(), *i, p.clone())) {
                o.consumed_by.clone_from(by);
            }
        }
        Operation::Engine(EngineWrite::Applied(tx)) => {
            applied.insert((tx.txid.clone(), tx.topic.clone()), true);
        }
        Operation::Backend(MemoryWrite::Put { table, key, value }) => {
            rows.backend
                .insert((table.clone(), key.clone()), value.clone());
        }
        Operation::Backend(MemoryWrite::Increment { table, key }) => {
            let value = rows
                .backend
                .entry((table.clone(), key.clone()))
                .or_default();
            let n = if value.is_empty() {
                0
            } else {
                u64::from_le_bytes(
                    value
                        .as_slice()
                        .try_into()
                        .expect("native increment row must be u64"),
                )
            };
            *value = n.saturating_add(1).to_le_bytes().to_vec();
        }
        Operation::Event(event) => {
            rows.sequence += 1;
            rows.outbox.insert(
                rows.sequence,
                MemoryOutboxRow {
                    event: event.clone(),
                    attempts: 0,
                    last_error: None,
                },
            );
        }
    }
}
