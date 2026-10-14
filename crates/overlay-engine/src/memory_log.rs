// Included in storage::memory so the transaction snapshots every stored table.
use crate::admission::*;
use crate::engine::Engine;
use std::collections::BTreeMap;
use std::future::Future;
use std::task::{Context, Poll, Waker};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CrashPoint {
    #[default]
    None,
    BeforeUnit,
    AfterCommit,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct OutboxRow {
    pub seq: u64,
    pub index: usize,
    pub event: OutboxEvent,
    pub attempts: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MemoryUnit {
    pub seq: u64,
    pub txid: String,
    pub now_ms: i64,
}

#[derive(Debug)]
struct MemoryScope {
    seq: u64,
    now_ms: i64,
    evidence: std::sync::Arc<AdmitEvidence>,
    notes: RefusalNotes,
    site: String,
    failed: Option<String>,
    deferred: Option<AdmitRefusal>,
    writes: usize,
    dry: bool,
}

#[derive(Debug, Default)]
struct MemoryLogState {
    scope: Option<MemoryScope>,
    head: u64,
    units: Vec<MemoryUnit>,
    rows: BTreeMap<(String, Vec<u8>), Vec<u8>>,
    outbox: Vec<OutboxRow>,
    sql_statements: Vec<SqlStmt>,
    after_commit: Vec<(u64, AfterCommit)>,
    crash: CrashPoint,
    awaited_io: u64,
    poisoned: bool,
}

impl MemoryStorage {
    /// One-shot fault seam. Counts every engine, manager and outbox write.
    fn write_pin(&self) -> Result<(), StorageError> {
        let mut fault = self.admission_fault.lock().unwrap();
        fault.1 += 1;
        let fails = fault.0 == Some(fault.1);
        drop(fault);
        let mut log = self.log.lock().unwrap();
        if let Some(scope) = &mut log.scope {
            if scope.dry {
                scope
                    .failed
                    .get_or_insert_with(|| "write attempted during dry judgement".into());
                return Err(StorageError::Other(
                    "write attempted during dry judgement".into(),
                ));
            }
            scope.writes += 1;
            if fails {
                scope
                    .failed
                    .get_or_insert_with(|| "E657 injected write fault".into());
            }
        }
        if fails {
            Err(StorageError::Other("E657 injected write fault".into()))
        } else {
            Ok(())
        }
    }

    pub fn crash_at(&self, point: CrashPoint) {
        self.log.lock().unwrap().crash = point;
    }
    pub fn log_head(&self) -> u64 {
        self.log.lock().unwrap().head
    }
    pub fn log_units(&self) -> Vec<MemoryUnit> {
        self.log.lock().unwrap().units.clone()
    }
    pub fn outbox(&self) -> Vec<OutboxRow> {
        self.log.lock().unwrap().outbox.clone()
    }
    pub fn awaited_io(&self) -> u64 {
        self.log.lock().unwrap().awaited_io
    }

    /// The memory counterpart of a manager/service table statement. Such
    /// tables are owned by the same transaction as the engine's tables.
    pub fn put_admission_row(
        &self,
        table: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), StorageError> {
        if self.log.lock().unwrap().scope.is_none() {
            return Err(StorageError::Other(
                "manager write outside admission unit".into(),
            ));
        }
        self.write_pin()?;
        self.log
            .lock()
            .unwrap()
            .rows
            .insert((table.into(), key.to_vec()), value.to_vec());
        Ok(())
    }
    pub fn admission_row(&self, table: &str, key: &[u8]) -> Option<Vec<u8>> {
        self.log
            .lock()
            .unwrap()
            .rows
            .get(&(table.into(), key.to_vec()))
            .cloned()
    }

    /// Execute exactly one synchronous poll. The future is dropped BEFORE a
    /// rollback, so neither a pending continuation nor its Drop can escape it.
    /// The rollback guard also restores on unwinding. The writer lock is local
    /// to this store and prevents reentrant units and simultaneous threads.
    pub fn transaction<T, F: Future<Output = Result<T, AdmitRefusal>>>(
        &self,
        txid: &str,
        evidence: &AdmitEvidence,
        run: impl FnOnce() -> F,
    ) -> Result<T, LogError> {
        if self.log.lock().unwrap().poisoned { return Err(LogError::AnswerLost); }
        let _writer = self
            .writer
            .try_lock()
            .map_err(|_| AdmitRefusal::not_now("admission_busy", "one writer per log"))?;
        {
            let mut log = self.log.lock().unwrap();
            if log.crash == CrashPoint::BeforeUnit {
                log.crash = CrashPoint::None;
                return Err(LogError::AnswerLost);
            }
        }
        let mut rollback = MemoryRollback {
            store: self,
            snapshot: Some(MemorySnapshot::take(self)),
        };
        let now_ms = i64::try_from(*self.clock_secs.lock().unwrap())
            .unwrap_or(i64::MAX)
            .saturating_mul(1000);
        {
            let mut log = self.log.lock().unwrap();
            log.scope = Some(MemoryScope {
                seq: log.head + 1,
                now_ms,
                evidence: std::sync::Arc::new(evidence.clone()),
                notes: vec![],
                site: "admit".into(),
                failed: None,
                deferred: None,
                writes: 0,
                dry: false,
            });
        }
        let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut future = std::pin::pin!(run());
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
        }));
        let polled = match polled {
            Ok(polled) => polled,
            Err(_) => {
                self.log.lock().unwrap().poisoned = true;
                return Err(LogError::AnswerLost);
            }
        };
        let scope = self.log.lock().unwrap().scope.take().expect("unit scope");
        let result = match polled {
            Poll::Pending => {
                self.log.lock().unwrap().awaited_io += 1;
                Err(AdmitRefusal::transient("admit_awaited_io", scope.site))
            }
            Poll::Ready(result) => result,
        };
        // A failed statement takes precedence over a later verdict derived
        // from the incomplete store. It must remain retryable.
        let result = match scope.deferred {
            Some(refusal) => Err(refusal),
            None => result,
        };
        let result = match (result, scope.failed) {
            (_, Some(error)) => Err(AdmitRefusal::transient("admit_statement_failed", error)),
            (result, _) => result,
        }
        .map_err(|error| error.with_notes(scope.notes));
        let value = result?;
        {
            let mut log = self.log.lock().unwrap();
            if scope.writes != 0 {
                log.head = scope.seq;
                log.units.push(MemoryUnit {
                    seq: scope.seq,
                    txid: txid.into(),
                    now_ms,
                });
            }
        }
        rollback.snapshot = None;
        let mut log = self.log.lock().unwrap();
        if log.crash == CrashPoint::AfterCommit {
            log.crash = CrashPoint::None;
            return Err(LogError::AnswerLost);
        }
        Ok(value)
    }

    /// Predecessors commit as independent units. A refused successor never
    /// rolls back a predecessor; a predecessor fault stops the graph.
    pub fn admit(&self, engine: &Engine, walked: &Walked) -> Result<Admitted, LogError> {
        // Keep only keys on the landing stack. A full BEEF per stack frame
        // would multiply a large carried body by the predecessor bound.
        let mut pending = vec![(walked.txid.clone(), walked.tagged_beef.topics.clone())];
        let mut attempts = 0;
        let mut landed = Vec::new();
        while let Some((txid, topics)) = pending.last() {
            attempts += 1;
            if attempts > 257 {
                return Err(AdmitRefusal::not_now(
                    "predecessor_not_landed",
                    "landing allowance exhausted",
                )
                .into());
            }
            let carried_unit;
            let unit = if txid == &walked.txid {
                walked
            } else {
                let Some(carried) = walked.carried.iter().find(|c| &c.txid == txid) else {
                    return Err(
                        AdmitRefusal::not_now("admit_evidence_missing", txid.clone()).into(),
                    );
                };
                let mut beef = crate::beef_limits::parse_beef(
                    &walked.tagged_beef.beef,
                    &crate::beef_limits::ENGINE_BEEF_LIMITS,
                )
                .map_err(|e| AdmitRefusal::transient("carried_payload", e))?;
                let bytes = beef
                    .to_binary_atomic(txid)
                    .map_err(|e| AdmitRefusal::transient("carried_payload", e))?;
                carried_unit = Walked {
                    txid: txid.clone(),
                    tagged_beef: crate::types::TaggedBEEF::new(bytes, topics.clone()),
                    mode: walked.mode,
                    policy: walked.policy,
                    walk: carried.walk,
                    carried: walked.carried.clone(),
                    evidence: walked.evidence.clone(),
                    charge: walked.charge,
                    could_not_run: None,
                };
                &carried_unit
            };
            match self.transaction(&unit.txid, &unit.evidence, || engine.admit(unit)) {
                Ok(mut admitted) => {
                    admitted.seq = self.log_head();
                    if pending.len() > 1 {
                        landed.extend(
                            admitted
                                .report
                                .applied_topics
                                .iter()
                                .map(|topic| (unit.txid.clone(), topic.clone())),
                        );
                    }
                    pending.pop();
                    if pending.is_empty() {
                        admitted.report.landed_predecessors = landed;
                        return Ok(admitted);
                    }
                }
                Err(LogError::Refused(AdmitRefusal::NotNow {
                    predecessor: Some((id, topic)),
                    ..
                })) => {
                    if pending.iter().any(|(txid, _)| txid == &id) {
                        return Err(AdmitRefusal::not_now(
                            "predecessor_not_landed",
                            "cyclic landing",
                        )
                        .into());
                    }
                    pending.push((id, vec![topic]));
                }
                Err(LogError::Refused(error)) if pending.len() > 1 => {
                    return Err(
                        AdmitRefusal::not_now("predecessor_not_landed", error.to_string())
                            .with_notes(error.notes().clone())
                            .into(),
                    );
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("subject unit is always present")
    }

    /// At-least-once delivery. Failed sends retain the row and attempt count;
    /// cancellation after send but before ack permits safe redelivery.
    pub async fn drain_outbox<F, Fut>(&self, mut send: F) -> Result<usize, String>
    where
        F: FnMut(OutboxEvent) -> Fut,
        Fut: Future<Output = Result<(), String>>,
    {
        if self.log.lock().unwrap().scope.is_some() {
            return Err("outbox drain inside unit".into());
        }
        let mut count = 0;
        loop {
            let Some(row) = self.outbox().first().cloned() else {
                return Ok(count);
            };
            let result = send(row.event).await;
            let mut log = self.log.lock().unwrap();
            if let Some(index) = log
                .outbox
                .iter()
                .position(|r| (r.seq, r.index) == (row.seq, row.index))
            {
                match result {
                    Ok(()) => {
                        log.outbox.remove(index);
                        count += 1;
                    }
                    Err(error) => {
                        log.outbox[index].attempts += 1;
                        log.outbox[index].last_error = Some(error.clone());
                        return Err(error);
                    }
                }
            }
        }
    }
}

impl LogTxn for MemoryStorage {
    fn sql(&self) -> &dyn SqlExecutor { self }
    fn after_commit(&self, intent: AfterCommit) {
        if self.write_pin().is_err() { return; }
        let mut log = self.log.lock().unwrap();
        let seq = log.scope.as_ref().expect("open unit").seq;
        log.after_commit.push((seq, intent));
    }
    fn defer(&self, site: &str, reason: String) {
        self.log.lock().unwrap().scope.as_mut().expect("open unit")
            .deferred.get_or_insert_with(|| AdmitRefusal::not_now(site, reason));
    }
    fn deferral(&self) -> Option<AdmitRefusal> {
        self.log.lock().unwrap().scope.as_ref().expect("open unit").deferred.clone()
    }
    fn seq(&self) -> u64 {
        self.log.lock().unwrap().scope.as_ref().map_or(0, |s| s.seq)
    }
    fn now_ms(&self) -> i64 {
        self.log
            .lock()
            .unwrap()
            .scope
            .as_ref()
            .expect("open unit")
            .now_ms
    }
    fn evidence(&self) -> std::sync::Arc<AdmitEvidence> {
        self.log
            .lock()
            .unwrap()
            .scope
            .as_ref()
            .expect("open unit")
            .evidence
            .clone()
    }
    fn note(&self, topic: &str, note: RefusalNote) {
        self.log
            .lock()
            .unwrap()
            .scope
            .as_mut()
            .expect("open unit")
            .notes
            .push((topic.into(), note));
    }
    fn notes(&self) -> RefusalNotes {
        self.log
            .lock()
            .unwrap()
            .scope
            .as_ref()
            .expect("open unit")
            .notes
            .clone()
    }
    fn at(&self, site: &str) {
        self.log
            .lock()
            .unwrap()
            .scope
            .as_mut()
            .expect("open unit")
            .site = site.into();
    }
    fn fail(&self, reason: String) {
        self.log
            .lock()
            .unwrap()
            .scope
            .as_mut()
            .expect("open unit")
            .failed
            .get_or_insert(reason);
    }
    fn read_only(&self) -> Box<dyn DryRunScope + '_> {
        let mut log = self.log.lock().unwrap();
        let scope = log.scope.as_mut().expect("open unit");
        let was_dry = scope.dry;
        scope.dry = true;
        Box::new(MemoryReadOnly {
            store: self,
            was_dry,
        })
    }
    fn emit(&self, event: OutboxEvent) {
        if self.log.lock().unwrap().scope.is_none() {
            return;
        }
        if self.write_pin().is_err() { return; }
        let mut log = self.log.lock().unwrap();
        let seq = log.scope.as_ref().expect("open unit").seq;
        let index = log.outbox.iter().filter(|r| r.seq == seq).count();
        log.outbox.push(OutboxRow {
            seq,
            index,
            event,
            attempts: 0,
            last_error: None,
        });
    }
}

struct MemoryRollback<'a> {
    store: &'a MemoryStorage,
    snapshot: Option<MemorySnapshot>,
}
impl Drop for MemoryRollback<'_> {
    fn drop(&mut self) {
        if let Some(snapshot) = self.snapshot.take() {
            snapshot.restore(self.store);
        }
        self.store.log.lock().unwrap().scope = None;
    }
}

struct MemorySnapshot {
    outputs: HashMap<(String, u32, String), Output>,
    transactions: HashMap<String, Vec<u8>>,
    proven: std::collections::HashSet<String>,
    applied: HashMap<(String, String), bool>,
    sync_state: HashMap<(String, String), u64>,
    clock_secs: u64,
    created_at: HashMap<String, u64>,
    peer_health: HashMap<(String, String), (u64, u64)>,
    proven_at: HashMap<String, Option<u64>>,
    deferred_graphs: HashMap<(String, String, String), crate::gasp::DeferredGraph>,
    peer_yieldless: HashMap<(String, String), YieldStreakRow>,
    head: u64,
    units: Vec<MemoryUnit>,
    rows: BTreeMap<(String, Vec<u8>), Vec<u8>>,
    outbox: Vec<OutboxRow>,
    sql_statements: Vec<SqlStmt>,
    after_commit: Vec<(u64, AfterCommit)>,
}
impl MemorySnapshot {
    fn take(store: &MemoryStorage) -> Self {
        let log = store.log.lock().unwrap();
        Self {
            outputs: store.outputs.lock().unwrap().clone(),
            transactions: store.transactions.lock().unwrap().clone(),
            proven: store.proven.lock().unwrap().clone(),
            applied: store.applied.lock().unwrap().clone(),
            sync_state: store.sync_state.lock().unwrap().clone(),
            clock_secs: *store.clock_secs.lock().unwrap(),
            created_at: store.created_at.lock().unwrap().clone(),
            peer_health: store.peer_health.lock().unwrap().clone(),
            proven_at: store.proven_at.lock().unwrap().clone(),
            deferred_graphs: store.deferred_graphs.lock().unwrap().clone(),
            peer_yieldless: store.peer_yieldless.lock().unwrap().clone(),
            head: log.head,
            units: log.units.clone(),
            rows: log.rows.clone(),
            outbox: log.outbox.clone(),
            sql_statements: log.sql_statements.clone(),
            after_commit: log.after_commit.clone(),
        }
    }
    fn restore(self, store: &MemoryStorage) {
        *store.outputs.lock().unwrap() = self.outputs;
        *store.transactions.lock().unwrap() = self.transactions;
        *store.proven.lock().unwrap() = self.proven;
        *store.applied.lock().unwrap() = self.applied;
        *store.sync_state.lock().unwrap() = self.sync_state;
        *store.clock_secs.lock().unwrap() = self.clock_secs;
        *store.created_at.lock().unwrap() = self.created_at;
        *store.peer_health.lock().unwrap() = self.peer_health;
        *store.proven_at.lock().unwrap() = self.proven_at;
        *store.deferred_graphs.lock().unwrap() = self.deferred_graphs;
        *store.peer_yieldless.lock().unwrap() = self.peer_yieldless;
        let mut log = store.log.lock().unwrap();
        log.head = self.head;
        log.units = self.units;
        log.rows = self.rows;
        log.outbox = self.outbox;
        log.sql_statements = self.sql_statements;
        log.after_commit = self.after_commit;
    }
}

struct MemoryDryScope<'a> {
    rollback: MemoryRollback<'a>,
    _writer: std::sync::MutexGuard<'a, ()>,
}
impl DryRunScope for MemoryDryScope<'_> {
    fn failure(&self) -> Option<String> {
        self.rollback
            .store
            .log
            .lock()
            .unwrap()
            .scope
            .as_ref()
            .and_then(|s| s.failed.clone())
    }
}
impl MemoryStorage {
    fn dry_scope(
        &self,
        evidence: &AdmitEvidence,
    ) -> Result<Box<dyn DryRunScope + '_>, StorageError> {
        let writer = self
            .writer
            .try_lock()
            .map_err(|_| StorageError::Other("admission busy".into()))?;
        let rollback = MemoryRollback {
            store: self,
            snapshot: Some(MemorySnapshot::take(self)),
        };
        self.log.lock().unwrap().scope = Some(MemoryScope {
            seq: 0,
            now_ms: 0,
            evidence: std::sync::Arc::new(evidence.clone()),
            notes: vec![],
            site: "dry judgement".into(),
            failed: None,
            deferred: None,
            writes: 0,
            dry: true,
        });
        Ok(Box::new(MemoryDryScope {
            rollback,
            _writer: writer,
        }))
    }
}

struct MemoryReadOnly<'a> {
    store: &'a MemoryStorage,
    was_dry: bool,
}
impl DryRunScope for MemoryReadOnly<'_> {
    fn failure(&self) -> Option<String> {
        self.store
            .log
            .lock()
            .unwrap()
            .scope
            .as_ref()
            .and_then(|s| s.failed.clone())
    }
}
impl Drop for MemoryReadOnly<'_> {
    fn drop(&mut self) {
        if let Some(scope) = self.store.log.lock().unwrap().scope.as_mut() {
            scope.dry = self.was_dry;
        }
    }
}

impl UnitRunner for MemoryStorage {
    fn run_unit<'a>(
        &'a self,
        _kind: UnitKind,
        evidence: &'a AdmitEvidence,
        unit: LocalBoxFuture<'a, Result<UnitOutput, AdmitRefusal>>,
    ) -> Result<Committed, AdmitRefusal> {
        let result = self.transaction("", evidence, || async {
            let output = unit.await?;
            Ok((output, self.notes()))
        });
        match result {
            Ok((output, notes)) => Ok(Committed { seq: self.log_head(), output, notes }),
            Err(LogError::Refused(refusal)) => Err(refusal),
            Err(lost @ LogError::AnswerLost) => std::panic::resume_unwind(Box::new(lost)),
        }
    }
}

impl MemoryStorage {
    /// Statements recorded by the memory executor, not evaluated as SQLite.
    pub fn sql_statements(&self) -> Vec<SqlStmt> {
        self.log.lock().unwrap().sql_statements.clone()
    }
    /// Only committed intents may reach a host's external side effects.
    pub fn after_commit_intents(&self) -> Vec<(u64, AfterCommit)> {
        let log = self.log.lock().unwrap();
        log.after_commit.iter().filter(|(seq, _)| *seq <= log.head).cloned().collect()
    }
    fn record_sql(&self, sql: &str, binds: &[SqlValue], write: bool) -> Result<(), StorageError> {
        if self.log.lock().unwrap().scope.is_none() {
            return Err(StorageError::Other("SQL outside admission unit".into()));
        }
        if write { self.write_pin()?; }
        self.log.lock().unwrap().sql_statements.push(SqlStmt { sql: sql.into(), binds: binds.to_vec() });
        Ok(())
    }
}
/// A recording executor for testing the unit boundary. It returns empty reads
/// and zero changes; hosts needing SQL evaluation supply a SQLite executor.
#[async_trait(?Send)]
impl SqlExecutor for MemoryStorage {
    async fn run(&self, sql: &str, binds: &[SqlValue]) -> Result<SqlRun, StorageError> {
        self.record_sql(sql, binds, true)?;
        Ok(SqlRun { changes: 0, last_row_id: None })
    }
    async fn all(&self, sql: &str, binds: &[SqlValue]) -> Result<Vec<SqlRow>, StorageError> {
        self.record_sql(sql, binds, false)?;
        Ok(vec![])
    }
    async fn batch(&self, stmts: &[SqlStmt]) -> Result<Vec<SqlRun>, StorageError> {
        let start = self.log.lock().unwrap().sql_statements.len();
        let mut results = Vec::new();
        for stmt in stmts {
            match self.run(&stmt.sql, &stmt.binds).await {
                Ok(result) => results.push(result),
                Err(error) => {
                    self.log.lock().unwrap().sql_statements.truncate(start);
                    return Err(error);
                }
            }
        }
        Ok(results)
    }
}
