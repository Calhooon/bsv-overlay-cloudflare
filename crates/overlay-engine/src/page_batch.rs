//! The sync's storage per page (engine 0.2.2, the owner's word of
//! 2026-10-15 on #58's finding): a GASP page's admissions are written in one
//! storage batch at the page's end, and the reads they need are made for the
//! whole page at once.
//!
//! The engine installs a [`PageBuffer`] around a peer's sync future
//! ([`scope`], poll-scoped like [`crate::meter`], so a concurrent request
//! of the same isolate never sees it). The sync opens it at each page and
//! closes it at the page's end. While it is open the engine's storage
//! wrapper (`load_storage::MeteredStorage`):
//!
//! - queues the admission writes ([`PageWrite`]): an output with its
//!   transaction, an applied row, and a backend's own statement ([`defer`],
//!   the lookup index rows of the Worker);
//! - answers `find_output` (topic given, no spent filter, no BEEF) and
//!   `does_applied_transaction_exist` from the page's own writes, the page's
//!   prefetch ([`prefetch`]) and the answers already read in the page;
//! - flushes the queue before every other write and before every read of
//!   the tables the queue writes (a barrier), so the store sees the same
//!   writes in the same order as one write at a time would have written them.
//!
//! The queue stays in the buffer until the backend answers the batch `Ok`: a
//! batch that fails, or a sync dropped while its batch is in flight, leaves
//! it there, and the engine flushes it again after the sync or fails the
//! page. Every queued statement is idempotent (insert or ignore, the BEEF's
//! replace), so a batch applied twice writes once.
use crate::storage::{Storage, StorageError};
use crate::types::{AppliedTransaction, Output};
use std::{
    any::Any,
    cell::RefCell,
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

/// One write of a page's batch, in the order the admission made it.
#[derive(Clone)]
pub enum PageWrite {
    /// `Storage::insert_output_with_origin(output, gasp)`.
    Output { output: Rc<Output>, gasp: bool },
    /// `Storage::insert_applied_transaction`.
    Applied(AppliedTransaction),
    /// A statement of the backend's own (a lookup index row), opaque to the
    /// engine: the backend that deferred it applies it in its batch.
    Backend(Rc<dyn Any>),
}

/// The page's answer to a topic output read it knows: the output, or none
/// held.
pub(crate) struct Known(pub Option<Output>);

/// The key of a topic output read: `(txid, output_index, topic)`.
type OutputKey = (String, u32, String);

#[derive(Default)]
struct State {
    open: bool,
    writes: Vec<PageWrite>,
    /// Answers of `find_output(txid, vout, Some(topic), None, false)` known
    /// in this page: prefetched, read, or written by the page.
    outputs: HashMap<OutputKey, Option<Output>>,
    /// Answers of `does_applied_transaction_exist`, keyed `(txid, topic)`.
    applied: HashMap<(String, String), bool>,
}

/// A peer sync's page buffer (see the module).
#[derive(Clone, Default)]
pub struct PageBuffer(Rc<RefCell<State>>, Rc<RefCell<Option<Bound>>>);

/// The bound of one batch: the engine's finalize submit budget.
type Bound = (crate::engine::SleepFactory, u64);

impl PageBuffer {
    /// Bound each batch as one finalize submit's storage call is bounded
    /// (bsv-low #559): a batch with no answer in `budget_ms` is a fault.
    pub fn with_bound(self, sleep: crate::engine::SleepFactory, budget_ms: u64) -> Self {
        *self.1.borrow_mut() = Some((sleep, budget_ms));
        self
    }

    /// Start buffering: the sync's page begins.
    pub fn open(&self) {
        let mut state = self.0.borrow_mut();
        state.open = true;
        // Every queued write's key stays answered while it is queued.
        if state.writes.is_empty() {
            state.outputs.clear();
            state.applied.clear();
        }
    }

    /// Stop buffering. Writes still queued stay until a flush takes them.
    pub fn close(&self) {
        let mut state = self.0.borrow_mut();
        state.open = false;
        if state.writes.is_empty() {
            state.outputs.clear();
            state.applied.clear();
        }
    }

    pub fn is_open(&self) -> bool {
        self.0.borrow().open
    }

    /// Writes queued and not yet applied.
    pub fn pending(&self) -> usize {
        self.0.borrow().writes.len()
    }

    pub(crate) fn queue_output(&self, output: &Output, gasp: bool) {
        let mut state = self.0.borrow_mut();
        let mut held = output.clone();
        held.beef = None;
        state.outputs.insert(
            (
                output.txid.clone(),
                output.output_index,
                output.topic.clone(),
            ),
            Some(held),
        );
        state.writes.push(PageWrite::Output {
            output: Rc::new(output.clone()),
            gasp,
        });
    }

    pub(crate) fn queue_applied(&self, tx: &AppliedTransaction) {
        let mut state = self.0.borrow_mut();
        state
            .applied
            .insert((tx.txid.clone(), tx.topic.clone()), true);
        state.writes.push(PageWrite::Applied(tx.clone()));
    }

    /// The page's answer for a topic output read, if it has one.
    pub(crate) fn output(&self, txid: &str, vout: u32, topic: &str) -> Option<Known> {
        self.0
            .borrow()
            .outputs
            .get(&(txid.to_string(), vout, topic.to_string()))
            .cloned()
            .map(Known)
    }

    pub(crate) fn remember_output(
        &self,
        txid: &str,
        vout: u32,
        topic: &str,
        answer: Option<Output>,
    ) {
        self.0
            .borrow_mut()
            .outputs
            .entry((txid.to_string(), vout, topic.to_string()))
            .or_insert(answer);
    }

    pub(crate) fn applied(&self, tx: &AppliedTransaction) -> Option<bool> {
        self.0
            .borrow()
            .applied
            .get(&(tx.txid.clone(), tx.topic.clone()))
            .copied()
    }

    pub(crate) fn remember_applied(&self, tx: &AppliedTransaction, held: bool) {
        self.0
            .borrow_mut()
            .applied
            .entry((tx.txid.clone(), tx.topic.clone()))
            .or_insert(held);
    }

    /// Drop the queued writes unapplied: the page failed.
    pub fn discard(&self) {
        let mut state = self.0.borrow_mut();
        state.writes.clear();
        state.outputs.clear();
        state.applied.clear();
    }

    /// Forget every answer (a write the page does not model changed rows).
    pub(crate) fn forget_answers(&self) {
        let mut state = self.0.borrow_mut();
        state.outputs.clear();
        state.applied.clear();
    }

    /// Apply the queued writes as one batch through `storage` (the engine's
    /// own wrapper, which meters it). The writes leave the queue only when
    /// the batch answers `Ok`.
    pub async fn flush(&self, storage: &dyn Storage) -> Result<(), StorageError> {
        let writes = self.0.borrow().writes.clone();
        if writes.is_empty() {
            return Ok(());
        }
        let bound = self.1.borrow().clone();
        match bound {
            Some((sleep, budget_ms)) => {
                crate::gasp::race_or_deadline(storage.write_batch(&writes), sleep(budget_ms))
                    .await
                    .ok_or_else(|| {
                        StorageError::Database(format!(
                            "the page's batch had no answer within {budget_ms} ms"
                        ))
                    })??;
            }
            None => {
                storage.write_batch(&writes).await?;
            }
        }
        let mut state = self.0.borrow_mut();
        state.writes.drain(..writes.len());
        Ok(())
    }
}

thread_local! {
    static CURRENT: RefCell<Option<PageBuffer>> = const { RefCell::new(None) };
}

struct Restore(Option<PageBuffer>);
impl Drop for Restore {
    fn drop(&mut self) {
        CURRENT.with(|c| *c.borrow_mut() = self.0.take());
    }
}

/// `future` with `buffer` installed while it is polled.
pub struct Scoped<F> {
    buffer: PageBuffer,
    future: Pin<Box<F>>,
}

pub fn scope<F: Future>(buffer: PageBuffer, future: F) -> Scoped<F> {
    Scoped {
        buffer,
        future: Box::pin(future),
    }
}

impl<F: Future> Future for Scoped<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _restore = Restore(CURRENT.with(|c| c.borrow_mut().replace(self.buffer.clone())));
        self.future.as_mut().poll(cx)
    }
}

/// The installed buffer, whether or not a page is open.
pub fn installed() -> Option<PageBuffer> {
    CURRENT.with(|c| c.borrow().clone())
}

/// The installed buffer while a page is open.
pub fn current() -> Option<PageBuffer> {
    installed().filter(PageBuffer::is_open)
}

/// Queue a backend's own statement in the open page, or hand it back when no
/// page is open (the caller then runs it as before).
pub fn defer(statement: Rc<dyn Any>) -> Result<(), Rc<dyn Any>> {
    match current() {
        Some(page) => {
            page.0
                .borrow_mut()
                .writes
                .push(PageWrite::Backend(statement));
            Ok(())
        }
        None => Err(statement),
    }
}

/// Read, for the open page, what its admissions will ask: which of `inputs`
/// (`(txid, vout)`) are outputs of `topic`, and which of `txids` are applied
/// in it. Two reads for the page; an answer already known in the page (its
/// own write included) is kept. A read that faults prefetches nothing: each
/// question is then asked as before.
pub async fn prefetch(
    storage: &dyn Storage,
    topic: &str,
    inputs: &[(String, u32)],
    txids: &[String],
) {
    let Some(page) = current() else {
        return;
    };
    let inputs: Vec<_> = {
        let mut seen = HashSet::new();
        inputs
            .iter()
            .filter(|(txid, vout)| page.output(txid, *vout, topic).is_none())
            .filter(|key| seen.insert((*key).clone()))
            .cloned()
            .collect()
    };
    if !inputs.is_empty() {
        if let Ok(held) = storage.find_topic_outputs(topic, &inputs).await {
            let held: HashMap<_, _> = held
                .into_iter()
                .map(|o| ((o.txid.clone(), o.output_index), o))
                .collect();
            for (txid, vout) in &inputs {
                let answer = held.get(&(txid.clone(), *vout)).cloned();
                page.remember_output(txid, *vout, topic, answer);
            }
        }
    }
    let txids: Vec<_> = {
        let mut seen = HashSet::new();
        txids
            .iter()
            .filter(|txid| {
                page.applied(&AppliedTransaction {
                    txid: (*txid).clone(),
                    topic: topic.to_string(),
                })
                .is_none()
            })
            .filter(|txid| seen.insert((*txid).clone()))
            .cloned()
            .collect()
    };
    if !txids.is_empty() {
        if let Ok(applied) = storage.find_applied_transactions(topic, &txids).await {
            let applied: HashSet<_> = applied.into_iter().collect();
            for txid in txids {
                let held = applied.contains(&txid);
                page.remember_applied(
                    &AppliedTransaction {
                        txid,
                        topic: topic.to_string(),
                    },
                    held,
                );
            }
        }
    }
}
