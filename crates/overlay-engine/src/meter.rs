//! Poll-scoped counters: concurrent futures cannot charge each other's runs.
use serde::{Deserialize, Serialize};
use std::{
    cell::{Cell, RefCell},
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

/// Storage figures count attempted trait calls, including failures. They are
/// operations, not SQL rows; discovery lookups count as peer requests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SyncCost {
    pub calls_out: u64,
    pub peer_requests: u64,
    pub courier_rungs: u64,
    pub storage_reads: u64,
    pub storage_writes: u64,
    pub courier_not_found_memo_hits: u64,
    pub ms: u64,
}

impl SyncCost {
    pub fn since(self, before: Self) -> Self {
        Self {
            calls_out: self.calls_out.saturating_sub(before.calls_out),
            peer_requests: self.peer_requests.saturating_sub(before.peer_requests),
            courier_rungs: self.courier_rungs.saturating_sub(before.courier_rungs),
            storage_reads: self.storage_reads.saturating_sub(before.storage_reads),
            storage_writes: self.storage_writes.saturating_sub(before.storage_writes),
            courier_not_found_memo_hits: self
                .courier_not_found_memo_hits
                .saturating_sub(before.courier_not_found_memo_hits),
            ms: self.ms.saturating_sub(before.ms),
        }
    }
}

struct Data {
    cost: Cell<SyncCost>,
    started: f64,
}
#[derive(Clone)]
pub struct LoadMeter(Rc<Data>);
impl Default for LoadMeter {
    fn default() -> Self {
        Self(Rc::new(Data {
            cost: Cell::default(),
            started: crate::engine::current_timestamp_ms(),
        }))
    }
}
impl LoadMeter {
    pub fn snapshot(&self) -> SyncCost {
        let mut cost = self.0.cost.get();
        cost.ms = (crate::engine::current_timestamp_ms() - self.0.started).max(0.0) as u64;
        cost
    }
    pub fn scope<F: Future>(&self, future: F) -> Measured<F> {
        Measured {
            meter: self.clone(),
            future: Box::pin(future),
        }
    }
}
thread_local! {
    static CURRENT: RefCell<Option<LoadMeter>> = const { RefCell::new(None) };
    static ROOT: RefCell<Option<Rc<RootLimit>>> = const { RefCell::new(None) };
}
struct Restore(Option<LoadMeter>);
impl Drop for Restore {
    fn drop(&mut self) {
        CURRENT.with(|c| *c.borrow_mut() = self.0.take());
    }
}
pub struct Measured<F> {
    meter: LoadMeter,
    future: Pin<Box<F>>,
}
impl<F: Future> Future for Measured<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _restore = Restore(CURRENT.with(|c| c.borrow_mut().replace(self.meter.clone())));
        self.future.as_mut().poll(cx)
    }
}
pub fn snapshot() -> SyncCost {
    CURRENT.with(|c| {
        c.borrow()
            .as_ref()
            .map_or_else(SyncCost::default, LoadMeter::snapshot)
    })
}
fn note(f: impl FnOnce(&mut SyncCost)) {
    CURRENT.with(|c| {
        if let Some(m) = c.borrow().as_ref() {
            let mut v = m.0.cost.get();
            f(&mut v);
            m.0.cost.set(v);
        }
    });
}
pub fn peer_request() {
    note(|v| {
        v.calls_out += 1;
        v.peer_requests += 1;
    });
}
pub fn storage_read() {
    note(|v| v.storage_reads += 1);
}
pub fn storage_write() {
    note(|v| v.storage_writes += 1);
}
pub fn not_found_memo_hit() {
    note(|v| v.courier_not_found_memo_hits += 1);
}

/// Twelve courier HTTP attempts at at most 1,000 ms each fit in a 15,000 ms
/// graph slice, leaving 3,000 ms for peer/storage work. With the 30,000 ms
/// peer slice and 9,900 ms resume share, the page retains 20,100 ms. These
/// are stop bounds, not promises about finalize/storage latency.
pub const ROOT_MAX_COURIER_RUNGS: u32 = 12;
pub const COURIER_RUNG_TIMEOUT_MS: u64 = 1_000;
pub const ROOT_MAX_MS: u64 = 15_000;
#[derive(Default)]
struct RootLimit {
    calls: Cell<u32>,
    exhausted: Cell<bool>,
}
pub struct RootScope<F> {
    limit: Rc<RootLimit>,
    future: Pin<Box<F>>,
}
pub fn root_scope<F: Future>(future: F) -> RootScope<F> {
    RootScope {
        limit: Rc::default(),
        future: Box::pin(future),
    }
}
struct RestoreRoot(Option<Rc<RootLimit>>);
impl Drop for RestoreRoot {
    fn drop(&mut self) {
        ROOT.with(|c| *c.borrow_mut() = self.0.take());
    }
}
impl<F: Future> Future for RootScope<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _restore = RestoreRoot(ROOT.with(|c| c.borrow_mut().replace(self.limit.clone())));
        self.future.as_mut().poll(cx)
    }
}
pub fn root_scoped() -> bool {
    ROOT.with(|c| c.borrow().is_some())
}
pub fn root_exhausted() -> bool {
    ROOT.with(|c| c.borrow().as_ref().is_some_and(|r| r.exhausted.get()))
}
/// Charge before awaiting HTTP. A refused charge sends no request.
pub fn courier_request() -> Result<(), &'static str> {
    let allowed = ROOT.with(|c| {
        c.borrow().as_ref().is_none_or(|r| {
            if r.calls.get() >= ROOT_MAX_COURIER_RUNGS {
                r.exhausted.set(true);
                false
            } else {
                r.calls.set(r.calls.get() + 1);
                true
            }
        })
    });
    if !allowed {
        return Err("root courier rung budget exhausted");
    }
    note(|v| {
        v.calls_out += 1;
        v.courier_rungs += 1;
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn load_0112_cancelled_run_keeps_its_attempts() {
        let meter = LoadMeter::default();
        let mut task = Box::pin(meter.scope(async {
            peer_request();
            storage_write();
            std::future::pending::<()>().await;
        }));
        std::future::poll_fn(|cx| {
            assert!(task.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(task);
        assert_eq!(
            (
                meter.snapshot().peer_requests,
                meter.snapshot().storage_writes
            ),
            (1, 1)
        );
        assert_eq!(snapshot(), SyncCost::default());
    }

    #[tokio::test]
    async fn load_0112_poll_scopes_keep_interleaved_runs_separate() {
        let a = LoadMeter::default();
        let b = LoadMeter::default();
        let ((), ()) = tokio::join!(
            a.scope(async {
                peer_request();
                tokio::task::yield_now().await;
                storage_read();
            }),
            b.scope(async {
                storage_write();
                tokio::task::yield_now().await;
                courier_request().unwrap();
            })
        );
        assert_eq!(
            (
                a.snapshot().peer_requests,
                a.snapshot().storage_reads,
                a.snapshot().storage_writes
            ),
            (1, 1, 0)
        );
        assert_eq!(
            (
                b.snapshot().courier_rungs,
                b.snapshot().storage_writes,
                b.snapshot().storage_reads
            ),
            (1, 1, 0)
        );
        assert_eq!(snapshot(), SyncCost::default());
    }
}
