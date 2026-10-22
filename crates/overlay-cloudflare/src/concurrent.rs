//! bsv-low #838: independent awaits issued together, answered when the last lands.
//!
//! Under the fleet's load a D1 statement's latency is mostly its wait in the database's queue (loop 44:
//! `/health/invariants` spent 5 to 20 s of wall time on 20 to 170 ms of CPU, 31 statements awaited one after
//! another). Awaited in turn, independent reads pay that wait once each; issued together they pay it about once.
//! [`concurrently!`] is that join for a handful of heterogeneous futures, without a new dependency: each future
//! is polled until it is done, every poll of the join polls every future not yet done, and the outputs are bound
//! to the names the caller gives, in the caller's scope.
//!
//! Only INDEPENDENT work belongs in one join: no future may need another's effect (a read after a write, a
//! counter seeded by another). The request's D1 ledger still counts every statement: the join is polled inside
//! the request's scope (`d1_ledger::Scoped`), so each statement lands on the request that issued it.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// One future of a join: pending, or done with its output held until it is taken.
pub enum MaybeDone<F: Future> {
    /// Not yet done.
    Pending(Pin<Box<F>>),
    /// Done; the output until [`MaybeDone::take_output`].
    Done(Option<F::Output>),
}

impl<F: Future> MaybeDone<F> {
    /// A future not yet polled.
    pub fn new(f: F) -> Self {
        Self::Pending(Box::pin(f))
    }

    /// Poll it if it is still pending; `true` once it is done (a done future is never polled again).
    pub fn poll_done(&mut self, cx: &mut Context<'_>) -> bool {
        if let Self::Pending(f) = self {
            match f.as_mut().poll(cx) {
                Poll::Ready(v) => *self = Self::Done(Some(v)),
                Poll::Pending => return false,
            }
        }
        true
    }

    /// The output, once it is done (`None` before, and after it was taken).
    pub fn take_output(&mut self) -> Option<F::Output> {
        match self {
            Self::Done(v) => v.take(),
            Self::Pending(_) => None,
        }
    }
}

/// `concurrently! { a = fut_a, b = fut_b, .. }`: polls every future together and binds each output to its name
/// once all are done. The names are the caller's (`let a = ..` in the caller's scope).
#[macro_export]
macro_rules! concurrently {
    ($($name:ident = $fut:expr),+ $(,)?) => {
        $( let mut $name = $crate::concurrent::MaybeDone::new($fut); )+
        let ($($name,)+) = ::std::future::poll_fn(|cx| {
            let mut all = true;
            $( all &= $name.poll_done(cx); )+
            if !all {
                return ::std::task::Poll::Pending;
            }
            // every future is done, so every output is there, taken once
            match ($( $name.take_output(), )+) {
                ($( Some($name), )+) => ::std::task::Poll::Ready(($($name,)+)),
                _ => ::std::task::Poll::Pending,
            }
        })
        .await;
    };
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::task::Poll;

    /// A future that is pending for `polls` polls (waking itself), recording when it is first polled and when it
    /// finishes, in one shared log.
    fn step(
        name: &'static str,
        polls: u32,
        log: Rc<RefCell<Vec<String>>>,
        out: u32,
    ) -> impl std::future::Future<Output = u32> {
        let mut left = polls;
        let mut started = false;
        std::future::poll_fn(move |cx| {
            if !started {
                started = true;
                log.borrow_mut().push(format!("start {name}"));
            }
            if left == 0 {
                log.borrow_mut().push(format!("done {name}"));
                Poll::Ready(out)
            } else {
                left -= 1;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
    }

    /// bsv-low #838: every future of the join is STARTED before any of them finishes (the reads are issued
    /// together, so their waits overlap), each output is bound to its own name whatever order they finish in, and
    /// a done future is never polled again.
    #[tokio::test(flavor = "current_thread")]
    async fn fix838_concurrently_issues_every_future_before_any_finishes() {
        let log = Rc::new(RefCell::new(Vec::new()));
        concurrently! {
            slow = step("slow", 5, log.clone(), 1),
            quick = step("quick", 1, log.clone(), 2),
            middle = step("middle", 2, log.clone(), 3),
        }
        assert_eq!((slow, quick, middle), (1, 2, 3));
        let log = log.borrow().clone();
        let first_done = log.iter().position(|l| l.starts_with("done")).unwrap();
        let last_start = log.iter().rposition(|l| l.starts_with("start")).unwrap();
        assert!(
            last_start < first_done,
            "every future starts before the first finishes: {log:?}"
        );
        assert_eq!(
            log.iter().filter(|l| l.starts_with("done")).count(),
            3,
            "each finishes once, never polled after it is done: {log:?}"
        );
        assert_eq!(log[first_done], "done quick");
        assert_eq!(log.last().unwrap(), "done slow");
    }
}
