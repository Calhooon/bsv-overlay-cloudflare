//! bsv-low #936 (READ-936, loop 52's class S4): a write the request does not own.
//!
//! A Worker's request handler is dropped when its client goes away (the invocation's outcome `canceled`); what it
//! was awaiting is dropped with it, between two of its statements. The gated arm awaited its engine write in the
//! request, so a JOIN whose two seats' requests were cancelled under the fleet's D1 queue kept the `tm_pot` coin the
//! write had inserted and lost the lookup services' rows it had not reached (`pot_records`, `pot_beefs`): the refund
//! then found the coin, recorded the JOIN applied and deleted it, and the pot left the index with no eviction
//! record (`fad01028…`, 2026-10-27).
//!
//! [`owned_write`] answers the write's TASK, to be handed to `ctx.wait_until` (which the platform drives on after a
//! cancel), and a [`Handle`] the request awaits for the write's output. The request may drop the handle at any
//! point; the write is never dropped with it. If the handle goes while the write still runs, `cover` is started at
//! once and driven beside the write (the deferred job put at rest: the cover of a task the platform itself cuts);
//! if nobody takes the output, `orphan` runs with it (what the request would have run after its write).

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

struct Slot<T> {
    out: Option<T>,
    taken: bool,
    gone: bool,
    request: Option<Waker>,
    task: Option<Waker>,
}

/// The request's side of an [`owned_write`]: Ready with the write's output. Dropping it never drops the write.
pub struct Handle<T> {
    slot: Rc<RefCell<Slot<T>>>,
}

impl<T> Future for Handle<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut s = self.slot.borrow_mut();
        if let Some(v) = s.out.take() {
            s.taken = true;
            if let Some(w) = s.task.take() {
                w.wake();
            }
            return Poll::Ready(v);
        }
        s.request = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl<T> Drop for Handle<T> {
    fn drop(&mut self) {
        let mut s = self.slot.borrow_mut();
        if !s.taken {
            s.gone = true;
            if let Some(w) = s.task.take() {
                w.wake();
            }
        }
    }
}

/// The write's task (for `ctx.wait_until`) and the request's [`Handle`]. See the module's words.
pub fn owned_write<W, C, CF, G, A>(
    work: W,
    cover: C,
    orphan: G,
) -> (impl Future<Output = ()> + 'static, Handle<W::Output>)
where
    W: Future + 'static,
    W::Output: 'static,
    C: FnOnce() -> CF + 'static,
    CF: Future<Output = ()> + 'static,
    G: FnOnce(W::Output) -> A + 'static,
    A: Future<Output = ()> + 'static,
{
    let slot = Rc::new(RefCell::new(Slot {
        out: None,
        taken: false,
        gone: false,
        request: None,
        task: None,
    }));
    let handle = Handle { slot: slot.clone() };
    let task = async move {
        let mut work = std::pin::pin!(work);
        let mut cover = Some(cover);
        let mut covering: Option<Pin<Box<CF>>> = None;
        let out = std::future::poll_fn(|cx| {
            if let Poll::Ready(v) = work.as_mut().poll(cx) {
                return Poll::Ready(v);
            }
            let gone = {
                let mut s = slot.borrow_mut();
                s.task = Some(cx.waker().clone());
                s.gone
            };
            if gone {
                if let Some(c) = cover.take() {
                    covering = Some(Box::pin(c()));
                }
            }
            if let Some(f) = covering.as_mut() {
                if f.as_mut().poll(cx).is_ready() {
                    covering = None;
                    // `cover` stays taken: it runs once
                }
            }
            Poll::Pending
        })
        .await;
        let left = {
            let mut s = slot.borrow_mut();
            if s.gone {
                Some(out)
            } else {
                s.out = Some(out);
                if let Some(w) = s.request.take() {
                    w.wake();
                }
                None
            }
        };
        let left = match left {
            Some(out) => Some(out),
            None => {
                // handed over: wait for the request to take it, or to go without it
                std::future::poll_fn(|cx| {
                    let mut s = slot.borrow_mut();
                    if s.taken || s.gone {
                        Poll::Ready(())
                    } else {
                        s.task = Some(cx.waker().clone());
                        Poll::Pending
                    }
                })
                .await;
                slot.borrow_mut().out.take()
            }
        };
        if let Some(out) = left {
            orphan(out).await;
        }
        if let Some(f) = covering {
            f.await;
        }
    };
    (task, handle)
}

#[cfg(test)]
mod tests {
    //! Driven natively over a virtual clock (the platform's `wait_until` is a driver that keeps polling the task;
    //! the request is a future the platform drops on a cancel).
    use super::*;
    use std::cell::Cell;

    type Log = Rc<RefCell<Vec<&'static str>>>;

    /// A two-statement write: the coin's insert, then (from `then_ms` on the clock) the lookup services' rows.
    async fn two_statement_write(clock: Rc<Cell<u64>>, then_ms: u64, log: Log) -> &'static str {
        log.borrow_mut().push("insert_output");
        std::future::poll_fn(|_| {
            if clock.get() >= then_ms {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        log.borrow_mut().push("output_admitted_by_topic");
        "the write's report"
    }

    fn poll_once<F: Future + ?Sized>(f: Pin<&mut F>) -> Poll<F::Output> {
        let mut cx = Context::from_waker(Waker::noop());
        f.poll(&mut cx)
    }

    /// The class, as eaaee0ec ran it: the request awaits the write itself; the platform drops the request after
    /// the first statement, and the second never runs.
    #[test]
    fn fix936_c_a_write_the_request_owns_is_cut_by_its_cancel() {
        let clock = Rc::new(Cell::new(0));
        let log: Log = Rc::default();
        let mut request = Box::pin(two_statement_write(clock.clone(), 1_000, log.clone()));
        assert!(poll_once(request.as_mut()).is_pending());
        drop(request); // the client went away
        clock.set(5_000);
        assert_eq!(*log.borrow(), vec!["insert_output"], "the cut write");
    }

    /// The fix: the request drops its handle after the first statement; the task, driven on, runs the second,
    /// starts the cover once at the moment the request went, and hands the output to the orphan path.
    #[test]
    fn fix936_d_a_gone_request_never_cuts_the_owned_write() {
        let clock = Rc::new(Cell::new(0));
        let log: Log = Rc::default();
        let (cover_log, orphan_log) = (log.clone(), log.clone());
        let (task, handle) = owned_write(
            two_statement_write(clock.clone(), 1_000, log.clone()),
            move || async move { cover_log.borrow_mut().push("cover") },
            move |out| async move {
                assert_eq!(out, "the write's report");
                orphan_log.borrow_mut().push("orphan");
            },
        );
        let mut task = Box::pin(task);
        let mut request = Box::pin(handle);
        assert!(poll_once(task.as_mut()).is_pending());
        assert!(poll_once(request.as_mut()).is_pending());
        drop(request); // the client went away
        assert!(poll_once(task.as_mut()).is_pending());
        assert_eq!(
            *log.borrow(),
            vec!["insert_output", "cover"],
            "the cover starts when the request goes"
        );
        clock.set(1_000);
        assert!(poll_once(task.as_mut()).is_ready());
        assert_eq!(
            *log.borrow(),
            vec![
                "insert_output",
                "cover",
                "output_admitted_by_topic",
                "orphan"
            ],
            "the write finished whole, its after-write ran"
        );
    }

    /// The request that waits takes the output: no cover, no orphan.
    #[test]
    fn fix936_e_a_waiting_request_takes_the_output_and_nothing_else_runs() {
        let clock = Rc::new(Cell::new(0));
        let log: Log = Rc::default();
        let (cover_log, orphan_log) = (log.clone(), log.clone());
        let (task, handle) = owned_write(
            two_statement_write(clock.clone(), 1_000, log.clone()),
            move || async move { cover_log.borrow_mut().push("cover") },
            move |_| async move { orphan_log.borrow_mut().push("orphan") },
        );
        let mut task = Box::pin(task);
        let mut request = Box::pin(handle);
        assert!(poll_once(task.as_mut()).is_pending());
        assert!(poll_once(request.as_mut()).is_pending());
        clock.set(1_000);
        assert!(
            poll_once(task.as_mut()).is_pending(),
            "handed over, waiting to be taken"
        );
        assert_eq!(
            poll_once(request.as_mut()),
            Poll::Ready("the write's report")
        );
        assert!(poll_once(task.as_mut()).is_ready());
        assert_eq!(
            *log.borrow(),
            vec!["insert_output", "output_admitted_by_topic"]
        );
    }

    /// The write finished and was handed over, and the request went before taking it: the orphan path runs with
    /// the output; no cover (the write was whole when the request went).
    #[test]
    fn fix936_f_an_output_never_taken_runs_the_orphan_path() {
        let clock = Rc::new(Cell::new(1_000));
        let log: Log = Rc::default();
        let (cover_log, orphan_log) = (log.clone(), log.clone());
        let (task, handle) = owned_write(
            two_statement_write(clock.clone(), 1_000, log.clone()),
            move || async move { cover_log.borrow_mut().push("cover") },
            move |_| async move { orphan_log.borrow_mut().push("orphan") },
        );
        let mut task = Box::pin(task);
        assert!(poll_once(task.as_mut()).is_pending(), "handed over");
        drop(handle);
        assert!(poll_once(task.as_mut()).is_ready());
        assert_eq!(
            *log.borrow(),
            vec!["insert_output", "output_admitted_by_topic", "orphan"]
        );
    }
}
