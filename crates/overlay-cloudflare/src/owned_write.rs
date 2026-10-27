//! bsv-low #936 (READ-936, loop 52's class S4): a write the request does not own.
//!
//! A client that goes away cancels its Worker request (the invocation's outcome `canceled`). The gated arm awaited
//! its engine write in the request, so a JOIN whose two seats' requests were cancelled under the fleet's D1 queue
//! kept the `tm_pot` coin the write had inserted and lost the lookup services' rows it had not reached
//! (`pot_records`, `pot_beefs`): the refund then found the coin, recorded the JOIN applied and deleted it, and the
//! pot left the index with no eviction record (`fad01028…`, 2026-10-27).
//!
//! [`owned_write`] answers the write's TASK, to be handed to `ctx.wait_until` (which the platform drives on after a
//! cancel, to its cap about 30 s later), and a [`Handle`] the request awaits for the write's output.
//!
//! The FIX-936 lens's fold (`docs/audit/FIX-936-lens-2026-10-27.md` in bsv-low): on wasm32 a cancelled request's
//! future is NEVER DROPPED (the fetch export is a `future_to_promise` task, dropped only when it completes): it is
//! either never polled again (s1) or resumed by the task's own wake (s2), whose continuation does not run its
//! statements. So nothing here waits on, or learns from, the request's Rust future:
//! - the task runs `after` ITSELF on every write, whether or not the request still waits (the write-side guard,
//!   S2, the notes: each idempotent, so the request's own copy beside it costs reads, never a double effect);
//! - the request's going is learnt from `gone`, a future of the request's `AbortSignal` ([`RequestSignal`], under
//!   the `enable_request_signal` compatibility flag): it fires while the task runs, `cover` (the deferred job put
//!   at rest) is started once and driven beside the write, the cover of a task the platform itself cuts;
//! - the task never waits on the request: it hands the output over (if a request waits, it wakes it), runs `after`
//!   and ends.

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

pub use crate::ef_deferred::AfterAnswer;

struct Slot<T> {
    out: Option<T>,
    past_deadline: bool,
    request: Option<Waker>,
}

/// The request's side of an [`owned_write`]: Ready with the write's output. Dropping it, or never polling it again,
/// changes nothing the task does.
pub struct Handle<T> {
    slot: Rc<RefCell<Slot<T>>>,
}

impl<T> Handle<T> {
    /// bsv-low #838: the request stopped waiting at its answer deadline (it answers the 202); the task's after-write
    /// names that cause.
    pub fn past_deadline(&self) {
        self.slot.borrow_mut().past_deadline = true;
    }
}

impl<T> Future for Handle<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut s = self.slot.borrow_mut();
        if let Some(v) = s.out.take() {
            return Poll::Ready(v);
        }
        s.request = Some(cx.waker().clone());
        Poll::Pending
    }
}

/// The request's `AbortSignal` (`Request.signal`), read where the request is; a run with no request (a deferred
/// job's, a native pin's) holds none and is never gone.
#[derive(Clone, Default)]
// natively no request carries a signal: the field is read on wasm32 only
#[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
pub struct RequestSignal(Option<worker::web_sys::AbortSignal>);

impl RequestSignal {
    /// No request: never gone.
    pub fn none() -> Self {
        Self(None)
    }

    /// The request's signal, read through `Reflect` so a runtime without it (the flag unset) is no signal, never a
    /// throw through the wasm frame.
    #[cfg(target_arch = "wasm32")]
    pub fn of(req: &worker::Request) -> Self {
        use worker::wasm_bindgen::JsCast;
        let inner: &worker::wasm_bindgen::JsValue = req.inner().as_ref();
        let signal = worker::js_sys::Reflect::get(inner, &"signal".into())
            .ok()
            .filter(|v| v.is_object())
            .and_then(|v| v.dyn_into::<worker::web_sys::AbortSignal>().ok());
        Self(signal)
    }

    /// Native: no platform request, no signal.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn of(_req: &worker::Request) -> Self {
        Self(None)
    }

    /// Ready once the request is aborted (at once if it already is); pending for ever with no signal.
    pub fn gone(self) -> Pin<Box<dyn Future<Output = ()>>> {
        #[cfg(target_arch = "wasm32")]
        if let Some(signal) = self.0 {
            if signal.aborted() {
                return Box::pin(std::future::ready(()));
            }
            let fired = worker::js_sys::Promise::new(&mut |resolve, _reject| {
                signal.set_onabort(Some(&resolve));
            });
            return Box::pin(async move {
                let _ = worker::wasm_bindgen_futures::JsFuture::from(fired).await;
            });
        }
        let _ = self;
        Box::pin(std::future::pending())
    }
}

enum Stage<A> {
    Writing,
    After(Pin<Box<A>>),
    Done,
}

/// The write's task (for `ctx.wait_until`) and the request's [`Handle`]. `gone` fires when the request is aborted;
/// `cover` is started once if it fires while the task runs; `after` is called with the output and the cause (the
/// request past its deadline, else gone, else answered in the request) when the write ends, BEFORE the output is
/// handed over, and its future is run by the task. See the module's words.
pub fn owned_write<W, GF, C, CF, G, A>(
    work: W,
    gone: GF,
    cover: C,
    after: G,
) -> (impl Future<Output = ()> + 'static, Handle<W::Output>)
where
    W: Future + 'static,
    W::Output: 'static,
    GF: Future<Output = ()> + 'static,
    C: FnOnce() -> CF + 'static,
    CF: Future<Output = ()> + 'static,
    G: FnOnce(&W::Output, AfterAnswer) -> A + 'static,
    A: Future<Output = ()> + 'static,
{
    let slot = Rc::new(RefCell::new(Slot {
        out: None,
        past_deadline: false,
        request: None,
    }));
    let handle = Handle { slot: slot.clone() };
    let task = async move {
        let mut work = std::pin::pin!(work);
        let mut gone = std::pin::pin!(gone);
        let mut is_gone = false;
        let mut cover = Some(cover);
        let mut covering: Option<Pin<Box<CF>>> = None;
        let mut after = Some(after);
        let mut stage: Stage<A> = Stage::Writing;
        std::future::poll_fn(|cx| {
            if !is_gone && gone.as_mut().poll(cx).is_ready() {
                is_gone = true;
                if !matches!(stage, Stage::Done) {
                    if let Some(c) = cover.take() {
                        covering = Some(Box::pin(c()));
                    }
                }
            }
            if let Some(f) = covering.as_mut() {
                if f.as_mut().poll(cx).is_ready() {
                    covering = None; // `cover` stays taken: it runs once
                }
            }
            if matches!(stage, Stage::Writing) {
                if let Poll::Ready(out) = work.as_mut().poll(cx) {
                    let mut s = slot.borrow_mut();
                    let cause = if s.past_deadline {
                        AfterAnswer::PastDeadline
                    } else if is_gone {
                        AfterAnswer::RequestGone
                    } else {
                        AfterAnswer::InRequest
                    };
                    let run = after.take().map(|g| Box::pin(g(&out, cause)));
                    // handed over, never waited on: a request that waits is woken, one that does not is not asked
                    s.out = Some(out);
                    if let Some(w) = s.request.take() {
                        w.wake();
                    }
                    drop(s);
                    stage = match run {
                        Some(run) => Stage::After(run),
                        None => Stage::Done,
                    };
                }
            }
            if let Stage::After(run) = &mut stage {
                if run.as_mut().poll(cx).is_ready() {
                    stage = Stage::Done;
                }
            }
            if matches!(stage, Stage::Done) && covering.is_none() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
    };
    (task, handle)
}

#[cfg(test)]
mod tests {
    //! Driven natively over a virtual clock. The platform's `wait_until` is a driver that keeps polling the task; a
    //! cancelled request is NEVER dropped (the FIX-936 lens's HIGH-1): it is never polled again (s1) or resumed by
    //! the task's wake (s2), and its continuation's statements do not run. The request's going is its signal, here
    //! a flag the test raises.
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

    /// The request's AbortSignal: Ready once the test raises it.
    fn signal(raised: Rc<Cell<bool>>) -> impl Future<Output = ()> {
        std::future::poll_fn(move |_| {
            if raised.get() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
    }

    fn poll_once<F: Future + ?Sized>(f: Pin<&mut F>) -> Poll<F::Output> {
        let mut cx = Context::from_waker(Waker::noop());
        f.poll(&mut cx)
    }

    /// The model of `routes::finish_past_deadline`: the write-side guard, S2, the notes, and the counter of a write
    /// that outlived its request.
    fn after_write(
        log: Log,
        causes: Rc<RefCell<Vec<AfterAnswer>>>,
    ) -> impl FnOnce(&&'static str, AfterAnswer) -> Pin<Box<dyn Future<Output = ()>>> {
        move |out, cause| {
            assert_eq!(*out, "the write's report");
            causes.borrow_mut().push(cause);
            Box::pin(async move {
                log.borrow_mut().push("guard");
                log.borrow_mut().push("s2");
                if cause == AfterAnswer::RequestGone {
                    log.borrow_mut().push("counter");
                }
            })
        }
    }

    /// The class, as eaaee0ec ran it: the request awaits the write itself; the client goes away after the first
    /// statement and the request is never polled again (the platform's cancel): the second never runs.
    #[test]
    fn fix936_c_a_write_the_request_owns_is_cut_by_its_cancel() {
        let clock = Rc::new(Cell::new(0));
        let log: Log = Rc::default();
        let mut request = Box::pin(two_statement_write(clock.clone(), 1_000, log.clone()));
        assert!(poll_once(request.as_mut()).is_pending());
        // the client went away: never polled again
        clock.set(5_000);
        assert_eq!(*log.borrow(), vec!["insert_output"], "the cut write");
        std::mem::forget(request);
    }

    /// The fold, the lens's two shapes of a cancel (s1: the request never polled again; s2: resumed by the task's
    /// wake, taking the output, its own after-write not running): the abort fires mid-write, the cover starts at
    /// once, the write finishes whole and the TASK runs the guard, S2 and the counter, then ends (no wait on the
    /// request). RED on 8cf04f54 in its API (`orphan` for the after-write, `Handle::drop` for the going): "no
    /// cover, no orphan", the task Pending for ever in s1.
    #[test]
    fn fix936_g_a_cancelled_request_never_dropped_has_its_cover_and_its_after_write() {
        for resumed in [false, true] {
            let clock = Rc::new(Cell::new(0));
            let raised = Rc::new(Cell::new(false));
            let log: Log = Rc::default();
            let causes: Rc<RefCell<Vec<AfterAnswer>>> = Rc::default();
            let cover_log = log.clone();
            let (task, handle) = owned_write(
                two_statement_write(clock.clone(), 1_000, log.clone()),
                signal(raised.clone()),
                move || async move { cover_log.borrow_mut().push("cover") },
                after_write(log.clone(), causes.clone()),
            );
            let mut task = Box::pin(task);
            let mut request = Box::pin(handle);
            assert!(poll_once(task.as_mut()).is_pending());
            assert!(poll_once(request.as_mut()).is_pending());
            raised.set(true); // the client went away: the request is NOT dropped
            assert!(poll_once(task.as_mut()).is_pending());
            assert_eq!(
                *log.borrow(),
                vec!["insert_output", "cover"],
                "resumed={resumed}: the cover on the abort"
            );
            clock.set(1_000);
            let ended = poll_once(task.as_mut());
            if resumed {
                assert_eq!(
                    poll_once(request.as_mut()),
                    Poll::Ready("the write's report")
                );
            }
            assert!(
                ended.is_ready(),
                "resumed={resumed}: the task never waits on the request"
            );
            assert_eq!(
                *log.borrow(),
                vec![
                    "insert_output",
                    "cover",
                    "output_admitted_by_topic",
                    "guard",
                    "s2",
                    "counter"
                ],
                "resumed={resumed}: the write whole, then the task's own after-write"
            );
            assert_eq!(*causes.borrow(), vec![AfterAnswer::RequestGone]);
            std::mem::forget(request);
        }
    }

    /// The request that waits takes the output, and the task runs its after-write all the same (the request's own
    /// copy may be lost to a cancel it cannot see); no cover, no counter.
    #[test]
    fn fix936_e_a_waiting_request_takes_the_output_and_the_task_still_guards() {
        let clock = Rc::new(Cell::new(0));
        let log: Log = Rc::default();
        let causes: Rc<RefCell<Vec<AfterAnswer>>> = Rc::default();
        let cover_log = log.clone();
        let (task, handle) = owned_write(
            two_statement_write(clock.clone(), 1_000, log.clone()),
            signal(Rc::new(Cell::new(false))),
            move || async move { cover_log.borrow_mut().push("cover") },
            after_write(log.clone(), causes.clone()),
        );
        let mut task = Box::pin(task);
        let mut request = Box::pin(handle);
        assert!(poll_once(task.as_mut()).is_pending());
        assert!(poll_once(request.as_mut()).is_pending());
        clock.set(1_000);
        assert!(poll_once(task.as_mut()).is_ready(), "handed over and done");
        assert_eq!(
            poll_once(request.as_mut()),
            Poll::Ready("the write's report")
        );
        assert_eq!(
            *log.borrow(),
            vec!["insert_output", "output_admitted_by_topic", "guard", "s2"]
        );
        assert_eq!(*causes.borrow(), vec![AfterAnswer::InRequest]);
    }

    /// #838: the request stopped waiting at its deadline (its 202); the task's after-write names `past-deadline`
    /// and the request's handle dropped changes nothing (no cover without an abort).
    #[test]
    fn fix936_f_past_the_deadline_the_after_write_names_its_cause() {
        let clock = Rc::new(Cell::new(0));
        let log: Log = Rc::default();
        let causes: Rc<RefCell<Vec<AfterAnswer>>> = Rc::default();
        let cover_log = log.clone();
        let (task, handle) = owned_write(
            two_statement_write(clock.clone(), 1_000, log.clone()),
            signal(Rc::new(Cell::new(false))),
            move || async move { cover_log.borrow_mut().push("cover") },
            after_write(log.clone(), causes.clone()),
        );
        let mut task = Box::pin(task);
        assert!(poll_once(task.as_mut()).is_pending());
        handle.past_deadline();
        drop(handle);
        clock.set(1_000);
        assert!(poll_once(task.as_mut()).is_ready());
        assert_eq!(
            *log.borrow(),
            vec!["insert_output", "output_admitted_by_topic", "guard", "s2"]
        );
        assert_eq!(*causes.borrow(), vec![AfterAnswer::PastDeadline]);
    }

    /// An abort after the write landed, while the after-write still runs, starts the cover too (the platform may
    /// cut the after-write); an abort after the task ended does nothing (nothing polls it).
    #[test]
    fn fix936_d_the_cover_runs_once_and_only_while_the_task_runs() {
        let clock = Rc::new(Cell::new(1_000));
        let raised = Rc::new(Cell::new(false));
        let log: Log = Rc::default();
        let cover_log = log.clone();
        let slow = Rc::new(Cell::new(false));
        let (after_log, after_slow) = (log.clone(), slow.clone());
        let (task, _handle) = owned_write(
            two_statement_write(clock.clone(), 1_000, log.clone()),
            signal(raised.clone()),
            move || async move { cover_log.borrow_mut().push("cover") },
            move |_: &&'static str, _| {
                Box::pin(async move {
                    after_log.borrow_mut().push("guard");
                    std::future::poll_fn(|_| {
                        if after_slow.get() {
                            Poll::Ready(())
                        } else {
                            Poll::Pending
                        }
                    })
                    .await;
                    after_log.borrow_mut().push("s2");
                }) as Pin<Box<dyn Future<Output = ()>>>
            },
        );
        let mut task = Box::pin(task);
        assert!(poll_once(task.as_mut()).is_pending(), "in its after-write");
        raised.set(true);
        assert!(poll_once(task.as_mut()).is_pending());
        assert!(poll_once(task.as_mut()).is_pending());
        slow.set(true);
        assert!(poll_once(task.as_mut()).is_ready());
        assert_eq!(
            *log.borrow(),
            vec![
                "insert_output",
                "output_admitted_by_topic",
                "guard",
                "cover",
                "s2"
            ],
            "the cover started once, beside the after-write"
        );
    }
}
