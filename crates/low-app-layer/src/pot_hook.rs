//! E649 L4 (the E649 north-star plan, lane E649-HOOK, 2026-10-13): the write hooks `/internal/pot-changed` and
//! `/internal/hop-changed` are WRITES. Beta measured the pot hook at 15.3 s p50 and 30 s p90 (Observability,
//! 12:05:30 to 12:06:30Z): step 0 awaited one relay `/broadcast` per outpoint, whose answer waits on the relay's
//! sequential fan-out (11 to 15 s p50 under the fleet), then a full `/results` gather per attributed seat.
//!
//! The rule here: the answer path is D1 alone, in batches; no relay call and no read-model walk is awaited before
//! the answer. What the hook decides is [`apply_pot_changed`] / [`apply_hop_changed`], over a [`HookStore`] port
//! (the worker's is `routes::D1HookStore`; a queue consumer is the next caller, L3's seam). The publish is
//! [`announce`], run DETACHED (`ctx.wait_until`) under one wall ([`ANNOUNCE_WALL_MS`]) and counted. The owed
//! recompute stays where #469 and #648 put it: after the answer, coalesced and shed per identity.
//!
//! The seat's `pot` event no longer carries a gathered `/results` entry. No reader reads it: the client's parser
//! (`app/src/lib/lowEvents.ts` `parseLowEvent`) requires `entry` to be an OBJECT, and every consumer
//! (`engine/orchestrator.ts` `potEvent`, `pages/History.tsx`, `components/UnfinishedBusiness.tsx`,
//! `hooks/useIdentityEvents.ts`) reads the kind and the outpoint and re-reads the served rows. So the event
//! carries `entry: {}` ([`carrier_entry`]) and the seat's own read serves the entry.
use serde_json::{json, Value};
use std::cell::RefCell;
use std::future::Future;

/// The pots-room announce and the seats' events of one body run under ONE wall, under the platform's 30 s
/// `waitUntil` window with room for the counter flush after it. The relay call has no timeout of its own today;
/// this is it.
pub const ANNOUNCE_WALL_MS: u64 = 20_000;

/// Relay calls of the detached announce that answered 2xx.
pub const COUNTER_ANNOUNCE_SENT: &str = "pot_changed_announce_sent_total";
/// Relay calls not made: the deploy has no `RELAY_URL` / `BROADCAST_TOKEN` (/ `SERVER_PRIVATE_KEY` for a seat).
pub const COUNTER_ANNOUNCE_SKIPPED: &str = "pot_changed_announce_skipped_total";
/// Relay calls that failed (a transport fault, a non-2xx) or had not answered at the wall.
pub const COUNTER_ANNOUNCE_FAILED: &str = "pot_changed_announce_failed_total";

/// What one relay call came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    Sent,
    NotConfigured,
    Failed,
}

/// What the store read for one outpoint of a body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutpointRead {
    /// The decoded-pots read faulted (the whole batch, or this outpoint's statement).
    Failed(String),
    /// No decoded params (an evicted pot, a released hop): the identities the seats' own markers name.
    NoParams { marker_named: Vec<String> },
    /// Decoded: the seats `attribute_seats` proved (lowercase), and the marker-named identities.
    Decoded {
        seats: Vec<String>,
        marker_named: Vec<String>,
    },
}

/// The D1 side of the hooks. Each method is ONE round trip of batched statements (the worker's runs
/// `counted_batch`), never a statement per outpoint or per identity.
#[allow(async_fn_in_trait)]
pub trait HookStore {
    /// Every outpoint's decoded row and marker arms (one batch), then the decoded pots' seat markers (one batch).
    /// The answer is in the order of `outpoints`.
    async fn read_outpoints(&self, outpoints: &[(String, u32)]) -> Vec<OutpointRead>;
    /// Every stale mark of the body, in ONE batch. `Err` is logged by the caller; the recompute still runs.
    async fn mark_stale(&self, marks: &[(String, &'static str)]) -> Result<(), String>;
}

/// The relay side of the detached announce.
#[allow(async_fn_in_trait)]
pub trait RelayPort {
    async fn broadcast(&self, room: &'static str, body: Value) -> Delivery;
    async fn push(&self, recipient: &str, body: Value) -> Delivery;
}

/// What the detached work of one body is: the outpoints to announce in the pots room, and the seats to tell.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AnnouncePlan {
    pub outpoints: Vec<(String, u32)>,
    /// (identity, txid, vout): one `pot` event into the seat's `low_events` box.
    pub seat_events: Vec<(String, String, u32)>,
    pub at_ms: u64,
}

impl AnnouncePlan {
    pub fn calls(&self) -> u64 {
        (self.outpoints.len() + self.seat_events.len()) as u64
    }
}

/// What [`apply_pot_changed`] decided: the answer's lists, the identities whose owed rows the detached recompute
/// re-derives, and the detached announce.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PotChangedOutcome {
    pub filed: Vec<Value>,
    pub skipped: Vec<Value>,
    pub owed_identities: Vec<String>,
    pub stale_marks: Vec<(String, &'static str)>,
    pub stale_fault: Option<String>,
    pub plan: AnnouncePlan,
}

fn push_unique(v: &mut Vec<String>, id: &str) {
    if !v.iter().any(|x| x == id) {
        v.push(id.to_string());
    }
}

/// The pot hook's whole decision for one parsed body: one read through the store, one stale-mark batch, and the
/// plan of what runs after the answer. No relay call, no read-model walk, no owed recompute.
///
/// Per outpoint, the branches of the handler before L4: a read fault is skipped; with no decoded params, or with
/// decoded params and no attributed seat, the marker-named identities are marked stale and queued for the
/// recompute (fleet loop 11 and 15); an attributed pot queues a `pot` event for each seat and its recompute.
pub async fn apply_pot_changed<S: HookStore>(
    store: &S,
    outpoints: &[(String, u32)],
    at_ms: u64,
) -> PotChangedOutcome {
    let mut out = PotChangedOutcome {
        plan: AnnouncePlan {
            outpoints: outpoints.to_vec(),
            seat_events: Vec::new(),
            at_ms,
        },
        ..PotChangedOutcome::default()
    };
    let reads = store.read_outpoints(outpoints).await;
    for ((txid, vout), read) in outpoints.iter().zip(reads) {
        let (txid, vout) = (txid.as_str(), *vout);
        match read {
            OutpointRead::Failed(e) => {
                out.skipped.push(json!({ "txid": txid, "vout": vout, "why": format!("[pot-changed] decoded-pots read failed for {txid}:{vout}: {e}") }));
            }
            OutpointRead::NoParams { marker_named } => {
                for id in &marker_named {
                    out.stale_marks
                        .push((id.clone(), "pot-changed (by marker)"));
                    push_unique(&mut out.owed_identities, id);
                }
                out.skipped.push(json!({ "txid": txid, "vout": vout, "why": format!("[pot-changed] {txid}:{vout} has no decoded params yet, nothing to file ({} seat(s) attributed by marker)", marker_named.len()) }));
            }
            OutpointRead::Decoded {
                seats,
                marker_named,
            } if seats.is_empty() => {
                for id in &marker_named {
                    out.stale_marks
                        .push((id.clone(), "pot-changed (unattributed, by marker)"));
                    push_unique(&mut out.owed_identities, id);
                }
                out.skipped.push(json!({ "txid": txid, "vout": vout, "why": format!("[pot-changed] {txid}:{vout} has no attributed seats yet, nothing to file ({} seat(s) marked stale by marker)", marker_named.len()) }));
            }
            OutpointRead::Decoded { seats, .. } => {
                for id in seats {
                    out.plan
                        .seat_events
                        .push((id.clone(), txid.to_string(), vout));
                    push_unique(&mut out.owed_identities, &id);
                    out.filed
                        .push(json!({ "txid": txid, "vout": vout, "identity": id }));
                }
            }
        }
    }
    if !out.stale_marks.is_empty() {
        if let Err(e) = store.mark_stale(&out.stale_marks).await {
            out.stale_fault = Some(e);
        }
    }
    out
}

/// The hop hook's decision: every parsed identity marked stale in ONE batch; the recompute runs after the answer.
pub async fn apply_hop_changed<S: HookStore>(
    store: &S,
    identities: &[String],
) -> Result<(), String> {
    let marks: Vec<(String, &'static str)> = identities
        .iter()
        .map(|id| (id.clone(), "hop-changed"))
        .collect();
    if marks.is_empty() {
        return Ok(());
    }
    store.mark_stale(&marks).await
}

/// The `entry` of a seat's `pot` event since L4: an empty object (the client's parser requires an object; no
/// reader reads a field of it, the seat re-reads its served rows).
pub fn carrier_entry() -> Value {
    json!({})
}

/// What the detached announce of one body came to, in relay calls.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AnnounceTally {
    pub sent: u64,
    pub skipped: u64,
    pub failed: u64,
    /// The wall fell before every call answered (the unanswered ones are in `failed`).
    pub walled: bool,
}

impl AnnounceTally {
    fn note(&mut self, d: Delivery) {
        match d {
            Delivery::Sent => self.sent += 1,
            Delivery::NotConfigured => self.skipped += 1,
            Delivery::Failed => self.failed += 1,
        }
    }

    /// The counter rows to add (nonzero only), in the file's counter style (`ops_counters`).
    pub fn counters(&self) -> Vec<(String, u64)> {
        [
            (COUNTER_ANNOUNCE_SENT, self.sent),
            (COUNTER_ANNOUNCE_SKIPPED, self.skipped),
            (COUNTER_ANNOUNCE_FAILED, self.failed),
        ]
        .into_iter()
        .filter(|(_, n)| *n > 0)
        .map(|(k, n)| (k.to_string(), n))
        .collect()
    }
}

/// The detached announce of one body: every changed outpoint into the pots room (the deployed client matches
/// `potOutpoint` alone, so one body per outpoint; the calls of one hook run together, not one after another),
/// and one `pot` event into each attributed seat's box, all raced against ONE `wall`. A call that has not answered
/// at the wall is dropped and counted failed.
pub async fn announce<R: RelayPort, W: Future<Output = ()>>(
    relay: &R,
    plan: &AnnouncePlan,
    wall: W,
) -> AnnounceTally {
    use futures_util::future::{join_all, select, Either};
    let tally = RefCell::new(AnnounceTally::default());
    let pots = plan.outpoints.iter().map(|(txid, vout)| {
        let body = crate::internal_events::pot_changed_event_body(txid, *vout, plan.at_ms);
        let tally = &tally;
        async move {
            let d = relay
                .broadcast(crate::internal_events::POTS_ROOM, body)
                .await;
            tally.borrow_mut().note(d);
        }
    });
    let seats = plan.seat_events.iter().map(|(id, txid, vout)| {
        let body = crate::internal_events::pot_event_body(txid, *vout, carrier_entry(), plan.at_ms);
        let tally = &tally;
        async move {
            let d = relay.push(id, body).await;
            tally.borrow_mut().note(d);
        }
    });
    let all = async {
        futures_util::future::join(join_all(pots), join_all(seats)).await;
    };
    futures_util::pin_mut!(all, wall);
    let walled = matches!(select(all, wall).await, Either::Right(_));
    let mut t = *tally.borrow();
    if walled {
        let answered = t.sent + t.skipped + t.failed;
        t.failed += plan.calls().saturating_sub(answered);
        t.walled = true;
    }
    t
}

/// The worker's relay: the `RELAY` binding through `internal_events`.
pub struct WorkerRelay<'a> {
    pub env: &'a worker::Env,
}

impl RelayPort for WorkerRelay<'_> {
    async fn broadcast(&self, room: &'static str, body: Value) -> Delivery {
        crate::internal_events::push_broadcast(self.env, room, body).await
    }
    async fn push(&self, recipient: &str, body: Value) -> Delivery {
        crate::internal_events::first_party_push(self.env, recipient, body).await
    }
}

/// The handler's `wait_until` body: the announce under the wall, its log line and its counters.
pub async fn announce_detached(env: worker::Env, plan: AnnouncePlan) {
    let t0 = worker::Date::now().as_millis();
    let wall = worker::Delay::from(core::time::Duration::from_millis(ANNOUNCE_WALL_MS));
    let t = announce(&WorkerRelay { env: &env }, &plan, wall).await;
    worker::console_log!(
        "[pot-changed] announce: {} outpoint(s), {} seat event(s): sent={} skipped={} failed={}{} in {} ms",
        plan.outpoints.len(),
        plan.seat_events.len(),
        t.sent,
        t.skipped,
        t.failed,
        if t.walled { " (WALL)" } else { "" },
        worker::Date::now().as_millis().saturating_sub(t0)
    );
    let counters = t.counters();
    if !counters.is_empty() {
        crate::courier::flush(env.d1("OVERLAY_DB").ok(), counters).await;
    }
}

/// The `Server-Timing` segment of a hook's answer.
pub fn hook_timing(ms: u64) -> String {
    format!("hook;dur={ms}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;
    use std::time::{Duration, Instant};

    fn code_only(s: &str) -> String {
        s.lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n")
    }
    fn squash(s: &str) -> String {
        s.split_whitespace().collect()
    }
    /// One item of `routes.rs`, squashed: from `open` to the next `pub(crate)` async fn or struct.
    fn routes_item(open: &str) -> String {
        let routes = squash(&code_only(include_str!("routes.rs")));
        let start = routes.find(&squash(open)).expect("the item");
        let rest = &routes[start + 1..];
        let end = ["pub(crate) async fn", "pub(crate) struct"]
            .iter()
            .filter_map(|m| rest.find(&squash(m)))
            .min()
            .unwrap_or(rest.len());
        routes[start..start + 1 + end].to_string()
    }

    const A: &str = "02a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1";
    const B: &str = "02b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2";
    const C: &str = "03c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3";

    fn op(i: u32) -> (String, u32) {
        (format!("{i:064x}"), i % 3)
    }

    /// The D1 port as a model: its answers set per outpoint, its round trips counted.
    #[derive(Default)]
    struct ModelStore {
        reads: Vec<OutpointRead>,
        read_calls: Cell<u32>,
        mark_calls: Cell<u32>,
        marked: RefCell<Vec<(String, &'static str)>>,
        mark_fault: bool,
    }
    impl HookStore for ModelStore {
        async fn read_outpoints(&self, outpoints: &[(String, u32)]) -> Vec<OutpointRead> {
            self.read_calls.set(self.read_calls.get() + 1);
            assert_eq!(outpoints.len(), self.reads.len());
            self.reads.clone()
        }
        async fn mark_stale(&self, marks: &[(String, &'static str)]) -> Result<(), String> {
            self.mark_calls.set(self.mark_calls.get() + 1);
            self.marked.borrow_mut().extend(marks.iter().cloned());
            if self.mark_fault {
                Err("D1 down".into())
            } else {
                Ok(())
            }
        }
    }

    /// The relay as a fake: every call sleeps `delay`, then answers `answer`; calls started and answered counted.
    struct SlowRelay {
        delay: Duration,
        answer: Delivery,
        started: Rc<Cell<u32>>,
        answered_at: Rc<RefCell<Vec<Instant>>>,
        rooms: Rc<RefCell<Vec<(String, Value)>>>,
    }
    impl SlowRelay {
        fn new(delay: Duration, answer: Delivery) -> Self {
            SlowRelay {
                delay,
                answer,
                started: Rc::default(),
                answered_at: Rc::default(),
                rooms: Rc::default(),
            }
        }
        async fn call(&self, to: String, body: Value) -> Delivery {
            self.started.set(self.started.get() + 1);
            tokio::time::sleep(self.delay).await;
            self.answered_at.borrow_mut().push(Instant::now());
            self.rooms.borrow_mut().push((to, body));
            self.answer
        }
    }
    impl RelayPort for SlowRelay {
        async fn broadcast(&self, room: &'static str, body: Value) -> Delivery {
            self.call(room.to_string(), body).await
        }
        async fn push(&self, recipient: &str, body: Value) -> Delivery {
            self.call(recipient.to_string(), body).await
        }
    }

    fn attributed(n: u32) -> ModelStore {
        ModelStore {
            reads: (0..n)
                .map(|_| OutpointRead::Decoded {
                    seats: vec![A.into(), B.into()],
                    marker_named: vec![],
                })
                .collect(),
            ..ModelStore::default()
        }
    }

    /// What the handler does, in its order, with the worker's `ctx.wait_until` as a `spawn_local`: the seam, then
    /// the detached announce handed off, then the answer. Returns the time to the answer and the detached task.
    async fn serve(
        store: &ModelStore,
        relay: Rc<SlowRelay>,
        outpoints: &[(String, u32)],
        wall: Duration,
    ) -> (
        Duration,
        PotChangedOutcome,
        tokio::task::JoinHandle<AnnounceTally>,
    ) {
        let t0 = Instant::now();
        let outcome = apply_pot_changed(store, outpoints, 1).await;
        let plan = outcome.plan.clone();
        let detached = tokio::task::spawn_local(async move {
            announce(&*relay, &plan, tokio::time::sleep(wall)).await
        });
        (t0.elapsed(), outcome, detached)
    }

    /// (a) THE HANDLER AWAITS NO RELAY CALL BEFORE ITS ANSWER. Source: the pot handler names no relay call and no
    /// gather; it calls the seam. RED on `57751b4`: "the pot-changed handler calls push_broadcast( on its answer
    /// path". Executed: a relay fake that sleeps 40 s; the answer is in hand within 1 s with NO relay call started;
    /// a fake that answers in 50 ms is observed AFTER the answer, every call sent.
    #[tokio::test(flavor = "current_thread")]
    async fn e649_hook_a_the_pot_hook_awaits_no_relay_call_and_no_gather_before_its_answer() {
        let h = routes_item("pub(crate) async fn internal_pot_changed(");
        for banned in [
            "push_broadcast(",
            "first_party_push(",
            "gather_result_entries(",
        ] {
            assert!(
                !h.contains(&squash(banned)),
                "the pot-changed handler calls {banned} on its answer path"
            );
        }
        assert!(
            h.contains(&squash("crate::pot_hook::apply_pot_changed(")),
            "the handler calls the seam"
        );
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let slow = Rc::new(SlowRelay::new(Duration::from_secs(40), Delivery::Sent));
                let started = slow.started.clone();
                let (took, _, detached) =
                    serve(&attributed(1), slow, &[op(1)], Duration::from_millis(200)).await;
                assert!(took < Duration::from_secs(1), "answered in {took:?}");
                assert_eq!(started.get(), 0, "no relay call started before the answer");
                let t = detached.await.unwrap();
                assert!(
                    t.walled,
                    "the 40 s relay ran into the wall, after the answer"
                );

                let quick = Rc::new(SlowRelay::new(Duration::from_millis(50), Delivery::Sent));
                let (answered_at, rooms) = (quick.answered_at.clone(), quick.rooms.clone());
                let t0 = Instant::now();
                let (took, _, detached) =
                    serve(&attributed(1), quick, &[op(1)], Duration::from_secs(5)).await;
                let answer_at = t0 + took;
                assert!(
                    answered_at.borrow().is_empty(),
                    "nothing pushed before the answer"
                );
                let t = detached.await.unwrap();
                assert_eq!((t.sent, t.failed, t.walled), (3, 0, false));
                assert!(
                    answered_at.borrow().iter().all(|at| *at > answer_at),
                    "every push after the answer"
                );
                let rooms = rooms.borrow();
                assert!(rooms
                    .iter()
                    .any(|(to, b)| to == POTS_ROOM_NAME && b["kind"] == "pot-changed"));
                for seat in [A, B] {
                    let (_, b) = rooms
                        .iter()
                        .find(|(to, _)| to == seat)
                        .expect("each seat told");
                    assert_eq!(b["kind"], "pot");
                    assert!(
                        b["entry"].is_object(),
                        "the client's parser needs an object entry"
                    );
                    assert_eq!(b["entry"], json!({}), "no gathered entry rides the push");
                }
            })
            .await;
    }
    const POTS_ROOM_NAME: &str = crate::internal_events::POTS_ROOM;

    /// (b) ONE READ AND ONE WRITE BATCH PER BODY. Source: neither hook handler runs a statement of its own beside
    /// the seam; the D1 store's reads are `counted_batch` round trips (the attribution, then the seat markers) and
    /// its stale marks one. RED on `57751b4`: "internal_pot_changed runs a statement of its own (counted_all())
    /// beside the seam's batch". Executed: a body of 8 mixed outpoints is one read call and one mark call; a hop
    /// body of 8 identities is one mark call.
    #[tokio::test(flavor = "current_thread")]
    async fn e649_hook_b_the_hooks_reach_d1_only_through_the_seams_batches() {
        for name in ["internal_pot_changed", "internal_hop_changed"] {
            let h = routes_item(&format!("pub(crate) async fn {name}("));
            for banned in [
                "counted_all()",
                "counted_run()",
                "counted_first",
                "owed_mark_stale(",
                "owed_identities_by_marker(",
                "counted_batch(",
            ] {
                assert!(
                    !h.contains(&squash(banned)),
                    "{name} runs a statement of its own ({banned}) beside the seam's batch"
                );
            }
        }
        let store = routes_item("impl crate::pot_hook::HookStore for D1HookStore<'_> {");
        assert_eq!(
            store.matches("counted_batch(").count(),
            2,
            "attribution and stale marks: one batch each"
        );
        assert_eq!(
            store.matches("results_seat_markers_batched(").count(),
            1,
            "the seat markers: one batch"
        );
        for banned in [
            "counted_all()",
            "counted_run()",
            "results_seat_markers(self",
        ] {
            assert!(!store.contains(banned), "the store runs {banned}");
        }
        let routes = include_str!("routes.rs");
        let seat = &routes[routes
            .find("async fn results_seat_markers_batched(")
            .expect("the batched seat read")..];
        let seat = &seat[..seat.find("\n}\n").expect("its end")];
        assert_eq!(
            seat.matches("counted_batch(").count(),
            1,
            "the seat markers of a body: one batch"
        );
        assert!(
            !seat.contains("counted_all()"),
            "the batched seat read runs no statement of its own"
        );

        let ids: Vec<String> = (0..8).map(|i| format!("02{i:064x}")).collect();
        let store = ModelStore {
            reads: vec![
                OutpointRead::Failed("x".into()),
                OutpointRead::NoParams {
                    marker_named: vec![A.into(), C.into()],
                },
                OutpointRead::Decoded {
                    seats: vec![],
                    marker_named: vec![C.into()],
                },
                OutpointRead::Decoded {
                    seats: vec![A.into(), B.into()],
                    marker_named: vec![],
                },
                OutpointRead::Decoded {
                    seats: vec![B.into(), C.into()],
                    marker_named: vec![],
                },
                OutpointRead::NoParams {
                    marker_named: vec![],
                },
                OutpointRead::NoParams {
                    marker_named: vec![B.into()],
                },
                OutpointRead::Decoded {
                    seats: vec![A.into()],
                    marker_named: vec![A.into()],
                },
            ],
            ..ModelStore::default()
        };
        let ops: Vec<(String, u32)> = (0..8).map(op).collect();
        let _ = apply_pot_changed(&store, &ops, 1).await;
        assert_eq!(
            (store.read_calls.get(), store.mark_calls.get()),
            (1, 1),
            "one read, one write, per body"
        );
        assert_eq!(
            store.marked.borrow().len(),
            4,
            "every stale mark rode the one batch"
        );
        let hop = ModelStore::default();
        apply_hop_changed(&hop, &ids).await.unwrap();
        assert_eq!((hop.read_calls.get(), hop.mark_calls.get()), (0, 1));
        assert_eq!(hop.marked.borrow().len(), 8);
        let quiet = attributed(2);
        let _ = apply_pot_changed(&quiet, &ops[..2], 1).await;
        assert_eq!(quiet.mark_calls.get(), 0, "nothing to mark: no write");
    }

    /// (c) A BODY OF 8 OUTPOINTS, BOTH SEATS ATTRIBUTED, ANSWERS IN UNDER 1 S WITH THE RELAY 40 S SLOW: 8 pots-room
    /// calls and 16 seat events go to the detached announce, all started together (not one after another), and the
    /// wall ends them.
    #[tokio::test(flavor = "current_thread")]
    async fn e649_hook_c_a_body_of_8_outpoints_answers_under_a_second_with_the_relay_slow() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let slow = Rc::new(SlowRelay::new(Duration::from_secs(40), Delivery::Sent));
                let started = slow.started.clone();
                let ops: Vec<(String, u32)> = (0..8).map(op).collect();
                let (took, outcome, detached) =
                    serve(&attributed(8), slow, &ops, Duration::from_millis(300)).await;
                assert!(took < Duration::from_secs(1), "answered in {took:?}");
                assert_eq!(started.get(), 0);
                assert_eq!(outcome.plan.outpoints, ops, "every outpoint announced");
                assert_eq!(outcome.plan.seat_events.len(), 16);
                assert_eq!(outcome.filed.len(), 16);
                assert_eq!(outcome.owed_identities, vec![A.to_string(), B.to_string()]);
                let t0 = Instant::now();
                let t = detached.await.unwrap();
                assert!(
                    t0.elapsed() < Duration::from_secs(5),
                    "bounded by the wall, not 24 x 40 s"
                );
                assert_eq!(
                    started.get(),
                    24,
                    "every call started inside the wall: together, not in sequence"
                );
                assert_eq!((t.sent, t.failed, t.walled), (0, 24, true));
            })
            .await;
    }

    /// (d) THE DETACHED ANNOUNCE IS COUNTED on success, at the wall, on a failure, and when the relay is not
    /// configured; the counters are the file's `ops_counters` rows, nonzero only.
    #[tokio::test(flavor = "current_thread")]
    async fn e649_hook_d_the_detached_announce_is_counted_on_success_the_wall_and_a_failure() {
        let plan = AnnouncePlan {
            outpoints: vec![op(1), op(2)],
            seat_events: vec![(A.into(), op(1).0, 1)],
            at_ms: 7,
        };
        let wall = || tokio::time::sleep(Duration::from_millis(200));
        let ok = announce(
            &SlowRelay::new(Duration::ZERO, Delivery::Sent),
            &plan,
            wall(),
        )
        .await;
        assert_eq!(
            ok,
            AnnounceTally {
                sent: 3,
                skipped: 0,
                failed: 0,
                walled: false
            }
        );
        assert_eq!(ok.counters(), vec![(COUNTER_ANNOUNCE_SENT.to_string(), 3)]);
        let walled = announce(
            &SlowRelay::new(Duration::from_secs(40), Delivery::Sent),
            &plan,
            wall(),
        )
        .await;
        assert_eq!(
            walled,
            AnnounceTally {
                sent: 0,
                skipped: 0,
                failed: 3,
                walled: true
            }
        );
        assert_eq!(
            walled.counters(),
            vec![(COUNTER_ANNOUNCE_FAILED.to_string(), 3)]
        );
        let failed = announce(
            &SlowRelay::new(Duration::ZERO, Delivery::Failed),
            &plan,
            wall(),
        )
        .await;
        assert_eq!(
            failed,
            AnnounceTally {
                sent: 0,
                skipped: 0,
                failed: 3,
                walled: false
            }
        );
        assert_eq!(
            failed.counters(),
            vec![(COUNTER_ANNOUNCE_FAILED.to_string(), 3)]
        );
        let skipped = announce(
            &SlowRelay::new(Duration::ZERO, Delivery::NotConfigured),
            &plan,
            wall(),
        )
        .await;
        assert_eq!(
            skipped.counters(),
            vec![(COUNTER_ANNOUNCE_SKIPPED.to_string(), 3)]
        );
        assert_eq!(
            [
                COUNTER_ANNOUNCE_SENT,
                COUNTER_ANNOUNCE_SKIPPED,
                COUNTER_ANNOUNCE_FAILED
            ],
            [
                "pot_changed_announce_sent_total",
                "pot_changed_announce_skipped_total",
                "pot_changed_announce_failed_total"
            ]
        );
        const {
            assert!(
                ANNOUNCE_WALL_MS < 30_000,
                "the wall sits under the platform's waitUntil window"
            )
        };
        let src = squash(&code_only(include_str!("pot_hook.rs")));
        let det = &src[src.find("pubasyncfnannounce_detached(").unwrap()..];
        assert!(det.contains(&squash(
            "worker::Delay::from(core::time::Duration::from_millis(ANNOUNCE_WALL_MS))"
        )));
        assert!(
            det.contains(&squash(
                "crate::courier::flush(env.d1(\"OVERLAY_DB\").ok(), counters)"
            )),
            "the counters are flushed"
        );
    }

    /// (e) THE SEAM RUNS WITHOUT A `Request`: `apply_pot_changed` over a model store, each branch the handler had
    /// before L4. A read fault is skipped; no decoded params, or no attributed seat, marks the marker-named
    /// identities stale (fleet loops 11 and 15) and queues their recompute; an attributed pot files a `pot` event
    /// per seat. The plan announces EVERY parsed outpoint (loop 10 D2), attributed or not. A stale-mark fault is
    /// reported and the recompute still queued.
    #[tokio::test(flavor = "current_thread")]
    async fn e649_hook_e_apply_pot_changed_runs_from_a_harness_without_a_request() {
        let store = ModelStore {
            reads: vec![
                OutpointRead::Failed("D1 down".into()),
                OutpointRead::NoParams {
                    marker_named: vec![C.into()],
                },
                OutpointRead::Decoded {
                    seats: vec![],
                    marker_named: vec![A.into(), C.into()],
                },
                OutpointRead::Decoded {
                    seats: vec![A.into(), B.into()],
                    marker_named: vec![C.into()],
                },
            ],
            mark_fault: true,
            ..ModelStore::default()
        };
        let ops: Vec<(String, u32)> = (0..4).map(op).collect();
        let out = apply_pot_changed(&store, &ops, 99).await;
        assert_eq!(
            out.plan.outpoints, ops,
            "every parsed outpoint is announced"
        );
        assert_eq!(out.plan.at_ms, 99);
        assert_eq!(
            out.stale_marks,
            vec![
                (C.to_string(), "pot-changed (by marker)"),
                (A.to_string(), "pot-changed (unattributed, by marker)"),
                (C.to_string(), "pot-changed (unattributed, by marker)"),
            ]
        );
        assert_eq!(out.stale_fault.as_deref(), Some("D1 down"));
        assert_eq!(
            out.owed_identities,
            vec![C.to_string(), A.to_string(), B.to_string()]
        );
        assert_eq!(
            out.plan.seat_events,
            vec![
                (A.to_string(), ops[3].0.clone(), ops[3].1),
                (B.to_string(), ops[3].0.clone(), ops[3].1)
            ]
        );
        assert_eq!(out.filed.len(), 2);
        assert_eq!(out.filed[0]["identity"], A);
        assert_eq!(out.skipped.len(), 3);
        assert!(out.skipped[0]["why"]
            .as_str()
            .unwrap()
            .contains("decoded-pots read failed"));
        assert!(out.skipped[1]["why"]
            .as_str()
            .unwrap()
            .contains("no decoded params yet"));
        assert!(out.skipped[2]["why"]
            .as_str()
            .unwrap()
            .contains("no attributed seats yet"));
    }

    /// The deploy lane reads the hooks' timing: `Server-Timing: hook;dur=<ms>` and one log line per answer.
    #[test]
    fn e649_hook_f_each_hook_answers_its_timing_and_its_log_line() {
        assert_eq!(hook_timing(12), "hook;dur=12");
        for (name, line) in [
            (
                "internal_pot_changed",
                "\"[pot-changed] answered in {ms} ms, announce detached ({} outpoints)\"",
            ),
            (
                "internal_hop_changed",
                "\"[hop-changed] answered in {ms} ms, recompute detached ({} identities)\"",
            ),
        ] {
            let h = routes_item(&format!("pub(crate) async fn {name}("));
            assert!(h.contains(&squash(line)), "{name} logs its answer time");
            assert!(
                h.contains(&squash(
                    ".set(\"Server-Timing\", &crate::pot_hook::hook_timing(ms))?;"
                )),
                "{name} answers Server-Timing"
            );
        }
    }
}
