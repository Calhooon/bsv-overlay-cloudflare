//! bsv-low #935 pins: the shipped `submit` over a relay fake that applies the relay's own rule (relay 0.8.2
//! `send_budget`: 100 sends a SENDER in a tumbling 60 s window, then `429 ERR_SEND_BUDGET`), on tokio's paused clock.
use super::*;
use std::collections::VecDeque;
use std::rc::Rc;
use tokio::time::{Duration, Instant};

const SEAT: &str = "03c31ebc00000000000000000000000000000000000000000000000000000000aa";
const BASE_MS: u64 = 1_790_000_000_000;

#[derive(Default)]
struct Fake {
    start: RefCell<Option<Instant>>,
    /// Every relay call: (ms, recipient, body, the answer).
    calls: RefCell<Vec<(u64, String, Value, Answer)>>,
    /// The relay's windows, by minute (one sender: the app layer).
    window: RefCell<BTreeMap<u64, u64>>,
    /// Answers forced in order before the relay's rule applies.
    script: RefCell<VecDeque<Answer>>,
    logs: RefCell<Vec<String>>,
}

impl Fake {
    fn new() -> Rc<Self> {
        let f = Rc::new(Self::default());
        *f.start.borrow_mut() = Some(Instant::now());
        f
    }
    fn delivered(&self) -> Vec<(u64, Value)> {
        self.calls
            .borrow()
            .iter()
            .filter(|c| c.3 == Answer::Sent)
            .map(|c| (c.0, c.2.clone()))
            .collect()
    }
    fn refused(&self) -> usize {
        self.calls
            .borrow()
            .iter()
            .filter(|c| c.3 == Answer::BudgetRefused)
            .count()
    }
    /// The relay's rule, exactly: a send charged to its tumbling minute, refused past 100.
    fn relay(&self, now: u64) -> Answer {
        let mut w = self.window.borrow_mut();
        let n = w.entry(now / 60_000).or_insert(0);
        *n += 1;
        if *n > RELAY_SENDS_PER_MIN {
            Answer::BudgetRefused
        } else {
            Answer::Sent
        }
    }
}

impl PushPort for Fake {
    fn now_ms(&self) -> u64 {
        BASE_MS + self.start.borrow().unwrap().elapsed().as_millis() as u64
    }
    async fn sleep_ms(&self, ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
    async fn send(&self, recipient: &str, _message_id: Option<&str>, body: Value) -> Answer {
        let now = self.now_ms();
        let forced = self.script.borrow_mut().pop_front();
        let a = forced.unwrap_or_else(|| self.relay(now));
        self.calls
            .borrow_mut()
            .push((now, recipient.to_string(), body, a));
        a
    }
    fn log(&self, line: &str) {
        self.logs.borrow_mut().push(line.to_string());
    }
}

fn owed(n: u64) -> Value {
    crate::owed::owed_changed_event_body(SEAT, "judge", 3, n as i64)
}

fn pot(txid_byte: u8, at: u64) -> Value {
    crate::internal_events::pot_event_body(&hex::encode([txid_byte; 32]), 0, json!({}), at)
}

/// The storm: 2,005 trigger pushes to ONE seat in 10 minutes (L52's 16:15Z to 16:25Z count), each its own caller.
/// Through the helper: no refusal at the relay, at most 40 in any 60 s, the last state delivered within one window
/// of the last push, nothing dropped (every caller answered delivered; the newest state is the last delivered).
/// The control: the same storm sent straight to the relay (the base, every judgement its own push) is refused.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix935_a_a_storm_of_2005_pushes_to_one_seat_stays_under_the_relays_budget() {
    const PUSHES: u64 = 2_005;
    const SPAN_MS: u64 = 600_000;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let fake = Fake::new();
            let cell = Rc::new(RefCell::new(Coalescer::default()));
            let knobs = Knobs::default();
            let t0 = Instant::now();
            let mut handles = Vec::new();
            let mut last_push_ms = 0;
            for i in 0..PUSHES {
                tokio::time::sleep_until(t0 + Duration::from_millis(i * SPAN_MS / PUSHES)).await;
                last_push_ms = fake.now_ms();
                let (fake, cell) = (fake.clone(), cell.clone());
                handles.push(tokio::task::spawn_local(async move {
                    submit(&cell, &*fake, &knobs, SEAT, None, owed(i)).await
                }));
            }
            let mut answers = Vec::new();
            for h in handles {
                answers.push(h.await.unwrap());
            }
            assert!(
                answers.iter().all(|a| *a == Answer::Sent),
                "every caller answered delivered"
            );
            assert_eq!(fake.refused(), 0, "no ERR_SEND_BUDGET at the relay");
            let sent = fake.delivered();
            for (i, (t, _)) in sent.iter().enumerate() {
                let in_minute = sent[i..]
                    .iter()
                    .take_while(|(u, _)| *u < t + 60_000)
                    .count();
                assert!(
                    in_minute as u64 <= PUSH_BUDGET_PER_MIN_DEFAULT,
                    "{in_minute} sends in the 60 s from {t}"
                );
            }
            let (last_at, last_body) = sent.last().unwrap().clone();
            assert_eq!(
                last_body["computedAtMs"],
                PUSHES as i64 - 1,
                "the newest state goes last"
            );
            assert!(
                last_at <= last_push_ms + PUSH_COALESCE_MS_DEFAULT,
                "the last state within one window of the last push: {} ms",
                last_at - last_push_ms
            );
            let tally = cell.borrow().tally;
            assert_eq!(tally.given_up, 0);
            assert_eq!(tally.refused_budget, 0);
            assert!(tally.coalesced > 1_500, "{tally:?}");
            assert_eq!(
                sent.len() as u64,
                tally.windows + 1,
                "one leading send, the rest at windows' ends"
            );
            println!(
                "fix935_a: {PUSHES} pushes in {SPAN_MS} ms -> {} sends, {tally:?}",
                sent.len()
            );

            // The control: the base sends every push.
            let base = Fake::new();
            let t1 = Instant::now();
            for i in 0..PUSHES {
                tokio::time::sleep_until(t1 + Duration::from_millis(i * SPAN_MS / PUSHES)).await;
                base.send(SEAT, None, owed(i)).await;
            }
            assert!(
                base.refused() > 900,
                "the base is refused: {}",
                base.refused()
            );
        })
        .await;
}

/// A `429 ERR_SEND_BUDGET` is read by name, counted, and the state tried again at the next window; past three
/// windows it is given up, counted and logged with the identity's prefix and the kind.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix935_b_a_budget_refusal_is_retried_at_the_next_window_then_given_up() {
    let knobs = Knobs::default();
    let cell = RefCell::new(Coalescer::default());
    let fake = Fake::new();
    fake.script.borrow_mut().push_back(Answer::BudgetRefused);
    let a = submit(&cell, &*fake, &knobs, SEAT, None, owed(1)).await;
    assert_eq!(a, Answer::Sent);
    let calls = fake.calls.borrow().clone();
    assert_eq!(calls.len(), 2);
    assert!(
        calls[1].0 - calls[0].0 >= PUSH_COALESCE_MS_DEFAULT,
        "retried at the next window"
    );
    assert_eq!(cell.borrow().tally.refused_budget, 1);
    assert_eq!(cell.borrow().tally.given_up, 0);

    let cell = RefCell::new(Coalescer::default());
    let fake = Fake::new();
    fake.script.borrow_mut().extend([Answer::BudgetRefused; 8]);
    let a = submit(&cell, &*fake, &knobs, SEAT, None, owed(2)).await;
    assert_eq!(a, Answer::BudgetRefused);
    assert_eq!(
        fake.calls.borrow().len(),
        1 + PUSH_BUDGET_RETRY_WINDOWS as usize
    );
    let t = cell.borrow().tally;
    assert_eq!((t.refused_budget, t.given_up), (4, 1));
    {
        let logs = fake.logs.borrow();
        assert!(
            logs.iter().any(|l| l.contains("GIVEN UP")
                && l.contains(&SEAT[..12])
                && l.contains("kind=owed-changed")),
            "{logs:?}"
        );
    }

    // Any other refusal is today's: answered failed, never retried, never counted a budget refusal.
    let cell = RefCell::new(Coalescer::default());
    let fake = Fake::new();
    fake.script.borrow_mut().push_back(Answer::Failed);
    assert_eq!(
        submit(&cell, &*fake, &knobs, SEAT, None, owed(3)).await,
        Answer::Failed
    );
    assert_eq!(fake.calls.borrow().len(), 1);
    assert_eq!(cell.borrow().tally, Tally::default());
}

/// A kind that is not a trigger passes through uncoalesced, even inside an open window; two pot outpoints are two
/// keys (each delivered), one outpoint twice is one key (the newest delivered, the older coalesced).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix935_c_a_non_trigger_passes_through_and_each_pot_outpoint_is_its_own_key() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let knobs = Knobs::default();
            let cell = Rc::new(RefCell::new(Coalescer::default()));
            let fake = Fake::new();
            assert_eq!(
                submit(&cell, &*fake, &knobs, SEAT, None, owed(0)).await,
                Answer::Sent
            );
            let ladder = json!({"v": 1, "kind": "ladder", "gameId": "aa", "at": 1});
            assert_eq!(coalesce_key(&ladder), None);
            let t = fake.now_ms();
            for _ in 0..2 {
                assert_eq!(
                    submit(&cell, &*fake, &knobs, SEAT, None, ladder.clone()).await,
                    Answer::Sent
                );
            }
            let ladders: Vec<u64> = fake
                .calls
                .borrow()
                .iter()
                .filter(|c| c.2["kind"] == "ladder")
                .map(|c| c.0)
                .collect();
            assert_eq!(ladders, vec![t, t], "both at once, inside the open window");

            let mut hs = Vec::new();
            for (b, at, id) in [(1u8, 10, "m1"), (2, 11, "m2"), (1, 12, "m3")] {
                let (cell, fake) = (cell.clone(), fake.clone());
                hs.push(tokio::task::spawn_local(async move {
                    submit(&cell, &*fake, &knobs, SEAT, Some(id), pot(b, at)).await
                }));
            }
            for h in hs {
                assert_eq!(h.await.unwrap(), Answer::Sent);
            }
            let pots: Vec<(String, u64)> = fake
                .calls
                .borrow()
                .iter()
                .filter(|c| c.2["kind"] == "pot")
                .map(|c| {
                    (
                        c.2["potOutpoint"]["txid"].as_str().unwrap()[..2].to_string(),
                        c.2["at"].as_u64().unwrap(),
                    )
                })
                .collect();
            assert_eq!(pots.len(), 2, "{pots:?}");
            assert!(
                pots.contains(&("01".into(), 12)),
                "the newest of outpoint 1: {pots:?}"
            );
            assert!(pots.contains(&("02".into(), 11)));
            assert_eq!(cell.borrow().tally.coalesced, 1);
        })
        .await;
}

/// The knobs: defaults, the vars, the clamps under the relay's 100.
#[test]
fn fix935_d_the_knobs_parse_and_clamp() {
    assert_eq!(Knobs::parse(None, None, None), Knobs::default());
    assert_eq!(
        Knobs::default(),
        Knobs {
            window_ms: 2_000,
            per_identity_per_min: 40,
            sender_per_min: 80
        }
    );
    let k = Knobs::parse(Some("500"), Some("25"), Some("60"));
    assert_eq!(
        (k.window_ms, k.per_identity_per_min, k.sender_per_min),
        (500, 25, 60)
    );
    let k = Knobs::parse(Some("999999"), Some("1000"), Some("0"));
    assert_eq!(
        (k.window_ms, k.per_identity_per_min, k.sender_per_min),
        (10_000, 99, 1)
    );
    let k = Knobs::parse(Some("x"), Some("-3"), Some(" 50 "));
    assert_eq!(
        (k.window_ms, k.per_identity_per_min, k.sender_per_min),
        (2_000, 40, 50)
    );
    assert_eq!(
        answer_of(429, r#"{"status":"error","code":"ERR_SEND_BUDGET"}"#),
        Answer::BudgetRefused
    );
    assert_eq!(answer_of(429, "rate limited"), Answer::Failed);
    assert_eq!(
        answer_of(400, r#"{"code":"ERR_DUPLICATE_MESSAGE"}"#),
        Answer::Sent
    );
    assert_eq!(answer_of(201, ""), Answer::Sent);
}

/// The isolate-wide sender bucket holds what many seats push together: 60 seats each pushing once at one instant
/// reach the relay under its 100 in the minute, every state delivered (20 at once, the rest held, then sent), none
/// given up. Stated: past the sender's minute AND the hold bound (20 s) a held state is sent regardless, and the
/// relay may then refuse it (three retries, then given up).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix935_e_many_seats_share_the_senders_bucket() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let knobs = Knobs::default();
            let cell = Rc::new(RefCell::new(Coalescer::default()));
            let fake = Fake::new();
            let mut hs = Vec::new();
            for s in 0..60u32 {
                let seat = format!("02{:064x}", s);
                let (cell, fake) = (cell.clone(), fake.clone());
                hs.push(tokio::task::spawn_local(async move {
                    submit(
                        &cell,
                        &*fake,
                        &knobs,
                        &seat,
                        None,
                        crate::owed::owed_changed_event_body(&seat, "j", 1, 1),
                    )
                    .await
                }));
            }
            let answers: Vec<Answer> = futures_util::future::join_all(hs)
                .await
                .into_iter()
                .map(|h| h.unwrap())
                .collect();
            let refused = fake.refused();
            let sent = fake.delivered();
            let t = cell.borrow().tally;
            println!("fix935_e: {} sent, {refused} refused, {t:?}", sent.len());
            assert_eq!(refused, 0);
            assert!(answers.iter().all(|a| *a == Answer::Sent));
            assert_eq!(sent.len(), 60, "every seat's state goes");
            assert!(t.held > 0 && t.given_up == 0, "{t:?}");
        })
        .await;
}

/// The wiring: both first-party push sites call the one helper; the helper reads the knobs from the env.
#[test]
fn fix935_f_every_push_to_a_seat_calls_the_helper() {
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let judge = squash(include_str!("owed_judge.rs"));
    // FIX-935-FOLD: the judge QUEUES its push; the queue is flushed through the gate's `deliver` (`flush_queued`,
    // bsv-low #989: it was `push_to_seat` a push).
    assert!(judge.contains(&squash(
        "crate::push_coalesce::queue_seat_push(identity, body);"
    )));
    assert!(squash(include_str!("push_coalesce.rs"))
        .contains(&squash(".map(|chunk| deliver_env(env, chunk.to_vec())),")));
    assert!(!judge.contains("first_party_push"));
    let hook = squash(include_str!("pot_hook.rs"));
    assert!(hook.contains(&squash(
        "crate::push_coalesce::push_to_seat(&self.env, recipient, Some(message_id), body).await"
    )));
    assert!(!hook.contains(&squash("crate::internal_events::first_party_push_with_id(")));
    let me = squash(include_str!("push_coalesce.rs"));
    assert!(me.contains(&squash("let knobs = Knobs::from_env(env);")));
}

/// FIX-935-FOLD (the land's budget tier, PR 194): under the coalescer `/internal/pot-changed` answered in 2,068 ms
/// (`hook;dur=`, E649 L4's bound 1,000): its judge awaited the `owed-changed` push, and a second push to a seat waits
/// out the window. The judge's push is QUEUED in the isolate (newest body per seat and coalescing key, oldest first,
/// bounded) and nothing in the answer path waits on it: the queue is handed to `wait_until` after the answer.
#[test]
fn fix935_fold_the_judges_push_is_queued_newest_per_seat_and_bounded() {
    let _ = take_queued();
    let owed = |n: u64| json!({"kind": "owed-changed", "identity": "x", "rows": n});
    queue_seat_push("AA", owed(1));
    queue_seat_push("bb", owed(1));
    queue_seat_push("aa", owed(2));
    assert!(has_queued());
    let taken = take_queued();
    assert_eq!(
        taken,
        vec![("bb".to_string(), owed(1)), ("aa".to_string(), owed(2))],
        "one push per seat and key, the newest body, oldest first"
    );
    assert!(!has_queued());
    for i in 0..(PUSH_QUEUE_MAX + 5) {
        queue_seat_push(&format!("{i:08x}"), owed(i as u64));
    }
    let taken = take_queued();
    assert_eq!(taken.len(), PUSH_QUEUE_MAX, "bounded");
    assert_eq!(taken[0].0, format!("{:08x}", 5), "the oldest let go");
}

/// FIX-935-FOLD: the wiring. No coalescing wait in an answer path: the judge queues, the hook routes hand the flush to
/// `wait_until` after their judgement, and an unconfigured relay bypasses the coalescer (the detached announce's
/// parks settle at once, as before #935, so the tier's full-table cells see the table they filled).
#[test]
fn fix935_fold_no_coalescing_wait_in_an_answer_path() {
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let judge = squash(include_str!("owed_judge.rs"));
    assert!(
        !judge.contains(&squash("push_coalesce::push_to_seat(")),
        "the judge never sends in its caller's path"
    );
    let routes = squash(include_str!("routes.rs"));
    for drain in [
        "\"[owed] pot drain failed: {error}\"",
        "\"[owed] hop drain failed: {error}\"",
    ] {
        let at = routes.find(&squash(drain)).expect("the hook's drain");
        let tail = &routes[at..];
        let flush = tail
            .find("crate::push_coalesce::flush_after(ctx,env);")
            .expect("the flush handed after the drain");
        let answer = tail.find("json_response(").expect("the answer");
        assert!(
            flush < answer,
            "the flush is handed before the answer leaves, never awaited"
        );
    }
    let events = squash(include_str!("internal_events.rs"));
    assert!(events.contains(&squash("crate::push_coalesce::flush_after(ctx, env);")));
    let me = squash(include_str!("push_coalesce.rs"));
    assert!(me.contains(&squash("ctx.wait_until(flush_queued(env.clone()));")));
    assert!(me.contains(&squash(
        "if !crate::internal_events::relay_configured(env) { return first_party_push_answer_raw(env, recipient, message_id, body).await; }"
    )));
    let lib = squash(include_str!("lib.rs"));
    assert!(lib.contains(&squash("push_coalesce::flush_after(&ctx, &push_env);")));
    assert!(lib.contains(&squash("push_coalesce::flush_queued(env_backfill).await;")));
    let actor = squash(include_str!("board_view.rs"));
    let flush = &actor[actor
        .find("fnflush_counters_if_due(&self){")
        .expect("the actor's flush")..];
    assert!(flush.starts_with(&squash(
        "fn flush_counters_if_due(&self) { self.flush_pushes();"
    )));
}

/// bsv-low #989's model gate: forwards into ONE coalescer (the gate's isolate), as `WorkerGate` forwards to the
/// `BOARD_VIEW` object named `PUSH_GATE_ACTOR`; `unbound` is the base's rule (every isolate its own coalescer).
struct ModelGate {
    gate: Option<(Rc<RefCell<Coalescer>>, Rc<Fake>)>,
    forwards: RefCell<usize>,
}

/// The gate's own isolate: its pushes go to its cell.
struct Here;
impl SeatGate for Here {
    fn hosted_here(&self) -> bool {
        true
    }
    async fn forward(&self, _: Vec<GatePush>) -> Forwarded {
        unreachable!("the gate never forwards")
    }
}

impl SeatGate for ModelGate {
    fn hosted_here(&self) -> bool {
        false
    }
    async fn forward(&self, pushes: Vec<GatePush>) -> Forwarded {
        let Some((cell, fake)) = &self.gate else {
            return Forwarded::Unbound;
        };
        *self.forwards.borrow_mut() += 1;
        Forwarded::Answers(deliver(cell, &**fake, &Here, &Knobs::default(), pushes).await)
    }
}

fn seat_of(i: usize) -> String {
    format!("02{:064x}", i + 1)
}

/// L54's burst, shaped from the beta lines of 06:54:59.9Z to 06:55:59Z: the owed actor of each seat (its OWN isolate,
/// one Durable Object a seat) pushes one `owed-changed` an event, p22's 90 in one second; beside them the Worker's
/// fetch isolates push the same seats' `owed-changed` (the D1 judge in Phase B) and `pot`. Returns the relay fake and
/// every coalescer's tally summed.
async fn l54_burst(through_gate: bool) -> (Rc<Fake>, Tally, Vec<Answer>, usize) {
    const SEATS: usize = 30;
    const P22_EVENTS: u64 = 90;
    const SEAT_EVENTS: u64 = 10;
    const FETCH_ISOLATES: usize = 4;
    let fake = Fake::new();
    let gate_cell = Rc::new(RefCell::new(Coalescer::default()));
    let gate = Rc::new(ModelGate {
        gate: through_gate.then(|| (gate_cell.clone(), fake.clone())),
        forwards: RefCell::new(0),
    });
    let actors: Vec<_> = (0..SEATS)
        .map(|_| Rc::new(RefCell::new(Coalescer::default())))
        .collect();
    let fetches: Vec<_> = (0..FETCH_ISOLATES)
        .map(|_| Rc::new(RefCell::new(Coalescer::default())))
        .collect();
    let t0 = Instant::now();
    let mut hs = Vec::new();
    for (s, actor) in actors.iter().enumerate() {
        let events = if s == 0 { P22_EVENTS } else { SEAT_EVENTS };
        for e in 0..events {
            let at = t0 + Duration::from_millis(e * 1_000 / events);
            let (fake, gate, cell, seat) = (fake.clone(), gate.clone(), actor.clone(), seat_of(s));
            hs.push(tokio::task::spawn_local(async move {
                tokio::time::sleep_until(at).await;
                let body = crate::owed::owed_changed_event_body(&seat, "actor", 1, e as i64);
                let push = GatePush {
                    recipient: seat,
                    message_id: None,
                    body,
                };
                deliver(&cell, &*fake, &*gate, &Knobs::default(), vec![push]).await
            }));
        }
        for (f, fetch) in fetches.iter().enumerate() {
            let at = t0 + Duration::from_millis(100 + f as u64 * 200);
            let (fake, gate, cell, seat) = (fake.clone(), gate.clone(), fetch.clone(), seat_of(s));
            hs.push(tokio::task::spawn_local(async move {
                tokio::time::sleep_until(at).await;
                let pushes = vec![
                    GatePush {
                        recipient: seat.clone(),
                        message_id: None,
                        body: crate::owed::owed_changed_event_body(
                            &seat,
                            "judge",
                            1,
                            1_000 + f as i64,
                        ),
                    },
                    GatePush {
                        recipient: seat.clone(),
                        message_id: Some(format!("pot-{s}-{f}")),
                        body: crate::internal_events::pot_event_body(
                            &hex::encode([s as u8 + 1; 32]),
                            0,
                            json!({}),
                            f as u64,
                        ),
                    },
                ];
                deliver(&cell, &*fake, &*gate, &Knobs::default(), pushes).await
            }));
        }
    }
    let mut answers = Vec::new();
    for h in hs {
        answers.extend(h.await.unwrap());
    }
    let mut t = Tally::default();
    for c in actors
        .iter()
        .chain(fetches.iter())
        .chain(std::iter::once(&gate_cell))
    {
        let x = c.borrow().tally;
        t.coalesced += x.coalesced;
        t.held += x.held;
        t.refused_budget += x.refused_budget;
        t.given_up += x.given_up;
        t.windows += x.windows;
    }
    let forwards = *gate.forwards.borrow();
    (fake, t, answers, forwards)
}

/// bsv-low #989 (L54's S2): the actor's per-event `owed-changed` and the fetch isolates' pushes, each producer in its
/// own isolate, reach the relay ONLY through the fleet's one coalescer: p22's 90 events in one second are at most two
/// sends (the leading edge and the window's newest state), no 60 s carries more than the relay's 100, nothing is
/// refused or given up, and every seat's newest `owed-changed` is delivered. RED with the gate unbound (the base:
/// each isolate's own coalescer): refused at the relay and given up.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix989_a_the_actors_burst_goes_through_one_coalescer_under_the_relays_budget() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (fake, t, answers, forwards) = l54_burst(true).await;
            let calls = fake.calls.borrow().clone();
            let p22 = seat_of(0);
            let p22_owed = calls
                .iter()
                .filter(|c| c.1 == p22 && c.2["kind"] == "owed-changed")
                .count();
            let max_minute = fake.window.borrow().values().copied().max().unwrap_or(0);
            println!(
                "fix989_a: {} relay calls, {} refused, the busiest minute {max_minute}, p22 owed-changed {p22_owed}, {forwards} forwards, {t:?}",
                calls.len(),
                fake.refused()
            );
            assert_eq!(fake.refused(), 0, "no ERR_SEND_BUDGET at the relay");
            assert_eq!(t.given_up, 0, "nothing given up: {t:?}");
            assert!(max_minute <= RELAY_SENDS_PER_MIN, "{max_minute} sends in one relay minute");
            assert!(forwards > 0, "every producer forwards to the gate");
            assert!(p22_owed <= 2, "p22's 90 events are at most two sends: {p22_owed}");
            assert!(answers.iter().all(|a| *a == Answer::Sent), "every push answered delivered");
            for s in 0..30 {
                let seat = seat_of(s);
                let last = calls
                    .iter()
                    .rfind(|c| c.1 == seat && c.2["kind"] == "owed-changed" && c.3 == Answer::Sent)
                    .unwrap_or_else(|| panic!("seat {s} got no owed-changed"));
                let all: Vec<_> = calls
                    .iter()
                    .filter(|c| c.1 == seat && c.2["kind"] == "owed-changed")
                    .collect();
                assert_eq!(last.0, all.last().unwrap().0, "seat {s}: the newest state goes last");
            }
        })
        .await;
}

/// The control of `fix989_a`: the same burst with the gate unbound (the base's rule, `push_to_seat` submitting to
/// its own isolate's coalescer) is refused at the relay and given up, the shape of L54's `541 sends`.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix989_b_the_same_burst_per_isolate_is_refused_and_given_up() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (fake, t, _, forwards) = l54_burst(false).await;
            let max_minute = fake.window.borrow().values().copied().max().unwrap_or(0);
            println!(
                "fix989_b (per isolate): {} refused, the busiest minute {max_minute}, {t:?}",
                fake.refused()
            );
            assert_eq!(forwards, 0);
            assert!(fake.refused() > 0 && t.given_up > 0, "{t:?}");
            assert!(max_minute > RELAY_SENDS_PER_MIN);
        })
        .await;
}

/// A gate that faults sends nothing around itself: every push `Failed` (the pot hook parks and re-drives its event),
/// no relay call; the gate's answer body reads back.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix989_c_a_gate_fault_never_sends_around_the_gate() {
    struct Faulty;
    impl SeatGate for Faulty {
        fn hosted_here(&self) -> bool {
            false
        }
        async fn forward(&self, _: Vec<GatePush>) -> Forwarded {
            Forwarded::Fault("HTTP 500".into())
        }
    }
    let fake = Fake::new();
    let cell = RefCell::new(Coalescer::default());
    let push = GatePush {
        recipient: SEAT.into(),
        message_id: None,
        body: owed(1),
    };
    let a = deliver(
        &cell,
        &*fake,
        &Faulty,
        &Knobs::default(),
        vec![push.clone(), push],
    )
    .await;
    assert_eq!(a, vec![Answer::Failed, Answer::Failed]);
    assert!(
        fake.calls.borrow().is_empty(),
        "no relay call around the gate"
    );
    assert!(fake.logs.borrow()[0].contains("gate fault"));
    let words: Vec<_> = [
        Answer::Sent,
        Answer::BudgetRefused,
        Answer::Failed,
        Answer::NotConfigured,
    ]
    .iter()
    .map(|a| a.word())
    .collect();
    assert_eq!(
        answers_of(&json!({ "answers": words }).to_string()),
        Some(vec![
            Answer::Sent,
            Answer::BudgetRefused,
            Answer::Failed,
            Answer::NotConfigured
        ])
    );
    assert_eq!(answers_of(r#"{"answers":["nope"]}"#), None);
}

/// The wiring: `push_to_seat` and the queue's flush both go through `deliver` over the `BOARD_VIEW` gate; the gate's
/// route submits to its isolate's cell; the actor's push is queued and flushed (never sent around the helper); no
/// push site calls the relay's raw `/push` but the unconfigured short-cut.
#[test]
fn fix989_d_every_push_reaches_the_relay_through_the_gate() {
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let me = squash(include_str!("push_coalesce.rs"));
    assert!(me.contains(&squash(
        "deliver(cell, &port, &WorkerGate { env }, &knobs, pushes).await"
    )));
    assert!(me.contains(&squash("deliver_env(env, vec![push])")));
    assert!(me.contains(&squash(".map(|chunk| deliver_env(env, chunk.to_vec())),")));
    assert!(me.contains(&squash(".id_from_name(PUSH_GATE_ACTOR)")));
    assert!(me.contains(&squash("GATE_HOSTED.with(|h| h.set(true));")));
    let view = squash(include_str!("board_view.rs"));
    assert!(view.contains(&squash(
        "if req.path() == crate::push_coalesce::PUSH_GATE_PATH { let text = req.text().await?; return crate::push_coalesce::serve_gate(&self.env, &text).await; }"
    )));
    let actor = squash(include_str!("owed_actor/object.rs"));
    assert!(actor.contains(&squash(
        "crate::push_coalesce::queue_seat_push(&identity, push);"
    )));
    assert!(!actor.contains("first_party_push"));
    // The relay's raw `/push` is reached by the coalescer's port and the unconfigured short-cut only.
    assert_eq!(me.matches("first_party_push_answer(").count(), 2);
}

/// bsv-low #1002 (L55 S3, identityDown pair 10): one pot's two seats under a SATURATED sender minute. The fleet's gate
/// is one coalescer; `HOT` other seats keep it busy (an owed-changed every `HOT_EVERY_MS` each, the fleet's other
/// hands), so the sender bucket pays about one push a second, the rate the relay counted in that minute (10 `/push`
/// calls in each 10 s, 22:50:40Z to 22:51:00Z). At `HAND_MS` the pot hook's announce forwards the pot's `pot` event to
/// both seats in one body; the seats' owed actors push `owed-changed` at `HAND_MS` + 2, 4 and 6 s. Returns, per seat
/// of the pot, the first delivery of each kind (ms after `t0`), and the hot seats' worst wait.
async fn l55_hand() -> (Vec<(Option<u64>, Option<u64>)>, u64, Rc<Fake>) {
    const HOT: usize = 6;
    const HOT_EVERY_MS: u64 = 410;
    const SPAN_MS: u64 = 40_000;
    const HAND_MS: u64 = 10_000;
    let fake = Fake::new();
    let cell = Rc::new(RefCell::new(Coalescer::default()));
    let gate = Rc::new(ModelGate {
        gate: Some((cell.clone(), fake.clone())),
        forwards: RefCell::new(0),
    });
    let t0 = Instant::now();
    let mut hs = Vec::new();
    for h in 0..HOT {
        let seat = seat_of(100 + h);
        for e in 0..SPAN_MS / HOT_EVERY_MS {
            let at = t0 + Duration::from_millis(e * HOT_EVERY_MS + h as u64 * 37);
            let (fake, gate, seat) = (fake.clone(), gate.clone(), seat.clone());
            let local = Rc::new(RefCell::new(Coalescer::default()));
            hs.push(tokio::task::spawn_local(async move {
                tokio::time::sleep_until(at).await;
                let body = crate::owed::owed_changed_event_body(&seat, "actor", 1, e as i64);
                let push = GatePush {
                    recipient: seat,
                    message_id: None,
                    body,
                };
                deliver(&local, &*fake, &*gate, &Knobs::default(), vec![push]).await
            }));
        }
    }
    let (a, b) = (seat_of(0), seat_of(1));
    {
        let (fake, gate, a, b) = (fake.clone(), gate.clone(), a.clone(), b.clone());
        let local = Rc::new(RefCell::new(Coalescer::default()));
        hs.push(tokio::task::spawn_local(async move {
            tokio::time::sleep_until(t0 + Duration::from_millis(HAND_MS)).await;
            let pot =
                crate::internal_events::pot_event_body(&hex::encode([0xbd; 32]), 0, json!({}), 1);
            let pushes = [a, b]
                .into_iter()
                .map(|seat| GatePush {
                    message_id: Some(format!("pot-{seat}")),
                    recipient: seat,
                    body: pot.clone(),
                })
                .collect();
            deliver(&local, &*fake, &*gate, &Knobs::default(), pushes).await
        }));
    }
    for seat in [a.clone(), b.clone()] {
        for e in 1..=3u64 {
            let at = t0 + Duration::from_millis(HAND_MS + e * 2_000);
            let (fake, gate, seat) = (fake.clone(), gate.clone(), seat.clone());
            let local = Rc::new(RefCell::new(Coalescer::default()));
            hs.push(tokio::task::spawn_local(async move {
                tokio::time::sleep_until(at).await;
                let body = crate::owed::owed_changed_event_body(&seat, "owed-actor", 2, e as i64);
                let push = GatePush {
                    recipient: seat,
                    message_id: None,
                    body,
                };
                deliver(&local, &*fake, &*gate, &Knobs::default(), vec![push]).await
            }));
        }
    }
    let mut answers = Vec::new();
    for h in hs {
        answers.extend(h.await.unwrap());
    }
    let calls = fake.calls.borrow().clone();
    let start = BASE_MS;
    assert!(
        answers.iter().all(|a| *a == Answer::Sent),
        "every push answered delivered"
    );
    let first = |seat: &str, kind: &str| {
        calls
            .iter()
            .find(|c| c.1 == seat && c.2["kind"] == kind && c.3 == Answer::Sent)
            .map(|c| c.0 - start)
    };
    let seats = [a, b]
        .iter()
        .map(|s| (first(s, "pot"), first(s, "owed-changed")))
        .collect();
    // The hot seats' worst wait: from a push to the next delivery of that seat at or after it.
    let mut worst = 0;
    for h in 0..HOT {
        let seat = seat_of(100 + h);
        let sent: Vec<u64> = calls
            .iter()
            .filter(|c| c.1 == seat && c.3 == Answer::Sent)
            .map(|c| c.0 - start)
            .collect();
        for e in 0..SPAN_MS / HOT_EVERY_MS {
            let at = e * HOT_EVERY_MS + h as u64 * 37;
            if let Some(t) = sent.iter().find(|t| **t >= at) {
                worst = worst.max(t - at);
            }
        }
    }
    (seats, worst, fake)
}

/// bsv-low #1002: BOTH seats of one pot receive the pot's `pot` event and an `owed-changed` while the hand is on,
/// under a sender minute the fleet keeps full, and no seat waits out the hold bound. The sender bucket is served first
/// come, first served across seats ([`take`]'s queue): eight seats with a state each wait at most about eight sends
/// (a second each) and a window. RED on main's rule (the token to whichever waiter woke first after a refill, a quiet
/// seat's leading edge jumping the held states): a seat of the pot loses each race until its hold bound forces it.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix1002_a_both_seats_of_one_pot_get_their_events_under_a_saturated_sender_minute() {
    const HAND_MS: u64 = 10_000;
    const BOUND_MS: u64 = 10_000;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (seats, worst, fake) = l55_hand().await;
            println!(
                "fix1002_a: seats (pot, owed-changed) ms {seats:?}, the hot seats' worst wait {worst} ms, {} relay calls, {} refused",
                fake.calls.borrow().len(),
                fake.refused()
            );
            assert_eq!(fake.refused(), 0, "no ERR_SEND_BUDGET at the relay");
            for (i, (pot, owed)) in seats.iter().enumerate() {
                let pot = pot.unwrap_or_else(|| panic!("seat {i} never got the pot event"));
                let owed = owed.unwrap_or_else(|| panic!("seat {i} never got an owed-changed"));
                assert!(
                    pot <= HAND_MS + BOUND_MS,
                    "seat {i}: the pot event {} ms after the hook's announce",
                    pot - HAND_MS
                );
                assert!(
                    owed <= HAND_MS + 2_000 + BOUND_MS,
                    "seat {i}: the first owed-changed {} ms after the actor's push",
                    owed - HAND_MS - 2_000
                );
            }
            assert!(
                // bsv-low #1017: the bound #1002 read is the absolute one (the hot seats here keep the relay's own
                // minute full, so their holds pass the 4 s bar and are served over it on `/health.push.gate`).
                worst < PUSH_HOLD_WINDOWS_RELAY_FULL as u64 * PUSH_COALESCE_MS_DEFAULT,
                "a hot seat waited {worst} ms, its hold bound"
            );
        })
        .await;
}

/// bsv-low #1002, the window's half: a seat pushed twice a window (an owed actor's `owed-changed` an event, L55's p22
/// at 22:50:46.6Z, 48.6, 50.6), with the sender bucket idle, so every other push lands at the end of the window a
/// waiting state is on. That state goes at that end, carrying the newest, never postponed by the push that replaces
/// it. RED on main's rule (a push at or
/// after the window's end opened a new window over the waiting state, so the waiter woke superseded): one send at the
/// leading edge and the next only after the pushes stopped.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix1002_b_a_seat_pushed_twice_a_window_is_sent_every_window() {
    const PUSHES: u64 = 30;
    const EVERY_MS: u64 = PUSH_COALESCE_MS_DEFAULT / 2;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let fake = Fake::new();
            let cell = Rc::new(RefCell::new(Coalescer::default()));
            let t0 = Instant::now();
            let mut hs = Vec::new();
            for e in 0..PUSHES {
                let at = t0 + Duration::from_millis(e * EVERY_MS);
                let (fake, cell) = (fake.clone(), cell.clone());
                hs.push(tokio::task::spawn_local(async move {
                    tokio::time::sleep_until(at).await;
                    submit(&cell, &*fake, &Knobs::default(), SEAT, None, owed(e)).await
                }));
            }
            for h in hs {
                assert_eq!(h.await.unwrap(), Answer::Sent);
            }
            let sent: Vec<u64> = fake.delivered().iter().map(|(t, _)| t - BASE_MS).collect();
            println!("fix1002_b: sends at {sent:?}");
            let gaps: Vec<u64> = sent.windows(2).map(|w| w[1] - w[0]).collect();
            assert!(
                gaps.iter().all(|g| *g <= 2 * PUSH_COALESCE_MS_DEFAULT),
                "a gap past two windows between sends: {gaps:?}"
            );
            assert_eq!(
                fake.delivered().last().unwrap().1["computedAtMs"],
                PUSHES as i64 - 1,
                "the newest state goes last"
            );
        })
        .await;
}

// ── bsv-low #1017 (L56 S3, identityDown pair 10): the hold ends on a new kind or after two windows ──

/// The key a push delivers under (its pot outpoint's for a `pot`).
fn kind_key(body: &Value) -> String {
    coalesce_key(body).unwrap_or_default()
}

/// The 33-pair burst on the fleet's ONE gate (L56's loop shape): 33 pairs, 66 seats, one hand a pair started every
/// `STAGGER_MS`; each hand announces its pot's `pot` event to both seats (the JOIN), the seats' owed actors push an
/// `owed-changed` at +2, +4 and +6 s, the settle's `pot` event at +8 s and a last `owed-changed` at +10 s, every
/// producer in its own isolate forwarding to the gate. Returns every push's delivery latency (its send time to the
/// first delivery of its seat and key at or after it), the relay fake and the gate's cell.
async fn l56_burst() -> (Vec<u64>, Rc<Fake>, Rc<RefCell<Coalescer>>) {
    const PAIRS: usize = 33;
    const STAGGER_MS: u64 = 6_000;
    let fake = Fake::new();
    let cell = Rc::new(RefCell::new(Coalescer::default()));
    let gate = Rc::new(ModelGate {
        gate: Some((cell.clone(), fake.clone())),
        forwards: RefCell::new(0),
    });
    let t0 = Instant::now();
    let mut hs = Vec::new();
    let mut pushes: Vec<(u64, String, String)> = Vec::new();
    for p in 0..PAIRS {
        let hand = p as u64 * STAGGER_MS;
        let seats = [seat_of(2 * p), seat_of(2 * p + 1)];
        let pot = crate::internal_events::pot_event_body(
            &hex::encode([p as u8 + 1; 32]),
            0,
            json!({}),
            1,
        );
        let mut events: Vec<(u64, String, Value)> = Vec::new();
        for seat in &seats {
            events.push((hand, seat.clone(), pot.clone()));
            events.push((hand + 8_000, seat.clone(), pot.clone()));
            for (n, at) in [2_000u64, 4_000, 6_000, 10_000].into_iter().enumerate() {
                let body = crate::owed::owed_changed_event_body(seat, "owed-actor", 2, n as i64);
                events.push((hand + at, seat.clone(), body));
            }
        }
        for (at, seat, body) in events {
            pushes.push((at, seat.clone(), kind_key(&body)));
            let (fake, gate) = (fake.clone(), gate.clone());
            let local = Rc::new(RefCell::new(Coalescer::default()));
            hs.push(tokio::task::spawn_local(async move {
                tokio::time::sleep_until(t0 + Duration::from_millis(at)).await;
                let push = GatePush {
                    recipient: seat,
                    message_id: None,
                    body,
                };
                deliver(&local, &*fake, &*gate, &Knobs::default(), vec![push]).await
            }));
        }
    }
    for h in hs {
        for a in h.await.unwrap() {
            assert_eq!(a, Answer::Sent, "every push answered delivered");
        }
    }
    let calls = fake.calls.borrow().clone();
    let latencies = pushes
        .iter()
        .map(|(at, seat, key)| {
            calls
                .iter()
                .filter(|c| &c.1 == seat && &kind_key(&c.2) == key && c.3 == Answer::Sent)
                .map(|c| c.0 - BASE_MS)
                .find(|t| *t >= *at)
                .unwrap_or_else(|| panic!("{seat} {key} at {at} was never delivered"))
                - at
        })
        .collect();
    (latencies, fake, cell)
}

fn p95(mut v: Vec<u64>) -> (u64, u64, u64) {
    v.sort_unstable();
    let at = |q: f64| v[((q * v.len() as f64).ceil() as usize).clamp(1, v.len()) - 1];
    (at(0.5), at(0.95), *v.last().unwrap())
}

/// (fix1017_a) THE 33-PAIR BURST: every seat's `pot` and `owed-changed` reaches the relay within the hold bar (p95
/// and max under 5 s), the relay refuses nothing and no minute carries past its 100. RED on main (the hold 10
/// windows, a state forced only after 20 s): the fleet's demand sits past the sender bucket's 80 a minute and under
/// the relay's 100, so main's first-come queue grows through the burst and its holds run to the 20 s bound (L56's 18
/// to 26 s).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix1017_a_a_33_pair_burst_reaches_every_seat_within_the_hold_bar() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (lat, fake, gate) = l56_burst().await;
            let max_minute = fake.window.borrow().values().copied().max().unwrap_or(0);
            let (p50, p95, max) = p95(lat.clone());
            println!(
                "fix1017_a: {} pushes, {} relay calls, {} refused, busiest relay minute {max_minute}, delivery ms p50 {p50} p95 {p95} max {max}",
                lat.len(),
                fake.calls.borrow().len(),
                fake.refused()
            );
            assert_eq!(fake.refused(), 0, "no ERR_SEND_BUDGET at the relay");
            assert!(max_minute <= 100, "{max_minute} sends in one relay minute");
            assert!(p95 <= 5_000, "the delivery p95 {p95} ms is past the 5 s bar");
            assert!(max <= 5_000, "a seat's push waited {max} ms");
            // The gate's own figures (what `/health.push.gate` serves) read the same minute under the bar.
            let f = gate.borrow_mut().hold_figures(fake.now_ms());
            println!("fix1017_a: the gate's hold over its last minute {f:?}");
            assert!(f.samples > 0 && f.hold_ms_p95 <= PUSH_HOLD_BAR_MS, "{f:?}");
        })
        .await;
}

/// (fix1017_b) THE NEW KIND ENDS THE HOLD. A sender bucket kept spent (4 a minute: one token held), the relay's
/// line wide open: a seat's `owed-changed` is held at its window's end; a `pot` event for the seat at 2.5 s sends
/// BOTH at once (the coalesced set), its waiter answered delivered. RED on main: the pot waits behind the held
/// owed-changed for the bucket's refill or the 20 s bound.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix1017_b_a_new_kind_behind_a_held_state_sends_the_seats_set_at_once() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let fake = Fake::new();
            let cell = Rc::new(RefCell::new(Coalescer::default()));
            let knobs = Knobs::parse(None, None, Some("4"));
            let t0 = Instant::now();
            let mut hs = Vec::new();
            for (at, body) in [(0u64, owed(0)), (500, owed(1)), (2_500, pot(0xab, 1))] {
                let (fake, cell) = (fake.clone(), cell.clone());
                hs.push(tokio::task::spawn_local(async move {
                    tokio::time::sleep_until(t0 + Duration::from_millis(at)).await;
                    submit(&cell, &*fake, &knobs, SEAT, None, body).await
                }));
            }
            for h in hs {
                assert_eq!(h.await.unwrap(), Answer::Sent);
            }
            let sent: Vec<(u64, String)> = fake
                .delivered()
                .iter()
                .map(|(t, b)| (t - BASE_MS, b["kind"].as_str().unwrap().to_string()))
                .collect();
            println!("fix1017_b: sends {sent:?}");
            let at = |k: &str, n: usize| sent.iter().filter(|s| s.1 == k).nth(n).map(|s| s.0);
            assert_eq!(at("owed-changed", 0), Some(0), "the leading edge");
            assert_eq!(at("pot", 0), Some(2_500), "the pot goes when it arrives");
            assert_eq!(
                at("owed-changed", 1),
                Some(2_500),
                "the held owed-changed goes with it"
            );
            assert_eq!(
                fake.delivered().last().unwrap().1["kind"],
                "pot",
                "nothing left behind"
            );
        })
        .await;
}

/// (fix1017_c) A SUSTAINED BURST SENDS EVERY HOLD WITH THE COALESCED SET, never past the relay's line. One seat, two
/// keys pushed every 300 ms for a minute, the sender bucket at 4 a minute: each key goes at least every two windows
/// (its hold bar), carrying its newest state. RED on main: held to the 20 s bound between sends.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix1017_c_a_sustained_burst_sends_its_set_within_every_hold_bar() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let fake = Fake::new();
            let cell = Rc::new(RefCell::new(Coalescer::default()));
            let knobs = Knobs::parse(None, None, Some("4"));
            let t0 = Instant::now();
            let mut hs = Vec::new();
            for e in 0..200u64 {
                let (fake, cell) = (fake.clone(), cell.clone());
                let body = if e % 2 == 0 { owed(e) } else { pot(0xcd, e) };
                hs.push(tokio::task::spawn_local(async move {
                    tokio::time::sleep_until(t0 + Duration::from_millis(e * 300)).await;
                    submit(&cell, &*fake, &knobs, SEAT, None, body).await
                }));
            }
            for h in hs {
                assert_eq!(h.await.unwrap(), Answer::Sent);
            }
            assert_eq!(fake.refused(), 0);
            for kind in ["owed-changed", "pot"] {
                let t: Vec<u64> = fake
                    .delivered()
                    .iter()
                    .filter(|(_, b)| b["kind"] == kind)
                    .map(|(t, _)| t - BASE_MS)
                    .collect();
                let gaps: Vec<u64> = t.windows(2).map(|w| w[1] - w[0]).collect();
                println!("fix1017_c: {kind} sends {} gaps {gaps:?}", t.len());
                assert!(
                    gaps.iter().all(|g| *g <= 3 * PUSH_COALESCE_MS_DEFAULT),
                    "{kind}: a gap past the hold bar and its window: {gaps:?}"
                );
            }
            let f = cell.borrow_mut().hold_figures(fake.now_ms());
            assert!(f.hold_ms_max <= 2 * PUSH_COALESCE_MS_DEFAULT, "{f:?}");
        })
        .await;
}

/// (fix1017_d) THE SERVED FIGURE. The gate writes its hold over the trailing minute to one row of the belt's ledger
/// at most every 10 s and only with a sample; `/health` reads it as a primary-key row of the consumer statement it
/// already runs (no statement more) and serves `push.gate` {holdMsP50, holdMsP95, holdMsMax, samples, barMs,
/// overBar, ageMs}; `null` before the gate's first write. New API: does not compile on main.
#[test]
fn fix1017_d_the_gates_hold_is_served_on_health() {
    let cell = RefCell::new(Coalescer::default());
    let t = BASE_MS;
    assert!(
        gate_hold_stmt_due(&cell, t).is_none(),
        "no sample, no write"
    );
    for (i, h) in [0u64, 100, 2_000, 4_000, 6_000].into_iter().enumerate() {
        cell.borrow_mut().note_send(t + i as u64, Some(h));
    }
    let f = cell.borrow_mut().hold_figures(t + 10);
    assert_eq!(
        (f.samples, f.hold_ms_p50, f.hold_ms_p95, f.hold_ms_max),
        (5, 2_000, 6_000, 6_000)
    );
    let stmt = gate_hold_stmt_due(&cell, t + 10).expect("a write is due");
    assert_eq!(stmt.sql, GATE_HOLD_WRITE_SQL);
    assert!(
        gate_hold_stmt_due(&cell, t + 5_000).is_none(),
        "at most every 10 s"
    );
    assert!(gate_hold_stmt_due(&cell, t + 10 + GATE_HOLD_WRITE_EVERY_MS).is_some());
    // Past the minute the samples age out.
    assert_eq!(cell.borrow_mut().hold_figures(t + 70_000).samples, 0);
    // The view.
    assert_eq!(gate_hold_view(None, t), Value::Null);
    let v = gate_hold_view(Some(&gate_hold_cursor(&f, t)), t + 3_000);
    assert_eq!(v["holdMsP95"], 6_000);
    assert_eq!(v["holdMsP50"], 2_000);
    assert_eq!(v["holdMsMax"], 6_000);
    assert_eq!(v["barMs"], PUSH_HOLD_BAR_MS);
    assert_eq!(v["overBar"], true);
    assert_eq!(v["ageMs"], 3_000);
    let quiet = gate_hold_view(Some(&gate_hold_cursor(&HoldFigures::default(), t)), t);
    assert_eq!(quiet["overBar"], Value::Null, "no sample is no verdict");
    // A REAL-bound stamp reads (D1 binds a JS number).
    let real = r#"{"asOfMs":1790000000000.0,"samples":2,"holdMsP50":10.0,"holdMsP95":20.0,"holdMsMax":20.0}"#;
    assert_eq!(gate_hold_view(Some(real), t)["holdMsP95"], 20);
    // The wiring: the gate writes after its pushes; `/health` reads the row in its existing statement.
    assert!(crate::owed_events::HEALTH_CONSUMER_SQL.contains(&format!("'{GATE_HOLD_NAME}'")));
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let gate = squash(include_str!("push_coalesce.rs"));
    let serve = &gate[gate.find("pubasyncfnserve_gate(").unwrap()..];
    let serve = &serve[..serve.find("pubasyncfnpush_to_seat(").unwrap()];
    assert!(serve.contains("gate_hold_stmt_due(cell,port.now_ms())"));
    let routes = squash(include_str!("routes.rs"));
    assert!(routes.contains("owed_events::health_actor_fleet_push(&sql,&dirty)"));
    assert!(routes.contains("body[\"push\"][\"gate\"]=push_gate;"));
}
