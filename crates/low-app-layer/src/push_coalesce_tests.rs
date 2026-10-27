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
    // FIX-935-FOLD: the judge QUEUES its push; the queue is flushed through `push_to_seat` (`flush_queued`).
    assert!(judge.contains(&squash(
        "crate::push_coalesce::queue_seat_push(identity, body);"
    )));
    assert!(squash(include_str!("push_coalesce.rs"))
        .contains(&squash("push_to_seat(env, &identity, None, body).await;")));
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
