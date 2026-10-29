//! FIX-956 (bsv-low #956): the owed-events sweep re-publishes only a mark the consumer has PASSED, at most once a
//! pass; a pair a publisher knows it lost is re-published at the next tick; in Phase C a direct apply that landed
//! acknowledges its marks. The shipped statements over the overlay's migrations (`SqliteSql::migrated`).
//!
//! The pins above the cut line use only API the base `26f7443c` already has (`sweep`, `health`, raw rows), so the
//! file cut there runs on the base, and each is RED there. Below it (s956_c2, d2, e, f2) the fold's new API: they do
//! not compile on the base.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::pot_hook::tests::SqliteSql;
use std::cell::RefCell;

const ID: &str = "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const T0: i64 = 1_800_000_000_000;
const TICK: i64 = 120_000;

fn mark(db: &SqliteSql, game: &str, source: &str, key: &str, at: i64) {
    db.conn
        .execute_batch(&format!(
            "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('{ID}','{game}','{source}','{key}',{at})"
        ))
        .unwrap();
}

/// The consumer's row as a batch acknowledging a message published at `horizon` writes it (raw: the base has no
/// writer of the field).
fn consumer_at(db: &SqliteSql, horizon: i64, now: i64) {
    db.conn
        .execute_batch(&format!(
            "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('owed-events:consumer','{{\"atMs\":{now},\"horizonMs\":{horizon}}}',{now}) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs"
        ))
        .unwrap();
}

fn int(db: &SqliteSql, sql: &str) -> i64 {
    db.conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

fn hop() -> (String, String) {
    ("hop".to_string(), format!("{}:0", "ab".repeat(32)))
}

/// s956_a: THE CONSUMER HAS NOT REACHED THE MARK: NOTHING IS RE-PUBLISHED. A mark three ticks old, the consumer's
/// horizon a tick BEHIND it (its own message still waits in the queue), the long bound not yet passed: the sweep
/// re-publishes nothing for it, tick after tick, until the long bound. RED on `26f7443c`: the pair re-published at
/// the first tick past one tick (`left: [("hop", ..)] right: []`).
#[tokio::test]
async fn s956_a_a_mark_the_consumer_has_not_reached_is_not_republished() {
    let db = SqliteSql::migrated();
    let (s, k) = hop();
    mark(&db, "g1", &s, &k, T0);
    consumer_at(&db, T0 - TICK, T0 + 3 * TICK);
    for t in [
        T0 + TICK + 1,
        T0 + 2 * TICK,
        T0 + 3 * TICK,
        T0 + 5 * TICK - 1,
    ] {
        let pairs = sweep(&db, t).await.unwrap();
        assert_eq!(pairs, Vec::<Pair>::new(), "at {}: not yet consumed", t - T0);
    }
    // the long bound is the belt
    assert_eq!(sweep(&db, T0 + 5 * TICK + 1).await.unwrap(), vec![(s, k)]);
}

/// s956_b: THE HORIZON PASSED THE MARK: RE-PUBLISHED ONCE, AND NOT AGAIN BEFORE THE HORIZON PASSES THE RE-PUBLISH.
/// RED on `26f7443c`: the walk wraps and the second tick re-publishes it again (`left: [..] right: []`).
#[tokio::test]
async fn s956_b_a_passed_mark_is_republished_once_a_pass() {
    let db = SqliteSql::migrated();
    let (s, k) = hop();
    mark(&db, "g1", &s, &k, T0);
    // the consumer judged a message published two ticks after the mark: this mark's own message is lost
    let first = T0 + 2 * TICK + 1;
    consumer_at(&db, T0 + 2 * TICK, first);
    assert_eq!(
        sweep(&db, first).await.unwrap(),
        vec![(s.clone(), k.clone())],
        "re-published once the horizon passed it by the margin"
    );
    // the consumer moves on, but not yet past the re-publish by the margin: never twice
    for (h, t) in [
        (first, first + TICK),
        (first + TICK / 2, first + 2 * TICK),
        (first + TICK, first + 3 * TICK),
    ] {
        consumer_at(&db, h, t);
        assert_eq!(
            sweep(&db, t).await.unwrap(),
            Vec::<Pair>::new(),
            "at {}: re-published once a pass",
            t - first
        );
    }
    // the horizon passed the re-publish too and the mark stands: that message was lost as well
    let t = first + 4 * TICK;
    consumer_at(&db, first + TICK + 1, t);
    assert_eq!(sweep(&db, t).await.unwrap(), vec![(s, k)]);
}

/// s956_c: A STALLED CONSUMER: the long bound (five ticks) re-publishes a mark older than it whatever the horizon
/// (absent, or stuck far behind), once a long bound. RED on `26f7443c` at its first assertion (re-published at one
/// tick); RED against `sweep_bound` without its long-bound term (nothing re-published under a dead consumer).
#[tokio::test]
async fn s956_c_a_stalled_consumer_falls_to_the_long_bound() {
    const STALLED: i64 = 600_000;
    for horizon in [None, Some(T0 - 3_600_000)] {
        let db = SqliteSql::migrated();
        let (s, k) = hop();
        mark(&db, "g1", &s, &k, T0);
        if let Some(h) = horizon {
            consumer_at(&db, h, T0);
        }
        assert!(
            sweep(&db, T0 + STALLED - 1).await.unwrap().is_empty(),
            "{horizon:?}: inside the long bound"
        );
        assert_eq!(
            sweep(&db, T0 + STALLED + 1).await.unwrap(),
            vec![(s.clone(), k.clone())],
            "{horizon:?}: the long bound"
        );
        // and once a long bound, never every tick
        assert!(sweep(&db, T0 + STALLED + TICK).await.unwrap().is_empty());
    }
}

/// s956_d: A LOST NOTE'S PAIR IS RE-PUBLISHED AT THE NEXT TICK, its note cleared, whatever the horizon (here none,
/// and the mark younger than a tick). RED on `26f7443c`: nothing swept, the note kept.
#[tokio::test]
async fn s956_d_a_lost_notes_pair_is_republished_at_the_next_tick() {
    let db = SqliteSql::migrated();
    let (s, k) = hop();
    mark(&db, "g1", &s, &k, T0);
    db.conn
        .execute_batch(&format!(
            "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('owed-events:lost:{s} {k}','{{\"source\":\"{s}\",\"sourceKey\":\"{k}\",\"atMs\":{T0}}}',{T0})"
        ))
        .unwrap();
    assert_eq!(
        sweep(&db, T0 + 10).await.unwrap(),
        vec![(s.clone(), k.clone())]
    );
    assert_eq!(
        int(
            &db,
            "SELECT COUNT(*) FROM owed_belt_state WHERE name LIKE 'owed-events:lost:%'"
        ),
        0,
        "the note cleared in the sweep's batch"
    );
    assert!(sweep(&db, T0 + 20).await.unwrap().is_empty(), "once");
}

/// s956_f: THE HEALTH SERVES THE HORIZON: `/health.owedEvents.horizonMs` and `horizonAgeMs`, from the consumer's row
/// (no statement added); the consumer row's write never lowers it. RED on `26f7443c`: the fields absent (`Null`).
#[tokio::test]
async fn s956_f_the_health_serves_the_horizon() {
    let db = SqliteSql::migrated();
    db.now.set((T0 + 5_000) as u64);
    consumer_at(&db, T0, T0 + 1_000);
    let body = health(&db, &json!({})).await;
    assert_eq!(body["horizonMs"], json!(T0));
    assert_eq!(body["horizonAgeMs"], json!(5_000));
}

/// The issue's shape (#956): a consumer lagging past one tick (here four), 40 marks published one a tick, none lost:
/// the old rule re-published every mark once it was a tick old; the new re-publishes none. Printed for the PR.
#[tokio::test]
async fn s956_g_a_lagging_consumer_costs_no_republish() {
    let db = SqliteSql::migrated();
    let lag = 4 * TICK;
    let mut republished = 0usize;
    for i in 0..40i64 {
        let t = T0 + i * TICK;
        mark(
            &db,
            &format!("g{i}"),
            "hop",
            &format!("{}:{i}", "ab".repeat(32)),
            t,
        );
        // the consumer acknowledges the message published `lag` ago (its marks gone with it)
        let done = t - lag;
        db.conn
            .execute_batch(&format!("DELETE FROM owed_dirty WHERE markedAtMs<={done}"))
            .unwrap();
        if i >= 4 {
            consumer_at(&db, done, t);
        }
        republished += sweep(&db, t).await.unwrap().len();
    }
    println!("s956_g: 40 marks under a consumer {lag} ms behind: {republished} re-publishes");
    assert_eq!(republished, 0);
}

// ---- the cut line: the fold's new API below ----

/// s956_c2: the sweep's one bound, pure.
#[test]
fn s956_c2_the_sweep_bound() {
    assert_eq!(SWEEP_STALLED_AFTER_MS, 600_000);
    assert_eq!(sweep_bound(T0, None), T0 - SWEEP_STALLED_AFTER_MS);
    assert_eq!(sweep_bound(T0, Some(T0)), T0 - SWEEP_AFTER_MS);
    assert_eq!(sweep_bound(T0, Some(T0 - 3 * TICK)), T0 - 4 * TICK);
    assert_eq!(sweep_reason(T0, Some(T0 + TICK + 1)), SweepReason::Passed);
    assert_eq!(sweep_reason(T0, Some(T0 + TICK)), SweepReason::Stalled);
    assert!(swept_blocks(T0, T0 + TICK, Some(T0 + TICK)));
    assert!(!swept_blocks(T0, T0 + TICK, Some(T0 + TICK + 1)));
    assert!(!swept_blocks(T0, T0 + SWEEP_STALLED_AFTER_MS, None));
}

/// s956_d2: the PUBLISHER writes the note: a send that faults after the seq batch notes every unsent pair; a publish
/// the answer wall dropped is noted by the isolate's next publish (the pending set); a frame or a `txfacts` event is
/// never noted. Then the sweep takes it with the reason `Lost`, counted.
#[tokio::test]
async fn s956_d2_the_publisher_notes_what_it_lost() {
    let db = SqliteSql::migrated();
    db.now.set(T0 as u64);
    let (s, k) = hop();
    mark(&db, "g1", &s, &k, T0);
    let refused = SqlPublish {
        db: &db,
        send: |_: Vec<String>| async { Err("the queue refused the batch".to_string()) },
    };
    let note = publish::Noted {
        source: s.clone(),
        key: k.clone(),
        held: None,
    };
    let report = publish::publish_with(&refused, vec![note.clone()]).await;
    assert!(report.fault.is_some());
    assert_eq!(publish::lost_of(&report), vec![note.clone()]);
    assert_eq!(
        publish::write_lost(&refused, publish::lost_of(&report)).await,
        1
    );
    // the wall's pending set: a frame and a txfacts note are never noted
    publish::note_lost(vec![
        publish::Noted {
            source: "pot".into(),
            key: "cd".repeat(32) + ":1",
            held: None,
        },
        publish::Noted {
            source: "word".into(),
            key: "ef".repeat(32),
            held: Some(json!({"frame": {}})),
        },
        publish::Noted {
            source: owed_event::TXFACTS.into(),
            key: "ef".repeat(32),
            held: None,
        },
    ]);
    let pending = publish::take_lost();
    assert_eq!(pending.len(), 1, "{pending:?}");
    assert_eq!(publish::write_lost(&refused, pending).await, 1);
    assert_eq!(
        int(
            &db,
            "SELECT COUNT(*) FROM owed_belt_state WHERE name LIKE 'owed-events:lost:%'"
        ),
        2
    );
    let swept = sweep_tick(&db, T0 + 1).await.unwrap();
    assert_eq!(swept.count(SweepReason::Lost), 2, "{swept:?}");
    // the pot note has no mark: the publish skips it; both notes are cleared
    assert_eq!(
        int(
            &db,
            "SELECT COUNT(*) FROM owed_belt_state WHERE name LIKE 'owed-events:lost:%'"
        ),
        0
    );
    // the publish's source carries the note at its two fault sites
    let src: String = include_str!("../../../overlay-cloudflare/src/owed_events.rs")
        .split_whitespace()
        .collect();
    let env_fn = &src[src.find("pubasyncfnpublish_env(").unwrap()..];
    assert!(env_fn.contains("letmutlost=take_lost();lost.extend(lost_of(&report));"));
    let wall = &src[src.find("pubasyncfnpublish_before_answer(").unwrap()..];
    assert!(wall.contains("note_wall_drop();note_lost(carried);"));
}

/// A direct port over the SQLite record: one subject, the actor's answer scripted, and (the write's race) a mark
/// written while the call is in flight.
struct AckPort<'a> {
    db: &'a SqliteSql,
    phase_c: bool,
    answer: Result<Value, String>,
    calls: RefCell<u32>,
}
impl DirectPort for AckPort<'_> {
    async fn subjects(&self, _ev: &OwedEvent) -> Result<Vec<(String, Vec<String>)>, String> {
        Ok(vec![(ID.into(), vec!["g1".into(), "g2".into()])])
    }
    async fn call(&self, _body: Value) -> Result<Value, String> {
        *self.calls.borrow_mut() += 1;
        // a mark written after the bound was read: never acknowledged by this answer
        mark(self.db, "g1", "word", "late", T0 + 5);
        self.answer.clone()
    }
    fn acks(&self) -> bool {
        self.phase_c
    }
    async fn bound(&self, identity: &str, games: &[String]) -> Result<i64, String> {
        direct_bound(self.db, identity, games).await
    }
    async fn ack(&self, stmts: Vec<Stmt>) -> Result<(), String> {
        self.db.run_batch(stmts).await.map(|_| ())
    }
}

fn direct_event() -> OwedEvent {
    OwedEvent {
        v: owed_event::OWED_EVENT_V,
        seq: 7,
        source: "hop".into(),
        source_key: hop().1,
        facts: Value::Null,
        at_ms: T0,
    }
}

/// s956_e: PHASE C: A DIRECT CALL ANSWERED APPLIED ACKNOWLEDGES ITS MARKS (up to the generation read before the call:
/// the mark written during the call stays), and the games it did not apply keep theirs; a walled call, a held answer
/// and Phases A and B acknowledge nothing. New API (`DirectPort::acks`).
#[tokio::test]
async fn s956_e_a_phase_c_direct_apply_acknowledges_its_marks() {
    let applied =
        Ok(json!({"applied": true, "firstApply": true, "appliedGames": ["g1"], "atMs": T0}));
    let cases: [(bool, Result<Value, String>, i64, u64); 5] = [
        // (Phase C, the answer, the marks left, acked)
        (true, applied.clone(), 2, 1),
        (false, applied, 4, 0),
        (true, Err("owed actor: no answer in 2000 ms".into()), 4, 0),
        (
            true,
            Ok(json!({"held": true, "heldGames": ["g1", "g2"]})),
            4,
            0,
        ),
        (true, Err("owed actor answered 503".into()), 4, 0),
    ];
    for (phase_c, answer, left, acked) in cases {
        let db = SqliteSql::migrated();
        let (s, k) = hop();
        mark(&db, "g1", &s, &k, T0);
        mark(&db, "g2", &s, &k, T0);
        mark(&db, "g1", "pot", "p", T0 + 1);
        let port = AckPort {
            db: &db,
            phase_c,
            answer: answer.clone(),
            calls: RefCell::new(0),
        };
        let report = direct_with(&port, vec![direct_event()], None).await;
        assert_eq!(*port.calls.borrow(), 1);
        assert_eq!(
            (int(&db, "SELECT COUNT(*) FROM owed_dirty"), report.acked),
            (left, acked),
            "phase C {phase_c}, {answer:?}"
        );
        if acked > 0 {
            // what stands: g2's (not applied) and g1's late mark (written after the bound)
            assert_eq!(
                int(
                    &db,
                    "SELECT COUNT(*) FROM owed_dirty WHERE gameId='g2' OR sourceKey='late'"
                ),
                2
            );
            assert_eq!(
                int(
                    &db,
                    "SELECT COUNT(*) FROM view_derivation_state WHERE name='owed-actor-judged'"
                ),
                1,
                "a Phase C acknowledgement, as the consumer's"
            );
        }
    }
    // the worker's port reads the phase from the D1 judge's flag
    let src: String = include_str!("../owed_events.rs")
        .split_whitespace()
        .collect();
    let worker = &src[src.find("implDirectPortforWorkerDirect{").unwrap()..];
    assert!(worker.contains("fnacks(&self)->bool{!crate::owed_actor::d1_judge_on("));
}

/// The consumer row's write: the horizon of a batch is its acknowledged messages' greatest publish stamp, never
/// lowered by a later batch acknowledging older (or no) messages. New API.
#[tokio::test]
async fn s956_f2_the_consumer_row_keeps_the_greatest_horizon() {
    let db = SqliteSql::migrated();
    for (h, expect) in [
        (Some(T0), T0),
        (Some(T0 - 9), T0),
        (None, T0),
        (Some(T0 + 3), T0 + 3),
    ] {
        let row = consumer_row(T0 + 100, 0, 1, 1, h);
        db.run_batch(vec![consumer_row_stmt(&row, T0 + 100)])
            .await
            .unwrap();
        assert_eq!(
            int(&db, "SELECT json_extract(cursor,'$.horizonMs') FROM owed_belt_state WHERE name='owed-events:consumer'"),
            expect,
            "{h:?}"
        );
    }
    // the queue handler records the acknowledged messages' publish stamps
    let src: String = include_str!("../owed_events.rs")
        .split_whitespace()
        .collect();
    let q = &src[src.find("pubasyncfnqueue(").unwrap()..];
    let q = &q[..q.find("asyncfnfinish_batch(").unwrap()];
    assert_eq!(
        q.matches("horizon=horizon.max(Some(ev.at_ms));msg.ack();")
            .count(),
        2
    );
    assert_eq!(q.matches("horizon=horizon.max(Some(ev.at_ms));").count(), 2);
}
