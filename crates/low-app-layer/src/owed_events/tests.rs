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

// ---- D2-ACTOR-B-FOLD-12 (the seventh delta lens's NEW7-M1, NEW7-L1, NEW7-L3) ----

/// Binds every `Bind::Int` as a REAL while held, as the D1 port does (`routes.rs`: `JsValue::from_f64`).
struct D1Real;
impl D1Real {
    fn on() -> Self {
        crate::pot_hook::tests::INTS_AS_REAL.with(|c| c.set(true));
        D1Real
    }
}
impl Drop for D1Real {
    fn drop(&mut self) {
        crate::pot_hook::tests::INTS_AS_REAL.with(|c| c.set(false));
    }
}

/// fold12_a (NEW7-M1): THE SWEPT STAMP BLOCKS ON D1. `s956_b`'s shape and `s956_c`'s, with every integer bound as the
/// D1 port binds it (a REAL): the stamp is written and read back, so a passed pair is re-published once a pass (0 in
/// the three ticks the stamp blocks) and a stalled consumer's pair once a long bound (2 in ten ticks). A stamp an
/// older build stored as a REAL (`{"sweptAtMs":1800000240001.0}`) blocks too (the reader's `ms_of`). RED against
/// `fd57eb831`'s write and read (`json_object('sweptAtMs',?2)`, `as_i64`): the stored stamp
/// `{"sweptAtMs":1800000240001.0}`, `left: 3, right: 0`.
#[tokio::test]
async fn fold12_a_the_swept_stamp_blocks_on_d1() {
    let _real = D1Real::on();
    let db = SqliteSql::migrated();
    let (s, k) = hop();
    mark(&db, "g1", &s, &k, T0);
    let first = T0 + 2 * TICK + 1;
    consumer_at(&db, T0 + 2 * TICK, first);
    assert_eq!(
        sweep(&db, first).await.unwrap(),
        vec![(s.clone(), k.clone())]
    );
    let stored: String = db
        .conn
        .query_row(
            "SELECT cursor FROM owed_belt_state WHERE name LIKE 'owed-events:swept:%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let mut blocked = 0usize;
    for (h, t) in [
        (first, first + TICK),
        (first + TICK / 2, first + 2 * TICK),
        (first + TICK, first + 3 * TICK),
    ] {
        consumer_at(&db, h, t);
        blocked += sweep(&db, t).await.unwrap().len();
    }
    assert_eq!(blocked, 0, "re-published while its stamp blocks ({stored})");
    // the horizon passed the re-publish: once more
    let t = first + 4 * TICK;
    consumer_at(&db, first + TICK + 1, t);
    assert_eq!(sweep(&db, t).await.unwrap(), vec![(s.clone(), k.clone())]);

    // a stalled consumer (no horizon): ten ticks past the long bound re-publish twice (once a long bound)
    let db = SqliteSql::migrated();
    mark(&db, "g1", &s, &k, T0);
    let mut stalled = 0usize;
    for n in 0..10i64 {
        stalled += sweep(&db, T0 + 600_000 + 1 + n * TICK).await.unwrap().len();
    }
    assert_eq!(stalled, 2, "a stalled consumer's pair once a long bound");

    // a stamp an older build stored as a REAL blocks
    let db = SqliteSql::migrated();
    mark(&db, "g1", &s, &k, T0);
    let at = first;
    db.conn
        .execute_batch(&format!(
            "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('{}',json_object('sweptAtMs',CAST({at} AS REAL)),CAST({at} AS REAL))",
            swept_memo_name(&s, &k)
        ))
        .unwrap();
    consumer_at(&db, first, first + TICK);
    assert!(
        sweep(&db, first + TICK).await.unwrap().is_empty(),
        "a REAL stamp blocks"
    );
    assert_eq!(
        ms_of(&json!(1_800_000_240_001.0_f64)),
        Some(1_800_000_240_001)
    );
}

/// fold12_b (NEW7-L1): THE LOST READ AND THE PRUNE READ AT MOST WHAT THEY TAKE. (1) Over the lens's sizes (5,000 lost
/// notes, 20,000 young stamps) each shipped read is a primary-key range with no sort (`EXPLAIN QUERY PLAN` names no
/// `TEMP B-TREE`) and its VM steps are bounded by its LIMIT, not the range (printed; the lens measured 40,614 and
/// 100,023). (2) A lost note held by its fault memo never keeps a newer note out of the tick's 32 places: 32 held
/// notes, older, then one newer: the cursor passes the held ones and the newer pair is re-published at the second
/// tick. (3) The prune's window deletes the day-old stamps it reads and wraps. RED against `fd57eb831`'s
/// `LOST_READ_SQL` at (2): the newer note never read (`ORDER BY tickedAtMs`: the held notes take every place, tick
/// after tick; `left: [] right: [("hop", "ff..:0")]`). Measured: 238 VM steps for the 32 notes taken of 5,000 and 462
/// for the 64 stamps read of 20,000 (the lens: 40,614 and 100,023).
#[tokio::test]
async fn fold12_b_the_lost_read_and_the_prune_read_at_most_what_they_take() {
    // (2) first: it uses only `sweep`, as on the base
    let db = SqliteSql::migrated();
    let now = T0 + 10 * TICK;
    for i in 0..32 {
        let key = format!("{:064x}:0", i + 1);
        db.conn
            .execute_batch(&format!(
                "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('owed-events:lost:hop {key}','{{\"source\":\"hop\",\"sourceKey\":\"{key}\",\"atMs\":{T0}}}',{T0});
                 INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('owed-events:fault:hop {key}','{{\"faults\":3,\"heldUntilMs\":{}}}',{T0})",
                now + 3_600_000
            ))
            .unwrap();
    }
    let newer = format!("{}:0", "f".repeat(64));
    db.conn
        .execute_batch(&format!(
            "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('owed-events:lost:hop {newer}','{{\"source\":\"hop\",\"sourceKey\":\"{newer}\",\"atMs\":{now}}}',{now})"
        ))
        .unwrap();
    let mut got = Vec::new();
    for n in 0..3 {
        got.extend(sweep(&db, now + n * TICK).await.unwrap());
    }
    assert_eq!(
        got,
        vec![("hop".to_string(), newer.clone())],
        "the newer note is read past the held ones"
    );

    // (1) the plans and the VM steps over the lens's sizes
    let db = SqliteSql::migrated();
    let mut sql = String::from("BEGIN;");
    for i in 0..5_000 {
        sql.push_str(&format!(
            "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('owed-events:lost:hop {i:064x}:0','{{}}',{});",
            T0 + i
        ));
    }
    for i in 0..20_000 {
        sql.push_str(&format!(
            "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('owed-events:swept:hop {i:064x}:0','{{}}',{});",
            T0 + i
        ));
    }
    sql.push_str("COMMIT;");
    db.conn.execute_batch(&sql).unwrap();
    let plan = |q: &str, binds: &[rusqlite::types::Value]| -> String {
        let mut st = db.conn.prepare(&format!("EXPLAIN QUERY PLAN {q}")).unwrap();
        let rows: Vec<String> = st
            .query_map(rusqlite::params_from_iter(binds.iter()), |r| {
                r.get::<_, String>(3)
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        rows.join(" | ")
    };
    let steps = |q: &str, binds: &[rusqlite::types::Value]| -> (usize, i32) {
        let mut st = db.conn.prepare(q).unwrap();
        let n = st
            .query_map(rusqlite::params_from_iter(binds.iter()), |_| Ok(()))
            .unwrap()
            .count();
        (n, st.get_status(rusqlite::StatementStatus::VmStep))
    };
    use rusqlite::types::Value as V;
    for (q, binds, limit) in [
        (
            LOST_READ_SQL,
            vec![
                V::Integer(SWEEP_LOST_PER_TICK as i64),
                V::Text(publish::LOST_MEMO_PREFIX.into()),
            ],
            SWEEP_LOST_PER_TICK,
        ),
        (
            SWEPT_WINDOW_SQL,
            vec![
                V::Text(SWEPT_MEMO_PREFIX.into()),
                V::Integer(SWEPT_PRUNE_WINDOW as i64),
            ],
            SWEPT_PRUNE_WINDOW,
        ),
    ] {
        let p = plan(q, &binds);
        assert!(!p.contains("TEMP B-TREE"), "{q}: {p}");
        assert!(
            p.contains("PRIMARY KEY") || p.contains("autoindex"),
            "{q}: {p}"
        );
        let (n, vm) = steps(q, &binds);
        println!("fold12_b: {n} rows, {vm} VM steps, plan {p}: {q}");
        assert_eq!(n, limit);
        assert!(vm < 40 * limit as i32, "{q}: {vm} VM steps for {n} rows");
    }

    // (3) the prune: the window's day-old stamps deleted, a young one kept, the cursor wrapping on a short window
    let db = SqliteSql::migrated();
    let late = T0 + 25 * 3_600_000;
    for i in 0..70 {
        let at = if i == 5 { late } else { T0 };
        db.conn
            .execute_batch(&format!(
                "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('owed-events:swept:hop {i:064x}:0','{{}}',{at})"
            ))
            .unwrap();
    }
    let count = |db: &SqliteSql| {
        int(
            db,
            "SELECT COUNT(*) FROM owed_belt_state WHERE name LIKE 'owed-events:swept:%'",
        )
    };
    sweep(&db, late + 1).await.unwrap();
    assert_eq!(
        count(&db),
        70 - 63,
        "the first window: 63 of its 64 a day old"
    );
    sweep(&db, late + 2).await.unwrap();
    assert_eq!(
        count(&db),
        1,
        "the second window: the rest, then the cursor wraps"
    );
    let cursor: String = db
        .conn
        .query_row(
            "SELECT cursor FROM owed_belt_state WHERE name='owed-events:sweep'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let c: Value = serde_json::from_str(&cursor).unwrap();
    assert_eq!(c["pruneAfter"], json!(SWEPT_MEMO_PREFIX), "wrapped: {c}");
}

/// fold12_c (NEW7-L3): EACH IDENTITY'S APPLY CARRIES ITS OWN EVENTS. A batch of two identities with two events each:
/// each identity's body names its own oldest event and the other in `also`, so the four events are sampled four times
/// (each by its own first apply in its own actor), never eight, and each sample is that event's own lag. RED against
/// `coalesced_event_of` answering `coalesced_event` (the base's one context for the whole batch): the first identity's
/// body carries every other event of the batch (`left: [["pot",13],["pot",12],["pot",11]] right: [["pot",11]]`), so
/// each identity's fresh actor samples all four: 8.
#[test]
fn fold12_c_each_identitys_apply_carries_its_own_also_events() {
    let ev = |seq: i64, at: i64, key: &str| OwedEvent {
        v: 1,
        seq,
        source: "pot".into(),
        source_key: format!("{key}:0"),
        facts: Value::Null,
        at_ms: at,
    };
    let (a1, a2, b1, b2) = (
        ev(10, 1_000, &"a1".repeat(32)),
        ev(11, 2_000, &"a2".repeat(32)),
        ev(12, 1_500, &"b1".repeat(32)),
        ev(13, 3_000, &"b2".repeat(32)),
    );
    let (ida, idb) = (
        format!("02{}", "a".repeat(64)),
        format!("03{}", "b".repeat(64)),
    );
    let plan_of = |id: &str, game: &str| {
        let mut p = Plan::default();
        p.order.push(id.to_string());
        p.games.insert(id.to_string(), vec![game.to_string()]);
        p
    };
    let (pa1, pa2, pb1, pb2) = (
        plan_of(&ida, "ga"),
        plan_of(&ida, "ga"),
        plan_of(&idb, "gb"),
        plan_of(&idb, "gb"),
    );
    let ctx = coalesced_event_of(&[(&b2, &pb2), (&a1, &pa1), (&b1, &pb1), (&a2, &pa2)]).unwrap();
    let now = 10_000u64;
    let mut samples = Vec::new();
    for (id, own) in [
        (&ida, [(10, 1_000), (11, 2_000)]),
        (&idb, [(12, 1_500), (13, 3_000)]),
    ] {
        let body = crate::owed_actor::apply_body(id, &["g".to_string()], &ctx, Some(900));
        assert_eq!(
            (body["seq"].clone(), body["atMs"].clone()),
            (json!(own[0].0), json!(own[0].1)),
            "{id}: its own oldest"
        );
        assert_eq!(
            body["also"],
            json!([["pot", own[1].0]]),
            "{id}: only its own other event"
        );
        // a fresh actor: every event it carries is a first apply born warm
        let mut events = vec![(body["atMs"].as_u64(), true, true)];
        for at in body["alsoAtMs"].as_array().into_iter().flatten() {
            events.push((at.as_u64(), true, true));
        }
        samples.extend(crate::owed_actor::event_visibility(now, &events));
    }
    samples.sort_unstable();
    assert_eq!(
        samples,
        vec![7_000, 8_000, 8_500, 9_000],
        "four samples, each the event's own lag"
    );
    // one event's delivery is carried as it stands
    let one = coalesced_event_of(&[(&a1, &pa1)]).unwrap();
    assert!(one.by_identity.is_empty() && one.also.is_empty());
}

/// FIX-974 (bsv-low #974; L53's S3): THE MINED FRAME TAKES THE DIRECT ROAD. The overlay's `/arc-ingest` MINED push
/// rides as a `word` event carrying the `proof` frame, and the direct pass left EVERY frame to the consumer (its
/// ingest is the tracker's write), so the mined word's first apply could only be the queue's delivery and its sample
/// the backlog's lag (L53: the consumer 6,000 messages behind; and `mined` never read, no sample at all). Now a frame
/// that is the mined word's (`owed_actor::mined_word_event`) is INGESTED through the port first (the tracker's write,
/// whose trigger marks the seat the txid's spend names), then planned and called as any direct event, its answer noted
/// into the `mined` class; a SEEN frame stays the consumer's, untouched; a mined frame whose ingest faults is left to
/// the queue, uncalled. New API (`DirectPort::ingest_frame`, `DirectReport::mined_frames`): does not compile on
/// 149869bf, whose pass answers `left_to_queue: 2, calls: 0` for the same two events.
#[tokio::test]
async fn fix974_c_the_mined_frame_takes_the_direct_road() {
    struct FramePort<'a> {
        db: &'a SqliteSql,
        fail_ingest: bool,
        ingested: RefCell<Vec<Value>>,
        bodies: RefCell<Vec<Value>>,
    }
    impl DirectPort for FramePort<'_> {
        async fn subjects(&self, ev: &OwedEvent) -> Result<Vec<(String, Vec<String>)>, String> {
            Ok(plan_with(self.db, ev, false).await?.subjects())
        }
        async fn ingest_frame(&self, frame: &Value) -> Result<(), String> {
            if self.fail_ingest {
                return Err("tracker write faulted".into());
            }
            // the tracker's write: the word's trigger marks the seat whose hop the txid spends
            let txid = frame["txid"].as_str().unwrap().to_string();
            mark(self.db, "g1", "word", &txid, T0);
            self.ingested.borrow_mut().push(frame.clone());
            Ok(())
        }
        async fn call(&self, body: Value) -> Result<Value, String> {
            self.bodies.borrow_mut().push(body.clone());
            Ok(
                json!({"identity": body["identity"], "applied": true, "firstApply": true, "warmAtBirth": true, "atMs": body["atMs"], "judgedAtMs": T0 + 420, "applyMs": 30, "mined": body["mined"]}),
            )
        }
    }
    let txid = "71fa3ec66d30b961ea9322d3d32d85a8e556633bd32c3e83d64367d640d31d84";
    let frame_event = |kind: &str, body: Value, seq: i64| {
        let n = publish::frame_note(&json!({"txid": txid, "kind": kind, "body": body})).unwrap();
        OwedEvent {
            v: owed_event::OWED_EVENT_V,
            seq,
            source: n.source,
            source_key: n.key,
            facts: n.held.unwrap(),
            at_ms: T0,
        }
    };
    let events = || {
        vec![
            frame_event("proof", json!({"merklePath": "00"}), 7),
            frame_event("seen", json!({"source": "arcade", "at": T0}), 8),
        ]
    };
    // the proof frame ingested, planned off the mark its ingest made, called with `mined`, sampled once
    let db = SqliteSql::migrated();
    let port = FramePort {
        db: &db,
        fail_ingest: false,
        ingested: RefCell::new(Vec::new()),
        bodies: RefCell::new(Vec::new()),
    };
    let mined_before = crate::owed_actor::pending_bars_snapshot()
        .visible
        .get("mined")
        .map_or(0, Vec::len);
    let report = direct_with(&port, events(), Some(973_211)).await;
    assert_eq!(
        (
            report.events,
            report.calls,
            report.left_to_queue,
            report.mined_frames,
            report.faults
        ),
        (1, 1, 1, 1, 0),
        "{report:?}"
    );
    assert_eq!(
        port.ingested.borrow().len(),
        1,
        "the proof frame ingested, the seen frame left to the consumer"
    );
    assert_eq!(port.ingested.borrow()[0]["kind"], json!("proof"));
    let body = port.bodies.borrow()[0].clone();
    assert_eq!(
        (
            body["mined"].clone(),
            body["kind"].clone(),
            body["seq"].clone(),
            body["atMs"].clone(),
            body["class"].clone(),
            body["identity"].clone(),
            body["games"].clone()
        ),
        (
            json!(true),
            json!("word"),
            json!(7),
            json!(T0),
            json!("direct"),
            json!(ID),
            json!(["g1"])
        ),
        "{body}"
    );
    let mined_after = crate::owed_actor::pending_bars_snapshot()
        .visible
        .get("mined")
        .map_or(0, Vec::len);
    assert_eq!(
        mined_after - mined_before,
        1,
        "the first apply's note into the mined class"
    );
    // a mined frame whose ingest faults is the queue's: nothing called, nothing marked
    let db = SqliteSql::migrated();
    let port = FramePort {
        db: &db,
        fail_ingest: true,
        ingested: RefCell::new(Vec::new()),
        bodies: RefCell::new(Vec::new()),
    };
    let report = direct_with(&port, events(), Some(973_211)).await;
    assert_eq!(
        (
            report.events,
            report.calls,
            report.left_to_queue,
            report.mined_frames
        ),
        (0, 0, 2, 0),
        "{report:?}"
    );
    assert_eq!(int(&db, "SELECT COUNT(*) FROM owed_dirty"), 0);
    // the worker's port ingests through the consumer's own ingest
    let src: String = include_str!("../owed_events.rs")
        .split_whitespace()
        .collect();
    let worker = &src[src.find("implDirectPortforWorkerDirect{").unwrap()..];
    assert!(worker.contains(
        "asyncfningest_frame(&self,frame:&Value)->Result<(),String>{ingest_frame(&self.env,frame).await}"
    ));
    let pass = &src[src.find("pubasyncfndirect_with_class(").unwrap()..];
    assert!(pass.contains(
        "if!crate::owed_actor::mined_word_event(&ev)||port.ingest_frame(frame).await.is_err(){report.left_to_queue+=1;continue;}"
    ));
}
