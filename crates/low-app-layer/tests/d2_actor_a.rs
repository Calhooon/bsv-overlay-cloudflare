//! bsv-low #911 (lane D2-ACTOR-A): the `owed-events` consumer's routing and acknowledgement, the un-acked mark sweep,
//! a refused publish shipped by the sweep, and the event shape, through the shipped functions over real SQLite with
//! the overlay's migrations (`support/owed_db.rs`).
#[allow(dead_code)]
#[path = "support/owed_db.rs"]
mod owed_db;
#[path = "support/owed_render.rs"]
mod owed_render;

use low_app_layer::{owed_events, owed_events_publish as publish};
use overlay_discovery::owed_event::{self, OwedEvent};
use low_app_layer::pot_hook::SqlPort;
use owed_db::{seed_stranded_hop, Db, NOW};
use std::cell::RefCell;

const ID: &str = "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn event(source: &str, key: &str) -> OwedEvent {
    OwedEvent {
        v: owed_event::OWED_EVENT_V,
        seq: 1,
        source: source.into(),
        source_key: key.into(),
        facts: serde_json::Value::Null,
        at_ms: NOW as i64,
    }
}

fn hop_key(hop: &str) -> String {
    owed_event::outpoint_key(hop, 0)
}

/// (c) THE CONSUMER ROUTES BY THE MARKS AND ACKNOWLEDGES THEM AFTER THE JUDGE WROTE. A real source write (a verified
/// hop marker) plants its trigger's mark; the event of that pair is routed through the shipped `route`: the subject is
/// judged, its row written, and the mark is deleted in THE SAME BATCH that writes the row (never a batch before it).
/// A judge that faults leaves the mark and answers `Err` (the message is handed back).
#[tokio::test]
async fn d2_actor_a_c_the_consumer_routes_by_the_marks_and_acks_after_the_judge() {
    let db = Db::new();
    let hop = owed_db::hop(1);
    seed_stranded_hop(&db, ID, "g1", &hop);
    let marks = |db: &Db| {
        db.int(&format!(
            "SELECT COUNT(*) FROM owed_dirty WHERE source='hop' AND sourceKey='{}'",
            hop_key(&hop)
        ))
    };
    assert_eq!(marks(&db), 1, "the hop marker's trigger marked its pair");

    // a judge fault leaves the mark
    *db.fail_on.borrow_mut() = Some("INSERT INTO owed_rows".into());
    let err = owed_events::route(&db, &event("hop", &hop_key(&hop))).await;
    assert!(err.is_err(), "a judge fault is an Err: {err:?}");
    assert_eq!(marks(&db), 1, "the mark stays for the retry");
    assert_eq!(db.int("SELECT COUNT(*) FROM owed_rows"), 0);

    // the judge writes, and the ack rides its batch
    *db.fail_on.borrow_mut() = None;
    db.batches.borrow_mut().clear();
    let routed = owed_events::route(&db, &event("hop", &hop_key(&hop)))
        .await
        .unwrap();
    assert_eq!((routed.subjects, routed.marks, routed.more), (1, 1, false));
    assert_eq!(marks(&db), 0, "acknowledged");
    assert!(
        db.int(&format!(
            "SELECT COUNT(*) FROM owed_rows WHERE identity='{ID}'"
        )) >= 1,
        "the row was judged"
    );
    let batches = db.batches.borrow().clone();
    let ack_at = batches
        .iter()
        .position(|b| {
            b.iter()
                .any(|s| s.sql.starts_with("DELETE FROM owed_dirty"))
        })
        .expect("an ack");
    assert!(
        batches[ack_at]
            .iter()
            .any(|s| s.sql.contains("owed_rows") && !s.sql.starts_with("SELECT")),
        "the ack is in the judge's own write batch"
    );
    assert!(
        batches[..ack_at]
            .iter()
            .all(|b| b.iter().all(|s| !s.sql.contains("DELETE FROM owed_dirty"))),
        "no mark is deleted before the judge wrote"
    );
    // a txfacts event routes nothing
    assert_eq!(
        owed_events::route(&db, &event("txfacts", &hop))
            .await
            .unwrap(),
        Default::default()
    );
}

/// (c) the bound: a pair naming more than 64 subjects is judged 64 at a time, the rest a continuation; an identity
/// route reads the identity's own marks.
#[tokio::test]
async fn d2_actor_a_c_a_message_naming_more_than_64_games_is_split() {
    let db = Db::new();
    for g in 0..70 {
        db.exec(&format!("INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('{ID}','g{g:02}','tip','gate',{NOW})"));
    }
    let first = owed_events::route(&db, &event("tip", "gate"))
        .await
        .unwrap();
    assert_eq!((first.subjects, first.more), (64, true));
    assert_eq!(db.int("SELECT COUNT(*) FROM owed_dirty"), 6);
    let rest = owed_events::route(&db, &event("identity", ID))
        .await
        .unwrap();
    assert_eq!((rest.subjects, rest.more), (6, false));
    assert_eq!(db.int("SELECT COUNT(*) FROM owed_dirty"), 0);
}

/// (e) THE UN-ACKED SWEEP RE-PUBLISHES A MARK OLDER THAN A TICK AND NOTHING YOUNGER: the shipped `sweep` over the
/// backlog index, then the shipped publish (seq and facts in one batch), the sent bodies recorded. Amended by FIX-956
/// (bsv-low #956): the consumer's horizon is past every mark (it judged a message published at `now`), so a tick-old
/// mark reads lost; without the horizon only the long bound re-publishes (`owed_events::tests::s956_*`).
#[tokio::test]
async fn d2_actor_a_e_the_sweep_republishes_marks_older_than_a_tick_only() {
    let db = Db::new();
    let now = NOW as i64;
    let tick = owed_events::SWEEP_AFTER_MS;
    db.exec(&format!(
        "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('owed-events:consumer','{{\"horizonMs\":{}}}',{now})",
        now + tick
    ));
    db.exec(&format!(
        "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('{ID}','g1','tip','gate',{}), ('{ID}','g2','tip','untipped',{}), ('{ID}','g3','age','age',{})",
        now - tick - 1,
        now - tick + 1,
        now - 10
    ));
    let pairs = owed_events::sweep(&db, now).await.unwrap();
    assert_eq!(pairs, vec![("tip".to_string(), "gate".to_string())]);
    let sent = RefCell::new(Vec::new());
    let port = owed_events::SqlPublish {
        db: &db,
        send: |bodies: Vec<String>| {
            sent.borrow_mut().extend(bodies);
            async { Ok(()) }
        },
    };
    let report = publish::publish_with(&port, owed_events::notes_of(&pairs)).await;
    assert_eq!(report.sent.len(), 1);
    let ev = OwedEvent::decode(&sent.borrow()[0]).unwrap();
    assert_eq!(
        (ev.source.as_str(), ev.source_key.as_str()),
        ("tip", "gate")
    );
    assert_eq!(ev.facts["marks"], 1);
    // the cursor moved past it: the next tick (same clock) re-publishes nothing until the walk wraps
    let again = owed_events::sweep(&db, now).await.unwrap();
    assert!(again.is_empty() || again == pairs, "{again:?}");
}

/// (e) the choice: 64 distinct pairs a tick, oldest first, the cursor at the last row taken, wrapping at the end.
#[test]
fn d2_actor_a_e_the_sweep_takes_64_pairs_and_wraps() {
    let rows: Vec<serde_json::Value> = (0..200)
        .map(|i| serde_json::json!({"generation": i, "source": "pot", "sourceKey": format!("k{}", i / 2), "markedAtMs": i}))
        .collect();
    let (pairs, cursor) = owed_events::sweep_choice(&rows, 200);
    assert_eq!(pairs.len(), 64);
    assert_eq!(pairs[0].1, "k0");
    assert_eq!(cursor, Some((127, 127)));
    let (pairs, cursor) = owed_events::sweep_choice(&rows[..10], 256);
    assert_eq!(
        (pairs.len(), cursor),
        (5, None),
        "the end of the backlog wraps"
    );
}

/// (g) A CANCELLED OR REFUSED PUBLISH LEAVES THE MARK AND THE SWEEP SHIPS IT: a record filing's source write marks
/// its pair; the publish is refused (a Queues incident, a `wait_until` wall); the publisher's lost note (FIX-956) has
/// the next sweep tick re-publish that pair, and the consumer routes it.
#[tokio::test]
async fn d2_actor_a_g_a_refused_publish_leaves_the_mark_and_the_sweep_ships_it() {
    let db = Db::new();
    let hop = owed_db::hop(2);
    seed_stranded_hop(&db, ID, "g1", &hop);
    let refused = owed_events::SqlPublish {
        db: &db,
        send: |_: Vec<String>| async { Err("the queue refused the batch".to_string()) },
    };
    let note = publish::Noted {
        source: "hop".into(),
        key: hop_key(&hop),
        held: None,
    };
    let report = publish::publish_with(&refused, vec![note.clone()]).await;
    assert!(report.fault.is_some());
    assert_eq!(report.unsent, vec![note.clone()]);
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_dirty"),
        1,
        "the mark stands"
    );
    // the trigger dates its mark by the database's clock (the write's own time)
    let marked = db.int("SELECT markedAtMs FROM owed_dirty");
    // FIX-956 (bsv-low #956): with no lost note and no consumer past it, a tick-old mark is NOT re-published (its
    // message may still be in the queue); only the long bound would take it
    db.now.set((marked + owed_events::SWEEP_AFTER_MS + 1) as u64);
    assert!(owed_events::sweep(&db, db.now.get() as i64).await.unwrap().is_empty());
    // the publisher KNOWS it lost the pair (the send faulted): its lost note (`publish_env`'s step), and the next
    // tick re-publishes the pair, whatever the horizon, and it is routed
    assert_eq!(publish::write_lost(&refused, publish::lost_of(&report)).await, 1);
    db.now.set((marked + owed_events::SWEEP_AFTER_MS + 2) as u64);
    let pairs = owed_events::sweep(&db, db.now.get() as i64).await.unwrap();
    assert_eq!(pairs, vec![("hop".to_string(), hop_key(&hop))]);
    let sent = RefCell::new(Vec::new());
    let ok = owed_events::SqlPublish {
        db: &db,
        send: |b: Vec<String>| {
            sent.borrow_mut().extend(b);
            async { Ok(()) }
        },
    };
    assert_eq!(
        publish::publish_with(&ok, owed_events::notes_of(&pairs))
            .await
            .sent
            .len(),
        1
    );
    let ev = OwedEvent::decode(&sent.borrow()[0]).unwrap();
    owed_events::route(&db, &ev).await.unwrap();
    assert_eq!(db.int("SELECT COUNT(*) FROM owed_dirty"), 0);
}

/// (f) THE SHAPE, both sides: the app layer decodes the overlay-discovery vector byte for byte.
#[test]
fn d2_actor_a_f_the_app_layer_reads_the_event_vector() {
    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../overlay-discovery/vectors/owed_event_v1.json"
    ))
    .unwrap();
    for case in vector["cases"].as_array().unwrap() {
        let body = case["body"].as_str().unwrap();
        let ev = OwedEvent::decode(body).unwrap();
        assert_eq!(ev.encode(), body);
    }
    // the publish SQL the app layer compiles is the overlay's own file (one source)
    assert_eq!(
        publish::SEQ_BUMP_SQL,
        bsv_overlay_cloudflare::owed_events::SEQ_BUMP_SQL
    );
    assert_eq!(
        publish::READ_POT_SQL,
        bsv_overlay_cloudflare::owed_events::READ_POT_SQL
    );
}

/// The record door answers before any judge: its handler publishes the filing's events and keeps the detached judge
/// only for a Worker with no queue binding; the `Server-Timing` carries the write and the enqueue apart.
#[test]
fn d2_actor_a_b_the_door_publishes_and_answers_before_any_judge() {
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let src = squash(include_str!("../src/record_post.rs"));
    let at = src.find(&squash("pub async fn record_post(")).unwrap();
    let door = &src[at..at
        + src[at..]
            .find(&squash("let row_written = match outcome"))
            .unwrap()];
    assert!(door.contains(&squash("if crate::owed_events::configured(&ctx.env).await {")));
    assert!(door.contains(&squash(
        "latency::note_enqueue(crate::owed_events::publish_noted(&ctx.env).await);"
    )));
    let hand = squash(include_str!("../src/handfile.rs"));
    assert!(hand.contains(&squash(
        "crate::record_post::note_enqueue(crate::owed_events::publish_noted(&ctx.env).await);"
    )));
    let lat = include_str!("../src/record_post/latency.rs");
    assert!(lat.contains("write;dur={}, enqueue;dur={e}"));
}

/// A [`Db`] whose FIRST judge write batch (the one that writes `owed_rows`) waits on a gate: the first event's judge
/// has read its marks and hydrated, and commits only once the test opens the gate (MEDIUM-1's interleaving).
struct Held<'a> {
    db: &'a Db,
    gate: RefCell<Option<tokio::sync::oneshot::Receiver<()>>>,
    reached: std::cell::Cell<bool>,
}
impl SqlPort for Held<'_> {
    async fn run_batch(
        &self,
        stmts: Vec<low_app_layer::pot_hook::Stmt>,
    ) -> Result<Vec<low_app_layer::pot_hook::StmtOut>, String> {
        if stmts.iter().any(|s| s.sql.contains("INSERT INTO owed_rows")) {
            let gate = self.gate.borrow_mut().take();
            if let Some(gate) = gate {
                self.reached.set(true);
                let _ = gate.await;
            }
        }
        self.db.run_batch(stmts).await
    }
    async fn counters(&self, deltas: Vec<(String, u64)>) {
        self.db.counters(deltas).await;
    }
    fn now_ms(&self) -> u64 {
        self.db.now_ms()
    }
    fn log(&self, line: &str) {
        self.db.log(line);
    }
}
impl low_app_layer::owed_judge::JudgeDb for Held<'_> {
    fn tip(&self) -> Option<u64> {
        self.db.tip()
    }
    async fn word_views(
        &self,
        rows: &std::collections::BTreeMap<String, low_app_layer::tracker_host::stored::Row>,
    ) -> Result<std::collections::BTreeMap<String, serde_json::Value>, String> {
        self.db.word_views(rows).await
    }
    async fn canonical_root(&self, height: u64) -> Option<String> {
        self.db.canonical_root(height).await
    }
    async fn push(&self, identity: &str, body: serde_json::Value) {
        self.db.push(identity, body).await;
    }
}

fn stored_rows(db: &Db) -> Vec<(String, Option<i64>, Option<String>)> {
    let conn = db.conn.borrow();
    let mut s = conn
        .prepare("SELECT outpoint,claimable,reason FROM owed_rows ORDER BY outpoint")
        .unwrap();
    s.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// The hop's spend (the index's own word: a sweep of it is no longer a press) and its pair.
fn spend_the_hop(db: &Db, hop: &str) {
    db.exec(&format!(
        "UPDATE pot_records SET spent=1,spendingTxid='{}' WHERE txid='{hop}' AND outputIndex=0",
        "cc".repeat(32)
    ));
}

/// MEDIUM-1 (the D2-ACTOR-A lens): TWO EVENTS FOR ONE IDENTITY, the first's judge hydrated BEFORE the second write
/// and committing AFTER the second event's judge: the interleaving two concurrent consumers make. Its rows are the
/// first's stale ones over the second's, with every mark acknowledged (nothing pending withholds the press): the
/// class `max_concurrency = 1` excludes. Run one at a time (the configured consumer), the second event's rows stand.
/// The renders: every `owed-events` consumer of both app-layer environments is one at a time unless the D1 judge is
/// off (PR 233, bsv-low #984: beta's committed Phase C render runs 4 with `OWED_D1_JUDGE = "false"`, the actor the
/// one writer); its dead letters' consumer is one at a time in every phase; prod (Phase A) reads 1 with the judge on.
/// And the inverse: a render at 4 with the judge on is refused.
#[tokio::test(flavor = "current_thread")]
async fn d2_actor_a_fold_m1_one_consumer_at_a_time_derives_the_second_events_rows() {
    let hop = owed_db::hop(7);
    let fresh = || {
        let db = Db::new();
        seed_stranded_hop(&db, ID, "g1", &hop);
        db
    };
    // the oracle: both writes, then one judge
    let oracle = fresh();
    spend_the_hop(&oracle, &hop);
    owed_events::route(&oracle, &event("identity", ID)).await.unwrap();
    let truth = stored_rows(&oracle);

    // serial (the configured consumer): E1 judged, W2, E2 judged
    let serial = fresh();
    owed_events::route(&serial, &event("hop", &hop_key(&hop))).await.unwrap();
    let before = stored_rows(&serial);
    spend_the_hop(&serial, &hop);
    owed_events::route(&serial, &event("pot", &hop_key(&hop))).await.unwrap();
    assert_eq!(stored_rows(&serial), truth, "one at a time: the second event's rows");
    assert_ne!(before, truth, "the second write moves the judgement");
    assert_eq!(serial.int("SELECT COUNT(*) FROM owed_dirty"), 0);

    // interleaved (two consumers): E1 hydrates, W2 lands, E2 judges and acks, E1 commits last
    let racy = fresh();
    let (open, gate) = tokio::sync::oneshot::channel();
    let held = Held {
        db: &racy,
        gate: RefCell::new(Some(gate)),
        reached: std::cell::Cell::new(false),
    };
    let e1 = event("hop", &hop_key(&hop));
    let first = owed_events::route(&held, &e1);
    let second = async {
        while !held.reached.get() {
            tokio::task::yield_now().await;
        }
        spend_the_hop(&racy, &hop);
        let r = owed_events::route(&racy, &event("pot", &hop_key(&hop))).await;
        let _ = open.send(());
        r
    };
    let (a, b) = tokio::join!(first, second);
    a.unwrap();
    b.unwrap();
    assert_eq!(
        racy.int("SELECT COUNT(*) FROM owed_dirty"),
        0,
        "every mark acknowledged: nothing pending withholds the stale row"
    );
    assert_eq!(
        stored_rows(&racy),
        before,
        "two at a time: the first event's stale rows stand over the second's"
    );

    // the renders: one consumer at a time for the queue unless the D1 judge is off, its dead letters always one
    let render = include_str!("../wrangler.toml");
    for env in ["", "env.beta."] {
        let p = owed_render::env_phase(render, env);
        assert_eq!(
            owed_render::breaches(&p),
            Vec::<String>::new(),
            "{env}: one consumer at a time unless the D1 judge is off: {p:?}"
        );
    }
    let prod = owed_render::env_phase(render, "");
    assert_eq!(
        (prod.live_max, prod.d1_judge.as_deref()),
        (
            Some(low_app_layer::owed_events::CONSUMER_MAX_CONCURRENCY),
            Some("true")
        ),
        "prod (Phase A): one consumer, the D1 judge on"
    );
    let inverse = owed_render::breaches(&owed_render::env_phase(owed_render::JUDGE_ON_AT_FOUR, ""));
    assert!(
        inverse
            .iter()
            .any(|b| b.contains("one consumer at a time unless the D1 judge is off")),
        "concurrency 4 with the D1 judge on is refused: {inverse:?}"
    );
}

/// MEDIUM-2 (the D2-ACTOR-A lens): A PAIR WHOSE JUDGE FAULTS FOR GOOD COSTS A BOUNDED, BACKING-OFF NUMBER OF ATTEMPTS
/// AND ONE PARK ROW, ITS CAUSE READABLE. A day of the shipped sweep (every 120 s belt tick) over a subject whose row
/// write errs every time, the platform's deliveries modelled (1 + `max_retries` = 4, a retry at once or after the
/// memo's hold, then the dead letter queue's park): the shipped `note_fault`, `sweep` and `park_letter`. Without the
/// memo's hold the same day is 2,880 attempts and 720 letters (the lens's figure; RED with `fault_hold_ms` answering
/// 0 in `note_fault`: "bounded and backing off: 2880 attempts a day", 720 letters).
#[tokio::test(flavor = "current_thread")]
async fn d2_actor_a_fold_m2_a_pair_faulting_for_good_is_held_doubling_and_parked_once() {
    // the doubling: none for two faults, then 120 s doubling to 6 h
    let holds: Vec<i64> = (1..=12).map(owed_events::fault_hold_ms).collect();
    assert_eq!(&holds[..5], &[0, 0, 120_000, 240_000, 480_000]);
    assert_eq!(holds[11], owed_events::FAULT_HOLD_CAP_MS);

    let db = Db::new();
    let hop = owed_db::hop(9);
    seed_stranded_hop(&db, ID, "g1", &hop);
    *db.fail_on.borrow_mut() = Some("INSERT INTO owed_rows".into());
    let start = db.int("SELECT markedAtMs FROM owed_dirty") + owed_events::SWEEP_AFTER_MS + 1;
    let day = 24 * 3_600_000i64;
    let tick = owed_events::SWEEP_AFTER_MS;
    let (mut attempts, mut letters) = (0u64, 0u64);
    // the message in flight: (due at, delivery number, its id)
    let mut flight: Option<(i64, u32, String)> = None;
    let mut t = start;
    let mut next_sweep = start;
    let mut sent = 0u32;
    while t < start + day {
        db.now.set(t as u64);
        while let Some((due, n, id)) = flight.clone() {
            if due > t {
                break;
            }
            attempts += 1;
            let ev = event("hop", &hop_key(&hop));
            let err = owed_events::route(&db, &ev).await.unwrap_err();
            let memo = owed_events::note_fault(&db, "hop", &hop_key(&hop), &err)
                .await
                .unwrap();
            if n < 4 {
                flight = Some((t + memo.hold_ms, n + 1, id));
            } else {
                letters += 1;
                let body = ev.encode();
                assert!(matches!(
                    owed_events::park_letter(&db, &id, &body).await.unwrap(),
                    owed_events::Parked::Row(_)
                ));
                flight = None;
            }
        }
        // one sweep a belt tick; its message's deliveries run from the next step on
        if flight.is_none() && t >= next_sweep {
            next_sweep = t + tick;
            let pairs = owed_events::sweep(&db, t).await.unwrap();
            if !pairs.is_empty() {
                sent += 1;
                flight = Some((t + 1, 1, format!("m{sent}")));
            }
        }
        let wake = flight.as_ref().map_or(next_sweep, |(due, _, _)| *due);
        t = wake.max(t + 1);
    }
    assert!(attempts <= 40, "bounded and backing off: {attempts} attempts a day");
    assert!(attempts >= 8, "the pair is still retried: {attempts}");
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_event_dead_letters"),
        1,
        "{letters} dead letters, one row"
    );
    assert_eq!(
        db.int("SELECT parks FROM owed_event_dead_letters") as u64,
        letters
    );
    let cause = db.text("SELECT lastFault FROM owed_event_dead_letters").unwrap();
    assert!(cause.contains("injected fault"), "the cause readable: {cause}");
    assert_eq!(db.int("SELECT COUNT(*) FROM owed_dirty"), 1, "its mark stays");

    // the health serves the memo: the pair's and its one subject's (NEW-M1), its faults and its last error
    // (D2-ACTOR-A-FOLD-4: the overlay's `/health/invariants.owedEvents`, its one shipped statement over this store)
    let state = db
        .run_batch(vec![low_app_layer::pot_hook::Stmt {
            sql: publish::HEALTH_STATE_SQL.into(),
            binds: vec![low_app_layer::pot_hook::Bind::Int(db.now_ms() as i64)],
        }])
        .await
        .unwrap();
    let health = publish::health_state_view(&state[0].rows[0]);
    assert_eq!(health["faultMemo"]["rows"], 2);
    assert!(health["faultMemo"]["last"]["lastFault"]
        .as_str()
        .unwrap()
        .contains("injected fault"));
    assert_eq!(health["deadLetters"]["parked"], 1);

    // the judge lands once the subject's hold ran out: both memos cleared, the letter resolved, in the consumer's
    // batch row
    *db.fail_on.borrow_mut() = None;
    db.now
        .set(db.now.get() + owed_events::FAULT_HOLD_CAP_MS as u64 + 1);
    owed_events::route(&db, &event("hop", &hop_key(&hop)))
        .await
        .unwrap();
    db.run_batch(owed_events::resolved_statements("hop", &hop_key(&hop)))
        .await
        .unwrap();
    assert_eq!(db.int("SELECT COUNT(*) FROM owed_event_dead_letters"), 0);
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_belt_state WHERE name LIKE 'owed-events:fault:%'"),
        0
    );
}

/// LOW-3, LOW-4 and LOW-5 (the lens): THE PARK over real SQLite: one row per pair (a redelivery counts nothing, a new
/// message of the pair counts one), an undecodable body keyed by its message, a full table refusing a NEW pair and
/// re-parking a held one, three re-drives then `exhausted` (never read by the lever again), the hand-back delay
/// doubling with the letter's age to 30 min.
#[tokio::test(flavor = "current_thread")]
async fn d2_actor_a_fold_l3_the_park_holds_one_row_a_pair_and_stops_at_three_redrives() {
    use owed_events::{park_letter, Parked};
    let db = Db::new();
    let body = event("pot", &hop_key(&owed_db::hop(1))).encode();
    assert_eq!(park_letter(&db, "m1", &body).await.unwrap(), Parked::Row("parked".into()));
    assert_eq!(park_letter(&db, "m1", &body).await.unwrap(), Parked::Row("parked".into()));
    assert_eq!(db.int("SELECT parks FROM owed_event_dead_letters"), 1, "a redelivery");
    park_letter(&db, "m2", &body).await.unwrap();
    assert_eq!(
        (
            db.int("SELECT COUNT(*) FROM owed_event_dead_letters"),
            db.int("SELECT parks FROM owed_event_dead_letters")
        ),
        (1, 2),
        "one row a pair"
    );
    park_letter(&db, "m3", "not an event").await.unwrap();
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_event_dead_letters WHERE source='' AND sourceKey='m3'"),
        1
    );

    // the re-drives: three, then exhausted
    let key = json_key("pot", &hop_key(&owed_db::hop(1)));
    for n in 1..=3 {
        db.exec(&publish::DEAD_LETTERS_REDRIVEN_SQL.replace("?1", &key));
        assert_eq!(db.int("SELECT redrives FROM owed_event_dead_letters WHERE source='pot'"), n);
        let again = park_letter(&db, &format!("r{n}"), &body).await.unwrap();
        let want = if n < publish::MAX_REDRIVES { "parked" } else { "exhausted" };
        assert_eq!(again, Parked::Row(want.into()), "re-drive {n}");
    }
    let readable = db.int(&format!(
        "SELECT COUNT(*) FROM ({})",
        publish::DEAD_LETTERS_READ_SQL.replace("?1", "100")
    ));
    assert_eq!(readable, 1, "the lever reads the undecodable letter only, never the exhausted one");

    // a full table: a new pair is refused, a held pair re-parks
    for i in 0..publish::DEAD_LETTERS_MAX {
        db.exec(&format!("INSERT INTO owed_event_dead_letters(source,sourceKey,messageId,body,parkedAt) VALUES ('seen','k{i}','x{i}','{{}}',1)"));
    }
    let other = event("pot", &hop_key(&owed_db::hop(2))).encode();
    assert_eq!(park_letter(&db, "n1", &other).await.unwrap(), Parked::Full);
    assert_eq!(
        park_letter(&db, "r9", &body).await.unwrap(),
        Parked::Row("exhausted".into())
    );

    // LOW-5: the hand-back delay
    assert_eq!(owed_events::park_retry_delay_s(0), 60);
    assert_eq!(owed_events::park_retry_delay_s(300_000), 300);
    assert_eq!(owed_events::park_retry_delay_s(10 * 3_600_000), 1800);
}

fn json_key(source: &str, key: &str) -> String {
    format!("'{}'", serde_json::json!([[source, key]]))
}

/// LOW-1 (the D2-ACTOR-A lens): the queue is the judge's trigger only once the overlay's certified migration count
/// holds the publish's statements: the figure is the overlay's own list, a short or unreadable count keeps the inline
/// judge, and every caller awaits the gate. RED on 59d98ec6 (`configured` was the binding alone).
#[test]
fn d2_actor_a_fold_l1_the_queue_waits_for_the_overlays_certified_migrations() {
    // D2-ACTOR-A-DELTA's NOTE-3: the gate is the count from which the publish's statements EXIST (`>=` is the
    // rule), so the pin holds them inside the overlay's first 473 migrations and lets the list grow past it
    let n_gate = owed_events::OWED_EVENTS_MIGRATION_COUNT as usize;
    assert!(n_gate <= bsv_overlay_cloudflare::d1::OVERLAY_MIGRATION_COUNT);
    let prefix = &bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS[..n_gate];
    for needed in [
        publish::SEQ_CREATE,
        publish::DEAD_LETTERS_CREATE,
        publish::RELEASED_MISSING_HOP_INDEX,
    ] {
        assert!(prefix.contains(&needed), "inside the gate: {needed}");
    }
    assert!(
        !bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS[..n_gate - 1]
            .contains(&bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS[n_gate - 1]),
        "the gate's last migration is its own"
    );
    let n = f64::from(owed_events::OWED_EVENTS_MIGRATION_COUNT);
    assert!(owed_events::ready_at(Some(n)));
    assert!(owed_events::ready_at(Some(n + 1.0)));
    assert!(!owed_events::ready_at(Some(n - 1.0)), "the overlay at 469");
    assert!(!owed_events::ready_at(None), "no certified count read");
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let gate = squash(include_str!("../src/owed_events.rs"));
    let at = gate.find("pubasyncfnconfigured(").unwrap();
    assert!(gate[at..].contains("letready=ready_at(count);"));
}

/// LOW-6 (the D2-ACTOR-A lens): a message pays the chain tip only when its marks name a subject (the plan before
/// the tip, the tip memoised 30 s), and an identity event is sent only for an identity holding a mark. RED on
/// 59d98ec6: the tip read preceded the routing, and an identity note was sent whatever its marks.
#[tokio::test(flavor = "current_thread")]
async fn d2_actor_a_fold_l6_no_subject_no_tip_and_no_identity_event_without_a_mark() {
    let db = Db::new();
    let plan = owed_events::plan(&db, &event("identity", ID)).await.unwrap();
    assert!(plan.is_empty(), "no mark, no subject");
    let sent = RefCell::new(Vec::new());
    let port = owed_events::SqlPublish {
        db: &db,
        send: |b: Vec<String>| {
            sent.borrow_mut().extend(b);
            async { Ok(()) }
        },
    };
    let note = publish::Noted {
        source: "identity".into(),
        key: ID.into(),
        held: None,
    };
    let quiet = publish::publish_with(&port, vec![note.clone()]).await;
    assert_eq!((quiet.sent.len(), quiet.skipped), (0, 1), "nothing pending: not sent");
    seed_stranded_hop(&db, ID, "g1", &owed_db::hop(3));
    let busy = publish::publish_with(&port, vec![note]).await;
    assert_eq!((busy.sent.len(), busy.skipped), (1, 0));
    assert!(!owed_events::plan(&db, &event("identity", ID)).await.unwrap().is_empty());
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let src = squash(include_str!("../src/owed_events.rs"));
    let at = src.find("pubasyncfnconsume_one(").unwrap();
    let body = &src[at..];
    let empty = body.find("ifplan.is_empty(){").unwrap();
    assert!(body.contains("returnOk(plan.empty_routed());"), "an empty plan answers its held count");
    let tip = body
        .find("lettip=tip_memoised(env,tip_reads_fresh(&ev.source)).await;")
        .unwrap();
    assert!(empty < tip, "the plan before the tip");
}

/// Every mark of the store moved onto one shared pair (the age repair's `age`/`age`, the tip's `tip`/`gate`).
fn onto_pair(db: &Db, source: &str, key: &str) {
    db.exec(&format!(
        "UPDATE owed_dirty SET source='{source}',sourceKey='{key}'"
    ));
}

fn rows_of(db: &Db, identity: &str, game: &str) -> i64 {
    db.int(&format!(
        "SELECT COUNT(*) FROM owed_rows WHERE identity='{identity}' AND gameId='{game}'"
    ))
}

/// D2-ACTOR-A-DELTA's NEW-M1: ONE POISONED SUBJECT NEVER HOLDS A SHARED PAIR. Three identities and a fourth's three
/// games marked under one shared pair (`age`/`age`, then `tip`/`gate`), the middle identity's game and the fourth's
/// middle game faulting at every judge (`fail_bind`). The first delivery judges every other subject and acknowledges
/// it, keeps the two poisoned marks, keys the fault memo by SUBJECT (no pair memo: the pair is not held), and answers
/// `Ok`; the next delivery of the pair judges a subject marked since. From the third fault a poisoned subject is held
/// (skipped, its mark kept) and the sweep leaves out a pair holding only held subjects. RED on `7ad5279f`: the first
/// delivery's `Err` at the first poisoned chunk, the subjects behind it never judged.
#[tokio::test(flavor = "current_thread")]
async fn d2_actor_a_fold2_m1_a_poisoned_subject_never_holds_a_shared_pair() {
    let ids = [
        format!("02{}", "a1".repeat(32)),
        format!("02{}", "a2".repeat(32)),
        format!("02{}", "a3".repeat(32)),
        format!("02{}", "a4".repeat(32)),
    ];
    for (source, key) in [("age", "age"), ("tip", "gate")] {
        let db = Db::new();
        seed_stranded_hop(&db, &ids[0], "g1", &owed_db::hop(1));
        seed_stranded_hop(&db, &ids[1], "gbad", &owed_db::hop(2));
        seed_stranded_hop(&db, &ids[2], "g3", &owed_db::hop(3));
        seed_stranded_hop(&db, &ids[3], "d1", &owed_db::hop(4));
        seed_stranded_hop(&db, &ids[3], "dbad", &owed_db::hop(5));
        seed_stranded_hop(&db, &ids[3], "d3", &owed_db::hop(6));
        onto_pair(&db, source, key);
        *db.fail_bind.borrow_mut() = Some("bad".into());

        let routed = owed_events::route(&db, &event(source, key)).await;
        let routed = routed.unwrap_or_else(|e| panic!("{source}/{key}: one poisoned subject failed the message: {e}"));
        assert_eq!(
            (routed.subjects, routed.faults),
            (4, 2),
            "{source}/{key}: the four others judged, the two poisoned faulted"
        );
        for (id, g) in [(&ids[0], "g1"), (&ids[2], "g3"), (&ids[3], "d1"), (&ids[3], "d3")] {
            assert!(rows_of(&db, id, g) >= 1, "{source}/{key}: {g} judged");
        }
        assert_eq!(
            db.int(&format!(
                "SELECT COUNT(*) FROM owed_dirty WHERE source='{source}' AND sourceKey='{key}'"
            )),
            2,
            "{source}/{key}: the poisoned marks stay, the others acknowledged"
        );
        assert_eq!(
            db.int(&format!(
                "SELECT COUNT(*) FROM owed_belt_state WHERE name='{}'",
                owed_events::fault_memo_name(source, key)
            )),
            0,
            "{source}/{key}: no pair memo: the pair is not held"
        );
        assert_eq!(
            db.int(&format!(
                "SELECT COUNT(*) FROM owed_belt_state WHERE name='{}'",
                owed_events::subject_memo_name(&ids[1], "gbad")
            )),
            1,
            "{source}/{key}: the memo is the subject's"
        );

        // the next delivery of the pair judges a subject marked since
        seed_stranded_hop(&db, &ids[0], "g5", &owed_db::hop(7));
        onto_pair(&db, source, key);
        let routed = owed_events::route(&db, &event(source, key)).await.unwrap();
        assert_eq!(routed.subjects, 1, "{source}/{key}: the new subject judged");
        assert!(rows_of(&db, &ids[0], "g5") >= 1);

        // the poisoned subjects alone: nothing lands, so the message is an Err (the pair's own memo is the consumer's);
        // that third fault holds each poisoned subject: skipped, its mark kept, never a judge
        let err = owed_events::route(&db, &event(source, key)).await;
        assert!(err.is_err(), "{source}/{key}: nothing landed: {err:?}");
        let routed = owed_events::route(&db, &event(source, key)).await.unwrap();
        assert_eq!(
            (routed.subjects, routed.faults, routed.held),
            (0, 0, 2),
            "{source}/{key}: held subjects are skipped"
        );
        // the sweep leaves out a pair whose every subject is held
        // the marks waited four ticks (the trigger stamps the wall clock; the test's clock is NOW)
        db.exec(&format!(
            "UPDATE owed_dirty SET markedAtMs={}",
            db.now.get() as i64 - 4 * owed_events::SWEEP_AFTER_MS
        ));
        assert!(
            owed_events::sweep(&db, db.now.get() as i64).await.unwrap().is_empty(),
            "{source}/{key}: a pair of held subjects is not re-published"
        );
        // the poison lifts: once the hold ends the subject is judged and its memo cleared
        *db.fail_bind.borrow_mut() = None;
        db.now.set(db.now.get() + owed_events::FAULT_HOLD_BASE_MS as u64 + 1);
        let routed = owed_events::route(&db, &event(source, key)).await.unwrap();
        assert_eq!(routed.subjects, 2, "{source}/{key}: judged after the hold");
        assert_eq!(
            db.int("SELECT COUNT(*) FROM owed_belt_state WHERE name LIKE 'owed-events:fault:%'"),
            0
        );
        assert_eq!(db.int("SELECT COUNT(*) FROM owed_dirty"), 0);
    }
}

/// D2-ACTOR-A-DELTA's LOW-C: a `redriven` letter whose message neither re-parked nor landed is returned to the lever
/// after the hour (its spent re-drive kept), and the family's discard takes `parked` and `exhausted` letters by pair,
/// never a `redriven` one in flight, never a mark; the body's bounds; both levers' wiring. RED on `7ad5279f`: no
/// `redrivenAt`, no stale return, no discard of the family.
#[tokio::test(flavor = "current_thread")]
async fn d2_actor_a_fold2_low_c_a_lost_redrive_returns_and_the_family_discards() {
    use owed_events::{park_letter, Parked};
    let db = Db::new();
    seed_stranded_hop(&db, ID, "g1", &owed_db::hop(1));
    for (i, n) in [1usize, 2, 3].iter().enumerate() {
        let body = event("pot", &hop_key(&owed_db::hop(*n))).encode();
        assert_eq!(
            park_letter(&db, &format!("m{i}"), &body).await.unwrap(),
            Parked::Row("parked".into())
        );
    }
    let t0: i64 = 1_790_000_000_000;
    let redrive = |key: &str, at: i64| {
        db.exec(
            &publish::DEAD_LETTERS_REDRIVEN_SQL
                .replace("?1", &json_key("pot", key))
                .replace("?2", &at.to_string()),
        );
    };
    redrive(&hop_key(&owed_db::hop(1)), t0);
    let status = |n: usize| {
        db.text(&format!(
            "SELECT status FROM owed_event_dead_letters WHERE sourceKey='{}'",
            hop_key(&owed_db::hop(n))
        ))
        .unwrap()
    };
    assert_eq!(status(1), "redriven");
    let stale = |now: i64| {
        db.int(&format!(
            "SELECT COUNT(*) FROM owed_event_dead_letters WHERE status='redriven' AND (redrivenAt IS NULL OR redrivenAt<{})",
            now - publish::REDRIVE_STALE_MS
        ))
    };
    assert_eq!(stale(t0 + publish::REDRIVE_STALE_MS - 1), 0, "inside the hour: in flight");
    assert_eq!(stale(t0 + publish::REDRIVE_STALE_MS + 1), 1);
    // the lever's call an hour and a millisecond on: its cutoff is `now - REDRIVE_STALE_MS`
    let now = t0 + publish::REDRIVE_STALE_MS + 1;
    db.exec(
        &publish::DEAD_LETTERS_STALE_RETURN_SQL
            .replace("?1", &(now - publish::REDRIVE_STALE_MS).to_string()),
    );
    assert_eq!(status(1), "parked", "returned to the lever");
    assert_eq!(
        db.int(&format!(
            "SELECT redrives FROM owed_event_dead_letters WHERE sourceKey='{}'",
            hop_key(&owed_db::hop(1))
        )),
        1,
        "its spent re-drive kept"
    );

    // the discard: a parked and an exhausted letter by pair; a redriven one in flight and an unknown key are not found
    redrive(&hop_key(&owed_db::hop(2)), t0);
    db.exec(&format!(
        "UPDATE owed_event_dead_letters SET status='exhausted' WHERE sourceKey='{}'",
        hop_key(&owed_db::hop(3))
    ));
    let marks = db.int("SELECT COUNT(*) FROM owed_dirty");
    let named = serde_json::json!([
        ["pot", hop_key(&owed_db::hop(1))],
        ["pot", hop_key(&owed_db::hop(2))],
        ["pot", hop_key(&owed_db::hop(3))],
        ["pot", "nothing"]
    ]);
    db.exec(&publish::DEAD_LETTERS_DISCARD_SQL.replace("?1", &format!("'{named}'")));
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_event_dead_letters WHERE source='pot'"),
        1,
        "the parked and the exhausted discarded"
    );
    assert_eq!(status(2), "redriven", "never a re-drive in flight");
    assert_eq!(db.int("SELECT COUNT(*) FROM owed_dirty"), marks, "the marks stay");

    // the body's bounds
    let one = br#"{"family":"owed-events","letters":[{"source":"pot","sourceKey":"k"}]}"#;
    assert_eq!(publish::parse_discard(one).unwrap(), vec![("pot".into(), "k".into())]);
    assert!(publish::parse_discard(br#"{"family":"owed-events","letters":[]}"#).is_err());
    assert!(publish::parse_discard(br#"{"letters":[{"source":"pot"}]}"#).is_err());
    let many: Vec<_> = (0..=publish::DISCARD_MAX)
        .map(|i| serde_json::json!({"source": "pot", "sourceKey": format!("k{i}")}))
        .collect();
    let many = serde_json::json!({"family": "owed-events", "letters": many}).to_string();
    assert!(publish::parse_discard(many.as_bytes()).is_err(), "past 50: refused");

    // the wiring: the discard lever routes the family; the re-drive returns stale letters before it reads
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let dl = squash(include_str!("../../overlay-cloudflare/src/dead_letters.rs"));
    let at = dl.find("pubasyncfninternal_discard(").unwrap();
    let body = &dl[at..];
    let gate = body.find("bearer_ok(").unwrap();
    let family = body
        .find("returncrate::owed_events::discard_dead_letters(&raw,env).await;")
        .unwrap();
    assert!(gate < family, "the bearer before the family");
    let ov = squash(include_str!("../../overlay-cloudflare/src/owed_events.rs"));
    let at = ov.find("pubasyncfnredrive_dead_letters(").unwrap();
    let body = &ov[at..];
    assert!(
        body.find(".prepare(DEAD_LETTERS_STALE_RETURN_SQL)").unwrap()
            < body.find(".prepare(DEAD_LETTERS_READ_SQL)").unwrap()
    );
}

/// D2-ACTOR-A-DELTA's LOW-A and LOW-B: the consumer races each message against its wall (handed back past it, the
/// pair's memo noted, the service time served), and a `tip` event reads the chain tip fresh.
#[test]
fn d2_actor_a_fold2_low_a_b_the_message_wall_and_the_fresh_tip() {
    assert!(owed_events::tip_reads_fresh("tip"));
    for other in ["age", "pot", "word", "identity", "backfill"] {
        assert!(!owed_events::tip_reads_fresh(other), "{other}");
    }
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let src = squash(include_str!("../src/owed_events.rs"));
    let at = src.find("pubasyncfnqueue(").unwrap();
    let body = &src[at..];
    let wall = body
        .find("futures_util::future::select(work,wall).await")
        .unwrap();
    let back = body
        .find("letmutdelay=CONSUMER_WALL_RETRY_S;")
        .unwrap();
    assert!(body.contains(".with_delay_seconds(delay)"), "the wall's hand-back (LOW-1: the subjects' hold, at least 60 s)");
    assert!(wall < back);
    assert!(body.contains("\"serviceMs\":service_max"));
    const { assert!(owed_events::CONSUMER_MESSAGE_WALL_MS < 15 * 60_000, "inside the platform's 15 minutes") };
}

/// 64 poisoned games of one identity, then one honest identity's game, every mark moved onto `(source, key)`, the
/// poisoned ones faulting at every judge; three deliveries fault them to their hold (nothing lands: `Err`).
async fn sixty_four_held_and_one_live(source: &str, key: &str) -> (Db, String) {
    let whale = format!("02{}", "b1".repeat(32));
    let honest = format!("02{}", "c1".repeat(32));
    let db = Db::new();
    for n in 0..64 {
        seed_stranded_hop(&db, &whale, &format!("bad{n:02}"), &owed_db::hop(100 + n));
    }
    seed_stranded_hop(&db, &honest, "g1", &owed_db::hop(1));
    onto_pair(&db, source, key);
    *db.fail_bind.borrow_mut() = Some("bad".into());
    for delivery in 1..=3 {
        let r = owed_events::route(&db, &event(source, key)).await;
        assert!(r.is_err(), "{source}/{key} delivery {delivery}: the 64 poisoned fault: {r:?}");
    }
    (db, honest)
}

/// D2-ACTOR-A-DELTA-2's NEW2-M1 (a): A HELD SUBJECT DOES NOT COUNT TOWARD THE 64-GAME CAP. 64 held subjects at the head
/// of a shared pair and one live subject behind them: the next delivery inside the hold judges the live one. RED on
/// `7bb95ea2`: `Ok subjects=0 held=64 more=true`, the honest `g1` never judged.
#[tokio::test(flavor = "current_thread")]
async fn d2_actor_a_fold3_m1_a_held_subjects_never_fill_the_cap() {
    for (source, key) in [("age", "age"), ("tip", "gate")] {
        let (db, honest) = sixty_four_held_and_one_live(source, key).await;
        let routed = owed_events::route(&db, &event(source, key))
            .await
            .unwrap_or_else(|e| panic!("{source}/{key}: delivery 4: {e}"));
        assert_eq!(
            (routed.subjects, routed.faults, routed.held, routed.more),
            (1, 0, 64, false),
            "{source}/{key}: the live subject judged past the 64 held, no continuation"
        );
        assert!(rows_of(&db, &honest, "g1") >= 1, "{source}/{key}: g1 judged");
        assert_eq!(
            db.int(&format!(
                "SELECT COUNT(*) FROM owed_dirty WHERE source='{source}' AND sourceKey='{key}'"
            )),
            64,
            "{source}/{key}: the held marks stand"
        );
    }
}

/// NEW2-M1 (b): AN ALL-HELD MESSAGE ENDS. Once the live subject is judged, a delivery of the pair inside the hold
/// meets only held subjects: `Ok`, no judge, no continuation (`continues` false), counted all-held; the consumer's
/// continuation and its counter go through `continues` and `all_held`. RED on `7bb95ea2`: `more = true` on every
/// such delivery, each publishing the pair again.
#[tokio::test(flavor = "current_thread")]
async fn d2_actor_a_fold3_m1_b_an_all_held_message_ends() {
    for (source, key) in [("age", "age"), ("tip", "gate")] {
        let (db, _) = sixty_four_held_and_one_live(source, key).await;
        owed_events::route(&db, &event(source, key)).await.unwrap();
        for delivery in 5..=7 {
            let routed = owed_events::route(&db, &event(source, key)).await.unwrap();
            assert_eq!(
                (routed.subjects, routed.faults, routed.held, routed.more),
                (0, 0, 64, false),
                "{source}/{key} delivery {delivery}: all held, no continuation"
            );
            assert!(!owed_events::continues(&routed), "{source}/{key}: no continuation");
            assert!(owed_events::all_held(&routed), "{source}/{key}: counted all-held");
        }
        // a message that judged something and has more behind it continues; one that judged nothing never does
        let mut r = owed_events::Routed {
            more: true,
            held: 3,
            ..Default::default()
        };
        assert!(!owed_events::continues(&r));
        r.subjects = 1;
        assert!(owed_events::continues(&r));
        assert!(!owed_events::all_held(&r));
    }
    let src = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/owed_events.rs")).unwrap();
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let body = squash(&src);
    assert!(body.contains(&squash("if continues(&routed) {")), "consume_one continues only through `continues`");
    assert!(
        body.contains(&squash("if all_held(&routed) {")),
        "the consumer counts an all-held message"
    );
    assert!(body.contains("owed_events_all_held_total"));
}

/// NEW2-M1 (c): THE SWEEP STILL RE-PUBLISHES THE PAIR WHEN THE HOLD EXPIRES. Inside the hold the pair of only held
/// subjects is left out; past it the pair is re-published, and its delivery judges the subjects again.
#[tokio::test(flavor = "current_thread")]
async fn d2_actor_a_fold3_m1_c_the_sweep_republishes_the_pair_when_the_hold_ends() {
    for (source, key) in [("age", "age"), ("tip", "gate")] {
        let (db, _) = sixty_four_held_and_one_live(source, key).await;
        owed_events::route(&db, &event(source, key)).await.unwrap();
        db.exec(&format!(
            "UPDATE owed_dirty SET markedAtMs={}",
            db.now.get() as i64 - 4 * owed_events::SWEEP_AFTER_MS
        ));
        assert!(
            owed_events::sweep(&db, db.now.get() as i64).await.unwrap().is_empty(),
            "{source}/{key}: held, left out"
        );
        *db.fail_bind.borrow_mut() = None;
        db.now.set(db.now.get() + owed_events::FAULT_HOLD_BASE_MS as u64 + 1);
        let swept = owed_events::sweep(&db, db.now.get() as i64).await.unwrap();
        assert_eq!(swept, vec![(source.to_string(), key.to_string())], "{source}/{key}: re-published");
        let routed = owed_events::route(&db, &event(source, key)).await.unwrap();
        assert_eq!((routed.subjects, routed.held, routed.more), (64, 0, false), "{source}/{key}");
        assert_eq!(db.int("SELECT COUNT(*) FROM owed_dirty"), 0);
    }
}

/// D2-ACTOR-A-DELTA-2's LOW-1 and LOW-2. LOW-1: a message the wall drops notes the SUBJECTS of the judge call in flight
/// on their own memos (the third such drop holds them), never the pair, and is handed back with the longest hold that
/// set (at least the wall's 60 s); with no call in flight the pair is noted as before. LOW-2: a memo whose hold ended
/// more than 6 h ago counts its next fault from 0.
#[tokio::test(flavor = "current_thread")]
async fn d2_actor_a_fold3_low1_low2_the_wall_notes_its_subjects_and_a_stale_memo_counts_from_zero() {
    let db = Db::new();
    let names = vec![
        owed_events::subject_memo_name(ID, "g1"),
        owed_events::subject_memo_name(ID, "g2"),
    ];
    assert_eq!(owed_events::note_wall_subjects(&db, &[]).await, None, "none in flight: the pair's");
    for drop in 1..=3 {
        let delay = owed_events::note_wall_subjects(&db, &names).await.unwrap();
        let want = if drop < 3 {
            owed_events::CONSUMER_WALL_RETRY_S
        } else {
            (owed_events::FAULT_HOLD_BASE_MS / 1000) as u32
        };
        assert_eq!(delay, want.max(owed_events::CONSUMER_WALL_RETRY_S), "drop {drop}");
    }
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_belt_state WHERE name LIKE 'owed-events:fault:subject %'"),
        2
    );
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_belt_state WHERE name LIKE 'owed-events:fault:%' AND name NOT LIKE 'owed-events:fault:subject %'"),
        0,
        "no pair memo"
    );
    // LOW-2: past the hold plus 6 h the next fault is the first
    let memo = db.int(&format!(
        "SELECT json_extract(cursor,'$.heldUntilMs') FROM owed_belt_state WHERE name='{}'",
        names[0]
    ));
    db.now.set((memo + owed_events::FAULT_HOLD_CAP_MS + 1) as u64);
    let m = owed_events::note_fault_named(&db, &names[0], "transient").await.unwrap();
    assert_eq!((m.faults, m.hold_ms), (1, 0), "a stale memo counts from 0");
    let c = serde_json::json!({"faults": 9, "heldUntilMs": 1_000});
    assert_eq!(owed_events::stale_memo_faults(&c, 1_000 + owed_events::FAULT_HOLD_CAP_MS), 9);
    assert_eq!(owed_events::stale_memo_faults(&c, 1_001 + owed_events::FAULT_HOLD_CAP_MS), 0);
    // the wiring: every judge call enters its subjects into the in-flight set and leaves it when it answers (FIX-952:
    // a set, the pass's identities delivered at once); the wall's branch takes them
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let src = squash(include_str!("../src/owed_events.rs"));
    assert_eq!(src.matches("enter_in_flight(&identity,chunk);").count(), 1);
    assert_eq!(src.matches("leave_in_flight(&identity,chunk);").count(), 1);
    assert_eq!(src.matches("enter_in_flight(&identity,one);").count(), 1);
    assert_eq!(src.matches("leave_in_flight(&identity,one);").count(), 1);
    assert!(src.contains("letin_flight=take_in_flight();"));
    // D2-ACTOR-B-FOLD-11: both arms call one rule, `wall_fate` (the subjects in flight, else the pair)
    assert!(src.contains("matchnote_wall_subjects(db,in_flight).await{"));
    assert!(src.contains("delay=wall_fate(&sql,&[(&ev.source,&ev.source_key)],&in_flight).await;"));
    assert!(src.contains("letin_flight=take_in_flight();letwalled_pairs"));
}

/// NEW2-M1's statements on their indexes: the marks' reads and the held counts walk `owed_dirty` by the named index and
/// probe `owed_belt_state` by its primary key (never a scan of either).
#[test]
fn d2_actor_a_fold3_m1_d_the_route_reads_are_index_searches() {
    let db = Db::new();
    for (sql, index) in [
        (owed_events::ROUTE_MARKS_SQL, "idx_owed_dirty_source_key"),
        (owed_events::ROUTE_HELD_SQL, "idx_owed_dirty_source_key"),
        (owed_events::ROUTE_IDENTITY_SQL, "idx_owed_dirty_identity_marked"),
        (owed_events::ROUTE_IDENTITY_HELD_SQL, "idx_owed_dirty_identity_marked"),
    ] {
        let conn = db.conn.borrow();
        let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        let plan: Vec<String> = stmt
            .query_map(rusqlite::params_from_iter(std::iter::repeat_n(rusqlite::types::Value::Null, sql.matches('?').count().min(4))), |r| r.get::<_, String>(3))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let text = plan.join(" | ");
        assert!(text.contains(index), "{sql}: {text}");
        assert!(!text.contains("SCAN owed_belt_state"), "{sql}: {text}");
        assert!(text.contains("owed_belt_state USING"), "{sql}: the memo probed by key: {text}");
    }
}

/// D2-ACTOR-A-DELTA-3's R1: the consumer's judge only queues its pushes (bsv-low #935's coalescer), so the queue
/// event flushes them, awaited, after `owed_events::queue` and before the batch's answer. RED with the flush
/// removed: "the queue event flushes the coalesced pushes after the consumer".
#[test]
fn delta3_r1_the_queue_event_flushes_the_consumers_pushes() {
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let lib = squash(include_str!("../src/lib.rs"));
    let at = lib.find("#[event(queue)]").unwrap();
    let end = at + lib[at..].find("#[event(fetch)]").unwrap();
    let wrapper = &lib[at..end];
    let consumer = wrapper
        .find("letout=owed_events::queue(batch,env.clone()).await;")
        .expect("the consumer");
    let flush = wrapper
        .find("push_coalesce::flush_queued(env).await;")
        .expect("the queue event flushes the coalesced pushes after the consumer");
    let answer = wrapper.rfind("out}").unwrap();
    assert!(
        consumer < flush && flush < answer,
        "after the consumer, before the answer"
    );
    // bsv-low #912 (the D2-ACTOR-B lens's MEDIUM-6): the one `wait_until` of the handler is Phase A's deferred actor
    // apply; the pushes' flush is awaited, never handed to it.
    assert_eq!(
        wrapper.matches("wait_until").count(),
        wrapper
            .matches("ctx.wait_until(owed_actor::run_deferred(env.clone(),deferred));")
            .count(),
        "the pushes are awaited in the handler, never handed to wait_until"
    );
}

/// D2-ACTOR-A-DELTA-3's LOW-1: the consumer's word ingest takes back the `word` note its write left, since the same
/// message routes that pair. RED without the line: "the ingest forgets its own word note".
#[test]
fn delta3_low1_the_ingest_forgets_its_word_note() {
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let src = squash(include_str!("../src/owed_events.rs"));
    let at = src.find("asyncfningest_frame(").unwrap();
    let body = &src[at..at + src[at..].find("pubconstTIP_MEMO_MS").unwrap()];
    let event = body.find("runtime::event(env,&txid,").unwrap();
    let forget = body
        .find("publish::forget(\"word\",&txid);")
        .expect("the ingest forgets its own word note");
    assert!(event < forget);
}

/// D2-ACTOR-A-FOLD-5 (bsv-low #911, E2): `/health.owedEvents` serves the queue's two totals as NUMBERS (bsv-low's
/// harness reads `publishedTotal - consumedTotal` there for its `owed-queue-backlog` row): 0 with no counter row,
/// the durable counters once written, beside the ages and the pointer to the overlay's totals; read by ONE
/// statement keyed by exactly the two names. RED on e9f806e3: the keys were gone (`Null`).
#[tokio::test(flavor = "current_thread")]
async fn d2_actor_a_fold5_the_health_serves_published_and_consumed_as_numbers() {
    let db = Db::new();
    let dirty = serde_json::json!({"count": 0, "oldestMarkAgeMs": null});
    let empty = owed_events::health(&db, &dirty).await;
    assert_eq!(
        (
            empty["publishedTotal"].as_u64(),
            empty["consumedTotal"].as_u64()
        ),
        (Some(0), Some(0)),
        "absent rows serve 0: {empty}"
    );
    db.exec(
        "INSERT INTO ops_counters(name,value) VALUES ('owed_events_published_total',7),\
         ('owed_events_consumed_total',5),('owed_events_swept_total',3)",
    );
    let body = owed_events::health(&db, &dirty).await;
    assert_eq!(body["publishedTotal"].as_u64(), Some(7), "{body}");
    assert_eq!(body["consumedTotal"].as_u64(), Some(5), "{body}");
    assert_eq!(body["totals"], "/health/invariants.owedEvents");
    assert_eq!(body["unackedMarks"], 0);
    assert!(
        body.get("sweptTotal").is_none(),
        "only the two totals: {body}"
    );
    let sql = owed_events::HEALTH_TOTALS_SQL;
    assert!(sql.contains("ops_counters") && sql.contains("name IN ("));
    assert_eq!(sql.matches("'owed_events_").count(), 2, "{sql}");
}

// ---- D2-ACTOR-B-FOLD-11 (the sixth delta lens's NEW6-M2) ----

/// A [`Db`] whose judge write of any subject naming `slow` never answers (a subject whose judge outlasts every wall).
struct Slow<'a> {
    db: &'a Db,
    slow: &'static str,
}
impl SqlPort for Slow<'_> {
    async fn run_batch(
        &self,
        stmts: Vec<low_app_layer::pot_hook::Stmt>,
    ) -> Result<Vec<low_app_layer::pot_hook::StmtOut>, String> {
        let writes_rows = stmts.iter().any(|s| s.sql.contains("INSERT INTO owed_rows"));
        let names = stmts.iter().any(|s| {
            s.binds.iter().any(
                |b| matches!(b, low_app_layer::pot_hook::Bind::Text(t) if t.contains(self.slow)),
            )
        });
        if writes_rows && names {
            std::future::pending::<()>().await;
        }
        self.db.run_batch(stmts).await
    }
    async fn counters(&self, deltas: Vec<(String, u64)>) {
        self.db.counters(deltas).await;
    }
    fn now_ms(&self) -> u64 {
        self.db.now_ms()
    }
    fn log(&self, line: &str) {
        self.db.log(line);
    }
}
impl low_app_layer::owed_judge::JudgeDb for Slow<'_> {
    fn tip(&self) -> Option<u64> {
        self.db.tip()
    }
    async fn word_views(
        &self,
        rows: &std::collections::BTreeMap<String, low_app_layer::tracker_host::stored::Row>,
    ) -> Result<std::collections::BTreeMap<String, serde_json::Value>, String> {
        self.db.word_views(rows).await
    }
    async fn canonical_root(&self, height: u64) -> Option<String> {
        self.db.canonical_root(height).await
    }
    async fn push(&self, identity: &str, body: serde_json::Value) {
        self.db.push(identity, body).await;
    }
}

fn fold11_wall() -> tokio::time::Sleep {
    tokio::time::sleep(std::time::Duration::from_millis(40))
}

/// One delivery of `evs` through the shipped consumer core: a batch of several through `batch_core`, then each
/// message the pass left (`Fate::Alone`; a batch of one is the one-message arm) through the one-message arm's own
/// rule: `route` raced against its wall, a wall noted by `wall_fate`, a fault by `fault_fate` (the queue handler's
/// single arm is those calls around `consume_one`, whose plan and judge are `route`'s). Answers each message's fate.
async fn fold11_deliver(db: &Db, slow: &Slow<'_>, evs: &[OwedEvent]) -> Vec<owed_events::Fate> {
    use owed_events::Fate;
    let refs: Vec<&OwedEvent> = evs.iter().collect();
    let mut fates = vec![Fate::Alone; evs.len()];
    if evs.len() > 1 {
        let mut plans = Vec::new();
        for ev in evs {
            plans.push(owed_events::plan(db, ev).await.unwrap());
        }
        let make = |_, _| async { Ok::<Slow, String>(Slow { db, slow: slow.slow }) };
        let out = owed_events::batch_core(db, &refs, &plans, make, fold11_wall())
            .await
            .unwrap();
        fates = out.fates;
        if out.walled {
            return fates;
        }
    }
    for (ev, fate) in evs.iter().zip(fates.iter_mut()) {
        if *fate != Fate::Alone {
            continue;
        }
        let _ = owed_events::take_in_flight();
        let work = std::pin::pin!(owed_events::route(slow, ev));
        let wall = std::pin::pin!(fold11_wall());
        *fate = match futures_util::future::select(work, wall).await {
            futures_util::future::Either::Left((Ok(r), _)) => Fate::Ack {
                resolve: r.subjects > 0 || r.held == 0,
                continues: owed_events::continues(&r),
            },
            futures_util::future::Either::Left((Err(e), _)) => Fate::Fault {
                delay_s: owed_events::fault_fate(db, &ev.source, &ev.source_key, &e).await,
            },
            futures_util::future::Either::Right(_) => {
                let in_flight = owed_events::take_in_flight();
                Fate::Retry {
                    delay_s: Some(
                        owed_events::wall_fate(db, &[(&ev.source, &ev.source_key)], &in_flight).await,
                    ),
                }
            }
        };
    }
    fates
}

/// (fold11_c) NEW6-M2: 16 MESSAGES FOR ONE IDENTITY WHOSE FIRST SUBJECT NEVER ANSWERS END WITH THAT SUBJECT HELD AND
/// THE OTHER 15 JUDGED. The first coalesced batch walls with the chunk in flight: each in-flight subject noted on its
/// own memo (as the one-message arm notes it) and handed back with its delay; the redelivery takes no message whose
/// subject carries a memo into the coalesced pass, so each is judged alone: the slow one walls alone (its second
/// note), the other 15 land and their memos clear; its third wall holds it; its next delivery is all held. And the
/// merged plan is ONE message's bound: three messages of 30 games, the third left to the one-message arm. On
/// `26f7443c6` (no `batch_core`; its batch arm hands all 16 back on every wall, noting nothing) the pin does not
/// compile; with `batch_core`'s memo read made inert (every message coalesced) it is RED: nothing is ever judged.
#[tokio::test(flavor = "current_thread")]
async fn fold11_c_a_walled_subject_is_held_and_the_other_fifteen_judged() {
    use owed_events::Fate;
    let db = Db::new();
    let games: Vec<String> = (0..16)
        .map(|k| if k == 0 { "slow00".to_string() } else { format!("g{k:02}") })
        .collect();
    for (k, g) in games.iter().enumerate() {
        seed_stranded_hop(&db, ID, g, &owed_db::hop(200 + k));
    }
    let evs: Vec<OwedEvent> = (0..16)
        .map(|k| event("hop", &hop_key(&owed_db::hop(200 + k))))
        .collect();
    let slow = Slow { db: &db, slow: "slow00" };
    // delivery 1: the batch walls, its subjects in flight noted, every message handed back
    let first = fold11_deliver(&db, &slow, &evs).await;
    assert!(
        first.iter().all(|f| matches!(f, Fate::Retry { delay_s: Some(_) })),
        "{first:?}"
    );
    let memo = |g: &str| {
        db.opt(&format!(
            "SELECT (SELECT json_extract(cursor,'$.faults') FROM owed_belt_state WHERE name='{}')",
            owed_events::subject_memo_name(ID, g)
        ))
    };
    assert_eq!(memo("slow00"), Some(1), "the walled subject noted");
    // delivery 2: no message with a noted subject is coalesced: each alone, the 15 land
    let second = fold11_deliver(&db, &slow, &evs).await;
    assert!(matches!(second[0], Fate::Retry { .. }), "{:?}", second[0]);
    assert!(second[1..].iter().all(|f| *f == Fate::Ack { resolve: true, continues: false }), "{second:?}");
    for g in &games[1..] {
        assert_eq!(rows_of(&db, ID, g), 1, "{g} judged");
        assert_eq!(memo(g), None, "{g}'s memo cleared by its landing");
    }
    assert_eq!(memo("slow00"), Some(2));
    // delivery 3 (the slow one alone): its third wall holds it
    fold11_deliver(&db, &slow, &evs[..1]).await;
    let until = db.int(&format!(
        "SELECT json_extract(cursor,'$.heldUntilMs') FROM owed_belt_state WHERE name='{}'",
        owed_events::subject_memo_name(ID, "slow00")
    ));
    assert!(until > db.now.get() as i64, "held");
    // delivery 4: all held, nothing resolved, its mark standing
    let fourth = fold11_deliver(&db, &slow, &evs[..1]).await;
    assert_eq!(fourth, vec![Fate::Ack { resolve: false, continues: false }]);
    assert_eq!(rows_of(&db, ID, "slow00"), 0);
    assert_eq!(db.int("SELECT COUNT(*) FROM owed_dirty"), 1, "the held subject's mark stands");

    // THE BOUND: three messages of 30 games each; the pass takes two (60 <= 64), the third is left alone.
    let db = Db::new();
    for k in 0..90 {
        seed_stranded_hop(&db, ID, &format!("b{k:02}"), &owed_db::hop(400 + k));
    }
    for m in 0..3 {
        db.exec(&format!(
            "UPDATE owed_dirty SET source='age',sourceKey='age{m}' WHERE gameId IN (SELECT value FROM json_each('{}'))",
            serde_json::json!((30 * m..30 * m + 30).map(|k| format!("b{k:02}")).collect::<Vec<_>>())
        ));
    }
    let evs: Vec<OwedEvent> = (0..3).map(|m| event("age", &format!("age{m}"))).collect();
    let mut plans = Vec::new();
    for ev in &evs {
        plans.push(owed_events::plan(&db, ev).await.unwrap());
    }
    assert!(plans.iter().all(|p| p.game_count() == 30));
    assert_eq!(owed_events::batch_selection(&plans, &[false; 3]), vec![true, true, false]);
    let refs: Vec<&OwedEvent> = evs.iter().collect();
    let make = |_, _| async { Ok::<Slow, String>(Slow { db: &db, slow: "~never~" }) };
    let out = owed_events::batch_core(&db, &refs, &plans, make, std::future::pending::<()>())
        .await
        .unwrap();
    assert_eq!(out.coalesced, 2);
    assert_eq!(out.routed.subjects, 60, "one message's bound");
    assert_eq!(out.fates[2], Fate::Alone);
    assert!(matches!(out.fates[0], Fate::Ack { resolve: true, .. }));
}

/// (fold11_d) NEW6-M2: A MESSAGE WHOSE EVERY SUBJECT WAS HELD LEAVES ITS FAULT MEMO AND ITS DEAD LETTER IN PLACE INSIDE
/// A COALESCED BATCH. Three messages: a live one (judged, resolved), one whose only subject is held (taken: nothing to
/// judge; resolves nothing, its dead letter stays), and one whose pair carries a fault memo (left to the one-message
/// arm: the batch touches neither its memo nor its dead letter). The statements applied are the queue handler's
/// (`resolved_statements` for each `Ack { resolve: true }`). On `26f7443c6` every message of a batch that judged a
/// subject was resolved (`routed.subjects > 0` over the MERGED routing): both dead letters and the memo went.
#[tokio::test(flavor = "current_thread")]
async fn fold11_d_an_all_held_message_resolves_nothing_inside_a_batch() {
    use owed_events::Fate;
    let db = Db::new();
    seed_stranded_hop(&db, ID, "live", &owed_db::hop(501));
    seed_stranded_hop(&db, ID, "heldgame", &owed_db::hop(502));
    seed_stranded_hop(&db, ID, "memo", &owed_db::hop(503));
    let evs: Vec<OwedEvent> = [501, 502, 503]
        .iter()
        .map(|n| event("hop", &hop_key(&owed_db::hop(*n))))
        .collect();
    let now = db.now.get() as i64;
    // the held subject's memo (held for an hour) and the third pair's fault memo; a dead letter for each of the two
    db.exec(&format!(
        "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('{}','{}',{now}),('{}','{}',{now})",
        owed_events::subject_memo_name(ID, "heldgame"),
        serde_json::json!({"faults": 3, "heldUntilMs": now + 3_600_000}),
        owed_events::fault_memo_name(&evs[2].source, &evs[2].source_key),
        serde_json::json!({"faults": 1, "heldUntilMs": now}),
    ));
    for ev in &evs[1..] {
        db.exec(&format!(
            "INSERT INTO owed_event_dead_letters(source,sourceKey,messageId,body,parkedAt) VALUES ('{}','{}','m','{{}}',{now})",
            ev.source, ev.source_key
        ));
    }
    let mut plans = Vec::new();
    for ev in &evs {
        plans.push(owed_events::plan(&db, ev).await.unwrap());
    }
    let refs: Vec<&OwedEvent> = evs.iter().collect();
    let make = |_, _| async { Ok::<Slow, String>(Slow { db: &db, slow: "~never~" }) };
    let out = owed_events::batch_core(&db, &refs, &plans, make, std::future::pending::<()>())
        .await
        .unwrap();
    assert!(out.routed.subjects > 0, "the batch judged the live subject");
    assert_eq!(
        out.fates,
        vec![
            Fate::Ack { resolve: true, continues: false },
            Fate::Ack { resolve: false, continues: false },
            Fate::Alone
        ]
    );
    // the queue handler's statements for the acked messages
    let mut stmts = Vec::new();
    for (ev, f) in evs.iter().zip(&out.fates) {
        if matches!(f, Fate::Ack { resolve: true, .. }) {
            stmts.extend(owed_events::resolved_statements(&ev.source, &ev.source_key));
        }
    }
    db.run_batch(stmts).await.unwrap();
    assert_eq!(db.int("SELECT COUNT(*) FROM owed_event_dead_letters"), 2, "both dead letters stay");
    assert_eq!(
        db.int(&format!(
            "SELECT COUNT(*) FROM owed_belt_state WHERE name='{}'",
            owed_events::fault_memo_name(&evs[2].source, &evs[2].source_key)
        )),
        1,
        "the pair's memo stays"
    );
    assert_eq!(rows_of(&db, ID, "live"), 1);
    assert_eq!(rows_of(&db, ID, "heldgame"), 0);
}

/// FIX-969 (bsv-low #969): the queue's deliveries of ONE message modelled over the SHIPPED wall decision
/// (`hand_back_walled`) and the DLQ's park (`park_letter`): 1 + `max_retries` (3) deliveries of a message spend its
/// retries, the fifth delivery is the dead letter queue's. `walls_before_landing`: how many deliveries in a row meet
/// the consumer's wall before one lands. Answers (re-queues, retries spent, parked).
struct Requeues(RefCell<Vec<(String, u32)>>, bool);

impl owed_events::RequeuePort for Requeues {
    async fn send_delayed(&self, body: String, delay_s: u32) -> Result<(), String> {
        if self.1 {
            return Err("the send faulted".into());
        }
        self.0.borrow_mut().push((body, delay_s));
        Ok(())
    }
}

async fn deliver_walled(db: &Db, body: &str, walls_before_landing: u32, port: &Requeues) -> (u32, u32, bool) {
    use owed_events::Handback;
    let (mut requeued, mut retries) = (0u32, 0u32);
    // the message on the queue: its body and the platform's delivery count (a fresh message starts at 0)
    let (mut body, mut attempts) = (body.to_string(), 0u32);
    for wall in 0..walls_before_landing + 20 {
        if wall >= walls_before_landing {
            return (requeued, retries, false); // this delivery landed: acked
        }
        attempts += 1;
        let ev = OwedEvent::decode(&body).expect("a re-queued body still decodes");
        let _ = owed_events::wall_fate(db, &[(&ev.source, &ev.source_key)], &[]).await;
        match owed_events::hand_back_walled(port, &body, owed_events::CONSUMER_WALL_RETRY_S).await {
            Handback::Requeued => {
                requeued += 1;
                body = port.0.borrow().last().unwrap().0.clone();
                attempts = 0;
            }
            Handback::Retried => {
                retries += 1;
                if attempts > 3 {
                    // max_retries spent: the dead letter queue's delivery parks it
                    owed_events::park_letter(db, "m1", &body).await.unwrap();
                    return (requeued, retries, true);
                }
            }
        }
    }
    unreachable!()
}

/// FIX-969 (a): A WALLED MESSAGE IS RE-QUEUED FRESH, NEVER PARKED. A message that meets the consumer's message wall
/// six times in a row under a backlog (each wall a batch past `CONSUMER_MESSAGE_WALL_MS`), then lands: re-queued six
/// times, no queue retry spent, no letter; the re-queued body decodes to the same event (the frozen v1 bytes plus the
/// `walls` key). The base rule (every wall a `retry`) parks it at its fourth wall: RED on 149869bf (the API is new),
/// and with `wall_carriage` answering `Retry` alone: `left: (0, 4, true) right: (6, 0, false)`.
#[tokio::test(flavor = "current_thread")]
async fn fix969_a_a_walled_message_is_requeued_never_a_spent_retry() {
    let db = Db::new();
    let ev = event("txfacts", "aa".repeat(32).as_str());
    let body = ev.encode();
    let port = Requeues(RefCell::new(Vec::new()), false);
    assert_eq!(deliver_walled(&db, &body, 6, &port).await, (6, 0, false));
    assert_eq!(db.int("SELECT COUNT(*) FROM owed_event_dead_letters"), 0);
    let sent = port.0.borrow();
    let last: serde_json::Value = serde_json::from_str(&sent[5].0).unwrap();
    assert_eq!(last[owed_events::WALLS_FIELD], 6);
    assert_eq!(sent[5].1, owed_events::CONSUMER_WALL_RETRY_S, "under the wall's delay");
    let again = OwedEvent::decode(&sent[5].0).unwrap();
    assert_eq!((again.seq, again.source, again.source_key, again.at_ms), (ev.seq, ev.source.clone(), ev.source_key.clone(), ev.at_ms));
    // an unwalled event's bytes are the frozen v1 bytes: no `walls` key until its first wall
    assert!(!body.contains("\"walls\""));
    assert_eq!(
        owed_events::wall_carriage("not an event"),
        owed_events::WallCarriage::Retry,
        "a body that is not an object keeps the queue's retry"
    );
}

/// FIX-969 (b): THE BOUND, AND A TRUE FAULT KEEPS ITS PARK. A message that walls for ever is re-queued
/// `WALL_REQUEUES_MAX` (8) times, then handed back by `retry`: the queue's 3 retries, then the dead letter queue parks
/// it ON ITS FOURTH such delivery, the letter's body carrying `walls: 8` and its `lastFault` the wall's ("the
/// consumer's message wall", the pair's memo): a WALL letter, readable as such. A send that faults falls back to the
/// retry. A judge fault is never re-queued (its arm hands back by `retry`, unchanged): its letter is parked with the
/// judge's own text and no `walls`.
#[tokio::test(flavor = "current_thread")]
async fn fix969_b_the_bound_and_a_fault_letter_keeps_its_park() {
    let db = Db::new();
    let ev = event("pot", &hop_key(&owed_db::hop(3)));
    let port = Requeues(RefCell::new(Vec::new()), false);
    let (requeued, retries, parked) = deliver_walled(&db, &ev.encode(), u32::MAX - 30, &port).await;
    assert_eq!((requeued, retries, parked), (owed_events::WALL_REQUEUES_MAX as u32, 4, true));
    let letter: serde_json::Value =
        serde_json::from_str(&db.text("SELECT body FROM owed_event_dead_letters").unwrap()).unwrap();
    assert_eq!(letter[owed_events::WALLS_FIELD], owed_events::WALL_REQUEUES_MAX);
    assert_eq!(
        db.text("SELECT lastFault FROM owed_event_dead_letters").as_deref(),
        Some("the consumer's message wall")
    );
    // a faulted send: the retry (its retries spent as before the fix)
    let down = Requeues(RefCell::new(Vec::new()), true);
    assert_eq!(
        owed_events::hand_back_walled(&down, &ev.encode(), 60).await,
        owed_events::Handback::Retried
    );
    // a true fault: the judge's text on the pair's memo, then its park (the fault arm is unchanged)
    let db = Db::new();
    let fault = event("hop", &hop_key(&owed_db::hop(4)));
    owed_events::fault_fate(&db, &fault.source, &fault.source_key, "a row write faulted").await;
    owed_events::park_letter(&db, "f1", &fault.encode()).await.unwrap();
    assert_eq!(
        db.text("SELECT lastFault FROM owed_event_dead_letters").as_deref(),
        Some("a row write faulted")
    );
    assert!(!db.text("SELECT body FROM owed_event_dead_letters").unwrap().contains("\"walls\""));
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let src = squash(include_str!("../src/owed_events.rs"));
    assert!(
        src.contains("Fate::Fault{delay_s}=>{*deltas.entry(\"owed_events_judge_faults_total\").or_default()+=1;ifdelay_s.is_some(){*deltas.entry(\"owed_events_fault_held_total\").or_default()+=1;}retry(*delay_s);}"),
        "a judge fault keeps the queue's retry (its park)"
    );
}

/// FIX-969 (c): THE WIRING AND THE COUNTERS. Every wall site of the consumer (the coalesced batch's walled fates, the
/// singles a walled batch leaves, the one-message arm) hands back through `hand_back_msg` (the shipped
/// `hand_back_walled`), never `retry` alone; each carriage is counted apart (`owed_events_wall_requeued_total`, and
/// `owed_events_wall_retried_total` for those that spend a queue retry), beside `owed_events_consumer_wall_drops_total`
/// (one a wall), every name seeded by the overlay. RED on 149869bf (the API does not compile there); with the walled
/// singles put back to `retry` alone: "the singles a walled batch leaves re-queue".
#[test]
fn fix969_c_every_wall_site_requeues_and_counts_apart() {
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let src = squash(include_str!("../src/owed_events.rs"));
    assert!(
        src.contains("Fate::Retry{delay_s}=>{hand_back_msg(msg,&requeue,delay_s.unwrap_or(0),&mutdeltas).await}"),
        "the coalesced wall re-queues"
    );
    assert!(
        src.contains("foriinsingles.drain(..){messages+=1;hand_back_msg(&raw[i],&requeue,0,&mutdeltas).await;}"),
        "the singles a walled batch leaves re-queue"
    );
    assert!(
        src.contains("hand_back_msg(msg,&requeue,delay,&mutdeltas).await;continue;"),
        "the one-message wall re-queues"
    );
    assert!(!src.contains("raw[i].retry();}"), "no walled single spends a retry");
    assert!(src.contains("matchhand_back_walled(port,&body,delay_s).await{Handback::Requeued=>{*deltas.entry(COUNTER_WALL_REQUEUED).or_default()+=1;msg.ack();}Handback::Retried=>{*deltas.entry(COUNTER_WALL_RETRIED).or_default()+=1;"));
    for name in [
        owed_events::COUNTER_WALL_REQUEUED,
        owed_events::COUNTER_WALL_RETRIED,
        "owed_events_consumer_wall_drops_total",
    ] {
        assert!(publish::COUNTERS.contains(&name), "{name} seeded");
    }
}
