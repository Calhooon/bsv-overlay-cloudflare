//! bsv-low #911 (lane D2-ACTOR-A): the `owed-events` consumer's routing and acknowledgement, the un-acked mark sweep,
//! a refused publish shipped by the sweep, and the event shape, through the shipped functions over real SQLite with
//! the overlay's migrations (`support/owed_db.rs`).
#[allow(dead_code)]
#[path = "support/owed_db.rs"]
mod owed_db;

use low_app_layer::{owed_events, owed_events_publish as publish};
use overlay_discovery::owed_event::{self, OwedEvent};
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
/// backlog index, then the shipped publish (seq and facts in one batch), the sent bodies recorded.
#[tokio::test]
async fn d2_actor_a_e_the_sweep_republishes_marks_older_than_a_tick_only() {
    let db = Db::new();
    let now = NOW as i64;
    let tick = owed_events::SWEEP_AFTER_MS;
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
/// its pair; the publish is refused (a Queues incident, a `wait_until` wall); one tick later the sweep re-publishes
/// that pair, and the consumer routes it.
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
    assert_eq!(report.unsent, vec![note]);
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_dirty"),
        1,
        "the mark stands"
    );
    // the trigger dates its mark by the database's clock (the write's own time)
    let marked = db.int("SELECT markedAtMs FROM owed_dirty");
    // inside the tick: nothing swept
    assert!(owed_events::sweep(&db, marked).await.unwrap().is_empty());
    // past the tick: the pair is re-published and routed
    db.now.set((marked + owed_events::SWEEP_AFTER_MS + 1) as u64);
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
    assert!(door.contains(&squash("if crate::owed_events::configured(&ctx.env) {")));
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
