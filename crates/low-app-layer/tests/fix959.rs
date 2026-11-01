//! FIX-959 (bsv-low #959, the (b) rows; the reading `docs/audit/ACTOR-BARS-WARM-READ-2026-10-31.md` in bsv-low):
//! what the direct pass and the warm apply read of D1 for the events the loop sends.
//!
//! - `fix959_a`: the writer's direct pass over the loop's non-carried events (three pair events, the shape every
//!   source but `identity` reads, a pot's and a hop's alike, and the hop hook's identity event) for two identities, in PHASE C (the bounds read): ONE D1 batch before the first actor call (the marks of
//!   every event and each body's acknowledgement bound together), so the first call is one round trip after the
//!   pass begins; each body's bound equals the per-body read (`direct_bound`), asserted value for value (review
//!   round 1's LOW-1: RED with `MAX(d.generation)+1000` in both bound statements), and its marks are acknowledged. RED with
//!   the port's two batched reads removed (the trait's defaults, the base's rule: one read an event, then one a body,
//!   in a row): 6 batches and 600 ms before the first call.
//! - `fix959_b`: Phase A/B (no acknowledgement): one batch, no bound statement in it.
#[allow(dead_code)]
#[path = "support/owed_db.rs"]
mod owed_db;

use low_app_layer::owed_events::{self, DirectPort, GameBounds, Subjects};
use low_app_layer::pot_hook::{SqlPort, Stmt, StmtOut};
use overlay_discovery::owed_event::{self, OwedEvent};
use owed_db::{seed_stranded_hop, Db, NOW};
use serde_json::{json, Value};
use std::cell::{Cell, RefCell};

const RTT_MS: u64 = 100;

fn identity(k: usize) -> String {
    format!("02{k:064x}")
}

fn event(source: &str, key: &str, seq: i64) -> OwedEvent {
    OwedEvent {
        v: owed_event::OWED_EVENT_V,
        seq,
        source: source.into(),
        source_key: key.into(),
        facts: Value::Null,
        at_ms: NOW as i64,
    }
}

/// D1 at one round trip a batch (tokio's paused clock), every batch counted with the virtual time it answered.
struct SlowD1<'a> {
    db: &'a Db,
    t0: tokio::time::Instant,
    batches: RefCell<Vec<(u64, Vec<String>)>>,
}
impl SqlPort for SlowD1<'_> {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        tokio::time::sleep(std::time::Duration::from_millis(RTT_MS)).await;
        let at = self.t0.elapsed().as_millis() as u64;
        self.batches
            .borrow_mut()
            .push((at, stmts.iter().map(|s| s.sql.to_string()).collect()));
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

/// The Worker's direct port's reads (`WorkerDirect`: `plan_with`, `direct_bound` and, since FIX-959, the two batched
/// reads) over [`SlowD1`], the actors answering at once.
struct Port<'a> {
    sql: SlowD1<'a>,
    acks: bool,
    first_call_at: Cell<Option<u64>>,
    calls: RefCell<Vec<Value>>,
}
impl DirectPort for Port<'_> {
    async fn subjects(&self, ev: &OwedEvent) -> Result<Subjects, String> {
        Ok(owed_events::plan_with(&self.sql, ev, false)
            .await?
            .subjects())
    }
    async fn call(&self, body: Value) -> Result<Value, String> {
        if self.first_call_at.get().is_none() {
            self.first_call_at
                .set(Some(self.sql.t0.elapsed().as_millis() as u64));
        }
        self.calls.borrow_mut().push(body.clone());
        Ok(
            json!({"applied": true, "firstApply": true, "appliedGames": body["games"], "rows": [], "atMs": NOW, "judgedAtMs": NOW + 40, "applyMs": 5, "noted": true}),
        )
    }
    fn acks(&self) -> bool {
        self.acks
    }
    async fn bound(&self, identity: &str, games: &[String]) -> Result<i64, String> {
        owed_events::direct_bound(&self.sql, identity, games).await
    }
    async fn ack(&self, stmts: Vec<Stmt>) -> Result<(), String> {
        self.sql.db.run_batch(stmts).await.map(|_| ())
    }
    // FIX-959: the two batched reads (remove both for the base's rule)
    async fn subjects_many(
        &self,
        events: &[OwedEvent],
        bounds: bool,
    ) -> (Vec<Result<Subjects, String>>, GameBounds) {
        owed_events::direct_subjects_many(&self.sql, events, bounds).await
    }
    async fn bounds_many(&self, asks: &[(String, Vec<String>)]) -> Vec<Result<i64, String>> {
        owed_events::direct_bounds_many(&self.sql, asks).await
    }
}

/// Two seats of a hand: each its stranded hop (its marker's `hop` mark and the pot row's `pot` mark), a second game
/// for seat 1 (a new game's first event), and the events the loop's writers send for them.
fn the_hand(db: &Db) -> (Vec<OwedEvent>, Vec<(String, Vec<String>)>) {
    let (a, b) = (identity(1), identity(2));
    let (h1, h2, h3) = (owed_db::hop(1), owed_db::hop(2), owed_db::hop(3));
    seed_stranded_hop(db, &a, "g1", &h1);
    seed_stranded_hop(db, &b, "g1", &h2);
    seed_stranded_hop(db, &a, "g2", &h3);
    let marks: Vec<(String, String, String, String)> = {
        let conn = db.conn.borrow();
        let mut s = conn
            .prepare("SELECT identity,gameId,source,sourceKey FROM owed_dirty ORDER BY generation")
            .unwrap();
        s.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    };
    assert!(!marks.is_empty(), "the seed writes marks");
    let mut events = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for (n, (_, _, source, key)) in marks.iter().enumerate() {
        if seen.insert((source.clone(), key.clone())) {
            events.push(event(source, key, 100 + n as i64));
        }
    }
    // the hop hook's identity event
    events.push(event(owed_event::ROUTE_IDENTITY, &b, 999));
    let expect = vec![
        (a.clone(), vec!["g1".to_string(), "g2".to_string()]),
        (b.clone(), vec!["g1".to_string()]),
    ];
    (events, expect)
}

async fn per_body_bounds(db: &Db, expect: &[(String, Vec<String>)]) -> Vec<i64> {
    let mut out = Vec::new();
    for (id, games) in expect {
        out.push(owed_events::direct_bound(db, id, games).await.unwrap());
    }
    out
}

/// (a) PHASE C: ONE D1 BATCH BEFORE THE FIRST CALL.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix959_a_phase_c_the_direct_pass_reads_marks_and_bounds_in_one_batch() {
    let db = Db::new();
    let (events, expect) = the_hand(&db);
    let n_events = events.len();
    assert!(
        n_events >= 4,
        "a pot, hop and identity event each: {n_events}"
    );
    let want_bounds = per_body_bounds(&db, &expect).await;
    // the review's LOW-1: each body's bound from the ONE batch equals the per-body read (`direct_bound`), the value
    // that decides which marks Phase C acknowledges (an inflated bound acknowledges marks the actor never read)
    let (_, batched) = owed_events::direct_subjects_many(&db, &events, true).await;
    let got_bounds: Vec<i64> = expect
        .iter()
        .map(|(id, games)| {
            owed_events::bound_of_games(&batched, id, games)
                .unwrap_or_else(|| panic!("every game of {id} read with the marks: {batched:?}"))
        })
        .collect();
    assert_eq!(
        got_bounds, want_bounds,
        "the batched bound is the per-body read's, body by body"
    );
    let port = Port {
        sql: SlowD1 {
            db: &db,
            t0: tokio::time::Instant::now(),
            batches: RefCell::new(Vec::new()),
        },
        acks: true,
        first_call_at: Cell::new(None),
        calls: RefCell::new(Vec::new()),
    };
    let report = owed_events::direct_with(&port, events, Some(900_000)).await;
    let first = port.first_call_at.get().expect("an actor was called");
    let before: Vec<_> = port
        .sql
        .batches
        .borrow()
        .iter()
        .filter(|(at, _)| *at <= first)
        .cloned()
        .collect();
    println!(
        "FIX-959 (a): {n_events} events, {} batches before the first call at {first} ms ({} statements), report {report:?}",
        before.len(),
        before.iter().map(|(_, s)| s.len()).sum::<usize>()
    );
    assert_eq!(
        (before.len(), first),
        (1, RTT_MS),
        "one D1 round trip before the first actor call"
    );
    assert_eq!(report.left_to_queue, 0, "{report:?}");
    let calls = port.calls.borrow();
    assert_eq!(calls.len(), 2, "one body an identity: {calls:?}");
    for (id, games) in &expect {
        let body = calls
            .iter()
            .find(|c| c["identity"].as_str() == Some(id.as_str()))
            .unwrap_or_else(|| panic!("a call for {id}"));
        let got: Vec<String> = serde_json::from_value(body["games"].clone()).unwrap();
        assert_eq!(&got, games);
    }
    // the bounds are the per-body read's: every mark at or under them acknowledged, none left
    assert!(want_bounds.iter().all(|b| *b > 0), "{want_bounds:?}");
    assert_eq!(report.acked, 3, "{report:?}");
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_dirty"),
        0,
        "every mark at or under the bound acknowledged"
    );
}

/// (b) PHASES A AND B: one batch, no bound read (nothing is acknowledged).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix959_b_phase_a_b_one_batch_no_bound() {
    let db = Db::new();
    let (events, _) = the_hand(&db);
    let marks_before = db.int("SELECT COUNT(*) FROM owed_dirty");
    let port = Port {
        sql: SlowD1 {
            db: &db,
            t0: tokio::time::Instant::now(),
            batches: RefCell::new(Vec::new()),
        },
        acks: false,
        first_call_at: Cell::new(None),
        calls: RefCell::new(Vec::new()),
    };
    let report = owed_events::direct_with(&port, events, Some(900_000)).await;
    let batches = port.sql.batches.borrow();
    assert_eq!(batches.len(), 1, "{batches:?}");
    assert!(
        batches[0]
            .1
            .iter()
            .all(|s| !s.contains("MAX(d.generation)")),
        "no bound read in Phases A and B"
    );
    assert_eq!((report.calls, report.acked), (2, 0));
    assert_eq!(db.int("SELECT COUNT(*) FROM owed_dirty"), marks_before);
    // the bound statements are key searches (the marks' window, then `owed_dirty`'s unique (identity, gameId, ..))
    use rusqlite::types::Value as V;
    for (sql, binds) in [
        (
            owed_events::GAME_BOUNDS_OF_PAIR_SQL,
            vec![
                V::Text("hop".into()),
                V::Text("k".into()),
                V::Integer(256),
                V::Integer(0),
            ],
        ),
        (
            owed_events::GAME_BOUNDS_OF_IDENTITY_SQL,
            vec![V::Text(identity(1)), V::Integer(256), V::Integer(0)],
        ),
    ] {
        let plan = db.plan(sql, &binds);
        assert!(
            plan.iter()
                .all(|l| !l.starts_with("SCAN d") && !l.contains("SCAN owed_dirty")),
            "{plan:?}"
        );
    }
}
