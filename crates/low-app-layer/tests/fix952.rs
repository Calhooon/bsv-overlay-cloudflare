//! FIX-952 (bsv-low #952 under #912, lane FIX-952-PHASE-C): the write path's visibility under load, for every writer
//! and for Phase C. The L53 loop (records 1544 to 1548) measured the `owed-events` consumer delivering a batch of 16
//! messages ONE identity at a time (61 to 70 s a batch against the 60 s wall, about 15 events a minute of 243): in
//! Phase B each identity's delivery is the actor's apply then the D1 judge's commit, and the pass ran them in a row.
//! The pins here run the shipped consumer core (`owed_events::batch_core`) over real SQLite with the overlay's
//! migrations (`support/owed_db.rs`) and a timed judge port on tokio's paused clock:
//!
//! - `fix952_c`: a Phase B batch of 16 distinct identities applies within about ONE identity's wall (the actor's
//!   apply plus the judge's commit), the actor still before the acknowledgement within each identity. RED on
//!   `149869bf`: 16 x (actor + judge), `4800 ms` against `300`.
//! - `fix952_d`: the Phase C consumer (`OWED_D1_JUDGE=false`) makes no D1 judge call and writes no `owed_rows`,
//!   `owed_row_words` or `owed_state` (the actor is the one writer), acknowledges the marks it applied, and its D1
//!   statements an event are counted (the Phase C concurrency's cost); the Phase C render pair
//!   (`CONSUMER_CONCURRENCY_PHASE_C`) equals the two `PHASE C:` lines of each render. The constants and the lines do
//!   not exist on `149869bf`; the no-judge behaviour was already Phase C's there.
//! - `fix952_e`: the overlay's hand-off (`change_flush::ship_owed_direct_with`, the function its `APP_LAYER` binding
//!   wrapper calls) reaches the identities' actors through a fake binding: the body the app layer's route parses
//!   (`parse_direct_body`) and its direct pass (`direct_with`) over a fake `DirectPort`: each actor call carries the
//!   event's `seq`, `atMs` and the class `direct`; a POST that walls or is refused is one fault and its events are
//!   the queue's; the chunking at `OWED_DIRECT_BODY_MAX`. New API on `149869bf` (the hand-off was `env`-bound).
//! - `fix952_b`: a dupe apply (the backstop's re-apply of an event the direct call already applied, `applied: false`)
//!   samples nothing, not even `apply`; an applied redelivery samples `apply` only; a first apply samples `visible`;
//!   through the shipped `note_answer_into` and the consumer's own Phase B delivery (`actor_then_d1`). D2-ACTOR-D's
//!   rule, re-stated for the lane: green on `149869bf`.
//! - `fix952_a` is the workerd cell `tools/lane-912/owed_direct_flood.mjs` (`make ci-d1-budget`): 32 frames for 32
//!   identities each visible under 1 s by the direct call with the queue behind, now in Phase B and in Phase C
//!   (where the direct call also acknowledges the marks).
#[allow(dead_code)]
#[path = "support/owed_db.rs"]
mod owed_db;

use low_app_layer::owed_actor;
use low_app_layer::owed_events::{self, DirectPort, Fate};
use low_app_layer::owed_judge::JudgeDb;
use low_app_layer::pot_hook::{Bind, SqlPort, Stmt, StmtOut};
use overlay_discovery::owed_event::{self, OwedEvent};
use owed_db::{seed_stranded_hop, Db, NOW};
use serde_json::{json, Value};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

/// The k-th identity: a 66-hex compressed key.
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

fn hop_key(hop: &str) -> String {
    owed_event::outpoint_key(hop, 0)
}

fn rows_of(db: &Db, identity: &str) -> i64 {
    db.int(&format!(
        "SELECT COUNT(*) FROM owed_rows WHERE identity='{identity}'"
    ))
}

fn marks_of(db: &Db, identity: &str) -> i64 {
    db.int(&format!(
        "SELECT COUNT(*) FROM owed_dirty WHERE identity='{identity}'"
    ))
}

/// A judge port over the real SQLite [`Db`] whose actor call takes `actor_ms` and whose judge COMMIT (the batch that
/// writes `owed_rows`) takes `judge_ms` on tokio's paused clock, under the phase its flags say; every actor call and
/// every judge commit recorded with the virtual time it answered.
struct Timed<'a> {
    db: &'a Db,
    actor_ms: u64,
    judge_ms: u64,
    d1_judges: bool,
    actor_reads: bool,
    /// The actor answers a covered DUPE (`applied: false`).
    dupe: bool,
    t0: tokio::time::Instant,
    /// `("actor" | "write", identity, virtual ms)`.
    log: &'a RefCell<Vec<(String, String, u64)>>,
}
impl Timed<'_> {
    fn now_virtual(&self) -> u64 {
        self.t0.elapsed().as_millis() as u64
    }
}
impl SqlPort for Timed<'_> {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        if stmts
            .iter()
            .any(|s| s.sql.contains("INSERT INTO owed_rows"))
        {
            if self.judge_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(self.judge_ms)).await;
            }
            let who = stmts
                .iter()
                .flat_map(|s| s.binds.iter())
                .find_map(|b| match b {
                    Bind::Text(t) if t.len() == 66 && t.starts_with("02") => Some(t.clone()),
                    _ => None,
                })
                .unwrap_or_default();
            self.log
                .borrow_mut()
                .push(("write".into(), who, self.now_virtual()));
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
impl JudgeDb for Timed<'_> {
    fn tip(&self) -> Option<u64> {
        self.db.tip()
    }
    async fn word_views(
        &self,
        rows: &BTreeMap<String, low_app_layer::tracker_host::stored::Row>,
    ) -> Result<BTreeMap<String, Value>, String> {
        self.db.word_views(rows).await
    }
    async fn canonical_root(&self, height: u64) -> Option<String> {
        self.db.canonical_root(height).await
    }
    async fn push(&self, identity: &str, body: Value) {
        self.db.push(identity, body).await;
    }
    fn d1_judges(&self) -> bool {
        self.d1_judges
    }
    fn actor_reads(&self) -> bool {
        self.actor_reads
    }
    /// Never the compare's one-in-eight sample (seq 1): the pins time the delivery, not the compare.
    fn event_seq(&self) -> Option<i64> {
        Some(1)
    }
    fn event_at_ms(&self) -> Option<u64> {
        Some(NOW)
    }
    async fn actor_deliver_bounded(
        &self,
        identity: &str,
        games: &[String],
        bound: Option<i64>,
    ) -> Result<Option<Value>, String> {
        if self.actor_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(self.actor_ms)).await;
        }
        self.log
            .borrow_mut()
            .push(("actor".into(), identity.to_string(), self.now_virtual()));
        let judged = NOW + 40;
        Ok(Some(if self.dupe {
            json!({"applied": false, "firstApply": false, "appliedGames": games, "rows": [], "atMs": NOW, "judgedAtMs": judged, "applyMs": 3, "bound": bound})
        } else {
            json!({"applied": true, "firstApply": true, "appliedGames": games, "rows": [], "atMs": NOW, "judgedAtMs": judged, "applyMs": 7, "bound": bound})
        }))
    }
}

/// 16 seats, one stranded hop each (its verified marker's trigger writes the `hop` mark), and the 16 messages.
fn sixteen_seats(db: &Db) -> Vec<OwedEvent> {
    (0..16)
        .map(|k| {
            let hop = owed_db::hop(9_000 + k);
            seed_stranded_hop(db, &identity(k), "g1", &hop);
            event("hop", &hop_key(&hop), 100 + k as i64)
        })
        .collect()
}

/// (c) PHASE B: A BATCH OF 16 DISTINCT IDENTITIES APPLIES WITHIN ABOUT ONE IDENTITY'S WALL. The shipped
/// `batch_core` over the 16 messages' merged plan, each identity's delivery the actor's apply (100 ms) then the D1
/// judge's commit (200 ms): the pass ends at about 300 ms of virtual time, never 16 x 300, every row judged and every
/// mark acknowledged; within each identity the actor answered BEFORE its `owed_rows` write (HIGH-3's order, kept per
/// identity). RED on `149869bf`: the identities were delivered in a row, `4800 ms`.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix952_c_phase_b_a_batch_of_16_identities_applies_within_about_one_judge_wall() {
    let db = Db::new();
    let evs = sixteen_seats(&db);
    let mut plans = Vec::new();
    for ev in &evs {
        plans.push(owed_events::plan(&db, ev).await.unwrap());
    }
    let refs: Vec<&OwedEvent> = evs.iter().collect();
    let (actor_ms, judge_ms) = (100u64, 200u64);
    let log = RefCell::new(Vec::new());
    let t0 = tokio::time::Instant::now();
    let make = |_, _| async {
        Ok::<Timed, String>(Timed {
            db: &db,
            actor_ms,
            judge_ms,
            d1_judges: true,
            actor_reads: true,
            dupe: false,
            t0,
            log: &log,
        })
    };
    let out = owed_events::batch_core(&db, &refs, &plans, make, std::future::pending::<()>())
        .await
        .unwrap();
    let elapsed = t0.elapsed().as_millis() as u64;
    assert_eq!(out.coalesced, 16);
    assert_eq!(
        (out.routed.subjects, out.routed.marks, out.routed.faults),
        (16, 16, 0)
    );
    assert!(
        out.fates.iter().all(|f| *f
            == Fate::Ack {
                resolve: true,
                continues: false
            }),
        "{:?}",
        out.fates
    );
    for k in 0..16 {
        assert_eq!(rows_of(&db, &identity(k)), 1, "identity {k} judged");
        assert_eq!(marks_of(&db, &identity(k)), 0, "identity {k} acknowledged");
    }
    let one = actor_ms + judge_ms;
    let serial = 16 * one;
    println!(
        "fix952_c: a Phase B batch of 16 identities took {elapsed} ms of virtual time; one identity's delivery is {one} ms (the actor {actor_ms}, the judge's commit {judge_ms}); the serial pass of 149869bf is {serial} ms"
    );
    assert!(
        elapsed <= 2 * one,
        "the batch's wall is about one identity's delivery, not sixteen in a row: {elapsed} ms against {one} (serial {serial})"
    );
    // HIGH-3's order per identity: the actor answered before the identity's rows were written
    let log = log.borrow();
    for k in 0..16 {
        let id = identity(k);
        let actor_at = log
            .iter()
            .find(|(w, i, _)| w == "actor" && *i == id)
            .map(|(_, _, t)| *t)
            .expect("the actor was called");
        let write_at = log
            .iter()
            .find(|(w, i, _)| w == "write" && *i == id)
            .map(|(_, _, t)| *t)
            .expect("the rows were written");
        assert!(
            actor_at <= write_at,
            "identity {k}: actor at {actor_at}, write at {write_at}"
        );
    }
    assert_eq!(
        log.iter().filter(|(w, _, _)| w == "actor").count(),
        16,
        "one actor call an identity"
    );
    // the bound the pass delivers at once is the batch's own size
    assert_eq!(
        owed_events::JUDGE_IDENTITIES_AT_ONCE,
        owed_events::CONSUMER_BATCH_MAX
    );
}

/// (d) PHASE C: THE CONSUMER MAKES NO D1 JUDGE CALL AND WRITES NO `owed_rows`. The shipped `batch_core` under
/// `OWED_D1_JUDGE=false` over 16 messages for 16 identities: every statement the pass ran names none of the judge's
/// tables, the 16 actors were called once each, the marks are acknowledged (the actor is the one writer), and the
/// D1 statements an event are counted and printed (the Phase C concurrency's cost). Then the Phase C render pair: the
/// constant, its rule by phase, and the two `PHASE C:` lines in each render of `wrangler.toml` (the lever's exact
/// edit) equal to it.
#[tokio::test(flavor = "current_thread")]
async fn fix952_d_phase_c_the_consumer_makes_no_judge_call_and_writes_no_owed_rows() {
    let db = Db::new();
    let evs = sixteen_seats(&db);
    db.batches.borrow_mut().clear();
    let mut plans = Vec::new();
    for ev in &evs {
        plans.push(owed_events::plan(&db, ev).await.unwrap());
    }
    let refs: Vec<&OwedEvent> = evs.iter().collect();
    let log = RefCell::new(Vec::new());
    let t0 = tokio::time::Instant::now();
    let make = |_, _| async {
        Ok::<Timed, String>(Timed {
            db: &db,
            actor_ms: 0,
            judge_ms: 0,
            d1_judges: false,
            actor_reads: true,
            dupe: false,
            t0,
            log: &log,
        })
    };
    let out = owed_events::batch_core(&db, &refs, &plans, make, std::future::pending::<()>())
        .await
        .unwrap();
    assert_eq!(
        (out.coalesced, out.routed.subjects, out.routed.faults),
        (16, 16, 0)
    );
    assert!(
        out.fates.iter().all(|f| *f
            == Fate::Ack {
                resolve: true,
                continues: false
            }),
        "{:?}",
        out.fates
    );
    let batches = db.batches.borrow().clone();
    for b in &batches {
        for s in b {
            let sql = s.sql.to_ascii_lowercase();
            assert!(
                !sql.contains("owed_rows")
                    && !sql.contains("owed_row_words")
                    && !sql.contains("owed_state"),
                "Phase C's consumer touched a judge table: {}",
                s.sql
            );
        }
    }
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_rows"),
        0,
        "no owed_rows written"
    );
    let log = log.borrow();
    assert_eq!(log.iter().filter(|(w, _, _)| w == "actor").count(), 16);
    assert!(log.iter().all(|(w, _, _)| w != "write"), "no judge commit");
    for k in 0..16 {
        assert_eq!(
            marks_of(&db, &identity(k)),
            0,
            "identity {k}'s marks acknowledged by the actor's apply"
        );
    }
    let statements: usize = batches.iter().map(Vec::len).sum();
    let round_trips = batches.len();
    // the batch's end (the queue handler's `finish_batch`): the counters, the consumer row and the health snapshot
    // in one batch, and each acknowledged message's resolution
    let resolved = owed_events::resolved_statements(&evs[0].source, &evs[0].source_key).len();
    println!(
        "fix952_d: the Phase C consumer over 16 events ran {statements} D1 statements in {round_trips} round trips inside the pass ({:.1} statements and {:.1} round trips an event), then {resolved} resolution statements a message and three batch-level statements once; no judge call, no owed_rows write",
        statements as f64 / 16.0,
        round_trips as f64 / 16.0
    );
    assert!(
        statements <= 8 * 16,
        "{statements} statements for 16 events"
    );
    assert_eq!(resolved, 4);

    // THE PHASE C RENDER PAIR (item 3): the constant, its rule, the lever's two lines in each render.
    use owed_events::{
        consumer_concurrency_for, consumer_concurrency_of, consumer_concurrency_refused,
        CONSUMER_CONCURRENCY_PHASE_C, CONSUMER_MAX_CONCURRENCY,
    };
    assert_eq!(consumer_concurrency_for(true), CONSUMER_MAX_CONCURRENCY);
    assert_eq!(consumer_concurrency_for(true), 1);
    assert_eq!(
        consumer_concurrency_for(false),
        CONSUMER_CONCURRENCY_PHASE_C
    );
    assert_eq!(CONSUMER_CONCURRENCY_PHASE_C, 4);
    assert!(
        !consumer_concurrency_refused(CONSUMER_CONCURRENCY_PHASE_C, false),
        "Phase C honours it"
    );
    assert!(
        consumer_concurrency_refused(CONSUMER_CONCURRENCY_PHASE_C, true),
        "Phases A and B refuse it"
    );
    let render = include_str!("../wrangler.toml");
    let var_lines: Vec<u32> = render
        .lines()
        .filter(|l| l.starts_with("# PHASE C: OWED_CONSUMER_CONCURRENCY = \""))
        .map(|l| consumer_concurrency_of(l.split('"').nth(1)))
        .collect();
    assert_eq!(
        var_lines,
        vec![CONSUMER_CONCURRENCY_PHASE_C; 2],
        "prod and beta: {var_lines:?}"
    );
    let live_lines = render
        .lines()
        .filter(|l| l.starts_with("OWED_CONSUMER_CONCURRENCY = \""))
        .count();
    assert_eq!(
        live_lines, 2,
        "the live knob stays 1 in both renders until the Phase C deploy"
    );
    let mut consumer_blocks = 0;
    for block in render.split("[[") {
        let is_consumer = block.starts_with("queues.consumers]]")
            || block.starts_with("env.beta.queues.consumers]]");
        let live_queue = block
            .lines()
            .nth(1)
            .is_some_and(|l| l.starts_with("queue = \"low-owed-events") && !l.contains("-dlq"));
        if !(is_consumer && live_queue) {
            continue;
        }
        consumer_blocks += 1;
        assert!(
            block.contains(&format!(
                "\n# PHASE C: max_concurrency = {CONSUMER_CONCURRENCY_PHASE_C}\n"
            )),
            "the Phase C line in the live consumer block: {block}"
        );
        assert!(
            block.contains(&format!("\nmax_concurrency = {CONSUMER_MAX_CONCURRENCY}\n")),
            "the live block stays at 1 until the Phase C deploy: {block}"
        );
    }
    assert_eq!(consumer_blocks, 2, "prod and beta");
    assert_eq!(
        render.matches("\n# PHASE C: max_concurrency = ").count(),
        2,
        "the Phase C consumer line in no other block"
    );
}

/// (b) A DUPE APPLY SAMPLES NOTHING. The backstop's re-apply of an event the direct call already applied answers
/// `applied: false` (the actor's seq check): through the shipped `note_answer_into` it samples neither `visible` nor
/// `apply`; an applied redelivery (`firstApply: false`) samples `apply` only; a first apply samples `visible`. And
/// through the consumer's own Phase B delivery (`actor_then_d1`, the shipped `note_answer`): the isolate's bars hold
/// no sample after a dupe and one after a first apply. D2-ACTOR-D's rule, re-stated for the lane (green on
/// `149869bf`).
#[tokio::test(flavor = "current_thread")]
async fn fix952_b_a_dupe_apply_samples_nothing() {
    use owed_actor::{note_answer_into, samples_visible, Bars};
    let mut bars = Bars::default();
    let dupe = json!({"applied": false, "firstApply": false, "appliedGames": ["g"], "atMs": 1_000, "judgedAtMs": 1_900, "applyMs": 3});
    assert!(!samples_visible(&dupe));
    note_answer_into(&mut bars, &dupe);
    assert_eq!(
        (bars.apply.max(), bars.visible.max()),
        (None, None),
        "a dupe samples nothing, not even apply"
    );
    let redelivery = json!({"applied": true, "firstApply": false, "atMs": 1_000, "judgedAtMs": 1_900, "applyMs": 3});
    assert!(!samples_visible(&redelivery));
    note_answer_into(&mut bars, &redelivery);
    assert_eq!(
        (bars.apply.max(), bars.visible.max()),
        (Some(3), None),
        "an applied redelivery samples apply only"
    );
    let first = json!({"applied": true, "firstApply": true, "atMs": 1_000, "judgedAtMs": 1_900, "applyMs": 5});
    assert!(samples_visible(&first));
    note_answer_into(&mut bars, &first);
    assert_eq!((bars.apply.max(), bars.visible.max()), (Some(5), Some(900)));

    // the consumer's Phase B delivery: the actor's covered dupe, then a first apply
    let db = Db::new();
    let hop = owed_db::hop(9_100);
    let id = identity(1);
    seed_stranded_hop(&db, &id, "g1", &hop);
    let games = vec!["g1".to_string()];
    let log = RefCell::new(Vec::new());
    let before = owed_actor::BARS.with(|b| (b.borrow().apply.max(), b.borrow().visible.max()));
    let fake = Timed {
        db: &db,
        actor_ms: 0,
        judge_ms: 0,
        d1_judges: true,
        actor_reads: true,
        dupe: true,
        t0: tokio::time::Instant::now(),
        log: &log,
    };
    owed_actor::actor_then_d1(&fake, &id, &games).await.unwrap();
    assert_eq!(
        owed_actor::BARS.with(|b| (b.borrow().apply.max(), b.borrow().visible.max())),
        before,
        "the consumer's dupe delivery sampled nothing"
    );
    assert_eq!(
        rows_of(&db, &id),
        1,
        "the D1 judge still judged (Phase B's mirror)"
    );
    let fake = Timed {
        dupe: false,
        ..fake
    };
    owed_actor::actor_then_d1(&fake, &id, &games).await.unwrap();
    let after = owed_actor::BARS.with(|b| (b.borrow().apply.max(), b.borrow().visible.max()));
    assert_eq!(
        after,
        (Some(7), Some(40)),
        "a first apply samples once: {after:?}"
    );
}

/// The fake `APP_LAYER` binding's far side: the app layer's direct pass over a fake `DirectPort` (the marks' read by a
/// fixed map, every actor call recorded and answered applied).
struct FakeActors {
    /// `sourceKey` to the identity and games its mark names.
    marks: BTreeMap<String, (String, Vec<String>)>,
    calls: RefCell<Vec<Value>>,
}
impl DirectPort for FakeActors {
    async fn subjects(&self, ev: &OwedEvent) -> Result<Vec<(String, Vec<String>)>, String> {
        Ok(self
            .marks
            .get(&ev.source_key)
            .cloned()
            .into_iter()
            .collect())
    }
    async fn call(&self, body: Value) -> Result<Value, String> {
        self.calls.borrow_mut().push(body.clone());
        let at = body["atMs"].as_i64().unwrap_or(0);
        Ok(
            json!({"applied": true, "firstApply": true, "appliedGames": body["games"], "rows": [], "atMs": at, "judgedAtMs": at + 40, "applyMs": 11}),
        )
    }
}

/// (e) THE OVERLAY WRITER'S DIRECT CALL REACHES THE ACTOR THROUGH THE BINDING. The overlay's shipped hand-off
/// (`change_flush::ship_owed_direct_with`, what `ship_owed_direct` runs over the `APP_LAYER` binding) over the
/// events a publish held, its POST answered by the app layer's own route body parser (`parse_direct_body`) and direct
/// pass (`direct_with`) over fake actors: ONE body for the four held events, each as it rides the queue (the same
/// `seq`, `atMs`, source and key read back), one actor call an identity carrying the FIRST event's `seq` and `atMs`,
/// the others in `also`, `class: "direct"`, no `bound` (a direct call always applies), the frame-carrying `word`
/// event left to the queue; a POST that never answers is ONE fault at the hand-off's wall with its events the
/// queue's; 65 events are two POSTs, and a wall that fell before the second leaves its event to the queue, counted.
/// The route keeps the internal bearer (`internal_bearer_ok`): on the public origin it answers 401, so the binding's
/// caller (the overlay, holding `INTERNAL_TOKEN`) is its only reach; it sets no CORS header and answers 202 before any
/// actor is called. New API on `149869bf` (the hand-off was `env`-bound: no native pin could run it).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix952_e_the_overlay_writers_direct_call_reaches_the_actor_through_the_binding() {
    use bsv_overlay_cloudflare::change_flush::{
        ship_owed_direct_with, OWED_DIRECT_BODY_MAX, OWED_DIRECT_POST_WALL_MS,
    };
    let (id1, id2) = (identity(1), identity(2));
    let (ka, kb, kc) = (
        owed_event::outpoint_key(&owed_db::hop(1), 0),
        owed_event::outpoint_key(&owed_db::hop(2), 1),
        owed_event::outpoint_key(&owed_db::hop(3), 0),
    );
    let mut held = vec![
        event("pot", &ka, 41),
        event("hop", &kb, 42),
        event("word", "ab", 43),
        event("pot", &kc, 44),
    ];
    held[0].at_ms = 5_000;
    held[1].at_ms = 5_010;
    held[2].at_ms = 5_020;
    held[2].facts = json!({"frame": {"txid": "ab", "kind": "seen"}});
    held[3].at_ms = 5_030;
    let actors = FakeActors {
        marks: BTreeMap::from([
            (ka.clone(), (id1.clone(), vec!["g1".to_string()])),
            (kb.clone(), (id2.clone(), vec!["g2".to_string()])),
            (kc.clone(), (id1.clone(), vec!["g1b".to_string()])),
        ]),
        calls: RefCell::new(Vec::new()),
    };
    let posted: RefCell<Vec<String>> = RefCell::new(Vec::new());
    let answers: RefCell<Vec<Value>> = RefCell::new(Vec::new());
    let now = Cell::new(100_000u64);
    let shipped = ship_owed_direct_with(
        held.clone(),
        |body| {
            posted.borrow_mut().push(body.clone());
            let (actors, answers) = (&actors, &answers);
            async move {
                // the app layer's route: the body parsed as the queue's own messages, the direct pass after the 202
                let events = owed_events::parse_direct_body(body.as_bytes()).map_err(|_| ())?;
                let report = owed_events::direct_with(actors, events, Some(900_000)).await;
                let answer = json!({"accepted": report.events, "leftToQueue": report.left_to_queue, "calls": report.calls, "walls": report.walls, "faults": report.faults});
                answers.borrow_mut().push(answer.clone());
                Ok(answer.to_string())
            }
        },
        || now.get(),
        now.get() + 20_000,
        |ms| tokio::time::sleep(std::time::Duration::from_millis(ms)),
    )
    .await;
    assert_eq!(
        (shipped.posts, shipped.faults, shipped.left_to_queue),
        (1, 0, 0),
        "{shipped:?}"
    );
    assert!(shipped.lines.is_empty(), "{:?}", shipped.lines);
    {
        // the body carries every held event as it rides the queue
        let posted = posted.borrow();
        let body: Value = serde_json::from_str(&posted[0]).unwrap();
        let carried: Vec<OwedEvent> = body["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| OwedEvent::decode(e.as_str().unwrap()).unwrap())
            .collect();
        assert_eq!(carried, held, "seq, atMs, source, key and facts read back");
        let answer = answers.borrow()[0].clone();
        assert_eq!(
            answer,
            json!({"accepted": 3, "leftToQueue": 1, "calls": 2, "walls": 0, "faults": 0}),
            "three events to two actors; the frame-carrying word is the queue's"
        );
        // one actor call an identity, the queue delivery's key on each
        let calls = actors.calls.borrow();
        assert_eq!(calls.len(), 2, "{calls:?}");
        let of = |id: &str| {
            calls
                .iter()
                .find(|c| c["identity"] == id)
                .cloned()
                .expect("called")
        };
        let c1 = of(&id1);
        assert_eq!(
            c1["games"],
            json!(["g1", "g1b"]),
            "the identity's two events coalesced into one apply"
        );
        assert_eq!(
            (c1["kind"].as_str(), c1["seq"].as_i64(), c1["atMs"].as_i64()),
            (Some("pot"), Some(41), Some(5_000))
        );
        assert_eq!(
            c1["also"],
            json!([["pot", 44]]),
            "the second event's key rides along"
        );
        assert_eq!(c1["class"], "direct");
        assert!(c1["bound"].is_null(), "a direct call always applies");
        let c2 = of(&id2);
        assert_eq!(
            (c2["kind"].as_str(), c2["seq"].as_i64(), c2["atMs"].as_i64()),
            (Some("hop"), Some(42), Some(5_010))
        );
        assert_eq!(c2["tip"], 900_000);
    }

    // a POST that never answers: one fault at the hand-off's wall, its events the queue's
    let shipped = ship_owed_direct_with(
        held.clone(),
        |_| std::future::pending::<Result<String, ()>>(),
        || now.get(),
        now.get() + 20_000,
        |ms| tokio::time::sleep(std::time::Duration::from_millis(ms)),
    )
    .await;
    assert_eq!(
        (shipped.posts, shipped.faults, shipped.left_to_queue),
        (1, 1, 4)
    );
    assert!(
        shipped.lines[0].contains(&format!("passed its {OWED_DIRECT_POST_WALL_MS} ms wall")),
        "{:?}",
        shipped.lines
    );
    // a refused POST: one fault, no line of its own (the transport logged it)
    let shipped = ship_owed_direct_with(
        held.clone(),
        |_| async { Err(()) },
        || now.get(),
        now.get() + 20_000,
        |ms| tokio::time::sleep(std::time::Duration::from_millis(ms)),
    )
    .await;
    assert_eq!(
        (shipped.posts, shipped.faults, shipped.left_to_queue),
        (1, 1, 4)
    );
    // 65 events: two POSTs; a clock past the wall before the second leaves its event to the queue, counted
    let many: Vec<OwedEvent> = (0..65)
        .map(|k| event("pot", &format!("{k:064x}:0"), 500 + k))
        .collect();
    let clock = Cell::new(200_000u64);
    let shipped = ship_owed_direct_with(
        many.clone(),
        |body| {
            let n = serde_json::from_str::<Value>(&body).unwrap()["events"]
                .as_array()
                .unwrap()
                .len();
            async move { Ok(n.to_string()) }
        },
        || clock.get(),
        clock.get() + 20_000,
        |ms| tokio::time::sleep(std::time::Duration::from_millis(ms)),
    )
    .await;
    assert_eq!(
        (shipped.posts, shipped.faults, shipped.left_to_queue),
        (2, 0, 0)
    );
    let shipped = ship_owed_direct_with(
        many,
        |_| {
            clock.set(clock.get() + OWED_DIRECT_POST_WALL_MS);
            async { Ok(String::new()) }
        },
        || clock.get(),
        clock.get() + 20_000,
        |ms| tokio::time::sleep(std::time::Duration::from_millis(ms)),
    )
    .await;
    assert_eq!(
        (shipped.posts, shipped.faults, shipped.left_to_queue),
        (1, 1, 65 - OWED_DIRECT_BODY_MAX)
    );
    assert!(
        shipped.lines[0].contains("the hand-off's wall fell: 1 event(s) left to the queue"),
        "{:?}",
        shipped.lines
    );

    // the route's reach: the binding's caller alone (the internal bearer), 202 before any actor, no CORS header
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let events_src = squash(include_str!("../src/owed_events.rs"));
    let route = &events_src[events_src.find("pubasyncfndirect_route(").unwrap()..];
    let end = route[10..]
        .find("pubasyncfn")
        .or_else(|| route[10..].find("pubfn"))
        .map_or(route.len(), |i| i + 10);
    let route = &route[..end];
    assert!(route.contains("if!crate::internal_events::internal_bearer_ok(&req,env){returnworker::Response::error(\"unauthorized\",401);}"), "the bearer the binding's caller holds");
    assert!(route.contains("ctx.wait_until(asyncmove{direct_events(&env,events).await;});"));
    assert!(route.contains("with_status(202)"));
    assert!(
        !route.contains("Access-Control"),
        "no CORS: never a browser's route"
    );
    let flush = squash(include_str!("../../overlay-cloudflare/src/change_flush.rs"));
    let wrapper = &flush[flush
        .find("pubasyncfnship_owed_direct(env:&Env,deadline_ms:u64){")
        .unwrap()..];
    assert!(
        wrapper.contains("ship_owed_direct_with(events,|body|{post_body_answer(env,&url,&token,\"/internal/owed-direct\",\"owed-direct\",body,)},"),
        "the wrapper runs the shipped function over the binding's POST"
    );
    let transport = &flush[flush.find("pubasyncfnpost_body_answer(").unwrap()..];
    assert!(
        transport.contains(
            "letsent=matchenv.service(\"APP_LAYER\"){Ok(svc)=>svc.fetch_request(req).await,"
        ),
        "the POST rides the APP_LAYER service binding"
    );
    assert!(
        transport.contains("headers.set(\"Authorization\",&format!(\"Bearer{}\",token.trim()))")
    );
}
