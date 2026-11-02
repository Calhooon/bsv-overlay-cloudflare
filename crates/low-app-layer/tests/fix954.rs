//! FIX-954 (bsv-low #954, D2-ACTOR-E; lane FIX-954): the FLOOD class's visibility. Beta's bars at baacff3b (read
//! 2026-11-02 13:40Z from `/health.owedActor` and the raw rings `owed_belt_state` `owed-actor:bars:flood`): 65 visible
//! samples, p50 7,799 ms, p95 14,616 ms, max 14,682; 105 applies, p50 630, p95 2,933; the turn's `wait` p95 0 (max
//! 2,282). The wait is the actor's queue for its turn: at 0 the time is not one identity's burst serialized in its
//! actor. The visibility is spent BEFORE the actor is called: the flood class is a pass that fanned out to
//! `FLOOD_CONCURRENCY` actors or more, and its first visibility past the apply is the CONSUMER's (the queue class
//! beside it reads visible p50 6,787, p95 11,840 over 256). The consumer's coalesced batch ran, before its first
//! judge, every message's frame ingest (the tracker's write, about three D1 round trips) and every message's plan read
//! (one round trip), one after another: a batch of sixteen word frames is about sixty-four round trips before any actor
//! is called, and the batches behind it wait for it.
//!
//! The pins run the shipped `consume_batch_with` (the frames, the plans, `batch_core`) on tokio's paused clock over real
//! SQLite with the overlay's migrations, every D1 batch costing `D1_MS` and every frame ingest `INGEST_MS`:
//!
//! - `fix954_a`: a batch of 16 word frames for 16 identities (Phase C): every actor answered within two ingest waves,
//!   one plan batch, the memo read and one identity's delivery, not sixteen ingests and sixteen plan reads in a row.
//!   RED against main's rule (the ingests and the plans one message at a time).
//! - `fix954_b`: the differential: the batch through `consume_batch_with` and the same messages planned one at a time
//!   (main's order) store the same `owed_rows`, every column, and acknowledge the same marks.
//! - `fix954_c`: a frame ingest that faults in the middle of the batch: the batch answers its fault, nothing is judged
//!   and no mark is acknowledged; every other frame was ingested once; the batch again (the platform's retry) judges
//!   every identity once (one row each, never two) and acknowledges every mark.
//! - `fix954_d`: the bound: at most `INGEST_FRAMES_AT_ONCE` ingests in flight; one txid's frames in message order, never
//!   two at once. RED with the bound removed (sixteen in flight).
//!
//! FIX-954-FOLD-1 (the ensemble A, B and C of 2026-11-02; the agreed gap and C's MEDIUM-1):
//!
//! - `fix954f_e`: a MIXED batch (two wanted envelopes, the wanted pair twice, an identity event, a word pair twice, a
//!   txfacts event) planned by `plans_many` answers main's plan for every message, in order, and through
//!   `consume_batch_with` stores main's rows, every column, with no envelope left. RED with the batch's envelope
//!   expansion switched off (A's mutant G, B's M1, C's M2) and under a plan-answer swap.
//! - `fix954f_f`: a frame whose ingest faults ENDS its txid's group: the txid's later frames are not ingested until the
//!   retry ingests the group again from its first frame (B's M2: RED with the group not ended).
#[allow(dead_code)]
#[path = "support/owed_db.rs"]
mod owed_db;

use low_app_layer::owed_events::{self, FrameIngest};
use low_app_layer::owed_judge::JudgeDb;
use low_app_layer::pot_hook::{SqlPort, Stmt, StmtOut};
use overlay_discovery::owed_event::{self, OwedEvent};
use owed_db::{seed_stranded_hop, Db, NOW};
use serde_json::{json, Value};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

/// Beta's own medians (the bars' raw rings at baacff3b): one D1 round trip (the queue class's `read` p50 147 ms), a
/// frame's ingest (three round trips: the live hold, the stored row, the write), one actor apply under the flood (the
/// flood class's apply p50 630 ms).
const D1_MS: u64 = 150;
const INGEST_MS: u64 = 3 * D1_MS;
const ACTOR_MS: u64 = 630;

fn identity(k: usize) -> String {
    format!("02{k:064x}")
}

fn word_txid(k: usize) -> String {
    format!("{:064x}", 0xabc000 + k)
}

fn word_event(txid: &str, seq: i64) -> OwedEvent {
    OwedEvent {
        v: owed_event::OWED_EVENT_V,
        seq,
        source: "word".into(),
        source_key: txid.into(),
        facts: json!({"frame": {"txid": txid, "kind": "callback", "body": {"txStatus": "SEEN_ON_NETWORK"}}}),
        at_ms: NOW as i64,
    }
}

/// 16 seats, each a stranded hop in game `g1` (its marker's trigger marks it), and a word frame per seat whose ingest
/// marks the seat's game under the word's pair (what the tracker's write and its trigger do).
fn sixteen(db: &Db) -> (Vec<OwedEvent>, BTreeMap<String, String>) {
    let mut owner = BTreeMap::new();
    let evs = (0..16)
        .map(|k| {
            seed_stranded_hop(db, &identity(k), "g1", &owed_db::hop(7_000 + k));
            owner.insert(word_txid(k), identity(k));
            word_event(&word_txid(k), 200 + k as i64)
        })
        .collect();
    (evs, owner)
}

/// The tracker's write, modelled: `INGEST_MS` of virtual time, then the word's mark for the frame's owner (idempotent:
/// the mark's UNIQUE key). Records each ingest, the peak in flight and each txid's in-flight count.
struct Ingest<'a> {
    db: &'a Db,
    owner: BTreeMap<String, String>,
    ms: u64,
    fail: RefCell<Option<String>>,
    /// one frame alone faults: (txid, the frame's `n`)
    fail_frame: RefCell<Option<(String, i64)>>,
    in_flight: Cell<usize>,
    peak: Cell<usize>,
    per_txid: RefCell<BTreeMap<String, usize>>,
    txid_overlap: Cell<bool>,
    order: RefCell<Vec<(String, i64)>>,
    count: RefCell<BTreeMap<String, usize>>,
}
impl<'a> Ingest<'a> {
    fn new(db: &'a Db, owner: BTreeMap<String, String>, ms: u64) -> Self {
        Self {
            db,
            owner,
            ms,
            fail: RefCell::new(None),
            fail_frame: RefCell::new(None),
            in_flight: Cell::new(0),
            peak: Cell::new(0),
            per_txid: RefCell::new(BTreeMap::new()),
            txid_overlap: Cell::new(false),
            order: RefCell::new(Vec::new()),
            count: RefCell::new(BTreeMap::new()),
        }
    }
}
impl FrameIngest for Ingest<'_> {
    async fn ingest_frame(&self, frame: &Value) -> Result<(), String> {
        let txid = frame["txid"].as_str().unwrap_or_default().to_owned();
        self.in_flight.set(self.in_flight.get() + 1);
        self.peak.set(self.peak.get().max(self.in_flight.get()));
        {
            let mut per = self.per_txid.borrow_mut();
            let n = per.entry(txid.clone()).or_default();
            *n += 1;
            if *n > 1 {
                self.txid_overlap.set(true);
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(self.ms)).await;
        *self.per_txid.borrow_mut().get_mut(&txid).unwrap() -= 1;
        self.in_flight.set(self.in_flight.get() - 1);
        self.order
            .borrow_mut()
            .push((txid.clone(), frame["n"].as_i64().unwrap_or(0)));
        if self.fail.borrow().as_deref() == Some(txid.as_str()) {
            return Err(format!("the tracker's write of {txid} faulted"));
        }
        let n = frame["n"].as_i64().unwrap_or(0);
        if self.fail_frame.borrow().as_ref() == Some(&(txid.clone(), n)) {
            return Err(format!("the tracker's write of {txid} frame {n} faulted"));
        }
        *self.count.borrow_mut().entry(txid.clone()).or_default() += 1;
        if let Some(id) = self.owner.get(&txid) {
            self.db.exec(&format!(
                "INSERT OR IGNORE INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('{id}','g1','word','{txid}',{NOW})"
            ));
        }
        Ok(())
    }
}

/// The batch's D1 (every batch `d1_ms`) and its judge port: Phase C's actor (`actor_ms`, answering applied) or, with
/// `d1_judges`, the D1 judge over the real record.
struct Timed<'a> {
    db: &'a Db,
    d1_ms: u64,
    actor_ms: u64,
    d1_judges: bool,
    t0: tokio::time::Instant,
    actors: &'a RefCell<Vec<(String, u64)>>,
}
impl SqlPort for Timed<'_> {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        if self.d1_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(self.d1_ms)).await;
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
        !self.d1_judges
    }
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
        if self.d1_judges {
            return Ok(None);
        }
        tokio::time::sleep(std::time::Duration::from_millis(self.actor_ms)).await;
        self.actors
            .borrow_mut()
            .push((identity.to_owned(), self.t0.elapsed().as_millis() as u64));
        Ok(Some(
            json!({"applied": true, "firstApply": true, "appliedGames": games, "rows": [], "atMs": NOW, "judgedAtMs": NOW + 40, "applyMs": ACTOR_MS, "bound": bound}),
        ))
    }
}

fn marks(db: &Db) -> i64 {
    db.int("SELECT COUNT(*) FROM owed_dirty")
}

/// (a) THE FLOOD LEG: A BATCH OF 16 WORD FRAMES FOR 16 IDENTITIES REACHES EVERY ACTOR WITHIN TWO INGEST WAVES, ONE PLAN
/// BATCH, THE MEMO READ AND ONE IDENTITY'S DELIVERY. Phase C (the actor is the judge: its bound read, its apply, its
/// acknowledgement). Main's consumer ingested and planned one message after another: 16 x (450 + 150) = 9,600 ms of
/// virtual time before the batch's memo read, 10,830 ms to the last answer (beta's flood class: visible p50
/// 7.8 s, p95 14.6 s).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix954_a_a_batch_of_16_word_frames_reaches_every_actor_within_one_delivery_not_sixteen() {
    let db = Db::new();
    let (evs, owner) = sixteen(&db);
    let refs: Vec<&OwedEvent> = evs.iter().collect();
    let ingest = Ingest::new(&db, owner, INGEST_MS);
    let actors = RefCell::new(Vec::new());
    let t0 = tokio::time::Instant::now();
    let timed = Timed {
        db: &db,
        d1_ms: D1_MS,
        actor_ms: ACTOR_MS,
        d1_judges: false,
        t0,
        actors: &actors,
    };
    let make = |_, _| async {
        Ok::<Timed, String>(Timed {
            db: &db,
            d1_ms: D1_MS,
            actor_ms: ACTOR_MS,
            d1_judges: false,
            t0,
            actors: &actors,
        })
    };
    let out =
        owed_events::consume_batch_with(&timed, &ingest, &refs, make, std::future::pending::<()>())
            .await
            .unwrap();
    let elapsed = t0.elapsed().as_millis() as u64;
    let actors = actors.borrow();
    assert_eq!(actors.len(), 16, "one actor call an identity");
    let first = actors.iter().map(|(_, t)| *t).min().unwrap();
    let last = actors.iter().map(|(_, t)| *t).max().unwrap();
    assert_eq!(out.routed.faults, 0);
    assert_eq!(out.routed.subjects, 16);
    let waves = 16u64.div_ceil(8);
    // the ingests' waves, the plans' one batch, the memo read, then per identity (all at once) the delivery's two reads
    // before its actor (Phase C: its marks and its bound) and the actor's apply
    let delivery = 2 * D1_MS + ACTOR_MS;
    let bar = waves * INGEST_MS + 2 * D1_MS + delivery;
    let serial = 16 * (INGEST_MS + D1_MS) + D1_MS + delivery;
    println!(
        "fix954_a: 16 word frames, 16 identities, Phase C: the first actor answered at {first} ms, the last at {last} ms, the batch ended at {elapsed} ms of virtual time (D1 {D1_MS} ms a batch, an ingest {INGEST_MS} ms, an apply {ACTOR_MS} ms); the bar {bar} ms; main's serial ingests and plans {serial} ms"
    );
    assert!(
        last <= bar,
        "every actor answered within the ingest waves, one plan batch and one delivery: the last at {last} ms against {bar} (main's serial rule {serial})"
    );
    assert_eq!(marks(&db), 0, "every mark the actors applied acknowledged");
}

/// Main's order, as the reference of the differential: each message's frame then its plan, one at a time.
async fn serial_plans(db: &Db, ingest: &Ingest<'_>, evs: &[&OwedEvent]) -> Vec<owed_events::Plan> {
    for ev in evs {
        if let Some(f) = ev.frame() {
            ingest.ingest_frame(f).await.unwrap();
        }
    }
    let mut plans = Vec::new();
    for ev in evs {
        plans.push(owed_events::plan(db, ev).await.unwrap());
    }
    plans
}

fn dump(db: &Db) -> Vec<String> {
    let conn = db.conn.borrow();
    let mut st = conn
        .prepare("SELECT * FROM owed_rows ORDER BY identity, outpoint")
        .unwrap();
    let n = st.column_count();
    let rows = st
        .query_map([], |r| {
            Ok((0..n)
                .map(|i| format!("{:?}", r.get_ref(i).unwrap()))
                .collect::<Vec<_>>()
                .join("|"))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    rows
}

/// (b) THE DIFFERENTIAL: WHAT IS JUDGED DOES NOT MOVE. The same 16 messages over two copies of the record, one through
/// `consume_batch_with` (the frames at once, the plans in one batch), one in main's order (each frame, then each plan,
/// one at a time) into the same `batch_core`, the D1 judge writing: the stored rows equal, every column, and both
/// acknowledge every mark. The actor's own differential against the D1 judge (the fleet compare at zero) rests on the
/// judge's input, which this lane does not touch: the record at the judge's time, after every frame landed.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix954_b_the_coalesced_batch_judges_what_main_judged_every_column() {
    let mut dumps = Vec::new();
    for coalesced in [true, false] {
        let db = Db::new();
        let (evs, owner) = sixteen(&db);
        let refs: Vec<&OwedEvent> = evs.iter().collect();
        let ingest = Ingest::new(&db, owner, 0);
        let actors = RefCell::new(Vec::new());
        let t0 = tokio::time::Instant::now();
        let make = |_, _| async {
            Ok::<Timed, String>(Timed {
                db: &db,
                d1_ms: 0,
                actor_ms: 0,
                d1_judges: true,
                t0,
                actors: &actors,
            })
        };
        let out = if coalesced {
            owed_events::consume_batch_with(&db, &ingest, &refs, make, std::future::pending::<()>())
                .await
                .unwrap()
        } else {
            let plans = serial_plans(&db, &ingest, &refs).await;
            owed_events::batch_core(&db, &refs, &plans, make, std::future::pending::<()>())
                .await
                .unwrap()
        };
        assert_eq!((out.routed.subjects, out.routed.faults), (16, 0));
        assert_eq!(
            marks(&db),
            0,
            "coalesced {coalesced}: every mark acknowledged"
        );
        let d = dump(&db);
        assert_eq!(d.len(), 16, "coalesced {coalesced}: a row a seat");
        dumps.push(d);
    }
    assert_eq!(dumps[0], dumps[1], "the coalesced batch stores main's rows");
}

/// (c) A FAULT IN THE MIDDLE LOSES AND DOUBLES NOTHING. The eighth frame's ingest faults: the batch answers the fault
/// (the caller then judges the batch one message at a time), nothing is judged, no mark acknowledged, every other frame
/// ingested once. The batch again with the fault gone (the platform's retry, each frame's ingest idempotent): every
/// identity judged once, one row each, every mark acknowledged.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix954_c_a_fault_in_the_middle_of_the_batch_loses_and_doubles_nothing() {
    let db = Db::new();
    let (evs, owner) = sixteen(&db);
    let refs: Vec<&OwedEvent> = evs.iter().collect();
    let ingest = Ingest::new(&db, owner, INGEST_MS);
    *ingest.fail.borrow_mut() = Some(word_txid(7));
    let actors = RefCell::new(Vec::new());
    let t0 = tokio::time::Instant::now();
    let make = |_, _| async {
        Ok::<Timed, String>(Timed {
            db: &db,
            d1_ms: 0,
            actor_ms: 0,
            d1_judges: true,
            t0,
            actors: &actors,
        })
    };
    let before = marks(&db);
    let Err(err) =
        owed_events::consume_batch_with(&db, &ingest, &refs, make, std::future::pending::<()>())
            .await
    else {
        panic!("the batch answers the ingest's fault");
    };
    assert!(err.contains("faulted"), "{err}");
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_rows"),
        0,
        "nothing judged"
    );
    assert_eq!(
        marks(&db),
        before + 15,
        "no mark acknowledged; fifteen words marked"
    );
    for k in 0..16 {
        let n = ingest
            .count
            .borrow()
            .get(&word_txid(k))
            .copied()
            .unwrap_or(0);
        assert_eq!(
            n,
            usize::from(k != 7),
            "frame {k} ingested once (the faulted one not)"
        );
    }
    // the retry
    *ingest.fail.borrow_mut() = None;
    let make = |_, _| async {
        Ok::<Timed, String>(Timed {
            db: &db,
            d1_ms: 0,
            actor_ms: 0,
            d1_judges: true,
            t0,
            actors: &actors,
        })
    };
    let out =
        owed_events::consume_batch_with(&db, &ingest, &refs, make, std::future::pending::<()>())
            .await
            .unwrap();
    assert_eq!((out.routed.subjects, out.routed.faults), (16, 0));
    for k in 0..16 {
        let id = identity(k);
        assert_eq!(
            db.int(&format!(
                "SELECT COUNT(*) FROM owed_rows WHERE identity='{id}'"
            )),
            1,
            "identity {k}: one row, never two"
        );
    }
    assert_eq!(marks(&db), 0, "every mark acknowledged after the retry");
    // a plans' batch that faults is the batch's fault, nothing judged
    let db2 = Db::new();
    let (evs2, owner2) = sixteen(&db2);
    let refs2: Vec<&OwedEvent> = evs2.iter().collect();
    let ingest2 = Ingest::new(&db2, owner2, 0);
    *db2.fail_on.borrow_mut() = Some("idx_owed_dirty_source_key".into());
    let make = |_, _| async {
        Ok::<Timed, String>(Timed {
            db: &db2,
            d1_ms: 0,
            actor_ms: 0,
            d1_judges: true,
            t0,
            actors: &actors,
        })
    };
    assert!(owed_events::consume_batch_with(
        &db2,
        &ingest2,
        &refs2,
        make,
        std::future::pending::<()>()
    )
    .await
    .is_err());
    *db2.fail_on.borrow_mut() = None;
    assert_eq!(db2.int("SELECT COUNT(*) FROM owed_rows"), 0);
}

/// (d) THE BOUND: AT MOST `INGEST_FRAMES_AT_ONCE` INGESTS IN FLIGHT, ONE TXID'S FRAMES IN ORDER. 16 frames of 16 txids
/// peak at the bound (RED with the bound removed: 16); two frames of ONE txid among them are ingested one after the
/// other in message order, never at once.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix954_d_the_ingests_in_flight_are_bounded_and_one_txids_frames_keep_their_order() {
    assert_eq!(owed_events::INGEST_FRAMES_AT_ONCE, 8);
    let db = Db::new();
    let (mut evs, owner) = sixteen(&db);
    // a second frame of txid 3, then a third, numbered
    for (i, n) in [(3usize, 1i64), (3, 2)] {
        let mut e = word_event(&word_txid(i), 300 + n);
        e.facts["frame"]["n"] = json!(n);
        evs.push(e);
    }
    let refs: Vec<&OwedEvent> = evs.iter().collect();
    let groups = owed_events::frame_groups(&refs);
    assert_eq!(groups.len(), 16, "one group a txid");
    assert!(groups.contains(&vec![3, 16, 17]), "{groups:?}");
    let ingest = Ingest::new(&db, owner, INGEST_MS);
    let t0 = tokio::time::Instant::now();
    owed_events::ingest_frames(&ingest, &refs).await.unwrap();
    let took = t0.elapsed().as_millis() as u64;
    println!(
        "fix954_d: 18 frames over 16 txids: peak in flight {}, {took} ms of virtual time",
        ingest.peak.get()
    );
    assert_eq!(
        ingest.peak.get(),
        owed_events::INGEST_FRAMES_AT_ONCE,
        "at most the bound in flight"
    );
    assert!(
        !ingest.txid_overlap.get(),
        "one txid's frames never at once"
    );
    let of3: Vec<i64> = ingest
        .order
        .borrow()
        .iter()
        .filter(|(t, _)| *t == word_txid(3))
        .map(|(_, n)| *n)
        .collect();
    assert_eq!(of3, vec![0, 1, 2], "txid 3's frames in message order");
}

/// The mixed batch's events over a record holding: X and Y each a stranded hop and a wanted envelope (`mark_wanted`),
/// W1 and W2 each a stranded hop whose word frame marks it, Z a stranded hop routed by its identity. `x_first` puts
/// the identity event of X before the wanted pair (main's plans equal message for message) or after it.
async fn mixed(db: &Db, x_first: bool) -> (Vec<OwedEvent>, BTreeMap<String, String>) {
    let (x, y, z, w1, w2) = (
        identity(20),
        identity(21),
        identity(22),
        identity(23),
        identity(24),
    );
    for (k, id) in [&x, &y, &z, &w1, &w2].into_iter().enumerate() {
        seed_stranded_hop(db, id, "g1", &owed_db::hop(8_000 + k));
    }
    // X holds a second game, so its expansion names more than one
    seed_stranded_hop(db, &x, "g2", &owed_db::hop(8_100));
    for id in [&x, &y] {
        low_app_layer::owed_judge::mark_wanted(db, id)
            .await
            .unwrap();
    }
    let mut owner = BTreeMap::new();
    owner.insert(word_txid(23), w1.clone());
    owner.insert(word_txid(24), w2.clone());
    let ev = |source: &str, key: &str, seq: i64| OwedEvent {
        v: owed_event::OWED_EVENT_V,
        seq,
        source: source.into(),
        source_key: key.into(),
        facts: json!({}),
        at_ms: NOW as i64,
    };
    let ident_x = ev(owed_event::ROUTE_IDENTITY, &x, 401);
    let mut evs = Vec::new();
    if x_first {
        evs.push(ident_x.clone());
    }
    evs.push(ev("wanted", "wanted", 402));
    evs.push(word_event(&word_txid(23), 403));
    evs.push(ev(owed_event::ROUTE_IDENTITY, &z, 404));
    evs.push(ev("wanted", "wanted", 405));
    let mut again = word_event(&word_txid(23), 406);
    again.facts["frame"]["n"] = json!(1);
    evs.push(again);
    evs.push(word_event(&word_txid(24), 407));
    evs.push(ev(owed_event::TXFACTS, &word_txid(24), 408));
    if !x_first {
        evs.push(ident_x);
    }
    (evs, owner)
}

fn envelopes(db: &Db) -> i64 {
    db.int("SELECT COUNT(*) FROM owed_dirty WHERE gameId=''")
}

/// (e) FIX-954-FOLD-1, THE MIXED BATCH (the ensemble's agreed gap: the batch's wanted-envelope expansion unpinned, A's
/// mutant G, B's M1, C's M2; C's MEDIUM-1: the plan path on a mixed batch). Over two copies of the record each, the
/// batch's plans (`ingest_frames`, then `plans_many`) against main's (each frame, then each message's `plan` one at a
/// time): equal message for message, in order (the identity event of X first: main's later read of the wanted pair
/// finds X's envelope gone, and the batch expands each envelope once, by its first message); then the whole batch
/// through `consume_batch_with` against main's plans through `batch_core`, the D1 judge writing: the same rows, every
/// column, in order, the same marks left, no envelope left. With X's identity event AFTER the wanted pair the batch's
/// read of X precedes the expansion (a plan main read after it), so only the rows and marks are compared there: what is
/// judged is the merged plan's, the same subjects.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix954f_e_a_mixed_batch_plans_and_judges_as_main_with_its_envelopes_expanded() {
    let t0 = tokio::time::Instant::now();
    let actors = RefCell::new(Vec::new());
    for x_first in [true, false] {
        // the plans, message for message
        if x_first {
            let (a, b) = (Db::new(), Db::new());
            let (evs_a, owner_a) = mixed(&a, true).await;
            let (evs_b, owner_b) = mixed(&b, true).await;
            let refs_a: Vec<&OwedEvent> = evs_a.iter().collect();
            let refs_b: Vec<&OwedEvent> = evs_b.iter().collect();
            owed_events::ingest_frames(&Ingest::new(&a, owner_a, 0), &refs_a)
                .await
                .unwrap();
            let batch = owed_events::plans_many(&a, &refs_a, true).await.unwrap();
            let main = serial_plans(&b, &Ingest::new(&b, owner_b, 0), &refs_b).await;
            assert_eq!(batch.len(), refs_a.len());
            for (i, (got, want)) in batch.iter().zip(&main).enumerate() {
                assert_eq!(
                    got, want,
                    "message {i} ({}): the batch's plan is main's",
                    refs_a[i].source
                );
            }
            assert_ne!(
                batch[1],
                owed_events::Plan::default(),
                "the wanted pair plans Y's games"
            );
            assert_eq!(
                batch[4],
                owed_events::Plan::default(),
                "the wanted pair again: its envelopes expanded"
            );
            assert_eq!(envelopes(&a), 0, "every envelope expanded by the batch");
        }
        // the rows
        let mut dumps = Vec::new();
        let mut left = Vec::new();
        for coalesced in [true, false] {
            let db = Db::new();
            let (evs, owner) = mixed(&db, x_first).await;
            let refs: Vec<&OwedEvent> = evs.iter().collect();
            assert_eq!(envelopes(&db), 2, "two wanted envelopes");
            let ingest = Ingest::new(&db, owner, 0);
            let make = |_, _| async {
                Ok::<Timed, String>(Timed {
                    db: &db,
                    d1_ms: 0,
                    actor_ms: 0,
                    d1_judges: true,
                    t0,
                    actors: &actors,
                })
            };
            let out = if coalesced {
                owed_events::consume_batch_with(
                    &db,
                    &ingest,
                    &refs,
                    make,
                    std::future::pending::<()>(),
                )
                .await
                .unwrap()
            } else {
                let plans = serial_plans(&db, &ingest, &refs).await;
                owed_events::batch_core(&db, &refs, &plans, make, std::future::pending::<()>())
                    .await
                    .unwrap()
            };
            assert_eq!(out.routed.faults, 0);
            assert_eq!(
                envelopes(&db),
                0,
                "x_first {x_first}, coalesced {coalesced}: no envelope left"
            );
            let d = dump(&db);
            for k in 20..25 {
                let id = identity(k);
                assert!(
                    db.int(&format!(
                        "SELECT COUNT(*) FROM owed_rows WHERE identity='{id}'"
                    )) > 0,
                    "x_first {x_first}, coalesced {coalesced}: identity {k} judged"
                );
            }
            left.push(db.int("SELECT COUNT(*) FROM owed_dirty"));
            dumps.push(d);
        }
        assert_eq!(
            dumps[0], dumps[1],
            "x_first {x_first}: the mixed batch stores main's rows, every column, in order"
        );
        assert_eq!(left[0], left[1], "x_first {x_first}: the same marks left");
    }
}

/// (f) FIX-954-FOLD-1, B's M2: A FRAME WHOSE INGEST FAULTS ENDS ITS TXID'S GROUP. Txid T carries three frames (`n` 0,
/// 1, 2); frame 0's ingest faults: frames 1 and 2 are not ingested (a later frame of one txid never lands before an
/// earlier one), every other txid's frame is, the batch answers the fault and judges nothing. The retry ingests T from
/// frame 0, in order. RED with the group not ended (frames 1 and 2 ingested after frame 0 faulted).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix954f_f_a_faulted_frame_ends_its_txids_group() {
    let db = Db::new();
    let (mut evs, owner) = sixteen(&db);
    for n in [1i64, 2] {
        let mut e = word_event(&word_txid(5), 300 + n);
        e.facts["frame"]["n"] = json!(n);
        evs.push(e);
    }
    let refs: Vec<&OwedEvent> = evs.iter().collect();
    let ingest = Ingest::new(&db, owner, INGEST_MS);
    *ingest.fail_frame.borrow_mut() = Some((word_txid(5), 0));
    let actors = RefCell::new(Vec::new());
    let t0 = tokio::time::Instant::now();
    let make = |_, _| async {
        Ok::<Timed, String>(Timed {
            db: &db,
            d1_ms: 0,
            actor_ms: 0,
            d1_judges: true,
            t0,
            actors: &actors,
        })
    };
    let Err(err) =
        owed_events::consume_batch_with(&db, &ingest, &refs, make, std::future::pending::<()>())
            .await
    else {
        panic!("the batch answers the ingest's fault");
    };
    assert!(err.contains("frame 0 faulted"), "{err}");
    let of5 = |ingest: &Ingest<'_>| -> Vec<i64> {
        ingest
            .order
            .borrow()
            .iter()
            .filter(|(t, _)| *t == word_txid(5))
            .map(|(_, n)| *n)
            .collect()
    };
    assert_eq!(
        of5(&ingest),
        vec![0],
        "the txid's group ended at its faulted frame"
    );
    for k in (0..16).filter(|k| *k != 5) {
        assert_eq!(
            ingest.count.borrow().get(&word_txid(k)).copied(),
            Some(1),
            "frame {k} ingested once"
        );
    }
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_rows"),
        0,
        "nothing judged"
    );
    // the retry: the group again from its first frame, in order
    *ingest.fail_frame.borrow_mut() = None;
    let make = |_, _| async {
        Ok::<Timed, String>(Timed {
            db: &db,
            d1_ms: 0,
            actor_ms: 0,
            d1_judges: true,
            t0,
            actors: &actors,
        })
    };
    let out =
        owed_events::consume_batch_with(&db, &ingest, &refs, make, std::future::pending::<()>())
            .await
            .unwrap();
    assert_eq!((out.routed.subjects, out.routed.faults), (16, 0));
    assert_eq!(
        of5(&ingest),
        vec![0, 0, 1, 2],
        "the retry ingests the group from its first frame, in order"
    );
}
