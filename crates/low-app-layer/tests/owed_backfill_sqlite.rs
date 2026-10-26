//! bsv-low #893 section 6.1 (lane D2-OWED-BACKFILL): the shipped backfill pass, the drain's age re-mark and the
//! window rule, on real SQLite under the overlay's own migrations.
#[allow(dead_code)]
#[path = "support/d1_fleet.rs"]
mod d1_fleet;
use low_app_layer::{
    owed_backfill as backfill,
    owed_judge::{self as judge, JudgeDb},
    pot_hook::{Bind, Deadline, SqlPort, Stmt, StmtOut},
};
use rusqlite::{
    types::{Value as SqlValue, ValueRef},
    Connection, StatementStatus,
};
use serde_json::{json, Value};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    rc::Rc,
};

/// One executed statement: its text, the rows it returned and its full-scan steps.
#[derive(Debug, Clone)]
struct Ran {
    sql: String,
    rows: usize,
    fullscan: i32,
}
struct Db {
    conn: RefCell<Connection>,
    ran: RefCell<Vec<Ran>>,
    batches: RefCell<Vec<Vec<String>>>,
    counters: RefCell<BTreeMap<String, u64>>,
    now: Cell<u64>,
    /// Virtual milliseconds each batch costs (the wall pin's clock).
    batch_ms: Cell<u64>,
    clock: Rc<Cell<u64>>,
}
impl Db {
    fn new() -> Self {
        let conn = Connection::open_in_memory().unwrap();
        for sql in bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                assert!(
                    bsv_overlay_cloudflare::d1::migration_error_is_benign(sql, &e.to_string()),
                    "{sql}: {e}"
                );
            }
        }
        Self {
            conn: RefCell::new(conn),
            ran: RefCell::new(Vec::new()),
            batches: RefCell::new(Vec::new()),
            counters: RefCell::new(BTreeMap::new()),
            now: Cell::new(1_790_000_000_000),
            batch_ms: Cell::new(0),
            clock: Rc::new(Cell::new(0)),
        }
    }
    /// The overlay's certification row, as `ensure_overlay_migrations` writes it.
    fn certify(&self, count: u32) {
        self.exec(&format!("INSERT INTO ops_counters(name,value) VALUES ('overlay_migration_count',{count}) ON CONFLICT(name) DO UPDATE SET value=excluded.value"));
    }
    fn exec(&self, sql: &str) {
        self.conn.borrow().execute_batch(sql).unwrap();
    }
    fn count(&self, sql: &str) -> i64 {
        self.conn.borrow().query_row(sql, [], |r| r.get(0)).unwrap()
    }
    fn seed(&self, identity: &str, game: &str) {
        self.conn.borrow().execute("INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt,sigValid) VALUES (?1,'other',?2,?3,0,1,'00',?3,1,1,1)",rusqlite::params![identity,game,format!("{identity}-{game}")]).unwrap();
    }
    fn reset(&self) {
        self.ran.borrow_mut().clear();
        self.batches.borrow_mut().clear();
    }
}
fn bind(v: &Bind) -> SqlValue {
    match v {
        Bind::Text(s) => SqlValue::Text(s.clone()),
        Bind::Int(n) => SqlValue::Integer(*n),
        Bind::Null => SqlValue::Null,
    }
}
impl SqlPort for Db {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        self.clock.set(self.clock.get() + self.batch_ms.get());
        self.batches
            .borrow_mut()
            .push(stmts.iter().map(|s| s.sql.clone()).collect());
        let mut conn = self.conn.borrow_mut();
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for st in stmts {
            let mut s = tx
                .prepare(&st.sql)
                .map_err(|e| format!("{e}: {}", st.sql))?;
            let names: Vec<_> = s.column_names().iter().map(|s| s.to_string()).collect();
            let mut rows = s
                .query(rusqlite::params_from_iter(st.binds.iter().map(bind)))
                .map_err(|e| e.to_string())?;
            let mut got = Vec::new();
            while let Some(row) = rows.next().map_err(|e| format!("{e}: {}", st.sql))? {
                let mut value = serde_json::Map::new();
                for (n, k) in names.iter().enumerate() {
                    value.insert(
                        k.clone(),
                        match row.get_ref(n).map_err(|e| e.to_string())? {
                            ValueRef::Null => Value::Null,
                            ValueRef::Integer(n) => json!(n),
                            ValueRef::Real(n) => json!(n),
                            ValueRef::Text(t) => json!(std::str::from_utf8(t).unwrap()),
                            ValueRef::Blob(b) => json!(hex::encode(b)),
                        },
                    );
                }
                got.push(Value::Object(value));
            }
            drop(rows);
            self.ran.borrow_mut().push(Ran {
                sql: st.sql.clone(),
                rows: got.len(),
                fullscan: s.get_status(StatementStatus::FullscanStep),
            });
            drop(s);
            out.push(StmtOut {
                rows: got,
                changes: Some(tx.changes()),
            });
        }
        tx.commit().map_err(|e| e.to_string())?;
        Ok(out)
    }
    async fn counters(&self, deltas: Vec<(String, u64)>) {
        for (k, v) in deltas {
            *self.counters.borrow_mut().entry(k).or_default() += v;
        }
    }
    fn now_ms(&self) -> u64 {
        self.now.get()
    }
    fn log(&self, _: &str) {}
}
impl JudgeDb for Db {
    fn tip(&self) -> Option<u64> {
        Some(900_000)
    }
    async fn word_views(
        &self,
        rows: &BTreeMap<String, low_app_layer::tracker_host::stored::Row>,
    ) -> Result<BTreeMap<String, Value>, String> {
        Ok(rows.iter().map(|(k, v)| (k.clone(), v.view())).collect())
    }
    async fn canonical_root(&self, _: u64) -> Option<String> {
        None
    }
    async fn push(&self, _: &str, _: Value) {}
}
/// A deadline on the Db's virtual clock; its timer never fires, so only the clock ends it (deterministic).
fn deadline(db: &Db, ms: u64, reserve: u64) -> Deadline {
    let clock = db.clock.clone();
    let t0 = clock.get();
    Deadline::new(
        ms,
        reserve,
        Box::new(move || clock.get() - t0),
        Box::new(|_| Box::pin(std::future::pending())),
    )
}
fn open_deadline(db: &Db) -> Deadline {
    deadline(
        db,
        backfill::OWED_BACKFILL_WALL_MS,
        backfill::OWED_BACKFILL_SAVE_RESERVE_MS,
    )
}
fn statements(db: &Db, sql: &str) -> usize {
    db.batches
        .borrow()
        .iter()
        .flatten()
        .filter(|s| *s == sql)
        .count()
}

/// Pin 7.5: with `owed-v2` stamped, the pass is ONE primary-key read of one row on the populated fixture: no party,
/// hop or dirty row is read and no statement full-scans.
#[tokio::test]
async fn owed893_backfill_a_stamped_replay_is_one_indexed_read_on_the_populated_fixture() {
    let db = Db::new();
    for row in d1_fleet::rows(1_790_000_000) {
        d1_fleet::execute_row(&db.conn.borrow(), &row);
    }
    for row in d1_fleet::owed_rows(1_790_000_000) {
        d1_fleet::execute_row(&db.conn.borrow(), &row);
    }
    db.certify(backfill::OWED_TRIGGERS_MIGRATION_COUNT);
    db.exec("INSERT INTO view_derivation_state(name) VALUES ('owed-v2')");
    db.reset();
    let r = backfill::backfill_pass(&db, &open_deadline(&db))
        .await
        .unwrap();
    assert_eq!(r.stop, backfill::Stop::Stamped);
    let ran = db.ran.borrow().clone();
    println!("stamped replay: {ran:?}");
    assert_eq!(ran.len(), 1, "a stamped replay is one statement: {ran:?}");
    assert_eq!(
        (ran[0].sql.as_str(), ran[0].rows, ran[0].fullscan),
        (backfill::STAMP_SQL, 1, 0)
    );
    let plan: Vec<String> = db
        .conn
        .borrow()
        .prepare(&format!("EXPLAIN QUERY PLAN {}", backfill::STAMP_SQL))
        .unwrap()
        .query_map([], |r| r.get(3))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(
        plan.iter()
            .all(|d| d.starts_with("SEARCH view_derivation_state USING")),
        "{plan:?}"
    );
    assert!(db.counters.borrow().is_empty());
}

/// Risk 8.4: never before the overlay's certified migration (the triggers first), and the first pass that finds the
/// gate open only arms the cursor: the work begins one cron tick later.
#[tokio::test]
async fn owed893_backfill_b_gated_until_certified_then_armed_then_one_tick_later() {
    let db = Db::new();
    db.seed("id-a", "g1");
    db.reset();
    let r = backfill::backfill_pass(&db, &open_deadline(&db))
        .await
        .unwrap();
    assert_eq!(r.stop, backfill::Stop::Gated, "no certification row");
    db.certify(backfill::OWED_TRIGGERS_MIGRATION_COUNT - 1);
    let r = backfill::backfill_pass(&db, &open_deadline(&db))
        .await
        .unwrap();
    assert_eq!(r.stop, backfill::Stop::Gated, "an older certified list");
    assert!(
        db.batches
            .borrow()
            .iter()
            .all(|b| b.len() == 1 && (b[0] == backfill::STAMP_SQL || b[0] == backfill::GATE_SQL)),
        "a gated pass writes nothing"
    );
    db.certify(backfill::OWED_TRIGGERS_MIGRATION_COUNT + 3);
    let r = backfill::backfill_pass(&db, &open_deadline(&db))
        .await
        .unwrap();
    assert_eq!(r.stop, backfill::Stop::Armed);
    assert_eq!(
        db.count("SELECT COUNT(*) FROM owed_rows"),
        0,
        "the arming pass judges nothing"
    );
    assert_eq!(
        db.count("SELECT COUNT(*) FROM owed_dirty WHERE source='backfill'"),
        0
    );
    let r = backfill::backfill_pass(&db, &open_deadline(&db))
        .await
        .unwrap();
    assert_eq!((r.stop, r.identities), (backfill::Stop::Finished, 1));
    assert_eq!(
        db.count("SELECT COUNT(*) FROM owed_rows WHERE identity='id-a'"),
        1
    );
}

/// Unstamped: at most 128 identities a pass, distinct and ascending from both party tables, the cursor saved after
/// each; the stamp lands with the save past the last identity; then one read.
#[tokio::test]
async fn owed893_backfill_c_unstamped_reads_128_identities_saves_after_each_and_stamps_at_the_end()
{
    let db = Db::new();
    db.certify(backfill::OWED_TRIGGERS_MIGRATION_COUNT);
    for n in 0..130 {
        db.seed(&format!("id-{n:03}"), "g1");
    }
    // One identity named only by a hop marker, one by both tables.
    db.exec("INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('id-001','x','h1',0,1,'03aa','00','00',2,'hop-1',1,1,1),('id-999','x','h2',0,1,'03bb','00','00',2,'hop-2',1,1,1)");
    backfill::backfill_pass(&db, &open_deadline(&db))
        .await
        .unwrap(); // armed
    db.reset();
    let r = backfill::backfill_pass(&db, &open_deadline(&db))
        .await
        .unwrap();
    assert_eq!((r.stop, r.identities), (backfill::Stop::Page, 128));
    assert_eq!(r.cursor.as_deref(), Some("id-127"));
    assert_eq!(statements(&db, backfill::CURSOR_SQL), 128);
    // Each identity's save follows its own judges and precedes the next identity's marks.
    let (mut marks, mut saves) = (0, 0);
    for b in db.batches.borrow().iter() {
        if b[0] == backfill::MARK_SQL {
            assert_eq!(
                saves, marks,
                "the cursor is saved before the next identity is marked"
            );
            marks += 1;
        }
        if b[0] == backfill::CURSOR_SQL {
            saves += 1;
        }
    }
    assert_eq!((marks, saves), (128, 128));
    let page = db
        .ran
        .borrow()
        .iter()
        .find(|r| r.sql == backfill::IDENTITIES_SQL)
        .cloned()
        .unwrap();
    assert_eq!((page.rows, page.fullscan), (128, 0));
    assert_eq!(statements(&db, backfill::STAMP_WRITE_SQL), 0);
    assert_eq!(db.counters.borrow()[backfill::COUNTER_IDENTITIES], 128);
    db.reset();
    let r = backfill::backfill_pass(&db, &open_deadline(&db))
        .await
        .unwrap();
    assert_eq!((r.stop, r.identities), (backfill::Stop::Finished, 3));
    assert_eq!(r.cursor.as_deref(), Some("id-999"));
    assert_eq!(
        db.count("SELECT COUNT(*) FROM view_derivation_state WHERE name='owed-v2'"),
        1
    );
    let last = db.batches.borrow().last().unwrap().clone();
    assert_eq!(
        last,
        vec![
            backfill::CURSOR_SQL.to_string(),
            backfill::STAMP_WRITE_SQL.to_string()
        ],
        "the stamp lands with the last save"
    );
    assert_eq!(db.counters.borrow()[backfill::COUNTER_IDENTITIES], 131);
    assert_eq!(
        db.count("SELECT COUNT(DISTINCT identity) FROM owed_state"),
        131
    );
    assert_eq!(
        db.count("SELECT COUNT(*) FROM owed_dirty WHERE source='backfill'"),
        0
    );
    db.reset();
    let r = backfill::backfill_pass(&db, &open_deadline(&db))
        .await
        .unwrap();
    assert_eq!(r.stop, backfill::Stop::Stamped);
    assert_eq!(db.ran.borrow().len(), 1);
}
/// Every game of an identity, judged 16 a batch; a legacy (keyless) row of a game no source names any longer is
/// judged away, a legacy row of a live game rewritten with its keys; the old overflow flag judged again.
#[tokio::test]
async fn owed893_backfill_d_all_games_judged_in_batches_of_16_and_legacy_rows_rewritten() {
    let db = Db::new();
    db.certify(backfill::OWED_TRIGGERS_MIGRATION_COUNT);
    for n in 0..40 {
        db.seed("whale", &format!("g{n:02}"));
    }
    // An old worker's whole-identity rows: no judgedAtMs, no keys; one of a vanished game.
    db.exec("INSERT INTO owed_rows(identity,outpoint,family,gameId,facts,updatedAtMs) VALUES ('whale','whale-g00:0','in-progress','g00','{}',1),('whale','gone:0','unbound','vanished','{}',1); INSERT INTO owed_state(identity,computedAtMs,tip,rows,stale,truncated) VALUES ('whale',1,1,2,0,1)");
    backfill::backfill_pass(&db, &open_deadline(&db))
        .await
        .unwrap(); // armed
    db.reset();
    let r = backfill::backfill_pass(&db, &open_deadline(&db))
        .await
        .unwrap();
    assert_eq!(
        (r.stop, r.identities, r.batches),
        (backfill::Stop::Finished, 1, 3)
    );
    let sizes: Vec<usize> = db
        .ran
        .borrow()
        .iter()
        .filter(|r| r.sql == backfill::GAMES_SQL)
        .map(|r| r.rows)
        .collect();
    assert_eq!(sizes, [16, 16, 9, 0], "40 live games and the vanished one");
    assert_eq!(r.rows, 40);
    assert_eq!(db.counters.borrow()[backfill::COUNTER_ROWS], 40);
    assert_eq!(
        db.count("SELECT COUNT(*) FROM owed_rows WHERE gameId='vanished'"),
        0
    );
    assert_eq!(
        db.count("SELECT COUNT(*) FROM owed_rows WHERE judgedAtMs IS NULL"),
        0
    );
    assert_eq!(
        db.count("SELECT truncated FROM owed_state WHERE identity='whale'"),
        0
    );
    assert_eq!(
        db.count("SELECT COUNT(*) FROM owed_dirty WHERE identity='whale'"),
        0
    );
}

/// The wall: an identity cut mid-way keeps its open row and remaining marks; the next pass finishes those marks and
/// never re-marks the games already judged; the cursor moves only once the identity is whole.
#[tokio::test]
async fn owed893_backfill_e_the_wall_leaves_marks_and_the_next_pass_resumes_without_re_marking() {
    let db = Db::new();
    db.certify(backfill::OWED_TRIGGERS_MIGRATION_COUNT);
    for n in 0..40 {
        db.seed("whale", &format!("g{n:02}"));
    }
    db.seed("zeta", "g1");
    backfill::backfill_pass(&db, &open_deadline(&db))
        .await
        .unwrap(); // armed
    db.reset();
    // Each batch costs 1 s of an 11 s wall with 3 s reserved: the stamp, the gate, the evidence phase's empty page and
    // its done mark (D2-OWED-FOLD-8), the page, the marks, one games page and one judge fit, then the wall.
    db.batch_ms.set(1_000);
    let r = backfill::backfill_pass(&db, &deadline(&db, 11_000, 3_000))
        .await
        .unwrap();
    assert_eq!((r.stop, r.identities), (backfill::Stop::Wall, 0));
    assert_eq!(r.batches, 1);
    assert_eq!(
        db.count("SELECT COUNT(*) FROM owed_dirty WHERE identity='whale' AND source='backfill'"),
        24
    );
    assert_eq!(
        db.count(
            "SELECT COUNT(*) FROM owed_belt_state WHERE name='backfill:owed-v2' AND cursor=''"
        ),
        1
    );
    assert_eq!(db.count("SELECT COUNT(*) FROM owed_belt_state WHERE name='backfill:owed-v2:open' AND cursor='whale'"), 1);
    assert_eq!(
        *db.counters.borrow(),
        BTreeMap::new(),
        "nothing finished, nothing counted"
    );
    db.batch_ms.set(0);
    db.reset();
    let r = backfill::backfill_pass(&db, &open_deadline(&db))
        .await
        .unwrap();
    assert_eq!(
        (r.stop, r.identities, r.batches),
        (backfill::Stop::Finished, 2, 3)
    );
    assert_eq!(
        statements(&db, backfill::MARK_SQL),
        1,
        "the open identity is not re-marked; zeta is marked once"
    );
    assert_eq!(
        db.count("SELECT COUNT(*) FROM owed_rows WHERE identity='whale'"),
        40
    );
    assert_eq!(db.count("SELECT COUNT(*) FROM owed_dirty"), 0);
}

/// Risk 8.2's third repair: rows judged over 24 h ago are re-marked `age`, at most 128 a pass, coalesced (a pending
/// mark is not rewritten), and a judged row is not re-marked again for a day.
#[tokio::test]
async fn owed893_backfill_f_the_age_remark_is_bounded_once_a_day_and_indexed() {
    let db = Db::new();
    let now = db.now.get() as i64;
    let old = now - backfill::OWED_AGE_REMARK_AFTER_MS - 1;
    for n in 0..150 {
        db.seed("aged", &format!("g{n:03}"));
    }
    db.exec("DELETE FROM owed_dirty");
    // Rows judged at the old stamp, one fresh row, one legacy (NULL) row.
    db.exec(&format!("WITH RECURSIVE n(x) AS (SELECT 0 UNION ALL SELECT x+1 FROM n WHERE x<149) INSERT INTO owed_rows(identity,outpoint,family,gameId,facts,updatedAtMs,judgedAtMs) SELECT 'aged','aged-g'||printf('%03d',x)||':0','in-progress','g'||printf('%03d',x),'{{}}',0,{old} FROM n; INSERT INTO owed_rows(identity,outpoint,family,gameId,facts,updatedAtMs,judgedAtMs) VALUES ('fresh','f:0','in-progress','gf','{{}}',0,{now}),('legacy','l:0','in-progress','gl','{{}}',0,NULL)"));
    db.reset();
    assert_eq!(backfill::mark_aged(&db).await.unwrap(), 128);
    let ran = db.ran.borrow()[0].clone();
    assert_eq!(ran.fullscan, 0, "the age mark reads the judged index");
    assert_eq!(
        db.count("SELECT COUNT(*) FROM owed_dirty WHERE source='age'"),
        128
    );
    assert_eq!(
        db.count("SELECT COUNT(*) FROM owed_dirty WHERE identity='fresh'"),
        0
    );
    // the D2-OWED lens's MEDIUM-5: the keyless (legacy) row is repaired too, first, inside the same 128
    assert_eq!(
        db.count("SELECT COUNT(*) FROM owed_dirty WHERE identity='legacy'"),
        1
    );
    let gen: i64 = db.count("SELECT MAX(generation) FROM owed_dirty");
    assert_eq!(
        backfill::mark_aged(&db).await.unwrap(),
        0,
        "a pending age mark is not rewritten"
    );
    assert_eq!(db.count("SELECT MAX(generation) FROM owed_dirty"), gen);
    // The drain judges them (restamping judgedAtMs; the legacy row names no source and is judged away); the next
    // marks are the remaining 23, then none for a day.
    while db.count("SELECT COUNT(*) FROM owed_dirty") > 0 {
        let r = judge::drain_dirty(&db, 64).await.unwrap();
        assert!(r.faults.is_empty(), "{:?}", r.faults);
    }
    assert_eq!(backfill::mark_aged(&db).await.unwrap(), 23);
    while db.count("SELECT COUNT(*) FROM owed_dirty") > 0 {
        judge::drain_dirty(&db, 64).await.unwrap();
    }
    assert_eq!(backfill::mark_aged(&db).await.unwrap(), 0);
    db.now
        .set(db.now.get() + backfill::OWED_AGE_REMARK_AFTER_MS as u64 + 1);
    assert_eq!(
        backfill::mark_aged(&db).await.unwrap(),
        128,
        "a day later, again"
    );
    assert_eq!(db.counters.borrow()[backfill::COUNTER_AGED], 128 + 23 + 128);
    // At most 128 age marks are ever pending, and a source's mark marked after them is judged first.
    db.exec("DELETE FROM owed_dirty");
    db.now
        .set(db.now.get() + backfill::OWED_AGE_REMARK_AFTER_MS as u64 + 1);
    assert_eq!(backfill::mark_aged(&db).await.unwrap(), 128);
    db.exec("DELETE FROM owed_dirty WHERE rowid IN (SELECT rowid FROM owed_dirty WHERE source='age' LIMIT 28)");
    assert_eq!(
        backfill::mark_aged(&db).await.unwrap(),
        28,
        "the room left under the pending bound"
    );
    assert_eq!(
        db.count("SELECT COUNT(*) FROM owed_dirty WHERE source='age'"),
        128
    );
    db.exec(&format!("INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('aged','g149','party','k',{})", db.now.get() + 1));
    db.exec("DELETE FROM owed_dirty WHERE source='age' AND gameId='g149'");
    let r = judge::drain_dirty(&db, 1).await.unwrap();
    assert_eq!(r.subjects, 1);
    assert_eq!(
        db.count("SELECT COUNT(*) FROM owed_dirty WHERE gameId='g149'"),
        0,
        "the source's mark is judged before the older age marks"
    );
    let plan: Vec<String> = db
        .conn
        .borrow()
        .prepare(&format!("EXPLAIN QUERY PLAN {}", backfill::AGE_MARK_SQL))
        .unwrap()
        .query_map([1, 2, 3], |r| r.get(3))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(
        plan.iter().any(|d| d.contains("idx_owed_rows_judged")),
        "{plan:?}"
    );
}

/// Risk 8.4's window rule: the belt's candidate reads `facts.claimable` where an old worker left the column NULL.
#[tokio::test]
async fn owed893_backfill_g_the_belt_candidate_reads_facts_claimable_under_a_null_column() {
    let db = Db::new();
    db.exec(r#"INSERT INTO owed_rows(identity,outpoint,family,gameId,facts,updatedAtMs,claimable,beltDueAtMs) VALUES
        ('i','a:0','hop-stranded','g','{"claimable":true}',0,NULL,1),
        ('i','b:0','hop-stranded','g','{"claimable":false}',0,NULL,2),
        ('i','c:0','hop-stranded','g','{"claimable":true}',0,0,3),
        ('i','d:0','hop-stranded','g','{}',0,NULL,4)"#);
    // the chosen row's read, by its key (the D2-OWED lens's HIGH-3 split the walk from it)
    let got: Vec<Option<i64>> = ["a:0", "b:0", "c:0", "d:0"]
        .iter()
        .map(|outpoint| {
            db.conn
                .borrow()
                .query_row(low_app_layer::owed_belt::CANDIDATES_SQL, ["i", outpoint], |r| {
                    r.get("claimable")
                })
                .unwrap()
        })
        .collect();
    assert_eq!(got, [Some(1), Some(0), Some(0), None]);
}

/// Ruling 1's end: once backfilled, an identity's read writes nothing (the first-read mark is unreachable); the mark
/// path itself stays for a later `owed-v3`, as an un-backfilled identity still shows.
#[tokio::test]
async fn owed893_backfill_h_a_backfilled_identitys_read_writes_nothing() {
    let db = Db::new();
    db.certify(backfill::OWED_TRIGGERS_MIGRATION_COUNT);
    db.seed("seat", "g1");
    db.seed("later", "g1");
    db.exec("DELETE FROM owed_dirty");
    backfill::backfill_pass(&db, &open_deadline(&db))
        .await
        .unwrap(); // armed
                   // Both identities backfilled; then `later`'s state removed, as for an identity no backfill has reached.
    backfill::backfill_pass(&db, &open_deadline(&db))
        .await
        .unwrap();
    db.exec("DELETE FROM owed_state WHERE identity='later'; DELETE FROM owed_rows WHERE identity='later'");
    db.reset();
    let snap = low_app_layer::owed_read::read(&db, "seat").await.unwrap();
    assert!(!snap.wanted);
    assert_eq!(db.batches.borrow().len(), 1, "the read's one batch");
    assert!(db.batches.borrow()[0]
        .iter()
        .all(|s| !s.starts_with("INSERT") && !s.starts_with("UPDATE") && !s.starts_with("DELETE")));
    assert_eq!(db.count("SELECT COUNT(*) FROM owed_dirty"), 0);
    db.reset();
    let snap = low_app_layer::owed_read::read(&db, "later").await.unwrap();
    assert!(
        snap.wanted,
        "the kept first-read path for an identity no backfill reached"
    );
    assert_eq!(statements(&db, judge::MARK_WANTED_SQL), 1);
}

/// The gate's figure is the overlay's list through the #893 block; the counters are the overlay's seeds.
#[test]
fn owed893_backfill_i_the_gate_and_the_counters_are_the_overlays() {
    let list = bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
    let n = backfill::OWED_TRIGGERS_MIGRATION_COUNT as usize;
    assert!(n <= list.len());
    let held = &list[..n];
    use bsv_overlay_cloudflare::derived_owed as o;
    for sql in [
        o::JUDGED_INDEX,
        o::DIRTY_CREATE,
        o::POT_INSERT,
        o::PARTY_INSERT,
        o::HOP_INSERT,
        o::WORD_INSERT,
        o::REFUSAL_DELETE,
        o::WORD_LINK_UPDATE,
        o::SWEEP_INDEX,
        o::UNTIPPED_INDEX,
        o::BELT_TURN_INDEX,
        o::DIRTY_SOURCE_KEY_INDEX,
        o::DIRTY_IDENTITY_MARKED_INDEX,
        o::UNJUDGED_INDEX,
        o::ROWS_GAME_INDEX,
        o::ROWS_COUNT_INSERT,
        o::RELEASED_HOP_EVIDENCE_MISSING_CREATE,
    ] {
        assert!(held.contains(&sql), "the gate's list holds {sql}");
    }
    assert_eq!(
        list[n - 1],
        o::RELEASED_HOP_MISSING_INSERT,
        "the figure is the block's end (D2-OWED-FOLD-8's last migration: the evidence phase writes its table)"
    );
    assert_eq!(
        backfill::counter_names(),
        bsv_overlay_cloudflare::applayer_counters::OWED_BACKFILL
            .map(str::to_owned)
            .to_vec()
    );
    for name in backfill::counter_names() {
        assert!(bsv_overlay_cloudflare::applayer_counters::seeded_names().contains(&name));
    }
}

/// D2-OWED-FOLD-8: the evidence phase visits every open eviction that names no evidence row once, 32 a page, on the
/// fleet fixture's evictions (none of them the writer's), saves its cursor in each eviction's batch, ends at the done
/// mark, and is not run again: a pass after it reads no eviction. A readmitted eviction and one the writer already
/// gave evidence are never read. Its statements full-scan nothing but the page's own ledger walk.
#[tokio::test]
async fn fold8_c_the_evidence_phase_visits_each_open_eviction_once_and_ends() {
    let db = Db::new();
    for row in d1_fleet::rows(1_790_000_000) {
        d1_fleet::execute_row(&db.conn.borrow(), &row);
    }
    // a JOIN without bytes and one verified seat: its two released hops are named for that seat
    db.exec("INSERT INTO potparty_records_evicted(identity,potTxid,gameId,opponentIdentity,seatSettlePubkey,sigValid) VALUES ('seat','ff00000000000000000000000000000000000000000000000000000000000000','g','o','02aa',1)");
    db.exec("INSERT INTO pot_evictions(txid,reason,evictedAt,releasedSpends) VALUES ('ff00000000000000000000000000000000000000000000000000000000000000','R',5,'[{\"table\":\"pot_records\",\"txid\":\"ab00000000000000000000000000000000000000000000000000000000000000\",\"vout\":0},{\"txid\":\"ab00000000000000000000000000000000000000000000000000000000000000\",\"vout\":1}]')");
    // a readmitted eviction is never read
    db.exec("INSERT INTO pot_evictions(txid,reason,evictedAt,readmittedAt,releasedSpends) VALUES ('fe00000000000000000000000000000000000000000000000000000000000000','R',5,6,'[]')");
    let open = db.count("SELECT COUNT(*) FROM pot_evictions WHERE readmittedAt IS NULL AND releasedSpends IS NOT NULL");
    db.certify(backfill::OWED_TRIGGERS_MIGRATION_COUNT);
    backfill::backfill_pass(&db, &open_deadline(&db))
        .await
        .unwrap(); // armed
    db.reset();
    let r = backfill::backfill_pass(&db, &open_deadline(&db))
        .await
        .unwrap();
    assert_eq!(r.evictions as i64, open, "{r:?}");
    assert_eq!(
        statements(&db, backfill::EVICTIONS_SQL) as i64,
        open / 32 + 1
    );
    assert_eq!(
        statements(&db, backfill::EVICTIONS_CURSOR_SQL) as i64,
        open + 1
    );
    assert_eq!(
        db.count("SELECT COUNT(*) FROM owed_belt_state WHERE name='backfill:owed-v2:evictions' AND cursor='~'"),
        1
    );
    assert_eq!(
        db.count("SELECT COUNT(*) FROM released_hop_evidence_missing WHERE identity='seat' AND gameId='g' AND bytesRead=0"),
        2,
        "the missing JOIN's two released hops, one with no table field"
    );
    assert_eq!(r.evidence_candidates, 0, "no fleet JOIN's bytes are stored");
    // NOTE-A (D2-OWED-FOLD-9): the one released spend with no table field is counted, not silent
    assert_eq!(r.tableless_spends, 1, "{r:?}");
    assert!(r.missing_outpoints >= 2, "{r:?}");
    assert_eq!(
        db.counters.borrow()["owed_backfill_evictions_total"] as i64,
        open
    );
    for ran in db
        .ran
        .borrow()
        .iter()
        .filter(|r| r.sql != backfill::EVICTIONS_SQL)
    {
        assert_eq!(ran.fullscan, 0, "{}", ran.sql);
    }
    let plan: Vec<String> = db
        .conn
        .borrow()
        .prepare(&format!("EXPLAIN QUERY PLAN {}", backfill::EVICTIONS_SQL))
        .unwrap()
        .query_map(rusqlite::params!["", 32], |r| r.get(3))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(
        plan.iter().any(
            |d| d.starts_with("SEARCH e USING INDEX sqlite_autoindex_pot_evictions")
                || d.starts_with("SEARCH e USING PRIMARY KEY")
        ),
        "{plan:?}"
    );
    assert!(
        plan.iter()
            .any(|d| d.starts_with("SEARCH h USING INDEX sqlite_autoindex_released_hop_evidence")),
        "{plan:?}"
    );
    // the phase is not run again
    db.exec("DELETE FROM view_derivation_state WHERE name='owed-v2'");
    db.reset();
    let again = backfill::backfill_pass(&db, &open_deadline(&db))
        .await
        .unwrap();
    assert_eq!(again.evictions, 0);
    assert_eq!(statements(&db, backfill::EVICTIONS_SQL), 0);
}

/// D2-OWED-FOLD-9, the fifth delta lens's DELTA5-L1: an eviction whose batch faults on every pass is counted on its
/// own row, recorded and passed after [`backfill::OWED_BACKFILL_IDENTITY_FAULTS_MAX`] passes (the identity loop's
/// LOW-1 bound), so one eviction cannot hold the evidence phase, every identity and the stamp back. The evictions
/// after it are written; the skipped one is named (its row, its counter, the report) and its outpoints stay unjudged
/// by the phase, never silent. Literal names only, so the pin compiles on the base (708b08c8) and is RED there: the
/// third pass faults like the first two and the stamp is never written.
#[tokio::test]
async fn fold9_l1_an_eviction_faulting_every_pass_is_skipped_and_counted_after_three() {
    let wedged = "aa00000000000000000000000000000000000000000000000000000000000000";
    let honest = "bb00000000000000000000000000000000000000000000000000000000000000";
    let db = Db::new();
    for (txid, seat, hop) in [(wedged, "seat-a", "a1"), (honest, "seat-b", "b1")] {
        db.exec(&format!("INSERT INTO potparty_records_evicted(identity,potTxid,gameId,opponentIdentity,seatSettlePubkey,sigValid) VALUES ('{seat}','{txid}','g-{seat}','o','02aa',1)"));
        db.exec(&format!("INSERT INTO pot_evictions(txid,reason,evictedAt,releasedSpends) VALUES ('{txid}','R',5,'[{{\"table\":\"pot_records\",\"txid\":\"{hop}{}\",\"vout\":0}}]')", "0".repeat(62)));
    }
    db.exec(&format!("CREATE TRIGGER wedge BEFORE INSERT ON released_hop_evidence_missing WHEN NEW.potTxid='{wedged}' BEGIN SELECT RAISE(ABORT,'wedged'); END;"));
    db.certify(backfill::OWED_TRIGGERS_MIGRATION_COUNT);
    backfill::backfill_pass(&db, &open_deadline(&db))
        .await
        .unwrap(); // armed
    for pass in 1..backfill::OWED_BACKFILL_IDENTITY_FAULTS_MAX {
        let r = backfill::backfill_pass(&db, &open_deadline(&db)).await;
        assert!(r.is_err(), "pass {pass}: the fault is the pass's: {r:?}");
        assert_eq!(
            db.count("SELECT COUNT(*) FROM released_hop_evidence_missing WHERE potTxid LIKE 'bb%'"),
            0,
            "pass {pass}: the phase stops at the faulted eviction"
        );
    }
    let r = backfill::backfill_pass(&db, &open_deadline(&db)).await;
    let r = r.expect("the third faulted pass records the eviction and goes on");
    assert_eq!(
        db.count(&format!("SELECT COUNT(*) FROM owed_belt_state WHERE name='backfill:owed-v2:skipped-eviction:{wedged}' AND cursor LIKE '%wedged%'")),
        1,
        "recorded with its fault"
    );
    assert_eq!(
        db.counters.borrow()["owed_backfill_skipped_evictions_total"],
        1
    );
    assert_eq!(
        db.count(&format!("SELECT COUNT(*) FROM released_hop_evidence_missing WHERE potTxid='{honest}' AND identity='seat-b'")),
        1,
        "the eviction after it is written"
    );
    assert_eq!(
        db.count("SELECT COUNT(*) FROM owed_belt_state WHERE name='backfill:owed-v2:evictions' AND cursor='~'"),
        1,
        "the phase is done"
    );
    assert_eq!(
        db.count("SELECT COUNT(*) FROM view_derivation_state WHERE name='owed-v2'"),
        1,
        "the identities are visited and the stamp written: {r:?}"
    );
    assert_eq!(
        db.count("SELECT COUNT(*) FROM owed_belt_state WHERE name='backfill:owed-v2:eviction-faults' AND cursor<>'{}'"),
        0,
        "the fault count is cleared once the eviction is passed"
    );
}

/// D2-OWED-FOLD-9, the fifth delta lens's NOTE-B: an evidence row of a READMITTED eviction no longer hides the seat's
/// missing-evidence row of a later open eviction releasing the same hop. The judge's evidence read (`RELEASED_SQL`)
/// honours an evidence row only under its open ledger and its own `evictedAt`; the missing read's exclusion now reads
/// the same, so the seat is served the could-not-judge row and never nothing. RED on 708b08c8 (the re-export of
/// `MISSING_SQL` grafted): no row.
#[test]
fn fold9_b_a_readmitted_evictions_evidence_hides_no_missing_row() {
    let db = Db::new();
    let (a, b, hop) = (
        format!("a1{}", "0".repeat(62)),
        format!("b2{}", "0".repeat(62)),
        format!("cc{}", "0".repeat(62)),
    );
    db.exec(&format!("INSERT INTO pot_evictions(txid,reason,evictedAt,readmittedAt,releasedSpends) VALUES ('{a}','R',5,6,'[]')"));
    db.exec(&format!("INSERT INTO pot_evictions(txid,reason,evictedAt,releasedSpends) VALUES ('{b}','R',7,'[]')"));
    db.exec(&format!("INSERT INTO released_hop_evidence(hopTxid,hopVout,identity,gameId,potTxid,seatSettlePubkey,hopSats,evictedAt) VALUES ('{hop}',0,'s','g','{a}','02aa',1000,5)"));
    db.exec(&format!("INSERT INTO released_hop_evidence_missing(identity,gameId,hopTxid,hopVout,potTxid,evictedAt,bytesRead) VALUES ('s','g','{hop}',0,'{b}',7,0)"));
    let served = |db: &Db| {
        db.conn
            .borrow()
            .prepare(judge::MISSING_SQL)
            .unwrap()
            .query_map(rusqlite::params!["s", "[\"g\"]"], |r| r.get::<_, String>("potTxid"))
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>()
    };
    assert_eq!(served(&db), vec![b.clone()], "the readmitted eviction's evidence is not the seat's");
    // the control: evidence of the OPEN eviction itself still excludes its missing row
    db.exec(&format!("UPDATE released_hop_evidence SET potTxid='{b}',evictedAt=7"));
    assert!(served(&db).is_empty(), "the seat's own evidence names the outpoint");
}

/// D2-OWED-FOLD-10, the sixth delta lens's NOTE-1: the evidence the judge honours is ONE predicate. `RELEASED_SQL` (the
/// judge's evidence read) and `MISSING_SQL`'s exclusion read the same text, once each, so an edit to one is an edit to
/// both. RED on 237ae72b (the rule written twice, in two spellings): `RELEASED_SQL` names no shared predicate.
#[test]
fn fold10_n1_the_honoured_evidence_is_one_predicate_both_reads_use() {
    let released = judge::RELEASED_SQL;
    let predicate = released
        .strip_prefix("SELECT h.* FROM ")
        .and_then(|rest| rest.strip_suffix(" AND h.hopTxid=?1 AND h.hopVout=?2"))
        .unwrap_or_else(|| panic!("RELEASED_SQL is the shared predicate and its key: {released}"));
    assert!(
        predicate.contains("he.readmittedAt IS NULL AND h.evictedAt=he.evictedAt"),
        "{predicate}"
    );
    assert_eq!(
        judge::MISSING_SQL.matches(predicate).count(),
        1,
        "MISSING_SQL's exclusion reads the same predicate: {}",
        judge::MISSING_SQL
    );
    assert_eq!(released.matches(predicate).count(), 1);
    // the judge's evidence read stays a key search
    let db = Db::new();
    let plan: Vec<String> = db
        .conn
        .borrow()
        .prepare(&format!("EXPLAIN QUERY PLAN {released}"))
        .unwrap()
        .query_map(rusqlite::params!["", 0], |r| r.get(3))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(
        plan.iter()
            .any(|d| d.starts_with("SEARCH h USING INDEX sqlite_autoindex_released_hop_evidence")),
        "{plan:?}"
    );
    assert!(!plan.iter().any(|d| d.starts_with("SCAN")), "{plan:?}");
}
