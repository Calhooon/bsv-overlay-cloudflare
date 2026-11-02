//! bsv-low #893 section 7.4: the belt's bound, the shipped tick over the shipped judge on real SQLite, with a courier
//! fake that counts every call.
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
use low_app_layer::{
    owed_belt::{self as belt, Courier},
    owed_judge::{self as judge, JudgeDb},
    pot_hook::{Bind, SqlPort, Stmt, StmtOut},
};
use rusqlite::{
    types::{Value as SqlValue, ValueRef},
    Connection,
};
use serde_json::{json, Value};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
};

const NOW: u64 = 1_790_000_000_000;
const HOUR: i64 = 3_600_000;

struct Db {
    conn: RefCell<Connection>,
    batches: RefCell<Vec<Vec<Stmt>>>,
    counters: RefCell<BTreeMap<String, u64>>,
    now: Cell<u64>,
    tip: Cell<Option<u64>>,
    /// FIX-1057 (item 4): the clock moves this much after each batch (0: still), so a stamp bound before a read and a
    /// clock read after it differ, as on a Worker.
    step: Cell<u64>,
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
            batches: RefCell::new(Vec::new()),
            counters: RefCell::new(BTreeMap::new()),
            now: Cell::new(NOW),
            tip: Cell::new(Some(900_000)),
            step: Cell::new(0),
        }
    }
    fn exec(&self, sql: &str) {
        self.conn.borrow().execute_batch(sql).unwrap();
    }
    fn int(&self, sql: &str) -> i64 {
        self.conn.borrow().query_row(sql, [], |r| r.get(0)).unwrap()
    }
    fn opt(&self, sql: &str) -> Option<i64> {
        self.conn.borrow().query_row(sql, [], |r| r.get(0)).unwrap()
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
        self.batches.borrow_mut().push(stmts.clone());
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
            out.push(StmtOut {
                rows: got,
                changes: Some(tx.changes()),
            });
        }
        tx.commit().map_err(|e| e.to_string())?;
        self.now.set(self.now.get() + self.step.get());
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
        self.tip.get()
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

type Answer = Option<(u16, Vec<u8>)>;

/// Answers by URL fragment; counts every call it is asked (a refused ask never reaches it).
#[derive(Default)]
struct Fake {
    calls: RefCell<Vec<String>>,
    answers: RefCell<Vec<(String, Answer)>>,
    default_spent: RefCell<Answer>,
}
impl Fake {
    fn on(&self, fragment: &str, answer: Answer) {
        self.answers.borrow_mut().push((fragment.into(), answer));
    }
}
impl Courier for Fake {
    async fn get(&self, url: String) -> Option<(u16, Vec<u8>)> {
        self.calls.borrow_mut().push(url.clone());
        for (fragment, answer) in self.answers.borrow().iter().rev() {
            if url.contains(fragment.as_str()) {
                return answer.clone();
            }
        }
        if url.ends_with("/spent") {
            return self.default_spent.borrow().clone();
        }
        if url.ends_with("/spend") {
            return Some((200, br#"{"spent":false}"#.to_vec()));
        }
        Some((404, b"{}".to_vec()))
    }
}

fn hop(n: usize) -> String {
    format!("b3{n:062x}")
}
fn spender_of(hop_txid: &str) -> (String, Vec<u8>) {
    let mut tx = Transaction::new();
    tx.add_input(TransactionInput::new(hop_txid.to_string(), 0))
        .unwrap();
    tx.add_output(TransactionOutput {
        satoshis: Some(20_000),
        locking_script: LockingScript::from_hex(&format!("76a914{}88ac", "11".repeat(20))).unwrap(),
        change: false,
    })
    .unwrap();
    (tx.id(), tx.to_binary())
}

/// 64 games, one stranded hop each, judged by the shipped drain; each row dated by the judge, then given a distinct
/// due time so the tick's order is known: hop 0 is due first.
async fn seeded() -> Db {
    let db = Db::new();
    // `createdAt` is in seconds
    let made = (NOW as i64 - 2 * HOUR) / 1000;
    for n in 0..64 {
        let (game, h) = (format!("g{n:02}"), hop(n));
        db.exec(&format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind) VALUES ('{h}',0,0,{made},'p2pkh'); INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('owner','other','{game}',0,20190,'seat','00','00',2,'{h}',1,{made},1);"));
    }
    while db.int("SELECT COUNT(*) FROM owed_dirty") > 0 {
        let r = judge::drain_dirty(&db, 64).await.unwrap();
        assert!(r.faults.is_empty(), "{:?}", r.faults);
    }
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_rows WHERE family='hop-stranded'"),
        64,
        "an index-unspent hop past its 30 minutes, with no word"
    );
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_rows WHERE beltDueAtMs IS NOT NULL"),
        64,
        "the judge dates every row that owes the chain a question: {:?}",
        db.int("SELECT COUNT(*) FROM owed_rows WHERE family='hop-stranded'")
    );
    for n in 0..64 {
        db.exec(&format!(
            "UPDATE owed_rows SET beltDueAtMs={} WHERE outpoint='{}:0'",
            NOW as i64 - 100_000 + n as i64,
            hop(n)
        ));
    }
    db
}
fn due_of(db: &Db, n: usize) -> Option<i64> {
    db.opt(&format!(
        "SELECT beltDueAtMs FROM owed_rows WHERE outpoint='{}:0'",
        hop(n)
    ))
}
fn memo_of(db: &Db, n: usize) -> i64 {
    db.int(&format!(
        "SELECT COUNT(*) FROM hop_chain_probes WHERE outpoint='{}.0'",
        hop(n)
    ))
}

#[tokio::test]
async fn owed893_belt_one_tick_asks_eight_by_due_time_within_24_calls() {
    let db = seeded().await;
    let fake = Fake::default();
    *fake.default_spent.borrow_mut() = Some((404, b"{}".to_vec()));
    let first = belt::tick(&db, &fake).await.unwrap();
    assert_eq!(
        (first.candidates, first.asked, first.found),
        (8, 8, 8),
        "{first:?}"
    );
    assert!(fake.calls.borrow().len() <= belt::OWED_BELT_CALLS_PER_TICK);
    assert_eq!(first.calls, fake.calls.borrow().len());
    for n in 0..8 {
        assert_eq!(memo_of(&db, n), 1, "hop {n}'s find is memoised");
        assert!(
            due_of(&db, n).unwrap() > NOW as i64,
            "hop {n} is dated past this tick"
        );
    }
    for n in 8..64 {
        assert_eq!(memo_of(&db, n), 0, "hop {n} is not asked in the first tick");
    }
    let asked_first: Vec<String> = fake.calls.borrow().clone();
    fake.calls.borrow_mut().clear();
    db.now.set(NOW + belt::OWED_BELT_TICK_MS as u64);
    let second = belt::tick(&db, &fake).await.unwrap();
    assert_eq!((second.asked, second.found), (8, 8));
    assert!(fake.calls.borrow().len() <= belt::OWED_BELT_CALLS_PER_TICK);
    for n in 8..16 {
        assert_eq!(
            memo_of(&db, n),
            1,
            "the next tick asks the next eight by due time: hop {n}"
        );
    }
    for n in 16..64 {
        assert_eq!(memo_of(&db, n), 0);
    }
    assert!(fake.calls.borrow().iter().all(|u| !asked_first.contains(u)));
    assert_eq!(db.counters.borrow()["owed_belt_ticks_total"], 2);
    assert_eq!(db.counters.borrow()["owed_belt_asked_total"], 16);
    assert_eq!(db.counters.borrow()["owed_belt_found_total"], 16);
    assert_eq!(
        db.int("SELECT value FROM ops_counters WHERE name='owed_belt_due_total'"),
        48
    );
    assert_eq!(
        db.conn
            .borrow()
            .query_row(
                "SELECT cursor FROM owed_belt_state WHERE name='belt'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        format!("{}:0", hop(15))
    );
}

#[tokio::test]
async fn owed893_belt_a_429_closes_its_rung_and_rearms_each_candidate_once() {
    let db = seeded().await;
    let fake = Fake::default();
    // WoC can look and says unspent; its corroborator BananaBlocks answers 429, Bitails faults: no candidate is
    // corroborated, every rung's turn ends, the tick does not.
    *fake.default_spent.borrow_mut() = Some((404, b"{}".to_vec()));
    fake.on("bananablocks.com", Some((429, b"{}".to_vec())));
    fake.on("bitails.io", Some((503, b"{}".to_vec())));
    let report = belt::tick(&db, &fake).await.unwrap();
    assert_eq!(
        (report.candidates, report.found, report.rearmed),
        (8, 0, 8),
        "{report:?}"
    );
    assert!(fake.calls.borrow().len() <= belt::OWED_BELT_CALLS_PER_TICK);
    let banana = fake
        .calls
        .borrow()
        .iter()
        .filter(|u| u.contains("bananablocks.com"))
        .count();
    let bitails = fake
        .calls
        .borrow()
        .iter()
        .filter(|u| u.contains("bitails.io"))
        .count();
    assert_eq!(banana, 1, "a 429 ends that rung's turn for the tick");
    assert_eq!(bitails, 3, "three faults close the rung's circuit");
    assert_eq!(report.faults[2], 1);
    assert_eq!(report.faults[1], 3);
    for n in 0..8 {
        assert_eq!(memo_of(&db, n), 0, "an unanswered ask writes no memo");
        assert_eq!(
            due_of(&db, n),
            Some(NOW as i64 + belt::OWED_BELT_UNANSWERED_RETRY_MS),
            "hop {n}: re-armed once at now + 300 s, never pushed further"
        );
    }
    for n in 8..64 {
        assert!(due_of(&db, n).unwrap() < NOW as i64, "hop {n} untouched");
    }
    // a second unanswered tick inside the window asks the NEXT eight: the re-armed rows are not due
    let again = belt::tick(&db, &fake).await.unwrap();
    assert_eq!(again.rearmed, 8);
    for n in 0..8 {
        assert_eq!(
            due_of(&db, n),
            Some(NOW as i64 + belt::OWED_BELT_UNANSWERED_RETRY_MS)
        );
    }
    assert_eq!(
        due_of(&db, 8),
        Some(NOW as i64 + belt::OWED_BELT_UNANSWERED_RETRY_MS)
    );
    assert_eq!(
        db.counters.borrow()["owed_belt_faults_bananablocks_total"],
        2
    );
}

#[tokio::test]
async fn owed893_belt_a_find_is_one_batch_and_a_faulted_row_insert_leaves_no_memo() {
    let db = seeded().await;
    let fake = Fake::default();
    *fake.default_spent.borrow_mut() = Some((404, b"{}".to_vec()));
    let (spender, raw) = spender_of(&hop(0));
    fake.on(
        &format!("{}/0/spent", hop(0)),
        Some((
            200,
            json!({"txid": spender, "vin": 0, "status": "unconfirmed"})
                .to_string()
                .into_bytes(),
        )),
    );
    fake.on(
        &format!("tx/{spender}/hex"),
        Some((200, hex::encode(&raw).into_bytes())),
    );
    db.exec("CREATE TRIGGER fail_owed BEFORE INSERT ON owed_rows WHEN NEW.outpoint LIKE 'b30000%' BEGIN SELECT RAISE(ABORT,'injected row fault'); END;");
    let faulted = belt::tick(&db, &fake).await.unwrap();
    assert!(faulted
        .write_faults
        .iter()
        .any(|(o, e)| o == &format!("{}:0", hop(0)) && e.contains("injected row fault")));
    assert_eq!(memo_of(&db, 0), 0, "a faulted row insert leaves no memo");
    assert_eq!(
        db.int(&format!(
            "SELECT COUNT(*) FROM owed_spender_bytes WHERE txid='{spender}'"
        )),
        0,
        "nor the spender's bytes"
    );
    assert_eq!(
        due_of(&db, 0),
        Some(NOW as i64 + belt::OWED_BELT_UNANSWERED_RETRY_MS)
    );
    db.exec("DROP TRIGGER fail_owed;");
    // its re-armed look comes due first (the other due rows are older; bring hop 0's date to the head)
    db.exec(&format!(
        "UPDATE owed_rows SET beltDueAtMs={} WHERE outpoint='{}:0'",
        NOW as i64 - 200_000,
        hop(0)
    ));
    db.batches.borrow_mut().clear();
    db.now.set(NOW + belt::OWED_BELT_UNANSWERED_RETRY_MS as u64);
    let found = belt::tick(&db, &fake).await.unwrap();
    assert!(found.write_faults.is_empty(), "{found:?}");
    assert_eq!(memo_of(&db, 0), 1);
    assert_eq!(
        db.int(&format!(
            "SELECT COUNT(*) FROM owed_spender_bytes WHERE txid='{spender}'"
        )),
        1
    );
    // the memo, the bytes and the judge's rows for that subject: one batch
    let batches = db.batches.borrow();
    let batch = batches
        .iter()
        .find(|b| {
            b.iter()
                .any(|s| s.sql.starts_with("INSERT INTO owed_spender_bytes"))
        })
        .expect("the find's batch");
    let at = |p: &str| {
        batch
            .iter()
            .position(|s| s.sql.starts_with(p))
            .unwrap_or_else(|| panic!("{p}"))
    };
    assert!(at("INSERT INTO hop_chain_probes") < at("INSERT INTO owed_spender_bytes"));
    assert!(at("INSERT INTO owed_spender_bytes") < at("INSERT INTO owed_rows"));
    assert!(batch
        .iter()
        .any(|s| s.sql.starts_with("DELETE FROM owed_dirty")));
    // no other batch of the tick wrote a memo
    assert_eq!(
        batches
            .iter()
            .flatten()
            .filter(|s| s.sql.starts_with("INSERT INTO hop_chain_probes"))
            .count(),
        batch
            .iter()
            .filter(|s| s.sql.starts_with("INSERT INTO hop_chain_probes"))
            .count()
            + 7,
        "each find's memo rides its own judge batch"
    );
    let facts: Value = serde_json::from_str(
        &db.conn
            .borrow()
            .query_row(
                &format!("SELECT facts FROM owed_rows WHERE outpoint='{}:0'", hop(0)),
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        facts["chainSpender"]
            .as_str()
            .or(facts["spendingTxid"].as_str()),
        Some(spender.as_str()),
        "{facts}"
    );
    eprintln!("judged over the find: {facts}");
}

#[test]
fn owed893_belt_candidates_ride_the_belt_index() {
    let db = Db::new();
    let conn = db.conn.borrow();
    let plan = |sql: &str, binds: &[&dyn rusqlite::ToSql]| -> Vec<String> {
        let mut plan = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        plan.query_map(binds, |r| r.get(3))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    };
    let due = plan(belt::BELT_DUE_SQL, &[&"owner", &(NOW as i64), &8]);
    assert!(
        due.iter().any(|d| d.contains(
            "SEARCH owed_rows USING COVERING INDEX idx_owed_rows_belt_turn (identity=? AND beltDueAtMs<?)"
        )),
        "{due:?}"
    );
    let walk = plan(belt::BELT_IDENTITIES_SQL, &[&"", &64]);
    assert!(
        walk.iter().any(|d| d.contains("idx_owed_rows_belt_turn (identity>?)")),
        "{walk:?}"
    );
    for d in due.iter().chain(&walk) {
        assert!(!d.starts_with("SCAN owed_rows"), "{d}");
    }
}

#[tokio::test]
async fn owed893_belt_a_spender_whose_raw_no_courier_serves_keeps_its_row_and_a_date() {
    let db = seeded().await;
    let before: Vec<String> = (0..8)
        .map(|n| {
            db.conn
                .borrow()
                .query_row(
                    &format!("SELECT facts FROM owed_rows WHERE outpoint='{}:0'", hop(n)),
                    [],
                    |r| r.get(0),
                )
                .unwrap()
        })
        .collect();
    let fake = Fake::default();
    // every outpoint is named spent by WoC, and no courier serves the spender's raw: four asks each if unbounded
    *fake.default_spent.borrow_mut() = Some((
        200,
        json!({"txid": "cc".repeat(32), "vin": 0, "status": "unconfirmed"})
            .to_string()
            .into_bytes(),
    ));
    let report = belt::tick(&db, &fake).await.unwrap();
    assert_eq!(
        fake.calls.borrow().len(),
        belt::OWED_BELT_CALLS_PER_TICK,
        "three an outpoint, 24 a tick"
    );
    assert_eq!((report.found, report.rearmed), (0, 8), "{report:?}");
    for (n, facts) in before.iter().enumerate() {
        let now: String = db
            .conn
            .borrow()
            .query_row(
                &format!("SELECT facts FROM owed_rows WHERE outpoint='{}:0'", hop(n)),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(&now, facts, "hop {n}: an unverified name changes no fact");
        assert_eq!(
            due_of(&db, n),
            Some(NOW as i64 + belt::OWED_BELT_UNANSWERED_RETRY_MS)
        );
        assert_eq!(memo_of(&db, n), 0);
    }
}

#[test]
fn owed893_belt_counters_are_seeded_and_served_by_the_overlay() {
    let names: Vec<String> = bsv_overlay_cloudflare::applayer_counters::OWED_BELT
        .iter()
        .map(|n| n.to_string())
        .collect();
    assert_eq!(belt::counter_names(), names);
    let seeded = bsv_overlay_cloudflare::applayer_counters::seeded_names();
    assert!(names.iter().all(|n| seeded.contains(n)));
    let view = bsv_overlay_cloudflare::applayer_counters::owed_belt_view(&json!({
        "owed_belt_ticks_total": 30, "owed_belt_asked_total": 200, "owed_belt_found_total": 150,
        "owed_belt_due_total": 12, "owed_belt_faults_bananablocks_total": 4,
    }));
    assert_eq!(
        (
            view["ticks"].as_u64(),
            view["asked"].as_u64(),
            view["found"].as_u64(),
            view["due"].as_u64()
        ),
        (Some(30), Some(200), Some(150), Some(12))
    );
    assert_eq!(view["faults"]["bananablocks"], 4);
    assert_eq!(view["faults"]["woc"], 0);
    assert_eq!(
        view["bounds"]["callsPerTick"].as_u64(),
        Some(belt::OWED_BELT_CALLS_PER_TICK as u64)
    );
    assert_eq!(
        view["bounds"]["outpointsPerTick"].as_u64(),
        Some(belt::OWED_BELT_OUTPOINTS_PER_TICK as u64)
    );
    assert_eq!(
        view["bounds"]["tickMs"].as_i64(),
        Some(belt::OWED_BELT_TICK_MS)
    );
}

#[tokio::test]
async fn owed893_belt_a_tick_without_a_chain_tip_asks_nothing_and_moves_no_date() {
    let db = seeded().await;
    let fake = Fake::default();
    *fake.default_spent.borrow_mut() = Some((404, b"{}".to_vec()));
    db.tip.set(None);
    let report = belt::tick(&db, &fake).await.unwrap();
    assert!(report.no_tip, "{report:?}");
    assert_eq!(
        fake.calls.borrow().len(),
        0,
        "the judge refuses every find without a tip: no courier call is spent on one"
    );
    for n in 0..64 {
        assert_eq!(
            due_of(&db, n),
            Some(NOW as i64 - 100_000 + n as i64),
            "hop {n}: still due"
        );
        assert_eq!(memo_of(&db, n), 0);
    }
    db.tip.set(Some(900_000));
    let next = belt::tick(&db, &fake).await.unwrap();
    assert_eq!((next.asked, next.found), (8, 8), "{next:?}");
    for n in 0..8 {
        assert_eq!(memo_of(&db, n), 1, "the first eight by due time");
    }
}

/// FIX-1049 (bsv-low #1049, L58 pair 17): one stranded hop of `identity` (`n` names its hop and game) whose FILED
/// sweep the tracker holds at `word`, its last broadcaster hint `age_s` seconds before NOW, and, with `seen_s`, the
/// sweep's bytes first held `seen_s` seconds before NOW (its first sight); judged by the shipped drain at NOW.
async fn landing_into(
    db: &Db,
    identity: &str,
    n: usize,
    word: &str,
    age_s: i64,
    seen_s: Option<i64>,
) -> String {
    landing_filed(db, identity, n, word, age_s, seen_s, 2 * 3_600).await
}
/// [`landing_into`] with the filing taken `filed_s` seconds before NOW (`hopsweep_records.createdAt`).
async fn landing_filed(
    db: &Db,
    identity: &str,
    n: usize,
    word: &str,
    age_s: i64,
    seen_s: Option<i64>,
    filed_s: i64,
) -> String {
    let made = (NOW as i64 - 2 * HOUR) / 1000;
    let filed = NOW as i64 / 1000 - filed_s;
    let h = hop(1049 + n);
    let game = format!("g{}", 1049 + n);
    let (sweep, raw) = spender_of(&h);
    db.exec(&format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind) VALUES ('{h}',0,0,{made},'p2pkh'); INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('{identity}','other','{game}',0,20190,'seat','00','00',2,'{h}',1,{made},1);"));
    db.exec(&format!("INSERT INTO hopsweep_records(identity,gameId,hopTxid,hopVout,sweepTxid,sweepRawHex,txid,outputIndex,createdAt) VALUES ('{identity}','{game}','{h}',0,'{sweep}','{}','e5{:062x}',0,{filed})", hex::encode(&raw), 1049 + n));
    db.exec(&format!("INSERT INTO tx_words(txid,word,tier,hint_source,hint_at,updated_at) VALUES ('{sweep}','{word}','hint','arcade',{},{})", NOW as i64 / 1000 - age_s, NOW as i64 / 1000));
    if let Some(seen) = seen_s {
        db.exec(&format!(
            "INSERT INTO owed_spender_bytes(txid,bytesJson,createdAtMs) VALUES ('{sweep}','{{\"inputs\":[[\"{h}\",0]],\"outputs\":[]}}',{})",
            NOW as i64 - seen * 1000
        ));
    }
    while db.int("SELECT COUNT(*) FROM owed_dirty") > 0 {
        let r = judge::drain_dirty(db, 64).await.unwrap();
        assert!(r.faults.is_empty(), "{:?}", r.faults);
    }
    format!("{h}:0")
}
async fn landing(word: &str, age_s: i64, seen_s: Option<i64>) -> (Db, String) {
    let db = Db::new();
    let outpoint = landing_into(&db, "owner", 0, word, age_s, seen_s).await;
    (db, outpoint)
}
fn row_of(db: &Db, outpoint: &str) -> (String, Value, Option<i64>) {
    db.conn
        .borrow()
        .query_row(
            &format!("SELECT family,facts,beltDueAtMs FROM owed_rows WHERE outpoint='{outpoint}'"),
            [],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    serde_json::from_str(&r.get::<_, String>(1)?).unwrap(),
                    r.get::<_, Option<i64>>(2)?,
                ))
            },
        )
        .unwrap()
}

/// FIX-1049, as folded (FIX-1049-FOLD-1, fold 3): a landing wait whose sweep's bytes the record first held a minute
/// ago is YOUNG and is re-asked on the belt's NEXT tick, whether the couriers answered unspent or not at all; the
/// served next-ask date says so; the row never offers a sweep; the young-rows gauge counts the row once a tick.
#[tokio::test]
async fn fix1049_b_a_young_miss_is_re_asked_on_the_next_tick() {
    let (db, outpoint) = landing("announced", 60, Some(60)).await;
    let (family, facts, due) = row_of(&db, &outpoint);
    assert_eq!(family, "hop-stranded");
    assert_eq!(facts["claimable"], false, "the swept hop offers no sweep");
    assert_eq!(facts["sweepLanding"], true);
    assert_eq!(facts["sweepFirstSeenAtMs"], json!(NOW as i64 - 60_000));
    let tick = belt::OWED_BELT_TICK_MS;
    assert_eq!(
        due,
        Some(NOW as i64 + tick),
        "the judge dates the young landing for the next tick"
    );
    let fake = Fake::default();
    *fake.default_spent.borrow_mut() = Some((404, b"{}".to_vec()));
    let at = NOW + tick as u64;
    db.now.set(at);
    let report = belt::tick(&db, &fake).await.unwrap();
    assert_eq!((report.candidates, report.found), (1, 1), "{report:?}");
    assert_eq!(report.young_rows, 1, "{report:?}");
    let (_, facts, due) = row_of(&db, &outpoint);
    assert_eq!(facts["claimable"], false);
    assert_eq!(
        due,
        Some(at as i64 + tick),
        "a young miss is asked again on the next tick, never after the 300 s floor"
    );
    assert_eq!(
        db.int("SELECT value FROM ops_counters WHERE name='owed_belt_young_rows'"),
        1,
        "the gauge: one row in its young window"
    );
    // no rung answers (every courier 429s): still the next tick
    let fake = Fake::default();
    for host in ["whatsonchain.com", "bananablocks.com", "bitails.io"] {
        fake.on(host, Some((429, b"{}".to_vec())));
    }
    let at = at + tick as u64;
    db.now.set(at);
    let report = belt::tick(&db, &fake).await.unwrap();
    assert_eq!(
        (report.found, report.rearmed, report.young_rows),
        (0, 1, 1),
        "{report:?}"
    );
    assert_eq!(row_of(&db, &outpoint).2, Some(at as i64 + tick));
}

/// FIX-1049: the back-off is unchanged for a look with no young window: a landing wait only the tracker names (no
/// first sight of the sweep's bytes), one whose bytes were first held an hour ago, and a stranded hop with no filing.
#[tokio::test]
async fn fix1049_c_an_old_miss_backs_off_as_before() {
    for seen in [None, Some(3_600)] {
        let (db, outpoint) = landing("announced", 60, seen).await;
        let (_, facts, due) = row_of(&db, &outpoint);
        assert_eq!(facts["claimable"], false, "a live sweep offers no sweep");
        assert_eq!(due, Some(NOW as i64 + 600_000), "the family's base date");
        let fake = Fake::default();
        *fake.default_spent.borrow_mut() = Some((404, b"{}".to_vec()));
        let at = NOW + 600_000;
        db.now.set(at);
        let report = belt::tick(&db, &fake).await.unwrap();
        assert_eq!((report.found, report.young_rows), (1, 0), "{report:?}");
        assert!(
            row_of(&db, &outpoint).2.unwrap() >= at as i64 + 600_000,
            "an old miss waits its period: {:?}",
            row_of(&db, &outpoint).2
        );
        // an unanswered old miss: the 300 s floor and its doubling, as before
        let fake = Fake::default();
        for host in ["whatsonchain.com", "bananablocks.com", "bitails.io"] {
            fake.on(host, Some((429, b"{}".to_vec())));
        }
        let at = row_of(&db, &outpoint).2.unwrap() as u64;
        db.now.set(at);
        let report = belt::tick(&db, &fake).await.unwrap();
        assert_eq!((report.rearmed, report.young_rows), (1, 0), "{report:?}");
        assert!(
            row_of(&db, &outpoint).2.unwrap() >= at as i64 + belt::OWED_BELT_UNANSWERED_RETRY_MS
        );
    }
    // a stranded hop with no filing: the press, its base date, no young re-ask
    let db = seeded().await;
    let fake = Fake::default();
    *fake.default_spent.borrow_mut() = Some((404, b"{}".to_vec()));
    let report = belt::tick(&db, &fake).await.unwrap();
    assert_eq!(report.young_rows, 0, "{report:?}");
    for n in 0..8 {
        assert!(due_of(&db, n).unwrap() >= NOW as i64 + 600_000, "hop {n}");
    }
}

/// FIX-1049-FOLD-1 (fold 2; A MEDIUM-2, B NOTE-1): THE WAIT HAS AN EXIT, through the shipped judge and belt. A filed
/// sweep whose last broadcaster word is 55 minutes old (it never landed: no hint since) is a landing wait dated for
/// the wait's END (5 minutes out, not the family's 10), and the belt's look there (the couriers still see the hop
/// unspent) re-judges the row into the press again, the filed bytes riding it. RED on ee8b6753: dated 600 s out, and
/// still the wait after the look.
#[tokio::test]
async fn fix1049f1_b_a_dead_sweep_presses_again_at_the_belts_look_after_the_wait() {
    let (db, outpoint) = landing("seen", 55 * 60, None).await;
    let (_, facts, due) = row_of(&db, &outpoint);
    assert_eq!(facts["sweepLanding"], true, "inside the wait: {facts}");
    let end = NOW as i64 - 55 * 60_000 + 3_600_000;
    assert_eq!(facts["sweepLandingUntilMs"], json!(end));
    assert_eq!(due, Some(end), "the look comes at the wait's end");
    let fake = Fake::default();
    *fake.default_spent.borrow_mut() = Some((404, b"{}".to_vec()));
    db.now.set(end as u64);
    let report = belt::tick(&db, &fake).await.unwrap();
    assert_eq!((report.candidates, report.found), (1, 1), "{report:?}");
    let (family, facts, _) = row_of(&db, &outpoint);
    assert_eq!(family, "hop-stranded");
    assert_eq!(
        (facts["claim"].clone(), facts["claimable"].clone()),
        (json!("sweep-hop"), json!(true)),
        "past the wait the press is back: {facts}"
    );
    assert!(facts["sweepRawHex"].is_string(), "the filed bytes ride it");
    assert!(facts.get("sweepLanding").is_none());
}

/// FIX-1049-FOLD-1 (fold 5; A NOTE-2, B RISK): through the shipped tick, young looks take at most half of the
/// places: eight landing waits of eight identities in their young window beside 64 due stranded hops of `owner`.
#[tokio::test]
async fn fix1049f1_d_young_looks_take_at_most_half_a_tick() {
    let db = seeded().await;
    for n in 0..8 {
        landing_into(&db, &format!("young{n}"), 1 + n, "announced", 60, Some(60)).await;
    }
    db.exec("UPDATE owed_rows SET beltDueAtMs=1 WHERE json_extract(facts,'$.sweepLanding')=1");
    let fake = Fake::default();
    *fake.default_spent.borrow_mut() = Some((404, b"{}".to_vec()));
    let report = belt::tick(&db, &fake).await.unwrap();
    assert_eq!(
        report.candidates,
        belt::OWED_BELT_OUTPOINTS_PER_TICK,
        "{report:?}"
    );
    assert_eq!(
        report.young_rows,
        belt::OWED_BELT_YOUNG_PER_TICK,
        "{report:?}"
    );
}

fn cursor_of(db: &Db) -> String {
    db.conn
        .borrow()
        .query_row(
            "SELECT cursor FROM owed_belt_state WHERE name='belt:identities'",
            [],
            |r| r.get::<_, String>(0),
        )
        .unwrap_or_default()
}

/// FIX-1049-FOLD-2 (fold 1; the round-2 ensemble's B MEDIUM-1, A LOW-1; lens B's P1): D-L1 under young rows. Six
/// identities `a0..a5` each hold one due landing wait that is NOT young (no first sight), four `z0..z3` one young
/// landing each, walked after them. The places give the rows that are not young four and the young four, so a4 and a5
/// get no place this tick while the young z's after them do: the walk's cursor must stay before a4 (no identity holding
/// a due row is passed unserved). RED on 9eab66d7 (only a cut YOUNG row held the cursor back): "the cursor \"z3\"
/// passed a4".
#[tokio::test]
async fn fix1049f2_1a_the_cursor_never_passes_a_due_row_the_places_cut() {
    let db = Db::new();
    let mut n = 0;
    for i in 0..6 {
        landing_into(&db, &format!("a{i}"), n, "announced", 60, None).await;
        n += 1;
    }
    for i in 0..4 {
        landing_into(&db, &format!("z{i}"), n, "announced", 60, Some(60)).await;
        n += 1;
    }
    db.exec("UPDATE owed_rows SET beltDueAtMs=1");
    let fake = Fake::default();
    *fake.default_spent.borrow_mut() = Some((404, b"{}".to_vec()));
    let report = belt::tick(&db, &fake).await.unwrap();
    let cur = cursor_of(&db);
    let served_a: Vec<String> = (0..6)
        .filter(|i| {
            let o = format!("{}:0", hop(1049 + i));
            row_of(&db, &o).2 != Some(1)
        })
        .map(|i| format!("a{i}"))
        .collect();
    assert_eq!(
        served_a.len(),
        4,
        "the places cut two of the a's: {report:?}"
    );
    for i in 0..6 {
        let a = format!("a{i}");
        if !served_a.contains(&a) {
            assert!(
                cur.as_str() < a.as_str(),
                "the cursor {cur:?} passed {a}, which holds a due row the places cut and was not served"
            );
        }
    }
}

/// FIX-1049-FOLD-2 (fold 2; B LOW-1, A NOTE-3; lens B's P2): THE YOUNG CAP IDLES NO PLACE. With ONLY young rows due
/// (eight identities) the tick asks eight: the cap of four binds only while rows that are not young compete for the
/// places. RED on 9eab66d7: the tick asked 4 and left 4 places idle.
#[tokio::test]
async fn fix1049f2_2a_young_rows_alone_take_every_place() {
    let db = Db::new();
    for i in 0..8 {
        landing_into(&db, &format!("y{i}"), i, "announced", 60, Some(60)).await;
    }
    db.exec("UPDATE owed_rows SET beltDueAtMs=1");
    let fake = Fake::default();
    *fake.default_spent.borrow_mut() = Some((404, b"{}".to_vec()));
    let report = belt::tick(&db, &fake).await.unwrap();
    assert_eq!(
        (report.candidates, report.young_rows),
        (
            belt::OWED_BELT_OUTPOINTS_PER_TICK,
            belt::OWED_BELT_OUTPOINTS_PER_TICK
        ),
        "eight due young rows and nothing else due: {report:?}"
    );
}

/// FIX-1049-FOLD-2 (fold 1; lens B's P1b): the cost of the pass. Six a's, four young z's and 120 rows that are not
/// young after them; a tick every 120 s until a4 (cut by the places on the first tick) is looked at: the next tick,
/// never a revolution of the walk later. RED on 9eab66d7: 17 ticks.
#[tokio::test]
async fn fix1049f2_1b_a_cut_row_is_looked_at_on_the_next_tick() {
    let db = Db::new();
    let mut n = 0;
    for i in 0..6 {
        landing_into(&db, &format!("a{i}"), n, "announced", 60, None).await;
        n += 1;
    }
    for i in 0..4 {
        landing_into(&db, &format!("z{i}"), n, "announced", 60, Some(60)).await;
        n += 1;
    }
    for i in 0..120 {
        landing_into(&db, &format!("zm{i:03}"), n, "announced", 60, None).await;
        n += 1;
    }
    db.exec("UPDATE owed_rows SET beltDueAtMs=1");
    let a4 = format!("{}:0", hop(1049 + 4));
    let mut ticks = 0;
    let mut at = NOW;
    while row_of(&db, &a4).2 == Some(1) && ticks < 200 {
        let fake = Fake::default();
        *fake.default_spent.borrow_mut() = Some((404, b"{}".to_vec()));
        db.now.set(at);
        belt::tick(&db, &fake).await.unwrap();
        ticks += 1;
        at += belt::OWED_BELT_TICK_MS as u64;
    }
    assert!(ticks <= 2, "a4 waited {ticks} ticks");
}

/// FIX-1049-FOLD-2 (fold 4; B LOW-2; lens B's P3): a landing wait (no first sight) looked at again and again inside
/// its wait, the couriers still answering unspent: the back-off doubles, and the row's next look never passes the
/// wait's end. RED with the landing cap taken out of the back-off's date: the look at +4,800 s past the end at +3,540 s.
#[tokio::test]
async fn fix1049f2_4_the_backoff_never_dates_a_landing_past_its_end() {
    let (db, outpoint) = landing("seen", 60, None).await;
    let (_, facts, mut due) = row_of(&db, &outpoint);
    let end = facts["sweepLandingUntilMs"].as_i64().unwrap();
    let mut looks = 0;
    while let Some(d) = due {
        if d >= end || looks > 20 {
            break;
        }
        let fake = Fake::default();
        *fake.default_spent.borrow_mut() = Some((404, b"{}".to_vec()));
        db.now.set(d as u64);
        belt::tick(&db, &fake).await.unwrap();
        looks += 1;
        let (_, f, nd) = row_of(&db, &outpoint);
        if f["sweepLanding"] != true {
            break;
        }
        assert!(
            nd.unwrap() <= end,
            "the next look +{} s is past the wait's end +{} s",
            (nd.unwrap() - NOW as i64) / 1000,
            (end - NOW as i64) / 1000
        );
        due = nd;
    }
    assert!(looks >= 3, "the back-off doubled before the end: {looks}");
    assert_eq!(
        due,
        Some(end),
        "the last look inside the wait is dated at its end"
    );
}

/// FIX-1049-FOLD-2 (fold 5; B LOW-4, A RISK wait-past-end-unanswered): PAST ITS END THE WAIT RE-JUDGES WITHOUT A
/// COURIER ANSWER. A filed sweep's landing wait dated for its end; at the belt's look there every rung refuses (429):
/// the judgement needs no new fact, so the look judges the row anyway and the press is back on the stored evidence
/// (the filed bytes riding it), never only on a courier's answer or the daily age repair. RED on 9eab66d7: the look
/// re-armed the row and it still said "waiting" past its end.
#[tokio::test]
async fn fix1049f2_5_past_its_end_the_wait_re_judges_with_no_courier_answer() {
    let (db, outpoint) = landing("seen", 55 * 60, None).await;
    let (_, facts, due) = row_of(&db, &outpoint);
    let end = facts["sweepLandingUntilMs"].as_i64().unwrap();
    assert_eq!(due, Some(end), "the look comes at the wait's end");
    let fake = Fake::default();
    for host in ["whatsonchain.com", "bananablocks.com", "bitails.io"] {
        fake.on(host, Some((429, b"{}".to_vec())));
    }
    db.now.set(end as u64);
    let report = belt::tick(&db, &fake).await.unwrap();
    assert_eq!(report.candidates, 1, "{report:?}");
    assert!(report.write_faults.is_empty(), "{report:?}");
    let (family, facts, _) = row_of(&db, &outpoint);
    assert_eq!(family, "hop-stranded");
    assert_eq!(
        (facts["claim"].clone(), facts["claimable"].clone()),
        (json!("sweep-hop"), json!(true)),
        "past the wait the press is back with no courier answer: {facts}"
    );
    assert!(facts["sweepRawHex"].is_string(), "the filed bytes ride it");
    assert_eq!(
        db.int("SELECT COUNT(*) FROM owed_dirty"),
        0,
        "the look's own mark is acknowledged by the judge that read it"
    );
    // inside the wait an unanswered look still judges nothing: the row is only re-armed
    let (db, outpoint) = landing("seen", 60, None).await;
    let before = row_of(&db, &outpoint);
    let fake = Fake::default();
    for host in ["whatsonchain.com", "bananablocks.com", "bitails.io"] {
        fake.on(host, Some((429, b"{}".to_vec())));
    }
    db.now.set(before.2.unwrap() as u64);
    let report = belt::tick(&db, &fake).await.unwrap();
    assert_eq!((report.found, report.rearmed), (0, 1), "{report:?}");
    assert_eq!(row_of(&db, &outpoint).1, before.1, "nothing re-judged");
}

/// FIX-1049-FOLD-2 (fold 3; B LOW-3, the #1041 class): THE WAIT'S END IS A CLOCK FACT AT THE COMPARE. One record, one
/// judge (the actor and the D1 judge run one derivation, `owed::derive_owed_rows`): judged inside the wait and judged
/// again past its end, the two judgements agree with a fresh judgement at the same instant (the operator's compare,
/// `owed_actor::rows_view`, names no difference); and a row stored inside the wait against a fresh judgement past its
/// end (the fleet compare's shape: an actor that judged before the end, D1 judged fresh after it) is a CLOCK LAG,
/// served in `tipLag`, never in `diff` (the STOP). RED on 9eab66d7: `diff` named the outpoint.
#[tokio::test]
async fn fix1049f2_3_the_compare_reads_the_waits_end_as_the_judgements_clock() {
    use low_app_layer::owed_actor::{diff_rows, rows_view};
    let (db, outpoint) = landing("seen", 55 * 60, None).await;
    let game = "g1049".to_string();
    async fn stored(db: &Db) -> Vec<Value> {
        db.run_batch(vec![Stmt {
            sql: low_app_layer::owed_actor::ROWS_SQL.into(),
            binds: vec![Bind::Text("owner".into())],
        }])
        .await
        .unwrap()
        .remove(0)
        .rows
    }
    let inside = stored(&db).await;
    assert_eq!(inside.len(), 1, "{inside:?}");
    let fresh_inside = judge::judge_fresh_rows(&db, "owner", std::slice::from_ref(&game))
        .await
        .unwrap();
    assert!(
        diff_rows(&fresh_inside, &inside).is_empty(),
        "inside the wait the two judgements agree"
    );
    let end = row_of(&db, &outpoint).1["sweepLandingUntilMs"]
        .as_i64()
        .unwrap();
    db.now.set(end as u64 + 1);
    let fresh_past = judge::judge_fresh_rows(&db, "owner", std::slice::from_ref(&game))
        .await
        .unwrap();
    assert_eq!(
        diff_rows(&fresh_past, &inside),
        vec![outpoint.clone()],
        "the premise: the verdict moved with the clock alone"
    );
    let view = rows_view(
        "owner",
        Ok(json!({"rows": inside})),
        &fresh_past,
        None,
        None,
    );
    assert_eq!(
        view["diff"],
        json!([]),
        "no STOP across the wait's end: {view}"
    );
    assert_eq!(
        view["tipLag"],
        json!([outpoint]),
        "served as the clock's lag: {view}"
    );
    // the row stored past the end (the belt's look re-judged it) agrees with the fresh judgement past the end
    let fake = Fake::default();
    *fake.default_spent.borrow_mut() = Some((404, b"{}".to_vec()));
    belt::tick(&db, &fake).await.unwrap();
    let past = stored(&db).await;
    let fresh_past = judge::judge_fresh_rows(&db, "owner", std::slice::from_ref(&game))
        .await
        .unwrap();
    assert!(
        diff_rows(&fresh_past, &past).is_empty(),
        "past the end the two judgements agree"
    );
    // a verdict difference that is NOT the wait's end stays a diff: the same pair with the press's claim changed
    let mut wrong = fresh_past.clone();
    let mut facts: Value = serde_json::from_str(wrong[0]["facts"].as_str().unwrap()).unwrap();
    facts["sweepTxid"] = json!("ff".repeat(32));
    wrong[0]["facts"] = json!(facts.to_string());
    let view = rows_view("owner", Ok(json!({"rows": inside})), &wrong, None, None);
    assert_eq!(
        view["diff"],
        json!([outpoint]),
        "another sweep is a real difference: {view}"
    );
    // FIX-1057 item 6 (round 3's C LOW-1, its probe P1): THE PREDICATE READS BOTH VERDICTS. The same sweep, the
    // later side withheld for another reason (no claim, not claimable, another reason): a judge disagreement, a STOP,
    // never a clock lag. RED on abf78cb4: `tipLag` named it and `diff` was `[]`.
    for withheld in [
        json!({"claim": null, "claimable": false, "claimReason": "another reason"}),
        json!({"claim": "sweep-hop", "claimable": false}),
        json!({"claim": "internalize", "claimable": true}),
    ] {
        let mut other = fresh_past.clone();
        let mut facts: Value = serde_json::from_str(other[0]["facts"].as_str().unwrap()).unwrap();
        for (k, v) in withheld.as_object().unwrap() {
            facts[k] = v.clone();
        }
        other[0]["facts"] = json!(facts.to_string());
        if withheld["claimable"] == false {
            other[0]["claimable"] = json!(0);
            other[0]["reason"] = json!("another reason");
        }
        let view = rows_view("owner", Ok(json!({"rows": inside})), &other, None, None);
        assert_eq!(
            view["diff"],
            json!([outpoint]),
            "a later verdict that is not the wait's press is a STOP ({withheld}): {view}"
        );
        assert_eq!(
            view["tipLag"],
            json!([]),
            "never a lag ({withheld}): {view}"
        );
    }
}

// ── FIX-1057 (bsv-low #1057): PR 264's LOWs, every bound of the belt pinned ─────────────────────────────────────────

/// PHASE C over the shipped tick: the same record, the D1 judge off (`d1_judges` false), so the tick reads the actors'
/// asks (`ask_candidates`), writes a find's facts alone and hands it back (`ask_find`), and dates the ask from the
/// actor's answer (`ask_backoff`). The actor is a stand-in over the same SQLite: it judges the game's marks with the
/// shipped judge (its own rows), publishes its dated rows' asks through the shipped `ask_of_row` and `ASK_PUT_SQL`, and
/// answers its rows (`owed_actor::ROWS_SQL`), as the object does.
struct PhaseC(Db);
impl SqlPort for PhaseC {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        self.0.run_batch(stmts).await
    }
    async fn counters(&self, deltas: Vec<(String, u64)>) {
        self.0.counters(deltas).await
    }
    fn now_ms(&self) -> u64 {
        self.0.now_ms()
    }
    fn log(&self, _: &str) {}
}
impl JudgeDb for PhaseC {
    fn tip(&self) -> Option<u64> {
        self.0.tip()
    }
    async fn word_views(
        &self,
        rows: &BTreeMap<String, low_app_layer::tracker_host::stored::Row>,
    ) -> Result<BTreeMap<String, Value>, String> {
        self.0.word_views(rows).await
    }
    async fn canonical_root(&self, h: u64) -> Option<String> {
        self.0.canonical_root(h).await
    }
    async fn push(&self, _: &str, _: Value) {}
    fn d1_judges(&self) -> bool {
        false
    }
    async fn actor_apply(
        &self,
        identity: &str,
        games: &[String],
        _kind: &str,
    ) -> Result<Option<Value>, String> {
        Ok(Some(actor_apply(&self.0, identity, games).await))
    }
}
/// The stand-in actor's apply: judge the marks, publish the game's asks, answer the identity's rows.
async fn actor_apply(db: &Db, identity: &str, games: &[String]) -> Value {
    while db.int("SELECT COUNT(*) FROM owed_dirty") > 0 {
        let r = judge::drain_dirty(db, 64).await.unwrap();
        assert!(r.faults.is_empty(), "{:?}", r.faults);
    }
    let dated = db
        .run_batch(vec![Stmt {
            sql: low_app_layer::owed_actor::ASK_ROWS_SQL.into(),
            binds: vec![
                Bind::Text(identity.into()),
                Bind::Text(json!(games).to_string()),
            ],
        }])
        .await
        .unwrap()
        .remove(0)
        .rows;
    let puts = dated
        .iter()
        .filter_map(|r| belt::ask_of_row(identity, r))
        .map(|(name, body, due)| Stmt {
            sql: low_app_layer::owed_actor::ASK_PUT_SQL.into(),
            binds: vec![
                Bind::Text(name),
                Bind::Text(body.to_string()),
                Bind::Int(due),
            ],
        })
        .collect::<Vec<_>>();
    if !puts.is_empty() {
        db.run_batch(puts).await.unwrap();
    }
    let rows = db
        .run_batch(vec![Stmt {
            sql: low_app_layer::owed_actor::ROWS_SQL.into(),
            binds: vec![Bind::Text(identity.into())],
        }])
        .await
        .unwrap()
        .remove(0)
        .rows;
    json!({ "rows": rows })
}
fn ask_due_of(db: &Db, identity: &str, outpoint: &str) -> Option<i64> {
    db.opt(&format!(
        "SELECT tickedAtMs FROM owed_belt_state WHERE name='{}'",
        belt::ask_name(identity, outpoint)
    ))
}
fn game_of(outpoint: &str, db: &Db) -> String {
    db.conn
        .borrow()
        .query_row(
            &format!("SELECT gameId FROM owed_rows WHERE outpoint='{outpoint}'"),
            [],
            |r| r.get::<_, String>(0),
        )
        .unwrap()
}
fn unspent() -> Fake {
    let fake = Fake::default();
    *fake.default_spent.borrow_mut() = Some((404, b"{}".to_vec()));
    fake
}
fn all_429() -> Fake {
    let fake = Fake::default();
    for host in ["whatsonchain.com", "bananablocks.com", "bitails.io"] {
        fake.on(host, Some((429, b"{}".to_vec())));
    }
    fake
}

/// FIX-1057 item 1 (round 3's A LOW-1): PHASE C's back-off never dates a landing past its end. The `fix1049f2_4`
/// shape through the ask ledger: a landing wait (no first sight) looked at again and again inside its wait, the
/// couriers answering unspent: the ask's next look doubles and never passes the wait's end, and the last look inside
/// it is dated at its end. RED with the cap taken out of `ask_backoff` (`now + period` bound in place of
/// `next_look`): the third look dated +4,080 s, past the end at +3,540 s.
#[tokio::test]
async fn fix1057_1_phase_c_backoff_never_dates_a_landing_past_its_end() {
    let db = PhaseC(Db::new());
    let outpoint = landing_into(&db.0, "owner", 0, "seen", 60, None).await;
    let game = game_of(&outpoint, &db.0);
    actor_apply(&db.0, "owner", std::slice::from_ref(&game)).await;
    let end = row_of(&db.0, &outpoint).1["sweepLandingUntilMs"]
        .as_i64()
        .unwrap();
    let mut due = ask_due_of(&db.0, "owner", &outpoint);
    assert_eq!(
        due,
        Some(NOW as i64 + 600_000),
        "the ask published at the family's base date"
    );
    let mut looks = 0;
    while let Some(d) = due {
        if d >= end || looks > 20 {
            break;
        }
        db.0.now.set(d as u64);
        let report = belt::tick(&db, &unspent()).await.unwrap();
        assert_eq!(
            (report.candidates, report.handed_back),
            (1, 1),
            "{report:?}"
        );
        looks += 1;
        let nd = ask_due_of(&db.0, "owner", &outpoint);
        assert!(
            nd.unwrap() <= end,
            "phase C: the next look +{} s is past the wait's end +{} s",
            (nd.unwrap() - NOW as i64) / 1000,
            (end - NOW as i64) / 1000
        );
        due = nd;
    }
    assert!(looks >= 3, "the back-off doubled before the end: {looks}");
    assert_eq!(
        due,
        Some(end),
        "the last look inside the wait is dated at its end"
    );
    assert_eq!(
        db.0.int("SELECT COUNT(*) FROM owed_rows WHERE json_extract(facts,'$.sweepLanding')=1"),
        1,
        "still the wait inside it"
    );
}

/// FIX-1057 item 2 (round 3's A LOW-2): PHASE C's hand-back of an ended wait with no courier answer. A landing wait
/// whose ask comes due at its end; every rung answers 429: the look is an empty find of the row's subject, written to
/// the record alone, handed back to the actor (`belt-find`), whose judgement at the look's instant gives the press back
/// (the filed bytes riding it). RED with `ended_wait_look`'s `Ok(None)` arm made to pass the answer through: the ask
/// re-armed and the row still the wait.
#[tokio::test]
async fn fix1057_2_phase_c_hands_an_ended_wait_back_with_no_courier_answer() {
    let db = PhaseC(Db::new());
    let outpoint = landing_into(&db.0, "owner", 0, "seen", 55 * 60, None).await;
    let game = game_of(&outpoint, &db.0);
    actor_apply(&db.0, "owner", std::slice::from_ref(&game)).await;
    let end = row_of(&db.0, &outpoint).1["sweepLandingUntilMs"]
        .as_i64()
        .unwrap();
    assert_eq!(
        ask_due_of(&db.0, "owner", &outpoint),
        Some(end),
        "asked at the wait's end"
    );
    db.0.now.set(end as u64);
    let fake = all_429();
    let report = belt::tick(&db, &fake).await.unwrap();
    assert_eq!(
        (
            report.candidates,
            report.found,
            report.handed_back,
            report.rearmed
        ),
        (1, 1, 1, 0),
        "the ended wait is handed back with no answer: {report:?}"
    );
    assert!(report.write_faults.is_empty(), "{report:?}");
    let (family, facts, _) = row_of(&db.0, &outpoint);
    assert_eq!(family, "hop-stranded");
    assert_eq!(
        (facts["claim"].clone(), facts["claimable"].clone()),
        (json!("sweep-hop"), json!(true)),
        "the actor's row presses again: {facts}"
    );
    assert!(facts["sweepRawHex"].is_string(), "the filed bytes ride it");
    assert!(facts.get("sweepLanding").is_none());
}

/// FIX-1057 item 3 (round 3's B LOW-1, its P1 shape): a young landing walked FIRST (`a0`), two older landings that
/// are not young (`b0`, `b1`), and a first-class flood (the 64 `owner` rows block-dated, courier-only). The young row
/// keeps one of the second class's reserved places (it is asked on the first tick), the walk's cursor moves (it never
/// freezes on the first identity), and the older row the places cut is asked on the next tick. RED on abf78cb4 (fold 2's
/// places, `(6, 2, 0)`): "young rows asked: 0", the cursor "" (no write: every tick restarts at a0).
#[tokio::test]
async fn fix1057_3_a_young_row_walked_first_keeps_a_place_under_a_first_class_flood() {
    let db = seeded().await;
    let a0 = landing_into(&db, "a0", 0, "announced", 60, Some(60)).await;
    let b0 = landing_into(&db, "b0", 1, "announced", 60, None).await;
    let b1 = landing_into(&db, "b1", 2, "announced", 60, None).await;
    db.exec("UPDATE owed_rows SET beltDueAtMs=1 WHERE identity IN ('a0','b0','b1')");
    db.exec("UPDATE owed_rows SET probeTip=899999, claimable=1, facts=json_set(facts,'$.status','unknown') WHERE identity='owner'");
    let fake = unspent();
    let report = belt::tick(&db, &fake).await.unwrap();
    let asked = |fake: &Fake, outpoint: &str| {
        let txid = outpoint.split(':').next().unwrap().to_string();
        fake.calls.borrow().iter().any(|u| u.contains(&txid))
    };
    assert_eq!(
        report.candidates,
        belt::OWED_BELT_OUTPOINTS_PER_TICK,
        "{report:?}"
    );
    assert_eq!(report.young_rows, 1, "young rows asked: {report:?}");
    assert!(
        asked(&fake, &a0),
        "the young row walked first is asked on the first tick"
    );
    let cursor = cursor_of(&db);
    assert!(!cursor.is_empty(), "the walk's cursor moved: {cursor:?}");
    assert_eq!(
        [asked(&fake, &b0), asked(&fake, &b1)],
        [true, false],
        "one reserved place to the rows that are not young, in walk order"
    );
    // the next tick: the cut older row is next
    db.now.set(NOW + belt::OWED_BELT_TICK_MS as u64);
    let fake = unspent();
    belt::tick(&db, &fake).await.unwrap();
    assert!(
        asked(&fake, &b1),
        "the row the places cut is asked on the next tick"
    );
}

/// FIX-1057 item 4 (round 3's B LOW-2): the find's own judgement serves the RECORD's first sight. A landing wait (no
/// first sight); the belt's look finds the hop spent by its filed sweep, unconfirmed, and writes the sweep's bytes
/// (`owed_spender_bytes.createdAtMs`, the tick's instant); the clock moves a second after each batch, so the judge's
/// hydration reads a later clock. The row judged in the find's batch states the record's stamp. RED on abf78cb4:
/// `left: Some(1790000601000) right: Some(1790000600000)` class (the hydration clock).
#[tokio::test]
async fn fix1057_4_the_finds_first_sight_is_the_records_stamp() {
    let (db, outpoint) = landing("seen", 60, None).await;
    let hop_txid = outpoint.split(':').next().unwrap().to_string();
    let (sweep, raw) = spender_of(&hop_txid);
    let fake = Fake::default();
    *fake.default_spent.borrow_mut() = Some((404, b"{}".to_vec()));
    fake.on(
        &format!("{hop_txid}/0/spent"),
        Some((
            200,
            json!({"txid": sweep, "vin": 0, "status": "unconfirmed"})
                .to_string()
                .into_bytes(),
        )),
    );
    fake.on(
        &format!("tx/{sweep}/hex"),
        Some((200, hex::encode(&raw).into_bytes())),
    );
    let at = NOW + 600_000;
    db.now.set(at);
    db.step.set(1_000);
    let report = belt::tick(&db, &fake).await.unwrap();
    db.step.set(0);
    assert_eq!(report.found, 1, "{report:?}");
    let stamp = db.opt(&format!(
        "SELECT createdAtMs FROM owed_spender_bytes WHERE txid='{sweep}'"
    ));
    assert_eq!(
        stamp,
        Some(at as i64),
        "the record's stamp is the tick's instant"
    );
    // FIX-1057-FOLD-1 (the ensemble's C LOW-1): one clock read a tick, passed down: the find's judgement is stamped
    // at the tick's instant, never the hydration's later read (the clock here moves a second a batch). RED with
    // `Find::apply` leaving `i.now_ms` as hydrated: the row's `judgedAtMs` past the tick's.
    assert_eq!(
        db.int(&format!(
            "SELECT judgedAtMs FROM owed_rows WHERE outpoint='{outpoint}'"
        )),
        at as i64,
        "the find's judgement is at the tick's instant"
    );
    let (family, facts, _) = row_of(&db, &outpoint);
    assert!(
        facts.get("sweepFirstSeenAtMs").is_some(),
        "the row states a first sight: {family} {facts}"
    );
    assert_eq!(
        facts["sweepFirstSeenAtMs"].as_i64(),
        stamp,
        "the row's first sight is the record's own stamp: {facts}"
    );
}

/// FIX-1057-FOLD-1 (the FIX-1057 ensemble's C LOW-3, A LOW-1, B LOW-1): THE FIGURE THAT TELLS A STARVED CLASS. Over
/// fix1057_3's flood (64 first-class rows, a young landing walked first, two older second-class rows due since t=1),
/// each tick SETS the oldest due row's age per class it read (`owed_belt_oldest_due_age_ms_*`, the age plus one), so
/// the older second-class row the places cut shows its age grown by the tick on the next; and a tick that reads no
/// young row sets that class to 0 (none). New API (no such gauge on 5f24a4eb); RED with the gauges' binds held at 0:
/// "the second class's oldest due age".
#[tokio::test]
async fn fix1057f1_3_the_tick_serves_each_classs_oldest_due_age() {
    let db = seeded().await;
    landing_into(&db, "a0", 0, "announced", 60, Some(60)).await;
    landing_into(&db, "b0", 1, "announced", 60, None).await;
    landing_into(&db, "b1", 2, "announced", 60, None).await;
    db.exec("UPDATE owed_rows SET beltDueAtMs=1 WHERE identity IN ('a0','b0','b1')");
    db.exec("UPDATE owed_rows SET probeTip=899999, claimable=1, facts=json_set(facts,'$.status','unknown') WHERE identity='owner'");
    let gauge = |db: &Db, class: &str| {
        db.int(&format!(
            "SELECT value FROM ops_counters WHERE name='owed_belt_oldest_due_age_ms_{class}'"
        ))
    };
    let first_due = db.int(
        "SELECT MIN(beltDueAtMs) FROM owed_rows WHERE identity='owner' AND beltDueAtMs IS NOT NULL",
    );
    let report = belt::tick(&db, &unspent()).await.unwrap();
    let now = NOW as i64;
    assert_eq!(report.young_rows, 1, "{report:?}");
    assert_eq!(
        gauge(&db, "second"),
        now,
        "the second class's oldest due age (the age plus one)"
    );
    assert_eq!(gauge(&db, "young"), now, "the young class's");
    assert_eq!(
        gauge(&db, "first"),
        now - first_due + 1,
        "the first class's"
    );
    // the next tick: the row the places cut has waited a tick more
    let next = NOW + belt::OWED_BELT_TICK_MS as u64;
    db.now.set(next);
    let report = belt::tick(&db, &unspent()).await.unwrap();
    assert_eq!(
        gauge(&db, "second"),
        next as i64,
        "the cut row's age grew by the tick: {report:?}"
    );
    if report.young_rows == 0 {
        assert_eq!(gauge(&db, "young"), 0, "no young row read: none");
    }
    // the served view: the age, null for none
    let mut counters = serde_json::Map::new();
    for class in ["first", "second", "young"] {
        counters.insert(
            format!("owed_belt_oldest_due_age_ms_{class}"),
            json!(gauge(&db, class)),
        );
    }
    let ages = (gauge(&db, "second") - 1, gauge(&db, "young"));
    let view = bsv_overlay_cloudflare::applayer_counters::owed_belt_view(&Value::Object(counters));
    assert_eq!(view["oldestDueAgeMs"]["second"], json!(ages.0), "{view}");
    if ages.1 == 0 {
        assert_eq!(view["oldestDueAgeMs"]["young"], Value::Null, "{view}");
    }
}

/// FIX-1057 item 5 (round 3's B LOW-3): PHASE C's young cap in `ask_candidates`. Eight young landing asks (`a*`,
/// walked first) beside six that are not young (`z*`), all due: the tick takes four of each (`places(0, 6, 8)`).
/// RED with the cap removed (`room` always true): the eight young asks take every place, `left: (8, 8)`.
#[tokio::test]
async fn fix1057_5_phase_c_young_asks_take_at_most_half_a_tick() {
    let db = PhaseC(Db::new());
    let mut n = 0;
    let mut games = Vec::new();
    for i in 0..8 {
        let o = landing_into(&db.0, &format!("a{i}"), n, "announced", 60, Some(60)).await;
        games.push((format!("a{i}"), game_of(&o, &db.0)));
        n += 1;
    }
    for i in 0..6 {
        let o = landing_into(&db.0, &format!("z{i}"), n, "announced", 60, None).await;
        games.push((format!("z{i}"), game_of(&o, &db.0)));
        n += 1;
    }
    for (identity, game) in &games {
        actor_apply(&db.0, identity, std::slice::from_ref(game)).await;
    }
    db.0.exec(
        "UPDATE owed_belt_state SET tickedAtMs=1 WHERE name>='belt-ask:' AND name<'belt-ask;'",
    );
    assert_eq!(
        db.0.int(
            "SELECT COUNT(*) FROM owed_belt_state WHERE name>='belt-ask:' AND name<'belt-ask;'"
        ),
        14
    );
    let report = belt::tick(&db, &unspent()).await.unwrap();
    assert_eq!(
        (report.candidates, report.young_rows),
        (
            belt::OWED_BELT_OUTPOINTS_PER_TICK,
            belt::OWED_BELT_YOUNG_PER_TICK
        ),
        "four young asks and four that are not young: {report:?}"
    );
}

/// FIX-1057 item 8 (round 3's B NOTE-1): THE WAIT'S ABSOLUTE BOUND, through the shipped judge and belt. A filing
/// taken 5 h 55 min ago whose sweep a source keeps hinting (the last hint a minute old, renewed before the look): the
/// wait states its end at six hours after the filing, the belt looks there, and the press is back although the last
/// hint is a minute old. RED on abf78cb4 (the end the hint's time plus an hour): the row still the wait at the look.
#[tokio::test]
async fn fix1057_8_the_wait_ends_six_hours_after_the_filing_whatever_the_hints() {
    let db = Db::new();
    let outpoint = landing_filed(&db, "owner", 0, "seen", 60, None, 5 * 3_600 + 55 * 60).await;
    let (_, facts, due) = row_of(&db, &outpoint);
    let end = NOW as i64 + 5 * 60_000;
    assert_eq!(facts["sweepLanding"], true, "inside the wait: {facts}");
    assert_eq!(
        facts["sweepLandingUntilMs"],
        json!(end),
        "six hours after the filing"
    );
    assert_eq!(due, Some(end), "the look comes at the bound");
    let hop_txid = outpoint.split(':').next().unwrap().to_string();
    let (sweep, _) = spender_of(&hop_txid);
    // a source keeps speaking of the sweep: the last hint a minute before the look
    db.exec(&format!(
        "UPDATE tx_words SET hint_at={} WHERE txid='{sweep}'",
        end / 1000 - 60
    ));
    db.now.set(end as u64);
    let report = belt::tick(&db, &unspent()).await.unwrap();
    assert_eq!(report.candidates, 1, "{report:?}");
    let (_, facts, _) = row_of(&db, &outpoint);
    assert_eq!(
        (facts["claim"].clone(), facts["claimable"].clone()),
        (json!("sweep-hop"), json!(true)),
        "past the bound the press is back whatever the hints: {facts}"
    );
    assert!(facts["sweepRawHex"].is_string(), "the filed bytes ride it");
}
