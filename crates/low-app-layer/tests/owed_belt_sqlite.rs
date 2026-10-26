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
