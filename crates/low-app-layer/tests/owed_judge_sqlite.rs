//! The shipped judge, its reads, and its one transaction on real SQLite.
#[allow(dead_code)]
#[path = "support/d1_fleet.rs"]
mod d1_fleet;
use low_app_layer::{
    owed,
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

struct Db {
    conn: RefCell<Connection>,
    reads: RefCell<Vec<(String, usize)>>,
    batches: RefCell<Vec<Vec<Stmt>>>,
    pushes: RefCell<Vec<Value>>,
    now: Cell<u64>,
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
        // Exercise the rest of the judge while the separate migration pin remains red.
        for sql in judge::REQUIRED_GAME_INDEXES {
            conn.execute_batch(sql).unwrap();
        }
        Self {
            conn: RefCell::new(conn),
            reads: RefCell::new(Vec::new()),
            batches: RefCell::new(Vec::new()),
            pushes: RefCell::new(Vec::new()),
            now: Cell::new(1_790_000_000_000),
        }
    }
    fn exec(&self, sql: &str) {
        self.conn.borrow().execute_batch(sql).unwrap();
    }
    fn count(&self, table: &str) -> i64 {
        self.conn
            .borrow()
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    }
    fn seed_game(&self, game: &str, pot: &str) {
        self.conn.borrow().execute("INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt,sigValid) VALUES ('owner','other',?1,?2,0,1,'00',?2,1,1,1)",rusqlite::params![game,pot]).unwrap();
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
            let values = st.binds.iter().map(bind);
            let read = s.readonly();
            let mut rows = s
                .query(rusqlite::params_from_iter(values))
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
            if read {
                self.reads.borrow_mut().push((st.sql.clone(), got.len()));
            }
            out.push(StmtOut {
                rows: got,
                changes: Some(tx.changes()),
            });
        }
        tx.commit().map_err(|e| e.to_string())?;
        Ok(out)
    }
    async fn counters(&self, _: Vec<(String, u64)>) {}
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
    async fn push(&self, _: &str, body: Value) {
        self.pushes.borrow_mut().push(body);
    }
}
fn subject() -> judge::Subject {
    judge::Subject {
        identity: "owner".into(),
        game_id: "game".into(),
    }
}
fn filing() -> Stmt {
    Stmt{sql:"INSERT INTO potrefund_records(identity,gameId,potTxid,potVout,refundRawHex,sigHex,txid,outputIndex,createdAt,refundValid) VALUES ('owner','game','pot',0,'00','00','filed:refund',0,1,1)".into(),binds:vec![]}
}

#[tokio::test]
async fn owed893_filing_row_and_dirty_mark_are_atomic_on_fault_and_success() {
    let db = Db::new();
    db.seed_game("game", "pot");
    db.exec("DELETE FROM owed_dirty; CREATE TRIGGER fail_owed BEFORE INSERT ON owed_rows BEGIN SELECT RAISE(ABORT,'injected row fault'); END;");
    let subjects = [subject()];
    let marks = [];
    let write =
        || judge::judge_with_write(&db, &subjects, "filing", vec![filing()], &marks, |_| Ok(()));
    assert!(write().await.unwrap_err().contains("injected row fault"));
    assert_eq!(
        (
            db.count("potrefund_records"),
            db.count("owed_rows"),
            db.count("owed_dirty")
        ),
        (0, 0, 0)
    );
    assert!(db.pushes.borrow().is_empty());
    db.exec("DROP TRIGGER fail_owed;");
    write().await.unwrap();
    assert_eq!(
        (
            db.count("potrefund_records"),
            db.count("owed_rows"),
            db.count("owed_dirty")
        ),
        (1, 1, 1)
    );
    assert_eq!(db.pushes.borrow().len(), 1);
    let batch = db.batches.borrow();
    let committed = batch.last().unwrap();
    assert!(committed[0]
        .sql
        .starts_with("INSERT INTO potrefund_records"));
}

#[tokio::test]
async fn owed893_a_fault_keeps_existing_marks_and_a_concurrent_remark_survives() {
    let db = Db::new();
    db.seed_game("game", "pot");
    let before = db.count("owed_dirty");
    db.exec("CREATE TRIGGER fail_owed BEFORE INSERT ON owed_rows BEGIN SELECT RAISE(ABORT,'fault'); END;");
    assert!(
        judge::judge_subjects(&db, "owner", &["game".into()], "test")
            .await
            .is_err()
    );
    assert_eq!(db.count("owed_dirty"), before);
    db.exec("DROP TRIGGER fail_owed;");
    judge::judge_with_write(&db, &[subject()], "test", vec![], &[], |_| {
        db.exec("UPDATE potparty_records SET sigValid=1 WHERE identity='owner';");
        Ok(())
    })
    .await
    .unwrap();
    assert!(
        db.count("owed_dirty") > 0,
        "the same source re-mark advanced its generation while hydration ran"
    );
    judge::judge_subjects(&db, "owner", &["game".into()], "test")
        .await
        .unwrap();
    assert_eq!(db.count("owed_dirty"), 0);
}

#[tokio::test]
async fn owed893_wanted_is_one_insert_without_a_read_or_judge() {
    let db = Db::new();
    judge::mark_wanted(&db, "owner").await.unwrap();
    judge::mark_wanted(&db, "owner").await.unwrap();
    assert_eq!(db.count("owed_dirty"), 1);
    assert_eq!(db.count("owed_rows"), 0);
    assert!(db.reads.borrow().is_empty());
    assert!(db.pushes.borrow().is_empty());
    assert!(db
        .batches
        .borrow()
        .iter()
        .all(|b| b.len() == 1 && b[0].sql == judge::MARK_WANTED_SQL));
}

#[tokio::test]
async fn owed893_judges_sixteen_subjects_and_leaves_the_rest_marked() {
    let db = Db::new();
    let mut games = Vec::new();
    for n in 0..20 {
        let g = format!("g{n:02}");
        db.seed_game(&g, &format!("p{n}"));
        games.push(g);
    }
    let r = judge::judge_subjects(&db, "owner", &games, "test")
        .await
        .unwrap();
    assert_eq!((r.subjects, r.deferred), (16, 4));
    assert_eq!(db.count("owed_rows"), 16);
    assert_eq!(db.count("owed_dirty"), 4);
}

#[tokio::test]
async fn owed893_wanted_drains_first_and_expands_to_durable_game_marks() {
    let db = Db::new();
    db.seed_game("game", "pot");
    db.exec("UPDATE owed_dirty SET identity='later';");
    judge::mark_wanted(&db, "owner").await.unwrap();
    let r = judge::drain_dirty(&db, 1).await.unwrap();
    assert!(r.faults.is_empty(), "{:?}", r.faults);
    assert_eq!(r.subjects, 1);
    assert_eq!(db.count("owed_rows"), 1);
    let id: String = db
        .conn
        .borrow()
        .query_row("SELECT identity FROM owed_rows", [], |r| r.get(0))
        .unwrap();
    assert_eq!(id, "owner");
    assert_eq!(db.count("owed_dirty"), 1);
}

#[tokio::test]
async fn owed893_two_games_naming_one_pot_do_not_rewrite_its_representative() {
    let db = Db::new();
    db.seed_game("game", "pot");
    db.conn.borrow().execute("INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt,seatSettlePubkey,seatSigHex,sigValid) SELECT identity,opponentIdentity,'rogue',potTxid,potVout,recoveryHeight,sigHex,'other-marker',outputIndex,createdAt+1,seatSettlePubkey,seatSigHex,sigValid FROM potparty_records",[]).unwrap();
    judge::judge_subjects(&db, "owner", &["game".into()], "test")
        .await
        .unwrap();
    judge::judge_subjects(&db, "owner", &["rogue".into()], "test")
        .await
        .unwrap();
    assert_eq!(db.count("owed_rows"), 1);
    let game: String = db
        .conn
        .borrow()
        .query_row("SELECT gameId FROM owed_rows", [], |r| r.get(0))
        .unwrap();
    assert_eq!(game, "game");
    let context = judge::hydrate_inputs(&db, "owner", &["rogue".into()])
        .await
        .unwrap();
    assert_eq!(context.results[0].game_id, "game");
}

#[tokio::test]
async fn owed893_refund_judges_both_seats_in_the_filing_batch_and_acknowledges_only_its_key() {
    let db = Db::new();
    db.seed_game("game", "pot");
    db.exec("INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt) VALUES ('other','owner','game','pot',0,1,'00','peer',0,1); DELETE FROM owed_dirty;");
    let subjects = judge::refund_subjects(&db, "pot", 0, "owner", "game")
        .await
        .unwrap();
    assert_eq!(subjects.len(), 2);
    let marks = [judge::FilingMark {
        source: "refund".into(),
        source_key: "filed:refund".into(),
    }];
    let r = judge::judge_with_write(&db, &subjects, "filing", vec![filing()], &marks, |inputs| {
        assert_eq!(inputs.len(), 2);
        for i in inputs {
            i.valid_refunds.insert(
                "pot:0".into(),
                owed::ValidRefund {
                    raw_hex: "00".into(),
                    my_sats: Some(20_000),
                },
            );
        }
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(
        (r.subjects, db.count("owed_rows"), db.count("owed_dirty")),
        (2, 2, 0)
    );
    assert_eq!(db.pushes.borrow().len(), 2);
    let batches = db.batches.borrow();
    let batch = batches.last().unwrap();
    assert!(batch[0].sql.starts_with("INSERT INTO potrefund_records"));
    assert_eq!(
        batch
            .iter()
            .filter(|s| s.sql.starts_with("INSERT INTO owed_state"))
            .count(),
        2
    );
}

#[tokio::test]
async fn owed893_stored_ceiling_preserves_actionable_rows_and_records_truncation() {
    let db = Db::new();
    db.exec("WITH RECURSIVE n(x) AS (SELECT 0 UNION ALL SELECT x+1 FROM n WHERE x<499) INSERT INTO owed_rows(identity,outpoint,family,gameId,facts,updatedAtMs) SELECT 'owner','noise'||x,'unbound','noise','{}',0 FROM n;");
    db.seed_game("game", "pot");
    judge::judge_with_write(&db, &[subject()], "test", vec![], &[], |inputs| {
        inputs[0].results[0].my_seat = Some(low_app_layer::results::SeatLetter::A);
        inputs[0].results[0].stake_sats = Some(20_000);
        inputs[0].results[0].spent = Some(false);
        inputs[0].results[0].cov_recovery_height = Some(1_000_000);
        inputs[0].results[0].recovery_height = 1_000_000;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(db.count("owed_rows"), 500);
    assert_eq!(
        db.conn
            .borrow()
            .query_row(
                "SELECT family FROM owed_rows WHERE outpoint='pot:0'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "in-progress"
    );
    assert_eq!(
        db.conn
            .borrow()
            .query_row(
                "SELECT truncated FROM owed_state WHERE identity='owner'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    let (count, cap): (i64, i64) = db
        .conn
        .borrow()
        .query_row(
            "SELECT rows,500 FROM owed_state WHERE identity='owner'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(count, cap);
}

#[tokio::test]
async fn owed893_fleet_one_subject_hydration_reads_47_rows_not_the_250_result_270_hop_history() {
    let db = Db::new();
    for sql in d1_fleet::owed_rows(1_790_000_000) {
        d1_fleet::execute_row(&db.conn.borrow(), &sql);
    }
    let inputs = judge::hydrate_inputs(&db, d1_fleet::OWED_SEAT, &[format!("a1{:062x}", 0)])
        .await
        .unwrap();
    assert_eq!((inputs.results.len(), inputs.hops.len()), (1, 1));
    judge::assert_decomposition(&inputs.as_inputs());
    let reads_before = db.reads.borrow().clone();
    // A second 250/270-game history under another identity cannot change these inputs or their read plans.
    for sql in d1_fleet::owed_rows(1_790_000_000) {
        let sql = sql
            .replace("'a1", "'b1")
            .replace("'a2", "'b2")
            .replace("'a3", "'b3")
            .replace(d1_fleet::OWED_SEAT, "noise");
        d1_fleet::execute_row(&db.conn.borrow(), &sql);
    }
    db.reads.borrow_mut().clear();
    db.batches.borrow_mut().clear();
    let after = judge::hydrate_inputs(&db, d1_fleet::OWED_SEAT, &[format!("a1{:062x}", 0)])
        .await
        .unwrap();
    assert_eq!(inputs.results, after.results);
    assert_eq!(inputs.hops, after.hops);
    assert_eq!(*db.reads.borrow(), reads_before);
    for (sql, _) in db.reads.borrow().iter() {
        let conn = db.conn.borrow();
        let mut st = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        let details: Vec<String> = st
            .query_map(
                rusqlite::params_from_iter(std::iter::repeat_n(
                    SqlValue::Null,
                    st.parameter_count(),
                )),
                |r| r.get(3),
            )
            .unwrap()
            .map(Result::unwrap)
            .collect();
        if sql == judge::PARTIES_SQL {
            assert!(
                details
                    .iter()
                    .any(|d| d.contains("idx_potparty_game_identity") && d.contains("gameId=?")),
                "the party read must be bounded by game: {details:?}"
            );
        }
        assert!(
            !details.iter().any(|s| s.starts_with("SCAN ")
                && !s.contains("VIRTUAL TABLE")
                && !s.contains("CONSTANT")
                && !s.contains("subquery")
                && !s.contains("CO-ROUTINE")),
            "unbounded plan: {details:?}: {sql}"
        );
    }
    if let Ok(root) = std::env::var("OWED_JUDGE_CAPTURE") {
        let conn = db.conn.borrow();
        let mut q=conn.prepare("SELECT sql FROM sqlite_schema WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'index' THEN 1 ELSE 2 END,rowid").unwrap();
        let schema = q
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>()
            .join(";\n");
        std::fs::write(format!("{root}/schema.sql"), format!("{schema};\n")).unwrap();
        std::fs::write(
            format!("{root}/seed.sql"),
            d1_fleet::owed_rows(1_790_000_000)
                .into_iter()
                .map(|s| format!("{s};\n"))
                .collect::<String>(),
        )
        .unwrap();
        let queries:Vec<_>=db.batches.borrow().iter().flatten().map(|s|json!({"sql":s.sql,"binds":s.binds.iter().map(|b|match b{Bind::Text(s)=>json!(s),Bind::Int(n)=>json!(n),Bind::Null=>Value::Null}).collect::<Vec<_>>()})).collect();
        std::fs::write(
            format!("{root}/reads.json"),
            serde_json::to_string_pretty(&queries).unwrap(),
        )
        .unwrap();
    }
}

#[tokio::test]
async fn owed893_judge_readback_equals_the_walk_through_both_read_statements() {
    let db = Db::new();
    db.seed_game("game", "pot");
    let i = judge::hydrate_inputs(&db, "owner", &["game".into()])
        .await
        .unwrap();
    let mut expected = owed::derive_owed_rows(&i.as_inputs());
    expected.iter_mut().for_each(judge::strip_words);
    judge::judge_subjects(&db, "owner", &["game".into()], "test")
        .await
        .unwrap();
    let joined = db
        .run_batch(vec![Stmt {
            sql: judge::ROWS_READ_SQL.into(),
            binds: vec![Bind::Text("owner".into())],
        }])
        .await
        .unwrap()
        .remove(0)
        .rows;
    let normalize = |rows: Vec<Value>| {
        let mut set = std::collections::BTreeSet::new();
        for r in rows {
            let mut facts: Value = serde_json::from_str(r["facts"].as_str().unwrap()).unwrap();
            for k in [
                "coinWord",
                "spenderWord",
                "fundingWord",
                "tracker",
                "beltNextAtMs",
            ] {
                facts.as_object_mut().unwrap().remove(k);
            }
            set.insert(
                json!([
                    r["identity"],
                    r["outpoint"],
                    r["family"],
                    r["gameId"],
                    r["sats"],
                    r["opponentIdentity"],
                    r["atHeight"],
                    r["reason"],
                    facts
                ])
                .to_string(),
            );
        }
        set
    };
    let joined = normalize(joined);
    let plan = owed::owed_write_plan(
        "owner",
        &expected,
        db.now.get() as i64,
        Some(900_000),
        false,
    )
    .into_iter()
    .map(|(sql, binds)| Stmt {
        sql: sql.into(),
        binds: binds
            .into_iter()
            .map(|b| match b {
                owed::OwedBind::Text(s) => Bind::Text(s),
                owed::OwedBind::Int(n) => Bind::Int(n),
                owed::OwedBind::Null => Bind::Null,
            })
            .collect(),
    })
    .collect();
    db.run_batch(plan).await.unwrap();
    let legacy = db
        .run_batch(vec![Stmt {
            sql: owed::OWED_ROWS_READ_SQL.into(),
            binds: vec![Bind::Text("owner".into())],
        }])
        .await
        .unwrap()
        .remove(0)
        .rows;
    assert_eq!(joined, normalize(legacy));
}

#[test]
fn owed893_judge_game_indexes_are_shipped_by_the_overlay() {
    for sql in judge::REQUIRED_GAME_INDEXES {
        assert!(
            bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS.contains(&sql),
            "missing required game index in overlay migrations: {sql}"
        );
    }
}

#[tokio::test]
async fn owed893_rows_store_query_keys_and_word_references_without_duplicating_words() {
    let db = Db::new();
    let pot = "ab".repeat(32);
    let pay = "cd".repeat(32);
    db.seed_game("game", &pot);
    judge::judge_with_write(&db, &[subject()], "test", vec![], &[], |inputs| {
        let e = &mut inputs[0].results[0];
        e.spent = Some(true);
        e.spent_confirmed = Some(true);
        e.settle_txid = Some(pay.clone());
        e.verdict = Some(low_app_layer::results::PotVerdict::WinnerA);
        e.outcome = low_app_layer::results::Outcome::Won;
        e.my_seat = Some(low_app_layer::results::SeatLetter::A);
        e.at_height = Some(899_999);
        Ok(())
    })
    .await
    .unwrap();
    let conn = db.conn.borrow();
    let (raw, stamp, claim): (String, i64, i64) = conn
        .query_row(
            "SELECT facts,judgedAtMs,claimable FROM owed_rows",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    let facts: Value = serde_json::from_str(&raw).unwrap();
    for key in ["coinWord", "spenderWord", "fundingWord", "tracker"] {
        assert!(facts.get(key).is_none());
    }
    assert_eq!(stamp, db.now.get() as i64);
    assert_eq!(claim, 1);
    let mut q = conn
        .prepare("SELECT role,txid FROM owed_row_words ORDER BY role")
        .unwrap();
    let words: Vec<(String, String)> = q
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(
        words,
        vec![
            ("funding".into(), pot),
            ("pay".into(), pay.clone()),
            ("settle".into(), pay.clone()),
            ("spender".into(), pay)
        ]
    );
}
