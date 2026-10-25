//! The public read must never buy a walk, even for an unbackfilled identity.
#[test]
fn owed_read_has_no_work_or_clock() {
    let source = include_str!("../src/routes.rs");
    let route = source
        .split("pub async fn owed(req:")
        .nth(1)
        .unwrap()
        .split("async fn gather_result_entries(")
        .next()
        .unwrap();
    for forbidden in [
        "Date::now",
        "wait_until",
        "owed_compute",
        "owed_recompute",
        "owed_schedule",
        "owed_shed",
        "owed_first_read",
        "chaintracks_present_height",
        "OWED_RATE",
    ] {
        assert!(
            !route.contains(forbidden),
            "GET /owed must not call {forbidden}"
        );
    }
    assert!(route.contains("crate::owed_read::read"));
    assert!(route.contains("tracker_host::runtime"));
    let entry = include_str!("../src/lib.rs");
    assert!(entry.contains("if stored_owed {\n        d1_ledger::note_deferred_request();"));
    assert!(entry.contains("state.wait = (!stored_owed).then(|| ctx.clone());"));
}

use low_app_layer::{
    owed_read,
    pot_hook::{Bind, SqlPort, Stmt, StmtOut},
};
use rusqlite::{
    types::{Value as SqlValue, ValueRef},
    Connection,
};
use serde_json::{json, Value};
use std::{cell::RefCell, collections::BTreeMap};

struct Db {
    conn: RefCell<Connection>,
    batches: RefCell<Vec<Vec<Stmt>>>,
}
impl Db {
    fn new() -> Self {
        let conn = Connection::open_in_memory().unwrap();
        for sql in bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS {
            if let Err(error) = conn.execute_batch(sql) {
                assert!(bsv_overlay_cloudflare::d1::migration_error_is_benign(
                    sql,
                    &error.to_string()
                ));
            }
        }
        Self {
            conn: RefCell::new(conn),
            batches: RefCell::new(Vec::new()),
        }
    }
    fn exec(&self, sql: &str) {
        self.conn.borrow().execute_batch(sql).unwrap();
    }
}
impl SqlPort for Db {
    async fn run_batch(&self, statements: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        self.batches.borrow_mut().push(statements.clone());
        let mut conn = self.conn.borrow_mut();
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        let mut result = Vec::new();
        for statement in statements {
            let mut query = tx.prepare(&statement.sql).map_err(|e| e.to_string())?;
            let names: Vec<_> = query
                .column_names()
                .into_iter()
                .map(str::to_owned)
                .collect();
            let binds = statement.binds.into_iter().map(|v| match v {
                Bind::Text(s) => SqlValue::Text(s),
                Bind::Int(n) => SqlValue::Integer(n),
                Bind::Null => SqlValue::Null,
            });
            let rows = query
                .query_map(rusqlite::params_from_iter(binds), |row| {
                    let mut value = serde_json::Map::new();
                    for (index, name) in names.iter().enumerate() {
                        let v = match row.get_ref(index)? {
                            ValueRef::Null => Value::Null,
                            ValueRef::Integer(n) => json!(n),
                            ValueRef::Real(n) => json!(n),
                            ValueRef::Text(s) => json!(std::str::from_utf8(s).unwrap()),
                            ValueRef::Blob(_) => panic!("no blobs in this projection"),
                        };
                        value.insert(name.clone(), v);
                    }
                    Ok(Value::Object(value))
                })
                .map_err(|e| e.to_string())?
                .collect::<Result<_, _>>()
                .map_err(|e| e.to_string())?;
            result.push(StmtOut {
                rows,
                changes: Some(tx.changes()),
            });
        }
        tx.commit().map_err(|e| e.to_string())?;
        Ok(result)
    }
    async fn counters(&self, _: Vec<(String, u64)>) {
        panic!("a read must not write counters")
    }
    fn now_ms(&self) -> u64 {
        panic!("a read must not sample a clock")
    }
    fn log(&self, _: &str) {}
}

fn seed(db: &Db) {
    db.exec("INSERT INTO owed_state(identity,computedAtMs,rows,truncated,recomputeSeq,tip) VALUES ('owner',1234,1,0,3,100);
      INSERT INTO owed_rows(identity,outpoint,family,gameId,sats,facts,updatedAtMs,probeTip) VALUES ('owner','pot:0','hop-stranded','game',20,'{\"claim\":\"sweep-hop\",\"claimable\":true,\"refresh\":\"withheld\",\"carried\":true,\"judgedAtMs\":99}',1234,100);
      INSERT INTO owed_row_words(identity,outpoint,role,txid) VALUES ('owner','pot:0','funding','fund'),('owner','pot:0','spender','spent');
      INSERT INTO tx_words(txid,word,tier,updated_at) VALUES ('fund','seen','hint',1),('spent','seen','hint',1);
      DELETE FROM owed_dirty;");
}

#[tokio::test]
async fn owed_read_joins_words_once_and_preserves_stored_claims() {
    let db = Db::new();
    seed(&db);
    db.exec("INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('owner','game','tip','tip',0)");
    let snapshot = owed_read::read(&db, "owner").await.unwrap();
    assert_eq!(db.batches.borrow().len(), 1);
    assert_eq!(db.batches.borrow()[0].len(), 3);
    assert!(snapshot.needs_tip());
    let words = snapshot
        .word_rows()
        .unwrap()
        .into_iter()
        .map(|(id, row)| (id, row.view()))
        .collect();
    let body = snapshot.body("owner", Some(101), &words).unwrap();
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);
    assert_eq!(body["rows"][0]["facts"]["coinWord"]["word"], "pending");
    // the D2-OWED lens's HIGH-1: judged at 100, served at 101, the stored press degrades to a chain-wait
    assert_eq!(body["rows"][0]["facts"]["claimable"], false);
    assert_eq!(body["rows"][0]["facts"]["claim"], Value::Null);
    assert_eq!(body["rows"][0]["facts"]["chainWait"], "block");
    assert_eq!(body["rows"][0]["facts"]["tipMoved"], true);
    assert_eq!(body["rows"][0]["facts"]["servedTip"], 101);
    assert_eq!(body["rows"][0]["facts"]["beltNextAtMs"], Value::Null);
    for key in ["refresh", "carried", "judgedAtMs"] {
        assert!(body["rows"][0]["facts"].get(key).is_none());
    }
    assert_eq!(body["pending"], 1);
    assert_eq!(body["retryAfterMs"], 1500);
    assert_eq!(body["markerlessProgress"], Value::Null);
}

#[tokio::test]
async fn owed_read_unbackfilled_writes_one_wanted_mark_and_unknown_writes_nothing() {
    let db = Db::new();
    db.exec("INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt) VALUES ('owner','other','game','pot',0,1,'00','marker',0,1)");
    let snapshot = owed_read::read(&db, "owner").await.unwrap();
    let body = snapshot.body("owner", None, &BTreeMap::new()).unwrap();
    assert_eq!(body["pending"], true);
    assert_eq!(body["rows"], json!([]));
    assert_eq!(body["retryAfterMs"], 1500);
    assert_eq!(
        db.batches.borrow().iter().map(Vec::len).collect::<Vec<_>>(),
        [3, 1, 1]
    );
    {
        let conn = db.conn.borrow();
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM owed_dirty WHERE source='wanted' AND gameId=''",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM owed_state", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
    db.batches.borrow_mut().clear();
    let unknown = owed_read::read(&db, "unknown").await.unwrap();
    assert_eq!(
        unknown.body("unknown", None, &BTreeMap::new()).unwrap()["pending"],
        0
    );
    assert_eq!(
        db.batches.borrow().iter().map(Vec::len).collect::<Vec<_>>(),
        [3, 1]
    );
}

#[tokio::test]
async fn owed_read_preserves_a_complete_proof_for_the_served_recheck() {
    let db = Db::new();
    seed(&db);
    db.exec("UPDATE tx_words SET word='mined',tier='chain',height=98,proof_height=98,proof_root='root',proof_hex='proof',header_hash='header' WHERE txid='spent'");
    let snapshot = owed_read::read(&db, "owner").await.unwrap();
    let words = snapshot.word_rows().unwrap();
    assert_eq!(words["spent"].proof_hex.as_deref(), Some("proof"));
    assert_eq!(words["spent"].proof_height, Some(98.0));
    assert_eq!(words["spent"].header_hash.as_deref(), Some("header"));
}

#[tokio::test]
async fn owed_read_projects_the_funding_refusal_without_legacy_transaction_bytes() {
    let db = Db::new();
    seed(&db);
    db.exec("UPDATE tx_words SET legacy_body='{\"trackerTerminal\":true,\"trackerTerminalWord\":\"rejected\",\"rawHex\":\"legacy transaction bytes\"}' WHERE txid='fund'; UPDATE owed_rows SET facts='{\"fundingHeld\":\"refused\",\"claimable\":false}'");
    let snapshot = owed_read::read(&db, "owner").await.unwrap();
    let rows = snapshot.word_rows().unwrap();
    assert!(!rows["fund"]
        .legacy_body
        .as_ref()
        .unwrap()
        .contains("rawHex"));
    let words = rows.into_iter().map(|(id, row)| (id, row.view())).collect();
    let body = snapshot.body("owner", None, &words).unwrap();
    assert_eq!(body["rows"][0]["facts"]["fundingWord"], "rejected");
}

#[tokio::test]
async fn owed_read_body_byte_compatibility_except_named_d2_fields() {
    let db = Db::new();
    seed(&db);
    let snapshot = owed_read::read(&db, "owner").await.unwrap();
    let words: BTreeMap<_, _> = snapshot
        .word_rows()
        .unwrap()
        .into_iter()
        .map(|(id, row)| (id, row.view()))
        .collect();
    let mut body = snapshot.body("owner", Some(100), &words).unwrap();
    for key in ["pending", "markerlessProgress"] {
        body.as_object_mut().unwrap().remove(key);
    }
    body["rows"][0]["facts"]
        .as_object_mut()
        .unwrap()
        .remove("beltNextAtMs");
    let coin = low_app_layer::owed::spender_coin_word(&words["spent"], "spent", None);
    let rows = [low_app_layer::owed::OwedRow {
        identity: "owner".into(),
        outpoint: "pot:0".into(),
        family: low_app_layer::owed::OwedFamily::HopStranded,
        game_id: "game".into(),
        sats: Some(20),
        opponent_identity: None,
        at_height: None,
        facts: json!({"claim":"sweep-hop","claimable":true,"coinWord":coin,"spenderWord":words["spent"]}),
        reason: None,
    }];
    assert_eq!(
        body.to_string(),
        low_app_layer::owed::owed_body("owner", Some(100), &rows, 1234, false, Some(3))
    );
}
