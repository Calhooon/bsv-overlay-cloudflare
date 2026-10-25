//! One real-SQLite port for the owed fold's pins: the overlay's migrations, the judge's port, a settable clock and
//! tip, every batch recorded.
use low_app_layer::{
    owed_judge::JudgeDb,
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

pub const NOW: u64 = 1_790_000_000_000;
pub const HOUR: i64 = 3_600_000;

pub struct Db {
    pub conn: RefCell<Connection>,
    pub batches: RefCell<Vec<Vec<Stmt>>>,
    pub counters: RefCell<BTreeMap<String, u64>>,
    pub pushes: RefCell<Vec<Value>>,
    pub now: Cell<u64>,
    pub tip: Cell<Option<u64>>,
    /// Fail every batch whose statements contain this text (a judge fault on demand).
    pub fail_on: RefCell<Option<String>>,
}
impl Db {
    pub fn new() -> Self {
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
            pushes: RefCell::new(Vec::new()),
            now: Cell::new(NOW),
            tip: Cell::new(Some(900_000)),
            fail_on: RefCell::new(None),
        }
    }
    pub fn exec(&self, sql: &str) {
        self.conn.borrow().execute_batch(sql).unwrap();
    }
    pub fn int(&self, sql: &str) -> i64 {
        self.conn.borrow().query_row(sql, [], |r| r.get(0)).unwrap()
    }
    pub fn opt(&self, sql: &str) -> Option<i64> {
        self.conn.borrow().query_row(sql, [], |r| r.get(0)).unwrap()
    }
    pub fn text(&self, sql: &str) -> Option<String> {
        self.conn.borrow().query_row(sql, [], |r| r.get(0)).unwrap()
    }
    /// The query plan's detail lines for `sql` bound with `binds`.
    pub fn plan(&self, sql: &str, binds: &[SqlValue]) -> Vec<String> {
        let conn = self.conn.borrow();
        let mut plan = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        plan.query_map(rusqlite::params_from_iter(binds.iter()), |r| r.get(3))
            .unwrap()
            .map(Result::unwrap)
            .collect()
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
        if let Some(text) = self.fail_on.borrow().as_deref() {
            if stmts.iter().any(|s| s.sql.contains(text)) {
                return Err(format!("injected fault on {text}"));
            }
        }
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
    async fn push(&self, _: &str, body: Value) {
        self.pushes.borrow_mut().push(body);
    }
}

pub fn hop(n: usize) -> String {
    format!("b3{n:062x}")
}

/// An index-unspent stranded hop for `identity`: the hop's P2PKH row two hours old, its verified marker, no JOIN.
pub fn seed_stranded_hop(db: &Db, identity: &str, game: &str, hop_txid: &str) {
    let made = (db.now.get() as i64 - 2 * HOUR) / 1000;
    db.exec(&format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind) VALUES ('{hop_txid}',0,0,{made},'p2pkh'); INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('{identity}','other','{game}',0,20190,'seat','00','00',2,'{hop_txid}',1,{made},1);"));
}

/// Drain every mark through the shipped scheduled drain.
pub async fn drain_all(db: &Db) {
    for _ in 0..10_000 {
        if db.int("SELECT COUNT(*) FROM owed_dirty") == 0 {
            return;
        }
        let r = low_app_layer::owed_judge::drain_dirty(db, 64).await.unwrap();
        assert!(r.faults.is_empty(), "{:?}", r.faults);
        if r.subjects == 0 {
            return;
        }
    }
    panic!("the drain did not converge");
}

/// The production read, then the body at `present_tip` with the stored words' views.
pub async fn served(db: &Db, identity: &str, present_tip: Option<u64>) -> Value {
    let snapshot = low_app_layer::owed_read::read(db, identity).await.unwrap();
    let words: BTreeMap<_, _> = snapshot
        .word_rows()
        .unwrap()
        .into_iter()
        .map(|(id, row)| (id, row.view()))
        .collect();
    snapshot.body(identity, present_tip, &words).unwrap()
}
