//! bsv-low #893, the D2-OWED lens's MEDIUM-3: the OLD owed walk's own text (the `owed_walk_*` regions of `routes.rs`,
//! its cursor, its markerless ring, its probes, its write) expanded a second time, here, over SQLite, a test clock and
//! couriers that never answer, so a pin runs it beside the judge on the SAME data and compares the rows each stores.
//!
//! Every name below shadows the one the regions resolve in `routes.rs` (`WalkDb` is `worker::D1Database` there): the
//! regions' text is the walk's, untouched; only the platform under it differs. What the shims cannot be: the D1 binding's
//! value conversions (a whole JS number binds INTEGER here, as D1's integer affinity stores it), the header recheck of a
//! word (`walk_views` reads each row's own view, as the judge's test port does), the read-side reorg guard of
//! `load_stored_beef` (the stored bytes are read as the judge reads them), and the couriers (none answers).
#![allow(dead_code, unused_imports, unused_macros, clippy::all)]
use super::*;
use rusqlite::types::{Value as SqlValue, ValueRef};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;

thread_local! {
    static NOW_MS: Cell<u64> = const { Cell::new(1_790_000_000_000) };
    pub(crate) static LOG: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}
pub(crate) fn set_now(ms: u64) {
    NOW_MS.with(|n| n.set(ms));
}

macro_rules! console_log {
    ($($t:tt)*) => { LOG.with(|l| l.borrow_mut().push(format!($($t)*))) };
}
macro_rules! console_warn {
    ($($t:tt)*) => { LOG.with(|l| l.borrow_mut().push(format!($($t)*))) };
}
macro_rules! console_error {
    ($($t:tt)*) => { LOG.with(|l| l.borrow_mut().push(format!($($t)*))) };
}

/// The bind values the regions build (`JsValue::from_str`, `from_f64`, ...).
#[derive(Debug, Clone)]
pub(crate) enum JsValue {
    Text(String),
    Num(f64),
    Bool(bool),
    Null,
}
impl JsValue {
    pub const NULL: JsValue = JsValue::Null;
    pub const UNDEFINED: JsValue = JsValue::Null;
    pub fn from_str(s: &str) -> Self {
        JsValue::Text(s.to_owned())
    }
    pub fn from_f64(n: f64) -> Self {
        JsValue::Num(n)
    }
    pub fn from_bool(b: bool) -> Self {
        JsValue::Bool(b)
    }
    pub fn null() -> Self {
        JsValue::Null
    }
    fn sql(&self) -> SqlValue {
        match self {
            JsValue::Text(s) => SqlValue::Text(s.clone()),
            JsValue::Num(n) if n.fract() == 0.0 && n.abs() < 9.0e15 => SqlValue::Integer(*n as i64),
            JsValue::Num(n) => SqlValue::Real(*n),
            JsValue::Bool(b) => SqlValue::Integer(i64::from(*b)),
            JsValue::Null => SqlValue::Null,
        }
    }
}
impl From<&str> for JsValue {
    fn from(s: &str) -> Self {
        JsValue::from_str(s)
    }
}
impl From<String> for JsValue {
    fn from(s: String) -> Self {
        JsValue::Text(s)
    }
}
impl From<f64> for JsValue {
    fn from(n: f64) -> Self {
        JsValue::Num(n)
    }
}

/// `worker::D1Database` over one SQLite connection.
pub(crate) struct WalkDb {
    pub conn: RefCell<rusqlite::Connection>,
}
impl WalkDb {
    pub fn prepare(&self, sql: impl Into<String>) -> WalkStmt<'_> {
        WalkStmt {
            db: self,
            sql: sql.into(),
            binds: Vec::new(),
        }
    }
    fn run(&self, sql: &str, binds: &[JsValue]) -> worker::Result<WalkResult> {
        let conn = self.conn.borrow();
        let mut q = conn
            .prepare(sql)
            .map_err(|e| worker::Error::RustError(format!("{e}: {sql}")))?;
        let names: Vec<String> = q.column_names().iter().map(|c| c.to_string()).collect();
        let mut rows = Vec::new();
        let mut it = q
            .query(rusqlite::params_from_iter(binds.iter().map(JsValue::sql)))
            .map_err(|e| worker::Error::RustError(e.to_string()))?;
        while let Some(r) = it
            .next()
            .map_err(|e| worker::Error::RustError(e.to_string()))?
        {
            let mut m = serde_json::Map::new();
            for (k, c) in names.iter().enumerate() {
                let v = match r.get_ref(k).map_err(|e| worker::Error::RustError(e.to_string()))? {
                    ValueRef::Null => serde_json::Value::Null,
                    ValueRef::Integer(n) => serde_json::json!(n),
                    ValueRef::Real(f) => serde_json::json!(f),
                    ValueRef::Text(t) => serde_json::json!(String::from_utf8_lossy(t)),
                    ValueRef::Blob(b) => serde_json::json!(b),
                };
                m.insert(c.clone(), v);
            }
            rows.push(serde_json::Value::Object(m));
        }
        Ok(WalkResult { rows })
    }
}

/// `worker::D1PreparedStatement` and the counted reads (`d1_ledger::Counted`).
#[derive(Clone)]
pub(crate) struct WalkStmt<'a> {
    db: &'a WalkDb,
    sql: String,
    binds: Vec<JsValue>,
}
impl<'a> WalkStmt<'a> {
    pub fn bind(mut self, values: &[JsValue]) -> worker::Result<Self> {
        self.binds = values.to_vec();
        Ok(self)
    }
    pub async fn counted_all(&self) -> worker::Result<WalkResult> {
        self.db.run(&self.sql, &self.binds)
    }
    pub async fn counted_run(&self) -> worker::Result<WalkResult> {
        self.db.run(&self.sql, &self.binds)
    }
    pub async fn counted_first<T: serde::de::DeserializeOwned>(&self) -> worker::Result<Option<T>> {
        let r = self.db.run(&self.sql, &self.binds)?;
        match r.rows.into_iter().next() {
            Some(row) => Ok(Some(
                serde_json::from_value(row).map_err(|e| worker::Error::RustError(e.to_string()))?,
            )),
            None => Ok(None),
        }
    }
    pub async fn all(&self) -> worker::Result<WalkResult> {
        self.db.run(&self.sql, &self.binds)
    }
}

/// `worker::D1Result`.
pub(crate) struct WalkResult {
    rows: Vec<serde_json::Value>,
}
impl WalkResult {
    pub fn results<T: serde::de::DeserializeOwned>(&self) -> worker::Result<Vec<T>> {
        self.rows
            .iter()
            .map(|r| {
                serde_json::from_value(r.clone()).map_err(|e| worker::Error::RustError(e.to_string()))
            })
            .collect()
    }
}

/// `d1_ledger::counted_batch`: one transaction, all or nothing.
pub(crate) async fn walk_counted_batch(
    db: &WalkDb,
    stmts: Vec<WalkStmt<'_>>,
) -> worker::Result<Vec<WalkResult>> {
    db.conn
        .borrow()
        .execute_batch("BEGIN")
        .map_err(|e| worker::Error::RustError(e.to_string()))?;
    let mut out = Vec::new();
    for s in &stmts {
        match db.run(&s.sql, &s.binds) {
            Ok(r) => out.push(r),
            Err(e) => {
                let _ = db.conn.borrow().execute_batch("ROLLBACK");
                return Err(e);
            }
        }
    }
    db.conn
        .borrow()
        .execute_batch("COMMIT")
        .map_err(|e| worker::Error::RustError(e.to_string()))?;
    Ok(out)
}

/// `worker::Env`: what the walk asks of the platform, beside D1.
pub(crate) struct WalkEnv {
    pub db: std::rc::Rc<WalkDb>,
    pub tip: Option<u64>,
    pub era_ms: Option<i64>,
}

/// `worker::Date`.
pub(crate) struct WalkDate(u64);
impl WalkDate {
    pub fn now() -> Self {
        WalkDate(NOW_MS.with(|n| n.get()))
    }
    pub fn as_millis(&self) -> u64 {
        self.0
    }
}

pub(crate) async fn walk_canonical_root(_: &WalkEnv, _: u64) -> Option<String> {
    None
}
pub(crate) async fn walk_read_soft(
    env: &WalkEnv,
    ids: &[String],
) -> worker::Result<Option<BTreeMap<String, crate::tracker_host::stored::Row>>> {
    if ids.is_empty() {
        return Ok(Some(BTreeMap::new()));
    }
    let r = env.db.run(
        crate::tracker_host::stored::READ,
        &[JsValue::Text(serde_json::to_string(ids).unwrap())],
    )?;
    let rows: Vec<crate::tracker_host::stored::Row> = r.results()?;
    Ok(Some(rows.into_iter().map(|r| (r.txid.clone(), r)).collect()))
}
pub(crate) async fn walk_views(
    _: &WalkEnv,
    rows: &BTreeMap<String, crate::tracker_host::stored::Row>,
) -> BTreeMap<String, serde_json::Value> {
    rows.iter().map(|(k, v)| (k.clone(), v.view())).collect()
}
pub(crate) fn written_off_before_ms_env(env: &WalkEnv) -> Option<i64> {
    env.era_ms
}
pub(crate) async fn chaintracks_present_height_env(
    env: &WalkEnv,
    _tag: &str,
) -> std::result::Result<u64, (&'static str, u16)> {
    env.tip.ok_or(("no tip", 503))
}
/// The stored bytes as the judge reads them (`pot_beefs`, then `transactions`).
pub(crate) async fn load_stored_beef(
    _env: &WalkEnv,
    db: &WalkDb,
    txid: &str,
) -> std::result::Result<Option<Vec<u8>>, LoadFault> {
    let r = db
        .run(
            "SELECT hex(beef) AS beef FROM pot_beefs WHERE txid=?1 UNION ALL SELECT hex(beef) AS beef FROM transactions WHERE txid=?1",
            &[JsValue::from_str(&txid.to_ascii_lowercase())],
        )
        .map_err(|_| LoadFault::Db)?;
    Ok(r.rows
        .iter()
        .filter_map(|r| r["beef"].as_str().and_then(|h| hex::decode(h).ok()))
        .find(|b| !b.is_empty()))
}
/// No courier answers natively: every outpoint is "could not look".
async fn spent_any_resolve_cached(
    _txid_lc: &str,
    _vout: u32,
    _caller: &'static str,
    _max_age_ms: f64,
) -> SpentAnyCached {
    SpentAnyCached {
        known: false,
        spent: None,
        spending_txid: None,
        spent_confirmed: None,
        reason: Some("provider-fault"),
    }
}
async fn resolve_tx_any(
    _index_raw: Option<String>,
    _index_height: Option<u64>,
    _key: &str,
    _now: f64,
    _caller: &'static str,
    _env: &WalkEnv,
) -> TxAnyCached {
    TxAnyCached::default()
}
fn tx_any_cached(_key: &str, _now: f64) -> Option<TxAnyCached> {
    None
}

owed_walk_party_cursor!();
owed_walk_compute!();
owed_walk_home!();
owed_walk_results!();
owed_walk_memos!();
owed_walk_spender_store!();

/// The old walk, run once for `identity` at `tip` over `db` as the scheduled walk would (an unclaimed slice of a fresh
/// invocation), its rows written by its own batch.
pub(crate) async fn run_old_walk(
    db: std::rc::Rc<WalkDb>,
    identity: &str,
    tip: Option<u64>,
    era_ms: Option<i64>,
    now_ms: u64,
) -> std::result::Result<OwedComputed, String> {
    set_now(now_ms);
    let env = WalkEnv {
        db: db.clone(),
        tip,
        era_ms,
    };
    let now = now_ms as i64;
    let walk = OwedWalk {
        rid: "rnative".into(),
        slice: crate::owed::OwedWalkSlice {
            started_ms: now,
            outward_deadline_ms: now + crate::owed::OWED_RECOMPUTE_TIME_BUDGET_MS,
            hard_deadline_ms: now + crate::owed::OWED_WALK_HARD_DEADLINE_MS,
        },
    };
    owed_recompute_at_tip(&env, &db, identity, "native", tip, &walk).await
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod pins {
    use super::*;
    use crate::pot_hook::tests::SqliteSql;

    pub(super) const NOW_MS: u64 = 1_790_000_000_000;
    pub(super) const TIP: u64 = 900_000;

    fn migrated() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        for sql in bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                assert!(
                    bsv_overlay_cloudflare::d1::migration_error_is_benign(sql, &e.to_string()),
                    "{sql}: {e}"
                );
            }
        }
        conn
    }

    /// The stored rows of `identity`, the word facts and the write-path stamps stripped (the D2-OWED lens's
    /// MEDIUM-3: "the stored rows compared byte for byte, the word facts stripped").
    pub(super) fn stored(conn: &rusqlite::Connection, identity: &str) -> BTreeMap<String, String> {
        let mut s = conn
            .prepare("SELECT outpoint,family,gameId,sats,opponentIdentity,atHeight,reason,facts FROM owed_rows WHERE identity=?1")
            .unwrap();
        s.query_map([identity], |r| {
            let mut facts: serde_json::Value =
                serde_json::from_str(&r.get::<_, String>(7)?).unwrap();
            // the word facts (joined at the read), the write-path stamps, and the old markerless RING's own cursor
            // facts (the ring is replaced by the eviction's evidence row, design section 8.1: no per-game form)
            for k in [
                "coinWord",
                "spenderWord",
                "fundingWord",
                "tracker",
                "beltNextAtMs",
                "judgedAtMs",
                "markerlessCandidates",
                "markerlessCursor",
                "markerlessEvidence",
                "markerlessExamined",
                "markerlessJoin",
            ] {
                facts.as_object_mut().unwrap().remove(k);
            }
            Ok((
                r.get::<_, String>(0)?,
                serde_json::json!([
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<i64>>(3)?,
                    r.get::<_, Option<String>>(4)?,
                    r.get::<_, Option<i64>>(5)?,
                    r.get::<_, Option<String>>(6)?,
                    facts
                ])
                .to_string(),
            ))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect()
    }

    mod fleet {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/support/d1_fleet.rs"));
    }

    /// One fixture: its seed statements (run through the overlay's migrations and triggers), the identities compared.
    pub(super) struct Fixture {
        pub name: &'static str,
        pub seed: Vec<String>,
        pub identities: Vec<String>,
    }

    fn h(prefix: &str, n: usize) -> String {
        format!("{prefix}{n:0>width$x}", width = 64 - prefix.len())
    }

    /// A JOIN spending two hops, each paying P2PKH(hash160(its seat key)): (join txid, BEEF, hops, keys).
    fn eviction_join() -> (String, Vec<u8>, Vec<String>, Vec<String>) {
        use bsv_rs::script::LockingScript;
        use bsv_rs::transaction::{Beef, Transaction, TransactionInput, TransactionOutput};
        let keys = vec![format!("02{}", "aa".repeat(32)), format!("03{}", "bb".repeat(32))];
        let mut beef = Beef::new();
        let mut hops = Vec::new();
        for (i, key) in keys.iter().enumerate() {
            let mut tx = Transaction::new();
            tx.inputs.push(TransactionInput::new(format!("{:064x}", i + 7), 0));
            tx.outputs.push(TransactionOutput {
                satoshis: Some(20_190),
                locking_script: LockingScript::from_binary(&overlay_discovery::pot::p2pkh_lock(
                    &bsv_rs::primitives::hash::hash160(&hex::decode(key).unwrap()),
                ))
                .unwrap(),
                change: false,
            });
            hops.push(tx.id());
            beef.merge_raw_tx(tx.to_binary(), None);
        }
        let mut join = Transaction::new();
        for hop in &hops {
            join.inputs.push(TransactionInput::new(hop.clone(), 0));
        }
        join.outputs.push(TransactionOutput {
            satoshis: Some(40_000),
            locking_script: LockingScript::from_hex("51").unwrap(),
            change: false,
        });
        let id = join.id();
        beef.merge_raw_tx(join.to_binary(), None);
        (id, beef.to_binary(), hops, keys)
    }

    pub(super) fn fixtures() -> Vec<Fixture> {
        let now_s = NOW_MS / 1000;
        let made = now_s as i64 - 7_200;
        let mut out = Vec::new();
        // The #638 fleet and the owed judge's lived-in seat (250 pots, 270 hops).
        let mut seed = fleet::rows(now_s);
        seed.extend(fleet::owed_rows(now_s));
        out.push(Fixture {
            name: "d1_fleet",
            seed,
            identities: [fleet::WHALE, fleet::OWED_SEAT]
                .into_iter()
                .map(str::to_owned)
                .chain([0u32, 1, 57, 119].map(|k| format!("02{k:064x}")))
                .collect(),
        });
        // Stranded hops of every arm the belt fixtures seed: index-unspent, an unverified marker, a hop the index
        // holds spent by a transaction it does not hold, by a pot, a door refusal, and a young hop.
        let mut seed = Vec::new();
        for n in 0..6usize {
            let hop = h("b3", n);
            let (spent, spender, valid, at) = match n {
                1 => (0, "NULL".to_string(), 0, made),
                2 => (1, format!("'{}'", h("c4", 2)), 1, made),
                3 => (1, format!("'{}'", h("c5", 3)), 1, made),
                5 => (0, "NULL".to_string(), 1, now_s as i64 - 60),
                _ => (0, "NULL".to_string(), 1, made),
            };
            seed.push(format!("INSERT INTO pot_records(txid,outputIndex,spent,spendingTxid,createdAt,lockKind) VALUES ('{hop}',0,{spent},{spender},{at},'p2pkh')"));
            seed.push(format!("INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('owner','other','{}',0,20190,'seat','00','00',2,'{hop}',1,{at},{valid})", h("9a", n)));
        }
        seed.push(format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind,paramsDecoded,recoveryHeight) VALUES ('{}',0,0,{made},'covenant',1,900100)", h("c5", 3)));
        seed.push(format!("INSERT INTO submit_refusals(hopTxid,hopVout,refusedTxid,reason,refusedAt) VALUES ('{}',0,'{}','script-refused',{})", h("b3", 4), h("c6", 4), NOW_MS - 60_000));
        out.push(Fixture {
            name: "stranded hops",
            seed,
            identities: vec!["owner".into()],
        });
        // Two games naming one pot (the representative rule), and a released hop of an eviction.
        let pot = h("d7", 1);
        let mut seed = vec![
            format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind,paramsDecoded,recoveryHeight) VALUES ('{pot}',0,0,{made},'covenant',1,900100)"),
            format!("INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt,sigValid) VALUES ('owner','other','{}','{pot}',0,900100,'00','{}',1,{made},1)", h("9b", 1), h("e8", 1)),
            format!("INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt,sigValid) VALUES ('owner','other','{}','{pot}',0,900100,'00','{}',1,{},0)", h("9b", 2), h("e8", 2), made + 10),
        ];
        // The eviction's post-state, as `eviction_write::evict` leaves it: the JOIN's twins (its covenant row with
        // its decoded keys, both seats' verified party rows), the hops' pointers released, the release list, the
        // JOIN's stored BEEF (each hop paying hash160 of its seat key), and the evidence row the judge reads.
        let (join, beef, hops, keys) = eviction_join();
        let evicted_at = NOW_MS - 3_600_000;
        let released: Vec<String> = hops
            .iter()
            .map(|h| format!("{{\"table\":\"pot_records\",\"txid\":\"{h}\",\"vout\":0}}"))
            .collect();
        seed.push(format!("INSERT INTO pot_records_evicted(txid,outputIndex,spent,createdAt,lockKind,paramsDecoded,recoveryHeight,pubA,pubB,payPkhA,payPkhB) VALUES ('{join}',0,0,{made},'covenant',1,900100,'{}','{}','{}','{}')", keys[0], keys[1], "dd".repeat(20), "ee".repeat(20)));
        for (n, key) in keys.iter().enumerate() {
            let who = if n == 0 { "owner" } else { "other" };
            seed.push(format!("INSERT INTO potparty_records_evicted(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt,sigValid,seatSettlePubkey) VALUES ('{who}','x','{}','{join}',0,900100,'00','{}',1,{made},1,'{key}')", h("9b", 3), h("e9", n)));
            seed.push(format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind) VALUES ('{}',0,0,{made},'p2pkh')", hops[n]));
        }
        seed.push(format!("INSERT INTO pot_beefs(txid,beef,createdAt) VALUES ('{join}',x'{}',{made})", hex::encode(&beef)));
        seed.push(format!("INSERT INTO pot_evictions(txid,evictedAt,reason,releasedSpends) VALUES('{join}',{evicted_at},'fixture','[{}]')", released.join(",")));
        seed.push(format!("INSERT INTO released_hop_evidence(hopTxid,hopVout,identity,gameId,potTxid,seatSettlePubkey,hopSats,evictedAt) VALUES('{}',0,'owner','{}','{join}','{}',20190,{evicted_at})", hops[0], h("9b", 3), keys[0]));
        out.push(Fixture {
            name: "representatives and a released hop",
            seed,
            identities: vec!["owner".into()],
        });
        // One hop output named by two of its author's markers under two games (the lens's MEDIUM-3 divergence).
        let hop = h("b5", 1);
        out.push(Fixture {
            name: "a hop under two games",
            seed: vec![
                format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind) VALUES ('{hop}',0,0,{made},'p2pkh')"),
                format!("INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('owner','other','{}',0,20190,'seat','00','00',3,'{hop}',1,{made},1)", h("9c", 1)),
                format!("INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('owner','other','{}',0,20190,'seat','00','00',3,'{hop}',2,{},1)", h("9c", 2), made + 10),
            ],
            identities: vec!["owner".into()],
        });
        out
    }

    /// The fixture seeded on the judge's side, then copied whole to the walk's side: the SAME D1 data.
    pub(super) fn seeded(f: &Fixture) -> (SqliteSql, std::rc::Rc<WalkDb>) {
        let judge = SqliteSql::migrated();
        judge.now.set(NOW_MS);
        judge.tip.set(Some(TIP));
        judge.conn.execute_batch("BEGIN").unwrap();
        for row in &f.seed {
            if f.name == "d1_fleet" {
                fleet::execute_row(&judge.conn, row);
            } else {
                judge.conn.execute_batch(row).unwrap();
            }
        }
        judge.conn.execute_batch("COMMIT").unwrap();
        let path = std::env::temp_dir().join(format!(
            "d2fold-medium3-{}-{}.sqlite",
            std::process::id(),
            f.name.replace(' ', "-")
        ));
        let _ = std::fs::remove_file(&path);
        judge
            .conn
            .execute("VACUUM INTO ?1", [path.to_str().unwrap()])
            .unwrap();
        // an unlinked file cannot hold its journal: the copy is removed at the next run's start
        let walk = rusqlite::Connection::open(&path).unwrap();
        (
            judge,
            std::rc::Rc::new(WalkDb {
                conn: RefCell::new(walk),
            }),
        )
    }

    /// Every game of `identity`, judged a batch of 16 at a time (the backfill's shape).
    pub(super) async fn judge_identity(judge: &SqliteSql, identity: &str) {
        let games: Vec<String> = judge
            .conn
            .prepare("SELECT gameId FROM potparty_records WHERE identity=?1 UNION SELECT gameId FROM hopparty_records WHERE identity=?1 UNION SELECT gameId FROM released_hop_evidence WHERE identity=?1")
            .unwrap()
            .query_map([identity], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        for chunk in games.chunks(crate::owed_judge::OWED_JUDGE_SUBJECTS_PER_INVOCATION) {
            crate::owed_judge::judge_subjects(judge, identity, chunk, "walk-pin")
                .await
                .unwrap();
        }
    }

    /// The D2-OWED lens's MEDIUM-3: the OLD walk (`owed_recompute_at_tip`, its cursor, ring, probes and write) and the
    /// judge over the same D1 data store the same rows, byte for byte, the word facts stripped. A walk its own page
    /// bound cut serves a subset: each row it stored is the judge's, byte for byte.
    ///
    /// The walk's text IS main's (the delta lens's MEDIUM-3 gap): `python3 tools/lane-893/old_walk_text_diff.py
    /// 8e5e73f7`, run from the repository root, diffs the six `owed_walk_*` regions of `routes.rs` against main
    /// `8e5e73f7`'s `routes.rs` line for line: 3,282 lines, each region the length of its span on main (3392..3460,
    /// 3531..5834, 5836..6016, 7445..7959, 8468..8649, 10053..10083), 53 alias edits (`WalkDb`, `WalkDate`, `WalkEnv`,
    /// `walk_read_soft`, `walk_views`, `walk_canonical_root`, `walk_counted_batch`, `console_log!` unqualified) and the
    /// retired-walk `#[allow(dead_code)]` marks, and NO logic line differs (it exits 1 on one; a one-word change inside
    /// a region is caught). The write path is compared too: `d2delta_medium3_a_filing_judged_by_the_write_path_*`
    /// enters a filing through `record_post` and compares the rows its `judge_with_write` stored with the walk's.
    ///
    /// The word facts (`coinWord`, `spenderWord`, `fundingWord`, `tracker`) are stripped on purpose, NOT a gap: a
    /// stored word fact is never served. The read removes the stored copies and joins the words in their place,
    /// identically for every row (`owed_read::Snapshot::body`, over `owed_row_words` and `tx_words`), and a row's word
    /// roles are the txids of its own facts (`settleTxid`, `spendingTxid`, `payTxid`, `chainSpender`, `sweepTxid`, the
    /// outpoint's funding: `owed_judge::row_statements`), which ARE compared here; so what is served is the same
    /// function of the same word rows for both sides.
    #[tokio::test]
    async fn d2fold_medium3_the_old_walk_and_the_judge_store_the_same_rows() {
        let mut compared = 0;
        let mut diffs = Vec::new();
        for f in fixtures() {
            let (judge, walk_db) = seeded(&f);
            for id in &f.identities {
                let walked = run_old_walk(walk_db.clone(), id, Some(TIP), None, NOW_MS)
                    .await
                    .unwrap_or_else(|e| panic!("{}: {id}: the walk faulted: {e}", f.name));
                if f.name == "d1_fleet" {
                    judge_identity(&judge, id).await;
                } else {
                    // the shipped drain, one subject a judge, as every source write's marks are judged
                    for _ in 0..100 {
                        let r = crate::owed_judge::drain_dirty(&judge, 64).await.unwrap();
                        assert!(r.faults.is_empty(), "{:?}", r.faults);
                        if r.subjects == 0 {
                            break;
                        }
                    }
                }
                let w = stored(&walk_db.conn.borrow(), id);
                let j = stored(&judge.conn, id);
                assert_eq!(w.len(), walked.rows.len().min(crate::owed::OWED_MAX_ROWS));
                // A walk its page bound cut, or a list past the stored ceiling, keeps a different subset by design
                // (the walk's cursor pages against the judge's ranked 500): the rows both hold are compared.
                let cut = walked.truncated || walked.budget_cut || j.len() >= crate::owed::OWED_MAX_ROWS;
                for (outpoint, row) in &w {
                    if cut && !j.contains_key(outpoint) {
                        continue;
                    }
                    compared += 1;
                    if j.get(outpoint) != Some(row) {
                        diffs.push(format!(
                            "{} {id} {outpoint}\n  walk  {row}\n  judge {:?}",
                            f.name,
                            j.get(outpoint)
                        ));
                    }
                }
                if !cut {
                    for outpoint in j.keys().filter(|o| !w.contains_key(*o)) {
                        diffs.push(format!(
                            "{} {id} {outpoint}: the judge stores a row the walk does not: {}",
                            f.name, j[outpoint]
                        ));
                    }
                }
                eprintln!(
                    "{}: {id}: walk {} rows{}, judge {} rows",
                    f.name,
                    w.len(),
                    if cut { " (cut)" } else { "" },
                    j.len()
                );
            }
        }
        assert!(diffs.is_empty(), "{} of {compared} rows differ:\n{}", diffs.len(), diffs.join("\n"));
        assert!(compared > 0);
    }

    /// The delta lens's MEDIUM-3 gap (the fixtures above enter by SQL and are judged by the shipped drain): a filing
    /// that enters through `record_post` (a signed potrefund marker for a seat of an indexed pot, through
    /// `verify_record_post` and `file_verified`) is judged by the WRITE path (`judge_with_write`, the filing's source
    /// statements and the judge in one batch), with no drain after it; the old walk over the same D1 data stores the
    /// same rows, byte for byte, the word facts stripped.
    #[tokio::test]
    async fn d2delta_medium3_a_filing_judged_by_the_write_path_stores_the_walks_rows() {
        use crate::record_post::{self, tests as rp, DoorOutcome, RecordKind};
        let judge = SqliteSql::migrated();
        judge.now.set(NOW_MS);
        judge.tip.set(Some(TIP));
        let victim = rp::wallet(0x41);
        let id = hex::encode(rp::identity(&victim));
        let game = [0x7a; 32];
        rp::md_index_the_pot(&judge);
        rp::md_seat(&judge, &id, &game);
        judge.conn.execute_batch("DELETE FROM owed_dirty").unwrap();
        let script = rp::potrefund_script(&victim, &game, &rp::presigned_refund());
        let mut filing = record_post::verify_record_post(RecordKind::Potrefund, &script, &id).unwrap();
        let out = record_post::file_verified(&judge, &mut filing, &id, (NOW_MS / 1000) as i64)
            .await
            .unwrap();
        assert!(matches!(out, DoorOutcome::Written { row_written: true }), "{out:?}");
        assert_eq!(judge.counter(crate::owed_judge::COUNTER_FILING_FAULTS), 0, "judged by the write path");
        let pending: i64 = judge
            .conn
            .query_row("SELECT COUNT(*) FROM owed_dirty WHERE identity=?1", [&id], |r| r.get(0))
            .unwrap();
        assert_eq!(pending, 0, "the write path acknowledged the filer's marks: nothing left for a drain");
        let j = stored(&judge.conn, &id);
        assert!(!j.is_empty(), "the filing's judgement stored the seat's rows");
        // the same D1 data on the walk's side, the judge's rows for the identity taken away first
        let path = std::env::temp_dir().join(format!("d2delta-medium3-filing-{}.sqlite", std::process::id()));
        let _ = std::fs::remove_file(&path);
        judge.conn.execute("VACUUM INTO ?1", [path.to_str().unwrap()]).unwrap();
        let walk = rusqlite::Connection::open(&path).unwrap();
        walk.execute_batch(&format!("DELETE FROM owed_row_words WHERE identity='{id}'; DELETE FROM owed_rows WHERE identity='{id}'; DELETE FROM owed_state WHERE identity='{id}';")).unwrap();
        let walk_db = std::rc::Rc::new(WalkDb { conn: RefCell::new(walk) });
        let walked = run_old_walk(walk_db.clone(), &id, Some(TIP), None, NOW_MS).await.unwrap();
        assert!(!walked.truncated && !walked.budget_cut);
        let w = stored(&walk_db.conn.borrow(), &id);
        let _ = std::fs::remove_file(&path);
        eprintln!("filing: walk {w:#?}\njudge {j:#?}");
        assert_eq!(w, j, "the walk and the write path's judgement store the same rows");
    }

    /// The second delta lens's D2-N2: the write-path comparison above covers one row and it is not a press. Here a
    /// PRESS: the seat's own index-UNSPENT stranded hop (its `pot_records` row unspent, its marker verified) and the
    /// seat's pre-signed sweep of it filed through `record_post` (`verify_record_post`, `file_verified`), judged by the
    /// WRITE path with no drain after it: the judge stores the `hop-stranded` row `claim: "sweep-hop"`, `claimable:
    /// true`, and the old walk over the same D1 data stores the same row, byte for byte, the word facts stripped.
    #[tokio::test]
    async fn d2delta2_n2_a_press_filed_through_record_post_judged_by_the_write_path_stores_the_walks_row() {
        use crate::hopsweep::tests as hs;
        use crate::record_post::{self, tests as rp, DoorOutcome, RecordKind};
        let judge = SqliteSql::migrated();
        judge.now.set(NOW_MS);
        judge.tip.set(Some(TIP));
        let w = hs::wallet(0x47);
        let identity = hs::identity(&w);
        let id = hex::encode(&identity);
        let seat = hs::skey(16);
        let made = (NOW_MS as i64 - 2 * 3_600_000) / 1000;
        judge
            .conn
            .execute_batch(&format!(
                "INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind) VALUES ('{}',0,0,{made},'p2pkh')",
                hex::encode(hs::HOP)
            ))
            .unwrap();
        rp::md_index_the_hop(&judge, &hs::hop_ctx(&w, &seat, true), Some(1));
        judge.conn.execute_batch("DELETE FROM owed_dirty").unwrap();
        let raw = hs::sweep_raw(&seat, &seat, 0, hs::HOP_SATS, &hs::DEST);
        let script = rp::md_sweep_script(&identity, &hs::GID, &raw);
        let mut filing = record_post::verify_record_post(RecordKind::Hopsweep, &script, &id).unwrap();
        let out = record_post::file_verified(&judge, &mut filing, &id, (NOW_MS / 1000) as i64)
            .await
            .unwrap();
        assert!(matches!(out, DoorOutcome::Written { row_written: true }), "{out:?}");
        assert_eq!(judge.counter(crate::owed_judge::COUNTER_FILING_FAULTS), 0, "judged by the write path");
        let pending: i64 = judge
            .conn
            .query_row("SELECT COUNT(*) FROM owed_dirty WHERE identity=?1", [&id], |r| r.get(0))
            .unwrap();
        assert_eq!(pending, 0, "the write path acknowledged the filer's marks: nothing left for a drain");
        let j = stored(&judge.conn, &id);
        let press = j
            .values()
            .map(|row| serde_json::from_str::<serde_json::Value>(row).unwrap())
            .find(|row| row[0] == "hop-stranded")
            .unwrap_or_else(|| panic!("no hop-stranded row: {j:#?}"));
        assert_eq!(press[6]["claim"], "sweep-hop", "{press}");
        assert_eq!(press[6]["claimable"], true, "the write path judged a press: {press}");
        assert_eq!(press[6]["status"], "unspent", "{press}");
        let path = std::env::temp_dir().join(format!("d2delta2-n2-press-{}.sqlite", std::process::id()));
        let _ = std::fs::remove_file(&path);
        judge.conn.execute("VACUUM INTO ?1", [path.to_str().unwrap()]).unwrap();
        let walk = rusqlite::Connection::open(&path).unwrap();
        walk.execute_batch(&format!("DELETE FROM owed_row_words WHERE identity='{id}'; DELETE FROM owed_rows WHERE identity='{id}'; DELETE FROM owed_state WHERE identity='{id}';")).unwrap();
        let walk_db = std::rc::Rc::new(WalkDb { conn: RefCell::new(walk) });
        let walked = run_old_walk(walk_db.clone(), &id, Some(TIP), None, NOW_MS).await.unwrap();
        assert!(!walked.truncated && !walked.budget_cut);
        let w = stored(&walk_db.conn.borrow(), &id);
        let _ = std::fs::remove_file(&path);
        eprintln!("press: walk {w:#?}\njudge {j:#?}");
        assert_eq!(w, j, "the walk and the write path's judgement store the same press");
    }

    #[tokio::test]
    async fn d2fold_medium3_smoke_the_old_walk_runs_natively() {
        let made = (NOW_MS as i64 - 2 * 3_600_000) / 1000;
        let seed = format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind) VALUES ('{h}',0,0,{made},'p2pkh'); INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('owner','other','g00',0,20190,'seat','00','00',2,'{h}',1,{made},1);", h = "b3".to_string() + &"00".repeat(31));
        let walk_conn = migrated();
        walk_conn.execute_batch(&seed).unwrap();
        let db = std::rc::Rc::new(WalkDb {
            conn: RefCell::new(walk_conn),
        });
        let walked = run_old_walk(db.clone(), "owner", Some(TIP), None, NOW_MS).await;
        LOG.with(|l| eprintln!("{:#?}", l.borrow()));
        let walked = walked.unwrap();
        eprintln!("walk rows: {:#?}", walked.rows);
        let judge = SqliteSql::migrated();
        judge.now.set(NOW_MS);
        judge.conn.execute_batch(&seed).unwrap();
        crate::owed_judge::drain_dirty(&judge, 64).await.unwrap();
        let w = stored(&db.conn.borrow(), "owner");
        let j = stored(&judge.conn, "owner");
        eprintln!("walk {w:#?}\njudge {j:#?}");
        assert_eq!(w, j);
    }
}
