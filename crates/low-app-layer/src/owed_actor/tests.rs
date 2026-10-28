#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::owed_walk_frozen::pins::{fixtures, seeded, Fixture, NOW_MS, TIP};
use crate::pot_hook::tests::SqliteSql;
use std::cell::Cell;

/// The judge's own tables the actor writes (its rows, their word references, its state, the marks it reads).
const JUDGE_TABLES: [&str; 4] = ["owed_rows", "owed_row_words", "owed_state", "owed_dirty"];

fn idempotent(sql: &str) -> String {
    for (from, to) in [
        ("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS "),
        ("CREATE UNIQUE INDEX ", "CREATE UNIQUE INDEX IF NOT EXISTS "),
        ("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS "),
        ("CREATE TRIGGER ", "CREATE TRIGGER IF NOT EXISTS "),
    ] {
        if let Some(rest) = sql.strip_prefix(from) {
            if rest.starts_with("IF NOT EXISTS") {
                return sql.to_string();
            }
            return format!("{to}{rest}");
        }
    }
    sql.to_string()
}

/// The schema file as the overlay's migrations make it.
fn generated() -> String {
    let db = SqliteSql::migrated();
    let tables: Vec<&str> = FAMILIES
        .iter()
        .map(|f| f.table)
        .chain(JUDGE_TABLES)
        .collect();
    let mut ddl = Vec::new();
    let mut cols = Vec::new();
    let mut blobs = Vec::new();
    for t in &tables {
        let sql: String = db
            .conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1",
                [t],
                |r| r.get(0),
            )
            .unwrap_or_else(|_| panic!("the record holds {t}"));
        ddl.push(idempotent(&sql));
        let mut q = db.conn.prepare("SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=?1 AND sql IS NOT NULL ORDER BY name").unwrap();
        ddl.extend(
            q.query_map([t], |r| r.get::<_, String>(0))
                .unwrap()
                .map(|s| idempotent(&s.unwrap())),
        );
        let mut q = db.conn.prepare(&format!("PRAGMA table_info({t})")).unwrap();
        let info: Vec<(String, String)> = q
            .query_map([], |r| Ok((r.get(1)?, r.get(2)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        cols.push((
            t.to_string(),
            info.iter().map(|(c, _)| c.clone()).collect::<Vec<_>>(),
        ));
        blobs.extend(
            info.iter()
                .filter(|(_, ty)| ty.eq_ignore_ascii_case("BLOB"))
                .map(|(c, _)| (t.to_string(), c.clone())),
        );
    }
    // The row-count triggers of `owed_rows` (the judge's state reads `owed_state.rows`); never a dirty mark.
    let mut q = db.conn.prepare("SELECT sql FROM sqlite_master WHERE type='trigger' AND tbl_name IN ('owed_rows','owed_row_words') AND name NOT LIKE 'owed_dirty%' AND sql NOT LIKE '%owed_dirty%' ORDER BY name").unwrap();
    ddl.extend(
        q.query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(|s| idempotent(&s.unwrap())),
    );
    let digest = hex::encode(bsv_rs::primitives::hash::sha256(ddl.join("\n").as_bytes()));
    let head: String = include_str!("schema.rs")
        .split("pub const MIRROR_DDL")
        .next()
        .unwrap()
        .to_string();
    let tail: String = include_str!("schema.rs")
        .split("pub fn columns")
        .nth(1)
        .map(|t| format!("pub fn columns{t}"))
        .unwrap();
    let mut out = head;
    out.push_str("pub const MIRROR_DDL: &[&str] = &[\n");
    for d in &ddl {
        out.push_str(&format!("    r#\"{d}\"#,\n"));
    }
    out.push_str("];\npub const COLUMNS: &[(&str, &[&str])] = &[\n");
    for (t, c) in &cols {
        out.push_str(&format!(
            "    (\"{t}\", &[{}]),\n",
            c.iter()
                .map(|c| format!("\"{c}\""))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    out.push_str("];\npub const BLOBS: &[(&str, &str)] = &[\n");
    for (t, c) in &blobs {
        out.push_str(&format!("    (\"{t}\", \"{c}\"),\n"));
    }
    out.push_str("];\n/// The schema's digest, held in the actor's `meta` (`schema`): a mirror built under another is dropped and rebuilt.\n");
    out.push_str(&format!("pub const DIGEST: &str = \"{digest}\";\n\n"));
    out.push_str(&tail);
    out
}

/// The mirror's DDL is the overlay's, generated: RED when a migration moves a mirrored table and the file was not
/// written again (`OWED_ACTOR_SCHEMA_WRITE=1` writes it).
#[test]
fn d2_actor_b_the_mirror_is_the_overlays_schema() {
    let want = generated();
    if std::env::var("OWED_ACTOR_SCHEMA_WRITE").as_deref() == Ok("1") {
        std::fs::write(
            concat!(env!("CARGO_MANIFEST_DIR"), "/src/owed_actor/schema.rs"),
            &want,
        )
        .unwrap();
    }
    assert_eq!(
        include_str!("schema.rs"),
        want,
        "the mirror's schema is not the overlay's: OWED_ACTOR_SCHEMA_WRITE=1"
    );
}

/// A mirror: the actor's SQLite under its own DDL alone (no overlay migration, no trigger but the row counts).
fn mirror() -> SqliteSql {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    for d in mirror_ddl() {
        conn.execute_batch(&d)
            .unwrap_or_else(|e| panic!("{e}: {d}"));
    }
    SqliteSql {
        conn,
        batches: Cell::new(0),
        stmts: Cell::new(0),
        now: Cell::new(NOW_MS),
        recorded: RefCell::default(),
        counted: RefCell::default(),
        lines: RefCell::default(),
        tip: Cell::new(Some(TIP)),
    }
}

/// The pins' outside: the word views as the native D1 judge's fakes answer them, no canonical root.
struct Fakes;
impl Outside for Fakes {
    async fn word_views(
        &self,
        rows: &BTreeMap<String, crate::tracker_host::stored::Row>,
    ) -> Result<BTreeMap<String, Value>, String> {
        Ok(rows.iter().map(|(k, v)| (k.clone(), v.view())).collect())
    }
    async fn canonical_root(&self, _: u64) -> Option<String> {
        None
    }
}

fn games_of(db: &SqliteSql, identity: &str) -> Vec<String> {
    db.conn
        .prepare("SELECT gameId FROM potparty_records WHERE identity=?1 UNION SELECT gameId FROM hopparty_records WHERE identity=?1 UNION SELECT gameId FROM released_hop_evidence WHERE identity=?1 ORDER BY 1")
        .unwrap()
        .query_map([identity], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// The stored rows, every column but the write's own stamp, the facts as stored text: compared byte for byte.
fn stored(db: &SqliteSql, identity: &str) -> Vec<String> {
    let mut q = db.conn.prepare("SELECT json_array(outpoint,family,gameId,sats,opponentIdentity,atHeight,facts,reason,judgedAtMs,claimable,gateHeight,beltDueAtMs,probeTip) FROM owed_rows WHERE identity=?1 ORDER BY outpoint").unwrap();
    q.query_map([identity], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn h(prefix: &str, n: usize) -> String {
    format!("{prefix}{n:0>width$x}", width = 64 - prefix.len())
}

/// The facts arms (MEDIUM-3) the frozen fixtures do not reach: a hop the index holds spent by a SWEEP whose bytes
/// the engine stores (`transactions`), and an index-unspent hop its seat's held JOIN spends (`pot_hop_seats`, the
/// JOIN's own `pot_beefs`): the judge reads the first through `FactsFrom::Stored`, the second through `PotBeefs`.
fn spender_fixture() -> Fixture {
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::{Beef, Transaction, TransactionInput, TransactionOutput};
    let made = (NOW_MS / 1000) as i64 - 7_200;
    let pkh = [0x5au8; 20];
    let p2pkh = |sats| TransactionOutput {
        satoshis: Some(sats),
        locking_script: LockingScript::from_binary(&overlay_discovery::pot::p2pkh_lock(&pkh))
            .unwrap(),
        change: false,
    };
    let (hop_a, hop_b) = (h("b7", 1), h("b7", 2));
    let mut sweep = Transaction::new();
    sweep.inputs.push(TransactionInput::new(hop_a.clone(), 0));
    sweep.outputs.push(p2pkh(20_000));
    let mut sweep_beef = Beef::new();
    sweep_beef.merge_raw_tx(sweep.to_binary(), None);
    let mut join = Transaction::new();
    join.inputs.push(TransactionInput::new(hop_b.clone(), 0));
    join.outputs.push(p2pkh(40_000));
    let mut join_beef = Beef::new();
    join_beef.merge_raw_tx(join.to_binary(), None);
    let (s, j) = (sweep.id(), join.id());
    let mut seed = Vec::new();
    for (n, (hop, spent)) in [(&hop_a, format!("1,'{s}'")), (&hop_b, "0,NULL".to_string())]
        .into_iter()
        .enumerate()
    {
        seed.push(format!("INSERT INTO pot_records(txid,outputIndex,spent,spendingTxid,createdAt,lockKind) VALUES ('{hop}',0,{spent},{made},'p2pkh')"));
        seed.push(format!("INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('owner','other','{}',0,20190,'seat','00','00',2,'{hop}',1,{made},1)", h("9d", n)));
    }
    seed.push(format!(
        "INSERT INTO transactions(txid,beef,has_proof,created_at) VALUES ('{s}',x'{}',0,{made})",
        hex::encode(sweep_beef.to_binary())
    ));
    seed.push(format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind,paramsDecoded,recoveryHeight) VALUES ('{j}',0,0,{made},'covenant',1,900100)"));
    seed.push(format!(
        "INSERT INTO pot_beefs(txid,beef,createdAt) VALUES ('{j}',x'{}',{made})",
        hex::encode(join_beef.to_binary())
    ));
    seed.push(format!("INSERT OR IGNORE INTO pot_hop_seats(potTxid,potVout,markerRowid) SELECT '{j}',0,rowid FROM hopparty_records WHERE txid='{hop_b}'"));
    Fixture {
        name: "spenders by their stored bytes",
        seed,
        identities: vec!["owner".into()],
    }
}

fn all_fixtures() -> Vec<Fixture> {
    let mut f = fixtures();
    f.push(spender_fixture());
    f
}

async fn d1_judge(f: &Fixture) -> SqliteSql {
    let d1 = seeded(f);
    for id in &f.identities {
        for chunk in games_of(&d1, id).chunks(16) {
            crate::owed_judge::judge_subjects(&d1, id, chunk, "d1")
                .await
                .unwrap();
        }
    }
    d1
}

async fn actor_judge(record: &SqliteSql, identity: &str) -> (SqliteSql, Vec<ApplyReport>) {
    let local = mirror();
    let mut reports = Vec::new();
    for chunk in games_of(record, identity).chunks(GAMES_PER_APPLY) {
        let (r, _) = apply(
            record,
            &local,
            &Fakes,
            identity,
            chunk,
            Some(("bootstrap", 1)),
            Some(TIP),
            None,
        )
        .await
        .unwrap();
        local.conn.execute("DELETE FROM applied", []).unwrap();
        reports.push(r);
    }
    (local, reports)
}

/// THE BYTE-IDENTITY PIN (MEDIUM-3, section 11): `derive_owed_rows` over the ACTOR's storage (its mirror, its
/// `txfacts`, no bytes) stores the D1 judge's rows, byte for byte, on every frozen fixture's every identity.
#[tokio::test]
async fn d2_actor_b_the_actor_stores_the_d1_judges_rows() {
    let mut compared = 0;
    let mut rows = 0;
    let mut diffs = Vec::new();
    for f in all_fixtures() {
        let d1 = d1_judge(&f).await;
        for id in &f.identities {
            let want = stored(&d1, id);
            let (local, reports) = actor_judge(&d1, id).await;
            assert!(
                reports.iter().all(|r| !r.missed),
                "{}: {id}: the last judgement asked what no round took",
                f.name
            );
            let got = stored(&local, id);
            compared += 1;
            rows += want.len();
            if got != want {
                diffs.push(format!(
                    "{} {id}: d1 {} rows, actor {} rows\n  d1:    {:?}\n  actor: {:?}",
                    f.name,
                    want.len(),
                    got.len(),
                    want.iter().find(|w| !got.contains(w)),
                    got.iter().find(|g| !want.contains(g))
                ));
            }
        }
    }
    eprintln!("d2_actor_b byte identity: {compared} identities, {rows} rows");
    assert!(diffs.is_empty(), "{}", diffs.join("\n"));
    assert!(compared >= 10 && rows > 0);
}

fn stranded() -> Fixture {
    fixtures()
        .into_iter()
        .find(|f| f.name == "stranded hops")
        .unwrap()
}
const SPENT_BY_A_STRANGER: &str = "UPDATE pot_records SET spent=1,spendingTxid='c900000000000000000000000000000000000000000000000000000000000009' WHERE txid='b300000000000000000000000000000000000000000000000000000000000000'";

/// A slow chaintracks: every outside answer yields to the executor first, and the calls are counted (the pin
/// below asserts the judge asked it, so the interleave it guards is real).
struct SlowOutside(usize, Cell<usize>);
impl Outside for SlowOutside {
    async fn word_views(
        &self,
        rows: &BTreeMap<String, crate::tracker_host::stored::Row>,
    ) -> Result<BTreeMap<String, Value>, String> {
        self.1.set(self.1.get() + 1);
        for _ in 0..self.0 {
            tokio::task::yield_now().await;
        }
        Fakes.word_views(rows).await
    }
    async fn canonical_root(&self, h: u64) -> Option<String> {
        self.1.set(self.1.get() + 1);
        for _ in 0..self.0 {
            tokio::task::yield_now().await;
        }
        Fakes.canonical_root(h).await
    }
}

/// MEDIUM-2, THE SLOW-CHAINTRACKS PAIR (section 4's pin as the design writes it): the actor bootstrapped (the
/// steady state: one round an apply), then two events for one identity; the FIRST reads the record, then its
/// judgement waits on a slow chaintracks answer while the record moves and the SECOND event arrives. Under the
/// object's turn the second waits and its judgement (the newer record's rows) is the one stored. RED with the turn
/// removed (`apply_in_turn` calling `apply` bare): the first event's judgement, hydrated from the older mirror,
/// lands after the second's.
#[tokio::test]
async fn d2_actor_b_two_events_under_a_slow_chaintracks_derive_the_newer_rows() {
    let record = d1_judge(&stranded()).await;
    let games = games_of(&record, "owner");
    let actor = mirror();
    apply(&record, &actor, &Fakes, "owner", &games, None, Some(TIP), None)
        .await
        .unwrap();
    let older = stored(&actor, "owner");
    let control_record = d1_judge(&stranded()).await;
    control_record
        .conn
        .execute_batch(SPENT_BY_A_STRANGER)
        .unwrap();
    let control = mirror();
    apply(&control_record, &control, &Fakes, "owner", &games, None, Some(TIP), None)
        .await
        .unwrap();
    assert_ne!(stored(&control, "owner"), older, "the move changes the rows");
    let turn = futures_util::lock::Mutex::new(());
    let slow = SlowOutside(64, Cell::new(0));
    let first = apply_in_turn(
        &turn,
        &record,
        &actor,
        &slow,
        "owner",
        &games,
        Some(("pot", 1)),
        Some(TIP),
        None,
    );
    let second = async {
        tokio::task::yield_now().await;
        record.conn.execute_batch(SPENT_BY_A_STRANGER).unwrap();
        apply_in_turn(
            &turn,
            &record,
            &actor,
            &Fakes,
            "owner",
            &games,
            Some(("pot", 2)),
            Some(TIP),
            None,
        )
        .await
    };
    let (a, b) = tokio::join!(first, second);
    a.unwrap();
    b.unwrap();
    assert!(slow.1.get() > 0, "the first event's judgement asked the slow chaintracks");
    assert_eq!(
        stored(&actor, "owner"),
        stored(&control, "owner"),
        "the newer event's rows are stored"
    );
}

/// MEDIUM-1: an out-of-order pair derives the same rows (each apply reads the record as it stands), and a
/// redelivery of an applied `(kind, seq)` is a no-op.
#[tokio::test]
async fn d2_actor_b_an_out_of_order_pair_derives_the_same_rows() {
    let record = d1_judge(&stranded()).await;
    record.conn.execute_batch(SPENT_BY_A_STRANGER).unwrap();
    let games = games_of(&record, "owner");
    let (in_order, out_of_order) = (mirror(), mirror());
    for (m, seqs) in [(&in_order, [1, 2]), (&out_of_order, [2, 1])] {
        for s in seqs {
            apply(
                &record,
                m,
                &Fakes,
                "owner",
                &games,
                Some(("pot", s)),
                Some(TIP),
                None,
            )
            .await
            .unwrap();
        }
    }
    assert_eq!(stored(&in_order, "owner"), stored(&out_of_order, "owner"));
    let (again, _) = apply(
        &record,
        &in_order,
        &Fakes,
        "owner",
        &games,
        Some(("pot", 2)),
        Some(TIP),
        None,
    )
    .await
    .unwrap();
    assert!(again.skipped_redelivery);
}

/// A D1 judge whose actor is a mirror over the same record (the consumer's delivery, natively).
struct WithActor {
    d1: SqliteSql,
    actor: SqliteSql,
    d1_judges: bool,
}
impl SqlPort for WithActor {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        self.d1.run_batch(stmts).await
    }
    async fn counters(&self, d: Vec<(String, u64)>) {
        self.d1.counters(d).await;
    }
    fn now_ms(&self) -> u64 {
        self.d1.now_ms()
    }
    fn log(&self, l: &str) {
        self.d1.log(l);
    }
}
impl JudgeDb for WithActor {
    fn tip(&self) -> Option<u64> {
        Some(TIP)
    }
    async fn word_views(
        &self,
        rows: &BTreeMap<String, crate::tracker_host::stored::Row>,
    ) -> Result<BTreeMap<String, Value>, String> {
        Fakes.word_views(rows).await
    }
    async fn canonical_root(&self, _: u64) -> Option<String> {
        None
    }
    async fn push(&self, _: &str, _: Value) {}
    fn d1_judges(&self) -> bool {
        self.d1_judges
    }
    async fn actor_deliver(
        &self,
        identity: &str,
        games: &[String],
    ) -> Result<Option<Value>, String> {
        self.actor_apply(identity, games, "pot").await
    }
    /// As the object's `/apply`: the apply in its turn, Phase C's asks published inside it.
    async fn actor_apply(
        &self,
        identity: &str,
        games: &[String],
        _kind: &str,
    ) -> Result<Option<Value>, String> {
        let turn = futures_util::lock::Mutex::new(());
        apply_and_publish_in_turn(
            &turn,
            &self.d1,
            &self.actor,
            &Fakes,
            identity,
            games,
            None,
            Some(TIP),
            None,
            !self.d1_judges,
        )
        .await?;
        let rows = self
            .actor
            .run_batch(vec![Stmt::new(ROWS_SQL, vec![Bind::Text(identity.into())])])
            .await?;
        Ok(Some(json!({"rows": rows[0].rows, "applyMs": 1})))
    }
    async fn actor_rows(&self, identity: &str) -> Result<Option<Vec<Value>>, String> {
        let rows = self
            .actor
            .run_batch(vec![Stmt::new(ROWS_SQL, vec![Bind::Text(identity.into())])])
            .await?;
        Ok(Some(rows[0].rows.clone()))
    }
}

/// A delivery that answers the actor's stored rows without applying (the planted-diff compare).
struct Planted<'a>(&'a WithActor);
impl SqlPort for Planted<'_> {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        self.0.run_batch(stmts).await
    }
    async fn counters(&self, d: Vec<(String, u64)>) {
        self.0.counters(d).await;
    }
    fn now_ms(&self) -> u64 {
        self.0.now_ms()
    }
    fn log(&self, l: &str) {
        self.0.log(l);
    }
}
impl JudgeDb for Planted<'_> {
    fn tip(&self) -> Option<u64> {
        Some(TIP)
    }
    async fn word_views(
        &self,
        rows: &BTreeMap<String, crate::tracker_host::stored::Row>,
    ) -> Result<BTreeMap<String, Value>, String> {
        Fakes.word_views(rows).await
    }
    async fn canonical_root(&self, _: u64) -> Option<String> {
        None
    }
    async fn push(&self, _: &str, _: Value) {}
    async fn actor_deliver(&self, identity: &str, _: &[String]) -> Result<Option<Value>, String> {
        let rows = self
            .0
            .actor
            .run_batch(vec![Stmt::new(ROWS_SQL, vec![Bind::Text(identity.into())])])
            .await?;
        Ok(Some(json!({"rows": rows[0].rows})))
    }
}

/// Phase A: the consumer's delivery judges in D1 and the actor beside it, compared and counted; the compare (and
/// the reconcile's digest) finds a PLANTED diff in D1's rows.
#[tokio::test]
async fn d2_actor_b_phase_a_compares_and_the_reconcile_finds_a_planted_diff() {
    let w = WithActor {
        d1: d1_judge(&stranded()).await,
        actor: mirror(),
        d1_judges: true,
    };
    let games = games_of(&w.d1, "owner");
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    assert_eq!(
        (w.d1.counter(COUNTERS[0]), w.d1.counter(COUNTERS[1])),
        (1, 0),
        "{:?}",
        w.d1.lines.borrow()
    );
    let rows = |db: &SqliteSql| {
        db.conn.prepare("SELECT json_object('outpoint',outpoint,'family',family,'gameId',gameId,'sats',sats,'opponentIdentity',opponentIdentity,'atHeight',atHeight,'reason',reason,'facts',facts,'claimable',claimable) FROM owed_rows WHERE identity='owner' ORDER BY outpoint").unwrap().query_map([], |r| r.get::<_, String>(0)).unwrap().map(|s| serde_json::from_str::<Value>(&s.unwrap()).unwrap()).collect::<Vec<_>>()
    };
    assert_eq!(rows_digest(&rows(&w.d1)), rows_digest(&rows(&w.actor)));
    w.d1.conn.execute_batch("UPDATE owed_rows SET reason='planted' WHERE rowid=(SELECT MIN(rowid) FROM owed_rows WHERE identity='owner')").unwrap();
    assert_ne!(
        rows_digest(&rows(&w.d1)),
        rows_digest(&rows(&w.actor)),
        "the digest sees the planted row"
    );
    assert_eq!(diff_rows(&rows(&w.d1), &rows(&w.actor)).len(), 1);
    // A divergent ACTOR is counted by the next delivery's compare: its judged rows planted, then compared against the
    // D1 judge's fresh rows through `beside` itself (the actor's own apply would repair them, so the compare reads
    // the planted rows through a delivery that only reads).
    w.actor.conn.execute_batch("UPDATE owed_rows SET reason='actor-planted' WHERE rowid=(SELECT MIN(rowid) FROM owed_rows)").unwrap();
    let planted = Planted(&w);
    beside(&planted, "owner", &games).await;
    assert_eq!(
        (w.d1.counter(COUNTERS[0]), w.d1.counter(COUNTERS[1])),
        (2, 1),
        "the planted diff counted"
    );
    assert!(w
        .d1
        .lines
        .borrow()
        .iter()
        .any(|l| l.starts_with("[owed-actor] DIFF owner")));
}

/// Phase C: with the D1 judge off the actor IS the judge; the marks are acknowledged after it applied, and D1's
/// `owed_rows` are not written.
#[tokio::test]
async fn d2_actor_b_phase_c_acks_the_marks_after_the_actor_applied() {
    let d1 = seeded(&stranded());
    let marks: i64 = d1
        .conn
        .query_row(
            "SELECT COUNT(*) FROM owed_dirty WHERE identity='owner'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(marks > 0, "the fixture's writes marked the identity");
    let w = WithActor {
        d1,
        actor: mirror(),
        d1_judges: false,
    };
    let games = games_of(&w.d1, "owner");
    let report = crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    assert!(report.rows > 0);
    let left: i64 =
        w.d1.conn
            .query_row(
                "SELECT COUNT(*) FROM owed_dirty WHERE identity='owner' AND gameId<>''",
                [],
                |r| r.get(0),
            )
            .unwrap();
    let d1_rows: i64 =
        w.d1.conn
            .query_row(
                "SELECT COUNT(*) FROM owed_rows WHERE identity='owner'",
                [],
                |r| r.get(0),
            )
            .unwrap();
    assert_eq!((left, d1_rows), (0, 0));
    assert_eq!(stored(&w.actor, "owner").len(), report.rows);
}

/// MEDIUM-9 and Rule 23: the KV copy is display only and never an empty list served as "nothing owed".
#[test]
fn d2_actor_b_the_kv_copy_is_never_an_empty_list() {
    let none = display_from_copy(None);
    assert_eq!(
        (none["pending"].clone(), none["rows"].clone()),
        (json!(true), Value::Null)
    );
    let empty = display_from_copy(Some(&kv_copy_body("02ab", &[], 1)));
    assert_eq!(
        (empty["pending"].clone(), empty["rows"].clone()),
        (json!(true), Value::Null)
    );
    let one = display_from_copy(Some(&kv_copy_body(
        "02ab",
        &[json!({"outpoint": "x:0", "claimable": 1})],
        7,
    )));
    assert_eq!(
        (
            one["rows"][0]["claimable"].clone(),
            one["displayOnly"].clone(),
            one["judgedAtMs"].clone()
        ),
        (json!(false), json!(true), json!(7))
    );
    assert!(kv_copy_due(None, 5) && !kv_copy_due(Some(5), 900) && kv_copy_due(Some(5), 1_005));
}

/// MEDIUM-8: the bars served, their quantiles over the ring; the counters seeded by the overlay; LOW-1's name.
#[test]
fn d2_actor_b_the_bars_are_served_and_the_counters_seeded() {
    let mut b = Bars::default();
    for ms in 1..=100 {
        b.apply.note(ms);
        b.visible.note(ms * 10);
    }
    b.oldest_unapplied_at_ms = Some(400);
    let s = b.served(1_000);
    assert_eq!(
        (
            s["applyMsP50"].clone(),
            s["applyMsP95"].clone(),
            s["rowsVisibleMsP95"].clone(),
            s["applyCeilingMs"].clone(),
            s["oldestUnappliedEventAgeMs"].clone()
        ),
        (
            json!(50),
            json!(95),
            json!(950),
            json!(APPLY_CEILING_MS),
            json!(600)
        )
    );
    for k in [
        "collectAfterMinedMsP95",
        "kvCopies",
        "bootstraps",
        "reconcileDiffs",
    ] {
        assert!(s.get(k).is_some(), "{k}");
    }
    assert_eq!(
        COUNTERS,
        bsv_overlay_cloudflare::applayer_counters::OWED_ACTOR
    );
    let seeded = bsv_overlay_cloudflare::applayer_counters::seeded_names();
    assert!(COUNTERS.iter().all(|c| seeded.iter().any(|s| s == c)));
    let key = format!("02{}", "AB".repeat(32));
    assert_eq!(actor_name(&key), Some(key.to_ascii_lowercase()));
    assert_eq!(actor_name("owner"), None);
    assert!(
        !flag_on(None)
            && flag_on(Some("true".into()))
            && d1_judge_on(None)
            && !d1_judge_on(Some("false".into()))
    );
}

/// The direct call's apply on the fleet-shaped fixture: one game of the whale, after its bootstrap, timed (native,
/// the closure and the section; the platform's hop, a cold start and wasm32 are the tier's). Its 1 s bar binds in a
/// release build (`cargo test --release --lib -- d2_actor_b_the_apply_on_the_fleet --nocapture`).
#[tokio::test]
async fn d2_actor_b_the_apply_on_the_fleet_fixture_is_timed() {
    let f = fixtures()
        .into_iter()
        .find(|f| f.name == "d1_fleet")
        .unwrap();
    let record = d1_judge(&f).await;
    let id = &f.identities[0];
    let games = games_of(&record, id);
    let local = mirror();
    for chunk in games.chunks(GAMES_PER_APPLY) {
        apply(&record, &local, &Fakes, id, chunk, None, Some(TIP), None)
            .await
            .unwrap();
    }
    let mut m = Meter::default();
    let mut rounds = Vec::new();
    for g in games.iter().take(20) {
        let t = std::time::Instant::now();
        let (r, _) = apply(
            &record,
            &local,
            &Fakes,
            id,
            std::slice::from_ref(g),
            None,
            Some(TIP),
            None,
        )
        .await
        .unwrap();
        m.note(t.elapsed().as_millis() as u64);
        rounds.push(r.rounds);
    }
    eprintln!("d2_actor_b fleet apply: {} games, one-game apply p50 {:?} ms p95 {:?} ms max {:?} ms, rounds {:?}", games.len(), m.quantile(0.5), m.quantile(0.95), m.max(), rounds);
    assert!(m.quantile(0.95).unwrap() < APPLY_CEILING_MS);
    // The amended bar (rows visible under 1 s p95 of the write) is measured in a release build: 2026-10-27, native,
    // p50 278 ms, p95 302 ms, max 340 ms, one round each. A debug build holds the served ceiling alone.
    if !cfg!(debug_assertions) {
        assert!(m.quantile(0.95).unwrap() < 1_000, "the 1 s bar");
    }
}

/// The belt's courier fake: answers by URL fragment, counts every call (a refused ask never reaches it).
#[derive(Default)]
struct Couriers {
    calls: RefCell<Vec<String>>,
    answers: Vec<(String, (u16, Vec<u8>))>,
}
impl crate::owed_belt::Courier for Couriers {
    async fn get(&self, url: String) -> Option<(u16, Vec<u8>)> {
        self.calls.borrow_mut().push(url.clone());
        for (f, a) in self.answers.iter().rev() {
            if url.contains(f.as_str()) {
                return Some(a.clone());
            }
        }
        Some((404, b"{}".to_vec()))
    }
}

const STRANDED_HOP: &str = "b300000000000000000000000000000000000000000000000000000000000000";

fn asks_of(d1: &SqliteSql) -> Vec<(String, Value, i64)> {
    let mut q = d1
        .conn
        .prepare("SELECT name,cursor,tickedAtMs FROM owed_belt_state WHERE name>='belt-ask:' AND name<'belt-ask;' ORDER BY name")
        .unwrap();
    q.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            serde_json::from_str(&r.get::<_, String>(1)?).unwrap(),
            r.get::<_, i64>(2)?,
        ))
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

/// HIGH-3, THE BELT-ASK ROUND TRIP (Phase C, `OWED_D1_JUDGE=false`: no D1 row is dated). The actor judges and
/// PUBLISHES a `belt-ask` per dated row into the belt's ledger; the belt singleton's shipped tick reads the asks, asks
/// the courier through its ONE gate (every call counted here; the actor asks none), writes the find's facts to the
/// record and hands the find back as a `belt-find` event; the actor's apply reads them and DOWNGRADES the stranded hop's
/// sweep press (its spend found), and its asks follow its rows. D1's `owed_rows` are never written (one writer).
/// RED with the hand-back removed (`ask_find` writing the facts alone): the press stands claimable.
#[tokio::test]
async fn d2_actor_b_the_belt_ask_round_trip() {
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
    let w = WithActor {
        d1: seeded(&stranded()),
        actor: mirror(),
        d1_judges: false,
    };
    let games = games_of(&w.d1, "owner");
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    let hop = format!("{STRANDED_HOP}:0");
    let asks = asks_of(&w.d1);
    let (_, ask, due) = asks
        .iter()
        .find(|(n, _, _)| *n == crate::owed_belt::ask_name("owner", &hop))
        .cloned()
        .unwrap_or_else(|| panic!("the stranded hop's ask is published: {asks:?}"));
    assert_eq!(
        (ask["identity"].clone(), ask["question"]["ask"].clone(), ask["question"]["family"].clone(), ask["dueAtMs"].clone()),
        (json!("owner"), json!("outpoint"), json!("hop-stranded"), json!(due))
    );
    let press = |db: &SqliteSql| -> (String, Option<i64>) {
        db.conn
            .query_row(
                "SELECT family,claimable FROM owed_rows WHERE identity='owner' AND outpoint=?1",
                [&hop],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap()
    };
    assert_eq!(press(&w.actor), ("hop-stranded".to_string(), Some(1)));
    let d1_rows = |db: &SqliteSql| -> i64 {
        db.conn
            .query_row("SELECT COUNT(*) FROM owed_rows", [], |r| r.get(0))
            .unwrap()
    };
    assert_eq!(d1_rows(&w.d1), 0, "Phase C: the D1 judge writes no row");
    // The ask comes due; a stranger's spend of the hop is what the courier knows.
    let at = due as u64 + 1;
    w.d1.now.set(at);
    w.actor.now.set(at);
    let mut spend = Transaction::new();
    spend
        .add_input(TransactionInput::new(STRANDED_HOP.to_string(), 0))
        .unwrap();
    spend
        .add_output(TransactionOutput {
            satoshis: Some(20_000),
            locking_script: LockingScript::from_hex(&format!("76a914{}88ac", "11".repeat(20)))
                .unwrap(),
            change: false,
        })
        .unwrap();
    let spender = spend.id();
    let courier = Couriers {
        answers: vec![
            (
                format!("{STRANDED_HOP}/0/spent"),
                (
                    200,
                    json!({"txid": spender, "vin": 0, "status": "unconfirmed"})
                        .to_string()
                        .into_bytes(),
                ),
            ),
            (
                format!("tx/{spender}/hex"),
                (200, hex::encode(spend.to_binary()).into_bytes()),
            ),
        ],
        ..Default::default()
    };
    let report = crate::owed_belt::tick(&w, &courier).await.unwrap();
    assert!(report.write_faults.is_empty(), "{report:?}");
    assert!(report.handed_back >= 1 && report.found >= 1, "{report:?}");
    assert_eq!(
        courier.calls.borrow().len(),
        report.calls,
        "every courier call is the belt gate's"
    );
    assert!(report.calls <= crate::owed_belt::OWED_BELT_CALLS_PER_TICK);
    let memo: i64 = w
        .d1
        .conn
        .query_row(
            "SELECT COUNT(*) FROM hop_chain_probes WHERE outpoint=?1",
            [&format!("{STRANDED_HOP}.0")],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(memo, 1, "the find's facts are the record's");
    let (family, claimable) = press(&w.actor);
    assert!(
        family != "hop-stranded" || claimable != Some(1),
        "the actor applied the find: the press is downgraded ({family}, {claimable:?})"
    );
    assert_eq!(d1_rows(&w.d1), 0, "still one writer");
    // The asks follow the actor's rows: the hop's ask is gone or re-asks the row as it now stands.
    if let Some((_, a, _)) = asks_of(&w.d1)
        .into_iter()
        .find(|(n, _, _)| *n == crate::owed_belt::ask_name("owner", &hop))
    {
        assert_eq!(a["question"]["family"], json!(family));
    }
    assert_eq!(
        w.d1.counter(COUNTERS[9]),
        report.handed_back as u64,
        "the hand-back counted"
    );
    assert!(w.d1.counter(COUNTERS[8]) > 0, "the asks counted");
}

fn plant_actor(w: &WithActor) {
    w.actor
        .conn
        .execute_batch("UPDATE owed_rows SET reason='actor-planted' WHERE rowid=(SELECT MIN(rowid) FROM owed_rows WHERE identity='owner')")
        .unwrap();
}

/// MEDIUM-10, THE SAMPLED DIGEST RECONCILE finds a planted diff, on the belt's driver (`reconcile_step`): one
/// identity per 22.5 minutes, D1's rows against the actor's in Phases A and B (a planted divergence of the ACTOR found,
/// counted and repaired by a re-apply of its games), and in Phase C the actor's rows against a re-apply from the
/// record. RED with the digest made inert (`rows_digest` answering one constant): no diff is seen.
#[tokio::test]
async fn d2_actor_b_the_reconcile_finds_a_planted_diff() {
    let w = WithActor {
        d1: d1_judge(&stranded()).await,
        actor: mirror(),
        d1_judges: true,
    };
    let games = games_of(&w.d1, "owner");
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    let t0 = NOW_MS as i64;
    let clean = reconcile_step(&w, t0).await.unwrap().unwrap();
    assert_eq!((clean.identity.as_str(), clean.diff, clean.no_actor), ("owner", false, false), "{clean:?}");
    assert_eq!(reconcile_step(&w, t0 + 1).await.unwrap(), None, "one identity per interval");
    plant_actor(&w);
    let found = reconcile_step(&w, t0 + RECONCILE_INTERVAL_MS).await.unwrap().unwrap();
    assert!(found.diff && found.reapplied > 0, "{found:?}");
    assert_eq!(
        (w.d1.counter(COUNTERS[6]), w.d1.counter(COUNTERS[7]), w.d1.counter(COUNTERS[5])),
        (2, 1, found.reapplied as u64)
    );
    assert!(w.d1.lines.borrow().iter().any(|l| l.starts_with("[owed-actor] RECONCILE DIFF owner")));
    let healed = reconcile_step(&w, t0 + 2 * RECONCILE_INTERVAL_MS).await.unwrap().unwrap();
    assert!(!healed.diff, "the re-apply repaired the actor: {healed:?}");
    // Phase C: the D1 rows are cold; the actor's rows against a re-apply of its games from the record.
    let c = WithActor {
        d1: seeded(&stranded()),
        actor: mirror(),
        d1_judges: false,
    };
    crate::owed_events::deliver(&c, "owner", &games_of(&c.d1, "owner"))
        .await
        .unwrap();
    let clean = reconcile_step(&c, t0).await.unwrap().unwrap();
    assert_eq!((clean.identity.as_str(), clean.diff), ("owner", false), "{clean:?}");
    plant_actor(&c);
    let found = reconcile_step(&c, t0 + RECONCILE_INTERVAL_MS).await.unwrap().unwrap();
    assert!(found.diff && found.reapplied > 0, "{found:?}");
    assert_eq!(c.d1.counter(COUNTERS[7]), 1);
}

/// LOW-2, the wipe's lever: at most 50 keys a call, lowercased and deduplicated, a key that is not 66-hex skipped;
/// both bearer routes are wired before the front door; the object's `/reset` deletes all and its alarm.
#[test]
fn d2_actor_b_the_wipe_lever_resets_the_named_actors() {
    let key = |c: char| format!("02{}", c.to_string().repeat(64));
    assert!(object::reset_plan(&json!({})).is_err());
    assert!(object::reset_plan(&json!({"identities": []})).is_err());
    let many: Vec<String> = (0..51).map(|n| format!("02{n:064x}")).collect();
    assert!(object::reset_plan(&json!({ "identities": many })).is_err());
    let (reset, skipped) = object::reset_plan(
        &json!({"identities": [key('A'), key('a'), key('b'), "owner"]}),
    )
    .unwrap();
    assert_eq!(reset, vec![key('a'), key('b')]);
    assert_eq!(skipped, vec!["\"owner\"".to_string()]);
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let lib = squash(include_str!("../lib.rs"));
    for route in [
        "req.path() == \"/internal/owed-actor-identities\" { return owed_actor::object::identities_lever(req, &env).await;",
        "req.path() == \"/internal/owed-actor-reset\" { return owed_actor::object::reset_lever(req, &env).await;",
    ] {
        assert!(lib.contains(&squash(route)), "{route}");
    }
    let object = squash(include_str!("object.rs"));
    assert!(object.contains(&squash(
        "self.state.storage().delete_all().await?; let _ = self.state.storage().delete_alarm().await;"
    )));
    assert!(object.contains(&squash("if !crate::internal_events::internal_bearer_ok(&req, env) {")));
}

/// The captain's harness (bsv-low `owedActorBars.mjs`) reads the flags at `/health.owedActor.flags`.
#[test]
fn d2_actor_b_the_health_serves_the_flags() {
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let routes = squash(include_str!("../routes.rs"));
    let at = routes
        .find(&squash("body[\"owedActor\"][\"flags\"] = serde_json::json!({ crate::owed_actor::READ_FLAG: body[\"owedActor\"][\"readFlag\"], crate::owed_actor::D1_JUDGE_FLAG: body[\"owedActor\"][\"d1Judge\"], });"))
        .expect("the flags object is served");
    for k in ["readFlag", "d1Judge", "bound"] {
        assert!(routes[..at].contains(&format!("body[\"owedActor\"][\"{k}\"]=")), "{k}");
    }
    assert_eq!((READ_FLAG, D1_JUDGE_FLAG), ("OWED_ACTOR_READ", "OWED_D1_JUDGE"));
}
