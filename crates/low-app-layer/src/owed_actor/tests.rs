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
        // ONE event's chunks under one `(kind, seq)`: the per-game key applies each (the lens's HIGH-1; the pin
        // deleted `applied` between its chunks before the fold).
        assert!(!r.skipped_redelivery);
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
    apply(
        &record,
        &actor,
        &Fakes,
        "owner",
        &games,
        None,
        Some(TIP),
        None,
    )
    .await
    .unwrap();
    let older = stored(&actor, "owner");
    let control_record = d1_judge(&stranded()).await;
    control_record
        .conn
        .execute_batch(SPENT_BY_A_STRANGER)
        .unwrap();
    let control = mirror();
    apply(
        &control_record,
        &control,
        &Fakes,
        "owner",
        &games,
        None,
        Some(TIP),
        None,
    )
    .await
    .unwrap();
    assert_ne!(
        stored(&control, "owner"),
        older,
        "the move changes the rows"
    );
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
    assert!(
        slow.1.get() > 0,
        "the first event's judgement asked the slow chaintracks"
    );
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
    /// The event the consumer's delivery carries (`D1Judge::with_event`): its kind and seq reach the actor's
    /// redelivery key, so the pins run the shipped idempotency (the D2-ACTOR-B lens's HIGH-1).
    event: Option<(&'static str, i64)>,
    /// Phase B (`OWED_ACTOR_READ`): the read serves the actor's rows.
    actor_reads: bool,
    /// The actor's apply faults (an unreachable object, a record fault inside it).
    actor_fails: Cell<bool>,
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
    fn actor_reads(&self) -> bool {
        self.actor_reads
    }
    async fn actor_deliver(
        &self,
        identity: &str,
        games: &[String],
    ) -> Result<Option<Value>, String> {
        self.actor_call(identity, games, self.event).await
    }
    /// As the object's `/apply`: the apply in its turn, Phase C's asks published inside it.
    async fn actor_apply(
        &self,
        identity: &str,
        games: &[String],
        _kind: &str,
    ) -> Result<Option<Value>, String> {
        self.actor_call(identity, games, None).await
    }
    async fn actor_rows(&self, identity: &str) -> Result<Option<Vec<Value>>, String> {
        let rows = self
            .actor
            .run_batch(vec![Stmt::new(ROWS_SQL, vec![Bind::Text(identity.into())])])
            .await?;
        Ok(Some(rows[0].rows.clone()))
    }
}

impl WithActor {
    /// As the object's `/apply`: the apply in its turn, Phase C's asks published inside it, the answer's rows and
    /// the games the event has applied.
    async fn actor_call(
        &self,
        identity: &str,
        games: &[String],
        event: Option<(&'static str, i64)>,
    ) -> Result<Option<Value>, String> {
        if self.actor_fails.get() {
            return Err("owed actor: unreachable (pin)".into());
        }
        let turn = futures_util::lock::Mutex::new(());
        let (report, _) = apply_and_publish_in_turn(
            &turn,
            &self.d1,
            &self.actor,
            &Fakes,
            identity,
            games,
            event,
            Some(TIP),
            None,
            !self.d1_judges,
        )
        .await?;
        let rows = self
            .actor
            .run_batch(vec![Stmt::new(ROWS_SQL, vec![Bind::Text(identity.into())])])
            .await?;
        Ok(Some(answer_of(&report, rows[0].rows.clone())))
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
        event: None,
        actor_reads: false,
        actor_fails: Cell::new(false),
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
        event: None,
        actor_reads: false,
        actor_fails: Cell::new(false),
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
    let me = format!("02{}", "ab".repeat(32));
    for copy in [None, Some(kv_copy_body(&me, &[], 1))] {
        match fallback_body(&me, copy.as_ref(), KV_COPY_REASON, "kv-copy") {
            Fallback::Unavailable(b) => assert_eq!(
                (b["pending"].clone(), b["rows"].clone()),
                (json!(true), Value::Null)
            ),
            other => panic!("no copy rows must be a 503, never rows: [] ({other:?})"),
        }
    }
    let row = json!({"identity": me, "outpoint": format!("{}:0", "c1".repeat(32)), "family": "payout", "gameId": "9d".repeat(32), "sats": 5, "facts": "{\"claim\":\"internalize\",\"claimable\":true}", "reason": null});
    let Fallback::Display(one) = fallback_body(
        &me,
        Some(&kv_copy_body(&me, &[row], 7)),
        KV_COPY_REASON,
        "kv-copy",
    ) else {
        panic!("a copy with a row is displayed");
    };
    assert_eq!(
        (
            one["rows"][0]["facts"]["claimable"].clone(),
            one["displayOnly"].clone(),
            one["computedAtMs"].clone()
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
    // The caller's isolate notes an answer's bars: a `mined` word's event is the collect-after-mined sample.
    let mut c = Bars::default();
    note_answer_into(
        &mut c,
        &json!({"applyMs": 40, "atMs": 1_000, "judgedAtMs": 1_300, "mined": true}),
    );
    note_answer_into(
        &mut c,
        &json!({"applyMs": 60, "atMs": 1_000, "judgedAtMs": 1_200}),
    );
    let s = c.served(2_000);
    assert_eq!(
        (
            s["applyMsP95"].clone(),
            s["rowsVisibleMsP95"].clone(),
            s["collectAfterMinedMsP95"].clone()
        ),
        (json!(60), json!(300), json!(300))
    );
    let routes = include_str!("../routes.rs")
        .split_whitespace()
        .collect::<String>();
    assert!(routes.contains(
        &"body[\"owedActor\"] = crate::owed_actor::health_body( actor_read.as_ref(), worker::Date::now().as_millis(), d1_judges, &body[\"owedEvents\"][\"oldestUnackedMarkAgeMs\"], );"
            .split_whitespace()
            .collect::<String>()
    ));
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
    // p50 278 ms, p95 302 ms, max 340 ms, one round each; after the lens fold's fixpoint commit (its copy of the
    // identity's rows aside), 2026-10-28: p50 290 ms, p95 322 ms, max 325 ms. A debug build holds the served ceiling alone.
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
        event: None,
        actor_reads: false,
        actor_fails: Cell::new(false),
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
        (
            ask["identity"].clone(),
            ask["question"]["ask"].clone(),
            ask["question"]["family"].clone(),
            ask["dueAtMs"].clone()
        ),
        (
            json!("owner"),
            json!("outpoint"),
            json!("hop-stranded"),
            json!(due)
        )
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
    let memo: i64 =
        w.d1.conn
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
        event: None,
        actor_reads: false,
        actor_fails: Cell::new(false),
    };
    let games = games_of(&w.d1, "owner");
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    let t0 = NOW_MS as i64;
    let clean = reconcile_step(&w, t0).await.unwrap().unwrap();
    assert_eq!(
        (clean.identity.as_str(), clean.diff, clean.no_actor),
        ("owner", false, false),
        "{clean:?}"
    );
    assert_eq!(
        reconcile_step(&w, t0 + 1).await.unwrap(),
        None,
        "one identity per interval"
    );
    plant_actor(&w);
    let found = reconcile_step(&w, t0 + RECONCILE_INTERVAL_MS)
        .await
        .unwrap()
        .unwrap();
    assert!(found.diff && found.reapplied > 0, "{found:?}");
    assert_eq!(
        (
            w.d1.counter(COUNTERS[6]),
            w.d1.counter(COUNTERS[7]),
            w.d1.counter(COUNTERS[5])
        ),
        (2, 1, found.reapplied as u64)
    );
    assert!(w
        .d1
        .lines
        .borrow()
        .iter()
        .any(|l| l.starts_with("[owed-actor] RECONCILE DIFF owner")));
    let healed = reconcile_step(&w, t0 + 2 * RECONCILE_INTERVAL_MS)
        .await
        .unwrap()
        .unwrap();
    assert!(!healed.diff, "the re-apply repaired the actor: {healed:?}");
    // Phase C: the D1 rows are cold; the actor's rows against a re-apply of its games from the record.
    let c = WithActor {
        d1: seeded(&stranded()),
        actor: mirror(),
        d1_judges: false,
        event: None,
        actor_reads: false,
        actor_fails: Cell::new(false),
    };
    crate::owed_events::deliver(&c, "owner", &games_of(&c.d1, "owner"))
        .await
        .unwrap();
    let clean = reconcile_step(&c, t0).await.unwrap().unwrap();
    assert_eq!(
        (clean.identity.as_str(), clean.diff),
        ("owner", false),
        "{clean:?}"
    );
    plant_actor(&c);
    let found = reconcile_step(&c, t0 + RECONCILE_INTERVAL_MS)
        .await
        .unwrap()
        .unwrap();
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
    let (reset, skipped) =
        object::reset_plan(&json!({"identities": [key('A'), key('a'), key('b'), "owner"]}))
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
    assert!(object.contains(&squash(
        "if !crate::internal_events::internal_bearer_ok(&req, env) {"
    )));
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
        assert!(
            routes[..at].contains(&format!("body[\"owedActor\"][\"{k}\"]=")),
            "{k}"
        );
    }
    assert_eq!(
        (READ_FLAG, D1_JUDGE_FLAG),
        ("OWED_ACTOR_READ", "OWED_D1_JUDGE")
    );
}

// ---- D2-ACTOR-B-FOLD: the lens's four HIGHs (`D2-ACTOR-B-lens-2026-10-28.md`) ----

/// A fixture identity holding at least `n` games whose D1 list is under the ceiling, and its first `n` games.
fn identity_with_games(n: usize) -> (Fixture, String, Vec<String>) {
    for f in all_fixtures() {
        let d1 = seeded(&f);
        for id in f.identities.clone() {
            let g = games_of(&d1, &id);
            if g.len() >= n {
                return (f, id, g.into_iter().take(n).collect());
            }
        }
    }
    panic!("no fixture identity holds {n} games");
}

/// HIGH-1: ONE event naming 20 games of one identity, through the shipped consumer (`route`: the plan, then
/// `judge_plan`'s chunks of 16) in Phase C: the second chunk carries the same `(kind, seq)` and is applied (the
/// idempotency is per game), and only the marks of games the actor applied are acknowledged; the actor's rows are
/// the D1 judge's over the same 20 games. RED on `8af15944`: the second chunk answered `skipped_redelivery`, its
/// games unjudged and their marks acknowledged.
#[tokio::test]
async fn d2_actor_b_fold_h1_one_event_of_twenty_games_is_applied_whole() {
    let (f, id, games) = identity_with_games(20);
    let reference = seeded(&f);
    for chunk in games.chunks(16) {
        crate::owed_judge::judge_subjects(&reference, &id, chunk, "d1")
            .await
            .unwrap();
    }
    let want = stored(&reference, &id);
    assert!(!want.is_empty(), "the 20 games hold rows");
    let d1 = seeded(&f);
    d1.conn.execute("DELETE FROM owed_dirty", []).unwrap();
    for g in &games {
        d1.conn
            .execute(
                "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES (?1,?2,'age','age',1)",
                [&id, g],
            )
            .unwrap();
    }
    let w = WithActor {
        d1,
        actor: mirror(),
        d1_judges: false,
        event: Some(("age", 7)),
        actor_reads: true,
        actor_fails: Cell::new(false),
    };
    let ev = overlay_discovery::owed_event::OwedEvent {
        v: overlay_discovery::owed_event::OWED_EVENT_V,
        seq: 7,
        source: "age".into(),
        source_key: "age".into(),
        facts: Value::Null,
        at_ms: NOW_MS as i64,
    };
    let routed = crate::owed_events::route(&w, &ev).await.unwrap();
    let left: i64 =
        w.d1.conn
            .query_row("SELECT COUNT(*) FROM owed_dirty", [], |r| r.get(0))
            .unwrap();
    assert_eq!(
        (routed.subjects, routed.faults, left),
        (20, 0, 0),
        "every game judged, every mark acknowledged after its apply"
    );
    let got = stored(&w.actor, &id);
    assert_eq!(
        got.len(),
        want.len(),
        "the actor's rows over the event's 20 games"
    );
    assert_eq!(
        got, want,
        "the actor's rows are the D1 judge's over the event's 20 games"
    );
    // A redelivery of the same event skips every game it applied, and acknowledges nothing it did not apply.
    let mut answer = w.actor_deliver(&id, &games[..3]).await.unwrap().unwrap();
    assert_eq!(answer["skippedRedelivery"], json!(true));
    assert_eq!(answer["appliedGames"].take(), json!(games[..3]));
}

/// A mirror whose SQL faults the judge's commit after running its first `after` statements (a statement that
/// faults ends the batch and its earlier statements stand: `object.rs`'s `DoSql`).
struct FaultCommit<'a>(&'a SqliteSql, Cell<bool>, usize);
impl SqlPort for FaultCommit<'_> {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        if self.1.get()
            && stmts
                .iter()
                .any(|s| s.sql == crate::owed_judge::GAME_DELETE_SQL)
        {
            let ran: Vec<Stmt> = stmts.into_iter().take(self.2).collect();
            self.0.run_batch(ran).await?;
            return Err("the commit faulted (pin)".into());
        }
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

/// A record that answers its first `n` batches and faults after (the lens's probe 3).
struct FaultAfter<'a>(&'a SqliteSql, Cell<usize>, usize);
impl SqlPort for FaultAfter<'_> {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        let n = self.1.get();
        self.1.set(n + 1);
        if n >= self.2 {
            return Err("record fault (pin)".into());
        }
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

/// HIGH-2: (a) a record fault on a fresh actor's second round leaves its rows exactly as before the apply (none),
/// on every fixture identity; (b) a read made while an apply waits on its outside answers serves the rows before the
/// apply or the fixpoint's, never a non-fixpoint round's; (c) a commit that faults half way restores the games' rows
/// as they stood. RED on `8af15944`: round 1's judgement over a partial mirror kept (10 of 10 identities).
#[tokio::test]
async fn d2_actor_b_fold_h2_only_the_fixpoint_round_is_committed() {
    let mut faulted = 0;
    let mut kept = Vec::new();
    for f in all_fixtures() {
        let record = d1_judge(&f).await;
        for id in &f.identities {
            let games = games_of(&record, id);
            if games.is_empty() {
                continue;
            }
            let local = mirror();
            let rec = FaultAfter(&record, Cell::new(0), 1);
            let r = apply(
                &rec,
                &local,
                &Fakes,
                id,
                &games,
                Some(("pot", 9)),
                Some(TIP),
                None,
            )
            .await;
            if r.is_err() {
                faulted += 1;
                let got = stored(&local, id);
                if !got.is_empty() {
                    kept.push(format!("{} {id}: {} rows", f.name, got.len()));
                }
            }
        }
    }
    assert!(faulted >= 10, "the second round faulted on {faulted}");
    assert!(
        kept.is_empty(),
        "a faulted apply kept a partial judgement: {kept:?}"
    );

    // (b) A reader sampling the actor's rows at every yield of a fresh actor's slow apply.
    let record = d1_judge(&stranded()).await;
    let games = games_of(&record, "owner");
    let actor = mirror();
    let slow = SlowOutside(8, Cell::new(0));
    let done = Cell::new(false);
    let samples = RefCell::new(Vec::new());
    let applying = async {
        let r = apply(
            &record,
            &actor,
            &slow,
            "owner",
            &games,
            Some(("pot", 1)),
            Some(TIP),
            None,
        )
        .await;
        done.set(true);
        r
    };
    let reading = async {
        while !done.get() {
            samples.borrow_mut().push(stored(&actor, "owner"));
            tokio::task::yield_now().await;
        }
    };
    let (r, ()) = tokio::join!(applying, reading);
    assert!(
        r.unwrap().0.rounds >= 2,
        "a fresh actor's apply takes rounds"
    );
    let fixpoint = stored(&actor, "owner");
    let partial: Vec<_> = samples
        .borrow()
        .iter()
        .filter(|s| !s.is_empty() && **s != fixpoint)
        .cloned()
        .collect();
    assert!(
        partial.is_empty(),
        "a read mid-apply served a non-fixpoint judgement ({} of {} samples)",
        partial.len(),
        samples.borrow().len()
    );

    // (c) The actor judged, the record moved, the next apply's commit faults after its delete and two rows.
    let before = stored(&actor, "owner");
    assert!(!before.is_empty());
    record.conn.execute_batch(SPENT_BY_A_STRANGER).unwrap();
    let faulting = FaultCommit(&actor, Cell::new(true), 3);
    let r = apply(
        &record,
        &faulting,
        &Fakes,
        "owner",
        &games,
        Some(("pot", 2)),
        Some(TIP),
        None,
    )
    .await;
    assert!(r.is_err(), "the commit faulted");
    assert_eq!(
        stored(&actor, "owner"),
        before,
        "the games' rows are restored as they stood"
    );
}

/// HIGH-3 (option (a)): in Phase B a read between the D1 judge's commit and a faulted actor apply serves no press for
/// the downgraded game: the actor applies FIRST, its fault hands the message back, and the D1 judge (whose batch
/// acknowledges the marks) runs only after it, so the downgrade's mark stays pending and the read withholds. RED on
/// `8af15944`: the D1 judge acknowledged the mark, the actor's fault was swallowed, and the read served the actor's
/// stale claimable sweep.
#[tokio::test]
async fn d2_actor_b_fold_h3_phase_b_never_serves_a_press_over_a_faulted_actor() {
    let w = WithActor {
        d1: d1_judge(&stranded()).await,
        actor: mirror(),
        d1_judges: true,
        event: None,
        actor_reads: true,
        actor_fails: Cell::new(false),
    };
    let games = games_of(&w.d1, "owner");
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    let served = |w: &WithActor| -> Vec<(String, Value)> {
        let mut snapshot = futures_executor_block(crate::owed_read::read(&w.d1, "owner")).unwrap();
        snapshot.joined = w
            .actor
            .conn
            .prepare(crate::owed_read::ROWS_SQL)
            .unwrap()
            .query_map(["owner"], |r| {
                let n = r.as_ref().column_count();
                let mut m = serde_json::Map::new();
                for i in 0..n {
                    let v: rusqlite::types::Value = r.get(i)?;
                    let name = r.as_ref().column_name(i)?.to_string();
                    m.insert(
                        name,
                        match v {
                            rusqlite::types::Value::Null => Value::Null,
                            rusqlite::types::Value::Integer(i) => json!(i),
                            rusqlite::types::Value::Real(f) => json!(f),
                            rusqlite::types::Value::Text(t) => json!(t),
                            rusqlite::types::Value::Blob(b) => json!(hex::encode(b)),
                        },
                    );
                }
                Ok(Value::Object(m))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let body = snapshot.body("owner", Some(TIP), &BTreeMap::new()).unwrap();
        body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                (
                    r["outpoint"].as_str().unwrap().to_string(),
                    r["facts"]["claimable"].clone(),
                )
            })
            .collect()
    };
    let downgraded = "b300000000000000000000000000000000000000000000000000000000000000:0";
    assert!(
        served(&w).contains(&(downgraded.to_string(), json!(true))),
        "the sweep is a press before the competitor: {:?}",
        served(&w)
    );
    w.d1.conn.execute_batch(SPENT_BY_A_STRANGER).unwrap();
    w.actor_fails.set(true);
    let event = { (crate::owed_events::deliver(&w, "owner", &games).await).err() };
    let presses: Vec<_> = served(&w)
        .into_iter()
        .filter(|(o, c)| o == downgraded && *c == json!(true))
        .collect();
    assert!(
        presses.is_empty(),
        "a press served over a downgrade the actor has not applied (delivery: {event:?})"
    );
    assert!(event.is_some(), "the actor's fault hands the message back");
    // The actor reachable again: the redelivery applies both judges and the downgrade is served.
    w.actor_fails.set(false);
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    assert!(!served(&w).contains(&(downgraded.to_string(), json!(true))));
}

fn futures_executor_block<F: std::future::Future>(f: F) -> F::Output {
    futures_util::FutureExt::now_or_never(f).expect("the native SQL answers at once")
}

/// HIGH-4: an actor holding the previous digest's tables (an `owed_rows(identity, outpoint)` of an older DDL, its
/// bootstrap done) rebuilt under the new digest applies and stores the D1 judge's rows, its `bootstrapped` cleared
/// and its bootstrap re-armed (a read serves `pending` until it ends). RED on `8af15944`: the judge's write named a
/// column the old `owed_rows` lacks.
#[tokio::test]
async fn d2_actor_b_fold_h4_a_digest_change_rebuilds_the_judges_tables() {
    let record = d1_judge(&stranded()).await;
    let games = games_of(&record, "owner");
    let actor = mirror();
    actor
        .conn
        .execute_batch(
            "DROP TABLE owed_rows; CREATE TABLE owed_rows (identity TEXT NOT NULL, outpoint TEXT NOT NULL, PRIMARY KEY(identity,outpoint)); \
             INSERT INTO meta(name,value) VALUES ('schema','an older digest'),('identity','owner'),('bootstrapped','1')",
        )
        .unwrap();
    for s in crate::owed_actor::object::schema_plan(Some("an older digest")) {
        actor
            .conn
            .execute_batch(&s)
            .unwrap_or_else(|e| panic!("{e}: {s}"));
    }
    let r = apply(
        &record,
        &actor,
        &Fakes,
        "owner",
        &games,
        Some(("pot", 1)),
        Some(TIP),
        None,
    )
    .await;
    assert!(r.is_ok(), "the apply under the new digest: {r:?}");
    assert_eq!(stored(&actor, "owner"), stored(&record, "owner"));
    let meta = |n: &str| -> Option<String> {
        actor
            .conn
            .query_row("SELECT value FROM meta WHERE name=?1", [n], |r| r.get(0))
            .ok()
    };
    assert_eq!(
        (meta("bootstrapped"), meta("bootCursor")),
        (None, Some("0".into())),
        "the bootstrap is re-armed"
    );
}

// ---- D2-ACTOR-B-FOLD-2: the lens's nine MEDIUMs (`D2-ACTOR-B-lens-2026-10-28.md`) ----

/// A Rust mirror of bsv-low `app/src/lib/owed.ts` `parseOwedBody` (the wire's one reader): `None` where the client
/// reads the body as malformed (its `v`, its identity, its `rows` not an array); else each row the client keeps,
/// with its `claimable` as the client reads it (`facts.claimable !== false`).
fn client_reads(body: &Value) -> Option<Vec<(String, bool)>> {
    let hex = |s: &str, n: usize| s.len() == n && s.bytes().all(|b| b.is_ascii_hexdigit());
    if body["v"] != json!(crate::owed::OWED_WIRE_VERSION) {
        return None;
    }
    let id = body["identity"].as_str()?;
    if !(id.len() == 66 && (id.starts_with("02") || id.starts_with("03")) && hex(&id[2..], 64)) {
        return None;
    }
    let mut out = Vec::new();
    for r in body["rows"].as_array()? {
        let Some((t, v)) = r["outpoint"].as_str().and_then(|o| o.split_once(':')) else {
            continue;
        };
        if !hex(t, 64) || v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        if !r["gameId"].as_str().is_some_and(|g| hex(g, 64)) {
            continue;
        }
        let claimable = r["facts"]
            .as_object()
            .is_none_or(|f| f.get("claimable") != Some(&json!(false)));
        out.push((format!("{t}:{v}").to_ascii_lowercase(), claimable));
    }
    Some(out)
}

fn rows_of_actor(actor: &SqliteSql, identity: &str) -> Vec<Value> {
    futures_executor_block(
        actor.run_batch(vec![Stmt::new(ROWS_SQL, vec![Bind::Text(identity.into())])]),
    )
    .unwrap()
    .remove(0)
    .rows
}

/// MEDIUM-2: the KV copy's fallback is the owed wire (`v`, the facts an object) and every row is display only as
/// the client reads it; the frozen body is the one bsv-low's own parser was run over
/// (`tests/fixtures/owed_actor_fallback_body.json`, `OWED_ACTOR_FIXTURE_WRITE=1` writes it). RED on `ee48085c`: its
/// `display_from_copy` body has no `v`, so the client read every copy as malformed (`client_reads` is `None`).
#[tokio::test]
async fn d2_actor_b_fold2_m2_the_fallback_bodies_are_the_owed_wire() {
    let record = d1_judge(&stranded()).await;
    let games = games_of(&record, "owner");
    let actor = mirror();
    apply(
        &record,
        &actor,
        &Fakes,
        "owner",
        &games,
        Some(("pot", 1)),
        Some(TIP),
        None,
    )
    .await
    .unwrap();
    let rows = rows_of_actor(&actor, "owner");
    assert!(
        rows.iter().any(|r| r["claimable"] == json!(1)),
        "the copy holds a press"
    );
    let me = format!("02{}", "ab".repeat(32));
    let copy = kv_copy_body(&me, &rows, NOW_MS);
    // The base's shape, as `ee48085c` served it: the client drops it whole.
    let base = json!({"rows": rows, "pending": false, "source": "kv-copy", "displayOnly": true, "reason": KV_COPY_REASON, "judgedAtMs": NOW_MS});
    assert_eq!(
        client_reads(&base),
        None,
        "the base's copy body is not the wire"
    );
    for (reason, source) in [
        (KV_COPY_REASON, "kv-copy"),
        (BOOTSTRAP_REASON, "owed-actor-bootstrap"),
    ] {
        let Fallback::Display(body) = fallback_body(&me, Some(&copy), reason, source) else {
            panic!("a copy with rows is displayed");
        };
        let read = client_reads(&body).expect("the client parses the fallback body");
        assert_eq!(
            read.len(),
            rows.len(),
            "every copy row is kept by the client"
        );
        assert!(read.iter().all(|(_, c)| !c), "no row is a press: {read:?}");
        assert_eq!(
            (
                body["pending"].clone(),
                body["refresh"].clone(),
                body["reason"].clone()
            ),
            (json!(true), json!("shed"), json!(reason))
        );
        if source == "kv-copy" {
            let path = concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/owed_actor_fallback_body.json"
            );
            let text = serde_json::to_string_pretty(&body).unwrap() + "\n";
            if std::env::var("OWED_ACTOR_FIXTURE_WRITE").is_ok() {
                std::fs::write(path, &text).unwrap();
            }
            assert_eq!(
                std::fs::read_to_string(path).unwrap(),
                text,
                "OWED_ACTOR_FIXTURE_WRITE=1"
            );
        }
    }
    let Fallback::Unavailable(none) = fallback_body(&me, None, KV_COPY_REASON, "none") else {
        panic!("no copy is never a 200");
    };
    assert_eq!(
        (none["pending"].clone(), none["rows"].clone()),
        (json!(true), Value::Null)
    );
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let routes = squash(include_str!("../routes.rs"));
    assert!(routes.contains(&squash(
        "let mut resp = json_response(body.to_string(), 503)?;"
    )));
}

/// MEDIUM-3 and LOW-5: Phase B's read asks D1 first (the state and the pending marks, never `ROWS_SQL`); an identity
/// D1 does not know is answered from D1 and no actor is called or minted; a read of a fresh object writes nothing.
/// RED on `ee48085c` (source): `read_snapshot` read D1's whole list and called the actor for any key, and `/rows`
/// wrote the identity, the cursor and an alarm.
#[tokio::test]
async fn d2_actor_b_fold2_m3_a_stranger_read_mints_no_actor() {
    let d1 = d1_judge(&stranded()).await;
    let known = crate::owed_read::read_with(&d1, "owner", false)
        .await
        .unwrap();
    assert!(object::known_to_d1(&known) && known.joined.is_empty());
    let stranger = format!("02{}", "ee".repeat(32));
    d1.recorded.borrow_mut().clear();
    let s = crate::owed_read::read_with(&d1, &stranger, false)
        .await
        .unwrap();
    assert!(!object::known_to_d1(&s), "a key D1 does not know");
    let ran: Vec<String> = d1
        .recorded
        .borrow()
        .iter()
        .flatten()
        .map(|st| st.sql.clone())
        .collect();
    assert!(
        !ran.iter().any(|q| q == crate::owed_read::ROWS_SQL),
        "LOW-5: the actor's read never runs D1's rows"
    );
    assert!(
        !ran.iter().any(|q| q.starts_with("INSERT")),
        "a stranger's read writes nothing: {ran:?}"
    );
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let object = squash(include_str!("object.rs"));
    let read = &object[object.find("pub(crate)asyncfnread_snapshot(").unwrap()..];
    let gate = read
        .find(&squash("if !known_to_d1(&snapshot) {"))
        .expect("the D1 gate");
    let call = read
        .find(&squash("call_rows(env, identity, READ_WALL_MS)"))
        .expect("the actor's call");
    assert!(gate < call, "D1 is asked before the actor");
    let fetch = &object[object.find("asyncfnfetch(").unwrap()..];
    let fresh = fetch
        .find("self.is_fresh()")
        .expect("a fresh object's read");
    assert!(fresh < fetch.find("self.check_epoch().await").unwrap());
    assert!(fresh < fetch.find("self.rearm_after_rebuild().await").unwrap());
    let rows_route = &object[object.find("asyncfnrows_route(").unwrap()
        ..object.find("asyncfnbootstrap_page(").unwrap()];
    assert!(
        !rows_route.contains("put_meta") && !rows_route.contains("set_alarm"),
        "/rows writes nothing"
    );
}

/// MEDIUM-4: the tip alarm selects the gates in `(last tip judged, tip]` and the courier-dated sweeps below the tip
/// (never an index-unspent sweep), and arms at the next gate. RED on `ee48085c`'s statement (`gateHeight<=?1`, no lower
/// bound, no status): an opened gate re-judged on every alarm for ever (`["gA", "gC", "gD"]`).
#[test]
fn d2_actor_b_fold2_m4_the_tip_alarm_judges_each_gate_once() {
    let actor = mirror();
    actor
        .conn
        .execute_batch(
            "INSERT INTO owed_rows(identity,outpoint,family,gameId,facts,updatedAtMs,claimable,gateHeight,probeTip) VALUES \
             ('owner','a:0','refund-due','gA','{}',0,0,100,NULL),\
             ('owner','b:0','refund-due','gB','{}',0,0,105,NULL),\
             ('owner','c:0','hop-stranded','gC','{\"status\":\"spent\"}',0,1,NULL,99),\
             ('owner','d:0','hop-stranded','gD','{\"status\":\"unspent\"}',0,1,NULL,99)",
        )
        .unwrap();
    let gated = |tip: i64, last: i64| -> Vec<String> {
        let mut q = actor.conn.prepare(GATED_GAMES_SQL).unwrap();
        let mut g: Vec<String> = q
            .query_map([tip, last], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        g.sort();
        g
    };
    assert_eq!(gated(101, -1), vec!["gA", "gC"], "the first window");
    assert_eq!(
        gated(102, 101),
        vec!["gC"],
        "an opened gate is never re-judged"
    );
    assert_eq!(gated(105, 102), vec!["gB", "gC"]);
    let (gate, sweeps): (Option<i64>, Option<i64>) = actor
        .conn
        .query_row(NEXT_GATE_SQL, [101], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap();
    assert_eq!((gate, sweeps), (Some(105), Some(1)));
    assert_eq!(
        next_tip_alarm_ms(0, Some(101), Some(105), false),
        Some(4 * BLOCK_MS)
    );
    assert_eq!(
        next_tip_alarm_ms(0, Some(101), Some(105), true),
        Some(BLOCK_MS)
    );
    assert_eq!(next_tip_alarm_ms(0, None, Some(105), false), Some(BLOCK_MS));
    assert_eq!(
        next_tip_alarm_ms(0, Some(101), None, false),
        None,
        "no gate: no alarm"
    );
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let object = squash(include_str!("object.rs"));
    assert!(object.contains(&squash(
        "self.put_meta(&local, META_TIP_JUDGED, &t.to_string());"
    )));
    assert!(
        !object.contains(&squash("now as i64 + TIP_ALARM_MS as i64")),
        "no ten-minute re-arm"
    );
}

/// MEDIUM-5: the era stamp is the render's two vars the wipe moves; another era's actor is reset before it answers.
#[test]
fn d2_actor_b_fold2_m5_another_eras_actor_is_reset() {
    let now = actor_epoch(Some(" beta-2026-10-26-1 ".into()), Some(1_790_000_000_000));
    assert_eq!(now, "beta-2026-10-26-1|1790000000000");
    assert_eq!(epoch_verdict(None, &now), EpochVerdict::Stamp);
    assert_eq!(epoch_verdict(Some(&now), &now), EpochVerdict::Keep);
    let bumped = actor_epoch(Some("beta-2026-11-02-1".into()), Some(1_790_000_000_000));
    assert_eq!(epoch_verdict(Some(&now), &bumped), EpochVerdict::Reset);
    let cutoff = actor_epoch(Some("beta-2026-10-26-1".into()), Some(1_791_000_000_000));
    assert_eq!(epoch_verdict(Some(&now), &cutoff), EpochVerdict::Reset);
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let object = squash(include_str!("object.rs"));
    let fetch =
        &object[object.find("asyncfnfetch(").unwrap()..object.find("asyncfnalarm(").unwrap()];
    assert!(fetch.contains(&squash(
        "if path != \"/reset\" && path != \"/bars\" { self.check_epoch().await; }"
    )));
    let alarm = &object[object.find("asyncfnalarm(").unwrap()..];
    assert!(alarm.starts_with(&squash(
        "async fn alarm(&self) -> Result<Response> { self.check_epoch().await;"
    )));
    let check = &object[object.find("asyncfncheck_epoch(").unwrap()..];
    let reset = check.find("EpochVerdict::Reset=>{").unwrap();
    assert!(check[reset..].starts_with(&squash(
        "EpochVerdict::Reset => { let _turn = self.turn.lock().await; let _ = self.state.storage().delete_all().await; let _ = self.state.storage().delete_alarm().await;"
    )));
}

/// A Phase A judge whose actor's apply is deferred to after the batch (MEDIUM-6), the actor's calls counted.
struct Deferring<'a> {
    w: &'a WithActor,
    deferred: RefCell<Vec<(String, Vec<String>)>>,
    calls: Cell<usize>,
}
impl SqlPort for Deferring<'_> {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        self.w.run_batch(stmts).await
    }
    async fn counters(&self, d: Vec<(String, u64)>) {
        self.w.counters(d).await;
    }
    fn now_ms(&self) -> u64 {
        self.w.now_ms()
    }
    fn log(&self, l: &str) {
        self.w.log(l);
    }
}
impl JudgeDb for Deferring<'_> {
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
    fn defer_actor(&self, identity: &str, games: &[String]) -> bool {
        self.deferred
            .borrow_mut()
            .push((identity.to_string(), games.to_vec()));
        true
    }
    async fn actor_deliver(
        &self,
        identity: &str,
        games: &[String],
    ) -> Result<Option<Value>, String> {
        self.calls.set(self.calls.get() + 1);
        self.w.actor_deliver(identity, games).await
    }
}

/// MEDIUM-6: in Phase A the consumer's delivery is the D1 judge alone; the actor's apply and its compare run after
/// the batch (`run_deferred` under the queue handler's `ctx.wait_until`). Timed on the fleet's whale, a chunk of 16
/// games, inline against deferred (printed; the PR body carries the figures). RED on `ee48085c`: `deliver` awaited
/// `beside` inline (`left: 1 right: 0`).
#[tokio::test]
async fn d2_actor_b_fold2_m6_phase_a_applies_the_actor_off_the_consumer() {
    let f = fixtures()
        .into_iter()
        .find(|f| f.name == "d1_fleet")
        .unwrap();
    let w = WithActor {
        d1: d1_judge(&f).await,
        actor: mirror(),
        d1_judges: true,
        event: Some(("pot", 9)),
        actor_reads: false,
        actor_fails: Cell::new(false),
    };
    let id = f.identities[0].clone();
    let every = games_of(&w.d1, &id);
    let games: Vec<String> = every.iter().take(16).cloned().collect();
    // the actor bootstrapped (MEDIUM-1: in Phase A): its rows are the whole identity's, under the same 500-row ceiling
    // as D1's (an actor holding 16 games of the whale differs from D1's ceiling: a bootstrap's absence, not a diff)
    for chunk in every.chunks(GAMES_PER_APPLY) {
        apply(&w.d1, &w.actor, &Fakes, &id, chunk, None, Some(TIP), None)
            .await
            .unwrap();
    }
    crate::owed_events::deliver(&w, &id, &games).await.unwrap();
    let d = Deferring {
        w: &w,
        deferred: RefCell::default(),
        calls: Cell::new(0),
    };
    crate::owed_events::deliver(&d, &id, &games).await.unwrap();
    assert_eq!(
        d.calls.get(),
        0,
        "the consumer's delivery never calls the actor in Phase A"
    );
    assert_eq!(
        d.deferred.borrow().len(),
        1,
        "the apply is handed to after the batch"
    );
    let before = w.d1.counter(COUNTERS[0]);
    // after the batch: the event's own apply (a fresh seq here: the warm delivery applied seq 9) and its compare
    let answer = w
        .actor_call(&id, &games, Some(("pot", 10)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(answer["skippedRedelivery"], json!(false));
    compare(&w, &id, &games, &answer).await;
    assert_eq!(
        (
            w.d1.counter(COUNTERS[0]) - before,
            w.d1.counter(COUNTERS[1])
        ),
        (1, 0),
        "the deferred apply compares equal: {:?}",
        w.d1.lines.borrow()
    );
    let (mut inline, mut deferred) = (Meter::default(), Meter::default());
    for n in 0..5 {
        // the base's inline path: the D1 judge, then the actor's apply of a fresh event, then the compare
        let t = std::time::Instant::now();
        crate::owed_judge::judge_subjects(&w, &id, &games, "owed-events")
            .await
            .unwrap();
        let a = w
            .actor_call(&id, &games, Some(("pot", 100 + n)))
            .await
            .unwrap()
            .unwrap();
        compare(&w, &id, &games, &a).await;
        inline.note(t.elapsed().as_millis() as u64);
        let t = std::time::Instant::now();
        crate::owed_events::deliver(&d, &id, &games).await.unwrap();
        deferred.note(t.elapsed().as_millis() as u64);
    }
    eprintln!(
        "d2_actor_b_fold2 m6 consumer service ({} games of {}): inline (D1 judge + actor + compare) p50 {:?} ms max {:?} ms; deferred (D1 judge alone) p50 {:?} ms max {:?} ms",
        games.len(),
        games_of(&w.d1, &id).len(),
        inline.quantile(0.5),
        inline.max(),
        deferred.quantile(0.5),
        deferred.max()
    );
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let lib = squash(include_str!("../lib.rs"));
    assert!(lib.contains(&squash(
        "let deferred = owed_actor::take_deferred(); if !deferred.is_empty() { ctx.wait_until(owed_actor::run_deferred(env.clone(), deferred));"
    )));
}

/// MEDIUM-7 and MEDIUM-8: one isolate's samples flushed by class are what ANOTHER isolate serves (the fleet's
/// figures, the actor's own oldest unapplied event), beside the compare's durable totals and the bootstraps against
/// the identities holding rows. RED on `ee48085c`: the bars were one isolate's (`rowsVisibleMsP95` null here) and the
/// oldest unapplied age in Phase A was D1's un-acked mark.
#[tokio::test]
async fn d2_actor_b_fold2_m7_the_bars_are_the_fleets() {
    let d1 = d1_judge(&stranded()).await;
    let mut a = PendingBars::default();
    for n in 0..10u64 {
        a.note_answer("queue", &json!({"identity": "x", "applyMs": 100 + n, "atMs": 1_000, "judgedAtMs": 1_200 + n * 10}));
    }
    a.note_answer(
        "direct",
        &json!({"identity": "y", "applyMs": 50, "atMs": 1_000, "judgedAtMs": 1_090}),
    );
    a.note_answer(
        "queue",
        &json!({"identity": "z", "applyMs": 70, "atMs": 1_000, "judgedAtMs": 1_290, "mined": true}),
    );
    a.note_fault("owner", Some(4_000));
    a.note_fault("owner", Some(3_000));
    flush_bars_from(&d1, 5_000, &a).await.unwrap();
    d1.conn
        .execute_batch(
            "INSERT INTO ops_counters(name,value) VALUES ('owed_actor_compared_total',40),('owed_actor_diffs_total',2),('owed_actor_faults_total',1),('owed_actor_bootstraps_total',3) \
             ON CONFLICT(name) DO UPDATE SET value=excluded.value",
        )
        .unwrap();
    let read = d1
        .run_batch(vec![Stmt::new(HEALTH_SQL, vec![])])
        .await
        .unwrap()
        .remove(0)
        .rows
        .remove(0);
    let served = health_body(Some(&read), 10_000, true, &json!(99));
    assert_eq!(served["rowsVisibleMsP95"], json!(290), "{served}");
    assert_eq!(served["rowsVisibleDirectMsP95"], json!(90));
    assert_eq!(served["collectAfterMinedMsP95"], json!(290));
    assert_eq!(served["classes"]["mined"]["visibleSamples"], json!(1));
    assert_eq!(
        served["oldestUnappliedEventAgeMs"],
        json!(7_000),
        "the actor's own, in Phase A"
    );
    assert_eq!(
        (
            served["comparedTotal"].clone(),
            served["diffsTotal"].clone(),
            served["faultsTotal"].clone(),
            served["bootstraps"].clone()
        ),
        (json!(40), json!(2), json!(1), json!(3))
    );
    assert!(served["activeIdentities"].as_u64().unwrap() >= 1);
    assert_eq!(
        health_body(Some(&read), 10_000, false, &json!(99))["oldestUnappliedEventAgeMs"],
        json!(99),
        "Phase C: D1's un-acked mark"
    );
    // A later apply of the identity clears its unapplied age.
    let mut b = PendingBars::default();
    b.note_fault("owner", Some(3_000));
    b.note_answer("queue", &json!({"identity": "owner", "applyMs": 1}));
    flush_bars_from(&d1, 6_000, &b).await.unwrap();
    let read = d1
        .run_batch(vec![Stmt::new(HEALTH_SQL, vec![])])
        .await
        .unwrap()
        .remove(0)
        .rows
        .remove(0);
    assert_eq!(
        health_body(Some(&read), 10_000, true, &Value::Null)["oldestUnappliedEventAgeMs"],
        Value::Null
    );
    // The ring keeps the last 256.
    let ring: Value = serde_json::from_str(&merge_bars_row(
        None,
        &(0..300).collect::<Vec<_>>(),
        &[],
        None,
    ))
    .unwrap();
    assert_eq!(
        (
            ring["apply"].as_array().unwrap().len(),
            ring["apply"][0].clone()
        ),
        (METER_SAMPLES, json!(44))
    );
    // MEDIUM-8: the diff line names its identity and game.
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    assert!(squash(include_str!("../owed_actor.rs"))
        .contains(&squash("\"[owed-actor] DIFF {identity} game {game}:")));
}

/// MEDIUM-1 (and LOW-6): an actor's first apply arms its bootstrap; the bootstrap pages every game of the identity by
/// keyset (a game entering the union between two pages shifts no page: the base's `OFFSET` skipped one), and a
/// finished bootstrap is counted durably; a read of an actor whose bootstrap is unfinished is the display fallback.
#[tokio::test]
async fn d2_actor_b_fold2_m1_the_bootstrap_runs_in_phase_a_and_pages_by_keyset() {
    let f = fixtures()
        .into_iter()
        .find(|f| f.name == "d1_fleet")
        .unwrap();
    let d1 = seeded(&f);
    let id = f.identities[0].clone();
    let all: BTreeSet<String> = games_of(&d1, &id).into_iter().collect();
    assert!(all.len() > 32);
    let page = |at: i64, after: &str| -> Vec<(String, i64)> {
        let mut q = d1
            .conn
            .prepare(crate::owed_judge::IDENTITY_GAMES_AFTER_SQL)
            .unwrap();
        q.query_map(rusqlite::params![id, at, after], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect()
    };
    let (mut at, mut after) = object::boot_keyset(Some(object::BOOT_CURSOR_START));
    assert_eq!(
        object::boot_keyset(Some("0")),
        (i64::MAX, String::new()),
        "HIGH-4's re-arm reads as the start"
    );
    let mut seen: Vec<String> = Vec::new();
    let mut pages = 0;
    loop {
        let p = page(at, &after);
        pages += 1;
        if pages == 2 {
            // a new game enters the union, newest, between two pages
            d1.conn.execute_batch(&format!("INSERT INTO potparty_records SELECT * FROM potparty_records WHERE identity='{id}' LIMIT 1; UPDATE potparty_records SET gameId='{}', createdAt=createdAt+100000000 WHERE rowid=(SELECT MAX(rowid) FROM potparty_records)", "fe".repeat(32))).ok();
        }
        seen.extend(p.iter().map(|(g, _)| g.clone()));
        if p.len() < 16 {
            break;
        }
        let cursor = json!([p.last().unwrap().1, p.last().unwrap().0]).to_string();
        (at, after) = object::boot_keyset(Some(&cursor));
    }
    let got: BTreeSet<String> = seen.iter().cloned().collect();
    assert_eq!(seen.len(), got.len(), "no game twice");
    assert!(
        all.is_subset(&got),
        "no game skipped: {:?}",
        all.difference(&got).collect::<Vec<_>>()
    );
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let object = squash(include_str!("object.rs"));
    let apply =
        &object[object.find("asyncfnapply_route(").unwrap()..object.find("fnrows_of(").unwrap()];
    assert!(
        apply.contains(&squash("let _ = self.arm_bootstrap(&me).await;")),
        "the first event arms the bootstrap"
    );
    assert!(object.contains(&squash(
        "record.counters(vec![(COUNTERS[10].into(), 1)]).await;"
    )));
    let read = &object[object.find("pub(crate)asyncfnread_snapshot(").unwrap()..];
    assert!(read.contains(&squash("if answer[\"bootstrapped\"] != true {")));
    assert!(read.contains(&squash("BOOTSTRAP_REASON, \"owed-actor-bootstrap\",")));
    let lib = squash(include_str!("../lib.rs"));
    assert!(lib.contains(&squash("req.path() == \"/internal/owed-actor-bootstrap\" { return owed_actor::object::bootstrap_lever(req, &env).await;")));
}

/// MEDIUM-9: the `owed-v3` re-judge rebuilds D1's `owed_rows` from D1's own evidence after Phase C left them stale,
/// resumable over bounded calls, then stamped (a stamped call judges nothing). New API: no `owed-v3` existed on
/// `ee48085c`.
#[tokio::test]
async fn d2_actor_b_fold2_m9_the_owed_v3_rejudge_restores_d1s_rows() {
    let d1 = d1_judge(&stranded()).await;
    let all = |d1: &SqliteSql| -> Vec<String> {
        let mut q = d1.conn.prepare("SELECT json_array(identity,outpoint,family,gameId,sats,facts,reason,claimable,gateHeight,beltDueAtMs,probeTip) FROM owed_rows ORDER BY identity,outpoint").unwrap();
        q.query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    };
    let want = all(&d1);
    assert!(!want.is_empty());
    // Phase C's stale mirror: rows changed and one lost while the actor judged.
    d1.conn
        .execute_batch("UPDATE owed_rows SET reason='stale at the flip', claimable=1; DELETE FROM owed_rows WHERE rowid=(SELECT MIN(rowid) FROM owed_rows)")
        .unwrap();
    assert_ne!(all(&d1), want);
    let mut calls = 0;
    loop {
        calls += 1;
        let r = owed_v3_pass(&d1, NOW_MS as i64, 3).await.unwrap();
        if r.stamped {
            break;
        }
        assert!(calls < 200, "the re-judge converges");
    }
    assert!(calls > 2, "resumable over bounded calls ({calls})");
    assert_eq!(all(&d1), want, "D1's rows are the D1 judge's again");
    assert!(d1.counter(COUNTERS[14]) >= 1);
    d1.recorded.borrow_mut().clear();
    assert!(owed_v3_pass(&d1, NOW_MS as i64, 3).await.unwrap().stamped);
    assert_eq!(d1.recorded.borrow().len(), 1, "a stamped call is one read");
    let lib = include_str!("../lib.rs")
        .split_whitespace()
        .collect::<String>();
    assert!(lib.contains(
        "req.path()==\"/internal/owed-v3-pass\"{returnowed_actor::owed_v3_lever(req,&env).await;"
    ));
}
