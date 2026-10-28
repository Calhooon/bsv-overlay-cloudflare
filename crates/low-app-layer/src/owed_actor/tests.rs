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
        // ONE event's chunks under one `(kind, seq)`: each chunk applies its games (the lens's HIGH-1).
        assert_eq!(r.applied_games.len(), chunk.len());
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
/// redelivery of an applied `(kind, seq)` is applied again and stores the same rows (NEW-H1 (b): never a skip).
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
    let before = stored(&in_order, "owner");
    // The delta lens's NEW-H1 (b): a redelivery of an applied `(kind, seq)` is applied again, the same judgement.
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
    assert_eq!(again.applied_games.len(), games.len());
    assert_eq!(stored(&in_order, "owner"), before);
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
        // FOLD-8's item 11 pin: a word written between the two judges (after the D1 judge, before the actor reads).
        if FOLD8_J_BETWEEN.with(|c| c.replace(false)) {
            self.d1.conn.execute_batch(SPENT_BY_A_STRANGER).unwrap();
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
        // NEW-H1's pin: a downgrade written while the actor applied (after its record read, before the D1 judge).
        if NEW_H1_INJECT.with(|c| c.replace(false)) {
            self.d1.conn.execute_batch(SPENT_BY_A_STRANGER).unwrap();
        }
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
    beside(&planted, "owner", &games, None).await;
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
        &"body[\"owedActor\"] = crate::owed_actor::health_body( actor_snapshot.as_ref(), worker::Date::now().as_millis(), d1_judges, &body[\"owedEvents\"][\"oldestUnackedMarkAgeMs\"], );"
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
    // A redelivery of the same event applies its games again (the delta lens's NEW-H1 (b): never a skip), the same
    // judgement, and names exactly the games it applied.
    let mut answer = w.actor_deliver(&id, &games[..3]).await.unwrap().unwrap();
    assert_eq!(answer["appliedGames"].take(), json!(games[..3]));
    assert_eq!(stored(&w.actor, &id), want);
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
        "self.put_meta(&local, META_TIP_JUDGED, &window.tip_judged.to_string());"
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
    fn defer_actor(&self, identity: &str, games: &[String], _bound: Option<i64>) -> bool {
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
    assert_eq!(answer["appliedGames"], json!(games));
    compare(&w, &id, &games, &answer, None).await;
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
        compare(&w, &id, &games, &a, None).await;
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
        lib_rev(),
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
        .contains(&squash("\"[owed-actor] DIFF {identity} game {}:")));
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
    // D2-ACTOR-B-FOLD-7: the first event of an actor not bootstrapped is HELD and arms the bootstrap.
    assert!(
        apply.contains(&squash(
            "let armed = self.arm_bootstrap(&me, false).await.armed;"
        )),
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

// ---- D2-ACTOR-B-FOLD-3: the delta lens's NEW-H1 (bsv-low `D2-ACTOR-B-delta-2026-10-28.md`) ----

thread_local! { static NEW_H1_INJECT: Cell<bool> = const { Cell::new(false) }; }

/// The read as Phase B serves it: D1's state and pending marks, the actor's rows (the lens's `delta_served`).
fn new_h1_served(w: &WithActor) -> Vec<(String, Value)> {
    let mut snapshot =
        futures_executor_block(crate::owed_read::read_with(&w.d1, "owner", false)).unwrap();
    snapshot.joined = w
        .actor
        .conn
        .prepare(crate::owed_read::ROWS_SQL)
        .unwrap()
        .query_map(["owner"], |r| {
            let mut m = serde_json::Map::new();
            for i in 0..r.as_ref().column_count() {
                let v: rusqlite::types::Value = r.get(i)?;
                m.insert(
                    r.as_ref().column_name(i)?.to_string(),
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
}
const NEW_H1_DOWN: &str = "b300000000000000000000000000000000000000000000000000000000000000:0";
fn new_h1_marks(w: &WithActor) -> i64 {
    w.d1.conn
        .query_row(
            "SELECT COUNT(*) FROM owed_dirty WHERE identity='owner'",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

/// NEW-H1 (a), the lens's `delta_probe_a`: Phase B, a downgrade written WHILE the actor applies (after its record
/// read, before the D1 judge's mark read) is never acknowledged by the D1 judge: the D1 judge's acknowledgement is
/// bounded by the marks' generation read before the actor's call, so the mark stands and the read withholds the
/// press. RED on `42dd7f7e`: "marks left 0", `b3..00:0` served `claimable: true`.
#[tokio::test]
async fn d2_actor_b_fold3_h1_a_phase_b_a_mark_during_the_actor_apply_is_never_acked() {
    let w = WithActor {
        d1: d1_judge(&stranded()).await,
        actor: mirror(),
        d1_judges: true,
        event: Some(("pot", 7)),
        actor_reads: true,
        actor_fails: Cell::new(false),
    };
    let games = games_of(&w.d1, "owner");
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    assert!(
        new_h1_served(&w).contains(&(NEW_H1_DOWN.to_string(), json!(true))),
        "a press before"
    );
    let w = WithActor {
        event: Some(("pot", 8)),
        ..w
    };
    w.d1.conn
        .execute(
            "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('owner',?1,'age','age',1)",
            [&games[0]],
        )
        .unwrap();
    NEW_H1_INJECT.with(|c| c.set(true));
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    let served = new_h1_served(&w);
    assert!(
        !served.contains(&(NEW_H1_DOWN.to_string(), json!(true))),
        "a press served over a downgrade the D1 judge acknowledged after the actor applied (marks left {}): {served:?}",
        new_h1_marks(&w)
    );
    assert!(new_h1_marks(&w) > 0, "the downgrade's marks stand");
    // The next delivery (the mark's own event) applies the downgrade in the actor and acknowledges it.
    let w = WithActor {
        event: Some(("pot", 9)),
        ..w
    };
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    assert_eq!(new_h1_marks(&w), 0);
    assert!(!new_h1_served(&w).contains(&(NEW_H1_DOWN.to_string(), json!(true))));
}

/// NEW-H1 (b), the lens's `delta_probe_b`: Phase C, a delivery whose actor apply landed and whose acknowledgement was
/// lost, then a downgrade, then the redelivery: the redelivery re-applies (no skip), so the downgrade is judged before
/// its marks are acknowledged. RED on `42dd7f7e`: the redelivery answered `Ok` (6 subjects) leaving 0 marks and
/// `b3..00:0` served `claimable: true`.
#[tokio::test]
async fn d2_actor_b_fold3_h1_b_phase_c_a_redelivery_never_acks_a_newer_mark_unjudged() {
    let w = WithActor {
        d1: d1_judge(&stranded()).await,
        actor: mirror(),
        d1_judges: false,
        event: Some(("pot", 7)),
        actor_reads: true,
        actor_fails: Cell::new(false),
    };
    let games = games_of(&w.d1, "owner");
    w.d1.conn
        .execute(
            "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('owner',?1,'age','age',1)",
            [&games[0]],
        )
        .unwrap();
    // The first delivery: the actor applied, its acknowledgement lost.
    w.actor_deliver("owner", &games).await.unwrap();
    assert!(
        new_h1_served(&w).contains(&(NEW_H1_DOWN.to_string(), json!(true))),
        "a press before"
    );
    w.d1.conn.execute_batch(SPENT_BY_A_STRANGER).unwrap();
    assert!(new_h1_marks(&w) > 0);
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    let served = new_h1_served(&w);
    assert!(
        !served.contains(&(NEW_H1_DOWN.to_string(), json!(true))),
        "a press served over a downgrade a redelivery acknowledged unjudged (marks left {}): {served:?}",
        new_h1_marks(&w)
    );
}

/// NEW-H1, the D1-only judges: under the actor's read (`OWED_ACTOR_READ`, the D1 judge on) the drain, the hooks' drain
/// and the `owed-v2` pass judge nothing and acknowledge nothing; the `owed-v3` re-judge (the rollback's first render)
/// rewrites D1's rows and acknowledges no mark; the `owed-v2` lever refuses (409). New API (`Stop::ActorRead`); RED
/// with the drain's gate reverted: "assertion `left == right` failed: the drain".
#[tokio::test]
async fn d2_actor_b_fold3_h1_c_no_d1_only_judge_acknowledges_under_the_actor_read() {
    let w = WithActor {
        d1: d1_judge(&stranded()).await,
        actor: mirror(),
        d1_judges: true,
        event: Some(("pot", 7)),
        actor_reads: true,
        actor_fails: Cell::new(false),
    };
    let games = games_of(&w.d1, "owner");
    w.d1.conn.execute_batch(SPENT_BY_A_STRANGER).unwrap();
    let marks = new_h1_marks(&w);
    assert!(marks > 0);
    let drained = crate::owed_judge::drain_dirty(&w, 64).await.unwrap();
    assert_eq!(
        (drained.subjects, new_h1_marks(&w)),
        (0, marks),
        "the drain"
    );
    let hooks = crate::owed_judge::drain_identities(&w, &["owner".into()], "pot-changed")
        .await
        .unwrap();
    assert_eq!((hooks.subjects, new_h1_marks(&w)), (0, marks), "the hooks");
    let deadline = crate::pot_hook::Deadline::new(
        20_000,
        3_000,
        Box::new(|| 0),
        Box::new(|_| Box::pin(std::future::pending())),
    );
    let v2 = crate::owed_backfill::backfill_pass(&w, &deadline)
        .await
        .unwrap();
    assert_eq!(v2.stop, crate::owed_backfill::Stop::ActorRead);
    w.d1.conn
        .execute("UPDATE owed_rows SET reason='stale at the flip'", [])
        .unwrap();
    loop {
        if owed_v3_pass(&w, NOW_MS as i64, 16).await.unwrap().stamped {
            break;
        }
    }
    let stale: i64 =
        w.d1.conn
            .query_row(
                "SELECT COUNT(*) FROM owed_rows WHERE reason='stale at the flip'",
                [],
                |r| r.get(0),
            )
            .unwrap();
    assert_eq!(stale, 0, "the owed-v3 pass re-judged D1's rows");
    assert_eq!(new_h1_marks(&w), marks, "and acknowledged no mark");
    // The consumer then applies the actor first and acknowledges.
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    assert_eq!(new_h1_marks(&w), 0);
    let lever = include_str!("../owed_backfill.rs")
        .split_whitespace()
        .collect::<String>();
    assert!(lever.contains(
        &"\"the owed actor serves the read (OWED_ACTOR_READ=true): the owed-v2 lever's D1-only judge does not run\", 409,"
            .split_whitespace()
            .collect::<String>()
    ));
}

/// NEW-M1: a player first seen after the Phase C flip (a party row, no D1 `owed_state`: nothing writes it in Phase C)
/// is known to the actor read by the identity probe alone, and no read writes a mark: the mark count is flat across
/// two reads (no wanted envelope for the sweep to expand into a whole-identity re-judge). RED with `42dd7f7e`'s branch
/// put back (the probe writes the wanted mark): "read 1 wrote a mark".
#[tokio::test]
async fn d2_actor_b_fold3_m1_a_new_player_after_the_flip_reads_without_a_mark() {
    let d1 = d1_judge(&stranded()).await;
    d1.conn
        .execute_batch("DELETE FROM owed_state WHERE identity='owner'; DELETE FROM owed_dirty")
        .unwrap();
    let marks = |d1: &SqliteSql| -> i64 {
        d1.conn
            .query_row("SELECT COUNT(*) FROM owed_dirty", [], |r| r.get(0))
            .unwrap()
    };
    for read in 1..=2 {
        let s = crate::owed_read::read_with(&d1, "owner", false)
            .await
            .unwrap();
        assert_eq!(marks(&d1), 0, "read {read} wrote a mark");
        assert!(
            object::known_to_d1(&s),
            "read {read}: the probe knows the player"
        );
        assert!(!s.wanted);
    }
    // The D1 read (Phases A and B off the actor) still writes its one wanted envelope.
    crate::owed_read::read(&d1, "owner").await.unwrap();
    assert_eq!(marks(&d1), 1);
}

/// NEW-M3: the Phase C rollback cannot serve D1's stale rows as presses. A Phase C acknowledgement stamps
/// `owed-actor-judged` and clears `owed-v3`; the D1 read (`OWED_ACTOR_READ` off) then withholds every press, until the
/// `owed-v3` re-judge (the D1 judge back on, run under the actor's read: it acknowledges nothing) stamps; the next
/// Phase C delivery clears the stamp again. New API; RED with the mark's read made inert (`42dd7f7e`'s read): "the D1
/// read serves no press from the rows of the flip" (`b3..00:0` served `claimable: true`).
#[tokio::test]
async fn d2_actor_b_fold3_m3_the_d1_read_serves_no_press_before_owed_v3() {
    let claimable = |d1: &SqliteSql| -> Vec<String> {
        let snapshot = futures_executor_block(crate::owed_read::read(d1, "owner")).unwrap();
        let body = snapshot.body("owner", Some(TIP), &BTreeMap::new()).unwrap();
        body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| r["facts"]["claimable"] == json!(true))
            .map(|r| r["outpoint"].as_str().unwrap().to_string())
            .collect()
    };
    let w = WithActor {
        d1: d1_judge(&stranded()).await,
        actor: mirror(),
        d1_judges: false,
        event: Some(("pot", 7)),
        actor_reads: true,
        actor_fails: Cell::new(false),
    };
    let games = games_of(&w.d1, "owner");
    let before = claimable(&w.d1);
    assert!(
        before.contains(&NEW_H1_DOWN.to_string()),
        "D1 presses before the flip"
    );
    // Phase C: the actor judges and acknowledges; D1's rows stand as they were at the flip.
    w.d1.conn
        .execute(
            "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('owner',?1,'age','age',1)",
            [&games[0]],
        )
        .unwrap();
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    assert_eq!(
        claimable(&w.d1),
        Vec::<String>::new(),
        "the D1 read serves no press from the rows of the flip"
    );
    let served = futures_executor_block(crate::owed_read::read(&w.d1, "owner")).unwrap();
    let body = served.body("owner", Some(TIP), &BTreeMap::new()).unwrap();
    let row = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["outpoint"] == NEW_H1_DOWN)
        .unwrap();
    assert_eq!(
        row["facts"]["claimReason"],
        json!(crate::owed_read::ROLLBACK_PENDING_REASON)
    );
    // The rollback's first render: the D1 judge on, the actor's read still on; the re-judge to its stamp.
    let w = WithActor {
        d1_judges: true,
        ..w
    };
    while !owed_v3_pass(&w, NOW_MS as i64, 16).await.unwrap().stamped {}
    assert_eq!(
        claimable(&w.d1),
        before,
        "the re-judge's rows serve their presses"
    );
    // A later Phase C delivery clears the stamp again.
    let w = WithActor {
        d1_judges: false,
        ..w
    };
    w.d1.conn
        .execute(
            "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('owner',?1,'age','age',1)",
            [&games[0]],
        )
        .unwrap();
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    assert_eq!(claimable(&w.d1), Vec::<String>::new());
}

/// NEW-M2 (a): an INDEX-ONLY digest move keeps the actor's rows and its bootstrap (the held indexes dropped, the
/// render's created); a move of a table's shape rebuilds as HIGH-4 does. New API; RED with `42dd7f7e`'s rule (the
/// index-only arm removed, every digest move a rebuild): "the rows kept" (`owed_rows` dropped).
#[test]
fn d2_actor_b_fold3_m2_an_index_only_digest_move_keeps_the_rows() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    for s in object::schema_plan(None) {
        conn.execute_batch(&s).unwrap();
    }
    conn.execute_batch(
        "INSERT INTO owed_rows(identity,outpoint,family,gameId,facts,updatedAtMs) VALUES ('owner','b3:0','payout','g','{}',1);
         INSERT INTO meta(name,value) VALUES ('bootstrapped','1');
         CREATE INDEX idx_an_older_index ON owed_rows(gameId);
         DROP INDEX IF EXISTS idx_owed_rows_belt;",
    )
    .unwrap();
    let indexes = |c: &rusqlite::Connection| -> Vec<String> {
        c.prepare(object::HELD_INDEXES_SQL)
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    };
    let held = indexes(&conn);
    let shape = actor_shape();
    for s in object::schema_plan_kept(Some("an older digest"), Some(&shape), &held) {
        conn.execute_batch(&s)
            .unwrap_or_else(|e| panic!("{e}: {s}"));
    }
    let count = |c: &rusqlite::Connection, sql: &str| -> i64 {
        c.query_row(sql, [], |r| r.get(0)).unwrap()
    };
    assert_eq!(
        count(&conn, "SELECT COUNT(*) FROM owed_rows"),
        1,
        "the rows kept"
    );
    assert_eq!(
        count(&conn, "SELECT COUNT(*) FROM meta WHERE name='bootstrapped'"),
        1,
        "the bootstrap kept"
    );
    let now = indexes(&conn);
    assert!(
        !now.contains(&"idx_an_older_index".to_string()),
        "the held index dropped"
    );
    assert!(
        now.contains(&"idx_owed_rows_belt".to_string()),
        "the render's index created"
    );
    let fresh = rusqlite::Connection::open_in_memory().unwrap();
    for s in object::schema_plan(None) {
        fresh.execute_batch(&s).unwrap();
    }
    assert_eq!(now, indexes(&fresh), "exactly the render's indexes");
    // A shape move rebuilds.
    let plan = object::schema_plan_kept(Some("an older digest"), Some("an older shape"), &held);
    assert!(plan.iter().any(|s| s == "DROP TABLE IF EXISTS owed_rows"));
    assert!(plan.iter().any(|s| s.starts_with("DELETE FROM meta")));
    // The shape is the tables, the UNIQUE indexes and the triggers: never a plain index.
    assert!(is_plain_index("CREATE INDEX IF NOT EXISTS i ON t(c)"));
    assert!(!is_plain_index(
        "CREATE UNIQUE INDEX IF NOT EXISTS i ON t(c)"
    ));
}

/// NEW-M2 (b): the fleet's bootstrap budget: the shipped statements under real SQLite hand out
/// `BOOTSTRAP_PAGES_PER_MINUTE` tokens a minute, the next page waits for a slot in a later minute spread by its
/// identity, and a new minute starts the count again. New API (no budget on `42dd7f7e`); RED with `boot_pace` made
/// inert (every page run: `left: None`).
#[test]
fn d2_actor_b_fold3_m2_the_bootstrap_pages_take_a_fleet_budget() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE owed_belt_state (name TEXT PRIMARY KEY, cursor TEXT, tickedAtMs INTEGER)",
    )
    .unwrap();
    let take = |now: u64| -> i64 {
        let rows: Vec<Value> = conn
            .prepare(BOOT_BUDGET_TAKE_SQL)
            .unwrap()
            .query_map([now as i64], |r| Ok(json!({"taken": r.get::<_, i64>(0)?})))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        boot_taken(&rows)
    };
    let t0 = 1_800_000_000_000u64;
    let id = format!("02{}", "ab".repeat(32));
    for n in 1..=BOOTSTRAP_PAGES_PER_MINUTE {
        assert_eq!(
            boot_pace(take(t0 + n as u64), t0, &id),
            None,
            "page {n} runs"
        );
    }
    let wait = boot_pace(take(t0 + 61), t0 + 61, &id).expect("past the budget the page waits");
    assert!(wait >= (t0 / 60_000 + 1) * 60_000 && wait < (t0 / 60_000 + 2) * 60_000);
    let other = boot_pace(61, t0, &format!("03{}", "cd".repeat(32))).unwrap();
    assert_ne!(wait, other, "the waits are spread");
    let next = (t0 / 60_000 + 1) * 60_000;
    assert_eq!(take(next), 1, "a new minute counts from one");
    assert_eq!(boot_pace(1, next, &id), None);
    assert_eq!(BOOTSTRAP_PAGE_DELAY_MS, 1_000);
    let object = include_str!("object.rs")
        .split_whitespace()
        .collect::<String>();
    let page = &object[object.find("asyncfnbootstrap_page(").unwrap()..];
    let budget = page
        .find("BOOT_BUDGET_TAKE_SQL")
        .expect("the page takes a token");
    assert!(
        !page.contains("BOOT_BUDGET_READ_SQL"),
        "NEW2-L3: one statement"
    );
    // D2-ACTOR-B-FOLD-7: the page's read is `bootstrap_step`'s (the shipped step the pins run natively), the next
    // page's alarm an OFFSET (`alarm_in`, the captain's item 11).
    assert!(
        budget < page.find("bootstrap_step(").unwrap(),
        "before its read"
    );
    let actor = include_str!("../owed_actor.rs")
        .split_whitespace()
        .collect::<String>();
    let step = &actor[actor.find("pubasyncfnbootstrap_step").unwrap()..];
    assert!(step[..step.find("drain_held(").unwrap()].contains("IDENTITY_GAMES_AFTER_SQL"));
    assert!(page.contains("self.alarm_in(BOOTSTRAP_PAGE_DELAY_MS).await;"));
}

/// The delta lens's LOWs. NEW-L1: the tip alarm moves `tipJudged` only when every apply of its window answered (a
/// source pin: the alarm runs in a Durable Object). NEW-L2: the KV copy names its era and a read of another era
/// serves none. NEW-L3: the bootstrap lever's coverage fold (a full, contiguous pass with nothing uncovered is the
/// gate; a later call from the middle breaks it). NEW-L6: the deferred applies counted at their start and at their
/// end. Each new API (none on `42dd7f7e`); RED with its rule made inert (L1: `tipJudged` put unconditionally, the
/// source assertion; L2: `kv_copy_of_era` the identity, `left: Some(..)`).
#[test]
fn d2_actor_b_fold3_lows() {
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let object = squash(include_str!("object.rs"));
    let alarm = &object[object.find("asyncfnalarm(").unwrap()..];
    // Since the second delta lens's NEW2-L5 the window moves `tipJudged` past its answered games and carries a
    // faulted one ([`tip_window`], pinned by `d2_actor_b_fold4_l5_*`).
    assert!(alarm.contains(&squash(
        "self.put_meta(&local, META_TIP_JUDGED, &window.tip_judged.to_string());"
    )));
    // D2-ACTOR-B-FOLD-7: a held answer (202) is a landed apply for the window (the bootstrap applies it).
    assert!(alarm.contains(&squash(
        "self.apply_route(body) .await .is_ok_and(|r| matches!(r.status_code(), 200 | 202))"
    )));
    // L2
    let me = format!("02{}", "ab".repeat(32));
    let mut copy = kv_copy_body(&me, &[], 1);
    copy["epoch"] = json!("2|1000");
    assert_eq!(
        kv_copy_of_era(Some(copy.clone()), "2|1000"),
        Some(copy.clone())
    );
    assert_eq!(
        kv_copy_of_era(Some(copy), "3|1000"),
        None,
        "another era's copy"
    );
    assert_eq!(
        kv_copy_of_era(Some(kv_copy_body(&me, &[], 1)), "2|1000"),
        None,
        "a copy with no era"
    );
    assert!(object.contains(&squash("body[\"epoch\"] = json!(env_epoch(&self.env));")));
    // L3
    let c = coverage_fold(None, "", 50, 3, Some("id50"), 10);
    let c = coverage_fold(Some(&c), "id50", 20, 0, None, 20);
    assert_eq!(
        (
            c["complete"].clone(),
            c["contiguous"].clone(),
            c["identities"].clone(),
            c["uncovered"].clone()
        ),
        (json!(true), json!(true), json!(70), json!(3))
    );
    let clean = coverage_fold(None, "", 50, 0, Some("id50"), 30);
    let clean = coverage_fold(Some(&clean), "id50", 20, 0, None, 40);
    assert_eq!(
        (clean["uncovered"].clone(), clean["startedAtMs"].clone()),
        (json!(0), json!(30))
    );
    let broken = coverage_fold(Some(&clean), "id77", 5, 0, None, 50);
    assert_eq!(
        broken["contiguous"],
        json!(false),
        "a call from the middle of a closed pass"
    );
    let health = health_body(
        Some(
            &json!({"coverage": clean.to_string(), "counters": json!({"owed_actor_deferred_total": 4, "owed_actor_deferred_done_total": 3}).to_string()}),
        ),
        1,
        true,
        &Value::Null,
    );
    assert_eq!(health["coverage"]["complete"], json!(true));
    assert_eq!(
        (health["deferred"].clone(), health["deferredDone"].clone()),
        (json!(4), json!(3))
    );
    // L6
    let actor = squash(include_str!("../owed_actor.rs"));
    let run = &actor[actor.find("pubasyncfnrun_deferred(").unwrap()..];
    let started = run.find("COUNTERS[13]").expect("counted at the start");
    assert!(started < run.find("fordinitems").unwrap());
    assert!(run.find("COUNTERS[16]").unwrap() > run.find("fordinitems").unwrap());
}

// ---- D2-ACTOR-B-FOLD-4: the second delta lens (bsv-low `D2-ACTOR-B-delta-2-2026-10-28.md`) ----

/// The outpoints the D1 read (`owed_read::read`, D1's rows) serves as presses.
fn fold4_d1_presses(d1: &SqliteSql) -> Vec<String> {
    let s = futures_executor_block(crate::owed_read::read(d1, "owner")).unwrap();
    let b = s.body("owner", Some(TIP), &BTreeMap::new()).unwrap();
    b["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["facts"]["claimable"] == json!(true))
        .map(|r| r["outpoint"].as_str().unwrap().to_string())
        .collect()
}

/// NEW2-M1 (the lens's `lens2_p5`): a SECOND Phase C epoch. Rollback 1's pass ran to its stamp (its cursor row at the
/// end); Phase C again, a stranger spends `b3..00:0`, the actor downgrades it and its marks are acknowledged; rollback
/// 2's pass must re-judge every identity before it stamps, and the D1 read then serves no press over the spent hop.
/// RED on `e3e70b65`: one call answers `stamped true games 0 rows 0` and the D1 read serves `b3..00:0` as a press.
#[tokio::test]
async fn d2_actor_b_fold4_m1_a_second_rollback_rejudges_every_identity_before_it_stamps() {
    let w = WithActor {
        d1: d1_judge(&stranded()).await,
        actor: mirror(),
        d1_judges: false,
        event: Some(("pot", 7)),
        actor_reads: true,
        actor_fails: Cell::new(false),
    };
    let games = games_of(&w.d1, "owner");
    // Phase C epoch 1, then rollback 1 (render 1, the pass to its stamp).
    w.d1.conn
        .execute(
            "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('owner',?1,'age','age',1)",
            [&games[0]],
        )
        .unwrap();
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    let w = WithActor {
        d1_judges: true,
        ..w
    };
    while !owed_v3_pass(&w, NOW_MS as i64, 16).await.unwrap().stamped {}
    assert!(
        fold4_d1_presses(&w.d1).contains(&NEW_H1_DOWN.to_string()),
        "rollback 1 serves the re-judged press"
    );
    // Phase C epoch 2: the spend, judged by the actor alone, its marks acknowledged.
    let w = WithActor {
        d1_judges: false,
        event: Some(("pot", 9)),
        ..w
    };
    w.d1.conn.execute_batch(SPENT_BY_A_STRANGER).unwrap();
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    assert_eq!(new_h1_marks(&w), 0, "epoch 2's marks acknowledged");
    // Rollback 2, render 1: the pass to its stamp; it re-judges every identity first.
    let w = WithActor {
        d1_judges: true,
        ..w
    };
    let mut games_judged = 0;
    loop {
        let r = owed_v3_pass(&w, NOW_MS as i64, 256).await.unwrap();
        games_judged += r.games;
        if r.stamped {
            break;
        }
    }
    // Render 2: the D1 read.
    let p = fold4_d1_presses(&w.d1);
    assert!(
        games_judged >= games.len() && !p.contains(&NEW_H1_DOWN.to_string()),
        "rollback 2's pass stamped having re-judged {games_judged} games; the D1 read serves {p:?}"
    );
}

/// NEW2-L1 (the lens's P6): in Phase B a mark written while the actor applies is judged by D1 and left standing for
/// the actor's next apply; the compare's DIFF on its game is the designed state, counted APART
/// (`owed_actor_compare_held_total`, served `/health.owedActor.heldDiffsTotal`), `owed_actor_diffs_total` flat, and
/// its log line names the class. RED on `e3e70b65`: `owed_actor_diffs_total` 0 -> 1.
#[tokio::test]
async fn d2_actor_b_fold4_l1_a_held_marks_diff_is_counted_apart() {
    let w = WithActor {
        d1: d1_judge(&stranded()).await,
        actor: mirror(),
        d1_judges: true,
        event: Some(("pot", 7)),
        actor_reads: true,
        actor_fails: Cell::new(false),
    };
    let games = games_of(&w.d1, "owner");
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    let diffs = w.d1.counter("owed_actor_diffs_total");
    let w = WithActor {
        event: Some(("pot", 8)),
        ..w
    };
    w.d1.conn
        .execute(
            "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('owner',?1,'age','age',1)",
            [&games[0]],
        )
        .unwrap();
    NEW_H1_INJECT.with(|c| c.set(true));
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    let lines: Vec<String> =
        w.d1.lines
            .borrow()
            .iter()
            .filter(|l| l.contains("DIFF"))
            .cloned()
            .collect();
    assert_eq!(
        w.d1.counter("owed_actor_diffs_total"),
        diffs,
        "a held mark's DIFF counted as a DIFF: {lines:?}"
    );
    assert_eq!(w.d1.counter("owed_actor_compare_held_total"), 1);
    assert!(
        lines
            .iter()
            .any(|l| l.contains("held mark") && l.contains("NEW2-L1")),
        "{lines:?}"
    );
    // The next delivery applies the mark in the actor: no DIFF of either class.
    let w = WithActor {
        event: Some(("pot", 9)),
        ..w
    };
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    assert_eq!(w.d1.counter("owed_actor_diffs_total"), diffs);
    assert_eq!(w.d1.counter("owed_actor_compare_held_total"), 1);
}

/// NEW2-L4: `digest` and `shape` are stamped only when every statement of the plan answered: a plan with one failing
/// `CREATE INDEX` stamps nothing (the next ensure computes the same plan and retries it), the same plan whole stamps
/// both. New API; RED with `ensure_schema` grafted to the base's rule (stamp after the loop whatever answered):
/// `left: 2 right: 0`. Amended by the third delta lens's LOW-2: a failed `CREATE INDEX` stamps `shape` (every table
/// statement answered) and never the digest; a failed TABLE statement stamps neither.
#[test]
fn d2_actor_b_fold4_l4_a_failed_plan_statement_stamps_nothing() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch(META_CREATE).unwrap();
    let held = Some("an-older-digest");
    let plan = object::schema_plan_kept(held, Some(actor_shape().as_str()), &[]);
    assert!(!plan.is_empty(), "an index-only move");
    let mut failing = plan.clone();
    failing.push("CREATE INDEX idx_nowhere ON no_such_table(x)".into());
    let stamps = |conn: &rusqlite::Connection| -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM meta WHERE name IN ('schema','shape')",
            [],
            |r| r.get(0),
        )
        .unwrap()
    };
    let mut lines = Vec::new();
    let mut table_failing = plan.clone();
    table_failing.push("CREATE TABLE no_such_schema.t(x)".into());
    let whole = object::ensure_schema(
        &table_failing,
        |s| conn.execute_batch(s).map_err(|e| e.to_string()),
        |l| lines.push(l),
        |n, v| {
            conn.execute(META_PUT_SQL, [n, v]).unwrap();
        },
    );
    assert!(!whole);
    assert_eq!(
        stamps(&conn),
        0,
        "a failed table statement stamped: {lines:?}"
    );
    assert!(lines.iter().any(|l| l.contains("NOT stamped")));
    let whole = object::ensure_schema(
        &failing,
        |s| conn.execute_batch(s).map_err(|e| e.to_string()),
        |l| lines.push(l),
        |n, v| {
            conn.execute(META_PUT_SQL, [n, v]).unwrap();
        },
    );
    assert!(!whole);
    assert_eq!(stamps(&conn), 1, "only the shape: {lines:?}");
    let schema: Option<String> = conn
        .query_row("SELECT value FROM meta WHERE name='schema'", [], |r| {
            r.get(0)
        })
        .ok();
    assert_eq!(schema, None, "a failed index stamped the digest");
    let whole = object::ensure_schema(
        &plan,
        |s| conn.execute_batch(s).map_err(|e| e.to_string()),
        |_| {},
        |n, v| {
            conn.execute(META_PUT_SQL, [n, v]).unwrap();
        },
    );
    assert!(whole);
    assert_eq!(stamps(&conn), 2);
    let digest: String = conn
        .query_row("SELECT value FROM meta WHERE name='schema'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(digest, actor_digest());
    let object = include_str!("object.rs")
        .split_whitespace()
        .collect::<String>();
    let local = &object[object.find("fnlocal(&self)->DoSql").unwrap()..];
    let local = &local[..local.find("fnrearm_after_rebuild").unwrap()];
    assert!(
        local.contains("ensure_schema("),
        "local() runs the plan through ensure_schema"
    );
    assert!(
        !local.contains("SqlStorageValue::from(actor_digest())"),
        "no stamp outside it"
    );
}

/// NEW2-L5: one game of three faults deterministically in the tip alarm's window: `tipJudged` moves past the window,
/// the other two are applied once and never re-applied for it, the faulting game is carried and retried, parked at its
/// third fault and then left out; each alarm with a fault counts once and names the game; an apply that lands by
/// another road clears it. New API; RED against two revert mutants: the park removed ("alarm 3: once per alarm with a
/// fault", `left: 1 right: 0`: the faulting game re-applied on every alarm, NEW2-L5's shape) and the per-game split
/// removed (`{"a": 3, "b": 3, "c": 3}`, `left: 3 right: 2`: one faulting game carries its whole window, the base's).
#[tokio::test]
async fn d2_actor_b_fold4_l5_a_faulting_tip_game_never_holds_the_window() {
    // Amended by the third delta lens's LOW-1: a fault counts toward the park only in an alarm where another apply
    // landed, so each alarm opens one more gate (`d`, `e`, `f`) beside the carried `b`.
    let gates: [(&str, i64); 6] = [
        ("a", 101),
        ("b", 102),
        ("c", 103),
        ("d", 104),
        ("e", 105),
        ("f", 106),
    ];
    let applied = std::cell::RefCell::new(BTreeMap::<String, usize>::new());
    let mut last = 100i64;
    let mut held = BTreeMap::new();
    let mut counted = 0;
    let mut lines = Vec::new();
    for alarm in 0..4 {
        let tip = 103 + alarm;
        let selected: Vec<String> = gates
            .iter()
            .filter(|(_, h)| *h > last && *h <= tip)
            .map(|(g, _)| g.to_string())
            .collect();
        let w = tip_window("owner", selected, &held, tip, |chunk: Vec<String>| {
            for g in &chunk {
                *applied.borrow_mut().entry(g.clone()).or_default() += 1;
            }
            let ok = !chunk.iter().any(|g| g == "b");
            async move { ok }
        })
        .await;
        last = w.tip_judged;
        held = w.faults.clone();
        counted += w.counted;
        lines.extend(w.line.clone());
        assert_eq!(
            w.counted,
            u64::from(alarm < 3),
            "alarm {alarm}: once per alarm with a fault"
        );
    }
    let applied = applied.into_inner();
    assert_eq!(
        applied["a"], 2,
        "A: its chunk and its single, once: {applied:?}"
    );
    assert_eq!(applied["c"], 2, "C: once: {applied:?}");
    assert_eq!(
        applied["b"], 6,
        "B: its chunk and its single on three alarms, then parked: {applied:?}"
    );
    assert_eq!(
        (applied["d"], applied["e"], applied["f"]),
        (2, 2, 1),
        "{applied:?}"
    );
    assert_eq!(last, 106, "tipJudged moved with every alarm");
    assert_eq!(held.get("b"), Some(&TIP_FAULTS_MAX), "parked");
    assert_eq!(counted, 3);
    assert!(lines.iter().all(|l| l.contains("[\"b\"]")), "{lines:?}");
    assert!(lines[2].contains("parked [\"b\"]"), "{lines:?}");
    // A landed apply by another road clears it (the object's `apply_route`): the next window applies it again.
    held.remove("b");
    let w = tip_window("owner", Vec::new(), &held, 107, |_| async { true }).await;
    assert!(w.faults.is_empty() && w.line.is_none());
    let object = include_str!("object.rs")
        .split_whitespace()
        .collect::<String>();
    let alarm = &object[object.find("asyncfnalarm(").unwrap()..];
    assert!(alarm.contains("tip_window(&identity,games,&held,"));
    assert!(alarm.contains("COUNTERS[18]"));
    let route = &object[object.find("asyncfnapply_route(").unwrap()..];
    // D2-ACTOR-B-FOLD-7: the landed apply's clear is `clear_tip_faults` (the drain's held events take it too).
    assert!(route[..route.find("answer_of(&report").unwrap()]
        .contains("self.clear_tip_faults(&local,&body.games);"));
    let clear = &object[object.find("fnclear_tip_faults(").unwrap()..];
    assert!(clear[..clear.find("fnpublish_pushes(").unwrap()].contains("META_TIP_FAULTS"));
}

/// NEW2-L3: a full minute's take writes nothing: the 61st take answers no row (the page waits) and leaves the row's
/// count at 60 and its change count 0; the next minute counts from one. RED with the base's take (an unconditional
/// upsert): `left: 1 right: 0` (the waiter's take wrote the row).
#[test]
fn d2_actor_b_fold4_l3_a_waiters_take_writes_nothing() {
    assert!(BOOT_BUDGET_TAKE_SQL.contains(&format!("<{BOOTSTRAP_PAGES_PER_MINUTE} ")));
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE owed_belt_state (name TEXT PRIMARY KEY, cursor TEXT, tickedAtMs INTEGER)",
    )
    .unwrap();
    let t0 = 1_800_000_000_000i64;
    let take = |now: i64| -> (Vec<Value>, u64) {
        let rows: Vec<Value> = conn
            .prepare(BOOT_BUDGET_TAKE_SQL)
            .unwrap()
            .query_map([now], |r| Ok(json!({"taken": r.get::<_, i64>(0)?})))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        (rows, conn.changes())
    };
    for n in 1..=BOOTSTRAP_PAGES_PER_MINUTE {
        assert_eq!(boot_taken(&take(t0).0), n);
    }
    let (rows, changes) = take(t0 + 1);
    assert_eq!(changes, 0, "the waiter's take wrote the row");
    assert!(rows.is_empty());
    assert!(boot_pace(boot_taken(&rows), t0 as u64, "02ab").is_some());
    let count: String = conn
        .query_row("SELECT cursor FROM owed_belt_state", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, "60");
    assert_eq!(
        boot_taken(&take(t0 + 60_000).0),
        1,
        "a new minute counts from one"
    );
}

/// The third delta lens's LOW-1 (its `lens3_pd`): a window of 130 games and three alarms in which EVERY apply faults
/// (the actor's D1 record unreachable): the fault is the actor's, not a game's, so no game is parked, each alarm's
/// fault is still counted once and its line says so; when the fault clears, every carried game is applied. RED on
/// `5a88734d` (the count moved in every alarm): `parked after three whole-actor faults: left: 130 right: 0`.
#[tokio::test]
async fn d2_actor_b_fold5_l1_a_whole_actor_fault_parks_no_game() {
    let games: Vec<String> = (0..130).map(|i| format!("g{i:03}")).collect();
    let mut held = BTreeMap::new();
    let mut counted = 0;
    let mut parked_counted = 0;
    let mut lines = Vec::new();
    for alarm in 0..3 {
        let sel = if alarm == 0 {
            games.clone()
        } else {
            Vec::new()
        };
        let w = tip_window("owner", sel, &held, 200 + alarm, |_c: Vec<String>| async {
            false
        })
        .await;
        counted += w.counted;
        parked_counted += w.parked_counted;
        lines.extend(w.line.clone());
        held = w.faults;
    }
    let parked = held.values().filter(|n| **n >= TIP_FAULTS_MAX).count();
    assert_eq!(parked, 0, "parked after three whole-actor faults");
    assert_eq!(parked_counted, 0);
    assert_eq!(counted, 3, "the fault counted once an alarm");
    assert_eq!(held.len(), 130, "every game carried");
    assert!(
        lines.iter().all(|l| l.contains("no apply landed")),
        "{lines:?}"
    );
    let applied = std::cell::RefCell::new(Vec::<String>::new());
    let w = tip_window("owner", vec!["new".into()], &held, 300, |c: Vec<String>| {
        applied.borrow_mut().extend(c);
        async { true }
    })
    .await;
    assert!(applied.borrow().contains(&"g000".to_string()));
    assert_eq!(applied.borrow().len(), 131);
    assert!(w.faults.is_empty() && w.line.is_none());
    let object = include_str!("object.rs")
        .split_whitespace()
        .collect::<String>();
    let alarm = &object[object.find("asyncfnalarm(").unwrap()..];
    assert!(alarm.contains("(COUNTERS[19].into(),window.parked_counted)"));
    assert_eq!(COUNTERS[19], "owed_actor_tip_parked_total");
}

/// LOW-1's control: a game that faults ALONE among landed games is still parked at its third such alarm and counted
/// (`parked_counted` 1 in that alarm); a whole-actor alarm in between does not move its count.
#[tokio::test]
async fn d2_actor_b_fold5_l1_a_game_faulting_among_landed_games_is_parked() {
    let mut held = BTreeMap::new();
    let mut parked_counted = 0;
    for alarm in 0..4 {
        let sel = if alarm == 0 {
            vec!["a".to_string(), "b".to_string()]
        } else {
            vec![format!("n{alarm}")]
        };
        let whole_actor = alarm == 1;
        let w = tip_window("owner", sel, &held, 100 + alarm, |c: Vec<String>| {
            let ok = !whole_actor && !c.iter().any(|g| g == "b");
            async move { ok }
        })
        .await;
        if alarm == 1 {
            assert!(!w.landed);
            assert_eq!(
                w.faults.get("b"),
                Some(&1),
                "a whole-actor alarm moves no count"
            );
        }
        parked_counted += w.parked_counted;
        if alarm == 3 {
            assert_eq!(w.parked, vec!["b".to_string()]);
        }
        held = w.faults;
    }
    assert_eq!(held.get("b"), Some(&TIP_FAULTS_MAX), "parked");
    assert_eq!(parked_counted, 1, "counted");
}

/// The third delta lens's LOW-2: a REBUILD plan whose one `CREATE INDEX` fails stamps `shape` (every table statement
/// answered) and not the digest, so the next instance takes the INDEX-ONLY path: `bootstrapped` kept, no second
/// rebuild; the plan reports not whole (the caller counts `owed_actor_schema_partial_total`). RED on `5a88734d`
/// (nothing stamped): `the next instance rebuilds again`.
#[test]
fn d2_actor_b_fold5_l2_a_failed_index_on_a_rebuild_retries_as_index_only() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch(META_CREATE).unwrap();
    for (n, v) in [
        ("schema", "an-older-digest"),
        ("shape", "an-older-shape"),
        ("identity", "owner"),
        ("bootstrapped", "1"),
    ] {
        conn.execute(META_PUT_SQL, [n, v]).unwrap();
    }
    let meta = |conn: &rusqlite::Connection, name: &str| -> Option<String> {
        conn.query_row(META_GET_SQL, [name], |r| r.get(0)).ok()
    };
    let plan = object::schema_plan_kept(Some("an-older-digest"), Some("an-older-shape"), &[]);
    assert!(
        plan.iter().any(|s| s.starts_with("DROP TABLE")),
        "a rebuild"
    );
    let failing = plan
        .iter()
        .find(|s| s.trim_start().starts_with("CREATE INDEX"))
        .cloned()
        .unwrap();
    let mut lines = Vec::new();
    let whole = object::ensure_schema(
        &plan,
        |s| {
            if s == failing {
                return Err("SQLITE_ERROR: refused".into());
            }
            conn.execute_batch(s).map_err(|e| e.to_string())
        },
        |l| lines.push(l),
        |n, v| {
            conn.execute(META_PUT_SQL, [n, v]).unwrap();
        },
    );
    assert!(!whole, "not whole: the caller counts it");
    assert_eq!(meta(&conn, "schema").as_deref(), Some("an-older-digest"));
    // The rebuild ran once; its bootstrap ends and stamps `bootstrapped`.
    conn.execute(META_PUT_SQL, ["bootstrapped", "1"]).unwrap();
    // The next instance.
    let held_indexes: Vec<String> = {
        let mut st = conn.prepare(object::HELD_INDEXES_SQL).unwrap();
        st.query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    };
    let shape = meta(&conn, "shape");
    let next = object::schema_plan_kept(
        meta(&conn, "schema").as_deref(),
        shape.as_deref(),
        &held_indexes,
    );
    assert!(
        !next
            .iter()
            .any(|s| s.starts_with("DROP TABLE") || s.starts_with("DELETE FROM meta")),
        "the next instance rebuilds again: {lines:?}"
    );
    let whole = object::ensure_schema(
        &next,
        |s| conn.execute_batch(s).map_err(|e| e.to_string()),
        |_| {},
        |n, v| {
            conn.execute(META_PUT_SQL, [n, v]).unwrap();
        },
    );
    assert!(whole);
    assert_eq!(meta(&conn, "bootstrapped").as_deref(), Some("1"), "kept");
    assert_eq!(meta(&conn, "schema"), Some(actor_digest()));
    let object = include_str!("object.rs")
        .split_whitespace()
        .collect::<String>();
    let local = &object[object.find("fnlocal(&self)->DoSql").unwrap()..];
    assert!(local.contains("if!whole{*self.schema_partial.borrow_mut()=true;}"));
    let rearm = &object[object.find("asyncfnrearm_after_rebuild").unwrap()..];
    assert!(rearm.contains("(COUNTERS[20].into(),1)"));
    assert_eq!(COUNTERS[20], "owed_actor_schema_partial_total");
}

// ---- D2-ACTOR-B-FOLD-6: the land tier's refusal (`health-dirty-snapshot`: 49 reads, 6 statements, largest 44) ----

/// A fleet-shaped record for the health read: 600 identities holding owed rows, the actor's 21 counters, the three
/// class rows and a coverage row.
fn fold6_fleet() -> SqliteSql {
    let db = SqliteSql::migrated();
    for i in 0..600 {
        db.conn
            .execute(
                "INSERT INTO owed_state(identity,computedAtMs,tip,rows,stale,truncated,recomputeSeq) VALUES (?1,1,NULL,3,0,0,1)",
                [format!("02{i:064x}")],
            )
            .unwrap();
    }
    for (i, c) in COUNTERS.iter().enumerate() {
        db.conn
            .execute(
                "INSERT INTO ops_counters(name,value) VALUES (?1,?2) ON CONFLICT(name) DO UPDATE SET value=excluded.value",
                rusqlite::params![c, 10 + i as i64],
            )
            .unwrap();
    }
    for class in BAR_CLASSES {
        db.conn
            .execute(
                "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES (?1,?2,5)",
                [
                    bars_state_name(class),
                    merge_bars_row(
                        None,
                        &[100, 200, 300],
                        &[40, 50],
                        Some(Some(1_000)),
                        lib_rev(),
                    ),
                ],
            )
            .unwrap();
    }
    let cov = coverage_fold(None, "", 600, 0, None, 7);
    db.conn
        .execute(
            "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES (?1,?2,7)",
            [COVERAGE_NAME.to_string(), cov.to_string()],
        )
        .unwrap();
    db
}

/// One statement's cost on SQLite, D1's engine: the rows it answered and the rows a full scan stepped through.
fn fold6_cost(db: &SqliteSql, sql: &str) -> (usize, i32) {
    let mut st = db.conn.prepare(sql).unwrap();
    let n = st.query_map([], |_| Ok(())).unwrap().count();
    (n, st.get_status(rusqlite::StatementStatus::FullscanStep))
}

/// The health route's owed blocks read within the tier's ceilings (7 reads, 5 statements, the largest 5): the
/// statements the route's `health` names, each a primary-key read of at most 5 rows with no scan, three in all (main's
/// D2-ACTOR-A-FOLD-5 count). RED on `7b455de3`: the route names `owed_actor::HEALTH_SQL`, a fourth statement whose
/// `owed_state` count scans the fleet (600 rows here, the tier's 44).
#[tokio::test]
async fn d2_actor_b_fold6_the_health_read_is_one_snapshot_row_within_the_ceilings() {
    let db = fold6_fleet();
    let routes = include_str!("../routes.rs");
    let route = &routes[routes.find("pub async fn health(").unwrap()..];
    let route = &route[..route.find("\n}\n").unwrap()];
    let named: Vec<(&str, &str)> = [
        (
            "owed_judge::dirty_health",
            crate::owed_judge::DIRTY_HEALTH_SQL,
        ),
        (
            "owed_events::health",
            crate::owed_events::HEALTH_CONSUMER_SQL,
        ),
        ("owed_events::health", crate::owed_events::HEALTH_TOTALS_SQL),
        ("owed_actor::HEALTH_SQL", HEALTH_SQL),
    ]
    .into_iter()
    .filter(|(name, _)| route.contains(name))
    .collect();
    let costs: Vec<(usize, i32)> = named.iter().map(|(_, sql)| fold6_cost(&db, sql)).collect();
    assert!(
        named.len() <= 3 && costs.iter().all(|&(rows, scan)| rows <= 5 && scan == 0),
        "the health route's owed statements {:?}: (rows, scanned) {costs:?}",
        named.iter().map(|n| n.0).collect::<Vec<_>>()
    );
    assert!(route.contains("owed_events::health_and_actor"));
    // The snapshot is ONE of the consumer statement's rows.
    db.run_batch(vec![health_snapshot_stmt(9, true)])
        .await
        .unwrap();
    assert_eq!(
        fold6_cost(&db, crate::owed_events::HEALTH_CONSUMER_SQL),
        (1, 0)
    );
}

/// The snapshot's cadence: two writes inside a minute leave one row, the second skipped (no composition run: no scan,
/// a few VM steps); past the minute it is rewritten; `force` writes whatever the stamp. Each writer carries it in its
/// own batch. RED on `7b455de3`: no snapshot statement exists (the route read the composition per request).
#[tokio::test]
async fn d2_actor_b_fold6_the_snapshot_is_written_at_most_once_a_minute() {
    let db = fold6_fleet();
    let snap = |db: &SqliteSql| -> Value {
        db.conn
            .query_row(
                "SELECT cursor FROM owed_belt_state WHERE name=?1",
                [HEALTH_SNAPSHOT_NAME],
                |r| r.get::<_, String>(0),
            )
            .map(|c| serde_json::from_str(&c).unwrap())
            .unwrap_or(Value::Null)
    };
    let compared = |v: &Value| health_body(Some(v), 0, true, &Value::Null)["comparedTotal"].clone();
    db.run_batch(vec![health_snapshot_stmt(1_000_000, false)])
        .await
        .unwrap();
    assert_eq!(snap(&db)["asOfMs"], json!(1_000_000));
    assert_eq!(compared(&snap(&db)), json!(10));
    db.conn
        .execute(
            "UPDATE ops_counters SET value=41 WHERE name=?1",
            [COUNTERS[0]],
        )
        .unwrap();
    // The second write inside the minute: skipped, and it runs none of the composition (no scan of `owed_state`).
    let mut st = db.conn.prepare(HEALTH_SNAPSHOT_SQL).unwrap();
    let changed = st
        .execute(rusqlite::params![
            1_030_000i64,
            HEALTH_SNAPSHOT_EVERY_MS as i64
        ])
        .unwrap();
    let (scan, steps) = (
        st.get_status(rusqlite::StatementStatus::FullscanStep),
        st.get_status(rusqlite::StatementStatus::VmStep),
    );
    drop(st);
    assert_eq!(
        (changed, scan),
        (0, 0),
        "skipped, nothing read past the stamp"
    );
    assert!(steps < 100, "the skipped write ran {steps} VM steps");
    let one: i64 = db
        .conn
        .query_row(
            "SELECT COUNT(*) FROM owed_belt_state WHERE name=?1",
            [HEALTH_SNAPSHOT_NAME],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(one, 1);
    assert_eq!(
        (snap(&db)["asOfMs"].clone(), compared(&snap(&db))),
        (json!(1_000_000), json!(10))
    );
    // Past the minute: rewritten.
    db.run_batch(vec![health_snapshot_stmt(1_061_000, false)])
        .await
        .unwrap();
    assert_eq!(
        (snap(&db)["asOfMs"].clone(), compared(&snap(&db))),
        (json!(1_061_000), json!(41))
    );
    // Forced (the coverage pass's last call): written at once.
    db.run_batch(vec![health_snapshot_stmt(1_062_000, true)])
        .await
        .unwrap();
    assert_eq!(snap(&db)["asOfMs"], json!(1_062_000));
    // The read serves it.
    let (_, actor) = crate::owed_events::health_and_actor(&db, &Value::Null).await;
    assert_eq!(actor.unwrap()["asOfMs"], json!(1_062_000));
    // Each writer carries the snapshot in its own batch; no read writes it.
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let actor_src = squash(include_str!("../owed_actor.rs"));
    let flush = &actor_src[actor_src.find("pubasyncfnflush_bars_from(").unwrap()..];
    assert!(flush[..flush.find("Ok(())").unwrap()]
        .contains("stmts.push(health_snapshot_stmt(now,false));"));
    let events = squash(include_str!("../owed_events.rs"));
    assert!(
        events.contains("batch_row.push(crate::owed_actor::health_snapshot_stmt(nowasu64,false));")
    );
    let object = squash(include_str!("object.rs"));
    assert!(object.contains(
        "crate::owed_actor::health_snapshot_stmt(Date::now().as_millis(),next.is_none()),"
    ));
    let routes = squash(include_str!("../routes.rs"));
    assert!(
        !routes.contains("health_snapshot_stmt"),
        "a read never writes it"
    );
}

/// The served body's KEYS are the pre-fold body's (the harness's bars cell and the cutover doc read them by name),
/// with the same values on a fixture, plus `asOfMs`; no snapshot yet serves them null. RED on `7b455de3`: no
/// `asOfMs`, and a missing read served the totals 0.
#[tokio::test]
async fn d2_actor_b_fold6_the_served_keys_are_the_pre_fold_bodys() {
    let db = fold6_fleet();
    let pre = db
        .run_batch(vec![Stmt::new(HEALTH_SQL, vec![])])
        .await
        .unwrap()
        .remove(0)
        .rows
        .remove(0);
    let before = health_body(Some(&pre), 2_000, true, &json!(99));
    db.run_batch(vec![health_snapshot_stmt(1_500, true)])
        .await
        .unwrap();
    let (_, snap) = crate::owed_events::health_and_actor(&db, &Value::Null).await;
    let after = health_body(snap.as_ref(), 2_000, true, &json!(99));
    let keys = |v: &Value| {
        v.as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
    };
    let mut want = keys(&before);
    want.insert("asOfMs".into());
    assert_eq!(keys(&after), want);
    for k in keys(&before) {
        if k != "isolate" && k != "asOfMs" {
            assert_eq!(after[&k], before[&k], "{k}");
        }
    }
    assert_eq!(after["asOfMs"], json!(1_500));
    for k in [
        "applyMsP50",
        "applyMsP95",
        "applyCeilingMs",
        "rowsVisibleMsP95",
        "collectAfterMinedMsP95",
        "oldestUnappliedEventAgeMs",
        "kvCopies",
        "bootstraps",
        "reconcileDiffs",
        "comparedTotal",
        "diffsTotal",
        "faultsTotal",
        "heldDiffsTotal",
        "tipFaults",
        "tipParked",
        "schemaPartial",
        "coverage",
        "epochResets",
        "deferred",
        "deferredDone",
        "activeIdentities",
        "unknownReads",
        "v3",
        "classes",
        "isolate",
        "healthRead",
        "asOfMs",
    ] {
        assert!(after.get(k).is_some(), "{k}");
    }
    assert_eq!(after["comparedTotal"], json!(10));
    assert_eq!(after["activeIdentities"], json!(600));
    assert_eq!(after["coverage"]["complete"], json!(true));
    let none = health_body(None, 2_000, true, &json!(99));
    assert_eq!(keys(&none), keys(&after));
    for k in [
        "asOfMs",
        "comparedTotal",
        "diffsTotal",
        "bootstraps",
        "coverage",
        "activeIdentities",
    ] {
        assert_eq!(none[k], Value::Null, "{k}");
    }
}

// ---- D2-ACTOR-B-FOLD-7: an unseen identity's first event, held and bootstrapped (beta's first window) ----

/// The fleet's heaviest seat, scaled (beta's pair 20 proof seats: hundreds of games each): `n` games, each a hop
/// marker of `owner` whose hop's CHANGE funds the next game's hop (`pot_records` `(H_k, 1)` spent by `H_{k+1}`, the
/// seat's ordinary chain of coins) and whose stake a covenant JOIN spends (`(H_k, 0)` spent by `P_k`). The chain is
/// what a cold closure walked one link a round in both directions (the change's spender, then that spender's own
/// outputs, then the outputs it spends) until the 8 rounds ran out.
fn fold7_chained_seat(n: usize) -> Fixture {
    let made = (NOW_MS / 1000) as i64 - 7_200;
    let mut seed = Vec::new();
    for k in 0..n {
        let hop = h("b8", k);
        let pot = h("c8", k);
        let next = if k + 1 < n {
            format!("1,'{}'", h("b8", k + 1))
        } else {
            "0,NULL".into()
        };
        seed.push(format!("INSERT INTO pot_records(txid,outputIndex,spent,spendingTxid,createdAt,lockKind) VALUES ('{hop}',0,1,'{pot}',{},'p2pkh')", made + k as i64));
        seed.push(format!("INSERT INTO pot_records(txid,outputIndex,spent,spendingTxid,createdAt,lockKind) VALUES ('{hop}',1,{next},{},'p2pkh')", made + k as i64));
        seed.push(format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind,paramsDecoded,recoveryHeight) VALUES ('{pot}',0,0,{},'covenant',1,900100)", made + k as i64));
        seed.push(format!("INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('owner','other','{}',0,20190,'seat','00','00',2,'{hop}',2,{},1)", h("9e", k), made + k as i64));
        seed.push(format!("INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt,sigValid) VALUES ('owner','other','{}','{pot}',0,900100,'00','{}',1,{},1)", h("9e", k), h("e7", k), made + k as i64));
    }
    Fixture {
        name: "fold7 chained seat",
        seed,
        identities: vec!["owner".into()],
    }
}

/// The bootstrap run to its end the way the object's alarm runs it (`bootstrap_step` from the first cursor, each page
/// after the last), answering the pages and the drain. A page's closure-bound fault is retried as the alarm retries
/// it (`retries` counts them); any other fault fails the pin.
async fn fold7_bootstrap(
    turn: &futures_util::lock::Mutex<()>,
    record: &SqliteSql,
    local: &SqliteSql,
    me: &str,
) -> (usize, usize, Drain) {
    let mut cursor = object::BOOT_CURSOR_START.to_string();
    let (mut pages, mut retries) = (0, 0);
    let mut drained = Drain::default();
    let mut alarms = 0usize;
    loop {
        match bootstrap_step(
            turn,
            record,
            local,
            &Fakes,
            me,
            Some(&cursor),
            Some(TIP),
            None,
            false,
        )
        .await
        {
            Ok(BootStep::Page {
                cursor: next,
                games,
            }) => {
                assert_eq!(games, BOOTSTRAP_PAGE_GAMES);
                pages += 1;
                cursor = next;
            }
            // FOLD-8 (NEW4-L1): a drain past its per-alarm bound goes on at the next alarm, cursor `drain`.
            Ok(BootStep::Draining { drain }) => {
                drained.applied.extend(drain.applied);
                drained.pushes.extend(drain.pushes);
                cursor = BOOT_CURSOR_DRAIN.to_string();
                alarms += 1;
            }
            Ok(BootStep::Done { mut drain, .. }) => {
                drained.applied.append(&mut drain.applied);
                drained.applied.sort_by_key(|e| e.seq.unwrap_or(0));
                drain.applied = drained.applied;
                let _ = alarms;
                return (pages + 1, retries, drain);
            }
            Err(e) if is_closure_bound(&e) && retries < 8 => {
                eprintln!("fold7 retry {retries}: {e}");
                retries += 1
            }
            Err(e) => panic!("bootstrap page {pages}: {e}"),
        }
    }
}

fn fold7_held(local: &SqliteSql) -> Vec<String> {
    let mut q = local
        .conn
        .prepare("SELECT name FROM meta WHERE name>='held:' AND name<'held;' ORDER BY name")
        .unwrap();
    q.query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn fold7_meta(local: &SqliteSql, name: &str) -> Option<String> {
    local
        .conn
        .query_row("SELECT value FROM meta WHERE name=?1", [name], |r| r.get(0))
        .ok()
}

fn fold7_event(kind: &str, seq: i64, games: &[String], at_ms: u64) -> HeldEvent {
    HeldEvent {
        kind: Some(kind.into()),
        seq: Some(seq),
        games: games.to_vec(),
        at_ms: Some(at_ms),
        mined: false,
        tip: Some(TIP),
        held_at_ms: at_ms,
    }
}

/// THE FINDING (beta, 2026-10-28, pair 20's proof seats) and the design's rule (section 6): an identity shaped like
/// the fleet's heaviest seat (600 games, 3,000 record rows, its hops chained by their change) whose actor holds no
/// `bootstrapped`: its first event is HELD inside the direct call's wall (no closure, no fault), the bootstrap pages
/// to its short page with no page faulting, the held event is applied in the drain, the rows are the D1 judge's
/// byte for byte, and the cold time is measured.
/// RED on `d910ad5d`: the same one-game event's apply over the cold mirror, `Err("owed actor: the closure of owner did
/// not close in 8 rounds (52 txids)")` (the probe run on that tree's `apply`, quoted in the commit); a 16-game page the
/// same (`... (210 txids)`).
#[tokio::test]
async fn d2_actor_b_fold7_a_an_unseen_heavy_seat_is_held_bootstrapped_then_applied() {
    let f = fold7_chained_seat(600);
    let d1 = d1_judge(&f).await;
    let want = stored(&d1, "owner");
    let record = seeded(&f);
    let games = games_of(&record, "owner");
    assert_eq!(games.len(), 600);
    let local = mirror();
    let turn = futures_util::lock::Mutex::new(());
    let t = std::time::Instant::now();
    let ev = fold7_event("pot", 9, &games[300..301], NOW_MS - 1_000);
    let first = apply_or_hold(&turn, &record, &local, &Fakes, "owner", &ev, None, false)
        .await
        .unwrap();
    let held_ms = t.elapsed().as_millis() as u64;
    assert!(
        matches!(first, Applied::Held { rearmed: false }),
        "{first:?}"
    );
    assert!(held_ms < DIRECT_CALL_WALL_MS, "held in {held_ms} ms");
    assert_eq!(fold7_held(&local).len(), 1);
    assert!(
        stored(&local, "owner").is_empty(),
        "no closure over the empty mirror"
    );
    let t = std::time::Instant::now();
    let (pages, retries, drain) = fold7_bootstrap(&turn, &record, &local, "owner").await;
    let boot_ms = t.elapsed().as_millis() as u64;
    assert_eq!(retries, 0, "no bootstrap page met the closure's bound");
    assert_eq!(pages, 600 / BOOTSTRAP_PAGE_GAMES + 1);
    assert_eq!(drain.applied.len(), 1);
    assert_eq!(drain.applied[0], ev);
    assert!(fold7_held(&local).is_empty());
    assert!(fold7_meta(&local, "bootstrapped").is_some());
    let got = stored(&local, "owner");
    assert_eq!(got.len(), want.len());
    assert_eq!(got, want, "the actor's rows are the D1 judge's");
    let cold = drain.cold_ms.unwrap();
    // The amendment's item 9: the cold time under the pace as it stands (one page a second, 16 games a page) is the
    // pages' delay plus their work; the native work is measured here, the wasm32 and D1 hop is the beta rehearsal's.
    let paced = (pages as u64 - 1) * BOOTSTRAP_PAGE_DELAY_MS + boot_ms;
    eprintln!("fold7_a: held in {held_ms} ms; {pages} pages, native work {boot_ms} ms; cold (the fixture's clock) {cold} ms; under the pace about {paced} ms");
    // D2-ACTOR-B-FOLD-8 (the fourth delta lens's NOTE-1): the cold bar asserted ONCE, as what it measures: ONE cold
    // actor alone under the pace (its 38 pages one page delay apart) plus this process's measured native work is under
    // the design's 60 s. It is not the wasm32 and D1 time (beta's rehearsal reads that), and not a mass bootstrap's,
    // where the fleet's 60 pages a minute make a heavy seat's cold time the fleet's page count over 60 a minute.
    assert_eq!(pages, 38);
    assert!(paced < 60_000, "one cold actor under the pace: {paced} ms");
}

/// Two HELD events out of order (seq 12 held first, then seq 10, the record moved between them): the drain applies
/// them in seq order and the rows are the record's as it stands (the newer judgement), MEDIUM-1's shape.
#[tokio::test]
async fn d2_actor_b_fold7_b_two_held_events_apply_in_seq_order_and_derive_the_newer_rows() {
    let f = fold7_chained_seat(20);
    let record = seeded(&f);
    let games = games_of(&record, "owner");
    let local = mirror();
    let turn = futures_util::lock::Mutex::new(());
    let late = fold7_event("pot", 12, &games[3..4], NOW_MS - 500);
    let early = fold7_event("hop", 10, &games[3..4], NOW_MS - 900);
    for ev in [&late, &early] {
        let r = apply_or_hold(&turn, &record, &local, &Fakes, "owner", ev, None, false)
            .await
            .unwrap();
        assert!(matches!(r, Applied::Held { rearmed: false }));
        // The record moves between the holds: game 3's JOIN is spent by a settle.
        record.conn.execute_batch(&format!("UPDATE pot_records SET spent=1,spendingTxid='{}' WHERE txid='{}' AND outputIndex=0", h("f1", 3), h("c8", 3))).unwrap();
    }
    let (_, _, drain) = fold7_bootstrap(&turn, &record, &local, "owner").await;
    let seqs: Vec<Option<i64>> = drain.applied.iter().map(|d| d.seq).collect();
    assert_eq!(seqs, vec![Some(10), Some(12)], "applied in seq order");
    let reference = seeded(&f);
    reference
        .conn
        .execute_batch(&format!(
            "UPDATE pot_records SET spent=1,spendingTxid='{}' WHERE txid='{}' AND outputIndex=0",
            h("f1", 3),
            h("c8", 3)
        ))
        .unwrap();
    for chunk in games.chunks(16) {
        crate::owed_judge::judge_subjects(&reference, "owner", chunk, "d1")
            .await
            .unwrap();
    }
    assert_eq!(
        stored(&local, "owner"),
        stored(&reference, "owner"),
        "the newer rows"
    );
    // `applied` holds both events' tokens.
    let applied: i64 = local
        .conn
        .query_row(
            "SELECT COUNT(*) FROM applied WHERE seq IN (10,12)",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(applied, 2);
}

/// A BOOTSTRAPPED actor whose record grew under it past the closure's bound (a chain of word rows each naming the
/// next, 20 links: the 8 rounds cannot reach its end): the apply re-arms the bootstrap and HOLDS the event, never a
/// 503; the re-bootstrap's page persists each attempt's rounds and closes on a retry, then the held event is applied.
/// RED on `d910ad5d`: the apply answers `Err("owed actor: the closure of owner did not close in 8 rounds ...")`, the
/// object's 503.
#[tokio::test]
async fn d2_actor_b_fold7_c_a_bootstrapped_actor_past_the_bound_rearms_and_holds() {
    let f = fold7_chained_seat(20);
    let record = seeded(&f);
    let games = games_of(&record, "owner");
    let local = mirror();
    let turn = futures_util::lock::Mutex::new(());
    let (_, _, _) = fold7_bootstrap(&turn, &record, &local, "owner").await;
    assert!(fold7_meta(&local, "bootstrapped").is_some());
    // The record grows: game 5's hop names a chain of 20 word rows.
    let hop = h("b8", 5);
    let mut chain = vec![hop.clone()];
    chain.extend((0..20).map(|i| h("fa", i)));
    for w in chain.windows(2) {
        record.conn.execute(
            "INSERT OR REPLACE INTO tx_words(txid,word,tier,updated_at,legacy_body) VALUES (?1,'seen','hint',1,?2)",
            [&w[0], &format!("{{\"next\":\"{}\"}}", w[1])],
        ).unwrap();
    }
    let ev = fold7_event("word", 31, &games[5..6], NOW_MS - 100);
    let r = apply_or_hold(&turn, &record, &local, &Fakes, "owner", &ev, None, false)
        .await
        .unwrap();
    assert!(matches!(r, Applied::Held { rearmed: true }), "{r:?}");
    assert!(
        fold7_meta(&local, "bootstrapped").is_none(),
        "un-stamped: its reads serve the fallback"
    );
    assert_eq!(fold7_held(&local).len(), 1);
    let (_, retries, drain) = fold7_bootstrap(&turn, &record, &local, "owner").await;
    // Each attempt's rounds persist and the next starts from the mirror's own fixpoint (FOLD-7's local walk), so the
    // page closes within a retry or two.
    assert!(
        retries <= 2,
        "the re-bootstrap converges: {retries} retries"
    );
    assert_eq!(drain.applied.len(), 1);
    assert!(fold7_meta(&local, "bootstrapped").is_some());
    let reference = seeded(&f);
    for w in chain.windows(2) {
        reference.conn.execute(
            "INSERT OR REPLACE INTO tx_words(txid,word,tier,updated_at,legacy_body) VALUES (?1,'seen','hint',1,?2)",
            [&w[0], &format!("{{\"next\":\"{}\"}}", w[1])],
        ).unwrap();
    }
    for chunk in games.chunks(16) {
        crate::owed_judge::judge_subjects(&reference, "owner", chunk, "d1")
            .await
            .unwrap();
    }
    assert_eq!(stored(&local, "owner"), stored(&reference, "owner"));
}

/// The steady state: a bootstrapped actor's apply of one game of the heavy seat is ONE round (the closure keyed by the
/// games: no identity-wide marker key, no spend chain walked), and its record read is the game's, not the seat's.
#[tokio::test]
async fn d2_actor_b_fold7_d_the_steady_state_apply_is_one_round() {
    let f = fold7_chained_seat(600);
    let record = seeded(&f);
    let games = games_of(&record, "owner");
    let local = mirror();
    let turn = futures_util::lock::Mutex::new(());
    fold7_bootstrap(&turn, &record, &local, "owner").await;
    for g in [&games[0], &games[299], &games[599]] {
        let ev = fold7_event("pot", 40, std::slice::from_ref(g), NOW_MS);
        match apply_or_hold(&turn, &record, &local, &Fakes, "owner", &ev, None, false)
            .await
            .unwrap()
        {
            Applied::Done(r, _) => {
                assert_eq!(r.rounds, 1, "{g}: {r:?}");
                assert!(
                    r.record_rows < 20,
                    "{g}: the game's rows, not the seat's: {r:?}"
                );
            }
            Applied::Held { .. } => panic!("a bootstrapped actor held"),
        }
    }
}

/// The actor's answer to a held event, as the object answers it (`object::apply_route`), for the consumer's pins.
struct Holding<'a>(&'a WithActor);
impl SqlPort for Holding<'_> {
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
impl JudgeDb for Holding<'_> {
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
        self.0.d1_judges
    }
    fn actor_reads(&self) -> bool {
        self.0.actor_reads
    }
    async fn actor_deliver(
        &self,
        identity: &str,
        games: &[String],
    ) -> Result<Option<Value>, String> {
        let turn = futures_util::lock::Mutex::new(());
        let ev = fold7_event("pot", 77, games, NOW_MS);
        match apply_or_hold(
            &turn,
            &self.0.d1,
            &self.0.actor,
            &Fakes,
            identity,
            &ev,
            None,
            !self.0.d1_judges,
        )
        .await?
        {
            Applied::Held { rearmed } => {
                Ok(Some(held_answer(identity, games, ev.at_ms, true, rearmed)))
            }
            Applied::Done(report, _) => Ok(Some(answer_of(&report, Vec::new()))),
        }
    }
}

/// THE CONSUMER'S ACK OF A HELD EVENT: the held games' marks are acknowledged as an applied answer's are (up to the
/// generation read before the call), because the actor holds the event durably and serves no row until it has
/// applied it (held means not bootstrapped). Phase C (the actor the judge): the marks go, D1 writes no row, the event
/// stands in the actor; its bootstrap then derives the D1 judge's rows. Phase B: the D1 judge acknowledges, no compare
/// is counted (a held answer has no rows), no fault. RED on `d910ad5d`: `held_answer` and `apply_or_hold` do not
/// exist; Phase C's ack read `appliedGames` alone, so a held answer was `Err("... applied none of the N games")`.
#[tokio::test]
async fn d2_actor_b_fold7_e_the_consumer_acks_a_held_event_and_the_actor_applies_it() {
    for phase_c in [true, false] {
        let w = WithActor {
            d1: seeded(&stranded()),
            actor: mirror(),
            d1_judges: !phase_c,
            event: None,
            actor_reads: !phase_c,
            actor_fails: Cell::new(false),
        };
        let games = games_of(&w.d1, "owner");
        let holding = Holding(&w);
        crate::owed_events::deliver(&holding, "owner", &games)
            .await
            .unwrap();
        let left: i64 =
            w.d1.conn
                .query_row(
                    "SELECT COUNT(*) FROM owed_dirty WHERE identity='owner' AND gameId<>''",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
        assert_eq!(
            left, 0,
            "phase C {phase_c}: the held games' marks acknowledged"
        );
        assert_eq!(
            fold7_held(&w.actor).len(),
            1,
            "phase C {phase_c}: the event stands in the actor"
        );
        assert!(
            !w.d1
                .counted
                .borrow()
                .iter()
                .any(|(k, _)| k == COUNTERS[2] || k == COUNTERS[0]),
            "no fault, no compare"
        );
        let turn = futures_util::lock::Mutex::new(());
        let (_, _, drain) = fold7_bootstrap(&turn, &w.d1, &w.actor, "owner").await;
        assert_eq!(drain.applied.len(), 1);
        let reference = d1_judge(&stranded()).await;
        assert_eq!(
            stored(&w.actor, "owner"),
            stored(&reference, "owner"),
            "phase C {phase_c}"
        );
    }
}

/// Item 5: the health snapshot's `asOfMs` is its stamp whenever the figures are served. D1 binds every JS number as a
/// REAL (`routes::D1Sql`, `JsValue::from_f64`), so `json_object('asOfMs',?1)` stored `1793190803706.0` and `as_u64`
/// served null beside the figures. Run here with the REAL bind D1 makes. RED on `d910ad5d`: `asOfMs: null`.
#[test]
fn d2_actor_b_fold7_f_as_of_ms_is_the_snapshots_stamp() {
    let db = seeded(&Fixture {
        name: "empty",
        seed: Vec::new(),
        identities: Vec::new(),
    });
    let now: u64 = 1_793_190_803_706;
    db.conn
        .execute(HEALTH_SNAPSHOT_SQL, rusqlite::params![now as f64, 0.0f64])
        .unwrap();
    let cursor: String = db
        .conn
        .query_row(
            "SELECT cursor FROM owed_belt_state WHERE name='owed-actor:health'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let snap = health_snapshot_of(Some(&cursor)).unwrap();
    let body = health_body(Some(&snap), now + 5, true, &Value::Null);
    assert_eq!(body["asOfMs"], json!(now), "{cursor}");
    assert_eq!(body["healthRead"], json!(true));
    // A row written before the fold (a REAL in the JSON) is read as its stamp too.
    let old = json!({"asOfMs": now as f64, "bars": "[]", "counters": "{}"});
    assert_eq!(
        health_body(Some(&old), now, true, &Value::Null)["asOfMs"],
        json!(now)
    );
}

/// The amendment's item 7: the cold path's OWN bar. Two events held at 10 s and 4 s before the drain: ONE cold sample
/// a cold actor, from the FIRST held event's write time to the held events applied; served `coldMsP95`, `coldMsMax`,
/// `coldSamples` from the fleet's durable rows; never in the warm `rowsVisibleMsP95`.
#[tokio::test]
async fn d2_actor_b_fold7_g_the_cold_figure_is_noted_once_from_the_first_held_event() {
    let f = fold7_chained_seat(20);
    let record = seeded(&f);
    let games = games_of(&record, "owner");
    let local = mirror();
    local.now.set(NOW_MS);
    let turn = futures_util::lock::Mutex::new(());
    for (seq, at) in [(3, NOW_MS - 10_000), (4, NOW_MS - 4_000)] {
        let ev = fold7_event("pot", seq, &games[seq as usize..seq as usize + 1], at);
        apply_or_hold(&turn, &record, &local, &Fakes, "owner", &ev, None, false)
            .await
            .unwrap();
    }
    let (_, _, drain) = fold7_bootstrap(&turn, &record, &local, "owner").await;
    assert_eq!(drain.applied.len(), 2);
    assert_eq!(
        drain.cold_ms,
        Some(10_000),
        "one sample, from the first held event"
    );
    let mut pending = PendingBars::default();
    pending.note_cold(drain.cold_ms.unwrap());
    let d1 = seeded(&Fixture {
        name: "empty",
        seed: Vec::new(),
        identities: Vec::new(),
    });
    flush_bars_from(&d1, NOW_MS, &pending).await.unwrap();
    let rows = d1
        .run_batch(vec![Stmt::new(BARS_READ_SQL, vec![])])
        .await
        .unwrap()[0]
        .rows
        .clone();
    let fleet = fleet_bars(&rows, NOW_MS, lib_rev());
    assert_eq!(
        (
            fleet["coldMsP95"].clone(),
            fleet["coldMsMax"].clone(),
            fleet["coldSamples"].clone()
        ),
        (json!(10_000), json!(10_000), json!(1))
    );
    assert_eq!(
        fleet["rowsVisibleMsP95"],
        Value::Null,
        "the warm bar carries no cold cost"
    );
    // The served body (the snapshot) names it at the top.
    d1.run_batch(vec![health_snapshot_stmt(NOW_MS, true)])
        .await
        .unwrap();
    let cursor: String = d1
        .conn
        .query_row(
            "SELECT cursor FROM owed_belt_state WHERE name='owed-actor:health'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let body = health_body(
        health_snapshot_of(Some(&cursor)).as_ref(),
        NOW_MS,
        true,
        &Value::Null,
    );
    assert_eq!(body["coldMsP95"], json!(10_000));
    assert_eq!(body["held"], json!(0));
}

/// The captain's item 11, natively: workers-rs 0.8.7 reads an `i64` alarm as an OFFSET (`ScheduledTime: From<i64>`),
/// so every site passes one through `alarm_in` / `alarm_at` (`alarm_offset_ms`); no other `set_alarm` stands in the
/// object. RED on `d910ad5d`: seven sites passed an epoch stamp (`set_alarm(Date::now().as_millis() as i64 ...)`).
/// The fired-alarm proof on real workerd is `tools/lane-912/owed_actor_alarm.mjs` (`fold7_h`, and `fold7_i`: an actor
/// armed under a dead alarm re-armed by the lever and by a held event).
#[test]
fn d2_actor_b_fold7_h_every_alarm_is_an_offset() {
    assert_eq!(alarm_offset_ms(60_000, 0), 60_000);
    assert_eq!(alarm_offset_ms(NOW_MS + 600_000, NOW_MS), 600_000);
    assert_eq!(
        alarm_offset_ms(NOW_MS - 5, NOW_MS),
        0,
        "a stamp past is now"
    );
    // Item 12 (fold7_i natively; its workerd proof is the cell's `(i)`): a bootstrap's alarm lost, or set the way the
    // old build set it (an epoch stamp read as an offset: about 56 years out), is dead and re-armed; a live one is kept.
    assert!(alarm_is_dead(None, NOW_MS));
    assert!(alarm_is_dead(Some((NOW_MS + NOW_MS) as i64), NOW_MS));
    assert!(!alarm_is_dead(Some(NOW_MS as i64), NOW_MS));
    // FOLD-8 (NEW4-L2): the bound sits past the pace's longest wait (`fold8_d`).
    assert!(!alarm_is_dead(
        Some((NOW_MS + BOOT_ALARM_DEAD_AFTER_MS) as i64),
        NOW_MS
    ));
    assert!(alarm_is_dead(
        Some((NOW_MS + BOOT_ALARM_DEAD_AFTER_MS + 1) as i64),
        NOW_MS
    ));
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let object = squash(include_str!("object.rs"));
    let sets = object.matches(".set_alarm(").count();
    assert_eq!(sets, 2, "set_alarm only inside alarm_in and alarm_at");
    assert!(object.contains(".set_alarm(alarm_offset_ms(offset_ms,0))"));
    assert!(object.contains(".set_alarm(alarm_offset_ms(at_ms,Date::now().as_millis()))"));
}

// ---- D2-ACTOR-B-FOLD-8: the fourth delta lens on PR 206 (NEW4-M1, NEW4-M2, NEW4-L1 to NEW4-L4, NOTE-1, NOTE-2) ----

fn fold8_empty() -> SqliteSql {
    seeded(&Fixture {
        name: "empty",
        seed: Vec::new(),
        identities: Vec::new(),
    })
}

/// NEW4-M1: the fleet's bootstrap budget is real on D1. D1 binds every JS number as a REAL (`routes::D1Sql`,
/// `JsValue::from_f64`), so the take is run here with the bind D1 makes (`f64`, as `fold7_f` does): 70 takes inside
/// one minute, each a different millisecond, answer 1 to 60 and then no row (the page waits, `boot_pace`); the next
/// minute takes again from 1; the stored minute is an INTEGER. RED on `a1010ad0` (`?1/60000`, a fractional minute a
/// millisecond): every take answers 1 (`left: [1, 1, 1, ..]`).
#[test]
fn d2_actor_b_fold8_a_the_boot_budget_refuses_past_60_a_minute_under_d1s_real_bind() {
    let db = fold8_empty();
    let take = |now: u64| -> i64 {
        let mut q = db.conn.prepare(BOOT_BUDGET_TAKE_SQL).unwrap();
        let rows: Vec<Value> = q
            .query_map(rusqlite::params![now as f64], |r| {
                Ok(json!({"taken": r.get::<_, i64>(0)?}))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        boot_taken(&rows)
    };
    let minute = (NOW_MS / 60_000) * 60_000;
    let answers: Vec<i64> = (0..70u64).map(|k| take(minute + 7 + k * 13)).collect();
    let want: Vec<i64> = (1..=60).chain(std::iter::repeat_n(i64::MAX, 10)).collect();
    assert_eq!(answers, want, "1 to 60, then refused");
    assert!(boot_pace(answers[60], minute + 900, "02ab").is_some());
    let (ty, stored): (String, i64) = db
        .conn
        .query_row(
            "SELECT typeof(tickedAtMs),tickedAtMs FROM owed_belt_state WHERE name='owed-actor:boot-budget'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((ty.as_str(), stored), ("integer", (minute / 60_000) as i64));
    assert_eq!(take(minute + 60_000 + 3), 1, "the next minute takes again");
}

/// A record that faults every bootstrap page read (`IDENTITY_GAMES_AFTER_SQL`) and answers the rest.
struct PageFaults<'a>(&'a SqliteSql);
impl SqlPort for PageFaults<'_> {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        if stmts
            .iter()
            .any(|s| s.sql == crate::owed_judge::IDENTITY_GAMES_AFTER_SQL)
        {
            return Err("D1_ERROR: the page's read faulted".into());
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

fn fold8_holding(record: &SqliteSql) -> Vec<(String, i64)> {
    let mut q = record
        .conn
        .prepare("SELECT name,tickedAtMs FROM owed_belt_state WHERE name>='owed-actor:holding:' AND name<'owed-actor:holding;' ORDER BY name")
        .unwrap();
    q.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// NEW4-M2: an actor whose bootstrap page faults on every alarm is COUNTED (each fault), PARKED at its fifth in a
/// row (its line names it; the alarm retries it no more), SERVED (`/health.owedActor.holding` {`count`, `oldestMs`}
/// from the snapshot the writers compose, `bootstrapFaults`), and its seat's read is the fallback (Rule 23: no
/// `bootstrapped`, so `/rows` answers pending and the read serves no press). Its holding row is written ONCE (a later
/// hold writes nothing to D1); the lever clears the park once (`parked: true`), the page re-runs from its parked
/// cursor, and the stamp deletes the holding row. The alarm's wiring is pinned on `object.rs`'s source (the object
/// runs only on wasm32). RED on `a1010ad0`: none of `note_holding`, `boot_fault`, `boot_parked`, `clear_boot_park`
/// exists (the module does not compile); the alarm there logs `bootstrap page:` and re-arms 60 s for ever, uncounted,
/// and nothing served names the actor.
#[tokio::test]
async fn d2_actor_b_fold8_b_a_stuck_actor_is_counted_served_and_parked() {
    let f = fold7_chained_seat(20);
    let record = seeded(&f);
    record.now.set(NOW_MS);
    let games = games_of(&record, "owner");
    let local = mirror();
    local.now.set(NOW_MS);
    let turn = futures_util::lock::Mutex::new(());
    let ev = fold7_event("pot", 5, &games[2..3], NOW_MS - 1_000);
    let first = apply_or_hold(&turn, &record, &local, &Fakes, "owner", &ev, None, false)
        .await
        .unwrap();
    assert!(matches!(first, Applied::Held { rearmed: false }));
    assert_eq!(
        fold8_holding(&record),
        vec![("owed-actor:holding:owner".to_string(), NOW_MS as i64)]
    );
    // A later hold writes nothing to D1 (one row an actor, its first hold's time kept).
    let before = record.recorded.borrow().len();
    local.now.set(NOW_MS + 5_000);
    let ev2 = fold7_event("hop", 6, &games[3..4], NOW_MS + 4_000);
    apply_or_hold(&turn, &record, &local, &Fakes, "owner", &ev2, None, false)
        .await
        .unwrap();
    assert_eq!(record.recorded.borrow().len(), before, "no D1 write");
    assert_eq!(fold8_holding(&record)[0].1, NOW_MS as i64);
    // Five alarms: each page faults; four retry, the fifth parks.
    let faulty = PageFaults(&record);
    let mut verdicts = Vec::new();
    for _ in 0..7 {
        if boot_parked(&local).await {
            break;
        }
        let e = bootstrap_step(
            &turn,
            &faulty,
            &local,
            &Fakes,
            "owner",
            Some(object::BOOT_CURSOR_START),
            Some(TIP),
            None,
            false,
        )
        .await
        .unwrap_err();
        record.counters(vec![(COUNTERS[24].into(), 1)]).await;
        verdicts.push(boot_fault(&local, "owner", &e).await);
    }
    assert_eq!(verdicts.len(), 5, "the alarm stops at the park");
    assert_eq!(
        verdicts[..4],
        (1..=4)
            .map(|faults| BootFault::Retry { faults })
            .collect::<Vec<_>>()[..]
    );
    let BootFault::Parked { faults, line } = &verdicts[4] else {
        panic!("{:?}", verdicts[4]);
    };
    assert_eq!(*faults, BOOT_FAULTS_PARK);
    assert_eq!(
        line,
        "[owed-actor] bootstrap PARKED owner after 5 faults: D1_ERROR: the page's read faulted"
    );
    assert_eq!(record.counter(COUNTERS[24]), 5, "every fault counted");
    // Rule 23: not bootstrapped, so `/rows` answers `bootstrapped: false` and the read serves the fallback.
    assert!(fold7_meta(&local, "bootstrapped").is_none());
    match fallback_body("owner", None, BOOTSTRAP_REASON, "owed-actor-bootstrap") {
        Fallback::Unavailable(b) => assert_eq!(b["pending"], json!(true)),
        Fallback::Display(b) => panic!("no copy, no rows: {b}"),
    }
    // Served: the holding count and age, and the faults, from the snapshot the writers compose.
    for (name, n) in record.counted.borrow().iter() {
        record
            .conn
            .execute(
                "INSERT INTO ops_counters(name,value) VALUES (?1,?2) ON CONFLICT(name) DO UPDATE SET value=value+excluded.value",
                rusqlite::params![name, *n as i64],
            )
            .unwrap();
    }
    record
        .conn
        .execute(
            HEALTH_SNAPSHOT_SQL,
            rusqlite::params![(NOW_MS + 60_000) as f64, 0.0f64],
        )
        .unwrap();
    let cursor: String = record
        .conn
        .query_row(
            "SELECT cursor FROM owed_belt_state WHERE name='owed-actor:health'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let body = health_body(
        health_snapshot_of(Some(&cursor)).as_ref(),
        NOW_MS + 90_000,
        true,
        &Value::Null,
    );
    assert_eq!(body["holding"], json!({"count": 1, "oldestMs": 90_000}));
    assert_eq!(body["bootstrapFaults"], json!(5));
    // The lever clears the park once; the page re-runs from its parked cursor and the stamp ends the hold.
    assert!(clear_boot_park(&local).await, "parked: true");
    assert!(!clear_boot_park(&local).await, "once");
    let (_, retries, drain) = fold7_bootstrap(&turn, &record, &local, "owner").await;
    assert_eq!(retries, 0);
    assert_eq!(drain.applied.len(), 2);
    assert!(fold7_meta(&local, "bootstrapped").is_some());
    assert!(fold8_holding(&record).is_empty(), "the stamp deleted it");
    // The alarm's wiring (object.rs runs on wasm32 only).
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let object = squash(include_str!("object.rs"));
    let alarm = &object[object.find("asyncfnalarm(&self)").unwrap()..];
    for want in [
        "ifself.meta(META_BOOT_CURSOR).is_some()&&!boot_parked(&local).await{",
        "record.counters(vec![(COUNTERS[24].into(),1)]).await;",
        "matchboot_fault(&local,&me,&e.to_string()).await{",
        "BootFault::Retry{..}=>self.alarm_in(BOOT_FAULT_RETRY_MS).await,",
        "BootFault::Parked{line,..}=>console_log!(\"{line}\"),",
    ] {
        assert!(alarm.contains(want), "the alarm: {want}");
    }
    let arm = &object[object.find("asyncfnarm_bootstrap(").unwrap()..];
    assert!(arm.contains("if!lever||!clear_boot_park(&local).await{"));
    assert!(object.contains("boot_page_landed(local).await;"));
}

/// NEW4-L1: the drain applies the UNION of the held games in chunks of 16, at most [`DRAIN_CHUNKS_PER_ALARM`] a
/// call: 150 one-game events past a bootstrap are 128 events in one alarm (`Draining`, the cursor `drain`) and 22 in
/// the next (`Done`, stamped), the rows the D1 judge's; held events naming one game are one apply. RED on `a1010ad0`:
/// `drain_union`, `Draining` and the bound do not exist; the drain applied each of the 150 rows in one alarm.
#[tokio::test]
async fn d2_actor_b_fold8_c_the_drain_is_the_union_in_chunks_bounded_per_alarm() {
    let a = fold7_event("pot", 1, &["G1".into(), "g2".into()], NOW_MS);
    let b = fold7_event("pot", 2, &["g2".into()], NOW_MS);
    let c = fold7_event("pot", 3, &["g1".into(), "g3".into()], NOW_MS);
    assert_eq!(drain_union(&[a, b, c]), vec!["g1", "g2", "g3"]);
    let f = fold7_chained_seat(160);
    let d1 = d1_judge(&f).await;
    let record = seeded(&f);
    let games = games_of(&record, "owner");
    let local = mirror();
    let turn = futures_util::lock::Mutex::new(());
    for (k, g) in games.iter().take(150).enumerate() {
        let ev = fold7_event(
            "pot",
            100 + k as i64,
            std::slice::from_ref(g),
            NOW_MS - 1_000,
        );
        apply_or_hold(&turn, &record, &local, &Fakes, "owner", &ev, None, false)
            .await
            .unwrap();
    }
    assert_eq!(fold7_held(&local).len(), 150);
    let mut cursor = object::BOOT_CURSOR_START.to_string();
    let mut steps = Vec::new();
    loop {
        match bootstrap_step(
            &turn,
            &record,
            &local,
            &Fakes,
            "owner",
            Some(&cursor),
            Some(TIP),
            None,
            false,
        )
        .await
        .unwrap()
        {
            BootStep::Page { cursor: c, .. } => cursor = c,
            BootStep::Draining { drain } => {
                steps.push(("draining", drain.applied.len(), fold7_held(&local).len()));
                assert!(fold7_meta(&local, "bootstrapped").is_none());
                cursor = BOOT_CURSOR_DRAIN.into();
            }
            BootStep::Done { drain, .. } => {
                steps.push(("done", drain.applied.len(), fold7_held(&local).len()));
                assert!(drain.cold_ms.is_some(), "the cold sample at the stamp");
                break;
            }
        }
    }
    assert_eq!(steps, vec![("draining", 128, 22), ("done", 22, 0)]);
    assert!(fold7_meta(&local, "bootstrapped").is_some());
    assert_eq!(stored(&local, "owner"), stored(&d1, "owner"));
}

/// NEW4-L2: a paced wait is never dead (`boot_pace`'s wait is up to the next minute's edge plus a spread under a
/// minute), a lost alarm and a 56-year one still are. RED on `a1010ad0` (the bound one page delay and a minute):
/// `a paced wait is not dead` fails for every wait past now + 61 s.
#[test]
fn d2_actor_b_fold8_d_a_paced_wait_is_not_dead() {
    let mut longest = 0;
    for k in 0..600u64 {
        let now = NOW_MS + k * 977;
        let id = format!("02{:064x}", k.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let wait = boot_pace(i64::MAX, now, &id).unwrap();
        longest = longest.max(wait - now);
        assert!(
            !alarm_is_dead(Some(wait as i64), now),
            "a paced wait is not dead: {} ms out",
            wait - now
        );
    }
    assert!(longest > 61_000, "the pins reach the long waits: {longest}");
    assert!(alarm_is_dead(None, NOW_MS));
    assert!(alarm_is_dead(Some((NOW_MS + NOW_MS) as i64), NOW_MS));
}

/// NEW4-L3: each class's `flushedAtMs` is served as an integer when D1 stored the bound `tickedAtMs` as a REAL (the
/// `asOfMs` fact). RED on `a1010ad0`: `flushedAtMs: null`.
#[test]
fn d2_actor_b_fold8_e_flushed_at_ms_is_an_integer() {
    let db = fold8_empty();
    let at: u64 = 1_793_190_803_706;
    db.conn
        .execute(
            crate::owed_belt::STATE_SQL,
            rusqlite::params![
                "owed-actor:bars:queue",
                json!({"visible": [5], "libRev": lib_rev()}).to_string(),
                at as f64
            ],
        )
        .unwrap();
    db.conn
        .execute(
            HEALTH_SNAPSHOT_SQL,
            rusqlite::params![(at + 1) as f64, 0.0f64],
        )
        .unwrap();
    let cursor: String = db
        .conn
        .query_row(
            "SELECT cursor FROM owed_belt_state WHERE name='owed-actor:health'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let body = health_body(
        health_snapshot_of(Some(&cursor)).as_ref(),
        at + 2,
        true,
        &Value::Null,
    );
    assert_eq!(
        body["classes"]["queue"]["flushedAtMs"],
        json!(at),
        "{cursor}"
    );
    // A row read as D1 renders a REAL (the fleet's read of the bars, whatever the composition).
    let row = json!({"name": "owed-actor:bars:queue", "cursor": json!({"visible": [5], "libRev": "r1"}).to_string(), "tickedAtMs": at as f64});
    assert_eq!(
        fleet_bars(&[row], at, "r1")["classes"]["queue"]["flushedAtMs"],
        json!(at)
    );
}

/// NEW4-L4: the held answers are counted in the isolate's bars, ONE counter write a flush (never one D1 write a held
/// event); the tip window does not run while the bootstrap's cursor stands. RED on `a1010ad0`: the Held arm wrote
/// `(COUNTERS[21], 1)` with its own statement per answer, and the alarm ran the window whenever a gate stood.
#[tokio::test]
async fn d2_actor_b_fold8_f_held_answers_are_counted_a_flush_and_the_tip_window_waits() {
    let mut pending = PendingBars::default();
    for _ in 0..100 {
        pending.note_held();
    }
    let db = fold8_empty();
    flush_bars_from(&db, NOW_MS, &pending).await.unwrap();
    assert_eq!(
        *db.counted.borrow(),
        vec![(COUNTERS[21].to_string(), 100)],
        "one counter write"
    );
    assert!(tip_window_due(true, false));
    assert!(!tip_window_due(true, true), "never while the cursor stands");
    assert!(!tip_window_due(false, false));
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let object = squash(include_str!("object.rs"));
    assert!(!object.contains("COUNTERS[21]"), "no per-answer held write");
    let apply =
        &object[object.find("asyncfnapply_route(").unwrap()..object.find("fnrows_of(").unwrap()];
    assert!(apply.contains("note_held();"));
    assert!(apply.contains("flush_bars(&record,Date::now().as_millis(),false).await;"));
    let alarm = &object[object.find("asyncfnalarm(&self)").unwrap()..];
    assert!(alarm.contains(
        "tip_window_due(self.meta(META_GATE).is_some(),self.meta(META_BOOT_CURSOR).is_some(),),"
    ));
}

/// The key columns of a family (what selects a record row into the mirror), lowercased; `rowid` always.
fn fold8_keys(f: &Family) -> Vec<String> {
    let mut out = vec!["rowid".to_string()];
    for k in f.keys {
        match k {
            Mine(i, g) => out.extend([i.to_ascii_lowercase(), g.to_ascii_lowercase()]),
            Games(c) | Txid(c) | Marker(c) => out.push(c.to_ascii_lowercase()),
            ProbeRange => out.push("outpoint".into()),
        }
    }
    out
}

/// The violations of one statement: each family table it reads (by its alias) whose `WHERE` / `ON` columns name
/// none of the family's keys. Literals are stripped; a qualified column (`a.col`) is its alias's, a bare one is every
/// family's of the statement that has the column.
fn fold8_unkeyed(sql: &str) -> Vec<String> {
    let mut clean = String::new();
    let mut in_str = false;
    for ch in sql.chars() {
        if ch == '\'' {
            in_str = !in_str;
            clean.push(' ');
        } else if !in_str {
            clean.push(ch);
        }
    }
    let spaced: String = clean
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '.' {
                c
            } else {
                ' '
            }
        })
        .collect();
    let toks: Vec<String> = spaced
        .split_whitespace()
        .map(str::to_ascii_lowercase)
        .collect();
    const KW: [&str; 24] = [
        "where", "on", "join", "inner", "left", "cross", "select", "from", "group", "order",
        "limit", "union", "and", "or", "as", "using", "indexed", "not", "in", "exists", "having",
        "set", "values", "by",
    ];
    let family = |t: &str| FAMILIES.iter().find(|f| f.table.eq_ignore_ascii_case(t));
    let mut alias: BTreeMap<String, &'static Family> = BTreeMap::new();
    for (i, t) in toks.iter().enumerate() {
        if i == 0 || !matches!(toks[i - 1].as_str(), "from" | "join") {
            continue;
        }
        if let Some(f) = family(t) {
            alias.insert(t.clone(), f);
            let mut j = i + 1;
            if toks.get(j).map(String::as_str) == Some("as") {
                j += 1;
            }
            if let Some(a) = toks.get(j).filter(|a| !KW.contains(&a.as_str())) {
                alias.insert(a.clone(), f);
            }
        }
    }
    if alias.is_empty() {
        return Vec::new();
    }
    let mut cols: BTreeMap<&'static str, BTreeSet<String>> = BTreeMap::new();
    let mut cond = false;
    for t in &toks {
        match t.as_str() {
            "where" | "on" | "having" => {
                cond = true;
                continue;
            }
            "select" | "from" | "join" | "group" | "order" | "limit" | "union" | "set"
            | "values" => {
                cond = false;
                continue;
            }
            _ => {}
        }
        if !cond {
            continue;
        }
        if let Some((a, c)) = t.split_once('.') {
            if let Some(f) = alias.get(a) {
                cols.entry(f.table).or_default().insert(c.to_string());
            }
        } else {
            for f in alias.values() {
                if schema::columns(f.table)
                    .iter()
                    .any(|c| c.eq_ignore_ascii_case(t))
                    || t == "rowid"
                {
                    cols.entry(f.table).or_default().insert(t.clone());
                }
            }
        }
    }
    let mut out = Vec::new();
    let tables: BTreeSet<&'static str> = alias.values().map(|f| f.table).collect();
    for table in tables {
        let f = family(table).unwrap();
        let keys = fold8_keys(f);
        let got = cols.get(table).cloned().unwrap_or_default();
        if !got.iter().any(|c| keys.contains(c)) {
            out.push(format!("{table}: {got:?}"));
        }
    }
    out
}

/// NOTE-2 (the fourth delta lens): every statement the judge runs over a family selects it by one of that family's
/// KEYS (`FAMILIES`), so the actor's mirror (rows copied by those keys) holds every row the judge reads: a later judge
/// statement that selects `pot_records` by `spendingTxid` alone, or a marker of another game by its rowid, is RED here
/// (the actor alone would diverge). The judge's statements are those `judge_subjects` runs over every frozen fixture,
/// and the bootstrap's own page read. Stated: the check is lexical (a `WHERE` or `ON` column of the family, by its
/// alias or bare), so a filter beside a key is allowed; it holds the class the lens names, not a proof of the plan.
#[tokio::test]
async fn d2_actor_b_fold8_g_every_judge_statement_selects_a_family_by_a_key() {
    // The checker itself: an unkeyed selection is caught, a keyed one is not.
    assert_eq!(
        fold8_unkeyed("SELECT txid FROM pot_records WHERE spendingTxid=?1 AND spent=1"),
        vec!["pot_records: {\"spendingtxid\", \"spent\"}".to_string()]
    );
    assert!(fold8_unkeyed(
        "SELECT p.txid FROM pot_records p JOIN pot_beefs b ON b.txid=p.spendingTxid WHERE p.txid IN (SELECT value FROM json_each(?1))"
    )
    .is_empty());
    assert_eq!(
        fold8_unkeyed("SELECT h.txid FROM hopparty_records h WHERE h.markerValid=1").len(),
        1
    );
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for f in all_fixtures() {
        let d1 = d1_judge(&f).await;
        for batch in d1.recorded.borrow().iter() {
            for st in batch {
                seen.insert(st.sql.clone());
            }
        }
    }
    let read: Vec<&String> = seen
        .iter()
        .filter(|s| {
            let l = s.to_ascii_lowercase();
            FAMILIES.iter().any(|f| {
                l.contains(&format!("from {}", f.table.to_ascii_lowercase()))
                    || l.contains(&format!("join {}", f.table.to_ascii_lowercase()))
            })
        })
        .collect();
    eprintln!("fold8_g: {} judge statements read a family", read.len());
    assert!(read.len() > 5, "the judge's family reads: {}", read.len());
    let mut bad = Vec::new();
    for s in &read {
        for v in fold8_unkeyed(s) {
            bad.push(format!("{v}\n    in {s}"));
        }
    }
    assert!(
        bad.is_empty(),
        "unkeyed family selections:\n{}",
        bad.join("\n")
    );
}

/// The captain's item 9: THE FLEET BARS ARE THE BUILD'S. A bars row holding another build's samples (beta, an hour
/// after PR 206's T0: the old build's cold-closure retries, `rowsVisibleMsP95` 311,719 ms) is REPLACED, not merged,
/// by this build's first flush, and the served p95s read this build's samples alone (`barsLibRev` names it); a row
/// no flush of this build has touched serves no samples. RED on `a1010ad0`: `merge_bars_row` and `fleet_bars` take no
/// build (the old samples merged and served: `rowsVisibleMsP95` 311719).
#[tokio::test]
async fn d2_actor_b_fold8_h_the_fleet_bars_are_the_builds() {
    let db = fold8_empty();
    let old = json!({"apply": [7_818], "visible": [311_719], "libRev": "old-build"}).to_string();
    for class in ["queue", "direct"] {
        db.conn
            .execute(
                crate::owed_belt::STATE_SQL,
                rusqlite::params![bars_state_name(class), old, NOW_MS as i64 - 3_600_000],
            )
            .unwrap();
    }
    let read = |db: &SqliteSql| {
        db.conn
            .prepare(BARS_READ_SQL)
            .unwrap()
            .query_map([], |r| {
                Ok(json!({"name": r.get::<_, String>(0)?, "cursor": r.get::<_, String>(1)?, "tickedAtMs": r.get::<_, i64>(2)?}))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<Value>>()
    };
    let before = fleet_bars(&read(&db), NOW_MS, "new-build");
    assert_eq!(
        before["rowsVisibleMsP95"],
        Value::Null,
        "another build's samples are not served"
    );
    assert_eq!(before["barsLibRev"], json!("new-build"));
    // This build's first flush: one queue sample, replacing the old row (never merged).
    let fresh = merge_bars_row(Some(&old), &[40], &[90], None, "new-build");
    let row: Value = serde_json::from_str(&fresh).unwrap();
    assert_eq!(row["visible"], json!([90]), "replaced, not merged");
    assert_eq!(row["apply"], json!([40]));
    assert_eq!(row["libRev"], json!("new-build"));
    db.conn
        .execute(
            crate::owed_belt::STATE_SQL,
            rusqlite::params![bars_state_name("queue"), fresh, NOW_MS as i64],
        )
        .unwrap();
    let after = fleet_bars(&read(&db), NOW_MS, "new-build");
    assert_eq!(after["rowsVisibleMsP95"], json!(90));
    assert_eq!(
        after["applyMsP95"],
        json!(40),
        "the direct row's old 7,818 is not served"
    );
    assert_eq!(after["rowsVisibleDirectMsP95"], Value::Null);
    // The same build's next flush merges as before.
    let merged: Value =
        serde_json::from_str(&merge_bars_row(Some(&fresh), &[], &[95], None, "new-build")).unwrap();
    assert_eq!(merged["visible"], json!([90, 95]));
    // The shipped flush stamps this build.
    let mut pending = PendingBars::default();
    pending.note_answer("queue", &json!({"applyMs": 12}));
    flush_bars_from(&db, NOW_MS, &pending).await.unwrap();
    let q: String = db
        .conn
        .query_row(
            "SELECT cursor FROM owed_belt_state WHERE name='owed-actor:bars:queue'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let q: Value = serde_json::from_str(&q).unwrap();
    assert_eq!(q["libRev"], json!(lib_rev()));
    if lib_rev() != "new-build" {
        assert_eq!(
            q["apply"],
            json!([12]),
            "another build's row replaced by the shipped flush"
        );
    }
}

/// The captain's item 10: `GET /internal/owed-actor-rows?identity=` answers one actor's state and rows beside D1's
/// and the outpoints that differ, by the reconcile's own reads (`/stored`, `RECONCILE_D1_ROWS_SQL`) and functions
/// (`diff_rows`, `rows_digest`); 401 without the bearer, before any read. Run over a fixture judged by both judges
/// with one D1 row planted different. RED on `a1010ad0`: no route, no `rows_view`.
#[tokio::test]
async fn d2_actor_b_fold8_i_the_rows_lever_serves_the_reconciles_diff() {
    let f = fold7_chained_seat(20);
    let d1 = d1_judge(&f).await;
    let record = seeded(&f);
    let local = mirror();
    let turn = futures_util::lock::Mutex::new(());
    fold7_bootstrap(&turn, &record, &local, "owner").await;
    let actor_rows = |db: &SqliteSql| -> Vec<Value> {
        let mut q = db.conn.prepare(RECONCILE_D1_ROWS_SQL).unwrap();
        let cols: Vec<String> = q.column_names().iter().map(|c| c.to_string()).collect();
        q.query_map(["owner"], |r| {
            let mut m = serde_json::Map::new();
            for (k, c) in cols.iter().enumerate() {
                let v: rusqlite::types::Value = r.get(k)?;
                m.insert(
                    c.clone(),
                    match v {
                        rusqlite::types::Value::Integer(n) => json!(n),
                        rusqlite::types::Value::Text(t) => json!(t),
                        rusqlite::types::Value::Real(x) => json!(x),
                        _ => Value::Null,
                    },
                );
            }
            Ok(Value::Object(m))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect()
    };
    let same = rows_view(
        "owner",
        Ok(json!({"rows": actor_rows(&local), "bootstrapped": true, "held": 0})),
        &actor_rows(&d1),
    );
    assert_eq!(same["diff"], json!([]), "the two judges agree");
    assert_eq!(same["digests"]["d1"], same["digests"]["actor"]);
    // One D1 row planted different.
    let planted: String = d1
        .conn
        .query_row(
            "SELECT outpoint FROM owed_rows WHERE identity='owner' ORDER BY outpoint LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    d1.conn
        .execute(
            "UPDATE owed_rows SET sats=sats+1 WHERE identity='owner' AND outpoint=?1",
            [&planted],
        )
        .unwrap();
    let d1_rows = actor_rows(&d1);
    let a_rows = actor_rows(&local);
    let view = rows_view(
        "owner",
        Ok(
            json!({"rows": a_rows.clone(), "bootstrapped": true, "bootstrapArmed": false, "held": 0, "ackSeq": null, "judgedAtMs": NOW_MS}),
        ),
        &d1_rows,
    );
    assert_eq!(view["diff"], json!([planted.clone()]));
    assert_eq!(
        view["diff"],
        json!(diff_rows(&d1_rows, &a_rows)),
        "the reconcile's diff"
    );
    assert_ne!(view["digests"]["d1"], view["digests"]["actor"]);
    assert_eq!(view["actor"]["bootstrapped"], json!(true));
    assert_eq!(
        view["actor"]["rows"].as_array().unwrap().len(),
        a_rows.len()
    );
    assert_eq!(view["d1"]["rows"].as_array().unwrap().len(), d1_rows.len());
    // An actor out of reach is named, never an empty diff.
    let down = rows_view(
        "owner",
        Err("owed actor: no answer in 1500 ms".into()),
        &d1_rows,
    );
    assert_eq!(down["diff"], Value::Null);
    assert_eq!(
        down["actor"]["error"],
        json!("owed actor: no answer in 1500 ms")
    );
    // The lever's wiring (wasm32 only): the bearer first (401), one identity, the reconcile's reads, no write.
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let object = squash(include_str!("object.rs"));
    let lever = &object[object.find("pubasyncfnrows_lever(").unwrap()
        ..object.find("pubasyncfnidentities_lever(").unwrap()];
    assert!(lever.starts_with(&squash("pub async fn rows_lever(req: Request, env: &Env) -> Result<Response> { if !crate::internal_events::internal_bearer_ok(&req, env) { return Response::error(\"unauthorized\", 401); }")));
    assert!(lever.contains("RECONCILE_D1_ROWS_SQL"));
    assert!(lever.contains("call_stored_answer(env,&me,READ_WALL_MS)"));
    assert!(lever.contains("rows_view(&me,actor,&d1)"));
    assert!(!lever.contains("INSERT") && !lever.contains("DELETE") && !lever.contains("UPDATE"));
    let lib = squash(include_str!("../lib.rs"));
    assert!(lib.contains("req.path()==\"/internal/owed-actor-rows\"{returnowed_actor::object::rows_lever(req,&env).await;"));
    let stored = &object[object.find("path==\"/stored\"&&self.is_fresh()").unwrap()..];
    assert!(
        stored.starts_with("path==\"/stored\"&&self.is_fresh(){returnResponse::from_json("),
        "a fresh object writes nothing"
    );
}

thread_local! { static FOLD8_J_BETWEEN: Cell<bool> = const { Cell::new(false) }; }

/// The captain's item 11: PHASE A's compare is evidence-bound like Phase B's. A downgrade written BETWEEN the two
/// judges (the D1 judge has written its rows, the actor then reads the record with the new evidence) is the window's
/// one "real" DIFF (pair 20's seat at its settle): its game holds a mark above the bound read before the D1 judge, so
/// it is counted APART (`owed_actor_compare_held_total`), `owed_actor_diffs_total` flat; the next delivery heals it.
/// RED on `046b721e` (Phase A's `compare` ran with `bound: None`; the bound read made inert):
/// `owed_actor_diffs_total` 0 -> 1.
#[tokio::test]
async fn d2_actor_b_fold8_j_phase_a_counts_a_mark_between_the_judges_apart() {
    let w = WithActor {
        d1: d1_judge(&stranded()).await,
        actor: mirror(),
        d1_judges: true,
        event: Some(("pot", 7)),
        actor_reads: false,
        actor_fails: Cell::new(false),
    };
    let games = games_of(&w.d1, "owner");
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    let (diffs, held) = (
        w.d1.counter("owed_actor_diffs_total"),
        w.d1.counter("owed_actor_compare_held_total"),
    );
    let w = WithActor {
        event: Some(("pot", 8)),
        ..w
    };
    FOLD8_J_BETWEEN.with(|c| c.set(true));
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    let lines: Vec<String> =
        w.d1.lines
            .borrow()
            .iter()
            .filter(|l| l.contains("DIFF"))
            .cloned()
            .collect();
    assert_eq!(
        w.d1.counter("owed_actor_diffs_total"),
        diffs,
        "a mark between the judges is not a STOP: {lines:?}"
    );
    assert_eq!(
        w.d1.counter("owed_actor_compare_held_total"),
        held + 1,
        "{lines:?}"
    );
    // Item 12 on the held line: the outpoint and the field that differs.
    assert!(
        lines
            .iter()
            .any(|l| l.contains("held mark") && l.contains(" d1=") && l.contains(" actor=")),
        "{lines:?}"
    );
    // The next delivery judges the mark in both: no DIFF of either class.
    let w = WithActor {
        event: Some(("pot", 9)),
        ..w
    };
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    assert_eq!(w.d1.counter("owed_actor_diffs_total"), diffs);
    assert_eq!(w.d1.counter("owed_actor_compare_held_total"), held + 1);
    // The deferred path carries the same bound (wasm32: `D1Judge::defer_actor`).
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let events = squash(include_str!("../owed_events.rs"));
    assert!(events.contains(&squash("let bound = owed_judge::marks_bound(db, identity, games).await.ok(); let report = owed_judge::judge_subjects(db, identity, games, \"owed-events\").await?;")));
    assert!(events.contains(&squash("if !db.defer_actor(identity, games, bound) { crate::owed_actor::beside(db, identity, games, bound).await; }")));
    let actor = squash(include_str!("../owed_actor.rs"));
    assert!(actor.contains(&squash(
        "beside(&judge, &d.identity, &d.games, d.bound).await;"
    )));
}

/// The captain's item 12: THE DIFF LINES NAME WHAT DIFFERS. For the first three differing outpoints, each compared
/// field with both values (`family d1=unbound actor=hop-stranded`, `claimable d1=null actor=1`, a facts key by name),
/// a row on one side `absent`; the reconcile's line also says whether the actor was bootstrapped at the compare.
/// RED on `046b721e`: `diff_detail` does not exist; the lines carried counts only.
#[test]
fn d2_actor_b_fold8_k_the_diff_lines_name_what_differs() {
    let row = |o: &str, family: &str, claimable: Value, claim: &str| json!({"outpoint": o, "family": family, "gameId": "g", "sats": 5, "opponentIdentity": null, "atHeight": null, "reason": "r", "claimable": claimable, "facts": json!({"claim": claim, "ageMs": 9}).to_string()});
    let d1 = vec![
        row("a:0", "unbound", Value::Null, "none"),
        row("b:0", "payout", json!(1), "x"),
        row("c:0", "payout", json!(1), "x"),
        row("d:0", "payout", json!(1), "x"),
    ];
    let actor = vec![
        row("a:0", "hop-stranded", json!(1), "sweep-hop"),
        row("c:0", "payout", json!(0), "x"),
        row("d:0", "payout", json!(0), "x"),
        row("e:0", "payout", json!(0), "x"),
    ];
    let differ = diff_rows(&d1, &actor);
    assert_eq!(differ.len(), 5);
    let line = diff_detail(&d1, &actor, &differ);
    assert_eq!(
        line,
        "a:0 [family d1=unbound actor=hop-stranded, claimable d1=null actor=1, facts.claim d1=none actor=sweep-hop]; b:0 [d1=present actor=absent]; c:0 [claimable d1=1 actor=0]"
    );
    assert!(!line.contains("ageMs"), "the clock is not a difference");
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let src = squash(include_str!("../owed_actor.rs"));
    assert_eq!(
        src.matches("diff_detail(&d1,&actor,&").count(),
        2,
        "both compare lines"
    );
    assert!(src.contains("actorbootstrappedatthecompare:{};{}"));
    assert!(src.contains("diff_detail(&a,&b,&differ)"));
}
