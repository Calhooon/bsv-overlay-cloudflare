#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::owed_walk_frozen::pins::{fixtures, seeded, Fixture, NOW_MS, TIP};
use crate::pot_hook::tests::SqliteSql;
use std::cell::{Cell, RefCell};

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
    // the record's clock (a pin that moves it judges both sides at one time)
    local.now.set(record.now.get());
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
    // FIX-983: the outside is asked only for word rows the record holds (an empty ask is no ask), so the pin seeds a
    // word for every spender the judge reads.
    const SPENDER_WORDS: &str = "INSERT OR IGNORE INTO tx_words(txid,word,tier,updated_at) SELECT DISTINCT spendingTxid,'seen','hint',1 FROM pot_records WHERE spendingTxid IS NOT NULL";
    let record = d1_judge(&stranded()).await;
    record.conn.execute_batch(SPENDER_WORDS).unwrap();
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
    control_record.conn.execute_batch(SPENDER_WORDS).unwrap();
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
        // D2-ACTOR-E's e_c: the D1 judge's commits counted (its one write batch holds `GAME_DELETE_SQL`).
        if stmts
            .iter()
            .any(|s| s.sql == crate::owed_judge::GAME_DELETE_SQL)
        {
            E_COMMITS.with(|c| c.set(c.get() + 1));
        }
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
        self.actor_call(identity, games, self.event, None).await
    }
    async fn actor_deliver_bounded(
        &self,
        identity: &str,
        games: &[String],
        bound: Option<i64>,
    ) -> Result<Option<Value>, String> {
        self.actor_call(identity, games, self.event, bound).await
    }
    fn event_seq(&self) -> Option<i64> {
        self.event.map(|(_, s)| s)
    }
    fn note_concurrency(&self, n: u32) {
        NOTED_CONCURRENCY.with(|c| c.set(n));
    }
    /// As the object's `/apply`: the apply in its turn, Phase C's asks queued inside it and drained after.
    async fn actor_apply(
        &self,
        identity: &str,
        games: &[String],
        _kind: &str,
    ) -> Result<Option<Value>, String> {
        self.actor_call(identity, games, None, None).await
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
        bound: Option<i64>,
    ) -> Result<Option<Value>, String> {
        E_CALLS.with(|c| c.set(c.get() + 1));
        if self.actor_fails.get() {
            return Err("owed actor: unreachable (pin)".into());
        }
        // FOLD-8's item 11 pin: a word written between the two judges (after the D1 judge, before the actor reads).
        if FOLD8_J_BETWEEN.with(|c| c.replace(false)) {
            self.d1.conn.execute_batch(SPENT_BY_A_STRANGER).unwrap();
        }
        let turn = futures_util::lock::Mutex::new(());
        let (report, _) = apply_event_in_turn(
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
            bound,
        )
        .await?;
        // NEW-H1's pin: a downgrade written while the actor applied (after its record read, before the D1 judge).
        if NEW_H1_INJECT.with(|c| c.replace(false)) {
            self.d1.conn.execute_batch(SPENT_BY_A_STRANGER).unwrap();
        }
        // FIX-983: the object drains the asks lane after its answer (`after_apply`); the port does it here.
        drain_ask_lane(&self.d1).await;
        let rows = self
            .actor
            .run_batch(vec![Stmt::new(ROWS_SQL, vec![Bind::Text(identity.into())])])
            .await?;
        Ok(Some(answer_of(&report, rows[0].rows.clone())))
    }
}
thread_local! {
    /// FIX-983: the fan-out the consumer's pass noted on the port ([`JudgeDb::note_concurrency`]).
    static NOTED_CONCURRENCY: Cell<u32> = const { Cell::new(0) };
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
        &"body[\"owedActor\"] = crate::owed_actor::health_body( actor_snapshot.as_ref(), worker::Date::now().as_millis(), &body[\"owedEvents\"][\"oldestUnackedMarkAgeMs\"], );"
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
        .find(&squash("call_rows(env, identity, READ_WALL_MS, &marked)"))
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
        .actor_call(&id, &games, Some(("pot", 10)), None)
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
            .actor_call(&id, &games, Some(("pot", 100 + n)), None)
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
    let served = health_body(Some(&read), 10_000, &json!(99));
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
    // FIX-982 (bsv-low #982): the figure is the actor's own in EVERY phase; the un-acked mark is served beside it.
    let c = health_body(Some(&read), 10_000, &json!(99));
    assert_eq!(
        (c["oldestUnappliedEventAgeMs"].clone(), c["oldestUnappliedEventSource"].clone(), c["oldestUnackedMarkAgeMs"].clone()),
        (json!(7_000), json!("faulted"), json!(99)),
        "the figure is the actor's own, the mark named apart"
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
        health_body(Some(&read), 10_000, &Value::Null)["oldestUnappliedEventAgeMs"],
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
    assert_eq!(BOOTSTRAP_PAGE_DELAY_MS, 500);
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
    // D2-ACTOR-B-FOLD-14: the page's games are read in its first round's record call (`ApplyOpts::page`).
    assert!(step[..step.find("drain_held(").unwrap()].contains("page:Some((at,after)),"));
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
    let compared = |v: &Value| health_body(Some(v), 0, &Value::Null)["comparedTotal"].clone();
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
    let before = health_body(Some(&pre), 2_000, &json!(99));
    db.run_batch(vec![health_snapshot_stmt(1_500, true)])
        .await
        .unwrap();
    let (_, snap) = crate::owed_events::health_and_actor(&db, &Value::Null).await;
    let after = health_body(snap.as_ref(), 2_000, &json!(99));
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
    let none = health_body(None, 2_000, &json!(99));
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
        bound: None,
        touch: None,
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
    // actor alone under the pace (its pages one page delay apart) plus this process's measured native work is under
    // the design's 60 s. It is not the wasm32 and D1 time (beta's rehearsal reads that), and not a mass bootstrap's,
    // where the fleet's 60 pages a minute make a heavy seat's cold time the fleet's page count over 60 a minute.
    // FIX-962 (bsv-low #962): 19 pages of 32 games (38 of 16 on `3159bc90e`), each three D1 calls whatever its games.
    assert_eq!(pages, 19);
    // D2-ACTOR-B-FOLD-10 (the fifth delta lens's NEW5-L1): the bar is the PER-PAGE work against the pace's room
    // ([`COLD_PAGE_WORK_BAR_MS`]), asserted only in a release profile: a debug build in the parallel suite measured
    // 41 s of native work under a host load of 30 (60,035 and 78,252 ms against the 60 s), and passes alone.
    let per_page = boot_ms / pages as u64;
    eprintln!("fold7_a: {per_page} ms of native work a page (the bar {COLD_PAGE_WORK_BAR_MS} ms in a release build)");
    // FIX-962: the model at the real round trip, with this run's native work a page.
    eprintln!(
        "fold7_a: modelled cold time at {COLD_D1_ROUND_TRIP_MS} ms a D1 call: {} ms",
        cold_bootstrap_ms(
            600,
            BOOTSTRAP_PAGE_GAMES as u64,
            BOOTSTRAP_PAGE_DELAY_MS,
            COLD_D1_ROUND_TRIP_MS,
            per_page
        )
    );
    if !cfg!(debug_assertions) {
        assert!(
            per_page < COLD_PAGE_WORK_BAR_MS,
            "one cold actor's page work {per_page} ms (paced {paced} ms)"
        );
    }
}

/// NEW5-L1: the most native work a cold bootstrap page may take so that the pages, one page delay apart, fit the
/// design's 60 s. FIX-962 (bsv-low #962): at the REAL D1 round trip: the 19 pages' room less their three calls of
/// [`COLD_D1_ROUND_TRIP_MS`]: (60 s - 18 delays) / 19 - 3 x 700 = 584 ms (on `3159bc90e` (60 s - 37 s) / 38 = 605 ms
/// with no round trip at all, which beta's 670 ms p95 read could not meet).
const COLD_PAGE_WORK_BAR_MS: u64 =
    (60_000 - 18 * BOOTSTRAP_PAGE_DELAY_MS) / 19 - COLD_PAGE_D1_CALLS * COLD_D1_ROUND_TRIP_MS;

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
    let body = health_body(Some(&snap), now + 5, &Value::Null);
    assert_eq!(body["asOfMs"], json!(now), "{cursor}");
    assert_eq!(body["healthRead"], json!(true));
    // A row written before the fold (a REAL in the JSON) is read as its stamp too.
    let old = json!({"asOfMs": now as f64, "bars": "[]", "counters": "{}"});
    assert_eq!(
        health_body(Some(&old), now, &Value::Null)["asOfMs"],
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

/// A record that faults every bootstrap page read (`IDENTITY_GAMES_AFTER_SQL`, or since D2-ACTOR-B-FOLD-14 the page's
/// first round that carries it, [`page_select`]) and answers the rest.
struct PageFaults<'a>(&'a SqliteSql);
impl SqlPort for PageFaults<'_> {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        if stmts
            .iter()
            .any(|s| s.sql == crate::owed_judge::IDENTITY_GAMES_AFTER_SQL || s.sql == page_select())
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
        None,
        None,
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
        None,
        None,
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
        None,
        None,
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
    assert!(lever.contains("rows_view(&me,actor,&d1,"));
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
    // D2-ACTOR-D (bsv-low #952): every delivery carries its own event's mark (a delivery with none is covered by the
    // last apply: a dupe the actor answers from its seq check); seq 8 is one of the compare's one-in-eight.
    d_mark(&w, &games[0], "fold8-j-8");
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
        event: Some(("pot", 16)),
        ..w
    };
    d_mark(&w, &games[0], "fold8-j-16");
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    assert_eq!(w.d1.counter("owed_actor_diffs_total"), diffs);
    assert_eq!(w.d1.counter("owed_actor_compare_held_total"), held + 1);
    // The deferred path carries the same bound (wasm32: `D1Judge::defer_actor`).
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let events = squash(include_str!("../owed_events.rs"));
    // D2-ACTOR-D (bsv-low #952): the bound also bounds the D1 judge's acknowledgement and names the actor's seq check.
    // D2-ACTOR-E (bsv-low #954): with the bound, the txids its marks name, told to the actor's delivery.
    assert!(events.contains(&squash("let (bound, marks_touch) = owed_judge::marks_bound_keys(db, identity, games).await?; db.note_marks_touch(identity, marks_touch); let report = owed_judge::judge_subjects_acked_to(db, identity, games, \"owed-events\", bound).await?;")));
    assert!(events.contains(&squash("if !db.defer_actor(identity, games, Some(bound)) { crate::owed_actor::beside(db, identity, games, Some(bound)).await; }")));
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
        3,
        "the compare's three lines (FIX-964 added the lag line)"
    );
    assert!(src.contains("actorbootstrappedatthecompare:{};{}"));
    assert!(src.contains("diff_detail(&a,&b,&differ)"));
}

// ---- D2-ACTOR-D (bsv-low #952): every owed frame's publisher calls the actor directly; a visible sample once per
// event at its first apply; the consumer's per-event cost ----

/// One event's write in the record: a mark of `game` under `(source, key)` (the trigger's own row shape).
fn d_mark(w: &WithActor, game: &str, key: &str) {
    w.d1.conn
        .execute(
            "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('owner',?1,'pot',?2,1) ON CONFLICT(identity,gameId,source,sourceKey) DO UPDATE SET generation=(SELECT COALESCE(MAX(generation),0)+1 FROM owed_dirty)",
            [game, key],
        )
        .unwrap();
}
fn d_marks(w: &WithActor) -> i64 {
    new_h1_marks(w)
}
/// An answer as the object stamps it (its event's write time and its judgement's), noted through the SHIPPED rule.
fn d_note(bars: &mut PendingBars, class: &'static str, answer: &Value) {
    let mut a = answer.clone();
    a["atMs"] = json!(1_000);
    a["judgedAtMs"] = json!(1_300);
    a["applyMs"] = json!(40);
    bars.note_answer(class, &a);
}

/// d_b (bsv-low #952): A DUPE APPLY SAMPLES NOTHING AND AN APPLIED ONE SAMPLES ONCE. The writer's direct call applies
/// the event (its first apply: one `direct` visibility sample); the queue's delivery of the SAME event names its
/// acknowledgement bound and is covered: a dupe (`applied: false`, no apply, no sample), and the consumer's Phase B
/// delivery acknowledges its marks; a redelivery after a newer mark applies again (an apply sample) but is not the
/// event's first apply (no second visibility sample). The fleet's bars: `rowsVisibleMsP95` the first visibility over
/// both classes, `backstopLagMsP95` the queue's own, its bar named. New API (`apply_event_in_turn`'s bound, the answer's
/// `applied`/`firstApply`, `backstopLagMsP95`): does not compile on `046b721e`; RED against `samples_visible`
/// answering `true` (the base's rule: every answer sampled): `left: 2 right: 1`.
#[tokio::test]
async fn d2_actor_d_b_a_dupe_samples_nothing_and_an_applied_event_samples_once() {
    let w = WithActor {
        d1: d1_judge(&stranded()).await,
        actor: mirror(),
        d1_judges: true,
        event: Some(("pot", 41)),
        actor_reads: true,
        actor_fails: Cell::new(false),
    };
    let games = games_of(&w.d1, "owner");
    d_mark(&w, &games[0], "d-b");
    let mut bars = PendingBars::default();
    // the writer's direct call: no bound, always applied, the event's first apply
    let direct = w
        .actor_call("owner", &games, w.event, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (&direct["applied"], &direct["firstApply"]),
        (&json!(true), &json!(true))
    );
    d_note(&mut bars, "direct", &direct);
    // the queue's delivery of the same event names its bound: covered, a dupe
    let bound = crate::owed_judge::marks_bound(&w.d1, "owner", &games)
        .await
        .unwrap();
    assert!(bound > 0, "the event's mark stands");
    let dupe = w
        .actor_call("owner", &games, w.event, Some(bound))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(dupe["applied"], json!(false), "a dupe applied nothing");
    assert_eq!(
        dupe["rounds"],
        json!(0),
        "answered from the seq check, no closure"
    );
    d_note(&mut bars, "queue", &dupe);
    // the consumer's Phase B delivery (the shipped `deliver`): the dupe acknowledges the event's marks
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    assert_eq!(d_marks(&w), 0, "the covered marks acknowledged");
    // a newer mark, then a redelivery of the same event: applied again, never its first apply
    w.d1.conn.execute_batch(SPENT_BY_A_STRANGER).unwrap();
    let bound = crate::owed_judge::marks_bound(&w.d1, "owner", &games)
        .await
        .unwrap();
    let again = w
        .actor_call("owner", &games, w.event, Some(bound))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (&again["applied"], &again["firstApply"]),
        (&json!(true), &json!(false))
    );
    d_note(&mut bars, "queue", &again);
    let visible: usize = bars.visible.values().map(Vec::len).sum();
    assert_eq!(
        visible, 1,
        "one visibility sample for the event: {:?}",
        bars.visible
    );
    assert_eq!(bars.visible.get("direct").map(Vec::len), Some(1));
    assert_eq!(
        bars.apply.get("queue").map(Vec::len),
        Some(1),
        "the redelivery's apply is sampled, the dupe's is not"
    );
    // the fleet's bars: the first visibility over both classes, the backstop's lag apart, its bar named
    let rows = vec![
        json!({"name": bars_state_name("direct"), "cursor": json!({"libRev": "d", "visible": [300, 900]}).to_string(), "tickedAtMs": 1}),
        json!({"name": bars_state_name("queue"), "cursor": json!({"libRev": "d", "visible": []}).to_string(), "tickedAtMs": 1}),
    ];
    let fleet = fleet_bars(&rows, 2_000, "d");
    assert!(
        fleet["rowsVisibleMsP95"].as_u64().is_some(),
        "the first visibility is the direct class's too: {fleet}"
    );
    assert_eq!(fleet["rowsVisibleMsMax"], json!(900));
    assert!(fleet["rowsVisibleDirectMsP95"].as_u64().is_some());
    assert_eq!(
        fleet["backstopLagMsP95"],
        Value::Null,
        "no queue first apply"
    );
    assert_eq!(fleet["backstopLagBarMs"], json!(BACKSTOP_LAG_BAR_MS));
    assert_eq!(BACKSTOP_LAG_BAR_MS, 60_000);
}

/// A direct port over fakes: the identity is the event's key; `ok*` answers an applied first apply, `wall*` meets the
/// wall (`call_apply`'s words), any other answers a fault.
struct FakeDirect(RefCell<Vec<Value>>);
impl crate::owed_events::DirectPort for FakeDirect {
    async fn subjects(
        &self,
        ev: &overlay_discovery::owed_event::OwedEvent,
    ) -> Result<Vec<(String, Vec<String>)>, String> {
        Ok(vec![(ev.source_key.clone(), vec!["g".into()])])
    }
    async fn call(&self, body: Value) -> Result<Value, String> {
        self.0.borrow_mut().push(body.clone());
        let id = body["identity"].as_str().unwrap_or_default().to_string();
        if id.starts_with("ok") {
            Ok(
                json!({"applied": true, "firstApply": true, "atMs": body["atMs"], "judgedAtMs": 2, "applyMs": 1}),
            )
        } else if id.starts_with("wall") {
            Err(format!(
                "owed actor: no answer in {} ms",
                crate::owed_actor::DIRECT_CALL_WALL_MS
            ))
        } else {
            Err("owed actor answered 503: owed actor apply: D1 overloaded".into())
        }
    }
}
fn d_event(
    source: &str,
    key: &str,
    seq: i64,
    facts: Value,
) -> overlay_discovery::owed_event::OwedEvent {
    overlay_discovery::owed_event::OwedEvent {
        v: overlay_discovery::owed_event::OWED_EVENT_V,
        seq,
        source: source.into(),
        source_key: key.into(),
        facts,
        at_ms: 1_000 + seq,
    }
}

/// d_c (bsv-low #952): THE DIRECT SITE'S WALL IS COUNTED AND NEVER FAILS THE WRITE. One direct pass over five
/// published events: three identities (one applies, one meets the wall, one faults), a `word` event carrying its frame
/// and a `txfacts` event (both the queue's alone): every call carries its event's kind, seq and write time (the queue
/// delivery's key), the wall and the fault are counted apart, and the pass returns (no error reaches the writer). The
/// sites: the writers publish, then schedule the pass after their answer (`wait_until`), the overlay hands its set to
/// `POST /internal/owed-direct` from its flush points, and the publish holds what it sent. New API: does not compile on
/// `046b721e` (no direct pass; the record door's own call carried `seq: null`); RED against `is_wall` answering false:
/// `left: (0, 2) right: (1, 1)`.
#[tokio::test]
async fn d2_actor_d_c_the_direct_sites_wall_is_counted_and_never_fails_the_write() {
    let port = FakeDirect(RefCell::default());
    let events = vec![
        d_event("pot", "ok-a", 11, Value::Null),
        d_event("pot", "wall-b", 12, Value::Null),
        d_event("pot", "fault-c", 13, Value::Null),
        d_event(
            "word",
            "w",
            14,
            json!({"frame": {"txid": "ab", "kind": "seen"}}),
        ),
        d_event("txfacts", "t", 15, json!({})),
    ];
    let report = crate::owed_events::direct_with(&port, events, Some(TIP)).await;
    assert_eq!(
        (report.walls, report.faults),
        (1, 1),
        "the wall and the fault counted apart: {report:?}"
    );
    assert_eq!(
        (report.events, report.calls, report.left_to_queue),
        (3, 3, 2)
    );
    let calls = port.0.borrow();
    assert_eq!(calls[0]["kind"], json!("pot"));
    assert_eq!(
        calls[0]["seq"],
        json!(11),
        "the queue delivery's key: never seq null"
    );
    assert_eq!(calls[0]["atMs"], json!(1_011));
    assert!(
        calls.iter().all(|c| c["bound"].is_null()),
        "a direct call always applies"
    );
    // the sites: never awaited by a write's answer
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let events_src = squash(include_str!("../owed_events.rs"));
    let sched = &events_src[events_src.find("pubfnschedule_direct(").unwrap()..];
    let sched = &sched[..sched.find("pubconstDIRECT_BODY_MAX").unwrap()];
    assert!(sched.contains("ctx.wait_until(asyncmove{direct_events(&env,events).await;});"));
    let door = squash(include_str!("../record_post.rs"));
    let at = door
        .find("latency::note_enqueue(crate::owed_events::publish_noted(&ctx.env).await);")
        .unwrap();
    assert!(door[at..at + 700].contains("crate::owed_events::schedule_direct(&ctx.env,wait);"));
    let route = &events_src[events_src.find("pubasyncfndirect_route(").unwrap()..];
    assert!(route.contains("ctx.wait_until(asyncmove{direct_events(&env,events).await;});"));
    assert!(
        route.contains("with_status(202)"),
        "answered before any actor is called"
    );
    let publish = squash(include_str!(
        "../../../overlay-cloudflare/src/owed_events.rs"
    ));
    let env_fn = &publish[publish.find("pubasyncfnpublish_env(").unwrap()..];
    assert!(
        env_fn.contains("hold_direct(&report.sent);"),
        "the publish holds what it sent"
    );
    let pots = squash(include_str!(
        "../../../overlay-cloudflare/src/pot_changes.rs"
    ));
    assert_eq!(
        pots.matches("crate::change_flush::ship_owed_direct(&")
            .count(),
        3,
        "the overlay's three flush points hand their set to the app layer"
    );
    let lib = squash(include_str!("../lib.rs"));
    assert!(lib.contains("req.path()==\"/internal/owed-direct\"{returnowed_events::direct_route(req,&env,&ctx).await;}"));
}

/// d_e (bsv-low #952): AN OUT-OF-ORDER SEQ IS HELD AND FILLED. Phase C (the actor the judge). Two events of one game:
/// seq 5's write, then seq 7's; seq 7's direct call lands first and covers both marks; seq 5's delivery arrives after,
/// names its bound and is a dupe (no apply, no sample), its marks acknowledged with 7's; a third write whose direct
/// call and queue message were both lost holds its mark (never acknowledged unjudged), and the un-acked mark sweep's
/// re-publish (a new seq) fills it: applied, its marks acknowledged, the actor's rows the record's. New API: does not
/// compile on `046b721e`; RED against `covered_at` answering false: seq 5's delivery applied (`applied: true`).
#[tokio::test]
async fn d2_actor_d_e_an_out_of_order_seq_is_held_and_filled() {
    let mut w = WithActor {
        d1: d1_judge(&stranded()).await,
        actor: mirror(),
        d1_judges: false,
        event: Some(("pot", 5)),
        actor_reads: true,
        actor_fails: Cell::new(false),
    };
    let games = games_of(&w.d1, "owner");
    let g0 = vec![games[0].clone()];
    // the actor holds the identity (its bootstrap)
    for chunk in games.chunks(GAMES_PER_APPLY) {
        apply(
            &w.d1,
            &w.actor,
            &Fakes,
            "owner",
            chunk,
            None,
            Some(TIP),
            None,
        )
        .await
        .unwrap();
    }
    d_mark(&w, &g0[0], "k5");
    d_mark(&w, &g0[0], "k7");
    // seq 7's direct call first
    let seven = w
        .actor_call("owner", &g0, Some(("pot", 7)), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(seven["applied"], json!(true));
    // seq 5's delivery after it: the consumer's shipped Phase C delivery (bound read before the call)
    let bound = crate::owed_judge::marks_bound(&w.d1, "owner", &g0)
        .await
        .unwrap();
    let five = w
        .actor_call("owner", &g0, Some(("pot", 5)), Some(bound))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        five["applied"],
        json!(false),
        "seq 5 is covered by seq 7's apply: {five}"
    );
    assert_eq!(
        five["appliedGames"],
        json!(g0),
        "its covered games acknowledged"
    );
    crate::owed_events::deliver(&w, "owner", &g0).await.unwrap();
    assert_eq!(d_marks(&w), 0, "both marks acknowledged");
    // a lost event: its mark written, no direct call, no message
    w.d1.conn.execute_batch(SPENT_BY_A_STRANGER).unwrap();
    let lost = d_marks(&w);
    assert!(lost > 0, "the lost event's marks stand");
    // the sweep re-publishes the un-acked pairs (a new seq each), the consumer routes them
    let pairs = crate::owed_events::sweep(&w.d1, i64::MAX / 4)
        .await
        .unwrap();
    assert!(!pairs.is_empty(), "the sweep finds the lost marks");
    w.event = Some(("pot", 9));
    for (source, key) in pairs {
        let ev = d_event(&source, &key, 9, Value::Null);
        crate::owed_events::route(&w, &ev).await.unwrap();
    }
    assert_eq!(d_marks(&w), 0, "filled: every mark judged and acknowledged");
    // the actor's rows are the record's (a fresh actor over the same record)
    let control = mirror();
    for chunk in games.chunks(GAMES_PER_APPLY) {
        apply(
            &w.d1,
            &control,
            &Fakes,
            "owner",
            chunk,
            None,
            Some(TIP),
            None,
        )
        .await
        .unwrap();
    }
    assert_eq!(stored(&w.actor, "owner"), stored(&control, "owner"));
}

/// The concurrency knob (bsv-low #952, item 3): `OWED_CONSUMER_CONCURRENCY` in each render equals the consumer's
/// `max_concurrency`, 1 in both environments today; a value past 1 is refused while the D1 judge writes.
#[test]
fn d2_actor_d_the_consumer_concurrency_knob_moves_with_the_config() {
    use crate::owed_events::{consumer_concurrency_of, consumer_concurrency_refused};
    assert_eq!(consumer_concurrency_of(None), 1);
    assert_eq!(consumer_concurrency_of(Some("x")), 1);
    assert_eq!(consumer_concurrency_of(Some("0")), 1);
    assert_eq!(consumer_concurrency_of(Some(" 4 ")), 4);
    assert!(consumer_concurrency_refused(2, true));
    assert!(!consumer_concurrency_refused(2, false));
    assert!(!consumer_concurrency_refused(1, true));
    let render = include_str!("../../wrangler.toml");
    let var_line = format!("{} = \"", crate::owed_events::CONSUMER_CONCURRENCY_VAR);
    let vars: Vec<u32> = render
        .lines()
        .filter(|l| l.starts_with(&var_line))
        .map(|l| consumer_concurrency_of(l.split('"').nth(1)))
        .collect();
    assert_eq!(vars, vec![1, 1], "prod and beta");
    for (env, var) in ["", "env.beta."].iter().zip(vars) {
        let head = format!("{env}queues.consumers]]");
        let block = render
            .split("[[")
            .find(|b| {
                b.starts_with(&head)
                    && b.lines().nth(1).is_some_and(|l| {
                        l.starts_with("queue = \"low-owed-events") && !l.contains("-dlq")
                    })
            })
            .unwrap();
        assert!(
            block.contains(&format!("max_concurrency = {var}")),
            "{env}: {block}"
        );
    }
}

/// d_d (bsv-low #952): the consumer's per-event wall on the frozen fixture (the fleet's whale, a chunk of 16 games),
/// before and after, printed (the REPORT's number, not a pin). BEFORE: the base's delivery (the D1 judge, the actor's
/// full apply of a fresh event, the compare on every event). AFTER: each event's write (a mark), the writer's direct
/// call (off the consumer, not timed), then the consumer's Phase B delivery: the actor's seq check (a dupe), the D1
/// judge acknowledging to the bound, the compare on one event in eight.
#[tokio::test]
async fn d2_actor_d_d_the_consumers_per_event_wall_before_and_after() {
    let f = fixtures()
        .into_iter()
        .find(|f| f.name == "d1_fleet")
        .unwrap();
    let mut w = WithActor {
        d1: d1_judge(&f).await,
        actor: mirror(),
        d1_judges: true,
        event: None,
        actor_reads: true,
        actor_fails: Cell::new(false),
    };
    let id = f.identities[0].clone();
    let every = games_of(&w.d1, &id);
    let games: Vec<String> = every.iter().take(16).cloned().collect();
    for chunk in every.chunks(GAMES_PER_APPLY) {
        apply(&w.d1, &w.actor, &Fakes, &id, chunk, None, Some(TIP), None)
            .await
            .unwrap();
    }
    let mark = |w: &WithActor, n: i64| {
        w.d1.conn
            .execute(
                "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES (?1,?2,'pot',?3,1)",
                [id.as_str(), games[0].as_str(), &format!("d-d-{n}")],
            )
            .unwrap();
    };
    let (mut before, mut after) = (Meter::default(), Meter::default());
    for n in 0..8i64 {
        mark(&w, 1000 + n);
        let t = std::time::Instant::now();
        crate::owed_judge::judge_subjects(&w, &id, &games, "owed-events")
            .await
            .unwrap();
        let a = w
            .actor_call(&id, &games, Some(("pot", 1000 + n)), None)
            .await
            .unwrap()
            .unwrap();
        compare(&w, &id, &games, &a, None).await;
        before.note(t.elapsed().as_millis() as u64);
        let seq = 2001 + n;
        mark(&w, seq);
        w.actor_call(&id, &games, Some(("pot", seq)), None)
            .await
            .unwrap();
        w.event = Some(("pot", seq));
        let t = std::time::Instant::now();
        crate::owed_events::deliver(&w, &id, &games).await.unwrap();
        after.note(t.elapsed().as_millis() as u64);
    }
    eprintln!(
        "d2_actor_d d consumer per-event wall ({} games of {}, {} build): before (D1 judge + actor apply + compare) p50 {:?} ms max {:?} ms; after (actor seq check, D1 judge, compare 1 in 8) p50 {:?} ms max {:?} ms",
        games.len(),
        every.len(),
        if cfg!(debug_assertions) { "debug" } else { "release" },
        before.quantile(0.5),
        before.max(),
        after.quantile(0.5),
        after.max()
    );
    assert!(
        after.quantile(0.5) < before.quantile(0.5),
        "the consumer's per-event wall falls"
    );
}

// ---- D2-ACTOR-B-FOLD-9 (bsv-low #953): a courier-found spend is durable evidence the actor reaches ----

const FOLD9_HOP: &str = "b900000000000000000000000000000000000000000000000000000000000000";

/// The stranger's spend of the hop (a P2PKH that pays none of the seat's homes), its raw bytes and txid.
fn fold9_spend(hop: &str) -> (bsv_rs::transaction::Transaction, String) {
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
    let mut spend = Transaction::new();
    spend
        .add_input(TransactionInput::new(hop.to_string(), 0))
        .unwrap();
    spend
        .add_output(TransactionOutput {
            satoshis: Some(20_000),
            locking_script: LockingScript::from_hex(&format!("76a914{}88ac", "11".repeat(20)))
                .unwrap(),
            change: false,
        })
        .unwrap();
    let id = spend.id();
    (spend, id)
}

/// beta's traced hop (#953, record 1484) as the record holds it after the fold, three hours after the belt's find:
/// the index row of the hop unspent and naming no spender, the memo aged out and swept, the find's durable trace the
/// EVIDENCE row (`<hop>:0`) and the spender's bytes (keyed by the spender's txid), the spender SEEN. The byte-identity
/// pin's fixtures gain it ([`all_fixtures`] does not: its frozen set is the walk's; this pin runs it beside them).
fn fold9_fixture() -> Fixture {
    let made = (NOW_MS / 1000) as i64 - 4 * 3_600;
    let found = NOW_MS as i64 - 3 * 3_600_000;
    let (spend, s) = fold9_spend(FOLD9_HOP);
    let bytes = crate::owed::StoredSpenderBytes::from_parts(
        &crate::routes::spender_inputs_of(&spend),
        &crate::routes::spender_outputs_of(&spend),
    )
    .to_json_capped()
    .unwrap();
    Fixture {
        name: "fold9 courier-found spend, memo expired",
        seed: vec![
            format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind) VALUES ('{FOLD9_HOP}',0,0,{made},'p2pkh')"),
            format!("INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('owner','other','{}',0,20190,'seat','00','00',2,'{FOLD9_HOP}',1,{made},1)", h("9f", 1)),
            format!("INSERT INTO hop_chain_probes(outpoint,probedAtMs,spent,spendingTxid,spentConfirmed,tipHeight) VALUES ('{FOLD9_HOP}:0',{found},1,'{s}',1,{})", TIP - 18),
            format!("INSERT INTO owed_spender_bytes(txid,bytesJson,createdAtMs) VALUES ('{s}','{bytes}',{found})"),
            format!("INSERT INTO network_seen(txid,seenAt) VALUES ('{s}',{})", found / 1000),
        ],
        identities: vec!["owner".into()],
    }
}

fn fold9_row(
    db: &SqliteSql,
    outpoint: &str,
) -> Option<(String, Option<i64>, Value, Option<String>)> {
    db.conn
        .query_row(
            "SELECT family,claimable,facts,reason FROM owed_rows WHERE identity='owner' AND outpoint=?1",
            [outpoint],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<i64>>(1)?,
                    serde_json::from_str(&r.get::<_, String>(2)?).unwrap(),
                    r.get::<_, Option<String>>(3)?,
                ))
            },
        )
        .ok()
}

/// What the finding compares of the row: its family, claim, the chain's spender, the spend's kind, its confirmation
/// and the reason.
fn fold9_judgement(r: &(String, Option<i64>, Value, Option<String>)) -> Value {
    json!([
        r.0,
        r.1,
        r.2["chainProbe"],
        r.2["chainSpender"],
        r.2["spendKind"],
        r.2["spentConfirmed"],
        r.3
    ])
}

/// The belt finds the stranded hop spent by a stranger (Phase A), the clock passes the memo's two hours and the
/// overlay's TTL sweep runs (the shipped statement): answers the D1 record and the spender's txid.
async fn fold9_found() -> (WithActor, String) {
    let w = WithActor {
        d1: d1_judge(&stranded()).await,
        actor: mirror(),
        d1_judges: true,
        event: None,
        actor_reads: false,
        actor_fails: Cell::new(false),
    };
    let hop = format!("{STRANDED_HOP}:0");
    let due: i64 =
        w.d1.conn
            .query_row(
                "SELECT beltDueAtMs FROM owed_rows WHERE identity='owner' AND outpoint=?1",
                [&hop],
                |r| r.get(0),
            )
            .unwrap();
    let at = due as u64 + 1;
    w.d1.now.set(at);
    w.actor.now.set(at);
    let (spend, spender) = fold9_spend(STRANDED_HOP);
    let courier = Couriers {
        answers: vec![
            (
                format!("{STRANDED_HOP}/0/spent"),
                (
                    200,
                    json!({"txid": spender, "vin": 0, "status": "confirmed"})
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
    crate::owed_events_publish::drain();
    let report = crate::owed_belt::tick(&w, &courier).await.unwrap();
    assert!(
        report.write_faults.is_empty() && report.found >= 1,
        "{report:?}"
    );
    (w, spender)
}

/// Past the memo's window: the overlay's TTL sweep, as its pass binds it.
fn fold9_expire(db: &SqliteSql) -> usize {
    let later = db.now.get() + 3 * 3_600_000;
    db.now.set(later);
    db.conn
        .execute(
            bsv_overlay_cloudflare::hop_probe_memos::HOP_PROBE_MEMO_EXPIRE_SQL,
            [later as i64 - bsv_overlay_cloudflare::hop_probe_memos::HOP_PROBE_MEMO_TTL_MS],
        )
        .unwrap()
}

fn fold9_probe(db: &SqliteSql, key: &str) -> Option<Value> {
    db.conn
        .query_row(
            "SELECT json_array(spent,spendingTxid,spentConfirmed,tipHeight) FROM hop_chain_probes WHERE outpoint=?1",
            [key],
            |r| r.get::<_, String>(0),
        )
        .ok()
        .map(|s| serde_json::from_str(&s).unwrap())
}

/// (a) THE FIND IS DURABLE EVIDENCE keyed by the spent outpoint: beside the spender's bytes the belt writes
/// `hop_chain_probes` `<hop>:0` with `spent`, `spendingTxid`, `spentConfirmed` and `tipHeight`; the overlay's TTL
/// sweep takes the memo (`<hop>.0`) and leaves the evidence while the hop's index row stands, and takes it once the
/// index row is gone. RED on `046b721e`: no evidence row is written (`left: None`).
#[tokio::test]
async fn fold9_a_the_find_is_durable_evidence_keyed_by_the_spent_outpoint() {
    let (w, spender) = fold9_found().await;
    let evidence = format!("{STRANDED_HOP}:0");
    assert_eq!(
        fold9_probe(&w.d1, &evidence),
        Some(json!([1, spender, 1, TIP])),
        "the evidence row"
    );
    assert!(fold9_probe(&w.d1, &format!("{STRANDED_HOP}.0")).is_some());
    assert!(fold9_expire(&w.d1) >= 1, "the memo is swept");
    assert!(fold9_probe(&w.d1, &format!("{STRANDED_HOP}.0")).is_none());
    assert_eq!(
        fold9_probe(&w.d1, &evidence),
        Some(json!([1, spender, 1, TIP])),
        "the evidence outlives the memo while the index row stands"
    );
    w.d1.conn
        .execute_batch(&format!(
            "DELETE FROM pot_records WHERE txid='{STRANDED_HOP}' AND outputIndex=0"
        ))
        .unwrap();
    fold9_expire(&w.d1);
    assert!(
        fold9_probe(&w.d1, &evidence).is_none(),
        "an evicted hop's evidence ages out as a memo"
    );
    // A look that finds the outpoint unspent retires it.
    let find = crate::owed_belt::Find {
        cleared: vec![evidence.clone()],
        ..Default::default()
    };
    w.d1.conn
        .execute(
            "INSERT INTO hop_chain_probes(outpoint,probedAtMs,spent,spendingTxid) VALUES (?1,1,1,'x')",
            [&evidence],
        )
        .unwrap();
    w.d1.run_batch(find.statements(w.d1.now.get() as i64))
        .await
        .unwrap();
    assert!(fold9_probe(&w.d1, &evidence).is_none());
}

/// (b) THE CLOSURE REACHES THE BYTES by the input outpoint: an actor bootstrapped AFTER the memo's expiry copies the
/// evidence row (the hop's txid's probe range) and, through the spender it names, the spender's bytes into its mirror.
/// RED on `046b721e`: the mirror holds no `owed_spender_bytes` row.
#[tokio::test]
async fn fold9_b_the_closure_reaches_the_spenders_bytes_by_the_input_outpoint() {
    let (w, spender) = fold9_found().await;
    fold9_expire(&w.d1);
    let (local, _) = actor_judge(&w.d1, "owner").await;
    let held = |sql: &str| -> i64 { local.conn.query_row(sql, [&spender], |r| r.get(0)).unwrap() };
    assert_eq!(
        held("SELECT COUNT(*) FROM hop_chain_probes WHERE spendingTxid=?1"),
        1,
        "the evidence in the mirror"
    );
    assert_eq!(
        held("SELECT COUNT(*) FROM owed_spender_bytes WHERE txid=?1"),
        1,
        "the spender's bytes in the mirror"
    );
}

/// (c) THE FIND IS AN EVENT: it marks the subject's game (`hop`, the row's outpoint, a downgrade: it carries a
/// spend) in the find's own batch and notes the pair for the tick's publish, so a bootstrapped actor re-judges on it in
/// every phase. In Phase A the D1 judge reads the find in that batch and acknowledges the mark there (D-H1 (c); the
/// actor is called directly after); in Phase C there is no D1 judge, so the mark stands for the consumer to carry to
/// the actor. RED on `046b721e`: no mark, no note.
#[tokio::test]
async fn fold9_c_the_find_marks_its_game_and_publishes_its_pair() {
    let key = format!("{STRANDED_HOP}:0");
    let mark_of = |db: &SqliteSql| -> Option<(String, i64)> {
        db.conn
            .query_row(
                "SELECT gameId,downgrade FROM owed_dirty WHERE identity='owner' AND source='hop' AND sourceKey=?1",
                [&key],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .ok()
    };
    // Phase A: acknowledged in the find's own judge batch.
    let (a, _) = fold9_found().await;
    assert_eq!(mark_of(&a.d1), None, "Phase A: judged in its own batch");
    assert!(
        crate::owed_events_publish::snapshot()
            .iter()
            .any(|n| n.source == "hop" && n.key == key),
        "the find's pair is noted for the publish"
    );
    // Phase C: no D1 judge; the mark stands for the consumer.
    let w = WithActor {
        d1: seeded(&stranded()),
        actor: mirror(),
        d1_judges: false,
        event: None,
        actor_reads: false,
        actor_fails: Cell::new(false),
    };
    crate::owed_events::deliver(&w, "owner", &games_of(&w.d1, "owner"))
        .await
        .unwrap();
    let (_, _, due) = asks_of(&w.d1)
        .into_iter()
        .find(|(n, _, _)| *n == crate::owed_belt::ask_name("owner", &key))
        .unwrap();
    w.d1.now.set(due as u64 + 1);
    w.actor.now.set(due as u64 + 1);
    let (spend, spender) = fold9_spend(STRANDED_HOP);
    let courier = Couriers {
        answers: vec![
            (
                format!("{STRANDED_HOP}/0/spent"),
                (
                    200,
                    json!({"txid": spender, "vin": 0, "status": "confirmed"})
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
    crate::owed_events_publish::drain();
    let report = crate::owed_belt::tick(&w, &courier).await.unwrap();
    assert!(report.found >= 1 && report.handed_back >= 1, "{report:?}");
    assert_eq!(
        mark_of(&w.d1),
        Some((h("9a", 0), 1)),
        "Phase C: the find's mark stands for the consumer"
    );
    assert!(crate::owed_events_publish::snapshot()
        .iter()
        .any(|n| n.source == "hop" && n.key == key));
}

/// (d) BYTE-IDENTITY AFTER THE MEMO'S EXPIRY: over the same durable find, the D1 judge and an actor bootstrapped
/// afterwards store the same row: family `unbound`, the same `chainSpender`, `spendKind`, `spentConfirmed`, reason;
/// over the find made under the fold and over the fixture shaped as the record holds it, whole rows byte for byte.
/// RED on `046b721e`: the actor answers `hop-stranded` claimable there.
#[tokio::test]
async fn fold9_d_both_judges_agree_on_the_found_spend_after_the_memos_expiry() {
    let (w, spender) = fold9_found().await;
    let key = format!("{STRANDED_HOP}:0");
    let at_find = fold9_row(&w.d1, &key).unwrap();
    assert_eq!(at_find.0, "unbound", "{at_find:?}");
    assert_eq!(at_find.2["chainSpender"], json!(spender));
    fold9_expire(&w.d1);
    let (local, _) = actor_judge(&w.d1, "owner").await;
    let actor = fold9_row(&local, &key).unwrap();
    assert_eq!(
        fold9_judgement(&actor),
        fold9_judgement(&at_find),
        "actor {actor:?}"
    );
    for chunk in games_of(&w.d1, "owner").chunks(16) {
        crate::owed_judge::judge_subjects(&w.d1, "owner", chunk, "d1")
            .await
            .unwrap();
    }
    let d1 = fold9_row(&w.d1, &key).unwrap();
    assert_eq!(fold9_judgement(&d1), fold9_judgement(&at_find), "d1 {d1:?}");
    assert_eq!(stored(&local, "owner"), stored(&w.d1, "owner"));
    // The fixture as the record holds it (the finding's shape), whole rows.
    let f = fold9_fixture();
    let d1 = d1_judge(&f).await;
    let (local, _) = actor_judge(&d1, "owner").await;
    let hop = format!("{FOLD9_HOP}:0");
    let row = fold9_row(&d1, &hop).unwrap();
    assert_eq!(
        (row.0.as_str(), row.1, row.2["chainProbe"].clone()),
        ("unbound", None, json!("spent")),
        "{row:?}"
    );
    assert_eq!(stored(&local, "owner"), stored(&d1, "owner"));
}

/// (e) THE RECONCILE READS ZERO DIFFERING: D1's rows judged at the find, the memo swept, an actor bootstrapped
/// afterwards, the sampled digest compare of that identity (Phase A) finds no diff and re-applies nothing. RED on
/// `046b721e`: `RECONCILE DIFF owner` (the actor's `hop-stranded` against D1's `unbound`).
#[tokio::test]
async fn fold9_e_the_reconcile_reads_zero_outpoints_differing() {
    let (found, _) = fold9_found().await;
    // D1's rows of every game judged at the find (the finding's D1 side), then the memo gone (swept at the TTL's
    // bound) and the actor bootstrapped, at one clock: the digest then compares the judgement alone (a row's
    // `spenderPresence.at` is a clock fact the digest keeps, so a later clock differs on rows this pin is not about).
    for chunk in games_of(&found.d1, "owner").chunks(16) {
        crate::owed_judge::judge_subjects(&found.d1, "owner", chunk, "d1")
            .await
            .unwrap();
    }
    let now = found.d1.now.get();
    found
        .d1
        .conn
        .execute(
            bsv_overlay_cloudflare::hop_probe_memos::HOP_PROBE_MEMO_EXPIRE_SQL,
            [now as i64],
        )
        .unwrap();
    assert!(fold9_probe(&found.d1, &format!("{STRANDED_HOP}.0")).is_none());
    let (local, _) = actor_judge(&found.d1, "owner").await;
    let w = WithActor {
        d1: found.d1,
        actor: local,
        d1_judges: true,
        event: None,
        actor_reads: false,
        actor_fails: Cell::new(false),
    };
    let r = reconcile_step(&w, w.d1.now.get() as i64)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (r.identity.as_str(), r.diff, r.reapplied, r.no_actor),
        ("owner", false, 0, false),
        "{r:?}"
    );
}

/// (f) THE SWEEPS OF OLD MEMOS ARE NOT THE EVIDENCE'S: the overlay's TTL sweep at any age and the moved-latch delete
/// (`PROOF_LATCH_MOVED_DELETE_SQL`, its keys `proofmined:` latches) leave a find row whose index row stands; the
/// memos beside it go. RED on `046b721e`: the TTL sweep deletes the evidence row.
#[tokio::test]
async fn fold9_f_the_sweeps_of_old_memos_leave_the_find_rows() {
    let d1 = seeded(&fold9_fixture());
    let evidence = format!("{FOLD9_HOP}:0");
    d1.conn
        .execute(
            "INSERT INTO hop_chain_probes(outpoint,probedAtMs,spent,spendingTxid,spentConfirmed) VALUES (?1,1,1,?2,1)",
            [format!("{FOLD9_HOP}.0"), h("c1", 1)],
        )
        .unwrap();
    d1.conn
        .execute(
            "INSERT INTO hop_chain_probes(outpoint,probedAtMs,spent,spendingTxid,spentConfirmed) VALUES (?1,1,1,?2,1)",
            [format!("{}:3", h("d1", 1)), h("c1", 2)],
        )
        .unwrap();
    let swept = d1
        .conn
        .execute(
            bsv_overlay_cloudflare::hop_probe_memos::HOP_PROBE_MEMO_EXPIRE_SQL,
            [i64::MAX],
        )
        .unwrap();
    assert_eq!(
        swept, 2,
        "the memo and the evidence of an unindexed outpoint"
    );
    let spender = fold9_probe(&d1, &evidence).unwrap()[1]
        .as_str()
        .unwrap()
        .to_string();
    // A moved proof latch of the same spender: the judge's pass reads the memos (the find row among them, as the
    // hydrate hands them), and the shipped decision names the `proofmined:` key alone; its delete runs.
    let latch = format!(
        "{}{spender}.{}",
        crate::owed::PROOF_MINED_LATCH_PREFIX,
        TIP - 18
    );
    d1.conn
        .execute(
            "INSERT INTO hop_chain_probes(outpoint,probedAtMs,spent,spendingTxid,spentConfirmed) VALUES (?1,1,1,?2,1)",
            [latch.clone(), "aa".repeat(32)],
        )
        .unwrap();
    let memos: Vec<crate::hops_view::ProbeMemo> = [
        (latch.clone(), "aa".repeat(32)),
        (evidence.clone(), spender.clone()),
    ]
    .into_iter()
    .map(|(outpoint, txid)| crate::hops_view::ProbeMemo {
        outpoint,
        probed_at_ms: 1,
        spent: true,
        spending_txid: Some(txid),
        spent_confirmed: Some(true),
        tip_height: None,
    })
    .collect();
    let latches = crate::owed::proof_latches_of(std::slice::from_ref(&spender), &memos);
    let canonical = std::collections::HashMap::from([(TIP - 18, Some("bb".repeat(32)))]);
    let moved = crate::owed::moved_proof_latches(&latches, &canonical);
    assert_eq!(moved, vec![(latch.clone(), "aa".repeat(32))]);
    d1.conn
        .execute(
            crate::owed::PROOF_LATCH_MOVED_DELETE_SQL,
            [crate::owed::moved_proof_latch_delete_bind(&moved).unwrap()],
        )
        .unwrap();
    assert!(
        fold9_probe(&d1, &latch).is_none(),
        "the moved latch is gone"
    );
    assert!(fold9_probe(&d1, &evidence).is_some(), "the find row stands");
}

/// (j) THE STANDING FINDS (made before the fold: memo swept, the spender's bytes their only trace, keyed by the
/// spender's txid) are seeded ONCE by the age repair: the hop's evidence row naming the spender, the seat's game
/// marked (`hop`, a downgrade), the cursor `~` after a short page; a second pass writes nothing; then both judges
/// serve the spend. A hop the index holds spent, or one with evidence already, is left.
#[tokio::test]
async fn fold9_j_the_standing_finds_are_seeded_once() {
    let mut f = fold9_fixture();
    f.seed
        .retain(|s| !s.starts_with("INSERT INTO hop_chain_probes"));
    let d1 = seeded(&f);
    let hop = format!("{FOLD9_HOP}:0");
    assert!(fold9_probe(&d1, &hop).is_none());
    let first = crate::owed_backfill::age_repair(&d1).await.unwrap();
    assert_eq!(
        (
            first.find_evidence_seeded,
            first.find_evidence_cursor.as_deref()
        ),
        (1, Some(crate::owed_backfill::EVICTIONS_DONE))
    );
    let (_, s) = fold9_spend(FOLD9_HOP);
    assert_eq!(fold9_probe(&d1, &hop), Some(json!([1, s, null, null])));
    let marked: i64 = d1
        .conn
        .query_row(
            "SELECT downgrade FROM owed_dirty WHERE identity='owner' AND source='hop' AND sourceKey=?1",
            [&hop],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(marked, 1);
    let second = crate::owed_backfill::age_repair(&d1).await.unwrap();
    assert_eq!(second.find_evidence_seeded, 0);
    for chunk in games_of(&d1, "owner").chunks(16) {
        crate::owed_judge::judge_subjects(&d1, "owner", chunk, "d1")
            .await
            .unwrap();
    }
    let (local, _) = actor_judge(&d1, "owner").await;
    let row = fold9_row(&d1, &hop).unwrap();
    assert_eq!(
        (row.0.as_str(), row.2["chainSpender"].clone()),
        ("unbound", json!(s))
    );
    assert_eq!(stored(&local, "owner"), stored(&d1, "owner"));
}

/// A record port that counts what it answers (the rows the copy reads back, the statements).
struct Counting<'a>(&'a SqliteSql, Cell<usize>, Cell<usize>);
impl SqlPort for Counting<'_> {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        self.2.set(self.2.get() + stmts.len());
        let out = self.0.run_batch(stmts).await?;
        self.1
            .set(self.1.get() + out.iter().map(|o| o.rows.len()).sum::<usize>());
        Ok(out)
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

/// (h) THE COST, measured on FOLD-7's 600-game chained seat: one steady-state apply of one game after the bootstrap,
/// the record's rows answered and statements, and the mirror's statements (the evidence read is the judge's, on the
/// mirror: one key read per 90 hops). Printed for the REPORT; the record's side is bounded as FOLD-7's.
#[tokio::test]
async fn fold9_h_the_rows_read_per_apply_on_the_600_game_seat() {
    let f = fold7_chained_seat(600);
    let record = seeded(&f);
    let games = games_of(&record, "owner");
    let local = mirror();
    for chunk in games.chunks(GAMES_PER_APPLY) {
        apply(
            &record,
            &local,
            &Fakes,
            "owner",
            chunk,
            Some(("bootstrap", 1)),
            Some(TIP),
            None,
        )
        .await
        .unwrap();
    }
    let counted = Counting(&record, Cell::new(0), Cell::new(0));
    let before = local.stmts.get();
    apply(
        &counted,
        &local,
        &Fakes,
        "owner",
        &games[300..301],
        Some(("pot", 2)),
        Some(TIP),
        None,
    )
    .await
    .unwrap();
    eprintln!(
        "fold9_h: one apply of one game of 600: record rows answered {}, record statements {}, mirror statements {}",
        counted.1.get(),
        counted.2.get(),
        local.stmts.get() - before
    );
}

/// (g) THE RECONCILE'S RE-APPLY KEEPS THE JUDGED TIP (bsv-low #953 item 6, the captain's ruling; beta's identity
/// 0247bf52..: every one of its eleven differing rows rewritten by the reconcile's re-apply at 16:49:16Z as
/// `tipUnknown`, the reason "the chain tip is unavailable"): a bootstrapped actor whose rows were judged at tip T,
/// re-applied through the object's own path (`apply_or_hold`) by a body that carries NO tip, judges at its held tip
/// T: every row unchanged, none `tipUnknown`. And the belt's port sends the served tip on every apply, the reconcile's
/// included. RED on `171d1ca3`: the stranded hop's row rewritten `tipUnknown: true`, `snapshotTip: null`.
#[tokio::test]
async fn fold9_g_a_reconcile_reapply_keeps_the_judged_tip() {
    let record = seeded(&stranded());
    let local = mirror();
    let turn = futures_util::lock::Mutex::new(());
    fold7_bootstrap(&turn, &record, &local, "owner").await;
    let before = stored(&local, "owner");
    assert!(!before.is_empty());
    assert!(
        before.iter().all(|r| !r.contains("tipUnknown")),
        "{before:?}"
    );
    let games = games_of(&record, "owner");
    let ev = HeldEvent {
        kind: Some("reconcile".into()),
        seq: None,
        games: games.clone(),
        at_ms: Some(NOW_MS),
        mined: false,
        tip: None,
        held_at_ms: NOW_MS,
        bound: None,
        touch: None,
    };
    let applied = apply_or_hold(&turn, &record, &local, &Fakes, "owner", &ev, None, false)
        .await
        .unwrap();
    assert!(matches!(applied, Applied::Done(..)), "{applied:?}");
    let after = stored(&local, "owner");
    assert!(
        after.iter().all(|r| !r.contains("tipUnknown")),
        "a tipless re-apply wrote tip-unknown over judged rows: {:?}",
        after.iter().find(|r| r.contains("tipUnknown"))
    );
    assert_eq!(after, before, "judged at the held tip, every row unchanged");
    // A never-judged actor with no tip anywhere: nothing held.
    assert_eq!(held_tip(&mirror(), "owner").await, None);
    assert_eq!(held_tip(&local, "owner").await, Some(TIP));
    // The belt's port: every apply carries the served tip; the reconcile's judge is built with it.
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let judge = squash(include_str!("../owed_judge.rs"));
    assert!(judge.contains(&squash(
        "None => crate::owed_events::tip_memoised(&self.sql.env, false).await,"
    )));
    let belt = squash(include_str!("../owed_belt.rs"));
    let reconcile = &belt[belt.find("pubasyncfnreconcile_env(").unwrap()..];
    assert!(
        reconcile[..reconcile.find("reconcile_step(").unwrap()].contains("tip_memoised(env,false)")
    );
}

// ---- D2-ACTOR-B-FOLD-10 (the fifth delta lens on `e99472091`: NEW5-M1, NEW5-M2, NEW5-L1 to NEW5-L5; item 8) ----

/// The greatest tip the actor's rows of `game` were judged at.
fn fold10_probe_tip(actor: &SqliteSql, game: &str) -> Option<i64> {
    actor
        .conn
        .query_row(
            "SELECT MAX(probeTip) FROM owed_rows WHERE identity='owner' AND gameId=?1",
            [game],
            |r| r.get::<_, Option<i64>>(0),
        )
        .unwrap()
}

/// fold10_a (NEW5-M1): A DIRECT APPLY NEVER SWALLOWS A BLOCK'S TIP EVENT. The lens's scratch shape: every game applied
/// at TIP-1, the block's tip mark written, the hook's direct call judged at the isolate's stale memo (TIP-1, no bound),
/// then the consumer's delivery at the FRESH tip with its bound: it applies (`skipped: false`) and the game's stored
/// `probeTip` is the delivery's. A direct call whose tip read faulted judges at the held tip and covers no fresher
/// delivery either. The memo: the hook seeds the announced height (a lower one never replaces a fresher higher), a
/// direct pass over a `tip` event reads fresh, and the hook seeds before it writes the block's marks. RED on
/// `e99472091` (the API grafted inert: `covered_at` without the tip): `skipped=true`, `probeTip` 899999 for 900000.
#[tokio::test]
async fn fold10_a_a_stale_tip_direct_apply_never_makes_the_blocks_delivery_a_dupe() {
    let w = WithActor {
        d1: d1_judge(&stranded()).await,
        actor: mirror(),
        d1_judges: false,
        event: Some(("tip", 11)),
        actor_reads: true,
        actor_fails: Cell::new(false),
    };
    let games = games_of(&w.d1, "owner");
    let g0 = vec![games[0].clone()];
    for chunk in games.chunks(GAMES_PER_APPLY) {
        apply(
            &w.d1,
            &w.actor,
            &Fakes,
            "owner",
            chunk,
            None,
            Some(TIP - 1),
            None,
        )
        .await
        .unwrap();
    }
    d_mark(&w, &g0[0], "gate");
    let turn = futures_util::lock::Mutex::new(());
    let (direct, _) = apply_event_in_turn(
        &turn,
        &w.d1,
        &w.actor,
        &Fakes,
        "owner",
        &g0,
        Some(("tip", 11)),
        Some(TIP - 1),
        None,
        true,
        None,
    )
    .await
    .unwrap();
    assert!(!direct.skipped);
    let bound = crate::owed_judge::marks_bound(&w.d1, "owner", &g0)
        .await
        .unwrap();
    let (delivery, _) = apply_event_in_turn(
        &turn,
        &w.d1,
        &w.actor,
        &Fakes,
        "owner",
        &g0,
        Some(("tip", 11)),
        Some(TIP),
        None,
        true,
        Some(bound),
    )
    .await
    .unwrap();
    assert!(
        !delivery.skipped,
        "the block's delivery applies over a stale-tip coverage"
    );
    assert_eq!(fold10_probe_tip(&w.actor, &g0[0]), Some(TIP as i64));
    // The same delivery again is a dupe now (covered at its tip), and a delivery naming no tip compares as -1.
    let (again, _) = apply_event_in_turn(
        &turn,
        &w.d1,
        &w.actor,
        &Fakes,
        "owner",
        &g0,
        Some(("tip", 11)),
        Some(TIP),
        None,
        true,
        Some(bound),
    )
    .await
    .unwrap();
    assert!(again.skipped, "covered at the block's tip: a dupe");
    assert!(covered_at(&w.actor, &g0, bound, None).await);
    assert!(!covered_at(&w.actor, &g0, bound, Some(TIP + 1)).await);
    // The held tip: a tipless direct body at an actor whose rows stand at TIP-1 covers no delivery at TIP.
    let w2 = WithActor {
        d1: d1_judge(&stranded()).await,
        actor: mirror(),
        d1_judges: false,
        event: Some(("tip", 12)),
        actor_reads: true,
        actor_fails: Cell::new(false),
    };
    for chunk in games.chunks(GAMES_PER_APPLY) {
        apply(
            &w2.d1,
            &w2.actor,
            &Fakes,
            "owner",
            chunk,
            None,
            Some(TIP - 1),
            None,
        )
        .await
        .unwrap();
    }
    w2.actor
        .conn
        .execute(
            "INSERT INTO meta(name,value) VALUES ('bootstrapped','1') ON CONFLICT(name) DO NOTHING",
            [],
        )
        .unwrap();
    d_mark(&w2, &g0[0], "gate");
    let tipless = HeldEvent {
        kind: Some("tip".into()),
        seq: Some(12),
        games: g0.clone(),
        at_ms: Some(NOW_MS - 1_000),
        mined: false,
        tip: None,
        held_at_ms: NOW_MS,
        bound: None,
        touch: None,
    };
    let turn2 = futures_util::lock::Mutex::new(());
    let a = apply_or_hold(
        &turn2, &w2.d1, &w2.actor, &Fakes, "owner", &tipless, None, true,
    )
    .await
    .unwrap();
    assert!(matches!(a, Applied::Done(..)), "{a:?}");
    let bound2 = crate::owed_judge::marks_bound(&w2.d1, "owner", &g0)
        .await
        .unwrap();
    assert!(
        !covered_at(&w2.actor, &g0, bound2, Some(TIP)).await,
        "a held-tip apply covers no fresher delivery"
    );
    // The memo and the hook.
    crate::owed_events::seed_tip_at(TIP - 1, 1_000.0);
    crate::owed_events::seed_tip_at(TIP, 2_000.0);
    assert_eq!(crate::owed_events::memo_tip_at(2_500.0), Some(TIP));
    crate::owed_events::seed_tip_at(TIP - 3, 3_000.0);
    assert_eq!(
        crate::owed_events::memo_tip_at(3_500.0),
        Some(TIP),
        "a lower height never replaces a fresh higher one"
    );
    assert!(crate::owed_events::direct_reads_fresh(&[
        d_event("pot", "a", 1, Value::Null),
        d_event("tip", "gate", 2, Value::Null)
    ]));
    assert!(!crate::owed_events::direct_reads_fresh(&[d_event(
        "pot",
        "a",
        1,
        Value::Null
    )]));
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let hook = squash(include_str!("../internal_events.rs"));
    let hook = &hook[hook.find("pubasyncfntip_changed(").unwrap()..];
    let seed = hook.find("crate::owed_events::seed_tip(height);").unwrap();
    assert!(seed < hook.find("write_tip_marks(&judge,height)").unwrap());
    assert!(
        seed < hook
            .find("crate::owed_events::schedule_direct(env,ctx);")
            .unwrap()
    );
    let events = squash(include_str!("../owed_events.rs"));
    assert!(events.contains("lettip=tip_memoised(env,direct_reads_fresh(&events)).await;"));
}

/// fold10_b (NEW5-M1, Phase A): THE SAMPLED COMPARE COUNTS NO REAL DIFF FOR AN ORDINARY BLOCK. The block's `tip` event
/// on a sampled seq: the hook's direct apply at the previous height, then the consumer's delivery through the shipped
/// `deliver` (the D1 judge at the block, the actor at the block under its bound): the gate opens on both judges, so
/// `owed_actor_diffs_total` does not move and the actor's rows are judged at the block.
#[tokio::test]
async fn fold10_b_an_ordinary_block_is_no_real_diff_in_phase_a() {
    let w = WithActor {
        d1: d1_judge(&stranded()).await,
        actor: mirror(),
        d1_judges: true,
        event: Some(("tip", 16)),
        actor_reads: false,
        actor_fails: Cell::new(false),
    };
    assert!(compare_sampled(Some(16)));
    let games = games_of(&w.d1, "owner");
    for chunk in games.chunks(GAMES_PER_APPLY) {
        apply(
            &w.d1,
            &w.actor,
            &Fakes,
            "owner",
            chunk,
            None,
            Some(TIP - 1),
            None,
        )
        .await
        .unwrap();
    }
    let g0 = vec![games[0].clone()];
    d_mark(&w, &g0[0], "gate");
    let turn = futures_util::lock::Mutex::new(());
    apply_event_in_turn(
        &turn,
        &w.d1,
        &w.actor,
        &Fakes,
        "owner",
        &g0,
        Some(("tip", 16)),
        Some(TIP - 1),
        None,
        false,
        None,
    )
    .await
    .unwrap();
    let diffs = w.d1.counter("owed_actor_diffs_total");
    let compared = w.d1.counter("owed_actor_compared_total");
    crate::owed_events::deliver(&w, "owner", &g0).await.unwrap();
    assert_eq!(
        w.d1.counter("owed_actor_compared_total"),
        compared + 1,
        "the sampled event compared"
    );
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
        "an ordinary block is no STOP: {lines:?}"
    );
    assert_eq!(fold10_probe_tip(&w.actor, &g0[0]), Some(TIP as i64));
}

/// fold10_c (NEW5-M2): A MEMPOOL SPEND THE BELT FOUND THAT NEVER MINES RESTORES THE HOP'S HONEST PRESS within the
/// belt's cadence. The belt finds the stranded hop spent by a stranger's UNCONFIRMED transaction (WoC's `/spent`
/// `status: unconfirmed`): the row is `unbound`, the evidence durable; the memo ages out and the TTL sweep leaves the
/// evidence (the hop's index row stands). The row rests on an unconfirmed find, so it is DATED and its ask is the
/// OUTPOINT (never the spender, whose bytes are held); at its due time the couriers say the outpoint is unspent (the
/// spend was dropped): the look clears the evidence and the row is the hop's sweep press again. The seed's
/// `spentConfirmed`: 1 where a checked proof of the spender is latched, NULL where nothing proves it. RED on
/// `e99472091` (the predicate grafted inert): `beltDueAtMs` NULL, the row `unbound` for ever.
#[tokio::test]
async fn fold10_c_a_dropped_mempool_spend_restores_the_sweep_press() {
    let w = WithActor {
        d1: d1_judge(&stranded()).await,
        actor: mirror(),
        d1_judges: true,
        event: None,
        actor_reads: false,
        actor_fails: Cell::new(false),
    };
    let hop = format!("{STRANDED_HOP}:0");
    let due_of = |w: &WithActor| -> Option<i64> {
        w.d1.conn
            .query_row(
                "SELECT beltDueAtMs FROM owed_rows WHERE identity='owner' AND outpoint=?1",
                [&hop],
                |r| r.get(0),
            )
            .unwrap()
    };
    let at = due_of(&w).unwrap() as u64 + 1;
    w.d1.now.set(at);
    w.actor.now.set(at);
    let (spend, spender) = fold9_spend(STRANDED_HOP);
    let mempool = Couriers {
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
    crate::owed_events_publish::drain();
    let report = crate::owed_belt::tick(&w, &mempool).await.unwrap();
    assert!(report.found >= 1, "{report:?}");
    let found = fold9_row(&w.d1, &hop).unwrap();
    assert_eq!(found.0, "unbound", "{found:?}");
    assert_ne!(found.2["spentConfirmed"], json!(true), "{found:?}");
    assert!(crate::owed_belt::rests_on_unconfirmed_find(&found.2));
    let due = due_of(&w).expect("a row on an unconfirmed find is dated");
    // The memo's two hours pass; the TTL sweep leaves the evidence.
    fold9_expire(&w.d1);
    assert!(fold9_probe(&w.d1, &hop).is_some(), "the evidence stands");
    let later = w.d1.now.get().max(due as u64 + 1);
    w.d1.now.set(later);
    w.actor.now.set(later);
    let candidate = crate::owed_belt::candidate_of(&json!({
        "identity": "owner", "outpoint": hop, "gameId": "g", "family": "unbound",
        "facts": found.2.to_string(), "reason": found.3, "claimable": found.1,
    }));
    assert!(
        matches!(
            crate::owed_belt::ask_of(&candidate),
            Some(crate::owed_belt::Ask::Outpoint { .. })
        ),
        "the ask is the outpoint, never the spender"
    );
    // The spend was dropped: WoC holds no spend, BananaBlocks says unspent.
    let dropped = Couriers {
        answers: vec![("/spend".to_string(), (200, br#"{"spent":false}"#.to_vec()))],
        ..Default::default()
    };
    let report = crate::owed_belt::tick(&w, &dropped).await.unwrap();
    assert!(report.found >= 1, "{report:?}");
    assert!(fold9_probe(&w.d1, &hop).is_none(), "the evidence retired");
    let row = fold9_row(&w.d1, &hop).unwrap();
    assert_eq!(
        (row.0.as_str(), row.1),
        ("hop-stranded", Some(1)),
        "the honest sweep press is back: {row:?}"
    );
    // The seed's confirmation (the one-time seeding of the finds made before FOLD-9).
    let seed = crate::owed_backfill::FIND_EVIDENCE_SEED_SQL;
    assert!(seed.contains("'proofmined:'||b.txid||'.'"));
    assert!(seed.contains("t.has_proof=1) THEN 1 ELSE NULL END"));
    assert!(!seed.contains("b.txid,NULL,NULL"));
}

/// fold10_d (NEW5-L1): `fold7_a`'s cold bar asserts its page count always and its per-page work only in a release
/// profile, never a wall clock in the debug parallel suite (it red at 60,035 and 78,252 ms under load).
#[test]
fn fold10_d_the_cold_bar_is_no_wall_clock_in_a_debug_build() {
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let src = squash(include_str!("tests.rs"));
    let a = &src[src.find("asyncfnd2_actor_b_fold7_a_").unwrap()..];
    let a = &a[..a.find("asyncfnd2_actor_b_fold7_b_").unwrap()];
    assert!(a.contains("assert_eq!(pages,19);"));
    assert!(
        !a.contains("assert!(paced<60_000"),
        "no wall-clock bar in the debug suite"
    );
    assert!(a.contains("if!cfg!(debug_assertions){"));
    assert!(a.contains("per_page<COLD_PAGE_WORK_BAR_MS"));
    assert_eq!(
        COLD_PAGE_WORK_BAR_MS,
        (60_000 - 18 * BOOTSTRAP_PAGE_DELAY_MS) / 19 - COLD_PAGE_D1_CALLS * COLD_D1_ROUND_TRIP_MS
    );
}

/// fold10_e (NEW5-L2): THE CONSUMER DISCARDS ONLY ITS OWN EVENTS. A writer's event and the consumer's continuation
/// held in the isolate's direct set: the consumer's take removes its own and leaves the writer's for the writer's
/// flush point; the belt's tick calls its published set directly. New API (`take_direct_where`, `consumer_owns`):
/// does not compile on `e99472091`, where the consumer took the whole set (`take_direct()`).
#[test]
fn fold10_e_the_consumer_takes_its_own_direct_events_alone() {
    let _ = crate::owed_events_publish::take_direct();
    let ev = d_event("tip", "gate", 40, Value::Null);
    crate::owed_events_publish::hold_direct(&[
        d_event("pot", "writer:0", 41, Value::Null),
        d_event("tip", "gate", 42, Value::Null),
    ]);
    let own = crate::owed_events_publish::take_direct_where(|e| {
        crate::owed_events::consumer_owns(&ev, e)
    });
    assert_eq!(own.len(), 1);
    assert_eq!(own[0].seq, 42);
    let left = crate::owed_events_publish::take_direct();
    assert_eq!(left.len(), 1, "the writer's event stays held");
    assert_eq!(left[0].source_key, "writer:0");
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let events = squash(include_str!("../owed_events.rs"));
    let consume = &events[events.find("pubasyncfnconsume_one(").unwrap()..];
    let consume = &consume[..consume.find("pubfnconsumer_owns(").unwrap()];
    assert!(consume.contains("publish::take_direct_where(|e|consumer_owns(ev,e))"));
    assert!(!consume.contains("publish::take_direct()"));
    let belt = squash(include_str!("../owed_belt.rs"));
    let tick = &belt[belt.find("pubasyncfntick_env(").unwrap()..];
    let tick = &tick[..tick.find("pubasyncfnreconcile_env(").unwrap()];
    assert!(tick.contains("crate::owed_events::direct_held(env).await;"));
}

/// fold10_f (NEW5-L3): A BURST FOR ONE IDENTITY IS ONE CALL with the union of its games, never N calls: three events
/// naming one identity (a JOIN's pot, party and hop pairs) and one naming another are two calls, the first carrying
/// the union, the first event's key, the earliest write time, the others' keys (`also`) and `class: "direct"`; a
/// union past 16 games is its chunks. The overlay's hand-off shares one wall bounded by the flush's own deadline,
/// read before it. RED on `e99472091`: one call an event (`left: 4 right: 2`).
#[tokio::test]
async fn fold10_f_a_burst_for_one_identity_is_one_call() {
    struct Burst(RefCell<Vec<Value>>);
    impl crate::owed_events::DirectPort for Burst {
        async fn subjects(
            &self,
            ev: &overlay_discovery::owed_event::OwedEvent,
        ) -> Result<Vec<(String, Vec<String>)>, String> {
            Ok(match ev.source_key.as_str() {
                "b" => vec![("other".into(), vec!["gb".into()])],
                k => vec![("Owner".into(), vec!["g0".into(), format!("g-{k}")])],
            })
        }
        async fn call(&self, body: Value) -> Result<Value, String> {
            self.0.borrow_mut().push(body);
            Ok(json!({"applied": true, "firstApply": true}))
        }
    }
    let port = Burst(RefCell::default());
    let events = vec![
        d_event("pot", "p", 50, Value::Null),
        d_event("party", "q", 48, Value::Null),
        d_event("pot", "b", 51, Value::Null),
        d_event("hop", "r", 52, Value::Null),
    ];
    let report = crate::owed_events::direct_with(&port, events, Some(TIP)).await;
    assert_eq!(report.calls, 2, "{:?}", port.0.borrow());
    let calls = port.0.borrow();
    let owner = &calls[0];
    assert_eq!(owner["identity"], json!("owner"));
    assert_eq!(owner["games"], json!(["g0", "g-p", "g-q", "g-r"]));
    assert_eq!(
        (owner["kind"].clone(), owner["seq"].clone()),
        (json!("pot"), json!(50))
    );
    assert_eq!(owner["atMs"], json!(1_048), "the earliest write time");
    assert_eq!(owner["also"], json!([["party", 48], ["hop", 52]]));
    assert_eq!(owner["class"], json!("direct"));
    assert!(calls[1]["also"].is_null());
    let many: Vec<(EventCtx, crate::owed_events::Subjects)> = (0..3)
        .map(|n| {
            (
                EventCtx {
                    kind: "pot".into(),
                    seq: n,
                    at_ms: 1,
                    mined: false,
                    txids: None,
                    also: Vec::new(),
                    by_identity: Default::default(),
                },
                vec![(
                    "owner".into(),
                    (0..GAMES_PER_APPLY / 2 + 1)
                        .map(|g| format!("g{n}-{g}"))
                        .collect(),
                )],
            )
        })
        .collect();
    assert_eq!(
        crate::owed_events::coalesce_direct(many, None).len(),
        2,
        "a union past the call's games is its chunks"
    );
    // The overlay's hand-off: one wall, the flush's deadline read before it.
    use bsv_overlay_cloudflare::change_flush::{owed_direct_end, OWED_DIRECT_POST_WALL_MS};
    assert_eq!(
        owed_direct_end(1_000, 50_000),
        1_000 + OWED_DIRECT_POST_WALL_MS
    );
    assert_eq!(owed_direct_end(1_000, 1_500), 1_500);
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let pots = squash(include_str!(
        "../../../overlay-cloudflare/src/pot_changes.rs"
    ));
    assert_eq!(
        pots.matches("crate::change_flush::ship_owed_direct(&env,deadline_ms).await;")
            .count()
            + pots
                .matches("crate::change_flush::ship_owed_direct(&env2,deadline_ms).await;")
                .count(),
        3
    );
    for (n, _) in pots.match_indices("crate::change_flush::ship_owed_direct(&") {
        assert!(
            pots[..n].rfind("letdeadline_ms=").is_some()
                || pots[..n].contains("BackgroundFlush::Ship{deadline_ms}")
        );
    }
}

/// fold10_g (NEW5-L4): A BUILD'S REPLACE OF THE BARS ROWS CARRIES THE OLDEST UNAPPLIED WRITE TIME: the samples of the
/// old build go, `oldestUnappliedAtMs` stays (a fact about the fleet's actors); a new value replaces it, a clear
/// clears it. RED on `e99472091`: the replaced row has no `oldestUnappliedAtMs` (`left: Null right: 1234`).
#[test]
fn fold10_g_a_builds_replace_carries_the_oldest_unapplied() {
    let old =
        json!({"libRev": "old", "apply": [5, 6], "visible": [7], "oldestUnappliedAtMs": 1234})
            .to_string();
    let row: Value =
        serde_json::from_str(&merge_bars_row(Some(&old), &[1], &[], None, "new")).unwrap();
    assert_eq!(row["libRev"], json!("new"));
    assert_eq!(row["apply"], json!([1]), "the old build's samples go");
    assert_eq!(row["oldestUnappliedAtMs"], json!(1234));
    let row: Value =
        serde_json::from_str(&merge_bars_row(Some(&old), &[], &[], Some(Some(99)), "new")).unwrap();
    assert_eq!(row["oldestUnappliedAtMs"], json!(99));
    let row: Value =
        serde_json::from_str(&merge_bars_row(Some(&old), &[], &[], Some(None), "new")).unwrap();
    assert_eq!(row["oldestUnappliedAtMs"], Value::Null);
}

/// fold10_h (NEW5-L5): THE OBJECT NOTES ITS FIRST-APPLY VISIBILITY ITSELF. A direct call that walled still has its
/// event applied (the caller gone): the object notes the answer into the fleet's bars under the call's class and marks
/// it `noted`, so the caller's note of a noted answer adds no sample (only its unapplied bookkeeping), and the
/// consumer's later covered delivery samples nothing. The object's source: noted before it answers, an event apply
/// only (the tip alarm notes nothing). RED on `e99472091`: the object's samples reach its local meter only.
#[test]
fn fold10_h_the_object_notes_its_own_first_apply() {
    let answer = json!({"applied": true, "firstApply": true, "identity": "owner", "atMs": 1_000, "judgedAtMs": 1_300, "applyMs": 40});
    let mut object = PendingBars::default();
    object.note_answer(bars_class(Some("direct"), None), &answer);
    assert_eq!(object.visible.get("direct").map(Vec::len), Some(1));
    let mut noted = answer.clone();
    noted["noted"] = json!(true);
    let mut caller = PendingBars::default();
    caller.unapplied.insert("owner".into(), 5);
    caller.note_answer("direct", &noted);
    assert!(
        caller.visible.is_empty() && caller.apply.is_empty(),
        "no second sample"
    );
    assert!(
        caller.unapplied.is_empty(),
        "the caller's unapplied bookkeeping kept"
    );
    assert_eq!(bars_class(None, None), "queue");
    assert_eq!(bars_class(Some("tip"), None), "queue");
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let object = squash(include_str!("object.rs"));
    let route = &object[object.find("asyncfnapply_route(").unwrap()..];
    let note = route
        .find("ifsamples{note_answer_as(bars_class(body.class.as_deref(),body.concurrent),&answer);answer[\"noted\"]=json!(true);}")
        .expect("the object notes its own answer");
    assert!(route.contains("letsamples=samples_bars(body.at_ms,body.class.as_deref());"), "the note is gated by samples_bars (FOLD-16 item 5)");
    assert!(
        note < route
            .find("Response::from_json(&answer)\n")
            .unwrap_or(usize::MAX)
            .min(route.len())
    );
}

/// fold10_i (item 8): THE DIRECT PATH'S FAULTS LOG THEIR FIRST TEXT once a minute an isolate
/// (`[owed-actor] direct fault: <status or error>`), so an operator can read why `direct.faults` moved. RED on
/// `e99472091`: no fault text is kept (`DirectReport::first_fault` absent; the line counted only).
#[tokio::test]
async fn fold10_i_the_direct_faults_log_their_first_text_once_a_minute() {
    let port = FakeDirect(RefCell::default());
    let report = crate::owed_events::direct_with(
        &port,
        vec![
            d_event("pot", "ok-a", 1, Value::Null),
            d_event("pot", "fault-b", 2, Value::Null),
            d_event("pot", "wall-c", 3, Value::Null),
        ],
        None,
    )
    .await;
    assert_eq!((report.faults, report.walls), (1, 1));
    let text = report.first_fault.clone().unwrap();
    assert!(
        text.contains("503") || text.contains("no answer in"),
        "{text}"
    );
    let line = crate::owed_events::direct_fault_line(&report, 100_000, None).unwrap();
    assert!(line.starts_with("[owed-actor] direct fault: "), "{line}");
    assert!(line.contains(&text));
    assert!(crate::owed_events::direct_fault_line(&report, 130_000, Some(100_000)).is_none());
    assert!(crate::owed_events::direct_fault_line(&report, 160_000, Some(100_000)).is_some());
    let clean = crate::owed_events::DirectReport::default();
    assert!(crate::owed_events::direct_fault_line(&clean, 0, None).is_none());
    let logged = RefCell::new(Vec::<String>::new());
    crate::owed_events::log_direct_fault(&report, 500_000, |l| logged.borrow_mut().push(l.into()));
    crate::owed_events::log_direct_fault(&report, 510_000, |l| logged.borrow_mut().push(l.into()));
    assert_eq!(logged.borrow().len(), 1, "once a minute");
}

/// fold10_j (the captain's item of 2026-10-29, beta's fourth RECONCILE DIFF): A STATE JUDGED UNDER ANOTHER BUILD IS
/// JUDGED AGAIN. An actor bootstrapped on FOLD-7's build kept a row the FOLD-9 judge no longer produces: the lever's
/// `arm_bootstrap` answered `already` whenever `bootstrapped` was set. Now the bootstrap's end stamps the build
/// (`judgedLibRev`), and a bootstrapped actor whose stamp is another build's (or absent: stamped before this fold) is
/// re-armed from the start on the lever and after an event it applies, counted `owed_actor_rejudged_total` (both
/// Workers' lists), served beside `bootstraps`; the lever's answer names `rejudged` apart from `armed`; the same build
/// answers `already`. RED on `e99472091`: `rejudge_due` does not exist (the lever answered `already` for every stamp).
#[test]
fn fold10_j_a_state_judged_under_another_build_bootstraps_again() {
    // D2-ACTOR-B-FOLD-11 (NEW6-L2): the stamp is the judge's version, not the build (`fold11_f`)
    let running = JUDGE_VERSION;
    assert!(
        !rejudge_due(true, Some(running), running),
        "the same build: already"
    );
    assert!(rejudge_due(true, Some("an-older-build"), running));
    assert!(
        rejudge_due(true, None, running),
        "stamped before the fold: re-judged"
    );
    assert!(
        !rejudge_due(false, None, running),
        "a cold actor bootstraps as before"
    );
    assert_eq!(COUNTERS[26], "owed_actor_rejudged_total");
    assert_eq!(
        bsv_overlay_cloudflare::applayer_counters::OWED_ACTOR[26],
        COUNTERS[26]
    );
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let object = squash(include_str!("object.rs"));
    let arm = &object[object.find("asyncfnarm_bootstrap(").unwrap()..];
    let arm = &arm[..arm
        .find("ifself.meta(META_BOOT_CURSOR).is_some(){ifboot_parked")
        .unwrap()];
    let check = arm
        .find("if!rejudge_due(true,judged.as_deref(),JUDGE_VERSION){returnArmed{already:true,")
        .expect("`already` only under the running build");
    let rearm = arm
        .find("self.put_meta(&local,META_BOOT_CURSOR,BOOT_CURSOR_START);")
        .unwrap();
    assert!(check < rearm);
    assert!(arm.contains("record.counters(vec![(COUNTERS[26].into(),1)]).await;"));
    assert!(arm.contains("rejudged:true"));
    let done = &object[object.find("BootStep::Done{drain,..}=>{").unwrap()..];
    assert!(done[..400].contains("self.put_meta(local,META_JUDGED_REV,JUDGE_VERSION);"));
    let route = &object[object.find("asyncfnapply_route(").unwrap()..];
    assert!(route.contains("JUDGE_VERSION,){self.arm_bootstrap(&me,false).await;}"));
    assert!(object.contains("\"armed\":armed,\"rejudged\":rejudged,\"bootstrapped\":done,"));
    let actor = squash(include_str!("../owed_actor.rs"));
    assert!(actor.contains("body[\"rejudged\"]=n(COUNTERS[26]);"));
    assert!(
        actor.contains("'owed_actor_direct_walls_total','owed_actor_rejudged_total','owed_actor_compare_tip_lag_total','owed_actor_reconcile_stale_total','owed_d1_rejudge_identities_total','owed_d1_rejudge_faults_total'))AScounters")
    );
}

// ---- D2-ACTOR-E (bsv-low #954): the apply costs the event's own rows; a burst is one judge pass; the consumer
// coalesces per identity; a cold seat's re-published events never sample the warm bars ----

thread_local! {
    /// e_c: the actor deliveries and the D1 judge's commits a delivery made ([`WithActor`]).
    static E_CALLS: Cell<usize> = const { Cell::new(0) };
    static E_COMMITS: Cell<usize> = const { Cell::new(0) };
}

/// A record port that yields before every batch, as D1 does (an await that lets another caller in): the burst's
/// events queue up behind the turn the way they do inside the object.
struct Yielding<'a>(&'a SqliteSql);
impl SqlPort for Yielding<'_> {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        tokio::task::yield_now().await;
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

/// The stored rows of one game.
fn stored_game(db: &SqliteSql, identity: &str, game: &str) -> Vec<String> {
    let mut q = db.conn.prepare("SELECT json_array(outpoint,family,gameId,sats,opponentIdentity,atHeight,facts,reason,claimable,gateHeight,beltDueAtMs,probeTip) FROM owed_rows WHERE identity=?1 AND gameId=?2 ORDER BY outpoint").unwrap();
    q.query_map([identity, game], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// The 600-game chained seat with its whole closure read (every game warm).
async fn e_warm_seat(n: usize) -> (SqliteSql, SqliteSql, Vec<String>) {
    let record = seeded(&fold7_chained_seat(n));
    let games = games_of(&record, "owner");
    let local = mirror();
    for chunk in games.chunks(GAMES_PER_APPLY) {
        apply(
            &record,
            &local,
            &Fakes,
            "owner",
            chunk,
            Some(("bootstrap", 1)),
            Some(TIP),
            None,
        )
        .await
        .unwrap();
    }
    (record, local, games)
}

/// One write to game `k` of the chained seat: its hop's stake confirmed spent (`pot_records` `(H_k, 0)`), the
/// trigger's mark (or, where the fixture carries no trigger, the mark the trigger writes).
fn e_write(record: &SqliteSql, games: &[String], k: usize) -> String {
    let hop = h("b8", k);
    record
        .conn
        .execute(
            "UPDATE pot_records SET spentConfirmed=1,spentHeight=900001 WHERE txid=?1 AND outputIndex=0",
            [&hop],
        )
        .unwrap();
    record
        .conn
        .execute(
            "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('owner',?1,'pot',?2,1) ON CONFLICT(identity,gameId,source,sourceKey) DO UPDATE SET generation=(SELECT COALESCE(MAX(generation),0)+1 FROM owed_dirty)",
            [&games[k], &format!("{hop}:0")],
        )
        .unwrap();
    hop
}

/// (e_a) A WARM APPLY READS THE EVENT'S OWN ROWS (bsv-low #954 item 1): on FOLD-7's 600-game chained seat, every game
/// warm, one write to game 300 applied with the txids its event named reads the record in at most 3 statements (the
/// pending marks, then the event's rows in one compound statement): beta's figure was about 24 statements an apply
/// (one a family, FOLD-9's), and the base's `apply` of the same event, measured here as `fold9_h` measures it, is 25.
/// The rows it writes are the rows a cold apply of that game writes from the whole record. New API (`apply_with`,
/// `ApplyOpts`); on `e99472091` the pin does not compile, and the base's apply of this event reads 25 statements.
#[tokio::test]
async fn d2_actor_e_a_a_warm_apply_reads_the_events_rows_under_4_statements() {
    let (record, local, games) = e_warm_seat(600).await;
    let hop = e_write(&record, &games, 300);
    let counted = Counting(&record, Cell::new(0), Cell::new(0));
    let opts = ApplyOpts {
        touch: Some(vec![hop.clone()]),
        ..ApplyOpts::default()
    };
    let (report, _) = apply_with(
        &counted,
        &local,
        &Fakes,
        "owner",
        &games[300..301],
        Some(("pot", 2)),
        Some(TIP),
        None,
        &opts,
    )
    .await
    .unwrap();
    eprintln!(
        "e_a: one warm apply of one game of 600: record statements {}, record rows answered {}, rounds {}, misses {}, narrowed {}",
        counted.2.get(),
        counted.1.get(),
        report.rounds,
        report.misses,
        report.narrowed
    );
    assert!(report.narrowed, "the warm game is read narrowly");
    assert_eq!(
        report.misses, 0,
        "the warm mirror held every key the judgement named"
    );
    assert!(
        counted.2.get() < 4,
        "a warm apply reads under 4 record statements: {}",
        counted.2.get()
    );
    // The full apply (no txids) of the same game: one compound statement a round (it was 24 a round).
    let full = Counting(&record, Cell::new(0), Cell::new(0));
    let cold = mirror();
    let (r, _) = apply(
        &full,
        &cold,
        &Fakes,
        "owner",
        &games[300..301],
        Some(("pot", 2)),
        Some(TIP),
        None,
    )
    .await
    .unwrap();
    eprintln!(
        "e_a: the same game's cold full apply: record statements {}, rows {}, rounds {}",
        full.2.get(),
        full.1.get(),
        r.rounds
    );
    assert_eq!(full.2.get(), r.rounds, "one compound statement a round");
    // The narrowed judgement is the cold apply's, row for row.
    assert_eq!(
        stored_game(&local, "owner", &games[300]),
        stored_game(&cold, "owner", &games[300])
    );
    assert!(!stored_game(&local, "owner", &games[300]).is_empty());
}

/// (e_a) A cold game, a mark asking for the record (`age`), and a game past its refresh window read the whole closure.
#[tokio::test]
async fn d2_actor_e_a_b_cold_and_repair_marks_and_stale_games_read_the_whole_closure() {
    let (record, local, games) = e_warm_seat(32).await;
    let touch = ApplyOpts {
        touch: Some(vec![h("b8", 3)]),
        ..ApplyOpts::default()
    };
    let run = |g: usize| {
        let (record, local, games, touch) = (&record, &local, &games, &touch);
        async move {
            apply_with(
                record,
                local,
                &Fakes,
                "owner",
                &games[g..g + 1],
                None,
                Some(TIP),
                None,
                touch,
            )
            .await
            .unwrap()
            .0
        }
    };
    assert!(run(3).await.narrowed);
    // an age mark (the repair's): the record as it stands
    record
        .conn
        .execute(
            "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('owner',?1,'age','age',1)",
            [&games[4]],
        )
        .unwrap();
    assert!(
        !run(4).await.narrowed,
        "an age mark reads the whole closure"
    );
    // past the refresh window
    local.now.set(NOW_MS + FULL_REFRESH_EVERY_MS as u64 + 1);
    let stale = run(5).await;
    assert!(
        !stale.narrowed && stale.misses > 0,
        "a stale game reads the whole closure"
    );
    // and is warm again after it
    assert!(run(5).await.narrowed);
    // a game the mirror never read
    let fresh = mirror();
    let r = apply_with(
        &record,
        &fresh,
        &Fakes,
        "owner",
        &games[6..7],
        None,
        Some(TIP),
        None,
        &touch,
    )
    .await
    .unwrap()
    .0;
    assert!(!r.narrowed);
}

/// (e_a) Phase A's coverage: a narrowed apply records no coverage (the D1 judge acknowledges first, so a later
/// delivery is applied, never a dupe); Phases B and C (`narrow_covers`) record the marks' bound.
#[tokio::test]
async fn d2_actor_e_a_c_a_narrowed_apply_covers_only_where_no_judge_acks_first() {
    let (record, local, games) = e_warm_seat(32).await;
    let hop = e_write(&record, &games, 7);
    let bound = crate::owed_judge::marks_bound(&record, "owner", &games[7..8])
        .await
        .unwrap();
    for (covers, want) in [(false, false), (true, true)] {
        let opts = ApplyOpts {
            touch: Some(vec![hop.clone()]),
            narrow_covers: covers,
            ..ApplyOpts::default()
        };
        local
            .conn
            .execute("DELETE FROM applied WHERE kind='covered'", [])
            .unwrap();
        let r = apply_with(
            &record,
            &local,
            &Fakes,
            "owner",
            &games[7..8],
            None,
            Some(TIP),
            None,
            &opts,
        )
        .await
        .unwrap()
        .0;
        assert!(r.narrowed);
        assert_eq!(
            covered_at(&local, &games[7..8], bound, Some(TIP)).await,
            want,
            "narrow_covers {covers}"
        );
    }
}

/// (e_b) A BURST IS ONE JUDGE PASS (bsv-low #954 item 2): 64 events for one identity, each a write to its own game,
/// arriving while the turn is held, are applied by the object's next turn holder as one pass over the union of their
/// games: 2 passes for 64 events (the first caller's own, then the 63 that queued behind it), every event answered
/// exactly (its games, its first apply, its `(kind, seq)` recorded), the rows those of a full apply of every game.
/// On `e99472091` (no `Burst`) each event is its own apply: 64 passes. The workerd cell
/// `tools/lane-912/owed_burst_one_identity.mjs` holds the same burst under 1 s each on real workerd.
#[tokio::test]
async fn d2_actor_e_b_a_burst_of_64_events_for_one_identity_is_two_judge_passes() {
    let (record, local, games) = e_warm_seat(64).await;
    let mut hops = Vec::new();
    for k in 0..64 {
        hops.push(e_write(&record, &games, k));
    }
    let burst = Burst::default();
    let turn = futures_util::lock::Mutex::new(());
    let yielding = Yielding(&record);
    let calls = (0..64).map(|k| {
        let item = BurstItem {
            games: vec![games[k].clone()],
            kind_seq: Some(("pot".into(), 100 + k as i64)),
            tip: Some(TIP),
            bound: None,
            touch: Some(vec![hops[k].clone()]),
        };
        apply_coalesced(
            &burst, &turn, &yielding, &local, &Fakes, "owner", item, None, false, false,
        )
    });
    let answers = futures_util::future::join_all(calls).await;
    eprintln!("e_b: 64 events, {} judge passes", burst.passes.get());
    assert_eq!(burst.events.get(), 64);
    assert!(
        burst.passes.get() <= 3,
        "a burst is a few passes: {}",
        burst.passes.get()
    );
    for (k, a) in answers.into_iter().enumerate() {
        let (report, _) = a.unwrap();
        assert_eq!(report.applied_games, vec![games[k].clone()]);
        assert!(report.first_apply && !report.skipped);
        assert!(!first_apply_of(&local, Some(("pot", 100 + k as i64)), &games[k..k + 1]).await);
    }
    // the rows are a full apply's of every game
    let full = mirror();
    for chunk in games.chunks(GAMES_PER_APPLY) {
        apply(
            &record,
            &full,
            &Fakes,
            "owner",
            chunk,
            None,
            Some(TIP),
            None,
        )
        .await
        .unwrap();
    }
    assert_eq!(stored_rows_no_clock(&local), stored_rows_no_clock(&full));
}

/// The stored rows without the judgement's own clock (`judgedAtMs`).
fn stored_rows_no_clock(db: &SqliteSql) -> Vec<String> {
    let mut q = db.conn.prepare("SELECT json_array(outpoint,family,gameId,sats,opponentIdentity,atHeight,facts,reason,claimable,gateHeight,beltDueAtMs,probeTip) FROM owed_rows WHERE identity='owner' ORDER BY outpoint").unwrap();
    q.query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// (e_c) THE CONSUMER COALESCES PER IDENTITY (bsv-low #954 item 3): eight messages of one batch, each naming one game
/// of one identity, judged one by one are eight deliveries (eight actor applies, eight D1 judge commits); merged
/// ([`crate::owed_events::Plan::merged`], the batch's one pass) they are ONE delivery: one actor apply and one D1
/// judge pass, every mark acknowledged. And the consumer's batch is the renders' `max_batch_size` (pinned). New API;
/// on `e99472091` the consumer judges each message alone (`max_batch_size = 1`).
#[tokio::test]
async fn d2_actor_e_c_a_batch_of_n_events_for_one_identity_is_one_apply_and_one_judge_pass() {
    let w = WithActor {
        d1: d1_judge(&fold7_chained_seat(8)).await,
        actor: mirror(),
        d1_judges: true,
        event: Some(("pot", 9)),
        actor_reads: false,
        actor_fails: Cell::new(false),
    };
    let games = games_of(&w.d1, "owner");
    let events = |round: usize| -> Vec<overlay_discovery::owed_event::OwedEvent> {
        (0..8)
            .map(|k| {
                let key = format!("e-c-{round}-{k}");
                d_mark(&w, &games[k], &key);
                overlay_discovery::owed_event::OwedEvent {
                    v: 1,
                    seq: (round * 100 + k) as i64,
                    source: "pot".into(),
                    source_key: key,
                    facts: Value::Null,
                    at_ms: NOW_MS as i64,
                }
            })
            .collect()
    };
    // one by one
    E_CALLS.with(|c| c.set(0));
    E_COMMITS.with(|c| c.set(0));
    for ev in events(1) {
        let plan = crate::owed_events::plan(&w.d1, &ev).await.unwrap();
        crate::owed_events::judge_plan(&w, plan).await.unwrap();
    }
    let alone = (E_CALLS.with(Cell::get), E_COMMITS.with(Cell::get));
    // merged
    let mut plans = Vec::new();
    for ev in events(2) {
        plans.push(crate::owed_events::plan(&w.d1, &ev).await.unwrap());
    }
    E_CALLS.with(|c| c.set(0));
    E_COMMITS.with(|c| c.set(0));
    let routed = crate::owed_events::judge_plan(&w, crate::owed_events::Plan::merged(&plans))
        .await
        .unwrap();
    let merged = (E_CALLS.with(Cell::get), E_COMMITS.with(Cell::get));
    eprintln!("e_c: 8 messages for one identity: alone {alone:?} (actor applies, D1 commits), coalesced {merged:?}");
    assert_eq!(alone, (8, 8));
    assert_eq!(merged, (1, 1), "one apply and one judge pass");
    assert_eq!(routed.subjects, 8);
    assert_eq!(d_marks(&w), 0, "every mark acknowledged");
    // The batch the consumer takes is the renders' `max_batch_size`.
    let toml = include_str!("../../wrangler.toml");
    let sizes: Vec<&str> = toml
        .split("queues.consumers]]")
        .skip(1)
        .filter(|b| {
            matches!(
                b.lines().find(|l| l.starts_with("queue = ")),
                Some("queue = \"low-owed-events\"" | "queue = \"low-owed-events-beta\"")
            )
        })
        .map(|b| {
            b.lines()
                .find(|l| l.starts_with("max_batch_size"))
                .unwrap_or("")
        })
        .collect();
    assert_eq!(sizes.len(), 2, "{sizes:?}");
    for s in sizes {
        assert_eq!(
            s,
            format!(
                "max_batch_size = {}",
                crate::owed_events::CONSUMER_BATCH_MAX
            ),
            "the consumer's batch"
        );
    }
}

/// (e_d) A COLD SEAT'S RE-PUBLISHED EVENTS NEVER SAMPLE THE WARM BARS (bsv-low #954 item 4): the actor's answer
/// carries `warmAtBirth`, true only for an event written at or after its bootstrap's stamp; the bars' one rule
/// (`samples_visible`, read by the object and by the consumer's and the direct pass's notes) samples nothing for an
/// event born while the actor held: the sweep's re-publish of a held mark (written before the stamp, applied after
/// it) adds nothing to `rowsVisible` or the backstop's lag, an event written after it samples as before. RED on
/// `e99472091` (no `warmAtBirth`): the re-published event's 364 s lag sampled into the queue class.
#[tokio::test]
async fn d2_actor_e_d_a_cold_seats_republished_events_sample_nothing_warm() {
    let record = seeded(&fold7_chained_seat(20));
    let games = games_of(&record, "owner");
    let local = mirror();
    let turn = futures_util::lock::Mutex::new(());
    let held_at = NOW_MS;
    let ev = fold7_event("pot", 50, &games[0..1], held_at);
    assert!(matches!(
        apply_or_hold(&turn, &record, &local, &Fakes, "owner", &ev, None, false)
            .await
            .unwrap(),
        Applied::Held { .. }
    ));
    local.now.set(NOW_MS + 30_000);
    let (pages, _, drain) = fold7_bootstrap(&turn, &record, &local, "owner").await;
    assert!(pages >= 1 && !drain.applied.is_empty());
    let stamp: u64 = fold7_meta(&local, "bootstrapped").unwrap().parse().unwrap();
    assert!(stamp >= NOW_MS + 30_000);
    // the sweep's re-publish of the held mark, written while the actor held, applied now (its queue lag 364 s)
    let republished = fold7_event("pot", 51, &games[0..1], held_at + 1_000);
    let Applied::Done(report, _) = apply_or_hold(
        &turn,
        &record,
        &local,
        &Fakes,
        "owner",
        &republished,
        None,
        false,
    )
    .await
    .unwrap() else {
        panic!("a bootstrapped actor applies");
    };
    let mut answer = answer_of(&report, Vec::new());
    answer["atMs"] = json!(held_at + 1_000);
    answer["judgedAtMs"] = json!(held_at + 365_000);
    answer["warmAtBirth"] = json!(warm_at_birth(Some(held_at + 1_000), Some(stamp)));
    assert!(report.first_apply, "its first apply");
    assert!(
        !samples_visible(&answer),
        "a held-era event samples nothing warm"
    );
    let mut bars = Bars::default();
    note_answer_into(&mut bars, &answer);
    assert_eq!(bars.visible.samples.len(), 0, "rowsVisible untouched");
    // an event written after the stamp samples as before
    let warm = fold7_event("pot", 52, &games[1..2], stamp + 10);
    let Applied::Done(report, _) =
        apply_or_hold(&turn, &record, &local, &Fakes, "owner", &warm, None, false)
            .await
            .unwrap()
    else {
        panic!("applies");
    };
    let mut answer = answer_of(&report, Vec::new());
    answer["atMs"] = json!(stamp + 10);
    answer["judgedAtMs"] = json!(stamp + 200);
    answer["warmAtBirth"] = json!(warm_at_birth(Some(stamp + 10), Some(stamp)));
    assert!(samples_visible(&answer));
    note_answer_into(&mut bars, &answer);
    assert_eq!(bars.visible.samples.len(), 1);
    // an older build's answer (no field) samples as before; an actor with no stamp too
    assert!(warm_at_birth(Some(1), None) && warm_at_birth(None, Some(5)));
    // the object computes it from its stamp before sampling (source pin: wasm32 only)
    let object = include_str!("object.rs")
        .split_whitespace()
        .collect::<String>();
    assert!(object.contains(
        &"if let (true, true, Some(at)) = (report.first_apply, warm_at_birth, body.at_ms) {"
            .split_whitespace()
            .collect::<String>()
    ));
}

/// (e_e) A BOOTSTRAP PAGE AT THE APPLY'S COST (bsv-low #954 item 5): one page of 16 games of FOLD-7's chained seat
/// reads the record in one statement a closure round (it was 24 a round, about 4.8 s a page on beta's D1), the
/// page's games list beside it. Printed for the REPORT with the rows. On `e99472091` the same page reads 24
/// statements a round (`fold9_h`'s shape).
#[tokio::test]
async fn d2_actor_e_e_a_bootstrap_page_reads_under_4_statements() {
    let record = seeded(&fold7_chained_seat(600));
    let local = mirror();
    let turn = futures_util::lock::Mutex::new(());
    let counted = Counting(&record, Cell::new(0), Cell::new(0));
    let step = bootstrap_step(
        &turn,
        &counted,
        &local,
        &Fakes,
        "owner",
        Some(object::BOOT_CURSOR_START),
        Some(TIP),
        None,
        false,
    )
    .await
    .unwrap();
    assert!(matches!(step, BootStep::Page { .. }), "{step:?}");
    eprintln!(
        "e_e: one bootstrap page of 16 games: record statements {}, rows answered {}",
        counted.2.get(),
        counted.1.get()
    );
    assert!(
        counted.2.get() < 4,
        "a page reads under 4 record statements: {}",
        counted.2.get()
    );
}

/// A record port that answers D1's value bound (`SQLITE_TOOBIG`) for any statement aggregating `fault_on`'s rows.
struct TooBig<'a>(&'a SqliteSql, &'static str, Cell<usize>);
impl SqlPort for TooBig<'_> {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        if stmts.iter().any(|s| {
            s.sql.contains("json_group_array") && s.sql.contains(&format!("FROM {} t", self.1))
        }) {
            self.2.set(self.2.get() + 1);
            return Err("D1_ERROR: string or blob too big: SQLITE_TOOBIG".into());
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

/// (e_e) THE READ IS A STATEMENT D1 ACCEPTS (found by the burst cell's first workerd run: a 25-arm `UNION ALL` is
/// refused, "too many terms in compound SELECT", D1's bound is 5): the round's read is one SELECT of one scalar
/// subquery a family, no compound at its top, every inner compound at most 3 terms, under D1's 100 KB statement
/// bound. A family whose rows pass D1's 2 MB value bound falls back, the BEEF families row by row (one compound of 2
/// terms), then one statement a family, and the judgement is the same.
#[tokio::test]
async fn d2_actor_e_e_b_the_read_is_one_statement_d1_accepts_and_falls_back_past_a_value_bound() {
    let all: Vec<usize> = (0..FAMILIES.len()).collect();
    let sql = aggregate_select(true, &all);
    assert!(sql.len() < 100_000, "{} bytes", sql.len());
    assert!(!sql.contains("UNION ALL"));
    for f in FAMILIES {
        assert!(selection(f).matches(" UNION ").count() < 5, "{}", f.table);
    }
    let beefs: Vec<usize> = all.iter().copied().filter(|n| FAMILIES[*n].beef).collect();
    assert!(beefs.len() <= 5 && rows_select(&beefs).matches(" UNION ALL ").count() < 5);
    let record = seeded(&fold7_chained_seat(20));
    let games = games_of(&record, "owner");
    let want = mirror();
    apply(
        &record,
        &want,
        &Fakes,
        "owner",
        &games,
        None,
        Some(TIP),
        None,
    )
    .await
    .unwrap();
    for (table, falls) in [("pot_beefs", 1), ("pot_records", 2)] {
        let port = TooBig(&record, table, Cell::new(0));
        let got = mirror();
        apply(&port, &got, &Fakes, "owner", &games, None, Some(TIP), None)
            .await
            .unwrap();
        assert!(port.2.get() >= falls, "{table}: {} refusals", port.2.get());
        assert_eq!(
            stored_rows_no_clock(&got),
            stored_rows_no_clock(&want),
            "{table}"
        );
    }
}

// ---- D2-ACTOR-B-FOLD-11 (the sixth delta lens) ----

/// D1's judgement and the actor's of every game of `owner`, an ordinary event between: the D1 judge over the record
/// as it stands, the actor's narrowed apply told only `touch` (a txid no row of the fixture names).
async fn fold11_both_judge(d1: &SqliteSql, local: &SqliteSql, touch: &str) -> ApplyReport {
    fold11_both_judge_on(d1, d1, local, touch).await
}

/// [`fold11_both_judge`], the actor reading the record through `record` (a counting port).
async fn fold11_both_judge_on(
    d1: &SqliteSql,
    record: &impl SqlPort,
    local: &SqliteSql,
    touch: &str,
) -> ApplyReport {
    let games = games_of(d1, "owner");
    for chunk in games.chunks(16) {
        crate::owed_judge::judge_subjects(d1, "owner", chunk, "d1")
            .await
            .unwrap();
    }
    let opts = ApplyOpts {
        touch: Some(vec![touch.to_string()]),
        ..ApplyOpts::default()
    };
    let (report, _) = apply_with(
        record,
        local,
        &Fakes,
        "owner",
        &games,
        Some(("word", 77)),
        Some(TIP),
        None,
        &opts,
    )
    .await
    .unwrap();
    report
}

/// (fold11_a) NEW6-M1, THE LENS'S SCRATCH SHAPE: beta's #953 hop (index unspent, the evidence `<hop>:0` CONFIRMED),
/// both judges `unbound` and the game warm; then the overlay's reorg clear (the shipped statement) deletes the
/// confirmed evidence and marks nothing; then one ordinary event naming another txid. The warm actor's narrowed apply
/// stores the D1 judge's rows: `hop-stranded`, `index-unspent`, the press. RED on `26f7443c6`: the actor keeps
/// `unbound`, spent, no press (`narrowed true, misses 0`).
#[tokio::test]
async fn fold11_a_a_warm_apply_after_an_unmarked_reorg_clear_is_the_d1_judges() {
    let d1 = d1_judge(&fold9_fixture()).await;
    let (local, _) = actor_judge(&d1, "owner").await;
    let hop = format!("{FOLD9_HOP}:0");
    let before = fold9_row(&d1, &hop).unwrap();
    assert_eq!(before.0, "unbound", "{before:?}");
    assert_eq!(
        fold9_judgement(&fold9_row(&local, &hop).unwrap()),
        fold9_judgement(&before)
    );
    let marks = |db: &SqliteSql| -> i64 {
        db.conn
            .query_row("SELECT COUNT(*) FROM owed_dirty", [], |r| r.get(0))
            .unwrap()
    };
    let marked = marks(&d1);
    let cleared = d1
        .conn
        .execute(
            bsv_overlay_cloudflare::hop_probe_memos::HOP_PROBE_MEMO_REORG_CLEAR_SQL,
            [],
        )
        .unwrap();
    assert_eq!(cleared, 1, "the confirmed evidence");
    assert_eq!(marks(&d1), marked, "the reorg clear marks nothing");
    let counted = Counting(&d1, Cell::new(0), Cell::new(0));
    let report = fold11_both_judge_on(&d1, &counted, &local, &h("c1", 1)).await;
    assert!(report.narrowed, "the warm game read narrowly: {report:?}");
    // the cost after the fold: two record statements a warm apply (the marks, then every family in one statement),
    // the statement's text unchanged (only its binds carry the whole key set)
    let all: Vec<usize> = (0..FAMILIES.len()).collect();
    let bytes = aggregate_select(true, &all).len();
    eprintln!(
        "fold11_a: warm apply record statements {}, rows answered {}, the compound statement {bytes} bytes",
        counted.2.get(),
        counted.1.get()
    );
    assert!(counted.2.get() < 4 && bytes < 100_000);
    let d1_row = fold9_row(&d1, &hop).unwrap();
    assert_eq!(
        (d1_row.0.as_str(), d1_row.1, d1_row.2["chainProbe"].clone()),
        ("hop-stranded", Some(1), json!("index-unspent")),
        "{d1_row:?}"
    );
    let actor = fold9_row(&local, &hop).unwrap();
    assert_eq!(
        fold9_judgement(&actor),
        fold9_judgement(&d1_row),
        "the narrowed actor agrees with the D1 judge: {actor:?}"
    );
    assert_eq!(stored_rows_no_clock(&local), stored_rows_no_clock(&d1));
}

/// A filed sweep of the #953 hop paying the seat's home, its bytes in `transactions` with `has_proof` 1 (the
/// index's verified proof, `hydrate.rs`'s filed-sweep read).
fn fold11_sweep_fixture() -> Fixture {
    let made = (NOW_MS / 1000) as i64 - 4 * 3_600;
    let (spend, s) = fold9_spend(FOLD9_HOP);
    let raw = hex::encode(spend.to_binary());
    let mut beef = bsv_rs::transaction::Beef::new();
    beef.merge_raw_tx(spend.to_binary(), None);
    Fixture {
        name: "fold11 a filed sweep proven by the index",
        seed: vec![
            format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind) VALUES ('{FOLD9_HOP}',0,0,{made},'p2pkh')"),
            format!("INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('owner','other','{}',0,20190,'seat','00','00',2,'{FOLD9_HOP}',1,{made},1)", h("9f", 1)),
            format!("INSERT INTO hopsweep_records(identity,gameId,hopTxid,hopVout,sweepTxid,sweepRawHex,txid,outputIndex,createdAt) VALUES ('owner','{}','{FOLD9_HOP}',0,'{s}','{raw}','{}',0,{made})", h("9f", 1), h("5e", 1)),
            format!("INSERT INTO transactions(txid,beef,has_proof,proofHeight,created_at) VALUES ('{s}',x'{}',1,{},{made})", hex::encode(beef.to_binary()), TIP - 18),
        ],
        identities: vec!["owner".into()],
    }
}

/// (fold11_b) NEW6-M1, THE REVERSE DIRECTION: the filed sweep's payout rests on `transactions.has_proof`; the engine's
/// refutation (`TRANSACTION_UNPROVE_SQL`, the shipped statement, no trigger) withdraws it, then an ordinary event: the
/// warm actor withdraws what the D1 judge withdraws. RED on `26f7443c6`: the warm actor keeps the proven payout.
#[tokio::test]
async fn fold11_b_a_warm_apply_after_an_unmarked_unprove_is_the_d1_judges() {
    let d1 = d1_judge(&fold11_sweep_fixture()).await;
    let (local, _) = actor_judge(&d1, "owner").await;
    let before = stored_rows_no_clock(&d1);
    assert_eq!(stored_rows_no_clock(&local), before);
    let (_, s) = fold9_spend(FOLD9_HOP);
    let unproved = d1
        .conn
        .execute(
            bsv_overlay_cloudflare::d1_storage::TRANSACTION_UNPROVE_SQL,
            [&s],
        )
        .unwrap();
    assert_eq!(unproved, 1);
    let report = fold11_both_judge(&d1, &local, &h("c1", 2)).await;
    assert!(report.narrowed, "{report:?}");
    let after = stored_rows_no_clock(&d1);
    eprintln!("fold11_b: d1 before {before:?}\nfold11_b: d1 after  {after:?}");
    assert_ne!(after, before, "the withdrawn proof moves D1's judgement");
    assert_eq!(
        stored_rows_no_clock(&local),
        after,
        "the warm actor withdraws what D1 withdraws"
    );
    // and the latch back (`MARK_TX_PROVEN_AT_SQL`, no trigger either): both read it again
    d1.conn
        .execute(
            bsv_overlay_cloudflare::d1_storage::MARK_TX_PROVEN_AT_SQL,
            rusqlite::params![(TIP - 18) as f64, &s],
        )
        .unwrap();
    fold11_both_judge(&d1, &local, &h("c1", 3)).await;
    assert_eq!(stored_rows_no_clock(&d1), before);
    assert_eq!(stored_rows_no_clock(&local), before);
}

/// (fold11_e) NEW6-L1: A COALESCED DELIVERY SAMPLES PER EVENT. A batch of two events for one identity: the oldest
/// (`pot` 10) was applied by its direct call, the second's (`pot` 11) direct call faulted. The consumer's delivery
/// carries the oldest under its key and the second in `also` with its write time ([`crate::owed_events::coalesced_event`],
/// [`apply_body`]); the object reads each one's first apply before it records them ([`first_apply_of`]) and samples
/// each first apply born warm ([`event_visibility`]): the second's lag goes into the backstop figure. On `26f7443c6`
/// the delivery carried the oldest alone (`firstApply: false`) and sampled nothing (the old answer, no `visibleMs`,
/// noted below: zero samples).
#[tokio::test]
async fn fold11_e_a_coalesced_delivery_samples_its_faulted_tail() {
    let (record, local, games) = e_warm_seat(4).await;
    let ev = |seq: i64, at: i64| overlay_discovery::owed_event::OwedEvent {
        v: 1,
        seq,
        source: "pot".into(),
        source_key: format!("{}:0", h("b8", seq as usize)),
        facts: Value::Null,
        at_ms: at,
    };
    let (a, b) = (ev(10, 1_000), ev(11, 2_000));
    let ctx = crate::owed_events::coalesced_event(&[&b, &a]).unwrap();
    assert_eq!(
        (ctx.kind.as_str(), ctx.seq, ctx.at_ms),
        ("pot", 10, 1_000),
        "the oldest's key"
    );
    assert_eq!(ctx.also, vec![("pot".to_string(), 11, 2_000)]);
    let body = apply_body("owner", &games[..1], &ctx, Some(TIP));
    assert_eq!(body["also"], json!([["pot", 11]]));
    assert_eq!(body["alsoAtMs"], json!([2_000]));
    // the oldest applied by its direct call; the second's call faulted (no `applied` row)
    apply(
        &record,
        &local,
        &Fakes,
        "owner",
        &games[..1],
        Some(("pot", 10)),
        Some(TIP),
        None,
    )
    .await
    .unwrap();
    let first_a = first_apply_of(&local, Some(("pot", 10)), &games[..1]).await;
    let first_b = first_apply_of(&local, Some(("pot", 11)), &games[..1]).await;
    assert_eq!((first_a, first_b), (false, true));
    let now = 5_000u64;
    let visible = event_visibility(
        now,
        &[(Some(1_000), first_a, true), (Some(2_000), first_b, true)],
    );
    assert_eq!(
        visible,
        vec![3_000],
        "the faulted tail's first apply is sampled"
    );
    // an event born before the bootstrap stamp, or with no write time, samples nothing
    assert!(event_visibility(now, &[(Some(2_000), true, false), (None, true, true)]).is_empty());
    // the bars read the answer's samples
    let old = json!({"applied": true, "firstApply": false, "atMs": 1_000, "judgedAtMs": now, "applyMs": 5});
    let mut bars = Bars::default();
    note_answer_into(&mut bars, &old);
    assert_eq!(
        bars.visible.samples.len(),
        0,
        "the base's answer: nothing sampled"
    );
    let mut new = old.clone();
    new["visibleMs"] = json!(visible);
    note_answer_into(&mut bars, &new);
    assert_eq!(bars.visible.samples.len(), 1);
    let mut pending = PendingBars::default();
    pending.note_answer("queue", &new);
    assert_eq!(pending.visible.get("queue").map(Vec::len), Some(1));
    // the object reads each `also` event's first apply and samples per event (source pin: wasm32 only)
    let object = include_str!("object.rs")
        .split_whitespace()
        .collect::<String>();
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    assert!(object.contains(&squash(
        "let first = first_apply_of(&local, Some((kind.as_str(), *seq)), &body.games).await;"
    )));
    assert!(object.contains(&squash("let visible = event_visibility(now, &events);")));
    assert!(object.contains(&squash("answer[\"visibleMs\"] = json!(visible);")));
    let events = include_str!("../owed_events.rs")
        .split_whitespace()
        .collect::<String>();
    assert!(events.contains(&squash("ctx.also = also;")));
}

/// (fold11_f) NEW6-L2: THE JUDGE'S VERSION, AND COLD PAGES FIRST. A bootstrapped actor stamped with this
/// [`JUDGE_VERSION`] is re-armed by no build (whatever its git HEAD, [`lib_rev`]); one stamped by a moved version
/// (or an older build's git sha) is re-judged. Under the fleet's budget a re-judge takes only the minute's first
/// [`BOOTSTRAP_REJUDGE_PAGES_PER_MINUTE`] tokens, so with the re-judges at their share a cold page still takes the
/// next; and a waiting cold page's slot comes before a waiting re-judge's. The shipped statements under real SQLite.
/// On `26f7443c6` the stamp was `lib_rev()` (every deploy re-judged the fleet) and one take served both (no pin
/// compiles: `JUDGE_VERSION`, `BOOT_BUDGET_REJUDGE_TAKE_SQL` and `boot_pace_of` are new).
#[test]
fn fold11_f_the_judge_version_and_cold_pages_first() {
    // the version, not the build
    assert!(!rejudge_due(true, Some(JUDGE_VERSION), JUDGE_VERSION));
    assert!(
        rejudge_due(true, Some("fold10-older"), JUDGE_VERSION),
        "a moved version re-judges"
    );
    assert!(
        rejudge_due(true, Some("663a31d09"), JUDGE_VERSION),
        "an old build's sha re-judges once"
    );
    assert!(
        !rejudge_due(false, Some("x"), JUDGE_VERSION),
        "a cold actor is bootstrapped, not re-judged"
    );
    assert_ne!(JUDGE_VERSION, lib_rev());
    let object = include_str!("object.rs")
        .split_whitespace()
        .collect::<String>();
    assert!(!object.contains("rejudge_due(true,judged.as_deref(),lib_rev())"));
    assert!(object.contains("rejudge_due(true,judged.as_deref(),JUDGE_VERSION)"));
    assert!(object.contains("self.put_meta(local,META_JUDGED_REV,JUDGE_VERSION);"));
    assert_eq!(
        object.matches("lib_rev()").count(),
        0,
        "the actor stamps and compares the judge's version only"
    );
    // the budget: re-judges to their share, then a cold page still runs
    assert!(
        BOOT_BUDGET_REJUDGE_TAKE_SQL.contains(&format!("<{BOOTSTRAP_REJUDGE_PAGES_PER_MINUTE} "))
    );
    let db = fold8_empty();
    let take = |sql: &str, now: u64| -> i64 {
        let mut q = db.conn.prepare(sql).unwrap();
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
    let (cold_id, rejudge_id) = (
        format!("02{}", "ab".repeat(32)),
        format!("02{}", "ab".repeat(32)),
    );
    for n in 1..=BOOTSTRAP_REJUDGE_PAGES_PER_MINUTE {
        let t = take(BOOT_BUDGET_REJUDGE_TAKE_SQL, minute + n as u64);
        assert_eq!(
            boot_pace_of(t, minute, &rejudge_id, true),
            None,
            "re-judge page {n} runs"
        );
    }
    let refused = take(BOOT_BUDGET_REJUDGE_TAKE_SQL, minute + 100);
    let rejudge_wait = boot_pace_of(refused, minute + 100, &rejudge_id, true)
        .expect("past its share the re-judge waits");
    for n in BOOTSTRAP_REJUDGE_PAGES_PER_MINUTE + 1..=BOOTSTRAP_PAGES_PER_MINUTE {
        let t = take(BOOT_BUDGET_TAKE_SQL, minute + 200 + n as u64);
        assert_eq!(t, n);
        assert_eq!(
            boot_pace_of(t, minute, &cold_id, false),
            None,
            "cold page {n} runs beside the re-judges"
        );
    }
    let cold_wait = boot_pace_of(
        take(BOOT_BUDGET_TAKE_SQL, minute + 500),
        minute + 500,
        &cold_id,
        false,
    )
    .expect("past the minute the cold page waits");
    let next = minute + 60_000;
    assert!(
        (next..next + 30_000).contains(&cold_wait),
        "a cold wait in the next minute's first half"
    );
    assert!(
        (next + 30_000..next + 60_000).contains(&rejudge_wait),
        "a re-judge's in its second half"
    );
    assert!(
        cold_wait < rejudge_wait,
        "cold pages first in the slot order"
    );
    let page = &object[object.find("asyncfnbootstrap_page(").unwrap()..];
    assert!(page.contains("letrejudge=self.meta(META_BOOTSTRAPPED).is_some();"));
    assert!(page.contains("boot_pace_of(taken,now,&me,rejudge)"));
}

/// The record's own port under the render's era cutoff (`WRITTEN_OFF_BEFORE_MS`), the D1 judge's as `D1Judge` sets it.
struct Era<'a>(&'a SqliteSql, i64);
impl SqlPort for Era<'_> {
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
impl crate::owed_judge::JudgeDb for Era<'_> {
    fn tip(&self) -> Option<u64> {
        crate::owed_judge::JudgeDb::tip(self.0)
    }
    fn era_ms(&self) -> Option<i64> {
        Some(self.1)
    }
    async fn word_views(
        &self,
        rows: &BTreeMap<String, crate::tracker_host::stored::Row>,
    ) -> Result<BTreeMap<String, Value>, String> {
        crate::owed_judge::JudgeDb::word_views(self.0, rows).await
    }
    async fn canonical_root(&self, h: u64) -> Option<String> {
        crate::owed_judge::JudgeDb::canonical_root(self.0, h).await
    }
    async fn push(&self, identity: &str, body: Value) {
        crate::owed_judge::JudgeDb::push(self.0, identity, body).await;
    }
}

const FOLD11_G_HOP: &str = "8d00000000000000000000000000000000000000000000000000000000000001";
const FOLD11_G_JOIN: &str = "8d00000000000000000000000000000000000000000000000000000000000002";

/// bsv-low #957 item 1, beta's live shape: a hop-only game (no party row), its verified marker and its hop's index
/// row (unspent) made five days before the era's cutoff, the JOIN that spent it EVICTED (the ledger open) and the
/// eviction's released-hop evidence for the seat (`evictedAt` 12 s after the marker, beta's `markerCreatedAtMs`).
fn fold11_g_fixture(made_s: i64) -> Fixture {
    let game = h("ad", 1);
    let evicted = made_s * 1000 + 12_383;
    Fixture {
        name: "fold11 #957 an old-era hop-only game, its JOIN evicted",
        seed: vec![
            format!("INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind) VALUES ('{FOLD11_G_HOP}',0,0,{made_s},'p2pkh')"),
            format!("INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('owner','other','{game}',0,20190,'seat','00','00',2,'{FOLD11_G_HOP}',1,{made_s},1)"),
            format!("INSERT INTO pot_evictions(txid,evictedAt,reason,releasedSpends) VALUES ('{FOLD11_G_JOIN}',{evicted},'refused','[{{\"txid\":\"{FOLD11_G_HOP}\",\"vout\":0,\"table\":\"pot_records\"}}]')"),
            format!("INSERT INTO released_hop_evidence(hopTxid,hopVout,identity,gameId,potTxid,seatSettlePubkey,hopSats,evictedAt) VALUES ('{FOLD11_G_HOP}',0,'owner','{game}','{FOLD11_G_JOIN}','seat',20190,{evicted})"),
        ],
        identities: vec!["owner".into()],
    }
}

/// (fold11_g) bsv-low #957 item 1: THE RELEASED-HOP EVIDENCE TAKES THE ERA'S RULE. Beta's live disagreement: the
/// actor held `hop-stranded`, claimable, 20,190 sats, `joinRefusedBy: eviction`, `markerCreatedAtMs` the eviction's
/// time (the evidence road's fingerprint: `hydrate.rs` stamps the rebuilt hop with `evictedAt`), for an Oct 21 hop
/// D1 held no row for. The marker road writes an old-era hop off (`rank::select_hops`); the evidence road did not, so
/// an old-era Collect press came back through the eviction (the owner's rule, #375: no old-era Collect anywhere).
/// Under the era both judges now write it off, the same rows; with no era both serve the press (the shape reaches
/// it). RED on `26f7443c6`: under the era both judges store the `hop-stranded` claimable row.
#[tokio::test]
async fn fold11_g_an_old_era_released_hop_is_written_off_by_both_judges() {
    let made = (NOW_MS / 1000) as i64 - 8 * 86_400;
    let era = (made + 5 * 86_400) * 1000;
    let hop = format!("{FOLD11_G_HOP}:0");
    let judge_both = |era: Option<i64>| async move {
        let d1 = seeded(&fold11_g_fixture(made));
        let games = games_of(&d1, "owner");
        assert_eq!(games.len(), 1, "a hop-only game");
        match era {
            Some(e) => crate::owed_judge::judge_subjects(&Era(&d1, e), "owner", &games, "d1")
                .await
                .unwrap(),
            None => crate::owed_judge::judge_subjects(&d1, "owner", &games, "d1")
                .await
                .unwrap(),
        };
        let local = mirror();
        apply(
            &d1,
            &local,
            &Fakes,
            "owner",
            &games,
            Some(("bootstrap", 1)),
            Some(TIP),
            era,
        )
        .await
        .unwrap();
        (d1, local)
    };
    // no era: the shape is the live row (the press)
    let (d1, local) = judge_both(None).await;
    let row = fold9_row(&d1, &hop).expect("the press");
    assert_eq!(
        (row.0.as_str(), row.1),
        ("hop-stranded", Some(1)),
        "{row:?}"
    );
    assert_eq!(row.2["joinRefusedBy"], json!("eviction"));
    assert_eq!(stored_rows_no_clock(&local), stored_rows_no_clock(&d1));
    // the era: written off by both
    let (d1, local) = judge_both(Some(era)).await;
    assert_eq!(fold9_row(&d1, &hop), None, "D1 writes the old-era hop off");
    assert_eq!(fold9_row(&local, &hop), None, "the actor writes it off too");
    assert_eq!(stored_rows_no_clock(&local), stored_rows_no_clock(&d1));
}

/// (fold11_h) bsv-low #957 item 2: TWO JUDGES OF ONE RECORD AT TWO TIPS COMPARE EQUAL WITH THE SAME VERDICT. Beta's
/// `329dba34…:0` differed only in `facts.tip` (D1 973080, the actor its bootstrap's tip), both `unbound`. The compare
/// reads the judgement without its clock ([`CLOCK_FACTS`] now holds `tip`); a verdict change still differs
/// (`claimable` and the family are compared apart). RED on `26f7443c6`: the same verdict at two tips is one diff.
#[test]
fn fold11_h_two_tips_with_one_verdict_compare_equal() {
    let row = |tip: u64, family: &str, claimable: Option<i64>| {
        json!({"outpoint": "329d:0", "family": family, "gameId": "g", "sats": null, "opponentIdentity": "o",
            "atHeight": null, "reason": "the index holds no spend word for this pot", "claimable": claimable,
            "facts": json!({"claim": null, "tip": tip, "ageMs": tip}).to_string()})
    };
    let (d1, actor) = (row(973_080, "unbound", None), row(973_131, "unbound", None));
    assert!(
        diff_rows(std::slice::from_ref(&d1), std::slice::from_ref(&actor)).is_empty(),
        "one verdict at two tips"
    );
    assert_eq!(
        rows_digest(std::slice::from_ref(&d1)),
        rows_digest(&[actor])
    );
    assert_eq!(
        diff_rows(
            std::slice::from_ref(&d1),
            &[row(973_131, "hop-stranded", Some(1))]
        ),
        vec!["329d:0".to_string()]
    );
    assert_eq!(
        diff_rows(&[d1], &[row(973_131, "unbound", Some(1))]),
        vec!["329d:0".to_string()]
    );
    assert!(CLOCK_FACTS.contains(&"tip"));
}

/// (fold12_d) D2-ACTOR-B-FOLD-12, the seventh delta lens's NEW7-L2: A TIP-ONLY DIFFERENCE IS COUNTED APART. Two judges
/// of one record at two tips with one verdict (`facts.tip` and the `probeTip` column apart): zero diffs (FOLD-11's
/// clock fact stands) and ONE tip-lag diff (`owed_actor_compare_tip_lag_total`, its `TIP LAG` line, never the STOP);
/// a verdict change is a diff as before and no tip lag. Through the shipped `compare` over the D1 row in `owed_rows`.
/// RED against `tip_lag_rows` answering nothing (FOLD-11's drop): `left: [] right: ["329d:0"]`.
#[tokio::test]
async fn fold12_d_a_tip_only_difference_is_counted_apart() {
    let row = |tip: u64, family: &str, claimable: Option<i64>| {
        json!({"outpoint": "329d:0", "family": family, "gameId": "g", "sats": null, "opponentIdentity": "o",
            "atHeight": null, "reason": "r", "claimable": claimable, "probeTip": tip,
            "facts": json!({"claim": null, "tip": tip}).to_string()})
    };
    let (d1, lag, verdict) = (
        row(973_080, "unbound", None),
        row(973_081, "unbound", None),
        row(973_081, "hop-stranded", Some(1)),
    );
    assert!(diff_rows(std::slice::from_ref(&d1), std::slice::from_ref(&lag)).is_empty());
    assert_eq!(
        tip_lag_rows(std::slice::from_ref(&d1), std::slice::from_ref(&lag)),
        vec!["329d:0".to_string()]
    );
    assert!(tip_lag_rows(std::slice::from_ref(&d1), std::slice::from_ref(&verdict)).is_empty());
    assert!(tip_lag_rows(std::slice::from_ref(&d1), std::slice::from_ref(&d1)).is_empty());
    // the probeTip column alone is a lag too
    let mut col = d1.clone();
    col["probeTip"] = json!(973_081);
    assert_eq!(tip_lag_rows(std::slice::from_ref(&d1), &[col]).len(), 1);

    // through the shipped compare: D1's row stored, the actor's answer at the next tip
    let db = SqliteSql::migrated();
    let id = "02".to_string() + &"c".repeat(64);
    db.conn
        .execute(
            "INSERT INTO owed_rows(identity,outpoint,family,gameId,opponentIdentity,facts,updatedAtMs,reason,claimable,probeTip) VALUES (?1,'329d:0','unbound','g','o',?2,1,'r',NULL,973080)",
            rusqlite::params![&id, json!({"claim": null, "tip": 973_080}).to_string()],
        )
        .unwrap();
    let games = vec!["g".to_string()];
    compare(&db, &id, &games, &json!({"rows": [lag]}), None).await;
    assert_eq!(
        (db.counter(COUNTERS[1]), db.counter(COUNTERS[28])),
        (0, 1),
        "one verdict at two tips: a lag, never a STOP: {:?}",
        db.lines.borrow()
    );
    compare(&db, &id, &games, &json!({"rows": [verdict]}), None).await;
    assert_eq!(
        (db.counter(COUNTERS[1]), db.counter(COUNTERS[28])),
        (1, 1),
        "a verdict change is a diff"
    );
    assert_eq!(COUNTERS[28], "owed_actor_compare_tip_lag_total");
}

/// D2-ACTOR-B-FOLD-13's port: the record under the render's era, the actor's stored rows read without an apply, and
/// every re-apply counted (the actor's closure under the same era).
struct Fold13<'a> {
    d1: &'a SqliteSql,
    actor: &'a SqliteSql,
    era: Option<i64>,
    applies: Cell<usize>,
    /// The D1 judge's commits fault while this is above 0 (each fault takes one).
    commit_faults: Cell<u32>,
    /// D2-ACTOR-B-FOLD-15: the port's tip (both judges', the reconcile's served tip), and the era the ACTOR's
    /// closure judges under (`None`: the port's own; a different one plants a genuine judge disagreement).
    tip: Cell<u64>,
    actor_era: Option<Option<i64>>,
    /// FOLD-15: the record answers this many more calls, then every call faults (a tick the platform ends).
    die_after: Cell<u32>,
    /// FOLD-15: the D1 judge's commits (batches holding `GAME_DELETE_SQL`): one per judge call.
    commits: Cell<u32>,
    /// FOLD-16: a game whose mark the actor's apply writes to the record WHILE the two judge (taken once).
    mark_during: RefCell<Vec<String>>,
    /// FIX-960: a game whose mark lands AFTER the actor's closure read the record and before the reconcile's held
    /// read (taken once): neither judgement's input.
    mark_after: RefCell<Vec<String>>,
}
impl SqlPort for Fold13<'_> {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        if self.die_after.get() == 0 {
            return Err("the invocation ended (pin)".into());
        }
        self.die_after.set(self.die_after.get() - 1);
        if stmts
            .iter()
            .any(|s| s.sql == crate::owed_judge::GAME_DELETE_SQL)
        {
            self.commits.set(self.commits.get() + 1);
        }
        if self.commit_faults.get() > 0
            && stmts
                .iter()
                .any(|s| s.sql == crate::owed_judge::GAME_DELETE_SQL)
        {
            self.commit_faults.set(self.commit_faults.get() - 1);
            return Err("D1 overloaded (pin)".into());
        }
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
impl JudgeDb for Fold13<'_> {
    fn tip(&self) -> Option<u64> {
        Some(self.tip.get())
    }
    fn era_ms(&self) -> Option<i64> {
        self.era
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
    async fn actor_stored(&self, identity: &str) -> Result<Option<Value>, String> {
        let rows = self
            .actor
            .run_batch(vec![Stmt::new(ROWS_SQL, vec![Bind::Text(identity.into())])])
            .await?;
        Ok(Some(
            json!({"rows": rows[0].rows.clone(), "bootstrapped": true}),
        ))
    }
    async fn actor_apply(
        &self,
        identity: &str,
        games: &[String],
        _kind: &str,
    ) -> Result<Option<Value>, String> {
        self.applies.set(self.applies.get() + 1);
        for g in self.mark_during.borrow_mut().drain(..) {
            self.d1
                .conn
                .execute(
                    "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES (?1,?2,'word','during',1)",
                    rusqlite::params![identity, g],
                )
                .unwrap();
        }
        let (report, _) = apply(
            self.d1,
            self.actor,
            &Fakes,
            identity,
            games,
            None,
            Some(self.tip.get()),
            self.actor_era.unwrap_or(self.era),
        )
        .await?;
        for g in self.mark_after.borrow_mut().drain(..) {
            self.d1
                .conn
                .execute(
                    "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES (?1,?2,'word','after',1)",
                    rusqlite::params![identity, g],
                )
                .unwrap();
        }
        // The object's answer: its rows and the generation its closure read (`answer_of`).
        let mut answer = self.actor_stored(identity).await?.unwrap_or_default();
        answer["readGeneration"] = answer_of(&report, Vec::new())["readGeneration"].clone();
        Ok(Some(answer))
    }
}

/// (fold13_a) bsv-low #958: THE RECONCILE JUDGES D1 FRESH BEFORE IT COMPARES. Beta's shape: D1's stored rows are the
/// old build's judgement (an old-era game's press, judged with no era rule on that road), the fresh actor writes it
/// off under FOLD-11's era rule. The reconcile judges the identity's differing games in D1 first, compares the FRESH
/// rows (equal), counts a stale D1 (`owed_actor_reconcile_stale_total`, its line `stale-d1`), counts no diff and
/// re-applies nothing; D1's stale row is gone. RED on `c08be8b51` (the stored row compared): a diff counted and the
/// game re-applied, `left: (true, 1) right: (false, 0)`.
#[tokio::test]
async fn fold13_a_the_reconcile_judges_d1_fresh_and_never_counts_a_stale_row() {
    let made = (NOW_MS / 1000) as i64 - 8 * 86_400;
    let era = (made + 5 * 86_400) * 1000;
    let hop = format!("{FOLD11_G_HOP}:0");
    let d1 = seeded(&fold11_g_fixture(made));
    let games = games_of(&d1, "owner");
    // The old build's judgement in D1 (no era on the released-hop road): the press stands.
    crate::owed_judge::judge_subjects(&d1, "owner", &games, "old-build")
        .await
        .unwrap();
    assert_eq!(
        fold9_row(&d1, &hop).map(|r| (r.0, r.1)),
        Some(("hop-stranded".to_string(), Some(1))),
        "D1 holds the old judgement's press"
    );
    // The fresh actor under the era: written off.
    let local = mirror();
    apply(
        &d1,
        &local,
        &Fakes,
        "owner",
        &games,
        Some(("bootstrap", 1)),
        Some(TIP),
        Some(era),
    )
    .await
    .unwrap();
    assert_eq!(fold9_row(&local, &hop), None);
    let p = Fold13 {
        d1: &d1,
        actor: &local,
        era: Some(era),
        applies: Cell::new(0),
        commit_faults: Cell::new(0),
        tip: Cell::new(TIP),
        actor_era: None,
        die_after: Cell::new(u32::MAX),
        commits: Cell::new(0),
        mark_during: RefCell::new(Vec::new()),
        mark_after: RefCell::new(Vec::new()),
    };
    let r = reconcile_step(&p, NOW_MS as i64).await.unwrap().unwrap();
    assert_eq!(
        (r.diff, r.reapplied),
        (false, 0),
        "a stale D1 row is never a diff: {r:?}"
    );
    // FOLD-15 (NEW8-M1): the actor is re-applied ONCE over the picked game, its fresh side of the compare (its
    // closure re-reads the record); nothing is healed into it (`reapplied` 0).
    assert_eq!(p.applies.get(), 1, "the actor's fresh judgement, one apply");
    assert_eq!(
        (r.judged, r.unjudged, r.stale_outpoints),
        (1, 0, 1),
        "{r:?}"
    );
    assert_eq!(
        fold9_row(&d1, &hop),
        None,
        "D1 re-judged fresh: written off"
    );
    assert_eq!(d1.counter(COUNTERS[7]), 0, "no reconcile diff");
    assert_eq!(d1.counter(COUNTERS[29]), 1, "one stale D1");
    {
        let lines = d1.lines.borrow();
        assert!(lines
            .iter()
            .any(|l| l.starts_with("[owed-actor] RECONCILE stale-d1 owner: 1 outpoints")));
        assert!(!lines.iter().any(|l| l.contains("RECONCILE DIFF")));
    }
    // A genuine disagreement (the actor planted) still counts a diff and re-applies, the fresh D1 the side.
    local
        .conn
        .execute_batch("INSERT INTO owed_rows(identity,outpoint,family,gameId,facts,reason,updatedAtMs) VALUES ('owner','planted:0','unbound',(SELECT gameId FROM released_hop_evidence LIMIT 1),'{}','actor-planted',1)")
        .unwrap();
    let r = reconcile_step(&p, NOW_MS as i64 + RECONCILE_INTERVAL_MS)
        .await
        .unwrap()
        .unwrap();
    assert!(r.diff && r.reapplied > 0 && r.stale_outpoints == 0, "{r:?}");
    assert_eq!(d1.counter(COUNTERS[7]), 1);
    // FOLD-15: the planted row is the ACTOR's miss (its stored rows against its own fresh re-apply), healed.
    assert!(d1
        .lines
        .borrow()
        .iter()
        .any(|l| l.contains("RECONCILE DIFF owner") && l.contains("the actor missed the record")));
    // A genuine judge disagreement (FOLD-15: both sides judged fresh at one tip): the actor's closure under no era
    // keeps the press D1 writes off.
    let p2 = Fold13 {
        d1: &d1,
        actor: &local,
        era: Some(era),
        applies: Cell::new(0),
        commit_faults: Cell::new(0),
        tip: Cell::new(TIP),
        actor_era: Some(None),
        die_after: Cell::new(u32::MAX),
        commits: Cell::new(0),
        mark_during: RefCell::new(Vec::new()),
        mark_after: RefCell::new(Vec::new()),
    };
    apply(
        &d1,
        &local,
        &Fakes,
        "owner",
        &games,
        Some(("bootstrap", 2)),
        Some(TIP),
        None,
    )
    .await
    .unwrap();
    let r = reconcile_step(&p2, NOW_MS as i64 + 2 * RECONCILE_INTERVAL_MS)
        .await
        .unwrap()
        .unwrap();
    assert!(r.diff, "{r:?}");
    assert_eq!(d1.counter(COUNTERS[7]), 2);
    assert!(d1
        .lines
        .borrow()
        .iter()
        .any(|l| l.contains("RECONCILE DIFF owner") && l.contains("a judge disagreement")));
}

/// The identities the re-judge walks (the reconcile's union).
fn fold13_identities(d1: &SqliteSql) -> Vec<String> {
    let mut q = d1.conn.prepare("SELECT identity FROM owed_state UNION SELECT identity FROM potparty_records UNION SELECT identity FROM hopparty_records UNION SELECT identity FROM released_hop_evidence ORDER BY 1").unwrap();
    q.query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn fold13_stamp(d1: &SqliteSql) -> Value {
    d1.conn
        .query_row(D1_REJUDGE_STATE_SQL, [], |r| r.get::<_, String>(0))
        .map(|c| serde_json::from_str(&c).unwrap())
        .unwrap_or(Value::Null)
}

/// Mark every stored D1 row as an old build's (its reason), and count those left.
fn fold13_plant_old(d1: &SqliteSql) -> usize {
    d1.conn
        .execute("UPDATE owed_rows SET reason='old-build'", [])
        .unwrap()
}
fn fold13_old_left(d1: &SqliteSql) -> i64 {
    d1.conn
        .query_row(
            "SELECT COUNT(*) FROM owed_rows WHERE reason='old-build'",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

/// Run the re-judge to its end; the ticks it took.
async fn fold13_run(p: &Fold13<'_>, mut now: i64) -> (usize, Vec<RejudgeReport>) {
    let mut reports = Vec::new();
    for tick in 1..=200 {
        now += 120_000;
        match d1_rejudge_step(p, now).await.unwrap() {
            Some(r) => {
                let done = r.done;
                reports.push(r);
                if done {
                    return (tick, reports);
                }
            }
            None => return (tick, reports),
        }
    }
    panic!("the re-judge did not end in 200 ticks");
}

/// (fold13_b) bsv-low #958: D1'S ROWS RE-JUDGED FLEET-WIDE AFTER A JUDGE_VERSION MOVE, PACED. With no stamp (the first
/// build carrying it) or another version's, every identity of the union is re-judged once, 32 games a tick at most,
/// and the stamp moves to the running version (`done`, served on `/health.owedActor.d1Rejudge`); the same version
/// re-judges none (one read, no write); a faulted identity is retried the next tick and counted, and skipped at its
/// third consecutive fault. RED on `c08be8b51`: nothing re-judges D1 (the old rows stand; with the step made inert,
/// `left: 6 right: 0` old rows left).
#[tokio::test]
async fn fold13_b_a_version_move_rejudges_every_identity_once_and_moves_the_stamp() {
    // A fleet of two: the stranded hops' owner and an old-era hop-only seat under a second identity.
    let d1 = d1_judge(&stranded()).await;
    let made = (NOW_MS / 1000) as i64 - 8 * 86_400;
    for st in fold11_g_fixture(made).seed {
        d1.conn
            .execute_batch(&st.replace("'owner'", "'owner2'"))
            .unwrap();
    }
    crate::owed_judge::judge_subjects(&d1, "owner2", &games_of(&d1, "owner2"), "d1")
        .await
        .unwrap();
    let ids = fold13_identities(&d1);
    assert!(ids.len() >= 2, "a fleet: {ids:?}");
    let old = fold13_plant_old(&d1);
    assert!(old > 0);
    let p = Fold13 {
        d1: &d1,
        actor: &d1,
        era: None,
        applies: Cell::new(0),
        commit_faults: Cell::new(0),
        tip: Cell::new(TIP),
        actor_era: None,
        die_after: Cell::new(u32::MAX),
        commits: Cell::new(0),
        mark_during: RefCell::new(Vec::new()),
        mark_after: RefCell::new(Vec::new()),
    };
    let t0 = NOW_MS as i64;
    let (_, reports) = fold13_run(&p, t0).await;
    assert!(reports[0].started, "no stamp: a run starts");
    assert!(
        reports.iter().all(|r| r.games <= D1_REJUDGE_GAMES_PER_TICK),
        "paced: {reports:?}"
    );
    assert_eq!(fold13_old_left(&d1), 0, "every identity's rows re-judged");
    let stamp = fold13_stamp(&d1);
    assert_eq!(stamp["version"], json!(JUDGE_VERSION));
    assert_eq!(stamp["done"], json!(true));
    assert_eq!(stamp["identities"], json!(ids.len()), "each identity once");
    assert_eq!(d1.counter(COUNTERS[30]), ids.len() as u64);
    assert!(stamp["endedAtMs"].as_i64() >= stamp["startedAtMs"].as_i64());
    // Served on /health.owedActor.d1Rejudge (the snapshot the done tick forced).
    let snap: String = d1
        .conn
        .query_row(
            "SELECT cursor FROM owed_belt_state WHERE name='owed-actor:health'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let body = health_body(
        health_snapshot_of(Some(&snap)).as_ref(),
        NOW_MS,
        &Value::Null,
    );
    assert_eq!(
        (
            &body["d1Rejudge"]["version"],
            &body["d1Rejudge"]["done"],
            &body["d1Rejudge"]["identities"]
        ),
        (&json!(JUDGE_VERSION), &json!(true), &json!(ids.len())),
        "{}",
        body["d1Rejudge"]
    );
    assert!(body["d1Rejudge"]["startedAtMs"].is_u64() && body["d1Rejudge"]["endedAtMs"].is_u64());
    // The same version: nothing runs, nothing is re-judged.
    fold13_plant_old(&d1);
    assert_eq!(d1_rejudge_step(&p, t0 + 10_000_000).await.unwrap(), None);
    assert!(fold13_old_left(&d1) > 0, "the same version re-judges none");
    // A version move (the stamp another build's): a run again, the previous version kept.
    d1.conn
        .execute(
            "UPDATE owed_belt_state SET cursor=json_set(cursor,'$.version','fold10-old') WHERE name=?1",
            [D1_REJUDGE_NAME],
        )
        .unwrap();
    // A faulted identity: retried next tick and counted.
    p.commit_faults.set(1);
    let first = d1_rejudge_step(&p, t0 + 20_000_000).await.unwrap().unwrap();
    assert!(
        first.started && first.faulted && first.identities == 0,
        "{first:?}"
    );
    assert_eq!(d1.counter(COUNTERS[31]), 1);
    let s = fold13_stamp(&d1);
    assert_eq!(s["previous"]["version"], json!("fold10-old"));
    assert_eq!(
        (s["openFaults"].clone(), s["open"][0].clone()),
        (json!(1), json!(ids[0]))
    );
    let (_, rest) = fold13_run(&p, t0 + 20_000_000).await;
    assert!(!rest[0].faulted && rest.last().unwrap().done);
    assert_eq!(fold13_old_left(&d1), 0);
    assert_eq!(
        fold13_stamp(&d1)["identities"],
        json!(ids.len()),
        "the faulted one retried, done"
    );
    // Three consecutive faults skip it; the run goes on to its end.
    d1.conn
        .execute(
            "UPDATE owed_belt_state SET cursor=json_set(cursor,'$.version','fold10-old') WHERE name=?1",
            [D1_REJUDGE_NAME],
        )
        .unwrap();
    p.commit_faults.set(3);
    let (_, skipped) = fold13_run(&p, t0 + 30_000_000).await;
    assert_eq!(skipped.iter().map(|r| r.skipped).sum::<usize>(), 1);
    let s = fold13_stamp(&d1);
    assert_eq!(
        (s["skipped"].clone(), s["identities"].clone()),
        (json!(1), json!(ids.len() - 1))
    );
    assert!(d1
        .lines
        .borrow()
        .iter()
        .any(|l| l.contains("D1 re-judge under") && l.contains("SKIPPED")));
}

/// (fold13_c) bsv-low #958: THE ROUTE AND THE RECONCILE SAY WHICH SIDE IS STALE. The rows lever serves
/// `d1JudgedUnder` (the stamp's version once its run passed the identity, the previous run's for a judgement older
/// than the run, `unknown` otherwise) and `d1Rejudge`; the reconcile's `stale-d1` line names whether D1's re-judge had
/// passed the identity, counted apart (`reconcileStaleTotal`), never a diff. RED on `c08be8b51`: neither field
/// exists and a stale D1 is a `RECONCILE DIFF`.
#[tokio::test]
async fn fold13_c_the_route_and_the_reconcile_name_a_stale_d1() {
    let st = |done: bool, after: &str| {
        json!({"version": JUDGE_VERSION, "startedAtMs": 1_000, "done": done, "after": after,
            "previous": {"version": "fold10-old", "startedAtMs": 10, "done": true}})
    };
    assert_eq!(d1_judged_under(None, "b", Some(5)), json!("unknown"));
    assert_eq!(
        d1_judged_under(Some(&st(true, "")), "b", Some(5)),
        json!(JUDGE_VERSION)
    );
    assert_eq!(
        d1_judged_under(Some(&st(false, "m")), "b", None),
        json!(JUDGE_VERSION)
    );
    assert_eq!(
        d1_judged_under(Some(&st(false, "m")), "z", Some(500)),
        json!("fold10-old")
    );
    assert_eq!(
        d1_judged_under(Some(&st(false, "m")), "z", Some(5_000)),
        json!("unknown")
    );
    assert!(
        rejudge_passed(Some(&st(false, "m")), "b") && !rejudge_passed(Some(&st(false, "m")), "z")
    );
    let view = rows_view(
        "z",
        Ok(json!({"rows": []})),
        &[],
        Some(&st(false, "m")),
        Some(500),
    );
    assert_eq!(view["d1JudgedUnder"], json!("fold10-old"));
    assert_eq!(view["d1Rejudge"]["passed"], json!(false));
    assert_eq!(view["d1Rejudge"]["stamp"]["running"], json!(JUDGE_VERSION));
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let object = squash(include_str!("object.rs"));
    let lever = &object[object.find("pubasyncfnrows_lever(").unwrap()
        ..object.find("pubasyncfnidentities_lever(").unwrap()];
    assert!(lever.contains("Stmt::new(D1_JUDGED_UNDER_SQL,vec![Bind::Text(me.clone())])"));
    // The reconcile: a stale D1 row before the fleet re-judge passed the identity.
    let w = WithActor {
        d1: d1_judge(&stranded()).await,
        actor: mirror(),
        d1_judges: true,
        event: None,
        actor_reads: false,
        actor_fails: Cell::new(false),
    };
    crate::owed_events::deliver(&w, "owner", &games_of(&w.d1, "owner"))
        .await
        .unwrap();
    w.d1.conn
        .execute_batch("UPDATE owed_rows SET reason='old-build' WHERE rowid=(SELECT MIN(rowid) FROM owed_rows WHERE identity='owner')")
        .unwrap();
    let r = reconcile_step(&w, NOW_MS as i64).await.unwrap().unwrap();
    assert_eq!(
        (r.diff, r.stale_outpoints, r.rejudge_passed),
        (false, 1, false),
        "{r:?}"
    );
    assert!(w
        .d1
        .lines
        .borrow()
        .iter()
        .any(|l| l.starts_with("[owed-actor] RECONCILE stale-d1 owner")
            && l.contains("had passed the identity: false")));
    assert_eq!(
        (w.d1.counter(COUNTERS[7]), w.d1.counter(COUNTERS[29])),
        (0, 1)
    );
    // After D1's re-judge passed it, the line says so.
    w.d1.conn
        .execute(
            "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES (?1,?2,0)",
            rusqlite::params![D1_REJUDGE_NAME, st(true, "").to_string()],
        )
        .unwrap();
    w.d1.conn
        .execute_batch("UPDATE owed_rows SET reason='old-build' WHERE rowid=(SELECT MIN(rowid) FROM owed_rows WHERE identity='owner')")
        .unwrap();
    let r = reconcile_step(&w, NOW_MS as i64 + RECONCILE_INTERVAL_MS)
        .await
        .unwrap()
        .unwrap();
    assert!(
        r.rejudge_passed && !r.diff && r.stale_outpoints == 1,
        "{r:?}"
    );
    // Served: the health body's `reconcileStaleTotal`.
    assert!(squash(include_str!("../owed_actor.rs"))
        .contains("body[\"reconcileStaleTotal\"]=n(COUNTERS[29]);"));
}

// ---- D2-ACTOR-B-FOLD-14 (bsv-low #954 item 9): the warm apply is ONE record read; the apply's stages served; a
// burst's callers answered from the pass ----

/// The mirror's key set for `games` as [`apply_with`] walks it before its read (the local walk to its fixpoint, the
/// last judgement's txids).
async fn fold14_mirror_keys(local: &SqliteSql, games: &[String]) -> Keys {
    let mut keys = Keys::default();
    loop {
        let mut next = keys.clone();
        keys_of_mirror(local, "owner", games, &keys, &mut next)
            .await
            .unwrap();
        if next == keys {
            break;
        }
        keys = next;
    }
    let judged = local
        .run_batch(vec![Stmt::new(
            JUDGED_TXIDS_SQL,
            vec![
                Bind::Text("owner".into()),
                Bind::Text(json!(games).to_string()),
            ],
        )])
        .await
        .unwrap();
    judged
        .iter()
        .flat_map(|o| &o.rows)
        .for_each(|r| txids_in_value(r, &mut keys.txids));
    keys
}

/// Each family's rows, as sorted JSON text (the read's `DISTINCT` fixes no order).
fn fold14_sorted(rows: &[Vec<Value>]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|f| {
            let mut v: Vec<String> = f.iter().map(Value::to_string).collect();
            v.sort();
            v
        })
        .collect()
}

/// (fold14_a) THE WARM APPLY IS ONE RECORD CALL (bsv-low #954 item 9): on FOLD-7's 600-game chained seat, every game
/// warm, one write to game 300 applied with its event's txids reads the record in ONE batch of one statement (on
/// `c08be8b51` two sequential batches: the marks, then the compound over keys that depend on them), then writes the
/// mirror; and the one statement reads exactly the rows the two-statement read reads, family by family, byte for
/// byte, over exactly its key set. RED on `c08be8b51` (the marked read inert, `read_marked` answering `Ok(None)`:
/// the base's two sequential reads): `left: 2 right: 1`.
#[tokio::test]
async fn fold14_a_a_warm_apply_is_one_record_read() {
    let (record, local, games) = e_warm_seat(600).await;
    let hop = e_write(&record, &games, 300);
    let game = &games[300..301];
    // The two-statement read of the base, over the same mirror.
    let mut n = fold14_mirror_keys(&local, game).await;
    txids_in(&hop, &mut n.txids);
    let mut two = n.clone();
    let marks = record
        .run_batch(vec![Stmt::new(
            NARROW_MARKS_SQL,
            vec![
                Bind::Text("owner".into()),
                Bind::Text(json!(game).to_string()),
            ],
        )])
        .await
        .unwrap();
    marks[0]
        .rows
        .iter()
        .for_each(|m| txids_in_value(&m["sourceKey"], &mut two.txids));
    let (rows_two, _) = read_round(&record, "owner", game, &two, false)
        .await
        .unwrap();
    let one = read_marked(&record, "owner", game, &n, None)
        .await
        .unwrap()
        .expect("no value past D1's bound");
    assert_eq!(
        one.keys, two,
        "the one statement reads the two reads' key set"
    );
    assert_eq!(
        fold14_sorted(&one.rows),
        fold14_sorted(&rows_two),
        "the one statement's rows are the two reads' rows"
    );
    assert!(one.rows.iter().map(Vec::len).sum::<usize>() > 0);
    assert_eq!(one.marks.len(), marks[0].rows.len());
    // The apply: one record batch, one statement.
    let (b0, s0) = (record.batches.get(), record.stmts.get());
    let opts = ApplyOpts {
        touch: Some(vec![hop.clone()]),
        ..ApplyOpts::default()
    };
    let (report, _) = apply_with(
        &record,
        &local,
        &Fakes,
        "owner",
        game,
        Some(("pot", 2)),
        Some(TIP),
        None,
        &opts,
    )
    .await
    .unwrap();
    let (batches, stmts) = (record.batches.get() - b0, record.stmts.get() - s0);
    eprintln!(
        "fold14_a: one warm apply of one game of 600: record batches {batches}, statements {stmts}, rounds {}, misses {}",
        report.rounds, report.misses
    );
    assert!(report.narrowed && report.misses == 0);
    assert_eq!(batches, 1, "a warm apply is one record read");
    assert_eq!(stmts, 1);
    // and its judgement is a cold full apply's
    let cold = mirror();
    apply(
        &record,
        &cold,
        &Fakes,
        "owner",
        game,
        Some(("pot", 2)),
        Some(TIP),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        stored_game(&local, "owner", &games[300]),
        stored_game(&cold, "owner", &games[300])
    );
    // A mark asking for the record (`age`): the marked read is the full apply's first round, still one call for it.
    record
        .conn
        .execute(
            "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('owner',?1,'age','age',1)",
            [&games[301]],
        )
        .unwrap();
    let b1 = record.batches.get();
    let (r, _) = apply_with(
        &record,
        &local,
        &Fakes,
        "owner",
        &games[301..302],
        None,
        Some(TIP),
        None,
        &ApplyOpts {
            touch: Some(vec![h("b8", 301)]),
            ..ApplyOpts::default()
        },
    )
    .await
    .unwrap();
    assert!(!r.narrowed);
    assert_eq!(
        (record.batches.get() - b1) as usize,
        r.rounds,
        "a full apply reads one call a round, its first the marked read"
    );
}

/// (fold14_b) A BOOTSTRAP PAGE READS ITS GAMES IN ITS FIRST ROUND: one page of FOLD-7's 600-game seat on a fresh
/// mirror makes as many record calls as an apply of the same 16 games (one a round; on `c08be8b51` one more, the
/// games' read before the apply), and stores the same rows. RED on `c08be8b51` (the page's marked read inert, the
/// games read first): `left: 3 right: 2`.
#[tokio::test]
async fn fold14_b_a_bootstrap_page_reads_its_games_in_its_first_round() {
    let record = seeded(&fold7_chained_seat(600));
    let paged = mirror();
    let turn = futures_util::lock::Mutex::new(());
    let b0 = record.batches.get();
    let step = bootstrap_step(
        &turn,
        &record,
        &paged,
        &Fakes,
        "owner",
        Some(object::BOOT_CURSOR_START),
        Some(TIP),
        None,
        false,
    )
    .await
    .unwrap();
    let page_calls = record.batches.get() - b0;
    let BootStep::Page { games: n, .. } = step else {
        panic!("{step:?}")
    };
    assert_eq!(n, BOOTSTRAP_PAGE_GAMES);
    let page: Vec<String> = record
        .run_batch(vec![Stmt::new(
            crate::owed_judge::IDENTITY_GAMES_AFTER_SQL,
            vec![
                Bind::Text("owner".into()),
                Bind::Int(i64::MAX),
                Bind::Text(String::new()),
            ],
        )])
        .await
        .unwrap()
        .remove(0)
        .rows
        .iter()
        .map(|r| r["gameId"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(page.len(), n);
    let alone = mirror();
    let b1 = record.batches.get();
    let (report, _) = apply(
        &record,
        &alone,
        &Fakes,
        "owner",
        &page,
        None,
        Some(TIP),
        None,
    )
    .await
    .unwrap();
    let apply_calls = record.batches.get() - b1;
    eprintln!(
        "fold14_b: one bootstrap page of {n} games: record calls {page_calls}; the same games' apply {apply_calls} ({} rounds)",
        report.rounds
    );
    assert_eq!(apply_calls as usize, report.rounds);
    assert_eq!(page_calls, apply_calls, "a page is its rounds' reads");
    assert_eq!(stored_rows_no_clock(&paged), stored_rows_no_clock(&alone));
    assert!(!stored_rows_no_clock(&paged).is_empty());
}

/// A local port whose clock moves one millisecond each time it is read.
struct Ticking<'a>(&'a SqliteSql);
impl SqlPort for Ticking<'_> {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        self.0.run_batch(stmts).await
    }
    async fn counters(&self, d: Vec<(String, u64)>) {
        self.0.counters(d).await;
    }
    fn now_ms(&self) -> u64 {
        let t = self.0.now.get() + 1;
        self.0.now.set(t);
        t
    }
    fn log(&self, l: &str) {
        self.0.log(l);
    }
}

/// (fold14_c) THE APPLY'S STAGES (bsv-low #954 item 9): an event applied in the object's turn answers its stages
/// (`readMs`, `judgeMs`, `writeMs`, `waitMs`), which sum to its time to the millisecond of the clock's own reads, a
/// joined caller's included; and the bars row serves each stage's p95 per class and over the classes beside
/// `applyMsP95`. New API; on `c08be8b51` no answer carries a stage.
#[tokio::test]
async fn fold14_c_the_apply_stages_sum_to_the_apply_and_are_served() {
    let (record, local, games) = e_warm_seat(32).await;
    let hops: Vec<String> = (0..3).map(|k| e_write(&record, &games, k)).collect();
    let ticking = Ticking(&local);
    let burst = Burst::default();
    let turn = futures_util::lock::Mutex::new(());
    let yielding = Yielding(&record);
    let item = |k: usize| BurstItem {
        games: vec![games[k].clone()],
        kind_seq: Some(("pot".into(), 200 + k as i64)),
        tip: Some(TIP),
        bound: None,
        touch: Some(vec![hops[k].clone()]),
    };
    let start = local.now.get();
    let timed = |k: usize| {
        let (burst, turn, yielding, ticking) = (&burst, &turn, &yielding, &ticking);
        async move {
            let t0 = ticking.now_ms();
            let r = apply_coalesced(
                burst,
                turn,
                yielding,
                ticking,
                &Fakes,
                "owner",
                item(k),
                None,
                false,
                false,
            )
            .await
            .unwrap()
            .0;
            (r, t0, ticking.now_ms())
        }
    };
    let answers = futures_util::future::join_all((0..3).map(timed)).await;
    assert!(local.now.get() > start);
    let mut joined = 0;
    for (report, t0, t1) in &answers {
        let stages = report.read_ms + report.judge_ms + report.write_ms + report.wait_ms;
        let elapsed = t1 - t0;
        assert!(
            stages <= elapsed && elapsed - stages <= 2,
            "the stages sum to the apply: {stages} of {elapsed} ({report:?})"
        );
        assert!(report.read_ms > 0 && report.judge_ms > 0 && report.write_ms > 0);
        if report.coalesced > 0 && report.wait_ms > 0 {
            joined += 1;
        }
    }
    assert!(joined > 0, "a caller waited in the turn");
    // Served: the answer, the class's ring, the fleet's p95 per class and over the classes.
    let a = answer_of(&answers[0].0, Vec::new());
    for (field, _) in APPLY_STAGES {
        assert!(a[field].as_u64().is_some(), "{field} answered");
    }
    let mut pending = PendingBars::default();
    let mut served = a.clone();
    served["applyMs"] = json!(answers[0].2 - answers[0].1);
    pending.note_answer("direct", &served);
    flush_bars_from(&record, NOW_MS, &pending).await.unwrap();
    let rows: Vec<Value> = record
        .run_batch(vec![Stmt::new(BARS_READ_SQL, vec![])])
        .await
        .unwrap()
        .remove(0)
        .rows;
    let bars = fleet_bars(&rows, NOW_MS, lib_rev());
    for (field, _) in APPLY_STAGES {
        let k = format!("{field}P95");
        assert_eq!(
            bars["classes"]["direct"][&k],
            json!(a[field]),
            "{k} served for the class"
        );
        assert_eq!(bars["stages"][&k], json!(a[field]), "{k} served");
    }
    assert!(bars["applyMsP95"].as_u64().is_some());
}

/// (fold14_d) A JOINED CALLER IS ANSWERED BY THE PASS, NOT THE TURN (bsv-low #954 item 9, item 3): an event a holder's
/// pass applied returns as soon as the pass leaves its answer, while the turn is still held (by a later caller's pass:
/// futures-util's `Mutex` wakes its lowest slab slot, which a later caller may have taken). RED on `c08be8b51` (the
/// answer read only after the turn, `apply_coalesced` awaiting `turn.lock()` alone): the joined caller times out.
#[tokio::test]
async fn fold14_d_a_joined_caller_is_answered_by_the_pass_not_the_turn() {
    use std::future::Future;
    let record = mirror();
    let local = mirror();
    let burst = Burst::default();
    let turn = futures_util::lock::Mutex::new(());
    let held = turn.lock().await;
    let item = BurstItem {
        games: vec!["g".into()],
        kind_seq: Some(("pot".into(), 1)),
        ..BurstItem::default()
    };
    let mut joiner = Box::pin(apply_coalesced(
        &burst, &turn, &record, &local, &Fakes, "owner", item, None, false, false,
    ));
    let first = futures_util::future::poll_fn(|cx| {
        std::task::Poll::Ready(joiner.as_mut().poll(cx).is_pending())
    })
    .await;
    assert!(first, "the caller waits while the turn is held");
    // A holder's pass took the caller's event and leaves its answer; the turn stays held (a later caller's pass).
    let taken = burst.take();
    assert_eq!(taken.len(), 1);
    let report = ApplyReport {
        applied_games: vec!["g".into()],
        read_ms: 3,
        ..ApplyReport::default()
    };
    burst.answer(vec![(taken[0].0, Ok((report, Vec::new())))]);
    let got = tokio::time::timeout(std::time::Duration::from_secs(2), joiner)
        .await
        .expect("the joined caller is answered while the turn is held")
        .unwrap();
    assert_eq!(got.0.applied_games, vec!["g".to_string()]);
    assert_eq!(got.0.read_ms, 3);
    drop(held);
}

// ---- D2-ACTOR-B-FOLD-15 (the eighth delta lens's NEW8-M1, NEW8-M2, NEW8-L1; bsv-low #958) ----

fn fold15_port<'a>(d1: &'a SqliteSql, actor: &'a SqliteSql) -> Fold13<'a> {
    Fold13 {
        d1,
        actor,
        era: None,
        applies: Cell::new(0),
        commit_faults: Cell::new(0),
        tip: Cell::new(TIP),
        actor_era: None,
        die_after: Cell::new(u32::MAX),
        commits: Cell::new(0),
        mark_during: RefCell::new(Vec::new()),
        mark_after: RefCell::new(Vec::new()),
    }
}

/// A fleet identity holding a gated row more than 3 blocks from its gate: (fixture's D1, identity, outpoint, game).
async fn fold15_gated() -> (SqliteSql, String, String, String) {
    for f in fixtures() {
        let d1 = d1_judge(&f).await;
        for id in f.identities.clone() {
            let rows = d1
                .run_batch(vec![Stmt::new(ROWS_SQL, vec![Bind::Text(id.clone())])])
                .await
                .unwrap()[0]
                .rows
                .clone();
            let gated = rows.iter().find(|r| {
                r["facts"]
                    .as_str()
                    .and_then(|s| serde_json::from_str::<Value>(s).ok())
                    .is_some_and(|f| f["blocksToGate"].as_i64().is_some_and(|b| b > 3))
            });
            if let Some(g) = gated {
                let (o, game) = (
                    g["outpoint"].as_str().unwrap().to_string(),
                    g["gameId"].as_str().unwrap().to_string(),
                );
                return (d1, id, o, game);
            }
        }
    }
    panic!("no fixture identity holds a row more than 3 blocks from its gate");
}

async fn fold15_bootstrap(d1: &SqliteSql, local: &SqliteSql, id: &str, tip: u64) {
    for chunk in games_of(d1, id).chunks(GAMES_PER_APPLY) {
        apply(
            d1,
            local,
            &Fakes,
            id,
            chunk,
            Some(("bootstrap", 1)),
            Some(tip),
            None,
        )
        .await
        .unwrap();
    }
}

fn fold15_rows(db: &SqliteSql, id: &str, game: &str) -> Vec<Value> {
    let mut q = db
        .conn
        .prepare("SELECT outpoint,family,gameId,sats,opponentIdentity,atHeight,facts,reason,claimable,probeTip FROM owed_rows WHERE identity=?1 AND gameId=?2 ORDER BY outpoint")
        .unwrap();
    q.query_map(rusqlite::params![id, game], |r| {
        Ok(json!({
            "outpoint": r.get::<_, String>(0)?, "family": r.get::<_, Option<String>>(1)?,
            "gameId": r.get::<_, Option<String>>(2)?, "sats": r.get::<_, Option<i64>>(3)?,
            "opponentIdentity": r.get::<_, Option<String>>(4)?, "atHeight": r.get::<_, Option<i64>>(5)?,
            "facts": r.get::<_, Option<String>>(6)?, "reason": r.get::<_, Option<String>>(7)?,
            "claimable": r.get::<_, Option<i64>>(8)?, "probeTip": r.get::<_, Option<i64>>(9)?,
        }))
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

/// (fold15_a) NEW8-M1: THE RECONCILE COMPARES FRESH WITH FRESH AT ONE TIP, AND THE ROUTE NAMES A TIP LAG. The lens's
/// scratch a: an actor bootstrapped at TIP, D1's stored row of a gated game made stale (another reason), the
/// reconcile 3 blocks on. Both sides are judged at the reconcile's tip: no reconcile diff (`blocksToGate` 97 on both),
/// the stale D1 named, the actor's own lag counted apart (`RECONCILE TIP LAG`). On the route, D1 judged 3 blocks
/// after the actor: the gated row is `tipLag` (both tips stated), never `diff`; a verdict difference still is.
/// RED on `800d8b2e3`: `RECONCILE DIFF .. (a judge disagreement ..) [facts.blocksToGate d1=97 actor=100]` counted.
#[tokio::test]
async fn fold15_a_the_reconcile_compares_fresh_with_fresh_and_the_route_names_a_tip_lag() {
    let (d1, id, outpoint, game) = fold15_gated().await;
    let local = mirror();
    fold15_bootstrap(&d1, &local, &id, TIP).await;
    let p = fold15_port(&d1, &local);
    p.tip.set(TIP + 3);
    d1.conn
        .execute(
            "UPDATE owed_rows SET reason='old-build' WHERE identity=?1 AND outpoint=?2",
            rusqlite::params![id, outpoint],
        )
        .unwrap();
    // The reconcile's cursor just before the identity (the port's other identities hold no bootstrapped actor).
    let before_id = {
        let mut b = id.clone().into_bytes();
        let last = b.len() - 1;
        b[last] -= 1;
        String::from_utf8(b).unwrap()
    };
    d1.conn
        .execute(
            "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES (?1,?2,0)",
            rusqlite::params![RECONCILE_STATE_NAME, before_id],
        )
        .unwrap();
    let r = reconcile_step(&p, NOW_MS as i64 + RECONCILE_INTERVAL_MS)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.identity, id);
    let lines = d1.lines.borrow().clone();
    assert!(
        !r.diff && d1.counter(COUNTERS[7]) == 0,
        "a tip lag is never a judge disagreement: {r:?} {:?}",
        lines
            .iter()
            .filter(|l| l.contains("RECONCILE"))
            .collect::<Vec<_>>()
    );
    assert_eq!((r.judged, r.stale_outpoints), (1, 1), "{r:?}");
    assert!(
        d1.counter(COUNTERS[28]) >= 1,
        "the actor's own lag counted apart"
    );
    assert!(lines
        .iter()
        .any(|l| l.starts_with(&format!("[owed-actor] RECONCILE TIP LAG (not a STOP) {id}"))));
    assert!(!lines.iter().any(|l| l.contains("RECONCILE DIFF")));
    // Both fresh at TIP + 3: the gated row's distance is the same on both sides.
    let gate = |db: &SqliteSql| {
        fold15_rows(db, &id, &game)
            .into_iter()
            .find(|r| r["outpoint"] == outpoint.as_str())
            .and_then(|r| {
                r["facts"]
                    .as_str()
                    .and_then(|s| serde_json::from_str::<Value>(s).ok())
            })
            .map(|f| f["blocksToGate"].clone())
    };
    assert_eq!(gate(&d1), gate(&local));
    // The route: D1 judged at TIP + 3, a fresh actor bootstrapped at TIP.
    let old = mirror();
    fold15_bootstrap(&d1, &old, &id, TIP).await;
    let (d1_rows, actor_rows) = (fold15_rows(&d1, &id, &game), fold15_rows(&old, &id, &game));
    let view = rows_view(&id, Ok(json!({ "rows": actor_rows })), &d1_rows, None, None);
    assert!(
        view["tipLag"]
            .as_array()
            .is_some_and(|a| a.contains(&json!(outpoint))),
        "{}",
        view["tipLag"]
    );
    assert_eq!(view["diff"], json!([]), "only a tip lag: {}", view["diff"]);
    assert_eq!(view["tipLagTips"]["actor"], json!([TIP]));
    assert_eq!(view["tipLagTips"]["d1"], json!([TIP + 3]));
    // A verdict difference beside the lag is still a diff.
    let mut moved = actor_rows.clone();
    for r in moved
        .iter_mut()
        .filter(|r| r["outpoint"] == outpoint.as_str())
    {
        r["reason"] = json!("another verdict");
    }
    let view = rows_view(&id, Ok(json!({ "rows": moved })), &d1_rows, None, None);
    assert_eq!(view["diff"], json!([outpoint]));
    assert_eq!(view["tipLag"], json!([]));
}

/// (fold15_b) NEW8-M2: THE RE-JUDGE IS BOUNDED BY CALLS AND SAVED PER IDENTITY. The lens's scratch b: 2,000
/// identities of `owed_state` holding no game. One tick opens at most `D1_REJUDGE_IDENTITIES_PER_TICK` (each an
/// identity, counted) in at most two calls each and the save; a tick the platform ends after its tenth call has
/// saved the identities it passed. RED on `800d8b2e3`: ONE tick walked all 2,000 in 4,003 calls (`left: 2000`), and
/// a dying tick saved nothing.
#[tokio::test]
async fn fold15_b_the_rejudge_is_bounded_by_calls_and_saved_per_identity() {
    let d1 = SqliteSql::migrated();
    for i in 0..2000 {
        d1.conn
            .execute(
                &format!(
                    "INSERT INTO owed_state(identity,computedAtMs,rows) VALUES ('02{i:064x}',1,0)"
                ),
                [],
            )
            .unwrap();
    }
    let local = mirror();
    let p = fold15_port(&d1, &local);
    let (b, st) = (d1.batches.get(), d1.stmts.get());
    let r = d1_rejudge_step(&p, NOW_MS as i64).await.unwrap().unwrap();
    let (calls, stmts) = (d1.batches.get() - b, d1.stmts.get() - st);
    eprintln!("fold15_b: one tick over 2,000 no-game identities: {r:?}, {calls} calls, {stmts} statements");
    assert_eq!(r.identities, D1_REJUDGE_IDENTITIES_PER_TICK, "{r:?}");
    assert!(
        calls as usize <= 2 * D1_REJUDGE_IDENTITIES_PER_TICK + 2,
        "{calls} calls"
    );
    assert_eq!(
        fold13_stamp(&d1)["after"],
        json!(format!("02{:064x}", D1_REJUDGE_IDENTITIES_PER_TICK - 1))
    );
    // A tick that dies after its tenth call: the identities it passed are saved (the save rides the next read).
    let before = fold13_stamp(&d1)["identities"].as_u64().unwrap();
    p.die_after.set(10);
    assert!(d1_rejudge_step(&p, NOW_MS as i64 + 120_000).await.is_err());
    let after = fold13_stamp(&d1)["identities"].as_u64().unwrap();
    assert!(
        after >= before + 3,
        "saved per identity: {before} then {after}"
    );
    // The run goes on to its end, each identity once.
    p.die_after.set(u32::MAX);
    let (ticks, _) = fold13_run(&p, NOW_MS as i64 + 240_000).await;
    let s = fold13_stamp(&d1);
    assert_eq!(
        (s["done"].clone(), s["identities"].clone()),
        (json!(true), json!(2000))
    );
    eprintln!("fold15_b: the 2,000 done in {} more ticks", ticks);
}

/// (fold15_c) NEW8-M2: THE RECONCILE'S FRESH JUDGE IS ONE CALL A STEP, and the alarm's two phases measured on the
/// fleet's whale (every stored row of it stale: the #958 window). The step judges 16 games in ONE judge call and
/// marks the rest; a re-judge tick at the default pace (32) is two judge calls and yields the reconcile, at 16 one
/// call beside it; either alarm under 1,000 statements with the belt tick's 314. RED on `800d8b2e3`: the fresh
/// judge made 4 calls (64 games).
#[tokio::test]
async fn fold15_c_the_reconciles_fresh_judge_is_one_call_and_the_alarm_is_bounded() {
    let f = fixtures()
        .into_iter()
        .find(|f| f.name == "d1_fleet")
        .unwrap();
    let d1 = d1_judge(&f).await;
    let local = mirror();
    let p = fold15_port(&d1, &local);
    let mut now = NOW_MS as i64;
    // The re-judge at the default pace (32: two judge calls, the reconcile yielding), then at 16 (one call, the
    // reconcile beside it): each run to its end on the fleet, the worst tick's statements kept.
    let mut worst = [0u32; 2];
    for (k, pace) in [(0usize, None), (1, Some("16"))] {
        d1.conn
            .execute(
                "DELETE FROM owed_belt_state WHERE name='owed-judge:version'",
                [],
            )
            .unwrap();
        loop {
            now += 120_000;
            let (s, c) = (d1.stmts.get(), p.commits.get());
            let r = d1_rejudge_step_paced(&p, now, pace).await.unwrap();
            let st = d1.stmts.get() - s;
            let calls = p.commits.get() - c;
            worst[k] = worst[k].max(st);
            let Some(r) = r else { break };
            assert_eq!(r.judge_calls as u32, calls);
            assert!(r.games <= r.games_per_tick && calls <= 2, "{r:?}");
            assert_eq!(reconcile_yields(&r), calls > 1);
            if pace.is_some() {
                assert!(calls <= 1 && !reconcile_yields(&r));
            }
            if r.done {
                break;
            }
        }
    }
    let worst_rejudge = worst[1];
    let id = f.identities[0].clone();
    fold15_bootstrap(&d1, &local, &id, TIP).await;
    d1.conn
        .execute(
            "UPDATE owed_rows SET reason='old-build' WHERE identity=?1",
            rusqlite::params![id],
        )
        .unwrap();
    d1.conn
        .execute(
            "DELETE FROM owed_belt_state WHERE name LIKE 'reconcile%'",
            [],
        )
        .unwrap();
    let mut got = None;
    for _ in 0..200 {
        now += RECONCILE_INTERVAL_MS;
        let (s, c) = (d1.stmts.get(), p.commits.get());
        if let Some(r) = reconcile_step(&p, now).await.unwrap() {
            if r.identity == id {
                got = Some((r, d1.stmts.get() - s, p.commits.get() - c));
                break;
            }
        }
    }
    let (r, stmts, commits) = got.expect("the whale reconciled");
    eprintln!(
        "fold15_c: the worst re-judge tick at 32 games {} statements (the reconcile yields: the alarm {}), at 16 {worst_rejudge}; the whale's reconcile judged {} marked {} in {commits} judge calls, {stmts} statements; with the belt tick's 314 the alarm's worst at 16 {}",
        worst[0],
        314 + worst[0],
        r.judged,
        r.unjudged,
        314 + worst_rejudge + stmts
    );
    assert!(
        314 + worst[0] < 1000,
        "the default pace alone: {}",
        worst[0]
    );
    assert_eq!(commits, 1, "the fresh judge is one call");
    assert_eq!(r.judged, RECONCILE_JUDGE_GAMES_MAX);
    assert!(r.unjudged > 0 && !r.diff, "{r:?}");
    assert!(
        314 + worst_rejudge + stmts < 1000,
        "the alarm's one invocation under D1's 1,000: {worst_rejudge} + {stmts}"
    );
}

/// (fold15_d) NEW8-L1: `d1JudgedUnder` names the previous run's version only when that run was done. RED on
/// `800d8b2e3`: `left: "fold10-old" right: "unknown"`.
#[test]
fn fold15_d_d1_judged_under_names_only_a_finished_previous_run() {
    let st = |prev_done: bool| {
        json!({"version": JUDGE_VERSION, "startedAtMs": 1_000, "done": false, "after": "m",
            "previous": {"version": "fold10-old", "startedAtMs": 10, "done": prev_done}})
    };
    assert_eq!(
        d1_judged_under(Some(&st(false)), "z", Some(500)),
        json!("unknown")
    );
    assert_eq!(
        d1_judged_under(Some(&st(true)), "z", Some(500)),
        json!("fold10-old")
    );
}

/// (fold15_e) THE CAPTAIN'S PACE LEVER: the pace row wins over the render's var, the var over the default, all
/// clamped to 32; `0` PAUSES the walk with its cursor kept and the stamp serving `paused`; a positive pace resumes it
/// from that cursor; the lever's body checked (an integer to 32, or null to clear); the route and both renders' var
/// wired. New API: no RED on `800d8b2e3` (it does not compile there).
#[tokio::test]
async fn fold15_e_the_pace_is_a_lever_and_zero_pauses_with_the_cursor_kept() {
    assert_eq!(rejudge_pace(None, None), 32);
    assert_eq!(rejudge_pace(None, Some("8")), 8);
    assert_eq!(
        rejudge_pace(Some(&json!({"gamesPerTick": 4})), Some("8")),
        4
    );
    assert_eq!(rejudge_pace(Some(&json!({"gamesPerTick": 4.0})), None), 4);
    assert_eq!(
        rejudge_pace(None, Some("500")),
        D1_REJUDGE_GAMES_PER_TICK_MAX
    );
    assert_eq!(rejudge_pace(None, Some("junk")), 32);
    assert!(pace_lever_plan(&json!({"gamesPerTick": 33}), 1).is_err());
    assert!(pace_lever_plan(&json!({"gamesPerTick": -1}), 1).is_err());
    assert!(pace_lever_plan(&json!({}), 1).is_err());
    assert_eq!(
        pace_lever_plan(&json!({"gamesPerTick": null}), 1).unwrap()[0].sql,
        D1_REJUDGE_PACE_CLEAR_SQL
    );
    // A fleet of no-game identities: one tick, then the lever pauses (its own statements), a paused tick walks none.
    let d1 = SqliteSql::migrated();
    for i in 0..100 {
        d1.conn
            .execute(
                &format!(
                    "INSERT INTO owed_state(identity,computedAtMs,rows) VALUES ('02{i:064x}',1,0)"
                ),
                [],
            )
            .unwrap();
    }
    let local = mirror();
    let p = fold15_port(&d1, &local);
    let t0 = NOW_MS as i64;
    let r = d1_rejudge_step(&p, t0).await.unwrap().unwrap();
    assert_eq!(r.identities, D1_REJUDGE_IDENTITIES_PER_TICK);
    let cursor = fold13_stamp(&d1)["after"].clone();
    d1.run_batch(pace_lever_plan(&json!({"gamesPerTick": 0}), t0).unwrap())
        .await
        .unwrap();
    assert_eq!(fold13_stamp(&d1)["paused"], json!(true));
    for k in 1..=3 {
        let r = d1_rejudge_step(&p, t0 + k * 120_000)
            .await
            .unwrap()
            .unwrap();
        assert!(
            r.paused && r.identities == 0 && r.games_per_tick == 0,
            "{r:?}"
        );
    }
    let s = fold13_stamp(&d1);
    assert_eq!(
        (s["after"].clone(), s["done"].clone()),
        (cursor.clone(), json!(false))
    );
    assert_eq!(rejudge_served(Some(&s))["paused"], json!(true));
    // Served on /health.owedActor.d1Rejudge (the lever forced the snapshot).
    let snap: String = d1
        .conn
        .query_row(
            "SELECT cursor FROM owed_belt_state WHERE name='owed-actor:health'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let body = health_body(
        health_snapshot_of(Some(&snap)).as_ref(),
        NOW_MS,
        &Value::Null,
    );
    assert_eq!(
        body["d1Rejudge"]["paused"],
        json!(true),
        "{}",
        body["d1Rejudge"]
    );
    // Resumed at 8 a tick from the kept cursor.
    d1.run_batch(pace_lever_plan(&json!({"gamesPerTick": 8}), t0).unwrap())
        .await
        .unwrap();
    let r = d1_rejudge_step(&p, t0 + 10 * 120_000)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !r.paused && r.games_per_tick == 8 && r.identities > 0,
        "{r:?}"
    );
    assert!(fold13_stamp(&d1)["after"].as_str() > cursor.as_str());
    assert_eq!(fold13_stamp(&d1)["paused"], json!(false));
    // The route, the alarm's var read and yield, and both renders' var.
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let lib = squash(include_str!("../lib.rs"));
    assert!(lib.contains("returnowed_actor::rejudge_pace_lever(req,&env).await;"));
    let belt = squash(include_str!("../owed_belt.rs"));
    assert!(belt.contains(".var(\"OWED_D1_REJUDGE_GAMES_PER_TICK\")"));
    assert!(belt.contains(".is_some_and(crate::owed_actor::reconcile_yields);"));
    let renders = include_str!("../../wrangler.toml");
    assert_eq!(
        renders
            .matches("\nOWED_D1_REJUDGE_GAMES_PER_TICK = \"32\"\n")
            .count(),
        2
    );
}

// ---- D2-ACTOR-B-FOLD-16 (the ninth delta lens's NEW9-M1, NEW9-L1; bsv-low #958) ----

/// One reconcile of `fold13_a`'s planted judge disagreement (D1 writes the hop press off under the era, the actor's
/// closure under no era keeps it), the identity's games holding `start` marks before the reconcile and `during` marks
/// written by the actor's re-apply (while the two judge). Answers (diff, reconcileDiffs, heldDiffs, the lines).
async fn fold16_a_run(start: bool, during: bool, after: bool) -> (bool, u64, u64, Vec<String>) {
    let made = (NOW_MS / 1000) as i64 - 8 * 86_400;
    let era = (made + 5 * 86_400) * 1000;
    let d1 = seeded(&fold11_g_fixture(made));
    let games = games_of(&d1, "owner");
    let local = mirror();
    let pe = Fold13 {
        era: Some(era),
        ..fold15_port(&d1, &local)
    };
    crate::owed_judge::judge_subjects(&pe, "owner", &games, "d1-era")
        .await
        .unwrap();
    apply(
        &d1,
        &local,
        &Fakes,
        "owner",
        &games,
        Some(("bootstrap", 1)),
        Some(TIP),
        None,
    )
    .await
    .unwrap();
    d1.conn.execute("DELETE FROM owed_dirty", []).unwrap();
    if start {
        for g in &games {
            d1.conn
                .execute(
                    "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('owner',?1,'age','age',1)",
                    rusqlite::params![g],
                )
                .unwrap();
        }
    }
    let p = Fold13 {
        era: Some(era),
        actor_era: Some(None),
        ..fold15_port(&d1, &local)
    };
    if during {
        *p.mark_during.borrow_mut() = games.clone();
    }
    if after {
        *p.mark_after.borrow_mut() = games.clone();
    }
    let r = reconcile_step(&p, NOW_MS as i64).await.unwrap().unwrap();
    assert_eq!(r.identity, "owner");
    let lines: Vec<String> = d1
        .lines
        .borrow()
        .iter()
        .filter(|l| l.contains("RECONCILE"))
        .cloned()
        .collect();
    (
        r.diff,
        d1.counter(COUNTERS[7]),
        d1.counter(COUNTERS[17]),
        lines,
    )
}

/// (fold16_a) NEW9-M1: A MARK PENDING AT THE RECONCILE'S START NEVER TURNS A JUDGE DISAGREEMENT INTO A HELD NOTE. The
/// lens's scratch a: the actor holds a press D1 writes off (a genuine disagreement). With no mark it is a STOP; with
/// an `age` mark on its games before the reconcile (both fresh judgements read the record after it) it is STILL a STOP
/// (`reconcileDiffs` 1, `heldDiffs` 0, the line "a judge disagreement"); only a mark written WHILE the two judge (the
/// actor's re-apply writes it, a generation above the bound) holds it (`heldDiffs` 1, "not a STOP"). RED on
/// `d57bc0fa0` at the start mark: `left: 0 right: 1` (the disagreement counted held).
#[tokio::test]
async fn fold16_a_a_start_mark_never_holds_a_judge_disagreement() {
    let control = fold16_a_run(false, false, false).await;
    eprintln!("fold16_a control: {control:?}");
    assert_eq!(
        (control.0, control.1, control.2),
        (true, 1, 0),
        "the control is a STOP"
    );
    let start = fold16_a_run(true, false, false).await;
    eprintln!("fold16_a start mark: {start:?}");
    assert_eq!(
        start.1, 1,
        "a mark both judges read past is still a STOP: {start:?}"
    );
    assert_eq!(start.2, 0, "never a held note: {start:?}");
    assert!(start.0);
    assert!(
        start.3.iter().any(|l| l.contains("a judge disagreement"))
            && !start.3.iter().any(|l| l.contains("held mark")),
        "{:?}",
        start.3
    );
    let during = fold16_a_run(false, true, false).await;
    eprintln!("fold16_a mark during the judge: {during:?}");
    assert_eq!(
        (during.1, during.2),
        (0, 1),
        "a mark written while they judged holds it: {during:?}"
    );
    assert!(during.3.iter().any(|l| l.contains("held mark; not a STOP")));
}

/// (fix960_a) bsv-low #960, the tenth delta lens's NEW10-L1: A MARK THE ACTOR'S CLOSURE NEVER READ EXCUSES NOTHING.
/// `fold16_a`'s planted judge disagreement with a mark written AFTER the actor's re-apply read the record and BEFORE
/// the reconcile's held read (a generation above the bound and above the actor's `readGeneration`): neither fresh
/// judgement read it, so the difference is a STOP (`reconcileDiffs` 1, `heldDiffs` 0, "a judge disagreement"). The
/// held side is kept: a mark written while the actor read (at most its `readGeneration`) still holds (`fold16_a`'s
/// during leg), and so does the same mark beside it. RED on `1ab40c95` with `readGeneration` grafted inert (the held
/// read uncut): `left: (0, 1) right: (1, 0)`, the disagreement filed as held.
#[tokio::test]
async fn fix960_a_a_mark_after_the_actors_read_never_holds_a_disagreement() {
    let after = fold16_a_run(false, false, true).await;
    eprintln!("fix960_a mark after the actor's read: {after:?}");
    assert_eq!(
        (after.1, after.2),
        (1, 0),
        "a mark neither judgement read is a STOP: {after:?}"
    );
    assert!(after.0);
    assert!(
        after.3.iter().any(|l| l.contains("a judge disagreement"))
            && !after.3.iter().any(|l| l.contains("held mark")),
        "{:?}",
        after.3
    );
    let both = fold16_a_run(false, true, true).await;
    eprintln!("fix960_a marks during and after: {both:?}");
    assert_eq!(
        (both.1, both.2),
        (0, 1),
        "a mark the actor read still holds it: {both:?}"
    );
}

/// A direct port over the SQLite record (the drain's direct pass): each event's subjects read from its marks
/// (`plan_with`, as `WorkerDirect`), every call answered applied; Phases A and B (no acknowledgement).
struct Fold16Direct<'a>(&'a SqliteSql);
impl crate::owed_events::DirectPort for Fold16Direct<'_> {
    async fn subjects(
        &self,
        ev: &overlay_discovery::owed_event::OwedEvent,
    ) -> Result<Vec<(String, Vec<String>)>, String> {
        Ok(crate::owed_events::plan_with(self.0, ev, false)
            .await?
            .subjects())
    }
    async fn call(&self, _body: Value) -> Result<Value, String> {
        Ok(json!({"applied": true, "firstApply": false, "appliedGames": []}))
    }
}

/// The D1 statements of the alarm's drain with a full sweep (64 pairs, a stalled consumer): the sweep's tick, its
/// publish batch, the direct pass's mark reads, the bars' flush and the backlog snapshot, plus the counter deltas the
/// Worker writes one statement each (`FOLD16_DRAIN_COUNTER_STMTS`: the publish's eight and the direct pass's four).
async fn fold16_drain_statements() -> (u32, usize) {
    let d1 = SqliteSql::migrated();
    let now: i64 = 1_800_000_000_000;
    d1.now.set(now as u64);
    for i in 0..80u32 {
        d1.conn
            .execute(
                "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES (?1,?2,'hop',?3,?4)",
                rusqlite::params![
                    format!("02{:064x}", i),
                    format!("g{i}"),
                    format!("{:064x}:0", i + 1),
                    now - 3_600_000
                ],
            )
            .unwrap();
    }
    let s0 = d1.stmts.get();
    let swept = crate::owed_events::sweep_tick(&d1, now).await.unwrap();
    let pairs = swept.pairs.clone();
    let port = crate::owed_events::SqlPublish {
        db: &d1,
        send: |_: Vec<String>| async { Ok::<(), String>(()) },
    };
    let report =
        crate::owed_events_publish::publish_with(&port, crate::owed_events::notes_of(&pairs)).await;
    assert!(report.fault.is_none());
    crate::owed_events::direct_with(&Fold16Direct(&d1), report.sent.clone(), Some(TIP)).await;
    crate::owed_actor::flush_bars(&d1, now as u64, true).await;
    crate::owed_judge::snapshot_dirty(&d1).await.unwrap();
    (
        d1.stmts.get() - s0 + FOLD16_DRAIN_COUNTER_STMTS,
        pairs.len(),
    )
}
const FOLD16_DRAIN_COUNTER_STMTS: u32 = 12;
/// The belt tick's statements (the budget tier's `owed-belt-tick`, as `fold15_c` takes it).
const FOLD16_BELT_TICK_STMTS: u32 = 314;

/// (fold16_b) NEW9-L1: THE DRAIN'S STATEMENTS COUNT IN THE ALARM'S BOUND. A full sweep (64 pairs re-published) is
/// measured, then the re-judge run to its end on the fleet in an alarm whose drain did work: every tick makes at most
/// ONE judge call and the reconcile yields, so the worst alarm (the drain, the belt tick, the worst re-judge tick) and
/// the alarm whose re-judge made no call (the drain, the belt tick, the whale's reconcile) both stay under D1's 1,000.
/// The wiring: the alarm hands the drain's work to the re-judge. Measured: the drain 276 statements, the worst alarm
/// 915 (the drain, 314, 325), the reconcile's 872. RED against the drain ignored (`rejudge_calls_max` answering
/// `ceil(pace / 16)` and `reconcile_yields` at `> 1` alone, `d57bc0fa0`'s rule): "one judge call under a working
/// drain" (two calls a tick: the alarm 276 + 314 + 635 = 1,225). The API is new (it does not compile on `d57bc0fa0`).
#[tokio::test]
async fn fold16_b_the_drains_statements_count_in_the_alarms_bound() {
    let (drain, pairs) = fold16_drain_statements().await;
    assert_eq!(pairs, 64, "a full sweep");
    let f = fixtures()
        .into_iter()
        .find(|f| f.name == "d1_fleet")
        .unwrap();
    let d1 = d1_judge(&f).await;
    let local = mirror();
    let p = fold15_port(&d1, &local);
    let mut now = NOW_MS as i64;
    let mut worst = 0u32;
    let mut ticks = 0usize;
    loop {
        now += 120_000;
        let (s, c) = (d1.stmts.get(), p.commits.get());
        let r = d1_rejudge_step_after_drain(&p, now, None, true)
            .await
            .unwrap();
        worst = worst.max(d1.stmts.get() - s);
        let calls = p.commits.get() - c;
        let Some(r) = r else { break };
        ticks += 1;
        assert!(r.drain_bound && r.games_per_tick == 32);
        assert!(calls <= 1, "one judge call under a working drain: {r:?}");
        assert_eq!(reconcile_yields(&r), calls == 1, "{r:?}");
        if r.done {
            break;
        }
    }
    assert!(ticks > 1);
    // The alarm whose re-judge made no call (done or paused): the reconcile runs beside the drain.
    let id = f.identities[0].clone();
    fold15_bootstrap(&d1, &local, &id, TIP).await;
    d1.conn
        .execute(
            "UPDATE owed_rows SET reason='old-build' WHERE identity=?1",
            rusqlite::params![id],
        )
        .unwrap();
    d1.conn
        .execute(
            "DELETE FROM owed_belt_state WHERE name LIKE 'reconcile%'",
            [],
        )
        .unwrap();
    let mut reconcile = None;
    for _ in 0..200 {
        now += RECONCILE_INTERVAL_MS;
        let s = d1.stmts.get();
        if let Some(r) = reconcile_step(&p, now).await.unwrap() {
            if r.identity == id {
                reconcile = Some(d1.stmts.get() - s);
                break;
            }
        }
    }
    let reconcile = reconcile.expect("the whale reconciled");
    let with_rejudge = drain + FOLD16_BELT_TICK_STMTS + worst;
    let with_reconcile = drain + FOLD16_BELT_TICK_STMTS + reconcile;
    eprintln!(
        "fold16_b: a full 64-pair drain {drain} statements; the worst re-judge tick under it {worst} (one judge call, the reconcile yielding): the alarm {with_rejudge}; the whale's reconcile {reconcile}: the alarm {with_reconcile}"
    );
    assert!(with_rejudge < 1000, "{with_rejudge}");
    assert!(with_reconcile < 1000, "{with_reconcile}");
    // The pure rules and the alarm's wiring.
    assert_eq!(rejudge_calls_max(32, false), 2);
    assert_eq!(rejudge_calls_max(32, true), 1);
    assert_eq!(rejudge_calls_max(0, true), 0);
    let src: String = include_str!("../owed_belt.rs").split_whitespace().collect();
    assert!(
        src.contains("r.swept>0||r.subjects>0"),
        "the drain's work read"
    );
    assert!(
        src.contains("d1_rejudge_env(env,drain_bound)"),
        "handed to the re-judge"
    );
    assert!(src.contains("d1_rejudge_step_after_drain(&judge,"));
    assert!(src.contains("DrainReport{swept,..Default::default()}"));
}

/// A direct port answering every call as an applied first apply with its timings (an answer the object did NOT note),
/// recording each body it was given.
struct Fold16Sampled(RefCell<Vec<Value>>);
impl crate::owed_events::DirectPort for Fold16Sampled {
    async fn subjects(
        &self,
        ev: &overlay_discovery::owed_event::OwedEvent,
    ) -> Result<Vec<(String, Vec<String>)>, String> {
        let games: Vec<String> = (0..16).map(|i| format!("{}-{i}", ev.source_key)).collect();
        Ok(vec![("02".to_string() + &"ab".repeat(32), games)])
    }
    async fn call(&self, body: Value) -> Result<Value, String> {
        self.0.borrow_mut().push(body.clone());
        Ok(
            json!({"identity": body["identity"], "applied": true, "firstApply": true, "warmAtBirth": true,
            "applyMs": 3533, "judgeMs": 3105, "readMs": 428, "waitMs": 0,
            "atMs": body["atMs"], "judgedAtMs": body["atMs"].as_u64().map(|a| a + 3533)}),
        )
    }
}

/// (fold16_c) THE CAPTAIN'S ITEM 5 (beta at 5f820ebf9: the direct class served two samples of 3,533 ms apply with no
/// writer's event behind them): the bars are the writers'. Found: the reconcile's fresh re-apply (`kind`
/// "reconcile", `D1Judge::actor_apply`) carried an event's `atMs` and no class, so the object sampled it into the
/// QUEUE class; the DIRECT samples are the drain's sweep re-publishing the `backfill` marks the reconcile writes for
/// the games past its 16 (`RECONCILE_MARK_REST_SQL`), its direct pass calling the actor with up to 64 games under
/// `class: "direct"`. Now the reconcile's body carries no `atMs` and the sweep's pass `class: "sweep"`: neither is
/// sampled by the object (`samples_bars`) nor by the caller; a writer's direct pass still is. The D1 re-judge calls no
/// actor. RED against the sweep's pass as on `d57bc0fa0` (its bodies under `class: "direct"`, its answers noted): the
/// assertion that every body the sweep sent carries `class: "sweep"` (`fold16-red-item5.txt`). The object's gate is
/// pinned by its source (wasm32 only); not run on beta.
#[tokio::test]
async fn fold16_c_the_reconcile_and_the_sweep_sample_nothing_into_the_writers_bars() {
    // The reconcile's body: no event, no class: sampled by no one.
    let games: Vec<String> = (0..16).map(|i| format!("g{i}")).collect();
    let body = reconcile_apply_body("Owner", &games, "reconcile", Some(TIP));
    assert!(body["atMs"].is_null() && body["seq"].is_null() && body["class"].is_null());
    assert!(!samples_bars(body["atMs"].as_u64(), body["class"].as_str()));
    assert!(!samples_bars(Some(1), Some(SWEEP_CLASS)));
    assert!(samples_bars(Some(1), Some("direct")) && samples_bars(Some(1), None));
    // The sweep's direct pass over N events: nothing into the bars; a writer's pass: its samples.
    let direct_samples = || {
        let p = pending_bars_snapshot();
        (
            p.apply.get("direct").map_or(0, Vec::len),
            p.visible.get("direct").map_or(0, Vec::len),
            p.apply.get("queue").map_or(0, Vec::len),
        )
    };
    let events: Vec<overlay_discovery::owed_event::OwedEvent> = (0..4)
        .map(|i| {
            serde_json::from_value(json!({"v": 1, "seq": 100 + i, "source": "backfill",
                "sourceKey": format!("owed-v2-{i}"), "facts": null, "atMs": NOW_MS}))
            .unwrap()
        })
        .collect();
    let before = direct_samples();
    let port = Fold16Sampled(RefCell::new(Vec::new()));
    let r =
        crate::owed_events::direct_with_class(&port, events.clone(), Some(TIP), SWEEP_CLASS).await;
    assert_eq!(r.calls, 1, "{r:?}");
    assert!(port.0.borrow().iter().all(
        |b| b["class"] == SWEEP_CLASS && !samples_bars(b["atMs"].as_u64(), b["class"].as_str())
    ));
    assert_eq!(
        direct_samples(),
        before,
        "the sweep's pass sampled into the bars"
    );
    let writer = Fold16Sampled(RefCell::new(Vec::new()));
    crate::owed_events::direct_with(&writer, events, Some(TIP)).await;
    assert_eq!(
        direct_samples().0,
        before.0 + 1,
        "a writer's direct pass still samples"
    );
    assert!(writer.0.borrow().iter().all(|b| b["class"] == "direct"));
    // The wiring: the reconcile's call, the object's two notes, the sweep's pass.
    let judge: String = include_str!("../owed_judge.rs")
        .split_whitespace()
        .collect();
    assert!(judge.contains(
        "ifkind==\"reconcile\"{crate::owed_actor::reconcile_apply_body(identity,games,kind,tip)"
    ));
    let object: String = include_str!("object.rs").split_whitespace().collect();
    assert!(object.contains("letsamples=samples_bars(body.at_ms,body.class.as_deref());ifsamples{letmutbars=self.bars.borrow_mut();"));
    assert!(object.contains("ifsamples{note_answer_as(bars_class(body.class.as_deref(),body.concurrent),&answer);"));
    let events_src: String = include_str!("../owed_events.rs")
        .split_whitespace()
        .collect();
    assert!(events_src.contains(
        "direct_events_as(env,report.sent.clone(),crate::owed_actor::SWEEP_CLASS).await;"
    ));
}

/// The row of bsv-low #963's line: beta's `b084427d…:0` under Phase B, `hop-sweep` `internalize` waiting on its
/// block, the two judges' facts equal save `spenderPresence` (given verbatim by the caller).
fn fix963_row(presence: Value) -> Value {
    json!({"outpoint": "b084427d:0", "family": "payout", "gameId": "024866f7", "sats": 20190,
        "opponentIdentity": null, "atHeight": null, "reason": "the sweep is seen, awaiting the block",
        "claimable": 0, "probeTip": 973_200,
        "facts": json!({"outcome": "hop-sweep", "claim": "internalize", "claimable": false, "chainWait": "block",
            "claimReason": "the sweep is seen, awaiting the block", "payTxid": "ab12", "marker": "cd34",
            "hopSats": 20190, "tip": 973_200, "spenderPresence": presence}).to_string()})
}

/// (fix963_a) bsv-low #963: A PRESENCE'S OBSERVATION STAMP IS NOT A JUDGEMENT. Two rows equal save
/// `facts.spenderPresence.at` are equal to every compare: `diff_rows` (the per-event compare and the reconcile's
/// `disagree`), both digests, and the operator route's `diff`. RED on `3159bc90e`: the outpoint is one diff.
#[test]
fn fix963_a_rows_equal_save_the_presence_stamp_compare_equal() {
    let p = |at: i64| json!({"word": "spent", "tier": "pending", "source": "spent-any", "at": at});
    let (d1, actor) = (
        fix963_row(p(1_793_288_680_506)),
        fix963_row(p(1_793_288_679_526)),
    );
    assert_eq!(
        diff_rows(std::slice::from_ref(&d1), std::slice::from_ref(&actor)),
        Vec::<String>::new()
    );
    assert_eq!(
        rows_digest(std::slice::from_ref(&d1)),
        rows_digest(std::slice::from_ref(&actor))
    );
    let view = rows_view(
        "03c31ebc",
        Ok(json!({"rows": [actor.clone()], "bootstrapped": true})),
        std::slice::from_ref(&d1),
        None,
        None,
    );
    assert_eq!(view["diff"], json!([]));
    assert_eq!(view["tipLag"], json!([]));
    assert_eq!(view["digests"]["d1"], view["digests"]["actor"]);
    // the served rows keep the stamp: only the compare drops it
    assert_eq!(view["d1"]["rows"][0], d1);
    assert!(STAMP_FACTS.contains(&("spenderPresence", "at")));
}

/// (fix963_b) bsv-low #963: the presence is still compared by `{word, tier, source}`: a change of any of them is a
/// diff (the judgement), stamps equal or not.
#[test]
fn fix963_b_the_presence_word_tier_and_source_still_differ() {
    let p = |word: &str, tier: &str, source: &str, at: i64| json!({"word": word, "tier": tier, "source": source, "at": at});
    let d1 = fix963_row(p("spent", "pending", "spent-any", 1));
    for other in [
        p("spent-outside", "pending", "spent-any", 1),
        p("spent", "confirmed", "spent-any", 2),
        p("spent", "pending", "courier", 2),
    ] {
        let actor = fix963_row(other.clone());
        assert_eq!(
            diff_rows(std::slice::from_ref(&d1), std::slice::from_ref(&actor)),
            vec!["b084427d:0".to_string()],
            "{other}"
        );
        assert_ne!(
            rows_digest(std::slice::from_ref(&d1)),
            rows_digest(std::slice::from_ref(&actor))
        );
    }
    // a presence on one side only is a diff (no stamp to drop makes it equal)
    let mut none = fix963_row(Value::Null);
    let mut f: Value = serde_json::from_str(none["facts"].as_str().unwrap()).unwrap();
    f.as_object_mut().unwrap().remove("spenderPresence");
    none["facts"] = json!(f.to_string());
    assert_eq!(
        diff_rows(std::slice::from_ref(&d1), &[none]),
        vec!["b084427d:0".to_string()]
    );
}

/// (fix963_c) bsv-low #963's line, both `spenderPresence` objects verbatim from beta's log at 2026-10-29 15:44:46Z:
/// classified EQUAL (no diff, no tip lag, no detail named). RED on `3159bc90e`: `diff_rows` names `b084427d:0` and
/// `diff_detail` names `facts.spenderPresence`.
#[test]
fn fix963_c_the_line_of_963_is_equal() {
    let d1p: Value = serde_json::from_str(
        r#"{"at":1793288680506,"source":"spent-any","tier":"pending","word":"spent"}"#,
    )
    .unwrap();
    let actorp: Value = serde_json::from_str(
        r#"{"at":1793288679526,"source":"spent-any","tier":"pending","word":"spent"}"#,
    )
    .unwrap();
    let (d1, actor) = (fix963_row(d1p), fix963_row(actorp));
    let (d1, actor) = (std::slice::from_ref(&d1), std::slice::from_ref(&actor));
    assert_eq!(diff_rows(d1, actor), Vec::<String>::new());
    assert_eq!(tip_lag_rows(d1, actor), Vec::<String>::new());
    assert_eq!(tip_derived_rows(d1, actor), Vec::<String>::new());
    let detail = diff_detail(d1, actor, &["b084427d:0".to_string()]);
    assert!(!detail.contains("spenderPresence"), "{detail}");
}

// ---- FIX-962 (bsv-low #962): a bootstrap page in flight is never a dead alarm; the cold bar at the real round trip ----

/// fix962_a: A PAGE IN FLIGHT IS LIVE. A Durable Object's `getAlarm()` answers null while its alarm handler runs until
/// the handler sets the next alarm, so a held event (or the lever) landing during a page read `None` and re-armed a live
/// page (beta's three `armed under a dead alarm (None)` lines in one cold bootstrap). The verdict reads the page's stamp:
/// a stamp younger than the wall with no alarm is live (no re-arm, no count, no line); a stamp past the wall with no
/// alarm, or no stamp, is the dead class as before (FOLD-7's item 12 kept); a live alarm is live with or without a
/// stamp. The object stamps the page in the alarm's bootstrap branch, clears it when the branch ends, and
/// `arm_bootstrap` (the held event's road and the lever's alike) asks this verdict with the stamp.
/// RED on `3159bc90e`'s rule (the verdict `alarm_is_dead(held, now)`, the stamp unread): `a page in flight is live`.
#[test]
fn fix962_a_a_page_in_flight_is_never_a_dead_alarm() {
    let started = NOW_MS - 2_000;
    assert!(
        !boot_alarm_dead(None, Some(started), NOW_MS),
        "a page in flight is live"
    );
    assert!(!boot_alarm_dead(
        None,
        Some(NOW_MS - BOOT_PAGE_WALL_MS + 1),
        NOW_MS
    ));
    // Past the wall with no alarm: the dead class (re-armed, counted, logged).
    assert!(boot_alarm_dead(
        None,
        Some(NOW_MS - BOOT_PAGE_WALL_MS),
        NOW_MS
    ));
    assert!(boot_alarm_dead(None, None, NOW_MS));
    // The old build's alarm decades out with no page running is dead; a live alarm is live.
    assert!(boot_alarm_dead(
        Some((NOW_MS + NOW_MS) as i64),
        None,
        NOW_MS
    ));
    assert!(!boot_alarm_dead(Some((NOW_MS + 500) as i64), None, NOW_MS));
    assert!(page_in_flight(Some(NOW_MS), NOW_MS));
    assert!(!page_in_flight(None, NOW_MS));
    assert_eq!(BOOT_PAGE_WALL_MS, BOOT_ALARM_DEAD_AFTER_MS);
    // The object: the stamp set before the page and cleared after its branch; `arm_bootstrap` reads it (one road for
    // the held event and the lever).
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let object = squash(include_str!("object.rs"));
    let alarm = &object[object.find("asyncfnalarm(&self)").unwrap()..];
    let stamp = alarm
        .find("self.put_meta(&local,META_PAGE_STARTED,")
        .expect("the alarm stamps the page");
    let page = alarm.find("self.bootstrap_page(&local)").unwrap();
    let clear = alarm
        .find("DELETEFROMmetaWHEREname='pageStartedAtMs'")
        .expect("the branch clears the stamp");
    assert!(stamp < page && page < clear);
    assert!(
        clear < alarm.find("META_KV_DIRTY").unwrap(),
        "inside the bootstrap branch"
    );
    let arm = &object[object.find("asyncfnarm_bootstrap(").unwrap()..];
    let arm = &arm[..arm.find("asyncfnarm_tip_alarm(").unwrap()];
    assert!(arm.contains("self.meta(META_PAGE_STARTED)"));
    assert!(arm.contains(
        "if!boot_alarm_dead(held,started,Date::now().as_millis()){returnArmed::default();}"
    ));
    assert!(
        !arm.contains("alarm_is_dead("),
        "no road reads the alarm alone"
    );
    assert_eq!(
        object.matches("lever:bool").count(),
        1,
        "one arm for both roads"
    );
}

/// fix962_b: THE COLD BAR AT THE REAL ROUND TRIP. One bootstrap page of FOLD-7's 600-game seat on a fresh mirror is 32
/// games in two record calls of one statement each (FOLD-14), the object's budget take the third (one statement before
/// the step): [`COLD_PAGE_D1_CALLS`]. At beta's round trip (700 ms) the model lands the seat under 60 s; under
/// `3159bc90e`'s constants (16 games a page, 1,000 ms apart) it does not (116,800 ms before native work, the
/// rehearsal's 94,789 ms). RED on `3159bc90e` (16, 1,000): `left: 16 right: 32` and the model 116,800 ms.
#[tokio::test]
async fn fix962_b_a_cold_600_game_seat_lands_under_60s_at_the_real_round_trip() {
    let record = seeded(&fold7_chained_seat(600));
    let paged = mirror();
    let turn = futures_util::lock::Mutex::new(());
    let (b0, s0) = (record.batches.get(), record.stmts.get());
    let step = bootstrap_step(
        &turn,
        &record,
        &paged,
        &Fakes,
        "owner",
        Some(object::BOOT_CURSOR_START),
        Some(TIP),
        None,
        false,
    )
    .await
    .unwrap();
    let (calls, stmts) = (record.batches.get() - b0, record.stmts.get() - s0);
    let BootStep::Page { games, .. } = step else {
        panic!("{step:?}")
    };
    eprintln!("fix962_b: one page of {games} games: {calls} record calls, {stmts} statements (+1 the budget take)");
    assert_eq!(games, 32);
    assert_eq!(BOOTSTRAP_PAGE_GAMES, 32);
    assert_eq!(
        calls as u64 + 1,
        COLD_PAGE_D1_CALLS,
        "the page's rounds and its take"
    );
    assert_eq!(stmts, calls, "one statement a call");
    // The model at the real round trip.
    let now = cold_bootstrap_ms(
        600,
        BOOTSTRAP_PAGE_GAMES as u64,
        BOOTSTRAP_PAGE_DELAY_MS,
        COLD_D1_ROUND_TRIP_MS,
        0,
    );
    assert_eq!(now, 19 * 2_100 + 18 * 500);
    assert!(
        now + 19 * COLD_PAGE_WORK_BAR_MS <= 60_000,
        "{now} ms and the native bar"
    );
    assert_eq!(
        cold_bootstrap_ms(600, 16, 1_000, COLD_D1_ROUND_TRIP_MS, 0),
        116_800,
        "the base's cadence"
    );
    assert_eq!(BOOTSTRAP_PAGE_DELAY_MS, 500);
    // The page's take is ONE statement before the step (the third call).
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let object = squash(include_str!("object.rs"));
    let page = &object[object.find("asyncfnbootstrap_page(").unwrap()..];
    let page = &page[..page.find("bootstrap_step(").unwrap()];
    assert_eq!(page.matches(".run_batch(").count(), 1);
    // The fleet's budget is unchanged.
    assert_eq!(BOOTSTRAP_PAGES_PER_MINUTE, 60);
    assert_eq!(BOOTSTRAP_REJUDGE_PAGES_PER_MINUTE, 30);
}

/// fix962_c: THE CADENCE IS READ FROM THE LOGS AND THE BARS. One line a page (games, its work, its wait since the last
/// page, the cursor it leaves) and the cold page's work into the fleet's `cold` row's `page` ring, served
/// `classes.cold.pageMsP95` beside `visibleMsP95`; a flush holding only page samples writes the cold row. New API
/// (`page_line`, `note_cold_page`, `COLD_PAGE_RING`): none on `3159bc90e`.
#[tokio::test]
async fn fix962_c_the_page_line_and_the_cold_page_bar() {
    let id = format!("03{}", "c2".repeat(32));
    assert_eq!(
        page_line(&id, 32, 1_480, Some(512), "[1790000000,\"g\"]"),
        format!("[owed-actor] page {id}: games=32 ms=1480 waitMs=512 cursor=[1790000000,\"g\"]")
    );
    assert!(page_line(&id, 0, 9, None, "drain").ends_with("waitMs=- cursor=drain"));
    let db = fold8_empty();
    let mut pending = PendingBars::default();
    for ms in [1_400, 1_500, 2_100] {
        pending.note_cold_page(ms);
    }
    assert!(pending.due());
    flush_bars_from(&db, NOW_MS, &pending).await.unwrap();
    let rows = db
        .run_batch(vec![Stmt::new(BARS_READ_SQL, vec![])])
        .await
        .unwrap()
        .remove(0)
        .rows;
    let served = fleet_bars(&rows, NOW_MS, lib_rev());
    let cold = &served["classes"]["cold"];
    assert_eq!(cold["pageSamples"], json!(3), "{cold}");
    assert_eq!(cold["pageMsP95"], json!(2_100));
    assert_eq!(cold["pageMsMax"], json!(2_100));
    assert!(cold.get("visibleMsP95").is_some());
    assert!(served["classes"]["direct"].get("pageMsP95").is_none());
    // The object: the line and the sample at every page that ran (a cold one's sample alone), flushed at the end.
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let object = squash(include_str!("object.rs"));
    let page = &object[object.find("asyncfnbootstrap_page(").unwrap()..];
    let page = &page[..page.find("asyncfnwrite_kv_copy(").unwrap()];
    assert!(page.contains("console_log!(\"{}\",page_line(&me,page_games,page_ms,wait,&left));"));
    assert!(page.contains("if!rejudge{note_cold_page(page_ms);}"));
    assert!(page.contains("if!rejudge{flush_bars(&record,now,true).await;}"));
}

// ---- FIX-964 (bsv-low #964): under a queue backlog a stale actor row is held by LAG, never a judge disagreement ----

/// The D1 judge's stored rows of `identity` (the compare's own read) and the first row's outpoint and game.
fn fix964_rows(d1: &SqliteSql, identity: &str) -> Vec<Value> {
    let mut q = d1
        .conn
        .prepare("SELECT json_object('identity',identity,'outpoint',outpoint,'family',family,'gameId',gameId,'sats',sats,'opponentIdentity',opponentIdentity,'atHeight',atHeight,'facts',facts,'reason',reason,'judgedAtMs',judgedAtMs,'claimable',claimable,'probeTip',probeTip,'gateHeight',gateHeight,'beltDueAtMs',beltDueAtMs) FROM owed_rows WHERE identity=?1 ORDER BY outpoint")
        .unwrap();
    q.query_map([identity], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|s| serde_json::from_str(&s.unwrap()).unwrap())
        .collect()
}
/// A pending mark of `game`, written at `at_ms` (before the compare), its generation answered.
fn fix964_mark(d1: &SqliteSql, game: &str, key: &str, at_ms: i64) -> i64 {
    d1.conn
        .execute(
            "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('owner',?1,'pot',?2,?3)",
            rusqlite::params![game, key, at_ms],
        )
        .unwrap();
    d1.conn.last_insert_rowid()
}
/// The fixture judged in D1, its `owner` rows, and an actor answer whose first row is STALE (the same outpoint, an
/// older judgement): `(d1, games, the stale outpoint's game, the answer)`.
async fn fix964_stale() -> (SqliteSql, Vec<String>, String, Value, String) {
    let d1 = d1_judge(&stranded()).await;
    d1.conn.execute("DELETE FROM owed_dirty", []).unwrap();
    let games = games_of(&d1, "owner");
    let mut actor = fix964_rows(&d1, "owner");
    assert!(!actor.is_empty(), "the fixture holds rows of owner");
    let outpoint = actor[0]["outpoint"].as_str().unwrap().to_string();
    let game = actor[0]["gameId"].as_str().unwrap().to_string();
    actor[0]["family"] = json!("unbound");
    actor[0]["claimable"] = json!(0);
    actor[0]["reason"] = json!("the spend is not classified yet (no verdict)");
    (d1, games, game, json!({ "rows": actor }), outpoint)
}
fn fix964_lines(d1: &SqliteSql) -> Vec<String> {
    d1.lines
        .borrow()
        .iter()
        .filter(|l| l.contains("DIFF"))
        .cloned()
        .collect()
}

/// fix964_a: a mark queued on the game BEFORE the compare and not consumed by the delivery (the backlog's mark, at or
/// below the acknowledgement bound, still pending) holds the stale actor row by LAG: `owed_actor_compare_held_total`
/// +1, `owed_actor_diffs_total` flat, the line `DIFF (held: lag <age> ms)` with the oldest unapplied mark's age.
/// RED on 149869bf's rule (only a mark above the bound is held; grafted: `held_partition` answering no lag):
/// `owed_actor_diffs_total` 0 -> 1.
#[tokio::test]
async fn fix964_a_a_queued_mark_older_than_the_compare_holds_by_lag() {
    let (d1, games, game, answer, _) = fix964_stale().await;
    d1.now.set(1_000_000);
    let gen = fix964_mark(&d1, &game, "backlog:0", 400_000);
    let (diffs, held) = (
        d1.counter("owed_actor_diffs_total"),
        d1.counter("owed_actor_compare_held_total"),
    );
    super::compare(&d1, "owner", &games, &answer, Some(gen)).await;
    let lines = fix964_lines(&d1);
    assert_eq!(
        d1.counter("owed_actor_diffs_total"),
        diffs,
        "a stale row under a queued mark is not a STOP: {lines:?}"
    );
    assert_eq!(d1.counter("owed_actor_compare_held_total"), held + 1);
    assert!(
        lines
            .iter()
            .any(|l| l.contains("DIFF (held: lag 600000 ms")),
        "{lines:?}"
    );
    // With no bound (its read faulted) every pending mark is lag: held, never a STOP.
    super::compare(&d1, "owner", &games, &answer, None).await;
    assert_eq!(d1.counter("owed_actor_diffs_total"), diffs);
    assert_eq!(d1.counter("owed_actor_compare_held_total"), held + 2);
    // FOLD-16's class stands: a mark written while the two judged (above the bound) is NEW2-L1's line.
    let above = fix964_mark(&d1, &game, "while:0", 999_999);
    super::compare(&d1, "owner", &games, &answer, Some(above - 1)).await;
    assert_eq!(d1.counter("owed_actor_diffs_total"), diffs);
    assert_eq!(d1.counter("owed_actor_compare_held_total"), held + 3);
    assert!(fix964_lines(&d1)
        .iter()
        .any(|l| l.contains("held mark") && l.contains("NEW2-L1")));
}

/// fix964_b: NO unapplied mark of the game and two outputs: the judge disagreement `owed_actor_diffs_total` counts
/// (the cutover's STOP), never held; a mark on ANOTHER game holds nothing; the rows equal count nothing. A control,
/// green on 149869bf by construction; RED against the compare holding every difference by lag:
/// `owed_actor_diffs_total` 0 -> 0 where 1 was wanted.
#[tokio::test]
async fn fix964_b_no_unapplied_mark_and_two_outputs_is_a_diff() {
    let (d1, games, game, answer, _) = fix964_stale().await;
    let other = games.iter().find(|g| **g != game).cloned();
    if let Some(o) = &other {
        fix964_mark(&d1, o, "other:0", 1);
    }
    let (diffs, held) = (
        d1.counter("owed_actor_diffs_total"),
        d1.counter("owed_actor_compare_held_total"),
    );
    super::compare(&d1, "owner", &games, &answer, Some(i64::MAX / 2)).await;
    let lines = fix964_lines(&d1);
    assert_eq!(d1.counter("owed_actor_diffs_total"), diffs + 1, "{lines:?}");
    assert_eq!(d1.counter("owed_actor_compare_held_total"), held);
    assert!(lines
        .iter()
        .any(|l| l.starts_with("[owed-actor] DIFF ") && !l.contains("held")));
    // The same rows on both sides: nothing counted.
    let same = json!({ "rows": fix964_rows(&d1, "owner") });
    super::compare(&d1, "owner", &games, &same, Some(0)).await;
    assert_eq!(d1.counter("owed_actor_diffs_total"), diffs + 1);
    assert_eq!(d1.counter("owed_actor_compare_held_total"), held);
}

/// fix964_c: #964's sampled line. D1's row is the consumer's judgement WITH the pot's verdict (a payout, outcome
/// refund from the chain, claimable, `claim: internalize`); the actor's is the direct apply at the earlier pointer's
/// write (unbound, outcome unresolved, "the spend is not classified yet (no verdict)"); the verdict's mark is still
/// queued (written before the compare, not consumed). Held by lag, `owed_actor_diffs_total` flat; and the operator's
/// rows lever keeps the outpoint in `diff` and names it in `heldByLag`. RED on 149869bf's rule (grafted as in
/// fix964_a): `owed_actor_diffs_total` 0 -> 1; the lever's `heldByLag` absent.
#[tokio::test]
async fn fix964_c_the_issues_line_with_the_verdicts_mark_queued_is_held() {
    let (d1, games, game, mut answer, outpoint) = fix964_stale().await;
    let facts = json!({"outcome": "refund", "outcomeSource": "chain", "claim": "internalize", "payTxid": "cb59934f00000000000000000000000000000000000000000000000000000000"}).to_string();
    d1.conn
        .execute(
            "UPDATE owed_rows SET family='payout',claimable=1,reason=NULL,facts=?2 WHERE identity='owner' AND outpoint=?1",
            rusqlite::params![outpoint, facts],
        )
        .unwrap();
    answer["rows"][0]["facts"] =
        json!(json!({"outcome": "unresolved", "claimable": false}).to_string());
    d1.now.set(2_000_000);
    let gen = fix964_mark(&d1, &game, &format!("{outpoint}:verdict"), 1_000_000);
    let (diffs, held) = (
        d1.counter("owed_actor_diffs_total"),
        d1.counter("owed_actor_compare_held_total"),
    );
    super::compare(&d1, "owner", &games, &answer, Some(gen)).await;
    let lines = fix964_lines(&d1);
    assert_eq!(d1.counter("owed_actor_diffs_total"), diffs, "{lines:?}");
    assert_eq!(d1.counter("owed_actor_compare_held_total"), held + 1);
    assert!(
        lines
            .iter()
            .any(|l| l.contains("DIFF (held: lag 1000000 ms") && l.contains(&outpoint)),
        "{lines:?}"
    );
    // The lever: the shipped read of the pending marks, then the pure naming.
    let pending = d1
        .run_batch(vec![Stmt::new(
            LEVER_PENDING_SQL,
            vec![Bind::Text("owner".into())],
        )])
        .await
        .unwrap()
        .remove(0)
        .rows;
    let view = rows_view(
        "owner",
        Ok(answer.clone()),
        &fix964_rows(&d1, "owner"),
        None,
        None,
    );
    assert!(view["diff"]
        .as_array()
        .unwrap()
        .iter()
        .any(|o| o == outpoint.as_str()));
    assert_eq!(held_by_lag(&view, &pending), vec![outpoint.clone()]);
    // With the mark consumed the same difference is no lag.
    d1.conn.execute("DELETE FROM owed_dirty", []).unwrap();
    assert!(held_by_lag(&view, &[]).is_empty());
    let lever = include_str!("object.rs")
        .split_whitespace()
        .collect::<String>();
    assert!(lever.contains("view[\"heldByLag\"]=json!(held_by_lag(&view,&pending));"));
}

// ---------------------------------------------------------------------------------------------------------------
// FIX-968 (bsv-low #968, Wave E row E3): THE D1-MARK READ GATE named and decided (`owed_read::Snapshot::
// withholding_mark`). L53 S1: in Phase B the actor-served read withheld a press for as long as the serial consumer
// lagged (36 passes of "re-judging: a downgrade of this game is recorded and not yet judged" over a row the
// writer's direct call had already re-judged in the actor). The rule now: a press is withheld only by a DOWNGRADE
// mark the served judge has not applied; a plain mark never withholds; a downgrade mark at or below the actor's
// covered generation (its last apply read the record after the mark) withholds nothing. Each pin uses API this lane
// adds (`Snapshot::covered`, `withholding_mark`, `COVERED_BY_GAME_SQL`, `covered_of`) and does not compile on
// 149869bf; `fix968_b`'s covered leg is the behaviour RED there (the press withheld with the mark pending).
// ---------------------------------------------------------------------------------------------------------------

/// The actor-served read as `object::read_snapshot` builds it, natively: D1's state and pending marks
/// (`owed_read::read_with(.., false)`), the actor's rows, and the actor's covered generations of the marked games
/// through the shipped `COVERED_BY_GAME_SQL` and `covered_of`.
fn fix968_served_by_actor(w: &WithActor) -> Value {
    let mut snapshot =
        futures_executor_block(crate::owed_read::read_with(&w.d1, "owner", false)).unwrap();
    snapshot.joined = fix968_rows(&w.actor, ROWS_SQL, vec![Bind::Text("owner".into())]);
    let marked: Vec<String> = snapshot
        .downgrades
        .keys()
        .map(|g| g.to_ascii_lowercase())
        .take(COVERED_ASK_MAX)
        .collect();
    let covered = if marked.is_empty() {
        Vec::new()
    } else {
        fix968_rows(
            &w.actor,
            COVERED_BY_GAME_SQL,
            vec![Bind::Text(json!(marked).to_string())],
        )
    };
    snapshot.covered = covered_of(&json!({ "covered": covered }));
    snapshot.body("owner", Some(TIP), &BTreeMap::new()).unwrap()
}

/// The D1-served read (`OWED_ACTOR_READ` off): D1's rows, the same gate over an empty `covered`.
fn fix968_served_by_d1(w: &WithActor) -> Value {
    let snapshot = futures_executor_block(crate::owed_read::read(&w.d1, "owner")).unwrap();
    snapshot.body("owner", Some(TIP), &BTreeMap::new()).unwrap()
}

fn fix968_rows(db: &SqliteSql, sql: &str, binds: Vec<Bind>) -> Vec<Value> {
    futures_executor_block(db.run_batch(vec![Stmt::new(sql, binds)]))
        .unwrap()
        .remove(0)
        .rows
}

fn fix968_facts<'a>(body: &'a Value, outpoint: &str) -> &'a Value {
    fix968_facts_opt(body, outpoint).unwrap_or_else(|| panic!("no row {outpoint} in {body}"))
}

/// A row the judgement may have taken out of the list (a hop a stranger spent).
fn fix968_facts_opt<'a>(body: &'a Value, outpoint: &str) -> Option<&'a Value> {
    body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["outpoint"] == outpoint)
        .map(|r| &r["facts"])
}

fn fix968_mark(w: &WithActor, game: &str, source: &str, key: &str, downgrade: i64) -> i64 {
    w.d1.conn
        .execute(
            "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs,downgrade) VALUES ('owner',?1,?2,?3,?4,?5)",
            rusqlite::params![game, source, key, w.d1.now_ms() as i64, downgrade],
        )
        .unwrap();
    w.d1.conn.last_insert_rowid()
}

fn fix968_pending(w: &WithActor) -> i64 {
    w.d1.conn
        .query_row(
            "SELECT COUNT(*) FROM owed_dirty WHERE identity='owner'",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

/// The writer's DIRECT call (D2-ACTOR-D): the actor applies the game from the record as it stands and records its
/// coverage; in Phase B it acknowledges nothing (the consumer's D1 judge does), in Phase C a walled caller does not
/// either (FIX-956 item 3 acknowledges only after an answer it received).
async fn fix968_direct(w: &WithActor, games: &[String], seq: i64) {
    let (report, _) = apply(
        &w.d1,
        &w.actor,
        &Fakes,
        "owner",
        games,
        Some(("pot", seq)),
        Some(TIP),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        report.applied_games.len(),
        games.len(),
        "the direct call applied the games"
    );
}

const FIX968_PRESS: &str = "b300000000000000000000000000000000000000000000000000000000000000:0";

async fn fix968_phase(d1_judges: bool) -> WithActor {
    WithActor {
        d1: d1_judge(&stranded()).await,
        actor: mirror(),
        d1_judges,
        event: None,
        actor_reads: true,
        actor_fails: Cell::new(false),
    }
}

/// (a) A pending NON-downgrade mark never withholds the actor-served press: a party filing's, a tip's and an age
/// mark on the press's game are counted in `pending` and change no row; the gate names none of them.
#[tokio::test]
async fn fix968_a_a_pending_non_downgrade_mark_never_withholds_the_actor_served_press() {
    let w = fix968_phase(true).await;
    let games = games_of(&w.d1, "owner");
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    let body = fix968_served_by_actor(&w);
    assert_eq!(
        fix968_facts(&body, FIX968_PRESS)["claimable"],
        true,
        "{body}"
    );
    let game = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["outpoint"] == FIX968_PRESS)
        .unwrap()["gameId"]
        .as_str()
        .unwrap()
        .to_string();
    for (source, key) in [
        ("party", "p1"),
        ("tip", "gate"),
        ("age", "age"),
        ("sweep", "s1"),
    ] {
        fix968_mark(&w, &game, source, key, 0);
    }
    assert_eq!(fix968_pending(&w), 4);
    let snapshot = crate::owed_read::read_with(&w.d1, "owner", false)
        .await
        .unwrap();
    assert_eq!(snapshot.pending, 4);
    assert!(snapshot.downgrades.is_empty(), "{:?}", snapshot.downgrades);
    assert_eq!(
        snapshot.withholding_mark(&game),
        None,
        "the gate names no plain mark"
    );
    let body = fix968_served_by_actor(&w);
    let facts = fix968_facts(&body, FIX968_PRESS);
    assert_eq!(
        facts["claimable"], true,
        "a plain mark delays no press: {facts}"
    );
    assert!(facts.get("refresh").is_none(), "{facts}");
    assert_eq!(
        body["pending"], 4,
        "the count still says what is being re-judged: {body}"
    );
    // the D1-served read reads the same rule
    let facts = fix968_facts(&fix968_served_by_d1(&w), FIX968_PRESS).clone();
    assert_eq!(facts["claimable"], true, "{facts}");
}

/// (b) THE DECISION, Phase B. A pending DOWNGRADE mark the actor has NOT applied withholds the press with its
/// reason (as D2-L1 built it); the same mark once the writer's direct call applied the actor (its covered generation
/// at or past the mark's) withholds nothing although the consumer has not acknowledged it (RED on 149869bf: the
/// press withheld, `claimable: false`, `refresh: "pending"`); a downgrade mark written AFTER that apply withholds
/// again; the consumer's delivery (actor, then the D1 judge's acknowledgement) lifts it. The D1-served read keeps
/// withholding on every pending downgrade mark: its judge has not judged it. A REAL competitor applied directly is
/// served as the actor judged it (no press, no `refresh`): covered means the row IS the record's judgement.
#[tokio::test]
async fn fix968_b_a_downgrade_mark_the_actor_applied_serves_the_press_one_it_has_not_withholds() {
    let w = fix968_phase(true).await;
    let games = games_of(&w.d1, "owner");
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    assert_eq!(fix968_pending(&w), 0);
    let body = fix968_served_by_actor(&w);
    assert_eq!(
        fix968_facts(&body, FIX968_PRESS)["claimable"],
        true,
        "{body}"
    );
    let game = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["outpoint"] == FIX968_PRESS)
        .unwrap()["gameId"]
        .as_str()
        .unwrap()
        .to_string();

    // 1. a downgrade-class mark the actor has not applied (the admission's class, D4-L1): withheld, as today
    let g1 = fix968_mark(&w, &game, "pot", "aa00:0", 1);
    let snapshot = crate::owed_read::read_with(&w.d1, "owner", false)
        .await
        .unwrap();
    assert_eq!(
        snapshot.downgrades.get(&game),
        Some(&vec![(g1, "pot".to_string())]),
        "PENDING_SQL carries the mark's generation"
    );
    let facts = fix968_facts(&fix968_served_by_actor(&w), FIX968_PRESS).clone();
    assert_eq!(facts["claimable"], false, "uncovered: withheld: {facts}");
    assert_eq!(facts["refresh"], "pending");
    assert_eq!(facts["pendingSource"], "pot");
    assert_eq!(facts["claimableBeforePending"], true);
    assert_eq!(
        facts["claimReason"],
        crate::owed_read::PENDING_DOWNGRADE_REASON
    );

    // 2. the writer's direct call applies the actor; Phase B's direct call acknowledges nothing, the mark stays
    fix968_direct(&w, std::slice::from_ref(&game), 7).await;
    assert_eq!(
        fix968_pending(&w),
        1,
        "the consumer has not acknowledged the mark"
    );
    let covered = covered_of(
        &json!({"covered": fix968_rows(&w.actor, COVERED_BY_GAME_SQL, vec![Bind::Text(json!([game.to_ascii_lowercase()]).to_string())])}),
    );
    assert!(
        covered
            .get(&game.to_ascii_lowercase())
            .is_some_and(|c| *c >= g1),
        "the apply read the record after the mark: {covered:?} against {g1}"
    );
    let body = fix968_served_by_actor(&w);
    let facts = fix968_facts(&body, FIX968_PRESS);
    assert_eq!(
        facts["claimable"], true,
        "a downgrade mark the actor applied withholds nothing (RED on 149869bf): {facts}"
    );
    assert!(facts.get("refresh").is_none(), "{facts}");
    assert_eq!(
        body["pending"], 1,
        "the count is still the consumer's backlog: {body}"
    );
    // the D1-served read: its judge has not judged the mark, so it withholds (`covered` is empty there)
    let d1 = fix968_facts(&fix968_served_by_d1(&w), FIX968_PRESS).clone();
    assert_eq!(
        d1["claimable"], false,
        "the D1 read withholds on every pending downgrade: {d1}"
    );
    assert_eq!(d1["pendingSource"], "pot");

    // 3. a downgrade mark written AFTER the apply: above the covered generation, withheld again
    let g2 = fix968_mark(&w, &game, "eviction", "dd00", 1);
    assert!(g2 > g1);
    let facts = fix968_facts(&fix968_served_by_actor(&w), FIX968_PRESS).clone();
    assert_eq!(
        facts["claimable"], false,
        "a mark past the apply withholds: {facts}"
    );
    assert_eq!(
        facts["pendingSource"], "eviction",
        "the newest uncovered mark's source: {facts}"
    );
    assert_eq!(
        facts["claimReason"],
        crate::owed_read::PENDING_DOWNGRADE_REASON
    );

    // 4. the consumer's delivery: the actor applies (covering g2), the D1 judge acknowledges both marks
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    assert_eq!(fix968_pending(&w), 0);
    let facts = fix968_facts(&fix968_served_by_actor(&w), FIX968_PRESS).clone();
    assert_eq!(facts["claimable"], true, "{facts}");
    assert!(facts.get("refresh").is_none());

    // 5. a REAL competitor (the trigger's downgrade mark), applied directly: served as judged, no press, no refresh
    w.d1.conn.execute_batch(SPENT_BY_A_STRANGER).unwrap();
    assert_eq!(
        w.d1.conn
            .query_row(
                "SELECT COUNT(*) FROM owed_dirty WHERE identity='owner' AND downgrade=1",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1,
        "the index's spend is a downgrade-class mark"
    );
    let before = fix968_facts(&fix968_served_by_actor(&w), FIX968_PRESS).clone();
    assert_eq!(
        before["claimable"], false,
        "not yet applied: withheld: {before}"
    );
    assert_eq!(before["refresh"], "pending");
    fix968_direct(&w, std::slice::from_ref(&game), 8).await;
    assert!(fix968_pending(&w) >= 1, "still unacknowledged");
    let body = fix968_served_by_actor(&w);
    if let Some(facts) = fix968_facts_opt(&body, FIX968_PRESS) {
        assert_ne!(
            facts["claimable"], true,
            "the actor's own judgement downgraded it: {facts}"
        );
        assert!(
            facts.get("refresh").is_none(),
            "served as judged, not as re-judging: {facts}"
        );
    }
}

/// (c) PHASE C (`OWED_D1_JUDGE=false`), pinned apart. The triggers still write `owed_dirty` (they know no flag);
/// the consumer's `as_the_judge` acknowledges the applied games' marks; the actor-served read consults the marks
/// through the same gate: a downgrade mark the actor has not applied withholds (the actor unreachable), one it
/// applied by a direct call whose caller walled (no acknowledgement) withholds nothing, a plain mark never does;
/// and the D1-served read serves NO press at all once a Phase C acknowledgement stamped `owed-actor-judged`
/// (NEW-M3), the gate never reached.
#[tokio::test]
async fn fix968_c_phase_c_the_marks_are_written_acknowledged_and_read_through_the_one_gate() {
    let w = fix968_phase(false).await;
    let games = games_of(&w.d1, "owner");
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    assert_eq!(fix968_pending(&w), 0, "Phase C's acknowledgement");
    let body = fix968_served_by_actor(&w);
    assert_eq!(
        fix968_facts(&body, FIX968_PRESS)["claimable"],
        true,
        "{body}"
    );
    let game = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["outpoint"] == FIX968_PRESS)
        .unwrap()["gameId"]
        .as_str()
        .unwrap()
        .to_string();
    // NEW-M3: the D1 read serves no press after a Phase C acknowledgement, before the rollback's stamp
    let d1 = fix968_facts(&fix968_served_by_d1(&w), FIX968_PRESS).clone();
    assert_eq!(d1["claimable"], false, "{d1}");
    assert_eq!(d1["pendingSource"], "owed-v3");
    assert_eq!(d1["claimReason"], crate::owed_read::ROLLBACK_PENDING_REASON);

    // a plain mark: counted, no withhold
    fix968_mark(&w, &game, "party", "p1", 0);
    let body = fix968_served_by_actor(&w);
    assert_eq!(
        fix968_facts(&body, FIX968_PRESS)["claimable"],
        true,
        "{body}"
    );
    assert_eq!(body["pending"], 1);
    w.d1.conn.execute_batch("DELETE FROM owed_dirty").unwrap();

    // a downgrade mark the actor has not applied (the actor out of reach): withheld
    let g1 = fix968_mark(&w, &game, "pot", "aa00:0", 1);
    w.actor_fails.set(true);
    assert!(crate::owed_events::deliver(&w, "owner", &games)
        .await
        .is_err());
    assert_eq!(
        fix968_pending(&w),
        1,
        "a faulted delivery acknowledges nothing"
    );
    let facts = fix968_facts(&fix968_served_by_actor(&w), FIX968_PRESS).clone();
    assert_eq!(facts["claimable"], false, "{facts}");
    assert_eq!(
        facts["claimReason"],
        crate::owed_read::PENDING_DOWNGRADE_REASON
    );
    // the direct call lands in the actor, its caller walled: no acknowledgement, the mark covered
    w.actor_fails.set(false);
    fix968_direct(&w, std::slice::from_ref(&game), 9).await;
    assert_eq!(fix968_pending(&w), 1);
    let facts = fix968_facts(&fix968_served_by_actor(&w), FIX968_PRESS).clone();
    assert_eq!(
        facts["claimable"], true,
        "covered: the press stands in Phase C too: {facts}"
    );
    assert!(facts.get("refresh").is_none());
    let snapshot = crate::owed_read::read_with(&w.d1, "owner", false)
        .await
        .unwrap();
    assert_eq!(snapshot.downgrades.get(&game).map(Vec::len), Some(1));
    assert_eq!(
        snapshot.withholding_mark(&game),
        Some("pot"),
        "with no covered answer the gate withholds"
    );

    // the real competitor: the trigger writes its downgrade mark under Phase C too; the consumer acknowledges it
    w.d1.conn.execute_batch(SPENT_BY_A_STRANGER).unwrap();
    assert_eq!(
        w.d1.conn
            .query_row(
                "SELECT COUNT(*) FROM owed_dirty WHERE identity='owner' AND downgrade=1 AND generation>?1",
                [g1],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1,
        "the trigger knows no flag"
    );
    let facts = fix968_facts(&fix968_served_by_actor(&w), FIX968_PRESS).clone();
    assert_eq!(
        facts["claimable"], false,
        "the spend is not applied yet: withheld: {facts}"
    );
    crate::owed_events::deliver(&w, "owner", &games)
        .await
        .unwrap();
    assert_eq!(
        fix968_pending(&w),
        0,
        "as_the_judge acknowledged the applied games' marks"
    );
    let body = fix968_served_by_actor(&w);
    if let Some(facts) = fix968_facts_opt(&body, FIX968_PRESS) {
        assert_ne!(facts["claimable"], true, "{facts}");
        assert!(facts.get("refresh").is_none(), "{facts}");
    }
}

/// (d) The read's wiring: `read_snapshot` names the downgrade-marked games to the actor and keeps its covered
/// answer; `/rows` answers them from the shipped statement and writes nothing; the answer's parse.
#[test]
fn fix968_d_the_read_asks_the_actor_its_coverage_of_the_marked_games() {
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let object = squash(include_str!("object.rs"));
    let read = &object[object.find("pub(crate)asyncfnread_snapshot(").unwrap()..];
    let read = &read[..read.find("pubasyncfncall_bootstrap(").unwrap()];
    assert!(read.contains(&squash(
        "let marked: Vec<String> = snapshot.downgrades.keys().cloned().collect();"
    )));
    assert!(read.contains(&squash("call_rows(env, identity, READ_WALL_MS, &marked)")));
    assert!(read.contains(&squash("snapshot.covered = covered_of(&answer);")));
    let rows_route = &object[object.find("asyncfnrows_route(").unwrap()
        ..object.find("asyncfnbootstrap_page(").unwrap()];
    assert!(rows_route.contains("COVERED_BY_GAME_SQL"));
    assert!(rows_route.contains(&squash("\"covered\": covered,")));
    assert!(!rows_route.contains("put_meta") && !rows_route.contains("set_alarm"));
    let call = &object[object.find("pubasyncfncall_rows(").unwrap()..];
    assert!(call.contains(".take(COVERED_ASK_MAX)"));
    assert_eq!(
        covered_of(
            &json!({"covered": [{"game": "G1", "seq": 12}, {"game": "g2", "seq": 3.0}, {"game": 7}]})
        ),
        BTreeMap::from([("g1".to_string(), 12), ("g2".to_string(), 3)])
    );
    assert!(covered_of(&json!({"fresh": true})).is_empty());
    // the statement's shape on a mirror
    let m = mirror();
    m.conn
        .execute_batch("INSERT INTO applied(kind,seq,game,atMs) VALUES ('covered',5,'ga',1),('covered',9,'ga',2),('covered',4,'gb',3),('covered-tip',99,'ga',4)")
        .unwrap();
    let rows = fix968_rows(
        &m,
        COVERED_BY_GAME_SQL,
        vec![Bind::Text("[\"ga\",\"gc\"]".into())],
    );
    assert_eq!(
        covered_of(&json!({"covered": rows})),
        BTreeMap::from([("ga".to_string(), 9)])
    );
}

/// FIX-974 (bsv-low #974; L53's S3): THE MINED WORD IS READ OFF THE SHAPES THE PUBLISH SENDS. A `word` event carries
/// either the overlay's tracker frame (`facts.frame`: `{txid, kind, body}`, `owed_events_publish::frame_note`) or the
/// row's facts under `facts.row` (`READ_WORD_SQL`, wrapped `{marks, row}` by the publish). Neither shape holds a word
/// at `facts.word` or at `facts.frame.word`, which is what `EventCtx::of` read, so no event of L53's 133 cells ever
/// carried `mined: true` to the actor and the collect-after-mined bar had no sample through the loop. Now the `proof`
/// frame (the verified bump the overlay notes at `/arc-ingest` MINED, `proof_fetcher::verify_bump_detailed`) IS the
/// mined word's event; the body carries it, the answer echoes it, and the first apply's note lands
/// `collectAfterMinedMsP95`. A SEEN frame, a MINED `callback` frame (Arcade's claim over a bump the overlay did not
/// verify, else the `proof` frame rides) and a row-shaped word event (the tracker's own write: the tick's re-ask, the
/// POST belt) are not: the class is the callback's road, as the design's bar 3 reads it. RED on 149869bf: `mined`
/// false on the proof frame (`facts.frame.word` is no field), the bar null.
#[test]
fn fix974_a_the_mined_word_is_read_off_the_published_shapes() {
    let txid = "71fa3ec66d30b961ea9322d3d32d85a8e556633bd32c3e83d64367d640d31d84";
    // the frame as the overlay's `tracker_events::note` builds it and `frame_note` carries it
    let frame = |kind: &str, body: Value| json!({"txid": txid, "kind": kind, "body": body});
    let event = |facts: Value, seq: i64| overlay_discovery::owed_event::OwedEvent {
        v: overlay_discovery::owed_event::OWED_EVENT_V,
        seq,
        source: "word".into(),
        source_key: txid.into(),
        facts,
        at_ms: 1_000,
    };
    let proof =
        crate::owed_events_publish::frame_note(&frame("proof", json!({"merklePath": "00"})))
            .unwrap();
    assert_eq!((proof.source.as_str(), proof.key.as_str()), ("word", txid));
    let proof_ev = event(proof.held.clone().unwrap(), 7);
    assert!(proof_ev.frame().is_some(), "{}", proof_ev.facts);
    let ctx = EventCtx::of(&proof_ev);
    assert!(
        ctx.mined,
        "a proof frame is the mined word's event: {}",
        proof_ev.facts
    );
    // not the mined word's: a SEEN frame, a MINED callback frame, a submit frame, a row-shaped word event
    for (kind, body) in [
        ("seen", json!({"source": "arcade", "at": 1})),
        ("callback", json!({"txStatus": "MINED", "merklePath": "00"})),
        ("submit", Value::Null),
    ] {
        let n = crate::owed_events_publish::frame_note(&frame(kind, body)).unwrap();
        assert!(!EventCtx::of(&event(n.held.unwrap(), 8)).mined, "{kind}");
    }
    let row = |word: &str| json!({"marks": 1, "row": {"word": word, "tier": "chain", "height": 973211, "proofRoot": "ab", "headerHash": "cd", "hintSource": null, "hintAt": null, "updatedAt": 1}});
    assert!(
        !EventCtx::of(&event(row("mined"), 9)).mined,
        "the tracker's own write is not the callback's road"
    );
    assert!(!EventCtx::of(&event(row("seen"), 10)).mined);
    let mut other = proof_ev.clone();
    other.source = "pot".into();
    assert!(!EventCtx::of(&other).mined, "only a word event");
    // the shapes are the publish's own (the overlay's source, compiled here as `owed_events_publish`)
    let publish: String = include_str!("../../../overlay-cloudflare/src/owed_events.rs")
        .split_whitespace()
        .collect();
    assert!(
        publish.contains("json!({\"marks\":marks,\"row\":facts})"),
        "the publish wraps a row's facts under `row`"
    );
    assert!(
        publish.contains("held:Some(json!({\"frame\":frame})),"),
        "a frame rides under `frame`"
    );
    assert!(crate::owed_events_publish::READ_WORD_SQL.contains("json_object('word',word,"));
    let events: String = include_str!("../../../overlay-cloudflare/src/tracker_events.rs")
        .split_whitespace()
        .collect();
    assert!(events.contains(
        "json!({\"txid\":txid.to_ascii_lowercase(),\"kind\":kind,\"body\":body.unwrap_or(Value::Null)})"
    ));
    let fetcher: String = include_str!("../../../overlay-cloudflare/src/proof_fetcher.rs")
        .split_whitespace()
        .collect();
    assert!(
        fetcher.contains(
            "BumpVerdict::Valid=>{crate::tracker_events::note(txid,\"proof\",Some(serde_json::json!({\"merklePath\":bump_hex})),);"
        ),
        "the proof frame is noted for a verified bump alone"
    );
    // the body carries it, the object echoes it and notes the bar, the caller's note is the sample
    let body = apply_body("owner", &["g1".to_string()], &ctx, Some(973_211));
    assert_eq!(body["mined"], json!(true), "{body}");
    let object: String = include_str!("object.rs").split_whitespace().collect();
    assert!(object.contains("answer[\"mined\"]=json!(body.mined);"));
    assert!(object.contains("ifbody.mined{bars.collect_after_mined.note(now.saturating_sub(at));}"));
    let answer = json!({"identity": "owner", "applied": true, "firstApply": true, "warmAtBirth": true, "atMs": 1_000, "judgedAtMs": 1_300, "applyMs": 40, "mined": body["mined"]});
    let mut pending = PendingBars::default();
    pending.note_answer("direct", &answer);
    assert_eq!(
        pending.visible.get("mined").map(Vec::len),
        Some(1),
        "the mined class's visible sample: {:?}",
        pending.visible
    );
    assert_eq!(
        pending.visible.get("direct").map(Vec::len),
        Some(1),
        "and the direct class's first visibility"
    );
    let mut bars = Bars::default();
    note_answer_into(&mut bars, &answer);
    assert_eq!(bars.served(2_000)["collectAfterMinedMsP95"], json!(300));
}

/// FIX-974 (bsv-low #974): A DUPE NEVER SAMPLES THE MINED BAR, NOR DOES A REDELIVERY OR A COLD-BORN EVENT: a covered
/// dupe (`applied: false`), an applied redelivery (`firstApply: false`) and an event born before the actor's bootstrap
/// (`warmAtBirth: false`) of a mined word's event add nothing to the `mined` class or the bar, in the caller's pending
/// bars and in the isolate's served bars alike (#952's rule: one sample an event, at its first apply); the served key
/// stands whatever the samples. Green on 149869bf (the guard the fix keeps).
#[test]
fn fix974_b_a_dupe_never_samples_the_mined_bar() {
    let mut pending = PendingBars::default();
    let mut bars = Bars::default();
    for answer in [
        json!({"identity": "owner", "applied": false, "rounds": 0, "atMs": 1_000, "judgedAtMs": 1_300, "applyMs": 4, "mined": true}),
        json!({"identity": "owner", "applied": true, "firstApply": false, "warmAtBirth": true, "atMs": 1_000, "judgedAtMs": 1_300, "applyMs": 40, "mined": true}),
        json!({"identity": "owner", "applied": true, "firstApply": true, "warmAtBirth": false, "atMs": 1_000, "judgedAtMs": 1_300, "applyMs": 40, "mined": true}),
    ] {
        pending.note_answer("queue", &answer);
        note_answer_into(&mut bars, &answer);
    }
    assert!(
        pending.visible.get("mined").is_none_or(Vec::is_empty),
        "{:?}",
        pending.visible
    );
    let served = bars.served(2_000);
    assert_eq!(served["collectAfterMinedMsP95"], Value::Null);
    assert!(
        served.get("collectAfterMinedMsP95").is_some(),
        "the served key stands"
    );
}

// ---- FIX-983 and FIX-982 (bsv-low #983, #982; Wave E row E3) ----

/// An outside that counts what the judge asks of it (the calls, the word keys, the roots).
struct CountingOutside(Cell<usize>, Cell<usize>, Cell<usize>);
impl Outside for CountingOutside {
    async fn word_views(
        &self,
        rows: &BTreeMap<String, crate::tracker_host::stored::Row>,
    ) -> Result<BTreeMap<String, Value>, String> {
        self.0.set(self.0.get() + 1);
        self.1.set(self.1.get() + rows.len());
        Fakes.word_views(rows).await
    }
    async fn canonical_root(&self, h: u64) -> Option<String> {
        self.2.set(self.2.get() + 1);
        Fakes.canonical_root(h).await
    }
}

/// A record whose belt-ask publish faults (the clear's statement), every other statement answered.
struct AsksDown<'a>(&'a SqliteSql);
impl SqlPort for AsksDown<'_> {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        if stmts.iter().any(|s| s.sql == ASKS_CLEAR_SQL) {
            return Err("D1 is down (pin)".into());
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

/// The stranded fixture (a hand's six games, one dated row) bootstrapped into a warm, stamped mirror.
async fn fix983_warm_hand() -> (SqliteSql, SqliteSql, Vec<String>) {
    let record = seeded(&stranded());
    let games = games_of(&record, "owner");
    let local = mirror();
    for chunk in games.chunks(GAMES_PER_APPLY) {
        apply(&record, &local, &Fakes, "owner", chunk, Some(("bootstrap", 1)), Some(TIP), None)
            .await
            .unwrap();
    }
    local
        .conn
        .execute_batch("INSERT INTO meta(name,value) VALUES ('bootstrapped','1') ON CONFLICT(name) DO NOTHING")
        .unwrap();
    (record, local, games)
}
fn fix983_event(games: &[String], seq: i64) -> HeldEvent {
    HeldEvent {
        kind: Some("pot".into()),
        seq: Some(seq),
        games: games.to_vec(),
        at_ms: Some(NOW_MS - 50),
        mined: false,
        tip: Some(TIP),
        held_at_ms: NOW_MS,
        bound: None,
        touch: Some(vec![]),
    }
}

/// (fix983_a) THE PHASE C APPLY MAKES NO RECORD CALL OF ITS OWN PAST PHASE B'S (bsv-low #983). On the stranded hand
/// (six games, one dated row), a warm apply of one game: in Phase B ONE record batch (the marked read); in Phase C
/// the SAME ONE, its belt ask QUEUED on the lane (one batch, one ask) and published by the object's drain after the
/// answer, counted once a drain; a second Phase C apply whose dated rows did not move queues nothing; a drain that
/// faults marks the identity, so its next apply publishes every game again. RED on a31436e6 (the probe of this
/// lane, base API): the Phase C apply made 2 record batches (3 statements) and a counters round trip where Phase B
/// made 1 batch of 1 statement, inside the measured apply: `left: 2 right: 1`.
#[tokio::test]
async fn fix983_a_the_phase_c_apply_makes_no_record_call_past_phase_bs_and_queues_its_asks() {
    let (record, local, games) = fix983_warm_hand().await;
    let turn = futures_util::lock::Mutex::new(());
    let game = &games[0..1];
    let counted = Counting(&record, Cell::new(0), Cell::new(0));
    let b0 = record.batches.get();
    let r = apply_or_hold(&turn, &counted, &local, &Fakes, "owner", &fix983_event(game, 2), None, false)
        .await
        .unwrap();
    let (batches_b, stmts_b) = (record.batches.get() - b0, counted.2.get());
    let Applied::Done(report_b, _) = r else { panic!("held") };
    assert_eq!(report_b.asks, 0, "Phase B queues no ask");
    assert_eq!(ask_lane_len(), 0);
    let counted = Counting(&record, Cell::new(0), Cell::new(0));
    let b0 = record.batches.get();
    let r = apply_or_hold(&turn, &counted, &local, &Fakes, "owner", &fix983_event(game, 3), None, true)
        .await
        .unwrap();
    let (batches_c, stmts_c) = (record.batches.get() - b0, counted.2.get());
    let Applied::Done(report_c, _) = r else { panic!("held") };
    eprintln!(
        "fix983_a: warm apply of one game of {}: Phase B {batches_b} record batch(es), {stmts_b} statement(s); Phase C {batches_c} batch(es), {stmts_c} statement(s), {} ask(s) queued, lane {}",
        games.len(),
        report_c.asks,
        ask_lane_len()
    );
    assert_eq!(
        (batches_c, stmts_c),
        (batches_b, stmts_b),
        "the Phase C apply makes record calls of its own"
    );
    assert_eq!((batches_b, stmts_b), (1, 1), "a warm apply is one record read");
    assert_eq!(report_c.asks, 1, "the dated row's ask is queued");
    assert_eq!(ask_lane_len(), 1, "one batch for the apply");
    assert!(asks_of(&record).is_empty(), "nothing published inside the apply");
    // The object's drain: one record batch for the queued apply, the asks counted once.
    let counted = Counting(&record, Cell::new(0), Cell::new(0));
    let c0 = record.counter(COUNTERS[8]);
    let drained = drain_lane_once(&counted).await;
    assert_eq!(drained, AsksDrained { batches: 1, asks: 1, faults: 0 });
    assert_eq!(counted.2.get(), 2, "the clear and one put, one batch");
    assert_eq!(record.counter(COUNTERS[8]) - c0, 1, "counted once a drain");
    let hop = format!("{STRANDED_HOP}:0");
    assert!(
        asks_of(&record).iter().any(|(n, _, _)| *n == crate::owed_belt::ask_name("owner", &hop)),
        "the stranded hop's ask is published by the drain: {:?}",
        asks_of(&record)
    );
    // Nothing moved: the next Phase C apply queues nothing and makes the one record read.
    let counted = Counting(&record, Cell::new(0), Cell::new(0));
    let b0 = record.batches.get();
    let r = apply_or_hold(&turn, &counted, &local, &Fakes, "owner", &fix983_event(game, 4), None, true)
        .await
        .unwrap();
    let Applied::Done(report, _) = r else { panic!("held") };
    assert_eq!((record.batches.get() - b0, counted.2.get(), report.asks, ask_lane_len()), (1, 1, 0, 0));
    // A drain that faults marks the identity: the next apply publishes the game again.
    record.conn.execute_batch("DELETE FROM owed_belt_state WHERE name LIKE 'belt-ask:%'").unwrap();
    local
        .conn
        .execute_batch("DELETE FROM meta WHERE name LIKE 'askdigest:%'")
        .unwrap();
    apply_or_hold(&turn, &record, &local, &Fakes, "owner", &fix983_event(game, 5), None, true)
        .await
        .unwrap();
    assert_eq!(ask_lane_len(), 1);
    let down = drain_lane_once(&AsksDown(&record)).await;
    assert_eq!(down, AsksDrained { batches: 1, asks: 0, faults: 1 });
    assert!(asks_of(&record).is_empty());
    let r = apply_or_hold(&turn, &record, &local, &Fakes, "owner", &fix983_event(game, 6), None, true)
        .await
        .unwrap();
    let Applied::Done(report, _) = r else { panic!("held") };
    assert_eq!(report.asks, 1, "the faulted identity publishes every game again");
    assert_eq!(drain_lane_once(&record).await, AsksDrained { batches: 1, asks: 1, faults: 0 });
    assert!(!asks_of(&record).is_empty());
}

/// (fix983_b) A WARM HAND'S APPLY IS UNDER THE BAR ON THE FIXTURE (bsv-low #983). The stranded hand, warm, in Phase C:
/// five applies of one game timed on the real clock (the judge's stage the derivation and the mirror's reads; the
/// outside asked once; the asks queued, not published) stay under the design's 300 ms apply bar in a RELEASE build
/// (a debug build prints them). The stages sum to the apply and name the outside apart (`outsideMs`).
#[tokio::test]
async fn fix983_b_a_warm_hand_apply_is_under_the_bar() {
    let (record, local, games) = fix983_warm_hand().await;
    let turn = futures_util::lock::Mutex::new(());
    let game = &games[0..1];
    let mut walls: Vec<u128> = Vec::new();
    for seq in 10..15 {
        let t0 = std::time::Instant::now();
        let r = apply_or_hold(&turn, &record, &local, &Fakes, "owner", &fix983_event(game, seq), None, true)
            .await
            .unwrap();
        walls.push(t0.elapsed().as_millis());
        let Applied::Done(report, _) = r else { panic!("held") };
        let a = answer_of(&report, Vec::new());
        assert!(a["outsideMs"].as_u64().is_some() && a["asksQueued"].as_u64().is_some());
        take_ask_batches();
    }
    walls.sort_unstable();
    eprintln!(
        "fix983_b: warm Phase C applies of one game of {}: wall ms sorted {walls:?} ({} build)",
        games.len(),
        if cfg!(debug_assertions) { "debug" } else { "release" }
    );
    if !cfg!(debug_assertions) {
        assert!(
            walls[walls.len() - 1] < 300,
            "a warm hand's apply is under the 300 ms bar: {walls:?}"
        );
    }
}

/// (fix983_c) THE OUTSIDE IS ASKED ONCE PER APPLY (bsv-low #983). The stranded hand with a word row for every spender
/// the judge reads: a cold apply of its games takes more than one round, and the outside (the word views, CHAINTRACKS
/// on the worker) is asked ONCE for the words, not once per round; the ask's time is the apply's `outsideMs` stage.
/// RED on a31436e6: the outside asked every round (`left: 2 right: 1`).
#[tokio::test]
async fn fix983_c_the_outside_is_asked_once_per_apply() {
    let record = seeded(&stranded());
    record
        .conn
        .execute_batch("INSERT OR IGNORE INTO tx_words(txid,word,tier,updated_at) SELECT DISTINCT spendingTxid,'seen','hint',1 FROM pot_records WHERE spendingTxid IS NOT NULL")
        .unwrap();
    let games = games_of(&record, "owner");
    let out = CountingOutside(Cell::new(0), Cell::new(0), Cell::new(0));
    let local = mirror();
    let (report, _) = apply(&record, &local, &out, "owner", &games, Some(("pot", 9)), Some(TIP), None)
        .await
        .unwrap();
    eprintln!(
        "fix983_c: cold apply of {} games: rounds {}, outside calls {}, word keys {}, roots {}, outsideMs {}",
        games.len(),
        report.rounds,
        out.0.get(),
        out.1.get(),
        out.2.get(),
        report.outside_ms
    );
    assert!(report.rounds >= 2, "the pin needs a multi-round apply: {}", report.rounds);
    assert!(out.1.get() > 0, "the judge asked for words");
    assert_eq!(out.0.get(), 1, "the outside is asked once per apply, not once per round");
    // The ask's keys are the words the judge read, each once.
    let words: usize = record
        .conn
        .query_row("SELECT COUNT(*) FROM tx_words", [], |r| r.get::<_, i64>(0))
        .unwrap() as usize;
    assert!(out.1.get() <= words);
}

/// (fix983_d) THE FLOOD IS SERVED APART (bsv-low #983). A pass that fans out to [`FLOOD_CONCURRENCY`] actors or more
/// (the flood leg's 32 identities, a consumer batch of 16) is the STRESS class: its samples go to `classes.flood`
/// (`rowsVisibleFloodMsP95` beside), never into `classes.direct`, `rowsVisibleDirectMsP95` or the first-visibility
/// bar; a hand's pass (two seats) stays `direct`. The direct pass names its fan-out on every body (`concurrent`), the
/// consumer's pass notes its batch's on the port, and the object classes by it. RED on a31436e6: no body carries a
/// fan-out and every sample is `direct` (`bars_class` takes one argument there; the pin does not compile).
#[tokio::test]
async fn fix983_d_the_flood_is_served_apart() {
    assert_eq!(FLOOD_CONCURRENCY, 8);
    assert_eq!(bars_class(Some("direct"), Some(32)), "flood");
    assert_eq!(bars_class(Some("direct"), Some(8)), "flood");
    assert_eq!(bars_class(Some("direct"), Some(7)), "direct");
    assert_eq!(bars_class(Some("direct"), Some(2)), "direct");
    assert_eq!(bars_class(Some("direct"), None), "direct");
    assert_eq!(bars_class(None, Some(16)), "flood");
    assert_eq!(bars_class(None, Some(1)), "queue");
    assert_eq!(bars_class(None, None), "queue");
    // The fleet's bars: a flood's samples apart from the hand's.
    let d1 = SqliteSql::migrated();
    let mut p = PendingBars::default();
    p.note_answer("direct", &json!({"identity": "hand", "applyMs": 120, "atMs": 1_000, "judgedAtMs": 1_400, "applied": true, "firstApply": true}));
    p.note_answer("queue", &json!({"identity": "q", "applyMs": 90, "atMs": 1_000, "judgedAtMs": 1_900, "applied": true, "firstApply": true}));
    for n in 0..32u64 {
        p.note_answer("flood", &json!({"identity": format!("f{n}"), "applyMs": 3_000 + n, "atMs": 1_000, "judgedAtMs": 5_000 + n, "applied": true, "firstApply": true}));
    }
    flush_bars_from(&d1, 10_000, &p).await.unwrap();
    let rows: Vec<Value> = d1
        .run_batch(vec![Stmt::new(BARS_READ_SQL, vec![])])
        .await
        .unwrap()
        .remove(0)
        .rows;
    assert_eq!(rows.len(), 3, "three class rows: {rows:?}");
    let bars = fleet_bars(&rows, 10_000, lib_rev());
    assert_eq!(bars["classes"]["direct"]["applyMsP95"], json!(120));
    assert_eq!(bars["classes"]["direct"]["applySamples"], json!(1));
    assert_eq!(bars["rowsVisibleDirectMsP95"], json!(400));
    assert_eq!(bars["rowsVisibleMsP95"], json!(900), "the first visibility is the warm hand's and the queue's");
    assert_eq!(bars["applyMsP95"], json!(120));
    assert_eq!(bars["classes"]["flood"]["applySamples"], json!(32));
    assert_eq!(bars["classes"]["flood"]["applyMsP95"], json!(3_030));
    assert_eq!(bars["rowsVisibleFloodMsP95"], json!(4_030));
    assert_eq!(bars["floodConcurrency"], json!(FLOOD_CONCURRENCY));
    // The direct pass names its fan-out on every body.
    let events: Vec<overlay_discovery::owed_event::OwedEvent> = (0..3)
        .map(|i| {
            serde_json::from_value(json!({"v": 1, "seq": 500 + i, "source": "hop",
                "sourceKey": format!("{}:0", h("b9", i)), "facts": null, "atMs": NOW_MS}))
            .unwrap()
        })
        .collect();
    let port = Fold16Sampled(RefCell::new(Vec::new()));
    let r = crate::owed_events::direct_with(&port, events, Some(TIP)).await;
    {
        let bodies = port.0.borrow();
        assert_eq!(r.calls as usize, bodies.len());
        assert!(!bodies.is_empty());
        assert!(
            bodies.iter().all(|b| b["concurrent"] == json!(bodies.len())),
            "every body names the pass's fan-out: {bodies:?}"
        );
    }
    // The consumer's pass notes its batch's fan-out on the port (bounded by the identities it delivers at once).
    let w = WithActor {
        d1: seeded(&stranded()),
        actor: mirror(),
        d1_judges: false,
        event: Some(("pot", 7)),
        actor_reads: false,
        actor_fails: Cell::new(false),
    };
    let subjects: Vec<(String, Vec<String>)> = (0..40)
        .map(|i| (format!("02{i:064x}"), vec!["g".to_string()]))
        .collect();
    NOTED_CONCURRENCY.with(|c| c.set(0));
    let _ = crate::owed_events::judge_plan(&w, crate::owed_events::Plan::of_subjects(subjects[..3].to_vec())).await;
    assert_eq!(NOTED_CONCURRENCY.with(|c| c.get()), 3);
    let _ = crate::owed_events::judge_plan(&w, crate::owed_events::Plan::of_subjects(subjects)).await;
    assert_eq!(
        NOTED_CONCURRENCY.with(|c| c.get()),
        crate::owed_events::JUDGE_IDENTITIES_AT_ONCE as u32
    );
    // The wiring: the D1 judge's body carries it; the object classes its own note by it.
    let judge: String = include_str!("../owed_judge.rs").split_whitespace().collect();
    assert!(judge.contains("body[\"concurrent\"]=json!(self.concurrent.get());"));
    let object: String = include_str!("object.rs").split_whitespace().collect();
    assert!(object.contains("note_answer_as(bars_class(body.class.as_deref(),body.concurrent),&answer);"));
}

/// (fix983_e) THE RECORD WORK THAT IS NOT THE APPLY'S RUNS AFTER THE ANSWER (bsv-low #983): the object's `/apply`
/// defers the asks lane's drain, the misses' counter and the bars' flush to `wait_until` ([`OwedActor::after_apply`]),
/// and its alarm drains the lane too; the stage rings carry `outside`; the consumer's Phase C delivery and the direct
/// pass note a faulted apply's event ([`note_actor_fault`], the unapplied figure's source in every phase). Source pins
/// (wasm32 only).
#[test]
fn fix983_e_the_object_defers_the_record_work_that_is_not_the_applys() {
    let object: String = include_str!("object.rs").split_whitespace().collect();
    assert!(object.contains("self.after_apply(misses);Response::from_json(&answer)"));
    assert!(object.contains("self.state.wait_until(asyncmove{letOk(record)=crate::routes::D1Sql::of(&env)else{return;};ifmisses>0{record.counters(vec![(COUNTERS[27].into(),missesasu64)]).await;}drain_lane_once(&record).await;flush_bars(&record,Date::now().as_millis(),false).await;});"));
    assert!(object.contains("self.after_apply(0);Response::ok(\"ok\")"));
    assert!(!object.contains("flush_bars(&record,now,false).await;answer[\"noted\"]"));
    assert!(APPLY_STAGES.iter().any(|(f, r)| *f == "outsideMs" && *r == "outside"));
    let actor: String = include_str!("../owed_actor.rs").split_whitespace().collect();
    assert!(actor.contains("note_actor_fault(&id,db.event_at_ms());returnErr(e);"), "Phase C's delivery notes a faulted apply");
    let events: String = include_str!("../owed_events.rs").split_whitespace().collect();
    assert!(events.contains("b[\"concurrent\"]=json!(fan_out);"));
    assert!(events.contains("db.note_concurrency(order.len().min(JUDGE_IDENTITIES_AT_ONCE)asu32);"));
}

/// (fix982_a) THE UNAPPLIED FIGURE IS THE ACTOR'S IN EVERY PHASE (bsv-low #982). `oldestUnappliedEventAgeMs` is the
/// oldest event an actor holds unapplied: the oldest HELD event (an actor not bootstrapped, `holding.oldestAtMs`) or
/// the oldest event whose apply FAULTED with no later apply of its identity landed (the queue row's
/// `oldestUnappliedAtMs`, noted by the consumer and the direct pass), whichever is older, its source named; a D1
/// mark is never one (the reconcile's `backfill` marks, the age repair's, the sweep's: Phase C's dirty backlog, 590 s
/// on beta with `holding` 0), and its age is served beside as `oldestUnackedMarkAgeMs`. RED on a31436e6: with the D1
/// judge off the figure was the un-acked mark's age (`left: 590000 right: null`).
#[tokio::test]
async fn fix982_a_the_unapplied_figure_is_the_oldest_held_or_faulted_event_in_every_phase() {
    assert_eq!(unapplied_event_age(None, None), (None, None));
    assert_eq!(unapplied_event_age(Some(90_000), None), (Some(90_000), Some("held")));
    assert_eq!(unapplied_event_age(None, Some(7_000)), (Some(7_000), Some("faulted")));
    assert_eq!(unapplied_event_age(Some(5_000), Some(7_000)), (Some(7_000), Some("faulted")));
    assert_eq!(unapplied_event_age(Some(90_000), Some(7_000)), (Some(90_000), Some("held")));
    let d1 = SqliteSql::migrated();
    let now = 1_000_000u64;
    async fn snapshot(d1: &SqliteSql, now: u64) -> Value {
        d1.run_batch(vec![health_snapshot_stmt(now, true)]).await.unwrap();
        let cursor: String = d1
            .conn
            .query_row("SELECT cursor FROM owed_belt_state WHERE name='owed-actor:health'", [], |r| r.get(0))
            .unwrap();
        health_snapshot_of(Some(&cursor)).unwrap()
    }
    // Every event applied, a dirty mark 590 s old (Phase C's reconcile driver): no unapplied event, whatever the phase.
    let mark = json!(590_000);
    let body = health_body(Some(&snapshot(&d1, now).await), now, &mark);
    assert_eq!(
        (body["oldestUnappliedEventAgeMs"].clone(), body["oldestUnappliedEventSource"].clone(), body["oldestUnackedMarkAgeMs"].clone()),
        (Value::Null, Value::Null, json!(590_000)),
        "a mark is no unapplied event: {body}"
    );
    // A faulted apply's event (the consumer's or the direct pass's note), no later apply of its identity landed.
    let mut p = PendingBars::default();
    p.note_fault("owner", Some(now - 7_000));
    flush_bars_from(&d1, now, &p).await.unwrap();
    let body = health_body(Some(&snapshot(&d1, now).await), now, &mark);
    assert_eq!(
        (body["oldestUnappliedEventAgeMs"].clone(), body["oldestUnappliedEventSource"].clone()),
        (json!(7_000), json!("faulted"))
    );
    // An actor holding its events (not bootstrapped) since 90 s: the older of the two, named.
    d1.conn
        .execute(
            "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('owed-actor:holding:owner','{}',?1)",
            [now - 90_000],
        )
        .unwrap();
    let body = health_body(Some(&snapshot(&d1, now).await), now, &mark);
    assert_eq!(
        (body["oldestUnappliedEventAgeMs"].clone(), body["oldestUnappliedEventSource"].clone(), body["holding"]["oldestMs"].clone()),
        (json!(90_000), json!("held"), json!(90_000))
    );
    // The hold ends and a later apply of the identity lands: nothing unapplied, the mark still named apart.
    d1.conn
        .execute_batch("DELETE FROM owed_belt_state WHERE name='owed-actor:holding:owner'")
        .unwrap();
    let mut p = PendingBars::default();
    p.note_fault("owner", Some(now - 7_000));
    p.note_answer("queue", &json!({"identity": "owner", "applyMs": 1, "applied": true}));
    flush_bars_from(&d1, now, &p).await.unwrap();
    let body = health_body(Some(&snapshot(&d1, now).await), now, &mark);
    assert_eq!(
        (body["oldestUnappliedEventAgeMs"].clone(), body["oldestUnappliedEventSource"].clone(), body["oldestUnackedMarkAgeMs"].clone()),
        (Value::Null, Value::Null, json!(590_000))
    );
    // The route hands the figure the mark's age only to name it; no phase flag reaches it.
    let routes: String = include_str!("../routes.rs").split_whitespace().collect();
    assert!(routes.contains("body[\"owedActor\"]=crate::owed_actor::health_body(actor_snapshot.as_ref(),worker::Date::now().as_millis(),&body[\"owedEvents\"][\"oldestUnackedMarkAgeMs\"],);"));
}

// ── bsv-low #986 (READ-986 item 3, FIX-986): the actor's facts for a game equal the D1 read's by construction ──
//
// READ-986 family D: a pre-signed refund broadcast at its recovery height outside the overlay's door, found by the
// discovery pass and proved by the confirmation pass, carries no stored `pot_records.verdict`. D1's results view
// classifies it per request from the spender's BEEF (`results::classify_from_columns`); the actor's mirror holds no
// BEEF bytes, so it serves `unbound` / `outcome unresolved` where D1 serves `payout refund claimable`. The fold's
// 2(b) makes the stored column the one source both read: the janitor's second candidate set latches the verdict
// group, the row lands as a `pot` event and every actor applies it (no `JUDGE_VERSION` move, item 5).
mod fix986 {
    use super::*;
    use bsv_rs::primitives::ec::PrivateKey;
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::{
        Beef, MerklePath, MerklePathLeaf, Transaction, TransactionInput, TransactionOutput,
    };
    use overlay_discovery::pot::storage::PotRecord;
    use overlay_discovery::pot::{p2pkh_lock, CovenantParams, PotVerdict};

    fn id_a() -> String {
        format!("02{}", "a1".repeat(32))
    }
    fn id_b() -> String {
        format!("03{}", "b2".repeat(32))
    }
    fn key(scalar: u8) -> PrivateKey {
        let mut b = [0u8; 32];
        b[31] = scalar;
        PrivateKey::from_bytes(&b).unwrap()
    }
    /// A real covenant's committed params (the seats' settle keys real, so the seat attribution matches them).
    fn params() -> CovenantParams {
        CovenantParams {
            pub_a: key(1).public_key().to_compressed(),
            pub_b: key(2).public_key().to_compressed(),
            pub_tower: key(3).public_key().to_compressed(),
            pay_pkh_a: [0x11; 20],
            pay_pkh_b: [0x22; 20],
            rake_pkh: [0x33; 20],
            stake_a: 10_000,
            stake_b: 10_000,
            fee_sats: 200,
            recovery_height: 899_900,
        }
    }
    const POT_SATS: u64 = 20_000;
    const SPENT_HEIGHT: u64 = TIP - 50;

    /// The pot's pre-signed REFUND as the tower broadcast it at the recovery height: the T_refund exit (each
    /// seat's stake less its fee share), nLockTime at the committed height, the pot input non-final. Unsigned:
    /// the classifier reads outputs, lock time and sequence; the signers conclude `'unresolved'`.
    fn refund_tx(p: &CovenantParams, pot: &str) -> Transaction {
        let mut tx = Transaction::new();
        let mut input = TransactionInput::new(pot.to_string(), 0);
        input.sequence = 0xffff_fffe;
        tx.inputs.push(input);
        let fee_b = p.fee_sats / 2;
        let fee_a = p.fee_sats - fee_b;
        for (sats, pkh) in [
            (p.stake_a - fee_a, p.pay_pkh_a),
            (p.stake_b - fee_b, p.pay_pkh_b),
        ] {
            tx.outputs.push(TransactionOutput {
                satoshis: Some(sats),
                locking_script: LockingScript::from_binary(&p2pkh_lock(&pkh)).unwrap(),
                change: false,
            });
        }
        tx.lock_time = p.recovery_height as u32;
        tx
    }

    /// The winner-A settle (the T_a exit: the rake, then the net to seat A's home).
    fn settle_a_tx(p: &CovenantParams, pot: &str) -> Transaction {
        let mut tx = Transaction::new();
        tx.inputs.push(TransactionInput::new(pot.to_string(), 0));
        let net = POT_SATS - p.fee_sats;
        let rake = POT_SATS / overlay_discovery::pot::TEMPLATE_RAKE_DIVISOR;
        for (sats, pkh) in [(rake, p.rake_pkh), (net - rake, p.pay_pkh_a)] {
            tx.outputs.push(TransactionOutput {
                satoshis: Some(sats),
                locking_script: LockingScript::from_binary(&p2pkh_lock(&pkh)).unwrap(),
                change: false,
            });
        }
        tx
    }

    /// The spender's stored BEEF with its verified bump (the confirmation pass's proof), as `pot_beefs` holds it.
    fn beef_hex(tx: &Transaction, height: u64) -> String {
        let mut beef = Beef::new();
        let i = beef.merge_bump(
            MerklePath::new(
                height as u32,
                vec![vec![MerklePathLeaf::new_txid(0, tx.id())]],
            )
            .unwrap(),
        );
        beef.merge_raw_tx(tx.to_binary(), Some(i));
        hex::encode(beef.to_binary())
    }

    /// The pot's row as the record holds it after the discovery pass (`spendDiscoveryAt`), the confirmation pass
    /// (`spentConfirmed`, `spentHeight`) and the admission's decode (`paramsDecoded`, the ten params): `verdict`
    /// NULL unless `verdict` names the door's group.
    fn pot_row_sql(
        p: &CovenantParams,
        pot: &str,
        spender: &str,
        made: i64,
        verdict: Option<(&str, &str)>,
    ) -> String {
        let (v, vt, ss) = match verdict {
            Some((v, s)) => (format!("'{v}'"), format!("'{spender}'"), format!("'{s}'")),
            None => ("NULL".into(), "NULL".into(), "NULL".into()),
        };
        format!(
            "INSERT INTO pot_records(txid,outputIndex,spent,spendingTxid,spentConfirmed,spentHeight,spenderFinal,createdAt,lockKind,paramsDecoded,pubA,pubB,pubTower,payPkhA,payPkhB,rakePkh,stakeA,stakeB,feeSats,recoveryHeight,potSats,verdict,verdictTxid,settleSigners,spendDiscoveryAt) \
             VALUES ('{pot}',0,1,'{spender}',1,{SPENT_HEIGHT},0,{made},'covenant',1,'{}','{}','{}','{}','{}','{}',{},{},{},{},{POT_SATS},{v},{vt},{ss},{})",
            hex::encode(p.pub_a),
            hex::encode(p.pub_b),
            hex::encode(p.pub_tower),
            hex::encode(p.pay_pkh_a),
            hex::encode(p.pay_pkh_b),
            hex::encode(p.rake_pkh),
            p.stake_a,
            p.stake_b,
            p.fee_sats,
            p.recovery_height,
            made + 3_600,
        )
    }

    /// Both seats' verified party rows (`sigValid 1`, each seat's committed settle key).
    fn party_rows_sql(p: &CovenantParams, game: &str, pot: &str, made: i64) -> Vec<String> {
        [(id_a(), id_b(), p.pub_a, 1usize), (id_b(), id_a(), p.pub_b, 2)]
            .into_iter()
            .map(|(me, other, seat, n)| {
                format!(
                    "INSERT INTO potparty_records(identity,opponentIdentity,gameId,potTxid,potVout,recoveryHeight,sigHex,txid,outputIndex,createdAt,sigValid,seatSettlePubkey,seatSigHex) \
                     VALUES ('{me}','{other}','{game}','{pot}',0,{},'00','{}',1,{made},1,'{}','00')",
                    p.recovery_height,
                    h("e6", n),
                    hex::encode(seat)
                )
            })
            .collect()
    }

    /// FAMILY D as beta's record holds it: the covenant pot (`paramsDecoded 1`, `verdict NULL`), spent by the
    /// pre-signed refund the discovery pass found and the confirmation pass proved (`spentConfirmed 1`,
    /// `spentHeight`), the spender's `pot_beefs` row `proof_verified 1`, no result marker, both seats verified.
    fn discovered_refund_fixture() -> (Fixture, String, String, Transaction, CovenantParams) {
        let p = params();
        let made = (NOW_MS / 1000) as i64 - 7_200;
        let pot = h("c9", 0x86);
        let game = h("9a", 0x86);
        let refund = refund_tx(&p, &pot);
        let r = refund.id();
        let mut seed = vec![pot_row_sql(&p, &pot, &r, made, None)];
        seed.extend(party_rows_sql(&p, &game, &pot, made));
        seed.push(format!(
            "INSERT INTO pot_beefs(txid,beef,createdAt,has_proof,proof_verified,proofHeight) VALUES ('{r}',x'{}',{},1,1,{SPENT_HEIGHT})",
            beef_hex(&refund, SPENT_HEIGHT),
            made + 3_700
        ));
        (
            Fixture {
                name: "fix986 a discovery-found refund with no stored verdict",
                seed,
                identities: vec![id_a(), id_b()],
            },
            pot,
            r,
            refund,
            p,
        )
    }

    /// CLASS A: a won settle the identity itself files as collected (the door's held filing, its content key and
    /// `payTxid`), the door's own verdict group stored; before the filing the premise, the payout row.
    /// `(fixture, pot txid, game id, settle txid)`.
    fn collected_fixture() -> (Fixture, String, String, String) {
        let p = params();
        let made = (NOW_MS / 1000) as i64 - 7_200;
        let pot = h("ca", 0x87);
        let game = h("9a", 0x87);
        let settle = settle_a_tx(&p, &pot);
        let s = settle.id();
        let mut seed = vec![pot_row_sql(&p, &pot, &s, made, Some(("winner-a", "coop")))];
        seed.extend(party_rows_sql(&p, &game, &pot, made));
        seed.push(format!(
            "INSERT INTO pot_beefs(txid,beef,createdAt,has_proof,proof_verified,proofHeight) VALUES ('{s}',x'{}',{},1,1,{SPENT_HEIGHT})",
            beef_hex(&settle, SPENT_HEIGHT),
            made + 100
        ));
        (
            Fixture {
                name: "fix986 a collected won settle",
                seed,
                identities: vec![id_a(), id_b()],
            },
            pot,
            game,
            s,
        )
    }

    /// The identity's COLLECTED filing of its won settle, written as the door writes it (`record_post`'s own
    /// statement, the ONE writer of the column; `i492_dn1` holds every other source to that): the held content
    /// key as the row's txid, no signature column, the settle as `payTxid`.
    fn file_collected(db: &SqliteSql, game: &str, settle: &str) {
        let key = crate::record_post::held_content_key(&crate::record_post::HeldMarker {
            game_id: hex::decode(game).unwrap().try_into().unwrap(),
            identity_key: hex::decode(id_a()).unwrap(),
            pay_txid: hex::decode(settle).unwrap().try_into().unwrap(),
            sig: Vec::new(),
        });
        let made = (NOW_MS / 1000) as i64 - 7_200 + 3_000;
        db.conn
            .execute(
                crate::record_post::COLLECTED_FILE_SQL,
                rusqlite::params![
                    id_a(),
                    game,
                    key,
                    0i64,
                    Option::<String>::None,
                    made,
                    settle
                ],
            )
            .unwrap();
    }

    fn row_of(
        db: &SqliteSql,
        identity: &str,
        outpoint: &str,
    ) -> Option<(String, Option<i64>, Value, Option<String>)> {
        db.conn
            .query_row(
                "SELECT family,claimable,facts,reason FROM owed_rows WHERE identity=?1 AND outpoint=?2",
                [identity, outpoint],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Option<i64>>(1)?,
                        serde_json::from_str(&r.get::<_, String>(2)?).unwrap(),
                        r.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .ok()
    }

    /// The reconcile's own digest (`rows_digest` over `ROWS_SQL`'s rows), the comparison the compare route makes.
    async fn digest(db: &SqliteSql, identity: &str) -> String {
        let rows = db
            .run_batch(vec![Stmt::new(ROWS_SQL, vec![Bind::Text(identity.into())])])
            .await
            .unwrap();
        rows_digest(&rows[0].rows)
    }

    /// THE JANITOR'S LATCH replayed natively with the shipped pieces: the candidate scan
    /// (`spent_unclassified_candidates_sql`), the parse and the one classifier (`classify_stored_spend`), and the
    /// pointer CAS (`verdict_cas_sql`), as `backfill_decoded_params`'s second loop runs them on D1. Answers the
    /// candidates it found.
    fn janitor_latch(db: &SqliteSql, p: &CovenantParams) -> Vec<(String, String)> {
        use bsv_overlay_cloudflare::d1_discovery::{
            spent_unclassified_candidates_sql, verdict_cas_sql,
        };
        let sql = spent_unclassified_candidates_sql(16);
        let candidates: Vec<(String, u32, String)> = db
            .conn
            .prepare(&sql)
            .unwrap()
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, u32>(1)?,
                    r.get::<_, String>(3)?,
                ))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let mut latched = Vec::new();
        for (txid, vout, spender) in candidates {
            let beef: Vec<u8> = db
                .conn
                .query_row(
                    "SELECT beef FROM pot_beefs WHERE txid=?1",
                    [&spender],
                    |r| r.get(0),
                )
                .unwrap();
            let rec = PotRecord {
                txid: txid.clone(),
                output_index: vout,
                spent: true,
                spending_txid: Some(spender.clone()),
                spent_confirmed: true,
                params_decoded: true,
                lock_kind: Some("covenant".into()),
                pub_a: Some(hex::encode(p.pub_a)),
                pub_b: Some(hex::encode(p.pub_b)),
                pub_tower: Some(hex::encode(p.pub_tower)),
                pay_pkh_a: Some(hex::encode(p.pay_pkh_a)),
                pay_pkh_b: Some(hex::encode(p.pay_pkh_b)),
                rake_pkh: Some(hex::encode(p.rake_pkh)),
                stake_a: Some(p.stake_a),
                stake_b: Some(p.stake_b),
                fee_sats: Some(p.fee_sats),
                recovery_height: Some(p.recovery_height),
                pot_sats: Some(POT_SATS),
                ..Default::default()
            };
            let group =
                bsv_overlay_cloudflare::proof_fetcher::classify_stored_spend(&rec, &spender, &beef)
                    .expect("the held bytes classify");
            assert_eq!(group.verdict, PotVerdict::Refund);
            let w = group.write_concluded();
            let changed = db
                .conn
                .execute(
                    verdict_cas_sql(),
                    rusqlite::params![w.verdict, &spender, w.settle_signers, &txid, vout, &spender],
                )
                .unwrap();
            assert_eq!(changed, 1, "the CAS hits the live pointer");
            latched.push((txid, spender));
        }
        latched
    }

    /// (d) THE DIFFERENTIAL PIN over a discovery-found spend. The premise, today's disagreement (and the fixture's
    /// before the latch, after the fold too): D1 serves `payout refund claimable` (classified per request from the
    /// spender's BEEF), the actor `unbound` with "the spend is not classified yet (no verdict)", the digests
    /// differ. The janitor's second candidate set names the pot and nothing else; its latch, replayed with the
    /// shipped scan, classifier and CAS, writes `refund` keyed to the spender; D1 judged again and the actor applying
    /// the verdict row as a `pot` event (no bootstrap, no version move) store the same rows byte for byte and the
    /// same digest, `payout refund claimable` on both; an actor bootstrapped after the latch (seat B) the same. RED
    /// on main: the scan does not exist (the pin does not compile); with the latch skipped, the premise's digests
    /// stand apart (`unbound` against `payout`).
    #[tokio::test]
    async fn fix986_d_the_actor_and_d1_agree_on_a_discovery_found_spend_once_its_verdict_is_latched(
    ) {
        let (f, pot, refund, _, p) = discovered_refund_fixture();
        let d1 = d1_judge(&f).await;
        let (a, b) = (id_a(), id_b());
        let key = format!("{pot}:0");
        let (actor, _) = actor_judge(&d1, &a).await;
        // THE PREMISE: the same record, two judgements.
        let d1_row = row_of(&d1, &a, &key).expect("D1 holds a row for the pot");
        assert_eq!(
            (
                d1_row.0.as_str(),
                d1_row.1,
                d1_row.2["verdict"].clone(),
                d1_row.2["outcome"].clone(),
                d1_row.2["claim"].clone()
            ),
            (
                "payout",
                Some(1),
                json!("refund"),
                json!("refund"),
                json!("internalize")
            ),
            "D1 classifies the found spend from the spender's BEEF: {d1_row:?}"
        );
        let actor_row = row_of(&actor, &a, &key).expect("the actor holds a row for the pot");
        assert_eq!(
            (
                actor_row.0.as_str(),
                actor_row.1,
                actor_row.2["outcome"].clone(),
                actor_row.3.as_deref()
            ),
            (
                "unbound",
                None,
                json!("unresolved"),
                Some("the spend is not classified yet (no verdict)")
            ),
            "the actor holds no bytes to classify from: {actor_row:?}"
        );
        assert_ne!(digest(&actor, &a).await, digest(&d1, &a).await);
        assert_ne!(stored(&actor, &a), stored(&d1, &a));

        // THE JANITOR'S LATCH: the candidate set is the pot and nothing else, and the group lands.
        let latched = janitor_latch(&d1, &p);
        assert_eq!(latched, vec![(pot.clone(), refund.clone())]);
        let group: (Option<String>, Option<String>, Option<String>) = d1
            .conn
            .query_row(
                "SELECT verdict,verdictTxid,settleSigners FROM pot_records WHERE txid=?1 AND outputIndex=0",
                [&pot],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            group,
            (
                Some("refund".into()),
                Some(refund.clone()),
                Some("unresolved".into())
            )
        );
        assert!(
            janitor_latch(&d1, &p).is_empty(),
            "a latched row leaves the candidate set"
        );

        // THE VERDICT ROW IS A `pot` EVENT: D1 judged again; the warm actor applies it (no bootstrap).
        for id in [&a, &b] {
            for chunk in games_of(&d1, id).chunks(16) {
                crate::owed_judge::judge_subjects(&d1, id, chunk, "d1")
                    .await
                    .unwrap();
            }
        }
        apply(
            &d1,
            &actor,
            &Fakes,
            &a,
            &games_of(&d1, &a),
            Some(("pot", 2)),
            Some(TIP),
            None,
        )
        .await
        .unwrap();
        assert_eq!(stored(&actor, &a), stored(&d1, &a), "byte for byte");
        assert_eq!(
            digest(&actor, &a).await,
            digest(&d1, &a).await,
            "the compare's digest"
        );
        let row = row_of(&d1, &a, &key).unwrap();
        assert_eq!(
            (
                row.0.as_str(),
                row.1,
                row.2["verdict"].clone(),
                row.2["settleSigners"].clone(),
                row.2["claimable"].clone()
            ),
            (
                "payout",
                Some(1),
                json!("refund"),
                json!("unresolved"),
                json!(true)
            ),
            "{row:?}"
        );
        assert_eq!(row_of(&actor, &a, &key).unwrap(), row);
        // seat B: an actor bootstrapped after the latch reads the stored column as D1 does
        let (actor_b, _) = actor_judge(&d1, &b).await;
        assert_eq!(stored(&actor_b, &b), stored(&d1, &b));
        assert_eq!(digest(&actor_b, &b).await, digest(&d1, &b).await);
        let row_b = row_of(&d1, &b, &key).unwrap();
        assert_eq!(
            (row_b.0.as_str(), row_b.1),
            ("payout", Some(1)),
            "{row_b:?}"
        );
    }

    /// (e) THE CONTROLS, GREEN on both sides before and after the fold: CLASS C, a hop swept by a filed sweep the
    /// index proves (`fold11_sweep_fixture`), `payout hop-sweep` on both; CLASS A, a won settle the identity filed
    /// as collected: the payout row before the filing (the premise), no row on either side with it, the digests
    /// equal in both states.
    #[tokio::test]
    async fn fix986_e_a_hop_sweep_filing_and_a_collected_filing_agree_on_both_sides() {
        // CLASS C
        let d1 = d1_judge(&fold11_sweep_fixture()).await;
        let (actor, _) = actor_judge(&d1, "owner").await;
        let hop = format!("{FOLD9_HOP}:0");
        let row = row_of(&d1, "owner", &hop).expect("the swept hop's payout");
        assert_eq!(
            (row.0.as_str(), row.2["claim"].clone()),
            ("payout", json!("internalize")),
            "{row:?}"
        );
        assert_eq!(row.2["sweepSource"], json!("hopsweep-filing"), "{row:?}");
        assert_eq!(stored(&actor, "owner"), stored(&d1, "owner"));
        assert_eq!(digest(&actor, "owner").await, digest(&d1, "owner").await);
        // CLASS A: the premise, then the filing
        let (a, _) = (id_a(), id_b());
        let (f, pot, game, settle) = collected_fixture();
        let d1 = d1_judge(&f).await;
        let key = format!("{pot}:0");
        let row = row_of(&d1, &a, &key).expect("the won settle's payout before the filing");
        assert_eq!(
            (
                row.0.as_str(),
                row.1,
                row.2["outcome"].clone(),
                row.2["verdict"].clone()
            ),
            ("payout", Some(1), json!("won"), json!("winner-a")),
            "{row:?}"
        );
        let (actor, _) = actor_judge(&d1, &a).await;
        assert_eq!(stored(&actor, &a), stored(&d1, &a));
        // THE FILING through the door's own statement, then D1 judged again and the warm actor applying the
        // filing's event: a verified collected payout is no row on either side.
        file_collected(&d1, &game, &settle);
        for chunk in games_of(&d1, &a).chunks(16) {
            crate::owed_judge::judge_subjects(&d1, &a, chunk, "d1")
                .await
                .unwrap();
        }
        assert!(
            row_of(&d1, &a, &key).is_none(),
            "a verified collected payout is no row: {:?}",
            stored(&d1, &a)
        );
        apply(
            &d1,
            &actor,
            &Fakes,
            &a,
            &games_of(&d1, &a),
            Some(("collected", 2)),
            Some(TIP),
            None,
        )
        .await
        .unwrap();
        assert_eq!(stored(&actor, &a), stored(&d1, &a));
        assert_eq!(digest(&actor, &a).await, digest(&d1, &a).await);
        // and an actor bootstrapped after the filing
        let (actor, _) = actor_judge(&d1, &a).await;
        assert_eq!(stored(&actor, &a), stored(&d1, &a));
        assert_eq!(digest(&actor, &a).await, digest(&d1, &a).await);
    }
}
