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
