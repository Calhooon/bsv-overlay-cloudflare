//! #893: every dirty mark belongs to the real source statement's transaction.
use async_trait::async_trait;
use bsv_overlay_cloudflare::{
    admit_fast::{self, ColumnInfo, ShadowDb},
    d1::{self, QVal, Query, OVERLAY_MIGRATIONS},
    d1_discovery as discovery,
};
use overlay_discovery::{
    hopparty::storage::HoppartyRecord, potparty::storage::PotpartyRecord,
    result::storage::ResultRecord,
};
use rusqlite::{params_from_iter, types::Value, Connection, OptionalExtension};
use std::cell::RefCell;

#[path = "support/d1_fleet.rs"]
mod d1_fleet;

const DISCOVERY: &str = include_str!("../../overlay-cloudflare/src/d1_discovery.rs");
type Mark = (String, String, String, String);
type Program = (String, String, i32, i32, i32);

fn value(v: &QVal) -> Value {
    match v {
        QVal::Null => Value::Null,
        QVal::Int(v) => Value::Integer(*v),
        QVal::Text(v) => Value::Text(v.clone()),
        QVal::Bool(v) => Value::Integer(i64::from(*v)),
        QVal::Blob(v) => Value::Blob(v.clone()),
        QVal::Float(v) => Value::Real(*v),
    }
}

/// Read an inline production literal without making a second copy of its SQL.
/// Rust's escaped newline removes the following indentation as well.
fn literal(source: &str, anchor: &str) -> String {
    let tail = &source[source.find(anchor).expect("production anchor") + anchor.len()..];
    let mut chars = tail[tail.find('"').expect("production literal") + 1..]
        .chars()
        .peekable();
    let mut out = String::new();
    while let Some(c) = chars.next() {
        match c {
            '"' => return out,
            '\\' => match chars.next().expect("escape") {
                '\n' => {
                    while chars.peek().is_some_and(|c| c.is_whitespace()) {
                        chars.next();
                    }
                }
                'n' => out.push('\n'),
                't' => out.push('\t'),
                c => out.push(c),
            },
            c => out.push(c),
        }
    }
    panic!("unterminated production literal")
}

fn sql_literal(source: &str, prefix: &str) -> String {
    let start = source.find(&format!("\"{prefix}")).expect("production SQL");
    literal(&source[start..], "")
}

fn discovery_bound(name: &str) -> usize {
    DISCOVERY
        .split_once(&format!("const {name}: usize = "))
        .unwrap()
        .1
        .split(';')
        .next()
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

fn marks(source: &str, key: &str, identities: &[&str]) -> Vec<Mark> {
    let mut out: Vec<_> = identities
        .iter()
        .map(|id| {
            (
                (*id).into(),
                "game".into(),
                source.into(),
                key.into(),
            )
        })
        .collect();
    out.sort();
    out
}

struct Fixture {
    db: Connection,
    programs: RefCell<Vec<Program>>,
}

impl Fixture {
    fn new(skip: Option<&str>) -> Self {
        let db = Connection::open_in_memory().unwrap();
        for sql in OVERLAY_MIGRATIONS {
            if skip.is_some_and(|name| {
                sql.starts_with(&format!("CREATE TRIGGER IF NOT EXISTS {name} "))
            }) {
                continue;
            }
            if let Err(e) = db.execute_batch(sql) {
                assert!(
                    d1::migration_error_is_benign(sql, &e.to_string()),
                    "{e}: {sql}"
                );
            }
        }
        Self {
            db,
            programs: RefCell::new(Vec::new()),
        }
    }

    fn execute(&self, sql: &str, binds: Vec<Value>) {
        let mut stmt = self
            .db
            .prepare(sql)
            .unwrap_or_else(|e| panic!("{e}: {sql}"));
        let mut rows = stmt.query(params_from_iter(binds)).unwrap();
        while rows
            .next()
            .unwrap_or_else(|e| panic!("{e}: {sql}"))
            .is_some()
        {}
    }

    fn query(&self, q: &Query) {
        self.execute(q.sql(), q.params().iter().map(value).collect());
    }

    fn got(&self) -> Vec<Mark> {
        self.db
            .prepare(
                "SELECT identity, gameId, source, sourceKey FROM owed_dirty ORDER BY 1, 2, 3, 4",
            )
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    fn clear(&self) {
        self.db.execute("DELETE FROM owed_dirty", []).unwrap();
    }

    fn check(
        &self,
        label: &str,
        sql: &str,
        binds: Vec<Value>,
        want: Vec<Mark>,
    ) -> Result<(), String> {
        self.clear();
        let mut stmt = self
            .db
            .prepare(sql)
            .unwrap_or_else(|e| panic!("{label}: {e}: {sql}"));
        let memory = stmt.get_status(rusqlite::StatementStatus::MemUsed);
        let mut rows = stmt.query(params_from_iter(binds)).unwrap();
        while rows
            .next()
            .unwrap_or_else(|e| panic!("{label}: {e}: {sql}"))
            .is_some()
        {}
        drop(rows);
        self.programs.borrow_mut().push((
            label.into(),
            sql.into(),
            memory,
            stmt.get_status(rusqlite::StatementStatus::VmStep),
            stmt.get_status(rusqlite::StatementStatus::FullscanStep),
        ));
        let got = self.got();
        if got != want {
            return Err(format!("{label}: dirty marks {got:?}, expected {want:?}"));
        }
        Ok(())
    }

    fn check_query(&self, label: &str, q: &Query, want: Vec<Mark>) -> Result<(), String> {
        self.check(label, q.sql(), q.params().iter().map(value).collect(), want)
    }
}

fn seat(id: &str, txid: &str) -> PotpartyRecord {
    PotpartyRecord {
        identity: id.into(),
        opponent_identity: "other".into(),
        game_id: "game".into(),
        pot_txid: "pot".into(),
        pot_vout: 0,
        recovery_height: 100,
        sig_hex: "00".into(),
        seat_settle_pubkey: Some("03aa".into()),
        seat_sig_hex: Some("00".into()),
        txid: txid.into(),
        output_index: 0,
        created_at: 1,
    }
}

fn hop() -> HoppartyRecord {
    HoppartyRecord {
        identity: "alice".into(),
        opponent_identity: "bob".into(),
        game_id: "game".into(),
        hop_vout: 0,
        hop_sats: 1000,
        seat_settle_pubkey: "03aa".into(),
        seat_sig_hex: "00".into(),
        identity_sig_hex: "00".into(),
        hop_lock_hex: None,
        hop_sats_on_chain: None,
        container_outputs: 2,
        txid: "hop".into(),
        output_index: 1,
        created_at: 1,
        size_bytes: None,
        fee_sats: None,
    }
}

fn pot_binds(txid: &str, kind: &str) -> Vec<Value> {
    vec![
        txid.to_string().into(),
        0.into(),
        0.into(),
        Value::Null,
        0.into(),
        1.into(),
        kind.to_string().into(),
        "03aa".to_string().into(),
        "03bb".to_string().into(),
        "03cc".to_string().into(),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        100.into(),
        2000.into(),
        1.into(),
    ]
}

/// The source paths share a fixture, but every checked statement starts with an empty dirty table.
fn paths(f: &Fixture) -> Result<(), String> {
    use low_app_layer::{handfile, hopsweep, owed, record_post as record, tracker_host::stored};
    let a = seat("alice", "party-a");
    let b = seat("bob", "party-b");
    let hp = hop();
    f.check_query(
        "party admission",
        discovery::potparty_write::potparty_insert_query(&a, 1).query(),
        marks("party", "party-a", &["alice"]),
    )?;
    f.check_query(
        "party admission replay",
        discovery::potparty_write::potparty_insert_query(&a, 1).query(),
        vec![],
    )?;
    f.check_query(
        "party relatch",
        discovery::potparty_write::potparty_relatch_query(&a).query(),
        marks("party", "party-a", &["alice"]),
    )?;
    f.query(discovery::potparty_write::potparty_insert_query(&b, 1).query());
    f.check_query(
        "hop admission",
        discovery::hopparty_write::hopparty_insert_query(&hp, 1).query(),
        marks("hop", "hop:0", &["alice"]),
    )?;
    f.check_query(
        "hop relatch",
        discovery::hopparty_write::hopparty_relatch_query(&hp).query(),
        marks("hop", "hop:0", &["alice"]),
    )?;
    f.check(
        "hop funding admitted after its marker",
        discovery::store_record_sql(),
        pot_binds("hop", "p2pkh"),
        marks("pot", "hop:0", &["alice"]),
    )?;
    f.check(
        "pot admitted after its parties",
        discovery::store_record_sql(),
        pot_binds("pot", "covenant"),
        marks("pot", "pot:0", &["alice", "bob"]),
    )?;
    f.check(
        "pot admission upsert",
        discovery::store_record_sql(),
        pot_binds("pot", "covenant"),
        marks("pot", "pot:0", &["alice", "bob"]),
    )?;
    f.check(
        "JOIN catch-up records its hop",
        discovery::MARK_SPENT_IF_UNPOINTED_SQL,
        vec![
            "pot".to_string().into(),
            1.into(),
            "hop".to_string().into(),
            0.into(),
        ],
        marks("pot", "hop:0", &["alice"]),
    )?;
    for (confirmed, verdict, params) in [
        (
            false,
            false,
            vec![
                "settle".to_string().into(),
                "settle".to_string().into(),
                2.into(),
                2.into(),
                "pot".to_string().into(),
                0.into(),
            ],
        ),
        (
            false,
            true,
            vec![
                "settle".to_string().into(),
                "winner-a".to_string().into(),
                "settle".to_string().into(),
                "coop".to_string().into(),
                "settle".to_string().into(),
                1.into(),
                1.into(),
                "pot".to_string().into(),
                0.into(),
            ],
        ),
        (
            true,
            false,
            vec![
                "settle".to_string().into(),
                "settle".to_string().into(),
                10.into(),
                10.into(),
                "settle".to_string().into(),
                1.into(),
                1.into(),
                "pot".to_string().into(),
                0.into(),
            ],
        ),
        (
            true,
            true,
            vec![
                "settle".to_string().into(),
                "winner-a".to_string().into(),
                "settle".to_string().into(),
                "coop".to_string().into(),
                "settle".to_string().into(),
                10.into(),
                10.into(),
                "settle".to_string().into(),
                1.into(),
                1.into(),
                "pot".to_string().into(),
                0.into(),
            ],
        ),
    ] {
        f.check(
            "pot spend",
            discovery::mark_spent_sql(confirmed, verdict),
            params,
            marks("pot", "pot:0", &["alice", "bob"]),
        )?;
    }
    f.check(
        "pot verdict",
        discovery::verdict_cas_sql(),
        vec![
            "winner-a".to_string().into(),
            "settle".to_string().into(),
            "coop".to_string().into(),
            "pot".to_string().into(),
            0.into(),
            "settle".to_string().into(),
        ],
        marks("pot", "pot:0", &["alice", "bob"]),
    )?;
    more_pot_writers(f)?;
    f.check(
        "funding seen",
        bsv_overlay_cloudflare::ops::NETWORK_SEEN_INSERT_SQL,
        vec![
            "pot".to_string().into(),
            "arcade".to_string().into(),
            3000.into(),
        ],
        marks("seen", "pot", &["alice", "bob"]),
    )?;
    f.check(
        "spend seen",
        bsv_overlay_cloudflare::ops::NETWORK_SEEN_INSERT_SQL,
        vec![
            "settle".to_string().into(),
            "arcade".to_string().into(),
            3000.into(),
        ],
        marks("seen", "settle", &["alice", "bob"]),
    )?;
    f.check(
        "filed party",
        record::POTPARTY_FILE_SQL,
        vec![
            "alice".to_string().into(),
            "bob".to_string().into(),
            "game".to_string().into(),
            "pot".to_string().into(),
            0.into(),
            100.into(),
            "00".to_string().into(),
            "03aa".to_string().into(),
            "00".to_string().into(),
            "filed:party".to_string().into(),
            0.into(),
            1.into(),
            1.into(),
        ],
        marks("party", "filed:party", &["alice"]),
    )?;
    f.check(
        "filed hop",
        handfile::HOPPARTY_HANDFILE_FILE_SQL,
        vec![
            "alice".to_string().into(),
            "bob".to_string().into(),
            "game".to_string().into(),
            0.into(),
            1000.into(),
            "03aa".to_string().into(),
            "00".to_string().into(),
            "00".to_string().into(),
            Value::Null,
            Value::Null,
            2.into(),
            "hop".to_string().into(),
            2_147_483_648_i64.into(),
            1.into(),
            1.into(),
        ],
        marks("hop", "hop:0", &["alice"]),
    )?;
    let refund_sql = literal(DISCOVERY, "impl PotrefundStorage for D1PotrefundStorage");
    f.check(
        "chain refund admission",
        &refund_sql,
        vec![
            "outsider".to_string().into(),
            "game".to_string().into(),
            "pot".to_string().into(),
            0.into(),
            "00".to_string().into(),
            "00".to_string().into(),
            "refund".to_string().into(),
            0.into(),
            1.into(),
        ],
        marks("refund", "refund", &["alice", "bob", "outsider"]),
    )?;
    f.check(
        "filed refund",
        record::POTREFUND_FILE_SQL,
        vec![
            "alice".to_string().into(),
            "game".to_string().into(),
            "pot".to_string().into(),
            0.into(),
            "00".to_string().into(),
            "00".to_string().into(),
            "filed:refund".to_string().into(),
            0.into(),
            1.into(),
            0.into(),
        ],
        marks("refund", "filed:refund", &["alice", "bob"]),
    )?;
    f.check(
        "filed refund latch",
        record::POTREFUND_LATCH_SQL,
        vec!["filed:refund".to_string().into()],
        marks("refund", "filed:refund", &["alice", "bob"]),
    )?;
    f.check(
        "chain refund latch",
        record::POTREFUND_CHAIN_LATCH_SQL,
        vec!["refund".to_string().into(), 0.into()],
        marks("refund", "refund", &["alice", "bob", "outsider"]),
    )?;
    let result = ResultRecord {
        game_id: "game".into(),
        winner: "alice".into(),
        loser: "bob".into(),
        pot_txid: "pot".into(),
        settle_txid: "settle".into(),
        winner_sig_hex: "00".into(),
        loser_sig_hex: None,
        cards_hex: None,
        txid: "result".into(),
        output_index: 0,
        created_at: 1,
    };
    f.check_query(
        "chain result",
        discovery::result_write::result_insert_query(&result, 1).query(),
        marks("result", "result", &["alice", "bob"]),
    )?;
    f.check(
        "filed result",
        record::RESULT_FILE_SQL,
        vec![
            "game".to_string().into(),
            "alice".to_string().into(),
            "bob".to_string().into(),
            "pot".to_string().into(),
            "settle".to_string().into(),
            "00".to_string().into(),
            Value::Null,
            Value::Null,
            "filed:result".to_string().into(),
            0.into(),
            1.into(),
            0.into(),
        ],
        marks("result", "filed:result", &["alice", "bob"]),
    )?;
    let collected = literal(DISCOVERY, "impl CollectedStorage for D1CollectedStorage");
    f.check(
        "chain collected",
        &collected,
        vec![
            "alice".to_string().into(),
            "game".to_string().into(),
            "collected".to_string().into(),
            0.into(),
            "00".to_string().into(),
            1.into(),
        ],
        marks("collected", "collected", &["alice"]),
    )?;
    f.check(
        "filed collected",
        record::COLLECTED_FILE_SQL,
        vec![
            "alice".to_string().into(),
            "game".to_string().into(),
            "filed:collected".to_string().into(),
            0.into(),
            "00".to_string().into(),
            1.into(),
            Value::Null,
        ],
        marks("collected", "filed:collected", &["alice"]),
    )?;
    f.check(
        "filed held",
        record::COLLECTED_FILE_SQL,
        vec![
            "alice".to_string().into(),
            "game".to_string().into(),
            "filed:held".to_string().into(),
            0.into(),
            "00".to_string().into(),
            1.into(),
            "pay".to_string().into(),
        ],
        marks("collected", "filed:held", &["alice"]),
    )?;
    f.check(
        "filed sweep",
        hopsweep::HOPSWEEP_FILE_SQL,
        vec![
            "alice".to_string().into(),
            "game".to_string().into(),
            "hop".to_string().into(),
            0.into(),
            "sweep".to_string().into(),
            "00".to_string().into(),
            "filed:sweep".to_string().into(),
            0.into(),
            1.into(),
        ],
        marks("sweep", "filed:sweep", &["alice"]),
    )?;
    f.execute(
        owed::OWED_ROW_INSERT_SQL,
        vec![
            "alice".to_string().into(),
            "pot:0".to_string().into(),
            "payout".to_string().into(),
            "game".to_string().into(),
            1000.into(),
            "bob".to_string().into(),
            10.into(),
            "{}".to_string().into(),
            1000.into(),
            "paid".to_string().into(),
        ],
    );
    f.execute("INSERT INTO owed_row_words (identity,outpoint,role,txid) VALUES ('alice','pot:0','pay','pay')", vec![]);
    for (word, at) in [("seen", 1), ("mined", 2), ("stale", 3)] {
        f.check(
            word,
            stored::UPSERT,
            vec![
                "pay".to_string().into(),
                word.to_string().into(),
                "chain".to_string().into(),
                10.into(),
                "root".to_string().into(),
                10.into(),
                "00".to_string().into(),
                "header".to_string().into(),
                Value::Null,
                Value::Null,
                at.into(),
                Value::Null,
            ],
            marks("word", "pay", &["alice"]),
        )?;
    }
    f.check(
        "word before a row names it",
        stored::UPSERT,
        vec![
            "settle".to_string().into(),
            "mined".to_string().into(),
            "chain".to_string().into(),
            10.into(),
            "root".to_string().into(),
            10.into(),
            "00".to_string().into(),
            "header".to_string().into(),
            Value::Null,
            Value::Null,
            1.into(),
            Value::Null,
        ],
        marks("word", "settle", &["alice", "bob"]),
    )?;
    f.check(
        "stranger word",
        stored::UPSERT,
        vec![
            "unrelated".to_string().into(),
            "seen".to_string().into(),
            "chain".to_string().into(),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            1.into(),
            Value::Null,
        ],
        vec![],
    )?;
    f.check(
        "spender bytes",
        owed::OWED_SPENDER_BYTES_UPSERT_SQL,
        vec![
            "pay".to_string().into(),
            "{}".to_string().into(),
            1000.into(),
        ],
        marks("word", "pay", &["alice"]),
    )?;
    f.check(
        "same spender bytes",
        owed::OWED_SPENDER_BYTES_UPSERT_SQL,
        vec![
            "pay".to_string().into(),
            "{}".to_string().into(),
            1000.into(),
        ],
        vec![],
    )?;
    let beef_admit = literal(DISCOVERY, "pub(crate) const POT_BEEF_ADMIT_WRITE_SQL:");
    f.check(
        "funding beef",
        &beef_admit,
        vec![
            "pot".to_string().into(),
            Value::Blob(vec![0]),
            "pot".to_string().into(),
            1.into(),
            0.into(),
            "pot".to_string().into(),
        ],
        marks("beef", "pot", &["alice", "bob"]),
    )?;
    let beef_verified = literal(DISCOVERY, "pub(crate) const POT_BEEF_VERIFIED_WRITE_SQL:");
    f.check(
        "verified beef replaces admitted beef",
        &beef_verified,
        vec![
            "pot".to_string().into(),
            Value::Blob(vec![0]),
            1.into(),
            10.into(),
        ],
        marks("beef", "pot", &["alice", "bob"]),
    )?;
    let beef_proven = literal(DISCOVERY, "pub(crate) const POT_BEEF_MARK_PROVEN_SQL:");
    f.check(
        "beef latch",
        &beef_proven,
        vec!["pot".to_string().into()],
        marks("beef", "pot", &["alice", "bob"]),
    )?;
    let batch_source = &DISCOVERY[DISCOVERY
        .find("pub(crate) fn pot_beef_mark_proven_batch_sql(")
        .unwrap()..];
    let bound = discovery_bound("POT_BEEF_MARK_PROVEN_CHUNK");
    let batch = sql_literal(batch_source, "UPDATE pot_beefs SET proof_verified")
        .replace("{placeholders}", &vec!["?"; bound].join(", "));
    f.check(
        "beef batch latch at its production chunk bound",
        &batch,
        vec!["pot".to_string().into(); bound],
        marks("beef", "pot", &["alice", "bob"]),
    )?;
    let anchored = literal(DISCOVERY, "pub(crate) const POT_BEEF_MARK_PROVEN_AT_SQL:");
    f.check(
        "beef anchored latch",
        &anchored,
        vec![10.into(), "pot".to_string().into()],
        marks("beef", "pot", &["alice", "bob"]),
    )?;
    let batch_source = &DISCOVERY[DISCOVERY
        .find("pub(crate) fn pot_beef_mark_proven_at_batch_sql(")
        .unwrap()..];
    let arms = literal(batch_source, "let arms = vec![");
    let bound = discovery_bound("POT_BEEF_MARK_PROVEN_AT_CHUNK");
    let batch = sql_literal(batch_source, "UPDATE pot_beefs SET proof_verified")
        .replace("{arms}", &vec![arms; bound].join(" "))
        .replace("{placeholders}", &vec!["?"; bound].join(", "));
    let mut bindings = Vec::new();
    for _ in 0..bound {
        bindings.extend(["pot".to_string().into(), 10.into()]);
    }
    bindings.extend(vec!["pot".to_string().into(); bound]);
    f.check(
        "beef anchored batch latch at its production chunk bound",
        &batch,
        bindings,
        marks("beef", "pot", &["alice", "bob"]),
    )?;
    f.check(
        "beef reorg",
        discovery::unlatch_pot_beef_sql(),
        vec!["pot".to_string().into()],
        marks("beef", "pot", &["alice", "bob"]),
    )?;
    use bsv_overlay_cloudflare::submit_refusals as refusal;
    f.check(
        "door refusal",
        refusal::SUBMIT_REFUSAL_UPSERT_SQL,
        vec![
            "hop".to_string().into(),
            0.into(),
            "refused".to_string().into(),
            "rejected".to_string().into(),
            1.into(),
        ],
        marks("refusal", "hop:0", &["alice"]),
    )?;
    f.check(
        "door refusal replaced",
        refusal::SUBMIT_REFUSAL_UPSERT_SQL,
        vec![
            "hop".to_string().into(),
            0.into(),
            "refused-again".to_string().into(),
            "rejected".to_string().into(),
            2.into(),
        ],
        marks("refusal", "hop:0", &["alice"]),
    )?;
    f.check(
        "refusal aged out",
        refusal::SUBMIT_REFUSALS_GC_SQL,
        vec![3.into()],
        marks("refusal", "hop:0", &["alice"]),
    )?;
    f.execute(
        refusal::SUBMIT_REFUSAL_UPSERT_SQL,
        vec![
            "hop".to_string().into(),
            0.into(),
            "refused".to_string().into(),
            "rejected".to_string().into(),
            4.into(),
        ],
    );
    f.check(
        "refusal retired by admission",
        refusal::SUBMIT_REFUSALS_RETIRE_SQL,
        vec!["pot".to_string().into()],
        marks("refusal", "hop:0", &["alice"]),
    )?;
    eviction_paths(f)?;
    // Lane 3b will bind this new table in the eviction's batch. There is no production writer to call yet.
    f.check("released hop evidence", "INSERT INTO released_hop_evidence (hopTxid,hopVout,identity,gameId,potTxid,seatSettlePubkey,hopSats,evictedAt) VALUES (?,?,?,?,?,?,?,?)",
        vec!["hop".to_string().into(), 0.into(), "alice".to_string().into(), "game".to_string().into(), "pot".to_string().into(), "03aa".to_string().into(), 1000.into(), 4000.into()], marks("hop", "hop:0", &["alice"]))?;
    // No shipped writer deletes a hop marker. The specified DELETE trigger also protects a future owner cleanup.
    f.check(
        "hop marker deleted",
        "DELETE FROM hopparty_records WHERE txid = ?1",
        vec!["hop".to_string().into()],
        marks("hop", "hop:0", &["alice"]),
    )?;
    Ok(())
}

struct Shadow<'a>(&'a Fixture);
#[async_trait(?Send)]
impl ShadowDb for Shadow<'_> {
    async fn columns(&self, table: &str) -> Result<Vec<ColumnInfo>, String> {
        let mut stmt = self
            .0
            .db
            .prepare(&format!("PRAGMA table_info(\"{table}\")"))
            .unwrap();
        Ok(stmt
            .query_map([], |r| {
                Ok(ColumnInfo {
                    name: r.get(1)?,
                    ty: r.get(2)?,
                    notnull: r.get(3)?,
                    dflt_value: r.get(4)?,
                })
            })
            .unwrap()
            .map(Result::unwrap)
            .collect())
    }
    async fn count(&self, table: &str, key: &str, txid: &str) -> Result<u64, String> {
        self.0
            .db
            .query_row(
                &format!("SELECT COUNT(*) FROM \"{table}\" WHERE \"{key}\" = ?1"),
                [txid],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())
    }
    async fn vouts(&self, table: &str, txid: &str) -> Result<Vec<u32>, String> {
        let mut stmt = self
            .0
            .db
            .prepare(&format!(
                "SELECT outputIndex FROM \"{table}\" WHERE txid = ?1"
            ))
            .unwrap();
        Ok(stmt
            .query_map([txid], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect())
    }
    async fn exec(&self, sql: &str, binds: Vec<QVal>) -> Result<(), String> {
        let mut stmt = self.0.db.prepare(sql).map_err(|e| e.to_string())?;
        stmt.execute(params_from_iter(binds.iter().map(value)))
            .map_err(|e| e.to_string())?;
        self.0.programs.borrow_mut().push((
            "shadow move/readmission".into(),
            sql.into(),
            stmt.get_status(rusqlite::StatementStatus::MemUsed),
            stmt.get_status(rusqlite::StatementStatus::VmStep),
            stmt.get_status(rusqlite::StatementStatus::FullscanStep),
        ));
        Ok(())
    }
    async fn query_i64(&self, sql: &str, binds: Vec<QVal>) -> Result<Option<i64>, String> {
        self.0
            .db
            .query_row(sql, params_from_iter(binds.iter().map(value)), |r| r.get(0))
            .optional()
            .map(|v| v.flatten())
            .map_err(|e| e.to_string())
    }
}

fn run<F: std::future::Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match future.as_mut().poll(&mut cx) {
        std::task::Poll::Ready(v) => v,
        _ => panic!("SQLite does not wait"),
    }
}

fn eviction_paths(f: &Fixture) -> Result<(), String> {
    f.check(
        "open eviction",
        admit_fast::OPEN_EVICTION_MARKER_SQL,
        vec![
            "pot".to_string().into(),
            "rejected".to_string().into(),
            4000.into(),
        ],
        marks("eviction", "pot", &["alice", "bob"]),
    )?;
    f.clear();
    let moved = run(admit_fast::shadow_move_passes(
        &Shadow(f),
        "pot",
        "rejected",
        4000,
        false,
    ));
    assert!(moved.incomplete.is_empty(), "{:?}", moved.incomplete);
    let mut want = marks("pot", "pot:0", &["alice", "bob"]);
    want.extend(marks("party", "party-a", &["alice"]));
    want.extend(marks("party", "party-b", &["bob"]));
    want.extend(marks("party", "filed:party", &["alice"]));
    want.sort();
    if f.got() != want {
        return Err(format!(
            "eviction shadow move: dirty marks {:?}, expected {want:?}",
            f.got()
        ));
    }
    let columns = run(Shadow(f).columns("pot_records")).unwrap();
    f.check(
        "eviction releases hops",
        &admit_fast::release_pot_spends_sql(&columns),
        vec!["pot".to_string().into()],
        marks("pot", "hop:0", &["alice"]),
    )?;
    let released = serde_json::to_string(&vec![admit_fast::ReleasedSpend {
        table: "pot_records".into(),
        txid: "hop".into(),
        vout: 0,
    }])
    .unwrap();
    f.check(
        "close eviction after shadow move",
        admit_fast::CLOSE_EVICTION_MARKER_SQL,
        vec![
            "pot".to_string().into(),
            "rejected".to_string().into(),
            4000.into(),
            5.into(),
            released.into(),
        ],
        marks("eviction", "pot", &["alice", "bob"]),
    )?;
    f.clear();
    let (_, faults) = run(admit_fast::restore_twins(&Shadow(f), "pot"));
    assert!(faults.is_empty(), "{faults:?}");
    // The restored pot precedes its parties, so those INSERTs are independently responsible for their marks.
    let mut restored = marks("party", "party-a", &["alice"]);
    restored.extend(marks("party", "party-b", &["bob"]));
    restored.extend(marks("party", "filed:party", &["alice"]));
    restored.extend(marks("refund", "refund", &["alice", "bob", "outsider"]));
    restored.extend(marks("refund", "filed:refund", &["alice", "bob"]));
    restored.extend(marks("result", "result", &["alice", "bob"]));
    restored.extend(marks("result", "filed:result", &["alice", "bob"]));
    restored.sort();
    if f.got() != restored {
        return Err(format!(
            "readmission restore: dirty marks {:?}, expected {restored:?}",
            f.got()
        ));
    }
    let (remark, at) = admit_fast::remark_pot_spend_sql(&columns);
    assert!(at);
    f.check(
        "readmission remarks hop",
        &remark,
        vec![
            "pot".to_string().into(),
            5000.into(),
            "hop".to_string().into(),
            0.into(),
        ],
        marks("pot", "hop:0", &["alice"]),
    )?;
    // This exact literal is the readmission's stamp, extracted at its statement rather than copied.
    let stamp = sql_literal(
        include_str!("../../overlay-cloudflare/src/admit_fast.rs"),
        "UPDATE pot_evictions SET readmittedAt = ? WHERE txid = ? AND readmittedAt IS NULL",
    );
    f.check(
        "readmission stamp",
        &stamp,
        vec![5000.into(), "pot".to_string().into()],
        marks("eviction", "pot", &["alice", "bob"]),
    )?;
    let clear = sql_literal(
        include_str!("../../overlay-cloudflare/src/admit_fast.rs"),
        "UPDATE pot_evictions SET readmittedAt = NULL WHERE txid = ?",
    );
    f.check(
        "readmission reversal",
        &clear,
        vec!["pot".to_string().into()],
        marks("eviction", "pot", &["alice", "bob"]),
    )?;
    f.check(
        "readmission stamp restored",
        &stamp,
        vec![6000.into(), "pot".to_string().into()],
        marks("eviction", "pot", &["alice", "bob"]),
    )?;
    Ok(())
}

fn more_pot_writers(f: &Fixture) -> Result<(), String> {
    let want = || marks("pot", "pot:0", &["alice", "bob"]);
    let text = |s: &str| Value::Text(s.to_string());
    f.check(
        "spend proof",
        discovery::confirm_spend_cas_sql(),
        vec![10.into(), text("pot"), 0.into(), text("settle")],
        want(),
    )?;
    f.check(
        "proof reanchor",
        discovery::reanchor_confirmed_cas_sql(),
        vec![11.into(), text("pot"), 0.into(), text("settle")],
        want(),
    )?;
    f.check(
        "height-bound proof demotion",
        discovery::demote_confirmed_at_cas_sql(),
        vec![text("pot"), 0.into(), text("settle"), 11.into()],
        want(),
    )?;
    f.check(
        "confirmed displacement",
        discovery::displace_spend_cas_sql(),
        vec![
            text("settle"),
            12.into(),
            1.into(),
            text("pot"),
            0.into(),
            text("settle"),
        ],
        want(),
    )?;
    f.check(
        "proof demotion",
        discovery::demote_confirmed_cas_sql(),
        vec![text("pot"), 0.into(), text("settle")],
        want(),
    )?;
    f.check(
        "proof relatch",
        discovery::confirm_spend_cas_sql(),
        vec![12.into(), text("pot"), 0.into(), text("settle")],
        want(),
    )?;
    f.check(
        "stale proof CAS",
        discovery::confirm_spend_cas_sql(),
        vec![12.into(), text("pot"), 0.into(), text("wrong-spender")],
        vec![],
    )?;
    f.check(
        "funding facts",
        discovery::pot_funding_facts_fill_sql(),
        vec![200.into(), 10.into(), text("pot"), 0.into()],
        want(),
    )?;
    f.check(
        "spender facts",
        discovery::spender_facts_cas_sql(),
        vec![
            text("settle"),
            200.into(),
            200.into(),
            text("settle"),
            10.into(),
            10.into(),
            text("settle"),
            text("pot"),
            0.into(),
            text("settle"),
        ],
        want(),
    )?;
    f.check(
        "spender payouts",
        discovery::spender_payouts_cas_sql(),
        vec![1000.into(), 0.into(), text("pot"), 0.into(), text("settle")],
        want(),
    )?;
    f.check(
        "block-time stamp",
        discovery::POT_BLOCK_TIME_STAMP_SQL,
        vec![1.into(), 1.into(), text("pot")],
        want(),
    )?;
    Ok(())
}

#[test]
fn owed893_every_production_source_statement_marks_only_its_subjects() {
    paths(&Fixture::new(None)).unwrap();
}

#[test]
fn owed893_a_pot_arriving_after_its_hops_marks_the_linked_subject_without_trigger_ordering() {
    let f = Fixture::new(None);
    let mut hp = hop();
    hp.identity = "carol".into();
    let query = discovery::hopparty_write::hopparty_insert_query(&hp, 1);
    let mut binds: Vec<_> = query.query().params().iter().map(value).collect();
    // The admission's actual last bind is its signature verdict. Supply a verified marker to exercise links.
    *binds.last_mut().unwrap() = 1.into();
    f.execute(query.query().sql(), binds);
    f.execute(discovery::store_record_sql(), pot_binds("hop", "p2pkh"));
    f.execute(
        discovery::MARK_SPENT_IF_UNPOINTED_SQL,
        vec![
            Value::Text("pot".into()),
            1.into(),
            Value::Text("hop".into()),
            0.into(),
        ],
    );
    f.check(
        "pot arrives after hop and marker",
        discovery::store_record_sql(),
        pot_binds("pot", "covenant"),
        marks("pot", "pot:0", &["carol"]),
    )
    .unwrap();
    assert_eq!(
        f.db.query_row("SELECT COUNT(*) FROM hop_pot_links", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    f.check(
        "linked subject on pot update",
        discovery::pot_funding_facts_fill_sql(),
        vec![200.into(), 10.into(), Value::Text("pot".into()), 0.into()],
        marks("pot", "pot:0", &["carol"]),
    )
    .unwrap();
}

#[test]
fn owed893_each_source_trigger_removed_alone_turns_the_production_writes_red() {
    let f = Fixture::new(None);
    let names: Vec<String> = f.db.prepare("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'owed_dirty_%' ORDER BY name").unwrap()
        .query_map([], |r| r.get(0)).unwrap().map(Result::unwrap).collect();
    assert_eq!(
        names.len(),
        26,
        "fourteen source tables, one trigger per listed verb plus the refusal UPSERT's UPDATE"
    );
    for name in names {
        let err = paths(&Fixture::new(Some(&name)))
            .expect_err(&format!("{name} removed but every write stayed green"));
        assert!(
            err.contains("dirty marks"),
            "the missing trigger must lose a mark: {err}"
        );
        println!("RED {name}: {err}");
    }
}

#[test]
fn owed893_a_faulted_source_statement_leaves_neither_its_write_nor_a_dirty_mark() {
    let f = Fixture::new(None);
    f.query(discovery::potparty_write::potparty_insert_query(&seat("alice", "party-a"), 1).query());
    f.execute(discovery::store_record_sql(), pot_binds("pot", "covenant"));
    f.clear();
    f.db.execute_batch("CREATE TEMP TRIGGER fail_dirty AFTER INSERT ON owed_dirty BEGIN SELECT RAISE(ABORT,'dirty fault'); END;").unwrap();
    let sql = discovery::mark_spent_sql(false, false);
    let binds = vec![
        "settle".to_string().into(),
        "settle".to_string().into(),
        1.into(),
        1.into(),
        "pot".to_string().into(),
        0.into(),
    ];
    let err = f.db.execute(sql, params_from_iter(&binds)).unwrap_err();
    assert!(err.to_string().contains("dirty fault"), "{err}");
    assert_eq!(
        f.db.query_row("SELECT spent FROM pot_records WHERE txid='pot'", [], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap(),
        0
    );
    assert!(f.got().is_empty());
    f.db.execute_batch("DROP TRIGGER fail_dirty").unwrap();
    f.check(
        "same source write succeeds",
        sql,
        binds,
        marks("pot", "pot:0", &["alice"]),
    )
    .unwrap();
}

#[test]
fn owed893_a_word_upsert_coalesces_a_mark_already_waiting_for_the_judge() {
    let f = Fixture::new(None);
    f.query(discovery::potparty_write::potparty_insert_query(&seat("alice", "party-a"), 1).query());
    f.execute(discovery::store_record_sql(), pot_binds("pot", "covenant"));
    f.clear();
    for (word, at) in [("seen", 1), ("mined", 2), ("stale", 3)] {
        f.execute(
            low_app_layer::tracker_host::stored::UPSERT,
            vec![
                Value::Text("pot".into()),
                Value::Text(word.into()),
                Value::Text("chain".into()),
                10.into(),
                Value::Text("root".into()),
                10.into(),
                Value::Text("00".into()),
                Value::Text("header".into()),
                Value::Null,
                Value::Null,
                at.into(),
                Value::Null,
            ],
        );
        assert_eq!(f.got(), marks("word", "pot", &["alice"]));
    }
    assert_eq!(
        f.db.query_row("SELECT markedAtMs FROM owed_dirty", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn owed893_a_moved_pot_key_marks_both_old_and_new_subjects() {
    let f = Fixture::new(None);
    let a = seat("alice", "party-a");
    let mut b = seat("bob", "party-b");
    b.pot_txid = "pot-new".into();
    for party in [&a, &b] {
        f.query(discovery::potparty_write::potparty_insert_query(party, 1).query());
    }
    f.execute(discovery::store_record_sql(), pot_binds("pot", "covenant"));
    let mut want = marks("pot", "pot:0", &["alice"]);
    want.extend(marks("pot", "pot-new:0", &["bob"]));
    want.sort();
    // Production keeps this key fixed; the design also requires OLD and NEW keys to be covered.
    f.check(
        "pot key moved",
        "UPDATE pot_records SET txid=?1 WHERE txid=?2 AND outputIndex=0",
        vec![Value::Text("pot-new".into()), Value::Text("pot".into())],
        want,
    )
    .unwrap();
}

/// Includes every existing trigger, the production UPSERT's two programs and the shadow move.
/// The fleet has 14,719 pots, 7,549 hop markers and 27,000 network witnesses.
#[test]
fn owed893_trigger_programs_measured_358144_bytes_stay_below_384_kib() {
    const PREPARED_MAX: i32 = 384 * 1024;
    const VM_MAX: i32 = 4096;
    const SCAN_MAX: i32 = 8; // Small shadow twins; source-table lookups take zero fullscan steps.
    let trigger = bsv_overlay_cloudflare::derived_owed::POT_UPDATE;
    for index in [
        "idx_potparty_pot",
        "idx_hopparty_hop",
        "idx_hop_pot_links_pot",
        "idx_pot_spending_lower",
    ] {
        assert!(
            trigger.contains(&format!("INDEXED BY {index}")),
            "a missing {index} hint is a cost regression"
        );
    }
    let f = Fixture::new(None);
    f.db.execute_batch("BEGIN").unwrap();
    for row in d1_fleet::rows(1_790_000_000) {
        d1_fleet::execute_row(&f.db, &row);
    }
    f.db.execute_batch("COMMIT").unwrap();
    paths(&f).unwrap();
    let measured = f.programs.borrow();
    for (label, _, memory, steps, scans) in measured.iter() {
        println!("{label}: {memory} prepared bytes, {steps} VM steps, {scans} fullscan steps");
    }
    let peak = measured.iter().map(|m| m.2).max().unwrap();
    let vm_peak = measured.iter().map(|m| m.3).max().unwrap();
    let scan_peak = measured.iter().map(|m| m.4).max().unwrap();
    println!("PEAK: {peak} prepared bytes, {vm_peak} VM steps, {scan_peak} fullscan steps");
    assert!(
        peak <= PREPARED_MAX,
        "{peak} prepared bytes, ceiling {PREPARED_MAX}"
    );
    assert!(vm_peak <= VM_MAX, "{vm_peak} VM steps, ceiling {VM_MAX}");
    assert!(
        scan_peak <= SCAN_MAX,
        "{scan_peak} fullscan steps, ceiling {SCAN_MAX}"
    );
    drop(measured);

    // Cost RED: force the missing-hint failure mode on the real source statement and populated fleet.
    // The hint assertion above also rejects merely deleting INDEXED BY, even if SQLite happens to choose it.
    let missing = trigger.replace("INDEXED BY idx_potparty_pot", "NOT INDEXED");
    f.db.execute_batch("DROP TRIGGER owed_dirty_pot_records_update")
        .unwrap();
    f.db.execute_batch(&missing).unwrap();
    f.clear();
    let mut stmt =
        f.db.prepare(discovery::pot_funding_facts_fill_sql())
            .unwrap();
    stmt.execute(rusqlite::params![200, 10, "pot", 0]).unwrap();
    let scans = stmt.get_status(rusqlite::StatementStatus::FullscanStep);
    println!("RED forced missing party index: {scans} fullscan steps (ceiling {SCAN_MAX})");
    assert!(
        scans > SCAN_MAX,
        "the cost pin must reject a scan of the fleet"
    );
}
