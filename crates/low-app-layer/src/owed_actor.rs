//! bsv-low #912 (lane D2-ACTOR-B, `DESIGN-D2-ACTOR-2026-10-26.md` sections 4 to 8): the owed judge as an actor per
//! identity. `OwedActor` (a Durable Object, `new_sqlite_classes`, named by `idFromName` of the LOWERCASED identity
//! key, LOW-1) holds a MIRROR of the record's evidence for its identity in its own SQLite and judges there with the
//! SAME judge (`owed_judge::judge_with_write`, the pure `derive_owed_rows` inside it) over a [`JudgeDb`] whose SQL
//! runs on the mirror and whose transaction facts come from the mirror's `txfacts` table (MEDIUM-3: no bytes held).
//!
//! THE MIRROR. Its tables are the record's own tables the judge reads (section 5), their DDL generated from the
//! overlay's migration list (`owed_actor::schema`, pinned equal to it), so the judge's statements run unchanged.
//! An apply for (identity, games) refreshes that domain from the record by a KEY CLOSURE: each family is copied by
//! its indexed key columns (the identity and its games, the txids, the hop marker rowids), the keys taken from the
//! judge's own statements (their binds and their answers) and from the rows copied, round after round, until a round
//! adds no key (at most [`CLOSURE_ROUNDS_MAX`] rounds, each ONE batch of the record). The copy is a superset of what
//! the judge reads, and on a superset its statements answer as on the record (each is keyed by what the closure
//! holds). Each family's local rows matching a round's predicate are replaced by the record's (a row the record
//! deleted, an eviction's move, leaves the mirror too). The byte-identity pin
//! (`d2_actor_b_the_actor_stores_the_d1_judges_rows`) is what proves it, on the frozen fixtures.
//!
//! THE SECTION (MEDIUM-2). A Durable Object's input gate stays open across a fetch await, so the object holds ONE
//! turn (`apply_in_turn`, a mutex in the object): every apply, its closure's record reads and outside answers (the
//! word views' header rechecks, the canonical roots) included, runs inside it, and an event arriving meanwhile waits
//! and is applied after (pin `d2_actor_b_two_events_under_a_slow_chaintracks_derive_the_newer_rows`, RED with the
//! turn removed: the older judgement's claimable `hop-stranded` row stored over the newer `unbound` one). The judgement that stands is the FIXPOINT round's: the round whose statements named no key the mirror
//! did not hold. The closure starts from what the mirror already names for the games (closed locally, no record
//! read) and the last judgement's own txids, so a steady-state apply is ONE round: one record batch, one local
//! write, one judgement.
//!
//! ORDER (MEDIUM-1). An apply reads the record as it stands, so its rows are the record's whatever order the events
//! came in (pin `d2_actor_b_an_out_of_order_pair_derives_the_same_rows`); `applied (kind, seq)` is the redelivery
//! idempotency; the `txfacts` rows an event carries are last-writer-by-seq per row.
use crate::owed_judge::{FactsFrom, JudgeDb, TxFacts};
use crate::pot_hook::{Bind, SqlPort, Stmt, StmtOut};
use serde_json::{json, Value};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub mod object;
#[rustfmt::skip]
pub mod schema;

/// The binding of the class (`wrangler.toml`, `[durable_objects]`).
pub const BINDING: &str = "OWED_ACTOR";
/// The KV namespace of the display copy (MEDIUM-9).
pub const KV_BINDING: &str = "OWED_COPY";
/// Phase B's flag (section 6): the read serves the actor's rows. Off until E4.
pub const READ_FLAG: &str = "OWED_ACTOR_READ";
/// Phase C's flag: `false` removes the D1 judge from the consumer (the actor applies; the marks are acked after it).
pub const D1_JUDGE_FLAG: &str = "OWED_D1_JUDGE";
/// The rounds of one closure. A round that adds no key ends it; past the bound the apply is a fault (retried).
pub const CLOSURE_ROUNDS_MAX: usize = 8;
/// The keys one closure may hold (txids); past it the apply is a fault, never a judgement over a partial mirror.
pub const CLOSURE_KEYS_MAX: usize = 4_096;
/// The games of one message (section 4): a message naming more is split by the consumer.
pub const GAMES_PER_APPLY: usize = 64;
/// The direct call's bound after a writer's answer (the amended bar: under 1 s p95).
pub const DIRECT_CALL_WALL_MS: u64 = 2_000;
/// The served hard ceiling of an apply (the gauge beside the p95).
pub const APPLY_CEILING_MS: u64 = 5_000;
/// At most one KV copy a second per identity (KV's one write a second per key).
pub const KV_COPY_MIN_INTERVAL_MS: u64 = 1_000;
/// The reason a display copy is served with (Rule 23: never "nothing owed" by silence).
pub const KV_COPY_REASON: &str = "the judge is unreachable; re-read";

/// One flag of the render, read `true` only for the literal `true`.
pub fn flag_on(value: Option<String>) -> bool {
    value.is_some_and(|v| v.trim() == "true")
}
/// `OWED_D1_JUDGE` defaults ON (Phases A and B): only the literal `false` turns it off.
pub fn d1_judge_on(value: Option<String>) -> bool {
    value.is_none_or(|v| v.trim() != "false")
}
/// LOW-1: the object's name is the lowercased 66-hex identity key; anything else is refused before a stub is made.
pub fn actor_name(identity: &str) -> Option<String> {
    let id = identity.trim().to_ascii_lowercase();
    (id.len() == 66 && id.bytes().all(|b| b.is_ascii_hexdigit())).then_some(id)
}

/// One key column of a family: what selects a record row into the mirror.
#[derive(Debug, Clone, Copy)]
pub enum Key {
    /// `identity = me AND game IN games`.
    Mine(&'static str, &'static str),
    /// `game IN games` (the identity-free result families).
    Games(&'static str),
    /// A txid column, `IN keys`.
    Txid(&'static str),
    /// A hop marker's rowid, `IN rowids` (the links and the seats).
    Marker(&'static str),
    /// The probe memo's outpoint ranges (`txid:vout`, `homeproof:txid.`, `proofmined:txid.`, the reorg mark).
    ProbeRange,
}

/// One family of the mirror: a record table the judge reads, its keys, the columns held as hex (blobs), and whether
/// its BEEF is decoded into `txfacts` (never held).
#[derive(Debug, Clone, Copy)]
pub struct Family {
    pub table: &'static str,
    pub keys: &'static [Key],
    pub beef: bool,
}

use Key::*;
/// The judge's families (section 5), every table its statements read on the frozen fixtures
/// (`schema::tests::the_mirror_names_every_table_the_judge_reads`).
pub const FAMILIES: &[Family] = &[
    Family {
        table: "potparty_records",
        keys: &[Mine("identity", "gameId"), Txid("potTxid"), Txid("txid")],
        beef: false,
    },
    Family {
        table: "hopparty_records",
        keys: &[Mine("identity", "gameId"), Txid("txid")],
        beef: false,
    },
    Family {
        table: "potparty_records_evicted",
        keys: &[Mine("identity", "gameId"), Txid("potTxid")],
        beef: false,
    },
    Family {
        table: "collected_markers_v2",
        keys: &[Mine("identity", "gameId"), Txid("txid")],
        beef: false,
    },
    Family {
        table: "hopsweep_records",
        keys: &[Mine("identity", "gameId"), Txid("hopTxid"), Txid("txid")],
        beef: false,
    },
    Family {
        table: "released_hop_evidence",
        keys: &[Mine("identity", "gameId"), Txid("hopTxid")],
        beef: false,
    },
    Family {
        table: "released_hop_evidence_missing",
        keys: &[Mine("identity", "gameId"), Txid("hopTxid")],
        beef: false,
    },
    Family {
        table: "potrefund_records",
        keys: &[Mine("identity", "gameId"), Txid("potTxid"), Txid("txid")],
        beef: false,
    },
    Family {
        table: "result_markers_v2",
        keys: &[Games("gameId"), Txid("potTxid"), Txid("txid")],
        beef: false,
    },
    Family {
        table: "hand_markers",
        keys: &[Games("gameId"), Txid("txid")],
        beef: false,
    },
    Family {
        table: "proof_markers",
        keys: &[Games("gameId"), Txid("txid")],
        beef: false,
    },
    Family {
        table: "proof_posts",
        keys: &[Games("gameId")],
        beef: false,
    },
    Family {
        table: "hop_pot_links",
        keys: &[Marker("markerRowid"), Txid("potTxid")],
        beef: false,
    },
    Family {
        table: "pot_hop_seats",
        keys: &[Marker("markerRowid"), Txid("potTxid")],
        beef: false,
    },
    Family {
        table: "pot_records",
        keys: &[Txid("txid"), Txid("spendingTxid")],
        beef: false,
    },
    Family {
        table: "pot_records_evicted",
        keys: &[Txid("txid")],
        beef: false,
    },
    Family {
        table: "pot_evictions",
        keys: &[Txid("txid")],
        beef: false,
    },
    Family {
        table: "submit_refusals",
        keys: &[Txid("hopTxid")],
        beef: false,
    },
    Family {
        table: "network_seen",
        keys: &[Txid("txid")],
        beef: false,
    },
    Family {
        table: "owed_spender_bytes",
        keys: &[Txid("txid")],
        beef: false,
    },
    Family {
        table: "tx_words",
        keys: &[Txid("txid")],
        beef: false,
    },
    Family {
        table: "hop_chain_probes",
        keys: &[ProbeRange],
        beef: false,
    },
    Family {
        table: "pot_beefs",
        keys: &[Txid("txid")],
        beef: true,
    },
    Family {
        table: "transactions",
        keys: &[Txid("txid")],
        beef: true,
    },
];

/// The probe memo prefixes a txid key reaches (`owed::HOME_SPEND_LATCH_PREFIX`, `owed::PROOF_MINED_LATCH_PREFIX`).
const PROBE_PREFIXES: [&str; 3] = [
    "",
    crate::owed::HOME_SPEND_LATCH_PREFIX,
    crate::owed::PROOF_MINED_LATCH_PREFIX,
];

/// The selection of one family, as a FROM clause aliased `t`: `?1` me, `?2` games, `?3` txids, `?4` marker rowids,
/// `?5` probe ranges. The record answers `SELECT ... FROM <this>`; the mirror deletes `rowid IN (SELECT t.rowid ...)`.
pub fn selection(f: &Family) -> String {
    if let [ProbeRange] = f.keys {
        return format!(
            "json_each(?5) k CROSS JOIN {} t ON t.outpoint>=k.value AND t.outpoint<k.value||'~'",
            f.table
        );
    }
    let arms: Vec<String> = f
        .keys
        .iter()
        .map(|k| match k {
            Mine(i, g) => format!(
                "SELECT rowid FROM {} WHERE {i}=?1 AND {g} IN (SELECT value FROM json_each(?2))",
                f.table
            ),
            Games(g) => format!(
                "SELECT rowid FROM {} WHERE {g} IN (SELECT value FROM json_each(?2))",
                f.table
            ),
            Txid(c) => format!(
                "SELECT rowid FROM {} WHERE {c} IN (SELECT value FROM json_each(?3))",
                f.table
            ),
            Marker(c) => format!(
                "SELECT rowid FROM {} WHERE {c} IN (SELECT value FROM json_each(?4))",
                f.table
            ),
            ProbeRange => unreachable!("a probe range is its family's only key"),
        })
        .collect();
    format!("{} t WHERE t.rowid IN ({})", f.table, arms.join(" UNION "))
}

/// The record's read of one family: the rowid, the columns (a blob as hex), and the decoded BEEF's hex apart.
pub fn copy_select(f: &Family) -> String {
    let cols = schema::columns(f.table);
    let mut out: Vec<String> = vec!["DISTINCT t.rowid AS \"__rowid\"".into()];
    for c in cols {
        if schema::is_blob(f.table, c) {
            if !(f.beef && *c == "beef") {
                out.push(format!("hex(t.{c}) AS \"{c}\""));
            }
        } else {
            out.push(format!("t.{c} AS \"{c}\""));
        }
    }
    if f.beef {
        out.push("hex(t.beef) AS \"__beef\"".into());
    }
    format!("SELECT {} FROM {}", out.join(","), selection(f))
}

/// The mirror's insert of one copied row: the record's rowid kept (the judge orders by marker rowids), a blob back
/// from hex, a BEEF never held (an empty blob in its place).
pub fn insert_sql(f: &Family) -> String {
    let cols = schema::columns(f.table);
    let names: Vec<String> = std::iter::once("rowid".to_string())
        .chain(cols.iter().map(|c| c.to_string()))
        .collect();
    let vals: Vec<String> = std::iter::once("?1".to_string())
        .chain(cols.iter().enumerate().map(|(n, c)| {
            if f.beef && *c == "beef" {
                format!("coalesce(?{}, x'')", n + 2)
            } else if schema::is_blob(f.table, c) {
                format!("unhex(?{})", n + 2)
            } else {
                format!("?{}", n + 2)
            }
        }))
        .collect();
    format!(
        "INSERT OR REPLACE INTO {}({}) VALUES ({})",
        f.table,
        names.join(","),
        vals.join(",")
    )
}

/// The binds a statement numbers (`?1` to its highest `?N`): SQLite counts its parameters by the highest index.
fn fitted(sql: String, binds: &[Bind]) -> Stmt {
    let b = sql.as_bytes();
    let mut high = 0usize;
    for (i, c) in b.iter().enumerate() {
        if *c == b'?' {
            let n: String = b[i + 1..]
                .iter()
                .take_while(|d| d.is_ascii_digit())
                .map(|d| *d as char)
                .collect();
            high = high.max(n.parse().unwrap_or(0));
        }
    }
    Stmt::new(sql, binds[..high.min(binds.len())].to_vec())
}

fn bind_of(v: &Value) -> Bind {
    match v {
        Value::Null => Bind::Null,
        Value::Bool(b) => Bind::Int(i64::from(*b)),
        Value::Number(n) => n
            .as_i64()
            .map_or_else(|| Bind::Text(n.to_string()), Bind::Int),
        Value::String(s) => Bind::Text(s.clone()),
        other => Bind::Text(other.to_string()),
    }
}

/// Every maximal run of exactly 64 hex digits in `s`, lowercased: the txids a statement or a row names (a 66-hex
/// identity key is no txid; a `txid:vout` or a memo key holds one).
pub fn txids_in(s: &str, out: &mut BTreeSet<String>) {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i].is_ascii_hexdigit() {
            let start = i;
            while i < b.len() && b[i].is_ascii_hexdigit() {
                i += 1;
            }
            if i - start == 64 {
                out.insert(s[start..i].to_ascii_lowercase());
            }
        } else {
            i += 1;
        }
    }
}
fn txids_in_value(v: &Value, out: &mut BTreeSet<String>) {
    match v {
        Value::String(s) => txids_in(s, out),
        Value::Array(a) => a.iter().for_each(|x| txids_in_value(x, out)),
        Value::Object(m) => m.values().for_each(|x| txids_in_value(x, out)),
        _ => {}
    }
}

/// What the closure holds: the txids and the hop marker rowids.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Keys {
    pub txids: BTreeSet<String>,
    pub markers: BTreeSet<i64>,
}
impl Keys {
    fn binds(&self, me: &str, games: &[String]) -> Vec<Bind> {
        let probes: Vec<String> = self
            .txids
            .iter()
            .flat_map(|t| PROBE_PREFIXES.iter().map(move |p| format!("{p}{t}")))
            .chain(std::iter::once("reorg-clear".to_string()))
            .collect();
        vec![
            Bind::Text(me.to_string()),
            Bind::Text(json!(games).to_string()),
            Bind::Text(json!(self.txids).to_string()),
            Bind::Text(json!(self.markers).to_string()),
            Bind::Text(json!(probes).to_string()),
        ]
    }
}

/// The local SQL with the keys of every statement it runs noted (binds and answers).
pub struct Noting<'a, L: SqlPort> {
    pub local: &'a L,
    pub seen: RefCell<BTreeSet<String>>,
}
impl<L: SqlPort> SqlPort for Noting<'_, L> {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        {
            let mut seen = self.seen.borrow_mut();
            for s in &stmts {
                for b in &s.binds {
                    if let Bind::Text(t) = b {
                        txids_in(t, &mut seen);
                    }
                }
            }
        }
        let out = self.local.run_batch(stmts).await?;
        let mut seen = self.seen.borrow_mut();
        for o in &out {
            o.rows.iter().for_each(|r| txids_in_value(r, &mut seen));
        }
        Ok(out)
    }
    async fn counters(&self, deltas: Vec<(String, u64)>) {
        self.local.counters(deltas).await;
    }
    fn now_ms(&self) -> u64 {
        self.local.now_ms()
    }
    fn log(&self, line: &str) {
        self.local.log(line);
    }
}

/// What the judge asks outside the mirror: the word views (their header rechecks) and the canonical roots. The
/// worker's is the CHAINTRACKS binding (`object.rs`); the pins' answer as the D1 judge's fakes do.
#[allow(async_fn_in_trait)]
pub trait Outside {
    async fn word_views(
        &self,
        rows: &BTreeMap<String, crate::tracker_host::stored::Row>,
    ) -> Result<BTreeMap<String, Value>, String>;
    async fn canonical_root(&self, height: u64) -> Option<String>;
}

/// The outside answers one closure took, held for the last judgement (MEDIUM-2): no await of it yields.
#[derive(Default)]
pub struct Held {
    pub views: RefCell<BTreeMap<String, Value>>,
    pub roots: RefCell<HashMap<u64, Option<String>>>,
    /// An answer the last judgement asked that no round took: the closure did not reach its fixpoint for it.
    pub missed: RefCell<bool>,
}

/// The actor's [`JudgeDb`]: the judge's SQL on the mirror, the facts from `txfacts`, the outside asked through
/// `outside` (a round) or answered from `held` (the last judgement, `outside` `None`). Pushes are collected.
pub struct ActorJudge<'a, L: SqlPort, O: Outside> {
    pub local: &'a L,
    pub outside: Option<&'a O>,
    pub held: &'a Held,
    pub tip: Option<u64>,
    pub era_ms: Option<i64>,
    pub pushes: RefCell<Vec<(String, Value)>>,
}
impl<L: SqlPort, O: Outside> SqlPort for ActorJudge<'_, L, O> {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        self.local.run_batch(stmts).await
    }
    async fn counters(&self, deltas: Vec<(String, u64)>) {
        self.local.counters(deltas).await;
    }
    fn now_ms(&self) -> u64 {
        self.local.now_ms()
    }
    fn log(&self, line: &str) {
        self.local.log(line);
    }
}
impl<L: SqlPort, O: Outside> JudgeDb for ActorJudge<'_, L, O> {
    fn tip(&self) -> Option<u64> {
        self.tip
    }
    fn era_ms(&self) -> Option<i64> {
        self.era_ms
    }
    async fn word_views(
        &self,
        rows: &BTreeMap<String, crate::tracker_host::stored::Row>,
    ) -> Result<BTreeMap<String, Value>, String> {
        if let Some(o) = self.outside {
            let v = o.word_views(rows).await?;
            self.held.views.borrow_mut().extend(v.clone());
            return Ok(v);
        }
        let held = self.held.views.borrow();
        let mut out = BTreeMap::new();
        for k in rows.keys() {
            match held.get(k) {
                Some(v) => {
                    out.insert(k.clone(), v.clone());
                }
                None => *self.held.missed.borrow_mut() = true,
            }
        }
        Ok(out)
    }
    async fn canonical_root(&self, height: u64) -> Option<String> {
        if let Some(o) = self.outside {
            let r = o.canonical_root(height).await;
            self.held.roots.borrow_mut().insert(height, r.clone());
            return r;
        }
        match self.held.roots.borrow().get(&height) {
            Some(r) => r.clone(),
            None => {
                *self.held.missed.borrow_mut() = true;
                None
            }
        }
    }
    async fn push(&self, identity: &str, body: Value) {
        self.pushes.borrow_mut().push((identity.to_string(), body));
    }
    async fn tx_facts(&self, txid: &str, from: FactsFrom) -> Result<Option<TxFacts>, String> {
        let sql = match from {
            FactsFrom::PotBeefs => TXFACTS_POT_SQL,
            FactsFrom::Stored => TXFACTS_STORED_SQL,
        };
        let out = self
            .local
            .run_batch(vec![Stmt::new(
                sql,
                vec![Bind::Text(txid.to_ascii_lowercase())],
            )])
            .await?;
        Ok(out
            .first()
            .and_then(|o| o.rows.first())
            .and_then(|r| r["facts"].as_str())
            .and_then(|s| serde_json::from_str::<Value>(s).ok())
            .and_then(|v| TxFacts::from_json(&v)))
    }
}

/// The decoded facts the mirror holds, one row per (txid, where it was read): `pot` (the pot's BEEF), `stored` (the
/// first stored BEEF carrying it, `owed_judge::STORED_BEEFS_SQL`'s order), `event` (a `txfacts` event). Per row
/// last-writer-by-seq (MEDIUM-1).
pub const TXFACTS_CREATE: &str = "CREATE TABLE IF NOT EXISTS txfacts (txid TEXT NOT NULL, src TEXT NOT NULL, facts TEXT NOT NULL, seq INTEGER NOT NULL, PRIMARY KEY (txid, src))";
pub const TXFACTS_UPSERT_SQL: &str = "INSERT INTO txfacts(txid,src,facts,seq) VALUES (?1,?2,?3,?4) ON CONFLICT(txid,src) DO UPDATE SET facts=excluded.facts,seq=excluded.seq WHERE excluded.seq>=txfacts.seq";
pub const TXFACTS_DELETE_SQL: &str = "DELETE FROM txfacts WHERE src IN ('pot','stored') AND txid IN (SELECT value FROM json_each(?1))";
const TXFACTS_POT_SQL: &str = "SELECT facts FROM txfacts WHERE txid=?1 AND src='pot'";
const TXFACTS_STORED_SQL: &str = "SELECT facts FROM txfacts WHERE txid=?1 AND src IN ('stored','event') ORDER BY src='event' LIMIT 1";
/// The redelivery idempotency (MEDIUM-1) and the actor's own state (the ack watermark, the gate, the reconcile).
pub const APPLIED_CREATE: &str = "CREATE TABLE IF NOT EXISTS applied (kind TEXT NOT NULL, seq INTEGER NOT NULL, atMs INTEGER NOT NULL, PRIMARY KEY (kind, seq))";
pub const META_CREATE: &str = "CREATE TABLE IF NOT EXISTS meta (name TEXT PRIMARY KEY, value TEXT)";
pub const APPLIED_SQL: &str =
    "INSERT INTO applied(kind,seq,atMs) VALUES (?1,?2,?3) ON CONFLICT DO NOTHING RETURNING seq";
pub const META_PUT_SQL: &str = "INSERT INTO meta(name,value) VALUES (?1,?2) ON CONFLICT(name) DO UPDATE SET value=excluded.value";
pub const META_GET_SQL: &str = "SELECT value FROM meta WHERE name=?1";

/// The mirror's DDL, then the actor's own tables, every statement idempotent.
pub fn mirror_ddl() -> Vec<String> {
    schema::MIRROR_DDL
        .iter()
        .map(|s| s.to_string())
        .chain([TXFACTS_CREATE, APPLIED_CREATE, META_CREATE].map(str::to_string))
        .collect()
}

/// One closure's account (served on `/health.owedActor` through the object's meters).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ApplyReport {
    pub rounds: usize,
    pub txids: usize,
    pub record_rows: usize,
    pub rows: usize,
    pub missed: bool,
    pub skipped_redelivery: bool,
}

/// The decoded facts of a copied BEEF row: the pot's own (`pot`, `pot_beefs` only) and the stored order's.
fn facts_rows(
    family: &Family,
    rows: &[Value],
    seq: i64,
    pending: &mut BTreeMap<(String, &'static str), (i64, Option<TxFacts>)>,
) {
    for r in rows {
        let Some(txid) = r["txid"].as_str().map(str::to_ascii_lowercase) else {
            continue;
        };
        let facts = r["__beef"]
            .as_str()
            .and_then(|h| crate::owed_judge::facts_of_beefs(&txid, [h]));
        if family.table == "pot_beefs" {
            pending.insert((txid.clone(), "pot"), (seq, facts.clone()));
        }
        // `stored`: the first stored BEEF carrying it, pot_beefs before transactions.
        let entry = pending.entry((txid, "stored")).or_insert((seq, None));
        if entry.1.is_none() {
            entry.1 = facts;
        }
    }
}

/// One round's record read: every family's selection over the closure's keys, ONE batch.
async fn read_round(
    record: &impl SqlPort,
    me: &str,
    games: &[String],
    keys: &Keys,
) -> Result<Vec<Vec<Value>>, String> {
    let binds = keys.binds(me, games);
    let stmts = FAMILIES
        .iter()
        .map(|f| fitted(copy_select(f), &binds))
        .collect();
    Ok(record
        .run_batch(stmts)
        .await?
        .into_iter()
        .map(|o| o.rows)
        .collect())
}

/// The mirror's write of one round: each family's domain replaced by the record's rows, the BEEFs' facts decoded.
fn write_round(
    me: &str,
    games: &[String],
    keys: &Keys,
    read: &[Vec<Value>],
    seq: i64,
) -> Vec<Stmt> {
    let binds = keys.binds(me, games);
    let mut out = Vec::new();
    let mut facts = BTreeMap::new();
    for (f, rows) in FAMILIES.iter().zip(read) {
        out.push(fitted(
            format!(
                "DELETE FROM {} WHERE rowid IN (SELECT t.rowid FROM {})",
                f.table,
                selection(f)
            ),
            &binds,
        ));
        let cols = schema::columns(f.table);
        for r in rows {
            let mut b = vec![bind_of(&r["__rowid"])];
            b.extend(cols.iter().map(|c| {
                if f.beef && *c == "beef" {
                    Bind::Null
                } else {
                    bind_of(&r[*c])
                }
            }));
            out.push(Stmt::new(insert_sql(f), b));
        }
        if f.beef {
            facts_rows(f, rows, seq, &mut facts);
        }
    }
    let beef_txids: Vec<String> = facts
        .keys()
        .map(|(t, _)| t.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if !beef_txids.is_empty() {
        out.push(Stmt::new(
            TXFACTS_DELETE_SQL,
            vec![Bind::Text(json!(beef_txids).to_string())],
        ));
    }
    for ((txid, src), (seq, f)) in facts {
        if let Some(f) = f {
            out.push(Stmt::new(
                TXFACTS_UPSERT_SQL,
                vec![
                    Bind::Text(txid),
                    Bind::Text(src.into()),
                    Bind::Text(f.to_json().to_string()),
                    Bind::Int(seq),
                ],
            ));
        }
    }
    out
}

/// The keys the mirror's rows name now: the txids of every family's rows in the domain, the marker rowids of the
/// identity's hop markers.
async fn keys_of_mirror(
    local: &impl SqlPort,
    me: &str,
    games: &[String],
    keys: &Keys,
    into: &mut Keys,
) -> Result<(), String> {
    let binds = keys.binds(me, games);
    let mut stmts: Vec<Stmt> = FAMILIES
        .iter()
        .map(|f| {
            let cols: Vec<String> = schema::columns(f.table)
                .iter()
                .filter(|c| !schema::is_blob(f.table, c))
                .map(|c| format!("t.{c}"))
                .collect();
            fitted(
                format!("SELECT {} FROM {}", cols.join(","), selection(f)),
                &binds,
            )
        })
        .collect();
    stmts.push(Stmt::new(
        "SELECT rowid AS r FROM hopparty_records WHERE identity=?1",
        vec![Bind::Text(me.into())],
    ));
    let out = local.run_batch(stmts).await?;
    for (n, o) in out.iter().enumerate() {
        for r in &o.rows {
            if n == FAMILIES.len() {
                if let Some(id) = r["r"].as_i64() {
                    into.markers.insert(id);
                }
            } else {
                txids_in_value(r, &mut into.txids);
            }
        }
    }
    Ok(())
}

/// The txids the last judgement of these games named (its rows' outpoints and facts, their word roles).
const JUDGED_TXIDS_SQL: &str = "SELECT r.outpoint,r.facts,w.txid FROM owed_rows r LEFT JOIN owed_row_words w ON w.identity=r.identity AND w.outpoint=r.outpoint WHERE r.identity=?1 AND r.gameId IN (SELECT value FROM json_each(?2))";

/// THE APPLY: the closure over `games` of `me`, then the last judgement over the mirror with the outside held.
/// `seq` is the event's (or the bootstrap's) token; a redelivery of an applied `(kind, seq)` returns at once.
#[allow(clippy::too_many_arguments)]
pub async fn apply<R: SqlPort, L: SqlPort, O: Outside>(
    record: &R,
    local: &L,
    outside: &O,
    me: &str,
    games: &[String],
    kind_seq: Option<(&str, i64)>,
    tip: Option<u64>,
    era_ms: Option<i64>,
) -> Result<(ApplyReport, Vec<(String, Value)>), String> {
    let me = me.to_ascii_lowercase();
    let games: Vec<String> = games
        .iter()
        .map(|g| g.to_ascii_lowercase())
        .take(GAMES_PER_APPLY)
        .collect();
    let mut report = ApplyReport::default();
    if let Some((kind, seq)) = kind_seq {
        let seen = local
            .run_batch(vec![Stmt::new(
                "SELECT 1 AS hit FROM applied WHERE kind=?1 AND seq=?2",
                vec![Bind::Text(kind.into()), Bind::Int(seq)],
            )])
            .await?;
        if seen.first().is_some_and(|o| !o.rows.is_empty()) {
            report.skipped_redelivery = true;
            return Ok((report, Vec::new()));
        }
    }
    let seq = kind_seq.map_or(0, |(_, s)| s);
    let held = Held::default();
    // The closure starts from what the mirror already names for these games (the last apply's domain): in the steady
    // state its first round finds no new key and its judgement stands.
    let mut keys = Keys::default();
    for _ in 0..CLOSURE_ROUNDS_MAX {
        let mut next = keys.clone();
        keys_of_mirror(local, &me, &games, &keys, &mut next).await?;
        if next == keys {
            break;
        }
        keys = next;
    }
    let judged = local
        .run_batch(vec![Stmt::new(
            JUDGED_TXIDS_SQL,
            vec![Bind::Text(me.clone()), Bind::Text(json!(games).to_string())],
        )])
        .await?;
    judged
        .iter()
        .flat_map(|o| &o.rows)
        .for_each(|r| txids_in_value(r, &mut keys.txids));
    let pushes = loop {
        if report.rounds >= CLOSURE_ROUNDS_MAX {
            return Err(format!("owed actor: the closure of {me} did not close in {CLOSURE_ROUNDS_MAX} rounds ({} txids)", keys.txids.len()));
        }
        report.rounds += 1;
        let read = read_round(record, &me, &games, &keys).await?;
        report.record_rows += read.iter().map(Vec::len).sum::<usize>();
        local
            .run_batch(write_round(&me, &games, &keys, &read, seq))
            .await?;
        let mut next = keys.clone();
        keys_of_mirror(local, &me, &games, &keys, &mut next).await?;
        let noting = Noting {
            local,
            seen: RefCell::default(),
        };
        let judge = ActorJudge {
            local: &noting,
            outside: Some(outside),
            held: &held,
            tip,
            era_ms,
            pushes: RefCell::default(),
        };
        let mut rows = 0;
        for chunk in games.chunks(crate::owed_judge::OWED_JUDGE_SUBJECTS_PER_INVOCATION) {
            rows = crate::owed_judge::judge_subjects(&judge, &me, chunk, "owed-actor")
                .await?
                .rows;
        }
        let pushes = judge.pushes.take();
        next.txids.extend(noting.seen.take());
        if next.txids.len() > CLOSURE_KEYS_MAX {
            return Err(format!(
                "owed actor: the closure of {me} passed {CLOSURE_KEYS_MAX} txids"
            ));
        }
        if next == keys {
            // THE FIXPOINT: this round's judgement read a mirror holding every key its statements named, so it
            // is the record's judgement and its write stands (the object's turn keeps any other apply out of it).
            report.rows = rows;
            break pushes;
        }
        keys = next;
    };
    report.txids = keys.txids.len();
    if let Some((kind, seq)) = kind_seq {
        local
            .run_batch(vec![
                Stmt::new(
                    APPLIED_SQL,
                    vec![
                        Bind::Text(kind.into()),
                        Bind::Int(seq),
                        Bind::Int(local.now_ms() as i64),
                    ],
                ),
                Stmt::new(
                    META_PUT_SQL,
                    vec![Bind::Text("ackSeq".into()), Bind::Text(seq.to_string())],
                ),
            ])
            .await?;
    }
    Ok((report, pushes))
}

/// MEDIUM-2's in-object serial queue: every apply of one actor takes its turn, so an event arriving during another's
/// outside reads is applied after it (never a judgement of an older closure written over a newer one).
#[allow(clippy::too_many_arguments)]
pub async fn apply_in_turn<R: SqlPort, L: SqlPort, O: Outside>(
    turn: &futures_util::lock::Mutex<()>,
    record: &R,
    local: &L,
    outside: &O,
    me: &str,
    games: &[String],
    kind_seq: Option<(&str, i64)>,
    tip: Option<u64>,
    era_ms: Option<i64>,
) -> Result<(ApplyReport, Vec<(String, Value)>), String> {
    let _turn = turn.lock().await;
    apply(record, local, outside, me, games, kind_seq, tip, era_ms).await
}

/// bsv-low #912 (HIGH-3, Phase C): the actor's dated rows of these games, the questions it owes the chain.
pub const ASK_ROWS_SQL: &str = "SELECT outpoint,gameId,family,facts,reason,probeTip,beltDueAtMs,judgedAtMs,claimable FROM owed_rows WHERE identity=?1 AND gameId IN (SELECT value FROM json_each(?2)) AND beltDueAtMs IS NOT NULL ORDER BY outpoint";
/// The identity's asks of these games that no dated row names any more (`?1`, `?2` the identity's range of the
/// ledger's key, `?3` the games, `?4` the asks kept): taken out of the belt's ledger.
pub const ASKS_CLEAR_SQL: &str = "DELETE FROM owed_belt_state WHERE name>=?1 AND name<?2 AND json_extract(cursor,'$.question.gameId') IN (SELECT value FROM json_each(?3)) AND name NOT IN (SELECT value FROM json_each(?4))";
/// One ask, rewritten only when its question moved (an unchanged re-publish keeps the belt's backoff).
pub const ASK_PUT_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES (?1,?2,?3) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs WHERE owed_belt_state.cursor IS NOT excluded.cursor";

/// PUBLISH THE BELT'S QUESTIONS (HIGH-3, Phase C): every dated row of the judged games is a `belt-ask` in the belt's
/// ledger ([`crate::owed_belt::ask_of_row`]), and an ask no dated row names any more is taken out; ONE record batch.
/// The actor asks no courier: the belt singleton does, at its cadence, through its one gate. Returns the asks held.
pub async fn publish_belt_asks<R: SqlPort, L: SqlPort>(
    record: &R,
    local: &L,
    me: &str,
    games: &[String],
) -> Result<usize, String> {
    let me = me.to_ascii_lowercase();
    let games: Vec<String> = games.iter().map(|g| g.to_ascii_lowercase()).collect();
    let gj = json!(games).to_string();
    let rows = local
        .run_batch(vec![Stmt::new(
            ASK_ROWS_SQL,
            vec![Bind::Text(me.clone()), Bind::Text(gj.clone())],
        )])
        .await?
        .into_iter()
        .next()
        .map(|o| o.rows)
        .unwrap_or_default();
    let asks: Vec<(String, Value, i64)> = rows
        .iter()
        .filter_map(|r| crate::owed_belt::ask_of_row(&me, r))
        .collect();
    let names: Vec<&String> = asks.iter().map(|(n, _, _)| n).collect();
    let mut stmts = vec![Stmt::new(
        ASKS_CLEAR_SQL,
        vec![
            Bind::Text(format!("{}{me} ", crate::owed_belt::BELT_ASK_PREFIX)),
            Bind::Text(format!("{}{me}!", crate::owed_belt::BELT_ASK_PREFIX)),
            Bind::Text(gj),
            Bind::Text(json!(names).to_string()),
        ],
    )];
    for (name, body, due) in &asks {
        stmts.push(Stmt::new(
            ASK_PUT_SQL,
            vec![
                Bind::Text(name.clone()),
                Bind::Text(body.to_string()),
                Bind::Int(*due),
            ],
        ));
    }
    record.run_batch(stmts).await?;
    if !asks.is_empty() {
        record
            .counters(vec![(COUNTERS[8].to_string(), asks.len() as u64)])
            .await;
    }
    Ok(asks.len())
}

/// [`apply_in_turn`], then (Phase C, `publish`) the asks of the games published INSIDE the same turn, so a later
/// apply's asks are never overwritten by an earlier one's.
#[allow(clippy::too_many_arguments)]
pub async fn apply_and_publish_in_turn<R: SqlPort, L: SqlPort, O: Outside>(
    turn: &futures_util::lock::Mutex<()>,
    record: &R,
    local: &L,
    outside: &O,
    me: &str,
    games: &[String],
    kind_seq: Option<(&str, i64)>,
    tip: Option<u64>,
    era_ms: Option<i64>,
    publish: bool,
) -> Result<(ApplyReport, Vec<(String, Value)>), String> {
    let _turn = turn.lock().await;
    let applied = apply(record, local, outside, me, games, kind_seq, tip, era_ms).await?;
    if publish && !applied.0.skipped_redelivery {
        publish_belt_asks(record, local, me, games).await?;
    }
    Ok(applied)
}

/// A `txfacts` event (MEDIUM-3): the transaction's facts as the overlay decoded them (`owed_event::txfacts_of_raw`),
/// held under `event`, last-writer-by-seq.
pub fn txfacts_event_stmt(facts: &Value, seq: i64) -> Option<Stmt> {
    let txid = facts["txid"].as_str()?.to_ascii_lowercase();
    let inputs: Vec<Value> = facts["inputs"]
        .as_array()?
        .iter()
        .filter_map(|k| {
            let (t, v) = k.as_str()?.rsplit_once(':')?;
            Some(json!([t.to_ascii_lowercase(), v.parse::<u32>().ok()?]))
        })
        .collect();
    let outputs: Vec<Value> = facts["outputs"]
        .as_array()?
        .iter()
        .map(|o| json!([o["vout"], o["pkh"], o["sats"], o["kind"] == "pot"]))
        .collect();
    let f = json!({"inputs": inputs, "outputs": outputs});
    TxFacts::from_json(&f)?;
    Some(Stmt::new(
        TXFACTS_UPSERT_SQL,
        vec![
            Bind::Text(txid),
            Bind::Text("event".into()),
            Bind::Text(f.to_string()),
            Bind::Int(seq),
        ],
    ))
}

/// The stored rows of the actor's identity, the word facts stripped as D1 stores them (MEDIUM-4: the read joins
/// the words and rechecks them).
pub const ROWS_SQL: &str = "SELECT identity,outpoint,family,gameId,sats,opponentIdentity,atHeight,facts,reason,judgedAtMs,claimable,probeTip,gateHeight,beltDueAtMs FROM owed_rows WHERE identity=?1 ORDER BY outpoint";

/// Phase A's compare (section 6): the actor's rows against the D1 judge's, by outpoint, each `(family, gameId, sats,
/// opponentIdentity, atHeight, reason, facts)`; the outpoints that differ, sorted.
/// The facts of a stored row as the compares read them: the judgement, not its clock (the facts relative to the
/// judge's `now` taken out, so two judges of one record at two instants compare equal).
pub const CLOCK_FACTS: [&str; 4] = ["ageMs", "chainProbeAgeMs", "beltNextAtMs", "asOfMs"];
pub fn compared_facts(row: &Value) -> Value {
    let mut facts: Value = row["facts"]
        .as_str()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(Value::Null);
    if let Some(m) = facts.as_object_mut() {
        for k in CLOCK_FACTS {
            m.remove(k);
        }
    }
    facts
}

pub fn diff_rows(d1: &[Value], actor: &[Value]) -> Vec<String> {
    fn keyed(rows: &[Value]) -> BTreeMap<String, Value> {
        rows.iter()
            .filter_map(|r| {
                let k = r["outpoint"].as_str()?.to_string();
                let facts = compared_facts(r);
                Some((
                    k,
                    json!([
                        r["family"],
                        r["gameId"],
                        r["sats"],
                        r["opponentIdentity"],
                        r["atHeight"],
                        r["reason"],
                        facts,
                        r["claimable"]
                    ]),
                ))
            })
            .collect()
    }
    let (a, b) = (keyed(d1), keyed(actor));
    a.keys()
        .chain(b.keys())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|k| a.get(*k) != b.get(*k))
        .cloned()
        .collect()
}

/// The KV display copy (MEDIUM-9): the rows, each display-only (`claimable` false, the reason), never an empty list
/// served as "nothing owed": a copy with no rows is served `pending`.
pub fn kv_copy_body(identity: &str, rows: &[Value], judged_at_ms: u64) -> Value {
    json!({"identity": identity, "judgedAtMs": judged_at_ms, "rows": rows})
}
/// The read's answer from a copy, or from no copy (Rule 23).
pub fn display_from_copy(copy: Option<&Value>) -> Value {
    match copy {
        Some(c) if c["rows"].as_array().is_some_and(|r| !r.is_empty()) => {
            let rows: Vec<Value> = c["rows"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|r| {
                    let mut r = r.clone();
                    r["claimable"] = json!(false);
                    r["claimReason"] = json!(KV_COPY_REASON);
                    r
                })
                .collect();
            json!({"rows": rows, "pending": false, "source": "kv-copy", "displayOnly": true, "reason": KV_COPY_REASON, "judgedAtMs": c["judgedAtMs"]})
        }
        _ => json!({"rows": null, "pending": true, "source": "none", "reason": KV_COPY_REASON}),
    }
}

/// Whether a KV copy is due: at most one a second per identity (the alarm coalesces the rest).
pub fn kv_copy_due(last_ms: Option<u64>, now_ms: u64) -> bool {
    last_ms.is_none_or(|l| now_ms.saturating_sub(l) >= KV_COPY_MIN_INTERVAL_MS)
}

/// One latency meter of the bars (MEDIUM-8): a bounded ring of samples, its p50, p95 and maximum.
#[derive(Debug, Default, Clone)]
pub struct Meter {
    samples: Vec<u64>,
    next: usize,
}
pub const METER_SAMPLES: usize = 256;
impl Meter {
    pub fn note(&mut self, ms: u64) {
        if self.samples.len() < METER_SAMPLES {
            self.samples.push(ms);
        } else {
            self.samples[self.next] = ms;
        }
        self.next = (self.next + 1) % METER_SAMPLES;
    }
    pub fn quantile(&self, q: f64) -> Option<u64> {
        if self.samples.is_empty() {
            return None;
        }
        let mut s = self.samples.clone();
        s.sort_unstable();
        let idx = ((q * s.len() as f64).ceil() as usize).clamp(1, s.len()) - 1;
        Some(s[idx])
    }
    pub fn max(&self) -> Option<u64> {
        self.samples.iter().copied().max()
    }
}

/// The served bars (`/health.owedActor`): the apply's p50 and p95 and its ceiling, the rows' visibility p95 after
/// the write (the event's `atMs` to the judgement), the collect-after-mined p95 (a `word` event's mined word to its
/// judgement), the oldest unapplied event's age, and the counts.
#[derive(Debug, Default, Clone)]
pub struct Bars {
    pub apply: Meter,
    pub visible: Meter,
    pub collect_after_mined: Meter,
    pub kv_copies: u64,
    pub bootstraps: u64,
    pub reconcile_diffs: u64,
    pub oldest_unapplied_at_ms: Option<u64>,
}
impl Bars {
    pub fn served(&self, now_ms: u64) -> Value {
        json!({
            "applyMsP50": self.apply.quantile(0.50),
            "applyMsP95": self.apply.quantile(0.95),
            "applyMsMax": self.apply.max(),
            "applyCeilingMs": APPLY_CEILING_MS,
            "rowsVisibleMsP95": self.visible.quantile(0.95),
            "rowsVisibleMsMax": self.visible.max(),
            "collectAfterMinedMsP95": self.collect_after_mined.quantile(0.95),
            "oldestUnappliedEventAgeMs": self.oldest_unapplied_at_ms.map(|a| now_ms.saturating_sub(a)),
            "kvCopies": self.kv_copies,
            "bootstraps": self.bootstraps,
            "reconcileDiffs": self.reconcile_diffs,
        })
    }
}

thread_local! {
    /// THIS isolate's bars (the direct calls and the consumer's deliveries measured where they are made).
    pub static BARS: RefCell<Bars> = RefCell::default();
}
pub fn note_visible(write_at_ms: u64, judged_at_ms: u64) {
    BARS.with(|b| {
        b.borrow_mut()
            .visible
            .note(judged_at_ms.saturating_sub(write_at_ms))
    });
}
pub fn note_apply(ms: u64) {
    BARS.with(|b| b.borrow_mut().apply.note(ms));
}
pub fn served_bars(now_ms: u64) -> Value {
    BARS.with(|b| b.borrow().served(now_ms))
}

/// The sampled digest compare (MEDIUM-10, the reconcile's second question; the exact first, the un-acked marks
/// re-published, is lane A's sweep on the same driver, `owed_events::sweep_publish`): 64 identities a day on the belt
/// singleton's alarm, ONE identity per [`RECONCILE_INTERVAL_MS`], in identity order over `owed_state` (wrapping).
pub const RECONCILE_SAMPLE_PER_DAY: usize = 64;
/// One identity every 22.5 minutes (86,400,000 / 64).
pub const RECONCILE_INTERVAL_MS: i64 = 86_400_000 / RECONCILE_SAMPLE_PER_DAY as i64;
/// The reconcile's place in the belt's ledger: the last identity compared and when.
pub const RECONCILE_STATE_NAME: &str = "reconcile:owed-actor";
pub const RECONCILE_STATE_SQL: &str =
    "SELECT cursor,tickedAtMs FROM owed_belt_state WHERE name='reconcile:owed-actor'";
/// The next identity past the cursor: the least of four key seeks (`owed_state`'s key; the party, hop marker and
/// released evidence identity indexes), so an identity the D1 judge never stated (Phase C) is sampled too.
pub const RECONCILE_NEXT_SQL: &str = "SELECT MIN(i) AS identity FROM (SELECT (SELECT MIN(identity) FROM owed_state WHERE identity>?1) AS i UNION ALL SELECT (SELECT MIN(identity) FROM potparty_records WHERE identity>?1) UNION ALL SELECT (SELECT MIN(identity) FROM hopparty_records WHERE identity>?1) UNION ALL SELECT (SELECT MIN(identity) FROM released_hop_evidence WHERE identity>?1))";
/// The games one reconcile re-applies, at most (four applies of [`GAMES_PER_APPLY`]).
pub const RECONCILE_GAMES_MAX: usize = 256;
/// The stored rows the digest compares (D1's `owed_rows` of one identity, the words stripped as stored).
pub const RECONCILE_D1_ROWS_SQL: &str = ROWS_SQL;

/// One reconcile's account.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReconcileReport {
    pub identity: String,
    /// The two digests compared (Phases A and B: D1's rows against the actor's; Phase C: the actor's rows before
    /// and after a re-apply of its games from the record).
    pub d1_digest: Option<String>,
    pub actor_digest: Option<String>,
    pub diff: bool,
    /// The games re-applied (on a diff; in Phase C, every game of the identity's rows, to the bound).
    pub reapplied: usize,
    /// No actor answered (none bound, or the actor out of reach).
    pub no_actor: bool,
}

fn games_of_rows(rows: &[Value], into: &mut BTreeSet<String>) {
    into.extend(
        rows.iter()
            .filter_map(|r| r["gameId"].as_str().map(str::to_string)),
    );
}

/// THE SAMPLED DIGEST RECONCILE (MEDIUM-10), one step on the belt's driver: at most one identity a
/// [`RECONCILE_INTERVAL_MS`] (64 a day). Phases A and B (`OWED_D1_JUDGE` on): D1's stored rows of the identity
/// against the actor's ([`rows_digest`], no apply); a diff re-applies the differing games (the actor's closure re-reads
/// the record), counted and logged. Phase C (the D1 rows are cold): the actor's rows before and after a re-apply of
/// its games from the record (a divergence the events missed is a diff). D1 reads: the state, one `owed_state` seek,
/// the identity's rows (A and B). Counted `owed_actor_reconcile_{compared,diffs,republished}_total`.
pub async fn reconcile_step(
    db: &impl JudgeDb,
    now: i64,
) -> Result<Option<ReconcileReport>, String> {
    let first = |out: Vec<StmtOut>| out.into_iter().next().map(|o| o.rows).unwrap_or_default();
    let state = first(
        db.run_batch(vec![Stmt::new(RECONCILE_STATE_SQL, vec![])])
            .await?,
    );
    let (cursor, last) = state.first().map_or((String::new(), None), |r| {
        (
            r["cursor"].as_str().unwrap_or_default().to_string(),
            r["tickedAtMs"].as_i64(),
        )
    });
    if last.is_some_and(|l| now.saturating_sub(l) < RECONCILE_INTERVAL_MS) {
        return Ok(None);
    }
    let next = |after: String| async move {
        db.run_batch(vec![Stmt::new(RECONCILE_NEXT_SQL, vec![Bind::Text(after)])])
            .await
            .map(|o| {
                first(o)
                    .first()
                    .and_then(|r| r["identity"].as_str().map(str::to_string))
            })
    };
    let identity = match next(cursor.clone()).await? {
        Some(i) => Some(i),
        None if !cursor.is_empty() => next(String::new()).await?,
        None => None,
    };
    let save = |id: String| async move {
        db.run_batch(vec![Stmt::new(
            crate::owed_belt::STATE_SQL,
            vec![
                Bind::Text(RECONCILE_STATE_NAME.into()),
                Bind::Text(id),
                Bind::Int(now),
            ],
        )])
        .await
    };
    let Some(identity) = identity else {
        save(String::new()).await?;
        return Ok(None);
    };
    let mut report = ReconcileReport {
        identity: identity.clone(),
        ..Default::default()
    };
    let actor = match db.actor_rows(&identity).await {
        Ok(Some(rows)) => rows,
        Ok(None) => {
            report.no_actor = true;
            save(identity).await?;
            return Ok(Some(report));
        }
        Err(e) => {
            db.log(&format!("[owed-actor] reconcile {identity}: {e}"));
            report.no_actor = true;
            save(identity).await?;
            return Ok(Some(report));
        }
    };
    let mut games = BTreeSet::new();
    let compare_to: Vec<Value> = if db.d1_judges() {
        let d1 = first(
            db.run_batch(vec![Stmt::new(
                RECONCILE_D1_ROWS_SQL,
                vec![Bind::Text(identity.clone())],
            )])
            .await?,
        );
        for o in diff_rows(&d1, &actor) {
            for r in d1.iter().chain(&actor).filter(|r| r["outpoint"] == o.as_str()) {
                games_of_rows(std::slice::from_ref(r), &mut games);
            }
        }
        d1
    } else {
        games_of_rows(&actor, &mut games);
        actor.clone()
    };
    let games: Vec<String> = games.into_iter().take(RECONCILE_GAMES_MAX).collect();
    let mut after = actor.clone();
    for chunk in games.chunks(GAMES_PER_APPLY) {
        match db.actor_apply(&identity, chunk, "reconcile").await {
            Ok(Some(a)) => {
                report.reapplied += chunk.len();
                if let Some(rows) = a["rows"].as_array() {
                    after = rows.clone();
                }
            }
            Ok(None) => break,
            Err(e) => {
                db.log(&format!("[owed-actor] reconcile {identity}: re-apply: {e}"));
                break;
            }
        }
    }
    // A and B: D1 against the actor as it stood; C: the actor before against after its re-apply.
    let (a, b) = if db.d1_judges() {
        (compare_to, actor)
    } else {
        (compare_to, after)
    };
    report.d1_digest = Some(rows_digest(&a));
    report.actor_digest = Some(rows_digest(&b));
    report.diff = report.d1_digest != report.actor_digest;
    let mut deltas = vec![(COUNTERS[6].to_string(), 1)];
    if report.diff {
        deltas.push((COUNTERS[7].to_string(), 1));
        BARS.with(|b| b.borrow_mut().reconcile_diffs += 1);
        db.log(&format!(
            "[owed-actor] RECONCILE DIFF {identity}: {} outpoints differ, {} games re-applied",
            diff_rows(&a, &b).len(),
            report.reapplied
        ));
    }
    if report.reapplied > 0 && db.d1_judges() {
        deltas.push((COUNTERS[5].to_string(), report.reapplied as u64));
    }
    db.counters(deltas).await;
    save(identity).await?;
    Ok(Some(report))
}
/// The digest of one identity's rows: the sorted outpoints with their compared fields, hashed.
pub fn rows_digest(rows: &[Value]) -> String {
    let mut keyed: Vec<String> = rows
        .iter()
        .map(|r| {
            let facts = compared_facts(r);
            json!([
                r["outpoint"],
                r["family"],
                r["gameId"],
                r["sats"],
                r["opponentIdentity"],
                r["atHeight"],
                r["reason"],
                facts,
                r["claimable"]
            ])
            .to_string()
        })
        .collect();
    keyed.sort();
    hex::encode(bsv_rs::primitives::hash::sha256(
        keyed.join("\n").as_bytes(),
    ))
}

#[cfg(test)]
mod tests;

/// The counters (seeded by the overlay, `applayer_counters::OWED_ACTOR`, pinned equal).
/// `reconcile_republished`: the games a reconcile's DIFF re-applied (Phases A and B). The last two (Phase C, HIGH-3):
/// the asks the actors published, and the belt's finds handed back to an actor.
pub const COUNTERS: [&str; 10] = [
    "owed_actor_compared_total",
    "owed_actor_diffs_total",
    "owed_actor_faults_total",
    "owed_actor_direct_calls_total",
    "owed_actor_direct_faults_total",
    "owed_actor_reconcile_republished_total",
    "owed_actor_reconcile_compared_total",
    "owed_actor_reconcile_diffs_total",
    "owed_actor_belt_asks_total",
    "owed_belt_handed_back_total",
];
/// The consumer's call to the actor (the queue's delivery), bounded under the message's wall.
pub const CONSUMER_CALL_WALL_MS: u64 = 20_000;

/// The event a delivery carries to the actor: its kind and seq (the redelivery idempotency), its write time (the
/// visibility bar) and whether its word is `mined` (the collect-after-mined bar).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EventCtx {
    pub kind: String,
    pub seq: i64,
    pub at_ms: i64,
    pub mined: bool,
}
impl EventCtx {
    pub fn of(ev: &overlay_discovery::owed_event::OwedEvent) -> Self {
        let word = ev.facts["word"]
            .as_str()
            .or_else(|| ev.facts["frame"]["word"].as_str());
        Self {
            kind: ev.source.clone(),
            seq: ev.seq,
            at_ms: ev.at_ms,
            mined: ev.source == "word" && word == Some("mined"),
        }
    }
}

/// The `/apply` body of one delivery.
pub fn apply_body(identity: &str, games: &[String], e: &EventCtx, tip: Option<u64>) -> Value {
    json!({"identity": identity.to_ascii_lowercase(), "games": games, "kind": e.kind, "seq": e.seq, "atMs": e.at_ms, "tip": tip, "mined": e.mined})
}

fn of_games(rows: &[Value], games: &[String]) -> Vec<Value> {
    rows.iter()
        .filter(|r| {
            r["gameId"]
                .as_str()
                .is_some_and(|g| games.iter().any(|x| x.eq_ignore_ascii_case(g)))
        })
        .cloned()
        .collect()
}

/// PHASE A (section 6): the actor applies beside the D1 judge; its rows for `games` are compared with the D1 judge's
/// just written, counted (`owed_actor_compared_total`, `owed_actor_diffs_total`) with a sampled diff line. The
/// actor's fault never fails the message (`owed_actor_faults_total`).
pub async fn beside(db: &impl JudgeDb, identity: &str, games: &[String]) {
    let answer = match db.actor_deliver(identity, games).await {
        Ok(Some(a)) => a,
        Ok(None) => return,
        Err(e) => {
            db.counters(vec![(COUNTERS[2].into(), 1)]).await;
            db.log(&format!("[owed-actor] FAULT {identity}: {e}"));
            return;
        }
    };
    note_answer(&answer);
    let d1 = match db
        .run_batch(vec![Stmt::new(
            ROWS_SQL,
            vec![Bind::Text(identity.to_ascii_lowercase())],
        )])
        .await
    {
        Ok(mut o) if !o.is_empty() => o.remove(0).rows,
        _ => return,
    };
    let actor: Vec<Value> = answer["rows"].as_array().cloned().unwrap_or_default();
    let diffs = diff_rows(&of_games(&d1, games), &of_games(&actor, games));
    let mut deltas = vec![(COUNTERS[0].to_string(), 1)];
    if !diffs.is_empty() {
        deltas.push((COUNTERS[1].to_string(), 1));
        db.log(&format!(
            "[owed-actor] DIFF {identity}: {} of {} games, first {}",
            diffs.len(),
            games.len(),
            diffs[0]
        ));
    }
    db.counters(deltas).await;
}

/// The fleet's reconcile diffs (`owed_actor_reconcile_diffs_total`), served as `/health.owedActor.reconcileDiffs`:
/// the reconcile runs on the belt's alarm, in its own isolate, so this isolate's bar would read 0.
pub async fn durable_reconcile_diffs(db: &impl SqlPort) -> Option<u64> {
    db.run_batch(vec![Stmt::new(
        "SELECT value FROM ops_counters WHERE name=?1",
        vec![Bind::Text(COUNTERS[7].into())],
    )])
    .await
    .ok()?
    .into_iter()
    .next()?
    .rows
    .first()
    .map_or(Some(0), |r| r["value"].as_u64())
}

/// The bars of one answer, in this isolate.
pub fn note_answer(answer: &Value) {
    if let Some(ms) = answer["applyMs"].as_u64() {
        note_apply(ms);
    }
}

/// PHASE C: the actor is the judge; the marks this delivery covers are acknowledged after it applied (the ack is
/// what empties `owed_dirty`; a mark written meanwhile keeps its newer generation).
pub async fn as_the_judge(
    db: &impl JudgeDb,
    identity: &str,
    games: &[String],
) -> Result<crate::owed_judge::JudgeReport, String> {
    let id = identity.to_ascii_lowercase();
    let gj = json!(games).to_string();
    let marks = db
        .run_batch(vec![Stmt::new(
            crate::owed_judge::MARKS_SQL,
            vec![Bind::Text(id.clone()), Bind::Text(gj.clone())],
        )])
        .await?;
    let seen = marks.first().map_or(0, |o| {
        o.rows
            .iter()
            .filter_map(|m| m["generation"].as_i64())
            .max()
            .unwrap_or(0)
    });
    let answer = db
        .actor_deliver(&id, games)
        .await?
        .ok_or("the D1 judge is off and no owed actor is bound")?;
    note_answer(&answer);
    db.run_batch(vec![Stmt::new(
        crate::owed_judge::ACK_DIRTY_SQL,
        vec![
            Bind::Text(id),
            Bind::Text(gj),
            Bind::Int(seen),
            Bind::Text("[]".into()),
        ],
    )])
    .await?;
    Ok(crate::owed_judge::JudgeReport {
        subjects: games.len(),
        rows: answer["rows"].as_array().map_or(0, Vec::len),
        ..Default::default()
    })
}

/// The word columns of a joined read row (`owed_read::ROWS_SQL`): the read takes them from D1's `tx_words`, never
/// the mirror's (MEDIUM-4).
pub const WORD_COLUMNS: [&str; 12] = [
    "word",
    "tier",
    "height",
    "proof_root",
    "proof_height",
    "proof_hex",
    "header_hash",
    "hint_source",
    "hint_at",
    "updated_at",
    "trackerTerminal",
    "trackerTerminalWord",
];
/// The read's word join over the actor's rows' word roles, one D1 statement.
pub const READ_WORDS_SQL: &str = "SELECT t.txid AS wordTxid,t.word,t.tier,t.height,t.proof_root,t.proof_height,t.proof_hex,t.header_hash,t.hint_source,t.hint_at,t.updated_at,CASE WHEN json_valid(t.legacy_body) THEN json_type(t.legacy_body,'$.trackerTerminal')='true' END AS trackerTerminal,CASE WHEN json_valid(t.legacy_body) THEN json_extract(t.legacy_body,'$.trackerTerminalWord') END AS trackerTerminalWord FROM tx_words t WHERE t.txid IN (SELECT value FROM json_each(?1))";

/// The txids the actor's joined rows name as words.
pub fn word_txids(joined: &[Value]) -> Vec<String> {
    joined
        .iter()
        .filter_map(|r| r["wordTxid"].as_str().map(str::to_string))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}
/// MEDIUM-4: the actor's joined rows with every word column replaced by D1's (absent there: null).
pub fn join_words(joined: &mut [Value], words: &[Value]) {
    let by: HashMap<&str, &Value> = words
        .iter()
        .filter_map(|w| Some((w["wordTxid"].as_str()?, w)))
        .collect();
    for r in joined.iter_mut() {
        let w = r["wordTxid"]
            .as_str()
            .and_then(|t| by.get(t).copied())
            .cloned();
        for c in WORD_COLUMNS {
            r[c] = w.as_ref().map_or(Value::Null, |w| w[c].clone());
        }
    }
}
