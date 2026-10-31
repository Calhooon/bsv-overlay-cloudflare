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
//! did not hold; it is the only one WRITTEN (the lens's HIGH-2: [`Noting`] stages each round's commit,
//! [`commit_fixpoint`] runs the fixpoint's and restores the identity's rows on its fault). The closure starts from what the mirror already names for the games (closed locally, no record
//! read) and the last judgement's own txids, so a steady-state apply is ONE round: one record batch, one local
//! write, one judgement.
//!
//! ORDER (MEDIUM-1). An apply reads the record as it stands, so its rows are the record's whatever order the events
//! came in (pin `d2_actor_b_an_out_of_order_pair_derives_the_same_rows`); `applied (kind, seq, game)` is the
//! redelivery idempotency (the D2-ACTOR-B lens's HIGH-1: one event's games arrive in several calls); the `txfacts`
//! rows an event carries are last-writer-by-seq per row.
//!
//! THE COLD ACTOR (D2-ACTOR-B-FOLD-7, beta's first window of 2026-10-28). The cold path is a MIGRATION and RECOVERY
//! path, never a request path: after the cutover every new identity's first event arrives with no history (its first
//! hop), so its mirror grows with it; a cold actor exists only for a standing identity the lever has not reached
//! (bounded by the lever's pace: `POST /internal/owed-actor-bootstrap`, 50 identities a call, and the fleet's budget of
//! 60 pages a minute), a rebuilt actor (HIGH-4; bounded by the alarm's pace, one page of 16 games a second under the
//! same budget) and a wipe (bounded by the wipe's list, `GET /internal/owed-actor-identities`, reset 50 a call). An
//! actor without `bootstrapped` HOLDS each event in its own `meta` and answers 202 at once ([`apply_or_hold`]); its
//! paged bootstrap applies the held events at its end ([`drain_held`]) and only then stamps `bootstrapped`; the cold
//! time is its own bar (`coldMsP95`). The closure is keyed by the GAMES: the hop markers the domain holds (never the
//! identity's every marker) and a pot row's own txid (its spender only where the judge names it), so a seat whose
//! coins chain through its games is never walked whole; the mirror's own fixpoint is walked to its end before the
//! record's rounds, so a retry starts where the last attempt stopped.
use crate::owed_judge::{FactsFrom, JudgeDb, TxFacts};
use crate::pot_hook::{Bind, SqlPort, Stmt, StmtOut};
use serde_json::{json, Value};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub mod compare;
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
        keys: &[Txid("txid")],
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
///
/// The D2-ACTOR-B lens's HIGH-2: the judge's COMMIT (its one write batch, the batch that holds
/// [`crate::owed_judge::GAME_DELETE_SQL`]) is STAGED here, never run: its binds are noted as a run batch's are, and it
/// answers as an empty judgement would (no row changed, the state read back as 0 rows). The apply runs the staged
/// commits of the FIXPOINT round alone, in one synchronous batch ([`commit_fixpoint`]); a round that is not the
/// fixpoint writes nothing a read can see.
pub struct Noting<'a, L: SqlPort> {
    pub local: &'a L,
    pub seen: RefCell<BTreeSet<String>>,
    pub staged: RefCell<Vec<Stmt>>,
}

/// Is this batch the judge's commit (`owed_judge::judge_with_write`'s plan)?
fn is_commit(stmts: &[Stmt]) -> bool {
    stmts
        .iter()
        .any(|s| s.sql == crate::owed_judge::GAME_DELETE_SQL)
}
/// The judge's state read back at the end of its commit (`owed_judge::judge_with_write`).
fn is_state_readback(s: &Stmt) -> bool {
    s.sql.starts_with("SELECT rows,truncated FROM owed_state")
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
        if is_commit(&stmts) {
            let out = stmts
                .iter()
                .map(|s| StmtOut {
                    changes: Some(0),
                    rows: if is_state_readback(s) {
                        vec![json!({"rows": 0, "truncated": 0})]
                    } else {
                        Vec::new()
                    },
                })
                .collect();
            self.staged.borrow_mut().extend(stmts);
            return Ok(out);
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
    /// FIX-983 (bsv-low #983): the time the outside (CHAINTRACKS) was awaited for this apply, on the local clock: the
    /// apply's `outsideMs` stage, so the judge stage is the derivation and the mirror's own reads.
    pub outside_ms: Cell<u64>,
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
            // FIX-983 (bsv-low #983): the outside is asked ONCE PER APPLY for a key: a later round of the same apply
            // (milliseconds after) takes the held answer, so a two-round apply no longer asks CHAINTRACKS twice for
            // every word; the ask's time is the apply's `outsideMs` stage.
            let mut out: BTreeMap<String, Value> = BTreeMap::new();
            let mut ask: BTreeMap<String, crate::tracker_host::stored::Row> = BTreeMap::new();
            {
                let held = self.held.views.borrow();
                for (k, row) in rows {
                    match held.get(k) {
                        Some(v) => {
                            out.insert(k.clone(), v.clone());
                        }
                        None => {
                            ask.insert(k.clone(), row.clone());
                        }
                    }
                }
            }
            if !ask.is_empty() {
                let t = self.local.now_ms();
                let v = o.word_views(&ask).await?;
                self.held
                    .outside_ms
                    .set(self.held.outside_ms.get() + self.local.now_ms().saturating_sub(t));
                self.held.views.borrow_mut().extend(v.clone());
                out.extend(v);
            }
            return Ok(out);
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
            if let Some(r) = self.held.roots.borrow().get(&height) {
                return r.clone();
            }
            let t = self.local.now_ms();
            let r = o.canonical_root(height).await;
            self.held
                .outside_ms
                .set(self.held.outside_ms.get() + self.local.now_ms().saturating_sub(t));
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
    /// FIX-997: the mirror holds no bytes (the view's hex is empty): the `pot` facts decoded from that row at the copy.
    async fn result_tx_facts(&self, txid: &str, _hex: &str) -> Result<Option<TxFacts>, String> {
        self.tx_facts(txid, FactsFrom::PotBeefs).await
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
/// The D2-ACTOR-B lens's HIGH-1: keyed per GAME, so a game is skipped only when the event itself applied it (one
/// event's games reach the actor in several calls: the consumer's chunks of 16, its singles after a fault).
pub const APPLIED_CREATE: &str = "CREATE TABLE IF NOT EXISTS applied (kind TEXT NOT NULL, seq INTEGER NOT NULL, game TEXT NOT NULL, atMs INTEGER NOT NULL, PRIMARY KEY (kind, seq, game))";
pub const META_CREATE: &str = "CREATE TABLE IF NOT EXISTS meta (name TEXT PRIMARY KEY, value TEXT)";
pub const APPLIED_SQL: &str = "INSERT INTO applied(kind,seq,game,atMs) SELECT ?1,?2,value,?3 FROM json_each(?4) WHERE 1 ON CONFLICT DO NOTHING";
/// LOW-2 (the D2-ACTOR-B lens): the redelivery rows older than [`APPLIED_KEEP_MS`] go.
pub const APPLIED_PRUNE_SQL: &str = "DELETE FROM applied WHERE atMs<?1";
pub const APPLIED_KEEP_MS: i64 = 7 * 86_400_000;

/// D2-ACTOR-D (bsv-low #952): THE COVERAGE of an apply. Every apply reads, BEFORE its closure reads the record, the
/// greatest generation of the marks of its games ([`crate::owed_judge::marks_bound`]) and records it, once the
/// fixpoint's commit landed, as one `applied` row a game under the kind [`COVERED_KIND`] (its `seq` that generation;
/// no new table: the digest does not move). A mark of generation `g` at most that number was written before the read
/// (generations are AUTOINCREMENT: a mark written after it takes a greater one), so its source change is in the
/// record the apply judged. A delivery that names its acknowledgement bound (`bound`, the generation the consumer read
/// before its call) over games each covered at or past it is a DUPE: answered from the seq check in milliseconds,
/// nothing applied, nothing sampled ([`COVERED_SQL`]); the marks it acknowledges were judged by the covering apply.
pub const COVERED_KIND: &str = "covered";
/// D2-ACTOR-B-FOLD-10 (the fifth delta lens's NEW5-M1): the TIP of each game's LAST apply, one `applied` row a game
/// under this kind (its `seq` the tip the apply judged at, `-1` for none; replaced by every apply that records its
/// coverage). The marks are not the apply's only input: a block is an input no mark carries past the tip marks'
/// generations, so a direct apply at a stale tip covered the block's `tip` marks and the consumer's delivery at the
/// block was a dupe, the actor's rows left at the previous height. A delivery is a dupe only when the game's last
/// apply judged at a tip no lower than the delivery's.
pub const COVERED_TIP_KIND: &str = "covered-tip";
/// The last apply's tip of each game replaced (`?1` the games): before [`APPLIED_SQL`] writes the new one.
pub const COVERED_TIP_CLEAR_SQL: &str =
    "DELETE FROM applied WHERE kind='covered-tip' AND game IN (SELECT value FROM json_each(?1))";
/// How many of `?2` (a JSON array of games) an apply covered at or past the bound `?1` and whose LAST apply judged at
/// a tip at least `?3` (`-1`: the delivery names no tip).
pub const COVERED_SQL: &str = "SELECT COUNT(*) AS n FROM (SELECT DISTINCT value AS g FROM json_each(?2)) j WHERE EXISTS (SELECT 1 FROM applied a WHERE a.kind='covered' AND a.seq>=?1 AND a.game=j.g) AND EXISTS (SELECT 1 FROM applied t WHERE t.kind='covered-tip' AND t.seq>=?3 AND t.game=j.g)";
/// FIX-968 (bsv-low #968): the actor's COVERED generation per game (lowercased) for the games `?1` (a JSON array)
/// names: the greatest `applied` `covered` seq of each, the marks bound its last apply read the record at. Answered
/// on `GET /rows?covered=<games>` as `covered: [{game, seq}]` and read by the D1-mark read gate
/// (`owed_read::Snapshot::withholding_mark`): a pending downgrade mark at or below it was judged by the actor whose
/// rows the read serves. No new index (the actor's digest does not move): the `covered` rows of one identity over
/// their seven days.
pub const COVERED_BY_GAME_SQL: &str = "SELECT lower(game) AS game, MAX(seq) AS seq FROM applied WHERE kind='covered' AND lower(game) IN (SELECT value FROM json_each(?1)) GROUP BY lower(game)";
/// FIX-968: the most games one read asks the actor's coverage of (the games holding a pending downgrade mark, in
/// `BTreeMap` order); a game past it is not asked and withholds as before.
pub const COVERED_ASK_MAX: usize = 64;
/// FIX-968, PURE: the actor's `/rows` answer's `covered` rows (`[{game, seq}]`, [`COVERED_BY_GAME_SQL`]) as the
/// read's map (game lowercased to its covered generation); an answer without them (an older object build, a fresh
/// object) is empty, so every pending downgrade mark withholds as before.
pub fn covered_of(answer: &Value) -> BTreeMap<String, i64> {
    answer["covered"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|r| {
            Some((
                r["game"].as_str()?.to_ascii_lowercase(),
                r["seq"].as_i64().or_else(|| r["seq"].as_f64().map(|f| f as i64))?,
            ))
        })
        .collect()
}
/// The tip an apply records, or a delivery is compared at ([`COVERED_TIP_KIND`]): `-1` for none.
pub fn covered_tip(tip: Option<u64>) -> i64 {
    tip.map_or(-1, |t| t.min(i64::MAX as u64) as i64)
}
/// How many of `?3`'s games the event `(?1, ?2)` already applied: none, its FIRST apply (the visibility sample's rule).
pub const EVENT_APPLIED_SQL: &str = "SELECT COUNT(*) AS n FROM applied WHERE kind=?1 AND seq=?2 AND game IN (SELECT value FROM json_each(?3))";
/// The version of the actor's own tables (`txfacts`, `applied`, `meta`) beside the mirror's generated digest: a move
/// of either rebuilds the actor (HIGH-4, [`actor_digest`]).
pub const ACTOR_TABLES_VERSION: &str = "applied-per-game-1";
/// The digest an actor holds in `meta` (`schema`): the mirror's and the actor's own tables'.
pub fn actor_digest() -> String {
    format!("{}+{ACTOR_TABLES_VERSION}", schema::DIGEST)
}
/// The delta lens's NEW-M2: the SHAPE an actor holds in `meta` (`shape`): the digest of every statement of
/// [`mirror_ddl`] but a plain `CREATE INDEX` (the tables, the `UNIQUE` indexes, which decide a write, and the
/// row-count triggers) with [`ACTOR_TABLES_VERSION`]. A digest move whose shape holds is an INDEX-ONLY change: the
/// actor drops its indexes and creates the render's, its rows, its bootstrap and its judgement kept
/// ([`object::schema_plan_kept`]); only a move of the shape rebuilds (HIGH-4).
pub fn actor_shape() -> String {
    let shape: Vec<String> = mirror_ddl()
        .into_iter()
        .filter(|s| !is_plain_index(s))
        .collect();
    format!(
        "{}+{ACTOR_TABLES_VERSION}",
        hex::encode(bsv_rs::primitives::hash::sha256(
            shape.join("\n").as_bytes()
        ))
    )
}
/// THE BOOTSTRAP'S FLEET BUDGET (the delta lens's NEW-M2): every actor's bootstrap page (a whole closure read over D1)
/// first takes one token of a window shared by the fleet, one row of the belt's ledger (`owed-actor:boot-budget`:
/// the minute's taken count in `cursor`, the minute in `tickedAtMs`), [`BOOTSTRAP_PAGES_PER_MINUTE`] a minute; past it
/// the page waits for its slot in a later minute ([`boot_pace`]), and an actor's pages are
/// [`BOOTSTRAP_PAGE_DELAY_MS`] apart (they were 1 ms). So a digest-moving or era-moving deploy re-bootstraps the
/// fleet at 60 pages (960 games) a minute, whatever the reads that arm it.
pub const BOOTSTRAP_PAGES_PER_MINUTE: i64 = 60;
/// FIX-962 (bsv-low #962): 500 ms (it was 1,000). One actor's pages are serial (the next page's alarm is set at the
/// end of the one before), so this is idle time, not a bound on its concurrent reads; the fleet's
/// [`BOOTSTRAP_PAGES_PER_MINUTE`] is the bound on D1. The floor: one actor at the measured page time (three round
/// trips of [`COLD_D1_ROUND_TRIP_MS`], about 2.1 s) plus this takes at most 60,000 / 2,600 = 23 pages a minute, under
/// half the fleet's minute, so a second cold actor always finds tokens.
pub const BOOTSTRAP_PAGE_DELAY_MS: u64 = 500;
/// The second delta lens's NEW2-L3: the take is ONE conditional upsert that writes NOTHING once this minute's
/// [`BOOTSTRAP_PAGES_PER_MINUTE`] tokens are taken (its `DO UPDATE ... WHERE` refuses, `RETURNING` answers no row: the
/// page waits, [`boot_pace`] over `i64::MAX`); it answers the token's number otherwise. So N waiting actors are N
/// one-row reads a minute of `owed-actor:boot-budget` and no write (they were N writes, each over-budget take
/// counted). The order is unchanged: each waiter's slot in the next minute is fixed by its identity's hash, so the
/// rebuild runs at 60 pages a minute in that order (no queue, no fairness beyond the spread), stated.
/// The fourth delta lens's NEW4-M1 (D2-ACTOR-B-FOLD-8): D1 binds a JS number as a REAL, so `?1/60000` was a
/// fractional minute, a new value every millisecond: every take reset the count and answered 1, and the budget never
/// refused on D1. The minute is `CAST(?1 AS INTEGER)/60000`, an integer whatever the bind.
pub const BOOT_BUDGET_TAKE_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('owed-actor:boot-budget','1',CAST(?1 AS INTEGER)/60000) ON CONFLICT(name) DO UPDATE SET cursor=CASE WHEN owed_belt_state.tickedAtMs=excluded.tickedAtMs THEN CAST(CAST(owed_belt_state.cursor AS INTEGER)+1 AS TEXT) ELSE '1' END,tickedAtMs=excluded.tickedAtMs WHERE owed_belt_state.tickedAtMs IS NOT excluded.tickedAtMs OR CAST(owed_belt_state.cursor AS INTEGER)<60 RETURNING CAST(cursor AS INTEGER) AS taken";
/// D2-ACTOR-B-FOLD-11 (the sixth delta lens's NEW6-L2): the pages a minute a RE-JUDGE may take (an actor already
/// bootstrapped whose state another judge version judged, [`rejudge_due`]). It takes from the same minute's count as a
/// cold page but only while the count is under this, so at least `BOOTSTRAP_PAGES_PER_MINUTE -
/// BOOTSTRAP_REJUDGE_PAGES_PER_MINUTE` pages a minute stay for COLD actors (a new seat, held events, a read served
/// display-only or a 503) whatever the fleet's re-judge; and a waiting cold page's slot is in the first half of the
/// next minute, a waiting re-judge's in the second ([`boot_pace_of`]): cold pages first in the slot order.
pub const BOOTSTRAP_REJUDGE_PAGES_PER_MINUTE: i64 = 30;
/// [`BOOT_BUDGET_TAKE_SQL`] for a re-judge's page: the same row and minute, refused at
/// [`BOOTSTRAP_REJUDGE_PAGES_PER_MINUTE`].
pub const BOOT_BUDGET_REJUDGE_TAKE_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('owed-actor:boot-budget','1',CAST(?1 AS INTEGER)/60000) ON CONFLICT(name) DO UPDATE SET cursor=CASE WHEN owed_belt_state.tickedAtMs=excluded.tickedAtMs THEN CAST(CAST(owed_belt_state.cursor AS INTEGER)+1 AS TEXT) ELSE '1' END,tickedAtMs=excluded.tickedAtMs WHERE owed_belt_state.tickedAtMs IS NOT excluded.tickedAtMs OR CAST(owed_belt_state.cursor AS INTEGER)<30 RETURNING CAST(cursor AS INTEGER) AS taken";
/// The token's number a take answered (no row: the minute is full, the page waits).
pub fn boot_taken(rows: &[Value]) -> i64 {
    rows.first()
        .and_then(|r| r["taken"].as_i64())
        .unwrap_or(i64::MAX)
}
/// The page's verdict for the `taken`-th token of this minute: `None` runs it; `Some(at)` is its wait, a slot in
/// the next minute spread by the identity (so the waiting actors do not all wake at the minute's edge).
pub fn boot_pace(taken: i64, now_ms: u64, identity: &str) -> Option<u64> {
    boot_pace_of(taken, now_ms, identity, false)
}

/// [`boot_pace`] for a cold page (`rejudge` false: the minute's [`BOOTSTRAP_PAGES_PER_MINUTE`], a wait in the first
/// half of the next minute) or a re-judge's (NEW6-L2: [`BOOTSTRAP_REJUDGE_PAGES_PER_MINUTE`], a wait in the second
/// half), each spread by the identity.
pub fn boot_pace_of(taken: i64, now_ms: u64, identity: &str, rejudge: bool) -> Option<u64> {
    let (cap, from) = if rejudge {
        (BOOTSTRAP_REJUDGE_PAGES_PER_MINUTE, 30_000)
    } else {
        (BOOTSTRAP_PAGES_PER_MINUTE, 0)
    };
    if taken <= cap {
        return None;
    }
    let spread = identity
        .bytes()
        .skip(2)
        .take(8)
        .fold(0u64, |a, b| a.wrapping_mul(31).wrapping_add(u64::from(b)))
        % 30_000;
    Some((now_ms / 60_000 + 1) * 60_000 + from + spread)
}

/// A `CREATE INDEX` statement (never `CREATE UNIQUE INDEX`).
pub fn is_plain_index(ddl: &str) -> bool {
    ddl.trim_start().starts_with("CREATE INDEX")
}
/// The judge's own tables (their indexes and triggers go with them): dropped and recreated on a digest change.
pub const JUDGE_TABLES: [&str; 4] = ["owed_rows", "owed_row_words", "owed_state", "owed_dirty"];
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
    /// The games this call applied (Phase C acknowledges exactly these games' marks, HIGH-1). The delta lens's NEW-H1
    /// (b): a redelivery is applied again, never skipped as "applied by an earlier delivery": the skip acknowledged a
    /// mark newer than that delivery's apply, unjudged.
    pub applied_games: Vec<String>,
    /// D2-ACTOR-D: a DUPE (every game covered at or past the delivery's bound, [`COVERED_SQL`]): nothing applied,
    /// nothing sampled; `applied_games` names the covered games (Phase C acknowledges their marks).
    pub skipped: bool,
    /// D2-ACTOR-D: the event's FIRST apply at this actor (no `applied` row of its `(kind, seq)` for these games; an
    /// event with no seq is its own). Only a first apply samples the rows' visibility: once per event.
    pub first_apply: bool,
    /// D2-ACTOR-E: the record reads past the event's own (a cold game's whole closure, a key a warm mirror had not
    /// read): `owed_actor_closure_misses_total`.
    pub misses: usize,
    /// D2-ACTOR-E: the apply read the event's rows over a warm mirror ([`apply_with`]).
    pub narrowed: bool,
    /// D2-ACTOR-E: the judge passes the burst this event rode took (0: applied alone, [`apply_burst`]).
    pub coalesced: usize,
    /// D2-ACTOR-B-FOLD-14 (bsv-low #954 item 9): the apply's stages, by the local clock: the record's reads, the
    /// judge (the rest of the apply: its rounds over the mirror, its outside reads, the key walks), the mirror's
    /// writes (the rounds' copies, the fixpoint's commit, the marks), and the turn's queueing before it.
    pub read_ms: u64,
    pub judge_ms: u64,
    pub write_ms: u64,
    pub wait_ms: u64,
    /// FIX-983 (bsv-low #983): the outside's (CHAINTRACKS) awaits, asked once per apply ([`ActorJudge`]), apart from
    /// the judge stage, which is then the derivation and the mirror's own reads.
    pub outside_ms: u64,
    /// FIX-983: the belt asks this apply QUEUED for the lane ([`queue_belt_asks`]; none when its dated rows did not
    /// move), published after the answer, never inside the measured apply.
    pub asks: usize,
    /// D2-ACTOR-B-FOLD-14: a bootstrap page's games and their `at`, in the page's order ([`ApplyOpts::page`]).
    pub page: Vec<(String, i64)>,
    /// FIX-960 (bsv-low #960, the tenth delta lens's NEW10-L1): the greatest generation of the games' marks this
    /// apply's closure READ, in the same snapshot as (or just before) the record's rows: a mark at or below it was
    /// written before the read, so its source change is in what the apply judged. Answered `readGeneration`; `None`
    /// (absent from the answer) where no marks were read with the rows, where the closure took more than one round
    /// (a later round's snapshot is newer than the number), and on a dupe. The reconcile cuts its held partition at it
    /// ([`HELD_GAMES_SQL`]).
    pub read_generation: Option<i64>,
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

/// D2-ACTOR-E (bsv-low #954): the most arguments one `json_array` call takes in the record's read (D1 caps a SQL
/// function at 32 arguments; `pot_records_evicted` has 37 columns, so a row is an array of arrays of at most this).
pub const COMPOUND_ARGS_PER_CALL: usize = 30;

/// The positional value expressions of one family's row in the record's read: the rowid, each column (a blob as hex,
/// a BEEF column skipped), then the BEEF's hex; and their names (the row's keys once parsed).
fn compound_columns(f: &Family) -> (Vec<String>, Vec<String>) {
    let mut exprs = vec!["t.rowid".to_string()];
    let mut names = vec!["__rowid".to_string()];
    for c in schema::columns(f.table) {
        if f.beef && *c == "beef" {
            continue;
        }
        exprs.push(if schema::is_blob(f.table, c) {
            format!("hex(t.{c})")
        } else {
            format!("t.{c}")
        });
        names.push((*c).to_string());
    }
    if f.beef {
        exprs.push("hex(t.beef)".into());
        names.push("__beef".into());
    }
    (exprs, names)
}

/// One family's row as nested JSON arrays ([`COMPOUND_ARGS_PER_CALL`] arguments a call).
fn row_json(f: &Family) -> String {
    let (exprs, _) = compound_columns(f);
    let calls: Vec<String> = exprs
        .chunks(COMPOUND_ARGS_PER_CALL)
        .map(|c| format!("json_array({})", c.join(",")))
        .collect();
    format!("json_array({})", calls.join(","))
}

/// The marks' bound column of the record's read: the greatest generation of the games' marks, read in the SAME
/// statement (one snapshot) as the rows (D2-ACTOR-D's coverage).
pub const BOUND_COLUMN: &str = "(SELECT MAX(generation) FROM owed_dirty WHERE identity=?1 AND gameId IN (SELECT value FROM json_each(?2))) AS b";

/// D2-ACTOR-E (bsv-low #954): ONE record statement for the families of a round (it was one statement a family, 24):
/// each family's selection a scalar subquery answering its rows as one JSON array (`f<n>`, `n` its index in
/// [`FAMILIES`]); with `bound` the marks' bound beside them. No compound SELECT (D1 refuses one past 5 terms).
pub fn aggregate_select(bound: bool, families: &[usize]) -> String {
    let mut cols: Vec<String> = Vec::new();
    if bound {
        cols.push(BOUND_COLUMN.into());
    }
    for &n in families {
        let f = &FAMILIES[n];
        cols.push(format!(
            "(SELECT json_group_array(json(r)) FROM (SELECT DISTINCT {} AS r FROM {})) AS f{n}",
            row_json(f),
            selection(f)
        ));
    }
    format!("SELECT {}", cols.join(","))
}

/// D2-ACTOR-B-FOLD-14 (bsv-low #954 item 9): the marks' txids, as [`txids_in`] reads a trigger's source key (`txid`
/// or `txid:n`, lowercased, exactly 64 hex digits; an identity's 66 or a constant key names none), in SQL.
const MARK_TXID_EXPR: &str = "lower(CASE WHEN instr(sourceKey,':')>0 THEN substr(sourceKey,1,instr(sourceKey,':')-1) ELSE sourceKey END)";

/// D2-ACTOR-B-FOLD-14 (bsv-low #954 item 9): the WARM apply's record read in ONE statement, so in ONE D1 round trip
/// (it was two sequential reads: [`NARROW_MARKS_SQL`], then [`aggregate_select`] over keys that depend on its answer).
/// The games' pending marks are a CTE (`mk_m`), their txids enter the closure's key set beside the bound ones (`mk_t`:
/// `?3` and the marks'), the probe ranges follow them (`mk_p`: `?5` and each of `?6`'s prefixes before each key), and
/// every family's selection reads `mk_t` and `mk_p` where the plain read reads `json_each(?3)` and `json_each(?5)`.
/// It answers the marks (`marks`, `[generation, source, sourceKey]`), the key set it read (`keys`: the apply writes
/// the same domain), and each family's rows (`f<n>`), from one snapshot: every mark the bound counts names a row read
/// in the same statement. Binds `?1`..`?5` as [`Keys::binds`], `?6` [`PROBE_PREFIXES`].
pub fn marked_select() -> String {
    marked_select_of(
        "mk_m AS MATERIALIZED (SELECT generation,source,sourceKey FROM owed_dirty WHERE identity=?1 AND gameId IN (SELECT value FROM json_each(?2)))",
        "",
    )
}

/// D2-ACTOR-B-FOLD-14: the BOOTSTRAP PAGE's first round in ONE statement (it was the page's games read, then the
/// apply's first read over them): the page's games a CTE (`mk_g`, [`crate::owed_judge::IDENTITY_GAMES_AFTER_SQL`]
/// with its `?2` and `?3` moved to `?7` and `?8`), every family's selection and the marks over them; answered
/// `games` (`[gameId, at]`, the page's order) beside the marked read's columns.
pub fn page_select() -> String {
    let games = crate::owed_judge::IDENTITY_GAMES_AFTER_SQL
        .replace("?3", "?8")
        .replace("?2", "?7");
    let ctes = format!(
        "mk_g AS MATERIALIZED ({games}),mk_m AS MATERIALIZED (SELECT generation,source,sourceKey FROM owed_dirty WHERE identity=?1 AND gameId IN (SELECT gameId FROM mk_g))"
    );
    marked_select_of(
        &ctes,
        "(SELECT json_group_array(json_array(gameId,at)) FROM mk_g) AS games,",
    )
    .replace("json_each(?2)", "(SELECT gameId AS value FROM mk_g)")
}

fn marked_select_of(ctes: &str, head: &str) -> String {
    let all: Vec<usize> = (0..FAMILIES.len()).collect();
    let rest = aggregate_select(false, &all);
    let rest = rest
        .strip_prefix("SELECT ")
        .unwrap_or(&rest)
        .replace("json_each(?3)", "mk_t")
        .replace("json_each(?5)", "mk_p");
    format!(
        "WITH {ctes},mk_t AS MATERIALIZED (SELECT value FROM json_each(?3) UNION SELECT v FROM (SELECT {MARK_TXID_EXPR} AS v FROM mk_m) WHERE length(v)=64 AND NOT (v GLOB '*[^0-9a-f]*')),mk_p AS MATERIALIZED (SELECT value FROM json_each(?5) UNION SELECT x.value||mk_t.value FROM mk_t CROSS JOIN json_each(?6) x) SELECT {head}(SELECT json_group_array(json_array(generation,source,sourceKey)) FROM mk_m) AS marks,(SELECT json_group_array(value) FROM mk_t) AS keys,{rest}"
    )
}

/// One marked read's answer ([`read_marked`]).
pub struct MarkedRead {
    /// The games' pending marks: `generation`, `source`, `sourceKey`.
    pub marks: Vec<Value>,
    /// The key set the read selected by (the bound keys and the marks' txids; the markers as bound).
    pub keys: Keys,
    /// Each family's rows.
    pub rows: Vec<Vec<Value>>,
    /// The page's games and their `at` (a page read only).
    pub games: Vec<(String, i64)>,
}

/// The marked read ([`marked_select`], or [`page_select`] with `page`): ONE record call. `Ok(None)` when a value
/// passed D1's bound (`TOOBIG`): the caller falls back to the sequential reads, whose own fallbacks stand.
async fn read_marked(
    record: &impl SqlPort,
    me: &str,
    games: &[String],
    keys: &Keys,
    page: Option<(i64, &str)>,
) -> Result<Option<MarkedRead>, String> {
    let mut binds = keys.binds(me, games);
    binds.push(Bind::Text(json!(PROBE_PREFIXES).to_string()));
    let sql = match page {
        Some((at, after)) => {
            binds.push(Bind::Int(at));
            binds.push(Bind::Text(after.into()));
            page_select()
        }
        None => marked_select(),
    };
    let row = match record.run_batch(vec![fitted(sql, &binds)]).await {
        Ok(o) => o
            .into_iter()
            .next()
            .and_then(|o| o.rows.into_iter().next())
            .unwrap_or(Value::Null),
        Err(e) if too_big(&e) => {
            record.log(&format!(
                "[owed-actor] {me}: the marked read passed a value bound ({e}): the sequential reads"
            ));
            return Ok(None);
        }
        Err(e) => return Err(e),
    };
    let names: Vec<Vec<String>> = FAMILIES.iter().map(|f| compound_columns(f).1).collect();
    let mut rows: Vec<Vec<Value>> = vec![Vec::new(); FAMILIES.len()];
    for (n, out) in rows.iter_mut().enumerate() {
        let got = json_of(&row[format!("f{n}")])?;
        for r in got.as_array().into_iter().flatten() {
            out.push(row_of(&names[n], r));
        }
    }
    let marks: Vec<Value> = json_of(&row["marks"])?
        .as_array()
        .into_iter()
        .flatten()
        .map(|m| json!({"generation": m[0], "source": m[1], "sourceKey": m[2]}))
        .collect();
    let mut read = Keys {
        txids: BTreeSet::new(),
        markers: keys.markers.clone(),
    };
    for t in json_of(&row["keys"])?.as_array().into_iter().flatten() {
        if let Some(t) = t.as_str() {
            read.txids.insert(t.to_string());
        }
    }
    let mut page_games: Vec<(String, i64)> = json_of(&row["games"])?
        .as_array()
        .into_iter()
        .flatten()
        // D2-ACTOR-B-FOLD-15 (the eighth delta lens's NOTE-3): the game's `at` through `ms_of`, so a REAL `at` (D1's
        // JS-number bind) reads its value, never 0 (which would end the bootstrap at that page).
        .filter_map(|g| {
            Some((
                g[0].as_str()?.to_string(),
                crate::owed_events::ms_of(&g[1]).unwrap_or(0),
            ))
        })
        .collect();
    page_games.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    Ok(Some(MarkedRead {
        marks,
        keys: read,
        rows,
        games: page_games,
    }))
}

/// The per-row read of the BEEF families (at most 5 arms: D1's compound bound), the fallback when a family's rows
/// together pass D1's 2 MB value bound.
pub fn rows_select(families: &[usize]) -> String {
    families
        .iter()
        .map(|&n| {
            let f = &FAMILIES[n];
            format!(
                "SELECT DISTINCT {n} AS f,{} AS r FROM {}",
                row_json(f),
                selection(f)
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

/// A family's row read back: the nested arrays flattened onto the family's column names.
fn row_of(names: &[String], r: &Value) -> Value {
    let flat: Vec<Value> = r
        .as_array()
        .map(|a| {
            a.iter()
                .flat_map(|c| c.as_array().cloned().unwrap_or_default())
                .collect()
        })
        .unwrap_or_default();
    let mut obj = serde_json::Map::new();
    for (name, v) in names.iter().zip(flat) {
        obj.insert(name.clone(), v);
    }
    Value::Object(obj)
}

fn json_of(v: &Value) -> Result<Value, String> {
    match v {
        Value::String(s) => serde_json::from_str(s).map_err(|e| format!("owed actor: {e}")),
        Value::Null => Ok(Value::Array(Vec::new())),
        other => Ok(other.clone()),
    }
}

/// Does a record fault say a value passed D1's bound (`SQLITE_TOOBIG`)?
fn too_big(e: &str) -> bool {
    e.contains("TOOBIG") || e.contains("too big")
}

/// One round's record read: every family's selection over the closure's keys in ONE statement
/// ([`aggregate_select`]); with `bound` the marks' bound beside the rows. A family's rows past D1's 2 MB value bound
/// fall back: the BEEF families row by row beside the others aggregated (two statements), then one statement a family.
async fn read_round(
    record: &impl SqlPort,
    me: &str,
    games: &[String],
    keys: &Keys,
    bound: bool,
) -> Result<(Vec<Vec<Value>>, Option<i64>), String> {
    let binds = keys.binds(me, games);
    let names: Vec<Vec<String>> = FAMILIES.iter().map(|f| compound_columns(f).1).collect();
    let all: Vec<usize> = (0..FAMILIES.len()).collect();
    let mut out: Vec<Vec<Value>> = vec![Vec::new(); FAMILIES.len()];
    let take_aggregate = |row: &Value, families: &[usize], out: &mut Vec<Vec<Value>>| {
        for &n in families {
            let rows = json_of(&row[format!("f{n}")])?;
            for r in rows.as_array().into_iter().flatten() {
                out[n].push(row_of(&names[n], r));
            }
        }
        Ok::<Option<i64>, String>(if bound {
            Some(row["b"].as_i64().unwrap_or(0))
        } else {
            None
        })
    };
    match record
        .run_batch(vec![fitted(aggregate_select(bound, &all), &binds)])
        .await
    {
        Ok(o) => {
            let row = o
                .into_iter()
                .next()
                .and_then(|o| o.rows.into_iter().next())
                .unwrap_or(Value::Null);
            let b = take_aggregate(&row, &all, &mut out)?;
            return Ok((out, b));
        }
        Err(e) if too_big(&e) => record.log(&format!(
            "[owed-actor] {me}: the record's read passed a value bound ({e}): the BEEFs row by row"
        )),
        Err(e) => return Err(e),
    }
    let (beefs, rest): (Vec<usize>, Vec<usize>) = all.iter().partition(|&&n| FAMILIES[n].beef);
    match record
        .run_batch(vec![
            fitted(aggregate_select(bound, &rest), &binds),
            fitted(rows_select(&beefs), &binds),
        ])
        .await
    {
        Ok(o) => {
            let mut o = o.into_iter();
            let row = o
                .next()
                .and_then(|o| o.rows.into_iter().next())
                .unwrap_or(Value::Null);
            let b = take_aggregate(&row, &rest, &mut out)?;
            for r in o.next().map(|o| o.rows).unwrap_or_default() {
                let n = r["f"].as_i64().unwrap_or(-1);
                let Some(cols) = usize::try_from(n).ok().and_then(|n| names.get(n)) else {
                    return Err(format!("owed actor: a BEEF row names family {n}"));
                };
                out[n as usize].push(row_of(cols, &json_of(&r["r"])?));
            }
            return Ok((out, b));
        }
        Err(e) if too_big(&e) => record.log(&format!(
            "[owed-actor] {me}: a family passed a value bound ({e}): one statement a family"
        )),
        Err(e) => return Err(e),
    }
    // The last fallback: one statement a family (as before D2-ACTOR-E), the bound its own.
    let mut stmts: Vec<Stmt> = FAMILIES
        .iter()
        .map(|f| fitted(copy_select(f), &binds))
        .collect();
    if bound {
        stmts.push(fitted(format!("SELECT {BOUND_COLUMN}"), &binds));
    }
    let o = record.run_batch(stmts).await?;
    let mut b = None;
    for (n, o) in o.into_iter().enumerate() {
        if n == FAMILIES.len() {
            b = o.rows.first().map(|r| r["b"].as_i64().unwrap_or(0));
        } else {
            out[n] = o.rows;
        }
    }
    Ok((out, b))
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
    stmts.push(fitted(
        format!(
            "SELECT t.rowid AS r FROM {} AND t.identity=?1",
            selection(&FAMILIES[1])
        ),
        &binds,
    ));
    let out = local.run_batch(stmts).await?;
    for (n, o) in out.iter().enumerate() {
        for r in &o.rows {
            if n == FAMILIES.len() {
                if let Some(id) = r["r"].as_i64() {
                    into.markers.insert(id);
                }
            } else if FAMILIES[n].table == "pot_records" {
                // FOLD-7: a pot row's own txid is its key; its SPENDER is a key only where the judge names it.
                txids_in_value(&r["txid"], &mut into.txids);
            } else {
                txids_in_value(r, &mut into.txids);
            }
        }
    }
    Ok(())
}

/// The txids the last judgement of these games named (its rows' outpoints and facts, their word roles).
const JUDGED_TXIDS_SQL: &str = "SELECT r.outpoint,r.facts,w.txid FROM owed_rows r LEFT JOIN owed_row_words w ON w.identity=r.identity AND w.outpoint=r.outpoint WHERE r.identity=?1 AND r.gameId IN (SELECT value FROM json_each(?2))";

/// D2-ACTOR-E (bsv-low #954): how long a game's last FULL refresh keeps it WARM ([`apply_with`]'s narrowed read).
/// Since D2-ACTOR-B-FOLD-11 (the sixth delta lens's NEW6-M1) warmth bounds no drift: a warm game's narrowed read
/// re-reads every family over the mirror's whole key set, so a write no trigger marks (the reorg clear, a proof
/// latched or withdrawn, the TTL sweep, an opponent's party row) is read by the game's next apply whatever its event.
/// What warmth still decides: a warm game's closure is already walked, so its first round reads the event's new keys
/// beside the mirror's (no miss round for them) and its coverage bound comes from the marks read before it.
pub const FULL_REFRESH_EVERY_MS: i64 = 600_000;
/// The kind of the `applied` rows that stamp a game's last full refresh (`seq` 0, `atMs` the refresh).
pub const FULL_KIND: &str = "full";
/// The full refresh's stamp, one row a game.
pub const FULL_STAMP_SQL: &str = "INSERT INTO applied(kind,seq,game,atMs) SELECT 'full',0,value,?1 FROM json_each(?2) WHERE 1 ON CONFLICT(kind,seq,game) DO UPDATE SET atMs=excluded.atMs";
/// How many of the games are warm (stamped after `?1`).
pub const WARM_GAMES_SQL: &str = "SELECT COUNT(DISTINCT game) AS n FROM applied WHERE kind='full' AND seq=0 AND atMs>?1 AND game IN (SELECT value FROM json_each(?2))";
/// The record's pending marks of the games (`owed_judge::MARKS_SQL`): the bound and the rows they name.
pub const NARROW_MARKS_SQL: &str = crate::owed_judge::MARKS_SQL;
/// The mark sources that name no row and ask for the record as it stands: a narrowed read is no answer to them.
pub const FULL_SOURCES: [&str; 3] = ["wanted", "age", "backfill"];

/// D2-ACTOR-E: what one apply is told beside its games.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApplyOpts {
    /// The txids the event's write named (its source key and its facts, [`EventCtx::txids`]); `None`: the whole
    /// closure is read (a tip alarm, a drain, the reconcile, a belt find, an older caller).
    pub touch: Option<Vec<String>>,
    /// A narrowed apply records its coverage (the seq check's dupe) only where no judge but the actor acknowledges
    /// a mark before the actor read it: Phases B and C. In Phase A the D1 judge acknowledges first, so a narrowed
    /// apply covers nothing (a later delivery is applied, narrowly).
    pub narrow_covers: bool,
    /// The other events this apply serves (a burst coalesced in the object, [`apply_burst`]): each recorded in
    /// `applied` as its own `(kind, seq)`.
    pub also: Vec<(String, i64)>,
    /// D2-ACTOR-B-FOLD-14: a BOOTSTRAP PAGE's keyset (`at`, the last game): the apply's games are the page's, read in
    /// the same record call as its first round ([`page_select`]); `games` is ignored. The page's games and their
    /// `at` are answered on [`ApplyReport::page`].
    pub page: Option<(i64, String)>,
}

/// THE APPLY: the closure over `games` of `me`, then the last judgement over the mirror with the outside held.
/// `kind_seq` is the event's token, recorded in `applied` (the served `ackSeq`); a redelivery is applied again (the
/// delta lens's NEW-H1 (b): its record read is the record as it stands, the same judgement or a newer one, and the
/// consumer acknowledges only the marks it read before this apply).
///
/// HIGH-2: only the FIXPOINT round's judgement is written ([`Noting`] stages each round's commit), in one
/// synchronous batch after the identity's judged rows are snapshotted; a fault of that batch restores them, so no
/// read (`/rows`, the KV copy, the reconcile's `/stored`) ever sees a judgement over a partial mirror or a commit cut
/// half way.
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
    apply_with(
        record,
        local,
        outside,
        me,
        games,
        kind_seq,
        tip,
        era_ms,
        &ApplyOpts::default(),
    )
    .await
}

/// [`apply`] told what the event wrote (D2-ACTOR-E, bsv-low #954).
///
/// A game is WARM when its whole closure was read within [`FULL_REFRESH_EVERY_MS`]: an apply of warm games told the
/// txids its event wrote (`opts.touch`) reads the record in ONE statement, so ONE D1 round trip
/// ([`marked_select`], D2-ACTOR-B-FOLD-14, bsv-low #954 item 9; it was two sequential reads, the games' pending marks
/// ([`NARROW_MARKS_SQL`]) then every family over keys that depend on them): the marks, and every family over the
/// mirror's WHOLE key set plus the event's and the marks' txids, from one snapshot, so every mark the apply's bound
/// covers has its row read. A value past D1's bound (`TOOBIG`) falls back to the two reads and their own fallbacks.
///
/// THE RULE (D2-ACTOR-B-FOLD-11, NEW6-M1): a warm game's judgement after ANY write equals the D1 judge's over the full
/// closure. As D2-ACTOR-E built it the narrowed read took only the event's and the marks' rows, so a write no trigger
/// marks (the overlay's reorg clear of confirmed evidence, `TRANSACTION_UNPROVE_SQL`, the TTL sweep, an opponent's
/// party row) left the warm mirror stale and its judgement stood until a full read or the reconcile. Reading the
/// unmarked families alone over the whole key set was the lens's narrower shape; it is not taken because the set of
/// families every writer marks for every identity that reaches them is empty on inspection (partial triggers: the
/// BEEF's delete, `network_seen`'s delete, a word's other columns; cross-identity txid keys), so the narrowing kept
/// is the statement count, never the rows. A key the judgement then names that the mirror
/// has not read is a MISS: read in its own round (its keys alone), counted (`owed_actor_closure_misses_total`).
/// Any other apply (a cold game, no `touch`, a mark of [`FULL_SOURCES`]) reads the whole closure as before, in one
/// statement a round (it was 24), and stamps its games warm; a warm apply that meets a mark of [`FULL_SOURCES`] takes
/// its marked read as that first round. A bootstrap page ([`ApplyOpts::page`]) reads its games in its first round's
/// statement ([`page_select`]): a page is its rounds' reads. The report carries the apply's stages (`read_ms`,
/// `judge_ms`, `write_ms`; `wait_ms` set by the turn's caller), which sum to the apply's time on the local clock.
#[allow(clippy::too_many_arguments)]
pub async fn apply_with<R: SqlPort, L: SqlPort, O: Outside>(
    record: &R,
    local: &L,
    outside: &O,
    me: &str,
    games: &[String],
    kind_seq: Option<(&str, i64)>,
    tip: Option<u64>,
    era_ms: Option<i64>,
    opts: &ApplyOpts,
) -> Result<(ApplyReport, Vec<(String, Value)>), String> {
    let me = me.to_ascii_lowercase();
    let mut report = ApplyReport::default();
    let started_ms = local.now_ms();
    // FOLD-14: a bootstrap page reads its games in its first round's record call.
    let mut paged: Option<(Keys, Vec<Vec<Value>>, i64)> = None;
    let page_games: Vec<String>;
    let games = match &opts.page {
        Some((at, after)) => {
            let t = local.now_ms();
            let marked =
                read_marked(record, &me, &[], &Keys::default(), Some((*at, after))).await?;
            let page = match marked {
                Some(m) => {
                    let bound = m
                        .marks
                        .iter()
                        .filter_map(|m| m["generation"].as_i64())
                        .max()
                        .unwrap_or(0);
                    paged = Some((m.keys, m.rows, bound));
                    m.games
                }
                None => record
                    .run_batch(vec![Stmt::new(
                        crate::owed_judge::IDENTITY_GAMES_AFTER_SQL,
                        vec![
                            Bind::Text(me.clone()),
                            Bind::Int(*at),
                            Bind::Text(after.clone()),
                        ],
                    )])
                    .await?
                    .into_iter()
                    .next()
                    .map(|o| o.rows)
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|r| {
                        Some((
                            r["gameId"].as_str()?.to_string(),
                            r["at"].as_i64().unwrap_or(0),
                        ))
                    })
                    .collect(),
            };
            report.read_ms += local.now_ms().saturating_sub(t);
            report.page = page.clone();
            if page.is_empty() {
                return Ok((report, Vec::new()));
            }
            page_games = page.into_iter().map(|(g, _)| g).collect();
            &page_games[..]
        }
        None => games,
    };
    let mut wanted: Vec<String> = Vec::new();
    for g in games.iter().map(|g| g.to_ascii_lowercase()) {
        if !wanted.contains(&g) {
            wanted.push(g);
        }
    }
    wanted.truncate(GAMES_PER_APPLY);
    let games = wanted;
    let seq = kind_seq.map_or(0, |(_, s)| s);
    let now = started_ms as i64;
    let held = Held::default();
    // The closure starts from what the mirror already names for these games (the last apply's domain): in the steady
    // state its first round finds no new key and its judgement stands.
    // D2-ACTOR-B-FOLD-7: the LOCAL walk runs to its own fixpoint (it reads the mirror alone; bounded by the keys,
    // [`CLOSURE_KEYS_MAX`]); until the fold it stopped at [`CLOSURE_ROUNDS_MAX`] too, so a closure deeper than the
    // local 8 and the record's 8 started every retry at the same depth and never closed, whatever the mirror held.
    let mut keys = Keys::default();
    loop {
        let mut next = keys.clone();
        keys_of_mirror(local, &me, &games, &keys, &mut next).await?;
        if next == keys {
            break;
        }
        if next.txids.len() > CLOSURE_KEYS_MAX {
            return Err(format!(
                "{CLOSURE_BOUND}{me} passed {CLOSURE_KEYS_MAX} txids in the mirror"
            ));
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
    // D2-ACTOR-E: the narrowed read, where every game is warm and the event named its rows.
    let warm = match &opts.touch {
        Some(_) if !games.is_empty() => local
            .run_batch(vec![Stmt::new(
                WARM_GAMES_SQL,
                vec![
                    Bind::Int(now - FULL_REFRESH_EVERY_MS),
                    Bind::Text(json!(games).to_string()),
                ],
            )])
            .await
            .ok()
            .and_then(|o| {
                o.first()
                    .and_then(|o| o.rows.first())
                    .and_then(|r| r["n"].as_i64())
            })
            .is_some_and(|n| n as usize >= games.len()),
        _ => false,
    };
    let mut narrow: Option<Keys> = None;
    let mut covered: Option<i64> = None;
    // D2-ACTOR-B-FOLD-14 (bsv-low #954 item 9): the first round's read, made with the marks in ONE record call.
    let mut pre: Option<(Keys, Vec<Vec<Value>>)> = None;
    if let Some((k, rows, bound)) = paged {
        // The page's first round: its games' families over the page's marks' keys (the mirror's own keys for those
        // games, read nowhere yet, are the next round's).
        pre = Some((k, rows));
        covered = Some(bound);
        report.read_generation = Some(bound);
    }
    if let (true, Some(touch)) = (warm, &opts.touch) {
        // The warm mirror's WHOLE key set, every family, beside the event's keys (FOLD-11, NEW6-M1: no family is
        // narrowed to the event's rows, because no family's every writer marks every identity its rows reach) and,
        // inside the statement, the marks' keys (their bound is what this apply covers: every mark it counts names a
        // row read in the same snapshot).
        let mut n = Keys {
            txids: keys.txids.clone(),
            markers: keys.markers.clone(),
        };
        touch.iter().for_each(|t| txids_in(t, &mut n.txids));
        let t = local.now_ms();
        let marked = read_marked(record, &me, &games, &n, None).await?;
        report.read_ms += local.now_ms().saturating_sub(t);
        let (marks, read) = match marked {
            Some(m) => (m.marks, Some((m.keys, m.rows))),
            None => {
                // A value past D1's bound: the marks, then the sequential read and its own fallbacks.
                let t = local.now_ms();
                let marks = record
                    .run_batch(vec![Stmt::new(
                        NARROW_MARKS_SQL,
                        vec![Bind::Text(me.clone()), Bind::Text(json!(games).to_string())],
                    )])
                    .await?;
                report.read_ms += local.now_ms().saturating_sub(t);
                let marks: Vec<Value> =
                    marks.into_iter().next().map(|o| o.rows).unwrap_or_default();
                marks
                    .iter()
                    .for_each(|m| txids_in_value(&m["sourceKey"], &mut n.txids));
                (marks, None)
            }
        };
        let bound = marks
            .iter()
            .filter_map(|m| m["generation"].as_i64())
            .max()
            .unwrap_or(0);
        report.read_generation = Some(bound);
        if marks
            .iter()
            .any(|m| FULL_SOURCES.contains(&m["source"].as_str().unwrap_or("")))
        {
            // A mark asking for the record as it stands: the whole closure. The marked read already read the
            // mirror's keys over the games (and the event's and the marks' beside them), so it is that apply's first
            // round; its bound is the marks' (the full read's own `BOUND_COLUMN` reads the same marks).
            if let Some(read) = read {
                pre = Some(read);
                covered = Some(bound);
            }
        } else {
            covered = opts.narrow_covers.then_some(bound);
            match read {
                Some((k, rows)) => {
                    narrow = Some(k.clone());
                    pre = Some((k, rows));
                }
                None => narrow = Some(n),
            }
        }
    }
    // What the record was read for in this apply (a later round reads only the keys past it).
    let mut fetched = Keys::default();
    let mut first = true;
    let (staged, mut pushes) = loop {
        if report.rounds >= CLOSURE_ROUNDS_MAX {
            return Err(format!(
                "{CLOSURE_BOUND}{me} did not close in {CLOSURE_ROUNDS_MAX} rounds ({} txids)",
                keys.txids.len()
            ));
        }
        report.rounds += 1;
        // The round's read: the narrowed one first (the event's rows over the warm mirror), else the closure's keys
        // (all of them on a full apply's first round; a later round only those no earlier round read: a MISS).
        let (read_keys, read_games): (Keys, Vec<String>) = match (&narrow, first) {
            (Some(n), true) => (n.clone(), games.clone()),
            (None, true) => (keys.clone(), games.clone()),
            (_, false) => (
                Keys {
                    txids: keys.txids.difference(&fetched.txids).cloned().collect(),
                    markers: keys.markers.difference(&fetched.markers).copied().collect(),
                },
                Vec::new(),
            ),
        };
        if !first || narrow.is_none() {
            report.misses += 1;
        }
        let (read_keys, read) = match pre.take() {
            // FOLD-14: the marked read was this round's (its key set is a superset of the mirror's).
            Some((k, rows)) if first => (k, rows),
            _ => {
                let t = local.now_ms();
                let (read, bound) = read_round(
                    record,
                    &me,
                    &read_games,
                    &read_keys,
                    first && narrow.is_none(),
                )
                .await?;
                report.read_ms += local.now_ms().saturating_sub(t);
                if first && narrow.is_none() {
                    covered = bound;
                    report.read_generation = bound;
                }
                (read_keys, read)
            }
        };
        report.record_rows += read.iter().map(Vec::len).sum::<usize>();
        let t = local.now_ms();
        local
            .run_batch(write_round(&me, &read_games, &read_keys, &read, seq))
            .await?;
        report.write_ms += local.now_ms().saturating_sub(t);
        fetched.txids.extend(read_keys.txids.iter().cloned());
        fetched.markers.extend(read_keys.markers.iter().copied());
        first = false;
        let mut next = keys.clone();
        next.txids.extend(read_keys.txids.iter().cloned());
        next.markers.extend(read_keys.markers.iter().copied());
        let walk_from = next.clone();
        keys_of_mirror(local, &me, &games, &walk_from, &mut next).await?;
        let noting = Noting {
            local,
            seen: RefCell::default(),
            staged: RefCell::default(),
        };
        let judge = ActorJudge {
            local: &noting,
            outside: Some(outside),
            held: &held,
            tip,
            era_ms,
            pushes: RefCell::default(),
        };
        for chunk in games.chunks(crate::owed_judge::OWED_JUDGE_SUBJECTS_PER_INVOCATION) {
            crate::owed_judge::judge_subjects(&judge, &me, chunk, "owed-actor").await?;
        }
        let pushes = judge.pushes.take();
        next.txids.extend(noting.seen.take());
        if next.txids.len() > CLOSURE_KEYS_MAX {
            return Err(format!(
                "{CLOSURE_BOUND}{me} passed {CLOSURE_KEYS_MAX} txids"
            ));
        }
        keys = next;
        if keys.txids.is_subset(&fetched.txids) && keys.markers.is_subset(&fetched.markers) {
            // THE FIXPOINT: this round's judgement read a mirror holding every key its statements named, so it
            // is the record's judgement and only its commit is run (the object's turn keeps any other apply out).
            break (noting.staged.take(), pushes);
        }
    };
    report.txids = keys.txids.len();
    report.narrowed = narrow.is_some();
    if report.rounds > 1 {
        // FIX-960: a later round read a newer snapshot than the marks' number names.
        report.read_generation = None;
    }
    let t = local.now_ms();
    report.rows = commit_fixpoint(local, &me, staged).await?;
    report.write_ms += local.now_ms().saturating_sub(t);
    for (_, push) in &mut pushes {
        push["rows"] = json!(report.rows);
    }
    let mut marks = Vec::new();
    if narrow.is_none() {
        marks.push(Stmt::new(
            FULL_STAMP_SQL,
            vec![Bind::Int(now), Bind::Text(json!(games).to_string())],
        ));
    }
    if let Some(cov) = covered {
        marks.push(Stmt::new(
            APPLIED_SQL,
            vec![
                Bind::Text(COVERED_KIND.into()),
                Bind::Int(cov),
                Bind::Int(now),
                Bind::Text(json!(games).to_string()),
            ],
        ));
        // NEW5-M1: the tip this apply judged at, the games' last.
        marks.push(Stmt::new(
            COVERED_TIP_CLEAR_SQL,
            vec![Bind::Text(json!(games).to_string())],
        ));
        marks.push(Stmt::new(
            APPLIED_SQL,
            vec![
                Bind::Text(COVERED_TIP_KIND.into()),
                Bind::Int(covered_tip(tip)),
                Bind::Int(local.now_ms() as i64),
                Bind::Text(json!(games).to_string()),
            ],
        ));
    }
    let events: Vec<(String, i64)> = kind_seq
        .map(|(k, s)| (k.to_string(), s))
        .into_iter()
        .chain(opts.also.iter().cloned())
        .collect();
    let acked = events.iter().map(|(_, s)| *s).max();
    for (kind, seq) in &events {
        marks.push(Stmt::new(
            APPLIED_SQL,
            vec![
                Bind::Text(kind.clone()),
                Bind::Int(*seq),
                Bind::Int(now),
                Bind::Text(json!(games).to_string()),
            ],
        ));
    }
    if let Some(seq) = acked {
        marks.push(Stmt::new(
            META_PUT_SQL,
            vec![Bind::Text("ackSeq".into()), Bind::Text(seq.to_string())],
        ));
        // LOW-2: `applied` keeps a week of events (the queues' retention is four days: no redelivery is older).
        marks.push(Stmt::new(
            APPLIED_PRUNE_SQL,
            vec![Bind::Int(now - APPLIED_KEEP_MS)],
        ));
    }
    if !marks.is_empty() {
        let t = local.now_ms();
        local.run_batch(marks).await?;
        report.write_ms += local.now_ms().saturating_sub(t);
    }
    report.applied_games.extend(games);
    // FOLD-14 item 2: the judge's stage is the rest of the apply (the judge's rounds over the mirror, the local key
    // walks), so the stages sum to the apply's own time. FIX-983 (bsv-low #983): the outside's awaits are their own
    // stage (`outsideMs`, asked once per apply), so the judge stage is the derivation and the mirror's own reads.
    let total = local.now_ms().saturating_sub(started_ms);
    report.outside_ms = held.outside_ms.get();
    report.judge_ms = total.saturating_sub(report.read_ms + report.write_ms + report.outside_ms);
    Ok((report, pushes))
}

/// The identity's judged rows, copied aside before the fixpoint's commit (HIGH-2): every row of the identity, since
/// the commit's ceiling may take another game's.
pub const SNAPSHOT_TABLES: [&str; 4] = JUDGE_TABLES;
fn snapshot_stmts(me: &str) -> Vec<Stmt> {
    SNAPSHOT_TABLES
        .iter()
        .flat_map(|t| {
            [
                Stmt::new(format!("DROP TABLE IF EXISTS {t}_snap"), Vec::new()),
                Stmt::new(
                    format!("CREATE TABLE {t}_snap AS SELECT * FROM {t} WHERE identity=?1"),
                    vec![Bind::Text(me.into())],
                ),
            ]
        })
        .collect()
}
fn restore_stmts(me: &str) -> Vec<Stmt> {
    let mut out: Vec<Stmt> = SNAPSHOT_TABLES
        .iter()
        .map(|t| {
            Stmt::new(
                format!("DELETE FROM {t} WHERE identity=?1"),
                vec![Bind::Text(me.into())],
            )
        })
        .collect();
    // `owed_rows` first: its row-count triggers find no state row (deleted above), and the state is restored last.
    for t in SNAPSHOT_TABLES {
        out.push(Stmt::new(
            format!("INSERT INTO {t} SELECT * FROM {t}_snap"),
            Vec::new(),
        ));
    }
    out.extend(drop_snapshot_stmts());
    out
}
fn drop_snapshot_stmts() -> Vec<Stmt> {
    SNAPSHOT_TABLES
        .iter()
        .map(|t| Stmt::new(format!("DROP TABLE IF EXISTS {t}_snap"), Vec::new()))
        .collect()
}

/// THE FIXPOINT'S COMMIT (HIGH-2): the identity's judged rows snapshotted, then the round's staged commits in ONE
/// batch (each statement runs synchronously on the object's SQLite, so no read lands between them); a fault restores
/// the snapshot (a statement that faults ends the batch and its earlier statements stand). Returns the identity's
/// rows as the last commit's state read back.
pub async fn commit_fixpoint<L: SqlPort>(
    local: &L,
    me: &str,
    staged: Vec<Stmt>,
) -> Result<usize, String> {
    if staged.is_empty() {
        return Ok(0);
    }
    local.run_batch(snapshot_stmts(me)).await?;
    let readbacks: Vec<usize> = staged
        .iter()
        .enumerate()
        .filter(|(_, s)| is_state_readback(s))
        .map(|(n, _)| n)
        .collect();
    match local.run_batch(staged).await {
        Ok(out) => {
            if let Err(e) = local.run_batch(drop_snapshot_stmts()).await {
                local.log(&format!("[owed-actor] snapshot not dropped: {e}"));
            }
            Ok(readbacks
                .last()
                .and_then(|n| out.get(*n))
                .and_then(|o| o.rows.first())
                .and_then(|r| r["rows"].as_u64())
                .unwrap_or(0) as usize)
        }
        Err(e) => match local.run_batch(restore_stmts(me)).await {
            Ok(_) => Err(e),
            Err(r) => Err(format!("{e}; the restore faulted too: {r}")),
        },
    }
}

/// The answer of one apply (the object's `/apply`, the pins' harness): the actor's rows, the redelivery verdict, the
/// games the event has applied.
pub fn answer_of(report: &ApplyReport, rows: Vec<Value>) -> Value {
    json!({"rows": rows, "rounds": report.rounds, "txids": report.txids, "recordRows": report.record_rows, "missed": report.missed, "appliedGames": report.applied_games, "applied": !report.skipped, "firstApply": report.first_apply, "misses": report.misses, "narrowed": report.narrowed, "coalesced": report.coalesced, "readMs": report.read_ms, "judgeMs": report.judge_ms, "writeMs": report.write_ms, "waitMs": report.wait_ms, "outsideMs": report.outside_ms, "asksQueued": report.asks, "readGeneration": report.read_generation})
}

/// D2-ACTOR-E (bsv-low #954) item 4: was the event born on a WARM actor (written at or after the actor's bootstrap
/// stamp, `meta.bootstrapped`)? Only such an event's first apply samples the warm bars (`rowsVisible`, the
/// backstop's lag): an event written while the actor held (a held event, the sweep's re-publish of a held mark) is
/// the cold path's. An event with no write time, or an actor with no stamp (an older build's), samples as before.
pub fn warm_at_birth(at_ms: Option<u64>, bootstrapped_at_ms: Option<u64>) -> bool {
    warm_at_birth_of(at_ms, bootstrapped_at_ms)
}

/// [`warm_at_birth`] under a name a scope that shadows it with its own verdict can call.
pub fn warm_at_birth_of(at_ms: Option<u64>, bootstrapped_at_ms: Option<u64>) -> bool {
    match (at_ms, bootstrapped_at_ms) {
        (Some(at), Some(stamp)) => at >= stamp,
        _ => true,
    }
}

/// D2-ACTOR-D: does this answer sample the rows' visibility? Only an APPLIED answer (never a covered dupe) of its
/// event's FIRST apply: once per event, in the class of the path that applied it. An answer from an older build
/// (neither field) samples, as before.
pub fn samples_visible(answer: &Value) -> bool {
    answer["applied"] != false && answer["firstApply"] != false && answer["warmAtBirth"] != false
}

/// D2-ACTOR-D: the event's first apply at this actor? (`None` seq: its own event.)
pub async fn first_apply_of<L: SqlPort>(
    local: &L,
    kind_seq: Option<(&str, i64)>,
    games: &[String],
) -> bool {
    let Some((kind, seq)) = kind_seq else {
        return true;
    };
    let games: Vec<String> = games.iter().map(|g| g.to_ascii_lowercase()).collect();
    local
        .run_batch(vec![Stmt::new(
            EVENT_APPLIED_SQL,
            vec![
                Bind::Text(kind.into()),
                Bind::Int(seq),
                Bind::Text(json!(games).to_string()),
            ],
        )])
        .await
        .ok()
        .and_then(|o| {
            o.first()
                .and_then(|o| o.rows.first())
                .and_then(|r| r["n"].as_i64())
        })
        .is_none_or(|n| n == 0)
}

/// D2-ACTOR-D: is every game covered at or past `bound` ([`COVERED_SQL`]), its last apply judged at a tip no lower
/// than the delivery's `tip` (NEW5-M1)? A read that faults is never covered.
pub async fn covered_at<L: SqlPort>(
    local: &L,
    games: &[String],
    bound: i64,
    tip: Option<u64>,
) -> bool {
    let mut games: Vec<String> = games.iter().map(|g| g.to_ascii_lowercase()).collect();
    games.sort();
    games.dedup();
    if games.is_empty() {
        return false;
    }
    local
        .run_batch(vec![Stmt::new(
            COVERED_SQL,
            vec![
                Bind::Int(bound),
                Bind::Text(json!(games).to_string()),
                Bind::Int(covered_tip(tip)),
            ],
        )])
        .await
        .ok()
        .and_then(|o| {
            o.first()
                .and_then(|o| o.rows.first())
                .and_then(|r| r["n"].as_i64())
        })
        .is_some_and(|n| n as usize >= games.len())
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

/// THE ASKS LANE (FIX-983, bsv-low #983). Until this fold the belt's questions were PUBLISHED inside the measured
/// apply, in the object's turn: a record batch (the clear and one put an ask) and a counters round trip per apply,
/// folded into the judge stage by the coalesced pass (beta's flood minute: the apply's wall p95 3,649 ms against a
/// CPU p95 of 135 ms; on the fixture a warm Phase C apply made 3 record round trips where Phase B made 1). Now the
/// apply QUEUES its asks here, inside the turn (so the lane's order is the turns' order and a later apply's asks are
/// never overwritten by an earlier one's), and the OBJECT drains the lane after its answer (`wait_until`), one
/// record batch a queued apply, the asks counted once a drain. An apply whose dated rows did not move since the
/// last publish of its games queues nothing (the per-game digest in the mirror's `meta`, [`ask_digest_name`]); a
/// drained batch that FAULTED marks its identity ([`ASK_FAULTED`]) so its next apply publishes every game again.
///
/// The lane is this isolate's (several actors of one class may share an isolate: the FIFO keeps each identity's
/// order and a batch is self-contained); the native pins drain it as the object does ([`drain_ask_lane`]).
#[derive(Debug, Clone, PartialEq)]
pub struct AskBatch {
    pub identity: String,
    pub games: Vec<String>,
    pub stmts: Vec<Stmt>,
    pub asks: usize,
}
thread_local! {
    static ASK_LANE: RefCell<std::collections::VecDeque<AskBatch>> = RefCell::default();
    /// The identities whose last drained batch faulted: their next apply publishes every game again.
    static ASK_FAULTED: RefCell<BTreeSet<String>> = RefCell::default();
}
/// The lane's batches, taken (the object's drain and the pins).
pub fn take_ask_batches() -> Vec<AskBatch> {
    ASK_LANE.with(|l| l.borrow_mut().drain(..).collect())
}
pub fn ask_lane_len() -> usize {
    ASK_LANE.with(|l| l.borrow().len())
}
/// The mirror's `meta` key of one game's last published asks (the sha256 of the asks' JSON).
pub fn ask_digest_name(game: &str) -> String {
    format!("askdigest:{}", game.to_ascii_lowercase())
}
/// The asks of these games in the mirror's `meta` (`?1` the JSON of the games' digest names).
const ASK_DIGESTS_SQL: &str =
    "SELECT name,value FROM meta WHERE name IN (SELECT value FROM json_each(?1))";

/// QUEUE THE BELT'S QUESTIONS (HIGH-3, Phase C; FIX-983): every dated row of the judged games is a `belt-ask` for the
/// belt's ledger ([`crate::owed_belt::ask_of_row`]), and an ask no dated row names any more is taken out; the games
/// whose asks did not move since their last publish are left out, and a batch is queued on the lane only for the
/// rest (none when nothing moved). The actor asks no courier: the belt singleton does, at its cadence, through its
/// one gate. Returns the asks queued (0: nothing queued).
pub async fn queue_belt_asks<L: SqlPort>(
    local: &L,
    me: &str,
    games: &[String],
) -> Result<usize, String> {
    let me = me.to_ascii_lowercase();
    let games: Vec<String> = games.iter().map(|g| g.to_ascii_lowercase()).collect();
    if games.is_empty() {
        return Ok(0);
    }
    let gj = json!(games).to_string();
    let digest_names: Vec<String> = games.iter().map(|g| ask_digest_name(g)).collect();
    let mut read = local
        .run_batch(vec![
            Stmt::new(
                ASK_ROWS_SQL,
                vec![Bind::Text(me.clone()), Bind::Text(gj.clone())],
            ),
            Stmt::new(
                ASK_DIGESTS_SQL,
                vec![Bind::Text(json!(digest_names).to_string())],
            ),
        ])
        .await?
        .into_iter();
    let rows = read.next().map(|o| o.rows).unwrap_or_default();
    let stored: BTreeMap<String, String> = read
        .next()
        .map(|o| o.rows)
        .unwrap_or_default()
        .iter()
        .filter_map(|r| Some((r["name"].as_str()?.to_string(), r["value"].as_str()?.to_string())))
        .collect();
    // Every game of a faulted identity is published again (its last drain did not land).
    let refresh = ASK_FAULTED.with(|f| f.borrow_mut().remove(&me));
    let mut per_game: BTreeMap<String, Vec<(String, Value, i64)>> =
        games.iter().map(|g| (g.clone(), Vec::new())).collect();
    for r in &rows {
        if let (Some(g), Some(ask)) = (
            r["gameId"].as_str().map(str::to_ascii_lowercase),
            crate::owed_belt::ask_of_row(&me, r),
        ) {
            per_game.entry(g).or_default().push(ask);
        }
    }
    let mut changed: Vec<String> = Vec::new();
    let mut digests: Vec<Stmt> = Vec::new();
    let mut asks: Vec<(String, Value, i64)> = Vec::new();
    for (g, list) in per_game {
        let digest = hex::encode(bsv_rs::primitives::hash::sha256(
            json!(list
                .iter()
                .map(|(n, b, d)| json!([n, b, d]))
                .collect::<Vec<_>>())
            .to_string()
            .as_bytes(),
        ));
        let name = ask_digest_name(&g);
        let moved = match stored.get(&name) {
            None => !list.is_empty(),
            Some(d) => d != &digest,
        };
        if refresh || moved {
            digests.push(Stmt::new(
                META_PUT_SQL,
                vec![Bind::Text(name), Bind::Text(digest)],
            ));
            changed.push(g);
            asks.extend(list);
        }
    }
    if changed.is_empty() {
        return Ok(0);
    }
    let names: Vec<&String> = asks.iter().map(|(n, _, _)| n).collect();
    let mut stmts = vec![Stmt::new(
        ASKS_CLEAR_SQL,
        vec![
            Bind::Text(format!("{}{me} ", crate::owed_belt::BELT_ASK_PREFIX)),
            Bind::Text(format!("{}{me}!", crate::owed_belt::BELT_ASK_PREFIX)),
            Bind::Text(json!(changed).to_string()),
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
    // The digests first (the mirror's, inside the turn): a drain that faults puts the identity back on the refresh
    // set, so an optimistic digest never hides a missing publish for longer than one apply.
    local.run_batch(digests).await?;
    let n = asks.len();
    ASK_LANE.with(|l| {
        l.borrow_mut().push_back(AskBatch {
            identity: me,
            games: changed,
            stmts,
            asks: n,
        })
    });
    Ok(n)
}

/// What one drain of the lane did (the object's `wait_until`, the pins).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AsksDrained {
    pub batches: usize,
    pub asks: usize,
    pub faults: usize,
}
/// DRAIN THE LANE (FIX-983): every queued batch in order, one record batch each; a batch that faults is logged and
/// dropped (its identity marked for a full re-publish at its next apply), the rest go on; the asks that landed are
/// counted ONCE (`owed_actor_belt_asks_total`), never a counters round trip per apply.
pub async fn drain_ask_lane<R: SqlPort>(record: &R) -> AsksDrained {
    let mut out = AsksDrained::default();
    for batch in take_ask_batches() {
        out.batches += 1;
        match record.run_batch(batch.stmts).await {
            Ok(_) => out.asks += batch.asks,
            Err(e) => {
                out.faults += 1;
                ASK_FAULTED.with(|f| f.borrow_mut().insert(batch.identity.clone()));
                record.log(&format!(
                    "[owed-actor] belt asks of {} not published ({} game(s)): {e}",
                    batch.identity,
                    batch.games.len()
                ));
            }
        }
    }
    if out.asks > 0 {
        record
            .counters(vec![(COUNTERS[8].to_string(), out.asks as u64)])
            .await;
    }
    out
}

/// [`apply_in_turn`], then (Phase C, `publish`) the asks of the games QUEUED inside the same turn (FIX-983: the lane,
/// drained by the object after its answer), so a later apply's asks are never overwritten by an earlier one's.
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
    apply_event_in_turn(
        turn, record, local, outside, me, games, kind_seq, tip, era_ms, publish, None,
    )
    .await
}

/// D2-ACTOR-D (bsv-low #952): [`apply_and_publish_in_turn`] for a delivery that names its acknowledgement `bound`:
/// inside the turn, the event's first-apply check ([`first_apply_of`]), then the seq check ([`covered_at`]): a
/// delivery whose every game is covered at or past its bound is a DUPE, answered without an apply (milliseconds);
/// any other is applied. `None` bound (a writer's direct call, the tip alarm, a held event's drain): always applied.
#[allow(clippy::too_many_arguments)]
pub async fn apply_event_in_turn<R: SqlPort, L: SqlPort, O: Outside>(
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
    bound: Option<i64>,
) -> Result<(ApplyReport, Vec<(String, Value)>), String> {
    let entered = local.now_ms();
    let _turn = turn.lock().await;
    let wait_ms = local.now_ms().saturating_sub(entered);
    let first = first_apply_of(local, kind_seq, games).await;
    if let Some(b) = bound {
        if covered_at(local, games, b, tip).await {
            let mut named: Vec<String> = Vec::new();
            for g in games.iter().map(|g| g.to_ascii_lowercase()) {
                if !named.contains(&g) {
                    named.push(g);
                }
            }
            return Ok((
                ApplyReport {
                    applied_games: named,
                    skipped: true,
                    first_apply: first,
                    ..ApplyReport::default()
                },
                Vec::new(),
            ));
        }
    }
    let (mut report, pushes) =
        apply(record, local, outside, me, games, kind_seq, tip, era_ms).await?;
    report.first_apply = first;
    report.wait_ms = wait_ms;
    if publish {
        report.asks = queue_belt_asks(local, me, games).await?;
    }
    Ok((report, pushes))
}

/// D2-ACTOR-E (bsv-low #954): one event waiting for the object's turn ([`apply_coalesced`]).
#[derive(Debug, Clone, Default)]
pub struct BurstItem {
    pub games: Vec<String>,
    pub kind_seq: Option<(String, i64)>,
    pub tip: Option<u64>,
    pub bound: Option<i64>,
    pub touch: Option<Vec<String>>,
}

type BurstResult = Result<(ApplyReport, Vec<(String, Value)>), String>;

/// D2-ACTOR-E: the object's waiting events. A caller adds its event and takes the turn; the turn's holder applies
/// EVERY event waiting by then as one judge pass over the union of their games ([`apply_burst`]) and leaves each
/// other caller its answer ([`Burst::answer`]), which that caller takes as soon as it is left, never after the turn
/// (D2-ACTOR-B-FOLD-14 item 3, [`AnswerOf`]): no caller waits for a pass, or a read, it does not need.
#[derive(Default)]
pub struct Burst {
    next: std::cell::Cell<u64>,
    pending: RefCell<Vec<(u64, BurstItem)>>,
    done: RefCell<BTreeMap<u64, BurstResult>>,
    /// The judge passes run, and the events they served (served on the object's `/bars`).
    pub passes: std::cell::Cell<u64>,
    pub events: std::cell::Cell<u64>,
    /// D2-ACTOR-B-FOLD-14 item 3: the callers waiting for a pass's answer, woken when it is left ([`Burst::answer`]).
    wakers: RefCell<Vec<std::task::Waker>>,
}

impl Burst {
    /// The waiting events, taken by the turn's holder for its pass.
    pub fn take(&self) -> Vec<(u64, BurstItem)> {
        std::mem::take(&mut *self.pending.borrow_mut())
    }
    /// Leave each other caller its answer and wake every waiting caller (the holder's own is returned to it).
    pub fn answer(&self, results: Vec<(u64, BurstResult)>) {
        self.done.borrow_mut().extend(results);
        for w in self.wakers.borrow_mut().drain(..) {
            w.wake();
        }
    }
}

/// D2-ACTOR-B-FOLD-14 (bsv-low #954 item 9) item 3: a caller's answer left by another caller's pass. A caller whose
/// event a holder's pass applied returns as soon as that pass leaves its answer, never after the turn: futures-util's
/// `Mutex` wakes the waiter in its lowest slab slot on unlock (a slot a later caller may have reused), so a joiner that
/// waited for the turn could wait out the whole pass of an event that arrived after its own was applied, reads and
/// all, for an answer already in hand.
struct AnswerOf<'a> {
    burst: &'a Burst,
    id: u64,
}
impl std::future::Future for AnswerOf<'_> {
    type Output = BurstResult;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<BurstResult> {
        if let Some(r) = self.burst.done.borrow_mut().remove(&self.id) {
            return std::task::Poll::Ready(r);
        }
        self.burst.wakers.borrow_mut().push(cx.waker().clone());
        std::task::Poll::Pending
    }
}

/// THE BURST (D2-ACTOR-E item 2): the events of `items` applied as ONE judge pass a [`GAMES_PER_APPLY`] chunk of
/// the union of their games, each event answered exactly: its first-apply verdict and its seq check read before the
/// pass (a covered dupe is answered without it), its games as its `applied_games`, its `(kind, seq)` recorded in
/// `applied`; the pass's pushes go to the first event it applied (one push per judgement).
#[allow(clippy::too_many_arguments)]
pub async fn apply_burst<R: SqlPort, L: SqlPort, O: Outside>(
    record: &R,
    local: &L,
    outside: &O,
    me: &str,
    items: Vec<(u64, BurstItem)>,
    era_ms: Option<i64>,
    publish: bool,
    narrow_covers: bool,
) -> Vec<(u64, BurstResult)> {
    let mut out: Vec<(u64, BurstResult)> = Vec::new();
    let mut live: Vec<(u64, BurstItem, bool)> = Vec::new();
    for (id, item) in items {
        let ks = item.kind_seq.as_ref().map(|(k, s)| (k.as_str(), *s));
        let first = first_apply_of(local, ks, &item.games).await;
        let named = lowered(&item.games);
        if let Some(b) = item.bound {
            if covered_at(local, &item.games, b, item.tip).await {
                out.push((
                    id,
                    Ok((
                        ApplyReport {
                            applied_games: named,
                            skipped: true,
                            first_apply: first,
                            ..ApplyReport::default()
                        },
                        Vec::new(),
                    )),
                ));
                continue;
            }
        }
        live.push((id, item, first));
    }
    if live.is_empty() {
        return out;
    }
    let mut union: Vec<String> = Vec::new();
    let mut touch: Option<Vec<String>> = Some(Vec::new());
    let mut tip: Option<u64> = None;
    for (_, item, _) in &live {
        for g in lowered(&item.games) {
            if !union.contains(&g) {
                union.push(g);
            }
        }
        touch = match (touch, &item.touch) {
            (Some(mut t), Some(more)) => {
                t.extend(more.iter().cloned());
                Some(t)
            }
            _ => None,
        };
        tip = tip.max(item.tip);
    }
    let mut pushes: Vec<(String, Value)> = Vec::new();
    let mut reports: Vec<ApplyReport> = Vec::new();
    let mut fault: Option<String> = None;
    for chunk in union.chunks(GAMES_PER_APPLY) {
        // each chunk records the events whose games it holds
        let mut events: Vec<(String, i64)> = live
            .iter()
            .filter(|(_, item, _)| lowered(&item.games).iter().any(|g| chunk.contains(g)))
            .filter_map(|(_, item, _)| item.kind_seq.clone())
            .collect();
        events.dedup();
        let opts = ApplyOpts {
            touch: touch.clone(),
            narrow_covers,
            also: events,
            page: None,
        };
        let applied = apply_with(record, local, outside, me, chunk, None, tip, era_ms, &opts).await;
        let applied = match applied {
            Ok((mut a, p)) if publish => queue_belt_asks(local, me, chunk).await.map(|n| {
                a.asks = n;
                (a, p)
            }),
            other => other,
        };
        match applied {
            Ok((report, p)) => {
                pushes.extend(p);
                reports.push(report);
            }
            Err(e) => {
                fault = Some(e);
                break;
            }
        }
    }
    let passes = reports.len();
    let mut pushes = Some(pushes);
    for (id, item, first) in live {
        if let Some(e) = &fault {
            out.push((id, Err(e.clone())));
            continue;
        }
        let mut report = reports.first().cloned().unwrap_or_default();
        report.rounds = reports.iter().map(|r| r.rounds).max().unwrap_or(0);
        report.misses = reports.iter().map(|r| r.misses).sum();
        report.record_rows = reports.iter().map(|r| r.record_rows).sum();
        report.narrowed = reports.iter().all(|r| r.narrowed);
        report.coalesced = passes;
        // FOLD-14 item 2: the pass's stages, every chunk's (each event of the pass answered the same pass).
        report.read_ms = reports.iter().map(|r| r.read_ms).sum();
        report.judge_ms = reports.iter().map(|r| r.judge_ms).sum();
        report.write_ms = reports.iter().map(|r| r.write_ms).sum();
        report.outside_ms = reports.iter().map(|r| r.outside_ms).sum();
        report.asks = reports.iter().map(|r| r.asks).sum();
        // FIX-960: the pass's read generation only when every chunk read the record at the same one.
        report.read_generation = match reports.first().map(|r| r.read_generation) {
            Some(g) if reports.iter().all(|r| r.read_generation == g) => g,
            _ => None,
        };
        report.applied_games = lowered(&item.games);
        report.first_apply = first;
        report.skipped = false;
        out.push((id, Ok((report, pushes.take().unwrap_or_default()))));
    }
    out
}

fn lowered(games: &[String]) -> Vec<String> {
    let mut named: Vec<String> = Vec::new();
    for g in games.iter().map(|g| g.to_ascii_lowercase()) {
        if !named.contains(&g) {
            named.push(g);
        }
    }
    named
}

/// D2-ACTOR-E: apply `item` in the object's turn, coalesced with every event waiting for it ([`Burst`]).
#[allow(clippy::too_many_arguments)]
pub async fn apply_coalesced<R: SqlPort, L: SqlPort, O: Outside>(
    burst: &Burst,
    turn: &futures_util::lock::Mutex<()>,
    record: &R,
    local: &L,
    outside: &O,
    me: &str,
    item: BurstItem,
    era_ms: Option<i64>,
    publish: bool,
    narrow_covers: bool,
) -> BurstResult {
    let id = burst.next.get();
    burst.next.set(id + 1);
    burst.pending.borrow_mut().push((id, item));
    let entered = local.now_ms();
    // FOLD-14 item 2: an event a holder's pass answered waited for the rest of its time.
    let joined = |r: BurstResult| {
        r.map(|(mut report, p)| {
            let total = local.now_ms().saturating_sub(entered);
            report.wait_ms = total.saturating_sub(
                report.read_ms + report.judge_ms + report.write_ms + report.outside_ms,
            );
            (report, p)
        })
    };
    // FOLD-14 item 3: the turn, or this event's answer left by a holder's pass, whichever comes first.
    let _turn =
        match futures_util::future::select(Box::pin(turn.lock()), AnswerOf { burst, id }).await {
            futures_util::future::Either::Left((guard, _)) => guard,
            futures_util::future::Either::Right((r, _)) => return joined(r),
        };
    if let Some(r) = burst.done.borrow_mut().remove(&id) {
        return joined(r);
    }
    let turned = local.now_ms();
    let items = burst.take();
    let n = items.len() as u64;
    let results = apply_burst(
        record,
        local,
        outside,
        me,
        items,
        era_ms,
        publish,
        narrow_covers,
    )
    .await;
    burst.passes.set(burst.passes.get() + 1);
    burst.events.set(burst.events.get() + n);
    let mut mine: Option<BurstResult> = None;
    let mut others = Vec::new();
    for (i, r) in results {
        if i == id {
            mine = Some(r);
        } else {
            others.push((i, r));
        }
    }
    burst.answer(others);
    mine.unwrap_or_else(|| Err("owed actor: the burst answered no result for its own event".into()))
        .map(|(mut report, p)| {
            // FOLD-14 item 2: the holder waited for the turn; the pass's own checks are the judge's stage (FIX-983:
            // the asks are QUEUED, not published, inside the pass: microseconds, never a record round trip).
            let end = local.now_ms();
            report.wait_ms = turned.saturating_sub(entered);
            let pass = end.saturating_sub(turned);
            report.judge_ms += pass.saturating_sub(
                report.read_ms + report.judge_ms + report.write_ms + report.outside_ms,
            );
            (report, p)
        })
}

/// The words every closure-bound fault begins with ([`CLOSURE_ROUNDS_MAX`], [`CLOSURE_KEYS_MAX`]): the apply could not
/// close over the mirror it holds, a state the bootstrap repairs (D2-ACTOR-B-FOLD-7), never a storage fault.
pub const CLOSURE_BOUND: &str = "owed actor: the closure of ";
/// Is this apply fault the closure's bound (D2-ACTOR-B-FOLD-7), as against a storage or record fault?
pub fn is_closure_bound(e: &str) -> bool {
    e.starts_with(CLOSURE_BOUND)
}

// ---- D2-ACTOR-B-FOLD-7: the cold actor's events are HELD, its bootstrap pages, then the held events apply ----
//
// THE COLD PATH IS A MIGRATION AND RECOVERY PATH, NEVER A REQUEST PATH (the captain's amendment, item 8). After the
// cutover every new identity's first event arrives with no history (its first hop), so its mirror grows with it; a
// cold actor exists only for a standing identity the lever has not reached (bounded by the lever's pace,
// `POST /internal/owed-actor-bootstrap`, 50 identities a call, and the fleet's bootstrap budget, 60 pages a minute),
// a rebuilt actor (HIGH-4; bounded by the alarm's pace, one page of 16 games a second per actor under the same
// budget) and a wipe (bounded by the wipe's list, `GET /internal/owed-actor-identities`, reset 50 a call).
//
// The design's rule (`DESIGN-D2-ACTOR-2026-10-26.md` section 6): "the first event for an identity the actor has
// never seen triggers a one-time hydrate from D1 ... Until it completes the actor serves `pending: true` and the KV
// copy"; section 7: "the actor never loads an identity whole except at bootstrap, which pages". Beta's first window
// (2026-10-28) ran every first event's closure over an EMPTY mirror: pair 20's proof seats (hundreds of games) faulted
// at the 8 rounds on every retry. So an actor without `bootstrapped` HOLDS the event in its own `meta` (one row an
// event, [`HELD_PREFIX`]; no new table: the digest does not move), arms its bootstrap and answers at once
// (`held: true`); the bootstrap's last page applies the held events in seq order over the warm mirror and only then
// stamps `bootstrapped` (one statement that refuses while a held row stands, [`STAMP_BOOTSTRAPPED_SQL`]), so no read
// serves the actor's rows before a held event is applied. A bootstrapped actor whose closure meets its bound
// un-stamps itself, re-arms its bootstrap and holds the event the same way (`owed_actor_closure_rearmed_total`).

/// The `meta` rows of the held events: `held:<seq, 20 digits>:<kind>:<the games' digest>`, so the name's order is the
/// seq order.
pub const HELD_PREFIX: &str = "held:";
/// The held events in seq order.
pub const HELD_LIST_SQL: &str =
    "SELECT name,value FROM meta WHERE name>='held:' AND name<'held;' ORDER BY name";
/// Hold the event unless the actor is bootstrapped: ONE statement (the check and the hold cannot be split by an
/// apply's stamp). A re-hold of the same name keeps the first write time (`atMs`, the cold bar's start) and counts
/// itself in `n`, so the drain's delete (only the value it applied) never takes a re-hold that landed meanwhile.
pub const HOLD_UNLESS_BOOTSTRAPPED_SQL: &str = "INSERT INTO meta(name,value) SELECT ?1,?2 WHERE NOT EXISTS (SELECT 1 FROM meta WHERE name='bootstrapped') ON CONFLICT(name) DO UPDATE SET value=json_set(excluded.value,'$.atMs',COALESCE(json_extract(meta.value,'$.atMs'),json_extract(excluded.value,'$.atMs')),'$.n',COALESCE(json_extract(meta.value,'$.n'),0)+1) RETURNING name";
/// The re-arm's hold (item 3): the actor's `bootstrapped` taken out and the event held, in one batch.
pub const UNSTAMP_SQL: &str = "DELETE FROM meta WHERE name='bootstrapped'";
/// A held event applied: its row deleted only as the drain read it.
pub const HELD_DELETE_SQL: &str = "DELETE FROM meta WHERE name=?1 AND value=?2";
/// The bootstrap's end: `bootstrapped` stamped only while no held event stands (one statement).
pub const STAMP_BOOTSTRAPPED_SQL: &str = "INSERT INTO meta(name,value) SELECT 'bootstrapped',?1 WHERE NOT EXISTS (SELECT 1 FROM meta WHERE name>='held:' AND name<'held;') ON CONFLICT(name) DO UPDATE SET value=excluded.value RETURNING name";
/// The games of one bootstrap page (`owed_judge::IDENTITY_GAMES_AFTER_SQL`'s `LIMIT`): a shorter page is the last.
/// FIX-962 (bsv-low #962): 32 (it was 16). A page's D1 calls do not follow its games (its budget take and its rounds'
/// reads, [`COLD_PAGE_D1_CALLS`]), so the page halves a cold seat's round trips and delays ([`cold_bootstrap_ms`]).
pub const BOOTSTRAP_PAGE_GAMES: usize = 32;
/// FIX-962: the D1 calls of one cold bootstrap page as measured (`fix962_b`): the budget take, then the page's two
/// rounds (FOLD-14: its games read in the first). The same at 16 and 32 games.
pub const COLD_PAGE_D1_CALLS: u64 = 3;
/// FIX-962: the D1 round trip the cold bar is held at: beta's `stages.readMsP95` in the hour of the rehearsal of
/// 2026-10-29 (670 ms), rounded up.
pub const COLD_D1_ROUND_TRIP_MS: u64 = 700;
/// FIX-962: the modelled cold time of one actor's bootstrap of `games` games alone (no fleet wait): its pages (the last
/// one short), each [`COLD_PAGE_D1_CALLS`] round trips of `round_trip_ms` and `native_page_ms` of its own work, one
/// [`BOOTSTRAP_PAGE_DELAY_MS`] apart. Under the base's constants (16 games, 1,000 ms) at 700 ms a call this is
/// 38 x 2,100 + 37,000 = 116,800 ms before native work, the rehearsal's 94,789 ms at a faster median round trip;
/// now 19 x 2,100 + 18 x 500 = 48,900 ms.
pub const fn cold_bootstrap_ms(
    games: u64,
    page_games: u64,
    delay_ms: u64,
    round_trip_ms: u64,
    native_page_ms: u64,
) -> u64 {
    let pages = games / page_games + 1;
    pages * (COLD_PAGE_D1_CALLS * round_trip_ms + native_page_ms) + (pages - 1) * delay_ms
}

/// One held event (its `/apply` body as the actor received it, and when it was held).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeldEvent {
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub seq: Option<i64>,
    pub games: Vec<String>,
    #[serde(default)]
    pub at_ms: Option<u64>,
    #[serde(default)]
    pub mined: bool,
    #[serde(default)]
    pub tip: Option<u64>,
    pub held_at_ms: u64,
    /// D2-ACTOR-D: the delivery's acknowledgement bound (the seq check's); never stored with a held event (a held
    /// event is applied at the drain, always).
    #[serde(default, skip_serializing)]
    pub bound: Option<i64>,
    /// D2-ACTOR-E: the txids the event's write named ([`ApplyOpts::touch`]); never stored with a held event (a held
    /// event's drain reads the whole closure).
    #[serde(default, skip_serializing)]
    pub touch: Option<Vec<String>>,
}
impl HeldEvent {
    pub fn name(&self) -> String {
        let games = json!(self
            .games
            .iter()
            .map(|g| g.to_ascii_lowercase())
            .collect::<Vec<_>>())
        .to_string();
        let digest = hex::encode(bsv_rs::primitives::hash::sha256(games.as_bytes()));
        format!(
            "{HELD_PREFIX}{:020}:{}:{}",
            self.seq.unwrap_or(0).max(0),
            self.kind.as_deref().unwrap_or(""),
            &digest[..16]
        )
    }
    /// The cold bar's start: the event's write time, else when the actor held it.
    pub fn started_ms(&self) -> u64 {
        self.at_ms.unwrap_or(self.held_at_ms)
    }
    fn value(&self) -> String {
        let mut v = json!(self);
        v["n"] = json!(0);
        v.to_string()
    }
}

/// bsv-low #953 item 6 (the captain's ruling, read on beta through `GET /internal/owed-actor-rows`): the tip the actor
/// holds, the greater of its rows' judged tip (`owed_rows.probeTip`) and the tip alarm's stamp (`meta.tipJudged`).
/// `None` only for an actor never judged at a tip.
pub const HELD_TIP_SQL: &str = "SELECT MAX(t) AS tip FROM (SELECT MAX(probeTip) AS t FROM owed_rows WHERE identity=?1 UNION ALL SELECT CAST(value AS INTEGER) FROM meta WHERE name='tipJudged')";

/// [`HELD_TIP_SQL`] read; a fault reads as no tip (the apply then judges as it did).
pub async fn held_tip<L: SqlPort>(local: &L, me: &str) -> Option<u64> {
    local
        .run_batch(vec![Stmt::new(HELD_TIP_SQL, vec![Bind::Text(me.into())])])
        .await
        .ok()?
        .first()?
        .rows
        .first()?["tip"]
        .as_u64()
}

/// How one apply ended: applied, or HELD (the actor not bootstrapped; `rearmed`, a bootstrapped actor whose closure
/// met its bound and un-stamped itself).
#[derive(Debug)]
pub enum Applied {
    Done(ApplyReport, Vec<(String, Value)>),
    Held { rearmed: bool },
}

/// THE APPLY OR THE HOLD (D2-ACTOR-B-FOLD-7, items 1 and 3): an actor without `bootstrapped` holds the event and
/// answers at once (no closure over a mirror the bootstrap has not filled); a bootstrapped one applies it, and a
/// closure-bound fault un-stamps it and holds the event. The caller arms the bootstrap on `Held`. Every other fault
/// (the record's, the mirror's) is the apply's `Err`.
#[allow(clippy::too_many_arguments)]
pub async fn apply_or_hold<R: SqlPort, L: SqlPort, O: Outside>(
    turn: &futures_util::lock::Mutex<()>,
    record: &R,
    local: &L,
    outside: &O,
    me: &str,
    ev: &HeldEvent,
    era_ms: Option<i64>,
    publish: bool,
) -> Result<Applied, String> {
    apply_or_hold_in(None, turn, record, local, outside, me, ev, era_ms, publish).await
}

/// [`apply_or_hold`] with the object's burst (D2-ACTOR-E): `Some((burst, narrow_covers))` applies the event
/// coalesced with every event waiting for the turn ([`apply_coalesced`]), narrowed where its games are warm.
#[allow(clippy::too_many_arguments)]
pub async fn apply_or_hold_in<R: SqlPort, L: SqlPort, O: Outside>(
    coalesce: Option<(&Burst, bool)>,
    turn: &futures_util::lock::Mutex<()>,
    record: &R,
    local: &L,
    outside: &O,
    me: &str,
    ev: &HeldEvent,
    era_ms: Option<i64>,
    publish: bool,
) -> Result<Applied, String> {
    let held = local
        .run_batch(vec![Stmt::new(
            HOLD_UNLESS_BOOTSTRAPPED_SQL,
            vec![Bind::Text(ev.name()), Bind::Text(ev.value())],
        )])
        .await?;
    if held.first().is_some_and(|o| !o.rows.is_empty()) {
        note_holding(record, local, me, ev).await;
        return Ok(Applied::Held { rearmed: false });
    }
    let kind_seq = ev.kind.as_deref().zip(ev.seq);
    // bsv-low #953 item 6: a body with no tip (the reconcile's re-apply as built, any caller whose tip read failed)
    // is judged at the tip the actor holds, never as tip-unknown over rows judged at a tip.
    let tip = match ev.tip {
        Some(t) => Some(t),
        None => held_tip(local, me).await,
    };
    let applied = match coalesce {
        Some((burst, narrow_covers)) => {
            let item = BurstItem {
                games: ev.games.clone(),
                kind_seq: kind_seq.map(|(k, s)| (k.to_string(), s)),
                tip,
                bound: ev.bound,
                touch: ev.touch.clone(),
            };
            apply_coalesced(
                burst,
                turn,
                record,
                local,
                outside,
                me,
                item,
                era_ms,
                publish,
                narrow_covers,
            )
            .await
        }
        None => {
            apply_event_in_turn(
                turn, record, local, outside, me, &ev.games, kind_seq, tip, era_ms, publish,
                ev.bound,
            )
            .await
        }
    };
    match applied {
        Ok((report, pushes)) => {
            // A holding row whose stamp's delete faulted is deleted at the next landed apply.
            clear_holding(record, local, me).await;
            Ok(Applied::Done(report, pushes))
        }
        Err(e) if is_closure_bound(&e) => {
            local.log(&format!(
                "[owed-actor] {e}: the bootstrap re-armed, the event held"
            ));
            local
                .run_batch(vec![
                    Stmt::new(UNSTAMP_SQL, vec![]),
                    Stmt::new(
                        HOLD_UNLESS_BOOTSTRAPPED_SQL,
                        vec![Bind::Text(ev.name()), Bind::Text(ev.value())],
                    ),
                ])
                .await?;
            note_holding(record, local, me, ev).await;
            Ok(Applied::Held { rearmed: true })
        }
        Err(e) => Err(e),
    }
}

/// The object's answer to a HELD event (202): `held`, whether its bootstrap is armed, no ack watermark (nothing
/// applied yet), the games held (the consumer's Phase C acknowledges exactly these, [`as_the_judge`]).
pub fn held_answer(
    me: &str,
    games: &[String],
    at_ms: Option<u64>,
    armed: bool,
    rearmed: bool,
) -> Value {
    json!({"held": true, "bootstrapArmed": armed, "ackSeq": null, "rearmed": rearmed, "identity": me, "heldGames": games, "atMs": at_ms})
}

/// The drain's account: the held events applied, in seq order; their judgements' pushes, the earliest write time
/// among them (the pushes' `atMs`), and the cold time (the first held event's write time to the stamp, item 7),
/// `None` when nothing was held or the drain is not finished; `more` when the per-alarm bound left held events for
/// the next alarm (the fourth delta lens's NEW4-L1).
#[derive(Debug, Default)]
pub struct Drain {
    pub applied: Vec<HeldEvent>,
    pub pushes: Vec<(String, Value)>,
    pub first_at_ms: Option<u64>,
    pub cold_ms: Option<u64>,
    pub more: bool,
}

/// THE DRAIN'S BOUND (the fourth delta lens's NEW4-L1, D2-ACTOR-B-FOLD-8): one alarm applies at most
/// [`DRAIN_CHUNKS_PER_ALARM`] chunks of [`DRAIN_CHUNK_GAMES`] games; the held events left wait for the next alarm
/// (the cursor [`BOOT_CURSOR_DRAIN`]). The union of the held events' games is applied, never each event alone: the
/// apply reads the record as it stands, so one apply of a game covers every held event naming it.
pub const DRAIN_CHUNK_GAMES: usize = 16;
pub const DRAIN_CHUNKS_PER_ALARM: usize = 8;
/// The boot cursor of an actor whose pages are done and whose held events are still draining (NEW4-L1): its next
/// alarm drains again and pages nothing.
pub const BOOT_CURSOR_DRAIN: &str = "drain";
/// The cold bar's start carried across the alarms of one drain (NEW4-L1): the first held event's write time.
pub const META_COLD_FROM: &str = "coldFrom";

/// The union of the held events' games, the first event's games first (so the oldest event always completes inside
/// one alarm's bound), each game once, lowercased.
pub fn drain_union(events: &[HeldEvent]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for ev in events {
        for g in ev.games.iter().map(|g| g.to_ascii_lowercase()) {
            if !out.contains(&g) {
                out.push(g);
            }
        }
    }
    out
}

/// THE DRAIN (item 2, bounded by NEW4-L1): inside ONE turn, the union of the held events' games applied in chunks of
/// [`DRAIN_CHUNK_GAMES`] over the warm mirror, at most [`DRAIN_CHUNKS_PER_ALARM`] chunks; each held event whose games
/// are all applied is recorded in `applied` (its `(kind, seq)`) and its row deleted as read, in one batch; a hold
/// that landed meanwhile is read on the next pass; with nothing held `bootstrapped` is stamped by the one statement
/// that refuses while a held row stands. Past the bound the drain answers `more` (the object re-arms; its cursor
/// [`BOOT_CURSOR_DRAIN`]). A held event's tip is the newer of its own and `tip_now`. An apply's fault ends the drain
/// with the rows standing and the actor unstamped (the bootstrap's alarm runs it again, counted and parked by
/// [`boot_fault`]).
#[allow(clippy::too_many_arguments)]
pub async fn drain_held<R: SqlPort, L: SqlPort, O: Outside>(
    turn: &futures_util::lock::Mutex<()>,
    record: &R,
    local: &L,
    outside: &O,
    me: &str,
    tip_now: Option<u64>,
    era_ms: Option<i64>,
    publish: bool,
) -> Result<Drain, String> {
    let _turn = turn.lock().await;
    let mut drain = Drain::default();
    let mut first: Option<u64> = local
        .run_batch(vec![Stmt::new(
            META_GET_SQL,
            vec![Bind::Text(META_COLD_FROM.into())],
        )])
        .await?
        .into_iter()
        .next()
        .and_then(|o| {
            o.rows
                .first()
                .and_then(|r| r["value"].as_str()?.parse().ok())
        });
    let mut chunks = 0usize;
    loop {
        let list = local
            .run_batch(vec![Stmt::new(HELD_LIST_SQL, vec![])])
            .await?
            .into_iter()
            .next()
            .map(|o| o.rows)
            .unwrap_or_default();
        if list.is_empty() {
            let stamped = local
                .run_batch(vec![Stmt::new(
                    STAMP_BOOTSTRAPPED_SQL,
                    vec![Bind::Text(local.now_ms().to_string())],
                )])
                .await?;
            if stamped.first().is_some_and(|o| !o.rows.is_empty()) {
                break;
            }
            continue;
        }
        let mut held: Vec<(String, String, HeldEvent)> = Vec::new();
        for row in list {
            let (Some(name), Some(value)) = (row["name"].as_str(), row["value"].as_str()) else {
                continue;
            };
            match serde_json::from_str::<HeldEvent>(value) {
                Ok(ev) => held.push((name.into(), value.into(), ev)),
                Err(_) => {
                    local.log(&format!(
                        "[owed-actor] held row {name} does not read: dropped"
                    ));
                    local
                        .run_batch(vec![Stmt::new(
                            HELD_DELETE_SQL,
                            vec![Bind::Text(name.into()), Bind::Text(value.into())],
                        )])
                        .await?;
                }
            }
        }
        if held.is_empty() {
            continue;
        }
        if chunks >= DRAIN_CHUNKS_PER_ALARM {
            drain.more = true;
            break;
        }
        let events: Vec<HeldEvent> = held.iter().map(|(_, _, e)| e.clone()).collect();
        let tip = events.iter().filter_map(|e| e.tip).chain(tip_now).max();
        let union = drain_union(&events);
        let mut done: Vec<String> = Vec::new();
        for chunk in union.chunks(DRAIN_CHUNK_GAMES) {
            if chunks >= DRAIN_CHUNKS_PER_ALARM {
                break;
            }
            let (_, pushes) = apply(record, local, outside, me, chunk, None, tip, era_ms).await?;
            if publish {
                queue_belt_asks(local, me, chunk).await?;
            }
            drain.pushes.extend(pushes);
            done.extend(chunk.iter().cloned());
            chunks += 1;
        }
        let now = local.now_ms() as i64;
        let mut stmts = Vec::new();
        let mut finished = Vec::new();
        for (name, value, ev) in held {
            if !ev
                .games
                .iter()
                .all(|g| done.contains(&g.to_ascii_lowercase()))
            {
                continue;
            }
            if let (Some(kind), Some(seq)) = (ev.kind.as_deref(), ev.seq) {
                stmts.push(Stmt::new(
                    APPLIED_SQL,
                    vec![
                        Bind::Text(kind.into()),
                        Bind::Int(seq),
                        Bind::Int(now),
                        Bind::Text(json!(ev.games).to_string()),
                    ],
                ));
            }
            stmts.push(Stmt::new(
                HELD_DELETE_SQL,
                vec![Bind::Text(name), Bind::Text(value)],
            ));
            first = Some(first.map_or(ev.started_ms(), |f| f.min(ev.started_ms())));
            drain.first_at_ms = Some(
                drain
                    .first_at_ms
                    .map_or(ev.started_ms(), |f| f.min(ev.started_ms())),
            );
            finished.push(ev);
        }
        if let Some(f) = first {
            stmts.push(Stmt::new(
                META_PUT_SQL,
                vec![Bind::Text(META_COLD_FROM.into()), Bind::Text(f.to_string())],
            ));
        }
        if !stmts.is_empty() {
            local.run_batch(stmts).await?;
        }
        finished.sort_by_key(|e| e.seq.unwrap_or(0));
        drain.applied.extend(finished);
    }
    if !drain.more {
        drain.cold_ms = first.map(|f| local.now_ms().saturating_sub(f));
        local
            .run_batch(vec![Stmt::new(
                "DELETE FROM meta WHERE name=?1",
                vec![Bind::Text(META_COLD_FROM.into())],
            )])
            .await?;
    }
    drain.applied.sort_by_key(|e| e.seq.unwrap_or(0));
    Ok(drain)
}

/// One bootstrap page's outcome: the next page's keyset cursor, or the bootstrap's end (the held events drained).
#[derive(Debug)]
pub enum BootStep {
    Page {
        cursor: String,
        games: usize,
    },
    /// NEW4-L1: the pages are done and the held events' drain met its per-alarm bound; the next alarm drains again
    /// (the cursor [`BOOT_CURSOR_DRAIN`]).
    Draining {
        drain: Drain,
    },
    Done {
        games: usize,
        drain: Drain,
    },
}

/// ONE BOOTSTRAP PAGE (the object's alarm runs it after its budget token, MEDIUM-1 and NEW-M2): the identity's next
/// [`BOOTSTRAP_PAGE_GAMES`] games past `cursor` (keyset, LOW-6) applied in one apply; a short page ends the bootstrap
/// with the held events' drain ([`drain_held`]), which stamps `bootstrapped`.
#[allow(clippy::too_many_arguments)]
pub async fn bootstrap_step<R: SqlPort, L: SqlPort, O: Outside>(
    turn: &futures_util::lock::Mutex<()>,
    record: &R,
    local: &L,
    outside: &O,
    me: &str,
    cursor: Option<&str>,
    tip: Option<u64>,
    era_ms: Option<i64>,
    publish: bool,
) -> Result<BootStep, String> {
    if cursor == Some(BOOT_CURSOR_DRAIN) {
        return drain_step(turn, record, local, outside, me, 0, tip, era_ms, publish).await;
    }
    let (at, after) = object::boot_keyset(cursor);
    // D2-ACTOR-B-FOLD-14 (bsv-low #954 item 9): the page's games are read in its first round's record call
    // ([`ApplyOpts::page`]): a page is its rounds' reads (it was one read more, the games, before them).
    let opts = ApplyOpts {
        page: Some((at, after)),
        ..ApplyOpts::default()
    };
    let rows = {
        let _turn = turn.lock().await;
        let (report, _) =
            apply_with(record, local, outside, me, &[], None, tip, era_ms, &opts).await?;
        if publish && !report.page.is_empty() {
            let games: Vec<String> = report.page.iter().map(|(g, _)| g.clone()).collect();
            queue_belt_asks(local, me, &games).await?;
        }
        report.page
    };
    let games: Vec<String> = rows.iter().map(|(g, _)| g.clone()).collect();
    if games.len() < BOOTSTRAP_PAGE_GAMES {
        return drain_step(
            turn,
            record,
            local,
            outside,
            me,
            games.len(),
            tip,
            era_ms,
            publish,
        )
        .await;
    }
    let (last_game, last_at) = rows.last().cloned().unwrap_or_default();
    Ok(BootStep::Page {
        cursor: json!([last_at, last_game]).to_string(),
        games: games.len(),
    })
}

/// The bootstrap's last step: the held events' drain; `Done` (its holding row cleared, [`clear_holding`]) once the
/// actor is stamped, `Draining` past the drain's per-alarm bound (NEW4-L1).
#[allow(clippy::too_many_arguments)]
async fn drain_step<R: SqlPort, L: SqlPort, O: Outside>(
    turn: &futures_util::lock::Mutex<()>,
    record: &R,
    local: &L,
    outside: &O,
    me: &str,
    games: usize,
    tip: Option<u64>,
    era_ms: Option<i64>,
    publish: bool,
) -> Result<BootStep, String> {
    let drain = drain_held(turn, record, local, outside, me, tip, era_ms, publish).await?;
    if drain.more {
        return Ok(BootStep::Draining { drain });
    }
    clear_holding(record, local, me).await;
    Ok(BootStep::Done { games, drain })
}

// ---- D2-ACTOR-B-FOLD-8: a stuck actor counted, served and parked (the fourth delta lens's NEW4-M2) ----
//
// A held answer is acknowledged (Phases B and C), so an actor whose bootstrap can never finish was invisible: its
// seat served no press (Rule 23 holds: display only, or 503 `pending`) and nothing served named it. Now (1) every
// bootstrap-page and drain fault is counted (`owed_actor_bootstrap_faults_total`, `/health.owedActor.bootstrapFaults`);
// (2) a holding actor keeps ONE D1 row, `owed-actor:holding:<identity>` in the belt's ledger, written at its first
// hold ([`note_holding`]) and deleted by its stamp ([`clear_holding`]), served as `/health.owedActor.holding`
// {`count`, `oldestMs`} from the health snapshot the writers compose (no statement on the read); (3) a page that has
// faulted [`BOOT_FAULTS_PARK`] alarms in a row is PARKED ([`boot_fault`]): its alarm stops retrying it, its line names
// it, and the lever's `/bootstrap` clears the park and re-arms from the parked cursor.

/// The fourth delta lens's NEW4-L4: does the alarm run the tip window? Only with a gate held and NO bootstrap cursor
/// (a re-armed actor's window re-held its games on every page alarm, and the drain judges at the tip anyway).
pub fn tip_window_due(gate: bool, boot_cursor: bool) -> bool {
    gate && !boot_cursor
}

/// The belt ledger's prefix of the holding actors' rows.
pub const HOLDING_PREFIX: &str = "owed-actor:holding:";
/// The holding row, written once (its `tickedAtMs` the first hold; a later hold writes nothing). `?1` the identity,
/// `?2` the row's cursor (the first held event's kind and seq), `?3` now.
pub const HOLDING_PUT_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('owed-actor:holding:'||?1,?2,CAST(?3 AS INTEGER)) ON CONFLICT(name) DO NOTHING";
/// The stamp's delete of the holding row.
pub const HOLDING_DELETE_SQL: &str =
    "DELETE FROM owed_belt_state WHERE name='owed-actor:holding:'||?1";
/// The actor's own note that its holding row is written (so a hold writes D1 once, never once an event).
pub const META_HOLDING: &str = "holding";
/// The consecutive bootstrap-page faults ([`boot_fault`]) and the park.
pub const META_BOOT_FAULTS: &str = "bootFaults";
pub const META_BOOT_PARKED: &str = "bootParked";
/// The alarms a page may fault in a row before it is parked.
pub const BOOT_FAULTS_PARK: u32 = 5;
/// A faulted page's retry.
pub const BOOT_FAULT_RETRY_MS: u64 = 60_000;

async fn meta_value<L: SqlPort>(local: &L, name: &str) -> Option<String> {
    local
        .run_batch(vec![Stmt::new(META_GET_SQL, vec![Bind::Text(name.into())])])
        .await
        .ok()?
        .into_iter()
        .next()?
        .rows
        .first()?["value"]
        .as_str()
        .map(str::to_string)
}

/// The first hold of a cold or re-armed actor writes its holding row (and the health snapshot, at most once a
/// minute fleet-wide); later holds write nothing to D1. A D1 fault is logged and retried at the next hold.
pub async fn note_holding<R: SqlPort, L: SqlPort>(record: &R, local: &L, me: &str, ev: &HeldEvent) {
    if meta_value(local, META_HOLDING).await.is_some() {
        return;
    }
    let now = local.now_ms();
    let put = record
        .run_batch(vec![
            Stmt::new(
                HOLDING_PUT_SQL,
                vec![
                    Bind::Text(me.to_ascii_lowercase()),
                    Bind::Text(json!({"kind": ev.kind, "seq": ev.seq}).to_string()),
                    Bind::Int(now as i64),
                ],
            ),
            health_snapshot_stmt(now, false),
        ])
        .await;
    match put {
        Ok(_) => {
            let _ = local
                .run_batch(vec![Stmt::new(
                    META_PUT_SQL,
                    vec![Bind::Text(META_HOLDING.into()), Bind::Text(now.to_string())],
                )])
                .await;
        }
        Err(e) => local.log(&format!(
            "[owed-actor] holding row of {me} not written: {e}"
        )),
    }
}

/// The stamp's end of a hold: the holding row deleted (and the snapshot, at most once a minute), then the actor's
/// note. Run at the stamp and again at each landed apply while the note stands (a delete that faulted is retried).
pub async fn clear_holding<R: SqlPort, L: SqlPort>(record: &R, local: &L, me: &str) {
    if meta_value(local, META_HOLDING).await.is_none() {
        return;
    }
    let now = local.now_ms();
    let deleted = record
        .run_batch(vec![
            Stmt::new(
                HOLDING_DELETE_SQL,
                vec![Bind::Text(me.to_ascii_lowercase())],
            ),
            health_snapshot_stmt(now, false),
        ])
        .await;
    match deleted {
        Ok(_) => {
            let _ = local
                .run_batch(vec![Stmt::new(
                    "DELETE FROM meta WHERE name=?1",
                    vec![Bind::Text(META_HOLDING.into())],
                )])
                .await;
        }
        Err(e) => local.log(&format!(
            "[owed-actor] holding row of {me} not deleted: {e}"
        )),
    }
}

/// What the alarm does after a bootstrap-page or drain fault ([`boot_fault`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootFault {
    /// Retry in [`BOOT_FAULT_RETRY_MS`]; `faults` in a row so far.
    Retry { faults: u32 },
    /// Parked at the [`BOOT_FAULTS_PARK`]-th fault: no retry; the line names it.
    Parked { faults: u32, line: String },
}

/// A bootstrap page (or its drain) faulted: the consecutive count moves, and at [`BOOT_FAULTS_PARK`] the page is
/// PARKED (`bootParked`, the reason): the alarm stops retrying it until the lever's `/bootstrap` clears the park
/// ([`clear_boot_park`]). The caller counts `owed_actor_bootstrap_faults_total` once a fault.
pub async fn boot_fault<L: SqlPort>(local: &L, me: &str, reason: &str) -> BootFault {
    let faults = meta_value(local, META_BOOT_FAULTS)
        .await
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0)
        + 1;
    let mut stmts = vec![Stmt::new(
        META_PUT_SQL,
        vec![
            Bind::Text(META_BOOT_FAULTS.into()),
            Bind::Text(faults.to_string()),
        ],
    )];
    let verdict = if faults >= BOOT_FAULTS_PARK {
        stmts.push(Stmt::new(
            META_PUT_SQL,
            vec![
                Bind::Text(META_BOOT_PARKED.into()),
                Bind::Text(
                    json!({"faults": faults, "reason": reason, "atMs": local.now_ms()}).to_string(),
                ),
            ],
        ));
        BootFault::Parked {
            faults,
            line: format!("[owed-actor] bootstrap PARKED {me} after {faults} faults: {reason}"),
        }
    } else {
        BootFault::Retry { faults }
    };
    let _ = local.run_batch(stmts).await;
    verdict
}
/// A page that landed: the consecutive faults cleared.
pub async fn boot_page_landed<L: SqlPort>(local: &L) {
    let _ = local
        .run_batch(vec![Stmt::new(
            "DELETE FROM meta WHERE name=?1",
            vec![Bind::Text(META_BOOT_FAULTS.into())],
        )])
        .await;
}
/// Is the actor's bootstrap parked?
pub async fn boot_parked<L: SqlPort>(local: &L) -> bool {
    meta_value(local, META_BOOT_PARKED).await.is_some()
}
/// The lever's `/bootstrap` on a parked actor: the park and the fault count cleared (the cursor kept); answers
/// whether it was parked (once: the next call finds no park).
pub async fn clear_boot_park<L: SqlPort>(local: &L) -> bool {
    if !boot_parked(local).await {
        return false;
    }
    let _ = local
        .run_batch(vec![Stmt::new(
            "DELETE FROM meta WHERE name IN (?1,?2)",
            vec![
                Bind::Text(META_BOOT_PARKED.into()),
                Bind::Text(META_BOOT_FAULTS.into()),
            ],
        )])
        .await;
    true
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
/// D2-ACTOR-B-FOLD-11 (bsv-low #957 item 2): `tip`, the judge's input tip `owed.rs` writes into several families' facts,
/// is the judgement's clock too (the verdict a tip moves is compared apart: `claimable`, `blocksToGate`, `tipUnknown`).
pub const CLOCK_FACTS: [&str; 5] = ["ageMs", "chainProbeAgeMs", "beltNextAtMs", "asOfMs", "tip"];
/// FIX-963 (bsv-low #963): the OBSERVATION STAMPS inside a stored fact, `(fact, key)`: the instant a word was seen,
/// not the word. `spenderPresence.at` is the one a stored row carries: `routes::mark_pending_spender` takes the
/// answer's `observed_at_ms`, else the judge's own `now_ms` less `chainProbeAgeMs`, and with no probe age (an
/// index- or memo-less word) that is the judge's `now`, so two judges of one record one apply apart stamp it apart.
/// The presence is compared by `{word, tier, source}`; the served rows keep the stamp. The word facts
/// (`coinWord`, `spenderWord`, `fundingWord`, `tracker`) are stripped at storage and joined at the read, so their
/// stamps never reach a compare.
pub const STAMP_FACTS: [(&str, &str); 1] = [("spenderPresence", "at")];
pub fn compared_facts(row: &Value) -> Value {
    let mut facts: Value = row["facts"]
        .as_str()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(Value::Null);
    if let Some(m) = facts.as_object_mut() {
        for k in CLOCK_FACTS {
            m.remove(k);
        }
        for (fact, k) in STAMP_FACTS {
            if let Some(o) = m.get_mut(fact).and_then(Value::as_object_mut) {
                o.remove(k);
            }
        }
    }
    facts
}

/// The operator's rows lever's answer (the captain's item 10): the actor's state and rows (`/stored`, or its fault),
/// D1's rows, the outpoints that differ and both digests, by the reconcile's own functions.
/// D2-ACTOR-B-FOLD-13 (bsv-low #958): beside its rows, `d1JudgedUnder` ([`d1_judged_under`]: the judge version D1's
/// rows of the identity were judged under, or `"unknown"`) and `d1Rejudge` (the fleet re-judge's stamp and whether it
/// has passed this identity), so a reader can tell which side is stale. D2-ACTOR-B-FOLD-15 (NEW8-M1): a difference
/// whose fields are only tip-derived ([`tip_derived_rows`]) between two different judged tips is served as `tipLag`
/// (both tips in `tipLagTips`), apart from `diff`.
pub fn rows_view(
    identity: &str,
    actor: std::result::Result<Value, String>,
    d1: &[Value],
    rejudge: Option<&Value>,
    computed_at: Option<i64>,
) -> Value {
    let (actor_rows, actor_state) = match actor {
        Ok(a) => {
            let rows = a["rows"].as_array().cloned().unwrap_or_default();
            (
                Some(rows.clone()),
                json!({
                    "bootstrapped": a["bootstrapped"],
                    "bootstrapArmed": a["bootstrapArmed"],
                    "bootParked": a["bootParked"],
                    "held": a["held"],
                    "ackSeq": a["ackSeq"],
                    "judgedAtMs": a["judgedAtMs"],
                    "fresh": a["fresh"] == true,
                    "rows": rows,
                }),
            )
        }
        Err(e) => (None, json!({"error": e})),
    };
    json!({
        "identity": identity,
        "actor": actor_state,
        "d1": {"rows": d1},
        "d1JudgedUnder": d1_judged_under(rejudge, identity, computed_at),
        "d1Rejudge": {
            "stamp": rejudge_served(rejudge),
            "passed": rejudge_passed(rejudge, identity),
        },
        // D2-ACTOR-B-FOLD-15 (NEW8-M1): a difference that is only tip-derived between two judged tips is a TIP LAG
        // (`tipLag`, both sides' tips in `tipLagTips`), never in `diff`.
        "diff": actor_rows.as_deref().map(|a| {
            let lag = tip_derived_rows(d1, a);
            diff_rows(d1, a).into_iter().filter(|o| !lag.contains(o)).collect::<Vec<_>>()
        }),
        "tipLag": actor_rows.as_deref().map(|a| tip_derived_rows(d1, a)),
        "tipLagTips": actor_rows.as_deref().map(|a| {
            let lag = tip_derived_rows(d1, a);
            json!({"d1": tips_of(d1, &lag), "actor": tips_of(a, &lag)})
        }),
        "digests": {"d1": rows_digest(d1), "actor": actor_rows.as_deref().map(rows_digest)},
    })
}

/// FIX-964 (bsv-low #964): the operator's rows lever's read of the identity's pending marks (the queue's unacknowledged
/// set), one row a game holding one: the search of the UNIQUE key's index on its `identity` prefix.
pub const LEVER_PENDING_SQL: &str = "SELECT gameId,MIN(CAST(markedAtMs AS INTEGER)) AS oldestMs FROM owed_dirty WHERE identity=?1 GROUP BY gameId";

/// PURE (FIX-964): the outpoints of a [`rows_view`]'s `diff` whose game holds a pending mark ([`LEVER_PENDING_SQL`]'s
/// rows): a difference the queue's backlog explains (`heldByLag`), named beside `diff`, which keeps naming them.
pub fn held_by_lag(view: &Value, pending: &[Value]) -> Vec<String> {
    let games: BTreeSet<String> = pending
        .iter()
        .filter_map(|r| r["gameId"].as_str().map(str::to_ascii_lowercase))
        .collect();
    let game_of = |o: &str| -> Option<String> {
        view["d1"]["rows"]
            .as_array()
            .into_iter()
            .flatten()
            .chain(view["actor"]["rows"].as_array().into_iter().flatten())
            .find(|r| r["outpoint"] == o)
            .and_then(|r| r["gameId"].as_str())
            .map(str::to_ascii_lowercase)
    };
    view["diff"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|o| game_of(o).is_some_and(|g| games.contains(&g)))
        .map(str::to_string)
        .collect()
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

/// PURE (D2-ACTOR-B-FOLD-12, the seventh delta lens's NEW7-L2): the outpoints on BOTH sides that [`diff_rows`] reads
/// equal (the same verdict) but whose JUDGED TIP differs: `facts.tip` (a clock fact the compare drops, FOLD-11) or the
/// stored `probeTip` column (never compared). A lag, not a STOP: counted apart (`owed_actor_compare_tip_lag_total`,
/// served `/health.owedActor.tipLagDiffsTotal`), where FOLD-11's drop left an actor judging at a lagging tip
/// unobservable (the read's #669 degrade withholds a courier-dependent sweep there that D1's fresher row presses: the
/// safe direction). A verdict difference is [`diff_rows`]'s, never this.
pub fn tip_lag_rows(d1: &[Value], actor: &[Value]) -> Vec<String> {
    let differ: BTreeSet<String> = diff_rows(d1, actor).into_iter().collect();
    let tip_of = |r: &Value| -> (Option<i64>, Option<i64>) {
        let facts: Value = r["facts"]
            .as_str()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or(Value::Null);
        let n = |v: &Value| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64));
        (n(&facts["tip"]), n(&r["probeTip"]))
    };
    let actor_by: BTreeMap<&str, &Value> = actor
        .iter()
        .filter_map(|r| Some((r["outpoint"].as_str()?, r)))
        .collect();
    let mut out: Vec<String> = d1
        .iter()
        .filter_map(|r| {
            let o = r["outpoint"].as_str()?;
            let a = actor_by.get(o)?;
            (!differ.contains(o) && tip_of(r) != tip_of(a)).then(|| o.to_string())
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

/// D2-ACTOR-B-FOLD-15 (the eighth delta lens's NEW8-M1): the facts a TIP moves with the record unchanged, beside
/// `facts.tip` (a [`CLOCK_FACTS`] key) and the `probeTip` column (never compared): a gate's distance and the
/// tip-unknown flag.
pub const TIP_DERIVED_FACTS: [&str; 2] = ["blocksToGate", "tipUnknown"];

/// The tip a stored row was judged at: `facts.tip`, else the `probeTip` column (`None`: neither).
pub fn judged_tip(r: &Value) -> Option<i64> {
    let facts: Value = r["facts"]
        .as_str()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(Value::Null);
    let n = |v: &Value| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64));
    n(&facts["tip"])
        .or_else(|| n(&r["probeTip"]))
        .or_else(|| n(&facts["servedTip"]))
}

/// PURE (D2-ACTOR-B-FOLD-15, NEW8-M1): the outpoints [`diff_rows`] reads different whose difference is ONLY
/// tip-derived: the row on both sides, equal once [`TIP_DERIVED_FACTS`] are taken out too, and judged at two
/// different tips ([`judged_tip`]). A tip lag, never a verdict difference: the operator's route serves them apart
/// (`tipLag`) and the reconcile counts an actor's own lag apart (`owed_actor_compare_tip_lag_total`).
pub fn tip_derived_rows(d1: &[Value], actor: &[Value]) -> Vec<String> {
    let strip = |r: &Value| -> Value {
        let mut r = r.clone();
        let mut facts = compared_facts(&r);
        if let Some(m) = facts.as_object_mut() {
            for k in TIP_DERIVED_FACTS {
                m.remove(k);
            }
            probe_degrade_undone(m);
        }
        r["facts"] = json!(facts.to_string());
        r
    };
    let by = |rows: &[Value]| -> BTreeMap<String, Value> {
        rows.iter()
            .filter_map(|r| Some((r["outpoint"].as_str()?.to_string(), r.clone())))
            .collect()
    };
    let (a, b) = (by(d1), by(actor));
    diff_rows(d1, actor)
        .into_iter()
        .filter(|o| match (a.get(o), b.get(o)) {
            (Some(x), Some(y)) => {
                judged_tip(x) != judged_tip(y) && diff_rows(&[strip(x)], &[strip(y)]).is_empty()
            }
            _ => false,
        })
        .collect()
}

/// FIX-1009 (bsv-low #1009): PURE, the #669 / #808 (b) degrade of a hop's courier probe UNDONE in a row's compared
/// facts. A courier `"unspent"` taken at `chainProbeTip` is served by a judge whose tip is past it as
/// `chainProbe: "index-unspent"` with `tipMoved: true` and `servedTip` (`owed::derive_owed_rows`' index-unspent arm):
/// nothing moved but the served tip. So a row carrying exactly that (`index-unspent`, `tipMoved` true, a
/// `chainProbeTip` below its `servedTip`) compares as the probe's own word at its tip (`unspent`, the two tip facts
/// taken out); a row of the other side that states the probe's word with no move compares the same, and the
/// remaining facts (`chainProbeTip`, the claim, `claimable`, the filed sweep) and the row's family, reason and
/// claimable must still be equal for [`tip_derived_rows`] to read the pair a tip lag. An `index-unspent` with no
/// probe (the courier silent, no `tipMoved`) is never undone: against a corroborated `unspent` it stays a diff.
fn probe_degrade_undone(m: &mut serde_json::Map<String, Value>) {
    let n = |v: Option<&Value>| v.and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)));
    let degraded = m.get("chainProbe").and_then(Value::as_str) == Some("index-unspent")
        && m.get("tipMoved") == Some(&json!(true))
        && matches!(
            (n(m.get("chainProbeTip")), n(m.get("servedTip"))),
            (Some(probe), Some(served)) if probe < served
        );
    if degraded {
        m.insert("chainProbe".into(), json!("unspent"));
        m.remove("tipMoved");
        m.remove("servedTip");
    }
}

/// The distinct tips the rows of `outpoints` were judged at, on one side (the route's `tipLagTips`).
fn tips_of(rows: &[Value], outpoints: &[String]) -> Vec<i64> {
    rows.iter()
        .filter(|r| outpoints.iter().any(|o| r["outpoint"] == o.as_str()))
        .filter_map(judged_tip)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// THE DIFF LINES NAME WHAT DIFFERS (the captain's item 12, D2-ACTOR-B-FOLD-8): for the first [`DIFF_DETAIL_MAX`]
/// differing outpoints, each compared field that differs with both values (`family d1=unbound actor=hop-stranded`,
/// `claimable d1=null actor=1`, a facts key by name: `facts.claim d1=.. actor=..`), the fields [`diff_rows`] compares;
/// a row on one side only is `absent`.
pub const DIFF_DETAIL_MAX: usize = 3;
pub fn diff_detail(d1: &[Value], actor: &[Value], outpoints: &[String]) -> String {
    let find = |rows: &[Value], o: &str| rows.iter().find(|r| r["outpoint"] == o).cloned();
    let short = |v: &Value| -> String {
        let t = match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        if t.chars().count() > 80 {
            format!("{}..", t.chars().take(80).collect::<String>())
        } else {
            t
        }
    };
    let mut parts = Vec::new();
    for o in outpoints.iter().take(DIFF_DETAIL_MAX) {
        let (a, b) = (find(d1, o), find(actor, o));
        let fields = match (&a, &b) {
            (Some(a), Some(b)) => {
                let mut f = Vec::new();
                for k in [
                    "family",
                    "gameId",
                    "sats",
                    "opponentIdentity",
                    "atHeight",
                    "reason",
                    "claimable",
                ] {
                    if a[k] != b[k] {
                        f.push(format!("{k} d1={} actor={}", short(&a[k]), short(&b[k])));
                    }
                }
                let (fa, fb) = (compared_facts(a), compared_facts(b));
                let keys: BTreeSet<String> = fa
                    .as_object()
                    .into_iter()
                    .chain(fb.as_object())
                    .flat_map(|m| m.keys().cloned())
                    .collect();
                for k in keys {
                    if fa[&k] != fb[&k] {
                        f.push(format!(
                            "facts.{k} d1={} actor={}",
                            short(&fa[&k]),
                            short(&fb[&k])
                        ));
                    }
                }
                f.join(", ")
            }
            (Some(_), None) => "d1=present actor=absent".into(),
            (None, Some(_)) => "d1=absent actor=present".into(),
            (None, None) => "absent on both".into(),
        };
        parts.push(format!("{o} [{fields}]"));
    }
    parts.join("; ")
}

/// The KV display copy (MEDIUM-9): the actor's stored rows (`ROWS_SQL`'s), written at most once a second.
pub fn kv_copy_body(identity: &str, rows: &[Value], judged_at_ms: u64) -> Value {
    json!({"identity": identity, "judgedAtMs": judged_at_ms, "rows": rows})
}
/// The delta lens's NEW-L2: the copy carries the era it was written in ([`actor_epoch`]), and a read serves no copy of
/// another era (an era reset or a rebuild's copy shows the previous era's rows otherwise, display only, until the
/// bootstrap ends). A copy with no era (written before the fold) is another era's.
pub fn kv_copy_of_era(copy: Option<Value>, epoch: &str) -> Option<Value> {
    copy.filter(|c| c["epoch"].as_str() == Some(epoch))
}
/// The reason a bootstrapping actor's read is served with (the D2-ACTOR-B lens's MEDIUM-1).
pub const BOOTSTRAP_REASON: &str = "the judge is building this identity's rows; re-read";

/// The read's answer when the actor's rows cannot be served as complete: the actor out of reach, or its bootstrap
/// unfinished (the D2-ACTOR-B lens's MEDIUM-1 and MEDIUM-2).
#[derive(Debug, Clone, PartialEq)]
pub enum Fallback {
    /// A 200 body in the owed wire's own shape (`owed_read::Snapshot::body`: `v`, the identity, the rows with their
    /// facts an object), each row display only (`facts.claimable: false`, `facts.claimReason`, `facts.displayOnly`),
    /// the body `pending: true`, `refresh: "shed"` and `retryAfterMs` (the client re-reads it).
    Display(Value),
    /// No copy to show: a 503 naming `pending: true` (the client keeps its last list and says "could not load";
    /// a 200 with `rows: []` reads "Nothing is owed to you right now", Rule 23).
    Unavailable(Value),
}

/// MEDIUM-2: the fallback of `identity`'s read from its KV copy (or none), through the wire's own builder.
pub fn fallback_body(identity: &str, copy: Option<&Value>, reason: &str, source: &str) -> Fallback {
    let unavailable = || {
        Fallback::Unavailable(
            json!({"v": crate::owed::OWED_WIRE_VERSION, "identity": identity, "rows": null, "pending": true, "retryAfterMs": 1500, "source": source, "reason": reason}),
        )
    };
    let rows: Vec<Value> = copy
        .and_then(|c| c["rows"].as_array())
        .into_iter()
        .flatten()
        .filter(|r| {
            r["outpoint"].is_string()
                && r["facts"]
                    .as_str()
                    .and_then(|f| serde_json::from_str::<Value>(f).ok())
                    .is_some_and(|f| f.is_object())
        })
        .cloned()
        .collect();
    if rows.is_empty() {
        return unavailable();
    }
    let snapshot = crate::owed_read::Snapshot {
        state: Some(
            json!({"computedAtMs": copy.and_then(|c| c["judgedAtMs"].as_u64()), "rows": rows.len(), "truncated": 0}),
        ),
        joined: rows,
        ..Default::default()
    };
    let Ok(mut body) = snapshot.body(identity, None, &BTreeMap::new()) else {
        return unavailable();
    };
    for r in body["rows"].as_array_mut().into_iter().flatten() {
        r["facts"]["claimable"] = json!(false);
        r["facts"]["claimReason"] = json!(reason);
        r["facts"]["displayOnly"] = json!(true);
    }
    body["pending"] = json!(true);
    body["refresh"] = json!("shed");
    body["retryAfterMs"] = json!(1500);
    body["displayOnly"] = json!(true);
    body["source"] = json!(source);
    body["reason"] = json!(reason);
    Fallback::Display(body)
}

/// THE ERA (the D2-ACTOR-B lens's MEDIUM-5): the stamp an actor holds in `meta` (`epoch`), the app layer's two render
/// vars the wipe moves (`STORAGE_EPOCH`, then `WRITTEN_OFF_BEFORE_MS`, WIPE-REHEARSAL-2 step 6 and WIPE-REHEARSAL-3's
/// era bump). Read on every apply, read and alarm: an actor holding another era's stamp is RESET (`deleteAll`) before
/// it answers, so no actor that re-hydrated from the pre-wipe D1 (the queue, the sweep, the belt, its own alarm, a
/// read, between step 4a's reset and the bump) serves that era afterwards.
pub const META_EPOCH: &str = "epoch";
pub fn actor_epoch(storage_epoch: Option<String>, written_off_before_ms: Option<i64>) -> String {
    format!(
        "{}|{}",
        crate::logic::normalize_storage_epoch(storage_epoch).unwrap_or_default(),
        written_off_before_ms.map_or(String::new(), |m| m.to_string())
    )
}
/// What an actor does with the era stamp it holds against the render's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochVerdict {
    /// No stamp held (a fresh actor, or one minted before the stamp existed): stamp it.
    Stamp,
    Keep,
    /// Another era's stamp: reset the actor (its storage and its alarm), then stamp.
    Reset,
}
pub fn epoch_verdict(held: Option<&str>, current: &str) -> EpochVerdict {
    match held {
        None => EpochVerdict::Stamp,
        Some(h) if h == current => EpochVerdict::Keep,
        Some(_) => EpochVerdict::Reset,
    }
}

/// THE TIP ALARM (the D2-ACTOR-B lens's MEDIUM-4): the games whose rows the tips since the last one judged have
/// reached: a gate in `(?2, ?1]` (`?2` the last tip the alarm judged, `meta.tipJudged`, -1 for none; `?1` the tip
/// now), or a claimable courier-dated sweep (`json_extract(facts,'$.status') IS NOT 'unspent'`, the D1 rule's
/// D-H1) judged below the tip, at most 1,024 games (applied 64 at a time). A gate already open is never selected
/// again; a re-judged sweep carries the tip it was judged at, so it waits for the next block.
pub const GATED_GAMES_SQL: &str = "SELECT DISTINCT gameId FROM owed_rows WHERE (gateHeight<=?1 AND gateHeight>?2) OR (family='hop-stranded' AND claimable=1 AND probeTip<?1 AND json_extract(facts,'$.status') IS NOT 'unspent') LIMIT 1024";
/// THE TIP WINDOW'S FAULTS (the second delta lens's NEW2-L5). A game whose apply faults in the tip alarm's window is
/// carried in the actor's `meta` (`tipFaults`, game to consecutive faults), so `tipJudged` moves past the window
/// whatever one game does and the window's other games are never re-applied for it; the carried game is retried on
/// the next alarms, and at [`TIP_FAULTS_MAX`] consecutive faults it is PARKED: left out of every later window until an
/// apply of it lands by another road (an event, the reconcile; `apply_route` clears its entry). Fail-safe: a parked
/// game keeps the rows of its last landed judgement, and the read withholds what its marks downgrade.
///
/// The third delta lens's LOW-1: a fault counts toward the park only in an alarm where some apply of the window
/// LANDED. An alarm in which no apply landed (the actor's D1 record unreachable, say) is the ACTOR's fault, not a
/// game's: its games are carried at the count they held (a new one at 0), retried, and never parked for it. So a
/// game that faults alone among landed games is parked at its third such fault; a window of one game that always
/// faults is retried on every alarm, never parked (stated). Each game parked is counted
/// (`owed_actor_tip_parked_total`, served `/health.owedActor.tipParked`).
pub const TIP_FAULTS_MAX: u32 = 3;

/// One tip alarm's window ([`tip_window`]).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TipWindow {
    /// The `tipJudged` the alarm stamps: the tip, always (the faulted games ride `faults`).
    pub tip_judged: i64,
    /// The `tipFaults` the alarm stores (a parked game's entry kept).
    pub faults: BTreeMap<String, u32>,
    /// The games whose apply faulted in this alarm, named in its line.
    pub faulted: Vec<String>,
    /// The games this alarm parked.
    pub parked: Vec<String>,
    /// `owed_actor_tip_faults_total`'s delta: 1 for an alarm with a faulted game, once whatever their number.
    pub counted: u64,
    /// LOW-1: `owed_actor_tip_parked_total`'s delta, the games this alarm parked.
    pub parked_counted: u64,
    /// LOW-1: whether any apply of this alarm landed (else no fault counted toward the park).
    pub landed: bool,
    /// The alarm's line, when a game faulted.
    pub line: Option<String>,
}

/// The tip alarm's window: the games `selected` for `(last, tip]` and the carried `held` faults' games below the park,
/// applied [`GAMES_PER_APPLY`] at a time by `apply`; a chunk that faults is applied one game at a time, so only the
/// faulting game is carried.
pub async fn tip_window<F, Fut>(
    identity: &str,
    selected: Vec<String>,
    held: &BTreeMap<String, u32>,
    tip: i64,
    mut apply: F,
) -> TipWindow
where
    F: FnMut(Vec<String>) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let mut games = selected;
    for (g, n) in held {
        if *n < TIP_FAULTS_MAX && !games.contains(g) {
            games.push(g.clone());
        }
    }
    games.retain(|g| held.get(g).is_none_or(|n| *n < TIP_FAULTS_MAX));
    let mut faults = held.clone();
    let mut faulted = Vec::new();
    let mut landed = false;
    for chunk in games.chunks(GAMES_PER_APPLY) {
        if apply(chunk.to_vec()).await {
            landed = true;
            for g in chunk {
                faults.remove(g);
            }
            continue;
        }
        for g in chunk {
            if chunk.len() > 1 && apply(vec![g.clone()]).await {
                landed = true;
                faults.remove(g);
            } else {
                faulted.push(g.clone());
            }
        }
    }
    // LOW-1: a fault counts toward the park only when some apply of this alarm landed; otherwise it is the actor's,
    // and each faulted game is carried at the count it held.
    for g in &faulted {
        let n = faults.entry(g.clone()).or_default();
        if landed {
            *n += 1;
        }
    }
    let parked: Vec<String> = faulted
        .iter()
        .filter(|g| faults.get(*g).is_some_and(|n| *n >= TIP_FAULTS_MAX))
        .cloned()
        .collect();
    let line = (!faulted.is_empty()).then(|| {
        format!(
            "[owed-actor] TIP WINDOW FAULT {identity}: games {faulted:?} faulted (carried, retried up to {TIP_FAULTS_MAX} alarms{}); parked {parked:?}; tipJudged {tip}",
            if landed { "" } else { "; no apply landed: the actor's fault, not counted toward the park" }
        )
    });
    TipWindow {
        tip_judged: tip,
        counted: u64::from(!faulted.is_empty()),
        parked_counted: parked.len() as u64,
        landed,
        faults,
        faulted,
        parked,
        line,
    }
}

/// The next gate above the tip (`?1`) and whether a courier-dated sweep is held (a block re-dates it).
pub const NEXT_GATE_SQL: &str = "SELECT (SELECT MIN(gateHeight) FROM owed_rows WHERE gateHeight>?1) AS gate,(SELECT 1 FROM owed_rows WHERE family='hop-stranded' AND claimable=1 AND probeTip IS NOT NULL AND json_extract(facts,'$.status') IS NOT 'unspent' LIMIT 1) AS sweeps";
/// About one block.
pub const BLOCK_MS: u64 = 600_000;
/// When the tip alarm is next owed: a held courier-dated sweep at the next block; else the next gate at its height's
/// expected time (a block of [`BLOCK_MS`] a height, at least one); else none (the hook's `tip` event stays the
/// second belt).
pub fn next_tip_alarm_ms(
    now: u64,
    tip: Option<u64>,
    next_gate: Option<u64>,
    sweeps: bool,
) -> Option<u64> {
    if sweeps {
        return Some(now + BLOCK_MS);
    }
    let gate = next_gate?;
    let blocks = tip.map_or(1, |t| gate.saturating_sub(t).max(1));
    Some(now + blocks.saturating_mul(BLOCK_MS))
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
    /// D2-ACTOR-B-FOLD-7: the cold time of each bootstrap that applied held events.
    pub cold: Meter,
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
            "coldMsP95": self.cold.quantile(0.95),
            "coldMsMax": self.cold.max(),
            "coldSamples": self.cold.samples.len(),
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
pub fn note_apply(ms: u64) {
    BARS.with(|b| b.borrow_mut().apply.note(ms));
}
pub fn served_bars(now_ms: u64) -> Value {
    BARS.with(|b| b.borrow().served(now_ms))
}

/// THE DURABLE BARS (the D2-ACTOR-B lens's MEDIUM-7): each answer's samples by CLASS, `direct` (a writer's direct
/// call), `queue` (the consumer's delivery, Phase A's deferred apply included) and `mined` (the MINED word's event,
/// the overlay's `proof` frame at `/arc-ingest` MINED, [`mined_word_event`]; the collect-after-mined bar, its sample
/// the event's write time to the judgement at its FIRST apply, on the direct road since FIX-974, bsv-low #974), kept
/// in the belt's ledger, one row a class (`owed-actor:bars:<class>`, its `cursor`
/// the last [`METER_SAMPLES`] of each meter), so `/health.owedActor` serves the FLEET's figures whatever isolate it
/// lands on. An isolate adds its samples at most once a [`BARS_FLUSH_EVERY_MS`] or every [`BARS_FLUSH_SAMPLES`]
/// (one read and one batch). The `queue` row also carries the actor's OWN oldest unapplied event: the oldest write
/// time of an event whose actor apply faulted in this isolate with no later apply of that identity landed.
/// D2-ACTOR-B-FOLD-7 (the captain's item 7): `cold`, one sample a cold actor, the first held event's write time to
/// its held events applied; its `visible` ring alone (never the warm `queue` and `direct` bars).
pub const BAR_CLASSES: [&str; 5] = ["direct", "queue", "mined", "cold", "flood"];
/// FIX-983 (bsv-low #983): THE STRESS CLASS. A pass that calls this many actors at once (the publisher's direct pass
/// over a burst of identities, a consumer batch of that many identities) is a FLOOD: its samples go to the `flood`
/// class row, apart from `direct` and `queue`, so the design's two bars (`classes.direct.applyMsP95` under 300 ms,
/// `rowsVisibleDirectMsP95` under 1 s) read a WARM HAND (a hand's two seats, a quiet batch) and the flood's figures
/// are served beside them (`classes.flood`, `rowsVisibleFloodMsP95`). The caller names its fan-out on the body
/// (`concurrent`); the object classes the sample ([`bars_class`]).
pub const FLOOD_CONCURRENCY: u32 = 8;
/// D2-ACTOR-D (bsv-low #952): the queue's backstop lag bar, named in the health block (`backstopLagBarMs`): an event
/// the writer's direct call did not land is visible within this long of its write through the queue.
pub const BACKSTOP_LAG_BAR_MS: u64 = 60_000;
pub const BARS_FLUSH_EVERY_MS: u64 = 60_000;
pub const BARS_FLUSH_SAMPLES: usize = 64;
pub fn bars_state_name(class: &str) -> String {
    format!("owed-actor:bars:{class}")
}
pub const BARS_READ_SQL: &str = "SELECT name,cursor,tickedAtMs FROM owed_belt_state WHERE name IN ('owed-actor:bars:direct','owed-actor:bars:queue','owed-actor:bars:mined','owed-actor:bars:cold','owed-actor:bars:flood')";

/// This isolate's samples not yet added to the durable rows.
#[derive(Debug, Default, Clone)]
pub struct PendingBars {
    pub apply: BTreeMap<&'static str, Vec<u64>>,
    /// D2-ACTOR-B-FOLD-14 (bsv-low #954 item 9): the apply's stages by class, each a ring of its own
    /// ([`APPLY_STAGES`]), beside `apply`.
    pub stages: BTreeMap<(&'static str, &'static str), Vec<u64>>,
    pub visible: BTreeMap<&'static str, Vec<u64>>,
    /// identity -> the oldest write time of an event whose apply faulted, no later apply landed.
    pub unapplied: BTreeMap<String, u64>,
    /// The queue class's unapplied set moved since the last flush (only then is the row's age rewritten).
    pub queue_touched: bool,
    pub noted: usize,
    /// The fourth delta lens's NEW4-L4: the held answers since the last flush (`owed_actor_apply_held_total`, added
    /// once a flush, never one D1 write a held event).
    pub held: u64,
    /// FIX-962 (bsv-low #962): each cold bootstrap page's own work (its budget take to its end), the `cold` row's
    /// `page` ring, served `classes.cold.pageMsP95`.
    pub cold_page: Vec<u64>,
    pub last_flush_ms: Option<u64>,
}
/// D2-ACTOR-B-FOLD-10 (the captain's item, beta 2026-10-29: an actor bootstrapped on FOLD-7's build kept a row the
/// FOLD-9 judge no longer produces, because the lever answered `already` whenever `bootstrapped` was set): a
/// bootstrapped actor whose state was judged by ANOTHER build (`meta.judgedLibRev`, stamped with `bootstrapped` at the
/// bootstrap's end; absent on an actor stamped before this fold) is re-judged: its bootstrap re-armed from the start
/// (its state kept and served until the pages rewrite it), on the lever and on an event it applies. The same build:
/// `already`.
pub fn rejudge_due(bootstrapped: bool, judged_rev: Option<&str>, running: &str) -> bool {
    bootstrapped && judged_rev != Some(running)
}

/// D2-ACTOR-B-FOLD-16 (the captain's item 5): the `class` of a direct pass that is no writer's (the drain's sweep
/// re-publishing marks that waited, the reconcile's own `backfill` marks among them): its applies sample NOTHING into
/// the fleet's bars, which are the writers' (the direct call's apply and visibility, the queue's backstop).
pub const SWEEP_CLASS: &str = "sweep";

/// PURE (FOLD-16, item 5): does an apply sample into the bars? Only a delivery (its event's write time, `atMs`) that is
/// a writer's or the queue's: the tip alarm's window and the reconcile's fresh re-apply carry no `atMs`; the sweep's
/// direct pass carries [`SWEEP_CLASS`].
pub fn samples_bars(at_ms: Option<u64>, class: Option<&str>) -> bool {
    at_ms.is_some() && class != Some(SWEEP_CLASS)
}

/// PURE (FOLD-16, item 5): the `/apply` body of the reconcile's fresh re-apply: no event (`seq` and `atMs` null), so
/// the object samples nothing (`samples_bars`) and records no first apply of an event.
pub fn reconcile_apply_body(
    identity: &str,
    games: &[String],
    kind: &str,
    tip: Option<u64>,
) -> Value {
    let e = EventCtx {
        kind: kind.into(),
        seq: 0,
        at_ms: 0,
        mined: false,
        txids: None,
        also: Vec::new(),
        by_identity: Default::default(),
    };
    let mut body = apply_body(identity, games, &e, tip);
    body["seq"] = Value::Null;
    body["atMs"] = Value::Null;
    body
}

/// NEW5-L5 (D2-ACTOR-B-FOLD-10): the bars class an object notes its own answer under (`direct`, else `queue`);
/// FIX-983 (bsv-low #983): `flood` when the caller's pass fanned out to [`FLOOD_CONCURRENCY`] actors or more
/// (`concurrent`, the body's), whatever its road.
pub fn bars_class(class: Option<&str>, concurrent: Option<u32>) -> &'static str {
    if concurrent.is_some_and(|n| n >= FLOOD_CONCURRENCY) {
        return "flood";
    }
    match class {
        Some("direct") => "direct",
        _ => "queue",
    }
}
impl PendingBars {
    pub fn note_answer(&mut self, class: &'static str, answer: &Value) {
        // D2-ACTOR-D: a covered dupe applied nothing: no apply sample, no visibility sample.
        if answer["applied"] == false {
            return;
        }
        // NEW5-L5: the object already noted this answer's samples itself; the caller keeps only its own isolate's
        // unapplied bookkeeping.
        if answer["noted"] == true {
            if let Some(id) = answer["identity"].as_str() {
                if self.unapplied.remove(id).is_some() {
                    self.queue_touched = true;
                }
            }
            return;
        }
        if let Some(ms) = answer["applyMs"].as_u64() {
            self.apply.entry(class).or_default().push(ms);
            self.noted += 1;
            for (field, ring) in APPLY_STAGES {
                if let Some(v) = answer[field].as_u64() {
                    self.stages.entry((class, ring)).or_default().push(v);
                }
            }
        }
        // D2-ACTOR-D: the rows' visibility once per event, at its first apply ([`samples_visible`]).
        // D2-ACTOR-B-FOLD-11 (NEW6-L1): an answer that names its samples (`visibleMs`, one an event of a coalesced
        // delivery) is sampled by them; the mined bar stays its own event's.
        if let Some(each) = answer["visibleMs"].as_array() {
            let v = self.visible.entry(class).or_default();
            v.extend(each.iter().filter_map(Value::as_u64));
        }
        if let (true, Some(at), Some(judged)) = (
            samples_visible(answer),
            answer["atMs"].as_u64(),
            answer["judgedAtMs"].as_u64(),
        ) {
            let ms = judged.saturating_sub(at);
            if answer["visibleMs"].is_null() {
                self.visible.entry(class).or_default().push(ms);
            }
            if answer["mined"] == true {
                self.visible.entry("mined").or_default().push(ms);
            }
        }
        if let Some(id) = answer["identity"].as_str() {
            if self.unapplied.remove(id).is_some() {
                self.queue_touched = true;
            }
        }
        if class == "queue" {
            self.queue_touched = true;
        }
    }
    pub fn note_cold(&mut self, ms: u64) {
        self.visible.entry("cold").or_default().push(ms);
        self.noted += 1;
    }
    /// FIX-962: one cold bootstrap page's work.
    pub fn note_cold_page(&mut self, ms: u64) {
        self.cold_page.push(ms);
        self.noted += 1;
    }
    /// NEW4-L4: one held answer, counted at the next flush.
    pub fn note_held(&mut self) {
        self.held += 1;
        self.noted += 1;
    }
    pub fn note_fault(&mut self, identity: &str, at_ms: Option<u64>) {
        if let Some(at) = at_ms {
            let e = self
                .unapplied
                .entry(identity.to_ascii_lowercase())
                .or_insert(at);
            *e = (*e).min(at);
            self.queue_touched = true;
        }
    }
    pub fn due(&self) -> bool {
        self.noted > 0 || self.queue_touched
    }
    fn flush_due(&self, now: u64) -> bool {
        self.due()
            && (self.noted >= BARS_FLUSH_SAMPLES
                || self
                    .last_flush_ms
                    .is_none_or(|l| now.saturating_sub(l) >= BARS_FLUSH_EVERY_MS))
    }
}
thread_local! {
    static PENDING_BARS: RefCell<PendingBars> = RefCell::default();
}
/// The pins' view of this isolate's pending samples (FOLD-16, item 5).
#[cfg(test)]
pub fn pending_bars_snapshot() -> PendingBars {
    PENDING_BARS.with(|p| p.borrow().clone())
}
/// One answer's samples under its class (this isolate's bars too).
pub fn note_answer_as(class: &'static str, answer: &Value) {
    BARS.with(|b| note_answer_into(&mut b.borrow_mut(), answer));
    PENDING_BARS.with(|p| p.borrow_mut().note_answer(class, answer));
}
/// D2-ACTOR-B-FOLD-7: one cold actor's time, into the fleet's `cold` bar at the next flush.
pub fn note_cold(ms: u64) {
    PENDING_BARS.with(|p| p.borrow_mut().note_cold(ms));
}
/// FIX-962: one cold bootstrap page's work, into the fleet's `cold` row's `page` ring at the next flush.
pub fn note_cold_page(ms: u64) {
    PENDING_BARS.with(|p| p.borrow_mut().note_cold_page(ms));
}
/// NEW4-L4: one held answer, into this isolate's pending bars (counted `owed_actor_apply_held_total` at the flush).
pub fn note_held() {
    PENDING_BARS.with(|p| p.borrow_mut().note_held());
}
/// A JSON number as milliseconds, an integer or a whole REAL (D1's bind of a JS number).
pub fn json_ms(v: &Value) -> Option<u64> {
    v.as_u64()
        .or_else(|| v.as_f64().filter(|f| *f >= 0.0).map(|f| f as u64))
}
/// An alarm's OFFSET from now for workers-rs 0.8.7 (`ScheduledTime: From<i64>` is an offset): `at_ms` an epoch stamp
/// (with `now_ms`), or an offset already (`now_ms` 0); 0 for a stamp past.
pub fn alarm_offset_ms(at_ms: u64, now_ms: u64) -> i64 {
    at_ms.saturating_sub(now_ms).min(i64::MAX as u64) as i64
}
/// The captain's item 12: is a bootstrap's stored alarm DEAD (none, or later than one page delay and a minute from
/// now: lost, or set decades out by a build that handed `set_alarm` an epoch stamp)? `held` the stored epoch time.
///
/// The fourth delta lens's NEW4-L2 (D2-ACTOR-B-FOLD-8): the bound sits past [`boot_pace`]'s longest wait (the next
/// minute's edge plus a spread under a minute: under two minutes from now) with a page delay and
/// [`BOOT_ALARM_SLACK_MS`] of slack, so a paced wait is never called dead (it was: the bound was one page delay and a
/// minute, and a held event or a lever call during a wait re-armed the page at once and spent a token).
pub fn alarm_is_dead(held: Option<i64>, now_ms: u64) -> bool {
    held.is_none_or(|h| h > (now_ms + BOOT_ALARM_DEAD_AFTER_MS) as i64)
}
/// FIX-962 (bsv-low #962): is the bootstrap's alarm DEAD, read beside the page in flight? A Durable Object's
/// `getAlarm()` answers null from the moment its alarm handler starts until the handler sets the next alarm
/// (Cloudflare's Alarms API: "If getAlarm is called while an alarm is already running, it returns null unless setAlarm
/// has also been called since the alarm handler started running"), so a held event or a lever call landing while a
/// page runs (the page's D1 awaits open the input gate) read `None` and called the live page dead: beta's three
/// `armed under a dead alarm (None)` lines in one cold bootstrap. A page stamped (`meta.pageStartedAtMs`) less than
/// [`BOOT_PAGE_WALL_MS`] ago is LIVE whatever the alarm reads; a stamp older than the wall with no alarm is the dead
/// class ([`alarm_is_dead`], FOLD-7's item 12).
pub fn boot_alarm_dead(held: Option<i64>, page_started_ms: Option<u64>, now_ms: u64) -> bool {
    !page_in_flight(page_started_ms, now_ms) && alarm_is_dead(held, now_ms)
}
/// FIX-962: a page stamped at `started_ms` is still running at `now_ms` (its stamp younger than [`BOOT_PAGE_WALL_MS`]).
pub fn page_in_flight(started_ms: Option<u64>, now_ms: u64) -> bool {
    started_ms.is_some_and(|s| now_ms.saturating_sub(s) < BOOT_PAGE_WALL_MS)
}
/// FIX-962: how long a stamped page is read as running: the dead bound itself ([`BOOT_ALARM_DEAD_AFTER_MS`]). A page
/// is three D1 round trips (about 2 s); one past two minutes is a handler the runtime has lost (it retries a failed
/// alarm with backoff from 2 s), and only then may a held event re-arm it.
pub const BOOT_PAGE_WALL_MS: u64 = BOOT_ALARM_DEAD_AFTER_MS;
/// FIX-962: the per-page line the alarm writes (`games`, the page's own work `ms` from its budget take to its end,
/// `waitMs` since the previous page ended, the cursor it leaves), so a rehearsal reads the cadence from the logs.
pub fn page_line(
    identity: &str,
    games: usize,
    ms: u64,
    wait_ms: Option<u64>,
    cursor: &str,
) -> String {
    format!(
        "[owed-actor] page {identity}: games={games} ms={ms} waitMs={} cursor={cursor}",
        wait_ms.map_or_else(|| "-".to_string(), |w| w.to_string())
    )
}
/// The slack past a paced wait before an alarm is dead.
pub const BOOT_ALARM_SLACK_MS: u64 = 5_000;
/// [`boot_pace`]'s longest wait (two minutes), a page delay and the slack.
pub const BOOT_ALARM_DEAD_AFTER_MS: u64 =
    2 * 60_000 + BOOTSTRAP_PAGE_DELAY_MS + BOOT_ALARM_SLACK_MS;
/// An actor apply that faulted: its event is unapplied for the actor until a later apply of the identity lands.
pub fn note_actor_fault(identity: &str, at_ms: Option<u64>) {
    PENDING_BARS.with(|p| p.borrow_mut().note_fault(identity, at_ms));
}

/// THE FLEET BARS ARE THE BUILD'S (the captain's item 9 of lane D2-ACTOR-B-FOLD-8): each class row names the build
/// that wrote its samples (`libRev`, the app layer's `OVERLAY_LIB_REV`); the first flush of another build REPLACES the
/// row (never merges into it), and the served figures read only the rows of the reading build ([`fleet_bars`]), so a
/// fixed build's p95s are never the old build's history (beta, an hour after PR 206's T0: `rowsVisibleMsP95` 311,719
/// ms, every sample the old build's cold-closure retries).
pub fn lib_rev() -> &'static str {
    option_env!("OVERLAY_LIB_REV").unwrap_or("unknown")
}

/// D2-ACTOR-B-FOLD-11 (the sixth delta lens's NEW6-L2): THE JUDGE'S VERSION, the stamp a finished bootstrap writes
/// (`meta.judgedLibRev`) and [`rejudge_due`] compares, so a deploy that changes no judgement re-judges no actor (it
/// was the build's git HEAD, [`lib_rev`]: every deploy re-judged the fleet through the cold bootstrap's budget). MOVE IT
/// BY HAND, in the commit that changes what an actor stores for the same record: the judge (`owed::derive_owed_rows`,
/// `owed_judge`), its hydration, the mirror's closure or read ([`apply_with`], [`FAMILIES`]), the actor's own tables.
/// A change of cost, bars, routing or logs moves nothing. Moved by FOLD-11 itself (NEW6-M1 changes the warm read).
/// The fleet bars stay keyed by [`lib_rev`] (a build's own samples). Moved by FIX-997 (bsv-low #997): the mirror's
/// `txfacts` carry the classifier's input and the bump's height, so a spend with no stored verdict is judged from them
/// (it was `unresolved` in the actor); every actor re-judges once under the bootstrap pace, and D1's fleet re-judge
/// (FOLD-13) runs once with the D1 judge on.
pub const JUDGE_VERSION: &str = "fix997-2026-10-30";

/// One class row after this isolate's samples: the stored rings with the new samples appended, the last
/// [`METER_SAMPLES`] kept; `unapplied` (the queue row only) the oldest unapplied write time, `Some(None)` cleared. A
/// stored row of another build (`libRev` not `rev`, or none) is replaced, not merged.
/// D2-ACTOR-B-FOLD-14 (bsv-low #954 item 9): the apply's stages an answer carries and the ring each is kept in on its
/// class's bars row: the record's reads, the judge, the mirror's writes, the turn's queueing (they sum to `applyMs`
/// within the object's own work before and after the apply: the event's facts, the coalesced events' checks).
/// FIX-962 (bsv-low #962): the `cold` class row's ring of bootstrap page work ([`PendingBars::cold_page`]).
pub const COLD_PAGE_RING: &str = "page";
pub const APPLY_STAGES: [(&str, &str); 5] = [
    ("readMs", "read"),
    ("judgeMs", "judge"),
    ("writeMs", "write"),
    ("waitMs", "wait"),
    // FIX-983 (bsv-low #983): the outside's awaits (CHAINTRACKS, once per apply), apart from the judge's derivation.
    ("outsideMs", "outside"),
];

pub fn merge_bars_row(
    stored: Option<&str>,
    apply: &[u64],
    visible: &[u64],
    unapplied: Option<Option<u64>>,
    rev: &str,
) -> String {
    merge_bars_row_staged(stored, apply, visible, &[], unapplied, rev)
}

/// [`merge_bars_row`] with the apply's stages ([`APPLY_STAGES`]' rings, by name).
pub fn merge_bars_row_staged(
    stored: Option<&str>,
    apply: &[u64],
    visible: &[u64],
    stages: &[(&str, &[u64])],
    unapplied: Option<Option<u64>>,
    rev: &str,
) -> String {
    let parsed: Option<Value> = stored
        .and_then(|s| serde_json::from_str(s).ok())
        .filter(Value::is_object);
    let mut row: Value = parsed
        .clone()
        .filter(|r: &Value| r["libRev"] == rev)
        .unwrap_or_else(|| json!({}));
    // D2-ACTOR-B-FOLD-10 (the fifth delta lens's NEW5-L4): the by-build replace keeps the SAMPLES apart; the oldest
    // unapplied write time is a fact about the fleet's actors, not a sample of a build, so it is carried across.
    if row.get("oldestUnappliedAtMs").is_none() {
        if let Some(u) = parsed
            .as_ref()
            .and_then(|p| p.get("oldestUnappliedAtMs"))
            .filter(|u| !u.is_null())
        {
            row["oldestUnappliedAtMs"] = u.clone();
        }
    }
    row["libRev"] = json!(rev);
    let rings: Vec<(&str, &[u64])> = [("apply", apply), ("visible", visible)]
        .into_iter()
        .chain(stages.iter().copied())
        .collect();
    for (k, new) in rings {
        // A stage ring is written once it holds a sample (a row an older build wrote keeps its shape).
        if new.is_empty() && row.get(k).is_none() && k != "apply" && k != "visible" {
            continue;
        }
        let mut ring: Vec<u64> = row[k]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_u64)
            .collect();
        ring.extend_from_slice(new);
        let cut = ring.len().saturating_sub(METER_SAMPLES);
        row[k] = json!(ring[cut..]);
    }
    if let Some(u) = unapplied {
        row["oldestUnappliedAtMs"] = json!(u);
    }
    row.to_string()
}

/// Add this isolate's samples to the durable rows when due (`force`: whatever the count and the time). A fault
/// keeps them for the next flush.
pub async fn flush_bars(db: &impl SqlPort, now: u64, force: bool) {
    let pending = PENDING_BARS.with(|p| {
        let p = p.borrow();
        (if force { p.due() } else { p.flush_due(now) }).then(|| p.clone())
    });
    let Some(pending) = pending else { return };
    match flush_bars_from(db, now, &pending).await {
        Ok(()) => PENDING_BARS.with(|p| {
            let mut p = p.borrow_mut();
            let unapplied = std::mem::take(&mut p.unapplied);
            *p = PendingBars {
                unapplied,
                last_flush_ms: Some(now),
                ..Default::default()
            };
        }),
        Err(e) => db.log(&format!("[owed-actor] bars not flushed: {e}")),
    }
}
/// The flush of one isolate's pending samples (the pins run it with an explicit state).
pub async fn flush_bars_from(
    db: &impl SqlPort,
    now: u64,
    pending: &PendingBars,
) -> Result<(), String> {
    let stored = db
        .run_batch(vec![Stmt::new(BARS_READ_SQL, vec![])])
        .await?
        .into_iter()
        .next()
        .map(|o| o.rows)
        .unwrap_or_default();
    let held = |class: &str| {
        let name = bars_state_name(class);
        stored
            .iter()
            .find(|r| r["name"] == name.as_str())
            .and_then(|r| r["cursor"].as_str().map(str::to_string))
    };
    let mut stmts = Vec::new();
    for class in BAR_CLASSES {
        let apply = pending.apply.get(class).cloned().unwrap_or_default();
        let visible = pending.visible.get(class).cloned().unwrap_or_default();
        let mut staged: Vec<(&str, Vec<u64>)> = APPLY_STAGES
            .iter()
            .map(|(_, ring)| {
                (
                    *ring,
                    pending
                        .stages
                        .get(&(class, *ring))
                        .cloned()
                        .unwrap_or_default(),
                )
            })
            .collect();
        // FIX-962: the cold row's page ring.
        if class == "cold" {
            staged.push((COLD_PAGE_RING, pending.cold_page.clone()));
        }
        let staged: Vec<(&str, &[u64])> = staged.iter().map(|(r, v)| (*r, &v[..])).collect();
        let unapplied = (class == "queue" && pending.queue_touched)
            .then(|| pending.unapplied.values().min().copied());
        let pages = class == "cold" && !pending.cold_page.is_empty();
        if apply.is_empty() && visible.is_empty() && unapplied.is_none() && !pages {
            continue;
        }
        stmts.push(Stmt::new(
            crate::owed_belt::STATE_SQL,
            vec![
                Bind::Text(bars_state_name(class)),
                Bind::Text(merge_bars_row_staged(
                    held(class).as_deref(),
                    &apply,
                    &visible,
                    &staged,
                    unapplied,
                    lib_rev(),
                )),
                Bind::Int(now as i64),
            ],
        ));
    }
    if pending.held > 0 {
        // NEW4-L4: the held answers since the last flush, one counter write a flush.
        db.counters(vec![(COUNTERS[21].to_string(), pending.held)])
            .await;
    }
    if !stmts.is_empty() {
        // D2-ACTOR-B-FOLD-6: the health snapshot after the class rows, in their batch (at most once a minute).
        stmts.push(health_snapshot_stmt(now, false));
        db.run_batch(stmts).await?;
    }
    Ok(())
}

/// The fleet's bars from the durable rows ([`BARS_READ_SQL`]'s): the apply's p50, p95 and maximum over the direct and
/// queue classes, the rows' FIRST visibility p95 over both classes (`rowsVisibleMsP95`, D2-ACTOR-D: each event sampled
/// once, at its first apply), the direct class's (`rowsVisibleDirectMsP95`) and the queue's own, the backstop's lag
/// (`backstopLagMsP95`, its bar `backstopLagBarMs`), the collect-after-mined p95, the actor's own oldest
/// unapplied event's age, and each class's sample count and flush time.
///
/// Item 9: only the rows `rev` wrote are read (a row of another build, or one written before the stamp, is not this
/// build's: its class serves no samples until this build flushes); `barsLibRev` names the build served.
pub fn fleet_bars(rows: &[Value], now: u64, rev: &str) -> Value {
    let ring = |class: &str, k: &str| -> (Meter, Option<u64>, Value) {
        let name = bars_state_name(class);
        let found = rows.iter().find(|r| r["name"] == name.as_str());
        let row: Value = found
            .and_then(|r| r["cursor"].as_str())
            .and_then(|s| serde_json::from_str::<Value>(s).ok())
            .filter(|row| row["libRev"] == rev)
            .unwrap_or(Value::Null);
        let r = found.filter(|_| !row.is_null());
        let mut m = Meter::default();
        for v in row[k]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_u64)
        {
            m.note(v);
        }
        // The fourth delta lens's NEW4-L3: D1 stores the bound `tickedAtMs` as a REAL; read either form.
        (m, r.and_then(|r| json_ms(&r["tickedAtMs"])), row)
    };
    let mut apply = Meter::default();
    for class in ["direct", "queue"] {
        for v in ring(class, "apply").0.samples {
            apply.note(v);
        }
    }
    // FOLD-14 item 2: the stages' p95 over the two classes, as the apply's.
    let stage_p95: serde_json::Map<String, Value> = APPLY_STAGES
        .iter()
        .map(|(field, r)| {
            let mut m = Meter::default();
            for class in ["direct", "queue"] {
                for v in ring(class, r).0.samples {
                    m.note(v);
                }
            }
            (format!("{field}P95"), json!(m.quantile(0.95)))
        })
        .collect();
    let (queue_visible, _, queue_row) = ring("queue", "visible");
    let (direct_visible, _, _) = ring("direct", "visible");
    let mut first_visible = Meter::default();
    for v in direct_visible.samples.iter().chain(&queue_visible.samples) {
        first_visible.note(*v);
    }
    let (mined, _, _) = ring("mined", "visible");
    let (cold, _, _) = ring("cold", "visible");
    // FIX-983: the flood's first visibility, apart from the warm hand's.
    let (flood_visible, _, _) = ring("flood", "visible");
    let classes: serde_json::Map<String, Value> = BAR_CLASSES
        .iter()
        .map(|c| {
            let (v, at, _) = ring(c, "visible");
            let (a, _, _) = ring(c, "apply");
            let mut out = json!({"applySamples": a.samples.len(), "applyMsP95": a.quantile(0.95), "visibleSamples": v.samples.len(), "visibleMsP95": v.quantile(0.95), "flushedAtMs": at});
            // FOLD-14 item 2: each stage's p95 beside the apply's.
            for (field, r) in APPLY_STAGES {
                out[format!("{field}P95")] = json!(ring(c, r).0.quantile(0.95));
            }
            // FIX-962: the cold bootstrap's page work beside its cold time.
            if *c == "cold" {
                let (p, _, _) = ring(c, COLD_PAGE_RING);
                out["pageSamples"] = json!(p.samples.len());
                out["pageMsP95"] = json!(p.quantile(0.95));
                out["pageMsMax"] = json!(p.max());
            }
            ((*c).to_string(), out)
        })
        .collect();
    json!({
        "applyMsP50": apply.quantile(0.50),
        "applyMsP95": apply.quantile(0.95),
        "applyMsMax": apply.max(),
        // D2-ACTOR-D (bsv-low #952; the design's bar 2): each event is sampled once, at its FIRST apply, in the class
        // of the path that applied it, so the union of the two classes is the rows' FIRST visibility.
        "rowsVisibleMsP95": first_visible.quantile(0.95),
        "rowsVisibleMsMax": first_visible.max(),
        "rowsVisibleDirectMsP95": direct_visible.quantile(0.95),
        // FIX-983 (bsv-low #983): the stress class's figures, served apart (never in the two bars above).
        "rowsVisibleFloodMsP95": flood_visible.quantile(0.95),
        "floodConcurrency": FLOOD_CONCURRENCY,
        // The queue's own first applies (the events the direct call missed): the durable backstop's lag, its bar.
        "backstopLagMsP95": queue_visible.quantile(0.95),
        "backstopLagMsMax": queue_visible.max(),
        "backstopLagBarMs": BACKSTOP_LAG_BAR_MS,
        "collectAfterMinedMsP95": mined.quantile(0.95),
        // D2-ACTOR-B-FOLD-7 (item 7): the cold path's own bar (the heaviest seat within 60 s of its first event).
        "coldMsP95": cold.quantile(0.95),
        "coldMsMax": cold.max(),
        "coldSamples": cold.samples.len(),
        "actorOldestUnappliedEventAgeMs": queue_row["oldestUnappliedAtMs"].as_u64().map(|a| now.saturating_sub(a)),
        "classes": classes,
        "stages": stage_p95,
        "barsLibRev": rev,
    })
}

/// What `/health.owedActor` serves from the FLEET (MEDIUM-1, MEDIUM-7, MEDIUM-8): the class rows, the actor's durable
/// totals, the identities holding owed rows (a count over `owed_state`, one row an identity) and the bootstrap's
/// coverage. Since the land tier's refusal (lane D2-ACTOR-B-FOLD-6: `health-dirty-snapshot` measured 49 reads, 6
/// statements, the largest 44 rows, against 7, 5 and 5) the READ never runs it: the WRITERS fold it into one snapshot
/// row ([`HEALTH_SNAPSHOT_SQL`]) and `/health` reads that row beside the queue's consumer row in one statement
/// (`owed_events::HEALTH_CONSUMER_SQL`).
macro_rules! health_select {
    () => {
        "SELECT (SELECT json_group_array(json_object('name',name,'cursor',cursor,'tickedAtMs',CAST(tickedAtMs AS INTEGER))) FROM owed_belt_state WHERE name IN ('owed-actor:bars:direct','owed-actor:bars:queue','owed-actor:bars:mined','owed-actor:bars:cold','owed-actor:bars:flood')) AS bars,(SELECT json_group_object(name,value) FROM ops_counters WHERE name IN ('owed_actor_compared_total','owed_actor_diffs_total','owed_actor_faults_total','owed_actor_reconcile_diffs_total','owed_actor_bootstraps_total','owed_actor_unknown_reads_total','owed_actor_epoch_resets_total','owed_actor_deferred_total','owed_v3_identities_total','owed_v3_rows_total','owed_actor_deferred_done_total','owed_actor_compare_held_total','owed_actor_tip_faults_total','owed_actor_tip_parked_total','owed_actor_schema_partial_total','owed_actor_apply_held_total','owed_actor_closure_rearmed_total','owed_actor_alarm_rearmed_total','owed_actor_bootstrap_faults_total','owed_actor_direct_calls_total','owed_actor_direct_faults_total','owed_actor_direct_walls_total','owed_actor_rejudged_total','owed_actor_compare_tip_lag_total','owed_actor_reconcile_stale_total','owed_d1_rejudge_identities_total','owed_d1_rejudge_faults_total')) AS counters,(SELECT COUNT(*) FROM owed_state WHERE rows>0) AS activeIdentities,(SELECT cursor FROM owed_belt_state WHERE name='owed-actor:coverage') AS coverage,(SELECT json_object('count',COUNT(*),'oldestAtMs',MIN(CAST(tickedAtMs AS INTEGER))) FROM owed_belt_state WHERE name>='owed-actor:holding:' AND name<'owed-actor:holding;') AS holding,(SELECT cursor FROM owed_belt_state WHERE name='owed-judge:version') AS d1Rejudge"
    };
}
pub const HEALTH_SQL: &str = health_select!();

/// THE HEALTH SNAPSHOT (lane D2-ACTOR-B-FOLD-6): one row of the belt's ledger, [`HEALTH_SNAPSHOT_NAME`], its cursor
/// [`HEALTH_SQL`]'s four columns and `asOfMs`. Written by the writers, never by a read: the consumer's batch (beside
/// its own row), the actor's meter flush ([`flush_bars_from`]) and the bootstrap lever's coverage fold; each at most
/// once a [`HEALTH_SNAPSHOT_EVERY_MS`] FLEET-WIDE, the stamp compared inside the statement (`?2` the window: a stamp
/// younger than it writes nothing and runs none of the composition's subqueries; `?2` 0 writes whatever the stamp:
/// the coverage pass's last call, so a finished pass is never served unfinished). `?1` now.
pub const HEALTH_SNAPSHOT_NAME: &str = "owed-actor:health";
pub const HEALTH_SNAPSHOT_EVERY_MS: u64 = 60_000;
pub const HEALTH_SNAPSHOT_SQL: &str = concat!(
    "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) SELECT 'owed-actor:health',json_object('asOfMs',CAST(?1 AS INTEGER),'bars',h.bars,'counters',h.counters,'activeIdentities',h.activeIdentities,'coverage',h.coverage,'holding',h.holding,'d1Rejudge',h.d1Rejudge),?1 FROM (",
    health_select!(),
    ") h WHERE NOT (?2>0 AND EXISTS (SELECT 1 FROM owed_belt_state WHERE name='owed-actor:health' AND tickedAtMs>?1-?2)) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs"
);
/// The snapshot's write: `force` the coverage pass's last call (no stamp compare).
pub fn health_snapshot_stmt(now: u64, force: bool) -> Stmt {
    Stmt::new(
        HEALTH_SNAPSHOT_SQL,
        vec![
            Bind::Int(now as i64),
            Bind::Int(if force {
                0
            } else {
                HEALTH_SNAPSHOT_EVERY_MS as i64
            }),
        ],
    )
}
/// The snapshot as `/health` read it (its cursor parsed); `None` before the first write.
pub fn health_snapshot_of(cursor: Option<&str>) -> Option<Value> {
    cursor
        .and_then(|c| serde_json::from_str::<Value>(c).ok())
        .filter(Value::is_object)
}

/// The served object: this isolate's bars under `isolate`, the fleet's figures at the top (the harness's keys),
/// the durable compare totals (MEDIUM-8: Phase B's gate), the bootstraps beside the identities holding rows
/// (MEDIUM-1: Phase B's other gate), and the oldest unapplied event's age: the actor's OWN in Phases A and B, the
/// fleet's oldest un-acked mark in Phase C (where the marks are acknowledged after the actor applied).
pub fn health_body(read: Option<&Value>, now: u64, d1_oldest_unacked: &Value) -> Value {
    let mut body = served_bars(now);
    body["isolate"] = body.clone();
    // A field of the snapshot: the composition's text (a `json_group_*` column) or the JSON itself.
    let field = |k: &str| -> Value {
        match read.map(|r| &r[k]) {
            Some(Value::String(s)) => serde_json::from_str(s).unwrap_or(Value::Null),
            Some(v) => v.clone(),
            None => Value::Null,
        }
    };
    let rows: Vec<Value> = field("bars").as_array().cloned().unwrap_or_default();
    let fleet = fleet_bars(&rows, now, lib_rev());
    for (k, v) in fleet.as_object().into_iter().flatten() {
        body[k.as_str()] = v.clone();
    }
    let counters = field("counters");
    // No snapshot yet: every total null (an unmeasured bar is red by design, never a 0).
    let n = |k: &str| -> Value {
        match read {
            Some(_) => json!(counters.get(k).and_then(Value::as_u64).unwrap_or(0)),
            None => Value::Null,
        }
    };
    body["comparedTotal"] = n(COUNTERS[0]);
    body["diffsTotal"] = n(COUNTERS[1]);
    body["faultsTotal"] = n(COUNTERS[2]);
    body["reconcileDiffs"] = n(COUNTERS[7]);
    // D2-ACTOR-B-FOLD-13 (bsv-low #958): the reconciles that found a stale D1 (healed by the fresh judgement, never a
    // diff), and D1's fleet re-judge after a judge-version move (the window reads it before the route's diff).
    body["reconcileStaleTotal"] = n(COUNTERS[29]);
    body["d1Rejudge"] = match read {
        Some(_) => {
            let mut r = rejudge_served(Some(&field("d1Rejudge")).filter(|v| v.is_object()));
            if r.is_object() {
                r["faults"] = n(COUNTERS[31]);
            }
            r
        }
        None => Value::Null,
    };
    body["bootstraps"] = n(COUNTERS[10]);
    // D2-ACTOR-B-FOLD-10: the actors re-judged under the running build, beside the bootstraps.
    body["rejudged"] = n(COUNTERS[26]);
    body["unknownReads"] = n(COUNTERS[11]);
    body["epochResets"] = n(COUNTERS[12]);
    body["deferred"] = n(COUNTERS[13]);
    // NEW-L6: the deferred applies started (above) and ended; the difference is what `wait_until`'s bound cut.
    body["deferredDone"] = n(COUNTERS[16]);
    // NEW2-L1: the Phase B DIFFs on a game holding a mark above the bound (not a STOP); NEW2-L5: the tip alarms whose
    // window held a faulting game.
    body["heldDiffsTotal"] = n(COUNTERS[17]);
    // NEW7-L2: the compares whose rows differed only in the judged tip (a lag, not a STOP).
    body["tipLagDiffsTotal"] = n(COUNTERS[28]);
    body["tipFaults"] = n(COUNTERS[18]);
    // The third delta lens's LOW-1: the games the tip alarm parked; LOW-2: the schema plans that did not apply whole.
    body["tipParked"] = n(COUNTERS[19]);
    body["schemaPartial"] = n(COUNTERS[20]);
    // D2-ACTOR-B-FOLD-7: the events held (a cold or re-armed actor) and the re-arms.
    body["held"] = n(COUNTERS[21]);
    body["closureRearmed"] = n(COUNTERS[22]);
    // Item 12: the actors armed under a dead alarm, re-armed by the lever or a held event.
    body["alarmRearmed"] = n(COUNTERS[23]);
    // The fourth delta lens's NEW4-M2: the bootstrap-page and drain faults, and the actors holding events now (their
    // count and the oldest hold's age; from the snapshot the writers compose, no statement on this read).
    body["bootstrapFaults"] = n(COUNTERS[24]);
    let holding = field("holding");
    body["holding"] = match read {
        Some(_) => json!({
            "count": holding["count"].as_u64().unwrap_or(0),
            "oldestMs": json_ms(&holding["oldestAtMs"]).map(|a| now.saturating_sub(a)),
        }),
        None => Value::Null,
    };
    // D2-ACTOR-D (bsv-low #952): the writers' direct calls (every owed frame's publisher), and those that met their
    // wall or faulted (the queue's message is the backstop of each).
    body["direct"] =
        json!({"calls": n(COUNTERS[3]), "walls": n(COUNTERS[25]), "faults": n(COUNTERS[4])});
    // NEW-L3: Phase B's gate is COVERAGE, the bootstrap lever's last full pass (`complete`, `contiguous`, `uncovered`
    // 0), never `bootstraps` against `activeIdentities` (two counts).
    body["coverage"] = field("coverage");
    body["v3"] = json!({"identities": n(COUNTERS[14]), "rows": n(COUNTERS[15])});
    body["activeIdentities"] = json!(read.and_then(|r| r["activeIdentities"].as_u64()));
    // The snapshot's time (lane D2-ACTOR-B-FOLD-6): one older than its cadence is served as it is.
    // D2-ACTOR-B-FOLD-7 (item 5): D1 binds every number as a JS number (a REAL), so a row written before the fold
    // holds `asOfMs` as `1793190803706.0`; read either form.
    body["asOfMs"] = json!(read.and_then(|r| json_ms(&r["asOfMs"])));
    // FIX-982 (bsv-low #982): in EVERY phase the oldest event an actor holds unapplied ([`unapplied_event_age`]): the
    // oldest hold (`holding.oldestAtMs`: an actor not bootstrapped, its events held) or the oldest write time of an
    // event whose apply faulted with no later apply of its identity landed (the queue row's `oldestUnappliedAtMs`,
    // noted by the consumer and the direct pass in every phase). Never a D1 mark: Phase C read the fleet's oldest
    // un-acked `owed_dirty` mark, and Phase C's marks are the reconcile's, the age repair's and the sweep's, waiting
    // for the sweep's stalled bound (600 s with a quiet consumer; beta read 590 s with `holding` 0): no event an actor
    // holds. The mark's age is served beside, named for what it is.
    let held_age = json_ms(&holding["oldestAtMs"]).map(|a| now.saturating_sub(a));
    let faulted_age = fleet["actorOldestUnappliedEventAgeMs"].as_u64();
    let (age, source) = unapplied_event_age(held_age, faulted_age);
    body["oldestUnappliedEventAgeMs"] = json!(age);
    body["oldestUnappliedEventSource"] = json!(source);
    body["oldestUnackedMarkAgeMs"] = d1_oldest_unacked.clone();
    body["healthRead"] = json!(read.is_some());
    body
}

/// PURE (FIX-982, bsv-low #982): the oldest unapplied event's age and its source: the older of the oldest HELD event
/// (an actor not bootstrapped) and the oldest FAULTED apply's event (no later apply of its identity landed); `None`
/// with neither (every event applied).
pub fn unapplied_event_age(
    held_age_ms: Option<u64>,
    faulted_age_ms: Option<u64>,
) -> (Option<u64>, Option<&'static str>) {
    match (held_age_ms, faulted_age_ms) {
        (Some(h), Some(f)) if f > h => (Some(f), Some("faulted")),
        (Some(h), _) => (Some(h), Some("held")),
        (None, Some(f)) => (Some(f), Some("faulted")),
        (None, None) => (None, None),
    }
}

thread_local! {
    /// FIX-983: one drain of the asks lane at a time an isolate ([`drain_lane_once`]).
    static LANE_DRAINING: Cell<bool> = const { Cell::new(false) };
}
/// FIX-983: drain the lane unless a drain is running in this isolate (that one takes what was queued meanwhile: it
/// drains until the lane is empty before it lets go). Answers what THIS call drained.
pub async fn drain_lane_once<R: SqlPort>(record: &R) -> AsksDrained {
    if LANE_DRAINING.with(|d| d.replace(true)) {
        return AsksDrained::default();
    }
    let mut out = AsksDrained::default();
    loop {
        let d = drain_ask_lane(record).await;
        out.batches += d.batches;
        out.asks += d.asks;
        out.faults += d.faults;
        if ask_lane_len() == 0 {
            break;
        }
    }
    LANE_DRAINING.with(|d| d.set(false));
    out
}

/// THE BOOTSTRAP'S COVERAGE (the delta lens's NEW-L3): one row of the belt's ledger (`owed-actor:coverage`), folded
/// by every call of the bootstrap lever ([`coverage_fold`]): a call from the start (`after` empty) opens a pass, each
/// call adds its identities and the UNCOVERED ones (armed now, still bootstrapping, a fault: any answer but
/// `bootstrapped`), a call whose `after` is not the pass's `next` breaks it (`contiguous: false`), and the call that
/// answers `next: null` closes it (`complete`). Phase B's gate: a pass `complete`, `contiguous` and `uncovered` 0,
/// started after the last deploy that could move a digest or an era (its `startedAtMs`).
pub const COVERAGE_NAME: &str = "owed-actor:coverage";
pub const COVERAGE_READ_SQL: &str =
    "SELECT cursor FROM owed_belt_state WHERE name='owed-actor:coverage'";
pub fn coverage_fold(
    prev: Option<&Value>,
    after: &str,
    identities: usize,
    uncovered: usize,
    next: Option<&str>,
    now: u64,
) -> Value {
    let mut c = match prev {
        Some(p) if !after.is_empty() => p.clone(),
        _ => {
            json!({"startedAtMs": now, "identities": 0, "uncovered": 0, "contiguous": true, "complete": false, "next": ""})
        }
    };
    if !after.is_empty() && (c["complete"] == true || c["next"].as_str() != Some(after)) {
        c["contiguous"] = json!(false);
    }
    c["identities"] = json!(c["identities"].as_u64().unwrap_or(0) + identities as u64);
    c["uncovered"] = json!(c["uncovered"].as_u64().unwrap_or(0) + uncovered as u64);
    c["next"] = json!(next);
    c["complete"] = json!(next.is_none());
    if next.is_none() {
        c["endedAtMs"] = json!(now);
    }
    c
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
/// D2-ACTOR-B-FOLD-13 (bsv-low #958): THE RECONCILE JUDGES D1 FRESH BEFORE IT COMPARES (Phases A and B). A stored D1
/// row is never the comparison's side: the games of the outpoints D1's STORED rows and the actor's differ on are
/// judged again by the D1 judge first (`owed_judge::judge_subjects_acked_to`, 16 a call, acknowledging NOTHING: the
/// marks stand for the consumer, which acknowledges under its own bound), at most [`RECONCILE_JUDGE_GAMES_MAX`] games a
/// reconcile, and the FRESH rows are compared. A game whose stored rows equal the actor's is not judged (both sides
/// agree; the actor is the record's fresh judgement). Past the bound (an identity whose stored rows differ on more
/// than 16 games since FOLD-15, the whole-identity staleness of a judge change before D1's fleet re-judge passed it) the first 16
/// games in game order are judged and compared; the rest are MARKED (`backfill`, [`RECONCILE_MARK_REST_SQL`]) for the
/// drain or the consumer and left out of the compare, counted on the report (`unjudged`). A stored difference the
/// fresh judgement heals is a STALE D1 (`stale-d1`, counted `owed_actor_reconcile_stale_total`, logged, never
/// re-applied); a difference that stands after it is a judge disagreement (`owed_actor_reconcile_diffs_total`), and
/// only then are the games re-applied into the actor (its closure re-reads the record: never a stored row).
/// D2-ACTOR-B-FOLD-15 (NEW8-M2): 16, ONE judge call a step (it was 64, four calls: about 1,013 statements in the alarm's
/// one invocation beside the belt tick and the re-judge); and the compare is fresh with fresh ([`reconcile_fresh`]).
pub const RECONCILE_JUDGE_GAMES_MAX: usize = 16;
/// The games past the reconcile's judge bound, marked for the drain (`?1` the identity, `?2` now, `?3` the games).
pub const RECONCILE_MARK_REST_SQL: &str = "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) SELECT ?1,value,'backfill','owed-v2',?2 FROM json_each(?3) WHERE value<>'' ON CONFLICT(identity,gameId,source,sourceKey) DO UPDATE SET generation=excluded.generation,markedAtMs=excluded.markedAtMs";

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
    /// D2-ACTOR-B-FOLD-13: the games the D1 judge judged fresh before the compare (Phases A and B).
    pub judged: usize,
    /// The games past [`RECONCILE_JUDGE_GAMES_MAX`]: marked, left out of the compare.
    pub unjudged: usize,
    /// The outpoints D1's STORED rows differed on that the fresh judgement healed (`stale-d1`; never a diff).
    pub stale_outpoints: usize,
    /// Whether D1's fleet re-judge ([`d1_rejudge_step`]) had passed the identity at the compare.
    pub rejudge_passed: bool,
}

/// D1's stored rows of one identity ([`RECONCILE_D1_ROWS_SQL`]).
async fn d1_stored_rows(db: &impl SqlPort, identity: &str) -> Result<Vec<Value>, String> {
    Ok(db
        .run_batch(vec![Stmt::new(
            RECONCILE_D1_ROWS_SQL,
            vec![Bind::Text(identity.to_string())],
        )])
        .await?
        .into_iter()
        .next()
        .map(|o| o.rows)
        .unwrap_or_default())
}

fn games_of_rows(rows: &[Value], into: &mut BTreeSet<String>) {
    into.extend(
        rows.iter()
            .filter_map(|r| r["gameId"].as_str().map(str::to_string)),
    );
}

/// THE SAMPLED DIGEST RECONCILE (MEDIUM-10), one step on the belt's driver: at most one identity a
/// [`RECONCILE_INTERVAL_MS`] (64 a day). Phases A and B (`OWED_D1_JUDGE` on): since D2-ACTOR-B-FOLD-13 (bsv-low #958)
/// D1 is JUDGED FRESH first over the games its stored rows and the actor's differ on ([`RECONCILE_JUDGE_GAMES_MAX`]),
/// and since D2-ACTOR-B-FOLD-15 (NEW8-M1) the actor is re-applied over the same games at the same tip and the two
/// FRESH judgements are compared ([`reconcile_fresh`]: a held game apart, an actor's own tip lag apart, a stale D1
/// named, a disagreement the STOP). Phase C (the D1 rows are cold): the actor's rows before and after a re-apply of
/// its games from the record (a divergence the events missed is a diff). D1 reads: the state, one `owed_state` seek,
/// the identity's rows (A and B). Counted `owed_actor_reconcile_{compared,diffs,republished}_total`.
pub async fn reconcile_step(
    db: &impl JudgeDb,
    now: i64,
) -> Result<Option<ReconcileReport>, String> {
    let first = |out: Vec<StmtOut>| out.into_iter().next().map(|o| o.rows).unwrap_or_default();
    let mut read = db
        .run_batch(vec![
            Stmt::new(RECONCILE_STATE_SQL, vec![]),
            Stmt::new(D1_REJUDGE_STATE_SQL, vec![]),
        ])
        .await?
        .into_iter();
    let state = read.next().map(|o| o.rows).unwrap_or_default();
    let rejudge = rejudge_state_of(read.next().map(|o| o.rows).unwrap_or_default().first());
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
        rejudge_passed: rejudge_passed(rejudge.as_ref(), &identity),
        ..Default::default()
    };
    // Item 12: the actor's stored answer, its rows and whether it was bootstrapped at the compare.
    let (actor, bootstrapped) = match db.actor_stored(&identity).await {
        Ok(Some(answer)) => (
            answer["rows"].as_array().cloned().unwrap_or_default(),
            answer["bootstrapped"].clone(),
        ),
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
    if db.d1_judges() {
        reconcile_fresh(db, identity, actor, bootstrapped, report, now)
            .await
            .map(Some)
    } else {
        reconcile_phase_c(db, identity, actor, bootstrapped, report, now)
            .await
            .map(Some)
    }
}

/// The reconcile's cursor saved (the identity compared, at `now`).
async fn reconcile_save(db: &impl JudgeDb, identity: &str, now: i64) -> Result<(), String> {
    db.run_batch(vec![Stmt::new(
        crate::owed_belt::STATE_SQL,
        vec![
            Bind::Text(RECONCILE_STATE_NAME.into()),
            Bind::Text(identity.to_string()),
            Bind::Int(now),
        ],
    )])
    .await
    .map(|_| ())
}

/// The games of `outpoints` on either side.
fn games_of_outpoints(sides: &[&[Value]], outpoints: &[String]) -> BTreeSet<String> {
    let mut g = BTreeSet::new();
    for o in outpoints {
        for side in sides {
            for r in side.iter().filter(|r| r["outpoint"] == o.as_str()) {
                games_of_rows(std::slice::from_ref(r), &mut g);
            }
        }
    }
    g
}

/// The pending marks of `?2`'s games of `?1`: each game's greatest generation (FOLD-15: the games a mark holds at the
/// reconcile's start, and the bound of what was written before both judges read).
pub const RECONCILE_MARKS_SQL: &str = "SELECT gameId,MAX(generation) AS generation FROM owed_dirty WHERE identity=?1 AND gameId IN (SELECT value FROM json_each(?2)) GROUP BY gameId";

/// PHASES A AND B (D2-ACTOR-B-FOLD-15, the eighth delta lens's NEW8-M1): THE RECONCILE COMPARES FRESH WITH FRESH AT
/// ONE TIP. D1's stored rows and the actor's pick the games (the outpoints they differ on); at most
/// [`RECONCILE_JUDGE_GAMES_MAX`] (16, ONE judge call: NEW8-M2) are judged, the rest MARKED (`backfill`) and left out.
/// The picked games' marks are read first (each game's greatest generation: the games a mark holds, and the bound).
/// Then BOTH sides judge them at the reconcile's tip (the port's): D1 by the D1 judge (acknowledging nothing), the
/// actor by a re-apply (its closure re-reads the record: never a stored row), and the two FRESH judgements are
/// compared. A game holding a mark written WHILE the two judged (above the bound) is HELD, the delivery compare's
/// NEW2-L1 partition: its difference of the two fresh sides counted apart (`owed_actor_compare_held_total`, the line
/// `RECONCILE DIFF (held mark; not a STOP)`), never a disagreement. A mark pending at the START is no such reason
/// (D2-ACTOR-B-FOLD-16, the ninth delta lens's NEW9-M1): both fresh judgements read the record after it, so it
/// explains only the actor's stored rows against its re-apply (`missed`). What else differs:
/// - fresh D1 against the fresh actor, on a game no mark was written to while they judged: a JUDGE DISAGREEMENT
///   (`owed_actor_reconcile_diffs_total`, the STOP), whatever marks the game held at the start;
/// - the actor's stored rows against its fresh re-apply, outside a game holding a mark (start or during): only tip-derived
///   ([`tip_derived_rows`]) is a TIP LAG (`owed_actor_compare_tip_lag_total`, `RECONCILE TIP LAG`); anything else the
///   actor MISSED the record (an unmarked write), healed by the re-apply (counted a diff, its games
///   `owed_actor_reconcile_republished_total`);
/// - D1's stored rows against its fresh judgement, where the two fresh sides agree: a STALE D1 (`stale-d1`, #958).
async fn reconcile_fresh(
    db: &impl JudgeDb,
    identity: String,
    actor: Vec<Value>,
    bootstrapped: Value,
    mut report: ReconcileReport,
    now: i64,
) -> Result<ReconcileReport, String> {
    let stored = d1_stored_rows(db, &identity).await?;
    let stored_diff = diff_rows(&stored, &actor);
    let picked: Vec<String> = games_of_outpoints(&[&stored, &actor], &stored_diff)
        .into_iter()
        .collect();
    let (judge, rest) = picked.split_at(picked.len().min(RECONCILE_JUDGE_GAMES_MAX));
    let mut deltas = vec![(COUNTERS[6].to_string(), 1)];
    if judge.is_empty() {
        report.d1_digest = Some(rows_digest(&stored));
        report.actor_digest = Some(rows_digest(&actor));
        db.counters(deltas).await;
        reconcile_save(db, &identity, now).await?;
        return Ok(report);
    }
    let judge_json = json!(judge).to_string();
    let first = |out: Vec<StmtOut>| out.into_iter().next().map(|o| o.rows).unwrap_or_default();
    let marks = first(
        db.run_batch(vec![Stmt::new(
            RECONCILE_MARKS_SQL,
            vec![Bind::Text(identity.clone()), Bind::Text(judge_json.clone())],
        )])
        .await?,
    );
    // D2-ACTOR-B-FOLD-16 (NEW9-M1): the games a mark holds at the START explain only the actor's stored rows against
    // its re-apply (`missed`); both fresh judgements read the record after such a mark, so they never excuse a
    // difference between the two (`disagree`). Only a mark written WHILE the two judged (`during`, above the bound)
    // does: the delivery compare's NEW2-L1 rule. FIX-960: and only one the actor's closure READ (at most its
    // `readGeneration`): a mark written after its read is neither judgement's input, so it excuses nothing.
    let start: BTreeSet<String> = marks
        .iter()
        .filter_map(|m| m["gameId"].as_str().map(str::to_string))
        .collect();
    let bound = marks
        .iter()
        .filter_map(|m| crate::owed_events::ms_of(&m["generation"]))
        .max()
        .unwrap_or(0);
    // D1 fresh, one judge call.
    if let Err(e) =
        crate::owed_judge::judge_subjects_acked_to(db, &identity, judge, "reconcile", 0).await
    {
        // No fresh judgement, no compare: a stored row is never the comparison's side.
        db.log(&format!(
            "[owed-actor] reconcile {identity}: the fresh D1 judgement faulted, no compare: {e}"
        ));
        reconcile_save(db, &identity, now).await?;
        return Ok(report);
    }
    report.judged = judge.len();
    // The actor fresh, at the same tip (the port's: the reconcile's served tip). FIX-960: with the greatest mark
    // generation its closure read, where the held partition is cut (HELD_GAMES_SQL's one rule).
    let answer = db.actor_apply(&identity, judge, "reconcile").await;
    let (fresh_actor, actor_read): (Vec<Value>, i64) = match answer {
        Ok(Some(a)) if !is_held(&a) => (
            a["rows"].as_array().cloned().unwrap_or_default(),
            crate::owed_events::ms_of(&a["readGeneration"]).unwrap_or(i64::MAX),
        ),
        Ok(Some(_)) => {
            db.log(&format!(
                "[owed-actor] reconcile {identity}: the actor holds (bootstrapping), no compare"
            ));
            report.no_actor = true;
            reconcile_save(db, &identity, now).await?;
            return Ok(report);
        }
        Ok(None) => {
            report.no_actor = true;
            reconcile_save(db, &identity, now).await?;
            return Ok(report);
        }
        Err(e) => {
            db.log(&format!(
                "[owed-actor] reconcile {identity}: the actor's fresh re-apply: {e}"
            ));
            report.no_actor = true;
            reconcile_save(db, &identity, now).await?;
            return Ok(report);
        }
    };
    // D1's fresh rows, the marks written while the two judged, and the rest marked: one call.
    let mut stmts = vec![
        Stmt::new(RECONCILE_D1_ROWS_SQL, vec![Bind::Text(identity.clone())]),
        Stmt::new(
            HELD_GAMES_SQL,
            vec![
                Bind::Text(identity.clone()),
                Bind::Text(judge_json),
                Bind::Int(bound),
                Bind::Int(actor_read),
            ],
        ),
    ];
    if !rest.is_empty() {
        report.unjudged = rest.len();
        stmts.push(Stmt::new(
            RECONCILE_MARK_REST_SQL,
            vec![
                Bind::Text(identity.clone()),
                Bind::Int(now),
                Bind::Text(json!(rest).to_string()),
            ],
        ));
    }
    let mut out = db.run_batch(stmts).await?.into_iter();
    let fresh_d1 = out.next().map(|o| o.rows).unwrap_or_default();
    let during: BTreeSet<String> = out
        .next()
        .map(|o| o.rows)
        .unwrap_or_default()
        .iter()
        .filter_map(|r| r["gameId"].as_str().map(str::to_string))
        .collect();
    let judged: BTreeSet<&str> = judge.iter().map(String::as_str).collect();
    let of_judged = |rows: &[Value]| -> Vec<Value> {
        rows.iter()
            .filter(|r| judged.contains(r["gameId"].as_str().unwrap_or_default()))
            .cloned()
            .collect()
    };
    let unjudged: BTreeSet<&str> = rest.iter().map(String::as_str).collect();
    let keep = |rows: &[Value]| -> Vec<Value> {
        rows.iter()
            .filter(|r| !unjudged.contains(r["gameId"].as_str().unwrap_or_default()))
            .cloned()
            .collect()
    };
    let (a, b) = (keep(&fresh_d1), keep(&fresh_actor));
    let game_of = |o: &str| -> String {
        a.iter()
            .chain(&b)
            .chain(&actor)
            .chain(&stored)
            .find(|r| r["outpoint"] == o)
            .and_then(|r| r["gameId"].as_str())
            .unwrap_or("?")
            .to_string()
    };
    let held_during = |o: &String| during.contains(&game_of(o));
    let (held_diffs, disagree): (Vec<String>, Vec<String>) =
        diff_rows(&a, &b).into_iter().partition(held_during);
    let held_any = |o: &String| {
        let g = game_of(o);
        start.contains(&g) || during.contains(&g)
    };
    let (actor_stored, actor_fresh) = (of_judged(&actor), of_judged(&fresh_actor));
    let actor_lag = tip_derived_rows(&actor_stored, &actor_fresh);
    let missed: Vec<String> = diff_rows(&actor_stored, &actor_fresh)
        .into_iter()
        .filter(|o| !actor_lag.contains(o) && !held_any(o))
        .collect();
    let (d1_stored, d1_fresh) = (of_judged(&stored), of_judged(&fresh_d1));
    let d1_moved = diff_rows(&d1_stored, &d1_fresh);
    let fresh_differ: BTreeSet<&String> = disagree.iter().chain(&held_diffs).collect();
    report.stale_outpoints = stored_diff
        .iter()
        .filter(|o| d1_moved.contains(o) && !fresh_differ.contains(o))
        .count();
    report.d1_digest = Some(rows_digest(&a));
    report.actor_digest = Some(rows_digest(&b));
    report.diff = !disagree.is_empty() || !missed.is_empty();
    let tip = db.tip();
    if !actor_lag.is_empty() || !tip_lag_rows(&a, &b).is_empty() {
        deltas.push((COUNTERS[28].to_string(), 1));
        db.log(&format!(
            "[owed-actor] RECONCILE TIP LAG (not a STOP) {identity}: {} outpoints of the actor's stored rows judged at tip {:?} differ only by the tip from its re-apply at tip {tip:?}, first {}",
            actor_lag.len(),
            tips_of(&actor_stored, &actor_lag),
            actor_lag.first().map_or("-", String::as_str)
        ));
    }
    if report.stale_outpoints > 0 {
        // bsv-low #958: a stale D1, never a judge disagreement (both fresh judgements agree; nothing healed in D1 by
        // the actor).
        deltas.push((COUNTERS[29].to_string(), 1));
        db.log(&format!(
            "[owed-actor] RECONCILE stale-d1 {identity}: {} outpoints of D1's stored rows differed and the fresh D1 judgement agrees with the actor's fresh re-apply; D1's re-judge under {} had passed the identity: {}; {} games judged, {} marked past the bound",
            report.stale_outpoints,
            JUDGE_VERSION,
            report.rejudge_passed,
            report.judged,
            report.unjudged
        ));
    }
    if !held_diffs.is_empty() {
        deltas.push((COUNTERS[17].to_string(), 1));
        db.log(&format!(
            "[owed-actor] RECONCILE DIFF (held mark; not a STOP) {identity} game {}: {} outpoints, first {}; {}",
            game_of(&held_diffs[0]),
            held_diffs.len(),
            held_diffs[0],
            diff_detail(&a, &b, &held_diffs)
        ));
    }
    let boot = match &bootstrapped {
        Value::Bool(b) => b.to_string(),
        _ => "unknown".into(),
    };
    if !disagree.is_empty() {
        db.log(&format!(
            "[owed-actor] RECONCILE DIFF {identity}: {} outpoints differ (a judge disagreement: D1 and the actor judged fresh over {} games at tip {tip:?}), actor bootstrapped at the compare: {boot}; {}",
            disagree.len(),
            report.judged,
            diff_detail(&a, &b, &disagree)
        ));
    }
    if !missed.is_empty() {
        report.reapplied = judge.len();
        deltas.push((COUNTERS[5].to_string(), report.reapplied as u64));
        db.log(&format!(
            "[owed-actor] RECONCILE DIFF {identity}: {} outpoints of the actor's stored rows differ from its fresh re-apply (the actor missed the record; {} games re-applied), actor bootstrapped at the compare: {boot}; {}",
            missed.len(),
            report.reapplied,
            diff_detail(&actor_stored, &actor_fresh, &missed)
        ));
    }
    if report.diff {
        deltas.push((COUNTERS[7].to_string(), 1));
        BARS.with(|b| b.borrow_mut().reconcile_diffs += 1);
    }
    db.counters(deltas).await;
    reconcile_save(db, &identity, now).await?;
    Ok(report)
}

/// PHASE C (the D1 rows are cold): the actor's rows before against after a re-apply of its games from the record (a
/// divergence the events missed is a diff).
async fn reconcile_phase_c(
    db: &impl JudgeDb,
    identity: String,
    actor: Vec<Value>,
    bootstrapped: Value,
    mut report: ReconcileReport,
    now: i64,
) -> Result<ReconcileReport, String> {
    let mut games = BTreeSet::new();
    games_of_rows(&actor, &mut games);
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
    let (a, b) = (actor, after);
    report.d1_digest = Some(rows_digest(&a));
    report.actor_digest = Some(rows_digest(&b));
    report.diff = report.d1_digest != report.actor_digest;
    let mut deltas = vec![(COUNTERS[6].to_string(), 1)];
    if report.diff {
        deltas.push((COUNTERS[7].to_string(), 1));
        BARS.with(|b| b.borrow_mut().reconcile_diffs += 1);
        let differ = diff_rows(&a, &b);
        db.log(&format!(
            "[owed-actor] RECONCILE DIFF {identity}: {} outpoints differ, {} games re-applied, actor bootstrapped at the compare: {}; {}",
            differ.len(),
            report.reapplied,
            match bootstrapped {
                Value::Bool(b) => b.to_string(),
                _ => "unknown".into(),
            },
            diff_detail(&a, &b, &differ)
        ));
    }
    db.counters(deltas).await;
    reconcile_save(db, &identity, now).await?;
    Ok(report)
}
/// D2-ACTOR-B-FOLD-13 (bsv-low #958): D1'S ROWS RE-JUDGED FLEET-WIDE AFTER A [`JUDGE_VERSION`] MOVE. The actor
/// re-judges itself on a version mismatch ([`rejudge_due`]); D1's `owed_rows` kept the old code's rows until a mark or
/// the age repair reached them, so the reconcile and the operator's route compared a fresh actor against a stale D1.
/// One row of the belt's ledger, [`D1_REJUDGE_NAME`] (no schema change), carries the version D1's rows were last
/// re-judged under: `{version, startedAtMs, endedAtMs, done, identities, games, rows, faults, skipped, after, open,
/// openFaults, previous}`. When the running build's version differs (or no row stands), a paced re-judge walks every
/// identity (the reconcile's four-seek union, [`RECONCILE_NEXT_SQL`]) and each identity's games by game id
/// (`owed_judge::IDENTITY_GAMES_BY_ID_SQL`), judging them with the D1 judge from D1's own evidence and acknowledging
/// NOTHING (the marks stand for the drain and the consumer, under their own bounds); then `done` and `endedAtMs`. A
/// deploy with the same version runs nothing: one primary-key read a tick. THE PACE: [`D1_REJUDGE_GAMES_PER_TICK`]
/// (32 by default, two judge calls with the reconcile yielding; the lever sets 0 to 32: FOLD-15) a belt alarm (120
/// s), inside the alarm's existing turn, before the reconcile: 23,040 games a day at 32, the whale of 1,033 games in
/// about 65 minutes (11,520 and about 130 minutes at 16), and at most
/// [`D1_REJUDGE_IDENTITIES_PER_TICK`] identities opened a tick (an identity with no game counts); its cost is the judge's commit (measured by the backfill
/// tier: about 3,000 rows read per 16 games on a 500-row identity, so at most about 4.3 M rows a day while a run
/// lasts, none after). A faulted identity is retried the next tick (counted `owed_d1_rejudge_faults_total`) and
/// skipped at its [`D1_REJUDGE_IDENTITY_FAULTS_MAX`]th consecutive fault (logged, `skipped`), so one poisoned identity
/// never stops the fleet's run. Phase C (the D1 judge off) runs nothing: the rollback's `owed-v3` pass rebuilds D1.
/// Served on `/health.owedActor.d1Rejudge`.
pub const D1_REJUDGE_NAME: &str = "owed-judge:version";
pub const D1_REJUDGE_STATE_SQL: &str =
    "SELECT cursor FROM owed_belt_state WHERE name='owed-judge:version'";
/// D2-ACTOR-B-FOLD-15 (NEW8-M2 and the captain's pace lever): THE PACE IS A LEVER. The games a tick are the pace
/// row's (`owed-judge:pace`, `{"gamesPerTick": N}`, set by `POST /internal/owed-rejudge` under `INTERNAL_TOKEN`, read
/// once per alarm with the stamp), else the render's `OWED_D1_REJUDGE_GAMES_PER_TICK`, else
/// [`D1_REJUDGE_GAMES_PER_TICK`] (32); clamped to [`D1_REJUDGE_GAMES_PER_TICK_MAX`]. `0` PAUSES the walk, its cursor
/// kept and the stamp serving `paused`. One judge call is 16 games ([`OWED_JUDGE_SUBJECTS_PER_INVOCATION`]), so a
/// tick makes `ceil(N / 16)` judge calls; a tick that made more than one yields the alarm's reconcile to the next
/// alarm ([`reconcile_yields`]), so the alarm's one invocation holds the belt tick and AT MOST two judge calls of the
/// re-judge (32 games), or the belt tick, one judge call and the reconcile's one (measured: `fold15_c`). D2-ACTOR-B-FOLD-16
/// (NEW9-L1): the alarm's drain counts too: a drain that did work (its sweep re-published pairs) holds the re-judge to
/// ONE judge call and the reconcile yields ([`rejudge_calls_max`]; the worst alarm measured by `fold16_b`).
///
/// THE RECONCILE'S READ (the ninth delta lens's NOTE-2): at 32 the re-judge makes two judge calls on nearly every tick
/// of beta's seats (hundreds of games each), so the reconcile yields almost every tick and a flat
/// `reconcileDiffs` there means nothing was compared. A Phase B read of `reconcileDiffs` needs the walk paused (`0`)
/// or at 16 or below, AND `owed_actor_reconcile_compared_total` rising over the read.
///
/// [`OWED_JUDGE_SUBJECTS_PER_INVOCATION`]: crate::owed_judge::OWED_JUDGE_SUBJECTS_PER_INVOCATION
pub const D1_REJUDGE_GAMES_PER_TICK: usize = 32;
/// The ceiling a tick (the pace row and the var clamped to it): two judge calls, the reconcile yielding.
pub const D1_REJUDGE_GAMES_PER_TICK_MAX: usize = 32;
/// The pace row's name, its read, and the lever's write (`?1` the cursor, `?2` now).
pub const D1_REJUDGE_PACE_NAME: &str = "owed-judge:pace";
pub const D1_REJUDGE_PACE_SQL: &str =
    "SELECT cursor FROM owed_belt_state WHERE name='owed-judge:pace'";
/// The lever's statement that clears the row (the render's default applies again).
pub const D1_REJUDGE_PACE_CLEAR_SQL: &str =
    "DELETE FROM owed_belt_state WHERE name='owed-judge:pace'";
/// The lever stamps the pace into a standing stamp (`?1` paused, `?2` the games a tick), so `d1Rejudge` serves it.
pub const D1_REJUDGE_STAMP_PACE_SQL: &str = "UPDATE owed_belt_state SET cursor=json_set(cursor,'$.paused',json(?1),'$.gamesPerTick',CAST(?2 AS INTEGER)) WHERE name='owed-judge:version'";

/// PURE: the games a tick: the pace row's, else the render's var, else the default; clamped to the ceiling.
pub fn rejudge_pace(row: Option<&Value>, var: Option<&str>) -> usize {
    let of_row = row.and_then(|r| crate::owed_events::ms_of(&r["gamesPerTick"]));
    let of_var = var.and_then(|v| v.trim().parse::<i64>().ok());
    of_row
        .or(of_var)
        .map_or(D1_REJUDGE_GAMES_PER_TICK, |n| n.max(0) as usize)
        .min(D1_REJUDGE_GAMES_PER_TICK_MAX)
}

/// PURE: does the alarm's reconcile yield to this re-judge tick? It made more than one judge call, or (D2-ACTOR-B-FOLD-16,
/// NEW9-L1) it made one in an alarm whose drain did work: the drain's statements count as one judge call's in the
/// invocation's bound, so the alarm holds the belt tick and two of {the drain, a judge call, the reconcile's} at most.
pub fn reconcile_yields(r: &RejudgeReport) -> bool {
    r.judge_calls > 1 || (r.drain_bound && r.judge_calls >= 1)
}

/// PURE (D2-ACTOR-B-FOLD-16, NEW9-L1): the judge calls a re-judge tick may make at `pace`: `ceil(pace / 16)`, or ONE
/// when the alarm's drain did work this invocation (the drain's sweep, its publish batch and its direct pass's mark
/// reads are measured at about 280 statements for a full 64-pair sweep: `fold16_b`).
pub fn rejudge_calls_max(pace: usize, drain_bound: bool) -> usize {
    let calls = pace.div_ceil(crate::owed_judge::OWED_JUDGE_SUBJECTS_PER_INVOCATION);
    if drain_bound {
        calls.min(1)
    } else {
        calls
    }
}
/// D2-ACTOR-B-FOLD-15 (NEW8-M2): the identities OPENED a tick, at most, whether they hold a game or none (each costs
/// one `next` read and one games page; the state's write rides in the next read): an identity with no game counts.
pub const D1_REJUDGE_IDENTITIES_PER_TICK: usize = 32;
pub const D1_REJUDGE_IDENTITY_FAULTS_MAX: u64 = 3;
/// The operator's route: the identity's `owed_state.computedAtMs` and the stamp, one statement.
pub const D1_JUDGED_UNDER_SQL: &str = "SELECT (SELECT CAST(computedAtMs AS INTEGER) FROM owed_state WHERE identity=?1) AS computedAtMs,(SELECT cursor FROM owed_belt_state WHERE name='owed-judge:version') AS stamp";

/// The stamp's state, parsed from its row (`cursor`).
pub fn rejudge_state_of(row: Option<&Value>) -> Option<Value> {
    row.and_then(|r| r["cursor"].as_str())
        .and_then(|c| serde_json::from_str::<Value>(c).ok())
        .filter(Value::is_object)
}

/// Has D1's re-judge under the RUNNING version passed `identity` (its run done, or its cursor at or past it)?
pub fn rejudge_passed(state: Option<&Value>, identity: &str) -> bool {
    let Some(s) = state else { return false };
    s["version"] == JUDGE_VERSION
        && (s["done"] == true
            || s["after"]
                .as_str()
                .is_some_and(|a| !a.is_empty() && identity <= a))
}

/// The judge version D1's rows of `identity` were judged under: the stamp's when its run passed the identity (done,
/// or the cursor at or past it); the PREVIOUS run's when the identity's last judgement (`owed_state.computedAtMs`, the
/// greatest of its games') is older than the stamp's run AND that previous run was done (FOLD-15, NEW8-L1);
/// `"unknown"` otherwise (no stamp, or a judgement since the
/// run began on an identity the run has not passed: its other games may be older).
pub fn d1_judged_under(state: Option<&Value>, identity: &str, computed_at: Option<i64>) -> Value {
    let Some(s) = state else {
        return json!("unknown");
    };
    let passed = s["done"] == true
        || s["after"]
            .as_str()
            .is_some_and(|a| !a.is_empty() && identity <= a);
    if passed {
        return s["version"].clone();
    }
    match (computed_at, s["startedAtMs"].as_i64()) {
        // D2-ACTOR-B-FOLD-15 (the eighth delta lens's NEW8-L1): the previous run's version only when that run was
        // DONE (it passed every identity); a run replaced mid-walk names no version its rows were judged under.
        (Some(c), Some(start)) if c < start && s["previous"]["done"] == true => {
            match &s["previous"]["version"] {
                Value::String(v) => json!(v),
                _ => json!("unknown"),
            }
        }
        _ => json!("unknown"),
    }
}

/// What `/health.owedActor.d1Rejudge` serves of the stamp.
pub fn rejudge_served(state: Option<&Value>) -> Value {
    match state {
        Some(s) => json!({
            "version": s["version"],
            "running": JUDGE_VERSION,
            "identities": s["identities"].as_u64().unwrap_or(0),
            "skipped": s["skipped"].as_u64().unwrap_or(0),
            "done": s["done"] == true,
            "paused": s["paused"] == true,
            "gamesPerTick": s["gamesPerTick"],
            "startedAtMs": json_ms(&s["startedAtMs"]),
            "endedAtMs": json_ms(&s["endedAtMs"]),
        }),
        None => Value::Null,
    }
}

/// One tick's account of D1's re-judge.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RejudgeReport {
    /// A run began this tick (the version moved, or no stamp stood).
    pub started: bool,
    pub identities: usize,
    pub games: usize,
    pub rows: usize,
    pub faulted: bool,
    pub skipped: usize,
    /// The run ended this tick (the stamp moved to the running version).
    pub done: bool,
    /// FOLD-15: the judge calls this tick made, and the games a tick in force (0: paused).
    pub judge_calls: usize,
    pub games_per_tick: usize,
    pub paused: bool,
    /// D2-ACTOR-B-FOLD-16 (NEW9-L1): the alarm's drain did work this invocation (its sweep re-published pairs, or,
    /// with no queue, it judged subjects), so the re-judge made at most ONE judge call and the reconcile yields.
    pub drain_bound: bool,
}

/// One paced tick of D1's re-judge ([`D1_REJUDGE_NAME`]): since FOLD-15 at most [`D1_REJUDGE_IDENTITIES_PER_TICK`]
/// identities opened and `ceil(pace / 16)` judge calls (two at the default 32; one in an alarm whose drain did work,
/// FOLD-16), the state saved after each identity; `None` when nothing runs (Phase C, or the stamp is the
/// running version and done).
pub async fn d1_rejudge_step(db: &impl JudgeDb, now: i64) -> Result<Option<RejudgeReport>, String> {
    d1_rejudge_step_paced(db, now, None).await
}

/// [`d1_rejudge_step`] under the render's pace var (`OWED_D1_REJUDGE_GAMES_PER_TICK`, `None` unset): the pace row
/// and the stamp read in ONE call.
pub async fn d1_rejudge_step_paced(
    db: &impl JudgeDb,
    now: i64,
    pace_var: Option<&str>,
) -> Result<Option<RejudgeReport>, String> {
    d1_rejudge_step_after_drain(db, now, pace_var, false).await
}

/// [`d1_rejudge_step_paced`] in the alarm (D2-ACTOR-B-FOLD-16, NEW9-L1): `drain_bound` when the alarm's drain did work
/// this invocation, so the tick makes at most one judge call ([`rejudge_calls_max`]) and the reconcile yields
/// ([`reconcile_yields`]).
pub async fn d1_rejudge_step_after_drain(
    db: &impl JudgeDb,
    now: i64,
    pace_var: Option<&str>,
    drain_bound: bool,
) -> Result<Option<RejudgeReport>, String> {
    if !db.d1_judges() {
        return Ok(None);
    }
    let mut read = db
        .run_batch(vec![
            Stmt::new(D1_REJUDGE_STATE_SQL, vec![]),
            Stmt::new(D1_REJUDGE_PACE_SQL, vec![]),
        ])
        .await?
        .into_iter();
    let held = rejudge_state_of(read.next().map(|o| o.rows).unwrap_or_default().first());
    let pace_row = read
        .next()
        .and_then(|o| o.rows.into_iter().next())
        .and_then(|r| {
            r["cursor"]
                .as_str()
                .and_then(|c| serde_json::from_str::<Value>(c).ok())
        });
    let pace = rejudge_pace(pace_row.as_ref(), pace_var);
    let mut report = RejudgeReport {
        games_per_tick: pace,
        paused: pace == 0,
        drain_bound,
        ..Default::default()
    };
    let calls_max = rejudge_calls_max(pace, drain_bound);
    if pace == 0 {
        // PAUSED: nothing walked, the cursor kept; the stamp says so once (the lever stamps it too).
        if let Some(mut s) = held.filter(|s| s["version"] == JUDGE_VERSION && s["done"] != true) {
            if s["paused"] != true {
                s["paused"] = json!(true);
                s["gamesPerTick"] = json!(0);
                db.run_batch(vec![
                    Stmt::new(
                        crate::owed_belt::STATE_SQL,
                        vec![
                            Bind::Text(D1_REJUDGE_NAME.into()),
                            Bind::Text(s.to_string()),
                            Bind::Int(now),
                        ],
                    ),
                    health_snapshot_stmt(now.max(0) as u64, true),
                ])
                .await?;
            }
        }
        return Ok(Some(report));
    }
    let mut st = match held {
        Some(s) if s["version"] == JUDGE_VERSION => {
            if s["done"] == true {
                return Ok(None);
            }
            s
        }
        old => {
            report.started = true;
            json!({
                "version": JUDGE_VERSION, "startedAtMs": now, "endedAtMs": null, "done": false,
                "identities": 0, "games": 0, "rows": 0, "faults": 0, "skipped": 0,
                "after": "", "open": null, "openFaults": 0,
                "previous": old.map(|o| json!({"version": o["version"], "startedAtMs": o["startedAtMs"], "done": o["done"]})),
            })
        }
    };
    st["paused"] = json!(false);
    st["gamesPerTick"] = json!(pace);
    let add = |st: &mut Value, k: &str, n: u64| {
        st[k] = json!(st[k].as_u64().unwrap_or(0) + n);
    };
    let save = |st: &Value| {
        vec![
            Stmt::new(
                crate::owed_belt::STATE_SQL,
                vec![
                    Bind::Text(D1_REJUDGE_NAME.into()),
                    Bind::Text(st.to_string()),
                    Bind::Int(now),
                ],
            ),
            health_snapshot_stmt(now.max(0) as u64, st["done"] == true),
        ]
    };
    // D2-ACTOR-B-FOLD-15 (the eighth delta lens's NEW8-M2): bounded by D1 CALLS as well as games. At most
    // [`D1_REJUDGE_IDENTITIES_PER_TICK`] identities are OPENED a tick (each one `next` read and one games page,
    // whether it holds a game or none) and at most the pace's games (16 a judge call, `calls_max` calls); an identity whose page comes back short
    // of its size is done with no empty page after it. The state is SAVED AFTER EACH IDENTITY or judge call: its
    // write rides in the next read's batch (no call added), the last in the tick's save, so a tick that dies loses
    // the one identity in flight at most.
    let state_stmt = |st: &Value| {
        Stmt::new(
            crate::owed_belt::STATE_SQL,
            vec![
                Bind::Text(D1_REJUDGE_NAME.into()),
                Bind::Text(st.to_string()),
                Bind::Int(now),
            ],
        )
    };
    let mut pending: Option<Stmt> = None;
    let with_pending = |pending: &mut Option<Stmt>, read: Stmt| -> Vec<Stmt> {
        let mut v: Vec<Stmt> = pending.take().into_iter().collect();
        v.push(read);
        v
    };
    let last = |out: Vec<StmtOut>| out.into_iter().last().map(|o| o.rows).unwrap_or_default();
    let mut faults = 0u64;
    let mut opened = 0usize;
    loop {
        let open = st["open"].as_array().and_then(|a| {
            Some((
                a.first()?.as_str()?.to_string(),
                a.get(1)?.as_str()?.to_string(),
            ))
        });
        let Some((identity, last_game)) = open else {
            if opened >= D1_REJUDGE_IDENTITIES_PER_TICK {
                break;
            }
            opened += 1;
            let next = last(
                db.run_batch(with_pending(
                    &mut pending,
                    Stmt::new(
                        RECONCILE_NEXT_SQL,
                        vec![Bind::Text(st["after"].as_str().unwrap_or_default().into())],
                    ),
                ))
                .await?,
            );
            match next.first().and_then(|r| r["identity"].as_str()) {
                Some(id) => {
                    st["open"] = json!([id, ""]);
                    st["openFaults"] = json!(0);
                }
                None => {
                    st["done"] = json!(true);
                    st["endedAtMs"] = json!(now);
                    st["open"] = Value::Null;
                    report.done = true;
                    break;
                }
            }
            continue;
        };
        if report.games >= pace || report.judge_calls >= calls_max {
            break;
        }
        let page = (pace - report.games).min(crate::owed_judge::OWED_JUDGE_SUBJECTS_PER_INVOCATION);
        let games: Vec<String> = last(
            db.run_batch(with_pending(
                &mut pending,
                Stmt::new(
                    crate::owed_judge::IDENTITY_GAMES_BY_ID_SQL,
                    vec![
                        Bind::Text(identity.clone()),
                        Bind::Text(last_game),
                        Bind::Int(page as i64),
                    ],
                ),
            ))
            .await?,
        )
        .iter()
        .filter_map(|r| r["gameId"].as_str().map(str::to_string))
        .collect();
        let identity_done = |st: &mut Value, report: &mut RejudgeReport| {
            report.identities += 1;
            add(st, "identities", 1);
            st["after"] = json!(identity);
            st["open"] = Value::Null;
        };
        if games.is_empty() {
            identity_done(&mut st, &mut report);
            pending = Some(state_stmt(&st));
            continue;
        }
        report.judge_calls += 1;
        match crate::owed_judge::judge_subjects_acked_to(db, &identity, &games, "rejudge", 0).await
        {
            Ok(judged) => {
                report.games += games.len();
                report.rows += judged.rows;
                add(&mut st, "games", games.len() as u64);
                add(&mut st, "rows", judged.rows as u64);
                st["openFaults"] = json!(0);
                if games.len() < page {
                    identity_done(&mut st, &mut report);
                } else {
                    st["open"] = json!([identity, games.last().cloned().unwrap_or_default()]);
                }
                pending = Some(state_stmt(&st));
            }
            Err(e) => {
                faults += 1;
                report.faulted = true;
                add(&mut st, "faults", 1);
                let n = st["openFaults"].as_u64().unwrap_or(0) + 1;
                if n >= D1_REJUDGE_IDENTITY_FAULTS_MAX {
                    db.log(&format!(
                        "[owed-actor] D1 re-judge under {JUDGE_VERSION} SKIPPED {identity} after {n} faults: {e}"
                    ));
                    report.skipped += 1;
                    add(&mut st, "skipped", 1);
                    st["after"] = json!(identity);
                    st["open"] = Value::Null;
                    st["openFaults"] = json!(0);
                } else {
                    db.log(&format!(
                        "[owed-actor] D1 re-judge under {JUDGE_VERSION} {identity}: fault {n}, retried next tick: {e}"
                    ));
                    st["openFaults"] = json!(n);
                }
                // The fault ends the tick: the identity is retried at the next one (or passed, once skipped).
                break;
            }
        }
    }
    drop(pending);
    db.run_batch(save(&st)).await?;
    let mut deltas = Vec::new();
    if report.identities > 0 {
        deltas.push((COUNTERS[30].to_string(), report.identities as u64));
    }
    if faults > 0 {
        deltas.push((COUNTERS[31].to_string(), faults));
    }
    if !deltas.is_empty() {
        db.counters(deltas).await;
    }
    if report.done {
        db.log(&format!(
            "[owed-actor] D1 re-judge under {JUDGE_VERSION} DONE: {} identities, {} games, {} skipped",
            st["identities"], st["games"], st["skipped"]
        ));
    }
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
/// The D2-ACTOR-B lens's MEDIUMs (lane D2-ACTOR-B-FOLD-2) add six: the bootstraps that finished (MEDIUM-1), the
/// reads of an identity D1 does not know, answered without an actor (MEDIUM-3), the actors reset on an epoch move
/// (MEDIUM-5), the Phase A applies run off the consumer (MEDIUM-6), and the `owed-v3` re-judge's identities and rows
/// (MEDIUM-9).
pub const COUNTERS: [&str; 32] = [
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
    "owed_actor_bootstraps_total",
    "owed_actor_unknown_reads_total",
    "owed_actor_epoch_resets_total",
    "owed_actor_deferred_total",
    "owed_v3_identities_total",
    "owed_v3_rows_total",
    // The delta lens's NEW-L6: the deferred applies that ENDED (`owed_actor_deferred_total` counts each at its start).
    "owed_actor_deferred_done_total",
    // The second delta lens's NEW2-L1: Phase B's DIFFs on a game holding a mark above the acknowledgement bound.
    "owed_actor_compare_held_total",
    // NEW2-L5: the tip alarms whose window held a game whose apply faulted (each named in its line).
    "owed_actor_tip_faults_total",
    // The third delta lens's LOW-1: the games the tip alarm parked (a fault counted only where another apply landed).
    "owed_actor_tip_parked_total",
    // LOW-2: an actor's schema plan that did not apply whole (its digest left unstamped, retried by its next instance).
    "owed_actor_schema_partial_total",
    // D2-ACTOR-B-FOLD-7: the events an actor HELD (not bootstrapped, or re-armed: answered 202, neither a fault nor an
    // apply sample), and the bootstrapped actors whose closure met its bound and re-armed their bootstrap.
    "owed_actor_apply_held_total",
    "owed_actor_closure_rearmed_total",
    // The captain's item 12: an actor armed under a dead alarm (lost, or set decades out by an older build) re-armed.
    "owed_actor_alarm_rearmed_total",
    // The fourth delta lens's NEW4-M2: every bootstrap-page and drain fault (a page parked at its fifth in a row).
    "owed_actor_bootstrap_faults_total",
    // D2-ACTOR-D (bsv-low #952): the writers' direct calls that met their wall (`DIRECT_CALL_WALL_MS`); a direct call
    // that answered an error is `owed_actor_direct_faults_total`, every call `owed_actor_direct_calls_total`.
    "owed_actor_direct_walls_total",
    // D2-ACTOR-B-FOLD-10 (the captain's item, beta 2026-10-29): the bootstrapped actors whose state another build
    // judged, re-armed from the start ([`rejudge_due`]).
    "owed_actor_rejudged_total",
    // D2-ACTOR-E (bsv-low #954): the record reads past an event's own rows (a cold game's whole closure, a key a warm
    // mirror had not read).
    "owed_actor_closure_misses_total",
    // D2-ACTOR-B-FOLD-12 (the seventh delta lens's NEW7-L2): the compares (Phases A and B, the delivery's and the
    // reconcile's) whose rows differed ONLY in the judged tip (`facts.tip`, the `probeTip` column): a lag, never a STOP.
    "owed_actor_compare_tip_lag_total",
    // D2-ACTOR-B-FOLD-13 (bsv-low #958): the reconciles whose stored D1 rows differed and whose fresh D1 judgement
    // agreed with the actor (`stale-d1`, never a judge disagreement), and D1's fleet re-judge after a judge-version
    // move: its identities done and its faults.
    "owed_actor_reconcile_stale_total",
    "owed_d1_rejudge_identities_total",
    "owed_d1_rejudge_faults_total",
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
    /// FIX-974 (bsv-low #974): the MINED word's event (the overlay's `proof` frame, [`mined_word_event`]): the
    /// collect-after-mined bar's sample at its first apply.
    pub mined: bool,
    /// D2-ACTOR-E (bsv-low #954): the txids the event's write named, its source key and its facts (the row's own
    /// columns the publish read: a pot's spender, a party's pot, a hop's evidence); `None` for an event that names
    /// no row (an identity note, the read's and the repairs' own marks, the tip): the apply reads its whole closure.
    pub txids: Option<Vec<String>>,
    /// D2-ACTOR-B-FOLD-11 (the sixth delta lens's NEW6-L1): the other events a coalesced consumer delivery carries
    /// (`kind`, `seq`, write time): each recorded as applying the delivery's games and SAMPLED by its own first apply
    /// and birth ([`event_visibility`]), so a batch whose oldest event an earlier call applied still samples the
    /// faulted tail it carries.
    pub also: Vec<(String, i64, i64)>,
    /// D2-ACTOR-B-FOLD-12 (the seventh delta lens's NEW7-L3): a coalesced consumer delivery's events PER IDENTITY (the
    /// identity lowercased; each `(kind, seq, atMs, mined)` an event whose plan names it), so each identity's apply
    /// carries only its own events ([`EventCtx::for_identity`], called by [`apply_body`]). Empty: one event's
    /// delivery, carried as it stands. One `also` served every identity of the batch, so an event naming another
    /// identity read as a first apply in each actor and was sampled once per identity.
    pub by_identity: BTreeMap<String, Vec<(String, i64, i64, bool)>>,
}

impl EventCtx {
    /// PURE (NEW7-L3): the delivery one identity's apply carries: its OWN events of a coalesced batch, the oldest under
    /// the delivery's key and the rest in `also`; the context as it stands when it names no identities (one event),
    /// or (never in a batch: every planned event names an identity) with no `also` for an identity it does not name.
    pub fn for_identity(&self, identity: &str) -> EventCtx {
        if self.by_identity.is_empty() {
            return self.clone();
        }
        let mut out = EventCtx {
            by_identity: BTreeMap::new(),
            also: Vec::new(),
            ..self.clone()
        };
        let Some(own) = self.by_identity.get(&identity.to_ascii_lowercase()) else {
            return out;
        };
        let mut own = own.clone();
        own.sort_by_key(|(_, seq, at, _)| (*at, *seq));
        own.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
        if let Some((kind, seq, at, mined)) = own.first().cloned() {
            out.kind = kind;
            out.seq = seq;
            out.at_ms = at;
            out.mined = mined;
        }
        out.also = own
            .into_iter()
            .skip(1)
            .map(|(kind, seq, at, _)| (kind, seq, at))
            .collect();
        out
    }
}

/// PURE (NEW6-L1): the rows' visibility samples of one delivery, `now - atMs` for each event it carries (its own,
/// then every coalesced one: `(atMs, first apply here, born on a warm actor)`) whose apply here is its FIRST and
/// whose birth is warm. An event with no write time samples nothing.
pub fn event_visibility(now_ms: u64, events: &[(Option<u64>, bool, bool)]) -> Vec<u64> {
    events
        .iter()
        .filter_map(|(at, first, warm)| match (at, first, warm) {
            (Some(at), true, true) => Some(now_ms.saturating_sub(*at)),
            _ => None,
        })
        .collect()
}

/// PURE (D2-ACTOR-E): the txids an event's write named, for an event of a trigger's pair.
pub fn event_txids(ev: &overlay_discovery::owed_event::OwedEvent) -> Option<Vec<String>> {
    if !overlay_discovery::owed_event::TRIGGER_SOURCES
        .iter()
        .any(|(_, s)| *s == ev.source)
    {
        return None;
    }
    let mut out = BTreeSet::new();
    txids_in(&ev.source_key, &mut out);
    txids_in_value(&ev.facts, &mut out);
    Some(out.into_iter().collect())
}
/// FIX-974 (bsv-low #974): the tracker frame kind that IS the mined word: `proof`, the bump the overlay's
/// `/arc-ingest` MINED arm verified against chaintracks (`proof_fetcher::verify_bump_detailed`'s `Valid` arm alone
/// notes it), the one frame of that push the publish carries.
pub const MINED_FRAME_KIND: &str = "proof";

/// PURE (FIX-974, bsv-low #974; L53's S3): is this event the MINED word's, the collect-after-mined bar's sample? A
/// `word` event carrying the overlay's `proof` frame (`facts.frame`, `{txid, kind, body}` as `tracker_events::note`
/// builds it and `owed_events_publish::frame_note` carries it): Arcade's MINED callback on the spend, the road the
/// design's bar 3 reads (the callback's `at` is the event's write time). Not the mined word's: a SEEN or submit frame,
/// a MINED `callback` frame (Arcade's claim over a bump the overlay did not verify; else the `proof` frame rides), and
/// a row-shaped `word` event (`facts.row`, `READ_WORD_SQL`: the tracker's own write, the tick's re-ask or the POST
/// belt, whatever its word). As built, `EventCtx::of` read `facts.word` and `facts.frame.word`, fields no published
/// shape holds, so no event of L53's 133 cells carried `mined: true` and the bar had no sample through the loop.
pub fn mined_word_event(ev: &overlay_discovery::owed_event::OwedEvent) -> bool {
    ev.frame()
        .is_some_and(|f| f["kind"].as_str() == Some(MINED_FRAME_KIND))
}

impl EventCtx {
    pub fn of(ev: &overlay_discovery::owed_event::OwedEvent) -> Self {
        Self {
            kind: ev.source.clone(),
            seq: ev.seq,
            at_ms: ev.at_ms,
            // FIX-974: the mined word's event is the proof frame's (`mined_word_event`)
            mined: mined_word_event(ev),
            txids: event_txids(ev),
            also: Vec::new(),
            by_identity: BTreeMap::new(),
        }
    }
}

/// The `/apply` body of one delivery.
pub fn apply_body(identity: &str, games: &[String], e: &EventCtx, tip: Option<u64>) -> Value {
    // NEW7-L3: the identity's own events of a coalesced batch
    let e = &e.for_identity(identity);
    let mut body = json!({"identity": identity.to_ascii_lowercase(), "games": games, "kind": e.kind, "seq": e.seq, "atMs": e.at_ms, "tip": tip, "mined": e.mined, "txids": e.txids});
    // NEW6-L1: a coalesced delivery's other events, with their write times (an older object ignores the times)
    if !e.also.is_empty() {
        body["also"] = json!(e
            .also
            .iter()
            .map(|(k, s, _)| json!([k, s]))
            .collect::<Vec<_>>());
        body["alsoAtMs"] = json!(e.also.iter().map(|(_, _, at)| at).collect::<Vec<_>>());
    }
    body
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
///
/// The captain's item 11 (D2-ACTOR-B-FOLD-8): `bound` is the acknowledgement's bound read BEFORE the D1 judge (as
/// [`actor_then_d1`] reads it before the actor), so a mark written between the two judges (a word landing between
/// them) makes its game's DIFF a HELD one (`owed_actor_compare_held_total`), never the cutover's STOP; a diff with no
/// mark above the bound stays real. `None` (the bound's read faulted) compares as before.
///
/// D2-ACTOR-D (bsv-low #952): the delivery names the same `bound` to the actor, so a delivery the writer's direct call
/// already covered is a DUPE the actor answers in milliseconds; the compare runs on one event in
/// [`COMPARE_SAMPLE_EVERY`] ([`compare_sampled`]).
pub async fn beside(db: &impl JudgeDb, identity: &str, games: &[String], bound: Option<i64>) {
    let answer = match db.actor_deliver_bounded(identity, games, bound).await {
        Ok(Some(a)) => a,
        Ok(None) => return,
        Err(e) => {
            db.counters(vec![(COUNTERS[2].into(), 1)]).await;
            note_actor_fault(identity, db.event_at_ms());
            db.log(&format!("[owed-actor] FAULT {identity}: {e}"));
            return;
        }
    };
    // D2-ACTOR-B-FOLD-7: a HELD answer (the actor bootstrapping) holds no rows to compare: not a fault, not a sample.
    if is_held(&answer) {
        return;
    }
    note_answer(&answer);
    if compare_sampled(db.event_seq()) {
        compare(db, identity, games, &answer, bound).await;
    }
}

/// D2-ACTOR-D (bsv-low #952; the consumer's per-event cost): Phases A and B compare the actor's rows with the D1
/// judge's on ONE event in this many (by the event's seq, consecutive tokens from the publish). The STOP signal
/// (`owed_actor_diffs_total`) is kept STATISTICALLY: a diff that persists across events is found within about eight
/// of its game's events; a diff that lives for one event is found with probability one in eight. A sampled DIFF still
/// counts and still names its game. An apply with no event (`None`: a belt find, the reconcile) always compares.
pub const COMPARE_SAMPLE_EVERY: i64 = 8;
pub fn compare_sampled(seq: Option<i64>) -> bool {
    seq.is_none_or(|s| s.rem_euclid(COMPARE_SAMPLE_EVERY) == 0)
}

/// D2-ACTOR-B-FOLD-7: the actor HELD the event (202 `held: true`): it is bootstrapping and applies the event at its
/// bootstrap's end, from its own durable storage.
pub fn is_held(answer: &Value) -> bool {
    answer["held"] == true
}

/// THE DEFERRED APPLY (the D2-ACTOR-B lens's MEDIUM-6, Phase A): the consumer's delivery judges in D1 and ACKS; the
/// actor's apply and its compare ([`beside`]) run after the batch, under the queue handler's `ctx.wait_until`
/// ([`run_deferred`]), so the serial consumer's per-message service time is the D1 judge's alone. At most
/// [`DEFERRED_MAX`] an isolate between two batches (past it the delivery applies inline, as before). The actor reads
/// the record as it stands (its closure), so a later apply is the same judgement or a newer one; its compare reads
/// D1's rows at that time, so a D1 judgement landing between them can count a diff the next apply heals (stated).
#[derive(Debug, Clone)]
pub struct Deferred {
    pub identity: String,
    pub games: Vec<String>,
    pub event: EventCtx,
    pub tip: Option<u64>,
    pub era_ms: Option<i64>,
    /// Item 11: the acknowledgement's bound read before the D1 judge (D2-ACTOR-D: the actor's seq check too).
    pub bound: Option<i64>,
}
pub const DEFERRED_MAX: usize = 256;
thread_local! {
    static DEFERRED: RefCell<Vec<Deferred>> = const { RefCell::new(Vec::new()) };
}
/// Queue one Phase A apply for after the batch; `false` past the bound.
pub fn defer(d: Deferred) -> bool {
    DEFERRED.with(|q| {
        let mut q = q.borrow_mut();
        if q.len() >= DEFERRED_MAX {
            return false;
        }
        q.push(d);
        true
    })
}
pub fn take_deferred() -> Vec<Deferred> {
    DEFERRED.with(|q| std::mem::take(&mut *q.borrow_mut()))
}
/// The deferred applies of one batch, each the identity's actor applying beside the D1 judge's rows, then this
/// isolate's bars flushed (the queue class).
pub async fn run_deferred(env: worker::Env, items: Vec<Deferred>) {
    let n = items.len() as u64;
    // The delta lens's NEW-L6: counted at the START (a platform cut of `wait_until` loses the rest uncounted otherwise)
    // and again at the end (`owed_actor_deferred_done_total`).
    if let Ok(db) = crate::routes::D1Sql::of(&env) {
        db.counters(vec![(COUNTERS[13].into(), n)]).await;
    }
    for d in items {
        let Ok(judge) = crate::owed_judge::D1Judge::new(&env, d.tip, d.era_ms) else {
            continue;
        };
        let judge = judge.with_event(d.event);
        beside(&judge, &d.identity, &d.games, d.bound).await;
    }
    if let Ok(db) = crate::routes::D1Sql::of(&env) {
        db.counters(vec![(COUNTERS[16].into(), n)]).await;
        flush_bars(&db, worker::Date::now().as_millis(), false).await;
    }
}

/// PHASE B (`OWED_ACTOR_READ` with the D1 judge on; the D2-ACTOR-B lens's HIGH-3, option (a), Phase C's rule): the
/// read serves the actor's rows, so the marks that withhold a press over a pending downgrade (D2-L1) may be
/// acknowledged only once BOTH judges applied. The actor applies FIRST: its fault is the message's (handed back, the
/// marks standing, the read withholding the game's presses); then the D1 judge, whose batch acknowledges the marks;
/// then the compare. `Ok(None)`: no actor is bound (the D1 judge alone).
pub async fn actor_then_d1(
    db: &impl JudgeDb,
    identity: &str,
    games: &[String],
) -> Result<crate::owed_judge::JudgeReport, String> {
    // The delta lens's NEW-H1 (a): the acknowledgement's bound is read BEFORE the actor reads the record; a mark
    // written while it applies is judged by D1 below but left standing for the actor's next apply.
    let bound = crate::owed_judge::marks_bound(db, identity, games).await?;
    // D2-ACTOR-D: the actor's call first, naming the bound: a delivery the writer's direct call covered is a dupe.
    let answer = match db.actor_deliver_bounded(identity, games, Some(bound)).await {
        Ok(a) => a,
        Err(e) => {
            db.counters(vec![(COUNTERS[2].into(), 1)]).await;
            note_actor_fault(identity, db.event_at_ms());
            db.log(&format!(
                "[owed-actor] FAULT {identity}: {e}; Phase B hands the message back, its marks kept"
            ));
            return Err(e);
        }
    };
    // D2-ACTOR-B-FOLD-7: a HELD answer acknowledges as an applied one (the actor serves no row until the held event
    // is applied: held means not bootstrapped, and its read is the fallback); it has no rows to compare.
    let answer = answer.filter(|a| !is_held(a));
    if let Some(a) = &answer {
        note_answer(a);
    }
    let report =
        crate::owed_judge::judge_subjects_acked_to(db, identity, games, "owed-events", bound)
            .await?;
    if let (Some(a), true) = (&answer, compare_sampled(db.event_seq())) {
        compare(db, identity, games, a, Some(bound)).await;
    }
    Ok(report)
}

/// The games of `?2` (a JSON array) holding a mark of `?1`'s above the acknowledgement bound `?3` and at most `?4`:
/// Phase B's compare (the second delta lens's NEW2-L1); since FIX-964 the reconcile's alone (the delivery's compare
/// reads [`PENDING_MARKS_SQL`]).
///
/// THE ONE RULE (FIX-960, bsv-low #960; the tenth delta lens's NEW10-L1), for both compares: a pending mark explains a
/// difference between two fresh judgements only when ONE of them read its change and the other did not, i.e. its
/// generation is above what the EARLIER reader read and at most what the LATER reader read. A mark neither read
/// (written after the later reader's read) explains nothing: the two judged one record, so their difference is a
/// judge disagreement. A mark both read explains nothing either. The reconcile judges D1 first and the actor second,
/// so `?3` is the bound read before D1's judge and `?4` the actor's own answer, the greatest generation its closure
/// read (`readGeneration`, [`ApplyReport::read_generation`]); an answer without one (an older object build, a
/// closure of more than one round) binds `?4` past every generation, FOLD-16's rule. The delivery compare
/// ([`PENDING_MARKS_SQL`]) runs the other order (the actor, then D1), so its earlier reader's number is the bound and
/// its later reader is the D1 judge, which answers no read generation: its `whileMarks` stay open above (NEW2-L1's
/// accepted tail). FIX-964's `lagMarks` (at or below the bound, still pending, older than the compare) are the
/// delivery's own class: the actor's stored rows predating a backlog, not two fresh reads; the reconcile compares
/// fresh with fresh and has none. Stated: a mark written between the bound's read and D1's judge read is read by
/// both and still held here (the bound is read one round trip before D1's judge), the residual this cut leaves.
pub const HELD_GAMES_SQL: &str = "SELECT DISTINCT gameId FROM owed_dirty WHERE identity=?1 AND gameId IN (SELECT value FROM json_each(?2)) AND generation>?3 AND generation<=?4";

/// FIX-964 (bsv-low #964): the delivery compare's ONE read of `owed_dirty` (the queue's unacknowledged set, lane A),
/// per compared game of `?1`'s (`?2`, a JSON array): `whileMarks` the marks above the acknowledgement bound `?3`
/// (written while the two judged: NEW2-L1, unchanged), `lagMarks` the marks at or below it written before the
/// compare's start `?4` that the delivery did NOT consume (still pending after the D1 judge's acknowledgement: a
/// subject the judge left, a backlog mark the actor's rows predate) and `lagOldestMs` the oldest of them. With no bound
/// (its read faulted) `?3` is bound past every generation, so every pending mark is lag. One search of the UNIQUE
/// key's index on `(identity, gameId)`, at most one row a compared game (a delivery's chunk: 16).
pub const PENDING_MARKS_SQL: &str = "SELECT gameId,SUM(generation>?3) AS whileMarks,SUM(generation<=?3 AND CAST(markedAtMs AS INTEGER)<=?4) AS lagMarks,MIN(CASE WHEN generation<=?3 AND CAST(markedAtMs AS INTEGER)<=?4 THEN CAST(markedAtMs AS INTEGER) END) AS lagOldestMs FROM owed_dirty WHERE identity=?1 AND gameId IN (SELECT value FROM json_each(?2)) GROUP BY gameId";

/// PURE (FIX-964): the compare's held partition from [`PENDING_MARKS_SQL`]'s rows: the games held because a mark was
/// written WHILE the two judged (NEW2-L1), and the games held by LAG (an unapplied mark older than the compare, none
/// written while), each with its oldest lag mark's time. A game with both is the NEW2-L1 class.
pub fn held_partition(rows: &[Value]) -> (BTreeSet<String>, BTreeMap<String, i64>) {
    let n = |v: &Value| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64));
    let mut while_games = BTreeSet::new();
    let mut lag = BTreeMap::new();
    for r in rows {
        let Some(g) = r["gameId"].as_str().map(str::to_ascii_lowercase) else {
            continue;
        };
        if n(&r["whileMarks"]).unwrap_or(0) > 0 {
            while_games.insert(g);
        } else if n(&r["lagMarks"]).unwrap_or(0) > 0 {
            lag.insert(g, n(&r["lagOldestMs"]).unwrap_or(0));
        }
    }
    (while_games, lag)
}

/// The compare of one answer with the D1 judge's rows just written (Phases A and B), counted. The second delta lens's
/// NEW2-L1: in Phase B (`bound`, the acknowledgement's bound read before the actor's call) a game holding a mark
/// above the bound at the compare is NEW-H1's designed state (the D1 judge judged the newer evidence, the actor will
/// at its next apply): its DIFF is counted APART (`owed_actor_compare_held_total`, served
/// `/health.owedActor.heldDiffsTotal`), its line `DIFF (held mark, NEW2-L1)`, never `owed_actor_diffs_total` (the
/// cutover's STOP).
///
/// FIX-964 (bsv-low #964): a game holding ANY unapplied mark older than the compare's start (the queue's backlog: a
/// mark still in `owed_dirty` after the delivery's acknowledgement, [`PENDING_MARKS_SQL`]) is held too, by LAG: the two
/// sides read two instants of the record, so their difference is counted `owed_actor_compare_held_total`, its line
/// `DIFF (held: lag <age> ms)` with the oldest unapplied mark's age, never `owed_actor_diffs_total`. What
/// `owed_actor_diffs_total` counts is a difference on a game with NO pending mark: the same inputs, two outputs.
async fn compare(
    db: &impl JudgeDb,
    identity: &str,
    games: &[String],
    answer: &Value,
    bound: Option<i64>,
) {
    let id = identity.to_ascii_lowercase();
    let start = db.now_ms() as i64;
    let gj = json!(games
        .iter()
        .map(|g| g.to_ascii_lowercase())
        .collect::<Vec<_>>())
    .to_string();
    let stmts = vec![
        Stmt::new(ROWS_SQL, vec![Bind::Text(id.clone())]),
        Stmt::new(
            PENDING_MARKS_SQL,
            vec![
                Bind::Text(id),
                Bind::Text(gj),
                Bind::Int(bound.unwrap_or(i64::MAX)),
                Bind::Int(start),
            ],
        ),
    ];
    let (d1, (held, lag_games)) = match db.run_batch(stmts).await {
        Ok(o) if !o.is_empty() => {
            let held = o
                .get(1)
                .map(|h| held_partition(&h.rows))
                .unwrap_or_default();
            (o[0].rows.clone(), held)
        }
        _ => return,
    };
    let actor: Vec<Value> = answer["rows"].as_array().cloned().unwrap_or_default();
    let diffs = diff_rows(&of_games(&d1, games), &of_games(&actor, games));
    let game_of = |outpoint: &str| -> String {
        d1.iter()
            .chain(&actor)
            .find(|r| r["outpoint"] == outpoint)
            .and_then(|r| r["gameId"].as_str())
            .unwrap_or("?")
            .to_string()
    };
    let (held_diffs, diffs): (Vec<String>, Vec<String>) = diffs
        .into_iter()
        .partition(|o| held.contains(&game_of(o).to_ascii_lowercase()));
    let (lag_diffs, diffs): (Vec<String>, Vec<String>) = diffs
        .into_iter()
        .partition(|o| lag_games.contains_key(&game_of(o).to_ascii_lowercase()));
    let mut deltas = vec![(COUNTERS[0].to_string(), 1)];
    // NEW7-L2: the rows whose verdict agrees and whose judged tip does not (a lag, counted apart, never a STOP)
    let lag = tip_lag_rows(&of_games(&d1, games), &of_games(&actor, games));
    if !lag.is_empty() {
        deltas.push((COUNTERS[28].to_string(), 1));
        db.log(&format!(
            "[owed-actor] TIP LAG (NEW7-L2; not a STOP) {identity} game {}: {} outpoints of {} games, first {}",
            game_of(&lag[0]),
            lag.len(),
            games.len(),
            lag[0]
        ));
    }
    if !diffs.is_empty() {
        deltas.push((COUNTERS[1].to_string(), 1));
        // MEDIUM-8: the line names the identity and the game (the cutover's STOP reads it).
        db.log(&format!(
            "[owed-actor] DIFF {identity} game {}: {} outpoints of {} games, first {}; {}",
            game_of(&diffs[0]),
            diffs.len(),
            games.len(),
            diffs[0],
            diff_detail(&d1, &actor, &diffs)
        ));
    }
    if !held_diffs.is_empty() {
        deltas.push((COUNTERS[17].to_string(), 1));
        db.log(&format!(
            "[owed-actor] DIFF (held mark, NEW2-L1; not a STOP) {identity} game {}: {} outpoints of {} games, first {}; {}",
            game_of(&held_diffs[0]),
            held_diffs.len(),
            games.len(),
            held_diffs[0],
            diff_detail(&d1, &actor, &held_diffs)
        ));
    }
    if !lag_diffs.is_empty() {
        let oldest = lag_diffs
            .iter()
            .filter_map(|o| lag_games.get(&game_of(o).to_ascii_lowercase()))
            .min()
            .copied()
            .unwrap_or(start);
        if held_diffs.is_empty() {
            deltas.push((COUNTERS[17].to_string(), 1));
        }
        db.log(&format!(
            "[owed-actor] DIFF (held: lag {} ms; FIX-964, not a STOP) {identity} game {}: {} outpoints of {} games, first {}; {}",
            start.saturating_sub(oldest).max(0),
            game_of(&lag_diffs[0]),
            lag_diffs.len(),
            games.len(),
            lag_diffs[0],
            diff_detail(&d1, &actor, &lag_diffs)
        ));
    }
    db.counters(deltas).await;
}

/// The bars of one answer, in this isolate: the apply's time, and (an answer naming its event's write time) the rows'
/// visibility after the write and, for a `mined` word's event, the collect-after-mined bar.
pub fn note_answer(answer: &Value) {
    note_answer_as("queue", answer);
}
pub fn note_answer_into(bars: &mut Bars, answer: &Value) {
    if answer["applied"] == false {
        return;
    }
    if let Some(ms) = answer["applyMs"].as_u64() {
        bars.apply.note(ms);
    }
    // NEW6-L1: an answer naming its samples (`visibleMs`) is sampled by them
    if let Some(each) = answer["visibleMs"].as_array() {
        each.iter()
            .filter_map(Value::as_u64)
            .for_each(|ms| bars.visible.note(ms));
    }
    if let (true, Some(at), Some(judged)) = (
        samples_visible(answer),
        answer["atMs"].as_u64(),
        answer["judgedAtMs"].as_u64(),
    ) {
        let ms = judged.saturating_sub(at);
        if answer["visibleMs"].is_null() {
            bars.visible.note(ms);
        }
        if answer["mined"] == true {
            bars.collect_after_mined.note(ms);
        }
    }
}

/// PHASE C: the actor is the judge; the marks of the games the actor APPLIED by THIS call (`appliedGames`; the
/// D2-ACTOR-B lens's HIGH-1) are acknowledged after it applied, up to the generation read BEFORE the call (the delta
/// lens's NEW-H1 (b): a redelivery is applied again, never skipped, so no mark newer than the apply's record read is
/// acknowledged; a mark written meanwhile keeps its newer generation). A game the answer does not name keeps its
/// marks.
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
    // D2-ACTOR-D: the bound read before the call names the seq check (a covered delivery is a dupe, its marks
    // acknowledged up to the same bound).
    let answer = match db.actor_deliver_bounded(&id, games, Some(seen)).await {
        Ok(Some(a)) => a,
        Ok(None) => return Err("the D1 judge is off and no owed actor is bound".into()),
        Err(e) => {
            note_actor_fault(&id, db.event_at_ms());
            return Err(e);
        }
    };
    note_answer(&answer);
    // D2-ACTOR-B-FOLD-7 (the ack rule of a held event): the actor HELD the games (`heldGames`) in its own durable
    // storage (the platform's output gate: the 202 leaves only after the row is written) and applies them at its
    // bootstrap's end, reading the record as it stands then, after every mark read here; until then it is not
    // bootstrapped and no read serves its rows (the fallback, never a press). So the held games' marks are
    // acknowledged up to the generation read BEFORE the call, as an applied answer's are (NEW-H1 (b)).
    let named = if is_held(&answer) {
        "heldGames"
    } else {
        "appliedGames"
    };
    let applied: Vec<String> = answer[named]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|g| g.as_str().map(str::to_ascii_lowercase))
                .filter(|g| games.iter().any(|x| x.eq_ignore_ascii_case(g)))
                .collect()
        })
        .unwrap_or_default();
    if applied.is_empty() {
        return Err(format!(
            "owed actor: {id} applied none of the {} games",
            games.len()
        ));
    }
    db.run_batch(phase_c_ack_statements(&id, &applied, seen))
        .await?;
    Ok(crate::owed_judge::JudgeReport {
        subjects: applied.len(),
        rows: answer["rows"].as_array().map_or(0, Vec::len),
        ..Default::default()
    })
}

/// PHASE C's acknowledgement of `games`' marks up to the generation `seen` read BEFORE the actor's call (the
/// consumer's [`as_the_judge`] and, FIX-956 item 3, a direct apply that landed: one rule, one batch).
pub fn phase_c_ack_statements(identity: &str, games: &[String], seen: i64) -> Vec<Stmt> {
    vec![
        Stmt::new(
            crate::owed_judge::ACK_DIRTY_SQL,
            vec![
                Bind::Text(identity.to_ascii_lowercase()),
                Bind::Text(json!(games).to_string()),
                Bind::Int(seen),
                Bind::Text("[]".into()),
            ],
        ),
        // NEW-M3: D1's `owed_rows` are no longer judged; the D1 read serves no press until `owed-v3` is stamped.
        Stmt::new(PHASE_C_MARK_SQL, vec![]),
        // The second delta lens's NEW2-M1: BOTH restart statements, the stamp AND the pass's cursor row, so the next
        // rollback's pass starts from the first identity and re-judges every one before it stamps (a stamped run's
        // cursor, left at the last identity, stamped again having judged nothing).
        Stmt::new(OWED_V3_RESTART_SQL[0], vec![]),
        Stmt::new(OWED_V3_RESTART_SQL[1], vec![]),
    ]
}

/// THE `owed-v3` RE-JUDGE (the D2-ACTOR-B lens's MEDIUM-9; `CUTOVER-D2-ACTOR-2026-10-28.md` section 4, Phase C's
/// rollback): with the D1 judge back on, every identity's `owed_rows` re-judged by the D1 judge from D1's OWN
/// evidence (never a copy of the actors' rows: the evidence is the record), resumable, bounded a call, then stamped
/// `owed-v3` in `view_derivation_state`. The identities are the reconcile's union (`owed_state`, the party, hop
/// marker and released evidence identities, [`RECONCILE_NEXT_SQL`]), each identity's games by game id past the open
/// one ([`crate::owed_judge::IDENTITY_GAMES_BY_ID_SQL`]), 16 a judge call (`judge_subjects`, which acknowledges the
/// marks it judged). Its state is one row of the belt's ledger (`rollback:owed-v3`: the last identity done and the
/// open one with its last game). Counted `owed_v3_identities_total`, `owed_v3_rows_total`.
/// The delta lens's NEW-M3: every Phase C acknowledgement stamps `owed-actor-judged` and deletes the `owed-v3` stamp
/// in its own batch, so the D1 read (`owed_read::PENDING_SQL`'s two probes) withholds every press from D1's
/// `owed_rows` (the rows of the flip) until the rollback's re-judge stamps `owed-v3` again. The second delta lens's
/// NEW2-M1: that batch also deletes the pass's cursor row (`rollback:owed-v3`), so a pass after a Phase C epoch always
/// starts from the first identity.
pub const PHASE_C_MARK_SQL: &str =
    "INSERT OR IGNORE INTO view_derivation_state(name) VALUES ('owed-actor-judged')";
pub const OWED_V3_STAMP: &str = "owed-v3";
pub const OWED_V3_STATE: &str = "rollback:owed-v3";
pub const OWED_V3_STAMP_SQL: &str =
    "SELECT 1 AS stamped FROM view_derivation_state WHERE name='owed-v3'";
pub const OWED_V3_STATE_SQL: &str =
    "SELECT cursor FROM owed_belt_state WHERE name='rollback:owed-v3'";
pub const OWED_V3_STAMP_WRITE_SQL: &str =
    "INSERT OR IGNORE INTO view_derivation_state(name) VALUES ('owed-v3')";
/// The lever's `{"restart": true}`: the stamp and the state cleared by the lever itself (and, NEW2-M1, by every Phase C
/// acknowledgement), never by hand.
pub const OWED_V3_RESTART_SQL: [&str; 2] = [
    "DELETE FROM view_derivation_state WHERE name='owed-v3'",
    "DELETE FROM owed_belt_state WHERE name='rollback:owed-v3'",
];
/// The games one lever call judges, at most.
pub const OWED_V3_GAMES_PER_CALL: usize = 256;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct V3Report {
    pub stamped: bool,
    pub identities: usize,
    pub games: usize,
    pub rows: usize,
    /// The state after this call: the last identity done and the open one.
    pub after: String,
    pub open: Option<(String, String)>,
}

/// One bounded call of the `owed-v3` re-judge (`games_budget` the games it may judge).
pub async fn owed_v3_pass(
    db: &impl JudgeDb,
    now: i64,
    games_budget: usize,
) -> Result<V3Report, String> {
    let first = |out: Vec<StmtOut>| out.into_iter().next().map(|o| o.rows).unwrap_or_default();
    let mut report = V3Report::default();
    let held = db
        .run_batch(vec![
            Stmt::new(OWED_V3_STAMP_SQL, vec![]),
            Stmt::new(OWED_V3_STATE_SQL, vec![]),
        ])
        .await?;
    if held.first().is_some_and(|o| !o.rows.is_empty()) {
        report.stamped = true;
        return Ok(report);
    }
    let state: Value = held
        .get(1)
        .and_then(|o| o.rows.first())
        .and_then(|r| r["cursor"].as_str())
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_else(|| json!({}));
    report.after = state["after"].as_str().unwrap_or_default().to_string();
    report.open = state["open"].as_array().and_then(|a| {
        Some((
            a.first()?.as_str()?.to_string(),
            a.get(1)?.as_str()?.to_string(),
        ))
    });
    let save = |r: &V3Report| {
        Stmt::new(
            crate::owed_belt::STATE_SQL,
            vec![
                Bind::Text(OWED_V3_STATE.into()),
                Bind::Text(
                    json!({"after": r.after, "open": r.open.as_ref().map(|(i, g)| json!([i, g]))})
                        .to_string(),
                ),
                Bind::Int(now),
            ],
        )
    };
    while report.games < games_budget {
        let Some((identity, last)) = report.open.clone() else {
            let next = first(
                db.run_batch(vec![Stmt::new(
                    RECONCILE_NEXT_SQL,
                    vec![Bind::Text(report.after.clone())],
                )])
                .await?,
            );
            match next.first().and_then(|r| r["identity"].as_str()) {
                Some(id) => report.open = Some((id.to_string(), String::new())),
                None => {
                    db.run_batch(vec![
                        save(&report),
                        Stmt::new(OWED_V3_STAMP_WRITE_SQL, vec![]),
                    ])
                    .await?;
                    report.stamped = true;
                    break;
                }
            }
            continue;
        };
        let page = (games_budget - report.games)
            .min(crate::owed_judge::OWED_JUDGE_SUBJECTS_PER_INVOCATION);
        let games: Vec<String> = first(
            db.run_batch(vec![Stmt::new(
                crate::owed_judge::IDENTITY_GAMES_BY_ID_SQL,
                vec![
                    Bind::Text(identity.clone()),
                    Bind::Text(last),
                    Bind::Int(page as i64),
                ],
            )])
            .await?,
        )
        .iter()
        .filter_map(|r| r["gameId"].as_str().map(str::to_string))
        .collect();
        if games.is_empty() {
            report.identities += 1;
            report.after = identity;
            report.open = None;
            db.run_batch(vec![save(&report)]).await?;
            continue;
        }
        // The delta lens's NEW-H1 and NEW-M3: under the actor's read (the rollback's first render, the D1 judge back
        // on with `OWED_ACTOR_READ` still on) the pass re-judges D1's rows and acknowledges NOTHING: the marks stand
        // for the consumer, which applies the actor first and acknowledges under its bound.
        let judged = if db.actor_reads() {
            crate::owed_judge::judge_subjects_acked_to(db, &identity, &games, "owed-v3", 0).await?
        } else {
            crate::owed_judge::judge_subjects(db, &identity, &games, "owed-v3").await?
        };
        report.games += games.len();
        report.rows += judged.rows;
        report.open = Some((identity, games.last().cloned().unwrap_or_default()));
        db.run_batch(vec![save(&report)]).await?;
    }
    db.counters(vec![
        (COUNTERS[14].into(), report.identities as u64),
        (COUNTERS[15].into(), report.rows as u64),
    ])
    .await;
    Ok(report)
}

/// `GET`/`POST /internal/owed-rejudge` (bearer `INTERNAL_TOKEN`; D2-ACTOR-B-FOLD-15, the captain's pace lever): `POST
/// {"gamesPerTick": N}` writes the pace row (`0` pauses; past [`D1_REJUDGE_GAMES_PER_TICK_MAX`] a 400) and stamps it
/// into a standing stamp; `{"gamesPerTick": null}` clears the row (the render's var applies again); `GET` answers the
/// row, the pace in force, the var, the ceiling and the stamp as `d1Rejudge` serves it.
pub async fn rejudge_pace_lever(
    mut req: worker::Request,
    env: &worker::Env,
) -> worker::Result<worker::Response> {
    if !crate::internal_events::internal_bearer_ok(&req, env) {
        return worker::Response::error("unauthorized", 401);
    }
    let db = crate::routes::D1Sql::of(env)?;
    let var = env
        .var("OWED_D1_REJUDGE_GAMES_PER_TICK")
        .ok()
        .map(|v| v.to_string());
    let now = worker::Date::now().as_millis() as i64;
    if req.method() == worker::Method::Post {
        let body: Value = req.json().await.unwrap_or(Value::Null);
        let stmts = match pace_lever_plan(&body, now) {
            Ok(stmts) => stmts,
            Err(e) => return worker::Response::error(e, 400),
        };
        if let Err(e) = db.run_batch(stmts).await {
            return worker::Response::error(format!("the pace's write faulted: {e}"), 503);
        }
    }
    let out = match db
        .run_batch(vec![
            Stmt::new(D1_REJUDGE_PACE_SQL, vec![]),
            Stmt::new(D1_REJUDGE_STATE_SQL, vec![]),
        ])
        .await
    {
        Ok(o) => o,
        Err(e) => return worker::Response::error(format!("the pace's read faulted: {e}"), 503),
    };
    let row = out.first().and_then(|o| o.rows.first()).and_then(|r| {
        r["cursor"]
            .as_str()
            .and_then(|c| serde_json::from_str::<Value>(c).ok())
    });
    let stamp = rejudge_state_of(out.get(1).and_then(|o| o.rows.first()));
    worker::Response::from_json(&json!({
        "row": row,
        "gamesPerTick": rejudge_pace(row.as_ref(), var.as_deref()),
        "var": var,
        "default": D1_REJUDGE_GAMES_PER_TICK,
        "max": D1_REJUDGE_GAMES_PER_TICK_MAX,
        "d1Rejudge": rejudge_served(stamp.as_ref()),
    }))
}

/// PURE: the lever's statements for a `POST` body (`Err`: the 400's words).
pub fn pace_lever_plan(body: &Value, now: i64) -> Result<Vec<Stmt>, String> {
    let n = match body.get("gamesPerTick") {
        Some(Value::Null) => None,
        Some(v) => match v.as_u64() {
            Some(n) if n as usize <= D1_REJUDGE_GAMES_PER_TICK_MAX => Some(n),
            _ => {
                return Err(format!(
                    "gamesPerTick must be an integer from 0 to {D1_REJUDGE_GAMES_PER_TICK_MAX}, or null"
                ))
            }
        },
        None => return Err("the body names gamesPerTick".into()),
    };
    let mut stmts = vec![match n {
        Some(n) => Stmt::new(
            crate::owed_belt::STATE_SQL,
            vec![
                Bind::Text(D1_REJUDGE_PACE_NAME.into()),
                Bind::Text(json!({ "gamesPerTick": n, "setAtMs": now }).to_string()),
                Bind::Int(now),
            ],
        ),
        None => Stmt::new(D1_REJUDGE_PACE_CLEAR_SQL, vec![]),
    }];
    if let Some(n) = n {
        stmts.push(Stmt::new(
            D1_REJUDGE_STAMP_PACE_SQL,
            vec![
                Bind::Text(if n == 0 { "true" } else { "false" }.into()),
                Bind::Int(n as i64),
            ],
        ));
    }
    stmts.push(health_snapshot_stmt(now.max(0) as u64, true));
    Ok(stmts)
}

/// `POST /internal/owed-v3-pass` (bearer `INTERNAL_TOKEN`), body empty, `{}` or `{"restart": true}`: one bounded call
/// of [`owed_v3_pass`], answered with its report. Refused (409) while `OWED_D1_JUDGE` is off: the rollback turns the
/// D1 judge back on FIRST (the render), then calls this until `stamped`, then flips the read back.
pub async fn owed_v3_lever(
    mut req: worker::Request,
    env: &worker::Env,
) -> worker::Result<worker::Response> {
    if !crate::internal_events::internal_bearer_ok(&req, env) {
        return worker::Response::error("unauthorized", 401);
    }
    if !d1_judge_on(env.var(D1_JUDGE_FLAG).ok().map(|v| v.to_string())) {
        return worker::Response::error(
            "the D1 judge is off (OWED_D1_JUDGE=false): turn it on before the owed-v3 re-judge",
            409,
        );
    }
    let text = req.text().await.unwrap_or_default();
    let body: Value = if text.trim().is_empty() {
        json!({})
    } else {
        match serde_json::from_str(&text) {
            Ok(b @ Value::Object(_)) => b,
            _ => {
                return worker::Response::error("the body is empty, {} or {\"restart\": true}", 400)
            }
        }
    };
    let db = crate::routes::D1Sql::of(env)?;
    if body["restart"] == true {
        db.run_batch(
            OWED_V3_RESTART_SQL
                .iter()
                .map(|s| Stmt::new(*s, vec![]))
                .collect(),
        )
        .await
        .map_err(worker::Error::RustError)?;
    }
    let tip = crate::owed_events::tip_memoised(env, false).await;
    let judge =
        crate::owed_judge::D1Judge::new(env, tip, crate::routes::written_off_before_ms_env(env))?;
    let now = worker::Date::now().as_millis() as i64;
    match owed_v3_pass(&judge, now, OWED_V3_GAMES_PER_CALL).await {
        Ok(r) => worker::Response::from_json(&json!({
            "stamped": r.stamped,
            "identities": r.identities,
            "games": r.games,
            "rows": r.rows,
            "after": r.after,
            "open": r.open.map(|(i, g)| json!({"identity": i, "game": g})),
        })),
        Err(e) => worker::Response::error(format!("owed-v3: {e}"), 503),
    }
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
