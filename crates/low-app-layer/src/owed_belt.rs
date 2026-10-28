//! bsv-low #893 section 5: THE BELT. The one place the owed list asks the chain explorers, BREAK GLASS ONLY: a slow
//! background look for a spend that never passed through our stack, never a read's dependency.
//!
//! A Durable Object alarm (the `BOARD_VIEW` actor named [`OWED_BELT_ACTOR`]; the deferred walk's alarm is gone) runs
//! one [`tick`] every [`OWED_BELT_TICK_MS`], and the scheduled pass re-arms it ([`arm`], the watchdog: a lost alarm
//! costs at most one 15 minute cron). A tick reads the [`OWED_BELT_OUTPOINTS_PER_TICK`] rows whose `beltDueAtMs` is
//! past: first the courier-only sweeps a block dated (`idx_owed_rows_sweep`, the press the read withholds until their
//! look; the second delta lens's D2-M1), and, in at least [`OWED_BELT_SECOND_CLASS_RESERVED`] places whenever it holds a
//! due row (the third delta lens's D3-M1), round-robin by identity over `idx_owed_rows_belt_turn` (up to 512 dated
//! identities walked past a cursor, turns only to those holding a row due now, one row of each before a second of any,
//! the D2-OWED lens's HIGH-3, `due_candidates`); the judge sets that column for exactly the rows that owe the
//! chain a question it can answer, `owed_judge::row_statements`) and asks each through the existing ladders behind ONE gate
//! ([`Gate`]): at most [`OWED_BELT_CALLS_PER_OUTPOINT`] courier calls an outpoint and [`OWED_BELT_CALLS_PER_TICK`] a
//! tick, the three-fault circuit per rung per tick, a rung's 429 ending that rung's turn (never the tick). Every find
//! is ONE batch (`owed_judge::judge_with_write`): the probe memo with the tip it was taken at, the spender's bytes
//! (content-addressed, DO NOTHING), a `proofmined:` or `homeproof:` latch when it checked, then the judge's rows for
//! that subject, judged over the same facts supplied in memory. A fault rolls all of it back. A candidate no rung
//! answered keeps its row and facts and is re-armed once, waiting as long again as it has since its judgement
//! ([`unanswered_period`]: at least [`OWED_BELT_UNANSWERED_RETRY_MS`]); a find that changed nothing the row says
//! doubles the row's period, a moving tip included; both to [`OWED_BELT_BACKOFF_MAX_MS`] (the delta lens's D-L2).
use crate::hops_view::{ChainSpendProbe, ProbeMemo};
use crate::owed::{self, HomeSpendWord, HopChainWord, StoredSpenderBytes};
use crate::owed_judge::{self, JudgeDb, Subject};
use crate::pot_hook::{Bind, Stmt};
use serde_json::Value;
use std::cell::RefCell;

pub const OWED_BELT_TICK_MS: i64 = 120_000;
/// A drain gets the same separate 17 s working wall on the alarm and scheduled pass. The alarm drains first,
/// so the courier tick cannot consume the dirty drain's reservation.
pub const OWED_DRAIN_WALL_MS: u64 = 20_000;
pub const OWED_DRAIN_RESERVE_MS: u64 = 3_000;
pub const OWED_BELT_OUTPOINTS_PER_TICK: usize = 8;
pub const OWED_BELT_CALLS_PER_TICK: usize = 24;
pub const OWED_BELT_CALLS_PER_OUTPOINT: usize = 3;
/// A candidate no rung answered (every rung faulted, rate-limited or closed) is looked at again at least this much
/// later; each further unanswered look doubles it ([`unanswered_period`]).
pub const OWED_BELT_UNANSWERED_RETRY_MS: i64 = 300_000;
/// A look that changed nothing (the D2-OWED lens's HIGH-3) doubles its row's period, to this ceiling (6 h). The
/// judge's own dating (a source write, a tip) starts it again at the family's base period.
pub const OWED_BELT_BACKOFF_MAX_MS: i64 = 6 * 3_600_000;
/// The rung's circuit: this many faults in one tick close it for the rest of the tick (`tracker_host::pacing`'s 3).
pub const OWED_BELT_RUNG_FAULTS: u8 = 3;
/// The singleton actor's name in `BOARD_VIEW` (fleet-wide: one belt whatever the number of readers or isolates).
pub const OWED_BELT_ACTOR: &str = "owed-belt:v1";
/// The `owed_belt_state` row the tick writes: its cursor (the last outpoint asked) and its stamp.
pub const OWED_BELT_STATE_NAME: &str = "belt";
/// The rungs, by courier host. `other` is any host the ladders reach that is none of the three.
pub const RUNGS: [&str; 4] = ["woc", "bitails", "bananablocks", "other"];

/// The identities the tick walks (the D2-OWED lens's HIGH-3): up to ?2 distinct identities holding a dated row,
/// ascending past the cursor ?1, one covering-index search each, each with its earliest date (`due`, one MIN seek on
/// the same index: the second delta lens's D2-M1 gives turns only to an identity holding a row due now); the tick wraps
/// to the start for the rest.
pub const BELT_IDENTITIES_SQL: &str = "WITH RECURSIVE walk(identity,n) AS (SELECT (SELECT MIN(identity) FROM owed_rows INDEXED BY idx_owed_rows_belt_turn WHERE beltDueAtMs IS NOT NULL AND identity>?1),1 UNION ALL SELECT (SELECT MIN(identity) FROM owed_rows INDEXED BY idx_owed_rows_belt_turn WHERE beltDueAtMs IS NOT NULL AND identity>walk.identity),n+1 FROM walk WHERE walk.identity IS NOT NULL AND n<?2) SELECT identity,(SELECT MIN(beltDueAtMs) FROM owed_rows INDEXED BY idx_owed_rows_belt_turn WHERE identity=walk.identity AND beltDueAtMs IS NOT NULL) AS due FROM walk WHERE identity IS NOT NULL";
/// The first class (the second delta lens's D2-M1): the courier-only sweeps a block dated and their look not yet made
/// (claimable `hop-stranded`, index status not `unspent`, judged below the present tip ?1, due by ?2), over
/// `idx_owed_rows_sweep` (the tip's own dating index: its WHERE is this statement's, so it reads only that class), at
/// most ?3 rows a tick. The due set is a SEEK (the third delta lens's D3-N3): the walk steps up the distinct judged
/// tips below ?1, oldest first as the index order the first build read (an unlooked row judged at an older tip before
/// a looked one), one MIN seek each, at most ?3 of them, and inside each takes the rows due by ?2 on the key's
/// `(probeTip, beltDueAtMs)` range, so a row of the class not due is never read. A row judged at a tip past the ?3
/// oldest distinct tips below the present is left to the second class (the walk of `idx_owed_rows_belt_turn`) on that
/// tick.
pub const BELT_SWEEPS_DUE_SQL: &str = "WITH RECURSIVE walk(t,n) AS (SELECT (SELECT MIN(probeTip) FROM owed_rows INDEXED BY idx_owed_rows_sweep WHERE family='hop-stranded' AND claimable=1 AND json_extract(facts,'$.status') IS NOT 'unspent' AND probeTip<?1),1 UNION ALL SELECT (SELECT MIN(probeTip) FROM owed_rows INDEXED BY idx_owed_rows_sweep WHERE family='hop-stranded' AND claimable=1 AND json_extract(facts,'$.status') IS NOT 'unspent' AND probeTip>walk.t AND probeTip<?1),n+1 FROM walk WHERE walk.t IS NOT NULL AND n<?3) SELECT r.identity,r.outpoint,r.beltDueAtMs FROM walk CROSS JOIN owed_rows r INDEXED BY idx_owed_rows_sweep WHERE walk.t IS NOT NULL AND r.family='hop-stranded' AND r.claimable=1 AND json_extract(r.facts,'$.status') IS NOT 'unspent' AND r.probeTip=walk.t AND r.beltDueAtMs<=?2 LIMIT ?3";
/// The places of a tick reserved for the second class (the third delta lens's D3-M1) whenever it holds a due row.
pub const OWED_BELT_SECOND_CLASS_RESERVED: usize = 2;
/// The first class's rows read a tick at most.
pub const OWED_BELT_SWEEP_ROWS_READ: usize = 256;
/// One walked identity's oldest due rows (at most a tick's worth).
pub const BELT_DUE_SQL: &str = "SELECT outpoint,beltDueAtMs FROM owed_rows INDEXED BY idx_owed_rows_belt_turn WHERE identity=?1 AND beltDueAtMs<=?2 ORDER BY beltDueAtMs,outpoint LIMIT ?3";
/// A chosen row, by its primary key.
pub const CANDIDATES_SQL: &str = "SELECT identity,outpoint,gameId,family,facts,reason,probeTip,beltDueAtMs,judgedAtMs,COALESCE(claimable, CASE json_type(facts,'$.claimable') WHEN 'true' THEN 1 WHEN 'false' THEN 0 END) AS claimable FROM owed_rows WHERE identity=?1 AND outpoint=?2";
/// The due identities one tick gives turns to at most. The round-robin's bound (the delta lens's D-L1; the first build claimed `ceil(N /
/// 64)` ticks, false at 8 looks a tick): the cursor passes only identities given a turn or holding no due row, so an
/// identity holding a due row is given its turn within `ceil(D / 8)` ticks of the walk's window reaching it, `D` the
/// identities holding a due row ahead of it; the window reaches [`OWED_BELT_WALKED_PER_TICK`] dated identities past the
/// cursor a tick, of which only those holding a row due now take turns (D2-M1).
pub const OWED_BELT_IDENTITIES_PER_TICK: usize = 64;
/// The dated identities one tick walks at most (two index seeks each): S identities dated and not due delay the second
/// class by `ceil(S / 512)` ticks at most; the first class (a block's courier-only sweeps) is read before the walk.
pub const OWED_BELT_WALKED_PER_TICK: usize = 512;
/// The walk's cursor row (the last identity a tick walked).
pub const OWED_BELT_WALK_STATE_NAME: &str = "belt:identities";
/// The row as the find's judge left it, for the backoff's comparison.
pub const AFTER_FIND_SQL: &str = "SELECT family,facts,reason,probeTip,beltDueAtMs FROM owed_rows WHERE identity=?1 AND outpoint=?2";
/// The backoff: a dated row whose look changed nothing.
pub const BACKOFF_SQL: &str = "UPDATE owed_rows SET beltDueAtMs=?3 WHERE identity=?1 AND outpoint=?2 AND beltDueAtMs IS NOT NULL";
/// A candidate no rung answered: its row and facts untouched, its next look dated once (never pushed further by a
/// second fault in the same tick: the statement sets, it does not add).
pub const REARM_SQL: &str = "UPDATE owed_rows SET beltDueAtMs=?3 WHERE identity=?1 AND outpoint=?2 AND beltDueAtMs IS NOT NULL";
pub const STATE_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES (?1,?2,?3) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// The backlog gauge: SET (not added), counted over the partial index.
pub const DUE_GAUGE_SQL: &str = "INSERT INTO ops_counters(name,value) SELECT 'owed_belt_due_total',COUNT(*) FROM owed_rows INDEXED BY idx_owed_rows_belt WHERE beltDueAtMs<=?1 ON CONFLICT(name) DO UPDATE SET value=excluded.value";

/// bsv-low #912 (lane D2-ACTOR-B, HIGH-3): PHASE C's asks. With the D1 judge off (`OWED_D1_JUDGE=false`) the D1
/// `owed_rows` are no longer dated, so each identity's actor PUBLISHES a `belt-ask` per dated row of its own
/// (`owed_actor::publish_belt_asks`): one row of the belt's ledger, `belt-ask:<identity> <outpoint>`, its `cursor` the
/// ask `{identity, outpoint, question, dueAtMs}` (the question: the row as the actor judged it, what the belt reads off
/// a candidate), its `tickedAtMs` the due time. The belt singleton STAYS the only thing that asks a courier: it reads
/// the asks at its cadence through its one gate, writes the find's facts to the record and hands the find back to the
/// actor as a `belt-find` event (the actor's closure re-reads them and re-publishes its asks).
pub const BELT_ASK_PREFIX: &str = "belt-ask:";
/// The asks' range ends before `belt-ask;` (`;` follows `:`).
pub const BELT_ASK_END: &str = "belt-ask;";
/// The asks one tick reads past its cursor (a range of the ledger's key), at most.
pub const OWED_BELT_ASKS_PAGE: usize = 256;
/// The asks' rotating cursor.
pub const OWED_BELT_ASKS_STATE_NAME: &str = "belt:asks";
pub const ASKS_PAGE_SQL: &str = "SELECT name,cursor,tickedAtMs FROM owed_belt_state WHERE name>=?1 AND name<'belt-ask;' ORDER BY name LIMIT ?2";
/// An ask's next look (the backoff, an unanswered look's re-arm): only its due time moves.
pub const ASK_DUE_SQL: &str = "UPDATE owed_belt_state SET tickedAtMs=?2 WHERE name=?1";
pub const ASKS_DUE_GAUGE_SQL: &str = "INSERT INTO ops_counters(name,value) SELECT 'owed_belt_due_total',COUNT(*) FROM owed_belt_state WHERE name>='belt-ask:' AND name<'belt-ask;' AND tickedAtMs<=?1 ON CONFLICT(name) DO UPDATE SET value=excluded.value";
/// The ledger name of one ask.
pub fn ask_name(identity: &str, outpoint: &str) -> String {
    format!("{BELT_ASK_PREFIX}{identity} {outpoint}")
}
/// The question an actor publishes for one of its dated rows (`owed_actor::ASK_ROWS_SQL`'s row): the row as the belt
/// reads a candidate, and the ask the belt will make of it ([`ask_of`]).
pub fn ask_of_row(identity: &str, row: &Value) -> Option<(String, Value, i64)> {
    let outpoint = row["outpoint"].as_str()?.to_owned();
    let due = row["beltDueAtMs"].as_i64()?;
    let facts: Value = row["facts"]
        .as_str()
        .and_then(|f| serde_json::from_str(f).ok())
        .unwrap_or(Value::Null);
    let c = candidate_of(&serde_json::json!({
        "identity": identity, "outpoint": outpoint, "gameId": row["gameId"], "family": row["family"],
        "facts": row["facts"], "reason": row["reason"], "claimable": row["claimable"],
        "probeTip": row["probeTip"], "beltDueAtMs": due, "judgedAtMs": row["judgedAtMs"],
    }));
    let ask = match ask_of(&c)? {
        Ask::Outpoint { .. } => "outpoint",
        Ask::Spender { .. } => "spender",
        Ask::Home => "home",
    };
    let body = serde_json::json!({
        "identity": identity,
        "outpoint": outpoint,
        "question": {
            "ask": ask, "gameId": row["gameId"], "family": row["family"], "facts": facts,
            "reason": row["reason"], "claimable": row["claimable"], "probeTip": row["probeTip"],
            "judgedAtMs": row["judgedAtMs"],
        },
        "dueAtMs": due,
    });
    Some((ask_name(identity, &outpoint), body, due))
}

pub const COUNTER_TICKS: &str = "owed_belt_ticks_total";
pub const COUNTER_ASKED: &str = "owed_belt_asked_total";
pub const COUNTER_FOUND: &str = "owed_belt_found_total";
pub const COUNTER_DUE: &str = "owed_belt_due_total";
pub fn fault_counter(rung: usize) -> String {
    format!(
        "owed_belt_faults_{}_total",
        RUNGS[rung.min(RUNGS.len() - 1)]
    )
}
/// Every durable name the belt writes (the overlay seeds and serves them on `/health/invariants.owedBelt`).
pub fn counter_names() -> Vec<String> {
    let mut v = vec![
        COUNTER_TICKS.to_owned(),
        COUNTER_ASKED.to_owned(),
        COUNTER_FOUND.to_owned(),
        COUNTER_DUE.to_owned(),
    ];
    v.extend((0..RUNGS.len()).map(fault_counter));
    v
}

/// The courier port: `provider_get`'s `(status, body)`, `None` on a transport fault.
#[allow(async_fn_in_trait)]
pub trait Courier {
    async fn get(&self, url: String) -> Option<(u16, Vec<u8>)>;
}

pub fn rung_of(url: &str) -> usize {
    if url.contains("whatsonchain.com") {
        0
    } else if url.contains("bitails.io") {
        1
    } else if url.contains("bananablocks.com") {
        2
    } else {
        3
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct GateState {
    pub tick_calls: usize,
    pub outpoint_calls: usize,
    pub faults: [u8; 4],
    pub fault_counts: [u64; 4],
}

/// The tick's one door to the couriers. A refused ask makes no call and answers `None`, which every ladder reads as
/// that rung's fault and falls through to the next rung.
pub struct Gate<'a, C> {
    courier: &'a C,
    pub state: RefCell<GateState>,
}
impl<'a, C: Courier> Gate<'a, C> {
    pub fn new(courier: &'a C) -> Self {
        Self {
            courier,
            state: RefCell::new(GateState::default()),
        }
    }
    fn begin_outpoint(&self) {
        self.state.borrow_mut().outpoint_calls = 0;
    }
    pub fn spent(&self) -> bool {
        self.state.borrow().tick_calls >= OWED_BELT_CALLS_PER_TICK
    }
    pub async fn get(&self, url: String) -> Option<(u16, Vec<u8>)> {
        let rung = rung_of(&url);
        {
            let mut s = self.state.borrow_mut();
            if s.tick_calls >= OWED_BELT_CALLS_PER_TICK
                || s.outpoint_calls >= OWED_BELT_CALLS_PER_OUTPOINT
                || s.faults[rung] >= OWED_BELT_RUNG_FAULTS
            {
                return None;
            }
            s.tick_calls += 1;
            s.outpoint_calls += 1;
        }
        let got = self.courier.get(url).await;
        let mut s = self.state.borrow_mut();
        match &got {
            // a 429 ends this rung's turn for the tick, never the tick
            Some((429, _)) => {
                s.faults[rung] = OWED_BELT_RUNG_FAULTS;
                s.fault_counts[rung] += 1;
            }
            None => {
                s.faults[rung] = s.faults[rung].saturating_add(1);
                s.fault_counts[rung] += 1;
            }
            Some((status, _)) if *status >= 500 => {
                s.faults[rung] = s.faults[rung].saturating_add(1);
                s.fault_counts[rung] += 1;
            }
            _ => {}
        }
        got
    }
}

/// One candidate row, as [`CANDIDATES_SQL`] reads it.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub identity: String,
    pub outpoint: String,
    pub game_id: String,
    pub family: String,
    pub facts: Value,
    /// The stored `claimable` column, or the row's `facts.claimable` where an old worker's row left it NULL during the
    /// deploy window (risk 8.4): the candidate never reads a judgement from a column the old worker could not write.
    pub claimable: Option<bool>,
    /// The row as the tick read it, for the backoff's comparison and its period.
    pub reason: Option<String>,
    pub probe_tip: Option<i64>,
    pub due_at_ms: Option<i64>,
    pub judged_at_ms: Option<i64>,
}

/// What the row owes the chain (section 5's list), read off the row the judge stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ask {
    /// A hop or pot outpoint's spend (`hop-stranded`, `in-progress`, and any other dated row).
    Outpoint { txid: String, vout: u32 },
    /// An `unbound` row's named spender whose bytes are not held: its raw, and its proof where WoC serves it.
    Spender { txid: String },
    /// A `courier-bytes` payout: its home outputs' spend, proven by the home key's signature.
    Home,
}

fn split_outpoint(outpoint: &str) -> Option<(String, u32)> {
    let (t, v) = outpoint.rsplit_once(':')?;
    if !crate::logic::valid_txid(t) {
        return None;
    }
    Some((t.to_ascii_lowercase(), v.parse().ok()?))
}

pub fn ask_of(c: &Candidate) -> Option<Ask> {
    let fact = |k: &str| {
        c.facts[k]
            .as_str()
            .filter(|t| crate::logic::valid_txid(t))
            .map(str::to_ascii_lowercase)
    };
    match c.family.as_str() {
        "payout" if c.facts["creditKind"] == "courier-bytes" => Some(Ask::Home),
        "unbound" => fact("chainSpender")
            .or_else(|| fact("spendingTxid"))
            .or_else(|| fact("settleTxid"))
            .map(|txid| Ask::Spender { txid })
            .or_else(|| {
                split_outpoint(&c.outpoint).map(|(txid, vout)| Ask::Outpoint { txid, vout })
            }),
        _ => split_outpoint(&c.outpoint).map(|(txid, vout)| Ask::Outpoint { txid, vout }),
    }
}

/// What one candidate's ask found: the durable facts (written first in the judge's batch) and the same facts in
/// memory (applied to the judge's hydrated inputs, so the rows are judged over exactly what is written).
#[derive(Debug, Default, Clone)]
pub struct Find {
    pub memos: Vec<ProbeMemo>,
    pub bytes: Vec<(String, StoredSpenderBytes)>,
    pub hop_words: Vec<(String, HopChainWord)>,
    pub proven: Vec<String>,
    pub home_words: Vec<(String, HomeSpendWord)>,
}
impl Find {
    fn is_empty(&self) -> bool {
        self.memos.is_empty() && self.bytes.is_empty() && self.home_words.is_empty()
    }
    pub fn statements(&self, now_ms: i64) -> Vec<Stmt> {
        let mut out = Vec::new();
        for m in &self.memos {
            out.push(Stmt {
                sql: crate::hops_view::PROBE_MEMO_UPSERT_SQL.into(),
                binds: vec![
                    Bind::Text(m.outpoint.clone()),
                    Bind::Int(m.probed_at_ms),
                    Bind::Int(i64::from(m.spent)),
                    m.spending_txid.clone().map_or(Bind::Null, Bind::Text),
                    m.spent_confirmed
                        .map_or(Bind::Null, |c| Bind::Int(i64::from(c))),
                    m.tip_height.map_or(Bind::Null, |t| Bind::Int(t as i64)),
                ],
            });
        }
        for (txid, bytes) in &self.bytes {
            if let Some(json) = bytes.to_json_capped() {
                out.push(Stmt {
                    sql: owed::OWED_SPENDER_BYTES_UPSERT_SQL.into(),
                    binds: vec![
                        Bind::Text(txid.clone()),
                        Bind::Text(json),
                        Bind::Int(now_ms),
                    ],
                });
            }
        }
        out
    }
    /// The same facts the statements write, into the judge's hydrated inputs.
    pub fn apply(&self, i: &mut owed_judge::JudgeInputs) {
        for (k, w) in &self.hop_words {
            i.hop_chain.insert(k.clone(), w.clone());
        }
        for (txid, b) in &self.bytes {
            i.spender_inputs.insert(txid.clone(), b.inputs.clone());
            i.spender_outputs.insert(txid.clone(), b.to_outputs());
            i.courier_spenders.insert(txid.clone());
        }
        i.proof_mined_spenders.extend(self.proven.iter().cloned());
        for (k, w) in &self.home_words {
            i.home_spends.insert(k.clone(), *w);
        }
    }
}

fn bytes_of_raw(raw: &[u8], txid: &str) -> Option<StoredSpenderBytes> {
    let tx = bsv_rs::transaction::Transaction::from_binary(raw).ok()?;
    if !tx.id().eq_ignore_ascii_case(txid) {
        return None;
    }
    Some(StoredSpenderBytes::from_parts(
        &crate::routes::spender_inputs_of(&tx),
        &crate::routes::spender_outputs_of(&tx),
    ))
}

fn probe_find(
    txid: &str,
    vout: u32,
    probe: &ChainSpendProbe,
    raw: Option<&[u8]>,
    now: i64,
    tip: Option<u64>,
) -> Find {
    let mut find = Find::default();
    if let Some(memo) = crate::hops_view::probe_memo_at_tip(txid, vout, probe, now, tip) {
        find.hop_words.push((
            owed::outpoint_key(txid, vout),
            HopChainWord {
                looked: true,
                spent: probe.spent,
                spending_txid: probe.spending_txid.clone(),
                spent_confirmed: probe.spent_confirmed,
                stale: false,
                age_ms: None,
                probe_tip: tip,
                predates_tip: false,
            },
        ));
        find.memos.push(memo);
    }
    if let (Some(sp), Some(raw)) = (probe.spending_txid.as_deref(), raw) {
        if let Some(b) = bytes_of_raw(raw, sp) {
            find.bytes.push((sp.to_ascii_lowercase(), b));
        }
    }
    find
}

/// The ask of one candidate through the gate. `None`: no rung answered (or the row owes nothing askable).
async fn ask<C: Courier>(
    db: &impl JudgeDb,
    gate: &Gate<'_, C>,
    c: &Candidate,
    now: i64,
) -> Result<Option<Find>, String> {
    let get = |url: String| gate.get(url);
    match ask_of(c) {
        None => Ok(None),
        Some(Ask::Outpoint { txid, vout }) => {
            let (probe, raw) = crate::routes::belt_spent_any_via(&txid, vout, get).await;
            let find = probe_find(&txid, vout, &probe, raw.as_deref(), now, db.tip());
            Ok((!find.is_empty()).then_some(find))
        }
        Some(Ask::Spender { txid }) => {
            let (raw, claimed) = crate::routes::belt_spender_via(&txid, now, get).await;
            let mut find = Find::default();
            let Some(bytes) = raw.as_deref().and_then(|r| bytes_of_raw(r, &txid)) else {
                return Ok(None);
            };
            find.bytes.push((txid.clone(), bytes));
            if let Some(height) = claimed {
                if let Some(root) = db.canonical_root(height).await {
                    if let Some((200, body)) = gate.get(crate::routes::belt_proof_url(&txid)).await
                    {
                        if crate::txany::courier_tsc_bump(&body, height).is_some_and(|bump| {
                            crate::txany::mined_proof_matches(&txid, &bump, height, &root)
                        }) {
                            find.memos.push(ProbeMemo {
                                outpoint: crate::hops_view::probe_memo_key(
                                    &format!("{}{txid}", owed::PROOF_MINED_LATCH_PREFIX),
                                    height as u32,
                                ),
                                probed_at_ms: now,
                                spent: true,
                                spending_txid: Some(root.to_ascii_lowercase()),
                                spent_confirmed: Some(true),
                                tip_height: None,
                            });
                            find.proven.push(txid.clone());
                        }
                    }
                }
            }
            Ok(Some(find))
        }
        Some(Ask::Home) => {
            let inputs = db
                .hydrate(&c.identity, std::slice::from_ref(&c.game_id))
                .await?;
            let homes = owed::courier_home_outputs(&inputs.as_inputs());
            let Some(home) = homes.iter().find(|h| {
                !inputs
                    .home_spends
                    .contains_key(&owed::outpoint_key(&h.sweep_txid, h.vout))
            }) else {
                return Ok(None);
            };
            let (probe, raw) =
                crate::routes::belt_spent_any_via(&home.sweep_txid, home.vout, get).await;
            let mut find = probe_find(
                &home.sweep_txid,
                home.vout,
                &probe,
                raw.as_deref(),
                now,
                db.tip(),
            );
            find.hop_words.clear();
            if let Some((word, latch)) = owed::home_word(&probe, raw.as_deref(), home, now) {
                find.home_words
                    .push((owed::outpoint_key(&home.sweep_txid, home.vout), word));
                find.memos.extend(latch);
            }
            Ok((!find.is_empty()).then_some(find))
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    pub candidates: usize,
    pub asked: usize,
    pub found: usize,
    pub rearmed: usize,
    /// Finds whose row read as before the look: their period doubled (HIGH-3).
    pub backed_off: usize,
    /// Candidates the tick's call budget left untouched (still due, first by due time next tick).
    pub left: usize,
    pub calls: usize,
    pub faults: [u64; 4],
    pub write_faults: Vec<(String, String)>,
    /// The tick had no chain tip: nothing was asked.
    pub no_tip: bool,
    /// bsv-low #912 (Phase C): finds handed back to their actor as a `belt-find` event.
    pub handed_back: usize,
}

/// The tick's places by class (the third delta lens's D3-M1, the ruling: no class owns the whole tick), given each
/// class's due candidates (each at most a tick's worth): the second class at least [`OWED_BELT_SECOND_CLASS_RESERVED`]
/// whenever it holds a due row, the first class the rest, and a class with nothing due (or fewer than its places)
/// yields them to the other. So under a flood of one class the other is served at its share: a first-class row within
/// `ceil(C / 6)` ticks, a second-class row within `ceil(D / 2)` ticks of the walk reaching it.
pub fn class_places(first: usize, second: usize) -> (usize, usize) {
    let total = OWED_BELT_OUTPOINTS_PER_TICK;
    let second_n = second.min(total).min(
        total
            .saturating_sub(first)
            .max(OWED_BELT_SECOND_CLASS_RESERVED),
    );
    let first_n = first.min(total - second_n);
    (first_n, second_n)
}

/// The tick's candidates in two classes (the second delta lens's D2-M1, the captain's ruling: a look a PRESS WAITS ON
/// comes before a look that can only downgrade). FIRST the courier-only sweeps a block dated
/// ([`BELT_SWEEPS_DUE_SQL`]: claimable, index status not `unspent`, judged below the present tip, due), the class the
/// read withholds at every block until its look, round-robin by identity (each identity's oldest before a second of
/// any). THEN, in the places left, the round-robin of the rest (the D2-OWED lens's HIGH-3): up to
/// [`OWED_BELT_WALKED_PER_TICK`] dated identities walked past the cursor (wrapping), one index seek and one MIN seek
/// each, of which only those holding a row DUE NOW are given turns (at most [`OWED_BELT_IDENTITIES_PER_TICK`]), each
/// one's oldest due rows in one batch, each identity's oldest before a second of any, in walk order,
/// [`OWED_BELT_OUTPOINTS_PER_TICK`] in all; the chosen rows read by primary key. The cursor moves to the last identity
/// GIVEN A TURN (the delta lens's D-L1), or past the whole walk when none was, so no identity holding a due row is
/// passed unserved. Neither class owns the whole tick ([`class_places`], the third delta lens's D3-M1): the walk runs
/// on every tick and the second class holds its reserved places whenever it has a due row. Reads are bounded by the
/// first class's [`OWED_BELT_SWEEP_ROWS_READ`] and the walk, never the due set.
async fn due_candidates(db: &impl JudgeDb, now: i64, tip: u64) -> Result<Vec<Value>, String> {
    let first = |out: Vec<crate::pot_hook::StmtOut>| {
        out.into_iter().next().map(|o| o.rows).unwrap_or_default()
    };
    let text = |r: &Value, k: &str| r[k].as_str().map(str::to_owned);
    let mut chosen: Vec<(String, String)> = Vec::new();
    // The first class: the block-dated courier-only sweeps, by due time then identity, round-robin by identity.
    let mut sweeps: Vec<(i64, String, String)> = first(
        db.run_batch(vec![Stmt {
            sql: BELT_SWEEPS_DUE_SQL.into(),
            binds: vec![
                Bind::Int(tip as i64),
                Bind::Int(now),
                Bind::Int(OWED_BELT_SWEEP_ROWS_READ as i64),
            ],
        }])
        .await?,
    )
    .iter()
    .filter_map(|r| {
        Some((
            r["beltDueAtMs"].as_i64()?,
            text(r, "identity")?,
            text(r, "outpoint")?,
        ))
    })
    .collect();
    sweeps.sort();
    let in_first: std::collections::BTreeSet<(String, String)> = sweeps
        .iter()
        .map(|(_, identity, outpoint)| (identity.clone(), outpoint.clone()))
        .collect();
    let mut groups: Vec<(String, Vec<String>)> = Vec::new();
    for (_, identity, outpoint) in sweeps {
        match groups.iter_mut().find(|(id, _)| *id == identity) {
            Some((_, rows)) => rows.push(outpoint),
            None => groups.push((identity, vec![outpoint])),
        }
    }
    let mut first_class: Vec<(String, String)> = Vec::new();
    for turn in 0..OWED_BELT_OUTPOINTS_PER_TICK {
        for (identity, rows) in &groups {
            if let Some(outpoint) = rows.get(turn) {
                if first_class.len() < OWED_BELT_OUTPOINTS_PER_TICK {
                    first_class.push((identity.clone(), outpoint.clone()));
                }
            }
        }
    }
    // The second class: the walk, run on every tick (the third delta lens's D3-M1: no class owns the whole tick).
    let mut reads = Vec::new();
    let cursor = first(
        db.run_batch(vec![Stmt {
            sql: "SELECT cursor FROM owed_belt_state WHERE name='belt:identities'".into(),
            binds: vec![],
        }])
        .await?,
    )
    .first()
    .and_then(|r| r["cursor"].as_str().map(str::to_owned))
    .unwrap_or_default();
    let walk = |after: String, n: usize| async move {
        db.run_batch(vec![Stmt {
            sql: BELT_IDENTITIES_SQL.into(),
            binds: vec![Bind::Text(after), Bind::Int(n as i64)],
        }])
        .await
        .map(|out| {
            first(out)
                .iter()
                .filter_map(|r| Some((r["identity"].as_str()?.to_owned(), r["due"].as_i64())))
                .collect::<Vec<_>>()
        })
    };
    let span = OWED_BELT_WALKED_PER_TICK;
    let mut walked = walk(cursor.clone(), span).await?;
    if walked.len() < span && !cursor.is_empty() {
        for (id, due) in walk(String::new(), span - walked.len()).await? {
            if id > cursor || walked.iter().any(|(w, _)| *w == id) {
                break;
            }
            walked.push((id, due));
        }
    }
    // Only an identity holding a row DUE NOW is given a turn (D2-M1 (a)); the walk ends at the last of them.
    let mut identities = Vec::new();
    let mut walked_to = None;
    for (id, due) in &walked {
        walked_to = Some(id.clone());
        if due.is_some_and(|d| d <= now) {
            identities.push(id.clone());
            if identities.len() == OWED_BELT_IDENTITIES_PER_TICK {
                break;
            }
        }
    }
    let due = if identities.is_empty() {
        Vec::new()
    } else {
        db.run_batch(
            identities
                .iter()
                .map(|id| Stmt {
                    sql: BELT_DUE_SQL.into(),
                    binds: vec![
                        Bind::Text(id.clone()),
                        Bind::Int(now),
                        Bind::Int(OWED_BELT_OUTPOINTS_PER_TICK as i64),
                    ],
                })
                .collect(),
        )
        .await?
    };
    // Each turn in WALK order, as the drain's (the delta lens's D-L1): the cursor then passes no identity holding a
    // due row that was not given its turn. A row the first class read is the first class's, never a second-class turn.
    let mut second_class: Vec<(usize, String, String)> = Vec::new();
    for turn in 0..OWED_BELT_OUTPOINTS_PER_TICK {
        for (at, (id, out)) in identities.iter().zip(&due).enumerate() {
            let Some(outpoint) = out.rows.get(turn).and_then(|r| r["outpoint"].as_str()) else {
                continue;
            };
            if in_first.contains(&(id.clone(), outpoint.to_owned())) {
                continue;
            }
            if second_class.len() < OWED_BELT_OUTPOINTS_PER_TICK {
                second_class.push((at, id.clone(), outpoint.to_owned()));
            }
        }
    }
    // The places (D3-M1): the second class is given at least its reserved places whenever it holds a due row, the
    // first class the rest; a class with nothing due yields its places to the other.
    let (first_n, second_n) = class_places(first_class.len(), second_class.len());
    chosen.extend(first_class.into_iter().take(first_n));
    second_class.truncate(second_n);
    // The next tick starts after the last identity this one gave a turn (the whole walk when none was given one and
    // the second class was not cut short by its places).
    let last = second_class
        .iter()
        .map(|(at, _, _)| *at)
        .max()
        .and_then(|at| identities.get(at).cloned())
        .or(walked_to);
    chosen.extend(
        second_class
            .into_iter()
            .map(|(_, id, outpoint)| (id, outpoint)),
    );
    if let Some(last) = last {
        reads.push(Stmt {
            sql: STATE_SQL.into(),
            binds: vec![
                Bind::Text(OWED_BELT_WALK_STATE_NAME.into()),
                Bind::Text(last),
                Bind::Int(now),
            ],
        });
    }
    if chosen.is_empty() && reads.is_empty() {
        return Ok(Vec::new());
    }
    let skip = reads.len();
    reads.extend(chosen.iter().map(|(id, outpoint)| Stmt {
        sql: CANDIDATES_SQL.into(),
        binds: vec![Bind::Text(id.clone()), Bind::Text(outpoint.clone())],
    }));
    Ok(db
        .run_batch(reads)
        .await?
        .into_iter()
        .skip(skip)
        .filter_map(|o| o.rows.into_iter().next())
        .collect())
}

/// A candidate row ([`CANDIDATES_SQL`]'s shape; an ask's question read into it) as the tick reads it.
pub fn candidate_of(r: &Value) -> Candidate {
    let text = |k: &str| r[k].as_str().unwrap_or_default().to_owned();
    Candidate {
        identity: text("identity"),
        outpoint: text("outpoint"),
        game_id: text("gameId"),
        family: text("family"),
        facts: r["facts"]
            .as_str()
            .and_then(|f| serde_json::from_str(f).ok())
            .unwrap_or(Value::Null),
        claimable: r["claimable"]
            .as_i64()
            .map(|n| n != 0)
            .or_else(|| r["claimable"].as_bool()),
        reason: r["reason"].as_str().map(str::to_owned),
        probe_tip: r["probeTip"].as_i64(),
        due_at_ms: r["beltDueAtMs"].as_i64(),
        judged_at_ms: r["judgedAtMs"].as_i64(),
    }
}

/// PHASE C's candidates: one page of the asks past the cursor (at most [`OWED_BELT_ASKS_PAGE`] ledger rows, a range
/// of its key), the due ones taken round-robin by identity (each identity's first ask before a second of any) up to
/// [`OWED_BELT_OUTPOINTS_PER_TICK`]; the cursor at the first due ask not taken (the next tick starts there), past the
/// page when every due ask of it was taken, back to the start when the page was short.
async fn ask_candidates(db: &impl JudgeDb, now: i64) -> Result<Vec<Value>, String> {
    let first = |out: Vec<crate::pot_hook::StmtOut>| {
        out.into_iter().next().map(|o| o.rows).unwrap_or_default()
    };
    let cursor = first(
        db.run_batch(vec![Stmt {
            sql: "SELECT cursor FROM owed_belt_state WHERE name='belt:asks'".into(),
            binds: vec![],
        }])
        .await?,
    )
    .first()
    .and_then(|r| r["cursor"].as_str().map(str::to_owned))
    .filter(|c| c.as_str() > BELT_ASK_PREFIX)
    .unwrap_or_else(|| BELT_ASK_PREFIX.to_owned());
    let page = first(
        db.run_batch(vec![Stmt {
            sql: ASKS_PAGE_SQL.into(),
            binds: vec![Bind::Text(cursor), Bind::Int(OWED_BELT_ASKS_PAGE as i64)],
        }])
        .await?,
    );
    let due: Vec<(String, Value)> = page
        .iter()
        .filter(|r| r["tickedAtMs"].as_i64().is_some_and(|d| d <= now))
        .filter_map(|r| {
            let ask: Value = serde_json::from_str(r["cursor"].as_str()?).ok()?;
            Some((r["name"].as_str()?.to_owned(), ask))
        })
        .collect();
    let mut groups: Vec<(String, Vec<usize>)> = Vec::new();
    for (n, (_, ask)) in due.iter().enumerate() {
        let id = ask["identity"].as_str().unwrap_or_default().to_owned();
        match groups.iter_mut().find(|(g, _)| *g == id) {
            Some((_, at)) => at.push(n),
            None => groups.push((id, vec![n])),
        }
    }
    let mut taken: Vec<usize> = Vec::new();
    'turns: for turn in 0..OWED_BELT_OUTPOINTS_PER_TICK {
        for (_, at) in &groups {
            if let Some(n) = at.get(turn) {
                if taken.len() == OWED_BELT_OUTPOINTS_PER_TICK {
                    break 'turns;
                }
                taken.push(*n);
            }
        }
    }
    let next = match (0..due.len()).find(|n| !taken.contains(n)) {
        Some(n) => due[n].0.clone(),
        None if page.len() < OWED_BELT_ASKS_PAGE => String::new(),
        None => format!(
            "{}\u{1}",
            page.last()
                .and_then(|r| r["name"].as_str())
                .unwrap_or_default()
        ),
    };
    db.run_batch(vec![Stmt {
        sql: STATE_SQL.into(),
        binds: vec![
            Bind::Text(OWED_BELT_ASKS_STATE_NAME.into()),
            Bind::Text(next),
            Bind::Int(now),
        ],
    }])
    .await?;
    taken.sort_unstable();
    Ok(taken
        .into_iter()
        .map(|n| {
            let (_, ask) = &due[n];
            let q = &ask["question"];
            serde_json::json!({
                "identity": ask["identity"], "outpoint": ask["outpoint"], "gameId": q["gameId"],
                "family": q["family"], "facts": q["facts"].to_string(), "reason": q["reason"],
                "claimable": q["claimable"], "probeTip": q["probeTip"], "beltDueAtMs": ask["dueAtMs"],
                "judgedAtMs": q["judgedAtMs"],
            })
        })
        .collect())
}

/// PHASE C's find: the find's durable facts written to the record ALONE (the D1 judge writes no `owed_rows` in this
/// phase: one writer), then handed back to the actor as a `belt-find` event, which re-reads them in its closure,
/// judges and re-publishes its asks; then the ask's own backoff over the actor's answer. A fault of either step leaves
/// the ask re-armed (the facts that landed are the record's; the actor's next event reads them).
async fn ask_find(
    db: &impl JudgeDb,
    c: &Candidate,
    find: &Find,
    now: i64,
    report: &mut TickReport,
) {
    if let Err(e) = db.run_batch(find.statements(now)).await {
        db.log(&format!(
            "[owed-belt] {} find's write faulted: {e}",
            c.outpoint
        ));
        report.write_faults.push((c.outpoint.clone(), e));
        return rearm_ask(db, c, now, report).await;
    }
    match db
        .actor_apply(&c.identity, std::slice::from_ref(&c.game_id), "belt-find")
        .await
    {
        Ok(Some(answer)) => {
            report.found += 1;
            report.handed_back += 1;
            let row = answer["rows"]
                .as_array()
                .and_then(|rows| rows.iter().find(|r| r["outpoint"] == c.outpoint.as_str()))
                .cloned();
            if let Some(row) = row {
                ask_backoff(db, c, &row, now, report).await;
            }
        }
        Ok(None) => {
            db.log(&format!(
                "[owed-belt] {} found, no owed actor bound: re-armed",
                c.outpoint
            ));
            rearm_ask(db, c, now, report).await;
        }
        Err(e) => {
            db.log(&format!(
                "[owed-belt] {} find's hand-back faulted: {e}",
                c.outpoint
            ));
            report.write_faults.push((c.outpoint.clone(), e));
            rearm_ask(db, c, now, report).await;
        }
    }
}

/// Phase B's find (the D2-ACTOR-B lens's HIGH-3): the find's facts written alone, the marks' bound read, the actor's
/// apply of them, then the D1 judge acknowledging up to that bound (NEW-H1 (a)); `Err` at the first fault (the
/// facts' marks standing).
async fn phase_b_find(
    db: &impl JudgeDb,
    c: &Candidate,
    subject: &Subject,
    find: &Find,
    now: i64,
) -> Result<(), String> {
    db.run_batch(find.statements(now)).await?;
    // The delta lens's NEW-H1 (a): the acknowledgement's bound is read after the facts' marks and BEFORE the actor
    // reads the record, so a downgrade written while it applies stands for its next apply.
    let bound = owed_judge::marks_bound(db, &c.identity, std::slice::from_ref(&c.game_id)).await?;
    if let Some(answer) = db
        .actor_apply(&c.identity, std::slice::from_ref(&c.game_id), "belt-find")
        .await?
    {
        crate::owed_actor::note_answer(&answer);
    }
    owed_judge::judge_with_write_bounded(
        db,
        std::slice::from_ref(subject),
        "belt",
        Vec::new(),
        &[],
        Some(bound),
        |_| Ok(()),
    )
    .await
    .map(|_| ())
}

/// The ask's backoff ([`backoff`]'s rule over the actor's row): a look that changed nothing doubles the period.
async fn ask_backoff(
    db: &impl JudgeDb,
    c: &Candidate,
    row: &Value,
    now: i64,
    report: &mut TickReport,
) {
    let Some(base_due) = row["beltDueAtMs"].as_i64() else {
        return;
    };
    let facts: Value = row["facts"]
        .as_str()
        .and_then(|f| serde_json::from_str(f).ok())
        .unwrap_or(Value::Null);
    let before = look_shape(&c.family, &c.facts, c.reason.as_deref());
    let after = look_shape(
        row["family"].as_str().unwrap_or_default(),
        &facts,
        row["reason"].as_str(),
    );
    if before != after {
        return;
    }
    let period = backoff_period(c.due_at_ms, c.judged_at_ms, (base_due - now).max(1));
    match db
        .run_batch(vec![Stmt {
            sql: ASK_DUE_SQL.into(),
            binds: vec![
                Bind::Text(ask_name(&c.identity, &c.outpoint)),
                Bind::Int(now + period),
            ],
        }])
        .await
    {
        Ok(_) => report.backed_off += 1,
        Err(e) => db.log(&format!(
            "[owed-belt] backoff of {} faulted: {e}",
            c.outpoint
        )),
    }
}

async fn rearm_ask(db: &impl JudgeDb, c: &Candidate, now: i64, report: &mut TickReport) {
    let stmt = Stmt {
        sql: ASK_DUE_SQL.into(),
        binds: vec![
            Bind::Text(ask_name(&c.identity, &c.outpoint)),
            Bind::Int(now + unanswered_period(now, c.judged_at_ms)),
        ],
    };
    match db.run_batch(vec![stmt]).await {
        Ok(_) => report.rearmed += 1,
        Err(e) => db.log(&format!(
            "[owed-belt] re-arm of {} faulted: {e}",
            c.outpoint
        )),
    }
}

pub async fn tick<C: Courier>(db: &impl JudgeDb, courier: &C) -> Result<TickReport, String> {
    let now = db.now_ms() as i64;
    // Without a chain tip a find's memo carries no tip and its judgement withholds every tip-dependent claim (TIP-877),
    // so the look buys nothing a press could rest on: ask nothing, spend no courier call, leave every date as it is
    // (the next tick with a tip asks them first).
    if db.tip().is_none() {
        db.counters(vec![(COUNTER_TICKS.to_owned(), 1)]).await;
        db.log("[owed-belt] tick: no chain tip, nothing asked");
        return Ok(TickReport {
            no_tip: true,
            ..Default::default()
        });
    }
    // bsv-low #912: Phase C (the D1 judge off) reads the actors' asks; Phases A and B the D1 judge's dated rows.
    let asks = !db.d1_judges();
    let rows = if asks {
        ask_candidates(db, now).await?
    } else {
        due_candidates(db, now, db.tip().unwrap_or_default()).await?
    };
    let gate = Gate::new(courier);
    let mut report = TickReport {
        candidates: rows.len(),
        ..Default::default()
    };
    let mut cursor = None;
    for r in rows {
        let c = candidate_of(&r);
        if gate.spent() {
            report.left += 1;
            continue;
        }
        gate.begin_outpoint();
        let before = gate.state.borrow().tick_calls;
        let found = ask(db, &gate, &c, now).await;
        if gate.state.borrow().tick_calls > before {
            report.asked += 1;
        }
        cursor = Some(c.outpoint.clone());
        let subject = Subject {
            identity: c.identity.clone(),
            game_id: c.game_id.clone(),
        };
        if asks {
            match found {
                Ok(Some(find)) => ask_find(db, &c, &find, now, &mut report).await,
                Ok(None) => rearm_ask(db, &c, now, &mut report).await,
                Err(e) => {
                    db.log(&format!("[owed-belt] {} ask faulted: {e}", c.outpoint));
                    report.write_faults.push((c.outpoint.clone(), e));
                    rearm_ask(db, &c, now, &mut report).await;
                }
            }
            continue;
        }
        match found {
            // Phase B (the D2-ACTOR-B lens's HIGH-3): the find's facts land alone, the actor applies them, and only
            // then the D1 judge (whose batch acknowledges the marks the facts wrote): an actor fault keeps them, so
            // the read withholds the game's presses and the sweep re-publishes the pair.
            Ok(Some(find)) if db.actor_reads() => {
                match phase_b_find(db, &c, &subject, &find, now).await {
                    Ok(()) => {
                        report.found += 1;
                        report.handed_back += 1;
                        backoff(db, &c, now, &mut report).await;
                    }
                    Err(e) => {
                        db.log(&format!("[owed-belt] {} find faulted: {e}", c.outpoint));
                        report.write_faults.push((c.outpoint.clone(), e));
                        rearm(db, &c, now, &mut report).await;
                    }
                }
            }
            Ok(Some(find)) => {
                let writes = find.statements(now);
                let identity = c.identity.clone();
                let judged =
                    owed_judge::judge_with_write(db, &[subject], "belt", writes, &[], |inputs| {
                        for i in inputs.iter_mut().filter(|i| i.identity == identity) {
                            find.apply(i);
                        }
                        Ok(())
                    })
                    .await;
                match judged {
                    Ok(_) => {
                        report.found += 1;
                        // Phase A: the find reaches the actor too (it was judged only in D1 and re-published by
                        // the sweep); its fault is counted, never the find's.
                        match db
                            .actor_apply(&c.identity, std::slice::from_ref(&c.game_id), "belt-find")
                            .await
                        {
                            Ok(Some(answer)) => {
                                report.handed_back += 1;
                                crate::owed_actor::note_answer(&answer);
                            }
                            Ok(None) => {}
                            Err(e) => {
                                db.counters(vec![(crate::owed_actor::COUNTERS[2].into(), 1)])
                                    .await;
                                db.log(&format!(
                                    "[owed-actor] FAULT {} (belt-find): {e}",
                                    c.identity
                                ));
                            }
                        }
                        backoff(db, &c, now, &mut report).await;
                    }
                    Err(e) => {
                        db.log(&format!("[owed-belt] {} find's write faulted: {e}", c.outpoint));
                        report.write_faults.push((c.outpoint.clone(), e));
                        rearm(db, &c, now, &mut report).await;
                    }
                }
            }
            Ok(None) => rearm(db, &c, now, &mut report).await,
            Err(e) => {
                db.log(&format!("[owed-belt] {} ask faulted: {e}", c.outpoint));
                report.write_faults.push((c.outpoint.clone(), e));
                rearm(db, &c, now, &mut report).await;
            }
        }
    }
    let state = gate.state.borrow().clone();
    report.calls = state.tick_calls;
    report.faults = state.fault_counts;
    let close = db
        .run_batch(vec![
            Stmt {
                sql: STATE_SQL.into(),
                binds: vec![
                    Bind::Text(OWED_BELT_STATE_NAME.into()),
                    cursor.map_or(Bind::Null, Bind::Text),
                    Bind::Int(now),
                ],
            },
            Stmt {
                sql: if asks {
                    ASKS_DUE_GAUGE_SQL
                } else {
                    DUE_GAUGE_SQL
                }
                .into(),
                binds: vec![Bind::Int(now)],
            },
        ])
        .await;
    if let Err(e) = close {
        db.log(&format!("[owed-belt] state write faulted: {e}"));
    }
    let mut deltas = vec![
        (COUNTER_TICKS.to_owned(), 1),
        (COUNTER_ASKED.to_owned(), report.asked as u64),
        (COUNTER_FOUND.to_owned(), report.found as u64),
    ];
    if report.handed_back > 0 {
        deltas.push((
            crate::owed_actor::COUNTERS[9].to_owned(),
            report.handed_back as u64,
        ));
    }
    for (rung, n) in report.faults.iter().enumerate() {
        if *n > 0 {
            deltas.push((fault_counter(rung), *n));
        }
    }
    db.counters(deltas).await;
    db.log(&format!(
        "[owed-belt] tick: candidates={} asked={} found={} rearmed={} left={} calls={} faults={:?} write_faults={}",
        report.candidates,
        report.asked,
        report.found,
        report.rearmed,
        report.left,
        report.calls,
        report.faults,
        report.write_faults.len()
    ));
    Ok(report)
}

/// What a look can change about a row: its family, its words of the claim, the chain's word. NOT the tip it was judged
/// at (the delta lens's D-L2): the find's judge stamps the present tip, so a look spanning a block read as a change and
/// reset the period on every block, and the 6 h ceiling was reached only on a still chain.
fn look_shape(family: &str, facts: &Value, reason: Option<&str>) -> Value {
    serde_json::json!([
        family,
        reason,
        facts["claim"],
        facts["claimable"],
        facts["chainProbe"],
        facts["chainSpender"],
        facts["spentConfirmed"],
        facts["creditKind"],
        facts["homeOnChain"],
    ])
}

/// The next period of a row whose find changed nothing: twice its last (`beltDueAtMs - judgedAtMs`), to the ceiling.
pub fn backoff_period(due_at_ms: Option<i64>, judged_at_ms: Option<i64>, base_ms: i64) -> i64 {
    let last = match (due_at_ms, judged_at_ms) {
        (Some(due), Some(judged)) if due > judged => due - judged,
        _ => base_ms,
    };
    last.max(base_ms)
        .saturating_mul(2)
        .min(OWED_BELT_BACKOFF_MAX_MS)
}

/// After a find landed: when the row it judged reads as it did before the look, the look bought nothing, so the row's
/// next look waits twice as long (the D2-OWED lens's HIGH-3). A row that changed keeps the judge's base date.
async fn backoff(db: &impl JudgeDb, c: &Candidate, now: i64, report: &mut TickReport) {
    let binds = vec![
        Bind::Text(c.identity.clone()),
        Bind::Text(c.outpoint.clone()),
    ];
    let after = db
        .run_batch(vec![Stmt {
            sql: AFTER_FIND_SQL.into(),
            binds: binds.clone(),
        }])
        .await;
    let Some(row) = after
        .ok()
        .and_then(|o| o.into_iter().next())
        .and_then(|o| o.rows.into_iter().next())
    else {
        return;
    };
    let Some(base_due) = row["beltDueAtMs"].as_i64() else {
        return;
    };
    let facts: Value = row["facts"]
        .as_str()
        .and_then(|f| serde_json::from_str(f).ok())
        .unwrap_or(Value::Null);
    let before = look_shape(&c.family, &c.facts, c.reason.as_deref());
    let now_shape = look_shape(
        row["family"].as_str().unwrap_or_default(),
        &facts,
        row["reason"].as_str(),
    );
    if before != now_shape {
        return;
    }
    let period = backoff_period(c.due_at_ms, c.judged_at_ms, (base_due - now).max(1));
    let mut binds = binds;
    binds.push(Bind::Int(now + period));
    match db
        .run_batch(vec![Stmt {
            sql: BACKOFF_SQL.into(),
            binds,
        }])
        .await
    {
        Ok(_) => report.backed_off += 1,
        Err(e) => db.log(&format!("[owed-belt] backoff of {} faulted: {e}", c.outpoint)),
    }
}

/// The next look of a row no rung answered (the delta lens's D-L2: it re-armed at a fixed 300 s for ever): it waits
/// as long again as it has gone since its last judgement, at least [`OWED_BELT_UNANSWERED_RETRY_MS`] and at most
/// [`OWED_BELT_BACKOFF_MAX_MS`]. No judge runs on an unanswered look, so `judgedAtMs` stays and each unanswered look
/// doubles the period (300 s, 600 s, 1,200 s, ... to 6 h); the judge's own dating starts it again.
pub fn unanswered_period(now: i64, judged_at_ms: Option<i64>) -> i64 {
    judged_at_ms
        .map_or(0, |j| now.saturating_sub(j))
        .clamp(OWED_BELT_UNANSWERED_RETRY_MS, OWED_BELT_BACKOFF_MAX_MS)
}

async fn rearm(db: &impl JudgeDb, c: &Candidate, now: i64, report: &mut TickReport) {
    let stmt = Stmt {
        sql: REARM_SQL.into(),
        binds: vec![
            Bind::Text(c.identity.clone()),
            Bind::Text(c.outpoint.clone()),
            Bind::Int(now + unanswered_period(now, c.judged_at_ms)),
        ],
    };
    match db.run_batch(vec![stmt]).await {
        Ok(_) => report.rearmed += 1,
        Err(e) => db.log(&format!(
            "[owed-belt] re-arm of {} faulted: {e}",
            c.outpoint
        )),
    }
}

/// The production courier: `provider_get` under caller `belt` (its counters and egress guard).
pub struct WorkerCourier;
impl Courier for WorkerCourier {
    async fn get(&self, url: String) -> Option<(u16, Vec<u8>)> {
        crate::routes::provider_get("belt", &url).await
    }
}

/// The scheduled pass's watchdog: the belt actor arms its alarm when none is set. Best effort.
pub async fn arm(env: &worker::Env) -> worker::Result<()> {
    let stub = env
        .durable_object("BOARD_VIEW")?
        .id_from_name(OWED_BELT_ACTOR)?
        .get_stub()?;
    let response = stub
        .fetch_with_str("https://board-view/owed-belt-arm")
        .await?;
    if response.status_code() != 200 {
        return Err(worker::Error::RustError("owed belt arm refused".into()));
    }
    Ok(())
}

/// The scheduled pass's drain runs in the very same actor as the alarm's. The actor serializes both turns.
pub async fn drain(env: &worker::Env) -> worker::Result<()> {
    let mut response = env
        .durable_object("BOARD_VIEW")?
        .id_from_name(OWED_BELT_ACTOR)?
        .get_stub()?
        .fetch_with_str("https://board-view/owed-drain")
        .await?;
    if response.status_code() != 200 {
        return Err(worker::Error::RustError(format!(
            "owed drain refused ({}): {}", response.status_code(), response.text().await?
        )));
    }
    Ok(())
}

/// Called only while the belt actor holds its turn. A fault or wall leaves the unacknowledged marks for the next
/// turn. The daily age repair stays on the scheduled pass; both triggers drain at most 64 subjects.
pub async fn drain_env(
    env: &worker::Env,
    repair_age: bool,
) -> worker::Result<owed_judge::DrainReport> {
    let deadline = crate::routes::worker_deadline(OWED_DRAIN_WALL_MS, OWED_DRAIN_RESERVE_MS);
    let work = async {
        crate::schema::ensure_latch_columns(env).await;
        let tip = crate::routes::chaintracks_present_height_env(env, "owed").await.ok();
        let judge = owed_judge::D1Judge::new(env, tip, crate::routes::written_off_before_ms_env(env))?;
        if repair_age {
            if let Err(error) = crate::owed_backfill::mark_aged(&judge).await {
                worker::console_warn!("[owed] age re-mark deferred: {error}");
            }
        }
        // bsv-low #911 (D2-ACTOR-A, HIGH-2's second belt): with the queue the drain no longer judges; it
        // RE-PUBLISHES the marks no consumer acknowledged within one tick, oldest first, 64 pairs a tick (the
        // consumer judges them). With no queue binding it judges as before.
        if crate::owed_events::configured(env).await {
            let swept = crate::owed_events::sweep_publish(env)
                .await
                .map_err(worker::Error::RustError)?;
            worker::console_log!("[owed-events] sweep: {swept} pair(s) re-published");
            return Ok(owed_judge::DrainReport::default());
        }
        owed_judge::drain_dirty(&judge, owed_judge::OWED_DIRTY_SUBJECTS_PER_PASS)
            .await.map_err(worker::Error::RustError)
    };
    let result = deadline.run_reserving(work).await;
    // Outside the judge's wall: even a timed-out drain must publish its remaining backlog.
    // This is a single bounded count, never work delegated to /health.
    let sql = crate::routes::D1Sql::of(env)?;
    if let Err(error) = owed_judge::snapshot_dirty(&sql).await {
        worker::console_warn!("[owed] backlog snapshot deferred: {error}");
    }
    match result {
        Some(Ok(report)) => {
            for (_, error) in &report.faults {
                worker::console_warn!("[owed] {} judgement deferred: {error}", if repair_age { "scheduled" } else { "alarm" });
            }
            Ok(report)
        }
        Some(Err(error)) => Err(error),
        None => Err(worker::Error::RustError(
            "owed drain reached its wall".into(),
        )),
    }
}

/// The 120 s alarm: drain under its own reservation, then run the existing courier tick. Its meter is returned
/// and logged separately from the scheduled pass's durable row.
pub async fn alarm_env(
    env: &worker::Env,
    turn: &futures_util::lock::Mutex<()>,
) -> worker::Result<worker::Response> {
    let meter = crate::pass_meter::PassMeter::new(Box::new(|| worker::Date::now().as_millis()), 1);
    // Serialize the two drains only. A slow courier must not hold a scheduled drain behind the belt's work.
    let drained = {
        let _turn = turn.lock().await;
        meter.phase("owed.drain", drain_env(env, false)).await
    };
    let drain = match drained {
        Ok(report) => {
            serde_json::json!({"subjects": report.subjects, "faults": report.faults.len()})
        }
        Err(error) => {
            worker::console_warn!("[owed] alarm drain deferred: {error}");
            serde_json::json!({"error": error.to_string()})
        }
    };
    let tick = match meter.phase("owed.belt_tick", tick_env(env)).await {
        Ok(report) => serde_json::json!({
            "candidates": report.candidates, "asked": report.asked, "found": report.found,
            "rearmed": report.rearmed, "calls": report.calls,
        }),
        Err(error) => {
            worker::console_warn!("[owed-belt] tick faulted: {error}");
            serde_json::json!({"error": error})
        }
    };
    // bsv-low #912 (MEDIUM-10): the sampled digest reconcile on the same driver, one identity a 22.5 minutes.
    let reconcile = match meter.phase("owed.reconcile", reconcile_env(env)).await {
        Ok(Some(r)) => serde_json::json!({
            "identity": r.identity, "diff": r.diff, "reapplied": r.reapplied, "noActor": r.no_actor,
        }),
        Ok(None) => Value::Null,
        Err(error) => {
            worker::console_warn!("[owed-actor] reconcile faulted: {error}");
            serde_json::json!({"error": error})
        }
    };
    let walls = meter.task_done().expect("one alarm task");
    worker::console_log!("{}", walls.line().replace("[scheduled-pass]", "[owed-alarm]"));
    worker::Response::from_json(&serde_json::json!({
        "drain": drain, "tick": tick, "reconcile": reconcile, "wallMs": walls.wall_ms(),
        "phases": serde_json::from_str::<Value>(&walls.phases_json()).unwrap_or_default(),
    }))
}

/// The alarm's tick (in the actor): the tip, the era, the judge's production port and the production courier.
pub async fn tick_env(env: &worker::Env) -> Result<TickReport, String> {
    crate::schema::ensure_latch_columns(env).await;
    let tip = crate::routes::chaintracks_present_height_env(env, "owed")
        .await
        .ok();
    let judge = owed_judge::D1Judge::new(env, tip, crate::routes::written_off_before_ms_env(env))
        .map_err(|e| e.to_string())?;
    tick(&judge, &WorkerCourier).await
}

/// The alarm's reconcile step (bsv-low #912): the D1 judge's port (its flag, its actor calls).
pub async fn reconcile_env(
    env: &worker::Env,
) -> Result<Option<crate::owed_actor::ReconcileReport>, String> {
    if env.durable_object(crate::owed_actor::BINDING).is_err() {
        return Ok(None);
    }
    let judge = owed_judge::D1Judge::new(env, None, crate::routes::written_off_before_ms_env(env))
        .map_err(|e| e.to_string())?;
    crate::owed_actor::reconcile_step(&judge, worker::Date::now().as_millis() as i64).await
}

/// `POST /internal/owed-belt-tick` (bearer `INTERNAL_TOKEN`): one tick, answered with its report.
pub async fn lever(req: worker::Request, env: &worker::Env) -> worker::Result<worker::Response> {
    if !crate::internal_events::internal_bearer_ok(&req, env) {
        return worker::Response::error("unauthorized", 401);
    }
    match tick_env(env).await {
        Ok(r) => worker::Response::from_json(&serde_json::json!({
            "candidates": r.candidates,
            "asked": r.asked,
            "found": r.found,
            "rearmed": r.rearmed,
            "left": r.left,
            "calls": r.calls,
            "faults": {"woc": r.faults[0], "bitails": r.faults[1], "bananablocks": r.faults[2], "other": r.faults[3]},
            "writeFaults": r.write_faults.len(),
            "noTip": r.no_tip,
            "handedBack": r.handed_back,
        })),
        Err(e) => worker::Response::error(format!("owed belt tick faulted: {e}"), 503),
    }
}

#[cfg(test)]
mod tests {
    fn squash(s: &str) -> String {
        s.split_whitespace().collect()
    }
    /// The alarm and its watchdog: the belt actor's alarm sets its next alarm BEFORE the tick, the cron arms it.
    #[test]
    fn owed893_belt_the_alarm_rearms_before_its_tick_and_the_cron_is_its_watchdog() {
        let actor = squash(include_str!("board_view.rs"));
        let alarm = &actor[actor.find("asyncfnserve_alarm(&self)").unwrap()..];
        let rearm = alarm
            .find("storage.set_alarm(crate::owed_belt::OWED_BELT_TICK_MS).await?;")
            .expect("the alarm re-arms");
        let tick = alarm
            .find("crate::owed_belt::alarm_env(&self.env,&self.owed_turn).await")
            .expect("the alarm ticks");
        assert!(rearm < tick, "re-armed before the tick runs");
        assert!(actor.contains(&squash(
            "if storage.get_alarm().await?.is_none() { storage.set_alarm(0).await?; return Response::ok(\"armed\"); }"
        )));
        let lib = squash(include_str!("lib.rs"));
        let cron = &lib[lib.find("asyncfnscheduled(").unwrap()..];
        assert!(cron.contains(&squash(
            ".phase(\"owed.belt_arm\", owed_belt::arm(&env_owed))"
        )));
        // one fleet-wide actor
        let me = squash(include_str!("owed_belt.rs"));
        assert!(me.contains(&squash(".id_from_name(OWED_BELT_ACTOR)?")));
    }
}
