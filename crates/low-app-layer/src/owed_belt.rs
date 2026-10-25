//! bsv-low #893 section 5: THE BELT. The one place the owed list asks the chain explorers, BREAK GLASS ONLY: a slow
//! background look for a spend that never passed through our stack, never a read's dependency.
//!
//! A Durable Object alarm (the `BOARD_VIEW` actor named [`OWED_BELT_ACTOR`]; the deferred walk's alarm is gone) runs
//! one [`tick`] every [`OWED_BELT_TICK_MS`], and the scheduled pass re-arms it ([`arm`], the watchdog: a lost alarm
//! costs at most one 15 minute cron). A tick reads the [`OWED_BELT_OUTPOINTS_PER_TICK`] rows whose `beltDueAtMs` is
//! past ([`CANDIDATES_SQL`] over `idx_owed_rows_belt`; the judge sets that column for exactly the rows that owe the
//! chain a question, `owed_judge::row_statements`) and asks each through the existing ladders behind ONE gate
//! ([`Gate`]): at most [`OWED_BELT_CALLS_PER_OUTPOINT`] courier calls an outpoint and [`OWED_BELT_CALLS_PER_TICK`] a
//! tick, the three-fault circuit per rung per tick, a rung's 429 ending that rung's turn (never the tick). Every find
//! is ONE batch (`owed_judge::judge_with_write`): the probe memo with the tip it was taken at, the spender's bytes
//! (content-addressed, DO NOTHING), a `proofmined:` or `homeproof:` latch when it checked, then the judge's rows for
//! that subject, judged over the same facts supplied in memory. A fault rolls all of it back. A candidate no rung
//! answered keeps its row and facts and is re-armed once at `now +` [`OWED_BELT_UNANSWERED_RETRY_MS`].
use crate::hops_view::{ChainSpendProbe, ProbeMemo};
use crate::owed::{self, HomeSpendWord, HopChainWord, StoredSpenderBytes};
use crate::owed_judge::{self, JudgeDb, Subject};
use crate::pot_hook::{Bind, Stmt};
use serde_json::Value;
use std::cell::RefCell;

pub const OWED_BELT_TICK_MS: i64 = 120_000;
pub const OWED_BELT_OUTPOINTS_PER_TICK: usize = 8;
pub const OWED_BELT_CALLS_PER_TICK: usize = 24;
pub const OWED_BELT_CALLS_PER_OUTPOINT: usize = 3;
/// A candidate no rung answered (every rung faulted, rate-limited or closed) is looked at again this much later.
pub const OWED_BELT_UNANSWERED_RETRY_MS: i64 = 300_000;
/// The rung's circuit: this many faults in one tick close it for the rest of the tick (`tracker_host::pacing`'s 3).
pub const OWED_BELT_RUNG_FAULTS: u8 = 3;
/// The singleton actor's name in `BOARD_VIEW` (fleet-wide: one belt whatever the number of readers or isolates).
pub const OWED_BELT_ACTOR: &str = "owed-belt:v1";
/// The `owed_belt_state` row the tick writes: its cursor (the last outpoint asked) and its stamp.
pub const OWED_BELT_STATE_NAME: &str = "belt";
/// The rungs, by courier host. `other` is any host the ladders reach that is none of the three.
pub const RUNGS: [&str; 4] = ["woc", "bitails", "bananablocks", "other"];

pub const CANDIDATES_SQL: &str = "SELECT identity,outpoint,gameId,family,facts,COALESCE(claimable, CASE json_type(facts,'$.claimable') WHEN 'true' THEN 1 WHEN 'false' THEN 0 END) AS claimable FROM owed_rows INDEXED BY idx_owed_rows_belt WHERE beltDueAtMs<=?1 ORDER BY beltDueAtMs,outpoint LIMIT ?2";
/// A candidate no rung answered: its row and facts untouched, its next look dated once (never pushed further by a
/// second fault in the same tick: the statement sets, it does not add).
pub const REARM_SQL: &str = "UPDATE owed_rows SET beltDueAtMs=?3 WHERE identity=?1 AND outpoint=?2 AND beltDueAtMs IS NOT NULL";
pub const STATE_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES (?1,?2,?3) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// The backlog gauge: SET (not added), counted over the partial index.
pub const DUE_GAUGE_SQL: &str = "INSERT INTO ops_counters(name,value) SELECT 'owed_belt_due_total',COUNT(*) FROM owed_rows INDEXED BY idx_owed_rows_belt WHERE beltDueAtMs<=?1 ON CONFLICT(name) DO UPDATE SET value=excluded.value";

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
    /// Candidates the tick's call budget left untouched (still due, first by due time next tick).
    pub left: usize,
    pub calls: usize,
    pub faults: [u64; 4],
    pub write_faults: Vec<(String, String)>,
    /// The tick had no chain tip: nothing was asked.
    pub no_tip: bool,
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
    let rows = db
        .run_batch(vec![Stmt {
            sql: CANDIDATES_SQL.into(),
            binds: vec![
                Bind::Int(now),
                Bind::Int(OWED_BELT_OUTPOINTS_PER_TICK as i64),
            ],
        }])
        .await?
        .into_iter()
        .next()
        .map(|o| o.rows)
        .unwrap_or_default();
    let gate = Gate::new(courier);
    let mut report = TickReport {
        candidates: rows.len(),
        ..Default::default()
    };
    let mut cursor = None;
    for r in rows {
        let text = |k: &str| r[k].as_str().unwrap_or_default().to_owned();
        let c = Candidate {
            identity: text("identity"),
            outpoint: text("outpoint"),
            game_id: text("gameId"),
            family: text("family"),
            facts: r["facts"]
                .as_str()
                .and_then(|f| serde_json::from_str(f).ok())
                .unwrap_or(Value::Null),
            claimable: r["claimable"].as_i64().map(|n| n != 0),
        };
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
        match found {
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
                    Ok(_) => report.found += 1,
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
                sql: DUE_GAUGE_SQL.into(),
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

async fn rearm(db: &impl JudgeDb, c: &Candidate, now: i64, report: &mut TickReport) {
    let stmt = Stmt {
        sql: REARM_SQL.into(),
        binds: vec![
            Bind::Text(c.identity.clone()),
            Bind::Text(c.outpoint.clone()),
            Bind::Int(now + OWED_BELT_UNANSWERED_RETRY_MS),
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
            .find("crate::owed_belt::tick_env(&self.env).await")
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
