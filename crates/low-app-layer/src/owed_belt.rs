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
/// FIX-1049 (bsv-low #1049, L58 pair 17): how long after the record FIRST held the sweep's bytes (its first sight,
/// `sweepFirstSeenAtMs`: `owed_spender_bytes.createdAtMs`, written once) a look at its hop is YOUNG ([`young_miss`]):
/// three blocks' worth. Inside it the belt re-asks on its next tick; past it the back-off runs as for any look that
/// changed nothing. The stamp is never renewed (a later find keeps it, ON CONFLICT DO NOTHING), so one sweep costs at
/// most `OWED_BELT_YOUNG_MISS_MS / OWED_BELT_TICK_MS` = 15 young looks while the record holds its bytes (8 days, the
/// store's retention; a row swept and written again starts a new window, 15 looks more).
pub const OWED_BELT_YOUNG_MISS_MS: i64 = 1_800_000;
/// FIX-1049-FOLD-1 (fold 5, A NOTE-2, B RISK): young looks take at most half of a tick's places while rows that are
/// not young compete for them, and never a reserved place such a row wants; as folded (FIX-1049-FOLD-2, fold 2) they
/// take every place the others leave ([`places`]).
pub const OWED_BELT_YOUNG_PER_TICK: usize = OWED_BELT_OUTPOINTS_PER_TICK / 2;
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
/// bsv-low #953: a find's dirty mark, the `hop` source keyed by the row's outpoint (the pair `owed_events` routes and
/// [`READ_HOP_SQL`](crate::owed_events_publish::READ_HOP_SQL) reads), class `?5` (1 when it carries evidence of a
/// spend: the read withholds the game's presses until the judge has read it), the trigger's conflict rule.
pub const FIND_MARK_SQL: &str = "INSERT INTO owed_dirty (identity,gameId,source,sourceKey,markedAtMs,downgrade) VALUES (?1,?2,'hop',?3,?4,?5) ON CONFLICT(identity,gameId,source,sourceKey) DO UPDATE SET generation=excluded.generation,source=excluded.source,sourceKey=excluded.sourceKey,markedAtMs=excluded.markedAtMs,downgrade=MAX(owed_dirty.downgrade,excluded.downgrade)";
/// #953: the find's pair's source.
pub const FIND_MARK_SOURCE: &str = "hop";
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
/// FIX-1027-FOLD-1: the outpoints the operator's re-ask lever ([`reask`]) asked.
pub const COUNTER_REASKED: &str = "owed_belt_reasked_total";
/// FIX-1049-FOLD-1 (fold 6, C NOTE-1): a GAUGE, set each tick: the ROWS the tick looked at in their young window
/// ([`young_miss`]), each once, whatever path re-dated it. A count of rows in a young wait, never of re-dates; no
/// `_total` (FIX-1049-FOLD-2, the round-2 ensemble's C NOTE-3: a gauge, never a total).
pub const COUNTER_YOUNG_ROWS: &str = "owed_belt_young_rows";
/// FIX-1057-FOLD-1 (the FIX-1057 ensemble's C LOW-3, A LOW-1, B LOW-1): GAUGES, set each tick: the AGE (ms) of the
/// oldest due row the tick READ, by class: the first class (the block-dated courier-only sweeps), the second class's
/// rows that are not young, and its young rows ([`OldestDue`]). Stored as the age PLUS ONE, `0` where the tick read no
/// due row of the class (the overlay's counter read clamps a negative to 0; served `null` there, the age otherwise).
/// A class the others starve shows as its age growing tick after tick while `due` does not fall: under a first-class
/// flood with young rows due, the second class's rows that are not young drain one a tick, and
/// `oldestDueAgeMs.second` grows by about a tick for every such row queued ahead of the oldest. bsv-low #1063 (the
/// FIX-1057 delta lens's NOTE-1): the meaning moves with the phase. In Phase C the tick reads the actors' asks
/// ([`ask_candidates`]), which have no first-class walk: every due ask is second or young, so `first` is always `0`
/// (served `null`) and the block-dated courier-only sweeps an actor publishes count under `second`. NOTE-2: a tick
/// with no chain tip returns before the gauge batch, so all three ages (and the young gauge) keep the last tick's
/// values through a tip outage.
pub const COUNTER_OLDEST_DUE_FIRST: &str = "owed_belt_oldest_due_age_ms_first";
pub const COUNTER_OLDEST_DUE_SECOND: &str = "owed_belt_oldest_due_age_ms_second";
pub const COUNTER_OLDEST_DUE_YOUNG: &str = "owed_belt_oldest_due_age_ms_young";
/// The tick's gauges in ONE statement: `?1` the young rows, `?2`..`?4` the oldest due ages (first, second, young).
pub const YOUNG_GAUGE_SQL: &str = "INSERT INTO ops_counters(name,value) VALUES ('owed_belt_young_rows',?1),('owed_belt_oldest_due_age_ms_first',?2),('owed_belt_oldest_due_age_ms_second',?3),('owed_belt_oldest_due_age_ms_young',?4) ON CONFLICT(name) DO UPDATE SET value=excluded.value";

/// FIX-1057-FOLD-1: the oldest DUE time among the due rows a tick read, by class (the tick's candidates and the rows
/// its places cut; never a scan of the due set, so a class's oldest row past what the tick reads is not seen: the
/// walk holds its cursor at an identity whose due row was cut, so a starved row is read again on every tick).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OldestDue {
    pub first: Option<i64>,
    pub second: Option<i64>,
    pub young: Option<i64>,
}

impl OldestDue {
    fn note(slot: &mut Option<i64>, due: Option<i64>, now_ms: i64) {
        if let Some(d) = due.filter(|d| *d <= now_ms) {
            *slot = Some(slot.map_or(d, |o| o.min(d)));
        }
    }
    /// The gauge's value: the class's age at `now_ms` plus one, `0` with none read.
    pub fn gauge(slot: Option<i64>, now_ms: i64) -> i64 {
        slot.map_or(0, |d| now_ms.saturating_sub(d).max(0) + 1)
    }
}
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
    v.push(COUNTER_REASKED.to_owned());
    v.push(COUNTER_YOUNG_ROWS.to_owned());
    v.push(COUNTER_OLDEST_DUE_FIRST.to_owned());
    v.push(COUNTER_OLDEST_DUE_SECOND.to_owned());
    v.push(COUNTER_OLDEST_DUE_YOUNG.to_owned());
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

/// D2-ACTOR-B-FOLD-10 (the fifth delta lens's NEW5-M2): does this row rest on a courier find of an UNCONFIRMED spend
/// (a hop row the chain rung found spent, `chainProbe: "spent"`, with no confirmation, `spentConfirmed` not `true`)?
/// Such evidence is durable (bsv-low #953) and only an Outpoint look retires it, so the belt asks the OUTPOINT for it
/// (never the spender: a spender's raw answers nothing about a spend that never mines), dated at its backoff
/// (`owed_judge::belt_due`): a later look that finds it unspent clears the evidence and the hop's press returns; one
/// that finds it confirmed refreshes `spentConfirmed` and the question ends. Never a press over a spend the couriers
/// still hold: the row stays `unbound` until a look says unspent.
pub fn rests_on_unconfirmed_find(facts: &Value) -> bool {
    facts["hopTxid"].is_string()
        && facts["chainProbe"] == "spent"
        && facts["spentConfirmed"] != true
}

/// FIX-1049 (bsv-low #1049, L58 pair 17), as folded (FIX-1049-FOLD-1, folds 3 and 4): is a look at this row's
/// outpoint YOUNG? The row waits on a spender the COURIERS named for the hop's outpoint, filed or not (the belt's own
/// find of the live spender is the hint): a courier-bytes payout whose sweep is seen and not mined
/// ([`payout_waits_on_block`], FIX-988's ask at the hop), or a swept hop's landing wait (`sweepLanding`) whose sweep's bytes the record holds;
/// and the record first held that sweep's bytes (`sweepFirstSeenAtMs`, written once, never renewed by a later find
/// or hint) less than [`OWED_BELT_YOUNG_MISS_MS`] ago. Such a miss is the network's relay, not an answer: the belt
/// asks again on its next tick, never after the 300 s floor or a doubled period. A row with no first sight (a landing
/// wait only the tracker names), or an old one, backs off as before.
pub fn young_miss(facts: &Value, now_ms: i64) -> bool {
    let waits = facts["sweepLanding"] == true
        || (facts["creditKind"] == "courier-bytes" && payout_waits_on_block(facts));
    waits
        && facts["sweepFirstSeenAtMs"]
            .as_i64()
            .filter(|at| *at > 0)
            .is_some_and(|at| (0..OWED_BELT_YOUNG_MISS_MS).contains(&now_ms.saturating_sub(at)))
}

/// FIX-1049-FOLD-2 (fold 5): is this row a landing wait whose end (`sweepLandingUntilMs`) the clock has reached? Its
/// judgement moves with the clock alone, so the belt's look judges it with or without a courier's answer.
pub fn landing_ended(facts: &Value, now_ms: i64) -> bool {
    facts["sweepLanding"] == true
        && facts["sweepLandingUntilMs"]
            .as_i64()
            .is_some_and(|end| now_ms >= end)
}

/// FIX-1049-FOLD-1 (fold 5, A NOTE-2, B RISK): the tick's places by class with the young looks known: the first
/// class's due, the second class's due that are not young, and its young ones. [`class_places`]'s rule, the second
/// class's reserved places held for the rows that are NOT young (a young look only re-asks a miss, never the look a
/// press or a downgrade waits on), the young looks at most [`OWED_BELT_YOUNG_PER_TICK`] of the second class's other
/// places, and the rows that are not young the rest. Answers (first, second not young, young). With no young row it
/// is [`class_places`] exactly.
///
/// FIX-1049-FOLD-2 (fold 2; the round-2 ensemble's B LOW-1, A LOW-2): THE CAP IDLES NO PLACE. The half binds only
/// while rows that are not young compete: young looks also take every place the others leave (young rows alone due
/// take all eight). And the reserved places are the second class's AS A WHOLE: the rows that are not young take them
/// first, and a young row takes what they leave, so under a first-class flood a young row keeps a reserved place (main
/// gave L58's courier-bytes payout one, before it was young).
///
/// FIX-1057 (bsv-low #1057 item 3; round 3's B LOW-1, A NOTE-3 (b)): THE RESERVED PLACES ARE SPLIT when both kinds
/// are due. Under a first-class flood with two or more second-class rows that are not young due, fold 2 gave them
/// both reserved places and a young row none (`places(6, 2, 1)` was `(6, 2, 0)`), and when that young row was the
/// first identity walked the walk's cursor stayed on it every tick. Now the young rows keep ONE of the two reserved
/// places whenever a young row and a row that is not young are both due (`(6, 1, 1)`): so whenever a class of the
/// second has a due row it has a place, and the first row walked (whichever kind) is always given its turn, so the
/// cursor never freezes on it. Its cost, stated: under a first-class flood the second class's rows that are not young
/// drain one a tick (not two) while young rows are due, each sweep's young rows bounded at 15 looks.
pub fn places(first: usize, second: usize, young: usize) -> (usize, usize, usize) {
    let total = OWED_BELT_OUTPOINTS_PER_TICK;
    let reserved = (second + young).min(OWED_BELT_SECOND_CLASS_RESERVED);
    let first_n = first.min(total - reserved);
    let left = total - first_n;
    // the reserved places the rows that are not young hold: all of them, less one a young row keeps when both are due
    let held = second.min(reserved - usize::from(young > 0 && second > 0 && reserved > 1));
    let young_n = young
        .min(left - held)
        .min(OWED_BELT_YOUNG_PER_TICK.max(left.saturating_sub(second)));
    let second_n = second.min(left - young_n);
    (first_n, second_n, young_n)
}

/// FIX-1057 (bsv-low #1057 item 7; round 3's C LOW-2): THE ONE DATING RULE of a row's next look, which the four sites
/// call (the judge's `owed_judge::belt_due`, Phase A's [`backoff`], Phase C's [`ask_backoff`], and [`rearm_due`] for
/// both phases' unanswered looks). A young miss ([`young_miss`]) is looked at on the belt's next tick. Any other row
/// at `base_due` (the site's own period), except that a landing wait's next look is never later than the wait's end
/// (`sweepLandingUntilMs`, while it is ahead), so the look that re-judges the row there gives the press back
/// (FIX-1049-FOLD-1, fold 2). A change to the rule moves every phase at once. FIX-1057-FOLD-1 (the ensemble's C
/// DELETE): the young date and the landing cap live here alone, so "no site dates by its own copy" holds by
/// structure.
pub fn next_look(facts: &Value, now_ms: i64, base_due: i64) -> i64 {
    if young_miss(facts, now_ms) {
        return now_ms + OWED_BELT_TICK_MS;
    }
    match facts["sweepLandingUntilMs"].as_i64() {
        Some(end) if facts["sweepLanding"] == true && end > now_ms => base_due.min(end),
        _ => base_due,
    }
}

/// FIX-988: a payout row whose spend is seen and not mined (`owed::derive_owed_rows`: `chainWait: "block"` exactly
/// while the sweep is unconfirmed).
pub fn payout_waits_on_block(facts: &Value) -> bool {
    facts["chainWait"] == "block"
}

pub fn ask_of(c: &Candidate) -> Option<Ask> {
    let fact = |k: &str| {
        c.facts[k]
            .as_str()
            .filter(|t| crate::logic::valid_txid(t))
            .map(str::to_ascii_lowercase)
    };
    match c.family.as_str() {
        // FIX-988 (bsv-low #988, L54's S3): a courier-bytes payout whose sweep is not yet confirmed waits on the
        // SWEEP's block, which only the hop's spend answers (the courier's mined word on the outpoint the row is
        // keyed by); its home outputs are a confirmed sweep's alone (`owed::courier_home_outputs`), so a home ask
        // answered nothing and re-armed with no call, for good.
        "payout" if c.facts["creditKind"] == "courier-bytes" && payout_waits_on_block(&c.facts) => {
            split_outpoint(&c.outpoint).map(|(txid, vout)| Ask::Outpoint { txid, vout })
        }
        "payout" if c.facts["creditKind"] == "courier-bytes" => Some(Ask::Home),
        "unbound" if rests_on_unconfirmed_find(&c.facts) => {
            split_outpoint(&c.outpoint).map(|(txid, vout)| Ask::Outpoint { txid, vout })
        }
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
    /// bsv-low #953: the durable evidence of a spend the look found, keyed by the spent outpoint
    /// (`hops_view::probe_evidence_key`): never aged out while the outpoint's index row stands.
    pub evidence: Vec<ProbeMemo>,
    /// #953: the evidence keys a look that found the outpoint UNSPENT retires.
    pub cleared: Vec<String>,
    /// #953: the subject the find marks dirty and the pair it publishes (`identity`, `gameId`, the row's outpoint):
    /// a find is an event, so a bootstrapped actor re-judges on it in every phase.
    pub subject: Option<(String, String, String)>,
    pub bytes: Vec<(String, StoredSpenderBytes)>,
    pub hop_words: Vec<(String, HopChainWord)>,
    pub proven: Vec<String>,
    pub home_words: Vec<(String, HomeSpendWord)>,
}
impl Find {
    /// FIX-1049-FOLD-2 (fold 5): the empty find of a candidate's own subject: no fact, only the row's mark and pair, so
    /// the judge that reads it re-judges the row at the look's instant (each phase's own road, as any find).
    pub fn of_subject(c: &Candidate) -> Find {
        Find {
            subject: Some((c.identity.clone(), c.game_id.clone(), c.outpoint.clone())),
            ..Find::default()
        }
    }
    fn is_empty(&self) -> bool {
        self.memos.is_empty() && self.bytes.is_empty() && self.home_words.is_empty()
    }
    /// #953: the find's dirty mark (`hop`, the row's outpoint): a downgrade when it carries evidence of a spend.
    pub fn mark(&self, now_ms: i64) -> Option<Stmt> {
        let (identity, game, outpoint) = self.subject.as_ref()?;
        Some(Stmt {
            sql: FIND_MARK_SQL.into(),
            binds: vec![
                Bind::Text(identity.to_ascii_lowercase()),
                Bind::Text(game.clone()),
                Bind::Text(outpoint.to_ascii_lowercase()),
                Bind::Int(now_ms),
                Bind::Int(i64::from(!self.evidence.is_empty())),
            ],
        })
    }
    /// #953: the find's mark as the judge's own (the D1 judge that reads the find in its batch acknowledges it).
    pub fn owned_mark(&self) -> Vec<owed_judge::FilingMark> {
        self.subject
            .iter()
            .map(|(_, _, outpoint)| owed_judge::FilingMark {
                source: FIND_MARK_SOURCE.into(),
                source_key: outpoint.to_ascii_lowercase(),
            })
            .collect()
    }
    /// #953: note the find's pair for the invocation's publish (the frame every writer sends).
    pub fn note(&self) {
        if let Some((_, _, outpoint)) = &self.subject {
            crate::owed_events_publish::note(FIND_MARK_SOURCE, outpoint);
        }
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
        for m in &self.evidence {
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
        for k in &self.cleared {
            out.push(Stmt {
                sql: crate::hops_view::PROBE_EVIDENCE_CLEAR_SQL.into(),
                binds: vec![Bind::Text(k.clone())],
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
        out.extend(self.mark(now_ms));
        out
    }
    /// The same facts the statements write, into the judge's hydrated inputs. `now_ms` is the stamp
    /// [`Find::statements`] binds (FIX-1057, bsv-low #1057 item 4; round 3's B LOW-2): the first sight a new spender's
    /// bytes get is the record's own `createdAtMs`, never the hydration's clock (a later read of `now_ms`).
    ///
    /// FIX-1057-FOLD-1 (the ensemble's C LOW-1, the re-derived clock): the judgement's instant is the TICK'S, the one
    /// clock read a tick takes and passes down (`i.now_ms`, which the judge stamps `judgedAtMs` and dates the row's
    /// next look from), never the hydration's later read of the clock.
    pub fn apply(&self, i: &mut owed_judge::JudgeInputs, now_ms: i64) {
        i.now_ms = now_ms;
        for (k, w) in &self.hop_words {
            i.hop_chain.insert(k.clone(), w.clone());
        }
        for (txid, b) in &self.bytes {
            // FIX-1049-FOLD-1: the statement's ON CONFLICT DO NOTHING keeps a held row's stamp; a new row's is the
            // judgement's instant, the one `statements` binds. FIX-1049-FOLD-2 (the round-2 ensemble's B NOTE-1): only
            // where `statements` writes the bytes (`to_json_capped`), so the first sight is the record's own stamp and
            // no judge serves one the record does not hold (the actor, and every later judgement, would read none)
            if b.to_json_capped().is_some() {
                i.spender_first_seen.entry(txid.clone()).or_insert(now_ms);
            }
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
        // bsv-low #953: a spend is durable evidence keyed by the spent outpoint; an unspent answer retires it.
        if memo.spent {
            find.evidence.push(ProbeMemo {
                outpoint: crate::hops_view::probe_evidence_key(txid, vout),
                ..memo.clone()
            });
        } else {
            find.cleared
                .push(crate::hops_view::probe_evidence_key(txid, vout));
        }
        find.memos.push(memo);
    }
    if let (Some(sp), Some(raw)) = (probe.spending_txid.as_deref(), raw) {
        if let Some(b) = bytes_of_raw(raw, sp) {
            find.bytes.push((sp.to_ascii_lowercase(), b));
        }
    }
    find
}

/// The ask of one candidate through the gate. `None`: no rung answered (or the row owes nothing askable). The find
/// names its subject (bsv-low #953: it marks that game and publishes its pair).
async fn ask<C: Courier>(
    db: &impl JudgeDb,
    gate: &Gate<'_, C>,
    c: &Candidate,
    now: i64,
) -> Result<Option<Find>, String> {
    Ok(ask_inner(db, gate, c, now).await?.map(|mut find| {
        find.subject = Some((c.identity.clone(), c.game_id.clone(), c.outpoint.clone()));
        find
    }))
}

async fn ask_inner<C: Courier>(
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
            // a home output is no hop: its spend has its own latch, never the hop's evidence
            find.evidence.clear();
            find.cleared.clear();
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
    /// FIX-1049-FOLD-1 (fold 6): the ROWS this tick looked at in their young window ([`young_miss`]), each once.
    pub young_rows: usize,
    /// FIX-1057-FOLD-1: the oldest due time among the due rows this tick read, by class (the gauges').
    pub oldest_due: OldestDue,
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
async fn due_candidates(
    db: &impl JudgeDb,
    now: i64,
    tip: u64,
) -> Result<(Vec<Value>, OldestDue), String> {
    let first = |out: Vec<crate::pot_hook::StmtOut>| {
        out.into_iter().next().map(|o| o.rows).unwrap_or_default()
    };
    let text = |r: &Value, k: &str| r[k].as_str().map(str::to_owned);
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
    let mut oldest = OldestDue::default();
    if let Some((due, _, _)) = sweeps.first() {
        OldestDue::note(&mut oldest.first, Some(*due), now);
    }
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
            // FIX-1049-FOLD-1 (fold 5): twice a tick's worth, so the places a young look may not take are filled by
            // the next turns' rows that are not young
            if second_class.len() < 2 * OWED_BELT_OUTPOINTS_PER_TICK {
                second_class.push((at, id.clone(), outpoint.to_owned()));
            }
        }
    }
    // The candidates' rows, read by primary key: every first-class row and every second-class turn (at most a tick's
    // worth each), so the places know which second-class rows are young (FIX-1049-FOLD-1, fold 5).
    let read = |pairs: Vec<(String, String)>| async move {
        if pairs.is_empty() {
            return Ok::<_, String>(Vec::new());
        }
        let stmts = pairs
            .iter()
            .map(|(id, outpoint)| Stmt {
                sql: CANDIDATES_SQL.into(),
                binds: vec![Bind::Text(id.clone()), Bind::Text(outpoint.clone())],
            })
            .collect();
        Ok(db
            .run_batch(stmts)
            .await?
            .into_iter()
            .map(|o| o.rows.into_iter().next())
            .collect::<Vec<_>>())
    };
    let first_rows = read(first_class).await?;
    let second_rows = read(
        second_class
            .iter()
            .map(|(_, id, outpoint)| (id.clone(), outpoint.clone()))
            .collect(),
    )
    .await?;
    let young_of = |r: &Option<Value>| {
        r.as_ref().is_some_and(|r| {
            let facts: Value = r["facts"]
                .as_str()
                .and_then(|f| serde_json::from_str(f).ok())
                .unwrap_or(Value::Null);
            young_miss(&facts, now)
        })
    };
    let young = second_rows.iter().filter(|r| young_of(r)).count();
    // The places (D3-M1, and fold 5): the second class is given at least its reserved places whenever it holds a due
    // row that is not young, the first class the rest; young looks at most half, in the places left.
    let (first_n, second_n, young_n) = places(first_rows.len(), second_rows.len() - young, young);
    let mut chosen: Vec<Value> = first_rows.into_iter().take(first_n).flatten().collect();
    let (mut old_taken, mut young_taken) = (0, 0);
    let mut given: Vec<usize> = Vec::new();
    let mut cut: Vec<usize> = Vec::new();
    for ((at, _, _), row) in second_class.iter().zip(second_rows) {
        let is_young = young_of(&row);
        let due = row.as_ref().and_then(|r| r["beltDueAtMs"].as_i64());
        OldestDue::note(
            if is_young {
                &mut oldest.young
            } else {
                &mut oldest.second
            },
            due,
            now,
        );
        let take = if is_young {
            young_taken < young_n && {
                young_taken += 1;
                true
            }
        } else {
            old_taken < second_n && {
                old_taken += 1;
                true
            }
        };
        if take {
            given.push(*at);
            chosen.extend(row);
        } else {
            cut.push(*at);
        }
    }
    // The next tick starts after the last identity this one gave a turn (the whole walk when none was given one and
    // the second class was not cut short by its places). FIX-1049-FOLD-2 (fold 1; the round-2 ensemble's B MEDIUM-1,
    // A LOW-1): never past an identity holding a due row the places cut and given NO turn this tick, young or not
    // (D-L1: no identity holding a due row is passed unserved), so the next tick starts at it. Fold 1 of the first
    // round held the cursor back for a cut YOUNG row only, so a row that is not young, cut while a young row after it
    // was served, waited a revolution of the walk.
    let held_back = cut.iter().copied().filter(|at| !given.contains(at)).min();
    let last = match held_back {
        Some(0) => None,
        Some(p) => identities.get(p - 1).cloned(),
        None => given
            .iter()
            .max()
            .and_then(|at| identities.get(*at).cloned())
            .or(walked_to),
    };
    if let Some(last) = last {
        db.run_batch(vec![Stmt {
            sql: STATE_SQL.into(),
            binds: vec![
                Bind::Text(OWED_BELT_WALK_STATE_NAME.into()),
                Bind::Text(last),
                Bind::Int(now),
            ],
        }])
        .await?;
    }
    Ok((chosen, oldest))
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
async fn ask_candidates(db: &impl JudgeDb, now: i64) -> Result<(Vec<Value>, OldestDue), String> {
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
    // each due ask with its ledger due time (`tickedAtMs`: the published date, or the belt's own re-arm or back-off)
    let (due, due_at): (Vec<(String, Value)>, Vec<i64>) = page
        .iter()
        .filter_map(|r| {
            let at = r["tickedAtMs"].as_i64().filter(|d| *d <= now)?;
            let ask: Value = serde_json::from_str(r["cursor"].as_str()?).ok()?;
            Some(((r["name"].as_str()?.to_owned(), ask), at))
        })
        .unzip();
    let mut groups: Vec<(String, Vec<usize>)> = Vec::new();
    for (n, (_, ask)) in due.iter().enumerate() {
        let id = ask["identity"].as_str().unwrap_or_default().to_owned();
        match groups.iter_mut().find(|(g, _)| *g == id) {
            Some((_, at)) => at.push(n),
            None => groups.push((id, vec![n])),
        }
    }
    // FIX-1049-FOLD-1 (fold 5): the young asks at most half of the places, and never the places the rows that are
    // not young take ([`places`] with no first class).
    let young: Vec<bool> = due
        .iter()
        .map(|(_, ask)| young_miss(&ask["question"]["facts"], now))
        .collect();
    let young_due_n = young.iter().filter(|y| **y).count();
    let mut oldest = OldestDue::default();
    for (at, y) in due_at.iter().zip(&young) {
        let slot = if *y {
            &mut oldest.young
        } else {
            &mut oldest.second
        };
        OldestDue::note(slot, Some(*at), now);
    }
    let (_, old_n, young_n) = places(0, due.len() - young_due_n, young_due_n);
    let (mut old_taken, mut young_taken) = (0, 0);
    let mut taken: Vec<usize> = Vec::new();
    'turns: for turn in 0..OWED_BELT_OUTPOINTS_PER_TICK {
        for (_, at) in &groups {
            if let Some(n) = at.get(turn) {
                if taken.len() == OWED_BELT_OUTPOINTS_PER_TICK {
                    break 'turns;
                }
                let room = if young[*n] {
                    young_taken < young_n && {
                        young_taken += 1;
                        true
                    }
                } else {
                    old_taken < old_n && {
                        old_taken += 1;
                        true
                    }
                };
                if room {
                    taken.push(*n);
                }
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
    let chosen = taken
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
        .collect();
    Ok((chosen, oldest))
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
    find.note();
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
    find.note();
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
        |inputs| {
            // FIX-1057-FOLD-1: the tick's clock, as `Find::apply` gives Phase A's judgement
            for i in inputs.iter_mut() {
                i.now_ms = now;
            }
            Ok(())
        },
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
    // a young miss is dated by [`next_look`] whatever the look's shape; any other row only when the look changed
    // nothing (its period doubles)
    let young = young_miss(&facts, now);
    let before = look_shape(&c.family, &c.facts, c.reason.as_deref());
    let after = look_shape(
        row["family"].as_str().unwrap_or_default(),
        &facts,
        row["reason"].as_str(),
    );
    if !young && before != after {
        return;
    }
    let period = backoff_period(c.due_at_ms, c.judged_at_ms, (base_due - now).max(1));
    match db
        .run_batch(vec![Stmt {
            sql: ASK_DUE_SQL.into(),
            binds: vec![
                Bind::Text(ask_name(&c.identity, &c.outpoint)),
                Bind::Int(next_look(&facts, now, now + period)),
            ],
        }])
        .await
    {
        Ok(_) if young => {}
        Ok(_) => report.backed_off += 1,
        Err(e) => db.log(&format!(
            "[owed-belt] backoff of {} faulted: {e}",
            c.outpoint
        )),
    }
}

async fn rearm_ask(db: &impl JudgeDb, c: &Candidate, now: i64, report: &mut TickReport) {
    if set_ask_due(db, c, rearm_due(c, now)).await {
        report.rearmed += 1;
    }
}

/// FIX-1049: an unanswered look's next date: the next tick for a young miss ([`young_miss`]), else
/// [`unanswered_period`], never past a landing wait's end ([`next_look`]).
pub fn rearm_due(c: &Candidate, now: i64) -> i64 {
    next_look(&c.facts, now, now + unanswered_period(now, c.judged_at_ms))
}

/// Phase C: set one ask's next look in the ledger.
async fn set_ask_due(db: &impl JudgeDb, c: &Candidate, due: i64) -> bool {
    let stmt = Stmt {
        sql: ASK_DUE_SQL.into(),
        binds: vec![
            Bind::Text(ask_name(&c.identity, &c.outpoint)),
            Bind::Int(due),
        ],
    };
    match db.run_batch(vec![stmt]).await {
        Ok(_) => true,
        Err(e) => {
            db.log(&format!(
                "[owed-belt] re-arm of {} faulted: {e}",
                c.outpoint
            ));
            false
        }
    }
}

/// FIX-1027-FOLD-1 (bsv-low #1027, the captain's item 1b): the re-ask lever's bound, one tick's outpoints a call (the
/// gate's 24 courier calls and 3 an outpoint bound the calls, as the tick's).
pub const OWED_BELT_REASK_MAX: usize = OWED_BELT_OUTPOINTS_PER_TICK;

/// The game a hop belongs to for the identity: its VERIFIED hop marker (`markerValid = 1`, round 2's LOW-2: the belt
/// dates only a row whose evidence verifies, `owed_judge::belt_due`), else the eviction's released evidence (a hop
/// whose game no such source names is not asked, skipped by name). Binds: identity, hop txid, vout.
pub const REASK_GAME_SQL: &str = "SELECT gameId FROM hopparty_records WHERE txid=?2 AND hopVout=?3 AND identity=?1 AND markerValid=1 UNION ALL SELECT gameId FROM released_hop_evidence WHERE hopTxid=?2 AND hopVout=?3 AND identity=?1 LIMIT 1";

/// What one re-ask call did: the tick's report over the named outpoints, and the ones it could not ask.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReaskReport {
    pub tick: TickReport,
    /// `(identity, outpoint, why)`: past the bound, not an outpoint, no game named, the game read faulted.
    pub skipped: Vec<(String, String, String)>,
}

/// FIX-1027-FOLD-1: THE RE-ASK OF NAMED HOP OUTPOINTS. The evidence a reorg clear DELETED before #1027 (beta's cron
/// clear of 2026-10-31 14:01:48Z, 101 rows, unmarked on 99e456a6) is asked by nothing: in Phase C the belt reads the
/// actors' published asks, and an actor that judged the spend `spent-elsewhere` (or retired the hop's payout) dates no
/// look. This pass asks each named `(identity, outpoint)` its OUTPOINT, at most [`OWED_BELT_REASK_MAX`] a call through
/// one [`Gate`], and writes each answer exactly as the tick does ([`ask_one`]): a spend found is the #953 evidence row,
/// the memo, the spender's bytes and the `hop` mark, handed back to the actor (Phase C) or judged in the batch (A, B);
/// an unspent answer retires the evidence. Asks nothing without a chain tip. The operator's road: page the fleet
/// compare's `diffs[].outpoints` into `POST /internal/owed-belt-reask`, then read the compare again.
///
/// A ONE-TIME REPAIR, never a routine road (round 2's LOW-2): it exists for the evidence the pre-FIX-1010 clears
/// deleted UNMARKED (no event, no actor told). Since FIX-1010 every clear marks and since #1027 a clear demotes rather
/// than deletes, so no write loses evidence without an event and the belt's own dating asks every look the judges
/// owe; once the fleet compare's diffs from that class are asked, the lever has no further use. An outpoint off the
/// compare's list costs one look's courier calls and a re-judge of that identity's game, never fabricated evidence
/// (only a courier's spent answer writes evidence).
pub async fn reask<C: Courier>(
    db: &impl JudgeDb,
    courier: &C,
    asks: &[(String, String)],
) -> Result<ReaskReport, String> {
    let now = db.now_ms() as i64;
    let mut out = ReaskReport::default();
    if db.tip().is_none() {
        out.tick.no_tip = true;
        db.log("[owed-belt] re-ask: no chain tip, nothing asked");
        return Ok(out);
    }
    let phase_c = !db.d1_judges();
    let gate = Gate::new(courier);
    for (n, (identity, outpoint)) in asks.iter().enumerate() {
        let skip = |why: &str| (identity.clone(), outpoint.clone(), why.to_owned());
        if n >= OWED_BELT_REASK_MAX {
            out.skipped.push(skip("past the call's bound"));
            continue;
        }
        let Some((txid, vout)) = split_outpoint(outpoint) else {
            out.skipped.push(skip("not an outpoint"));
            continue;
        };
        let identity = identity.to_ascii_lowercase();
        let game = match db
            .run_batch(vec![Stmt {
                sql: REASK_GAME_SQL.into(),
                binds: vec![
                    Bind::Text(identity.clone()),
                    Bind::Text(txid.clone()),
                    Bind::Int(i64::from(vout)),
                ],
            }])
            .await
        {
            Ok(o) => o
                .into_iter()
                .next()
                .and_then(|o| o.rows.into_iter().next())
                .and_then(|r| r["gameId"].as_str().map(str::to_owned)),
            Err(e) => {
                out.skipped
                    .push(skip(&format!("the game read faulted: {e}")));
                continue;
            }
        };
        let Some(game_id) = game else {
            out.skipped
                .push(skip("no verified hop marker or released evidence names it"));
            continue;
        };
        if gate.spent() {
            out.tick.left += 1;
            continue;
        }
        // An Outpoint ask (`ask_of`'s default arm), whatever the stored row says: the record lost the evidence the
        // stored row was judged on, so only the outpoint's spend can restore it.
        let c = Candidate {
            identity,
            outpoint: owed::outpoint_key(&txid, vout),
            game_id,
            family: "hop-stranded".into(),
            facts: Value::Object(Default::default()),
            claimable: None,
            reason: None,
            probe_tip: None,
            due_at_ms: None,
            judged_at_ms: None,
        };
        out.tick.candidates += 1;
        ask_one(db, &gate, &c, now, phase_c, &mut out.tick).await;
    }
    let state = gate.state.borrow().clone();
    out.tick.calls = state.tick_calls;
    out.tick.faults = state.fault_counts;
    let mut deltas = vec![
        (COUNTER_REASKED.to_owned(), out.tick.asked as u64),
        (COUNTER_FOUND.to_owned(), out.tick.found as u64),
    ];
    if out.tick.handed_back > 0 {
        deltas.push((
            crate::owed_actor::COUNTERS[9].to_owned(),
            out.tick.handed_back as u64,
        ));
    }
    for (rung, n) in out.tick.faults.iter().enumerate() {
        if *n > 0 {
            deltas.push((fault_counter(rung), *n));
        }
    }
    db.counters(deltas).await;
    db.log(&format!(
        "[owed-belt] re-ask: candidates={} asked={} found={} skipped={} left={} calls={} faults={:?} write_faults={}",
        out.tick.candidates,
        out.tick.asked,
        out.tick.found,
        out.skipped.len(),
        out.tick.left,
        out.tick.calls,
        out.tick.faults,
        out.tick.write_faults.len()
    ));
    Ok(out)
}

/// FIX-1049-FOLD-2 (fold 5; the round-2 ensemble's B LOW-4), made one pure function by FIX-1057 (bsv-low #1057 item
/// 2; round 3's A LOW-2): a landing wait at or past its end is judged on this look whether a rung answered or not.
/// The press it gives back needs no new fact (the clock passed the wait), so an unanswered look (`Ok(None)`) and a
/// look whose ask FAULTED (`Err`, named in the answer's second half for the log) are both an empty find of the row's
/// subject ([`Find::of_subject`]: its mark, its judgement, its event, each phase's own road), never only a re-arm that
/// leaves the row saying "waiting" until a rung answers or the age repair runs. A find, and every row whose wait has
/// not ended, pass unchanged. Stated (round 3's B NOTE-3): a landing row asks its OUTPOINT, whose ask never answers
/// `Err` today (only a payout's home ask hydrates); the arm stands for any ask that may, and is run by its pin.
pub fn ended_wait_look(
    found: Result<Option<Find>, String>,
    c: &Candidate,
    now: i64,
) -> (Result<Option<Find>, String>, Option<String>) {
    if !landing_ended(&c.facts, now) {
        return (found, None);
    }
    match found {
        Ok(Some(find)) => (Ok(Some(find)), None),
        Ok(None) => (Ok(Some(Find::of_subject(c))), None),
        Err(e) => (Ok(Some(Find::of_subject(c))), Some(e)),
    }
}

/// One candidate's ask and what its answer writes, as the tick runs it (and the re-ask lever, FIX-1027-FOLD-1): the
/// ask through the gate, then per phase the find written and handed back (C), the actor then the D1 judge (B), or the
/// D1 judge with the find in its batch (A); an unanswered ask re-armed.
async fn ask_one<C: Courier>(
    db: &impl JudgeDb,
    gate: &Gate<'_, C>,
    c: &Candidate,
    now: i64,
    asks: bool,
    report: &mut TickReport,
) {
    gate.begin_outpoint();
    let before = gate.state.borrow().tick_calls;
    let (found, fault) = ended_wait_look(ask(db, gate, c, now).await, c, now);
    if let Some(e) = fault {
        db.log(&format!(
            "[owed-belt] {} ask faulted, its landing wait ended: judged on the stored evidence: {e}",
            c.outpoint
        ));
    }
    // FIX-1018 (bsv-low #1018): the belt ASKED this outpoint, so its find's judge (the D1 judge's, and the actor's
    // through the hand-back's `beltAsked`) dates it fresh; every other dated row keeps its earlier date.
    let _asked = owed_judge::belt_asked_scope(&c.identity, std::slice::from_ref(&c.outpoint));
    if gate.state.borrow().tick_calls > before {
        report.asked += 1;
    }
    let subject = Subject {
        identity: c.identity.clone(),
        game_id: c.game_id.clone(),
    };
    if asks {
        match found {
            Ok(Some(find)) => ask_find(db, c, &find, now, report).await,
            Ok(None) => rearm_ask(db, c, now, report).await,
            Err(e) => {
                db.log(&format!("[owed-belt] {} ask faulted: {e}", c.outpoint));
                report.write_faults.push((c.outpoint.clone(), e));
                rearm_ask(db, c, now, report).await;
            }
        }
        return;
    }
    match found {
        // Phase B (the D2-ACTOR-B lens's HIGH-3): the find's facts land alone, the actor applies them, and only
        // then the D1 judge (whose batch acknowledges the marks the facts wrote): an actor fault keeps them, so
        // the read withholds the game's presses and the sweep re-publishes the pair.
        Ok(Some(find)) if db.actor_reads() => {
            match phase_b_find(db, c, &subject, &find, now).await {
                Ok(()) => {
                    report.found += 1;
                    report.handed_back += 1;
                    backoff(db, c, now, report).await;
                }
                Err(e) => {
                    db.log(&format!("[owed-belt] {} find faulted: {e}", c.outpoint));
                    report.write_faults.push((c.outpoint.clone(), e));
                    rearm(db, c, now, report).await;
                }
            }
        }
        Ok(Some(find)) => {
            let writes = find.statements(now);
            let identity = c.identity.clone();
            // bsv-low #953: the D1 judge reads the find in this batch, so it acknowledges the find's own mark
            // here (D-H1 (c): nothing left dirty); the actor is called directly below. In Phase C (no D1 judge)
            // the mark stands for the consumer to carry to the actor.
            let owned = find.owned_mark();
            let judged =
                owed_judge::judge_with_write(db, &[subject], "belt", writes, &owned, |inputs| {
                    for i in inputs.iter_mut().filter(|i| i.identity == identity) {
                        find.apply(i, now);
                    }
                    Ok(())
                })
                .await;
            match judged {
                Ok(_) => {
                    report.found += 1;
                    find.note();
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
                    backoff(db, c, now, report).await;
                }
                Err(e) => {
                    db.log(&format!(
                        "[owed-belt] {} find's write faulted: {e}",
                        c.outpoint
                    ));
                    report.write_faults.push((c.outpoint.clone(), e));
                    rearm(db, c, now, report).await;
                }
            }
        }
        Ok(None) => rearm(db, c, now, report).await,
        Err(e) => {
            db.log(&format!("[owed-belt] {} ask faulted: {e}", c.outpoint));
            report.write_faults.push((c.outpoint.clone(), e));
            rearm(db, c, now, report).await;
        }
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
    let (rows, oldest_due) = if asks {
        ask_candidates(db, now).await?
    } else {
        due_candidates(db, now, db.tip().unwrap_or_default()).await?
    };
    let gate = Gate::new(courier);
    let mut report = TickReport {
        candidates: rows.len(),
        oldest_due,
        ..Default::default()
    };
    let mut cursor = None;
    for r in rows {
        let c = candidate_of(&r);
        if gate.spent() {
            report.left += 1;
            continue;
        }
        cursor = Some(c.outpoint.clone());
        if young_miss(&c.facts, now) {
            report.young_rows += 1;
        }
        ask_one(db, &gate, &c, now, asks, &mut report).await;
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
            Stmt {
                sql: YOUNG_GAUGE_SQL.into(),
                binds: vec![
                    Bind::Int(report.young_rows as i64),
                    Bind::Int(OldestDue::gauge(report.oldest_due.first, now)),
                    Bind::Int(OldestDue::gauge(report.oldest_due.second, now)),
                    Bind::Int(OldestDue::gauge(report.oldest_due.young, now)),
                ],
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
        "[owed-belt] tick: candidates={} asked={} found={} rearmed={} young={} left={} calls={} faults={:?} write_faults={}",
        report.candidates,
        report.asked,
        report.found,
        report.rearmed,
        report.young_rows,
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
    // a young miss is dated by [`next_look`] whatever the look's shape; any other row only when the look changed
    // nothing (its period doubles)
    let young = young_miss(&facts, now);
    let before = look_shape(&c.family, &c.facts, c.reason.as_deref());
    let now_shape = look_shape(
        row["family"].as_str().unwrap_or_default(),
        &facts,
        row["reason"].as_str(),
    );
    if !young && before != now_shape {
        return;
    }
    let period = backoff_period(c.due_at_ms, c.judged_at_ms, (base_due - now).max(1));
    let mut binds = binds;
    binds.push(Bind::Int(next_look(&facts, now, now + period)));
    match db
        .run_batch(vec![Stmt {
            sql: BACKOFF_SQL.into(),
            binds,
        }])
        .await
    {
        Ok(_) if young => {}
        Ok(_) => report.backed_off += 1,
        Err(e) => db.log(&format!(
            "[owed-belt] backoff of {} faulted: {e}",
            c.outpoint
        )),
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
    let due = rearm_due(c, now);
    let stmt = Stmt {
        sql: REARM_SQL.into(),
        binds: vec![
            Bind::Text(c.identity.clone()),
            Bind::Text(c.outpoint.clone()),
            Bind::Int(due),
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

/// FIX-986 (READ-986 item 4): the fleet compare's ring actors (`owed_actor::compare::shard_actor_name`, one a shard,
/// FIX-998) asked to keep their alarms, as [`arm`] asks the belt's; every shard asked, the first refusal named.
pub async fn arm_fleet_compare(env: &worker::Env) -> worker::Result<()> {
    let shards = crate::owed_actor::compare::shards_env(env);
    let ns = env.durable_object("BOARD_VIEW")?;
    let mut refused = Vec::new();
    for k in 0..shards {
        let stub = ns
            .id_from_name(&crate::owed_actor::compare::shard_actor_name(k))?
            .get_stub()?;
        let response = stub
            .fetch_with_str(&format!(
                "https://board-view/owed-fleet-compare-arm?shard={k}"
            ))
            .await?;
        if response.status_code() != 200 {
            refused.push(k);
        }
    }
    if !refused.is_empty() {
        return Err(worker::Error::RustError(format!(
            "owed fleet compare arm refused for shards {refused:?}"
        )));
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
            "owed drain refused ({}): {}",
            response.status_code(),
            response.text().await?
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
        let tip = crate::routes::chaintracks_present_height_env(env, "owed")
            .await
            .ok();
        let judge =
            owed_judge::D1Judge::new(env, tip, crate::routes::written_off_before_ms_env(env))?;
        if repair_age {
            if let Err(error) =
                crate::owed_backfill::mark_aged_in(&judge, owed_judge::JudgeDb::d1_judges(&judge))
                    .await
            {
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
            return Ok(owed_judge::DrainReport {
                swept,
                ..Default::default()
            });
        }
        owed_judge::drain_dirty(&judge, owed_judge::OWED_DIRTY_SUBJECTS_PER_PASS)
            .await
            .map_err(worker::Error::RustError)
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
                worker::console_warn!(
                    "[owed] {} judgement deferred: {error}",
                    if repair_age { "scheduled" } else { "alarm" }
                );
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
    // D2-ACTOR-B-FOLD-16 (NEW9-L1): a drain that did work counts in the invocation's statement bound: the re-judge then
    // makes one judge call at most and the reconcile yields. A drain that faulted or reached its wall is counted busy.
    let drain_bound = drained
        .as_ref()
        .map_or(true, |r| r.swept > 0 || r.subjects > 0);
    let drain = match drained {
        Ok(report) => {
            serde_json::json!({"subjects": report.subjects, "faults": report.faults.len(), "swept": report.swept})
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
    // D2-ACTOR-B-FOLD-13 (bsv-low #958): D1's fleet re-judge after a judge-version move, paced (FOLD-15: the pace
    // lever, 32 games by default, two judge calls with the reconcile yielding; 0 to 32; at most 32 identities opened a
    // tick; FOLD-16: one judge call when the drain did work), BEFORE the reconcile so the sampler compares a re-judged
    // D1; one primary-key read a tick once done.
    let rejudged = meter
        .phase("owed.d1_rejudge", d1_rejudge_env(env, drain_bound))
        .await;
    let rejudge_report = rejudged.as_ref().ok().and_then(Option::clone);
    let rejudge = match rejudged {
        Ok(Some(r)) => serde_json::json!({
            "started": r.started, "identities": r.identities, "games": r.games, "faulted": r.faulted,
            "skipped": r.skipped, "done": r.done, "judgeCalls": r.judge_calls,
            "gamesPerTick": r.games_per_tick, "paused": r.paused, "drainBound": r.drain_bound,
        }),
        Ok(None) => Value::Null,
        Err(error) => {
            worker::console_warn!("[owed-actor] D1 re-judge faulted: {error}");
            serde_json::json!({"error": error})
        }
    };
    // bsv-low #912 (MEDIUM-10): the sampled digest reconcile on the same driver, one identity a 22.5 minutes.
    // D2-ACTOR-B-FOLD-15 (the captain's pace lever): a re-judge tick that made more than one judge call yields the
    // reconcile to the next alarm, so the invocation stays under D1's bound (`owed_actor::reconcile_yields`).
    let yields = rejudge_report
        .as_ref()
        .is_some_and(crate::owed_actor::reconcile_yields);
    let reconciled = if yields {
        Ok(None)
    } else {
        meter.phase("owed.reconcile", reconcile_env(env)).await
    };
    let reconcile = match reconciled {
        Ok(Some(r)) => serde_json::json!({
            "identity": r.identity, "diff": r.diff, "reapplied": r.reapplied, "noActor": r.no_actor,
        }),
        Ok(None) if yields => serde_json::json!({"yielded": true}),
        Ok(None) => Value::Null,
        Err(error) => {
            worker::console_warn!("[owed-actor] reconcile faulted: {error}");
            serde_json::json!({"error": error})
        }
    };
    let walls = meter.task_done().expect("one alarm task");
    worker::console_log!(
        "{}",
        walls.line().replace("[scheduled-pass]", "[owed-alarm]")
    );
    worker::Response::from_json(&serde_json::json!({
        "drain": drain, "tick": tick, "d1Rejudge": rejudge, "reconcile": reconcile,
        "wallMs": walls.wall_ms(),
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
    let report = tick(&judge, &WorkerCourier).await;
    // bsv-low #953: a find is an event: the pairs its writes noted are published as every writer's are (a pair whose
    // mark a judge already acknowledged is not sent; a publish that does not land leaves the mark to the sweep).
    if crate::owed_events::configured(env).await {
        crate::owed_events::publish_noted(env).await;
        // D2-ACTOR-B-FOLD-10 (NEW5-L2): the tick's published finds are called directly too, inline (the belt is a
        // writer like any other; its set was left held for the isolate's next taker).
        crate::owed_events::direct_held(env).await;
    }
    report
}

/// The alarm's reconcile step (bsv-low #912): the D1 judge's port (its flag, its actor calls).
pub async fn reconcile_env(
    env: &worker::Env,
) -> Result<Option<crate::owed_actor::ReconcileReport>, String> {
    if env.durable_object(crate::owed_actor::BINDING).is_err() {
        return Ok(None);
    }
    // bsv-low #953 item 6: the reconcile's re-apply carries the served tip (it carried none: its rows were rewritten
    // tip-unknown).
    let tip = crate::owed_events::tip_memoised(env, false).await;
    let judge = owed_judge::D1Judge::new(env, tip, crate::routes::written_off_before_ms_env(env))
        .map_err(|e| e.to_string())?;
    crate::owed_actor::reconcile_step(&judge, worker::Date::now().as_millis() as i64).await
}

/// The alarm's D1 re-judge step (bsv-low #958): the D1 judge's port under the served tip and the era.
pub async fn d1_rejudge_env(
    env: &worker::Env,
    drain_bound: bool,
) -> Result<Option<crate::owed_actor::RejudgeReport>, String> {
    let tip = crate::owed_events::tip_memoised(env, false).await;
    let judge = owed_judge::D1Judge::new(env, tip, crate::routes::written_off_before_ms_env(env))
        .map_err(|e| e.to_string())?;
    // FOLD-15: the render's pace (the lever's row wins), read once per alarm.
    let pace = env
        .var("OWED_D1_REJUDGE_GAMES_PER_TICK")
        .ok()
        .map(|v| v.to_string());
    crate::owed_actor::d1_rejudge_step_after_drain(
        &judge,
        worker::Date::now().as_millis() as i64,
        pace.as_deref(),
        drain_bound,
    )
    .await
}

/// FIX-1027-FOLD-1: `POST /internal/owed-belt-reask` (bearer `INTERNAL_TOKEN`), body
/// `{"asks": [{"identity": "<66 hex>", "outpoint": "<txid>:<vout>"}, ...]}`, 1 to [`OWED_BELT_REASK_MAX`] entries (more
/// is a 400, not clamped): one bounded [`reask`] pass, its finds published and called directly as the tick's are.
pub async fn reask_lever(
    mut req: worker::Request,
    env: &worker::Env,
) -> worker::Result<worker::Response> {
    if !crate::internal_events::internal_bearer_ok(&req, env) {
        return worker::Response::error("unauthorized", 401);
    }
    let body: Value = match req.json().await {
        Ok(b) => b,
        Err(_) => return worker::Response::error("body: {\"asks\": [{identity, outpoint}]}", 400),
    };
    let asks: Vec<(String, String)> = body["asks"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|e| {
                    Some((
                        e["identity"].as_str()?.to_owned(),
                        e["outpoint"].as_str()?.to_owned(),
                    ))
                })
                .collect()
        })
        .unwrap_or_default();
    if asks.is_empty() || asks.len() > OWED_BELT_REASK_MAX {
        return worker::Response::error(
            format!("asks: 1 to {OWED_BELT_REASK_MAX} entries of {{identity, outpoint}}"),
            400,
        );
    }
    crate::schema::ensure_latch_columns(env).await;
    let tip = crate::routes::chaintracks_present_height_env(env, "owed")
        .await
        .ok();
    let judge =
        match owed_judge::D1Judge::new(env, tip, crate::routes::written_off_before_ms_env(env)) {
            Ok(j) => j,
            Err(e) => return worker::Response::error(format!("owed belt re-ask: {e}"), 503),
        };
    let report = reask(&judge, &WorkerCourier, &asks).await;
    if crate::owed_events::configured(env).await {
        crate::owed_events::publish_noted(env).await;
        crate::owed_events::direct_held(env).await;
    }
    match report {
        Ok(r) => worker::Response::from_json(&serde_json::json!({
            "candidates": r.tick.candidates,
            "asked": r.tick.asked,
            "found": r.tick.found,
            "rearmed": r.tick.rearmed,
            "left": r.tick.left,
            "calls": r.tick.calls,
            "faults": {"woc": r.tick.faults[0], "bitails": r.tick.faults[1], "bananablocks": r.tick.faults[2], "other": r.tick.faults[3]},
            "writeFaults": r.tick.write_faults.len(),
            "handedBack": r.tick.handed_back,
            "noTip": r.tick.no_tip,
            "skipped": r.skipped.iter().map(|(i, o, w)| serde_json::json!({"identity": i, "outpoint": o, "why": w})).collect::<Vec<_>>(),
            "bound": OWED_BELT_REASK_MAX,
        })),
        Err(e) => worker::Response::error(format!("owed belt re-ask faulted: {e}"), 503),
    }
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
    /// FIX-1027-FOLD-1: the re-ask lever is routed, bearer-gated before its body, bounded at one tick's outpoints,
    /// and publishes and calls its finds as the tick does; its count is seeded by the overlay.
    #[test]
    fn fix1027f_1b_the_reask_lever_is_routed_gated_and_bounded() {
        let lib = squash(include_str!("lib.rs"));
        assert!(lib.contains(&squash(
            "req.path() == \"/internal/owed-belt-reask\" { let resp = owed_belt::reask_lever(req, &env).await;"
        )));
        let me = squash(include_str!("owed_belt.rs"));
        let lever = &me[me.find("pubasyncfnreask_lever(").unwrap()..];
        let lever = &lever[..lever.find("pubasyncfnlever(").unwrap()];
        let gate = lever.find("internal_bearer_ok(&req,env)").unwrap();
        assert!(gate < lever.find("req.json()").unwrap(), "the bearer first");
        assert!(lever.contains("asks.len()>OWED_BELT_REASK_MAX"));
        assert!(lever.contains("crate::owed_events::publish_noted(env).await;"));
        assert!(lever.contains("crate::owed_events::direct_held(env).await;"));
        assert_eq!(super::OWED_BELT_REASK_MAX, 8);
        assert!(super::counter_names().contains(&super::COUNTER_REASKED.to_owned()));
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

    /// FIX-1049-FOLD-1 (fold 3; A LOW-2): the young window's value and edge. A look 29 min 59 s after the sweep's
    /// first sight is young, at 30 min it is not; the window keys on the first sight (`sweepFirstSeenAtMs`), never on
    /// a hint's time (`sweepLandingAtMs`, which a later hint moves); and it covers a courier-bytes payout waiting on its
    /// block (the L58 shape) and a landing wait, nothing else.
    #[test]
    fn fix1049f1_c_the_young_window_runs_from_the_first_sight_to_30_minutes() {
        use super::*;
        use serde_json::json;
        assert_eq!(OWED_BELT_YOUNG_MISS_MS, 1_800_000);
        assert_eq!(
            OWED_BELT_YOUNG_MISS_MS / OWED_BELT_TICK_MS,
            15,
            "15 young looks at most"
        );
        let first = 1_000_000_000_000_i64;
        let payout = json!({"creditKind": "courier-bytes", "chainWait": "block", "sweepFirstSeenAtMs": first});
        let landing =
            json!({"sweepLanding": true, "chainWait": "block", "sweepFirstSeenAtMs": first});
        for facts in [&payout, &landing] {
            assert!(young_miss(facts, first), "at the first sight");
            assert!(young_miss(facts, first + 1_799_999), "29:59");
            assert!(!young_miss(facts, first + 1_800_000), "30:00");
            assert!(!young_miss(facts, first - 1), "a clock behind the stamp");
        }
        // a later hint moves the landing's hint time, never its first sight
        let renewed = json!({"sweepLanding": true, "sweepLandingAtMs": first + 1_799_000, "sweepFirstSeenAtMs": first});
        assert!(!young_miss(&renewed, first + 1_800_000));
        // no first sight: no young window (a landing only the tracker names)
        assert!(!young_miss(
            &json!({"sweepLanding": true, "sweepLandingAtMs": first}),
            first
        ));
        // a confirmed payout, a filed payout, and any other row: never young
        for facts in [
            json!({"creditKind": "courier-bytes", "sweepFirstSeenAtMs": first}),
            json!({"creditKind": "hopsweep-filing", "chainWait": "block", "sweepFirstSeenAtMs": first}),
            json!({"chainWait": "block", "sweepFirstSeenAtMs": first}),
        ] {
            assert!(!young_miss(&facts, first), "{facts}");
        }
    }

    /// FIX-1049-FOLD-1 (fold 5; A NOTE-2, B RISK), as folded (FIX-1049-FOLD-2, fold 2; the round-2 ensemble's B LOW-1,
    /// A LOW-2): young looks never take a reserved place a row that is not young wants, and never more than half of a
    /// tick's eight while such rows compete; they take every place the others leave (no place idles), and under a
    /// first-class flood a young row keeps the second class's reserved places; with no young row the places are D3-M1's
    /// `class_places` exactly. RED on 9eab66d7: `left: (0, 0, 4) right: (0, 0, 8)` (half the tick idle).
    #[test]
    fn fix1049f1_d_young_looks_take_half_the_places_and_never_the_reserved_ones() {
        use super::*;
        for first in 0..=8 {
            for second in 0..=8 {
                let (f, s) = class_places(first, second);
                assert_eq!(places(first, second, 0), (f, s, 0), "{first} {second}");
            }
        }
        assert_eq!(
            places(0, 0, 8),
            (0, 0, 8),
            "young rows alone take every place"
        );
        assert_eq!(
            places(8, 0, 8),
            (6, 0, 2),
            "a first-class flood leaves young the second class's reserved places"
        );
        // FIX-1057 (item 3): when both kinds are due under a flood, the reserved places are split, one each (it was
        // `(6, 2, 0)`: a young row walked first got no place and the cursor froze on it)
        assert_eq!(
            places(8, 2, 8),
            (6, 1, 1),
            "the reserved places split, one to the rows that are not young, one to the young"
        );
        assert_eq!(places(6, 2, 1), (6, 1, 1), "round 3's B shape");
        assert_eq!(
            places(8, 2, 0),
            (6, 2, 0),
            "no young row: both to the others"
        );
        assert_eq!(places(8, 1, 8), (6, 1, 1));
        assert_eq!(places(0, 6, 8), (0, 4, 4), "half each");
        assert_eq!(places(0, 16, 1), (0, 7, 1));
        assert_eq!(places(2, 1, 8), (2, 1, 5), "no place idles");
        for first in 0..=8 {
            for second in 0..=8 {
                for young in 0..=8 {
                    let (f, s, y) = places(first, second, young);
                    assert!(f + s + y <= OWED_BELT_OUTPOINTS_PER_TICK);
                    assert_eq!(
                        f + s + y,
                        (first + second + young).min(OWED_BELT_OUTPOINTS_PER_TICK),
                        "no place idles: {first} {second} {young}"
                    );
                    assert!(
                        y <= OWED_BELT_YOUNG_PER_TICK || s == second,
                        "young past half only in places the others leave: {first} {second} {young}"
                    );
                    if young > 0 && second == 0 {
                        assert!(
                            y >= young.min(OWED_BELT_SECOND_CLASS_RESERVED),
                            "a young row keeps the reserved places: {first} {young}"
                        );
                    }
                    // FIX-1057 (item 3): the rows that are not young hold the reserved places, less the one a young row
                    // keeps when both are due; so each kind with a due row has a place, and the first row walked is
                    // always given its turn (the walk's cursor never freezes on it)
                    let kept = usize::from(young > 0 && second > 0);
                    assert!(
                        s >= second.min(OWED_BELT_SECOND_CLASS_RESERVED - kept),
                        "the reserved places held: {first} {second} {young}"
                    );
                    assert!(second == 0 || s >= 1, "{first} {second} {young}");
                    assert!(young == 0 || y >= 1, "{first} {second} {young}");
                }
            }
        }
    }

    /// FIX-1049-FOLD-2 (fold 6, the round-2 ensemble's B NOTE-1): a find hands its judge a first sight of a spender's
    /// bytes ONLY where its statements write them: bytes past the store's cap are not kept, so no judge serves a first
    /// sight the record never holds. RED on 9eab66d7: the capped spender's first sight was the find's instant.
    #[test]
    fn fix1049f2_6_a_first_sight_only_where_the_bytes_are_written() {
        use super::*;
        let small = crate::owed::StoredSpenderBytes::from_parts(&[("ab".repeat(32), 0)], &[]);
        let big =
            crate::owed::StoredSpenderBytes::from_parts(&vec![("cd".repeat(32), 0); 2_000], &[]);
        assert!(small.to_json_capped().is_some() && big.to_json_capped().is_none());
        let find = Find {
            bytes: vec![("small".into(), small), ("big".into(), big)],
            ..Find::default()
        };
        let written = find
            .statements(7)
            .iter()
            .filter(|s| s.sql == crate::owed::OWED_SPENDER_BYTES_UPSERT_SQL)
            .count();
        assert_eq!(written, 1, "the capped bytes are not written");
        let mut i = owed_judge::JudgeInputs::default();
        i.now_ms = 7;
        find.apply(&mut i, 7);
        assert_eq!(i.spender_first_seen.get("small"), Some(&7));
        assert_eq!(
            i.spender_first_seen.get("big"),
            None,
            "no stamp the record does not hold"
        );
    }

    /// FIX-1049-FOLD-2 (fold 5): a landing wait has ended at its end exactly, not a millisecond before; no other row has.
    #[test]
    fn fix1049f2_5_landing_ended_at_its_end() {
        use super::*;
        let facts = serde_json::json!({"sweepLanding": true, "sweepLandingUntilMs": 5_000});
        assert!(!landing_ended(&facts, 4_999));
        assert!(landing_ended(&facts, 5_000));
        assert!(!landing_ended(
            &serde_json::json!({"sweepLandingUntilMs": 5_000}),
            9_000
        ));
        assert!(!landing_ended(
            &serde_json::json!({"sweepLanding": true}),
            9_000
        ));
    }

    /// FIX-1049-FOLD-1 (fold 2): a landing wait's next look is never past its end; any other row's date is untouched.
    #[test]
    fn fix1049f1_e_a_landing_waits_next_look_is_by_its_end() {
        use super::*;
        use serde_json::json;
        let facts = json!({"sweepLanding": true, "sweepLandingUntilMs": 5_000});
        assert_eq!(next_look(&facts, 1_000, 9_000), 5_000);
        assert_eq!(next_look(&facts, 1_000, 4_000), 4_000);
        assert_eq!(
            next_look(&facts, 6_000, 9_000),
            9_000,
            "past its end: the row is judged again"
        );
        assert_eq!(
            next_look(&json!({"sweepLandingUntilMs": 5_000}), 1_000, 9_000),
            9_000
        );
    }

    /// FIX-1057 item 2 (round 3's A LOW-2): the ended-wait judgement's every arm, run. Past its end an unanswered look
    /// (`Ok(None)`) and a FAULTED ask (`Err`, its text handed back for the log) are both the empty find of the row's
    /// subject; a find passes as it is; inside the wait (and on a row that is no landing) every answer passes
    /// unchanged. RED with the `Err` arm made to pass the fault through (`(Err(e), None)`): "a faulted ask past the
    /// end is judged on the stored evidence".
    #[test]
    fn fix1057_2_the_ended_wait_judges_an_unanswered_and_a_faulted_ask() {
        use super::*;
        let c = |facts: Value| Candidate {
            identity: "owner".into(),
            outpoint: format!("{}:0", "ab".repeat(32)),
            game_id: "g".into(),
            family: "hop-stranded".into(),
            facts,
            claimable: Some(false),
            reason: None,
            probe_tip: None,
            due_at_ms: None,
            judged_at_ms: None,
        };
        let ended = c(serde_json::json!({"sweepLanding": true, "sweepLandingUntilMs": 5_000}));
        let subject = Some(("owner".to_string(), "g".to_string(), ended.outpoint.clone()));
        let (found, fault) = ended_wait_look(Err("read fault".into()), &ended, 5_000);
        assert_eq!(
            (found.map(|f| f.map(|f| f.subject)), fault),
            (Ok(Some(subject.clone())), Some("read fault".to_string())),
            "a faulted ask past the end is judged on the stored evidence"
        );
        let (found, fault) = ended_wait_look(Ok(None), &ended, 5_000);
        assert_eq!(
            (found.map(|f| f.map(|f| f.subject)), fault),
            (Ok(Some(subject)), None),
            "an unanswered look past the end is the empty find of its subject"
        );
        let mut find = Find::default();
        find.cleared.push("k".into());
        let (found, _) = ended_wait_look(Ok(Some(find)), &ended, 5_000);
        assert_eq!(
            found.unwrap().unwrap().cleared,
            vec!["k".to_string()],
            "a find passes as it is"
        );
        // inside the wait, and a row that is no landing: unchanged
        for (row, now) in [(&ended, 4_999), (&c(serde_json::json!({})), 9_000)] {
            let (found, fault) = ended_wait_look(Err("e".into()), row, now);
            assert_eq!((found.map(|_| ()), fault), (Err("e".to_string()), None));
            let (found, fault) = ended_wait_look(Ok(None), row, now);
            assert!(matches!(found, Ok(None)) && fault.is_none());
        }
    }

    /// FIX-1057 item 4 (round 3's B LOW-2): `Find::apply` gives a new spender's bytes the stamp its statements bind,
    /// never the inputs' clock; a first sight the record already holds stands. RED with `or_insert(i.now_ms)`:
    /// `left: Some(999) right: Some(7)`.
    #[test]
    fn fix1057_4_a_finds_first_sight_is_the_stamp_its_statements_bind() {
        use super::*;
        let bytes = crate::owed::StoredSpenderBytes::from_parts(&[("ab".repeat(32), 0)], &[]);
        let find = Find {
            bytes: vec![("new".into(), bytes.clone()), ("held".into(), bytes)],
            ..Find::default()
        };
        let stamp = find
            .statements(7)
            .iter()
            .find(|s| s.sql == crate::owed::OWED_SPENDER_BYTES_UPSERT_SQL)
            .map(|s| s.binds[2].clone());
        assert!(matches!(stamp, Some(crate::pot_hook::Bind::Int(7))));
        let mut i = owed_judge::JudgeInputs::default();
        i.now_ms = 999;
        i.spender_first_seen.insert("held".into(), 3);
        find.apply(&mut i, 7);
        assert_eq!(i.spender_first_seen.get("new"), Some(&7));
        assert_eq!(
            i.spender_first_seen.get("held"),
            Some(&3),
            "the record's own stamp stands"
        );
    }

    /// FIX-1057 item 7 (round 3's C LOW-2): ONE dating rule. `next_look` is young's next tick, else the base capped at
    /// a landing's end; and the four sites (the judge's `belt_due`, Phase A's `backoff`, Phase C's `ask_backoff`,
    /// `rearm_due`) call it. RED against any site that dates by its own copy (e.g. `ask_backoff` binding
    /// `now + OWED_BELT_TICK_MS`): "one dating rule". FIX-1057-FOLD-1 (the ensemble's C DELETE): the young date and
    /// the landing cap are inlined into `next_look`, so its half that counted their callers is gone (no copy to call).
    #[test]
    fn fix1057_7_one_dating_rule_at_the_four_sites() {
        use super::*;
        use serde_json::json;
        let young = json!({"sweepLanding": true, "sweepLandingUntilMs": 9_000_000, "sweepFirstSeenAtMs": 1_000});
        assert_eq!(
            next_look(&young, 2_000, 5_000_000),
            2_000 + OWED_BELT_TICK_MS
        );
        let landing = json!({"sweepLanding": true, "sweepLandingUntilMs": 5_000});
        assert_eq!(next_look(&landing, 1_000, 9_000), 5_000);
        assert_eq!(next_look(&json!({}), 1_000, 9_000), 9_000);
        // each top-level fn's body, from its signature to its closing brace at column 0
        let belt_src = include_str!("owed_belt.rs");
        let belt_src = &belt_src[..belt_src.find("#[cfg(test)]").unwrap()];
        let judge_src = include_str!("owed_judge.rs");
        let judge_src = &judge_src[..judge_src.find("#[cfg(test)]").unwrap_or(judge_src.len())];
        let body = |src: &'static str, f: &str| -> &'static str {
            let at = src
                .find(&format!("fn {f}("))
                .unwrap_or_else(|| panic!("{f}"));
            let rest = &src[at..];
            &rest[..rest.find("\n}\n").unwrap()]
        };
        for f in ["backoff", "ask_backoff", "rearm_due"] {
            assert!(
                body(belt_src, f).contains("next_look("),
                "one dating rule: {f} calls next_look"
            );
        }
        assert!(
            body(judge_src, "belt_due").contains("owed_belt::next_look("),
            "one dating rule: belt_due calls next_look"
        );
    }
}
