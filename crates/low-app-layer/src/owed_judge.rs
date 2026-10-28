//! The game-scoped owed judge. The derivation remains in `owed`.
use crate::owed::{derive_owed_rows, OwedInputs, OwedRow};

/// Storage omits the words which the reader joins from `tx_words`.
pub fn strip_words(row: &mut OwedRow) {
    if let Some(facts) = row.facts.as_object_mut() {
        for key in ["coinWord", "spenderWord", "fundingWord", "tracker"] {
            facts.remove(key);
        }
    }
}

/// Derive a game using the same pure judgement as the whole-identity walk.
pub fn derive_game(inputs: &OwedInputs<'_>, game: &str) -> Vec<OwedRow> {
    let mut owners = HashMap::new();
    for r in inputs.results {
        owners
            .entry(crate::owed::outpoint_key(&r.pot_txid, r.pot_vout))
            .or_insert(r.game_id.as_str());
    }
    // A hop can name a pot represented under another game. Presence of that
    // bounded representative is context for the existing JOIN retirement rule.
    let mut context = HashSet::new();
    for h in inputs
        .hops
        .iter()
        .filter(|h| h.game_id.eq_ignore_ascii_case(game))
    {
        let key = crate::owed::outpoint_key(&h.hop_txid, h.hop_vout);
        context.extend(inputs.join_spent_hops.get(&key).cloned());
        context.extend(
            inputs
                .hop_chain
                .get(&key)
                .and_then(|w| w.spending_txid.clone()),
        );
        context.extend(h.spending_txid.clone());
    }
    let results: Vec<_> = inputs
        .results
        .iter()
        .filter(|r| {
            owners[&crate::owed::outpoint_key(&r.pot_txid, r.pot_vout)].eq_ignore_ascii_case(game)
                || context.contains(&r.pot_txid.to_ascii_lowercase())
        })
        .cloned()
        .collect();
    let pots: HashSet<_> = results
        .iter()
        .map(|r| crate::owed::outpoint_key(&r.pot_txid, r.pot_vout))
        .collect();
    let refunds: Vec<_> = inputs
        .refunds
        .iter()
        .filter(|r| pots.contains(&crate::owed::outpoint_key(&r.pot_txid, r.pot_vout)))
        .cloned()
        .collect();
    let mut seen: HashSet<_> = owners.keys().cloned().collect();
    let hops: Vec<_> = inputs
        .hops
        .iter()
        .filter(|r| seen.insert(crate::owed::outpoint_key(&r.hop_txid, r.hop_vout)))
        .filter(|r| r.game_id.eq_ignore_ascii_case(game))
        .cloned()
        .collect();
    let mut rows = derive_owed_rows(&OwedInputs {
        results: &results,
        refunds: &refunds,
        hops: &hops,
        ..*inputs
    });
    rows.retain(|r| r.game_id.eq_ignore_ascii_case(game));
    rows.iter_mut().for_each(strip_words);
    rows
}

/// The decomposition oracle shared by all of the committed input fixtures.
pub fn assert_decomposition(inputs: &OwedInputs<'_>) {
    let games: std::collections::BTreeSet<_> = inputs
        .results
        .iter()
        .map(|r| r.game_id.as_str())
        .chain(inputs.refunds.iter().map(|r| r.game_id.as_str()))
        .chain(inputs.hops.iter().map(|r| r.game_id.as_str()))
        .collect();
    let rows: Vec<_> = games
        .into_iter()
        .flat_map(|g| derive_game(inputs, g))
        .collect();
    let mut whole = derive_owed_rows(inputs);
    whole.iter_mut().for_each(strip_words);
    let as_set = |rows: Vec<OwedRow>| {
        rows.into_iter()
            .map(|r| serde_json::to_string(&r).unwrap())
            .collect::<std::collections::BTreeSet<_>>()
    };
    assert_eq!(
        as_set(rows),
        as_set(whole),
        "per-game judgement must equal the walk"
    );
}

use crate::pot_hook::{Bind, SqlPort, Stmt, StmtOut};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
mod hydrate;
pub use hydrate::{facts_of_beefs, FactsFrom, TxFacts, STORED_BEEFS_SQL};
pub mod rank;
mod writers;
pub use hydrate::{
    hydrate_inputs, COLLECTED_SQL, DOOR_REFUSAL_SQL, HONOURED_EVIDENCE, HOPS_SQL,
    HOP_COMPETITORS_SQL, LINKS_SQL, MISSING_SQL, PARTIES_SQL, RELEASED_SQL, REPRESENTATIVES_SQL,
};
pub use writers::{
    drain_changed, drain_changed_within, drain_identities, judge_written_word, judge_written_words,
    mark_tip, write_tip_marks, write_word, DATE_SWEEPS_SQL, MARK_TIP_SQL, MARK_UNTIPPED_SQL,
    UNTIPPED_MARKS_PER_TIP,
};

pub const OWED_JUDGE_SUBJECTS_PER_INVOCATION: usize = 16;
/// A filing whose judge could not run and landed alone (the D2-OWED lens's MEDIUM-2).
pub const COUNTER_FILING_FAULTS: &str = "owed_judge_filing_faults_total";
pub fn counter_names() -> Vec<String> {
    vec![COUNTER_FILING_FAULTS.to_owned()]
}
pub const OWED_DIRTY_SUBJECTS_PER_PASS: usize = 64;
/// The drain counts at most this many oldest marks; a count at the cap means at least this many.
pub const DIRTY_BACKLOG_COUNT_CAP: i64 = 65_536;
/// The health read is one primary-key lookup of the drain's durable snapshot.
pub const DIRTY_HEALTH_SQL: &str = "SELECT json_extract(cursor,'$.count') AS count,json_extract(cursor,'$.oldestMarkedAtMs') AS oldest,tickedAtMs AS asOfMs FROM owed_belt_state WHERE name='backlog:dirty'";
/// One bounded, covering-index count per drain tick, including its oldest mark in that same read.
pub const DIRTY_SNAPSHOT_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) SELECT 'backlog:dirty',json_object('count',COUNT(*),'oldestMarkedAtMs',MIN(markedAtMs),'tickAtMs',?1),?1 FROM (SELECT markedAtMs FROM owed_dirty INDEXED BY idx_owed_dirty_backlog ORDER BY markedAtMs LIMIT ?2) WHERE 1 ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";

pub async fn snapshot_dirty(db: &impl SqlPort) -> Result<(), String> {
    db.run_batch(vec![Stmt {
        sql: DIRTY_SNAPSHOT_SQL.into(),
        binds: vec![
            Bind::Int(db.now_ms() as i64),
            Bind::Int(DIRTY_BACKLOG_COUNT_CAP),
        ],
    }])
    .await?;
    Ok(())
}

pub async fn dirty_health(db: &impl SqlPort) -> Value {
    match db
        .run_batch(vec![Stmt {
            sql: DIRTY_HEALTH_SQL.into(),
            binds: vec![],
        }])
        .await
    {
        Ok(rows) => match rows.first().and_then(|out| out.rows.first()) {
            Some(row) => json!({
                "count": row["count"],
                "oldestMarkAgeMs": row["oldest"].as_u64().map(|at| db.now_ms().saturating_sub(at)),
                "asOfMs": row["asOfMs"],
            }),
            None => Value::Null,
        },
        Err(_) => Value::Null,
    }
}
/// A wanted envelope expands to the identity's newest games only (the design's section 4.1 bound, the D2-OWED lens's
/// MEDIUM-1); the rest are marked `backfill` and wait their identity's turn like any other mark.
pub const OWED_WANTED_GAMES: usize = 32;
/// Required source-table access paths, by index name: the overlay ships both (the native pin holds it).
pub const REQUIRED_GAME_INDEX_NAMES: [&str; 2] = [
    "idx_potparty_game_identity",
    "idx_evicted_party_identity_game",
];
pub const MARK_WANTED_SQL: &str = "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES (?1,'','wanted','wanted',strftime('%s','now') * 1000) ON CONFLICT(identity,gameId,source,sourceKey) DO UPDATE SET generation=excluded.generation,markedAtMs=excluded.markedAtMs";
/// A judge's per-game delete, over `idx_owed_rows_identity_game` (the D2-OWED lens's LOW-2).
pub const GAME_DELETE_SQL: &str = "DELETE FROM owed_rows WHERE identity=?1 AND gameId IN (SELECT value FROM json_each(?2))";
/// The identity's state row, created once with its rows counted; the row triggers keep `rows` from then on.
pub const STATE_CREATE_SQL: &str = "INSERT INTO owed_state(identity,computedAtMs,tip,rows,stale,truncated,recomputeSeq) SELECT ?1,?2,NULL,(SELECT COUNT(*) FROM owed_rows WHERE identity=?1),0,0,0 WHERE NOT EXISTS (SELECT 1 FROM owed_state WHERE identity=?1)";
/// The stored ceiling, read only when the kept count is past it: the state row is the outer loop, so under the
/// ceiling the identity's rows are never visited, and the delete is driven by the list's rowids.
pub const CEILING_DELETE_SQL: &str = "DELETE FROM owed_rows WHERE rowid IN (SELECT r.rowid FROM owed_state s CROSS JOIN owed_rows r WHERE s.identity=?1 AND s.rows>500 AND r.identity=?1 ORDER BY CASE r.family WHEN 'payout' THEN 0 WHEN 'refund-due' THEN 1 WHEN 'hop-stranded' THEN 2 WHEN 'in-progress' THEN 3 ELSE 4 END,COALESCE(r.atHeight,0) DESC,r.outpoint LIMIT -1 OFFSET 500)";
/// The judge's stamp on the state row; `changes()` is the ceiling delete's, just before it.
pub const STATE_UPDATE_SQL: &str = "UPDATE owed_state SET computedAtMs=MAX(computedAtMs,?2),tip=?3,stale=0,truncated=(truncated OR ?4 OR changes()>0),recomputeSeq=COALESCE(recomputeSeq,0)+1 WHERE identity=?1";
/// The judge's acknowledgement: the marks at or below the generation it read (amendment 1), and the marks its own
/// source statements raised, ?4 a JSON list of `[source, sourceKey]`.
pub const ACK_DIRTY_SQL: &str = "DELETE FROM owed_dirty WHERE identity=?1 AND gameId IN (SELECT value FROM json_each(?2)) AND (generation<=?3 OR EXISTS (SELECT 1 FROM json_each(?4) m WHERE json_extract(m.value,'$[0]')=owed_dirty.source AND json_extract(m.value,'$[1]')=owed_dirty.sourceKey))";
pub const MARKS_SQL: &str = "SELECT generation,source,sourceKey FROM owed_dirty WHERE identity=?1 AND gameId IN (SELECT value FROM json_each(?2))";
/// The wanted envelopes, oldest first: an envelope's source key is the constant `wanted` (the D2-OWED fold), so the
/// source-key index finds them, and one wanted mark stays five physical D1 writes (ruling 1, bsv-low record 1277).
pub const WANTED_ENVELOPES_SQL: &str = "SELECT identity,generation FROM owed_dirty INDEXED BY idx_owed_dirty_source_key WHERE sourceKey='wanted' AND source='wanted' AND gameId='' ORDER BY markedAtMs,generation LIMIT ?1";
/// The drain's round-robin walk (the D2-OWED lens's MEDIUM-1): up to ?2 distinct identities holding a mark, ascending
/// past the cursor ?1, one covering-index search each; the caller wraps to the start for the rest.
pub const DRAIN_IDENTITIES_SQL: &str = "WITH RECURSIVE walk(identity,n) AS (SELECT (SELECT MIN(identity) FROM owed_dirty INDEXED BY idx_owed_dirty_identity_marked WHERE identity>?1),1 UNION ALL SELECT (SELECT MIN(identity) FROM owed_dirty INDEXED BY idx_owed_dirty_identity_marked WHERE identity>walk.identity),n+1 FROM walk WHERE walk.identity IS NOT NULL AND n<?2) SELECT identity FROM walk WHERE identity IS NOT NULL";
/// One identity's oldest marks, a bounded few (the drain groups them by game): ?3 = 0 its source marks, ?3 = 1 its
/// age repair's (read only when the source marks leave room; at most 128 age marks are ever pending fleet-wide).
pub const DRAIN_MARKS_SQL: &str = "SELECT gameId,source,markedAtMs,generation FROM owed_dirty INDEXED BY idx_owed_dirty_identity_marked WHERE identity=?1 AND (source='age')=?3 ORDER BY markedAtMs,generation LIMIT ?2";
/// The drain's cursor: the last identity a pass gave a turn.
pub const DRAIN_CURSOR_NAME: &str = "drain";
pub const DRAIN_CURSOR_READ_SQL: &str = "SELECT cursor FROM owed_belt_state WHERE name='drain'";
pub const DRAIN_CURSOR_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('drain',?1,?2) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// A hook's subjects: the identities or source keys its change note carries (two index searches), oldest first, the
/// age repair's last.
pub const CHANGED_SQL: &str = "SELECT identity,gameId FROM owed_dirty WHERE gameId<>'' AND (identity IN (SELECT value FROM json_each(?1)) OR sourceKey IN (SELECT value FROM json_each(?2))) GROUP BY identity,gameId ORDER BY MIN(source='age'),MIN(markedAtMs),MIN(generation) LIMIT ?3";
pub const ROWS_READ_SQL: &str = "SELECT r.identity,r.outpoint,r.family,r.gameId,r.sats,r.opponentIdentity,r.atHeight,r.facts,r.reason,r.judgedAtMs,r.claimable,r.probeTip,w.role,t.txid,t.word,t.tier,t.height,t.proof_root,t.header_hash,t.hint_source,t.hint_at,t.updated_at FROM owed_rows r LEFT JOIN owed_row_words w ON w.identity=r.identity AND w.outpoint=r.outpoint LEFT JOIN tx_words t ON t.txid=w.txid WHERE r.identity=?1 ORDER BY r.outpoint,w.role LIMIT 2004";

/// Only the SQL port and the already obtained chain context surround the pure judgement.
#[allow(async_fn_in_trait)]
pub trait JudgeDb: SqlPort {
    fn tip(&self) -> Option<u64>;
    fn era_ms(&self) -> Option<i64> {
        None
    }
    async fn word_views(
        &self,
        rows: &BTreeMap<String, crate::tracker_host::stored::Row>,
    ) -> Result<BTreeMap<String, Value>, String>;
    async fn canonical_root(&self, height: u64) -> Option<String>;
    async fn push(&self, identity: &str, body: Value);
    /// bsv-low #912 (D2-ACTOR-B): does this judge write the D1 `owed_rows` (`OWED_D1_JUDGE`, Phases A and B)?
    fn d1_judges(&self) -> bool {
        true
    }
    /// bsv-low #912: the identity's actor applies the games (`owed_actor::object::call_apply`); `None` where no
    /// actor is bound (the native judges, a render without the class).
    async fn actor_deliver(
        &self,
        _identity: &str,
        _games: &[String],
    ) -> Result<Option<Value>, String> {
        Ok(None)
    }
    /// bsv-low #912 (Phase C's `belt-find`): the identity's actor applies the games as an event of `kind` (the belt's
    /// find handed back); `None` where no actor is bound.
    async fn actor_apply(
        &self,
        _identity: &str,
        _games: &[String],
        _kind: &str,
    ) -> Result<Option<Value>, String> {
        Ok(None)
    }
    /// bsv-low #912 (the reconcile, MEDIUM-10): the identity's actor's stored rows (`owed_actor::ROWS_SQL` on its
    /// SQLite), read without an apply; `None` where no actor is bound.
    async fn actor_rows(&self, _identity: &str) -> Result<Option<Vec<Value>>, String> {
        Ok(None)
    }
    /// MEDIUM-3 (bsv-low #912): a transaction's decoded facts. The D1 judge decodes the stored BEEF here; the
    /// actor serves them from its `txfacts` table and holds no bytes.
    async fn tx_facts(&self, txid: &str, from: FactsFrom) -> Result<Option<TxFacts>, String>
    where
        Self: Sized,
    {
        hydrate::stored_tx_facts(self, txid, from).await
    }
    async fn hydrate(&self, identity: &str, games: &[String]) -> Result<JudgeInputs, String>
    where
        Self: Sized,
    {
        hydrate_inputs(self, identity, games).await
    }
}

/// Production adapter. All SQL, including the final write, uses the counted D1 port.
pub struct D1Judge {
    sql: crate::routes::D1Sql,
    tip: Option<u64>,
    era_ms: Option<i64>,
    /// bsv-low #912: the event this judge serves (its kind, seq, write time, a mined word), for the actor's call.
    event: Option<crate::owed_actor::EventCtx>,
    d1_judges: bool,
}
impl D1Judge {
    pub fn new(env: &worker::Env, tip: Option<u64>, era_ms: Option<i64>) -> worker::Result<Self> {
        Ok(Self {
            sql: crate::routes::D1Sql::of(env)?,
            tip,
            era_ms,
            event: None,
            d1_judges: crate::owed_actor::d1_judge_on(
                env.var(crate::owed_actor::D1_JUDGE_FLAG)
                    .ok()
                    .map(|v| v.to_string()),
            ),
        })
    }
    /// bsv-low #912: the consumer's judge carries its event to the actor.
    pub fn with_event(mut self, event: crate::owed_actor::EventCtx) -> Self {
        self.event = Some(event);
        self
    }
}
impl SqlPort for D1Judge {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        self.sql.run_batch(stmts).await
    }
    async fn counters(&self, deltas: Vec<(String, u64)>) {
        self.sql.counters(deltas).await;
    }
    fn now_ms(&self) -> u64 {
        self.sql.now_ms()
    }
    fn log(&self, line: &str) {
        self.sql.log(line);
    }
}
impl JudgeDb for D1Judge {
    fn tip(&self) -> Option<u64> {
        self.tip
    }
    fn d1_judges(&self) -> bool {
        self.d1_judges
    }
    async fn actor_deliver(
        &self,
        identity: &str,
        games: &[String],
    ) -> Result<Option<Value>, String> {
        let Some(event) = &self.event else {
            return Ok(None);
        };
        if crate::owed_actor::object::stub_of(&self.sql.env, identity).is_none() {
            return Ok(None);
        }
        let body = crate::owed_actor::apply_body(identity, games, event, self.tip);
        crate::owed_actor::object::call_apply(
            &self.sql.env,
            &body,
            crate::owed_actor::CONSUMER_CALL_WALL_MS,
        )
        .await
        .map(Some)
    }
    async fn actor_rows(&self, identity: &str) -> Result<Option<Vec<Value>>, String> {
        if crate::owed_actor::object::stub_of(&self.sql.env, identity).is_none() {
            return Ok(None);
        }
        crate::owed_actor::object::call_stored(
            &self.sql.env,
            identity,
            crate::owed_actor::CONSUMER_CALL_WALL_MS,
        )
        .await
        .map(Some)
    }
    async fn actor_apply(
        &self,
        identity: &str,
        games: &[String],
        kind: &str,
    ) -> Result<Option<Value>, String> {
        if crate::owed_actor::object::stub_of(&self.sql.env, identity).is_none() {
            return Ok(None);
        }
        let event = crate::owed_actor::EventCtx {
            kind: kind.into(),
            seq: 0,
            at_ms: worker::Date::now().as_millis() as i64,
            mined: false,
        };
        let mut body = crate::owed_actor::apply_body(identity, games, &event, self.tip);
        body["seq"] = Value::Null;
        crate::owed_actor::object::call_apply(
            &self.sql.env,
            &body,
            crate::owed_actor::CONSUMER_CALL_WALL_MS,
        )
        .await
        .map(Some)
    }
    fn era_ms(&self) -> Option<i64> {
        self.era_ms
    }
    async fn word_views(
        &self,
        rows: &BTreeMap<String, crate::tracker_host::stored::Row>,
    ) -> Result<BTreeMap<String, Value>, String> {
        Ok(crate::tracker_host::runtime::views(&self.sql.env, rows).await)
    }
    async fn canonical_root(&self, height: u64) -> Option<String> {
        crate::beef_guard::canonical_root(&self.sql.env, height).await
    }
    async fn push(&self, identity: &str, body: Value) {
        // bsv-low #935: coalesced per seat under the relay's send budget. FIX-935-FOLD: QUEUED, never sent here, so
        // no coalescing window sits in a route's answer or a pass's wall; the fetch door, the scheduled tasks and the
        // belt actor flush the queue after their work (`push_coalesce::flush_queued`).
        crate::push_coalesce::queue_seat_push(identity, body);
    }
}

/// Owned inputs let a verified filing apply its delta before the source INSERT and judgement land together.
#[derive(Default)]
pub struct JudgeInputs {
    pub identity: String,
    pub tip: Option<u64>,
    pub now_ms: i64,
    pub results: Vec<crate::results::ResultEntry>,
    pub refunds: Vec<crate::refund_view::RefundEntry>,
    pub hops: Vec<crate::hops_view::HopEntry>,
    pub valid_refunds: HashMap<String, crate::owed::ValidRefund>,
    pub collected_verified: HashSet<String>,
    pub collected_present: HashSet<String>,
    pub held_verified: HashSet<String>,
    pub pot_spenders: HashSet<String>,
    pub hop_chain: HashMap<String, crate::owed::HopChainWord>,
    pub evicted_hop_outpoints: HashSet<String>,
    pub door_refused_hop_outpoints: HashMap<String, crate::owed::DoorRefusal>,
    pub hop_sweeps: HashMap<String, crate::hopsweep::FiledHopSweep>,
    pub evicted_pots: HashSet<String>,
    pub spender_outputs: HashMap<String, Vec<crate::owed::SpenderOutput>>,
    pub spender_inputs: HashMap<String, Vec<(String, u32)>>,
    pub courier_spenders: HashSet<String>,
    pub my_pkh_by_game: HashMap<String, String>,
    pub home_spends: HashMap<String, crate::owed::HomeSpendWord>,
    pub retired_spenders: HashSet<String>,
    pub proof_mined_spenders: HashSet<String>,
    pub funding_unheld: HashSet<String>,
    pub join_spent_hops: HashMap<String, String>,
    pub spender_words: BTreeMap<String, Value>,
    /// Pot outpoints whose chosen party row verifies (`sigValid = 1`): the only pot rows the belt dates (HIGH-3).
    pub verified_pots: HashSet<String>,
    /// D2-OWED-FOLD-8: the old walk's could-not-judge rows of the released hops of an evicted JOIN whose bytes do not
    /// read (`released_hop_evidence_missing`), merged after the derivation by the walk's own rule
    /// (`owed::merge_markerless_faults`: never over a row judged from other evidence).
    pub markerless_faults: Vec<OwedRow>,
    funding_seen: HashSet<String>,
    word_rows: BTreeMap<String, crate::tracker_host::stored::Row>,
    moved_latches: Vec<(String, String)>,
    /// The walk's post-derivation inputs (the D2-OWED lens's MEDIUM-3): the hops whose confirmed chain word was taken
    /// inside the reorg grace (`hops_view::mark_reorg_grace_rows`), the words served without their confirmation and
    /// the filed sweeps' proofs such a word set aside (`hops_view::mark_refused_word_rows`).
    pub(crate) grace_words: HashSet<String>,
    pub(crate) refused_words: HashMap<String, Option<String>>,
    pub(crate) proofs_set_aside: HashSet<String>,
}
impl JudgeInputs {
    pub fn as_inputs(&self) -> OwedInputs<'_> {
        OwedInputs {
            identity_lc: &self.identity,
            tip: self.tip,
            now_ms: self.now_ms,
            results: &self.results,
            refunds: &self.refunds,
            hops: &self.hops,
            valid_refunds: &self.valid_refunds,
            collected_verified: &self.collected_verified,
            collected_present: &self.collected_present,
            held_verified: &self.held_verified,
            pot_spenders: &self.pot_spenders,
            pot_spenders_faulted: false,
            hop_chain: &self.hop_chain,
            evicted_hop_outpoints: &self.evicted_hop_outpoints,
            door_refused_hop_outpoints: &self.door_refused_hop_outpoints,
            hop_sweeps: &self.hop_sweeps,
            evicted_pots: &self.evicted_pots,
            spender_outputs: &self.spender_outputs,
            spender_inputs: &self.spender_inputs,
            courier_spenders: &self.courier_spenders,
            my_pkh_by_game: &self.my_pkh_by_game,
            home_spends: &self.home_spends,
            retired_spenders: &self.retired_spenders,
            proof_mined_spenders: &self.proof_mined_spenders,
            funding_unheld: &self.funding_unheld,
            join_spent_hops: &self.join_spent_hops,
            spender_words: Some(&self.spender_words),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subject {
    pub identity: String,
    pub game_id: String,
}
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct JudgeReport {
    pub subjects: usize,
    pub deferred: usize,
    pub rows: usize,
    pub truncated: bool,
    /// Changes from the source statements at the front of the committed batch.
    pub source_changes: Vec<Option<u64>>,
    /// The judge could not run: the source statements landed alone (`record_post::owed_write::judge_filings`).
    pub judge_fault: Option<String>,
}
#[derive(Debug, Default)]
pub struct DrainReport {
    pub subjects: usize,
    pub faults: Vec<(Subject, String)>,
}
/// The exact trigger mark(s) belonging to the source writes at the front of the batch.
#[derive(Debug, Clone)]
pub struct FilingMark {
    pub source: String,
    pub source_key: String,
}

/// A read never judges. An empty game is a wanted-identity envelope which only the drain expands.
pub async fn mark_wanted(db: &impl SqlPort, identity: &str) -> Result<(), String> {
    db.run_batch(vec![Stmt {
        sql: MARK_WANTED_SQL.into(),
        binds: vec![Bind::Text(identity.to_ascii_lowercase())],
    }])
    .await?;
    Ok(())
}

pub async fn judge_subjects(
    db: &impl JudgeDb,
    identity: &str,
    games: &[String],
    source: &str,
) -> Result<JudgeReport, String> {
    let subjects: Vec<_> = games
        .iter()
        .map(|g| Subject {
            identity: identity.to_ascii_lowercase(),
            game_id: g.to_ascii_lowercase(),
        })
        .collect();
    judge_with_write(db, &subjects, source, Vec::new(), &[], |_| Ok(())).await
}

/// Source statements run first in the ONE atomic batch. `apply` receives all selected subjects' inputs;
/// it must apply only already verified filing/evidence facts. Use both seats from `refund_subjects` for refunds.
pub async fn judge_with_write(
    db: &impl JudgeDb,
    subjects: &[Subject],
    source: &str,
    writes: Vec<Stmt>,
    owned_marks: &[FilingMark],
    apply: impl FnOnce(&mut [JudgeInputs]) -> Result<(), String>,
) -> Result<JudgeReport, String> {
    if subjects.is_empty() && !writes.is_empty() {
        return Err("a source write requires an owed subject".into());
    }
    // The caller's order decides who is judged inside the bound (a refund's filer first, the D2-OWED lens's MEDIUM-2).
    let mut ordered: Vec<(String, String)> = Vec::new();
    for s in subjects {
        let key = (
            s.identity.to_ascii_lowercase(),
            s.game_id.to_ascii_lowercase(),
        );
        if !ordered.contains(&key) {
            ordered.push(key);
        }
    }
    let subjects = ordered;
    let deferred = subjects
        .len()
        .saturating_sub(OWED_JUDGE_SUBJECTS_PER_INVOCATION);
    let mut by_identity: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (id, game) in subjects
        .into_iter()
        .take(OWED_JUDGE_SUBJECTS_PER_INVOCATION)
    {
        by_identity.entry(id).or_default().push(game);
    }
    let mut seen = Vec::new();
    let mut inputs = Vec::new();
    let mut groups = Vec::new();
    for (id, games) in by_identity {
        let marks: Vec<Value> = hydrate::read(
            db,
            MARKS_SQL,
            vec![Bind::Text(id.clone()), Bind::Text(json!(games).to_string())],
        )
        .await?;
        seen.push(
            marks
                .iter()
                .filter_map(|m| m["generation"].as_i64())
                .max()
                .unwrap_or(0),
        );
        groups.push((id.clone(), games.clone()));
        inputs.push(db.hydrate(&id, &games).await?);
    }
    apply(&mut inputs)?;
    let source_count = writes.len();
    let mut plan = writes;
    let mut state_positions = Vec::new();
    let mut report = JudgeReport {
        subjects: groups.iter().map(|(_, g)| g.len()).sum(),
        deferred,
        ..Default::default()
    };
    for (idx, i) in inputs.iter().enumerate() {
        let (id, games) = &groups[idx];
        // The subjects and the context are not filing-controlled.
        if &i.identity != id {
            return Err("filing changed the judgement identity".into());
        }
        let mut rows = derive_owed_rows(&i.as_inputs());
        rows.retain(|r| games.contains(&r.game_id));
        crate::owed::merge_markerless_faults(
            &mut rows,
            i.markerless_faults
                .iter()
                .filter(|f| games.contains(&f.game_id))
                .cloned()
                .collect(),
        );
        // The old walk's display marks after its derivation, in its order (the walk-against-judge pin holds them).
        for row in &mut rows {
            crate::routes::mark_pending_spender(row, None, i.now_ms);
        }
        crate::hops_view::mark_refused_word_rows(&mut rows, &i.refused_words, &i.proofs_set_aside);
        crate::hops_view::mark_reorg_grace_rows(&mut rows, &i.grace_words);
        hydrate::finish_rows(db, &mut rows).await?;
        crate::owed::sort_rows_for_service(&mut rows);
        let cut = rows.len() > crate::owed::OWED_MAX_ROWS;
        rows.truncate(crate::owed::OWED_MAX_ROWS);
        plan.push(Stmt {
            sql: GAME_DELETE_SQL.into(),
            binds: vec![Bind::Text(id.clone()), Bind::Text(json!(games).to_string())],
        });
        for row in &rows {
            plan.extend(row_statements(row, i.now_ms, i.tip, Some(i)));
        }
        if let Some(moved) = crate::owed::moved_proof_latch_delete_bind(&i.moved_latches) {
            plan.push(Stmt {
                sql: crate::owed::PROOF_LATCH_MOVED_DELETE_SQL.into(),
                binds: vec![Bind::Text(moved)],
            });
        }
        // The identity's first state row counts its rows ONCE; from then on the row triggers keep the figure (the
        // D2-OWED lens's LOW-2), so neither the ceiling's guard nor the state write reads the identity's list.
        plan.push(Stmt {
            sql: STATE_CREATE_SQL.into(),
            binds: vec![Bind::Text(id.clone()), Bind::Int(i.now_ms)],
        });
        // Rank at the shared ceiling inside the transaction, so two different games cannot overfill it.
        plan.push(Stmt {
            sql: CEILING_DELETE_SQL.into(),
            binds: vec![Bind::Text(id.clone())],
        });
        // A partial judge cannot prove that other games' omitted rows were rebuilt.
        // Preserve an earlier overflow flag until a complete backfill resets it; `changes()` is the ceiling's.
        plan.push(Stmt {
            sql: STATE_UPDATE_SQL.into(),
            binds: vec![
                Bind::Text(id.clone()),
                Bind::Int(i.now_ms),
                i.tip.map_or(Bind::Null, |n| Bind::Int(n as i64)),
                Bind::Int(i64::from(cut)),
            ],
        });
        let owned: Vec<[&str; 2]> = owned_marks
            .iter()
            .map(|m| [m.source.as_str(), m.source_key.as_str()])
            .collect();
        plan.push(Stmt {
            sql: ACK_DIRTY_SQL.into(),
            binds: vec![
                Bind::Text(id.clone()),
                Bind::Text(json!(games).to_string()),
                Bind::Int(seen[idx]),
                Bind::Text(json!(owned).to_string()),
            ],
        });
        state_positions.push((plan.len(), id.clone(), i.now_ms));
        plan.push(Stmt {
            sql: "SELECT rows,truncated FROM owed_state WHERE identity=?1".into(),
            binds: vec![Bind::Text(id.clone())],
        });
    }
    if plan.is_empty() {
        return Ok(report);
    }
    let landed = db.run_batch(plan).await?;
    report.source_changes = landed.iter().take(source_count).map(|r| r.changes).collect();
    // The batch committed: nothing below may fail the call (a filing would land its source statements twice).
    // bsv-low #935: the pushes go together after the loop, so a coalescing window costs the call one wait at most.
    let mut pushes = Vec::new();
    for (pos, id, stamp) in state_positions {
        let Some(count) = landed
            .get(pos)
            .and_then(|s| s.rows.first())
            .and_then(|s| s["rows"].as_u64())
        else {
            db.log(&format!("[owed] judge state readback missing for {id}"));
            continue;
        };
        let count = count as usize;
        let state = &landed[pos].rows[0];
        report.rows += count;
        report.truncated |= state["truncated"].as_i64().unwrap_or(0) != 0;
        let body = crate::owed::owed_changed_event_body(&id, source, count, stamp);
        pushes.push((id, body));
    }
    futures_util::future::join_all(pushes.into_iter().map(|(id, body)| async move {
        db.push(&id, body).await;
    }))
    .await;
    Ok(report)
}

/// The spender a row names, in the belt's order (`owed_belt::ask_of`).
fn named_spender(row: &OwedRow) -> Option<String> {
    ["chainSpender", "spendingTxid", "settleTxid"]
        .into_iter()
        .find_map(|k| {
            row.facts[k]
                .as_str()
                .filter(|t| crate::logic::valid_txid(t))
                .map(str::to_ascii_lowercase)
        })
}

/// When the belt may next ask the chain about this row (section 5), NULL when nothing is owed. The D2-OWED lens's
/// HIGH-3: a row rests on VERIFIED evidence before it costs a look (a hop row on its marker's signature, a pot row on
/// its party's `sigValid`; anyone's byte-format row is never dated), and an `unbound` row is dated only while its
/// named spender's bytes are NOT held (held bytes are a judgement, not a question). `evidence` is the judge's inputs;
/// `None` (the fixtures' storage round trip) reads every row as verified with nothing held.
fn belt_due(row: &OwedRow, stamp: i64, evidence: Option<&JudgeInputs>) -> Option<i64> {
    use crate::owed::OwedFamily;
    let verified = if row.facts["hopTxid"].is_string() {
        row.facts["markerVerified"] == crate::hops_view::MarkerVerification::Verified.as_str()
    } else if row.facts["potTxid"].is_string() {
        evidence.is_none_or(|i| i.verified_pots.contains(&row.outpoint))
    } else {
        true
    };
    if !verified {
        return None;
    }
    match row.family {
        OwedFamily::HopStranded => Some(stamp + 600_000),
        OwedFamily::Unbound => {
            let spender = named_spender(row)?;
            let held = evidence.is_some_and(|i| i.spender_outputs.contains_key(&spender));
            (!held).then_some(stamp + 300_000)
        }
        OwedFamily::Payout if row.facts["creditKind"] == "courier-bytes" => Some(stamp + 600_000),
        OwedFamily::InProgress if row.facts["hopTxid"].is_string() => Some(
            row.facts["markerCreatedAtMs"]
                .as_i64()
                .unwrap_or(stamp)
                .saturating_add(crate::owed::HOP_STRANDED_AFTER_MS)
                .max(stamp),
        ),
        _ => None,
    }
}

/// `probeTip` is the tip the row was JUDGED at (NULL: judged with no tip, or an old worker's row): the reference the
/// read's #669 freshness gate degrades a claimable sweep against (`owed_read::Snapshot::body`), as the old walk's
/// snapshot tip (`owed_state.tip`) was; the probe's own tip stays in `facts.chainProbeTip`.
fn row_statements(
    row: &OwedRow,
    stamp: i64,
    judged_tip: Option<u64>,
    evidence: Option<&JudgeInputs>,
) -> Vec<Stmt> {
    let mut row = row.clone();
    let mut words = BTreeMap::new();
    for (role, key) in [
        ("settle", "settleTxid"),
        ("spender", "spendingTxid"),
        ("pay", "payTxid"),
    ] {
        if let Some(t) = row.facts[key]
            .as_str()
            .filter(|t| crate::logic::valid_txid(t))
        {
            words.insert(role, t.to_ascii_lowercase());
        }
    }
    // A chain-discovered spender can be absent from the index pointer.
    for key in ["chainSpender", "sweepTxid"] {
        if let Some(t) = row.facts[key]
            .as_str()
            .filter(|t| crate::logic::valid_txid(t))
        {
            words
                .entry("spender")
                .or_insert_with(|| t.to_ascii_lowercase());
        }
    }
    if let Some(t) = row.facts["spenderWord"]["txid"]
        .as_str()
        .or_else(|| row.facts["coinWord"]["txid"].as_str())
        .filter(|t| crate::logic::valid_txid(t))
    {
        words.insert("spender", t.to_ascii_lowercase());
    }
    if let Some(t) = row
        .outpoint
        .split(':')
        .next()
        .filter(|t| crate::logic::valid_txid(t))
    {
        words.insert("funding", t.to_ascii_lowercase());
    }
    let claimable = row.facts["claimable"].as_bool().map(i64::from);
    let gate = row.facts["recoveryHeight"].as_i64();
    let probe = judged_tip.map(|t| t as i64);
    let due = belt_due(&row, stamp, evidence);
    strip_words(&mut row);
    if let Some(facts) = row.facts.as_object_mut() {
        facts.insert("beltNextAtMs".into(), json!(due));
    }
    let int = |n: Option<i64>| n.map_or(Bind::Null, Bind::Int);
    let opt = |s: &Option<String>| s.clone().map_or(Bind::Null, Bind::Text);
    let mut out=vec![Stmt{sql:"INSERT INTO owed_rows(identity,outpoint,family,gameId,sats,opponentIdentity,atHeight,facts,updatedAtMs,reason,judgedAtMs,claimable,gateHeight,beltDueAtMs,probeTip) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15) ON CONFLICT(identity,outpoint) DO UPDATE SET family=excluded.family,gameId=excluded.gameId,sats=excluded.sats,opponentIdentity=excluded.opponentIdentity,atHeight=excluded.atHeight,facts=excluded.facts,updatedAtMs=excluded.updatedAtMs,reason=excluded.reason,judgedAtMs=excluded.judgedAtMs,claimable=excluded.claimable,gateHeight=excluded.gateHeight,beltDueAtMs=excluded.beltDueAtMs,probeTip=excluded.probeTip".into(),binds:vec![Bind::Text(row.identity.clone()),Bind::Text(row.outpoint.clone()),Bind::Text(row.family.as_str().into()),Bind::Text(row.game_id),int(row.sats.map(|v|v as i64)),opt(&row.opponent_identity),int(row.at_height.map(|v|v as i64)),Bind::Text(row.facts.to_string()),Bind::Int(stamp),opt(&row.reason),Bind::Int(stamp),int(claimable),int(gate),int(due),int(probe)]}];
    // UPSERT can replace a representative's game, so clear roles even when the subject delete did not find it.
    out.push(Stmt {
        sql: "DELETE FROM owed_row_words WHERE identity=?1 AND outpoint=?2".into(),
        binds: vec![
            Bind::Text(row.identity.clone()),
            Bind::Text(row.outpoint.clone()),
        ],
    });
    for (role, txid) in words {
        out.push(Stmt {
            sql: "INSERT INTO owed_row_words(identity,outpoint,role,txid) VALUES (?1,?2,?3,?4)"
                .into(),
            binds: vec![
                Bind::Text(row.identity.clone()),
                Bind::Text(row.outpoint.clone()),
                Bind::Text(role.into()),
                Bind::Text(txid),
            ],
        });
    }
    out
}

/// The pot's VERIFIED seats (a party row whose `sigValid = 1`, a hop marker whose `markerValid = 1`): anyone's
/// byte-format row is no subject of another identity's filing (the D2-OWED lens's MEDIUM-2; its trigger mark still
/// stands for the drain).
pub const REFUND_SUBJECTS_SQL: &str = "SELECT identity,gameId FROM potparty_records INDEXED BY idx_potparty_pot WHERE potTxid=?1 AND potVout=?2 AND sigValid=1 UNION SELECT hp.identity,hp.gameId FROM hop_pot_links l INDEXED BY idx_hop_pot_links_pot JOIN hopparty_records hp ON hp.rowid=l.markerRowid WHERE l.potTxid=?1 AND l.potVout=?2 AND hp.markerValid=1";

/// The filer FIRST (its own subject is judged inside the bound whatever else names the pot), then the pot's verified
/// seats, including the filing's subject before its new party row exists.
pub async fn refund_subjects(
    db: &impl SqlPort,
    pot: &str,
    vout: u32,
    filer: &str,
    game: &str,
) -> Result<Vec<Subject>, String> {
    let rows: Vec<Value> = hydrate::read(
        db,
        REFUND_SUBJECTS_SQL,
        vec![
            Bind::Text(pot.to_ascii_lowercase()),
            Bind::Int(i64::from(vout)),
        ],
    )
    .await?;
    let filer = (filer.to_ascii_lowercase(), game.to_ascii_lowercase());
    let seats: BTreeSet<_> = rows
        .iter()
        .filter_map(|r| {
            Some((
                r["identity"].as_str()?.to_ascii_lowercase(),
                r["gameId"].as_str()?.to_ascii_lowercase(),
            ))
        })
        .filter(|s| *s != filer)
        .collect();
    Ok(std::iter::once(filer)
        .chain(seats)
        .map(|(identity, game_id)| Subject { identity, game_id })
        .collect())
}

// One unit, milliseconds (the delta lens's D-L3): the party rows' `createdAt` is seconds, `evictedAt` (the eviction
// pass's `now_ms`) and `updatedAtMs` are milliseconds; mixed, a game with a released hop always sorted newest.
macro_rules! identity_games {
    () => {
        "SELECT gameId,MAX(at) AS at FROM (SELECT gameId,createdAt*1000 AS at FROM potparty_records WHERE identity=?1 UNION ALL SELECT gameId,createdAt*1000 FROM hopparty_records WHERE identity=?1 UNION ALL SELECT gameId,evictedAt FROM released_hop_evidence WHERE identity=?1 UNION ALL SELECT gameId,evictedAt FROM released_hop_evidence_missing WHERE identity=?1 UNION ALL SELECT gameId,updatedAtMs FROM owed_rows WHERE identity=?1) WHERE gameId<>'' GROUP BY gameId ORDER BY at DESC,gameId"
    };
}
/// bsv-low #912: every game of an identity, newest first (the actor's bootstrap pages them, `?2` the offset).
pub const IDENTITY_GAMES_PAGE_SQL: &str = concat!("SELECT gameId FROM (", identity_games!(), " LIMIT 16 OFFSET ?2) WHERE 1");
/// Expand the one wanted envelope to durable game marks: the newest [`OWED_WANTED_GAMES`] games `wanted`, the rest
/// `backfill` ([`EXPAND_BACKFILL_SQL`]). An interrupted drain leaves all remaining games marked.
pub const EXPAND_WANTED_SQL: &str = concat!("INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) SELECT ?1,gameId,'wanted',?1,strftime('%s','now') * 1000 FROM (", identity_games!(), " LIMIT 32) WHERE 1 ON CONFLICT(identity,gameId,source,sourceKey) DO UPDATE SET generation=excluded.generation,markedAtMs=excluded.markedAtMs");
/// The wanted identity's older games, as the backfill marks them (`owed_backfill::MARK_SQL`'s source and key).
pub const EXPAND_BACKFILL_SQL: &str = concat!("INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) SELECT ?1,gameId,'backfill','owed-v2',strftime('%s','now') * 1000 FROM (", identity_games!(), " LIMIT -1 OFFSET 32) WHERE 1 ON CONFLICT(identity,gameId,source,sourceKey) DO UPDATE SET generation=excluded.generation,markedAtMs=excluded.markedAtMs");

/// One identity's subjects in its turn order: oldest mark first, a `wanted` mark the tie-break.
fn identity_subjects(marks: &[Value]) -> Vec<String> {
    let mut games: BTreeMap<String, (i64, bool, i64)> = BTreeMap::new();
    for m in marks {
        let Some(game) = m["gameId"].as_str().filter(|g| !g.is_empty()) else {
            continue;
        };
        let at = m["markedAtMs"].as_i64().unwrap_or_default();
        let generation = m["generation"].as_i64().unwrap_or_default();
        let g = games
            .entry(game.to_owned())
            .or_insert((at, true, generation));
        g.0 = g.0.min(at);
        g.1 &= m["source"] != "wanted";
        g.2 = g.2.min(generation);
    }
    let mut ordered: Vec<_> = games.into_iter().collect();
    ordered.sort_by_key(|(_, key)| *key);
    ordered.into_iter().map(|(game, _)| game).collect()
}

/// A wanted envelope (`gameId` empty) expanded to its identity's newest games (`wanted`) and the rest (`backfill`),
/// the envelope deleted up to the generation read. The drain's and the `owed-events` consumer's one expansion.
pub async fn expand_wanted(db: &impl SqlPort, identity: &str, generation: i64) -> Result<(), String> {
    db.run_batch(vec![
        Stmt {
            sql: EXPAND_WANTED_SQL.into(),
            binds: vec![Bind::Text(identity.to_owned())],
        },
        Stmt {
            sql: EXPAND_BACKFILL_SQL.into(),
            binds: vec![Bind::Text(identity.to_owned())],
        },
        Stmt {
            sql: "DELETE FROM owed_dirty WHERE identity=?1 AND gameId='' AND source='wanted' AND generation<=?2".into(),
            binds: vec![Bind::Text(identity.to_owned()), Bind::Int(generation)],
        },
    ])
    .await?;
    Ok(())
}

/// The scheduled drain (the D2-OWED lens's MEDIUM-1): expand the wanted envelopes, then take up to `limit` subjects
/// round-robin across identities, each identity's oldest first, from the identity after the last pass's; a subject
/// marked only by the age repair after every other subject. No statement scans `owed_dirty`.
pub async fn drain_dirty(db: &impl JudgeDb, limit: usize) -> Result<DrainReport, String> {
    let limit = limit.min(OWED_DIRTY_SUBJECTS_PER_PASS);
    let mut report = DrainReport::default();
    if limit == 0 {
        return Ok(report);
    }
    // Empty game IDs belong only to wanted envelopes, not derivable subjects.
    let wanted: Vec<Value> =
        hydrate::read(db, WANTED_ENVELOPES_SQL, vec![Bind::Int(limit as i64)]).await?;
    for r in wanted {
        let id = r["identity"]
            .as_str()
            .ok_or("wanted identity missing")?
            .to_owned();
        let generation = r["generation"]
            .as_i64()
            .ok_or("wanted generation missing")?;
        expand_wanted(db, &id, generation).await?;
    }
    let cursor: Vec<Value> = hydrate::read(db, DRAIN_CURSOR_READ_SQL, vec![]).await?;
    let cursor = cursor
        .first()
        .and_then(|r| r["cursor"].as_str())
        .unwrap_or_default()
        .to_owned();
    let walk = |after: String, n: usize| async move {
        hydrate::read::<Value>(
            db,
            DRAIN_IDENTITIES_SQL,
            vec![Bind::Text(after), Bind::Int(n as i64)],
        )
        .await
        .map(|rows| {
            rows.iter()
                .filter_map(|r| r["identity"].as_str().map(str::to_owned))
                .collect::<Vec<_>>()
        })
    };
    // The walk reads a full pass's identities whatever `limit` is, so the age repair waits behind every walked source
    // mark, not only the ones a short pass reached.
    let span = OWED_DIRTY_SUBJECTS_PER_PASS;
    let mut identities = walk(cursor.clone(), span).await?;
    if identities.len() < span && !cursor.is_empty() {
        for id in walk(String::new(), span - identities.len()).await? {
            if id > cursor || identities.contains(&id) {
                break;
            }
            identities.push(id);
        }
    }
    let per = limit.div_ceil(identities.len().max(1));
    let mut chosen: Vec<Subject> = Vec::new();
    let mut turned = vec![false; identities.len()];
    // Every source mark's turn before any subject marked only by the age repair (its idle room). One D1 batch reads
    // every walked identity's few oldest marks.
    for aged in [false, true] {
        if chosen.len() >= limit || identities.is_empty() {
            break;
        }
        let reads = identities
            .iter()
            .map(|id| Stmt {
                sql: DRAIN_MARKS_SQL.into(),
                binds: vec![
                    Bind::Text(id.clone()),
                    Bind::Int((per * 4) as i64),
                    Bind::Int(i64::from(aged)),
                ],
            })
            .collect();
        let lists: Vec<Vec<String>> = db
            .run_batch(reads)
            .await?
            .into_iter()
            .map(|out| identity_subjects(&out.rows))
            .collect();
        for round in 0..per {
            for (at, (id, games)) in identities.iter().zip(&lists).enumerate() {
                let Some(game) = games.get(round) else {
                    continue;
                };
                let subject = Subject {
                    identity: id.clone(),
                    game_id: game.clone(),
                };
                if chosen.len() < limit && !chosen.contains(&subject) {
                    chosen.push(subject);
                    turned[at] = true;
                }
            }
        }
    }
    db.log(&format!(
        "[owed] drain: {} identities walked, {} subjects chosen",
        identities.len(),
        chosen.len()
    ));
    // The next pass starts after the last identity this one gave a turn (every walked one when none was chosen).
    let last = turned
        .iter()
        .rposition(|t| *t)
        .map_or_else(|| identities.last(), |at| identities.get(at));
    if let Some(last) = last {
        db.run_batch(vec![Stmt {
            sql: DRAIN_CURSOR_SQL.into(),
            binds: vec![Bind::Text(last.clone()), Bind::Int(db.now_ms() as i64)],
        }])
        .await?;
    }
    for subject in chosen {
        match judge_subjects(
            db,
            &subject.identity,
            std::slice::from_ref(&subject.game_id),
            "drain",
        )
        .await
        {
            Ok(r) => report.subjects += r.subjects,
            Err(e) => report.faults.push((subject, e)),
        }
    }
    Ok(report)
}

/// Every unit fixture also crosses both real SQLite storage/read statements.
#[cfg(test)]
pub(crate) fn assert_fixture_roundtrip(i: &OwedInputs<'_>) {
    use rusqlite::{types::Value as SqlValue, Connection};
    fn bind(b: Bind) -> SqlValue {
        match b {
            Bind::Text(s) => SqlValue::Text(s),
            Bind::Int(n) => SqlValue::Integer(n),
            Bind::Null => SqlValue::Null,
        }
    }
    fn schema() -> Connection {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(crate::owed::OWED_ROWS_CREATE).unwrap();
        for col in [
            "judgedAtMs",
            "claimable",
            "gateHeight",
            "beltDueAtMs",
            "probeTip",
        ] {
            c.execute_batch(&format!("ALTER TABLE owed_rows ADD COLUMN {col} INTEGER"))
                .unwrap();
        }
        c.execute_batch(crate::owed::OWED_STATE_CREATE).unwrap();
        c.execute_batch(crate::owed::OWED_STATE_SEQ_ALTER).unwrap();
        c.execute_batch(crate::tracker_host::stored::CREATE)
            .unwrap();
        c.execute_batch("CREATE TABLE owed_row_words(identity TEXT,outpoint TEXT,role TEXT,txid TEXT,PRIMARY KEY(identity,outpoint,role))").unwrap();
        c
    }
    /// The word facts (joined at the read) and the facts the judge's write stamps.
    const STRIPPED: &[&str] = &[
        "coinWord",
        "spenderWord",
        "fundingWord",
        "tracker",
        "tipMoved",
        "servedTip",
        "beltNextAtMs",
    ];
    fn rows(c: &Connection, sql: &str, identity: &str) -> BTreeSet<String> {
        let mut s = c.prepare(sql).unwrap();
        s.query_map([identity], |r| {
            let mut facts: Value = serde_json::from_str(&r.get::<_, String>("facts")?).unwrap();
            for key in STRIPPED {
                facts.as_object_mut().unwrap().remove(*key);
            }
            Ok(json!([
                r.get::<_, String>("identity")?,
                r.get::<_, String>("outpoint")?,
                r.get::<_, String>("family")?,
                r.get::<_, String>("gameId")?,
                r.get::<_, Option<i64>>("sats")?,
                r.get::<_, Option<String>>("opponentIdentity")?,
                r.get::<_, Option<i64>>("atHeight")?,
                r.get::<_, Option<String>>("reason")?,
                facts
            ])
            .to_string())
        })
        .unwrap()
        .map(Result::unwrap)
        .collect()
    }
    let derived = derive_owed_rows(i);
    let mut new = schema();
    let tx = new.transaction().unwrap();
    for r in &derived {
        for stmt in row_statements(r, i.now_ms, i.tip, None) {
            tx.execute(
                &stmt.sql,
                rusqlite::params_from_iter(stmt.binds.into_iter().map(bind)),
            )
            .unwrap();
        }
    }
    tx.commit().unwrap();
    // The derived rows, read back through the judge's own read, field for field (bsv-low #893 section 6.2: the old
    // walk's write plan, the other side of this comparison, retired with the walk).
    let expected: BTreeSet<String> = derived
        .iter()
        .map(|r| {
            let mut facts = r.facts.clone();
            for key in STRIPPED {
                if let Some(o) = facts.as_object_mut() {
                    o.remove(*key);
                }
            }
            json!([
                r.identity,
                r.outpoint,
                r.family.as_str(),
                r.game_id,
                r.sats.map(|v| v as i64),
                r.opponent_identity,
                r.at_height.map(|v| v as i64),
                r.reason,
                facts
            ])
            .to_string()
        })
        .collect();
    assert_eq!(
        rows(&new, ROWS_READ_SQL, i.identity_lc),
        expected,
        "the judge's stored rows must equal the derivation on every input fixture"
    );
    let mut stmt = new.prepare(crate::owed_read::ROWS_SQL).unwrap();
    let names: Vec<_> = stmt.column_names().into_iter().map(str::to_owned).collect();
    let joined = stmt
        .query_map([i.identity_lc], |row| {
            let mut out = serde_json::Map::new();
            for (index, name) in names.iter().enumerate() {
                use rusqlite::types::ValueRef;
                let value = match row.get_ref(index)? {
                    ValueRef::Null => Value::Null,
                    ValueRef::Integer(n) => json!(n),
                    ValueRef::Real(n) => json!(n),
                    ValueRef::Text(s) => json!(std::str::from_utf8(s).unwrap()),
                    ValueRef::Blob(_) => unreachable!("the read joins decoded columns"),
                };
                out.insert(name.clone(), value);
            }
            Ok(Value::Object(out))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let snapshot = crate::owed_read::Snapshot {
        joined,
        state: Some(json!({"computedAtMs":i.now_ms,"tip":i.tip,"truncated":0})),
        pending: 0,
        wanted: false,
        downgrades: BTreeMap::new(),
    };
    let empty = BTreeMap::new();
    let mut served = snapshot
        .body(i.identity_lc, i.tip, i.spender_words.unwrap_or(&empty))
        .unwrap();
    let mut previous_rows = derived.clone();
    crate::owed::sort_rows_for_service(&mut previous_rows);
    let truncated = previous_rows.len() > crate::owed::OWED_MAX_ROWS;
    previous_rows.truncate(crate::owed::OWED_MAX_ROWS);
    let mut previous: Value = serde_json::from_str(&crate::owed::owed_body(
        i.identity_lc,
        i.tip,
        &previous_rows,
        i.now_ms,
        truncated,
        None,
    ))
    .unwrap();
    if truncated {
        previous["judgement"] =
            json!("could not judge the complete list this pass: bounded or faulted evidence read");
    }
    // Compare every committed input fixture through the production read. The
    // new projection fields are checked separately; the stable body is bytes.
    for (row, old) in served["rows"]
        .as_array()
        .unwrap()
        .iter()
        .zip(previous["rows"].as_array().unwrap())
    {
        assert_eq!(row["outpoint"], old["outpoint"]);
        for key in ["coinWord", "spenderWord", "fundingWord", "tracker"] {
            if let Some(word) = old["facts"].get(key) {
                assert_eq!(
                    &row["facts"][key], word,
                    "the joined {key} must preserve its served bytes"
                );
            }
        }
    }
    for body in [&mut served, &mut previous] {
        body.as_object_mut().unwrap().remove("pending");
        for row in body["rows"].as_array_mut().unwrap() {
            for key in [
                "coinWord",
                "spenderWord",
                "fundingWord",
                "tracker",
                "refresh",
                "carried",
                "claimableBeforeShed",
                "supersededFallback",
                "judgedAtMs",
                "spenderUnread",
                "tipMoved",
                "servedTip",
                "beltNextAtMs",
            ] {
                row["facts"].as_object_mut().unwrap().remove(key);
            }
        }
    }
    assert_eq!(
        served.to_string(),
        previous.to_string(),
        "the stable owed body bytes must survive the joined read"
    );
}

#[cfg(test)]
mod cadence_health_tests {
    use super::*;
    #[tokio::test]
    async fn cadence_dirty_health_reads_only_the_drain_snapshot() {
        let db = crate::pot_hook::tests::SqliteSql::migrated();
        db.now.set(900_000);
        assert_eq!(dirty_health(&db).await, Value::Null);
        // A rolling old overlay replays its historical index drop; the new snapshot still works.
        db.conn.execute_batch(bsv_overlay_cloudflare::derived_owed::DIRTY_MARKED_DROP).unwrap();
        snapshot_dirty(&db).await.unwrap();
        assert_eq!(
            dirty_health(&db).await,
            json!({"count":0,"oldestMarkAgeMs":null,"asOfMs":900000})
        );
        db.conn.execute("INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES ('seat','game','pot','pot:0',1000),('seat','other','word','word',800000)", []).unwrap();
        // Reads never walk the newly written marks or pretend the last tick was current.
        assert_eq!(dirty_health(&db).await["count"], 0);
        snapshot_dirty(&db).await.unwrap();
        db.now.set(901_000);
        assert_eq!(
            dirty_health(&db).await,
            json!({"count":2,"oldestMarkAgeMs":900000,"asOfMs":900000})
        );
        db.conn.execute("DROP TABLE owed_dirty", []).unwrap();
        assert_eq!(
            dirty_health(&db).await["count"],
            2,
            "health must not touch the backlog table"
        );
        db.conn.execute("DROP TABLE owed_belt_state", []).unwrap();
        assert_eq!(dirty_health(&db).await, Value::Null);
    }
}
