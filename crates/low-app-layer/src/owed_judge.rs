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
mod writers;
pub use writers::{drain_changed, drain_identities, judge_word_write, mark_tip, MARK_TIP_SQL};
pub(crate) use hydrate::hydrate_filings;
pub use hydrate::{
    hydrate_inputs, HOPS_SQL, LINKS_SQL, PARTIES_SQL, RELEASED_SQL, REPRESENTATIVES_SQL,
};

pub const OWED_JUDGE_SUBJECTS_PER_INVOCATION: usize = 16;
pub const OWED_DIRTY_SUBJECTS_PER_PASS: usize = 64;
/// Required source-table access paths, by index name: the overlay ships both (the native pin holds it).
pub const REQUIRED_GAME_INDEX_NAMES: [&str; 2] =
    ["idx_potparty_game_identity", "idx_evicted_party_identity_game"];
pub const MARK_WANTED_SQL: &str = "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) VALUES (?1,'','wanted',?1,?2) ON CONFLICT(identity,gameId,source,sourceKey) DO UPDATE SET generation=excluded.generation,markedAtMs=excluded.markedAtMs";
pub const MARKS_SQL: &str = "SELECT generation,source,sourceKey FROM owed_dirty WHERE identity=?1 AND gameId IN (SELECT value FROM json_each(?2))";
/// Wanted first; a subject marked only by the daily age repair last (`owed_backfill::AGE_MARK_SQL`).
pub const SUBJECTS_SQL: &str = "SELECT identity,gameId FROM owed_dirty GROUP BY identity,gameId ORDER BY MAX(source='wanted') DESC,MIN(source='age'),MIN(markedAtMs),MIN(generation) LIMIT ?1";
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
    async fn hydrate(&self, identity: &str, games: &[String]) -> Result<JudgeInputs, String>
    where Self: Sized {
        hydrate_inputs(self, identity, games).await
    }
}

/// Production adapter. All SQL, including the final write, uses the counted D1 port.
pub struct D1Judge {
    sql: crate::routes::D1Sql,
    tip: Option<u64>,
    era_ms: Option<i64>,
}
impl D1Judge {
    pub fn new(env: &worker::Env, tip: Option<u64>, era_ms: Option<i64>) -> worker::Result<Self> {
        Ok(Self {
            sql: crate::routes::D1Sql::of(env)?,
            tip,
            era_ms,
        })
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
        crate::internal_events::first_party_push(&self.sql.env, identity, body).await;
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
    funding_seen: HashSet<String>,
    word_rows: BTreeMap<String, crate::tracker_host::stored::Row>,
    moved_latches: Vec<(String, String)>,
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
        binds: vec![
            Bind::Text(identity.to_ascii_lowercase()),
            Bind::Int(db.now_ms() as i64),
        ],
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
    let subjects: BTreeSet<_> = subjects
        .iter()
        .map(|s| {
            (
                s.identity.to_ascii_lowercase(),
                s.game_id.to_ascii_lowercase(),
            )
        })
        .collect();
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
        hydrate::finish_rows(db, &mut rows).await?;
        crate::owed::sort_rows_for_service(&mut rows);
        let cut = rows.len() > crate::owed::OWED_MAX_ROWS;
        rows.truncate(crate::owed::OWED_MAX_ROWS);
        plan.push(Stmt{sql:"DELETE FROM owed_rows WHERE identity=?1 AND gameId IN (SELECT value FROM json_each(?2))".into(),binds:vec![Bind::Text(id.clone()),Bind::Text(json!(games).to_string())]});
        for row in &rows {
            plan.extend(row_statements(row, i.now_ms));
        }
        if let Some(moved) = crate::owed::moved_proof_latch_delete_bind(&i.moved_latches) {
            plan.push(Stmt {
                sql: crate::owed::PROOF_LATCH_MOVED_DELETE_SQL.into(),
                binds: vec![Bind::Text(moved)],
            });
        }
        // Rank at the shared ceiling inside the transaction, so two different games cannot overfill it.
        plan.push(Stmt{sql:format!("DELETE FROM owed_rows WHERE identity=?1 AND outpoint IN (SELECT outpoint FROM owed_rows WHERE identity=?1 ORDER BY CASE family WHEN 'payout' THEN 0 WHEN 'refund-due' THEN 1 WHEN 'hop-stranded' THEN 2 WHEN 'in-progress' THEN 3 ELSE 4 END,COALESCE(atHeight,0) DESC,outpoint LIMIT -1 OFFSET {})",crate::owed::OWED_MAX_ROWS),binds:vec![Bind::Text(id.clone())]});
        // A partial judge cannot prove that other games' omitted rows were rebuilt.
        // Preserve an earlier overflow flag until a complete backfill resets it.
        plan.push(Stmt{sql:"INSERT INTO owed_state(identity,computedAtMs,tip,rows,stale,truncated,recomputeSeq) SELECT ?1,?2,?3,COUNT(*),0,(?4 OR changes()>0),1 FROM owed_rows WHERE identity=?1 ON CONFLICT(identity) DO UPDATE SET computedAtMs=MAX(owed_state.computedAtMs,excluded.computedAtMs),tip=excluded.tip,rows=excluded.rows,stale=0,truncated=(owed_state.truncated OR excluded.truncated),recomputeSeq=COALESCE(owed_state.recomputeSeq,0)+1".into(),binds:vec![Bind::Text(id.clone()),Bind::Int(i.now_ms),i.tip.map_or(Bind::Null,|n|Bind::Int(n as i64)),Bind::Int(i64::from(cut))]});
        let mut predicate = "generation <= ?3".to_owned();
        let mut binds = vec![
            Bind::Text(id.clone()),
            Bind::Text(json!(games).to_string()),
            Bind::Int(seen[idx]),
        ];
        for mark in owned_marks {
            predicate.push_str(&format!(
                " OR (source=?{} AND sourceKey=?{})",
                binds.len() + 1,
                binds.len() + 2
            ));
            binds.extend([
                Bind::Text(mark.source.clone()),
                Bind::Text(mark.source_key.clone()),
            ]);
        }
        plan.push(Stmt{sql:format!("DELETE FROM owed_dirty WHERE identity=?1 AND gameId IN (SELECT value FROM json_each(?2)) AND ({predicate})"),binds});
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
    for (pos, id, stamp) in state_positions {
        let state = landed
            .get(pos)
            .and_then(|s| s.rows.first())
            .ok_or("judge state readback missing")?;
        let count = state["rows"].as_u64().ok_or("judge state count missing")? as usize;
        report.rows += count;
        report.truncated |= state["truncated"].as_i64().unwrap_or(0) != 0;
        db.push(
            &id,
            crate::owed::owed_changed_event_body(&id, source, count, stamp),
        )
        .await;
    }
    Ok(report)
}

fn row_statements(row: &OwedRow, stamp: i64) -> Vec<Stmt> {
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
    let probe = row.facts["chainProbeTip"].as_i64();
    let due = match row.family {
        crate::owed::OwedFamily::HopStranded => Some(stamp + 600_000),
        crate::owed::OwedFamily::Unbound
            if row.facts["spendingTxid"].is_string()
                || row.facts["settleTxid"].is_string()
                || row.facts["chainSpender"].is_string() =>
        {
            Some(stamp + 300_000)
        }
        crate::owed::OwedFamily::Payout if row.facts["creditKind"] == "courier-bytes" => {
            Some(stamp + 600_000)
        }
        crate::owed::OwedFamily::InProgress if row.facts["hopTxid"].is_string() => Some(
            row.facts["markerCreatedAtMs"]
                .as_i64()
                .unwrap_or(stamp)
                .saturating_add(crate::owed::HOP_STRANDED_AFTER_MS)
                .max(stamp),
        ),
        _ => None,
    };
    strip_words(&mut row);
    if let Some(facts) = row.facts.as_object_mut() {
        for k in [
            "refresh",
            "carried",
            "claimableBeforeShed",
            "supersededFallback",
            "judgedAtMs",
            "spenderUnread",
            "tipMoved",
            "servedTip",
        ] {
            facts.remove(k);
        }
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

/// Both pot seats, including the filing's subject before its new party row exists.
pub async fn refund_subjects(
    db: &impl SqlPort,
    pot: &str,
    vout: u32,
    filer: &str,
    game: &str,
) -> Result<Vec<Subject>, String> {
    let rows:Vec<Value>=hydrate::read(db,"SELECT identity,gameId FROM potparty_records INDEXED BY idx_potparty_pot WHERE potTxid=?1 AND potVout=?2 UNION SELECT hp.identity,hp.gameId FROM hop_pot_links l INDEXED BY idx_hop_pot_links_pot JOIN hopparty_records hp ON hp.rowid=l.markerRowid WHERE l.potTxid=?1 AND l.potVout=?2",vec![Bind::Text(pot.to_ascii_lowercase()),Bind::Int(i64::from(vout))]).await?;
    let mut subjects: BTreeSet<_> = rows
        .iter()
        .filter_map(|r| {
            Some((
                r["identity"].as_str()?.to_ascii_lowercase(),
                r["gameId"].as_str()?.to_ascii_lowercase(),
            ))
        })
        .collect();
    subjects.insert((filer.to_ascii_lowercase(), game.to_ascii_lowercase()));
    Ok(subjects
        .into_iter()
        .map(|(identity, game_id)| Subject { identity, game_id })
        .collect())
}

/// Expand the one wanted envelope to durable game marks. An interrupted drain leaves all remaining games marked.
pub const EXPAND_WANTED_SQL:&str="INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) SELECT ?1,gameId,'wanted',?1,?2 FROM (SELECT gameId FROM potparty_records WHERE identity=?1 UNION SELECT gameId FROM hopparty_records WHERE identity=?1 UNION SELECT gameId FROM released_hop_evidence WHERE identity=?1 UNION SELECT gameId FROM owed_rows WHERE identity=?1) WHERE gameId<>'' ON CONFLICT(identity,gameId,source,sourceKey) DO UPDATE SET generation=excluded.generation,markedAtMs=excluded.markedAtMs";
pub async fn drain_dirty(db: &impl JudgeDb, limit: usize) -> Result<DrainReport, String> {
    let limit = limit.min(OWED_DIRTY_SUBJECTS_PER_PASS);
    let mut report = DrainReport::default();
    if limit == 0 {
        return Ok(report);
    }
    // Empty game IDs belong only to wanted envelopes, not derivable subjects.
    let wanted:Vec<Value>=hydrate::read(db,"SELECT identity,generation FROM owed_dirty WHERE source='wanted' AND gameId='' ORDER BY markedAtMs,generation LIMIT ?1",vec![Bind::Int(limit as i64)]).await?;
    for r in wanted {
        let id = r["identity"]
            .as_str()
            .ok_or("wanted identity missing")?
            .to_owned();
        let generation = r["generation"]
            .as_i64()
            .ok_or("wanted generation missing")?;
        db.run_batch(vec![Stmt{sql:EXPAND_WANTED_SQL.into(),binds:vec![Bind::Text(id.clone()),Bind::Int(db.now_ms() as i64)]},Stmt{sql:"DELETE FROM owed_dirty WHERE identity=?1 AND gameId='' AND source='wanted' AND generation<=?2".into(),binds:vec![Bind::Text(id),Bind::Int(generation)]}]).await?;
    }
    let candidates: Vec<Value> =
        hydrate::read(db, SUBJECTS_SQL, vec![Bind::Int(limit as i64)]).await?;
    for r in candidates {
        let subject = Subject {
            identity: r["identity"]
                .as_str()
                .ok_or("dirty identity missing")?
                .to_owned(),
            game_id: r["gameId"].as_str().ok_or("dirty game missing")?.to_owned(),
        };
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
    fn rows(c: &Connection, sql: &str, identity: &str) -> BTreeSet<String> {
        let mut s = c.prepare(sql).unwrap();
        s.query_map([identity], |r| {
            let mut facts: Value = serde_json::from_str(&r.get::<_, String>("facts")?).unwrap();
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
                facts.as_object_mut().unwrap().remove(key);
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
    let mut old = schema();
    let mut new = schema();
    let tx = old.transaction().unwrap();
    for (sql, binds) in
        crate::owed::owed_write_plan(i.identity_lc, &derived, i.now_ms, i.tip, false)
    {
        let values = binds.into_iter().map(|b| match b {
            crate::owed::OwedBind::Text(s) => SqlValue::Text(s),
            crate::owed::OwedBind::Int(n) => SqlValue::Integer(n),
            crate::owed::OwedBind::Null => SqlValue::Null,
        });
        let mut stmt = tx.prepare(sql).unwrap();
        let mut result = stmt.query(rusqlite::params_from_iter(values)).unwrap();
        while result.next().unwrap().is_some() {}
    }
    tx.commit().unwrap();
    let tx = new.transaction().unwrap();
    for r in &derived {
        for stmt in row_statements(r, i.now_ms) {
            tx.execute(
                &stmt.sql,
                rusqlite::params_from_iter(stmt.binds.into_iter().map(bind)),
            )
            .unwrap();
        }
    }
    tx.commit().unwrap();
    assert_eq!(
        rows(&old, crate::owed::OWED_ROWS_READ_SQL, i.identity_lc),
        rows(&new, ROWS_READ_SQL, i.identity_lc),
        "the judge's stored rows must equal the walk's on every input fixture"
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
        body.as_object_mut().unwrap().remove("markerlessProgress");
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
