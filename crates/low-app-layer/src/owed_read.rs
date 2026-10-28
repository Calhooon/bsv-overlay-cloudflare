//! The served owed projection. Reads never hydrate or judge a game.
use crate::{
    owed,
    pot_hook::{Bind, SqlPort, Stmt, StmtOut},
    tracker_host::stored::Row,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};

/// The extra proof columns keep the existing served-word header recheck intact.
pub const ROWS_SQL: &str = "SELECT r.identity,r.outpoint,r.family,r.gameId,r.sats,r.opponentIdentity,r.atHeight,r.facts,r.reason,r.judgedAtMs,r.claimable,r.probeTip,r.gateHeight,r.beltDueAtMs,w.role,w.txid AS wordTxid,t.word,t.tier,t.height,t.proof_root,t.proof_height,t.proof_hex,t.header_hash,t.hint_source,t.hint_at,t.updated_at,CASE WHEN json_valid(t.legacy_body) THEN json_type(t.legacy_body,'$.trackerTerminal')='true' END AS trackerTerminal,CASE WHEN json_valid(t.legacy_body) THEN json_extract(t.legacy_body,'$.trackerTerminalWord') END AS trackerTerminalWord FROM owed_rows r LEFT JOIN owed_row_words w ON w.identity=r.identity AND w.outpoint=r.outpoint LEFT JOIN tx_words t ON t.txid=w.txid WHERE r.identity=?1 ORDER BY CASE r.family WHEN 'payout' THEN 0 WHEN 'refund-due' THEN 1 WHEN 'hop-stranded' THEN 2 WHEN 'in-progress' THEN 3 ELSE 4 END,COALESCE(r.atHeight,0) DESC,r.outpoint,w.role LIMIT 2004";
pub const STATE_SQL: &str =
    "SELECT computedAtMs,rows,truncated,recomputeSeq,tip FROM owed_state WHERE identity=?1";
/// The identity's pending marks: their count, and (the second delta lens's D2-L1) the games holding an undrained mark
/// of the DOWNGRADE class (the third delta lens's D3-L1: the mark's own `downgrade`, written by the trigger: the index
/// recording a spend, a pot row deleted, an eviction; a `pot` stamp, facts fill or re-present is not one), over the
/// covering UNIQUE index's identity prefix and the row. The count already visits every pending mark of the identity, so naming
/// them adds no row and no statement. A mark still in `owed_dirty` is one no judge has acknowledged since it was
/// written (the judge deletes `generation <= seen` for each subject it judges), so its presence is what "newer than the
/// row's judgement" means; `markedAtMs` cannot say it (a `pot` mark carries the pot row's `createdAt`). A filing's, a
/// tip's, a wanted or an age mark is not a downgrade; a belt find is judged in its own batch and leaves no mark.
///
/// bsv-low #912 (the D2-ACTOR-B delta lens's NEW-M3): the same row carries the Phase C rollback's two stamps
/// (`view_derivation_state`, two primary-key probes, no statement added): `actorJudged`, written by every Phase C
/// acknowledgement (`owed_actor::PHASE_C_MARK_SQL`), and `owedV3`, the re-judge's stamp, which that acknowledgement
/// deletes. While the first stands without the second, D1's `owed_rows` are the rows of the Phase C flip, not a
/// judgement, and the D1 read serves no press from them.
pub const PENDING_SQL: &str = "SELECT COUNT(*) AS pending,json_group_array(CASE WHEN downgrade=1 THEN json_array(gameId,source) END) AS downgrades,(SELECT COUNT(*) FROM view_derivation_state WHERE name='owed-actor-judged') AS actorJudged,(SELECT COUNT(*) FROM view_derivation_state WHERE name='owed-v3') AS owedV3 FROM owed_dirty WHERE identity=?1";
/// The served reason of a press withheld over a pending downgrade (D2-L1).
pub const PENDING_DOWNGRADE_REASON: &str =
    "re-judging: a downgrade of this game is recorded and not yet judged; the press waits for the judgement";
/// The served reason of a press withheld while the Phase C rollback's re-judge has not stamped `owed-v3` (NEW-M3).
pub const ROLLBACK_PENDING_REASON: &str =
    "re-judging: the owed list is being rebuilt after the actor served it; the press waits for the re-judge";

#[derive(Debug, Default)]
pub struct Snapshot {
    pub joined: Vec<Value>,
    pub state: Option<Value>,
    pub pending: u64,
    pub wanted: bool,
    /// bsv-low #912 (the D2-ACTOR-B delta lens's NEW-M1): the actor's read (`rows: false`) found the identity by the
    /// indexed probe (a party, hop marker or evicted party row) with no stated list, and wrote NO wanted mark: the
    /// actor judges the identity (its events, its bootstrap), so a mark would only re-judge it whole each sweep.
    pub known: bool,
    /// NEW-M3: the D1 read of a database a Phase C actor judged, before the `owed-v3` re-judge stamped: no press.
    pub rollback_pending: bool,
    /// The games holding an undrained downgrade-class mark, each with its source (D2-L1).
    pub downgrades: BTreeMap<String, String>,
}

fn number(value: &Value) -> Option<u64> {
    value
        .as_f64()
        .filter(|v| v.is_finite() && *v >= 0.0)
        .map(|v| v as u64)
}

// A wanted envelope has no time gate. Its generation orders it, and the drain
// prioritizes its source. Zero lets the existing marker run without a clock.
struct WantedMarker<'a, D>(&'a D);
impl<D: SqlPort> SqlPort for WantedMarker<'_, D> {
    async fn run_batch(&self, statements: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        self.0.run_batch(statements).await
    }
    async fn counters(&self, _: Vec<(String, u64)>) {}
    fn now_ms(&self) -> u64 {
        0
    }
    fn log(&self, _: &str) {}
}

/// The only optional fourth read is the existing indexed identity probe.
/// A wanted envelope is one insert; the writer's drain owns all subsequent work.
pub async fn read(db: &impl SqlPort, identity: &str) -> Result<Snapshot, String> {
    read_with(db, identity, true).await
}

/// bsv-low #912 (the D2-ACTOR-B lens's LOW-5): the actor's read (Phase B) takes the state and the pending marks
/// only; its rows are the actor's. `rows: false` runs [`STATE_SQL`] and [`PENDING_SQL`] (and the identity probe of
/// an unstated identity), never [`ROWS_SQL`].
pub async fn read_with(db: &impl SqlPort, identity: &str, rows: bool) -> Result<Snapshot, String> {
    let sqls: &[&str] = if rows {
        &[ROWS_SQL, STATE_SQL, PENDING_SQL]
    } else {
        &[STATE_SQL, PENDING_SQL]
    };
    let mut batch = db
        .run_batch(
            sqls.iter()
                .map(|sql| Stmt {
                    sql: (*sql).into(),
                    binds: vec![Bind::Text(identity.into())],
                })
                .collect(),
        )
        .await?;
    if batch.len() != sqls.len() {
        return Err("incomplete owed read batch".into());
    }
    if !rows {
        batch.insert(0, StmtOut::default());
    }
    let marks = batch.pop().unwrap().rows.into_iter().next();
    let pending = marks
        .as_ref()
        .and_then(|r| number(&r["pending"]))
        .ok_or("invalid owed pending count")?;
    let mut downgrades = BTreeMap::new();
    let named: Value = marks
        .as_ref()
        .and_then(|r| r["downgrades"].as_str())
        .map(serde_json::from_str)
        .transpose()
        .map_err(|e| format!("owed pending downgrades: {e}"))?
        .unwrap_or(Value::Null);
    for mark in named.as_array().into_iter().flatten() {
        if let (Some(game), Some(source)) = (mark[0].as_str(), mark[1].as_str()) {
            downgrades
                .entry(game.to_owned())
                .or_insert_with(|| source.to_owned());
        }
    }
    let state = batch.pop().unwrap().rows.into_iter().next();
    let mut snapshot = Snapshot {
        joined: batch.pop().unwrap().rows,
        state,
        pending,
        wanted: false,
        known: false,
        rollback_pending: rows
            && marks
                .as_ref()
                .and_then(|r| number(&r["actorJudged"]))
                .unwrap_or(0)
                > 0
            && marks
                .as_ref()
                .and_then(|r| number(&r["owedV3"]))
                .unwrap_or(0)
                == 0,
        downgrades,
    };
    if snapshot.state.is_none() {
        snapshot.joined.clear();
        let probe = db
            .run_batch(vec![Stmt {
                sql: owed::OWED_IDENTITY_PROBE_SQL.into(),
                binds: vec![Bind::Text(identity.into())],
            }])
            .await?;
        if probe.first().is_some_and(|r| !r.rows.is_empty()) {
            if rows {
                crate::owed_judge::mark_wanted(&WantedMarker(db), identity).await?;
                snapshot.wanted = true;
            } else {
                snapshot.known = true;
            }
        }
    }
    Ok(snapshot)
}

impl Snapshot {
    /// A claimable sweep always needs the present tip: without it the read withholds the press (HIGH-1).
    pub fn needs_tip(&self) -> bool {
        self.joined.iter().any(|r| {
            number(&r["gateHeight"]).is_some()
                || number(&r["probeTip"]).is_some()
                || (r["family"] == owed::OwedFamily::HopStranded.as_str()
                    && r["claimable"].as_i64() == Some(1))
        })
    }

    /// All tracker inputs came from the first statement, including proof bytes.
    pub fn word_rows(&self) -> Result<BTreeMap<String, Row>, String> {
        let mut words = BTreeMap::new();
        for row in &self.joined {
            let Some(txid) = row["wordTxid"].as_str() else {
                continue;
            };
            if row["word"].is_null() {
                continue;
            }
            let mut word = row.clone();
            word["txid"] = json!(txid);
            // Retain the terminal hint, never the legacy transaction bytes.
            word["legacy_body"] = if number(&row["trackerTerminal"]) == Some(1) {
                json!(json!({"trackerTerminal":true,"trackerTerminalWord":row["trackerTerminalWord"]}).to_string())
            } else {
                Value::Null
            };
            let word: Row = serde_json::from_value(word).map_err(|e| format!("owed word: {e}"))?;
            words.insert(txid.into(), word);
        }
        Ok(words)
    }

    /// Only projection fields change here. Claim decisions stay with the judge.
    pub fn body(
        &self,
        identity: &str,
        present_tip: Option<u64>,
        words: &BTreeMap<String, Value>,
    ) -> Result<Value, String> {
        let tip = present_tip.or_else(|| self.state.as_ref().and_then(|s| number(&s["tip"])));
        let mut rows = Vec::<Value>::new();
        let mut positions = HashMap::<String, usize>::new();
        let mut roles = Vec::<BTreeMap<String, String>>::new();
        for joined in &self.joined {
            let outpoint = joined["outpoint"].as_str().ok_or("owed outpoint missing")?;
            let position = if let Some(position) = positions.get(outpoint) {
                *position
            } else {
                let mut facts: Value =
                    serde_json::from_str(joined["facts"].as_str().ok_or("owed facts missing")?)
                        .map_err(|e| e.to_string())?;
                let object = facts
                    .as_object_mut()
                    .ok_or("owed facts must be an object")?;
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
                ] {
                    object.remove(key);
                }
                // bsv-low #893 section 5: the belt's next look at this row, as the judge or the belt dated it (null:
                // the index or a checked word decides the row, nothing is owed the chain).
                object.insert("beltNextAtMs".into(), json!(number(&joined["beltDueAtMs"])));
                // bsv-low #669 (the D2-OWED lens's HIGH-1): the read-time freshness gate, per row. A claimable sweep
                // rests on the tip it was judged at (`probeTip`; NULL is unknown); a block since, or a present tip that
                // cannot be read, degrades it IN THE SERVED BODY to a pending chain-wait, never a dead sweep.
                // The delta lens's D-H1: an index-UNSPENT sweep keeps its press, the move stated
                // (`owed::state_index_unspent_sweep`); only a press resting on a courier probe alone is degraded.
                let mut reason = joined["reason"].as_str().map(str::to_owned);
                let is_sweep = joined["family"] == owed::OwedFamily::HopStranded.as_str();
                if is_sweep
                    && !owed::state_index_unspent_sweep(
                        &mut facts,
                        number(&joined["probeTip"]),
                        present_tip,
                    )
                    && facts["claimable"] == Value::Bool(true)
                {
                    let mut one = [owed::OwedRow {
                        identity: identity.into(),
                        outpoint: outpoint.into(),
                        family: owed::OwedFamily::HopStranded,
                        game_id: String::new(),
                        sats: None,
                        opponent_identity: None,
                        at_height: None,
                        facts,
                        reason,
                    }];
                    owed::degrade_stale_sweeps(&mut one, number(&joined["probeTip"]), present_tip);
                    let [row] = one;
                    facts = row.facts;
                    reason = row.reason;
                }
                // The second delta lens's D2-L1 (the captain's ruling): a read never does work, but never serves a
                // press over a pending downgrade. A game holding an undrained downgrade-class mark serves its press
                // withheld as re-judging; the drain's judgement lifts it.
                // NEW-M3: D1's rows after a Phase C actor judged, before the `owed-v3` re-judge: no press.
                if self.rollback_pending && facts["claimable"] == Value::Bool(true) {
                    facts["claimable"] = json!(false);
                    facts["claimableBeforePending"] = json!(true);
                    facts["refresh"] = json!("pending");
                    facts["pendingSource"] = json!("owed-v3");
                    facts["stale"] = json!(true);
                    facts["retryAfterMs"] = json!(1500);
                    facts["claimReason"] = json!(ROLLBACK_PENDING_REASON);
                }
                if facts["claimable"] == Value::Bool(true) {
                    if let Some(source) = joined["gameId"]
                        .as_str()
                        .and_then(|game| self.downgrades.get(game))
                    {
                        facts["claimable"] = json!(false);
                        facts["claimableBeforePending"] = json!(true);
                        facts["refresh"] = json!("pending");
                        facts["pendingSource"] = json!(source);
                        facts["stale"] = json!(true);
                        facts["retryAfterMs"] = json!(1500);
                        facts["claimReason"] = json!(PENDING_DOWNGRADE_REASON);
                    }
                }
                let position = rows.len();
                positions.insert(outpoint.into(), position);
                roles.push(BTreeMap::new());
                rows.push(json!({"outpoint":outpoint,"family":joined["family"],"gameId":joined["gameId"],"sats":number(&joined["sats"]),"opponentIdentity":joined["opponentIdentity"],"atHeight":number(&joined["atHeight"]),"facts":facts,"reason":reason}));
                position
            };
            if let (Some(role), Some(txid)) = (joined["role"].as_str(), joined["wordTxid"].as_str())
            {
                roles[position].insert(role.into(), txid.into());
            }
        }
        let truncated = self
            .state
            .as_ref()
            .is_some_and(|s| number(&s["truncated"]).unwrap_or(0) != 0)
            || rows.len() > owed::OWED_MAX_ROWS;
        rows.truncate(owed::OWED_MAX_ROWS);
        for (row, roles) in rows.iter_mut().zip(roles) {
            let facts = &mut row["facts"];
            let word = |txid: &str| {
                words.get(txid).cloned().unwrap_or_else(|| {
                    crate::tracker_host::served::fields(&bsv_tracker::State::new(txid))
                })
            };
            if let Some(txid) = roles.get("spender").or_else(|| roles.get("settle")) {
                let view = word(txid);
                let intended = facts["sweepTxid"]
                    .as_str()
                    .or_else(|| facts["settleTxid"].as_str());
                facts["coinWord"] = owed::spender_coin_word(&view, txid, intended);
                facts["spenderWord"] = view;
            }
            if facts["fundingHeld"] == owed::FUNDING_HELD_REFUSED {
                if let Some(txid) = roles.get("funding") {
                    if let Some(refusal) = owed::funding_refusal_word(Some(words), txid) {
                        facts["fundingWord"] = json!(refusal);
                    }
                }
            }
        }
        let mut body = json!({"v":owed::OWED_WIRE_VERSION,"identity":identity,"tip":tip,"rows":rows,"truncated":truncated,"computedAtMs":self.state.as_ref().and_then(|s|number(&s["computedAtMs"])).unwrap_or(0),"pending":self.pending});
        if let Some(seq) = self.state.as_ref().and_then(|s| number(&s["recomputeSeq"])) {
            body["recomputeSeq"] = json!(seq);
        }
        if self.wanted {
            body["pending"] = json!(true);
        }
        if self.wanted || self.pending > 0 {
            body["retryAfterMs"] = json!(1500);
        }
        if truncated {
            body["judgement"] = json!(
                "could not judge the complete list this pass: bounded or faulted evidence read"
            );
        }
        crate::tracker_host::runtime::decorate(&mut body, None, words);
        Ok(body)
    }
}
