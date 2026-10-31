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
/// recording a spend, a pot row deleted, an eviction; a `pot` stamp, facts fill or re-present is not one), each with
/// its source and its GENERATION (FIX-968: the gate compares it with the actor's covered generation, below), over the
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
///
/// bsv-low #1004 (lane FIX-1004, L55 S4): `framed`, every mark an EVENT carries (`gameId`, `generation`), i.e. every
/// source but [`UNFRAMED_SOURCES`]: the actor's read counts only those of them its last apply has not covered
/// ([`Snapshot::actor_pending`]), never D1's whole count. The list is three `<>` terms, never `NOT IN`: an IN list
/// is an ephemeral table SQLite probes once a mark, and D1 counts each probe a row read (the budget tier's
/// `owed-first-read`, lane FIX-1004-FOLD: the largest statement 19 to 37 with `NOT IN`, pinned by
/// `fix1004f_the_pending_read_probes_no_ephemeral_table`).
pub const PENDING_SQL: &str = "SELECT COUNT(*) AS pending,json_group_array(CASE WHEN downgrade=1 THEN json_array(gameId,source,generation) END) AS downgrades,json_group_array(CASE WHEN source<>'age' AND source<>'backfill' AND source<>'wanted' THEN json_array(gameId,generation) END) AS framed,(SELECT COUNT(*) FROM view_derivation_state WHERE name='owed-actor-judged') AS actorJudged,(SELECT COUNT(*) FROM view_derivation_state WHERE name='owed-v3') AS owedV3 FROM owed_dirty WHERE identity=?1";
/// bsv-low #1004: the mark sources no writer's event carries. An `age` mark (the drain's daily re-mark of D1 rows
/// judged a day ago: in Phase C D1's `owed_rows` have no writer, so the same oldest subjects were re-marked on every
/// scheduled pass), a `backfill` mark (the one-time backfill, a wanted envelope's rest, the reconcile's rest) and a
/// `wanted` envelope are acknowledged only when the sweep re-publishes them past its horizon, minutes later: they are
/// the reconcile's work, never an event the actor owes the read. Pinned equal to [`PENDING_SQL`]'s list.
pub const UNFRAMED_SOURCES: [&str; 3] = ["age", "backfill", "wanted"];
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
    /// The games holding an undrained downgrade-class mark (D2-L1), each mark's `(generation, source)` (FIX-968).
    pub downgrades: BTreeMap<String, Vec<(i64, String)>>,
    /// FIX-968 (bsv-low #968): the actor's COVERED generation per game (lowercased), from its `/rows` answer
    /// (`owed_actor::COVERED_BY_GAME_SQL`: the greatest `applied` `covered` seq of the game, the marks bound its
    /// last apply read the record at). Empty on the D1 read (`rows: true`) and where the actor answered none: then
    /// every pending downgrade mark withholds, as before FIX-968.
    pub covered: BTreeMap<String, i64>,
    /// D2-ACTOR-F (bsv-low #959): the EXACT generations above [`Snapshot::covered`] whose rows a carried apply applied
    /// (`owed_actor::carried::COVERED_EXACT_BY_GAME_SQL`, answered `coveredExact`): such a mark is applied, every
    /// other mark above the covered generation is not (the gate and `pending` count it, as before).
    pub covered_exact: BTreeMap<String, std::collections::BTreeSet<i64>>,
    /// bsv-low #1004: every pending mark an event carries (`PENDING_SQL`'s `framed`): its game and generation.
    pub framed: Vec<(String, i64)>,
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
            // FIX-968: a generation the row does not carry (an older statement's answer) reads as the greatest, so
            // the mark withholds as before (the safe direction).
            let generation = mark[2].as_i64().unwrap_or(i64::MAX);
            downgrades
                .entry(game.to_owned())
                .or_insert_with(Vec::new)
                .push((generation, source.to_owned()));
        }
    }
    let framed_marks: Value = marks
        .as_ref()
        .and_then(|r| r["framed"].as_str())
        .map(serde_json::from_str)
        .transpose()
        .map_err(|e| format!("owed pending framed marks: {e}"))?
        .unwrap_or(Value::Null);
    let framed = framed_marks
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| Some((m[0].as_str()?.to_owned(), m[1].as_i64().unwrap_or(i64::MAX))))
        .collect();
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
        covered: BTreeMap::new(),
        covered_exact: BTreeMap::new(),
        framed,
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
    /// bsv-low #1004 (lane FIX-1004, L55 S4): the `pending` the ACTOR's read serves (`OWED_ACTOR_READ`, Phases B and
    /// C): the events the actor whose rows are served has not applied, i.e. the [`Snapshot::framed`] marks above the
    /// actor's covered generation of their game (a mark at or below it was written before the actor's last apply read
    /// the record: applied, its acknowledgement only late). A mark of [`UNFRAMED_SOURCES`] is never counted: no event
    /// carries it to the actor, and in Phase C nothing acknowledges it before the sweep (L55 pair 18: a fresh device
    /// read `pending 1` and no rows for 2.3 minutes over the drain's `age` mark). A game the actor answered no covered
    /// generation for counts every framed mark of it (the safe direction).
    pub fn actor_pending(&self) -> u64 {
        self.framed
            .iter()
            .filter(|(game, generation)| !self.applied_by_actor(game, *generation))
            .count() as u64
    }

    /// Did the actor whose rows are served apply the mark `generation` of `game`: at or below its covered generation
    /// (FIX-968), or one of the exact marks a carried apply applied (D2-ACTOR-F)?
    pub fn applied_by_actor(&self, game: &str, generation: i64) -> bool {
        let game = game.to_ascii_lowercase();
        generation <= self.covered.get(&game).copied().unwrap_or(i64::MIN)
            || self
                .covered_exact
                .get(&game)
                .is_some_and(|s| s.contains(&generation))
    }

    /// The games the actor's read names for their covered generation: the downgrade games (FIX-968) and the games of
    /// every framed mark (#1004), lowercased, deduplicated, the downgrade games FIRST (the read asks at most
    /// `owed_actor::COVERED_ASK_MAX`; a game past it is counted pending and withholds, the safe direction).
    pub fn actor_marked_games(&self) -> Vec<String> {
        let mut seen = std::collections::BTreeSet::new();
        self.downgrades
            .keys()
            .chain(self.framed.iter().map(|(g, _)| g))
            .filter(|g| !g.is_empty())
            .map(|g| g.to_ascii_lowercase())
            .filter(|g| seen.insert(g.clone()))
            .collect()
    }

    /// THE D1-MARK READ GATE (bsv-low #968, lane FIX-968; the second delta lens's D2-L1 as narrowed). The ONE
    /// predicate a claimable row passes before its press is served, in every phase: the answer is `Some(source)`,
    /// the press withheld as re-judging ([`PENDING_DOWNGRADE_REASON`]), when `game` holds a pending mark that is
    ///
    /// 1. of the DOWNGRADE class (`owed_dirty.downgrade = 1`, the trigger's own class: the index recording a spend, a
    ///    pot row admitted or deleted, an eviction, a word refusing its transaction), never a plain mark (a filing's,
    ///    a tip's, a wanted, an age, a stamp, a facts fill: those are counted in `pending` and withhold nothing), AND
    /// 2. one the JUDGE WHOSE ROWS ARE SERVED has not applied:
    ///    - the D1 read (`OWED_ACTOR_READ` off, Phase A; `rows: true`): every pending downgrade mark. The D1 judge's
    ///      own commit acknowledges the marks it judged (`ACK_DIRTY_SQL`), so a pending one is unjudged by the judge
    ///      whose rows this read serves ([`Snapshot::covered`] is empty here).
    ///    - the actor's read (`OWED_ACTOR_READ` on, Phases B and C): a pending downgrade mark whose `generation` is
    ///      GREATER than the actor's covered generation for the game (the marks bound its last apply read the record
    ///      at, recorded as `applied` `covered`, answered on `/rows` as `covered`). A mark at or below it was written
    ///      before that read (generations are AUTOINCREMENT), so the row served IS the record's judgement after the
    ///      write; withholding its press would cost the player the consumer's lag for nothing (L53 S1: 36 passes of
    ///      "re-judging" over a row the actor had already re-judged, the mark waiting on the serial consumer). A game
    ///      with no covered generation (the actor never applied it, the actor answered none, an older object build)
    ///      withholds as before: the safe direction.
    ///
    /// THE MONEY RULE: a press on a row a downgrade would retract is the one to withhold (the actor has not read the
    /// write: its row predates the spend, the admission, the eviction, the refusal); a press delayed by a mark the
    /// served judge has already applied, or by any non-downgrade mark, is money the player cannot collect for the
    /// length of a backlog, with nothing to gain.
    ///
    /// PHASE C (the D1 judge off): `owed_dirty` is still written by the overlay's triggers on every source write (the
    /// triggers know no flag), acknowledged by the consumer's Phase C batch (`owed_actor::as_the_judge`, the games
    /// the actor applied, up to the generation read before its call) and by a direct apply that landed (FIX-956
    /// item 3); the actor's apply reads the marks only for the bound it records as `covered`; and the actor-served
    /// read consults them through [`PENDING_SQL`] and this gate, the same rule as Phase B. The D1 read in Phase C
    /// serves no press at all until the rollback's `owed-v3` stamp ([`ROLLBACK_PENDING_REASON`], NEW-M3), checked
    /// before this gate.
    pub fn withholding_mark(&self, game: &str) -> Option<&str> {
        let marks = self.downgrades.get(game)?;
        marks
            .iter()
            .filter(|(generation, _)| !self.applied_by_actor(game, *generation))
            .max_by_key(|(generation, _)| *generation)
            .map(|(_, source)| source.as_str())
    }

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
                // press over a pending downgrade. A game holding an undrained downgrade-class mark the served judge
                // has not applied serves its press withheld as re-judging (THE GATE, `Snapshot::withholding_mark`,
                // FIX-968); the judge's acknowledgement, or the actor's own apply of the mark, lifts it.
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
                        .and_then(|game| self.withholding_mark(game))
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
