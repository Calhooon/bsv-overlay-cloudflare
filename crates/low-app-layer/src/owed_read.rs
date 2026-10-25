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
pub const PENDING_SQL: &str = "SELECT COUNT(*) AS pending FROM owed_dirty WHERE identity=?1";

#[derive(Debug, Default)]
pub struct Snapshot {
    pub joined: Vec<Value>,
    pub state: Option<Value>,
    pub pending: u64,
    pub wanted: bool,
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
    let mut batch = db
        .run_batch(
            [ROWS_SQL, STATE_SQL, PENDING_SQL]
                .into_iter()
                .map(|sql| Stmt {
                    sql: sql.into(),
                    binds: vec![Bind::Text(identity.into())],
                })
                .collect(),
        )
        .await?;
    if batch.len() != 3 {
        return Err("incomplete owed read batch".into());
    }
    let pending = batch
        .pop()
        .unwrap()
        .rows
        .first()
        .and_then(|r| number(&r["pending"]))
        .ok_or("invalid owed pending count")?;
    let state = batch.pop().unwrap().rows.into_iter().next();
    let mut snapshot = Snapshot {
        joined: batch.pop().unwrap().rows,
        state,
        pending,
        wanted: false,
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
            crate::owed_judge::mark_wanted(&WantedMarker(db), identity).await?;
            snapshot.wanted = true;
        }
    }
    Ok(snapshot)
}

impl Snapshot {
    pub fn needs_tip(&self) -> bool {
        self.joined
            .iter()
            .any(|r| number(&r["gateHeight"]).is_some() || number(&r["probeTip"]).is_some())
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
                    "tipMoved",
                    "servedTip",
                ] {
                    object.remove(key);
                }
                // bsv-low #893 section 5: the belt's next look at this row, as the judge or the belt dated it (null:
                // the index or a checked word decides the row, nothing is owed the chain).
                object.insert("beltNextAtMs".into(), json!(number(&joined["beltDueAtMs"])));
                if let (Some(probe), Some(served)) = (number(&joined["probeTip"]), tip) {
                    if served > probe {
                        object.insert("tipMoved".into(), json!(true));
                        object.insert("servedTip".into(), json!(served));
                    }
                }
                let position = rows.len();
                positions.insert(outpoint.into(), position);
                roles.push(BTreeMap::new());
                rows.push(json!({"outpoint":outpoint,"family":joined["family"],"gameId":joined["gameId"],"sats":number(&joined["sats"]),"opponentIdentity":joined["opponentIdentity"],"atHeight":number(&joined["atHeight"]),"facts":facts,"reason":joined["reason"]}));
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
        let mut body = json!({"v":owed::OWED_WIRE_VERSION,"identity":identity,"tip":tip,"rows":rows,"truncated":truncated,"computedAtMs":self.state.as_ref().and_then(|s|number(&s["computedAtMs"])).unwrap_or(0),"pending":self.pending,"markerlessProgress":null});
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
