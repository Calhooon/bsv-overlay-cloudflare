//! D2-ACTOR-F (bsv-low #959; `DESIGN-D2-ACTOR-2026-10-26.md` sections 3 to 7: "the event carries the write's
//! facts"): THE CARRIED ROW. A writer's publish reads, in the statement that already reads its pair's facts and mark
//! count (`owed_events_publish::marks_and_payload`, one statement, so none added to the door's batch), the pair's marks
//! by name (`subjects`: `[identity, gameId, generation]`) and the mirror's rows of every table its write changed
//! (`payload`: items `{t, c, k, r}`, the WHOLE domain `c = k` of table `t` as the record holds it). The writer's direct
//! call carries both to each identity's actor ([`Carried::of_event`], per identity), and a WARM actor upserts the rows
//! into its mirror and judges the game from the mirror alone: no D1 read ([`crate::owed_actor::apply_with`]).
//!
//! What a carried apply is, and is not:
//! - It is a FIRST VISIBILITY, never a coverage: it records no `covered` generation (the seq check's dupe), so the
//!   queue's delivery of the same event is applied again by the narrowed D1 read, which covers it and refreshes any
//!   write no mark carried (the sixth delta lens's NEW6-M1 class). The direct call that carried it acknowledges
//!   nothing (Phase C's direct acknowledgement reads a bound from D1; a carried body skips it).
//! - It records EXACTLY the marks whose rows it applied (`applied` kind [`PAYLOAD_MARK_KIND`], seq the generation):
//!   the read's gate counts those marks applied and every other pending mark as before (`owed_read::Snapshot`).
//! - A carried row never overwrites a newer one: an item is skipped where the actor already holds the same domain at
//!   or past its generation, by an earlier carried item ([`PAYLOAD_GEN_KIND`]) or by a D1 read of one of its games at
//!   or past it ([`READ_BOUND_KIND`], or `covered`): that read's snapshot held the write (generations are
//!   AUTOINCREMENT and a read's bound is a generation it saw committed).
//! - Any doubt is the D1 read: an item naming a table the mirror does not hold, a column set that is not the mirror's
//!   (`schema::columns`, column for column), a key column the table lacks, a body whose events do not all carry their
//!   rows: the carried path is not taken at all.
use super::{schema, FAMILIES};
use crate::pot_hook::{Bind, Stmt};
use serde_json::{json, Value};
use std::collections::BTreeSet;

/// The `applied` kind of a mark whose rows a carried apply applied (seq the mark's generation, game its game).
pub const PAYLOAD_MARK_KIND: &str = "payload-mark";
/// The `applied` kind of a carried domain's generation (seq the generation, game the domain's name, [`CarriedItem::domain`]).
pub const PAYLOAD_GEN_KIND: &str = "payload-gen";
/// The `applied` kind of a D1 read's bound per game (seq the marks' bound it read the record at): any apply that read
/// the record, in every phase (`covered` is written only where the narrowed apply may cover).
pub const READ_BOUND_KIND: &str = "read";
/// A game's last read bound replaced (`?1` the games): before the new one.
pub const READ_BOUND_CLEAR_SQL: &str =
    "DELETE FROM applied WHERE kind='read' AND game IN (SELECT value FROM json_each(?1))";
/// A carried domain's generation replaced (`?1` the domains).
pub const PAYLOAD_GEN_CLEAR_SQL: &str =
    "DELETE FROM applied WHERE kind='payload-gen' AND game IN (SELECT value FROM json_each(?1))";
/// The newest the actor holds of one carried item: `?1` its domain, `?2` its games (lowercased). One row, `g`.
pub const HELD_GENERATION_SQL: &str = "SELECT MAX(seq) AS g FROM applied WHERE (kind='payload-gen' AND game=?1) OR (kind IN ('read','covered') AND game IN (SELECT value FROM json_each(?2)))";
/// FIX-968's coverage beside the carried marks: the exact generations a carried apply applied above the game's
/// covered generation (`?1` the games, lowercased). Answered on `/rows` as `coveredExact: [{game, seq}]`.
pub const COVERED_EXACT_BY_GAME_SQL: &str = "SELECT lower(p.game) AS game, p.seq AS seq FROM applied p WHERE p.kind='payload-mark' AND lower(p.game) IN (SELECT value FROM json_each(?1)) AND p.seq>COALESCE((SELECT MAX(c.seq) FROM applied c WHERE c.kind='covered' AND lower(c.game)=lower(p.game)),-1)";

/// One carried domain: the rows of `t` whose key columns `c` equal one of `k`, as the record held them at generation
/// `g`, for the identity's games `games`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CarriedItem {
    pub t: String,
    pub c: Vec<String>,
    /// Each key tuple, as JSON text (`["txid"]`, `["hopTxid", 3]`).
    pub k: Vec<String>,
    /// Each row, as JSON text (`__rowid` and the mirror's columns).
    pub r: Vec<String>,
    pub g: i64,
    pub games: Vec<String>,
}

impl CarriedItem {
    /// The domain's name in `applied` (the table, its key columns and the sorted keys).
    pub fn domain(&self) -> String {
        let mut k = self.k.clone();
        k.sort();
        format!("{}|{}|{}", self.t, self.c.join(","), k.join(","))
    }
}

/// What one identity's direct call carries: the domains, and the marks (`game`, generation) whose rows they are.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Carried {
    pub items: Vec<CarriedItem>,
    pub marks: Vec<(String, i64)>,
}

/// PURE: an event's carried subjects (`facts.subjects`), grouped by identity (lowercased) in the order the marks name
/// them, each identity's games deduplicated; `None` when the event carries no row (its subjects then come from the
/// marks' D1 read, as before).
pub fn carried_subjects(facts: &Value) -> Option<Vec<(String, Vec<String>)>> {
    facts.get("payload")?;
    let mut out: Vec<(String, Vec<String>)> = Vec::new();
    for m in facts["subjects"].as_array()? {
        let id = m[0].as_str()?.to_ascii_lowercase();
        let game = m[1].as_str()?.to_string();
        match out.iter_mut().find(|(i, _)| *i == id) {
            Some((_, games)) => {
                if !games.iter().any(|g| g.eq_ignore_ascii_case(&game)) {
                    games.push(game);
                }
            }
            None => out.push((id, vec![game])),
        }
    }
    (!out.is_empty()).then_some(out)
}

impl Carried {
    /// PURE: what an event (its `facts`) carries to `identity`: every domain, generation the greatest of the pair's
    /// marks (the row was read in the same transaction as all of them), and the identity's own marks. `None` when the
    /// event carries no row or names no mark of `identity`.
    pub fn of_event(facts: &Value, identity: &str) -> Option<Carried> {
        let subjects = facts["subjects"].as_array()?;
        let g = subjects.iter().filter_map(|m| m[2].as_i64()).max()?;
        let id = identity.to_ascii_lowercase();
        let mut marks = Vec::new();
        let mut games: Vec<String> = Vec::new();
        for m in subjects {
            if m[0].as_str().map(str::to_ascii_lowercase).as_deref() != Some(id.as_str()) {
                continue;
            }
            let game = m[1].as_str()?.to_ascii_lowercase();
            marks.push((game.clone(), m[2].as_i64()?));
            if !games.contains(&game) {
                games.push(game);
            }
        }
        if marks.is_empty() {
            return None;
        }
        let mut items = Vec::new();
        for it in facts["payload"].as_array()? {
            let t = it["t"].as_str()?.to_string();
            let c: Vec<String> = it["c"]
                .as_array()?
                .iter()
                .map(|c| c.as_str().map(str::to_string))
                .collect::<Option<_>>()?;
            let k: Vec<String> = it["k"].as_array()?.iter().map(Value::to_string).collect();
            let r: Vec<String> = match &it["r"] {
                Value::Array(a) => a.iter().map(Value::to_string).collect(),
                Value::String(s) => serde_json::from_str::<Vec<Value>>(s)
                    .ok()?
                    .iter()
                    .map(Value::to_string)
                    .collect(),
                _ => return None,
            };
            items.push(CarriedItem {
                t,
                c,
                k,
                r,
                g,
                games: games.clone(),
            });
        }
        (!items.is_empty()).then_some(Carried { items, marks })
    }

    /// PURE: merge another event's carried rows for the same identity (a coalesced body): a domain carried twice keeps
    /// the newer generation's rows.
    pub fn merge(&mut self, other: Carried) {
        for it in other.items {
            let d = it.domain();
            match self.items.iter_mut().find(|x| x.domain() == d) {
                Some(x) if x.g >= it.g => {
                    for g in it.games {
                        if !x.games.contains(&g) {
                            x.games.push(g);
                        }
                    }
                }
                Some(x) => *x = it,
                None => self.items.push(it),
            }
        }
        for m in other.marks {
            if !self.marks.contains(&m) {
                self.marks.push(m);
            }
        }
    }
}

/// PURE: the mirror's statements of one item: its domain deleted, its rows inserted as the record held them (rowid
/// kept). `Err` (the carried path is not taken) where the item does not name the mirror's own shape: a table the
/// mirror does not hold, a BEEF family (no bytes are carried), a key column the table lacks, a key tuple of the wrong
/// width, a row whose columns are not exactly `__rowid` and the mirror's columns.
pub fn item_statements(it: &CarriedItem) -> Result<Vec<Stmt>, String> {
    let family = FAMILIES
        .iter()
        .find(|f| f.table == it.t)
        .ok_or_else(|| format!("carried table {} is not the mirror's", it.t))?;
    if family.beef {
        return Err(format!("carried table {} holds bytes", it.t));
    }
    let cols = schema::columns(&it.t);
    if it.c.is_empty() || it.c.iter().any(|c| !cols.contains(&c.as_str())) {
        return Err(format!("carried key {:?} is not {}'s", it.c, it.t));
    }
    let mut out = Vec::new();
    let clause =
        it.c.iter()
            .enumerate()
            .map(|(n, c)| format!("{c}=?{}", n + 1))
            .collect::<Vec<_>>()
            .join(" AND ");
    for k in &it.k {
        let key: Vec<Value> = serde_json::from_str(k).map_err(|e| format!("carried key: {e}"))?;
        if key.len() != it.c.len() {
            return Err(format!("carried key {k} is not {} wide", it.c.len()));
        }
        out.push(Stmt::new(
            format!("DELETE FROM {} WHERE {clause}", it.t),
            key.iter().map(super::bind_of).collect(),
        ));
    }
    let want: BTreeSet<&str> = std::iter::once("__rowid")
        .chain(cols.iter().copied())
        .collect();
    for r in &it.r {
        let row: Value = serde_json::from_str(r).map_err(|e| format!("carried row: {e}"))?;
        let obj = row.as_object().ok_or("a carried row is not an object")?;
        let got: BTreeSet<&str> = obj.keys().map(String::as_str).collect();
        if got != want {
            return Err(format!(
                "a carried {} row's columns are not the mirror's",
                it.t
            ));
        }
        if !row["__rowid"].is_i64() {
            return Err(format!("a carried {} row has no rowid", it.t));
        }
        // the row is in its own domain (a row outside it would survive the next carried delete)
        let inside = it.k.iter().any(|k| {
            serde_json::from_str::<Vec<Value>>(k).is_ok_and(|key| {
                it.c.iter()
                    .zip(&key)
                    .all(|(c, v)| same(&row[c.as_str()], v))
            })
        });
        if !inside {
            return Err(format!("a carried {} row is outside its domain", it.t));
        }
        let mut b = vec![super::bind_of(&row["__rowid"])];
        b.extend(cols.iter().map(|c| super::bind_of(&row[*c])));
        out.push(Stmt::new(super::insert_sql(family), b));
    }
    Ok(out)
}

/// A key value and a column's value are the same value (a number and its text are not: the record's own types).
fn same(a: &Value, b: &Value) -> bool {
    match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) => x == y,
        _ => a == b,
    }
}

/// The statements recording what a carried apply applied: each applied domain's generation, each mark.
pub fn record_statements(applied: &[&CarriedItem], marks: &[(String, i64)], now: i64) -> Vec<Stmt> {
    let mut out = Vec::new();
    if !applied.is_empty() {
        let domains: Vec<String> = applied.iter().map(|i| i.domain()).collect();
        out.push(Stmt::new(
            PAYLOAD_GEN_CLEAR_SQL,
            vec![Bind::Text(json!(domains).to_string())],
        ));
        for it in applied {
            out.push(Stmt::new(
                super::APPLIED_SQL,
                vec![
                    Bind::Text(PAYLOAD_GEN_KIND.into()),
                    Bind::Int(it.g),
                    Bind::Int(now),
                    Bind::Text(json!([it.domain()]).to_string()),
                ],
            ));
        }
    }
    for (game, g) in marks {
        out.push(Stmt::new(
            super::APPLIED_SQL,
            vec![
                Bind::Text(PAYLOAD_MARK_KIND.into()),
                Bind::Int(*g),
                Bind::Int(now),
                Bind::Text(json!([game.to_ascii_lowercase()]).to_string()),
            ],
        ));
    }
    out
}

/// PURE: the read's map of the exact carried marks (`coveredExact` rows, [`COVERED_EXACT_BY_GAME_SQL`]).
pub fn covered_exact_of(answer: &Value) -> std::collections::BTreeMap<String, BTreeSet<i64>> {
    let mut out: std::collections::BTreeMap<String, BTreeSet<i64>> = Default::default();
    for r in answer["coveredExact"].as_array().into_iter().flatten() {
        let (Some(g), Some(s)) = (
            r["game"].as_str(),
            r["seq"]
                .as_i64()
                .or_else(|| r["seq"].as_f64().map(|f| f as i64)),
        ) else {
            continue;
        };
        out.entry(g.to_ascii_lowercase()).or_default().insert(s);
    }
    out
}
