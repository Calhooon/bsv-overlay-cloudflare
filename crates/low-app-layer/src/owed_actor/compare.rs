//! FIX-986 (bsv-low #986; READ-986 items 1 and 4): THE ACTOR AGAINST A FRESH D1 JUDGEMENT, ONE FUNCTION.
//!
//! READ-986 read the fleet compare of 2026-10-30 09:52Z: 42 of its 46 "diffs" were D1 STALE (a collected marker, a new
//! game or a sweep filing written after D1's stored row was judged): the operator's route compared the actor with
//! D1's STORED `owed_rows`, which Phase C leaves with no writer. Now the D1 side of every compare is the D1 judge's
//! FRESH judgement of the identity's games, writing nothing (`owed_judge::judge_fresh_rows`); the stored rows ride
//! beside as `d1Stored`, labelled stale. [`fresh_compare`] is the one compare: the operator's route
//! (`GET /internal/owed-actor-rows`), the standing route's one-identity re-compare
//! (`POST /internal/owed-actor-compare`) and the scheduled ring ([`ring_tick`]) all call it, and the ring and the POST
//! write its [`CompareRow`] into `owed_actor_compare`.
use super::{
    actor_name, diff_rows, held_by_lag, json_ms, rejudge_state_of, rows_digest, rows_view,
    D1_JUDGED_UNDER_SQL, LEVER_PENDING_SQL, ROWS_SQL,
};
use crate::owed_judge::JudgeDb;
use crate::pot_hook::{Bind, SqlPort, Stmt, StmtOut};
use serde_json::{json, Value};
use std::cell::Cell;
use std::collections::BTreeSet;

/// The games one compare judges, at most: every game an actor row or a stored D1 row names first, then the identity's
/// games newest first. Four hydrations of 16. An identity with more is compared over these and named `partial`
/// (`d1.games`). Measured natively on the frozen fleet (`fix986c2_e`): the judge's hydration reads about four to eight
/// statements a game (its per-pot and per-hop reads, not its chunk), the fleet's whale (1,033 games) 550 statements at
/// 64 (the lived-in seat 341, the four fleet seats about 505: the reads follow the rows more than the games), inside
/// D1's per-invocation bound of 1,000 with the ring's reserve; at 128 the whale was 1,101.
pub const FRESH_COMPARE_GAMES_MAX: usize = 64;
/// The identity's games, newest first (the judge's union: `owed_judge::identity_games!`), one statement.
pub const COMPARE_GAMES_SQL: &str = crate::owed_judge::IDENTITY_GAMES_NEWEST_SQL;
/// The Phase C flip as the ring observed it (`owed_belt_state` `owed-actor:phase-c`: `{sinceMs, endedAtMs}`).
pub const PHASE_C_NAME: &str = "owed-actor:phase-c";
pub const PHASE_C_READ_SQL: &str =
    "SELECT cursor FROM owed_belt_state WHERE name='owed-actor:phase-c'";

/// The games a compare judges ([`FRESH_COMPARE_GAMES_MAX`]): the rows' games first (sorted), then the identity's
/// games in the order given (newest first); `(picked, total)`, `total` the union's size.
pub fn pick_games(
    rows: &[&[Value]],
    identity_games: &[String],
    max: usize,
) -> (Vec<String>, usize) {
    let mut from_rows: BTreeSet<String> = BTreeSet::new();
    for side in rows {
        for r in side.iter() {
            if let Some(g) = r["gameId"].as_str().filter(|g| !g.is_empty()) {
                from_rows.insert(g.to_ascii_lowercase());
            }
        }
    }
    let mut picked: Vec<String> = from_rows.iter().cloned().collect();
    let mut all: BTreeSet<String> = from_rows;
    for g in identity_games {
        let g = g.to_ascii_lowercase();
        if all.insert(g.clone()) {
            picked.push(g);
        }
    }
    let total = all.len();
    picked.truncate(max);
    (picked, total)
}

/// A stored D1 row is stale when the D1 judge is off (Phase C: nothing writes `owed_rows`) or it was judged before the
/// last Phase C flip the ring observed (`sinceMs`; a rollback's `owed-v3` pass re-judges every row after it).
pub fn stored_row_stale(row: &Value, d1_judges: bool, phase_c: &Value) -> bool {
    if !d1_judges {
        return true;
    }
    match (json_ms(&phase_c["sinceMs"]), json_ms(&row["judgedAtMs"])) {
        (Some(flip), Some(judged)) => judged < flip,
        (Some(_), None) => true,
        _ => false,
    }
}

/// The compared fields that differ on one outpoint, both values (the fields [`diff_rows`] compares; a facts key by
/// name, `facts.<key>`); a row on one side only is `row: {d1, actor}` with the present side's family, game, sats and
/// claim. Each value cut at 200 characters, so a stored row stays bounded.
pub fn diff_fields(d1: &[Value], actor: &[Value], outpoint: &str) -> Value {
    let find = |rows: &[Value]| rows.iter().find(|r| r["outpoint"] == outpoint).cloned();
    let short = |v: &Value| -> Value {
        match v {
            Value::String(s) if s.chars().count() > 200 => {
                json!(format!("{}..", s.chars().take(200).collect::<String>()))
            }
            Value::String(_) | Value::Number(_) | Value::Bool(_) | Value::Null => v.clone(),
            other => {
                let t = other.to_string();
                if t.chars().count() > 200 {
                    json!(format!("{}..", t.chars().take(200).collect::<String>()))
                } else {
                    other.clone()
                }
            }
        }
    };
    let mut out = serde_json::Map::new();
    match (find(d1), find(actor)) {
        (Some(a), Some(b)) => {
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
                    out.insert(k.into(), json!({"d1": short(&a[k]), "actor": short(&b[k])}));
                }
            }
            let (fa, fb) = (super::compared_facts(&a), super::compared_facts(&b));
            let keys: BTreeSet<String> = fa
                .as_object()
                .into_iter()
                .chain(fb.as_object())
                .flat_map(|m| m.keys().cloned())
                .collect();
            for k in keys {
                if fa[&k] != fb[&k] {
                    out.insert(
                        format!("facts.{k}"),
                        json!({"d1": short(&fa[&k]), "actor": short(&fb[&k])}),
                    );
                }
            }
        }
        (Some(a), None) => {
            out.insert("row".into(), json!({"d1": "present", "actor": "absent"}));
            for k in ["family", "gameId", "sats", "claimable"] {
                out.insert(k.into(), json!({"d1": short(&a[k]), "actor": Value::Null}));
            }
        }
        (None, Some(b)) => {
            out.insert("row".into(), json!({"d1": "absent", "actor": "present"}));
            for k in ["family", "gameId", "sats", "claimable"] {
                out.insert(k.into(), json!({"d1": Value::Null, "actor": short(&b[k])}));
            }
        }
        (None, None) => {}
    }
    Value::Object(out)
}

/// The differing outpoints one stored compare row keeps, at most (each with its fields).
pub const COMPARE_ROW_OUTPOINTS_MAX: usize = 32;

/// THE ONE COMPARE (FIX-986). The actor's stored answer (`/stored`, or its fault) against the D1 judge's FRESH
/// judgement of the identity's games ([`pick_games`], [`crate::owed_judge::judge_fresh_rows`]: nothing written), by the
/// reconcile's own functions ([`rows_view`]: `diff`, `tipLag`, `digests`, their names and shapes kept, over the fresh
/// side). One D1 batch reads D1's stored rows, the judged-under stamp, the identity's pending marks, its games and the
/// Phase C flip; the stored rows ride beside as `d1Stored` (each row's `stale`, the outpoints the fresh judgement
/// moved, `staleOutpoints`). A partial compare (more than [`FRESH_COMPARE_GAMES_MAX`] games) restricts every side to
/// the judged games. `Err` only when the fresh judgement faults (no compare is ever made against a stored row).
pub async fn fresh_compare(
    db: &impl JudgeDb,
    identity: &str,
    actor: Result<Value, String>,
) -> Result<Value, String> {
    let me = identity.to_ascii_lowercase();
    let mut read = db
        .run_batch(vec![
            Stmt::new(ROWS_SQL, vec![Bind::Text(me.clone())]),
            Stmt::new(D1_JUDGED_UNDER_SQL, vec![Bind::Text(me.clone())]),
            Stmt::new(LEVER_PENDING_SQL, vec![Bind::Text(me.clone())]),
            Stmt::new(
                COMPARE_GAMES_SQL,
                vec![Bind::Text(me.clone()), Bind::Int(4_096)],
            ),
            Stmt::new(PHASE_C_READ_SQL, vec![]),
        ])
        .await?
        .into_iter();
    let mut next = || read.next().map(|o: StmtOut| o.rows).unwrap_or_default();
    let stored = next();
    let under = next().into_iter().next().unwrap_or_default();
    let pending = next();
    let identity_games: Vec<String> = next()
        .iter()
        .filter_map(|r| r["gameId"].as_str().map(str::to_string))
        .collect();
    let phase_c: Value = next()
        .first()
        .and_then(|r| r["cursor"].as_str())
        .and_then(|c| serde_json::from_str(c).ok())
        .unwrap_or(Value::Null);
    let actor_rows: Vec<Value> = actor
        .as_ref()
        .ok()
        .and_then(|a| a["rows"].as_array().cloned())
        .unwrap_or_default();
    let (games, total) = pick_games(
        &[&actor_rows, &stored],
        &identity_games,
        FRESH_COMPARE_GAMES_MAX,
    );
    let partial = total > games.len();
    let judged: BTreeSet<&str> = games.iter().map(String::as_str).collect();
    let within = |rows: &[Value]| -> Vec<Value> {
        rows.iter()
            .filter(|r| !partial || judged.contains(r["gameId"].as_str().unwrap_or_default()))
            .cloned()
            .collect()
    };
    let fresh = crate::owed_judge::judge_fresh_rows(db, &me, &games).await?;
    let stored = within(&stored);
    let actor = actor.map(|mut a| {
        a["rows"] = json!(within(&actor_rows));
        a
    });
    let stamp = rejudge_state_of(Some(&json!({"cursor": under["stamp"]})));
    let mut view = rows_view(
        &me,
        actor,
        &fresh,
        stamp.as_ref(),
        json_ms(&under["computedAtMs"]).map(|c| c as i64),
    );
    view["heldByLag"] = json!(held_by_lag(&view, &pending));
    let d1_judges = db.d1_judges();
    view["d1"]["fresh"] = json!(true);
    view["d1"]["tip"] = json!(db.tip());
    view["d1"]["games"] = json!({"judged": games.len(), "total": total, "partial": partial});
    let stored_marked: Vec<Value> = stored
        .iter()
        .map(|r| {
            let mut r = r.clone();
            r["stale"] = json!(stored_row_stale(&r, d1_judges, &phase_c));
            r
        })
        .collect();
    view["d1Stored"] = json!({
        "rows": stored_marked,
        "stale": !d1_judges || stored.iter().any(|r| stored_row_stale(r, d1_judges, &phase_c)),
        "d1Judges": d1_judges,
        "phaseC": phase_c,
        "staleOutpoints": diff_rows(&stored, &fresh),
        "digest": rows_digest(&stored),
    });
    let actor_now: Vec<Value> = view["actor"]["rows"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    view["diffFields"] = json!(view["diff"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(|o| json!({"outpoint": o, "fields": diff_fields(&fresh, &actor_now, o)}))
        .collect::<Vec<_>>());
    Ok(view)
}

/// One identity's stored compare ([`compare_row_of`]): the counts, the differing outpoints (at most
/// [`COMPARE_ROW_OUTPOINTS_MAX`], each with its fields and whether a pending mark holds it) and both digests, or the
/// compare's fault.
#[derive(Debug, Clone, PartialEq)]
pub struct CompareRow {
    pub identity: String,
    pub compared_at_ms: i64,
    /// The differences not held by a pending mark (`diff` less `heldByLag`): the STOP's count.
    pub diff_count: Option<i64>,
    pub held_by_lag_count: Option<i64>,
    pub stale_d1_count: Option<i64>,
    pub tip_lag_count: Option<i64>,
    pub outpoints: Value,
    pub d1_digest: Option<String>,
    pub actor_digest: Option<String>,
    pub fault: Option<String>,
    pub partial: bool,
}

/// PURE: the stored row of one [`fresh_compare`] answer (`Ok`), or of its fault. An actor that did not answer, or
/// answered not bootstrapped, is a fault (no comparison of rows it has not judged).
pub fn compare_row_of(identity: &str, now: i64, view: Result<&Value, &str>) -> CompareRow {
    let fault_row = |fault: String| CompareRow {
        identity: identity.to_string(),
        compared_at_ms: now,
        diff_count: None,
        held_by_lag_count: None,
        stale_d1_count: None,
        tip_lag_count: None,
        outpoints: json!([]),
        d1_digest: None,
        actor_digest: None,
        fault: Some(fault),
        partial: false,
    };
    let view = match view {
        Ok(v) => v,
        Err(e) => return fault_row(format!("fresh judgement: {e}")),
    };
    if let Some(e) = view["actor"]["error"].as_str() {
        return fault_row(format!("actor: {e}"));
    }
    if view["actor"]["bootstrapped"] != true {
        return fault_row("actor: not bootstrapped".into());
    }
    let strs = |v: &Value| -> Vec<String> {
        v.as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect()
    };
    let diff = strs(&view["diff"]);
    let held: BTreeSet<String> = strs(&view["heldByLag"]).into_iter().collect();
    let fields = view["diffFields"].as_array().cloned().unwrap_or_default();
    let outpoints: Vec<Value> = diff
        .iter()
        .take(COMPARE_ROW_OUTPOINTS_MAX)
        .map(|o| {
            json!({
                "outpoint": o,
                "held": held.contains(o),
                "fields": fields.iter().find(|f| f["outpoint"] == o.as_str()).map_or(Value::Null, |f| f["fields"].clone()),
            })
        })
        .collect();
    CompareRow {
        identity: identity.to_string(),
        compared_at_ms: now,
        diff_count: Some(diff.iter().filter(|o| !held.contains(*o)).count() as i64),
        held_by_lag_count: Some(held.len() as i64),
        stale_d1_count: Some(strs(&view["d1Stored"]["staleOutpoints"]).len() as i64),
        tip_lag_count: Some(strs(&view["tipLag"]).len() as i64),
        outpoints: json!(outpoints),
        d1_digest: view["digests"]["d1"].as_str().map(str::to_string),
        actor_digest: view["digests"]["actor"].as_str().map(str::to_string),
        fault: None,
        partial: view["d1"]["games"]["partial"] == true,
    }
}

/// One compare row written (an upsert by identity).
pub const COMPARE_UPSERT_SQL: &str = "INSERT INTO owed_actor_compare(identity,comparedAtMs,diffCount,heldByLagCount,staleD1Count,tipLagCount,outpoints,d1Digest,actorDigest,fault,partial) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11) ON CONFLICT(identity) DO UPDATE SET comparedAtMs=excluded.comparedAtMs,diffCount=excluded.diffCount,heldByLagCount=excluded.heldByLagCount,staleD1Count=excluded.staleD1Count,tipLagCount=excluded.tipLagCount,outpoints=excluded.outpoints,d1Digest=excluded.d1Digest,actorDigest=excluded.actorDigest,fault=excluded.fault,partial=excluded.partial";

pub fn upsert_stmt(row: &CompareRow) -> Stmt {
    let int = |n: Option<i64>| n.map_or(Bind::Null, Bind::Int);
    let text = |s: &Option<String>| s.clone().map_or(Bind::Null, Bind::Text);
    Stmt::new(
        COMPARE_UPSERT_SQL,
        vec![
            Bind::Text(row.identity.clone()),
            Bind::Int(row.compared_at_ms),
            int(row.diff_count),
            int(row.held_by_lag_count),
            int(row.stale_d1_count),
            int(row.tip_lag_count),
            Bind::Text(row.outpoints.to_string()),
            text(&row.d1_digest),
            text(&row.actor_digest),
            text(&row.fault),
            Bind::Int(i64::from(row.partial)),
        ],
    )
}

// ---------------------------------------------------------------------------------------------------------------------
// The scheduled ring (READ-986 item 4).

/// The ring's driver: `BOARD_VIEW` actors of its own, one a SHARD ([`shard_actor_name`]: shard 0 keeps this name, so
/// the actor PR 241 armed is shard 0), whose alarm is the shard's tick every [`FLEET_COMPARE_TICK_MS`], armed by every
/// cron as the belt is (`owed_belt::arm`'s watchdog). Each its own Durable Object invocation, so a tick's D1 statements
/// never share the belt alarm's per-invocation bound, nor another shard's.
///
/// FIX-998 (bsv-low #998): beta's ticks compared 1 to 4 identities each (stopped on the wall or the statements), a
/// revolution over 153 identities in about 65 minutes. One fresh compare of the fleet fixture's weight is about 505
/// statements (`fix986c2_e`), so ONE invocation holds ONE such compare under the 800-statement budget and D1's 1,000:
/// a revolution of 153 needs about 153 invocations whatever the per-tick count says. The ring now runs
/// [`FLEET_COMPARE_SHARDS`] shards (the identity space split by its first byte past the key's `02`/`03` prefix,
/// [`shard_segments`]), each ticking every 10 s under an 8 s wall: 4 x 60 = 240 invocations in ten minutes, a
/// revolution of 153 fixture-weight seats (553 statements each) in 48 ticks, 480 s (`fix998_a`; 24 ticks at beta's
/// 250). A shard that wraps RESTS until [`FLEET_COMPARE_REVOLUTION_FLOOR_MS`] after its revolution began, so the
/// ring compares the fleet once about every nine minutes, never faster, whatever its speed: the D1 cost is one fresh
/// compare an identity a revolution, as the brief's ten minutes assumed (the shards and the short tick buy the speed,
/// never more revolutions).
pub const FLEET_COMPARE_ACTOR: &str = "owed-fleet-compare";
pub const FLEET_COMPARE_TICK_MS: i64 = 10_000;
/// The shards by default (the render var [`SHARDS_VAR`] sets another, 1 to [`FLEET_COMPARE_SHARDS_MAX`]).
pub const FLEET_COMPARE_SHARDS: usize = 4;
pub const FLEET_COMPARE_SHARDS_MAX: usize = 16;
pub const SHARDS_VAR: &str = "OWED_FLEET_COMPARE_SHARDS";
/// A shard that wrapped compares nothing until this long after its revolution began.
pub const FLEET_COMPARE_REVOLUTION_FLOOR_MS: i64 = 540_000;
/// The identities one shard's tick compares at most (the render var [`PER_TICK_VAR`] sets another, 0 pauses the ring,
/// at most [`FLEET_COMPARE_PER_TICK_MAX`]); the statement budget and the wall stop a tick of heavy seats sooner.
pub const FLEET_COMPARE_PER_TICK: usize = 16;
pub const FLEET_COMPARE_PER_TICK_MAX: usize = 64;
pub const PER_TICK_VAR: &str = "OWED_FLEET_COMPARE_PER_TICK";
/// The tick's D1 statements: no identity is started once the statements spent and the largest identity's cost so far
/// this tick (at least [`FLEET_COMPARE_IDENTITY_RESERVE`]) would pass it. D1's per-invocation bound is 1,000.
pub const FLEET_COMPARE_STATEMENTS_PER_TICK: usize = 800;
pub const FLEET_COMPARE_IDENTITY_RESERVE: usize = 80;
/// The tick's wall: no identity is started past it (under the tick, so a tick usually ends before its next alarm).
pub const FLEET_COMPARE_WALL_MS: u64 = 8_000;
/// The ring's place in the belt's ledger, one row a shard ([`ring_state_name`]): `{after, revolution,
/// revolutionStartedAtMs, lastRevolutionMs, lastTickAt, restUntilMs}`. One shard keeps PR 241's name.
pub const RING_NAME: &str = "fleet-compare:ring";
pub const RING_READ_SQL: &str = "SELECT cursor FROM owed_belt_state WHERE name=?1";
/// Every shard's row, one range of the ledger's key.
pub const RING_SHARDS_READ_SQL: &str = "SELECT name,cursor FROM owed_belt_state WHERE name>='fleet-compare:ring' AND name<'fleet-compare:rinh' ORDER BY name";
/// The ring's identities: the set `GET /internal/owed-actor-identities` pages (every identity of `owed_state`, a party,
/// a hop marker or a released hop's evidence), `?2` of them past `?1` and at most `?3` in key order, walked by SEEKS:
/// each step the least of four `MIN(identity)` seeks past the last (the reconcile's `RECONCILE_NEXT_SQL`, on
/// `owed_state`'s key and the three identity indexes), so a tick reads about four index entries an identity, never the
/// union (the budget tier measured the union's page at 551 rows on its small seed). `?3` closes a shard's segment.
pub const RING_IDENTITIES_SQL: &str = "WITH RECURSIVE walk(identity,n) AS (SELECT (SELECT MIN(i) FROM (SELECT (SELECT MIN(identity) FROM owed_state WHERE identity>?1 AND identity<=?3) AS i UNION ALL SELECT (SELECT MIN(identity) FROM potparty_records WHERE identity>?1 AND identity<=?3) UNION ALL SELECT (SELECT MIN(identity) FROM hopparty_records WHERE identity>?1 AND identity<=?3) UNION ALL SELECT (SELECT MIN(identity) FROM released_hop_evidence WHERE identity>?1 AND identity<=?3))),1 UNION ALL SELECT (SELECT MIN(i) FROM (SELECT (SELECT MIN(identity) FROM owed_state WHERE identity>walk.identity AND identity<=?3) AS i UNION ALL SELECT (SELECT MIN(identity) FROM potparty_records WHERE identity>walk.identity AND identity<=?3) UNION ALL SELECT (SELECT MIN(identity) FROM hopparty_records WHERE identity>walk.identity AND identity<=?3) UNION ALL SELECT (SELECT MIN(identity) FROM released_hop_evidence WHERE identity>walk.identity AND identity<=?3))),n+1 FROM walk WHERE walk.identity IS NOT NULL AND n<?2) SELECT identity FROM walk WHERE identity IS NOT NULL";
/// At the end of a shard's revolution, the rows of its identities it did not compare (gone from the list) are dropped:
/// its two segments, `(?2, ?3]` and `(?4, ?5]`.
pub const RING_PRUNE_SQL: &str = "DELETE FROM owed_actor_compare WHERE comparedAtMs<?1 AND ((identity>?2 AND identity<=?3) OR (identity>?4 AND identity<=?5))";
/// The key past every identity: the last segment's upper bound.
pub const RING_KEY_END: &str = "\u{10FFFF}";

/// The shard count from the render var: unset or unreadable the default, clamped to 1 ..= [`FLEET_COMPARE_SHARDS_MAX`].
pub fn shards_of(var: Option<&str>) -> usize {
    var.and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(FLEET_COMPARE_SHARDS)
        .clamp(1, FLEET_COMPARE_SHARDS_MAX)
}

/// The render's shard count ([`SHARDS_VAR`], [`shards_of`]).
pub fn shards_env(env: &worker::Env) -> usize {
    shards_of(env.var(SHARDS_VAR).ok().map(|v| v.to_string()).as_deref())
}

/// The Durable Object name of shard `k` (shard 0 keeps [`FLEET_COMPARE_ACTOR`]).
pub fn shard_actor_name(k: usize) -> String {
    if k == 0 {
        FLEET_COMPARE_ACTOR.to_string()
    } else {
        format!("{FLEET_COMPARE_ACTOR}:{k}")
    }
}

/// The ledger row of shard `k` of `n` (one shard keeps [`RING_NAME`], PR 241's row).
pub fn ring_state_name(k: usize, n: usize) -> String {
    if n <= 1 {
        RING_NAME.to_string()
    } else {
        format!("{RING_NAME}:{k}/{n}")
    }
}

/// PURE: the key segments of shard `k` of `n`, each `(lo, hi]` in key order. The key space is cut at `2n` bounds: the
/// `02` keys at `02xx` for `xx` = `j * 256 / n`, the `03` keys likewise, so a compressed key's shard is its first byte
/// past the prefix (uniform: an x coordinate's first byte); segment `j` goes to shard `j mod n`, so each shard owns one
/// `02` segment and one `03` segment, and a key outside them (below `02`, past `03ff`) falls in the first or the last.
/// Every key is in exactly one segment.
pub fn shard_segments(k: usize, n: usize) -> [(String, String); 2] {
    let n = n.max(1);
    let bound = |j: usize| -> String {
        if j == 0 {
            String::new()
        } else if j == n {
            "03".to_string()
        } else if j == 2 * n {
            RING_KEY_END.to_string()
        } else if j < n {
            format!("02{:02x}", j * 256 / n)
        } else {
            format!("03{:02x}", (j - n) * 256 / n)
        }
    };
    [(bound(k), bound(k + 1)), (bound(k + n), bound(k + n + 1))]
}

/// PURE: the fleet's ring from its shards' rows (`name`, parsed `cursor`), as the route serves it: `revolution` the
/// least of the shards' (a revolution completes when every shard has wrapped; 0 while a shard has a row missing),
/// `lastRevolutionMs` the longest of the shards' last revolutions (null until every shard has one), `lastTickAt` the
/// latest, `ringCursor` shard 0's cursor, and each shard's own under `shards`. One shard is PR 241's ring unchanged.
pub fn fleet_ring(rows: &[(String, Value)], n: usize) -> Value {
    let n = n.max(1);
    let shards: Vec<Value> = (0..n)
        .map(|k| {
            let name = ring_state_name(k, n);
            rows.iter()
                .find(|(r, _)| *r == name)
                .map(|(_, v)| v.clone())
                .unwrap_or(Value::Null)
        })
        .collect();
    let all = shards.iter().all(Value::is_object);
    let revolution = if all {
        shards
            .iter()
            .map(|s| s["revolution"].as_i64().unwrap_or(0))
            .min()
            .unwrap_or(0)
    } else {
        0
    };
    let last: Option<Vec<i64>> = shards
        .iter()
        .map(|s| s["lastRevolutionMs"].as_i64())
        .collect();
    let last_tick = shards.iter().filter_map(|s| s["lastTickAt"].as_i64()).max();
    json!({
        "after": shards[0]["after"],
        "revolution": revolution,
        "lastRevolutionMs": last.and_then(|l| l.into_iter().max()),
        "lastTickAt": last_tick,
        "shardCount": n,
        "shards": shards.iter().enumerate().map(|(k, s)| json!({
            "shard": k,
            "after": s["after"],
            "revolution": s["revolution"],
            "lastRevolutionMs": s["lastRevolutionMs"],
            "lastTickAt": s["lastTickAt"],
            "restUntilMs": s["restUntilMs"],
        })).collect::<Vec<_>>(),
    })
}

/// The per-tick setting from the render var: unset or unreadable the default, clamped to
/// [`FLEET_COMPARE_PER_TICK_MAX`].
pub fn per_tick_of(var: Option<&str>) -> usize {
    var.and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(FLEET_COMPARE_PER_TICK)
        .min(FLEET_COMPARE_PER_TICK_MAX)
}

/// A port that counts the statements its inner judge runs (the ring's statement budget).
pub struct Metered<'a, J: JudgeDb> {
    pub inner: &'a J,
    pub statements: Cell<usize>,
}
impl<'a, J: JudgeDb> Metered<'a, J> {
    pub fn new(inner: &'a J) -> Self {
        Self {
            inner,
            statements: Cell::new(0),
        }
    }
}
impl<J: JudgeDb> SqlPort for Metered<'_, J> {
    async fn run_batch(&self, stmts: Vec<Stmt>) -> Result<Vec<StmtOut>, String> {
        self.statements.set(self.statements.get() + stmts.len());
        self.inner.run_batch(stmts).await
    }
    async fn counters(&self, deltas: Vec<(String, u64)>) {
        self.inner.counters(deltas).await;
    }
    fn now_ms(&self) -> u64 {
        self.inner.now_ms()
    }
    fn log(&self, line: &str) {
        self.inner.log(line);
    }
}
impl<J: JudgeDb> JudgeDb for Metered<'_, J> {
    fn tip(&self) -> Option<u64> {
        self.inner.tip()
    }
    fn era_ms(&self) -> Option<i64> {
        self.inner.era_ms()
    }
    async fn word_views(
        &self,
        rows: &std::collections::BTreeMap<String, crate::tracker_host::stored::Row>,
    ) -> Result<std::collections::BTreeMap<String, Value>, String> {
        self.inner.word_views(rows).await
    }
    async fn canonical_root(&self, height: u64) -> Option<String> {
        self.inner.canonical_root(height).await
    }
    async fn push(&self, identity: &str, body: Value) {
        self.inner.push(identity, body).await;
    }
    fn d1_judges(&self) -> bool {
        self.inner.d1_judges()
    }
    async fn actor_stored(&self, identity: &str) -> Result<Option<Value>, String> {
        self.inner.actor_stored(identity).await
    }
}

/// The actor's stored answer for a compare: `Err` naming why there is none (no actor bound, out of reach).
pub async fn actor_answer(db: &impl JudgeDb, identity: &str) -> Result<Value, String> {
    match db.actor_stored(identity).await {
        Ok(Some(a)) => Ok(a),
        Ok(None) => Err("no owed actor bound".into()),
        Err(e) => Err(e),
    }
}

/// One identity compared ([`fresh_compare`]) and its row built ([`compare_row_of`]). An actor out of reach, or one
/// not bootstrapped (no judgement of its own to compare), is the row's fault with NO fresh judgement made: the ring
/// spends no hydration on an identity it cannot compare.
pub async fn compare_one(
    db: &impl JudgeDb,
    identity: &str,
    now: i64,
) -> (CompareRow, Option<Value>) {
    let actor = actor_answer(db, identity).await;
    if let Ok(a) = &actor {
        if a["bootstrapped"] != true {
            return (
                compare_row_of(identity, now, Ok(&json!({"actor": a}))),
                None,
            );
        }
    }
    if let Err(e) = &actor {
        return (
            compare_row_of(identity, now, Ok(&json!({"actor": {"error": e}}))),
            None,
        );
    }
    match fresh_compare(db, identity, actor).await {
        Ok(view) => (compare_row_of(identity, now, Ok(&view)), Some(view)),
        Err(e) => (compare_row_of(identity, now, Err(&e)), None),
    }
}

/// One tick's account.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct RingReport {
    pub compared: Vec<String>,
    pub diffs: usize,
    pub faults: usize,
    pub statements: usize,
    pub cursor: String,
    pub wrapped: bool,
    pub stopped: Option<&'static str>,
}

/// One identity's compare as the ring makes it: production's is [`FreshCompare`] ([`compare_one`]); a pin gives the
/// ring a compare of a measured weight (`fix998_a`) so it counts the ring's own ticks, budgets and cursors.
pub(crate) trait RingCompare {
    async fn compare<J: JudgeDb>(&self, m: &Metered<'_, J>, identity: &str, now: i64)
        -> CompareRow;
}
/// The ring's compare in production: [`compare_one`].
pub(crate) struct FreshCompare;
impl RingCompare for FreshCompare {
    async fn compare<J: JudgeDb>(
        &self,
        m: &Metered<'_, J>,
        identity: &str,
        now: i64,
    ) -> CompareRow {
        compare_one(m, identity, now).await.0
    }
}

/// THE RING'S TICK (READ-986 item 4; FIX-998's shards): one tick of shard `shard` of `shards` ([`ring_tick_with`] over
/// [`FreshCompare`]).
pub async fn ring_tick(
    db: &impl JudgeDb,
    now: i64,
    shard: usize,
    shards: usize,
    per_tick: usize,
    elapsed: impl Fn() -> u64,
) -> Result<RingReport, String> {
    ring_tick_with(db, now, shard, shards, per_tick, elapsed, &FreshCompare).await
}

/// One shard's tick: up to `per_tick` identities of the shard's segments ([`shard_segments`]) past its cursor (the
/// identities list, in key order), each compared, each row written WITH the cursor in one batch (so a tick the platform
/// ends resumes after the last identity written); stopped before an identity the tick's statement budget or wall would
/// not hold. A short page past the shard's last segment wraps it (`revolution` + 1), drops the rows of its segments the
/// revolution did not refresh, and RESTS it until [`FLEET_COMPARE_REVOLUTION_FLOOR_MS`] after the revolution began
/// (`restUntilMs`; a resting tick compares nothing, `stopped` "resting"). The Phase C flip is observed here
/// (`owed-actor:phase-c`): the D1 judge seen off with no open period opens one at `now`, seen on with one open closes
/// it. `elapsed` reads the tick's wall.
pub(crate) async fn ring_tick_with<J: JudgeDb>(
    db: &J,
    now: i64,
    shard: usize,
    shards: usize,
    per_tick: usize,
    elapsed: impl Fn() -> u64,
    comparer: &impl RingCompare,
) -> Result<RingReport, String> {
    let shards = shards.max(1);
    let name = ring_state_name(shard, shards);
    let segments = shard_segments(shard, shards);
    let m = Metered::new(db);
    let mut read = m
        .run_batch(vec![
            Stmt::new(RING_READ_SQL, vec![Bind::Text(name.clone())]),
            Stmt::new(PHASE_C_READ_SQL, vec![]),
        ])
        .await?
        .into_iter();
    let parse = |o: Option<StmtOut>| -> Value {
        o.and_then(|o| o.rows.into_iter().next())
            .and_then(|r| {
                r["cursor"]
                    .as_str()
                    .and_then(|c| serde_json::from_str(c).ok())
            })
            .unwrap_or(Value::Null)
    };
    let mut ring = parse(read.next());
    let phase_c = parse(read.next());
    if !ring.is_object() {
        ring = json!({"after": "", "revolution": 0, "revolutionStartedAtMs": now});
    }
    // The Phase C flip, observed.
    let open = phase_c.is_object() && phase_c["endedAtMs"].is_null();
    let flip = match (m.d1_judges(), open) {
        (false, false) => Some(json!({"sinceMs": now, "endedAtMs": null})),
        (true, true) => {
            let mut p = phase_c.clone();
            p["endedAtMs"] = json!(now);
            Some(p)
        }
        _ => None,
    };
    let state = |name: &str, v: &Value| {
        Stmt::new(
            crate::owed_belt::STATE_SQL,
            vec![
                Bind::Text(name.into()),
                Bind::Text(v.to_string()),
                Bind::Int(now),
            ],
        )
    };
    if let Some(f) = &flip {
        m.run_batch(vec![state(PHASE_C_NAME, f)]).await?;
    }
    let mut report = RingReport::default();
    ring["lastTickAt"] = json!(now);
    let rest_until = ring["restUntilMs"].as_i64();
    if per_tick == 0 || rest_until.is_some_and(|r| now < r) {
        report.stopped = Some(if per_tick == 0 { "paused" } else { "resting" });
        m.run_batch(vec![state(&name, &ring)]).await?;
        report.statements = m.statements.get();
        report.cursor = ring["after"].as_str().unwrap_or_default().to_string();
        return Ok(report);
    }
    if let Some(r) = rest_until {
        // The rest is over: the next revolution begins at its end.
        ring.as_object_mut().map(|o| o.remove("restUntilMs"));
        ring["revolutionStartedAtMs"] = json!(r);
    }
    let after = ring["after"].as_str().unwrap_or_default().to_string();
    // The segment the cursor is in (a cursor in none, the shard's start).
    let first = if after.is_empty() {
        0
    } else {
        segments
            .iter()
            .position(|(lo, hi)| after.as_str() > lo.as_str() && after.as_str() <= hi.as_str())
            .unwrap_or(0)
    };
    let mut ids: Vec<String> = Vec::new();
    let mut exhausted = false;
    for (j, (lo, hi)) in segments.iter().enumerate().skip(first) {
        let from = if j == first && !after.is_empty() {
            after.clone()
        } else {
            lo.clone()
        };
        let need = per_tick - ids.len();
        let page: Vec<String> = m
            .run_batch(vec![Stmt::new(
                RING_IDENTITIES_SQL,
                vec![
                    Bind::Text(from),
                    Bind::Int(need as i64),
                    Bind::Text(hi.clone()),
                ],
            )])
            .await?
            .into_iter()
            .next()
            .map(|o| o.rows)
            .unwrap_or_default()
            .iter()
            .filter_map(|r| r["identity"].as_str().map(str::to_string))
            .collect();
        let short = page.len() < need;
        ids.extend(page);
        if !short {
            break;
        }
        exhausted = j + 1 == segments.len();
    }
    let mut cursor = after;
    let mut largest = FLEET_COMPARE_IDENTITY_RESERVE;
    let mut all_done = true;
    for id in &ids {
        let used = m.statements.get();
        if used + largest > FLEET_COMPARE_STATEMENTS_PER_TICK {
            report.stopped = Some("statements");
            all_done = false;
            break;
        }
        if elapsed() > FLEET_COMPARE_WALL_MS {
            report.stopped = Some("wall");
            all_done = false;
            break;
        }
        // A key with no actor (not 66-hex) is passed by the cursor with no row.
        if actor_name(id).is_none() {
            cursor = id.clone();
            continue;
        }
        let row = comparer.compare(&m, id, now).await;
        if row.diff_count.is_some_and(|n| n > 0) {
            report.diffs += 1;
        }
        if row.fault.is_some() {
            report.faults += 1;
        }
        cursor = id.clone();
        ring["after"] = json!(cursor);
        m.run_batch(vec![upsert_stmt(&row), state(&name, &ring)])
            .await?;
        largest = largest.max(m.statements.get() - used);
        report.compared.push(id.clone());
    }
    let mut end = Vec::new();
    if all_done && exhausted {
        // The shard's revolution's end: wrap, drop what it did not refresh, rest.
        report.wrapped = true;
        let started = ring["revolutionStartedAtMs"].as_i64().unwrap_or(now);
        let [(lo0, hi0), (lo1, hi1)] = segments;
        end.push(Stmt::new(
            RING_PRUNE_SQL,
            vec![
                Bind::Int(started),
                Bind::Text(lo0),
                Bind::Text(hi0),
                Bind::Text(lo1),
                Bind::Text(hi1),
            ],
        ));
        cursor = String::new();
        ring["revolution"] = json!(ring["revolution"].as_i64().unwrap_or(0) + 1);
        ring["lastRevolutionMs"] = json!(now - started);
        let rest = started + FLEET_COMPARE_REVOLUTION_FLOOR_MS;
        if rest > now {
            ring["restUntilMs"] = json!(rest);
        }
        ring["revolutionStartedAtMs"] = json!(rest.max(now));
    }
    ring["after"] = json!(cursor);
    end.push(state(&name, &ring));
    m.run_batch(end).await?;
    report.cursor = cursor;
    report.statements = m.statements.get();
    Ok(report)
}

// ---------------------------------------------------------------------------------------------------------------------
// The standing route (`GET` / `POST /internal/owed-actor-compare`).

/// The page of compare rows the standing route serves, at most.
pub const COMPARE_PAGE: usize = 64;
/// The fleet's counts over every stored compare row, one statement (the overlay's `/health/invariants` reads the same
/// aggregate in `owed_events::HEALTH_STATE_SQL`).
pub const COMPARE_SUMMARY_SQL: &str = "SELECT COUNT(*) AS identities,COALESCE(SUM(diffCount>0),0) AS diffs,COALESCE(SUM(heldByLagCount>0),0) AS heldByLag,COALESCE(SUM(staleD1Count),0) AS staleD1,COALESCE(SUM(fault IS NOT NULL),0) AS faults,MIN(comparedAtMs) AS oldest,MAX(comparedAtMs) AS newest FROM owed_actor_compare";
/// The rows worth naming past `?1` (a difference, a held one or a fault), `?2` a page; `?3` one identity or ''.
pub const COMPARE_PAGE_SQL: &str = "SELECT identity,comparedAtMs,diffCount,heldByLagCount,staleD1Count,tipLagCount,outpoints,d1Digest,actorDigest,fault,partial FROM owed_actor_compare WHERE identity>?1 AND (?3='' OR identity=?3) AND (diffCount>0 OR heldByLagCount>0 OR fault IS NOT NULL OR ?3<>'') ORDER BY identity LIMIT ?2";

/// PURE: the standing route's answer from its three reads (the summary, the page, the ring's state): every identity
/// whose compare differs (`diffs`: the differences no pending mark holds), whose differences a pending mark holds
/// (`heldByLag`), or whose compare faulted (`faults`), [`COMPARE_PAGE`] rows a page past `after` (`next`); narrowed to
/// one identity (`narrowed`), its row whatever it holds (`row`).
pub fn compare_answer(
    summary: &Value,
    page: &[Value],
    ring: &Value,
    now: i64,
    narrowed: bool,
) -> Value {
    let n = |v: &Value| {
        v.as_i64()
            .or_else(|| v.as_f64().map(|f| f as i64))
            .unwrap_or(0)
    };
    let entry = |r: &Value, count_key: &str, count: i64, held: bool| -> Value {
        let ops: Vec<Value> = r["outpoints"]
            .as_str()
            .and_then(|s| serde_json::from_str::<Value>(s).ok())
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default()
            .into_iter()
            .filter(|o| (o["held"] == true) == held)
            .collect();
        json!({
            "identity": r["identity"],
            "comparedAtMs": json_ms(&r["comparedAtMs"]),
            count_key: count,
            "staleD1": n(&r["staleD1Count"]),
            "tipLag": n(&r["tipLagCount"]),
            "partial": n(&r["partial"]) != 0,
            "digests": {"d1": r["d1Digest"], "actor": r["actorDigest"]},
            "outpoints": ops,
        })
    };
    let mut diffs = Vec::new();
    let mut held = Vec::new();
    let mut faults = Vec::new();
    for r in page {
        if let Some(f) = r["fault"].as_str() {
            faults.push(json!({"identity": r["identity"], "comparedAtMs": json_ms(&r["comparedAtMs"]), "fault": f}));
            continue;
        }
        if n(&r["diffCount"]) > 0 {
            diffs.push(entry(r, "diffCount", n(&r["diffCount"]), false));
        }
        if n(&r["heldByLagCount"]) > 0 {
            held.push(entry(r, "heldByLagCount", n(&r["heldByLagCount"]), true));
        }
    }
    let next = (page.len() == COMPARE_PAGE)
        .then(|| page.last().and_then(|r| r["identity"].as_str()))
        .flatten();
    let mut answer = json!({
        "identities": n(&summary["identities"]),
        "identitiesDiffering": n(&summary["diffs"]),
        "identitiesHeldByLag": n(&summary["heldByLag"]),
        "comparedWithinMs": json_ms(&summary["oldest"]).map(|o| now - o as i64),
        "newestComparedAtMs": json_ms(&summary["newest"]),
        "staleD1": n(&summary["staleD1"]),
        "faultCount": n(&summary["faults"]),
        "diffs": diffs,
        "heldByLag": held,
        "faults": faults,
        "ringCursor": ring["after"],
        "revolution": ring["revolution"],
        "lastRevolutionMs": ring["lastRevolutionMs"],
        "lastTickAt": ring["lastTickAt"],
        "shardCount": ring["shardCount"],
        "shards": ring["shards"],
        "next": next,
    });
    if narrowed {
        answer["row"] = page.first().map_or(Value::Null, |r| {
            let mut e = entry(r, "diffCount", n(&r["diffCount"]), false);
            e["heldByLagCount"] = json!(n(&r["heldByLagCount"]));
            e["fault"] = r["fault"].clone();
            e["outpoints"] = r["outpoints"]
                .as_str()
                .and_then(|s| serde_json::from_str::<Value>(s).ok())
                .unwrap_or(json!([]));
            e
        });
    }
    answer
}

/// `GET /internal/owed-actor-compare?after=&identity=` (bearer `INTERNAL_TOKEN`, 401 before any read): the ring's
/// stored compares. Three reads in one batch, no write.
pub async fn read_answer(
    db: &impl SqlPort,
    after: &str,
    identity: &str,
    now: i64,
    shards: usize,
) -> Result<Value, String> {
    let mut out = db
        .run_batch(vec![
            Stmt::new(COMPARE_SUMMARY_SQL, vec![]),
            Stmt::new(
                COMPARE_PAGE_SQL,
                vec![
                    Bind::Text(after.to_string()),
                    Bind::Int(COMPARE_PAGE as i64),
                    Bind::Text(identity.to_string()),
                ],
            ),
            Stmt::new(RING_SHARDS_READ_SQL, vec![]),
        ])
        .await?
        .into_iter();
    let summary = out
        .next()
        .and_then(|o| o.rows.into_iter().next())
        .unwrap_or_default();
    let page = out.next().map(|o| o.rows).unwrap_or_default();
    let rows: Vec<(String, Value)> = out
        .next()
        .map(|o| o.rows)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|r| {
            let cursor = r["cursor"]
                .as_str()
                .and_then(|c| serde_json::from_str(c).ok())?;
            Some((r["name"].as_str()?.to_string(), cursor))
        })
        .collect();
    let ring = fleet_ring(&rows, shards);
    Ok(compare_answer(
        &summary,
        &page,
        &ring,
        now,
        !identity.is_empty(),
    ))
}

/// `POST /internal/owed-actor-compare {"identity"}`: one identity compared NOW and its row written (the ring's cursor
/// not moved); answered the row and the compare's view.
pub async fn compare_now(db: &impl JudgeDb, identity: &str, now: i64) -> Result<Value, String> {
    let (row, view) = compare_one(db, identity, now).await;
    db.run_batch(vec![upsert_stmt(&row)]).await?;
    Ok(json!({
        "identity": row.identity,
        "comparedAtMs": row.compared_at_ms,
        "diffCount": row.diff_count,
        "heldByLagCount": row.held_by_lag_count,
        "staleD1Count": row.stale_d1_count,
        "tipLagCount": row.tip_lag_count,
        "outpoints": row.outpoints,
        "fault": row.fault,
        "partial": row.partial,
        "view": view,
    }))
}
