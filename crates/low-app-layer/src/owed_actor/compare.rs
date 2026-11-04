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
/// FIX-1056-FOLD-3 (bsv-low #1056, review 3's LOW-1): the identity's games in the ROTATION'S order, one statement: the
/// games past the identity's keyset cursor (`lap.after` of its stored PARTIAL compare row, `''` with none) in game-id
/// order, then the games at or before it from the first (the next lap's start), at most `?2` rows, each with the
/// union's size (`n`, the window count over every game before the limit). Game ids compare lowercased, as the compare
/// holds them. A game never moves in this order (its id is its place), so a lap walks every game present at its start.
pub const COMPARE_GAMES_SQL: &str = concat!(
    "SELECT gameId,n FROM (SELECT lower(gameId) AS gameId,COUNT(*) OVER () AS n FROM (",
    crate::owed_judge::identity_games!(),
    ")) ORDER BY CASE WHEN gameId>COALESCE((SELECT json_extract(lap,'$.after') FROM owed_actor_compare WHERE identity=?1 AND partial>0 AND json_valid(lap)),'') THEN 0 ELSE 1 END,gameId LIMIT ?2"
);
/// The rotation's page ([`COMPARE_GAMES_SQL`]'s `?2`): at least two windows past any cursor, so a walk of
/// [`FRESH_COMPARE_GAMES_MAX`] places never reaches the end of a page it was not given whole.
pub const COMPARE_GAMES_PAGE: usize = 4 * FRESH_COMPARE_GAMES_MAX;
/// The Phase C flip as the ring observed it (`owed_belt_state` `owed-actor:phase-c`: `{sinceMs, endedAtMs}`).
pub const PHASE_C_NAME: &str = "owed-actor:phase-c";
pub const PHASE_C_READ_SQL: &str =
    "SELECT cursor FROM owed_belt_state WHERE name='owed-actor:phase-c'";

/// bsv-low #1093: is this stored compare entry a COUNTED difference (no hold, no pending re-read, no clear)? The one
/// predicate [`classify_reread`] and [`carried_counted_games`] read.
pub fn is_counted_entry(p: &Value) -> bool {
    p["held"] == false
        && p["pendingReread"] != true
        && p["heldByApply"] != true
        && p["judgeMoved"] != true
        && p["bothMoved"] != true
}

/// bsv-low #1093 (lane FIX-BELT-INDEX, ruling 2): the most games of carried counted differences one compare reads,
/// half its window. The forward walk's half is kept by [`pick_games_keyset`], which takes at most half the window of
/// the pending and carried games TOGETHER (fold 1), so a lap always completes.
pub const COMPARE_CARRIED_FIRST_MAX: usize = FRESH_COMPARE_GAMES_MAX / 2;

/// PURE (bsv-low #1093, ruling 2): the games of the stored compare's COUNTED differences ([`is_counted_entry`]), in
/// stored order, distinct, at most [`COMPARE_CARRIED_FIRST_MAX`]. A partial compare carries a counted difference whose
/// game it did not judge (FIX-1056-FOLD-1's rule: a difference not judged is never a clear), so before this the
/// difference was counted again, unread, on every compare until the lap's window reached its game (beta, 2026-11-04:
/// `030d9e9f…`'s `95bcf03a…:0`, 177 games in windows of 64, the D1 side `hop-stranded` claimable judged BEFORE the
/// belt's find, `staleD1 1`, counted on every compare of the lap after the find). Now [`fresh_compare`] judges these
/// games first, beside the pending re-reads' (FIX-1056), so a counted difference is always the reading of a FRESH D1
/// judgement over the evidence the record holds at that compare, never a stored or earlier one. Past the bound the
/// rest are judged in their own window, as before.
pub fn carried_counted_games(prior: &Value) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for p in prior.as_array().into_iter().flatten() {
        if out.len() >= COMPARE_CARRIED_FIRST_MAX {
            break;
        }
        if !is_counted_entry(p) {
            continue;
        }
        if let Some(g) = p["gameId"].as_str().filter(|g| !g.is_empty()) {
            let g = g.to_ascii_lowercase();
            if !out.contains(&g) {
                out.push(g);
            }
        }
    }
    out
}

/// The games one compare judges ([`pick_games_keyset`]).
#[derive(Debug, Clone, PartialEq)]
pub struct GamePick {
    /// The games judged: the pending games first, then the window in the rotation's order.
    pub picked: Vec<String>,
    /// The union's size (the identity's games, the rows' games and the pending games; exact when the page was whole).
    pub total: usize,
    /// Whether every game was judged (no rotation: the cursor ends).
    pub whole: bool,
    /// The keyset cursor the next compare starts past (the last game this compare walked; `''`, the next lap's start).
    pub next_after: String,
    /// Whether this compare's walk passed the last game of the id order: a LAP completed (every game present when the
    /// lap began, at cursor `''`, has been judged in it; a game added since with a smaller id is the next lap's).
    pub wrapped: bool,
}

/// FIX-1056-FOLD-3 (review 3's LOW-1): the games a compare judges, at most `max`. The games of the identity's PENDING
/// re-reads first (`first`, FIX-1056: a pending outpoint is re-read when its game is judged; then, bsv-low #1093, the
/// carried counted differences'), at most `max / 2` of them (#1093's fold 1), then, when the rest does
/// not fit, the rest walked by KEYSET over the game id from past `after` (the stored cursor), wrapping to the first
/// game once it passes the last. The rest is the rows' games and the identity's games ([`COMPARE_GAMES_SQL`]'s `page`
/// in its order, `count` the identity's game count); a pending game is skipped where it stands (judged already), so
/// it never shifts the walk. A row's game past the end of a page that was cut is left to a later compare (its place is
/// not known to be inside what the page read). Replaces FIX-1056-FOLD-1's offset rotation, whose positions moved when
/// a game moved in the newest-first order (review 3's rv3_a: a game skipped in the lap that counted the wrap).
pub fn pick_games_keyset(
    first: &[String],
    rows: &[&[Value]],
    page: &[String],
    count: usize,
    after: &str,
    max: usize,
) -> GamePick {
    let after = after.to_ascii_lowercase();
    let mut picked: Vec<String> = Vec::new();
    let mut all: BTreeSet<String> = BTreeSet::new();
    for g in first {
        let g = g.to_ascii_lowercase();
        if !g.is_empty() && all.insert(g.clone()) {
            picked.push(g);
        }
    }
    // bsv-low #1093 (fold 1, the ensemble's C LOW-2 and A LOW-3): the games judged first (the pending re-reads', then
    // the carried counted differences') take at most HALF the window, so the forward walk keeps the other half and a
    // lap always completes. A first game left out is not judged in this compare: its pending re-read stays pending and
    // its counted difference stays counted as carried (FIX-1056's rule), never a clear.
    picked.truncate(max / 2);
    let mut from_rows: BTreeSet<String> = BTreeSet::new();
    for side in rows {
        for r in side.iter() {
            if let Some(g) = r["gameId"].as_str().filter(|g| !g.is_empty()) {
                from_rows.insert(g.to_ascii_lowercase());
            }
        }
    }
    let page: Vec<String> = page
        .iter()
        .map(|g| g.to_ascii_lowercase())
        .filter(|g| !g.is_empty())
        .collect();
    let complete = page.len() < COMPARE_GAMES_PAGE;
    let seg1_db: Vec<&String> = page
        .iter()
        .filter(|g| g.as_str() > after.as_str())
        .collect();
    let seg2_db: Vec<&String> = page
        .iter()
        .filter(|g| g.as_str() <= after.as_str())
        .collect();
    // The page reads the games past the cursor first: they are all there when the page is whole, or when it went on
    // past them into the next lap's games.
    let seg1_complete = complete || !seg2_db.is_empty();
    let seg1_bound = (!seg1_complete)
        .then(|| seg1_db.last().map(|g| g.to_string()))
        .flatten();
    let seg2_bound = (!complete)
        .then(|| seg2_db.last().map(|g| g.to_string()))
        .flatten();
    let mut seg1: BTreeSet<String> = seg1_db.iter().map(|g| g.to_string()).collect();
    let mut seg2: BTreeSet<String> = seg2_db.iter().map(|g| g.to_string()).collect();
    for g in &from_rows {
        if g.as_str() > after.as_str() {
            if seg1_bound.as_ref().is_none_or(|b| g <= b) {
                seg1.insert(g.clone());
            }
        } else if complete || seg2_bound.as_ref().is_some_and(|b| g <= b) {
            seg2.insert(g.clone());
        }
    }
    all.extend(seg1.iter().cloned());
    all.extend(seg2.iter().cloned());
    // A row's game is known to lie outside the identity's games only inside the ranges the page read whole.
    let in_db: BTreeSet<&String> = page.iter().collect();
    let covered = |g: &String| {
        if g.as_str() > after.as_str() {
            seg1_complete || seg1_bound.as_ref().is_some_and(|b| g <= b)
        } else {
            complete || seg2_bound.as_ref().is_some_and(|b| g <= b)
        }
    };
    let rows_only = from_rows
        .iter()
        .filter(|g| covered(g) && !in_db.contains(g))
        .count();
    let total = if complete {
        all.len()
    } else {
        (count + rows_only).max(all.len())
    };
    if complete && all.len() <= max {
        let mut whole = picked.clone();
        for g in seg1.iter().chain(seg2.iter()) {
            if !whole.contains(g) {
                whole.push(g.clone());
            }
        }
        // A whole compare in game order from the first game (the base's window held every game too).
        let mut rest: Vec<String> = whole.split_off(picked.len().min(whole.len()));
        rest.sort();
        whole.extend(rest);
        return GamePick {
            picked: whole,
            total,
            whole: true,
            next_after: String::new(),
            wrapped: false,
        };
    }
    let mut next_after = after.clone();
    let mut wrapped = false;
    if picked.len() < max {
        for g in &seg1 {
            if picked.len() >= max {
                break;
            }
            next_after = g.clone();
            if !picked.contains(g) {
                picked.push(g.clone());
            }
        }
        // Every game past the cursor walked (the page held them all): the lap completed in this compare.
        if seg1_complete
            && seg1
                .iter()
                .next_back()
                .is_none_or(|last| *last == next_after)
        {
            wrapped = true;
            next_after = String::new();
            for g in &seg2 {
                if picked.len() >= max {
                    break;
                }
                next_after = g.clone();
                if !picked.contains(g) {
                    picked.push(g.clone());
                }
            }
        }
    }
    GamePick {
        picked,
        total,
        whole: false,
        next_after,
        wrapped,
    }
}

/// FIX-1056-FOLD-3: the page [`COMPARE_GAMES_SQL`] answers for `games` (every game of an identity) past `after`, and the
/// count: the statement's order modelled for the pure pins.
pub fn games_page_of(games: &[String], after: &str) -> (Vec<String>, usize) {
    let after = after.to_ascii_lowercase();
    let mut all: Vec<String> = games
        .iter()
        .map(|g| g.to_ascii_lowercase())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let n = all.len();
    all.sort_by_key(|g| (g.as_str() <= after.as_str(), g.clone()));
    all.truncate(COMPARE_GAMES_PAGE);
    (all, n)
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

/// The compared fields that differ on one outpoint, both values (the VERDICT fields [`diff_rows`] compares,
/// [`super::VERDICT_COLUMNS`] and [`super::VERDICT_FACTS`] as `facts.<key>`; FIX-COMPARE-FACTS, bsv-low #1095: no other
/// fact is printed); a row on one side only is `row: {d1, actor}` with the present side's family, game, sats and
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
            for k in super::VERDICT_COLUMNS {
                if a[k] != b[k] {
                    out.insert(k.into(), json!({"d1": short(&a[k]), "actor": short(&b[k])}));
                }
            }
            let (fa, fb) = (super::compared_facts(&a), super::compared_facts(&b));
            for k in super::VERDICT_FACTS {
                if fa[k] != fb[k] {
                    out.insert(
                        format!("facts.{k}"),
                        json!({"d1": short(&fa[k]), "actor": short(&fb[k])}),
                    );
                }
            }
            for (k, x, y) in super::press_fields_differing(&a, &b) {
                out.insert(
                    format!("press.{k}"),
                    json!({"d1": short(&x), "actor": short(&y)}),
                );
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

/// PURE (FIX-COMPARE-FACTS, bsv-low #1095, ruling 3): a stored compare entry read by this build's verdict rule. Its
/// printed `fields` keep the verdict fields alone ([`super::is_verdict_field`]); an entry whose `fields` named fields
/// and none of them a verdict field (printed under a rule that counted a fact's spelling: `facts.snapshotTip`,
/// `facts.chainProbe`, the filed sweep's facts) is `None`, no difference by its own record. An entry with no printed
/// fields (past the cut, held, a tip lag) is kept as it is.
pub fn verdict_entry(p: &Value) -> Option<Value> {
    let Some(fields) = p["fields"].as_object().filter(|f| !f.is_empty()) else {
        return Some(p.clone());
    };
    let kept: serde_json::Map<String, Value> = fields
        .iter()
        .filter(|(k, _)| super::is_verdict_field(k))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    if kept.is_empty() {
        return None;
    }
    let mut p = p.clone();
    p["fields"] = Value::Object(kept);
    Some(p)
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
                vec![Bind::Text(me.clone()), Bind::Int(COMPARE_GAMES_PAGE as i64)],
            ),
            Stmt::new(PHASE_C_READ_SQL, vec![]),
            // FIX-1056: the identity's stored compare, read once here: its pending outpoints' games are judged first
            // ([`pick_games`]), and the re-read classifies against it ([`CompareRow::prior`], no second read).
            Stmt::new(PRIORS_SQL, vec![Bind::Text(json!([me]).to_string())]),
        ])
        .await?
        .into_iter();
    let mut next = || read.next().map(|o: StmtOut| o.rows).unwrap_or_default();
    let stored = next();
    let under = next().into_iter().next().unwrap_or_default();
    let pending = next();
    let games_page = next();
    let game_count = games_page
        .first()
        .and_then(|r| {
            r["n"]
                .as_u64()
                .or_else(|| r["n"].as_f64().map(|f| f as u64))
        })
        .unwrap_or(0) as usize;
    let identity_games: Vec<String> = games_page
        .iter()
        .filter_map(|r| r["gameId"].as_str().map(str::to_string))
        .collect();
    let phase_c: Value = next()
        .first()
        .and_then(|r| r["cursor"].as_str())
        .and_then(|c| serde_json::from_str(c).ok())
        .unwrap_or(Value::Null);
    let prior_rows = next();
    let prior = prior_of(&prior_rows).unwrap_or(json!([]));
    // FIX-1056-FOLD-3 (review 3's LOW-1 and LOW-2): the identity's lap as its last PARTIAL compare left it (the keyset
    // cursor, the lap's state, [`Lap`]), and the rotations and zero-held laps it has completed; a whole compare left
    // none (`partial` 0: the next partial compare starts a lap from the first game).
    let (lap_before, rotations_before, zero_held_before) =
        prior_rows.first().map_or((Lap::default(), 0, 0), |r| {
            if partial_int(&r["partial"]) > 0 {
                (
                    Lap::of_column(&r["lap"]),
                    rotations_of_partial(&r["partial"]),
                    zero_held_laps_of_partial(&r["partial"]),
                )
            } else {
                (Lap::default(), 0, 0)
            }
        });
    let mut pending_games: Vec<String> = prior
        .as_array()
        .into_iter()
        .flatten()
        .filter(|p| p["pendingReread"] == true)
        .filter_map(|p| p["gameId"].as_str().map(str::to_string))
        .collect();
    // bsv-low #1093 (ruling 2): a COUNTED difference carried from an earlier compare is re-judged before it is counted
    // again: its game is judged first, beside the pending ones.
    pending_games.extend(carried_counted_games(&prior));
    let actor_rows: Vec<Value> = actor
        .as_ref()
        .ok()
        .and_then(|a| a["rows"].as_array().cloned())
        .unwrap_or_default();
    let GamePick {
        picked: games,
        total,
        whole,
        next_after,
        wrapped,
    } = pick_games_keyset(
        &pending_games,
        &[&actor_rows, &stored],
        &identity_games,
        game_count,
        &lap_before.after,
        FRESH_COMPARE_GAMES_MAX,
    );
    let partial = !whole;
    let rotations = if partial {
        rotations_before + u64::from(wrapped)
    } else {
        0
    };
    let d1_judges = db.d1_judges();
    let judged: BTreeSet<&str> = games.iter().map(String::as_str).collect();
    let within = |rows: &[Value]| -> Vec<Value> {
        rows.iter()
            .filter(|r| !partial || judged.contains(r["gameId"].as_str().unwrap_or_default()))
            .cloned()
            .collect()
    };
    // FIX-1056-FOLD-1 (the review's MEDIUM-1 and LOW-2): the outpoints either side holds in a game this compare did
    // not judge (the actor's rows and D1's stored rows, before `within` drops them): neither side of them was read, so
    // nothing about them is a clear ([`classify_reread`]), whatever game their stored entry names. FIX-1056-FOLD-2
    // (the second review's LOW-3): a STALE stored row ([`stored_row_stale`]: Phase C, or judged before the flip) is no
    // side of the compare (the D1 side is the fresh judgement), so it marks nothing unjudged: an outpoint the actor
    // holds in one game and a frozen stored row in a far one is read whenever the actor's game is judged.
    let unjudged: BTreeSet<String> = if partial {
        actor_rows
            .iter()
            .chain(
                stored
                    .iter()
                    .filter(|r| !stored_row_stale(r, d1_judges, &phase_c)),
            )
            .filter(|r| !judged.contains(r["gameId"].as_str().unwrap_or_default()))
            .filter_map(|r| r["outpoint"].as_str().map(str::to_string))
            .collect()
    } else {
        BTreeSet::new()
    };
    // FIX-1056-FOLD-3 (review 3's MEDIUM-1): ONE CEILING RULE FOR BOTH SIDES. The actor's rows are its whole identity's
    // judgement ranked at the 500-row ceiling ([`crate::owed_judge::ceiling_key`]); a partial compare's D1 side is the
    // window's games, so it is judged UNCUT and cut at the same ceiling: a D1 row whose key sorts after the actor's
    // 500th row's is one the identity's ceiling cuts (served `pastCeiling`, never a difference). Below the ceiling (the
    // actor holds fewer than 500 rows) nothing is cut. A whole compare is ranked over every game at the ceiling by the
    // judge itself. No judge statement more.
    let (fresh, past_ceiling) = if partial {
        let uncut = crate::owed_judge::judge_fresh_rows_ranked(db, &me, &games, false).await?;
        ceiling_cut(uncut, &actor_rows, actor.is_ok())
    } else {
        (
            crate::owed_judge::judge_fresh_rows(db, &me, &games).await?,
            Vec::new(),
        )
    };
    let stored = within(&stored);
    // APP-L59 fold 1 (the ensemble's C (1), B LOW-2, B LOW-3): both sides through THE READ'S sweep projection at the
    // fresh judgement's tip ([`project_compared_rows`]), after the ceiling cut (it moves no key): a stored courier-
    // dependent press judged below the block is compared as the `/owed` read serves it, #669's chain wait, so the
    // served list and the compare agree with the D1 judge across blocks with no block re-judge and no bound.
    let tip = db.tip();
    let fresh = project_compared_rows(fresh, tip);
    let actor = actor.map(|mut a| {
        a["rows"] = json!(project_compared_rows(within(&actor_rows), tip));
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
    view["d1"]["fresh"] = json!(true);
    view["d1"]["tip"] = json!(db.tip());
    view["d1"]["games"] = json!({"judged": games.len(), "total": total, "partial": partial});
    if partial {
        // FIX-1056: the games a partial compare judged, so a pending outpoint of a game it left out stays pending.
        view["d1"]["games"]["judgedGames"] = json!(games);
        view["d1"]["games"]["unjudgedOutpoints"] = json!(unjudged);
        view["d1"]["games"]["nextAfter"] = json!(next_after);
        view["d1"]["games"]["wrapped"] = json!(wrapped);
        view["d1"]["games"]["rotations"] = json!(rotations);
        view["d1"]["games"]["lapBefore"] = lap_before.to_value();
        view["d1"]["games"]["zeroHeldLapsBefore"] = json!(zero_held_before);
        view["d1"]["pastCeiling"] = json!(past_ceiling);
    }
    // FIX-1056: the identity's stored compare outpoints as read before this compare (the re-read's prior).
    view["storedCompare"] = prior;
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

/// PURE (APP-L59 fold 1): stored owed rows (`facts` a JSON string, the `claimable` and `probeTip` columns) as the
/// `/owed` read serves them at `tip`: each through [`crate::owed_read::project_stored_sweep`], the read's one rule, its
/// `claimable` column following its facts (the column's own type kept). A row the rule leaves is returned as it was.
pub fn project_compared_rows(rows: Vec<Value>, tip: Option<u64>) -> Vec<Value> {
    let n = |v: &Value| {
        v.as_u64()
            .or_else(|| v.as_f64().filter(|f| *f >= 0.0).map(|f| f as u64))
    };
    rows.into_iter()
        .map(|mut r| {
            let Some(mut facts) = r["facts"]
                .as_str()
                .and_then(|f| serde_json::from_str::<Value>(f).ok())
            else {
                return r;
            };
            let before = facts.clone();
            let mut reason = r["reason"].as_str().map(str::to_owned);
            crate::owed_read::project_stored_sweep(
                r["family"].as_str().unwrap_or_default(),
                &mut facts,
                &mut reason,
                n(&r["probeTip"]),
                tip,
            );
            if facts == before {
                return r;
            }
            if facts["claimable"] == false {
                r["claimable"] = match &r["claimable"] {
                    Value::Bool(_) => json!(false),
                    Value::Null => Value::Null,
                    _ => json!(0),
                };
            }
            r["facts"] = json!(facts.to_string());
            r["reason"] = json!(reason);
            r
        })
        .collect()
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
    /// FIX-1046-FOLD-1 (MEDIUM-1): each outpoint on either side, the print of its D1 row and of its actor row
    /// ([`row_print`], `absent` for no row): what the re-read reads to tell which side moved. Not stored.
    pub prints: std::collections::BTreeMap<String, (String, String)>,
    /// FIX-1056: each outpoint on either side, its row's game (the D1 row's, else the actor's). Not stored.
    pub games: std::collections::BTreeMap<String, String>,
    /// FIX-1056: the games a PARTIAL compare judged (`None`: every game of the identity was judged). A pending outpoint
    /// whose game is not here was not read, and stays pending. Not stored.
    pub judged_games: Option<BTreeSet<String>>,
    /// FIX-1056: the identity's stored compare outpoints as [`fresh_compare`] read them (`None`: not read, the re-read
    /// reads them). Not stored.
    pub prior: Option<Value>,
    /// FIX-1056-FOLD-1 (MEDIUM-1, LOW-2): the outpoints either side holds in a game a PARTIAL compare did not judge
    /// (neither side read). Not stored.
    pub unjudged: BTreeSet<String>,
    /// FIX-1056-FOLD-3 (review 3's LOW-1): the lap after this compare ([`Lap`], its cursor the last game walked,
    /// [`pick_games_keyset`]); before [`classify_reread`], the lap as the prior compare left it with this compare's
    /// cursor. Stored in the `lap` column on a partial row.
    pub lap: Lap,
    /// FIX-1056-FOLD-3: whether this compare completed a lap ([`GamePick::wrapped`]). Not stored.
    pub wrapped: bool,
    /// FIX-1056-FOLD-3 (review 3's LOW-2): the laps this partial identity has completed with zero read throughout
    /// (no counted and no pending difference on any compare of the lap; reset to 0 by any), stored in the `partial`
    /// column ([`partial_column`]) and served (`partialZeroHeldLapsMin`): a partial identity's zero covers every game
    /// only once this is at least 1.
    pub zero_held_laps: u64,
    /// FIX-1056-FOLD-2 (the second review's item 3): the rotations this identity's partial compares have COMPLETED
    /// (each a walk of the window past the end of its rest, [`pick_games_rotating`]'s `wrapped`), counted from its
    /// last whole compare; stored in the `partial` column beside the offset ([`partial_column`]) and served
    /// (`partialUnrotated`, `partialRotationsMin`). 0 on a whole compare.
    pub rotations: u64,
}

/// FIX-1056-FOLD-2: the `partial` column's stride: the column is `1 + zero_held_laps + ROTATION_STRIDE * rotations` on a
/// partial row (FIX-1056-FOLD-3: the low part was the offset rotation's offset; the keyset cursor rides the `lap`
/// column, [`Lap`]). The column stays exact under D1's REAL bind (2^53) up to 2^21 rotations.
pub const ROTATION_STRIDE: i64 = 1 << 32;

/// FIX-1056-FOLD-3 (review 3's LOW-1, LOW-2, NOTE-2): a partial identity's LAP, stored in the compare row's `lap`
/// column (JSON; a fault row keeps it, as it keeps `partial`): the keyset cursor (`after`, the last game walked; `''`
/// a lap's start), whether every compare of the lap so far read zero (`lapClean`: no counted and no pending
/// difference), and the counted differences past [`COMPARE_ROW_COUNTED_MAX`] that no list holds (`overflow`, carried
/// in `diffCount` until a lap ends with no overflow in it, `overflowThisLap`).
#[derive(Debug, Clone, PartialEq)]
pub struct Lap {
    pub after: String,
    pub lap_clean: bool,
    pub overflow: i64,
    pub overflow_this_lap: bool,
}

impl Default for Lap {
    fn default() -> Self {
        Lap {
            after: String::new(),
            lap_clean: true,
            overflow: 0,
            overflow_this_lap: false,
        }
    }
}

impl Lap {
    /// The lap from a stored `lap` column (TEXT JSON, or none: a fresh lap).
    pub fn of_column(v: &Value) -> Lap {
        let v: Value = match v {
            Value::String(t) => serde_json::from_str(t).unwrap_or(Value::Null),
            other => other.clone(),
        };
        if !v.is_object() {
            return Lap::default();
        }
        Lap {
            after: v["after"].as_str().unwrap_or_default().to_ascii_lowercase(),
            lap_clean: v["lapClean"] != false,
            overflow: v["overflow"].as_i64().unwrap_or(0).max(0),
            overflow_this_lap: v["overflowThisLap"] == true,
        }
    }
    pub fn to_value(&self) -> Value {
        json!({"after": self.after, "lapClean": self.lap_clean, "overflow": self.overflow,
               "overflowThisLap": self.overflow_this_lap})
    }
}

/// PURE (FIX-1056-FOLD-3, review 3's MEDIUM-1): a partial compare's uncut D1 rows cut at the ACTOR'S ceiling: when the
/// actor's answer holds the ceiling's 500 rows (its whole identity ranked, [`crate::owed_judge::ceiling_key`]), every
/// D1 row whose key sorts after the actor's 500th row's key is past the identity's ceiling (returned apart, its
/// outpoints). The edge is the 500th key even when the actor holds more (FIX-1056-FOLD-4): an actor over its ceiling
/// shows its excess as differences. Sound in both directions: an actor equal to the D1 judge holds exactly the rows at or before that key,
/// so it reads zero; an actor that holds a row the D1 judge does not (or lacks one it holds) at or before the key is a
/// difference, and an actor whose ceiling a phantom row moved shows that phantom as a difference when its game is
/// judged (the truth's row it pushed past the ceiling is the only one served apart). An actor with fewer rows, or one
/// that did not answer, cuts nothing.
pub fn ceiling_cut(
    d1: Vec<Value>,
    actor_rows: &[Value],
    actor_ok: bool,
) -> (Vec<Value>, Vec<String>) {
    if !actor_ok || actor_rows.len() < crate::owed::OWED_MAX_ROWS {
        return (d1, Vec::new());
    }
    // FIX-1056-FOLD-4 (review 4's LOW-1): the edge is the actor's 500th key, never its largest. An actor past its
    // ceiling (its cut failed) would move a largest-key edge with its own excess and read zero; at the 500th, its rows
    // past the edge stay on its side and are differences (D1 cuts its own copy there, `pastCeiling`).
    let mut keys: Vec<_> = actor_rows
        .iter()
        .map(crate::owed_judge::ceiling_key)
        .collect();
    keys.sort();
    let Some(edge) = keys.into_iter().nth(crate::owed::OWED_MAX_ROWS - 1) else {
        return (d1, Vec::new());
    };
    let (kept, past): (Vec<Value>, Vec<Value>) = d1
        .into_iter()
        .partition(|r| crate::owed_judge::ceiling_key(r) <= edge);
    (
        kept,
        past.iter()
            .filter_map(|r| r["outpoint"].as_str().map(str::to_string))
            .collect(),
    )
}

/// The `partial` column's value: 0 for a whole compare, else `1 + zero_held_laps` (FIX-1056-FOLD-3) plus
/// [`ROTATION_STRIDE`] for each completed rotation (so every reader that reads it as a flag, `partial != 0`, still
/// does, and a row written before the fold reads 0 rotations and 0 zero-held laps).
pub fn partial_column(row: &CompareRow) -> i64 {
    if row.partial {
        1 + (row.zero_held_laps.min((ROTATION_STRIDE - 2) as u64) as i64)
            + ROTATION_STRIDE * (row.rotations.min((i64::MAX / ROTATION_STRIDE - 1) as u64) as i64)
    } else {
        0
    }
}

fn partial_int(v: &Value) -> i64 {
    v.as_i64()
        .or_else(|| v.as_f64().map(|f| f as i64))
        .unwrap_or(0)
}

/// FIX-1056-FOLD-3 (review 3's LOW-2): the laps completed with zero read throughout, from a stored `partial` column.
pub fn zero_held_laps_of_partial(v: &Value) -> u64 {
    let n = partial_int(v);
    if n > 0 {
        ((n - 1) % ROTATION_STRIDE) as u64
    } else {
        0
    }
}

/// FIX-1056-FOLD-2: the completed rotations from a stored `partial` column: 0 for none (or a row written before).
pub fn rotations_of_partial(v: &Value) -> u64 {
    let n = partial_int(v);
    if n > 0 {
        ((n - 1) / ROTATION_STRIDE) as u64
    } else {
        0
    }
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
        prints: Default::default(),
        games: Default::default(),
        judged_games: None,
        prior: None,
        unjudged: BTreeSet::new(),
        lap: Lap::default(),
        wrapped: false,
        zero_held_laps: 0,
        rotations: 0,
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
    // FIX-1009 (bsv-low #1009): a TIP LAG ([`super::tip_derived_rows`], the route's `tipLag`) is a difference the
    // judged tips explain, never a STOP: it is held by lag (counted in `heldByLagCount`, so the fleet's
    // `identitiesHeldByLag`, never `identitiesDiffering`), its outpoint named `held` and `tipLag`.
    let tip_lag = strs(&view["tipLag"]);
    let outpoints: Vec<Value> = diff
        .iter()
        .map(|o| {
            json!({
                "outpoint": o,
                "held": held.contains(o),
                "fields": fields.iter().find(|f| f["outpoint"] == o.as_str()).map_or(Value::Null, |f| f["fields"].clone()),
            })
        })
        .chain(tip_lag.iter().map(|o| json!({"outpoint": o, "held": true, "tipLag": true, "fields": Value::Null})))
        .collect();
    // FIX-1046-FOLD-1 (LOW-1, MEDIUM-1's cut): every difference is kept here, uncut; [`classify_reread`] cuts the
    // stored list after it has classified the whole set, so no difference is classed by its absence from a cut list.
    let mut prints: std::collections::BTreeMap<String, (String, String)> = Default::default();
    let mut games: std::collections::BTreeMap<String, String> = Default::default();
    let side = |v: &Value| v.as_array().cloned().unwrap_or_default();
    for (k, rows) in [side(&view["d1"]["rows"]), side(&view["actor"]["rows"])]
        .iter()
        .enumerate()
    {
        for r in rows {
            let Some(o) = r["outpoint"].as_str() else {
                continue;
            };
            if let Some(g) = r["gameId"].as_str().filter(|g| !g.is_empty()) {
                games
                    .entry(o.to_string())
                    .or_insert_with(|| g.to_ascii_lowercase());
            }
            let e = prints
                .entry(o.to_string())
                .or_insert_with(|| (ABSENT_PRINT.into(), ABSENT_PRINT.into()));
            if k == 0 {
                e.0 = row_print(r);
            } else {
                e.1 = row_print(r);
            }
        }
    }
    CompareRow {
        identity: identity.to_string(),
        compared_at_ms: now,
        diff_count: Some(diff.iter().filter(|o| !held.contains(*o)).count() as i64),
        held_by_lag_count: Some((held.len() + tip_lag.len()) as i64),
        stale_d1_count: Some(strs(&view["d1Stored"]["staleOutpoints"]).len() as i64),
        tip_lag_count: Some(tip_lag.len() as i64),
        outpoints: json!(outpoints),
        d1_digest: view["digests"]["d1"].as_str().map(str::to_string),
        actor_digest: view["digests"]["actor"].as_str().map(str::to_string),
        fault: None,
        partial: view["d1"]["games"]["partial"] == true,
        prints,
        games,
        judged_games: (view["d1"]["games"]["partial"] == true).then(|| {
            view["d1"]["games"]["judgedGames"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_ascii_lowercase)
                .collect()
        }),
        prior: view.get("storedCompare").cloned(),
        unjudged: view["d1"]["games"]["unjudgedOutpoints"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect(),
        lap: Lap {
            after: view["d1"]["games"]["nextAfter"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            ..Lap::of_column(&view["d1"]["games"]["lapBefore"])
        },
        wrapped: view["d1"]["games"]["wrapped"] == true,
        zero_held_laps: view["d1"]["games"]["zeroHeldLapsBefore"]
            .as_u64()
            .unwrap_or(0),
        rotations: view["d1"]["games"]["rotations"].as_u64().unwrap_or(0),
    }
}

/// FIX-1046-FOLD-1: the print of one side's row of an outpoint (its compared fields, [`super::rows_digest`] of the row
/// alone, cut to 16 hex), or [`ABSENT_PRINT`] for no row. Two prints are equal exactly when [`diff_rows`] reads the two
/// rows equal.
pub fn row_print(row: &Value) -> String {
    rows_digest(std::slice::from_ref(row))[..16].to_string()
}
pub const ABSENT_PRINT: &str = "absent";

/// One compare row written (an upsert by identity).
pub const COMPARE_UPSERT_SQL: &str = "INSERT INTO owed_actor_compare(identity,comparedAtMs,diffCount,heldByLagCount,staleD1Count,tipLagCount,outpoints,d1Digest,actorDigest,fault,partial,lap) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12) ON CONFLICT(identity) DO UPDATE SET comparedAtMs=excluded.comparedAtMs,diffCount=excluded.diffCount,heldByLagCount=excluded.heldByLagCount,staleD1Count=excluded.staleD1Count,tipLagCount=excluded.tipLagCount,outpoints=excluded.outpoints,d1Digest=excluded.d1Digest,actorDigest=excluded.actorDigest,fault=excluded.fault,partial=excluded.partial,lap=excluded.lap";

/// FIX-1046-FOLD-1 (LOW-2): a FAULT row written over a stored row keeps that row's `outpoints` (its pending re-reads
/// and its counted differences): a fault is no read of the rows, so it erases no state; the next good read classifies
/// against the state the fault left. The counts are the fault's (none), as before. The same statement shape, no read.
/// FIX-1056-FOLD-2 (the second review's MEDIUM-1): it keeps the stored `partial` too (the rotation's offset and its
/// completed rotations): a fault is no compare, so it moves no window; a whale that faults now and then still walks
/// its whole rotation. (Every summary counts `partial` only where `fault IS NULL`.) FIX-1056-FOLD-3: and its `lap`
/// (the keyset cursor, the lap's state, [`Lap`]).
pub const COMPARE_FAULT_UPSERT_SQL: &str = "INSERT INTO owed_actor_compare(identity,comparedAtMs,diffCount,heldByLagCount,staleD1Count,tipLagCount,outpoints,d1Digest,actorDigest,fault,partial,lap) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12) ON CONFLICT(identity) DO UPDATE SET comparedAtMs=excluded.comparedAtMs,diffCount=excluded.diffCount,heldByLagCount=excluded.heldByLagCount,staleD1Count=excluded.staleD1Count,tipLagCount=excluded.tipLagCount,d1Digest=excluded.d1Digest,actorDigest=excluded.actorDigest,fault=excluded.fault";

pub fn upsert_stmt(row: &CompareRow) -> Stmt {
    let int = |n: Option<i64>| n.map_or(Bind::Null, Bind::Int);
    let text = |s: &Option<String>| s.clone().map_or(Bind::Null, Bind::Text);
    Stmt::new(
        if row.fault.is_some() {
            COMPARE_FAULT_UPSERT_SQL
        } else {
            COMPARE_UPSERT_SQL
        },
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
            Bind::Int(partial_column(row)),
            if row.partial && row.fault.is_none() {
                Bind::Text(row.lap.to_value().to_string())
            } else {
                Bind::Null
            },
        ],
    )
}

// ---------------------------------------------------------------------------------------------------------------------
// FIX-1046 (bsv-low #1046; L58's fold, record 1777): THE APPLY'S LAG, NAMED AND NEVER COUNTED.
//
// Six ring ticks inside L58 each saw one differing identity that agreed on its next read: the fresh D1 judgement and
// the actor's stored rows read inside the actor's own visibility window (the D1 write landed, the compare read before
// the actor's apply of the same event landed). THE RULE: a difference no pending mark holds and no tip explains is
// counted only once it STANDS on a read at least the visibility window ([`reread_window_ms`]) after the read that
// first saw it. Until then its outpoint is `pendingReread` (named, held, never counted either way); a pending outpoint
// that a later read no longer finds differing cleared on its re-read: the apply's lag, `heldByApply` (named on that
// read's row, counted `owed_actor_compare_held_by_apply_total`, never a diff). A difference counted once stays
// counted for as long as it differs. The state is the stored row's own outpoints (the prior row read by the ring
// before its compares, [`PRIORS_SQL`]), so the ring's queue of due re-reads (`rereads` on its ledger row) only speeds
// the re-read: an identity the queue drops, or one compared by the operator's POST, is re-read by the next
// revolution under the same rule.

/// The visibility window's floor and cap (the issue's rule: twice the served `rowsVisibleMsP95`, floored at 5 s and
/// capped at 30 s; an unmeasured p95, no sample of this build yet, is the cap: a lag is never counted for want of a
/// measurement).
pub const REREAD_WINDOW_FLOOR_MS: i64 = 5_000;
pub const REREAD_WINDOW_CAP_MS: i64 = 30_000;
/// The due re-reads one shard's ledger row keeps, at most (the rest are re-read by the next revolution).
pub const FLEET_COMPARE_REREADS_MAX: usize = 64;
/// The actor's health snapshot (`owed_actor::HEALTH_SNAPSHOT_NAME`), whose bars serve `rowsVisibleMsP95`.
pub const VISIBILITY_READ_SQL: &str =
    "SELECT cursor FROM owed_belt_state WHERE name='owed-actor:health'";
/// The prior stored rows of the identities a tick compares (`?1` a JSON array of identities).
pub const PRIORS_SQL: &str = "SELECT identity,outpoints,partial,lap FROM owed_actor_compare WHERE identity IN (SELECT value FROM json_each(?1))";

/// PURE: the visibility window from the served first-visibility p95 (twice it, floored and capped; none, the cap).
pub fn reread_window_ms(rows_visible_p95: Option<u64>) -> i64 {
    rows_visible_p95.map_or(REREAD_WINDOW_CAP_MS, |p| {
        (p.saturating_mul(2).min(i64::MAX as u64) as i64)
            .clamp(REREAD_WINDOW_FLOOR_MS, REREAD_WINDOW_CAP_MS)
    })
}

/// PURE: the window from [`VISIBILITY_READ_SQL`]'s rows: the snapshot's bars folded as `/health` folds them
/// (`owed_actor::fleet_bars`, this build's rows), its `rowsVisibleMsP95`.
pub fn reread_window_of(rows: &[Value], now: i64) -> i64 {
    let cursor: Value = rows
        .first()
        .and_then(|r| r["cursor"].as_str())
        .and_then(|c| serde_json::from_str(c).ok())
        .unwrap_or(Value::Null);
    let bars: Vec<Value> = match &cursor["bars"] {
        Value::String(s) => serde_json::from_str::<Value>(s).unwrap_or(Value::Null),
        v => v.clone(),
    }
    .as_array()
    .cloned()
    .unwrap_or_default();
    let fleet = super::fleet_bars(&bars, now.max(0) as u64, super::lib_rev());
    reread_window_ms(fleet["rowsVisibleMsP95"].as_u64())
}

impl CompareRow {
    fn flagged(&self, flag: &str) -> usize {
        self.outpoints
            .as_array()
            .into_iter()
            .flatten()
            .filter(|o| o[flag] == true)
            .count()
    }
    /// The outpoints whose difference cleared on their re-read (FIX-1046): the apply's lag.
    pub fn held_by_apply(&self) -> usize {
        self.flagged("heldByApply")
    }
    /// The outpoints whose difference waits for its re-read (FIX-1046): counted neither way.
    pub fn pending_reread(&self) -> usize {
        self.flagged("pendingReread")
    }
    /// The outpoints whose difference cleared because the D1 judge's answer moved to the actor's
    /// (FIX-1046-FOLD-1): counted, named `judgeMoved` beside the count.
    pub fn judge_moved(&self) -> usize {
        self.flagged("judgeMoved")
    }
    /// When every pending outpoint is past the window (the earliest a re-read can settle them all), or `None`.
    pub fn reread_due_ms(&self, window: i64) -> Option<i64> {
        self.outpoints
            .as_array()
            .into_iter()
            .flatten()
            .filter(|o| o["pendingReread"] == true)
            .filter_map(|o| o["seenAtMs"].as_i64())
            .max()
            .map(|s| s + window)
    }
}

/// The PENDING differences a stored row keeps state for, at most (FIX-1046-FOLD-1, LOW-1; FIX-1056-FOLD-2: pending
/// alone), beside the [`COMPARE_ROW_OUTPOINTS_MAX`] the row names with their fields. A pending difference past it has
/// no state to hold it and is COUNTED (never left pending for want of a place).
pub const COMPARE_ROW_STATE_MAX: usize = 128;
/// FIX-1056-FOLD-2 (the second review's LOW-1): the COUNTED differences a stored row keeps, at most. THE BOUND THE
/// DESIGN ACCEPTS, stated: a counted difference past it is counted on the row that sees it and not stored, so it is
/// in no list on a later partial compare that does not judge its game (a false zero needs more than 2,048 standing
/// differences on ONE identity; each judge stores at most 500 rows an identity, the judge's ceiling, so a whole
/// compare holds at most 1,000 outpoints). About 150 bytes an entry with its fields dropped: about 310 KB at the bound,
/// under D1's 2 MB row.
pub const COMPARE_ROW_COUNTED_MAX: usize = 2_048;

/// PURE (FIX-1046, folded by FIX-1046-FOLD-1): one compare's row classified against the identity's prior stored
/// outpoints (`prior`, the row's `outpoints` as stored, or none) at `now` under `window`. THE RULE: the compare names a
/// lag and counts a disagreement.
///
/// - An unheld difference ([`compare_row_of`]'s `held: false`) is COUNTED when the prior row counted it, or held it
///   `pendingReread` since a read at least `window` before `now`; otherwise it is `pendingReread` (`held: true`,
///   `seenAtMs` the first read that saw it, `d1At` and `actorAt` the two sides' prints then, [`row_print`]).
/// - A prior `pendingReread` outpoint this read finds equal on both sides cleared. WHICH SIDE MOVED decides
///   (MEDIUM-1): the actor's print changed and the D1 print did not, the ACTOR caught up with the D1 judge, the
///   apply's lag: `heldByApply` (`held: true`, named, never counted); the actor's print did not change, the D1
///   JUDGE's answer moved to meet it: `judgeMoved` (`held: false`, COUNTED, a disagreement that happened); both moved
///   (or a prior with no prints): `bothMoved`, COUNTED.
/// - The whole differing set is classified before the stored list is cut, so an outpoint is never classed by its
///   absence from a cut list; `diffCount` counts every counted difference, listed or not (LOW-1).
/// - FIX-1056 (bsv-low #1056, the second review's LOW-1): a prior `pendingReread` outpoint whose game a PARTIAL
///   compare did not judge (`judged_games`) was not read on either side: it stays `pendingReread` as stored (never a
///   clear, never counted), and the next compare judges its game first. Each pending entry carries its `gameId`.
/// - FIX-1056-FOLD-1 (the review's MEDIUM-1): THE ONE RULE, a difference the compare did not judge is never a clear. A
///   prior COUNTED outpoint absent from this read's differences is carried COUNTED and unchanged when this read did
///   not judge it (a partial compare: its stored game not judged, any side holding its row in a game not judged, or
///   no game named and no side read in a judged game); it ends only on a read that judges its game and finds both
///   sides equal. Every unheld entry carries its `gameId`. The same "not read" rule keeps a pending outpoint pending
///   (LOW-2: a row that moved to an unjudged game is not read).
/// - FIX-1056-FOLD-1 (MEDIUM-2): every cleared entry is kept past the cut (fields dropped), so the counter and the
///   served class, both read from the stored flags, see each one at any number of standing differences
///   (FIX-1056-FOLD-2, the second review's NOTE-2: one mechanism; the separate `cleared` count is gone).
/// - FIX-1056-FOLD-2 (the second review's LOW-2): under a partial compare an entry no side holds in a judged game is
///   not read either (the fresh D1 judgement may hold it in a game not judged, which no stored row shows in Phase C):
///   carried (counted, or pending) with `absentSince`, and ended only after a whole rotation more with no side holding
///   it in a judged game, or by a whole compare. Every counted entry is stored, up to [`COMPARE_ROW_COUNTED_MAX`]
///   (LOW-1).
///
/// `heldByLagCount` is unchanged (the marks and the tips). A fault row is returned as it is.
pub fn classify_reread(
    mut row: CompareRow,
    prior: Option<&Value>,
    now: i64,
    window: i64,
) -> CompareRow {
    if row.fault.is_some() {
        return row;
    }
    // FIX-COMPARE-FACTS (bsv-low #1095, ruling 3): the prior entries read by this build's verdict rule
    // ([`verdict_entry`]): an entry printed under an older rule naming facts only is no verdict difference by its own
    // record and is not carried (a carried counted entry kept its old `fields`: beta's `facts.snapshotTip` after #1094).
    let prior: Vec<Value> = prior
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(verdict_entry)
        .collect();
    let prior_of = |o: &str| prior.iter().find(|p| p["outpoint"] == o);
    let current: Vec<Value> = row.outpoints.as_array().cloned().unwrap_or_default();
    let absent = || (ABSENT_PRINT.to_string(), ABSENT_PRINT.to_string());
    let print_of = |o: &str| row.prints.get(o).cloned().unwrap_or_else(absent);
    // FIX-1056-FOLD-1: was this outpoint READ on both sides by this compare? Always on a whole compare. On a partial
    // one: not when either side holds it in a game not judged, nor when its stored game was not judged; an entry
    // naming no game (stored before FIX-1056) is read only when a side holds it in a judged game.
    let read = |key: &str, stored_game: Option<&str>| -> bool {
        let Some(judged) = &row.judged_games else {
            return true;
        };
        if row.unjudged.contains(key) {
            return false;
        }
        match stored_game {
            Some(g) => judged.contains(&g.to_ascii_lowercase()),
            None => row.games.contains_key(key),
        }
    };
    let counted_entry = is_counted_entry;
    // Counted differences (standing, then the cleared ones a judge moved), pending ones, then the named-only ones.
    let mut counted: Vec<Value> = Vec::new();
    let mut moved: Vec<Value> = Vec::new();
    let mut pending: Vec<Value> = Vec::new();
    let mut named: Vec<Value> = Vec::new();
    let mut held: Vec<Value> = Vec::new();
    for mut o in current.iter().cloned() {
        let key = o["outpoint"].as_str().unwrap_or_default().to_string();
        if o["held"] == true {
            held.push(o);
            continue;
        }
        let p = prior_of(&key);
        let was_counted = p.is_some_and(counted_entry);
        if let Some(g) = row.games.get(&key) {
            o["gameId"] = json!(g);
        }
        let pending_prior = p.filter(|p| p["pendingReread"] == true);
        let pending_since = pending_prior.and_then(|p| p["seenAtMs"].as_i64());
        if was_counted || pending_since.is_some_and(|s| s + window <= now) {
            if o.get("gameId").is_none() {
                if let Some(g) = p.and_then(|p| p["gameId"].as_str()) {
                    o["gameId"] = json!(g);
                }
            }
            counted.push(o);
        } else {
            let (d1, actor) = print_of(&key);
            o["held"] = json!(true);
            o["pendingReread"] = json!(true);
            o["seenAtMs"] = json!(pending_since.unwrap_or(now));
            o["d1At"] = pending_prior.map_or(json!(d1), |p| p["d1At"].clone());
            o["actorAt"] = pending_prior.map_or(json!(actor), |p| p["actorAt"].clone());
            pending.push(o);
        }
    }
    // FIX-1056-FOLD-2 (the second review's LOW-2): under a partial compare an entry is POSITIVELY read only when a side
    // holds its outpoint in a judged game; an entry no side holds in a judged game may sit, on the side no stored row
    // shows (the fresh D1 judgement's row in Phase C), in a game not judged. It is carried until a whole rotation has
    // passed with no side holding it in any judged game (`absentSince`, the rotations completed when that was first
    // seen; it ends at two rotations more: one full walk of the rest since), or a whole compare.
    let positive = |key: &str| row.judged_games.is_none() || row.games.contains_key(key);
    let absent_done = |p: &Value| {
        p["absentSince"]
            .as_u64()
            .is_some_and(|s| row.rotations >= s + 2)
    };
    let absent_mark = |p: &Value| -> Value {
        let mut p = p.clone();
        if !p["absentSince"].is_u64() {
            p["absentSince"] = json!(row.rotations);
        }
        p
    };
    // FIX-1056-FOLD-1 (MEDIUM-1): a prior COUNTED outpoint this read did not judge is carried counted, as stored.
    for p in prior.iter().filter(|p| counted_entry(p)) {
        let key = p["outpoint"].as_str().unwrap_or_default();
        if key.is_empty() || current.iter().any(|o| o["outpoint"] == key) {
            continue;
        }
        if !read(key, p["gameId"].as_str()) {
            let mut p = p.clone();
            if row.unjudged.contains(key) {
                // A side holds it (in a game not judged): not absent.
                p.as_object_mut().map(|m| m.remove("absentSince"));
            }
            counted.push(p);
        } else if !positive(key) && !absent_done(p) {
            counted.push(absent_mark(p));
        }
    }
    // FIX-1056 (LOW-1): the pending outpoints whose game this read did not judge (a partial compare left it out):
    // neither side was read, so nothing about them is a clear. Each stays `pendingReread` exactly as stored, and its
    // game is judged first by the next compare ([`pick_games`]). One stored before FIX-1056 names no game: under a
    // partial compare it cannot be known judged, so its state is dropped (counted neither way; a difference that still
    // stands is seen afresh when its game is judged).
    let mut unread: Vec<Value> = Vec::new();
    for p in prior.iter().filter(|p| p["pendingReread"] == true) {
        let key = p["outpoint"].as_str().unwrap_or_default();
        if current.iter().any(|o| o["outpoint"] == key) {
            continue;
        }
        if !read(key, p["gameId"].as_str()) {
            if p["gameId"].is_string() {
                unread.push(p.clone());
            }
            // An entry naming no game: under a partial compare that read no side of it, its pending state is dropped
            // (counted neither way; a difference that still stands is seen afresh when its game is judged; the
            // review's NOTE-1, transitional: only entries stored before FIX-1056).
            continue;
        }
        if !positive(key) && !absent_done(p) {
            // FIX-1056-FOLD-2 (LOW-2): no side holds it in a judged game: not read, still pending.
            unread.push(absent_mark(p));
            continue;
        }
        let (d1_now, actor_now) = print_of(key);
        let (d1_at, actor_at) = (p["d1At"].as_str(), p["actorAt"].as_str());
        let actor_moved = actor_at.is_none_or(|a| a != actor_now);
        let judge_moved = d1_at.is_none_or(|d| d != d1_now);
        let mut e = json!({
            "outpoint": key,
            "seenAtMs": p["seenAtMs"],
            "fields": p["fields"],
        });
        if actor_moved && !judge_moved {
            e["held"] = json!(true);
            e["heldByApply"] = json!(true);
            named.push(e);
        } else {
            e["held"] = json!(false);
            e[if actor_moved {
                "bothMoved"
            } else {
                "judgeMoved"
            }] = json!(true);
            moved.push(e);
        }
    }
    // FIX-1056-FOLD-2 (the second review's LOW-1): the PENDING state is bounded ([`COMPARE_ROW_STATE_MAX`]); a pending
    // difference past it has no place and is counted (never pending for want of one). Every COUNTED entry is kept, up
    // to [`COMPARE_ROW_COUNTED_MAX`] (fields dropped past the [`COMPARE_ROW_OUTPOINTS_MAX`] named): a counted entry
    // not stored would vanish from the next partial compare that does not judge its game.
    let mut kept_pending: Vec<Value> = Vec::new();
    for mut o in pending {
        if kept_pending.len() < COMPARE_ROW_STATE_MAX {
            kept_pending.push(o);
        } else {
            for k in ["pendingReread", "seenAtMs", "d1At", "actorAt"] {
                o.as_object_mut().map(|m| m.remove(k));
            }
            o["held"] = json!(false);
            counted.push(o);
        }
    }
    // FIX-1056: an unread pending outpoint keeps its state while there is room; past the bound it is dropped, never
    // counted (it was not read).
    for o in unread {
        if kept_pending.len() < COMPARE_ROW_STATE_MAX {
            kept_pending.push(o);
        }
    }
    let mut diff_count = (counted.len() + moved.len()) as i64;
    let mut out: Vec<Value> = Vec::new();
    let mut kept_counted = 0usize;
    let mut overflow_now = 0i64;
    let counted_len = counted.len();
    for (i, o) in counted
        .into_iter()
        .chain(kept_pending)
        .chain(moved)
        .chain(named)
        .chain(held)
        .enumerate()
    {
        let is_counted = i < counted_len;
        // A pending entry (bounded above) and a cleared entry (at most the prior's pending entries, so at most
        // [`COMPARE_ROW_STATE_MAX`]) are classes the row serves: kept past the cut, fields dropped (FIX-1056-FOLD-1,
        // MEDIUM-2). A held-by-lag or tip-lag entry is named only within the cut, as before.
        let kept_past = o["pendingReread"] == true
            || o["heldByApply"] == true
            || o["judgeMoved"] == true
            || o["bothMoved"] == true;
        if out.len() < COMPARE_ROW_OUTPOINTS_MAX {
            kept_counted += usize::from(is_counted);
            out.push(o);
        } else if (is_counted && kept_counted < COMPARE_ROW_COUNTED_MAX) || kept_past {
            kept_counted += usize::from(is_counted);
            let mut o = o;
            o["fields"] = Value::Null;
            out.push(o);
        } else if is_counted {
            overflow_now += 1;
        }
    }
    if row.partial {
        // FIX-1056-FOLD-3 (review 3's NOTE-2): a counted difference past [`COMPARE_ROW_COUNTED_MAX`] is in no list, so
        // a later partial compare that does not judge its game would not carry it. Their count is carried in the lap
        // (`overflow`) and added to `diffCount` on every compare that overflows nothing, until a lap ends in which no
        // compare overflowed (every game judged with room to store what still stands). An overflowed difference judged
        // again inside the lap may be counted twice meanwhile: the count fails closed, it never reads 0 while one may
        // stand.
        let lap = &mut row.lap;
        if overflow_now > 0 {
            // FIX-1056-FOLD-4 (review 4's NOTE-1): a second overflow in one lap adds to the first, never replaces it
            // (each may name games the other's compare did not judge; a sum fails closed).
            lap.overflow = if lap.overflow_this_lap {
                lap.overflow.saturating_add(overflow_now)
            } else {
                overflow_now
            };
            lap.overflow_this_lap = true;
        } else {
            if row.wrapped && !lap.overflow_this_lap {
                lap.overflow = 0;
            }
            diff_count += lap.overflow;
        }
        // FIX-1056-FOLD-3 (review 3's LOW-2): the laps read zero throughout. Any counted or pending difference resets
        // the count and dirties the lap; a lap that ends clean counts one; the next lap starts as this compare read.
        let clean_now = diff_count == 0 && !out.iter().any(|o| o["pendingReread"] == true);
        if !clean_now {
            lap.lap_clean = false;
            row.zero_held_laps = 0;
        }
        if row.wrapped {
            if lap.lap_clean {
                row.zero_held_laps += 1;
            }
            lap.lap_clean = clean_now;
            lap.overflow_this_lap = overflow_now > 0;
        }
    } else {
        row.lap = Lap::default();
        row.zero_held_laps = 0;
    }
    row.diff_count = Some(diff_count);
    row.outpoints = json!(out);
    row
}

/// PURE: the prior stored outpoints from [`PRIORS_SQL`]'s rows (the row's `outpoints` as stored), or none.
fn prior_of(rows: &[Value]) -> Option<Value> {
    rows.first()
        .and_then(|r| r["outpoints"].as_str())
        .and_then(|s| serde_json::from_str(s).ok())
}

/// FIX-1046: one compare's row classified ([`classify_reread`]). A fault row is returned as it is and reads nothing;
/// any other row classifies against the prior [`fresh_compare`] read in its own batch (FIX-1056), or, a row without
/// one, first reads the identity's prior stored row, one statement ([`PRIORS_SQL`]), so a tick whose compares fault (no
/// actor bound, not bootstrapped) costs no statement more than before.
async fn classified(
    db: &impl SqlPort,
    raw: CompareRow,
    now: i64,
    window: i64,
) -> Result<CompareRow, String> {
    if raw.fault.is_some() {
        return Ok(raw);
    }
    if let Some(prior) = raw.prior.clone() {
        return Ok(classify_reread(raw, Some(&prior), now, window));
    }
    let rows = db
        .run_batch(vec![Stmt::new(
            PRIORS_SQL,
            vec![Bind::Text(json!([raw.identity]).to_string())],
        )])
        .await?
        .into_iter()
        .next()
        .map(|o| o.rows)
        .unwrap_or_default();
    let prior = prior_of(&rows);
    Ok(classify_reread(raw, prior.as_ref(), now, window))
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
/// FIX-1046-FOLD-1 (LOW-3): the statements one tick's due re-reads may spend on a tick the forward walk runs (half the
/// tick's [`FLEET_COMPARE_STATEMENTS_PER_TICK`]); served on the standing route (`rereadStatementsMax`) and the tick's
/// line. A resting tick's re-reads take the whole budget (no forward walk to share it with).
pub const FLEET_COMPARE_REREAD_STATEMENTS: usize = 400;
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
    // FIX-TIER-RING: the shards' transient tick faults by kind, summed.
    let mut tick_faults = serde_json::Map::new();
    for s in &shards {
        if let Some(o) = s["tickFaults"].as_object() {
            for (k, v) in o {
                let n = tick_faults.get(k).and_then(Value::as_i64).unwrap_or(0)
                    + v.as_i64().unwrap_or(0);
                tick_faults.insert(k.clone(), json!(n));
            }
        }
    }
    json!({
        "tickFaults": tick_faults,
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
            "tickFaults": s["tickFaults"],
            "lastFault": s["lastFault"],
        })).collect::<Vec<_>>(),
    })
}

/// FIX-998-SERVE (bsv-low #998): the public counts' snapshot write (`owed_events_publish::FLEET_COMPARE_HEALTH_SQL`),
/// carried by every tick's last batch: rewritten at most once a minute fleet-wide, `force`d at a shard's wrap; the
/// shards' rows named as [`ring_state_name`] names them for `shards`.
pub fn fleet_health_stmt(now: i64, shards: usize, force: bool) -> Stmt {
    let shards = shards.max(1);
    let pattern = if shards <= 1 {
        RING_NAME.to_string()
    } else {
        format!("{RING_NAME}:%/{shards}")
    };
    Stmt::new(
        crate::owed_events_publish::FLEET_COMPARE_HEALTH_SQL,
        vec![
            Bind::Int(now),
            Bind::Int(if force {
                0
            } else {
                crate::owed_events_publish::FLEET_COMPARE_HEALTH_EVERY_MS
            }),
            Bind::Int(shards as i64),
            Bind::Text(ring_state_name(0, shards)),
            Bind::Text(pattern),
        ],
    )
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
    /// FIX-TIER-RING: the kind of a transient D1 fault that ended the tick ([`transient_fault_kind`]), `stopped` then
    /// "busy"; counted on the shard's ring row (`tickFaults.<kind>`) and backed off by the alarm ([`busy_backoff_ms`]).
    pub fault_kind: Option<&'static str>,
    /// FIX-1046: the identities this tick RE-READ from the shard's queue (also in `compared`), the outpoints whose
    /// difference cleared on its re-read (`heldByApply`, never a diff), and the identities this tick left
    /// `pendingReread` (their difference not yet counted either way).
    pub rereads: Vec<String>,
    pub held_by_apply: usize,
    pub pending_reread: usize,
    /// FIX-1046-FOLD-1: the outpoints this tick counted because the D1 judge's answer moved to the actor's.
    pub judge_moved: usize,
    /// FIX-1046-FOLD-1 (LOW-3): the statements this tick's re-reads spent, and whether the re-reads yielded the tick
    /// to the forward walk (the previous tick's re-reads left it nothing).
    pub reread_statements: usize,
    pub rereads_yielded: bool,
    /// The visibility window this tick re-read under ([`reread_window_ms`]).
    pub window_ms: i64,
}

/// FIX-TIER-RING (bsv-low #998's follow-up): a D1 fault a later tick can outlive, by its kind: `busy` for the local
/// SQLite's lock (`SQLITE_BUSY`, "database is locked": `wrangler dev`'s D1, which Cloudflare's serialised D1 never
/// answers), `transient` for D1's own retryable faults (overloaded, a lost connection, a storage timeout, a reset).
/// Anything else is no transient fault (`None`): the tick's `Err` as before.
pub fn transient_fault_kind(e: &str) -> Option<&'static str> {
    let l = e.to_ascii_lowercase();
    if l.contains("sqlite_busy") || l.contains("database is locked") {
        Some("busy")
    } else if l.contains("overloaded")
        || l.contains("network connection lost")
        || l.contains("storage operation exceeded timeout")
        || l.contains("storage caused object to be reset")
        || l.contains("internal error in d1")
    {
        Some("transient")
    } else {
        None
    }
}

/// The most consecutive busy ticks the back-off doubles over: the shard's next alarm after its `n`th consecutive busy
/// tick ([`busy_backoff_ms`]) is [`FLEET_COMPARE_TICK_MS`] doubled up to this many times (10 s to 160 s).
pub const FLEET_COMPARE_BUSY_BACKOFF_DOUBLINGS: u32 = 4;

/// PURE: the shard's next alarm after its `n`th consecutive busy tick (`n` >= 1): the tick doubled per busy tick,
/// bounded at 2^[`FLEET_COMPARE_BUSY_BACKOFF_DOUBLINGS`] ticks; `n` 0 is the plain tick.
pub fn busy_backoff_ms(n: u32) -> i64 {
    FLEET_COMPARE_TICK_MS << n.min(FLEET_COMPARE_BUSY_BACKOFF_DOUBLINGS)
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
    let result = ring_tick_inner(db, now, shard, shards, per_tick, elapsed, comparer).await;
    let e = match result {
        Ok(r) => return Ok(r),
        Err(e) => e,
    };
    let Some(kind) = transient_fault_kind(&e) else {
        return Err(e);
    };
    // FIX-TIER-RING: a transient fault ends the tick, never the shard: the identities written so far keep their rows
    // and the cursor (each written with its row), the fault is counted on the shard's ring row (best effort: the
    // count's own write may meet the same lock, and is then lost), and the alarm backs off (`busy_backoff_ms`).
    let shards = shards.max(1);
    let name = ring_state_name(shard, shards);
    let count = async {
        let mut ring = db
            .run_batch(vec![Stmt::new(
                RING_READ_SQL,
                vec![Bind::Text(name.clone())],
            )])
            .await?
            .into_iter()
            .next()
            .and_then(|o| o.rows.into_iter().next())
            .and_then(|r| {
                r["cursor"]
                    .as_str()
                    .and_then(|c| serde_json::from_str::<Value>(c).ok())
            })
            .filter(Value::is_object)
            .unwrap_or_else(|| json!({"after": "", "revolution": 0, "revolutionStartedAtMs": now}));
        let n = ring["tickFaults"][kind].as_i64().unwrap_or(0) + 1;
        if !ring["tickFaults"].is_object() {
            ring["tickFaults"] = json!({});
        }
        ring["tickFaults"][kind] = json!(n);
        ring["lastFault"] =
            json!({"kind": kind, "atMs": now, "error": e.chars().take(160).collect::<String>()});
        ring["lastTickAt"] = json!(now);
        db.run_batch(vec![Stmt::new(
            crate::owed_belt::STATE_SQL,
            vec![
                Bind::Text(name.clone()),
                Bind::Text(ring.to_string()),
                Bind::Int(now),
            ],
        )])
        .await
        .map(|_| ())
    };
    if let Err(c) = count.await {
        db.log(&format!(
            "[owed-fleet-compare] shard {shard}/{shards} {kind} fault not counted: {c}"
        ));
    }
    Ok(RingReport {
        faults: 1,
        stopped: Some("busy"),
        fault_kind: Some(kind),
        ..RingReport::default()
    })
}

async fn ring_tick_inner<J: JudgeDb>(
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
            Stmt::new(VISIBILITY_READ_SQL, vec![]),
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
    // FIX-1046: the actor's visibility window, from the snapshot `/health` serves.
    let window = reread_window_of(&read.next().map(|o| o.rows).unwrap_or_default(), now);
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
    let mut report = RingReport {
        window_ms: window,
        ..RingReport::default()
    };
    ring["lastTickAt"] = json!(now);
    // FIX-1046: the shard's queue of re-reads, `[{identity, dueAtMs}]` by due time.
    let mut queue: Vec<(String, i64)> = ring["rereads"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|e| {
            Some((
                e["identity"].as_str()?.to_string(),
                json_ms(&e["dueAtMs"])? as i64,
            ))
        })
        .collect();
    let mut largest = FLEET_COMPARE_IDENTITY_RESERVE;
    let rest_until = ring["restUntilMs"].as_i64();
    let resting = rest_until.is_some_and(|r| now < r);
    // FIX-1046-FOLD-1 (LOW-3): a tick whose re-reads left the forward walk nothing (the previous tick's
    // `forwardOwed`) takes no re-read: the forward walk goes first, the queue waits one tick.
    let forward_owed = !resting && ring["forwardOwed"] == true;
    ring.as_object_mut().map(|o| o.remove("forwardOwed"));
    report.rereads_yielded = forward_owed;
    if per_tick > 0 && !forward_owed {
        // The due re-reads first, resting or not, each inside the tick's budget and wall: one the tick cannot afford
        // stays queued (its row `pendingReread`, counted neither way) for the next tick. FIX-1046-FOLD-1 (LOW-3): on a
        // tick the forward walk runs (not resting), the re-reads spend at most [`FLEET_COMPARE_REREAD_STATEMENTS`]
        // (no re-read is started past it by the largest compare's estimate), so a storm of re-reads never holds the
        // forward walk still.
        let due: Vec<String> = queue
            .iter()
            .filter(|(_, d)| *d <= now)
            .map(|(i, _)| i.clone())
            .collect();
        for id in &due {
            let used = m.statements.get();
            if used + largest > FLEET_COMPARE_STATEMENTS_PER_TICK {
                report.stopped = Some("statements");
                break;
            }
            if !resting && report.reread_statements + largest > FLEET_COMPARE_REREAD_STATEMENTS {
                break;
            }
            if elapsed() > FLEET_COMPARE_WALL_MS {
                report.stopped = Some("wall");
                break;
            }
            let raw = comparer.compare(&m, id, now).await;
            let row = classified(&m, raw, now, window).await?;
            ring_account(&mut report, &row, &mut queue, window, now);
            ring["rereads"] = queue_json(&queue);
            m.run_batch(vec![upsert_stmt(&row), state(&name, &ring)])
                .await?;
            held_by_apply_counted(&m, &row).await;
            let spent = m.statements.get() - used;
            report.reread_statements += spent;
            largest = largest.max(spent);
            report.rereads.push(id.clone());
            report.compared.push(id.clone());
        }
    }
    if per_tick == 0 || resting || report.stopped.is_some() {
        if !resting
            && !report.rereads.is_empty()
            && matches!(report.stopped, Some("statements") | Some("wall"))
        {
            // FIX-1046-FOLD-1 (LOW-3): the re-reads spent the tick; the forward walk goes first next tick.
            // FIX-1056 (the second review's LOW-2): a stop on the WALL inside the re-reads owes it exactly as a stop on
            // the statements does (slow re-reads, an actor out of reach, are bounded by the wall alone).
            ring["forwardOwed"] = json!(true);
        }
        if report.stopped.is_none() {
            report.stopped = Some(if per_tick == 0 { "paused" } else { "resting" });
        }
        m.run_batch(vec![
            state(&name, &ring),
            fleet_health_stmt(now, shards, false),
        ])
        .await?;
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
    let mut all_done = true;
    let mut forward = 0usize;
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
        // A key with no actor (not 66-hex) is passed by the cursor with no row; one this tick re-read already holds
        // this tick's row (FIX-1046).
        if actor_name(id).is_none() || report.rereads.contains(id) {
            cursor = id.clone();
            continue;
        }
        let raw = comparer.compare(&m, id, now).await;
        let row = classified(&m, raw, now, window).await?;
        ring_account(&mut report, &row, &mut queue, window, now);
        ring["rereads"] = queue_json(&queue);
        cursor = id.clone();
        ring["after"] = json!(cursor);
        m.run_batch(vec![upsert_stmt(&row), state(&name, &ring)])
            .await?;
        held_by_apply_counted(&m, &row).await;
        largest = largest.max(m.statements.get() - used);
        report.compared.push(id.clone());
        forward += 1;
    }
    // FIX-1046-FOLD-1 (LOW-3): re-reads that left the forward walk no compare on a page it had owe it the next tick.
    if !report.rereads.is_empty() && forward == 0 && !all_done {
        ring["forwardOwed"] = json!(true);
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
    // FIX-998-SERVE: the public counts, folded after this tick's rows (forced at a wrap, else once a minute).
    end.push(fleet_health_stmt(now, shards, report.wrapped));
    m.run_batch(end).await?;
    report.cursor = cursor;
    report.statements = m.statements.get();
    Ok(report)
}

/// FIX-1046: one classified row into the tick's account and the shard's queue (the identity's old entry dropped, a
/// new one at its due time while it holds a pending outpoint, at most [`FLEET_COMPARE_REREADS_MAX`] queued).
fn ring_account(
    report: &mut RingReport,
    row: &CompareRow,
    queue: &mut Vec<(String, i64)>,
    window: i64,
    now: i64,
) {
    if row.diff_count.is_some_and(|n| n > 0) {
        report.diffs += 1;
    }
    if row.fault.is_some() {
        report.faults += 1;
        // FIX-1046-FOLD-1 (LOW-2): a fault keeps the stored state (`COMPARE_FAULT_UPSERT_SQL`), so a queued re-read
        // stays queued, a window later (the stored row still holds its pending outpoints).
        if let Some(e) = queue.iter_mut().find(|(i, _)| *i == row.identity) {
            e.1 = now + window;
            queue.sort_by_key(|(_, d)| *d);
        }
        return;
    }
    report.held_by_apply += row.held_by_apply();
    report.judge_moved += row.judge_moved();
    queue.retain(|(i, _)| *i != row.identity);
    if let Some(due) = row.reread_due_ms(window) {
        report.pending_reread += 1;
        if queue.len() < FLEET_COMPARE_REREADS_MAX {
            queue.push((row.identity.clone(), due));
            queue.sort_by_key(|(_, d)| *d);
        }
    }
}

fn queue_json(queue: &[(String, i64)]) -> Value {
    json!(queue
        .iter()
        .map(|(i, d)| json!({"identity": i, "dueAtMs": d}))
        .collect::<Vec<_>>())
}

/// `owed_actor_compare_held_by_apply_total` and `owed_actor_compare_judge_moved_total` (FIX-1056, the second review's
/// NOTE-1: a judge-side clear lives one revolution on its row; the counter keeps it): one identity's cleared outpoints
/// by side, written as its row is written (FIX-1046-FOLD-1, LOW-4: a tick that dies after a row keeps that row's
/// counts; none when none). Best effort, as every counter.
async fn held_by_apply_counted(m: &impl SqlPort, row: &CompareRow) {
    let counts: Vec<(String, u64)> = [
        (super::COUNTERS[35], row.held_by_apply()),
        (super::COUNTERS[36], row.judge_moved()),
    ]
    .into_iter()
    .filter(|(_, n)| *n > 0)
    .map(|(c, n)| (c.to_string(), n as u64))
    .collect();
    if !counts.is_empty() {
        m.counters(counts).await;
    }
}

// ---------------------------------------------------------------------------------------------------------------------
// The standing route (`GET` / `POST /internal/owed-actor-compare`).

/// The page of compare rows the standing route serves, at most.
pub const COMPARE_PAGE: usize = 64;
/// The fleet's counts over every stored compare row, one statement (the overlay's `/health/invariants` reads the same
/// aggregate in `owed_events::HEALTH_STATE_SQL`).
pub const COMPARE_SUMMARY_SQL: &str = "SELECT COUNT(*) AS identities,COALESCE(SUM(diffCount>0),0) AS diffs,COALESCE(SUM(heldByLagCount>0),0) AS heldByLag,COALESCE(SUM((fault IS NULL AND json_valid(outpoints) AND EXISTS(SELECT 1 FROM json_each(outpoints) WHERE json_extract(value,'$.heldByApply')=1))),0) AS heldByApply,COALESCE(SUM((fault IS NULL AND json_valid(outpoints) AND EXISTS(SELECT 1 FROM json_each(outpoints) WHERE json_extract(value,'$.pendingReread')=1))),0) AS pendingReread,COALESCE(SUM((fault IS NULL AND json_valid(outpoints) AND EXISTS(SELECT 1 FROM json_each(outpoints) WHERE json_extract(value,'$.judgeMoved')=1))),0) AS judgeMoved,COALESCE(SUM(staleD1Count),0) AS staleD1,COALESCE(SUM(fault IS NOT NULL),0) AS faults,COALESCE(SUM(fault IS NULL AND partial>0),0) AS partial,COALESCE(SUM(fault IS NULL AND partial>0 AND CAST(partial AS INTEGER)<=4294967296),0) AS partialUnrotated,MIN(CASE WHEN fault IS NULL AND partial>0 THEN (CAST(partial AS INTEGER)-1)/4294967296 END) AS partialRotationsMin,MIN(CASE WHEN fault IS NULL AND partial>0 THEN (CAST(partial AS INTEGER)-1)%4294967296 END) AS partialZeroHeldLapsMin,MIN(comparedAtMs) AS oldest,MAX(comparedAtMs) AS newest FROM owed_actor_compare";
/// The rows worth naming past `?1` (a difference, a held one or a fault), `?2` a page; `?3` one identity or ''.
pub const COMPARE_PAGE_SQL: &str = "SELECT identity,comparedAtMs,diffCount,heldByLagCount,staleD1Count,tipLagCount,outpoints,d1Digest,actorDigest,fault,partial,lap FROM owed_actor_compare WHERE identity>?1 AND (?3='' OR identity=?3) AND (diffCount>0 OR heldByLagCount>0 OR fault IS NOT NULL OR (fault IS NULL AND json_valid(outpoints) AND EXISTS(SELECT 1 FROM json_each(outpoints) WHERE json_extract(value,'$.heldByApply')=1)) OR (fault IS NULL AND json_valid(outpoints) AND EXISTS(SELECT 1 FROM json_each(outpoints) WHERE json_extract(value,'$.pendingReread')=1)) OR ?3<>'') ORDER BY identity LIMIT ?2";

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
    // FIX-1046: an outpoint's class: `diff` (counted), `heldByApply`, `pendingReread`, else `heldByLag`.
    let class_of = |o: &Value| -> &'static str {
        if o["heldByApply"] == true {
            "heldByApply"
        } else if o["pendingReread"] == true {
            "pendingReread"
        } else if o["held"] == true {
            "heldByLag"
        } else {
            "diff"
        }
    };
    let entry = |r: &Value, count_key: &str, count: i64, class: &str| -> Value {
        let ops: Vec<Value> = r["outpoints"]
            .as_str()
            .and_then(|s| serde_json::from_str::<Value>(s).ok())
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default()
            .into_iter()
            .filter(|o| class_of(o) == class)
            .collect();
        json!({
            "identity": r["identity"],
            "comparedAtMs": json_ms(&r["comparedAtMs"]),
            count_key: count,
            "staleD1": n(&r["staleD1Count"]),
            "tipLag": n(&r["tipLagCount"]),
            "partial": n(&r["partial"]) != 0,
            "rotation": (n(&r["partial"]) != 0).then(|| {
                let lap = Lap::of_column(&r["lap"]);
                json!({
                    "after": lap.after,
                    "completed": rotations_of_partial(&r["partial"]),
                    "zeroHeldLaps": zero_held_laps_of_partial(&r["partial"]),
                    "lapClean": lap.lap_clean,
                    "overflow": lap.overflow,
                })
            }),
            "digests": {"d1": r["d1Digest"], "actor": r["actorDigest"]},
            "outpoints": ops,
        })
    };
    let mut diffs = Vec::new();
    let mut held = Vec::new();
    let mut by_apply = Vec::new();
    let mut pending = Vec::new();
    let mut faults = Vec::new();
    for r in page {
        if let Some(f) = r["fault"].as_str() {
            faults.push(json!({"identity": r["identity"], "comparedAtMs": json_ms(&r["comparedAtMs"]), "fault": f}));
            continue;
        }
        if n(&r["diffCount"]) > 0 {
            diffs.push(entry(r, "diffCount", n(&r["diffCount"]), "diff"));
        }
        if n(&r["heldByLagCount"]) > 0 {
            held.push(entry(
                r,
                "heldByLagCount",
                n(&r["heldByLagCount"]),
                "heldByLag",
            ));
        }
        for (class, list) in [
            ("heldByApply", &mut by_apply),
            ("pendingReread", &mut pending),
        ] {
            let e = entry(r, &format!("{class}Count"), 0, class);
            let k = e["outpoints"].as_array().map_or(0, Vec::len) as i64;
            if k > 0 {
                let mut e = e;
                e[format!("{class}Count")] = json!(k);
                list.push(e);
            }
        }
    }
    let next = (page.len() == COMPARE_PAGE)
        .then(|| page.last().and_then(|r| r["identity"].as_str()))
        .flatten();
    let mut answer = json!({
        "identities": n(&summary["identities"]),
        "identitiesDiffering": n(&summary["diffs"]),
        "identitiesHeldByLag": n(&summary["heldByLag"]),
        // FIX-1046: named, never counted (a difference that cleared on its re-read; one waiting for it).
        "identitiesHeldByApply": n(&summary["heldByApply"]),
        "identitiesPendingReread": n(&summary["pendingReread"]),
        // FIX-1046-FOLD-1: counted in `identitiesDiffering`, named: a difference that cleared because the D1 judge's
        // answer moved (each such outpoint `judgeMoved` in `diffs`).
        "identitiesJudgeMoved": n(&summary["judgeMoved"]),
        // FIX-1056-FOLD-1 (the review's LOW-1): the identities whose last compare did not judge every game (a
        // partial compare: its zero is over the games judged; the rotation judges the rest on its next compares).
        "identitiesPartial": n(&summary["partial"]),
        // FIX-1056-FOLD-2 (the second review's item 3): the rotation's completion (each row's `rotation` too).
        "identitiesPartialUnrotated": n(&summary["partialUnrotated"]),
        "partialRotationsMin": json_ms(&summary["partialRotationsMin"]),
        // FIX-1056-FOLD-3 (review 3's LOW-2): the fewest laps a partial identity has read zero throughout.
        "partialZeroHeldLapsMin": json_ms(&summary["partialZeroHeldLapsMin"]),
        "rereadStatementsMax": FLEET_COMPARE_REREAD_STATEMENTS,
        "comparedWithinMs": json_ms(&summary["oldest"]).map(|o| now - o as i64),
        "newestComparedAtMs": json_ms(&summary["newest"]),
        "staleD1": n(&summary["staleD1"]),
        "faultCount": n(&summary["faults"]),
        "diffs": diffs,
        "heldByLag": held,
        "heldByApply": by_apply,
        "pendingReread": pending,
        "faults": faults,
        "ringCursor": ring["after"],
        "revolution": ring["revolution"],
        "lastRevolutionMs": ring["lastRevolutionMs"],
        "lastTickAt": ring["lastTickAt"],
        "shardCount": ring["shardCount"],
        "shards": ring["shards"],
        "tickFaults": ring["tickFaults"],
        "next": next,
    });
    if narrowed {
        answer["row"] = page.first().map_or(Value::Null, |r| {
            let mut e = entry(r, "diffCount", n(&r["diffCount"]), "diff");
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
///
/// FIX-1046: the row is classified against the identity's prior stored row under the served visibility window
/// ([`classify_reread`]): a difference this POST first sees is `pendingReread` (no queue entry: the ring's next
/// revolution re-reads it under the same rule); a pending one past the window that stands is counted, one that cleared
/// is `heldByApply`.
pub async fn compare_now(db: &impl JudgeDb, identity: &str, now: i64) -> Result<Value, String> {
    let (raw, view) = compare_one(db, identity, now).await;
    // FIX-1046: a fault row is written as it is; any other reads the window and its prior row in one batch.
    let (row, window) = if raw.fault.is_some() {
        (raw, None)
    } else {
        // FIX-1056: the prior [`fresh_compare`] read in its own batch is used; a row without one reads it here.
        let mut stmts = vec![Stmt::new(VISIBILITY_READ_SQL, vec![])];
        if raw.prior.is_none() {
            stmts.push(Stmt::new(
                PRIORS_SQL,
                vec![Bind::Text(json!([raw.identity]).to_string())],
            ));
        }
        let mut read = db.run_batch(stmts).await?.into_iter().map(|o| o.rows);
        let window = reread_window_of(&read.next().unwrap_or_default(), now);
        let prior = match raw.prior.clone() {
            Some(p) => Some(p),
            None => prior_of(&read.next().unwrap_or_default()),
        };
        (
            classify_reread(raw, prior.as_ref(), now, window),
            Some(window),
        )
    };
    db.run_batch(vec![upsert_stmt(&row)]).await?;
    held_by_apply_counted(db, &row).await;
    Ok(json!({
        "identity": row.identity,
        "comparedAtMs": row.compared_at_ms,
        "diffCount": row.diff_count,
        "heldByLagCount": row.held_by_lag_count,
        "staleD1Count": row.stale_d1_count,
        "tipLagCount": row.tip_lag_count,
        "heldByApplyCount": row.held_by_apply(),
        "pendingRereadCount": row.pending_reread(),
        "judgeMovedCount": row.judge_moved(),
        "windowMs": window,
        "outpoints": row.outpoints,
        "fault": row.fault,
        "partial": row.partial,
        "view": view,
    }))
}
