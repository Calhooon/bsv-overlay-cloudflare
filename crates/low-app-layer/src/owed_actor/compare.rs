//! bsv-low #986 (the owner's ruling of record 1565): THE FLEET COMPARE AS A STANDING OPERATOR SURFACE.
//!
//! One function compares one identity ([`compare_identity`]): D1's rows (stored, or the D1 judge's FRESH rows of every
//! game of the identity, [`crate::owed_judge::fresh_rows`], which writes nothing, so it runs in Phase C) against the
//! actor's stored rows, through the rows lever's own [`rows_view`] and [`held_by_lag`]. Three callers, never a copy:
//! - `GET /internal/owed-actor-rows?identity=` (stored D1 rows; `&fresh=1` the fresh judgement);
//! - THE RING ([`ring_step`]): the `BOARD_VIEW` actor named [`COMPARE_ACTOR`] runs one step every
//!   [`COMPARE_TICK_MS`] from its own alarm (its own invocation, its own D1 bound), armed by the scheduled pass on
//!   every cron (the watchdog, as the belt's); a page of the identities `GET /internal/owed-actor-identities` reads
//!   ([`RESET_IDENTITIES_SQL`]) past its cursor, each compared fresh and its row written to `owed_actor_compare`
//!   with the cursor in one batch, so a tick the platform ends keeps what it finished;
//! - `POST /internal/owed-actor-compare {"identity": ..}`, the operator's hand lever: one identity now.
//!
//! `GET /internal/owed-actor-compare` serves the fleet's summary from the table ([`summary_body`]), and the overlay
//! serves its counts on `/health/invariants.owedActor.fleetCompare`.
use super::object::RESET_IDENTITIES_SQL;
use super::{
    actor_name, held_by_lag, json_ms, rejudge_state_of, rows_view, D1_JUDGED_UNDER_SQL,
    LEVER_PENDING_SQL, RECONCILE_D1_ROWS_SQL,
};
use crate::owed_judge::JudgeDb;
use crate::pot_hook::{Bind, Stmt, StmtOut};
use serde_json::{json, Value};
use std::collections::BTreeSet;

/// The ring's actor: a `BOARD_VIEW` instance of its own (not the belt's), so the compare's D1 statements are never
/// counted in the belt alarm's invocation.
pub const COMPARE_ACTOR: &str = "owed-compare:v1";
/// The storage key that makes a `BOARD_VIEW` instance the ring's (its alarm runs [`ring_step`]).
pub const COMPARE_STORAGE_KEY: &str = "owed-compare";
/// The ring's cadence: one step a minute, so beta's 153 identities at [`COMPARE_IDENTITIES_PER_TICK`] are covered
/// whole in about ten minutes. (The app layer's cron is `*/15`; at one step a cron a revolution was 150 minutes.)
pub const COMPARE_TICK_MS: i64 = 60_000;
/// The identities one step compares, by default (`OWED_FLEET_COMPARE_PER_TICK`, 0 pauses the ring, at most
/// [`COMPARE_IDENTITIES_PER_TICK_MAX`]).
pub const COMPARE_IDENTITIES_PER_TICK: usize = 16;
pub const COMPARE_IDENTITIES_PER_TICK_MAX: usize = 64;
/// The games one step judges fresh, at most (an identity is never split: one that does not fit waits for the next
/// step, unless it is the step's first, which is compared whole up to [`COMPARE_GAMES_MAX`]).
/// Measured in the budget tier (`owed-compare-ring`): a fresh judgement is about 5.3 D1 statements a game (the
/// hydration's reads), so 128 games are about 680 statements, under D1's 1,000 an invocation.
pub const COMPARE_GAMES_PER_TICK: usize = 128;
/// The games one identity's compare judges, at most, in game order; past it the compare reads the judged games' rows
/// alone on both sides (`gamesCut` names how many were left out), and the ring's window ROTATES by revolution
/// (window `w` starts at game `w * 128` modulo the identity's games, wrapping), so a bigger identity is covered whole
/// over `ceil(games / 128)` revolutions. The budget tier measured the lived-in identity's 271 games at 1,440
/// statements in one compare, past D1's 1,000 an invocation; 128 is about 680.
pub const COMPARE_GAMES_MAX: usize = 128;
/// The identity's games read to place the window, at most.
pub const COMPARE_GAMES_READ_MAX: usize = 4_096;
/// A step starts no identity after this long (the actor's wall and each read's await are inside it).
pub const COMPARE_TICK_WALL_MS: u64 = 20_000;
/// The differing outpoints a row keeps.
pub const COMPARE_OUTPOINTS_MAX: usize = 32;
/// The differing identities one summary page serves.
pub const SUMMARY_PAGE: usize = 64;
/// The ring's state row in the belt's ledger.
pub const RING_STATE_NAME: &str = "owed-actor:compare-ring";
/// The overlay migration that creates `owed_actor_compare` is the 474th; the ring writes nothing before it.
pub const COMPARE_MIGRATION_COUNT: u32 = 474;

/// The ring's state and the certified migration count, one batch.
pub const RING_STATE_SQL: &str =
    "SELECT cursor,tickedAtMs FROM owed_belt_state WHERE name='owed-actor:compare-ring'";
/// One identity's games in game order (`?2` an empty cursor, `?3` the bound plus one: the cut is seen).
pub const COMPARE_GAMES_SQL: &str = crate::owed_judge::IDENTITY_GAMES_BY_ID_SQL;
/// One identity's compare row.
pub const ROW_UPSERT_SQL: &str = "INSERT INTO owed_actor_compare(identity,comparedAtMs,diffCount,heldByLagCount,tipLagCount,outpoints,d1Digest,actorDigest,games,gamesCut,error) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11) ON CONFLICT(identity) DO UPDATE SET comparedAtMs=excluded.comparedAtMs,diffCount=excluded.diffCount,heldByLagCount=excluded.heldByLagCount,tipLagCount=excluded.tipLagCount,outpoints=excluded.outpoints,d1Digest=excluded.d1Digest,actorDigest=excluded.actorDigest,games=excluded.games,gamesCut=excluded.gamesCut,error=excluded.error";
/// At a revolution's end: the rows no compare of the revolution wrote (an identity that left the registry).
pub const PRUNE_SQL: &str = "DELETE FROM owed_actor_compare WHERE comparedAtMs<?1";
/// The summary's counts and the ring's state, one statement.
pub const SUMMARY_SQL: &str = "SELECT COUNT(*) AS identities,COALESCE(SUM(CASE WHEN diffCount>0 AND heldByLagCount=0 THEN 1 ELSE 0 END),0) AS diffs,COALESCE(SUM(CASE WHEN heldByLagCount>0 THEN 1 ELSE 0 END),0) AS heldByLag,COALESCE(SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END),0) AS errors,MIN(comparedAtMs) AS oldestAtMs,(SELECT cursor FROM owed_belt_state WHERE name='owed-actor:compare-ring') AS ring,(SELECT CAST(tickedAtMs AS INTEGER) FROM owed_belt_state WHERE name='owed-actor:compare-ring') AS tickedAtMs FROM owed_actor_compare";
/// The differing identities (a diff, held or not, or an error), past `?1`, in identity order (`?2` a page plus one).
pub const DIFFERING_SQL: &str = "SELECT identity,comparedAtMs,diffCount,heldByLagCount,tipLagCount,outpoints,d1Digest,actorDigest,games,gamesCut,error FROM owed_actor_compare WHERE (diffCount>0 OR error IS NOT NULL) AND identity>?1 ORDER BY identity LIMIT ?2";
/// One identity's row.
pub const ONE_SQL: &str = "SELECT identity,comparedAtMs,diffCount,heldByLagCount,tipLagCount,outpoints,d1Digest,actorDigest,games,gamesCut,error FROM owed_actor_compare WHERE identity=?1";

fn first(out: Vec<StmtOut>) -> Vec<Value> {
    out.into_iter().next().map(|o| o.rows).unwrap_or_default()
}

/// One identity's compare: the view and what it judged.
#[derive(Debug, Clone)]
pub struct Compared {
    pub identity: String,
    /// The rows lever's answer ([`rows_view`] with `heldByLag`); with `fresh`, `d1.fresh` true and its games.
    pub view: Value,
    /// The games judged fresh (0 for a stored compare) and those past [`COMPARE_GAMES_MAX`].
    pub games: usize,
    pub games_cut: usize,
}

/// THE ONE COMPARE (bsv-low #986). D1's rows of `identity` (its stored rows, or with `fresh` the D1 judge's fresh rows
/// of every game of the identity, at most [`COMPARE_GAMES_MAX`] in game order, written nowhere) against the actor's
/// stored rows (`/stored`, no apply), through [`rows_view`] and [`held_by_lag`] over the identity's pending marks: the
/// rows lever's answer exactly. Reads: one batch of three statements (four with `fresh`), the actor's `/stored`, and
/// the fresh judgement's hydration. An actor no binding reaches is the view's `actor.error`, never an empty diff.
pub async fn compare_identity(
    db: &impl JudgeDb,
    identity: &str,
    fresh: bool,
) -> Result<Compared, String> {
    compare_identity_window(db, identity, fresh, 0).await
}

/// PURE: the window of `games` (in game order) a compare judges: all of them within [`COMPARE_GAMES_MAX`], else
/// [`COMPARE_GAMES_MAX`] starting at `window * COMPARE_GAMES_MAX` modulo their count, wrapping.
pub fn games_window(games: &[String], window: u64) -> Vec<String> {
    if games.len() <= COMPARE_GAMES_MAX {
        return games.to_vec();
    }
    let n = games.len();
    let start = ((window as u128 * COMPARE_GAMES_MAX as u128) % n as u128) as usize;
    (0..COMPARE_GAMES_MAX)
        .map(|k| games[(start + k) % n].clone())
        .collect()
}

/// [`compare_identity`] over the `window`-th window of the identity's games ([`games_window`]); the ring passes its
/// revolution.
pub async fn compare_identity_window(
    db: &impl JudgeDb,
    identity: &str,
    fresh: bool,
    window: u64,
) -> Result<Compared, String> {
    let me = identity.to_ascii_lowercase();
    let mut stmts = vec![
        Stmt::new(RECONCILE_D1_ROWS_SQL, vec![Bind::Text(me.clone())]),
        // D2-ACTOR-B-FOLD-13 (bsv-low #958): which judge version D1's rows are (the stamp, `computedAtMs`).
        Stmt::new(D1_JUDGED_UNDER_SQL, vec![Bind::Text(me.clone())]),
        // FIX-964 (bsv-low #964): the identity's pending marks, so a difference the backlog explains is named.
        Stmt::new(LEVER_PENDING_SQL, vec![Bind::Text(me.clone())]),
    ];
    if fresh {
        stmts.push(Stmt::new(
            COMPARE_GAMES_SQL,
            vec![
                Bind::Text(me.clone()),
                Bind::Text(String::new()),
                Bind::Int(COMPARE_GAMES_READ_MAX as i64),
            ],
        ));
    }
    let mut read = db.run_batch(stmts).await?.into_iter();
    let stored = read.next().map(|o| o.rows).unwrap_or_default();
    let under = read
        .next()
        .and_then(|o| o.rows.into_iter().next())
        .unwrap_or_default();
    let pending = read.next().map(|o| o.rows).unwrap_or_default();
    let all: Vec<String> = read
        .next()
        .map(|o| o.rows)
        .unwrap_or_default()
        .iter()
        .filter_map(|r| r["gameId"].as_str().map(str::to_string))
        .collect();
    let games = games_window(&all, window);
    let games_cut = all.len() - games.len();
    let d1 = if fresh {
        crate::owed_judge::fresh_rows(db, &me, &games).await?
    } else {
        stored
    };
    let mut actor = match db.actor_stored(&me).await {
        Ok(Some(a)) => Ok(a),
        Ok(None) => Err("no owed actor for this identity".to_string()),
        Err(e) => Err(e),
    };
    // Past the bound: both sides read the judged games' rows alone.
    let (d1, games_cut) = if games_cut > 0 {
        let judged: BTreeSet<&str> = games.iter().map(String::as_str).collect();
        let keep = |rows: &[Value]| -> Vec<Value> {
            rows.iter()
                .filter(|r| judged.contains(r["gameId"].as_str().unwrap_or_default()))
                .cloned()
                .collect()
        };
        if let Ok(a) = actor.as_mut() {
            let rows = keep(a["rows"].as_array().map(Vec::as_slice).unwrap_or_default());
            a["rows"] = json!(rows);
        }
        (keep(&d1), games_cut)
    } else {
        (d1, 0)
    };
    let stamp = rejudge_state_of(Some(&json!({"cursor": under["stamp"]})));
    let mut view = rows_view(
        &me,
        actor,
        &d1,
        stamp.as_ref(),
        json_ms(&under["computedAtMs"]).map(|c| c as i64),
    );
    view["heldByLag"] = json!(held_by_lag(&view, &pending));
    view["d1"]["fresh"] = json!(fresh);
    if fresh {
        view["d1"]["games"] = json!(games.len());
        view["d1"]["gamesCut"] = json!(games_cut);
    }
    Ok(Compared {
        identity: me,
        games: if fresh { games.len() } else { 0 },
        games_cut,
        view,
    })
}

/// PURE: the table's row of a compare, as [`ROW_UPSERT_SQL`] binds it and the summary serves it.
pub fn compare_row(c: &Compared, now: i64) -> Value {
    let list = |k: &str| -> Option<Vec<String>> {
        c.view[k].as_array().map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
    };
    let diff = list("diff");
    let held = list("heldByLag").unwrap_or_default();
    let tip_lag = list("tipLag").unwrap_or_default();
    let outpoints: Vec<String> = diff
        .clone()
        .unwrap_or_default()
        .into_iter()
        .take(COMPARE_OUTPOINTS_MAX)
        .collect();
    json!({
        "identity": c.identity,
        "comparedAtMs": now,
        "diffCount": diff.as_ref().map(Vec::len),
        "heldByLagCount": held.len(),
        "tipLagCount": tip_lag.len(),
        "outpoints": outpoints,
        "d1Digest": c.view["digests"]["d1"],
        "actorDigest": c.view["digests"]["actor"],
        "games": c.games,
        "gamesCut": c.games_cut,
        "error": c.view["actor"]["error"],
    })
}

/// The row's write.
pub fn row_upsert(row: &Value) -> Stmt {
    let int = |k: &str| row[k].as_i64().map_or(Bind::Null, Bind::Int);
    let text = |k: &str| {
        row[k]
            .as_str()
            .map_or(Bind::Null, |s| Bind::Text(s.to_string()))
    };
    Stmt::new(
        ROW_UPSERT_SQL,
        vec![
            text("identity"),
            int("comparedAtMs"),
            int("diffCount"),
            Bind::Int(row["heldByLagCount"].as_i64().unwrap_or(0)),
            Bind::Int(row["tipLagCount"].as_i64().unwrap_or(0)),
            Bind::Text(row["outpoints"].to_string()),
            text("d1Digest"),
            text("actorDigest"),
            Bind::Int(row["games"].as_i64().unwrap_or(0)),
            Bind::Int(row["gamesCut"].as_i64().unwrap_or(0)),
            text("error"),
        ],
    )
}

/// The ring's state ([`RING_STATE_NAME`]'s cursor JSON).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RingState {
    /// The last identity compared (`""` at a revolution's start).
    pub after: String,
    pub revolution: u64,
    pub revolution_started_at_ms: i64,
    /// The last complete revolution's length.
    pub last_revolution_ms: Option<i64>,
}

impl RingState {
    pub fn of(cursor: Option<&str>) -> Self {
        let v: Value = cursor
            .and_then(|c| serde_json::from_str(c).ok())
            .unwrap_or(Value::Null);
        Self {
            after: v["after"].as_str().unwrap_or_default().to_string(),
            revolution: v["revolution"].as_u64().unwrap_or(0),
            revolution_started_at_ms: crate::owed_events::ms_of(&v["revolutionStartedAtMs"])
                .unwrap_or(0),
            last_revolution_ms: crate::owed_events::ms_of(&v["lastRevolutionMs"]),
        }
    }
    pub fn cursor(&self) -> String {
        json!({
            "after": self.after,
            "revolution": self.revolution,
            "revolutionStartedAtMs": self.revolution_started_at_ms,
            "lastRevolutionMs": self.last_revolution_ms,
        })
        .to_string()
    }
}

/// One step's account.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RingReport {
    pub compared: Vec<String>,
    /// Identities compared with a difference not held by lag.
    pub diffs: usize,
    pub errors: usize,
    pub games: usize,
    /// The registry's page came back short: the revolution ended (its stale rows pruned) and the cursor wrapped.
    pub wrapped: bool,
    /// The step stopped for its games or its wall with identities left in its page.
    pub stopped_early: bool,
    pub paused: bool,
    /// The overlay's certified migration count is short of [`COMPARE_MIGRATION_COUNT`]: nothing compared.
    pub ungated: bool,
}

/// The step's bounds (the render's vars, else the defaults).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RingBounds {
    pub identities: usize,
    pub games: usize,
    pub wall_ms: u64,
}

impl Default for RingBounds {
    fn default() -> Self {
        Self {
            identities: COMPARE_IDENTITIES_PER_TICK,
            games: COMPARE_GAMES_PER_TICK,
            wall_ms: COMPARE_TICK_WALL_MS,
        }
    }
}

impl RingBounds {
    /// `OWED_FLEET_COMPARE_PER_TICK` (identities; 0 pauses; clamped to the max) and
    /// `OWED_FLEET_COMPARE_GAMES_PER_TICK` (at least 1); unset or unreadable is the default.
    pub fn of_vars(identities: Option<&str>, games: Option<&str>) -> Self {
        let d = Self::default();
        let parse = |v: Option<&str>| v.and_then(|s| s.trim().parse::<usize>().ok());
        Self {
            identities: parse(identities)
                .map_or(d.identities, |n| n.min(COMPARE_IDENTITIES_PER_TICK_MAX)),
            games: parse(games).map_or(d.games, |n| n.max(1)),
            wall_ms: d.wall_ms,
        }
    }
}

/// THE RING, one step: the identities past the cursor (the registry `GET /internal/owed-actor-identities` reads),
/// at most `bounds.identities`, each compared FRESH ([`compare_identity`]) and its row written with the cursor in ONE
/// batch. A step stops starting identities once it judged `bounds.games` games or ran `bounds.wall_ms` (its first
/// identity always runs). A page shorter than the step asked ends the revolution: the rows its compares did not write
/// are pruned, the cursor wraps to `""` and the revolution's length is kept. A compare that faults (a D1 read) ends
/// the step with its cursor before that identity, so the next step starts there.
pub async fn ring_step(
    db: &impl JudgeDb,
    now: i64,
    bounds: RingBounds,
) -> Result<RingReport, String> {
    let mut report = RingReport::default();
    if bounds.identities == 0 {
        report.paused = true;
        return Ok(report);
    }
    let mut read = db
        .run_batch(vec![
            Stmt::new(RING_STATE_SQL, vec![]),
            Stmt::new(crate::owed_events::CERTIFIED_COUNT_SQL, vec![]),
        ])
        .await?
        .into_iter();
    let state_row = read.next().map(|o| o.rows).unwrap_or_default();
    let count = read
        .next()
        .and_then(|o| o.rows.into_iter().next())
        .and_then(|r| r["count"].as_f64());
    if !count.is_some_and(|c| c >= f64::from(COMPARE_MIGRATION_COUNT)) {
        report.ungated = true;
        return Ok(report);
    }
    let mut state = RingState::of(state_row.first().and_then(|r| r["cursor"].as_str()));
    if state.after.is_empty() && state.revolution_started_at_ms == 0 {
        state.revolution_started_at_ms = now;
    }
    let page: Vec<String> = first(
        db.run_batch(vec![Stmt::new(
            RESET_IDENTITIES_SQL,
            vec![
                Bind::Text(state.after.clone()),
                Bind::Int(bounds.identities as i64),
            ],
        )])
        .await?,
    )
    .iter()
    .filter_map(|r| r["identity"].as_str().map(str::to_string))
    .collect();
    let start = db.now_ms();
    let save = |state: &RingState| {
        Stmt::new(
            crate::owed_belt::STATE_SQL,
            vec![
                Bind::Text(RING_STATE_NAME.into()),
                Bind::Text(state.cursor()),
                Bind::Int(now),
            ],
        )
    };
    for (n, identity) in page.iter().enumerate() {
        if n > 0
            && (report.games >= bounds.games || db.now_ms().saturating_sub(start) >= bounds.wall_ms)
        {
            report.stopped_early = true;
            break;
        }
        let compared = compare_identity_window(db, identity, true, state.revolution).await?;
        let row = compare_row(&compared, now);
        report.games += compared.games;
        if row["error"].is_string() {
            report.errors += 1;
        } else if row["diffCount"].as_u64().unwrap_or(0) > 0 && row["heldByLagCount"] == 0 {
            report.diffs += 1;
        }
        state.after = compared.identity.clone();
        db.run_batch(vec![row_upsert(&row), save(&state)]).await?;
        report.compared.push(compared.identity);
    }
    if !report.stopped_early && page.len() < bounds.identities {
        report.wrapped = true;
        let started = state.revolution_started_at_ms;
        let next = RingState {
            after: String::new(),
            revolution: state.revolution + 1,
            revolution_started_at_ms: now,
            last_revolution_ms: Some(now.saturating_sub(started)),
        };
        db.run_batch(vec![
            Stmt::new(PRUNE_SQL, vec![Bind::Int(started)]),
            save(&next),
        ])
        .await?;
    } else if report.compared.is_empty() {
        db.run_batch(vec![save(&state)]).await?;
    }
    Ok(report)
}

/// PURE: the summary body over [`SUMMARY_SQL`]'s row and a page of [`DIFFERING_SQL`]'s (or [`ONE_SQL`]'s) rows.
pub fn summary_body(counts: &Value, rows: &[Value], now: i64, page: usize) -> Value {
    let n = |k: &str| crate::owed_events::ms_of(&counts[k]).unwrap_or(0);
    let ring = RingState::of(counts["ring"].as_str());
    let oldest = crate::owed_events::ms_of(&counts["oldestAtMs"]);
    let more = rows.len() > page;
    let rows: Vec<Value> = rows.iter().take(page).map(row_view).collect();
    let next = if more {
        rows.last().map(|r| r["identity"].clone())
    } else {
        None
    };
    json!({
        "identities": n("identities"),
        "comparedWithinMs": oldest.map(|o| now.saturating_sub(o).max(0)),
        "diffs": n("diffs"),
        "heldByLag": n("heldByLag"),
        "errors": n("errors"),
        "ringCursor": ring.after,
        "revolution": ring.revolution,
        "lastRevolutionMs": ring.last_revolution_ms,
        "lastTickAt": crate::owed_events::ms_of(&counts["tickedAtMs"]),
        "rows": rows,
        "next": next,
    })
}

/// A stored row as served: its outpoints a list.
pub fn row_view(r: &Value) -> Value {
    let n = |k: &str| crate::owed_events::ms_of(&r[k]);
    json!({
        "identity": r["identity"],
        "comparedAtMs": n("comparedAtMs"),
        "diffCount": n("diffCount"),
        "heldByLagCount": n("heldByLagCount").unwrap_or(0),
        "tipLagCount": n("tipLagCount").unwrap_or(0),
        "outpoints": r["outpoints"].as_str().and_then(|s| serde_json::from_str::<Value>(s).ok()).unwrap_or(json!([])),
        "d1Digest": r["d1Digest"],
        "actorDigest": r["actorDigest"],
        "games": n("games").unwrap_or(0),
        "gamesCut": n("gamesCut").unwrap_or(0),
        "error": r["error"],
    })
}

/// A step's report as the levers and the actor's alarm answer it.
pub fn ring_report_json(r: &RingReport) -> Value {
    json!({
        "compared": r.compared, "diffs": r.diffs, "errors": r.errors, "games": r.games, "wrapped": r.wrapped,
        "stoppedEarly": r.stopped_early, "paused": r.paused, "ungated": r.ungated,
    })
}

/// The summary's read: the counts and the differing rows past `after` (or one identity's row), one batch.
pub async fn summary(
    db: &impl crate::pot_hook::SqlPort,
    identity: Option<&str>,
    after: &str,
    now: i64,
) -> Result<Value, String> {
    let rows_stmt = match identity {
        Some(id) => Stmt::new(ONE_SQL, vec![Bind::Text(id.to_string())]),
        None => Stmt::new(
            DIFFERING_SQL,
            vec![
                Bind::Text(after.to_string()),
                Bind::Int(SUMMARY_PAGE as i64 + 1),
            ],
        ),
    };
    let mut out = db
        .run_batch(vec![Stmt::new(SUMMARY_SQL, vec![]), rows_stmt])
        .await?
        .into_iter();
    let counts = out
        .next()
        .and_then(|o| o.rows.into_iter().next())
        .unwrap_or_default();
    let rows = out.next().map(|o| o.rows).unwrap_or_default();
    Ok(summary_body(&counts, &rows, now, SUMMARY_PAGE))
}

/// The hand lever's work: one identity compared fresh now and its row written.
pub async fn compare_and_write(
    db: &impl JudgeDb,
    identity: &str,
    now: i64,
) -> Result<(Value, Value), String> {
    let compared = compare_identity(db, identity, true).await?;
    let row = compare_row(&compared, now);
    db.run_batch(vec![row_upsert(&row)]).await?;
    Ok((row, compared.view))
}

// ---------------------------------------------------------------------------------------------------------------
// The Worker's side: the levers, the ring's actor and its watchdog.
// ---------------------------------------------------------------------------------------------------------------

/// The production judge port for a compare: the served tip and the era (the D1 judge's, writing nothing here).
async fn judge_env(env: &worker::Env) -> worker::Result<crate::owed_judge::D1Judge> {
    let tip = crate::owed_events::tip_memoised(env, false).await;
    crate::owed_judge::D1Judge::new(env, tip, crate::routes::written_off_before_ms_env(env))
}

fn now_ms() -> i64 {
    worker::Date::now().as_millis() as i64
}

/// `GET /internal/owed-actor-compare[?identity=][&after=]` (bearer `INTERNAL_TOKEN`, 401 before any read): the
/// fleet's summary ([`summary_body`]); `POST /internal/owed-actor-compare {"identity": ..}`: one identity re-compared
/// fresh now and its row written, answered `{"row", "view"}`; `POST {"ring": true}`: one ring step now, answered its
/// report.
pub async fn compare_lever(
    mut req: worker::Request,
    env: &worker::Env,
) -> worker::Result<worker::Response> {
    if !crate::internal_events::internal_bearer_ok(&req, env) {
        return worker::Response::error("unauthorized", 401);
    }
    if req.method() == worker::Method::Post {
        let body: Value = match req.json().await {
            Ok(b) => b,
            Err(e) => return worker::Response::error(format!("bad compare body: {e}"), 400),
        };
        // One ring step on demand, under this request's D1 ledger (the budget tier's cell; the operator's nudge).
        if body["ring"] == true {
            return match ring_env(env).await {
                Ok(r) => worker::Response::from_json(&ring_report_json(&r)),
                Err(e) => worker::Response::error(format!("ring step faulted: {e}"), 503),
            };
        }
        let Some(me) = body["identity"].as_str().and_then(actor_name) else {
            return worker::Response::error("identity is not a 66-hex key", 400);
        };
        let judge = judge_env(env).await?;
        return match compare_and_write(&judge, &me, now_ms()).await {
            Ok((row, view)) => worker::Response::from_json(&json!({"row": row, "view": view})),
            Err(e) => worker::Response::error(format!("compare faulted: {e}"), 503),
        };
    }
    let url = req.url()?;
    let q = |k: &str| {
        url.query_pairs()
            .find(|(n, _)| n == k)
            .map(|(_, v)| v.to_string())
    };
    let identity = match q("identity") {
        Some(v) => match actor_name(&v) {
            Some(n) => Some(n),
            None => return worker::Response::error("identity is not a 66-hex key", 400),
        },
        None => None,
    };
    let after = q("after").unwrap_or_default();
    let db = crate::routes::D1Sql::of(env)?;
    match summary(&db, identity.as_deref(), &after, now_ms()).await {
        Ok(b) => worker::Response::from_json(&b),
        Err(e) => worker::Response::error(format!("compare summary faulted: {e}"), 503),
    }
}

/// The ring's step in its actor: the render's bounds, the judge's production port.
pub async fn ring_env(env: &worker::Env) -> Result<RingReport, String> {
    crate::schema::ensure_latch_columns(env).await;
    let bounds = RingBounds::of_vars(
        env.var("OWED_FLEET_COMPARE_PER_TICK")
            .ok()
            .map(|v| v.to_string())
            .as_deref(),
        env.var("OWED_FLEET_COMPARE_GAMES_PER_TICK")
            .ok()
            .map(|v| v.to_string())
            .as_deref(),
    );
    let judge = judge_env(env).await.map_err(|e| e.to_string())?;
    let report = ring_step(&judge, now_ms(), bounds).await?;
    worker::console_log!(
        "[owed-compare] step: {} compared, {} with a diff, {} errors, {} games, wrapped {}, stopped early {}, paused {}, ungated {}",
        report.compared.len(),
        report.diffs,
        report.errors,
        report.games,
        report.wrapped,
        report.stopped_early,
        report.paused,
        report.ungated
    );
    Ok(report)
}

/// The scheduled pass's watchdog for the ring: its actor arms its alarm when none is set. Best effort.
pub async fn arm(env: &worker::Env) -> worker::Result<()> {
    let stub = env
        .durable_object("BOARD_VIEW")?
        .id_from_name(COMPARE_ACTOR)?
        .get_stub()?;
    let response = stub
        .fetch_with_str("https://board-view/owed-compare-arm")
        .await?;
    if response.status_code() != 200 {
        return Err(worker::Error::RustError("owed compare arm refused".into()));
    }
    Ok(())
}
