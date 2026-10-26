//! bsv-low #893 section 6.1: the one-time `owed-v2` derivation behind a stamp, run by the scheduled pass and never by a
//! read (the 2026-10-12 lesson: a backfill never runs on a read path).
//!
//! A pass ([`backfill_pass`]) reads the stamp first and stops there once it is written (one indexed read, pinned at
//! zero scan steps). Unstamped, it reads the overlay's certified migration list (the triggers must exist before the
//! rows do) and its cursor; the first pass that finds the gate open only ARMS the cursor, so the work starts one cron
//! tick after the new version's first pass (risk 8.4). Then up to [`OWED_BACKFILL_IDENTITIES_PER_PASS`] identities
//! past the cursor, ascending, from `potparty_records` and `hopparty_records` by their identity indexes: for each, its
//! games marked `source = 'backfill'` and judged [`OWED_BACKFILL_GAMES_PER_BATCH`] a batch through the shipped judge,
//! then the cursor saved. The stamp lands in the batch that saves the cursor past the last identity.
//!
//! An identity the wall cuts keeps its remaining marks (the drain may judge them too) and its `open` row, so the next
//! pass finishes its marks and never re-marks it from the start: an identity bigger than one pass still converges.
//!
//! [`mark_aged`] is risk 8.2's third repair: the scheduled drain re-marks, at most once a day a row, the subjects whose
//! stored row was judged more than 24 h ago (`source = 'age'`), bounded [`OWED_AGE_REMARKS_PER_PASS`] a pass.
use crate::owed_judge::{self, JudgeDb};
use crate::pot_hook::{Bind, Deadline, SqlPort, Stmt};
use serde_json::Value;

/// The derivation's version, its `view_derivation_state` row. A later derivation ships `owed-v3` with its own cursor.
pub const OWED_BACKFILL_STAMP: &str = "owed-v2";
/// The `owed_belt_state` row holding the last identity backfilled ('' once armed, before the first).
pub const OWED_BACKFILL_CURSOR: &str = "backfill:owed-v2";
/// The `owed_belt_state` row naming the identity whose marks are written and not yet all judged.
pub const OWED_BACKFILL_OPEN: &str = "backfill:owed-v2:open";
pub const OWED_BACKFILL_IDENTITIES_PER_PASS: usize = 128;
/// One judge batch: the judge's own per-invocation subject bound.
pub const OWED_BACKFILL_GAMES_PER_BATCH: usize = owed_judge::OWED_JUDGE_SUBJECTS_PER_INVOCATION;
/// The pass's own wall and the cursor save's reserve inside it.
pub const OWED_BACKFILL_WALL_MS: u64 = 20_000;
pub const OWED_BACKFILL_SAVE_RESERVE_MS: u64 = 3_000;
/// A row judged longer ago than this is re-marked by the drain (once a day, since its judge restamps it).
pub const OWED_AGE_REMARK_AFTER_MS: i64 = 86_400_000;
pub const OWED_AGE_REMARKS_PER_PASS: usize = 128;
/// The passes one identity's judge may fault before the backfill records it, leaves its marks for the drain and moves
/// its cursor past it (the D2-OWED lens's LOW-1: one wedged identity held the stamp and every later identity back).
pub const OWED_BACKFILL_IDENTITY_FAULTS_MAX: usize = 3;
/// The `owed_belt_state` row counting the open identity's consecutive faulted passes (`{"identity":..,"faults":n}`).
pub const OWED_BACKFILL_FAULTS: &str = "backfill:owed-v2:faults";

pub const COUNTER_IDENTITIES: &str = "owed_backfill_identities_total";
pub const COUNTER_ROWS: &str = "owed_backfill_rows_total";
pub const COUNTER_AGED: &str = "owed_age_remarked_total";
pub const COUNTER_SKIPPED: &str = "owed_backfill_skipped_identities_total";
pub fn counter_names() -> Vec<String> {
    [COUNTER_IDENTITIES, COUNTER_ROWS, COUNTER_AGED, COUNTER_SKIPPED]
        .map(str::to_owned)
        .to_vec()
}

/// The stamped replay's one statement: a primary-key read of one row.
pub const STAMP_SQL: &str = "SELECT 1 AS stamped FROM view_derivation_state WHERE name='owed-v2'";
/// The gate (the overlay's certified migration list) and both cursor rows, each a primary-key read.
pub const GATE_SQL: &str = "SELECT (SELECT value FROM ops_counters WHERE name='overlay_migration_count') AS count,(SELECT cursor FROM owed_belt_state WHERE name='backfill:owed-v2') AS cursor,(SELECT cursor FROM owed_belt_state WHERE name='backfill:owed-v2:open') AS open,(SELECT cursor FROM owed_belt_state WHERE name='backfill:owed-v2:faults') AS faults";
/// The open identity's fault count, written by a faulted pass and cleared ('{}') by its identity's save.
pub const FAULTS_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('backfill:owed-v2:faults',?1,?2) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// An identity passed by after its faults: its row names the last fault (bounded), its marks stay for the drain.
pub const SKIPPED_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('backfill:owed-v2:skipped:'||?1,?2,?3) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// Arm the cursor; a pass that finds it armed begins.
pub const ARM_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('backfill:owed-v2','',?1) ON CONFLICT(name) DO NOTHING";
/// The identities past the cursor, distinct and ascending, each side bounded on its own identity index.
pub const IDENTITIES_SQL: &str = "SELECT identity FROM (SELECT identity FROM (SELECT DISTINCT identity FROM potparty_records INDEXED BY idx_potparty_identity WHERE identity>?1 ORDER BY identity LIMIT ?2) UNION SELECT identity FROM (SELECT DISTINCT identity FROM hopparty_records INDEXED BY idx_hopparty_identity WHERE identity>?1 ORDER BY identity LIMIT ?2)) ORDER BY identity LIMIT ?2";
/// Every game of the identity the judge's wanted expansion names, marked `backfill`.
pub const MARK_SQL: &str = "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) SELECT ?1,gameId,'backfill','owed-v2',?2 FROM (SELECT gameId FROM potparty_records WHERE identity=?1 UNION SELECT gameId FROM hopparty_records WHERE identity=?1 UNION SELECT gameId FROM released_hop_evidence WHERE identity=?1 UNION SELECT gameId FROM owed_rows WHERE identity=?1) WHERE gameId<>'' ON CONFLICT(identity,gameId,source,sourceKey) DO UPDATE SET generation=excluded.generation,markedAtMs=excluded.markedAtMs";
pub const OPEN_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('backfill:owed-v2:open',?1,?2) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// The whole identity is rebuilt by this backfill, so its earlier overflow flag is judged again (the judge ORs it).
pub const TRUNCATED_RESET_SQL: &str = "UPDATE owed_state SET truncated=0 WHERE identity=?1";
/// The next batch of the identity's backfill marks.
pub const GAMES_SQL: &str = "SELECT gameId FROM owed_dirty WHERE identity=?1 AND source='backfill' ORDER BY gameId LIMIT ?2";
pub const CURSOR_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('backfill:owed-v2',?1,?2) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
pub const STAMP_WRITE_SQL: &str =
    "INSERT OR IGNORE INTO view_derivation_state (name) VALUES ('owed-v2')";
/// Risk 8.2's third repair: the oldest judged rows' subjects, by the judged index, coalesced on the dirty key. At most
/// [`OWED_AGE_REMARKS_PER_PASS`] age marks are ever pending, and the drain judges them after every other mark
/// (`owed_judge::drain_dirty`), so the repair fills the drain's idle room and never delays a source's mark.
pub const AGE_MARK_SQL: &str = "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) SELECT identity,gameId,'age','age',?2 FROM owed_rows r INDEXED BY idx_owed_rows_judged WHERE judgedAtMs<?1 AND gameId<>'' AND NOT EXISTS (SELECT 1 FROM owed_dirty d WHERE d.identity=r.identity AND d.gameId=r.gameId AND d.source='age') ORDER BY judgedAtMs LIMIT max(0,?3-(SELECT COUNT(*) FROM owed_dirty INDEXED BY idx_owed_dirty_source_key WHERE sourceKey='age' AND source='age')) ON CONFLICT(identity,gameId,source,sourceKey) DO NOTHING";

/// The D2-OWED lens's MEDIUM-5: the subjects of KEYLESS rows (`judgedAtMs` NULL: an old worker's write in the deploy
/// window, or a rollback to the old walk and forward again), marked `age` in the same room as [`AGE_MARK_SQL`]: run
/// first in one batch, the two together never past [`OWED_AGE_REMARKS_PER_PASS`] pending. The judge rewrites each
/// with its keys, so the set drains; a game no source names any more is judged away.
pub const AGE_MARK_UNJUDGED_SQL: &str = "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) SELECT identity,gameId,'age','age',?1 FROM (SELECT DISTINCT r.identity,r.gameId FROM owed_rows r INDEXED BY idx_owed_rows_unjudged WHERE r.judgedAtMs IS NULL AND r.gameId<>'' AND NOT EXISTS (SELECT 1 FROM owed_dirty d WHERE d.identity=r.identity AND d.gameId=r.gameId AND d.source='age') LIMIT max(0,?2-(SELECT COUNT(*) FROM owed_dirty INDEXED BY idx_owed_dirty_source_key WHERE sourceKey='age' AND source='age'))) WHERE 1 ON CONFLICT(identity,gameId,source,sourceKey) DO NOTHING";

/// Where a pass stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Stop {
    /// The stamp was already written: the one read.
    #[default]
    Stamped,
    /// The overlay's certified migration list does not hold this build's triggers yet.
    Gated,
    /// The gate opened this pass: the cursor armed, nothing judged.
    Armed,
    /// The page was done; more identities may follow.
    Page,
    /// The cursor passed the last identity: the stamp written.
    Finished,
    /// The wall (less the save's reserve) fell.
    Wall,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BackfillReport {
    pub stop: Stop,
    pub identities: usize,
    pub rows: usize,
    pub batches: usize,
    pub cursor: Option<String>,
}

fn rows_of(out: Vec<crate::pot_hook::StmtOut>) -> Result<Vec<Value>, String> {
    out.into_iter()
        .next()
        .map(|o| o.rows)
        .ok_or_else(|| "backfill read returned no statement".to_string())
}

async fn read(db: &impl SqlPort, sql: &str, binds: Vec<Bind>) -> Result<Vec<Value>, String> {
    rows_of(
        db.run_batch(vec![Stmt {
            sql: sql.into(),
            binds,
        }])
        .await?,
    )
}

/// The overlay's migration count through the #893 block (its dirty triggers, `idx_owed_rows_judged`, and the D2-OWED
/// fold's indexes and row-count triggers, which this layer's statements name by `INDEXED BY`). The list is append-only,
/// so a certified count at or past it holds them; pinned against the overlay's list (the app layer does not link the
/// overlay, so the figure is this crate's). The deploy order is the window's (the D2-OWED lens's LOW-5): the overlay
/// first, its certified count read on `/health` at or past this figure, then the app layer.
pub const OWED_TRIGGERS_MIGRATION_COUNT: u32 = 407;

/// The gate: the overlay certified a migration list that holds the triggers (the rows' marks) and the age index.
pub fn gate_open(count: Option<f64>) -> bool {
    count.is_some_and(|c| c >= f64::from(OWED_TRIGGERS_MIGRATION_COUNT))
}

/// One identity's backfill: its marks (unless already open), its games judged a batch at a time. `Ok(None)`: the wall.
async fn backfill_identity(
    db: &impl JudgeDb,
    deadline: &Deadline,
    identity: &str,
    resume: bool,
    report: &mut BackfillReport,
) -> Result<Option<usize>, String> {
    let now = db.now_ms() as i64;
    if !resume {
        let marked = deadline
            .run_reserving(db.run_batch(vec![
                Stmt {
                    sql: MARK_SQL.into(),
                    binds: vec![Bind::Text(identity.into()), Bind::Int(now)],
                },
                Stmt {
                    sql: OPEN_SQL.into(),
                    binds: vec![Bind::Text(identity.into()), Bind::Int(now)],
                },
                Stmt {
                    sql: TRUNCATED_RESET_SQL.into(),
                    binds: vec![Bind::Text(identity.into())],
                },
            ]))
            .await;
        match marked {
            None => return Ok(None),
            Some(r) => {
                r?;
            }
        }
    }
    let mut rows = 0;
    let mut last: Option<Vec<String>> = None;
    loop {
        let Some(page) = deadline
            .run_reserving(read(
                db,
                GAMES_SQL,
                vec![
                    Bind::Text(identity.into()),
                    Bind::Int(OWED_BACKFILL_GAMES_PER_BATCH as i64),
                ],
            ))
            .await
        else {
            return Ok(None);
        };
        let games: Vec<String> = page?
            .iter()
            .filter_map(|r| r["gameId"].as_str().map(str::to_owned))
            .collect();
        if games.is_empty() {
            return Ok(Some(rows));
        }
        if last.as_ref() == Some(&games) {
            return Err(format!("backfill marks of {identity} did not clear"));
        }
        let Some(judged) = deadline
            .run_reserving(owed_judge::judge_subjects(db, identity, &games, "backfill"))
            .await
        else {
            return Ok(None);
        };
        rows = judged?.rows;
        report.batches += 1;
        last = Some(games);
    }
}

/// One scheduled pass of the backfill under `deadline` (its wall and the cursor save's reserve).
pub async fn backfill_pass(
    db: &impl JudgeDb,
    deadline: &Deadline,
) -> Result<BackfillReport, String> {
    let mut report = BackfillReport::default();
    if !read(db, STAMP_SQL, vec![]).await?.is_empty() {
        return Ok(report);
    }
    let gate = read(db, GATE_SQL, vec![]).await?;
    let gate = gate.first().cloned().unwrap_or_default();
    if !gate_open(gate["count"].as_f64()) {
        report.stop = Stop::Gated;
        return Ok(report);
    }
    let Some(cursor) = gate["cursor"].as_str().map(str::to_owned) else {
        db.run_batch(vec![Stmt {
            sql: ARM_SQL.into(),
            binds: vec![Bind::Int(db.now_ms() as i64)],
        }])
        .await?;
        report.stop = Stop::Armed;
        return Ok(report);
    };
    let open = gate["open"].as_str().map(str::to_owned);
    let faults: Value = gate["faults"]
        .as_str()
        .and_then(|f| serde_json::from_str(f).ok())
        .unwrap_or_default();
    report.cursor = Some(cursor.clone());
    let page = read(
        db,
        IDENTITIES_SQL,
        vec![
            Bind::Text(cursor),
            Bind::Int(OWED_BACKFILL_IDENTITIES_PER_PASS as i64),
        ],
    )
    .await?;
    let identities: Vec<String> = page
        .iter()
        .filter_map(|r| r["identity"].as_str().map(str::to_owned))
        .collect();
    let last_page = identities.len() < OWED_BACKFILL_IDENTITIES_PER_PASS;
    if identities.is_empty() {
        db.run_batch(vec![Stmt {
            sql: STAMP_WRITE_SQL.into(),
            binds: vec![],
        }])
        .await?;
        report.stop = Stop::Finished;
        return Ok(report);
    }
    report.stop = Stop::Page;
    let result = async {
        for (n, identity) in identities.iter().enumerate() {
            let resume = open.as_deref() == Some(identity.as_str());
            let mut save = vec![Stmt {
                sql: CURSOR_SQL.into(),
                binds: vec![Bind::Text(identity.clone()), Bind::Int(db.now_ms() as i64)],
            }];
            let rows = match backfill_identity(db, deadline, identity, resume, &mut report).await {
                Ok(Some(rows)) => rows,
                Ok(None) => {
                    report.stop = Stop::Wall;
                    return Ok::<(), String>(());
                }
                Err(error) => {
                    // The D2-OWED lens's LOW-1: counted per identity across passes; at the bound the identity is
                    // recorded, its marks left for the drain, and the cursor moves past it.
                    let seen = if faults["identity"] == identity.as_str() {
                        faults["faults"].as_u64().unwrap_or(0) as usize
                    } else {
                        0
                    } + 1;
                    let now = db.now_ms() as i64;
                    if seen < OWED_BACKFILL_IDENTITY_FAULTS_MAX {
                        db.run_batch(vec![Stmt {
                            sql: FAULTS_SQL.into(),
                            binds: vec![
                                Bind::Text(
                                    serde_json::json!({"identity": identity, "faults": seen})
                                        .to_string(),
                                ),
                                Bind::Int(now),
                            ],
                        }])
                        .await?;
                        return Err(error);
                    }
                    db.log(&format!(
                        "[owed-backfill] {identity}: the judge faulted on {seen} passes ({error}); recorded, its marks left for the drain, passed"
                    ));
                    let reason: String = error.chars().take(200).collect();
                    save.push(Stmt {
                        sql: SKIPPED_SQL.into(),
                        binds: vec![Bind::Text(identity.clone()), Bind::Text(reason), Bind::Int(now)],
                    });
                    db.counters(vec![(COUNTER_SKIPPED.into(), 1)]).await;
                    0
                }
            };
            let finished = last_page && n + 1 == identities.len();
            if faults["identity"] == identity.as_str() {
                save.push(Stmt {
                    sql: FAULTS_SQL.into(),
                    binds: vec![Bind::Text("{}".into()), Bind::Int(db.now_ms() as i64)],
                });
            }
            if finished {
                save.push(Stmt {
                    sql: STAMP_WRITE_SQL.into(),
                    binds: vec![],
                });
            }
            // The save takes the reserve the judges left it.
            match deadline.run(db.run_batch(save)).await {
                None => {
                    report.stop = Stop::Wall;
                    return Ok(());
                }
                Some(r) => {
                    r?;
                }
            }
            report.identities += 1;
            report.rows += rows;
            report.cursor = Some(identity.clone());
            if finished {
                report.stop = Stop::Finished;
            }
        }
        Ok(())
    }
    .await;
    if report.identities > 0 {
        db.counters(vec![
            (COUNTER_IDENTITIES.into(), report.identities as u64),
            (COUNTER_ROWS.into(), report.rows as u64),
        ])
        .await;
    }
    result.map(|()| report)
}

/// The drain's daily re-mark of rows judged more than 24 h ago. Returns the marks written.
pub async fn mark_aged(db: &impl SqlPort) -> Result<u64, String> {
    let now = db.now_ms() as i64;
    let out = db
        .run_batch(vec![
            Stmt {
                sql: AGE_MARK_UNJUDGED_SQL.into(),
                binds: vec![Bind::Int(now), Bind::Int(OWED_AGE_REMARKS_PER_PASS as i64)],
            },
            Stmt {
                sql: AGE_MARK_SQL.into(),
                binds: vec![
                    Bind::Int(now - OWED_AGE_REMARK_AFTER_MS),
                    Bind::Int(now),
                    Bind::Int(OWED_AGE_REMARKS_PER_PASS as i64),
                ],
            },
        ])
        .await?;
    let marked = out.iter().filter_map(|o| o.changes).sum::<u64>();
    if marked > 0 {
        db.counters(vec![(COUNTER_AGED.into(), marked)]).await;
    }
    Ok(marked)
}

/// The scheduled pass's task: the judge's production port under the backfill's own wall.
pub async fn run_env(env: &worker::Env) -> Result<BackfillReport, String> {
    let deadline =
        crate::routes::worker_deadline(OWED_BACKFILL_WALL_MS, OWED_BACKFILL_SAVE_RESERVE_MS);
    crate::schema::ensure_latch_columns(env).await;
    let tip = crate::routes::chaintracks_present_height_env(env, "owed")
        .await
        .ok();
    let judge = owed_judge::D1Judge::new(env, tip, crate::routes::written_off_before_ms_env(env))
        .map_err(|e| e.to_string())?;
    backfill_pass(&judge, &deadline).await
}

/// `POST /internal/owed-backfill-pass` (bearer `INTERNAL_TOKEN`): one pass, answered with its report (the tier's).
pub async fn lever(req: worker::Request, env: &worker::Env) -> worker::Result<worker::Response> {
    if !crate::internal_events::internal_bearer_ok(&req, env) {
        return worker::Response::error("unauthorized", 401);
    }
    // `?aged=1`: the drain's age repair alone (`mark_aged`, the D2-OWED lens's LOW-3: the tier runs it on local D1)
    if req
        .url()
        .is_ok_and(|u| u.query_pairs().any(|(k, v)| k == "aged" && v == "1"))
    {
        let sql = crate::routes::D1Sql::of(env)?;
        return match mark_aged(&sql).await {
            Ok(aged) => worker::Response::from_json(&serde_json::json!({ "aged": aged })),
            Err(e) => worker::Response::error(format!("owed age repair faulted: {e}"), 503),
        };
    }
    match run_env(env).await {
        Ok(r) => worker::Response::from_json(&serde_json::json!({
            "stop": format!("{:?}", r.stop),
            "identities": r.identities,
            "rows": r.rows,
            "batches": r.batches,
            "cursor": r.cursor,
        })),
        Err(e) => worker::Response::error(format!("owed backfill pass faulted: {e}"), 503),
    }
}

#[cfg(test)]
mod tests {
    fn squash(s: &str) -> String {
        s.split_whitespace().collect()
    }
    /// The wiring: the backfill is its own scheduled task under its own wall and save reserve, handed after the
    /// tracker's; the drain re-marks aged rows before it drains; the lever runs the task's function.
    #[test]
    fn owed893_backfill_the_scheduled_pass_runs_it_as_its_own_task_and_the_drain_remarks_aged_rows()
    {
        let lib = squash(include_str!("lib.rs"));
        let cron = &lib[lib.find("asyncfnscheduled(").unwrap()..];
        let cron = &cron[..cron.find("#[event(fetch)]").unwrap()];
        assert!(cron.contains(&squash(
            ".phase(\"owed.backfill\", owed_backfill::run_env(&env_backfill))"
        )));
        let tick = cron
            .find(&squash(
                "pot_hook::schedule_tick(&|f| ctx.wait_until(f), drain, intake, tracker);",
            ))
            .unwrap();
        let handed = cron.find(&squash("ctx.wait_until(backfill);")).unwrap();
        assert!(tick < handed, "handed after the tracker's task");
        let aged = cron
            .find(&squash("owed_backfill::mark_aged(&judge).await"))
            .unwrap();
        let drain = cron
            .find(&squash("owed_judge::drain_dirty(&judge,"))
            .unwrap();
        assert!(aged < drain, "the age re-mark precedes the drain");
        assert!(lib.contains(&squash(
            "req.path() == \"/internal/owed-backfill-pass\" { return owed_backfill::lever(req, &env).await; }"
        )));
        let me = squash(include_str!("owed_backfill.rs"));
        assert!(me.contains(&squash(
            "crate::routes::worker_deadline(OWED_BACKFILL_WALL_MS, OWED_BACKFILL_SAVE_RESERVE_MS);"
        )));
        assert_eq!(
            (
                super::OWED_BACKFILL_WALL_MS,
                super::OWED_BACKFILL_SAVE_RESERVE_MS
            ),
            (20_000, 3_000)
        );
        assert_eq!(super::OWED_BACKFILL_IDENTITIES_PER_PASS, 128);
        assert_eq!(super::OWED_BACKFILL_GAMES_PER_BATCH, 16);
        assert_eq!(super::OWED_AGE_REMARKS_PER_PASS, 128);
    }
}
