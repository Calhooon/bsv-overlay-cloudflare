//! bsv-low #781, #778 (B-FLEET-4): the scheduled pass's walls.
//!
//! The monitor's tip read times out on some ticks while the app layer's `/tip` answers quickly from outside (#781);
//! the suspect is the scheduled pass (#778: the backfill's CHAINTRACKS awaits). So each scheduled pass is measured:
//! the pass's wall (from the `scheduled` event to the end of its last task) and each phase's wall, in ONE log line
//! (`[scheduled-pass] ...`) and one D1 row ([`PASS_UPSERT_SQL`], `applayer_scheduled_pass`), which the overlay
//! serves as `/health/invariants.appLayerScheduledPass`, so a beta window can read whether `/tip`'s latency rides the
//! pass.
//!
//! The pass has four `wait_until` tasks: the pot drain, chain-event intake, tracker, and owed drain. Each phase is timed by
//! [`PassMeter::phase`] and each task tells the meter it ended ([`PassMeter::task_done`]); the task that ends LAST
//! writes the line and the row. Stated: a task the platform cancels never ends, so its pass writes nothing (the row's
//! `at` then stays at the previous pass, which is the signal); a phase a deadline dropped is timed to the drop.

use crate::pot_hook::{Bind, SqlPort, Stmt};
use std::cell::{Cell, RefCell};

/// Byte-identical to the overlay's migration (`d1::APPLAYER_SCHEDULED_PASS_CREATE`; the catch-up pin): the app layer
/// runs it before its write, so a pass before the overlay's migration still lands.
pub const CREATE: &str = "CREATE TABLE IF NOT EXISTS applayer_scheduled_pass (id INTEGER PRIMARY KEY CHECK (id = 1), at INTEGER NOT NULL, started_at INTEGER NOT NULL, wall_ms INTEGER NOT NULL, phases TEXT NOT NULL, passes INTEGER NOT NULL DEFAULT 0)";

/// The last pass, one row. `?1` the pass's end (ms), `?2` its start (ms), `?3` its wall (ms), `?4` the phases' walls
/// (a JSON object, phase to ms, in the order they ended).
pub const PASS_UPSERT_SQL: &str = "INSERT INTO applayer_scheduled_pass (id, at, started_at, wall_ms, phases, passes) \
     VALUES (1, ?1, ?2, ?3, ?4, 1) \
     ON CONFLICT(id) DO UPDATE SET at = excluded.at, started_at = excluded.started_at, \
     wall_ms = excluded.wall_ms, phases = excluded.phases, passes = applayer_scheduled_pass.passes + 1";

/// The four tasks of a scheduled pass: pot drain, chain-event intake, tracker, and owed drain.
pub const SCHEDULED_TASKS: u32 = 4;

/// One scheduled pass's clock: its start, each phase's wall, the tasks still running.
pub struct PassMeter {
    started_ms: u64,
    clock: Box<dyn Fn() -> u64>,
    phases: RefCell<Vec<(&'static str, u64)>>,
    open: Cell<u32>,
}

/// What a whole pass measured, handed to the task that ended last.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassWalls {
    pub started_ms: u64,
    pub ended_ms: u64,
    /// Each phase's wall, in the order the phases ended.
    pub phases: Vec<(&'static str, u64)>,
}

impl PassWalls {
    /// The pass's wall, from the `scheduled` event to the end of its last task.
    pub fn wall_ms(&self) -> u64 {
        self.ended_ms.saturating_sub(self.started_ms)
    }

    /// The pass's one log line.
    pub fn line(&self) -> String {
        let mut line = format!("[scheduled-pass] wall_ms={}", self.wall_ms());
        for (name, ms) in &self.phases {
            line.push_str(&format!(" {name}={ms}"));
        }
        line
    }

    /// The phases' walls as the row keeps them (a JSON object).
    pub fn phases_json(&self) -> String {
        let mut map = serde_json::Map::new();
        for (name, ms) in &self.phases {
            map.insert((*name).to_string(), serde_json::json!(ms));
        }
        serde_json::Value::Object(map).to_string()
    }

    /// The row's write, the catch-up's CREATE first, in one batch.
    pub fn statements(&self) -> Vec<Stmt> {
        vec![
            Stmt {
                sql: CREATE.into(),
                binds: vec![],
            },
            Stmt {
                sql: PASS_UPSERT_SQL.into(),
                binds: vec![
                    Bind::Int(self.ended_ms as i64),
                    Bind::Int(self.started_ms as i64),
                    Bind::Int(self.wall_ms() as i64),
                    Bind::Text(self.phases_json()),
                ],
            },
        ]
    }
}

impl PassMeter {
    /// A pass started now on `clock`, with `tasks` tasks to end.
    pub fn new(clock: Box<dyn Fn() -> u64>, tasks: u32) -> Self {
        PassMeter {
            started_ms: clock(),
            clock,
            phases: RefCell::new(Vec::new()),
            open: Cell::new(tasks),
        }
    }

    /// The worker's meter, on `Date::now`.
    pub fn worker() -> Self {
        PassMeter::new(
            Box::new(|| worker::Date::now().as_millis()),
            SCHEDULED_TASKS,
        )
    }

    /// `f`, its wall recorded under `name` when it ends.
    pub async fn phase<F: std::future::Future>(&self, name: &'static str, f: F) -> F::Output {
        let t0 = (self.clock)();
        let out = f.await;
        let ms = (self.clock)().saturating_sub(t0);
        self.phases.borrow_mut().push((name, ms));
        out
    }

    /// One task ended: `Some` (the whole pass's walls) to the LAST one, `None` to the others.
    pub fn task_done(&self) -> Option<PassWalls> {
        let left = self.open.get().saturating_sub(1);
        self.open.set(left);
        (left == 0).then(|| PassWalls {
            started_ms: self.started_ms,
            ended_ms: (self.clock)(),
            phases: self.phases.borrow().clone(),
        })
    }
}

/// The task's end: the last one logs the pass's line and writes its row (best effort: a fault is logged).
pub async fn end_task<S: SqlPort>(meter: &PassMeter, sql: Option<&S>) {
    let Some(walls) = meter.task_done() else {
        return;
    };
    let line = walls.line();
    match sql {
        Some(sql) => {
            sql.log(&line);
            if let Err(e) = sql.run_batch(walls.statements()).await {
                sql.log(&format!("[scheduled-pass] the row was not written: {e}"));
            }
        }
        None => worker::console_log!("{line}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pot_hook::tests::SqliteSql;
    use std::rc::Rc;

    fn squash(s: &str) -> String {
        s.lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<String>()
            .split_whitespace()
            .collect()
    }

    /// bsv-low #781, #778: the catch-up's CREATE is the overlay's migration, byte for byte, and the overlay's read is
    /// served from the row this module writes.
    #[test]
    fn b_fleet_4_the_create_is_the_overlays_migration() {
        assert_eq!(
            CREATE,
            bsv_overlay_cloudflare::d1::APPLAYER_SCHEDULED_PASS_CREATE
        );
        assert!(bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS.contains(&CREATE));
    }

    /// bsv-low #781, #778 (B-FLEET-4): a scheduled pass of two tasks, its phases timed on a virtual clock: the task
    /// that ends LAST writes ONE line and ONE row, the pass's wall from its start to that end and every phase's wall,
    /// through the shipped statements under real SQLite and read back by the overlay's own read; the first task to
    /// end writes nothing. A second pass replaces the row and counts it. And the worker's `scheduled` times each
    /// phase of both tasks and ends each task through the meter.
    #[tokio::test(flavor = "current_thread")]
    async fn b_fleet_4_the_last_task_writes_the_pass_and_each_phase_wall() {
        let now = Rc::new(Cell::new(1_000u64));
        let clock = {
            let now = now.clone();
            Box::new(move || now.get())
        };
        let meter = PassMeter::new(clock, SCHEDULED_TASKS);
        // The owed drain is the fourth independently bounded task.
        end_task::<SqliteSql>(&meter, None).await;
        let sql = SqliteSql::migrated();
        let step = |ms: u64| {
            let now = now.clone();
            async move { now.set(now.get() + ms) }
        };
        meter.phase("drain.boot", step(5)).await;
        meter.phase("tracker.backfill", step(20_000)).await;
        meter.phase("drain.pass", step(40)).await;
        end_task(&meter, Some(&sql)).await;
        assert!(
            sql.lines.borrow().is_empty(),
            "the first task to end writes nothing: {:?}",
            sql.lines.borrow()
        );
        // bsv-low #778: the third task (the chain-event intake, its own since)
        end_task(&meter, Some(&sql)).await;
        assert!(
            sql.lines.borrow().is_empty(),
            "the second task to end writes nothing"
        );
        meter.phase("tracker.chain_events", step(25_000)).await;
        end_task(&meter, Some(&sql)).await;
        assert_eq!(
            *sql.lines.borrow(),
            vec!["[scheduled-pass] wall_ms=45045 drain.boot=5 tracker.backfill=20000 drain.pass=40 tracker.chain_events=25000".to_string()]
        );
        let read = |conn: &rusqlite::Connection| {
            conn.query_row(
                bsv_overlay_cloudflare::ops::APPLAYER_SCHEDULED_PASS_READ_SQL,
                [],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, i64>(4)?,
                    ))
                },
            )
            .unwrap()
        };
        let (at, started, wall, phases, passes) = read(&sql.conn);
        assert_eq!((at, started, wall, passes), (46_045, 1_000, 45_045, 1));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&phases).unwrap(),
            serde_json::json!({"drain.boot": 5, "tracker.backfill": 20000, "drain.pass": 40, "tracker.chain_events": 25000})
        );
        let second = PassMeter::new(
            {
                let now = now.clone();
                Box::new(move || now.get())
            },
            1,
        );
        second.phase("tracker.tick", step(7)).await;
        end_task(&second, Some(&sql)).await;
        let (_, _, wall, phases, passes) = read(&sql.conn);
        assert_eq!(
            (wall, passes),
            (7, 2),
            "the row is the LAST pass's, counted"
        );
        assert_eq!(phases, r#"{"tracker.tick":7}"#);

        // the worker's wiring: every phase of both tasks is timed, each task ends through the meter
        let lib = squash(include_str!("lib.rs"));
        let sched = &lib[lib.find("asyncfnscheduled(").expect("scheduled")..];
        let sched = &sched[..sched.find("#[event(fetch)]").unwrap_or(sched.len())];
        assert!(sched.contains("letmeter=std::rc::Rc::new(pass_meter::PassMeter::worker());"));
        for phase in [
            "\"drain.boot\"",
            "\"drain.pass\"",
            "\"drain.flush\"",
            "\"tracker.boot\"",
            "\"tracker.board_era\"",
            "\"tracker.backfill\"",
            "\"tracker.chain_events\"",
            "\"tracker.tick\"",
            "\"tracker.sweep\"",
        ] {
            assert!(
                sched.contains(&format!(".phase({phase},")),
                "the scheduled pass times {phase}"
            );
        }
        assert_eq!(
            sched.matches("pass_meter::end_task(").count(),
            4,
            "each of the four tasks ends through the meter"
        );
    }
}
