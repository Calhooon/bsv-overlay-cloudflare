//! bsv-low #635 (B-COST-1): the cost bar on `/health/invariants.costBar`.
//!
//! Both Workers of one D1 (the overlay and the app layer) write what each isolate cost the plan into
//! `worker_cost_daily`, one row per UTC day and Worker (`d1_ledger`'s cost window: fetch requests, D1 rows read,
//! rows written and statements, written one upsert per window, never one per request). This module reads the last
//! `COST_DAYS_SERVED` days and serves each beside the plan's INCLUDED quotas per day, which are CONFIGURED VARS
//! (`QUOTA_VARS`), never constants here: an unset or unparsable var serves `null` for its quota, its ratio and its
//! share of the over-quota flag. The ratio is the day's total over BOTH Workers divided by the quota; `overQuota` is
//! `true` when any configured measure's total is past its quota, `false` when every configured one is within it,
//! `null` when none is configured or the rows are unreadable.
//!
//! Stated: the plan's quotas are the ACCOUNT's (every Worker and D1 of it, monthly); a per-day var is the share the
//! operator gives these two Workers. Requests are fetch requests served by this code (a cron, queue or Durable
//! Object alarm invocation is not counted as one; its D1 statements are). Today is a running figure.

use crate::d1::Query;
use serde::Deserialize;
use serde_json::{json, Value};
use worker::D1Database;

/// bsv-low #635: the days served, today included.
pub const COST_DAYS_SERVED: i64 = 7;

/// bsv-low #635: (the served measure, the var that configures its included quota per day).
pub const QUOTA_VARS: [(&str, &str); 3] = [
    ("requests", "COST_QUOTA_REQUESTS_PER_DAY"),
    ("d1RowsRead", "COST_QUOTA_D1_ROWS_READ_PER_DAY"),
    ("d1RowsWritten", "COST_QUOTA_D1_ROWS_WRITTEN_PER_DAY"),
];

/// bsv-low #635: the served days' rows, a range of the primary key (at most two Workers a day are written; the
/// LIMIT bounds the read whatever else writes the table).
pub const COST_DAILY_READ_SQL: &str = "SELECT day, worker, requests, rowsRead, rowsWritten, statements, updatedAt FROM worker_cost_daily WHERE day >= ?1 ORDER BY day DESC, worker LIMIT 64";

/// One row of `worker_cost_daily`.
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CostRow {
    /// The UTC day, `YYYY-MM-DD`.
    pub day: String,
    /// The Worker (its crate's package name).
    pub worker: String,
    /// Fetch requests.
    pub requests: f64,
    /// D1 rows read.
    pub rows_read: f64,
    /// D1 rows written.
    pub rows_written: f64,
    /// D1 statements.
    pub statements: f64,
    /// The last write, unix ms.
    pub updated_at: f64,
}

/// The configured quotas per day, in `QUOTA_VARS`' order.
pub type Quotas = [Option<u64>; 3];

/// PURE: the quotas from a var reader (a positive integer, `_` separators allowed; anything else is unset).
pub fn quotas_from(get: impl Fn(&str) -> Option<String>) -> Quotas {
    QUOTA_VARS.map(|(_, var)| {
        get(var)
            .and_then(|v| v.trim().replace('_', "").parse::<u64>().ok())
            .filter(|n| *n > 0)
    })
}

/// The quotas this Worker is configured with.
pub fn quotas_from_env(env: &worker::Env) -> Quotas {
    quotas_from(|name| env.var(name).ok().map(|v| v.to_string()))
}

/// Read the served days' rows.
pub async fn read(db: &D1Database, now_ms: i64) -> Result<Vec<CostRow>, String> {
    let today = crate::d1_ledger::utc_day(now_ms as f64);
    let first = crate::d1_ledger::utc_date(today - (COST_DAYS_SERVED - 1));
    Query::new(COST_DAILY_READ_SQL)
        .bind(first)
        .fetch_all::<CostRow>(db)
        .await
}

fn measures(rows: &[&CostRow]) -> [u64; 4] {
    let mut out = [0u64; 4];
    for r in rows {
        for (i, v) in [r.requests, r.rows_read, r.rows_written, r.statements]
            .into_iter()
            .enumerate()
        {
            out[i] = out[i].saturating_add(v.max(0.0) as u64);
        }
    }
    out
}

fn figures(m: [u64; 4]) -> Value {
    json!({ "requests": m[0], "d1RowsRead": m[1], "d1RowsWritten": m[2], "statements": m[3] })
}

/// PURE: one day's entry: each Worker's figures, the total, the ratio to each configured quota and the flag.
fn day_view(day: &str, rows: &[&CostRow], quotas: &Quotas) -> Value {
    let mut by_worker = serde_json::Map::new();
    for r in rows {
        let mut f = figures(measures(&[r]));
        f["updatedAt"] = json!(r.updated_at as i64);
        by_worker.insert(r.worker.clone(), f);
    }
    let total = measures(rows);
    let mut ratio = serde_json::Map::new();
    let mut over: Vec<&str> = Vec::new();
    let mut configured = false;
    for (i, (name, _)) in QUOTA_VARS.iter().enumerate() {
        match quotas[i] {
            Some(q) => {
                configured = true;
                ratio.insert((*name).to_string(), json!(total[i] as f64 / q as f64));
                if total[i] > q {
                    over.push(name);
                }
            }
            None => {
                ratio.insert((*name).to_string(), Value::Null);
            }
        }
    }
    json!({
        "day": day,
        "byWorker": by_worker,
        "total": figures(total),
        "ratio": ratio,
        "overQuota": if configured { json!(!over.is_empty()) } else { Value::Null },
        "overQuotaMeasures": over,
    })
}

/// PURE (bsv-low #635): `/health/invariants.costBar` from the rows read (or the read's fault), the quotas and now.
/// Every served day is present (a day with no row is all zeros), newest first.
pub fn view(rows: Result<&[CostRow], &str>, quotas: &Quotas, now_ms: i64) -> Value {
    let today = crate::d1_ledger::utc_day(now_ms as f64);
    let mut quotas_json = serde_json::Map::new();
    let mut vars_json = serde_json::Map::new();
    for (i, (name, var)) in QUOTA_VARS.iter().enumerate() {
        quotas_json.insert((*name).to_string(), json!(quotas[i]));
        vars_json.insert((*name).to_string(), json!(var));
    }
    let mut out = json!({
        "unit": "per UTC day, both Workers of this D1 summed: fetch requests, D1 meta.rows_read / rows_written / statements (each Worker's isolates write a window at most every 64 requests or 60 s; an isolate's unwritten tail is an under-count)",
        "today": crate::d1_ledger::utc_date(today),
        "quotasPerDay": quotas_json,
        "quotaVars": vars_json,
        "flushBound": { "requests": crate::d1_ledger::COST_FLUSH_REQUESTS, "ms": crate::d1_ledger::COST_FLUSH_MS as u64 },
    });
    match rows {
        Ok(rows) => {
            let days: Vec<Value> = (0..COST_DAYS_SERVED)
                .map(|back| {
                    let day = crate::d1_ledger::utc_date(today - back);
                    let of_day: Vec<&CostRow> = rows.iter().filter(|r| r.day == day).collect();
                    day_view(&day, &of_day, quotas)
                })
                .collect();
            out["overQuota"] = days[0]["overQuota"].clone();
            out["overQuotaDays"] = json!(days
                .iter()
                .filter(|d| d["overQuota"] == json!(true))
                .count());
            out["days"] = json!(days);
            out["readable"] = json!(true);
        }
        Err(e) => {
            out["overQuota"] = Value::Null;
            out["days"] = json!([]);
            out["readable"] = json!(false);
            out["fault"] = json!(e);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(day: &str, worker: &str, req: f64, read: f64, written: f64) -> CostRow {
        CostRow {
            day: day.into(),
            worker: worker.into(),
            requests: req,
            rows_read: read,
            rows_written: written,
            statements: req * 2.0,
            updated_at: 1.0,
        }
    }

    const NOW: i64 = 1_792_756_800_000; // 2026-10-23T12:00:00Z

    #[test]
    fn b_cost_1_the_view_serves_each_day_beside_its_configured_quotas_with_a_ratio_and_a_flag() {
        let rows = vec![
            row(
                "2026-10-23",
                "bsv-overlay-cloudflare",
                100.0,
                900_000.0,
                10.0,
            ),
            row("2026-10-23", "low-app-layer", 50.0, 200_000.0, 5.0),
            row("2026-10-22", "low-app-layer", 10.0, 10.0, 1.0),
            row("2026-10-01", "low-app-layer", 1e9, 1e12, 1e9), // outside the served days
        ];
        let quotas = quotas_from(|v| match v {
            "COST_QUOTA_REQUESTS_PER_DAY" => Some("1_000".into()),
            "COST_QUOTA_D1_ROWS_READ_PER_DAY" => Some(" 1000000 ".into()),
            _ => None, // rows written: unset
        });
        assert_eq!(quotas, [Some(1_000), Some(1_000_000), None]);
        let v = view(Ok(&rows), &quotas, NOW);
        assert_eq!(v["today"], "2026-10-23");
        assert_eq!(v["days"].as_array().unwrap().len(), 7);
        let today = &v["days"][0];
        assert_eq!(
            today["total"],
            json!({ "requests": 150, "d1RowsRead": 1_100_000, "d1RowsWritten": 15, "statements": 300 })
        );
        assert_eq!(today["byWorker"]["low-app-layer"]["d1RowsRead"], 200_000);
        assert_eq!(
            today["ratio"],
            json!({ "requests": 0.15, "d1RowsRead": 1.1, "d1RowsWritten": null })
        );
        assert_eq!(
            (
                today["overQuota"].clone(),
                today["overQuotaMeasures"].clone()
            ),
            (json!(true), json!(["d1RowsRead"]))
        );
        assert_eq!(v["overQuota"], true);
        assert_eq!(
            v["days"][1]["overQuota"], false,
            "yesterday within both configured quotas"
        );
        assert_eq!(
            v["days"][2]["total"]["requests"], 0,
            "a day with no row is zeros, not absent"
        );
        assert_eq!(v["overQuotaDays"], 1);
        assert_eq!(
            v["quotasPerDay"],
            json!({ "requests": 1_000, "d1RowsRead": 1_000_000, "d1RowsWritten": null })
        );
        assert_eq!(v["readable"], true);
    }

    #[test]
    fn b_cost_1_no_quota_configured_is_no_verdict_and_an_unreadable_table_is_not_zero() {
        let none = quotas_from(|v| (v == "COST_QUOTA_REQUESTS_PER_DAY").then(|| "0".into()));
        assert_eq!(none, [None, None, None], "zero and unset are no quota");
        let v = view(Ok(&[]), &none, NOW);
        assert_eq!(v["overQuota"], Value::Null);
        assert_eq!(v["days"][0]["ratio"]["requests"], Value::Null);
        let v = view(
            Err("no such table: worker_cost_daily"),
            &[Some(1), Some(1), Some(1)],
            NOW,
        );
        assert_eq!(
            (v["readable"].clone(), v["overQuota"].clone()),
            (json!(false), Value::Null)
        );
        assert_eq!(v["days"], json!([]));
    }

    /// The shipped statements under real SQLite: the migration creates the table, two windows of one Worker's day
    /// ADD (never replace), and the served read is a search of the primary key.
    #[test]
    fn b_cost_1_the_upsert_adds_windows_and_the_read_is_a_key_range() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        assert!(crate::d1::OVERLAY_MIGRATIONS.contains(&crate::d1_ledger::COST_DAILY_CREATE));
        conn.execute_batch(crate::d1_ledger::COST_DAILY_CREATE)
            .unwrap();
        // bsv-low #911: the queue family's column, appended
        conn.execute_batch(crate::d1_ledger::COST_DAILY_QUEUE_ALTER)
            .unwrap();
        for (w, req, read) in [
            ("low-app-layer", 64, 1_000),
            ("low-app-layer", 3, 20),
            ("bsv-overlay-cloudflare", 1, 5),
        ] {
            conn.execute(
                crate::d1_ledger::COST_DAILY_UPSERT_SQL,
                rusqlite::params!["2026-10-23", w, req as f64, read as f64, 2.0, 4.0, 9.0, 1.0],
            )
            .unwrap();
        }
        let mut stmt = conn.prepare(COST_DAILY_READ_SQL).unwrap();
        let got: Vec<(String, String, f64, f64, f64, f64)> = stmt
            .query_map(["2026-10-17"], |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                ))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            got,
            vec![
                (
                    "2026-10-23".into(),
                    "bsv-overlay-cloudflare".into(),
                    1.0,
                    5.0,
                    2.0,
                    4.0
                ),
                (
                    "2026-10-23".into(),
                    "low-app-layer".into(),
                    67.0,
                    1_020.0,
                    4.0,
                    8.0
                ),
            ]
        );
        let plan: String = conn
            .query_row(
                &format!("EXPLAIN QUERY PLAN {COST_DAILY_READ_SQL}"),
                ["2026-10-17"],
                |r| r.get(3),
            )
            .unwrap();
        assert!(
            plan.contains(
                "SEARCH worker_cost_daily USING INDEX sqlite_autoindex_worker_cost_daily_1"
            ),
            "{plan}"
        );
    }
}

/// bsv-low #635: the overlay's doors write the cost window, and the health body serves it.
#[cfg(test)]
mod b_cost_1_wiring {
    fn squash(s: &str) -> String {
        s.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    #[test]
    fn b_cost_1_every_overlay_door_counts_and_writes_the_window_and_health_serves_it() {
        let lib = squash(include_str!("lib.rs"));
        let dispatch = &lib[lib.find("async fn dispatch(").unwrap()..];
        let dispatch = &dispatch[..dispatch.find("// CORS preflight").unwrap()];
        assert!(
            dispatch.contains("crate::d1_ledger::cost_on_request(")
                && dispatch.contains("crate::d1_ledger::flush_cost(&db, window, now).await;"),
            "the fetch door counts the request and hands a due window to wait_until, before any route answers"
        );
        let cron = &lib[lib.find("async fn scheduled(").unwrap()..];
        let cron = &cron[..cron.find("async fn flush_cost_if_due(").unwrap()];
        assert!(
            cron.contains("flush_cost_if_due(&env).await"),
            "the cron writes a due window: {cron}"
        );
        let queue = &lib[lib.find("async fn queue_entry(").unwrap()..];
        let queue = &queue[..queue.find("async fn queue_handler(").unwrap()];
        assert_eq!(
            queue.matches("flush_cost_if_due(&").count(),
            2,
            "both queue arms write a due window"
        );
        let ops = squash(include_str!("ops.rs"));
        assert!(ops.contains("cost_rows = crate::cost_bar::read(db, now),"));
        assert!(ops.contains("body[\"costBar\"] = crate::cost_bar::view("));
        let ledger = include_str!("d1_ledger.rs");
        let note = &ledger[ledger
            .find("pub fn note(reads: u64, writes: u64) {")
            .unwrap()..];
        assert!(
            note[..note.find("\n}\n").unwrap()].contains("note_cost(reads, writes);"),
            "every noted statement is costed"
        );
    }
}
