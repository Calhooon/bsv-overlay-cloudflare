//! D1 coordination and bounded activity history for the two tick triggers.

use serde::Deserialize;
use worker::D1Database;

use crate::d1::Query;
use crate::tick::{TickReport, TickSource, TICK_LEASE_MS};

pub(crate) const LEASE_CREATE: &str = "CREATE TABLE IF NOT EXISTS ops_lease (
    name TEXT PRIMARY KEY, holder TEXT NOT NULL, until_ms INTEGER NOT NULL
)";
pub(crate) const EVENTS_CREATE: &str = "CREATE TABLE IF NOT EXISTS ops_tick_events (
    holder TEXT PRIMARY KEY, at_ms INTEGER NOT NULL, source TEXT NOT NULL,
    skipped INTEGER NOT NULL
)";
pub(crate) const EVENTS_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS idx_ops_tick_events_at ON ops_tick_events(at_ms)";
pub(crate) const SOURCE_COLUMN: &str = "ALTER TABLE ops_heartbeat ADD COLUMN tick_source TEXT";
pub(crate) const AT_COLUMN: &str = "ALTER TABLE ops_heartbeat ADD COLUMN tick_at_ms INTEGER";

const LEASE_READ: &str = "SELECT holder, until_ms FROM ops_lease WHERE name = 'tick'";
// A concurrent loser gets the current holder in RETURNING, without another
// read. The usual double fire returns from LEASE_READ without this write.
const LEASE_CLAIM: &str = "INSERT INTO ops_lease (name, holder, until_ms) VALUES ('tick', ?1, ?2)
    ON CONFLICT(name) DO UPDATE SET
        holder = CASE WHEN ops_lease.until_ms <= ?3 THEN excluded.holder ELSE ops_lease.holder END,
        until_ms = CASE WHEN ops_lease.until_ms <= ?3 THEN excluded.until_ms ELSE ops_lease.until_ms END
    RETURNING holder, until_ms";
const LEASE_RELEASE: &str = "DELETE FROM ops_lease WHERE name = 'tick' AND holder = ?1";
const EVENT_INSERT: &str = "INSERT INTO ops_tick_events (holder, at_ms, source, skipped)
    VALUES (?1, ?2, ?3, ?4)";
const EVENT_PRUNE: &str = "DELETE FROM ops_tick_events WHERE at_ms < ?1";
const HEARTBEAT: &str = "INSERT INTO ops_heartbeat (id, tick_source, tick_at_ms) VALUES (0, ?1, ?2)
    ON CONFLICT(id) DO UPDATE SET tick_source = excluded.tick_source, tick_at_ms = excluded.tick_at_ms";
const LAST_TICK: &str = "SELECT tick_source, tick_at_ms FROM ops_heartbeat WHERE id = 0";
const ACTIVITY: &str = "SELECT
    COALESCE(SUM(source = 'cron'), 0) AS cron,
    COALESCE(SUM(source = 'alarm'), 0) AS alarm,
    COALESCE(SUM(skipped), 0) AS skips
    FROM ops_tick_events WHERE at_ms >= ?1 AND at_ms <= ?2";
pub(crate) const DAY_MS: i64 = 86_400_000;

#[derive(Debug, Deserialize)]
pub(crate) struct Lease {
    pub holder: String,
    pub until_ms: f64,
}

#[async_trait::async_trait(?Send)]
trait LeaseStore {
    async fn read(&self) -> Result<Option<Lease>, String>;
    async fn claim(&self, holder: &str, now: i64) -> Result<Lease, String>;
}

#[async_trait::async_trait(?Send)]
impl LeaseStore for D1Database {
    async fn read(&self) -> Result<Option<Lease>, String> {
        Query::new(LEASE_READ).fetch_optional(self).await
    }

    async fn claim(&self, holder: &str, now: i64) -> Result<Lease, String> {
        Query::new(LEASE_CLAIM)
            .bind(holder)
            .bind(now.saturating_add(TICK_LEASE_MS))
            .bind(now)
            .fetch_optional(self)
            .await?
            .ok_or_else(|| "tick lease claim returned no row".to_string())
    }
}

async fn take_with(store: &impl LeaseStore, holder: &str, now: i64) -> Result<Lease, String> {
    if let Some(lease) = store.read().await? {
        if lease.until_ms > now as f64 {
            return Ok(lease);
        }
    }
    store.claim(holder, now).await
}

pub(crate) async fn take(db: &D1Database, holder: &str, now: i64) -> Result<Lease, String> {
    match take_with(db, holder, now).await {
        Err(e) if e.contains("no such table: ops_lease") => {
            crate::d1::ensure_overlay_migrations(db).await?;
            take_with(db, holder, crate::tick::now_ms()).await
        }
        result => result,
    }
}

pub(crate) async fn release(db: &D1Database, holder: &str) -> Result<(), String> {
    Query::new(LEASE_RELEASE).bind(holder).execute(db).await
}

pub(crate) async fn record_attempt(
    db: &D1Database,
    holder: &str,
    source: TickSource,
    now: i64,
    skipped: bool,
) {
    if let Err(e) = Query::new(EVENT_INSERT)
        .bind(holder)
        .bind(now)
        .bind(source.as_str())
        .bind(i64::from(skipped))
        .execute(db)
        .await
    {
        worker::console_log!("tick activity write failed: {e}");
    }
    // Keep pruning with the holder's work. The duplicate path costs one D1
    // read plus its observation write, with no migration or engine build.
    if !skipped {
        if let Err(e) = Query::new(EVENT_PRUNE).bind(now - DAY_MS).execute(db).await {
            worker::console_log!("tick activity prune failed: {e}");
        }
    }
}

pub(crate) async fn record_heartbeat(db: &D1Database, report: &TickReport) {
    if let Err(e) = Query::new(HEARTBEAT)
        .bind(report.source.as_str())
        .bind(report.at_ms)
        .execute(db)
        .await
    {
        worker::console_log!("tick source heartbeat failed: {e}");
    }
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct LastTick {
    pub tick_source: Option<String>,
    pub tick_at_ms: Option<f64>,
}

pub(crate) async fn last_tick(db: &D1Database) -> Result<LastTick, String> {
    Query::new(LAST_TICK)
        .fetch_optional(db)
        .await
        .map(Option::unwrap_or_default)
}

#[derive(Debug, Deserialize)]
pub(crate) struct Activity {
    pub cron: f64,
    pub alarm: f64,
    pub skips: f64,
}

pub(crate) async fn activity(db: &D1Database, now: i64) -> Result<Option<Activity>, String> {
    Query::new(ACTIVITY)
        .bind(now - DAY_MS)
        .bind(now)
        .fetch_optional(db)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{params, Connection, OptionalExtension};
    use std::cell::Cell;

    struct Sqlite {
        db: Connection,
        reads: Cell<usize>,
        claims: Cell<usize>,
    }

    impl Sqlite {
        fn new() -> Self {
            let db = Connection::open_in_memory().unwrap();
            for sql in crate::d1::OVERLAY_MIGRATIONS {
                if let Err(e) = db.execute_batch(sql) {
                    assert!(
                        crate::d1::migration_error_is_benign(sql, &e.to_string()),
                        "{e}: {sql}"
                    );
                }
            }
            Self {
                db,
                reads: Cell::new(0),
                claims: Cell::new(0),
            }
        }
    }

    fn lease(row: &rusqlite::Row<'_>) -> rusqlite::Result<Lease> {
        Ok(Lease {
            holder: row.get(0)?,
            until_ms: row.get(1)?,
        })
    }

    #[async_trait::async_trait(?Send)]
    impl LeaseStore for Sqlite {
        async fn read(&self) -> Result<Option<Lease>, String> {
            self.reads.set(self.reads.get() + 1);
            self.db
                .query_row(LEASE_READ, [], lease)
                .optional()
                .map_err(|e| e.to_string())
        }

        async fn claim(&self, holder: &str, now: i64) -> Result<Lease, String> {
            self.claims.set(self.claims.get() + 1);
            self.db
                .query_row(
                    LEASE_CLAIM,
                    params![holder, now + TICK_LEASE_MS, now],
                    lease,
                )
                .map_err(|e| e.to_string())
        }
    }

    #[tokio::test]
    async fn two_takers_one_wins_and_duplicate_costs_one_read() {
        let s = Sqlite::new();
        assert_eq!(take_with(&s, "cron-a", 100).await.unwrap().holder, "cron-a");
        let (reads, claims) = (s.reads.get(), s.claims.get());
        let other = take_with(&s, "alarm-b", 100).await.unwrap();
        assert_eq!(other.holder, "cron-a");
        assert_eq!(other.until_ms, (100 + TICK_LEASE_MS) as f64);
        assert_eq!(s.reads.get() - reads, 1);
        assert_eq!(s.claims.get(), claims);
    }

    #[tokio::test]
    async fn competing_claims_after_the_same_empty_read_cannot_both_win() {
        let s = Sqlite::new();
        assert!(s.read().await.unwrap().is_none());
        assert!(s.read().await.unwrap().is_none());
        assert_eq!(s.claim("cron-a", 100).await.unwrap().holder, "cron-a");
        assert_eq!(s.claim("alarm-b", 100).await.unwrap().holder, "cron-a");
    }

    #[tokio::test]
    async fn expired_lease_is_taken_and_old_holder_cannot_release_it() {
        let s = Sqlite::new();
        take_with(&s, "cron-a", 100).await.unwrap();
        let now = 100 + TICK_LEASE_MS;
        assert_eq!(
            take_with(&s, "alarm-b", now - 1).await.unwrap().holder,
            "cron-a"
        );
        assert_eq!(
            take_with(&s, "alarm-b", now).await.unwrap().holder,
            "alarm-b"
        );
        assert_eq!(s.db.execute(LEASE_RELEASE, ["cron-a"]).unwrap(), 0);
        assert_eq!(s.read().await.unwrap().unwrap().holder, "alarm-b");
        assert_eq!(s.db.execute(LEASE_RELEASE, ["alarm-b"]).unwrap(), 1);
        assert!(s.read().await.unwrap().is_none());
        assert_eq!(
            take_with(&s, "cron-c", now + 1).await.unwrap().holder,
            "cron-c"
        );
    }

    #[test]
    fn activity_window_and_source_heartbeat_preserve_completion_fields() {
        let s = Sqlite::new();
        let now = 2 * DAY_MS;
        for (holder, at, source, skipped) in [
            ("old", now - DAY_MS - 1, "cron", 1),
            ("edge", now - DAY_MS, "cron", 0),
            ("alarm", now, "alarm", 0),
            ("skip", now, "cron", 1),
            ("future", now + 1, "alarm", 1),
        ] {
            s.db.execute(EVENT_INSERT, params![holder, at, source, skipped])
                .unwrap();
        }
        let counts: (i64, i64, i64) =
            s.db.query_row(ACTIVITY, params![now - DAY_MS, now], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .unwrap();
        assert_eq!(counts, (2, 1, 1));
        assert_eq!(s.db.execute(EVENT_PRUNE, [now - DAY_MS]).unwrap(), 1);
        s.db.execute(
            "INSERT INTO ops_heartbeat (id, last_tick_ms, tick_count) VALUES (0, 123, 7)",
            [],
        )
        .unwrap();
        s.db.execute(HEARTBEAT, params!["alarm", now]).unwrap();
        let row: (i64, i64, String, i64) = s.db.query_row(
            "SELECT last_tick_ms, tick_count, tick_source, tick_at_ms FROM ops_heartbeat WHERE id = 0", [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        ).unwrap();
        assert_eq!(row, (123, 7, "alarm".to_string(), now));
    }
}
