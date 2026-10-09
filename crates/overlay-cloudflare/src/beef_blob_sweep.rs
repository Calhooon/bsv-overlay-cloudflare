//! bsv-low #585, door 3's fold: THE ORPHAN SWEEP of the queue's R2 objects (`BEEF_BLOBS`).
//!
//! Door 3 deletes an object when the message naming it is acked, when its dead letter is LOST or dropped as the
//! lighter copy, and on the operator's discard (`queue.rs`, the deletion rule). An object NOTHING names stayed for
//! good: a send that faulted after its write and was never re-presented, a delete that faulted, a batch that died
//! between its acks and its deletes, a message the platform dropped without the LOST line, a letter deleted by
//! SQL. Nothing listed them.
//!
//! ## The rule
//!
//! The scheduled tick (`*/15`) runs ONE PASS: it lists at most [`SWEEP_MAX_OBJECTS`] objects under
//! [`SWEEP_PREFIX`] from the key the last pass stopped at (the cursor is the last KEY, at rest in D1,
//! `beef_blob_sweep`: R2 lists in key order, and a key outlives any listing token), and deletes each object that
//! is both
//!
//! 1. OLDER than [`ORPHAN_WINDOW_S`] by R2's own `uploaded` stamp, and
//! 2. named by NO dead letter row (`mutation_dead_letters.r2_key`, whatever the row's status),
//!
//! at most [`SWEEP_MAX_DELETES`] a pass (a pass that meets more stops there and the next goes on from it). Before
//! each delete the object is read again (`head`): one written again since the listing (a client's
//! re-presentation writes the same key and a new stamp) is left.
//!
//! ## Why the window is what no queue message can outlive
//!
//! An object's names are queue messages and dead letter rows. A row is read (2). A message carries no stamp the
//! sweep can read, so the sweep waits until none can be left: every message naming an object was sent right
//! after a write of it (the door writes, then sends), so no message naming it is younger than the object's
//! `uploaded` stamp allows; the lever's re-drive sends without a write, and its row names the object from the
//! park to the ack. A message lives in the main queue at most that queue's retention, then in the dead letter
//! queue at most that queue's ([`QUEUE_RETENTION_S`] each, the platform's default of four days: neither queue
//! sets another), where it is parked (a row), LOST (its object deleted) or dropped by the platform. Hence two
//! retentions.
//!
//! #576's dead-letter window (about 48 h: 100 retries of 60 s doubling to 30 min, `dead_letters::dlq_retry_plan`)
//! is NOT enough alone: it starts at the message's first delivery from the dead letter queue, after its whole
//! life in the main queue, so a letter deferred at the ceiling to its last delivery, and parked there, names an
//! object already older than 48 h. An object swept under a live name is a replay with no bytes (a fault letter
//! unless its subject landed, the twin rule), so the window errs long: an orphan costs its bytes for eight days.
//!
//! Counted `queue_r2_orphans_swept_total` and `queue_r2_orphans_swept_bytes_total` (a delete that faults:
//! `queue_r2_orphan_sweep_faults_total`), each sweep logged `[beef-blobs] SWEPT`. `/health/invariants.queue.r2`
//! serves the objects AT REST from the listing itself: the count and bytes of the last complete round over the
//! bucket, and the round in progress.

use crate::d1::Query;
use serde::Deserialize;
use std::collections::HashSet;
use worker::{D1Database, Env};

/// The objects the door writes (`queue::r2_key`); nothing else in the bucket is listed or touched.
pub const SWEEP_PREFIX: &str = "mutations/";
/// One pass lists at most this many objects.
pub const SWEEP_MAX_OBJECTS: u32 = 200;
/// One pass deletes at most this many (each a `head` and a `delete`): a pass that meets more stops at the last
/// one it handled and the next pass goes on from there.
pub const SWEEP_MAX_DELETES: usize = 50;
/// The pass's wall-clock slice in the scheduled tick (one list, at most three D1 statements and 100 R2 calls). A
/// dropped pass wrote no cursor and is made again.
pub const SWEEP_BUDGET_MS: u64 = 30_000;
/// Cloudflare Queues' default message retention, four days. Neither the mutation queue nor its dead letter queue
/// sets another (it is set at `wrangler queues create`, not in a wrangler config: the operator's `wrangler queues
/// info` says); a queue given a LONGER retention needs this raised with it.
pub const QUEUE_RETENTION_S: u64 = 345_600;
/// An object older than this that no dead letter row names is an orphan: a message's whole life in the main
/// queue, then in the dead letter queue (the module doc).
pub const ORPHAN_WINDOW_S: u64 = 2 * QUEUE_RETENTION_S;

/// The sweep's state at rest: one row. Transient (a lost row restarts the round at the first key).
pub const SWEEP_STATE_CREATE: &str = "CREATE TABLE IF NOT EXISTS beef_blob_sweep (id INTEGER PRIMARY KEY CHECK (id = 1), start_after TEXT NOT NULL DEFAULT '', round_objects INTEGER NOT NULL DEFAULT 0, round_bytes INTEGER NOT NULL DEFAULT 0, round_started_at INTEGER, full_objects INTEGER, full_bytes INTEGER, full_at INTEGER, last_pass_at INTEGER, last_listed INTEGER NOT NULL DEFAULT 0, last_swept INTEGER NOT NULL DEFAULT 0)";
pub const SWEEP_STATE_SQL: &str = "SELECT start_after, round_objects, round_bytes, round_started_at, full_objects, full_bytes, full_at, last_pass_at, last_listed, last_swept FROM beef_blob_sweep WHERE id = 1";
/// Binds: start_after, round_objects, round_bytes, round_started_at, full_objects, full_bytes, full_at,
/// last_pass_at, last_listed, last_swept.
pub const SWEEP_STATE_SAVE_SQL: &str = "INSERT INTO beef_blob_sweep (id, start_after, round_objects, round_bytes, round_started_at, full_objects, full_bytes, full_at, last_pass_at, last_listed, last_swept) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) \
     ON CONFLICT(id) DO UPDATE SET start_after = excluded.start_after, round_objects = excluded.round_objects, round_bytes = excluded.round_bytes, round_started_at = excluded.round_started_at, full_objects = excluded.full_objects, full_bytes = excluded.full_bytes, full_at = excluded.full_at, last_pass_at = excluded.last_pass_at, last_listed = excluded.last_listed, last_swept = excluded.last_swept";
/// Every object a dead letter row names, whatever its status (at most the ceiling's 2000 rows hold one). Read
/// only by a pass that listed an object past the window.
pub const NAMED_KEYS_SQL: &str =
    "SELECT r2_key FROM mutation_dead_letters WHERE r2_key IS NOT NULL";

/// One listed object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listed {
    pub key: String,
    pub bytes: u64,
    /// R2's `uploaded` stamp, ms.
    pub uploaded_ms: i64,
}

/// The state at rest (`beef_blob_sweep`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepState {
    /// The last key the last pass handled; empty at the start of a round.
    pub start_after: String,
    /// The objects and bytes left at rest by the passes of the round in progress.
    pub round_objects: u64,
    pub round_bytes: u64,
    pub round_started_at: Option<i64>,
    /// The last COMPLETE round: every object under the prefix, as the listing said.
    pub full_objects: Option<u64>,
    pub full_bytes: Option<u64>,
    pub full_at: Option<i64>,
    pub last_pass_at: Option<i64>,
    pub last_listed: u64,
    pub last_swept: u64,
}

/// PURE: is the object past the window?
#[must_use]
pub fn past_window(o: &Listed, now_ms: i64, window_s: u64) -> bool {
    now_ms.saturating_sub(o.uploaded_ms) > (window_s as i64).saturating_mul(1000)
}

/// PURE: does the page hold an object past the window (the only case the named keys are read for)?
#[must_use]
pub fn any_past_window(listed: &[Listed], now_ms: i64, window_s: u64) -> bool {
    listed.iter().any(|o| past_window(o, now_ms, window_s))
}

/// What one pass does with its page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassPlan {
    /// How many of the page's objects, from its first, this pass handles (all of them unless the delete budget
    /// ran out: the cursor stops at the last handled).
    pub handled: usize,
    /// The indexes (into the page) of the orphans to delete: past the window and named by no dead letter.
    pub orphans: Vec<usize>,
}

/// PURE: the pass over one page. `named` is every key a dead letter row names.
#[must_use]
pub fn plan_pass(
    listed: &[Listed],
    named: &HashSet<String>,
    now_ms: i64,
    window_s: u64,
    max_deletes: usize,
) -> PassPlan {
    let mut orphans = Vec::new();
    for (i, o) in listed.iter().enumerate() {
        if past_window(o, now_ms, window_s) && !named.contains(&o.key) {
            if orphans.len() == max_deletes {
                return PassPlan {
                    handled: i,
                    orphans,
                };
            }
            orphans.push(i);
        }
    }
    PassPlan {
        handled: listed.len(),
        orphans,
    }
}

/// PURE: the read just before a delete. The object is deleted only if it is still there under the stamp the
/// listing gave: one written again since (the same key, a new stamp, a new message naming it) is left.
#[must_use]
pub fn still_orphan(listed: &Listed, head: Option<&Listed>) -> bool {
    head.is_some_and(|h| h.uploaded_ms == listed.uploaded_ms)
}

/// PURE: the state after a pass. `handled` are the page's objects the pass handled, `swept` the keys it deleted,
/// `more` whether the bucket holds keys past the last handled one (the listing was truncated, or the delete
/// budget stopped the pass inside its page).
#[must_use]
pub fn next_state(
    state: &SweepState,
    handled: &[Listed],
    swept: &HashSet<String>,
    more: bool,
    now_ms: i64,
) -> SweepState {
    let at_rest = handled.iter().filter(|o| !swept.contains(&o.key));
    let (objects, bytes) = at_rest.fold((0u64, 0u64), |(c, b), o| (c + 1, b + o.bytes));
    let round_objects = state.round_objects + objects;
    let round_bytes = state.round_bytes + bytes;
    let mut next = SweepState {
        last_pass_at: Some(now_ms),
        last_listed: handled.len() as u64,
        last_swept: swept.len() as u64,
        ..state.clone()
    };
    if more {
        next.start_after = handled
            .last()
            .map_or_else(|| state.start_after.clone(), |o| o.key.clone());
        next.round_objects = round_objects;
        next.round_bytes = round_bytes;
        next.round_started_at = state.round_started_at.or(Some(now_ms));
    } else {
        next.full_objects = Some(round_objects);
        next.full_bytes = Some(round_bytes);
        next.full_at = Some(now_ms);
        next.start_after = String::new();
        next.round_objects = 0;
        next.round_bytes = 0;
        next.round_started_at = None;
    }
    next
}

#[derive(Deserialize)]
struct StateRow {
    start_after: String,
    round_objects: f64,
    round_bytes: f64,
    round_started_at: Option<f64>,
    full_objects: Option<f64>,
    full_bytes: Option<f64>,
    full_at: Option<f64>,
    last_pass_at: Option<f64>,
    last_listed: f64,
    last_swept: f64,
}

impl From<StateRow> for SweepState {
    fn from(r: StateRow) -> Self {
        let n = |v: f64| v.max(0.0) as u64;
        Self {
            start_after: r.start_after,
            round_objects: n(r.round_objects),
            round_bytes: n(r.round_bytes),
            round_started_at: r.round_started_at.map(|v| v as i64),
            full_objects: r.full_objects.map(n),
            full_bytes: r.full_bytes.map(n),
            full_at: r.full_at.map(|v| v as i64),
            last_pass_at: r.last_pass_at.map(|v| v as i64),
            last_listed: n(r.last_listed),
            last_swept: n(r.last_swept),
        }
    }
}

/// The save of the state at rest ([`SWEEP_STATE_SAVE_SQL`]).
#[must_use]
pub fn save_query(s: &SweepState) -> Query {
    let opt = |v: Option<i64>| v.map_or(crate::d1::QVal::Null, crate::d1::QVal::Int);
    Query::new(SWEEP_STATE_SAVE_SQL)
        .bind(s.start_after.as_str())
        .bind(s.round_objects)
        .bind(s.round_bytes)
        .bind(opt(s.round_started_at))
        .bind(opt(s.full_objects.map(|v| v as i64)))
        .bind(opt(s.full_bytes.map(|v| v as i64)))
        .bind(opt(s.full_at))
        .bind(opt(s.last_pass_at))
        .bind(s.last_listed)
        .bind(s.last_swept)
}

async fn read_state(db: &D1Database) -> Result<SweepState, String> {
    Ok(Query::new(SWEEP_STATE_SQL)
        .fetch_optional::<StateRow>(db)
        .await?
        .map(SweepState::from)
        .unwrap_or_default())
}

fn listed_of(o: &worker::Object) -> Listed {
    Listed {
        key: o.key(),
        bytes: o.size(),
        uploaded_ms: o.uploaded().as_millis() as i64,
    }
}

/// ONE PASS of the sweep (the module doc), from the scheduled tick. Fail-closed: a state, listing or named-keys
/// read that faults deletes nothing and moves no cursor; a delete that faults leaves its object for the next
/// round.
pub async fn sweep_pass(env: &Env, db: &D1Database) {
    let bucket = match env.bucket(crate::queue::BEEF_BLOBS_BINDING) {
        Ok(b) => b,
        Err(e) => {
            worker::console_log!(
                "[beef-blobs] the orphan sweep found no {} binding ({e}); nothing listed",
                crate::queue::BEEF_BLOBS_BINDING
            );
            return;
        }
    };
    let state = match read_state(db).await {
        Ok(s) => s,
        Err(e) => {
            worker::console_log!(
                "[beef-blobs] the orphan sweep's state could not be read ({e}); nothing listed"
            );
            return;
        }
    };
    let mut list = bucket.list().prefix(SWEEP_PREFIX).limit(SWEEP_MAX_OBJECTS);
    if !state.start_after.is_empty() {
        list = list.start_after(state.start_after.as_str());
    }
    let page = match list.execute().await {
        Ok(p) => p,
        Err(e) => {
            worker::console_log!(
                "[beef-blobs] the orphan sweep's listing faulted ({e}); nothing swept"
            );
            return;
        }
    };
    let listed: Vec<Listed> = page.objects().iter().map(listed_of).collect();
    let now = worker::Date::now().as_millis() as i64;
    let named: HashSet<String> = if any_past_window(&listed, now, ORPHAN_WINDOW_S) {
        #[derive(Deserialize)]
        struct Named {
            r2_key: String,
        }
        match Query::new(NAMED_KEYS_SQL).fetch_all::<Named>(db).await {
            Ok(rows) => rows.into_iter().map(|r| r.r2_key).collect(),
            Err(e) => {
                worker::console_log!("[beef-blobs] the orphan sweep could not read the dead letters' keys ({e}); nothing swept");
                return;
            }
        }
    } else {
        HashSet::new()
    };
    let plan = plan_pass(&listed, &named, now, ORPHAN_WINDOW_S, SWEEP_MAX_DELETES);
    let mut swept: HashSet<String> = HashSet::new();
    let (mut swept_bytes, mut faults) = (0u64, 0u64);
    for o in plan.orphans.iter().map(|i| &listed[*i]) {
        let head = match bucket.head(o.key.as_str()).await {
            Ok(h) => h.as_ref().map(listed_of),
            Err(e) => {
                faults += 1;
                worker::console_log!(
                    "[beef-blobs] the orphan sweep's read of {} faulted ({e}); the object stays",
                    o.key
                );
                continue;
            }
        };
        if !still_orphan(o, head.as_ref()) {
            continue;
        }
        match bucket.delete(o.key.as_str()).await {
            Ok(()) => {
                swept.insert(o.key.clone());
                swept_bytes += o.bytes;
                worker::console_log!(
                    "[beef-blobs] SWEPT {} bytes={} age_s={} (an orphan: past the {ORPHAN_WINDOW_S} s window, no dead letter names it)",
                    o.key,
                    o.bytes,
                    now.saturating_sub(o.uploaded_ms) / 1000
                );
            }
            Err(e) => {
                faults += 1;
                worker::console_log!(
                    "[beef-blobs] the orphan sweep's delete of {} faulted ({e}); the object stays",
                    o.key
                );
            }
        }
    }
    let more = page.truncated() || plan.handled < listed.len();
    let next = next_state(&state, &listed[..plan.handled], &swept, more, now);
    if let Err(e) = save_query(&next).execute(db).await {
        worker::console_log!("[beef-blobs] the orphan sweep's state could not be saved ({e}); the next pass lists this page again");
    }
    crate::ops::bump_counter(
        db,
        crate::ops::COUNTER_QUEUE_R2_ORPHANS_SWEPT,
        swept.len() as u64,
    )
    .await;
    crate::ops::bump_counter(
        db,
        crate::ops::COUNTER_QUEUE_R2_ORPHANS_SWEPT_BYTES,
        swept_bytes,
    )
    .await;
    crate::ops::bump_counter(db, crate::ops::COUNTER_QUEUE_R2_ORPHAN_SWEEP_FAULTS, faults).await;
}

/// PURE: `/health/invariants.queue`: the R2 objects at rest as the sweep's listing counted them. `atRest` is the
/// last COMPLETE round over the bucket (`null` before the first), `round` the one in progress; `state` `None` is
/// an unreadable table (`readable: false`), distinct from a sweep that never ran (`lastPassAt: null`).
#[must_use]
pub fn queue_json(state: Option<&SweepState>, bound: bool) -> serde_json::Value {
    let sweep = serde_json::json!({
        "windowSecs": ORPHAN_WINDOW_S,
        "maxObjectsPerPass": SWEEP_MAX_OBJECTS,
        "maxDeletesPerPass": SWEEP_MAX_DELETES,
        "prefix": SWEEP_PREFIX,
        "lastPassAt": state.and_then(|s| s.last_pass_at),
        "lastListed": state.map(|s| s.last_listed),
        "lastSwept": state.map(|s| s.last_swept),
    });
    serde_json::json!({ "r2": {
        "bound": bound,
        "readable": state.is_some(),
        "atRest": state.and_then(|s| Some(serde_json::json!({
            "objects": s.full_objects?, "bytes": s.full_bytes?, "at": s.full_at,
        }))),
        "round": state.map(|s| serde_json::json!({
            "objects": s.round_objects, "bytes": s.round_bytes,
            "startedAt": s.round_started_at, "startAfter": s.start_after,
        })),
        "sweep": sweep,
    }})
}

/// `/health/invariants.queue` (one D1 read).
pub async fn health_json(db: &D1Database, env: &Env) -> serde_json::Value {
    let state = read_state(db).await.ok();
    queue_json(
        state.as_ref(),
        env.bucket(crate::queue::BEEF_BLOBS_BINDING).is_ok(),
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::d1::QVal;
    use crate::dead_letters;
    use crate::queue;
    use overlay_engine::types::SubmitMode;
    use std::collections::BTreeMap;

    fn binds(q: &Query) -> Vec<rusqlite::types::Value> {
        q.params()
            .iter()
            .map(|p| match p {
                QVal::Null => rusqlite::types::Value::Null,
                QVal::Int(i) => rusqlite::types::Value::Integer(*i),
                QVal::Text(s) => rusqlite::types::Value::Text(s.clone()),
                QVal::Bool(b) => rusqlite::types::Value::Integer(i64::from(*b)),
                QVal::Blob(b) => rusqlite::types::Value::Blob(b.clone()),
                QVal::Float(f) => rusqlite::types::Value::Real(*f),
            })
            .collect()
    }

    /// The shipped schema of the two tables the sweep reads and writes, from the migration list itself.
    fn db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        for m in crate::d1::OVERLAY_MIGRATIONS
            .iter()
            .filter(|m| m.contains("mutation_dead_letters") || m.contains("beef_blob_sweep"))
        {
            conn.execute(m, []).unwrap();
        }
        conn
    }

    fn exec(conn: &rusqlite::Connection, q: &Query) {
        let mut stmt = conn.prepare(q.sql()).unwrap();
        let mut rows = stmt
            .query(rusqlite::params_from_iter(binds(q).iter()))
            .unwrap();
        while rows.next().unwrap().is_some() {}
    }

    /// The state at rest, read by the shipped statement.
    fn state(conn: &rusqlite::Connection) -> SweepState {
        let n = |v: Option<i64>| v.map(|v| v as u64);
        conn.query_row(SWEEP_STATE_SQL, [], |r| {
            Ok(SweepState {
                start_after: r.get(0)?,
                round_objects: r.get::<_, i64>(1)? as u64,
                round_bytes: r.get::<_, i64>(2)? as u64,
                round_started_at: r.get(3)?,
                full_objects: n(r.get(4)?),
                full_bytes: n(r.get(5)?),
                full_at: r.get(6)?,
                last_pass_at: r.get(7)?,
                last_listed: r.get::<_, i64>(8)? as u64,
                last_swept: r.get::<_, i64>(9)? as u64,
            })
        })
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok(SweepState::default()),
            e => Err(e),
        })
        .unwrap()
    }

    /// The bucket: key -> (bytes, uploaded ms), listed in key order as R2 lists.
    #[derive(Default)]
    struct Bucket {
        objects: BTreeMap<String, (u64, i64)>,
        lists: usize,
        heads: usize,
        deletes: usize,
    }

    impl Bucket {
        fn put(&mut self, key: &str, bytes: u64, at_ms: i64) {
            self.objects.insert(key.to_string(), (bytes, at_ms));
        }
        fn list(&mut self, start_after: &str, limit: usize) -> (Vec<Listed>, bool) {
            self.lists += 1;
            let mut it = self
                .objects
                .iter()
                .filter(|(k, _)| k.starts_with(SWEEP_PREFIX) && k.as_str() > start_after);
            let page: Vec<Listed> = it
                .by_ref()
                .take(limit)
                .map(|(k, (b, u))| Listed {
                    key: k.clone(),
                    bytes: *b,
                    uploaded_ms: *u,
                })
                .collect();
            (page, it.next().is_some())
        }
        fn head(&mut self, key: &str) -> Option<Listed> {
            self.heads += 1;
            self.objects.get(key).map(|(b, u)| Listed {
                key: key.to_string(),
                bytes: *b,
                uploaded_ms: *u,
            })
        }
    }

    /// What one pass did.
    #[derive(Debug, Default, PartialEq, Eq)]
    struct Pass {
        swept: Vec<String>,
        swept_bytes: u64,
        named_read: bool,
    }

    /// ONE PASS as `sweep_pass` makes it, over the bucket above and the SHIPPED statements under real SQLite: the
    /// state read, the listing from the key at rest, the named keys (read only for a page with an object past
    /// the window; `named_faults` is that read faulting), the plan, the read before each delete, the state saved.
    /// `between` runs after the listing and before the deletes (a re-presentation landing meanwhile).
    fn pass(
        conn: &rusqlite::Connection,
        bucket: &mut Bucket,
        now: i64,
        named_faults: bool,
        between: impl FnOnce(&mut Bucket),
    ) -> Pass {
        let st = state(conn);
        let (listed, truncated) = bucket.list(&st.start_after, SWEEP_MAX_OBJECTS as usize);
        let mut out = Pass::default();
        let named: HashSet<String> = if any_past_window(&listed, now, ORPHAN_WINDOW_S) {
            out.named_read = true;
            if named_faults {
                return out;
            }
            conn.prepare(NAMED_KEYS_SQL)
                .unwrap()
                .query_map([], |r| r.get::<_, String>(0))
                .unwrap()
                .map(Result::unwrap)
                .collect()
        } else {
            HashSet::new()
        };
        let plan = plan_pass(&listed, &named, now, ORPHAN_WINDOW_S, SWEEP_MAX_DELETES);
        between(bucket);
        let mut swept = HashSet::new();
        for o in plan.orphans.iter().map(|i| &listed[*i]) {
            let head = bucket.head(&o.key);
            if !still_orphan(o, head.as_ref()) {
                continue;
            }
            bucket.objects.remove(&o.key);
            bucket.deletes += 1;
            swept.insert(o.key.clone());
            out.swept.push(o.key.clone());
            out.swept_bytes += o.bytes;
        }
        let more = truncated || plan.handled < listed.len();
        exec(
            conn,
            &save_query(&next_state(&st, &listed[..plan.handled], &swept, more, now)),
        );
        out
    }

    /// The door's write of a body past the room, and the key its message would name.
    fn written(
        bucket: &mut Bucket,
        seed: u8,
        topics: &[String],
        at_ms: i64,
    ) -> queue::MutationMessage {
        let beef: Vec<u8> = (0..8_000usize).map(|i| (i as u8) ^ seed).collect();
        let queue::Carriage::R2(m) = queue::plan_replay(
            &beef,
            topics,
            SubmitMode::HistoricalTx,
            queue::REPLAY_REASON_PHASE3_FAULT,
            queue::QUEUE_MESSAGE_ROOM_MIN,
            &overlay_engine::beef_limits::SUBMIT_BEEF_LIMITS,
        )
        .unwrap() else {
            panic!("8 KB rides by key under the lowest room")
        };
        let r = m.r2.as_ref().unwrap();
        assert!(
            r.key.starts_with(SWEEP_PREFIX),
            "the sweep lists the door's own prefix"
        );
        bucket.put(&r.key, r.bytes, at_ms);
        m
    }

    fn park(conn: &rusqlite::Connection, m: &queue::MutationMessage, now: i64) {
        let r = m.r2.as_ref().unwrap();
        let (txid, topics) = dead_letters::letter_key(m, None);
        exec(
            conn,
            &dead_letters::park_query_r2(
                &txid,
                &topics,
                &serde_json::to_string(m).unwrap(),
                dead_letters::FAULT_UNRECORDED,
                0,
                now,
                Some((r.key.as_str(), r.bytes)),
            ),
        );
    }

    const WINDOW_MS: i64 = ORPHAN_WINDOW_S as i64 * 1000;

    /// bsv-low #585, door 3's fold (the ORPHANS). An object written by a send that then faulted (the door writes
    /// R2 first; no message, no letter names it) and aged past the window is SWEPT; an object of the same age a
    /// parked letter names is not; an object younger than the window is not; one written again between the
    /// listing and its delete is not. The objects at rest are served from the listing. On `45aceff` there is no
    /// sweep: the orphan stays for good and nothing lists it.
    #[test]
    fn e585_d3f_an_orphan_past_the_window_is_swept_a_named_and_a_young_one_are_not() {
        let conn = db();
        let mut bucket = Bucket::default();
        let topics = vec!["tm_a".to_string()];
        let t0 = 1_800_000_000_000i64;
        // the faulted send: written, never enqueued, never re-presented
        let orphan = written(&mut bucket, 1, &topics, t0);
        // a dead-lettered submission, parked: its row names its object
        let held = written(&mut bucket, 2, &topics, t0);
        park(&conn, &held, t0 + 60_000);
        // a submission whose message is still in a queue
        let young = written(&mut bucket, 3, &topics, t0 + 2_000);
        // not the door's: never listed
        bucket.put("other/thing", 5, 0);
        let key = |m: &queue::MutationMessage| m.r2.as_ref().unwrap().key.clone();

        // at the window, to the millisecond: nothing is past it, the dead letters are not even read
        assert_eq!(
            pass(&conn, &mut bucket, t0 + WINDOW_MS, false, |_| {}),
            Pass::default()
        );
        assert_eq!(bucket.objects.len(), 4);
        assert_eq!(
            (state(&conn).full_objects, state(&conn).full_bytes),
            (Some(3), Some(24_000)),
            "the objects at rest, from the listing"
        );

        // one second later the two old objects are past it: the orphan is swept, the named one is not
        let now = t0 + WINDOW_MS + 1_000;
        let p = pass(&conn, &mut bucket, now, false, |_| {});
        assert_eq!(
            p,
            Pass {
                swept: vec![key(&orphan)],
                swept_bytes: 8_000,
                named_read: true
            }
        );
        assert!(!bucket.objects.contains_key(&key(&orphan)));
        assert!(
            bucket.objects.contains_key(&key(&held)),
            "a parked letter names it"
        );
        assert!(
            bucket.objects.contains_key(&key(&young)),
            "younger than the window"
        );
        assert!(bucket.objects.contains_key("other/thing"));
        let st = state(&conn);
        assert_eq!(
            (
                st.full_objects,
                st.full_bytes,
                st.full_at,
                st.last_swept,
                st.last_listed
            ),
            (Some(2), Some(16_000), Some(now), 1, 3)
        );
        assert_eq!(
            (bucket.heads, bucket.deletes),
            (1, 1),
            "one read and one delete, the orphan's"
        );

        // the client re-presents the young one's bytes long after: the same key, a NEW stamp, a new message. A
        // pass that listed the old stamp reads the object again before its delete and leaves it.
        let late = t0 + 2 * WINDOW_MS;
        let young_key = key(&young);
        let p = pass(&conn, &mut bucket, late, false, |b| {
            b.put(&young_key, 8_000, late - 1)
        });
        assert_eq!(p.swept, Vec::<String>::new());
        assert!(bucket.objects.contains_key(&young_key));
        // a named-keys read that faults deletes nothing
        let before = bucket.objects.len();
        let p = pass(&conn, &mut bucket, late + 3 * WINDOW_MS, true, |_| {});
        assert!(p.named_read && p.swept.is_empty());
        assert_eq!(bucket.objects.len(), before);
        // the letter row deleted by SQL (an operator's hand): its object is an orphan at the next pass
        conn.execute(
            "DELETE FROM mutation_dead_letters WHERE r2_key = ?1",
            [key(&held)],
        )
        .unwrap();
        let p = pass(&conn, &mut bucket, late + 3 * WINDOW_MS, false, |_| {});
        assert_eq!(
            p.swept.len(),
            2,
            "the unnamed letter's object and the re-presented one, both past the window now"
        );
        assert_eq!(state(&conn).full_objects, Some(0));
    }

    /// The fold: the sweep is BOUNDED per pass and resumes from the key at rest. 450 objects, 130 of them
    /// orphans: a pass lists at most 200 and deletes at most 50, stops at the 51st orphan of its page, and the
    /// next pass goes on from the last key handled (read from D1, not from memory). A round ends when the listing
    /// does; its count is the objects left at rest.
    #[test]
    fn e585_d3f_the_sweep_is_bounded_per_pass_and_resumes_from_the_key_at_rest() {
        let conn = db();
        let mut bucket = Bucket::default();
        let t0 = 1_800_000_000_000i64;
        let now = t0 + WINDOW_MS + 1;
        // keys in listing order; the first 130 are old (orphans), the rest young
        for i in 0..450u64 {
            let at = if i < 130 { t0 } else { now - 1_000 };
            bucket.put(&format!("{SWEEP_PREFIX}{i:064x}/{:032x}", 0), 100 + i, at);
        }
        let mut passes = Vec::new();
        for _ in 0..20 {
            let p = pass(&conn, &mut bucket, now, false, |_| {});
            let st = state(&conn);
            passes.push((p.swept.len(), st.last_listed, st.start_after.is_empty()));
            assert!(p.swept.len() <= SWEEP_MAX_DELETES);
            if st.start_after.is_empty() {
                break;
            }
        }
        assert_eq!(
            passes,
            vec![
                (50, 50, false), // stopped at the 51st orphan of its page
                (50, 50, false),
                (30, 200, false), // the last 30 orphans and 170 young
                (0, 150, true),   // the listing ended: the round is complete
            ]
        );
        assert_eq!(bucket.lists, 4, "one listing a pass");
        assert_eq!((bucket.heads, bucket.deletes), (130, 130));
        let st = state(&conn);
        assert_eq!(st.full_objects, Some(320));
        assert_eq!(st.full_bytes, Some((130..450u64).map(|i| 100 + i).sum()));
        assert_eq!(
            (st.round_objects, st.round_bytes, st.round_started_at),
            (0, 0, None)
        );
        // the next round starts at the first key, and a page with nothing past the window reads no dead letter
        let p = pass(&conn, &mut bucket, now, false, |_| {});
        assert!(!p.named_read && p.swept.is_empty());
        assert_eq!(state(&conn).round_objects, 200);
        assert_eq!(
            state(&conn).full_objects,
            Some(320),
            "the last complete count stands meanwhile"
        );
        // the health block
        let j = queue_json(Some(&state(&conn)), true);
        assert_eq!(j["r2"]["atRest"]["objects"], 320);
        assert_eq!(j["r2"]["round"]["objects"], 200);
        assert_eq!(j["r2"]["sweep"]["windowSecs"], ORPHAN_WINDOW_S);
        assert_eq!(j["r2"]["sweep"]["maxObjectsPerPass"], 200);
        assert_eq!(j["r2"]["sweep"]["maxDeletesPerPass"], 50);
        let never = queue_json(Some(&SweepState::default()), true);
        assert!(never["r2"]["atRest"].is_null() && never["r2"]["sweep"]["lastPassAt"].is_null());
        assert_eq!(queue_json(None, false)["r2"]["readable"], false);
    }

    /// The fold: the window is what no queue message can outlive, and the wiring. Two retentions (the main queue,
    /// then the dead letter queue), each the platform's default four days. #576's dead-letter window alone (about
    /// 48 h of backoff) is under it and is NOT the bound: it starts after the message's life in the main queue.
    #[test]
    fn e585_d3f_the_window_outlives_every_queue_message_and_the_cron_runs_the_pass() {
        assert_eq!(QUEUE_RETENTION_S, 4 * 86_400);
        assert_eq!(ORPHAN_WINDOW_S, 8 * 86_400);
        let dlq_backoff_s: u64 = (1..=dead_letters::DLQ_MAX_RETRIES)
            .map(|a| u64::from(dead_letters::dlq_retry_plan(Some(a)).delay_s))
            .sum();
        assert_eq!(
            dlq_backoff_s, 172_860,
            "#576's window: 48 h and a minute of backoff"
        );
        assert!(
            dlq_backoff_s > 48 * 3600,
            "a letter deferred to its last delivery is older than 48 h before its main-queue life is counted"
        );
        assert!(
            dlq_backoff_s < QUEUE_RETENTION_S,
            "the deferral fits the dead letter queue's retention"
        );
        assert!(QUEUE_RETENTION_S + dlq_backoff_s < ORPHAN_WINDOW_S);
        for cfg in [
            include_str!("../wrangler.toml"),
            include_str!("../wrangler.low.toml"),
        ] {
            assert!(
                !cfg.contains("retention"),
                "no config sets a retention: the default is the one the window is made of"
            );
            assert!(
                cfg.contains("crons = [\"*/15 * * * *\"]"),
                "the sweep's cadence"
            );
        }
        assert_eq!(
            crate::ops::COUNTER_QUEUE_R2_ORPHANS_SWEPT,
            "queue_r2_orphans_swept_total"
        );
        assert_eq!(
            crate::ops::COUNTER_QUEUE_R2_ORPHANS_SWEPT_BYTES,
            "queue_r2_orphans_swept_bytes_total"
        );

        let squash = |s: &str| {
            s.lines()
                .map(|l| l.split("//").next().unwrap_or(""))
                .collect::<String>()
                .split_whitespace()
                .collect::<String>()
        };
        let lib = include_str!("lib.rs");
        let start = lib.find("async fn scheduled(").unwrap();
        let tick = squash(&lib[start..start + lib[start..].find("\n}\n").unwrap()]);
        let sweep = tick
            .find("race_or_deadline(crate::beef_blob_sweep::sweep_pass(&env,&ops_db),crate::broadcaster::sleep_ms(crate::beef_blob_sweep::SWEEP_BUDGET_MS),)")
            .expect("the scheduled tick runs one bounded pass of the sweep");
        assert!(
            sweep < tick.find("engine.start_gasp_sync()").unwrap(),
            "before the GASP step"
        );
        let ops = squash(include_str!("ops.rs"));
        assert!(ops.contains("letqueue=crate::beef_blob_sweep::health_json(db,env).await;"));
        assert!(ops.contains("\"queue\":queue,"));
        assert!(crate::d1::OVERLAY_MIGRATIONS.contains(&SWEEP_STATE_CREATE));

        let me = include_str!("beef_blob_sweep.rs");
        let me = &me[..me.find("#[cfg(test)]").unwrap()];
        let start = me.find("pub async fn sweep_pass(").unwrap();
        let f = squash(&me[start..start + me[start..].find("\n}\n").unwrap()]);
        assert!(f.contains(".list().prefix(SWEEP_PREFIX).limit(SWEEP_MAX_OBJECTS)"));
        assert!(f.contains("list.start_after(state.start_after.as_str())"));
        assert!(f.contains("ifany_past_window(&listed,now,ORPHAN_WINDOW_S)"));
        assert!(f.contains("plan_pass(&listed,&named,now,ORPHAN_WINDOW_S,SWEEP_MAX_DELETES)"));
        let (named, head, check, delete, save) = (
            f.find("Query::new(NAMED_KEYS_SQL)").unwrap(),
            f.find("bucket.head(o.key.as_str())").unwrap(),
            f.find("if!still_orphan(o,head.as_ref()){continue;}")
                .unwrap(),
            f.find("bucket.delete(o.key.as_str())").unwrap(),
            f.find("save_query(&next).execute(db)").unwrap(),
        );
        assert!(named < head && head < check && check < delete && delete < save);
        assert_eq!(f.matches("bucket.delete(").count(), 1);
        assert_eq!(
            f[..delete].matches("return;").count(),
            4,
            "no binding, a state read, a listing or a named-keys read that faults: nothing is deleted"
        );
    }
}
