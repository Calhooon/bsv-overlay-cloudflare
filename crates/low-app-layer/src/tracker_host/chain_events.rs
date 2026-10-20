//! bsv-low #771 (L1b): the chain-event intake. The chaintracks feed (`GET /events?since=<cursor>`, the stack's
//! `docs/CHAIN-EVENTS.md`) is pulled over the CHAINTRACKS service binding in the scheduled pass, on a durable cursor
//! ([`CREATE`], one row). Each envelope passes `bsv_tracker::decode_envelope`, the crate's gate; an envelope it
//! refuses (an unknown kind or version, a malformed known shape, the feed's own `unreadable`) STOPS the pull at its
//! cursor. Each event's effect on the stored words is a durable re-ask queue ([`REASKS_CREATE`]) written in the SAME
//! D1 batch as the cursor's advance; the drain re-asks the queued rows, a few a pass. No read route writes here.

/// The feed cursor, one row: the last cursor consumed and its stamp, the last event's kind and cursor, an event held
/// across passes (its cursor and the sub-cursor of its affected rows), the envelope the pull stopped at, the tip.
/// Byte-identical to the overlay's migration (`d1::TRACKER_CHAIN_EVENTS_CREATE`; the catch-up pin).
pub const CREATE: &str = "CREATE TABLE IF NOT EXISTS tracker_chain_events (id INTEGER PRIMARY KEY CHECK (id = 1), cursor TEXT NOT NULL, at INTEGER NOT NULL, last_kind TEXT, last_cursor TEXT, held_cursor TEXT, held_height INTEGER, held_txid TEXT, stop_cursor TEXT, stop_fault TEXT, tip_height INTEGER, tip_age_seconds INTEGER)";

/// The durable re-ask queue: one row per txid, the crate's `Reask` (`reorg` or `fork`) and its height, the cursor of
/// the event that queued it, when, and the drain's failed attempts. Byte-identical to `d1::TRACKER_REASKS_CREATE`.
pub const REASKS_CREATE: &str = "CREATE TABLE IF NOT EXISTS tracker_reasks (txid TEXT PRIMARY KEY, kind TEXT NOT NULL, height INTEGER NOT NULL, event_cursor TEXT NOT NULL, written_at INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0)";

/// The three indexes, each the overlay's migration byte for byte, issued again on the scheduled path only (the
/// `tx_words_age` idiom: no read gains a schema statement).
pub const INDEXES: [&str; 3] = [
    "CREATE INDEX IF NOT EXISTS tracker_reasks_age ON tracker_reasks(written_at, txid)",
    "CREATE INDEX IF NOT EXISTS tx_words_mined_height ON tx_words(height, txid) WHERE word = 'mined'",
    "CREATE INDEX IF NOT EXISTS tx_words_mined_header ON tx_words(header_hash, txid) WHERE word = 'mined'",
];

use super::{stored::Row, HeaderSnapshot};
use crate::pot_hook::{Bind, SqlPort, Stmt};
use bsv_tracker::{
    decode_envelope, ChainEvent, Evidence, Input, Params, Proof, Reask, State, Word,
};
use serde_json::{json, Value};
use std::cell::RefCell;
use std::collections::BTreeMap;

/// The pages one pass pulls (the feed's page is at most 100 events).
pub const PAGES_PER_PASS: usize = 2;
/// The affected rows one pass queues, over every event it steps: so at most 256 for one event. An event whose set is
/// larger is HELD (its cursor not passed) with a durable sub-cursor, and walked on across passes.
pub const ROWS_PER_PASS: usize = 256;
/// The queued re-asks one pass drains, oldest first.
pub const DRAINS_PER_PASS: usize = 8;
/// A queued re-ask that faults this many passes is dropped, counted.
pub const DRAIN_ATTEMPTS: i64 = 3;
/// The chain-event step's one deadline (the pull, the step and the drain): a pass the deadline drops has written
/// only whole batches.
pub const WALL_MS: u64 = 25_000;

const PARAMS: Params = Params { age_threshold: 60 };

/// The row, or none before the first pass (the cursor `0`: the journal replays from its start).
pub const STATE_READ: &str = "SELECT cursor, at, last_kind, last_cursor, held_cursor, held_height, held_txid, stop_cursor, stop_fault, tip_height, tip_age_seconds FROM tracker_chain_events WHERE id = 1";

/// A fork's, reorg's or split's affected set: the mined rows at or above its height, after the sub-cursor
/// `(?1, ?2)` (the event's height and `''` to start), in `(height, txid)` order, `?3` at most. The partial index
/// `tx_words_mined_height`, never a scan.
pub const AFFECTED_AT_OR_ABOVE: &str = "SELECT txid, word, tier, height, proof_root, proof_height, proof_hex, header_hash, hint_source, hint_at, updated_at, NULL AS legacy_body FROM tx_words WHERE word = 'mined' AND (height, txid) > (?1, ?2) ORDER BY height, txid LIMIT ?3";

/// An invalidated block's affected set: the mined rows whose checked proof names its header, after the sub-cursor
/// `?2`, `?3` at most. The partial index `tx_words_mined_header`.
pub const AFFECTED_BY_HEADER: &str = "SELECT txid, word, tier, height, proof_root, proof_height, proof_hex, header_hash, hint_source, hint_at, updated_at, NULL AS legacy_body FROM tx_words WHERE word = 'mined' AND header_hash = ?1 AND txid > ?2 ORDER BY txid LIMIT ?3";

/// Queue the re-asks of one batch (`?1`, a JSON array of `{txid, kind, height, cursor}`). A txid already queued
/// keeps its place (`written_at`), takes the lower height and `reorg` over `fork`, the newer event's cursor, and a
/// fresh attempt count.
pub const QUEUE: &str = "INSERT INTO tracker_reasks (txid, kind, height, event_cursor, written_at, attempts) SELECT json_extract(value, '$.txid'), json_extract(value, '$.kind'), json_extract(value, '$.height'), json_extract(value, '$.cursor'), ?2, 0 FROM json_each(?1) WHERE true ON CONFLICT(txid) DO UPDATE SET kind = CASE WHEN tracker_reasks.kind = 'reorg' OR excluded.kind = 'reorg' THEN 'reorg' ELSE 'fork' END, height = MIN(tracker_reasks.height, excluded.height), event_cursor = excluded.event_cursor, attempts = 0";

/// The cursor's advance, in the SAME batch as [`QUEUE`]: never below the stored cursor (a late or concurrent pass
/// does not move it back).
pub const SAVE: &str = "INSERT INTO tracker_chain_events (id, cursor, at, last_kind, last_cursor, held_cursor, held_height, held_txid, stop_cursor, stop_fault, tip_height, tip_age_seconds) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) ON CONFLICT(id) DO UPDATE SET cursor = excluded.cursor, at = excluded.at, last_kind = COALESCE(excluded.last_kind, tracker_chain_events.last_kind), last_cursor = COALESCE(excluded.last_cursor, tracker_chain_events.last_cursor), held_cursor = excluded.held_cursor, held_height = excluded.held_height, held_txid = excluded.held_txid, stop_cursor = excluded.stop_cursor, stop_fault = excluded.stop_fault, tip_height = COALESCE(excluded.tip_height, tracker_chain_events.tip_height), tip_age_seconds = COALESCE(excluded.tip_age_seconds, tracker_chain_events.tip_age_seconds) WHERE CAST(tracker_chain_events.cursor AS INTEGER) <= CAST(excluded.cursor AS INTEGER)";

/// The feed's 409 `cursorAhead`: the cursor back to `0`, only from the cursor the pass read (`?2`).
pub const RESET: &str = "UPDATE tracker_chain_events SET cursor = '0', at = ?1, held_cursor = NULL, held_height = NULL, held_txid = NULL, stop_cursor = NULL, stop_fault = NULL WHERE id = 1 AND cursor = ?2";

/// The drain's read: the oldest queued re-asks (`?1` at most, `tracker_reasks_age`) and each one's stored row by its
/// primary key.
pub const DRAIN_READ: &str = "SELECT q.txid AS q_txid, q.kind AS q_kind, q.height AS q_height, q.event_cursor AS q_cursor, q.attempts AS q_attempts, w.txid, w.word, w.tier, w.height, w.proof_root, w.proof_height, w.proof_hex, w.header_hash, w.hint_source, w.hint_at, w.updated_at, w.legacy_body FROM (SELECT txid, kind, height, event_cursor, attempts, written_at FROM tracker_reasks ORDER BY written_at, txid LIMIT ?1) AS q LEFT JOIN tx_words AS w ON w.txid = q.txid ORDER BY q.written_at, q.txid";

/// A drained entry, deleted in the batch of its row's rewrite; only the entry the drain read (a newer event's
/// re-queue keeps it).
pub const DRAIN_DELETE: &str = "DELETE FROM tracker_reasks WHERE txid = ?1 AND event_cursor = ?2";

/// A re-ask that could not be judged this pass: one more attempt.
pub const DRAIN_RETRY: &str =
    "UPDATE tracker_reasks SET attempts = attempts + 1 WHERE txid = ?1 AND event_cursor = ?2";

/// The stored row, as read back. D1 answers numbers; a cursor is the feed's decimal string.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Cursor {
    pub cursor: String,
    pub at: Option<f64>,
    pub last_kind: Option<String>,
    pub last_cursor: Option<String>,
    pub held_cursor: Option<String>,
    pub held_height: Option<f64>,
    pub held_txid: Option<String>,
    pub stop_cursor: Option<String>,
    pub stop_fault: Option<String>,
    pub tip_height: Option<f64>,
    pub tip_age_seconds: Option<f64>,
}

impl Default for Cursor {
    fn default() -> Self {
        Cursor {
            cursor: "0".into(),
            at: None,
            last_kind: None,
            last_cursor: None,
            held_cursor: None,
            held_height: None,
            held_txid: None,
            stop_cursor: None,
            stop_fault: None,
            tip_height: None,
            tip_age_seconds: None,
        }
    }
}

impl Cursor {
    fn position(&self) -> u64 {
        parse_cursor(&self.cursor).unwrap_or(0)
    }
    /// The `chainEvents` health object.
    pub fn health(&self) -> Value {
        json!({
            "cursor": self.cursor,
            "lastKind": self.last_kind,
            "lastCursor": self.last_cursor,
            "lastAt": self.at.map(|t| t as u64),
            "tipHeight": self.tip_height.map(|h| h as u64),
            "tipAgeSeconds": self.tip_age_seconds.map(|s| s as u64),
            "heldCursor": self.held_cursor,
            "stopCursor": self.stop_cursor,
            "stopFault": self.stop_fault,
        })
    }
}

/// A feed cursor: an opaque decimal string inside D1's exact integer range.
pub fn parse_cursor(text: &str) -> Option<u64> {
    if text.is_empty() || text.len() > 16 || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// The feed's transport: one page of `GET /events?since=<since>`, its status and body, or the transport's fault.
#[allow(async_fn_in_trait)]
pub trait Feed {
    async fn page(&self, since: &str) -> Result<(u16, Vec<u8>), String>;
}

/// The drain's world: the host's headers (CHAINTRACKS) and the existing courier ladder.
#[allow(async_fn_in_trait)]
pub trait ReaskWorld {
    async fn headers(&self, height: u32) -> HeaderSnapshot;
    async fn fresh_proof(&self, txid: &str, reask: &Reask) -> Result<Option<Proof>, String>;
}

/// One queued re-ask, as written by [`QUEUE`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Queued {
    pub txid: String,
    pub reask: Reask,
    pub cursor: u64,
}

impl Queued {
    fn json(&self) -> Option<Value> {
        let (kind, height) = reask_columns(&self.reask)?;
        Some(
            json!({"txid": self.txid, "kind": kind, "height": height, "cursor": self.cursor.to_string()}),
        )
    }
}

/// The two re-ask kinds a chain event produces, as stored.
pub fn reask_columns(reask: &Reask) -> Option<(&'static str, u32)> {
    match reask {
        Reask::Reorg { fork_height } => Some(("reorg", *fork_height)),
        Reask::Fork { height } => Some(("fork", *height)),
        _ => None,
    }
}

/// The stored kind back to the crate's re-ask.
pub fn reask_of_columns(kind: &str, height: u32) -> Reask {
    if kind == "reorg" {
        Reask::Reorg {
            fork_height: height,
        }
    } else {
        Reask::Fork { height }
    }
}

/// The event's wire kind, for the counters and the row.
pub fn kind_name(event: &ChainEvent) -> &'static str {
    match event {
        ChainEvent::Tip { .. } => "tip",
        ChainEvent::Fork { .. } => "fork",
        ChainEvent::Reorg { .. } => "reorg",
        ChainEvent::Invalidated { .. } => "invalidated",
        ChainEvent::Frozen { .. } => "frozen",
        ChainEvent::TipAge { .. } => "tip_age",
        ChainEvent::Split { .. } => "split",
        ChainEvent::SplitResolved { .. } => "split_resolved",
    }
}

/// Which stored rows an event can move: the crate's own indexes (`Tracker::on_chain`): a fork's, a reorg's and a
/// split's mined rows `at_or_above` its height, an invalidation's rows by the checked header's hash. A tip asks the
/// crate's SUSPECT set, which here is the queue itself (the drain rechecks it); a resolved split rechecks only a
/// suspect word (`State::on_chain`), so it too is the queue's, and moves no fresh row; frozen and tip age move no word.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Selection {
    AtOrAbove(u32),
    Header(String),
    None,
}

pub fn selection(event: &ChainEvent) -> Selection {
    match event {
        ChainEvent::Fork { height, .. } => Selection::AtOrAbove(*height),
        ChainEvent::Reorg { fork_height, .. } | ChainEvent::Split { fork_height, .. } => {
            Selection::AtOrAbove(*fork_height)
        }
        ChainEvent::Invalidated { block_hash } => {
            Selection::Header(block_hash.to_ascii_lowercase())
        }
        ChainEvent::SplitResolved { .. }
        | ChainEvent::Tip { .. }
        | ChainEvent::TipAge { .. }
        | ChainEvent::Frozen { .. } => Selection::None,
    }
}

/// The crate's own step: the stored row's state, restored from its checked proof against its recorded header
/// ([`Row::restored_state`]), given the event as `Evidence::Chain` through `State::step`; the re-ask is the crate's.
/// A row whose proof does not restore takes [`host_reask`], the same shapes.
pub fn reask_of(row: &Row, event: &ChainEvent) -> Option<Reask> {
    match row.restored_state() {
        Ok(mut state) => {
            let none = HeaderSnapshot {
                tip: Ok(0),
                headers: BTreeMap::new(),
            };
            let _ = state.step(
                &PARAMS,
                &none,
                Input::Evidence(Evidence::Chain(event.clone())),
            );
            state.reask().cloned()
        }
        Err(_) => host_reask(row, event),
    }
}

/// The crate's marking reproduced over a row (`State::on_chain`, bsv-tracker 0.6.0): a reorg at or below the mined
/// height `Reask::Reorg { fork_height }`; a fork or a split there `Reask::Fork { height }`; an invalidation of the
/// row's own header `Reask::Reorg` at the row's height; nothing else.
pub fn host_reask(row: &Row, event: &ChainEvent) -> Option<Reask> {
    let height = row
        .height
        .map(|h| h as u32)
        .filter(|_| row.word == "mined")?;
    match event {
        ChainEvent::Reorg { fork_height, .. } if *fork_height <= height => Some(Reask::Reorg {
            fork_height: *fork_height,
        }),
        ChainEvent::Fork { height: at, .. } if *at <= height => Some(Reask::Fork { height: *at }),
        ChainEvent::Split { fork_height, .. } if *fork_height <= height => Some(Reask::Fork {
            height: *fork_height,
        }),
        ChainEvent::Invalidated { block_hash }
            if row
                .header_hash
                .as_deref()
                .is_some_and(|h| h.eq_ignore_ascii_case(block_hash)) =>
        {
            Some(Reask::Reorg {
                fork_height: height,
            })
        }
        _ => None,
    }
}

fn text(s: impl Into<String>) -> Bind {
    Bind::Text(s.into())
}
fn opt_text(s: &Option<String>) -> Bind {
    s.clone().map_or(Bind::Null, Bind::Text)
}
fn opt_int(n: Option<f64>) -> Bind {
    n.map_or(Bind::Null, |n| Bind::Int(n as i64))
}
fn stmt(sql: &str, binds: Vec<Bind>) -> Stmt {
    Stmt {
        sql: sql.into(),
        binds,
    }
}

/// THE ATOMIC STEP: the queue's write and the cursor's advance, one batch, in this order. Nothing else is written by
/// a pull that consumed or held an event.
pub fn advance_batch(queued: &[Queued], next: &Cursor, now: u64) -> Vec<Stmt> {
    let mut out = Vec::new();
    let entries: Vec<Value> = queued.iter().filter_map(Queued::json).collect();
    if !entries.is_empty() {
        out.push(stmt(
            QUEUE,
            vec![
                text(Value::Array(entries).to_string()),
                Bind::Int(now as i64),
            ],
        ));
    }
    out.push(stmt(
        SAVE,
        vec![
            text(next.cursor.clone()),
            Bind::Int(now as i64),
            opt_text(&next.last_kind),
            opt_text(&next.last_cursor),
            opt_text(&next.held_cursor),
            opt_int(next.held_height),
            opt_text(&next.held_txid),
            opt_text(&next.stop_cursor),
            opt_text(&next.stop_fault),
            opt_int(next.tip_height),
            opt_int(next.tip_age_seconds),
        ],
    ));
    out
}

/// What one pull did. Its counters are noted by the caller.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PullOutcome {
    pub pages: usize,
    pub consumed: Vec<(u64, &'static str)>,
    pub queued: Vec<Queued>,
    pub held: Option<u64>,
    pub stopped: Option<(u64, String)>,
    pub reset_from: Option<String>,
    pub fault: Option<String>,
    pub counters: Vec<(String, u64)>,
    pub cursor: Cursor,
}

struct Page {
    events: Vec<(u64, Value)>,
    has_more: bool,
}

fn parse_page(body: &[u8]) -> Result<Page, String> {
    let v: Value = serde_json::from_slice(body).map_err(|e| format!("events page: {e}"))?;
    if v["v"].as_u64() != Some(1) {
        return Err(format!("events page carrier version {}", v["v"]));
    }
    let has_more = v["hasMore"]
        .as_bool()
        .ok_or("events page: hasMore missing")?;
    let mut events = Vec::new();
    for item in v["events"]
        .as_array()
        .ok_or("events page: events missing")?
    {
        let c = match &item["cursor"] {
            Value::String(s) => parse_cursor(s),
            Value::Number(n) => n.as_u64(),
            _ => None,
        }
        .ok_or_else(|| format!("events page: malformed cursor {}", item["cursor"]))?;
        events.push((c, item["event"].clone()));
    }
    Ok(Page { events, has_more })
}

fn rows_of(out: Vec<crate::pot_hook::StmtOut>) -> Result<Vec<Row>, String> {
    out.into_iter()
        .next()
        .map(|o| o.rows)
        .unwrap_or_default()
        .into_iter()
        .map(|r| serde_json::from_value::<Row>(r).map_err(|e| format!("tx_words row: {e}")))
        .collect()
}

/// The stored cursor, or the empty row's `0`.
pub async fn read_cursor<S: SqlPort>(sql: &S) -> Result<Cursor, String> {
    let out = sql.run_batch(vec![stmt(STATE_READ, vec![])]).await?;
    match out
        .into_iter()
        .next()
        .and_then(|o| o.rows.into_iter().next())
    {
        Some(row) => serde_json::from_value(row).map_err(|e| format!("tracker_chain_events: {e}")),
        None => Ok(Cursor::default()),
    }
}

enum Stepped {
    /// Every affected row is in `queued`: the event's cursor is passed.
    Consumed(Vec<Queued>),
    /// The pass's room ran out inside the event: these rows queued, the event held at the last one.
    Held(Vec<Queued>, Option<u32>, String),
    /// No room left for a selecting event: the pull stops before it.
    NoRoom,
}

async fn step_event<S: SqlPort>(
    sql: &S,
    event: &ChainEvent,
    at: u64,
    resume: Option<(Option<u32>, String)>,
    room: usize,
) -> Result<Stepped, String> {
    let sel = selection(event);
    if sel == Selection::None {
        return Ok(Stepped::Consumed(Vec::new()));
    }
    if room == 0 {
        return Ok(Stepped::NoRoom);
    }
    let limit = Bind::Int(room as i64 + 1);
    let rows = match &sel {
        Selection::AtOrAbove(h) => {
            let (from, after) = match resume {
                Some((Some(rh), rt)) => (rh, rt),
                _ => (*h, String::new()),
            };
            sql.run_batch(vec![stmt(
                AFFECTED_AT_OR_ABOVE,
                vec![Bind::Int(i64::from(from)), text(after), limit],
            )])
            .await?
        }
        Selection::Header(hash) => {
            let after = resume.map(|(_, t)| t).unwrap_or_default();
            sql.run_batch(vec![stmt(
                AFFECTED_BY_HEADER,
                vec![text(hash.clone()), text(after), limit],
            )])
            .await?
        }
        Selection::None => unreachable!(),
    };
    let rows = rows_of(rows)?;
    let more = rows.len() > room;
    let walked: Vec<&Row> = rows.iter().take(room).collect();
    let queued: Vec<Queued> = walked
        .iter()
        .filter_map(|row| {
            reask_of(row, event).map(|reask| Queued {
                txid: row.txid.clone(),
                reask,
                cursor: at,
            })
        })
        .collect();
    if more {
        let last = walked.last().expect("room > 0");
        let height = match sel {
            Selection::AtOrAbove(_) => last.height.map(|h| h as u32),
            _ => None,
        };
        Ok(Stepped::Held(queued, height, last.txid.clone()))
    } else {
        Ok(Stepped::Consumed(queued))
    }
}

/// ONE PULL: at most [`PAGES_PER_PASS`] pages from the stored cursor, each envelope through the crate's gate, each
/// event's affected rows (at most [`ROWS_PER_PASS`] over the pass) queued in the batch that advances the cursor.
/// A cursor at or below the stored one is never consumed again (delivery is at least once). An envelope the crate
/// refuses stops the pull at its cursor, recorded on the row; a 409 resets the cursor to `0`; a transport or page
/// fault ends the pull with the cursor kept.
pub async fn pull<S: SqlPort, F: Feed>(sql: &S, feed: &F, now: u64) -> PullOutcome {
    let mut out = PullOutcome::default();
    let count = |out: &mut PullOutcome, name: &str, n: u64| {
        if n > 0 {
            out.counters.push((name.to_string(), n));
        }
    };
    let stored = match read_cursor(sql).await {
        Ok(c) => c,
        Err(e) => {
            out.fault = Some(e);
            count(&mut out, "tracker_chain_events_faults_total", 1);
            return out;
        }
    };
    let mut next = stored.clone();
    let mut room = ROWS_PER_PASS;
    'pages: for _ in 0..PAGES_PER_PASS {
        let since = next.cursor.clone();
        out.pages += 1;
        let (status, body) = match feed.page(&since).await {
            Ok(answer) => answer,
            Err(e) => {
                out.fault = Some(format!("events binding: {e}"));
                break;
            }
        };
        if status == 409 {
            out.reset_from = Some(since);
            break;
        }
        if status != 200 {
            out.fault = Some(format!("events HTTP {status}"));
            break;
        }
        let page = match parse_page(&body) {
            Ok(p) => p,
            Err(e) => {
                out.fault = Some(e);
                break;
            }
        };
        for (at, envelope) in page.events {
            if at <= next.position() {
                continue;
            }
            let bytes = serde_json::to_vec(&envelope).unwrap_or_default();
            let event = match decode_envelope(&bytes) {
                Ok(event) => event,
                Err(e) => {
                    out.stopped = Some((at, e.to_string()));
                    next.stop_cursor = Some(at.to_string());
                    next.stop_fault = Some(e.to_string());
                    break 'pages;
                }
            };
            let resume =
                (next.held_cursor.as_deref() == Some(at.to_string().as_str())).then(|| {
                    (
                        next.held_height.map(|h| h as u32),
                        next.held_txid.clone().unwrap_or_default(),
                    )
                });
            match step_event(sql, &event, at, resume, room).await {
                Ok(Stepped::Consumed(queued)) => {
                    room -= queued.len().min(room);
                    out.queued.extend(queued);
                    out.consumed.push((at, kind_name(&event)));
                    next.cursor = at.to_string();
                    next.last_kind = Some(kind_name(&event).into());
                    next.last_cursor = Some(at.to_string());
                    next.held_cursor = None;
                    next.held_height = None;
                    next.held_txid = None;
                    next.stop_cursor = None;
                    next.stop_fault = None;
                    match &event {
                        ChainEvent::Tip { height, .. } => {
                            next.tip_height = Some(f64::from(*height));
                        }
                        ChainEvent::TipAge { seconds, tip } => {
                            next.tip_height = Some(f64::from(tip.height));
                            next.tip_age_seconds = Some(*seconds as f64);
                        }
                        _ => {}
                    }
                }
                Ok(Stepped::Held(queued, height, txid)) => {
                    out.queued.extend(queued);
                    out.held = Some(at);
                    next.held_cursor = Some(at.to_string());
                    next.held_height = height.map(f64::from);
                    next.held_txid = Some(txid);
                    break 'pages;
                }
                Ok(Stepped::NoRoom) => break 'pages,
                Err(e) => {
                    out.fault = Some(e);
                    break 'pages;
                }
            }
        }
        if !page.has_more {
            break;
        }
    }
    if next != stored {
        if let Err(e) = sql.run_batch(advance_batch(&out.queued, &next, now)).await {
            out.fault = Some(format!("advance batch: {e}"));
            out.queued.clear();
            out.consumed.clear();
            out.held = None;
            next = stored.clone();
        }
    }
    if let Some((at, fault)) = &out.stopped {
        sql.log(&format!(
            "[chain-events] STOPPED at cursor {at}: {fault} (the cursor is not passed; the operator reads the stored row)"
        ));
        if stored.stop_cursor.as_deref() != Some(at.to_string().as_str()) {
            count(&mut out, "tracker_chain_events_unreadable_total", 1);
        }
    }
    if let Some(old) = out.reset_from.clone() {
        match sql
            .run_batch(vec![stmt(
                RESET,
                vec![Bind::Int(now as i64), text(next.cursor.clone())],
            )])
            .await
        {
            Ok(_) => {
                sql.log(&format!(
                    "[chain-events] RESET: the feed answered 409 cursorAhead at cursor {old}; replaying from 0"
                ));
                count(&mut out, "tracker_chain_events_cursor_reset_total", 1);
                next = Cursor {
                    cursor: "0".into(),
                    ..next
                };
            }
            Err(e) => out.fault = Some(format!("reset: {e}")),
        }
    }
    if let Some(fault) = &out.fault {
        sql.log(&format!(
            "[chain-events] pull ended at cursor {}: {fault}",
            next.cursor
        ));
        count(&mut out, "tracker_chain_events_faults_total", 1);
    }
    let mut kinds: BTreeMap<&str, u64> = BTreeMap::new();
    for (_, kind) in &out.consumed {
        *kinds.entry(kind).or_default() += 1;
    }
    let consumed = out.consumed.len() as u64;
    count(&mut out, "tracker_chain_events_total", consumed);
    for (kind, n) in kinds {
        count(&mut out, &format!("tracker_chain_events_{kind}_total"), n);
    }
    let queued = out.queued.len() as u64;
    count(&mut out, "tracker_chain_events_queued_total", queued);
    if out.held.is_some() {
        count(&mut out, "tracker_chain_events_held_total", 1);
    }
    out.cursor = next;
    out
}

/// What the drain did with one queued re-ask.
#[derive(Clone, Debug)]
pub enum Drained {
    /// No mined row to re-ask (gone, or no longer mined): the entry deleted.
    Gone,
    /// The stored proof's root is still the active header's: the row untouched, the entry deleted.
    Held,
    /// The row rewritten (`mined` from a fresh checked proof, or `stale`), the entry deleted in the same batch.
    Rewritten(Box<Row>),
    /// Not judged this pass (`cannot_check` on a header fault, `faulted` on a fetch fault): one more attempt.
    Retry(&'static str),
}

/// The re-ask of one stored mined row: its stored proof rechecked against the host's headers at its height
/// (`Evidence::Recheck`); a root that still matches holds; one that moved asks the courier ladder for a fresh proof
/// (checked against the host's headers inside `execute_reask`), which rewrites the row `mined`, or, with none,
/// `stale` (the crate's word for a root that no longer matches).
pub async fn drain_one<W: ReaskWorld>(
    row: Option<Row>,
    reask: &Reask,
    world: &W,
    now: u64,
) -> Drained {
    let Some(row) = row.filter(|r| r.word == "mined") else {
        return Drained::Gone;
    };
    let mut stale = None;
    if let (Ok(mut state), Some(height)) = (row.restored_state(), row.height) {
        let current = world.headers(height as u32).await;
        match state.step(&PARAMS, &current, Input::Evidence(Evidence::Recheck)) {
            Ok(()) if matches!(state.word(), Word::Mined(_)) => return Drained::Held,
            Ok(()) => stale = Some(state),
            Err(_) => return Drained::Retry("cannot_check"),
        }
    }
    match world.fresh_proof(&row.txid, reask).await {
        Err(_) => Drained::Retry("faulted"),
        Ok(Some(proof)) => {
            let headers = world.headers(proof.height()).await;
            let mut fresh = State::new(&row.txid);
            match fresh.step(&PARAMS, &headers, Input::Evidence(Evidence::Proof(proof))) {
                Ok(()) if matches!(fresh.word(), Word::Mined(_)) => Drained::Rewritten(Box::new(
                    Row::from_state(&fresh, now, row.legacy_body.clone()),
                )),
                _ => Drained::Retry("cannot_check"),
            }
        }
        Ok(None) => match stale {
            Some(state) => Drained::Rewritten(Box::new(Row::from_state(
                &state,
                now,
                row.legacy_body.clone(),
            ))),
            None => Drained::Retry("faulted"),
        },
    }
}

/// The row's upsert, the existing statement ([`super::stored::UPSERT`]).
pub fn upsert_stmt(row: &Row) -> Stmt {
    let n = |v: Option<f64>| v.map_or(Bind::Null, |n| Bind::Int(n as i64));
    stmt(
        super::stored::UPSERT,
        vec![
            text(row.txid.clone()),
            text(row.word.clone()),
            text(row.tier.clone()),
            n(row.height),
            opt_text(&row.proof_root),
            n(row.proof_height),
            opt_text(&row.proof_hex),
            opt_text(&row.header_hash),
            opt_text(&row.hint_source),
            n(row.hint_at),
            Bind::Int(row.updated_at as i64),
            opt_text(&row.legacy_body),
        ],
    )
}

/// What one drain did, per entry, and its counters.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DrainOutcome {
    pub entries: Vec<(String, &'static str)>,
    pub counters: Vec<(String, u64)>,
    pub fault: Option<String>,
}

/// THE DRAIN: at most [`DRAINS_PER_PASS`] queued re-asks, oldest first, each judged by [`drain_one`] and written in
/// one batch of its own: the row's rewrite and the entry's delete together, or the entry's delete alone, or one more
/// attempt (the [`DRAIN_ATTEMPTS`]th failure drops it, counted). No row but the entry's own is touched.
pub async fn drain<S: SqlPort, W: ReaskWorld>(sql: &S, world: &W, now: u64) -> DrainOutcome {
    let mut out = DrainOutcome::default();
    let read = match sql
        .run_batch(vec![stmt(
            DRAIN_READ,
            vec![Bind::Int(DRAINS_PER_PASS as i64)],
        )])
        .await
    {
        Ok(read) => read,
        Err(e) => {
            out.fault = Some(e);
            out.counters
                .push(("tracker_chain_events_faults_total".into(), 1));
            return out;
        }
    };
    let entries = read.into_iter().next().map(|o| o.rows).unwrap_or_default();
    let mut tally: BTreeMap<&'static str, u64> = BTreeMap::new();
    for entry in entries {
        let txid = entry["q_txid"].as_str().unwrap_or_default().to_string();
        let cursor = entry["q_cursor"].as_str().unwrap_or_default().to_string();
        let height = entry["q_height"].as_f64().unwrap_or(0.0) as u32;
        let attempts = entry["q_attempts"].as_f64().unwrap_or(0.0) as i64;
        let reask = reask_of_columns(entry["q_kind"].as_str().unwrap_or("fork"), height);
        let row = if entry["word"].is_string() {
            serde_json::from_value::<Row>(entry.clone()).ok()
        } else {
            None
        };
        let key = vec![text(txid.clone()), text(cursor)];
        let judged = drain_one(row, &reask, world, now).await;
        let (outcome, stmts) = match judged {
            Drained::Gone => ("gone", vec![stmt(DRAIN_DELETE, key)]),
            Drained::Held => ("held", vec![stmt(DRAIN_DELETE, key)]),
            Drained::Rewritten(row) => (
                if row.word == "mined" {
                    "mined"
                } else {
                    "stale"
                },
                vec![upsert_stmt(&row), stmt(DRAIN_DELETE, key)],
            ),
            Drained::Retry(why) if attempts + 1 >= DRAIN_ATTEMPTS => {
                sql.log(&format!(
                    "[chain-events] re-ask of {txid} dropped after {DRAIN_ATTEMPTS} attempts ({why})"
                ));
                *tally.entry("dropped").or_default() += 1;
                (why, vec![stmt(DRAIN_DELETE, key)])
            }
            Drained::Retry(why) => (why, vec![stmt(DRAIN_RETRY, key)]),
        };
        if let Err(e) = sql.run_batch(stmts).await {
            out.fault = Some(format!("drain batch: {e}"));
            out.counters
                .push(("tracker_chain_events_faults_total".into(), 1));
            break;
        }
        *tally.entry(outcome).or_default() += 1;
        out.entries.push((txid, outcome));
    }
    let total = out.entries.len() as u64;
    if total > 0 {
        out.counters
            .push(("tracker_chain_events_reask_total".into(), total));
    }
    for (outcome, n) in tally {
        let name = if outcome == "dropped" {
            "tracker_chain_events_reask_dropped_total".to_string()
        } else {
            format!("tracker_chain_events_reask_{outcome}_total")
        };
        out.counters.push((name, n));
    }
    out
}

/// The Worker's feed: the CHAINTRACKS service binding (Worker to Worker is a binding, never a URL fetch; the host
/// below is only the path the callee sees, as `super::headers` reads it).
pub struct BindingFeed<'a>(pub &'a worker::Env);

impl Feed for BindingFeed<'_> {
    async fn page(&self, since: &str) -> Result<(u16, Vec<u8>), String> {
        let svc = self.0.service("CHAINTRACKS").map_err(|e| e.to_string())?;
        let mut resp = svc
            .fetch(format!("https://chaintracks/events?since={since}"), None)
            .await
            .map_err(|e| e.to_string())?;
        let status = resp.status_code();
        let body = resp.bytes().await.map_err(|e| e.to_string())?;
        Ok((status, body))
    }
}

/// The Worker's drain world: the host's headers over CHAINTRACKS and the existing courier ladder
/// (`super::execute_reask`, Arcade first for a live-held subject), under `pacing.rs`'s circuits.
pub struct WorkerReasks<'a> {
    pub env: &'a worker::Env,
    pub arcade: String,
}

impl ReaskWorld for WorkerReasks<'_> {
    async fn headers(&self, height: u32) -> HeaderSnapshot {
        super::headers(self.env, &[height]).await
    }
    async fn fresh_proof(&self, txid: &str, reask: &Reask) -> Result<Option<Proof>, String> {
        let env = self.env;
        let first_party = super::runtime::live_hold(env, txid).await.unwrap_or(false);
        super::execute_reask(
            txid,
            first_party,
            &self.arcade,
            reask,
            |url| async move { crate::routes::provider_get("tracker-reask", &url).await },
            |height| async move { super::headers(env, &[height]).await },
        )
        .await
    }
}

/// THE SCHEDULED STEP (`lib.rs` `scheduled`, after the backfill), under ONE deadline ([`WALL_MS`]): the indexes'
/// catch-up, the pull and its step, then the drain. A pass the deadline drops has written only whole batches.
pub async fn run(env: &worker::Env) {
    use bsv_tracker::Clock;
    let sql = match crate::routes::D1Sql::of(env) {
        Ok(sql) => sql,
        Err(e) => {
            worker::console_warn!("[chain-events] no OVERLAY_DB: {e}");
            return;
        }
    };
    let deadline = crate::routes::worker_deadline(WALL_MS, 0);
    let now = super::WorkerClock.now();
    let pass = async {
        for index in INDEXES {
            if let Err(e) = sql.run_batch(vec![stmt(index, vec![])]).await {
                sql.log(&format!("[chain-events] index deferred: {e}"));
            }
        }
        let pulled = pull(&sql, &BindingFeed(env), now).await;
        note(&pulled.counters);
        if pulled.pages > 0 {
            LAST.with(|l| *l.borrow_mut() = Some(pulled.cursor.clone()));
        }
        super::pacing::begin_tick(now / 900);
        let world = WorkerReasks {
            env,
            arcade: crate::routes::arcade_url(),
        };
        note(&drain(&sql, &world, now).await.counters);
    };
    if deadline.run(pass).await.is_none() {
        note(&[("tracker_chain_events_deadline_total".into(), 1)]);
        worker::console_warn!("[chain-events] the pass met its {WALL_MS} ms deadline");
    }
}

thread_local! {
    static LAST: RefCell<Option<Cursor>> = const { RefCell::new(None) };
    static COUNTS: RefCell<BTreeMap<String, u64>> = const { RefCell::new(BTreeMap::new()) };
}

/// Note a pass's counters: this isolate's tally (`/health.chainEvents.counters`) and the durable census
/// (`courier::note_presence`, flushed to the overlay's `ops_counters`, served on `/health/invariants`).
pub fn note(counters: &[(String, u64)]) {
    for (name, n) in counters {
        COUNTS.with(|c| *c.borrow_mut().entry(name.clone()).or_default() += n);
        crate::courier::note_presence_n(name, *n);
    }
}

/// `/health.chainEvents`: the cursor row as this isolate's last pass left it (no read route touches D1 for it), and
/// this isolate's counters.
pub fn health_json() -> Value {
    let mut v = LAST.with(|l| l.borrow().as_ref().map(Cursor::health)).unwrap_or_else(|| {
        json!({"cursor": null, "lastKind": null, "lastAt": null, "tipHeight": null, "tipAgeSeconds": null})
    });
    v["counters"] = COUNTS.with(|c| json!(*c.borrow()));
    v
}

#[cfg(test)]
mod schema_pins {
    use super::*;

    /// bsv-low #771: the two tables and three indexes are the overlay's migrations byte for byte (epoch Rule 24: one
    /// artifact), and each is in the overlay's list exactly once.
    #[test]
    fn i771_the_intakes_schema_is_the_overlays_migrations() {
        use bsv_overlay_cloudflare::d1 as o;
        assert_eq!(CREATE, o::TRACKER_CHAIN_EVENTS_CREATE);
        assert_eq!(REASKS_CREATE, o::TRACKER_REASKS_CREATE);
        assert_eq!(
            INDEXES,
            [
                o::TRACKER_REASKS_AGE_INDEX,
                o::TX_WORDS_MINED_HEIGHT_INDEX,
                o::TX_WORDS_MINED_HEADER_INDEX
            ]
        );
        for stmt in [CREATE, REASKS_CREATE].iter().chain(INDEXES.iter()) {
            assert_eq!(
                o::OVERLAY_MIGRATIONS.iter().filter(|m| *m == stmt).count(),
                1,
                "{stmt}"
            );
        }
    }
}

#[cfg(test)]
mod pins {
    //! bsv-low #771 (L1b) pins (a) to (i). The statements run over real SQLite with the overlay's production
    //! migrations (`pot_hook::tests::SqliteSql`, one transaction a batch, rolled back on any fault).
    use super::*;
    use crate::pot_hook::tests::SqliteSql;
    use bsv_rs::transaction::MerklePath;
    use bsv_tracker::{Header, HeaderError};
    use std::cell::{Cell, RefCell};

    // The stack's docs/CHAIN-EVENTS.md (chaintracks-cloudflare a5c8ae5), "One example per kind", verbatim: the
    // regtest witness of the a3-32 scenario (bsv-stack-lean corpus/export/headers_proofs/
    // 2026-10-09-a3-32-chain-event-envelope.json: checkpoint 100 `578ad467..`, A1 101 `1bcfb203..`, B1 101
    // `11d40824..`, B2 102 `3422cb84..`; a depth-1 fork at 101, then the reorg forkHeight 101, deactivated [A1],
    // newTip B2). The corpus files carry the scenario's prose, not envelope bytes; these are its envelopes.
    const TIP: &str = r#"{"v":1,"kind":"tip","height":102,"hash":"3422cb84faba77c01514deff35bb5106a6b1027e7b19a9048b5b14e1876b40bf","time":1757280102,"header":{"bits":545259519,"chainWork":"0000000000000000000000000000000000000000000000000000000000000006","hash":"3422cb84faba77c01514deff35bb5106a6b1027e7b19a9048b5b14e1876b40bf","height":102,"merkleRoot":"0000000000000000baddb2278a30410c9cb24d661391dfe4d0de91080e2538b1","nonce":2,"previousHash":"11d40824c809fb901bd1d593fdcae111008d3441abccc2e1c26879a2e5fd1105","time":1757280102,"version":536870912}}"#;
    const FORK: &str = r#"{"v":1,"kind":"fork","height":101,"competingTips":[{"bits":545259519,"chainWork":"0000000000000000000000000000000000000000000000000000000000000004","hash":"1bcfb203aa391d9680a0a48530cc59aa016ca7421c480605fc88e113036f055b","height":101,"merkleRoot":"00000000000000000419948426d5797c378cdabd979f3b34a708a6f9e621e3d9","nonce":0,"previousHash":"578ad467ca9479201762f87874cc3c91dcd76b52cb0388bfbc617aaca6bfbd85","time":1757280101,"version":536870912},{"bits":545259519,"hash":"11d40824c809fb901bd1d593fdcae111008d3441abccc2e1c26879a2e5fd1105","height":101,"merkleRoot":"0000000000000000ed99394f49f49e75b96e4deb070e14301c2e57cfa4bc902d","nonce":0,"previousHash":"578ad467ca9479201762f87874cc3c91dcd76b52cb0388bfbc617aaca6bfbd85","time":1757280101,"version":536870912,"chainWork":"0000000000000000000000000000000000000000000000000000000000000004"}],"depth":1}"#;
    const REORG: &str = r#"{"deactivatedHeaders":[{"bits":545259519,"chainWork":"0000000000000000000000000000000000000000000000000000000000000004","hash":"1bcfb203aa391d9680a0a48530cc59aa016ca7421c480605fc88e113036f055b","height":101,"merkleRoot":"00000000000000000419948426d5797c378cdabd979f3b34a708a6f9e621e3d9","nonce":0,"previousHash":"578ad467ca9479201762f87874cc3c91dcd76b52cb0388bfbc617aaca6bfbd85","time":1757280101,"version":536870912}],"depth":1,"forkHeight":101,"kind":"reorg","newTip":{"bits":545259519,"chainWork":"0000000000000000000000000000000000000000000000000000000000000006","hash":"3422cb84faba77c01514deff35bb5106a6b1027e7b19a9048b5b14e1876b40bf","height":102,"merkleRoot":"0000000000000000baddb2278a30410c9cb24d661391dfe4d0de91080e2538b1","nonce":2,"previousHash":"11d40824c809fb901bd1d593fdcae111008d3441abccc2e1c26879a2e5fd1105","time":1757280102,"version":536870912},"v":1}"#;
    const INVALIDATED: &str = r#"{"v":1,"kind":"invalidated","blockHash":"1bcfb203aa391d9680a0a48530cc59aa016ca7421c480605fc88e113036f055b"}"#;
    const FROZEN: &str = r#"{"v":1,"kind":"frozen","outpoint":{"txid":"00000000000000000419948426d5797c378cdabd979f3b34a708a6f9e621e3d9","vout":0}}"#;
    const TIP_AGE: &str = r#"{"v":1,"kind":"tipAge","seconds":600,"tip":{"bits":545259519,"chainWork":"0000000000000000000000000000000000000000000000000000000000000006","hash":"3422cb84faba77c01514deff35bb5106a6b1027e7b19a9048b5b14e1876b40bf","height":102,"merkleRoot":"0000000000000000baddb2278a30410c9cb24d661391dfe4d0de91080e2538b1","nonce":2,"previousHash":"11d40824c809fb901bd1d593fdcae111008d3441abccc2e1c26879a2e5fd1105","time":1757280102,"version":536870912}}"#;
    // Version 2 (docs/SPLITS.md): the split of the same fork point, keyed by the checkpoint, and its resolution.
    const SPLIT: &str = r#"{"v":2,"kind":"split","key":"split:578ad467ca9479201762f87874cc3c91dcd76b52cb0388bfbc617aaca6bfbd85","forkHeight":101,"tips":[{"hash":"1bcfb203aa391d9680a0a48530cc59aa016ca7421c480605fc88e113036f055b","height":101,"chainWork":"0000000000000000000000000000000000000000000000000000000000000004","rungs":["bitails"]},{"hash":"3422cb84faba77c01514deff35bb5106a6b1027e7b19a9048b5b14e1876b40bf","height":102,"chainWork":"0000000000000000000000000000000000000000000000000000000000000006","rungs":["woc"]}]}"#;
    const SPLIT_RESOLVED: &str = r#"{"v":2,"kind":"splitResolved","key":"split:578ad467ca9479201762f87874cc3c91dcd76b52cb0388bfbc617aaca6bfbd85","forkHeight":101}"#;
    // The feed's own refusal of a stored row (CHAIN-EVENTS.md, 0.4.2), an unknown kind, an unknown version, and the
    // Go server's reorg shape of the 2026-10-08 scenario (`{orphanedHashes, commonAncestor, newTip, depth}`, no `v`).
    const UNREADABLE: &str = r#"{"v":1,"kind":"unreadable","cursor":2,"fault":"fork depth 0"}"#;
    const UNKNOWN_KIND: &str = r#"{"v":1,"kind":"retarget","height":101}"#;
    const UNKNOWN_VERSION: &str = r#"{"v":3,"kind":"tip","height":102}"#;
    const GO_SHAPE: &str = r#"{"orphanedHashes":["1bcfb203aa391d9680a0a48530cc59aa016ca7421c480605fc88e113036f055b"],"commonAncestor":{"height":100,"hash":"578ad467ca9479201762f87874cc3c91dcd76b52cb0388bfbc617aaca6bfbd85"},"newTip":{"height":102,"hash":"3422cb84faba77c01514deff35bb5106a6b1027e7b19a9048b5b14e1876b40bf"},"depth":1}"#;

    const CHECKPOINT: &str = "578ad467ca9479201762f87874cc3c91dcd76b52cb0388bfbc617aaca6bfbd85";
    const A1: &str = "1bcfb203aa391d9680a0a48530cc59aa016ca7421c480605fc88e113036f055b";

    fn ev(s: &str) -> Value {
        serde_json::from_str(s).unwrap()
    }
    fn txid(i: u32) -> String {
        format!("{i:064x}")
    }
    fn header_of(height: u32) -> String {
        match height {
            100 => CHECKPOINT.into(),
            101 => A1.into(),
            h => format!("{:064x}", 0xb000 + h),
        }
    }
    /// A mined row at `height`, its proof a coinbase path (root = txid), checked against a header named `hash`.
    fn mined(id: &str, height: u32, hash: &str) -> Row {
        let mut s = State::new(id);
        let snap = HeaderSnapshot {
            tip: Ok(height),
            headers: BTreeMap::from([(
                height,
                Ok(Some(Header {
                    hash: hash.into(),
                    merkle_root: id.into(),
                })),
            )]),
        };
        s.step(
            &PARAMS,
            &snap,
            Input::Evidence(Evidence::Proof(
                Proof::new(id, MerklePath::from_coinbase_txid(id, height)).unwrap(),
            )),
        )
        .unwrap();
        Row::from_state(&s, 100, Some(r#"{"confirmed":true}"#.into()))
    }
    fn block<T>(f: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }
    /// The fork fixture: mined rows at 99, 100, 101 (on A1), 102 and 103, and one unmined row.
    fn fixture() -> SqliteSql {
        let db = SqliteSql::migrated();
        for h in 99..=103 {
            put(&db, &mined(&txid(h), h, &header_of(h)));
        }
        let mut seen = mined(&txid(500), 101, A1);
        seen.word = "seen".into();
        seen.tier = "hint".into();
        put(&db, &seen);
        db
    }
    fn put(db: &SqliteSql, row: &Row) {
        block(db.run_batch(vec![upsert_stmt(row)])).unwrap();
    }
    fn queue_of(db: &SqliteSql) -> Vec<(String, String, i64, String, i64)> {
        let mut q = db
            .conn
            .prepare("SELECT txid, kind, height, event_cursor, attempts FROM tracker_reasks ORDER BY txid")
            .unwrap();
        q.query_map([], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect()
    }
    fn queued_txids(db: &SqliteSql) -> Vec<String> {
        queue_of(db).into_iter().map(|q| q.0).collect()
    }
    fn stored_cursor(db: &SqliteSql) -> Cursor {
        block(read_cursor(db)).unwrap()
    }
    fn counter(out: &[(String, u64)], name: &str) -> u64 {
        out.iter().filter(|(k, _)| k == name).map(|(_, n)| n).sum()
    }

    /// The feed: a journal served as `GET /events?since=` serves it (pages of at most 100 in cursor order, `hasMore`,
    /// a 409 for a cursor past its head). `replay` serves every page from the start whatever `since` says (the
    /// at-least-once delivery a consumer must survive).
    struct Journal {
        events: Vec<(u64, Value)>,
        calls: RefCell<Vec<String>>,
        replay: bool,
        answer: Option<Result<(u16, Vec<u8>), String>>,
    }
    impl Journal {
        fn of(events: Vec<(u64, &str)>) -> Self {
            Journal {
                events: events.into_iter().map(|(c, e)| (c, ev(e))).collect(),
                calls: RefCell::default(),
                replay: false,
                answer: None,
            }
        }
    }
    impl Feed for Journal {
        async fn page(&self, since: &str) -> Result<(u16, Vec<u8>), String> {
            self.calls.borrow_mut().push(since.to_string());
            if let Some(a) = &self.answer {
                return a.clone();
            }
            let since_n: u64 = since.parse().map_err(|_| "400")?;
            let head = self.events.iter().map(|e| e.0).max().unwrap_or(0);
            if since_n > head {
                return Ok((409, br#"{"error":"cursorAhead"}"#.to_vec()));
            }
            let from = if self.replay { 0 } else { since_n };
            let after: Vec<&(u64, Value)> = self.events.iter().filter(|e| e.0 > from).collect();
            let page: Vec<Value> = after
                .iter()
                .take(100)
                .map(|(c, e)| json!({"cursor": c.to_string(), "event": e}))
                .collect();
            let cursor = after[..after.len().min(100)]
                .last()
                .map_or(since_n, |e| e.0);
            Ok((
                200,
                json!({"v": 1, "cursor": cursor.to_string(), "events": page, "hasMore": after.len() > 100})
                    .to_string()
                    .into_bytes(),
            ))
        }
    }

    /// (a) The recorded envelopes decode through the crate and queue exactly the mined rows at or above the height
    /// (an invalidation: exactly the rows on its header), none below, none unmined; each through the crate's own
    /// step, equal to the host's reproduction.
    #[test]
    fn i771_a_the_recorded_envelopes_queue_exactly_the_rows_at_or_above_the_height() {
        let expect: [(&str, Vec<(u32, Reask)>); 8] = [
            (TIP, vec![]),
            (
                FORK,
                (101..=103)
                    .map(|h| (h, Reask::Fork { height: 101 }))
                    .collect(),
            ),
            (
                REORG,
                (101..=103)
                    .map(|h| (h, Reask::Reorg { fork_height: 101 }))
                    .collect(),
            ),
            (INVALIDATED, vec![(101, Reask::Reorg { fork_height: 101 })]),
            (FROZEN, vec![]),
            (TIP_AGE, vec![]),
            (
                SPLIT,
                (101..=103)
                    .map(|h| (h, Reask::Fork { height: 101 }))
                    .collect(),
            ),
            (SPLIT_RESOLVED, vec![]),
        ];
        for (envelope, want) in expect {
            let event = decode_envelope(envelope.as_bytes())
                .expect("the crate decodes the recorded envelope");
            let db = fixture();
            let out = block(pull(&db, &Journal::of(vec![(1, envelope)]), 2_000));
            assert_eq!(out.consumed.len(), 1, "{envelope}: {out:?}");
            let got: Vec<(String, String, i64)> =
                queue_of(&db).into_iter().map(|q| (q.0, q.1, q.2)).collect();
            let want: Vec<(String, String, i64)> = want
                .iter()
                .map(|(h, r)| {
                    let (k, at) = reask_columns(r).unwrap();
                    (txid(*h), k.to_string(), i64::from(at))
                })
                .collect();
            assert_eq!(got, want, "{}", kind_name(&event));
            // The crate's own step and the host's reproduction agree on every row, the unmined one included.
            for h in 99..=103 {
                let row = mined(&txid(h), h, &header_of(h));
                assert_eq!(
                    reask_of(&row, &event),
                    host_reask(&row, &event),
                    "{h} {event:?}"
                );
            }
        }
        // The whole scenario in order: the reorg supersedes the fork, the invalidation lands on the same row.
        let db = fixture();
        let journal = Journal::of(vec![
            (1, TIP),
            (2, FORK),
            (3, REORG),
            (4, TIP),
            (5, TIP_AGE),
            (6, INVALIDATED),
            (7, FROZEN),
        ]);
        let out = block(pull(&db, &journal, 2_000));
        assert_eq!(out.consumed.len(), 7);
        assert_eq!(
            queue_of(&db),
            (101..=103)
                .map(|h| {
                    let by = if h == 101 { "6" } else { "3" };
                    (txid(h), "reorg".to_string(), 101, by.to_string(), 0)
                })
                .collect::<Vec<_>>()
        );
        let c = stored_cursor(&db);
        assert_eq!(c.cursor, "7");
        assert_eq!(c.last_kind.as_deref(), Some("frozen"));
        assert_eq!(c.tip_height, Some(102.0));
        assert_eq!(c.tip_age_seconds, Some(600.0));
        assert_eq!(counter(&out.counters, "tracker_chain_events_total"), 7);
        assert_eq!(counter(&out.counters, "tracker_chain_events_tip_total"), 2);
        assert_eq!(
            counter(&out.counters, "tracker_chain_events_queued_total"),
            7
        );
        assert_eq!(c.health()["tipAgeSeconds"], 600);
    }

    /// (b) An unknown kind, an unknown version and the Go server's shape stop the pull AT their cursor: the events
    /// before it are consumed, nothing at or after it, the stop recorded on the row, logged, counted once.
    #[test]
    fn i771_b_an_envelope_the_crate_refuses_stops_at_its_cursor() {
        for bad in [UNKNOWN_KIND, UNKNOWN_VERSION, GO_SHAPE] {
            let db = fixture();
            let journal = Journal::of(vec![(1, TIP), (2, bad), (3, REORG)]);
            let out = block(pull(&db, &journal, 2_000));
            assert_eq!(out.consumed, vec![(1, "tip")], "{bad}");
            assert_eq!(out.stopped.as_ref().map(|s| s.0), Some(2));
            assert!(
                queued_txids(&db).is_empty(),
                "nothing past the stop is queued"
            );
            let c = stored_cursor(&db);
            assert_eq!(c.cursor, "1");
            assert_eq!(c.stop_cursor.as_deref(), Some("2"));
            assert!(c.stop_fault.is_some());
            assert!(db
                .lines
                .borrow()
                .iter()
                .any(|l| l.contains("STOPPED at cursor 2")));
            assert_eq!(
                counter(&out.counters, "tracker_chain_events_unreadable_total"),
                1
            );
            let again = block(pull(&db, &journal, 2_100));
            assert!(again.consumed.is_empty());
            assert_eq!(again.stopped.as_ref().map(|s| s.0), Some(2));
            assert_eq!(
                counter(&again.counters, "tracker_chain_events_unreadable_total"),
                0,
                "counted once"
            );
            assert_eq!(stored_cursor(&db).cursor, "1");
        }
    }

    /// (c) The feed's own `unreadable` kind is refused by the crate's gate and stops the same way.
    #[test]
    fn i771_c_unreadable_stops_at_its_cursor() {
        assert!(matches!(
            decode_envelope(UNREADABLE.as_bytes()),
            Err(bsv_tracker::DecodeError::UnknownKind(k)) if k == "unreadable"
        ));
        let db = fixture();
        let out = block(pull(
            &db,
            &Journal::of(vec![(1, FORK), (2, UNREADABLE), (3, REORG)]),
            2_000,
        ));
        assert_eq!(out.consumed, vec![(1, "fork")]);
        assert_eq!(out.stopped.as_ref().map(|s| s.0), Some(2));
        assert_eq!(stored_cursor(&db).cursor, "1");
        assert!(
            queue_of(&db).iter().all(|q| q.1 == "fork"),
            "the reorg past the stop is not stepped"
        );
    }

    /// (d) A 409 `cursorAhead` resets the cursor to 0 ONCE, counted and logged with the old cursor; the next pass
    /// replays from 0.
    #[test]
    fn i771_d_cursor_ahead_resets_to_zero_once() {
        let db = fixture();
        block(db.run_batch(advance_batch(
            &[],
            &Cursor {
                cursor: "50".into(),
                ..Cursor::default()
            },
            1,
        )))
        .unwrap();
        let journal = Journal::of(vec![(1, TIP), (2, FORK)]);
        let out = block(pull(&db, &journal, 2_000));
        assert_eq!(out.reset_from.as_deref(), Some("50"));
        assert_eq!(
            counter(&out.counters, "tracker_chain_events_cursor_reset_total"),
            1
        );
        assert!(db
            .lines
            .borrow()
            .iter()
            .any(|l| l.contains("409 cursorAhead at cursor 50")));
        assert_eq!(stored_cursor(&db).cursor, "0");
        let next = block(pull(&db, &journal, 2_100));
        assert_eq!(
            counter(&next.counters, "tracker_chain_events_cursor_reset_total"),
            0
        );
        assert_eq!(next.consumed.len(), 2);
        assert_eq!(stored_cursor(&db).cursor, "2");
    }

    /// (e) At least once: the same page delivered again (and a cursor twice on one page) queues nothing twice and
    /// never consumes a cursor at or below the stored one.
    #[test]
    fn i771_e_a_page_delivered_twice_queues_nothing_twice() {
        let db = fixture();
        let mut journal = Journal::of(vec![(1, FORK), (1, FORK), (3, TIP), (5, INVALIDATED)]);
        journal.replay = true;
        let first = block(pull(&db, &journal, 2_000));
        assert_eq!(
            first.consumed.len(),
            3,
            "cursor 1 twice on one page is consumed once"
        );
        let before = queue_of(&db);
        let second = block(pull(&db, &journal, 2_100));
        assert!(second.consumed.is_empty());
        assert!(second.queued.is_empty());
        assert_eq!(queue_of(&db), before);
        assert_eq!(stored_cursor(&db).cursor, "5");
    }

    /// (f) The atomicity: the queue's write and the cursor's advance are ONE batch, `[QUEUE, SAVE]`; a fault inside
    /// it (here the SAVE refused by SQLite) rolls the queue back with it: nothing queued, the cursor unmoved.
    #[test]
    fn i771_f_the_queue_and_the_cursor_are_one_batch() {
        let q = vec![Queued {
            txid: txid(101),
            reask: Reask::Reorg { fork_height: 101 },
            cursor: 3,
        }];
        let sqls: Vec<String> = advance_batch(&q, &Cursor::default(), 1)
            .into_iter()
            .map(|s| s.sql)
            .collect();
        assert_eq!(sqls, vec![QUEUE.to_string(), SAVE.to_string()]);
        let sqls: Vec<String> = advance_batch(&[], &Cursor::default(), 1)
            .into_iter()
            .map(|s| s.sql)
            .collect();
        assert_eq!(sqls, vec![SAVE.to_string()]);

        struct SaveFaults(SqliteSql, Cell<u32>);
        impl SqlPort for SaveFaults {
            async fn run_batch(
                &self,
                mut stmts: Vec<Stmt>,
            ) -> Result<Vec<crate::pot_hook::StmtOut>, String> {
                if stmts.iter().any(|s| s.sql == SAVE) {
                    self.1.set(self.1.get() + 1);
                    stmts.last_mut().unwrap().sql = "INSERT INTO no_such_table VALUES (1)".into();
                }
                self.0.run_batch(stmts).await
            }
            async fn counters(&self, d: Vec<(String, u64)>) {
                self.0.counters(d).await
            }
            fn now_ms(&self) -> u64 {
                self.0.now_ms()
            }
            fn log(&self, l: &str) {
                self.0.log(l)
            }
        }
        let db = SaveFaults(fixture(), Cell::new(0));
        let out = block(pull(&db, &Journal::of(vec![(1, REORG)]), 2_000));
        assert_eq!(db.1.get(), 1, "one advance batch");
        assert!(out.fault.is_some());
        assert!(
            queued_txids(&db.0).is_empty(),
            "the queue rolled back with the cursor"
        );
        assert_eq!(stored_cursor(&db.0).cursor, "0");
        // The pull writes nothing outside that batch: its only other statements are reads.
        let src = include_str!("chain_events.rs");
        let pull_src = &src
            [src.find("pub async fn pull<").unwrap()..src.find("/// What the drain did").unwrap()];
        assert_eq!(
            pull_src.matches("run_batch(").count(),
            2,
            "the advance batch and the reset, nothing else"
        );
    }

    struct World {
        headers: BTreeMap<u32, Result<Option<Header>, HeaderError>>,
        proofs: BTreeMap<String, Result<Option<Proof>, String>>,
        asked: RefCell<Vec<String>>,
    }
    impl ReaskWorld for World {
        async fn headers(&self, _height: u32) -> HeaderSnapshot {
            HeaderSnapshot {
                tip: Ok(1_000),
                headers: self.headers.clone(),
            }
        }
        async fn fresh_proof(&self, txid: &str, _reask: &Reask) -> Result<Option<Proof>, String> {
            self.asked.borrow_mut().push(txid.to_string());
            self.proofs.get(txid).cloned().unwrap_or(Ok(None))
        }
    }
    fn word_of(db: &SqliteSql, id: &str) -> (String, Option<i64>, Option<String>) {
        db.conn
            .query_row(
                "SELECT word, height, header_hash FROM tx_words WHERE txid = ?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap()
    }

    /// (g) The drain rewrites ONLY the queued rows and deletes each entry with its rewrite: a moved root with a fresh
    /// checked proof is `mined` at its new height, a moved root with none is `stale`, a root still active holds, a
    /// header fault waits; an unqueued row above the fork is not touched.
    #[test]
    fn i771_g_the_drain_rewrites_only_the_queued_rows() {
        let db = fixture();
        put(&db, &mined(&txid(104), 104, &header_of(104)));
        let queued: Vec<Queued> = [101, 102, 103]
            .iter()
            .map(|h| Queued {
                txid: txid(*h),
                reask: Reask::Reorg { fork_height: 101 },
                cursor: 3,
            })
            .chain([Queued {
                txid: txid(500),
                reask: Reask::Fork { height: 101 },
                cursor: 3,
            }])
            .collect();
        block(db.run_batch(advance_batch(
            &queued,
            &Cursor {
                cursor: "3".into(),
                ..Cursor::default()
            },
            10,
        )))
        .unwrap();
        let untouched_before = word_of(&db, &txid(104));
        // The active chain after the reorg: 101 is B1 (another root), 102 is B2 (another root), 103 holds its root;
        // 101's transaction is re-mined at 105 (a fresh proof the host's headers check).
        let header = |h: &str, root: &str| {
            Ok(Some(Header {
                hash: h.into(),
                merkle_root: root.into(),
            }))
        };
        let world = World {
            headers: BTreeMap::from([
                (
                    101,
                    header(
                        "11d40824c809fb901bd1d593fdcae111008d3441abccc2e1c26879a2e5fd1105",
                        &"aa".repeat(32),
                    ),
                ),
                (
                    102,
                    header(
                        "3422cb84faba77c01514deff35bb5106a6b1027e7b19a9048b5b14e1876b40bf",
                        &"bb".repeat(32),
                    ),
                ),
                (103, header(&header_of(103), &txid(103))),
                (105, header(&header_of(105), &txid(101))),
            ]),
            proofs: BTreeMap::from([(
                txid(101),
                Ok(Some(
                    Proof::new(txid(101), MerklePath::from_coinbase_txid(&txid(101), 105)).unwrap(),
                )),
            )]),
            asked: RefCell::default(),
        };
        let out = block(drain(&db, &world, 3_000));
        let outcomes: BTreeMap<String, &str> = out.entries.iter().cloned().collect();
        assert_eq!(outcomes[&txid(101)], "mined");
        assert_eq!(outcomes[&txid(102)], "stale");
        assert_eq!(outcomes[&txid(103)], "held");
        assert_eq!(outcomes[&txid(500)], "gone");
        assert_eq!(
            word_of(&db, &txid(101)),
            ("mined".into(), Some(105), Some(header_of(105)))
        );
        assert_eq!(word_of(&db, &txid(102)).0, "stale");
        assert_eq!(
            word_of(&db, &txid(103)),
            ("mined".into(), Some(103), Some(header_of(103)))
        );
        assert_eq!(
            word_of(&db, &txid(104)),
            untouched_before,
            "an unqueued row is not touched"
        );
        assert_eq!(word_of(&db, &txid(99)).0, "mined");
        assert_eq!(
            word_of(&db, &txid(500)).0,
            "seen",
            "a queued row that is not mined is not rewritten"
        );
        assert!(
            queue_of(&db).is_empty(),
            "every judged entry deleted with its rewrite"
        );
        assert_eq!(
            *world.asked.borrow(),
            vec![txid(101), txid(102)],
            "only a moved root asks the couriers"
        );
        assert_eq!(
            counter(&out.counters, "tracker_chain_events_reask_total"),
            4
        );
        assert_eq!(
            counter(&out.counters, "tracker_chain_events_reask_mined_total"),
            1
        );
        assert_eq!(
            counter(&out.counters, "tracker_chain_events_reask_stale_total"),
            1
        );
    }

    /// (h) The bounds: two pages a pass; 256 rows an event, the rest walked on the next pass with the event HELD
    /// (its cursor not passed until every affected row is queued); eight drains a pass; three attempts then dropped.
    #[test]
    fn i771_h_the_per_pass_bounds() {
        // Two pages of the 450 tips, then the next two.
        let db = fixture();
        let tips: Vec<(u64, &str)> = (1..=450).map(|c| (c, TIP)).collect();
        let journal = Journal::of(tips);
        let out = block(pull(&db, &journal, 2_000));
        assert_eq!(out.pages, 2);
        assert_eq!(*journal.calls.borrow(), vec!["0", "100"]);
        assert_eq!(stored_cursor(&db).cursor, "200");
        block(pull(&db, &journal, 2_001));
        assert_eq!(stored_cursor(&db).cursor, "400");

        // 300 mined rows above the fork: 256 queued and the event held, then the 44 and the event passed.
        let db = SqliteSql::migrated();
        for i in 0..300u32 {
            put(
                &db,
                &mined(&txid(10_000 + i), 200 + i / 7, &header_of(200 + i / 7)),
            );
        }
        let journal = Journal::of(vec![(1, TIP), (2, REORG), (3, TIP_AGE)]);
        let first = block(pull(&db, &journal, 2_000));
        assert_eq!(first.queued.len(), 256);
        assert_eq!(first.held, Some(2));
        let c = stored_cursor(&db);
        assert_eq!(c.cursor, "1", "the held event's cursor is not passed");
        assert_eq!(c.held_cursor.as_deref(), Some("2"));
        assert_eq!(queue_of(&db).len(), 256);
        let second = block(pull(&db, &journal, 2_100));
        assert_eq!(second.queued.len(), 44);
        assert_eq!(second.consumed, vec![(2, "reorg"), (3, "tip_age")]);
        assert_eq!(stored_cursor(&db).cursor, "3");
        assert_eq!(stored_cursor(&db).held_cursor, None);
        assert_eq!(queue_of(&db).len(), 300, "every affected row queued once");

        // Eight drains a pass; a re-ask that cannot be judged is retried, then dropped at its third attempt.
        let world = World {
            headers: BTreeMap::from([(200, Err(HeaderError("CHAINTRACKS down".into())))]),
            proofs: BTreeMap::new(),
            asked: RefCell::default(),
        };
        let out = block(drain(&db, &world, 3_000));
        assert_eq!(out.entries.len(), 8);
        assert_eq!(queue_of(&db).len(), 300);
        let waiting: Vec<i64> = queue_of(&db)
            .iter()
            .map(|q| q.4)
            .filter(|a| *a > 0)
            .collect();
        assert_eq!(waiting, vec![1; 8]);
        block(drain(&db, &world, 3_001));
        let third = block(drain(&db, &world, 3_002));
        assert_eq!(
            counter(&third.counters, "tracker_chain_events_reask_dropped_total"),
            8
        );
        assert_eq!(queue_of(&db).len(), 292);
    }

    /// (i) EXPLAIN QUERY PLAN: no new statement scans `tx_words` or `tracker_reasks` without its index.
    #[test]
    fn i771_i_the_statements_walk_their_indexes() {
        let db = fixture();
        let plan = |sql: &str, n: usize| -> Vec<String> {
            let mut q = db
                .conn
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .unwrap();
            let binds: Vec<rusqlite::types::Value> = (0..n)
                .map(|_| rusqlite::types::Value::Text("1".into()))
                .collect();
            q.query_map(rusqlite::params_from_iter(binds.iter()), |r| {
                r.get::<_, String>(3)
            })
            .unwrap()
            .map(Result::unwrap)
            .collect()
        };
        for (sql, n, uses) in [
            (
                AFFECTED_AT_OR_ABOVE,
                3,
                "SEARCH tx_words USING INDEX tx_words_mined_height ((height,txid)>(?,?))",
            ),
            (
                AFFECTED_BY_HEADER,
                3,
                "SEARCH tx_words USING INDEX tx_words_mined_header (header_hash=? AND txid>?)",
            ),
            (DRAIN_READ, 1, "tracker_reasks_age"),
            (DRAIN_DELETE, 2, "sqlite_autoindex_tracker_reasks_1"),
            (DRAIN_RETRY, 2, "sqlite_autoindex_tracker_reasks_1"),
            (STATE_READ, 0, "INTEGER PRIMARY KEY"),
        ] {
            let details = plan(sql, n);
            assert!(
                details.iter().any(|d| d.contains(uses)),
                "{sql}: {details:?}"
            );
            assert!(
                !details.iter().any(|d| (d.starts_with("SCAN tx_words")
                    || d.starts_with("SCAN tracker_reasks"))
                    && !d.contains("USING INDEX")),
                "a scan without its index: {sql}: {details:?}"
            );
            // The drain read's outer order sorts the at most DRAINS_PER_PASS rows its LIMIT took; nothing else sorts.
            if sql != DRAIN_READ {
                assert!(
                    !details.iter().any(|d| d.contains("USE TEMP B-TREE")),
                    "{sql}: {details:?}"
                );
            }
        }
        assert!(plan(DRAIN_READ, 1)
            .iter()
            .any(|d| d.contains("SEARCH w USING INDEX sqlite_autoindex_tx_words_1")));
    }

    /// The wiring: the scheduled pass runs the intake after the backfill and before the tick; no fetch handler and
    /// no read route runs it (NS-3: no read writes a word); the feed is the CHAINTRACKS binding, never a URL fetch;
    /// `/health` serves `chainEvents` from this isolate's memory, no D1 read.
    #[test]
    fn i771_the_scheduled_pass_alone_runs_the_intake_over_the_binding() {
        let lib = include_str!("../lib.rs");
        let at = |needle: &str| lib.find(needle).unwrap_or_else(|| panic!("{needle}"));
        assert!(
            at("tracker_host::runtime::backfill(&env)")
                < at("tracker_host::chain_events::run(&env)")
        );
        assert!(
            at("tracker_host::chain_events::run(&env)") < at("tracker_host::runtime::tick(&env)")
        );
        assert!(at("tracker_host::chain_events::run(&env)") < at("pub async fn fetch("));
        assert_eq!(lib.matches("chain_events::run(").count(), 1);
        let routes = include_str!("../routes.rs");
        for call in [
            "chain_events::run(",
            "chain_events::pull(",
            "chain_events::drain(",
        ] {
            assert!(!routes.contains(call), "a route runs {call}");
        }
        let health = &routes[routes.find("pub fn health(").unwrap()..];
        let health = &health[..health.find("\n}\n").unwrap()];
        assert!(health.contains("chain_events::health_json()"));
        let src = include_str!("chain_events.rs");
        let feed = &src[src.find("impl Feed for BindingFeed").unwrap()
            ..src.find("pub struct WorkerReasks").unwrap()];
        assert!(feed.contains(".service(\"CHAINTRACKS\")"));
        assert!(!feed.contains("Fetch::Url") && !feed.contains("Fetch::Request"));
        let health_fn = &src[src.find("pub fn health_json()").unwrap()..];
        assert!(!health_fn[..health_fn.find("\n}\n").unwrap()].contains("d1("));
    }
}
