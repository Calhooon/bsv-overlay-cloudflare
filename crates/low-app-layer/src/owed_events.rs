//! bsv-low #911 (lane D2-ACTOR-A; `DESIGN-D2-ACTOR-2026-10-26.md` sections 3, 6 and 11): THE CONSUMER of the
//! `owed-events` queue, its dead letters, the un-acked mark sweep and the served ages. The publish is ONE source for
//! both Workers (`owed_events_publish`, the overlay's `owed_events.rs` by path).
//!
//! The consumer is the judge's ONLY trigger (the hooks, the record door and the word ingest publish, never judge).
//! For each message ([`route`]): a `word` event's frame is ingested first (the tracker's own write, which the word
//! triggers mark); then the marks the event's source row made are read, `SELECT identity, gameId, generation FROM
//! owed_dirty WHERE source=? AND sourceKey=?` (one covering-index read on `idx_owed_dirty_source_key`; an identity
//! route reads the identity's own marks), each identity's games are judged through [`deliver`] (the ONE call lane B
//! swaps for the actor), and the judge's own committed batch ACKNOWLEDGES those marks (`ACK_DIRTY_SQL`,
//! generation-bounded, in the batch that writes the rows: never before the judge wrote). At most
//! [`owed_event::MAX_GAMES_PER_MESSAGE`] subjects a message: the rest are a continuation event published by the
//! consumer. A judge that faults leaves the marks and hands the message back (`max_retries = 3`, then the DLQ).
//!
//! THE SWEEP ([`sweep`], on the belt's 120 s alarm; FIX-956, bsv-low #956): `owed_dirty` is the durable intent ledger,
//! so a publish cancelled or refused is never a lost event; the sweep re-publishes a mark only once the record says
//! its message is LOST, never because the consumer has not reached it yet. Until FIX-956 every mark older than one
//! tick was re-published, so with the serial consumer's backstop lag past a tick (273 s p95 on beta) every mark was
//! re-published while its own message waited in the queue: 604 re-publishes in 6.7 quiet hours, each a direct call
//! and a queue message adding to the lag it was meant to cover. Now the consumer records its HORIZON in its batch row
//! (`horizonMs`: the greatest publish stamp, the event's `atMs`, among the messages its batch acknowledged; never
//! lowered, [`CONSUMER_ROW_SQL`]), and a mark older than one tick is UN-ACKED when EITHER the horizon has passed it by
//! [`SWEEP_HORIZON_MARGIN_MS`] (one tick, for the queue's re-ordering of retried messages: the consumer judged a
//! message published after this mark's, so this mark's own message is gone) OR it is older than
//! [`SWEEP_STALLED_AFTER_MS`] (five ticks: the consumer dead or the queue stalled, where re-publishing is the only
//! belt). One bind ([`sweep_bound`]) over `idx_owed_dirty_backlog`, oldest first past a rotating cursor, 64 pairs a
//! tick. AT MOST ONCE A PASS: a re-published pair is stamped (`owed-events:swept:<source> <sourceKey>`, the fault
//! memos' shape, in the sweep's cursor batch) and not re-published again until the horizon passes that stamp by the
//! margin, or the long bound passes it ([`swept_blocks`]); the consumer's landed judge of the pair clears the stamp.
//! A pair a publisher KNOWS it lost (its send or seq batch faulted, the answer wall dropped it) carries a LOST NOTE
//! (`owed_events_publish::LOST_MEMO_PREFIX`) and is re-published at the next tick whatever the horizon, the note
//! deleted in that tick's batch. A pair held by its fault memo waits out its hold. With no queue binding the drain
//! judges as before.
//!
//! THE D1-REAL CLASS, CLOSED IN THIS MODULE (D2-ACTOR-B-FOLD-12, the seventh delta lens's NEW7-M1; FOLD-7's `asOfMs`
//! and FOLD-8's NEW4-M1 before it): D1 binds every `Bind::Int` as a JS number, so a JSON value SQLite builds from an
//! integer bind (`json_object('k',?n)`) is stored as a REAL (`1793….0`) and an `as_i64` reader reads none. The swept
//! stamp was that case (`json_object('sweptAtMs',?2)`): on D1 it never blocked, so a passed pair was re-published every
//! tick until consumed and a stalled consumer's every 120 s, not 600 s. It is written `CAST(?2 AS INTEGER)` now and read
//! through [`ms_of`] (an integer, or a REAL's value). Every other `owed_belt_state` cursor the sweep, the publisher and
//! the consumer write is JSON TEXT built by serde from a Rust integer (the sweep's cursor, the consumer's row and its
//! `json_set` of two such values, the fault and subject memos, the lost notes), so it stores integers; each is read
//! through [`ms_of`] all the same, and the `tickedAtMs` column (a REAL on D1) is only compared in SQL or read through
//! [`ms_of`]. The sweep's two range reads (the lost notes, the prune) read at most what they take ([`LOST_READ_SQL`],
//! [`SWEPT_WINDOW_SQL`]: NEW7-L1).
use crate::owed_events_publish as publish;
use crate::owed_judge::{self, JudgeDb};
use crate::pot_hook::{Bind, SqlPort, Stmt};
use overlay_discovery::owed_event::{self, OwedEvent};
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[cfg(test)]
mod tests;

/// MEDIUM-1 (the D2-ACTOR-A lens): the `owed-events` consumer's `max_concurrency` in every app-layer render: ONE
/// judge at a time fleet-wide, the serialiser of the judge per identity until lane B's per-identity actor. Two
/// consumers judging one identity can commit the first's stale rows over the second's with every mark acknowledged
/// (pinned by `d2_actor_a_fold_m1_*`).
pub const CONSUMER_MAX_CONCURRENCY: u32 = 1;

/// D2-ACTOR-D (bsv-low #952): the render's knob of the consumer's concurrency (`[vars]`), moved by the cutover doc with
/// the phase and pinned equal to each render's `max_concurrency`. A value past 1 is honoured only with the D1 judge
/// OFF (Phase C: the actor is the one writer, each identity serialised by its own Durable Object); with it on (Phases
/// A and B) two consumers are two D1 writers of one identity's rows, so the consumer counts and logs the refusal
/// (every batch, [`consumer_concurrency_refused`]) and the cutover must not deploy it.
pub const CONSUMER_CONCURRENCY_VAR: &str = "OWED_CONSUMER_CONCURRENCY";
/// PURE: the knob's value (a missing or malformed var is 1).
pub fn consumer_concurrency_of(var: Option<&str>) -> u32 {
    var.and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(1)
}
/// PURE: is a consumer concurrency of `n` refused under the phase (`d1_judges`: Phases A and B)?
pub fn consumer_concurrency_refused(n: u32, d1_judges: bool) -> bool {
    n > CONSUMER_MAX_CONCURRENCY && d1_judges
}

/// FIX-952 (bsv-low #952, item 3): the consumer's concurrency in PHASE C (`OWED_D1_JUDGE=false`): the render's pair,
/// `max_concurrency` in the `low-owed-events` consumer block and `OWED_CONSUMER_CONCURRENCY` in `[vars]`, moved
/// TOGETHER by the Phase C deploy (the two commented `PHASE C:` lines in each render of `wrangler.toml` are the
/// lever's exact edit, pinned equal to this constant).
///
/// WHY FOUR. In Phase C the consumer makes NO D1 judge call and writes no `owed_rows`: per event it reads the pair's
/// marks (2 statements, one round trip), the marks' bound (1), calls the identity's actor once (the DO serialises the
/// identity; the actor's seq orders its events) and acknowledges the marks in one batch (4 statements); the batch's
/// identities go at once ([`JUDGE_IDENTITIES_AT_ONCE`]), so one consumer clears a batch of 16 in about one actor
/// apply and three D1 round trips (1 to 3 s under the loop's load): about 320 to 960 events a minute a consumer.
/// Four consumers cover the L53 peak (243 a minute, record 1547) at least five times over, while the actor applies
/// in flight stay at most 4 x 16 = 64, the direct pass's own fan-out ([`DIRECT_EVENTS_MAX`]): no more of the actors'
/// compound D1 reads land behind the alarm's batches (record 1537) than one direct pass already puts there. Why not
/// more: each further consumer adds 16 concurrent actor applies, each a D1 round trip, for a backlog four already
/// drain faster than the loop fills it. WHY N CONSUMERS CANNOT RACE ON D1 IN C: they write no `owed_rows` (the actor
/// is the one writer, in its own SQLite); their only D1 writes are the acknowledgement's deletes of marks at or below
/// a generation each read before its own call ([`crate::owed_actor::phase_c_ack_statements`]), idempotent and
/// monotone whatever their order, and the two restart statements, idempotent.
pub const CONSUMER_CONCURRENCY_PHASE_C: u32 = 4;
/// PURE: the consumer concurrency a render's phase takes: 1 while the D1 judge writes `owed_rows` (A and B:
/// [`CONSUMER_MAX_CONCURRENCY`]), [`CONSUMER_CONCURRENCY_PHASE_C`] with it off (C).
pub fn consumer_concurrency_for(d1_judges: bool) -> u32 {
    if d1_judges {
        CONSUMER_MAX_CONCURRENCY
    } else {
        CONSUMER_CONCURRENCY_PHASE_C
    }
}

/// The pair's marks, oldest generation first (`idx_owed_dirty_source_key`; the source filtered in the index's row),
/// the marks of a HELD subject left out (D2-ACTOR-A-DELTA-2's NEW2-M1: its subject memo's hold runs past `?4`, one
/// primary-key probe of `owed_belt_state` a mark), so a held subject never takes a place of the 64.
pub const ROUTE_MARKS_SQL: &str = "SELECT identity,gameId,generation FROM owed_dirty INDEXED BY idx_owed_dirty_source_key WHERE sourceKey=?2 AND source=?1 AND NOT EXISTS (SELECT 1 FROM owed_belt_state WHERE name='owed-events:fault:subject '||identity||'/'||gameId AND json_extract(cursor,'$.heldUntilMs')>?4) ORDER BY generation LIMIT ?3";
/// An identity route's marks, oldest first (`idx_owed_dirty_identity_marked`), a held subject's left out (`?3` now).
pub const ROUTE_IDENTITY_SQL: &str = "SELECT identity,gameId,generation FROM owed_dirty INDEXED BY idx_owed_dirty_identity_marked WHERE identity=?1 AND NOT EXISTS (SELECT 1 FROM owed_belt_state WHERE name='owed-events:fault:subject '||identity||'/'||gameId AND json_extract(cursor,'$.heldUntilMs')>?3) ORDER BY markedAtMs,generation LIMIT ?2";
/// NEW2-M1: the HELD subjects among the pair's first `?3` marks (`?4` now), in the batch of the marks' read: the
/// figure an all-held message is counted by (bounded by the window, never the pair's whole backlog).
pub const ROUTE_HELD_SQL: &str = "SELECT COUNT(DISTINCT d.identity||'/'||d.gameId) AS n FROM (SELECT identity,gameId FROM owed_dirty INDEXED BY idx_owed_dirty_source_key WHERE sourceKey=?2 AND source=?1 ORDER BY generation LIMIT ?3) d WHERE d.gameId<>'' AND EXISTS (SELECT 1 FROM owed_belt_state WHERE name='owed-events:fault:subject '||d.identity||'/'||d.gameId AND json_extract(cursor,'$.heldUntilMs')>?4)";
/// NEW2-M1: an identity route's held subjects among its first `?2` marks (`?3` now).
pub const ROUTE_IDENTITY_HELD_SQL: &str = "SELECT COUNT(DISTINCT d.identity||'/'||d.gameId) AS n FROM (SELECT identity,gameId FROM owed_dirty INDEXED BY idx_owed_dirty_identity_marked WHERE identity=?1 ORDER BY markedAtMs,generation LIMIT ?2) d WHERE d.gameId<>'' AND EXISTS (SELECT 1 FROM owed_belt_state WHERE name='owed-events:fault:subject '||d.identity||'/'||d.gameId AND json_extract(cursor,'$.heldUntilMs')>?3)";
/// Mark rows one message reads (a subject can hold several marks).
pub const ROUTE_READ_LIMIT: usize = 4 * owed_event::MAX_GAMES_PER_MESSAGE;

/// A mark is never re-published younger than this (one belt tick).
pub const SWEEP_AFTER_MS: i64 = crate::owed_belt::OWED_BELT_TICK_MS;
/// FIX-956: the consumer's horizon must pass a mark by this much before the mark reads lost (one tick: the queue
/// re-orders a retried message behind later ones).
pub const SWEEP_HORIZON_MARGIN_MS: i64 = SWEEP_AFTER_MS;
/// FIX-956: past this a mark is re-published whatever the horizon (five ticks: the consumer dead or the queue
/// stalled; the only belt there).
pub const SWEEP_STALLED_AFTER_MS: i64 = 5 * SWEEP_AFTER_MS;
/// FIX-956: the swept stamps' prefix (`owed-events:swept:<source> <sourceKey>`, cursor `{sweptAtMs}`).
pub const SWEPT_MEMO_PREFIX: &str = "owed-events:swept:";
/// The swept stamp's name of a pair.
pub fn swept_memo_name(source: &str, key: &str) -> String {
    format!("{SWEPT_MEMO_PREFIX}{source} {key}")
}
/// Write one swept stamp (`?1` its name, `?2` now).
pub const SWEPT_MEMO_WRITE_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES (?1,json_object('sweptAtMs',CAST(?2 AS INTEGER)),?2) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// A stamp no consumer cleared (its marks acknowledged by another road) is pruned after a day. D2-ACTOR-B-FOLD-12 (the
/// seventh delta lens's NEW7-L1): the prune reads at most [`SWEPT_PRUNE_WINDOW`] stamps a tick, the WINDOW past its
/// cursor (`pruneAfter` in the sweep's cursor row) in key order ([`SWEPT_WINDOW_SQL`], a primary-key range, no sort),
/// and deletes the window's stamps a day old by name ([`SWEPT_PRUNE_SQL`], the age read again in the statement, so a
/// stamp written again since the read is spared); a short window wraps the cursor. It read every stamp of its range
/// to find 64 a day old (100,023 VM steps over 20,000 young stamps); a round over N stamps is now `ceil(N / 64)`
/// ticks, each reading 64 rows at most.
pub const SWEPT_WINDOW_SQL: &str = "SELECT name,tickedAtMs FROM owed_belt_state WHERE name>?1 AND name<'owed-events:swept;' ORDER BY name LIMIT ?2";
/// The stamps the prune reads a tick.
pub const SWEPT_PRUNE_WINDOW: usize = 64;
/// Delete the window's stamps older than `?2` (`?1` a JSON array of their names).
pub const SWEPT_PRUNE_SQL: &str =
    "DELETE FROM owed_belt_state WHERE name IN (SELECT value FROM json_each(?1)) AND tickedAtMs<?2";
/// How long an uncleared swept stamp is kept.
pub const SWEPT_PRUNE_AFTER_MS: i64 = 24 * 3_600_000;
/// FIX-956 1b: the lost notes, at most `?1`, in key order past the cursor `?2` (`lostAfter` in the sweep's cursor row;
/// the prefix itself at a round's start). D2-ACTOR-B-FOLD-12 (NEW7-L1): a primary-key range read to its LIMIT, never
/// sorted (it sorted every note of the range by `tickedAtMs`: 40,614 VM steps over 5,000 notes). A full read moves the
/// cursor to its last note, a short one wraps it, so a note held by its fault memo (kept, the sweep leaves it) is passed
/// by the cursor and never keeps a newer note out of the tick's places: every note is read within `ceil(N / 32)` ticks
/// of N notes, where oldest-first kept up to 32 held notes in the places for as long as their holds ran.
pub const LOST_READ_SQL: &str = "SELECT name,cursor FROM owed_belt_state WHERE name>?2 AND name<'owed-events:lost;' ORDER BY name LIMIT ?1";
/// PURE (NEW7-L1): a range cursor read back: the saved name when it lies in the prefix's range, else the prefix (a
/// round's start; the prefix itself sorts before every name of its range).
pub fn range_cursor(saved: Option<&str>, prefix: &str) -> String {
    saved
        .filter(|c| c.starts_with(prefix))
        .unwrap_or(prefix)
        .to_owned()
}
/// PURE (NEW7-L1): the next cursor after a window read of `limit` rows: its last name when the window was full, else
/// the prefix (the round wraps).
pub fn next_range_cursor(names: &[&str], limit: usize, prefix: &str) -> String {
    match names.last() {
        Some(last) if names.len() >= limit => (*last).to_owned(),
        _ => prefix.to_owned(),
    }
}
/// FIX-956 1b: the most lost notes one tick takes (the rest of its 64 places are the candidates').
pub const SWEEP_LOST_PER_TICK: usize = SWEEP_PAIRS_PER_TICK / 2;
/// FIX-956 1b: delete one lost note (the sweep re-published its pair).
pub const LOST_CLEAR_SQL: &str = "DELETE FROM owed_belt_state WHERE name=?1";

/// PURE (FIX-956): the sweep's one bound: a mark older than one tick, AND passed by the consumer's horizon by the
/// margin OR older than the long bound (`horizon` `None`: the consumer never recorded one; the long bound alone).
pub fn sweep_bound(now_ms: i64, horizon_ms: Option<i64>) -> i64 {
    let passed = horizon_ms.map_or(i64::MIN, |h| h - SWEEP_HORIZON_MARGIN_MS);
    (now_ms - SWEEP_AFTER_MS).min(passed.max(now_ms - SWEEP_STALLED_AFTER_MS))
}

/// Why the sweep re-publishes a pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SweepReason {
    /// The consumer's horizon passed its oldest mark.
    Passed,
    /// The long bound alone.
    Stalled,
    /// A publisher noted it lost.
    Lost,
}

/// PURE: the reason of a mark marked at `marked_at_ms` under [`sweep_bound`] (passed when the horizon covers it).
pub fn sweep_reason(marked_at_ms: i64, horizon_ms: Option<i64>) -> SweepReason {
    if horizon_ms.is_some_and(|h| marked_at_ms < h - SWEEP_HORIZON_MARGIN_MS) {
        SweepReason::Passed
    } else {
        SweepReason::Stalled
    }
}

/// PURE (FIX-956, at most once a pass): does a swept stamp at `swept_at_ms` hold its pair back? Until the horizon has
/// passed the re-publish by the margin (the consumer has not yet reached the re-published message), and the long
/// bound has not passed it.
pub fn swept_blocks(swept_at_ms: i64, now_ms: i64, horizon_ms: Option<i64>) -> bool {
    let passed = horizon_ms.is_some_and(|h| swept_at_ms < h - SWEEP_HORIZON_MARGIN_MS);
    !passed && now_ms - swept_at_ms < SWEEP_STALLED_AFTER_MS
}
/// The pairs one sweep tick re-publishes.
pub const SWEEP_PAIRS_PER_TICK: usize = 64;
/// The mark rows one sweep tick reads past its cursor (several marks share a pair).
pub const SWEEP_READ_LIMIT: usize = 256;
/// The sweep's walk, oldest first past the cursor `(?2, ?3)`, only marks older than `?1` (`idx_owed_dirty_backlog`).
pub const SWEEP_SQL: &str = "SELECT generation,source,sourceKey,markedAtMs,identity,gameId FROM owed_dirty INDEXED BY idx_owed_dirty_backlog WHERE markedAtMs<?1 AND (markedAtMs>?2 OR (markedAtMs=?2 AND generation>?3)) ORDER BY markedAtMs,generation LIMIT ?4";
/// The sweep's cursor row and (FIX-956) the consumer's row, whose `horizonMs` bounds the sweep: two primary-key rows.
pub const SWEEP_CURSOR_READ_SQL: &str = "SELECT name,cursor FROM owed_belt_state WHERE name IN ('owed-events:sweep','owed-events:consumer')";
/// Write the sweep's cursor.
pub const SWEEP_CURSOR_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('owed-events:sweep',?1,?2) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// MEDIUM-8: the consumer's batch row: the oldest message's enqueue age (a Worker cannot read its queue's backlog;
/// the pass-meter precedent), served on `/health.owedEvents` and the overlay's `/health/invariants.owedEvents`.
/// FIX-956: the row carries the consumer's HORIZON, `horizonMs`, the greatest publish stamp among the messages a
/// batch acknowledged, never lowered (a batch that acknowledged none, or an older one, keeps the row's).
pub const CONSUMER_ROW_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('owed-events:consumer',?1,?2) ON CONFLICT(name) DO UPDATE SET cursor=json_set(excluded.cursor,'$.horizonMs',MAX(COALESCE(json_extract(excluded.cursor,'$.horizonMs'),0),COALESCE(json_extract(owed_belt_state.cursor,'$.horizonMs'),0))),tickedAtMs=excluded.tickedAtMs";
/// PURE (D2-ACTOR-B-FOLD-12, the seventh delta lens's NEW7-M1): a millisecond stamp in a cursor, read as an integer
/// or as the REAL D1 stores a JS-number bind as (`1793….0`), so a stamp stored either way reads.
pub fn ms_of(v: &Value) -> Option<i64> {
    v.as_i64().or_else(|| v.as_f64().map(|f| f as i64))
}
/// PURE: the horizon a consumer row's cursor names (`None`: none recorded).
pub fn horizon_of(cursor: &Value) -> Option<i64> {
    ms_of(&cursor["horizonMs"]).filter(|h| *h > 0)
}
/// The consumer's row, and beside it (D2-ACTOR-B-FOLD-6, the land tier's `health-dirty-snapshot`) the owed actor's
/// health snapshot (`owed_actor::HEALTH_SNAPSHOT_NAME`): two primary-key rows in one statement, so `/health.owedActor`
/// adds one row read and no statement.
pub const HEALTH_CONSUMER_SQL: &str = "SELECT name,cursor,tickedAtMs FROM owed_belt_state WHERE name IN ('owed-events:consumer','owed-actor:health','fleet-compare:health')";
/// D2-ACTOR-A-FOLD-5 (bsv-low #911, E2): the queue's two totals `/health.owedEvents` serves (the harness's
/// `owed-queue-backlog` row reads published minus consumed there): two primary-key rows of `ops_counters`, never the
/// family's whole counter read.
pub const HEALTH_TOTALS_SQL: &str = "SELECT name,value FROM ops_counters WHERE name IN ('owed_events_published_total','owed_events_consumed_total')";
/// LOW-3 and the lens's MEDIUM-2: park one dead letter ON ITS PAIR'S ROW (`?1` source, `?2` sourceKey, `?3` the
/// message id, `?4` the body, `?5` now, `?6` the pair's fault memo row, `?7` the bound, `?8`
/// `owed_events_publish::MAX_REDRIVES`). A new pair is taken only under the bound (a full table answers no row: the
/// platform retries it); a pair already parked is re-parked in place: the newest body, `parks` counted once a message
/// (a redelivery of the same message counts nothing), `lastFault` the memo's last error, and `exhausted` once the
/// lever re-drove it [`publish::MAX_REDRIVES`] times. Answers the row's status.
pub const DEAD_LETTER_PARK_SQL: &str = "INSERT INTO owed_event_dead_letters (source,sourceKey,messageId,body,parks,lastFault,status,parkedAt,redrives) SELECT ?1,?2,?3,?4,1,(SELECT json_extract(cursor,'$.lastFault') FROM owed_belt_state WHERE name=?6),'parked',?5,0 WHERE (SELECT COUNT(*) FROM (SELECT 1 FROM owed_event_dead_letters LIMIT ?7))<?7 OR EXISTS (SELECT 1 FROM owed_event_dead_letters WHERE source=?1 AND sourceKey=?2) ON CONFLICT(source,sourceKey) DO UPDATE SET parks=owed_event_dead_letters.parks+(owed_event_dead_letters.messageId IS NOT excluded.messageId),messageId=excluded.messageId,body=excluded.body,lastFault=COALESCE(excluded.lastFault,owed_event_dead_letters.lastFault),parkedAt=excluded.parkedAt,status=CASE WHEN owed_event_dead_letters.redrives>=?8 THEN 'exhausted' ELSE 'parked' END RETURNING status";
/// The pair's letter is resolved: its judge landed (in the batch that clears its fault memo).
pub const DEAD_LETTER_RESOLVE_SQL: &str =
    "DELETE FROM owed_event_dead_letters WHERE source=?1 AND sourceKey=?2";

/// MEDIUM-2 (the D2-ACTOR-A lens): THE FAULT MEMO, one `owed_belt_state` row per pair whose judge faulted, named
/// `owed-events:fault:<source> <sourceKey>`, its cursor `{faults, lastFault, lastFaultAtMs, heldUntilMs}`: the
/// consecutive faults (a landed judge deletes the row), the last error (the cause, readable), and the hold. From the
/// [`FAULT_HOLD_AFTER`]th fault the pair is HELD for a doubling period ([`fault_hold_ms`], 120 s to 6 h): the sweep
/// leaves it out and the consumer hands its message back with that delay. So a pair whose judge faults for good
/// costs four deliveries a hold (the platform's 1 + `max_retries`) and ONE park row, at most 16 attempts a day once
/// the hold is 6 h; it was 2,880 attempts and 720 park rows a day.
pub const FAULT_MEMO_PREFIX: &str = "owed-events:fault:";
/// The consecutive faults after which a pair is held.
pub const FAULT_HOLD_AFTER: i64 = 3;
/// The first hold: one sweep tick.
pub const FAULT_HOLD_BASE_MS: i64 = SWEEP_AFTER_MS;
/// The longest hold.
pub const FAULT_HOLD_CAP_MS: i64 = 6 * 3_600_000;
/// The memo rows of the pairs in `?1` (a JSON array of row names).
pub const FAULT_MEMO_READ_SQL: &str =
    "SELECT name,cursor FROM owed_belt_state WHERE name IN (SELECT value FROM json_each(?1))";
/// Write one pair's memo.
pub const FAULT_MEMO_WRITE_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES (?1,?2,?3) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// Clear one pair's memo (its judge landed).
pub const FAULT_MEMO_CLEAR_SQL: &str = "DELETE FROM owed_belt_state WHERE name=?1";
/// The memo row's name of one subject.
pub fn subject_memo_name(identity: &str, game: &str) -> String {
    format!("{FAULT_MEMO_PREFIX}subject {identity}/{game}")
}

/// The memo row's name of a pair.
pub fn fault_memo_name(source: &str, key: &str) -> String {
    format!("{FAULT_MEMO_PREFIX}{source} {key}")
}

/// PURE: the hold after `faults` consecutive faults: none before [`FAULT_HOLD_AFTER`], then 120 s doubling to 6 h.
pub fn fault_hold_ms(faults: i64) -> i64 {
    if faults < FAULT_HOLD_AFTER {
        return 0;
    }
    let shift = (faults - FAULT_HOLD_AFTER).clamp(0, 20) as u32;
    FAULT_HOLD_BASE_MS
        .saturating_mul(1i64 << shift)
        .min(FAULT_HOLD_CAP_MS)
}

/// What the memo says of one pair after a fault.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultMemo {
    /// Consecutive faults, this one included.
    pub faults: i64,
    /// The hold this fault set (0: none yet).
    pub hold_ms: i64,
    /// The hold's end (ms; `now` when none).
    pub held_until_ms: i64,
}

/// MEDIUM-2: record one judge fault of `(source, key)`: one read of its memo, one write (the fault path only).
pub async fn note_fault(
    db: &impl SqlPort,
    source: &str,
    key: &str,
    fault: &str,
) -> Result<FaultMemo, String> {
    note_fault_named(db, &fault_memo_name(source, key), fault).await
}

/// One fault on the memo row `name` (a pair's, or a subject's: NEW-M1).
pub async fn note_fault_named(db: &impl SqlPort, name: &str, fault: &str) -> Result<FaultMemo, String> {
    let name = name.to_owned();
    let now = db.now_ms() as i64;
    let held = rows(
        db,
        FAULT_MEMO_READ_SQL,
        vec![Bind::Text(json!([name]).to_string())],
    )
    .await?;
    let prior = held
        .first()
        .and_then(|r| r["cursor"].as_str())
        .and_then(|c| serde_json::from_str::<Value>(c).ok())
        .map(|c| stale_memo_faults(&c, now))
        .unwrap_or(0);
    let faults = prior + 1;
    let hold_ms = fault_hold_ms(faults);
    let memo = FaultMemo {
        faults,
        hold_ms,
        held_until_ms: now + hold_ms,
    };
    let clipped: String = fault.chars().take(300).collect();
    let cursor = json!({"faults": faults, "lastFault": clipped, "lastFaultAtMs": now, "heldUntilMs": memo.held_until_ms});
    db.run_batch(vec![Stmt {
        sql: FAULT_MEMO_WRITE_SQL.into(),
        binds: vec![
            Bind::Text(name),
            Bind::Text(cursor.to_string()),
            Bind::Int(now),
        ],
    }])
    .await?;
    Ok(memo)
}

/// PURE (D2-ACTOR-A-DELTA-2's LOW-2): the consecutive faults a memo carries into the next fault. A memo whose hold
/// ended more than [`FAULT_HOLD_CAP_MS`] ago counts from 0: its subject was judged meanwhile by another road (the
/// belt's find, the backfill), which clears no memo, or simply never faulted again; a transient fault then never
/// re-arms a 6 h hold at once.
pub fn stale_memo_faults(cursor: &Value, now_ms: i64) -> i64 {
    let faults = ms_of(&cursor["faults"]).unwrap_or(0);
    let until = ms_of(&cursor["heldUntilMs"])
        .or_else(|| ms_of(&cursor["lastFaultAtMs"]))
        .unwrap_or(now_ms);
    if now_ms > until.saturating_add(FAULT_HOLD_CAP_MS) {
        0
    } else {
        faults
    }
}

thread_local! {
    /// LOW-1 (the second delta lens): the subject memos of the judge calls in flight, so a message the wall drops
    /// notes THOSE subjects, never the pair (a judge that outlasts the wall every time is one identity's, not its
    /// pair's neighbours'). FIX-952 (bsv-low #952): a SET, since a pass delivers its identities at once
    /// ([`judge_plan`]): each identity's chunk enters when its call starts and leaves when it answers, and the wall's
    /// branch takes whatever is still in flight across them.
    static IN_FLIGHT: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// The subject memos of the judge calls in flight (taken: the wall's branch).
pub fn take_in_flight() -> Vec<String> {
    IN_FLIGHT.with(|f| std::mem::take(&mut *f.borrow_mut()))
}

/// One identity's chunk enters the in-flight set (its judge call starts).
fn enter_in_flight(identity: &str, games: &[String]) {
    IN_FLIGHT.with(|f| {
        f.borrow_mut()
            .extend(games.iter().map(|g| subject_memo_name(identity, g)));
    });
}

/// One identity's chunk leaves the in-flight set (its judge call answered).
fn leave_in_flight(identity: &str, games: &[String]) {
    IN_FLIGHT.with(|f| {
        let mut f = f.borrow_mut();
        for g in games {
            let name = subject_memo_name(identity, g);
            if let Some(i) = f.iter().position(|x| *x == name) {
                f.swap_remove(i);
            }
        }
    });
}

/// LOW-1: the wall fell during a judge call: note each subject in flight on its own memo; the delay to hand the
/// message back with is the longest hold that set, at least [`CONSUMER_WALL_RETRY_S`]. `None`: no call was in flight
/// (the plan's reads, the ingest): the caller notes the pair as before.
pub async fn note_wall_subjects(db: &impl SqlPort, names: &[String]) -> Option<u32> {
    if names.is_empty() {
        return None;
    }
    let mut hold_ms = 0i64;
    for name in names {
        if let Ok(m) = note_fault_named(db, name, "the consumer's message wall").await {
            hold_ms = hold_ms.max(m.hold_ms);
        }
    }
    Some(((hold_ms / 1000) as u32).clamp(CONSUMER_WALL_RETRY_S, 43_200))
}

/// The statements a landed judge of `(source, key)` adds to its consumer's batch row: the memo cleared, the pair's
/// swept stamp and lost note cleared (FIX-956), its dead letter resolved (four primary-key deletes).
pub fn resolved_statements(source: &str, key: &str) -> Vec<Stmt> {
    vec![
        Stmt {
            sql: FAULT_MEMO_CLEAR_SQL.into(),
            binds: vec![Bind::Text(fault_memo_name(source, key))],
        },
        // FIX-956: the pair's swept stamp and lost note go with it (the consumer passed the pair)
        Stmt {
            sql: FAULT_MEMO_CLEAR_SQL.into(),
            binds: vec![Bind::Text(swept_memo_name(source, key))],
        },
        Stmt {
            sql: FAULT_MEMO_CLEAR_SQL.into(),
            binds: vec![Bind::Text(publish::lost_memo_name(source, key))],
        },
        Stmt {
            sql: DEAD_LETTER_RESOLVE_SQL.into(),
            binds: vec![Bind::Text(source.into()), Bind::Text(key.into())],
        },
    ]
}

/// PURE: the pairs the sweep re-publishes, the held ones left out (`memos`: the memo rows of `pairs` and of their
/// subjects), and how many were held. A pair is held by its own memo, or (NEW-M1) when every subject the sweep read
/// for it is held by its subject memo (`subjects`: a subject's memo name, `None` for a wanted envelope, never held).
pub fn unheld(
    pairs: Vec<Pair>,
    subjects: &BTreeMap<Pair, Vec<Option<String>>>,
    memos: &[Value],
    now_ms: i64,
) -> (Vec<Pair>, usize) {
    let held_names: std::collections::BTreeSet<&str> = memos
        .iter()
        .filter(|r| {
            r["cursor"]
                .as_str()
                .and_then(|c| serde_json::from_str::<Value>(c).ok())
                .and_then(|c| ms_of(&c["heldUntilMs"]))
                .is_some_and(|until| until > now_ms)
        })
        .filter_map(|r| r["name"].as_str())
        .collect();
    let before = pairs.len();
    let kept: Vec<Pair> = pairs
        .into_iter()
        .filter(|pair| {
            let (s, k) = pair;
            if held_names.contains(fault_memo_name(s, k).as_str()) {
                return false;
            }
            !subjects.get(pair).is_some_and(|names| {
                !names.is_empty()
                    && names
                        .iter()
                        .all(|n| n.as_deref().is_some_and(|n| held_names.contains(n)))
            })
        })
        .collect();
    let held = before - kept.len();
    (kept, held)
}

/// What the dead letter queue's park did with one letter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Parked {
    /// On its pair's row, with the row's status (`parked`, or `exhausted` past the re-drives).
    Row(String),
    /// A new pair at a full table: handed back.
    Full,
}

/// LOW-3: park one dead letter (the DLQ consumer's one statement; the pins run exactly this).
pub async fn park_letter(
    db: &impl SqlPort,
    message_id: &str,
    body: &str,
) -> Result<Parked, String> {
    let (source, key) = match OwedEvent::decode(body) {
        Ok(ev) => (ev.source, ev.source_key),
        Err(_) => (String::new(), message_id.to_string()),
    };
    let memo = fault_memo_name(&source, &key);
    let out = rows(
        db,
        DEAD_LETTER_PARK_SQL,
        vec![
            Bind::Text(source),
            Bind::Text(key),
            Bind::Text(message_id.into()),
            Bind::Text(body.into()),
            Bind::Int(db.now_ms() as i64),
            Bind::Text(memo),
            Bind::Int(publish::DEAD_LETTERS_MAX),
            Bind::Int(publish::MAX_REDRIVES),
        ],
    )
    .await?;
    Ok(match out.first().and_then(|r| r["status"].as_str()) {
        Some(status) => Parked::Row(status.to_string()),
        None => Parked::Full,
    })
}

/// LOW-5 (the lens): the delay a letter whose park did not land is handed back with: its age since its message was
/// sent, 60 s to 30 min (#576's doubling shape, read off the clock since `attempts` is not exposed on a raw message):
/// over the DLQ's 100 retries about 48 h, where a fixed 60 s gave about 100 minutes.
pub fn park_retry_delay_s(age_ms: i64) -> u32 {
    (age_ms / 1000).clamp(60, 1800) as u32
}

/// What one message's routing did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Routed {
    /// `(identity, game)` subjects judged.
    pub subjects: usize,
    /// Marks the judged subjects held (the judge's batch acknowledged them).
    pub marks: usize,
    /// More marks remain for this pair: a continuation is published.
    pub more: bool,
    /// Subjects whose judge faulted alone (their marks kept, their subject memos noted).
    pub faults: usize,
    /// Subjects skipped while their subject hold runs (their marks kept).
    pub held: usize,
    /// D2-ACTOR-B-FOLD-11 (NEW6-M2): the `(identity, game)` subjects whose judge landed, and those whose judge faulted
    /// alone: a coalesced batch resolves each message by its OWN plan's routing ([`batch_core`]).
    pub landed: Vec<(String, String)>,
    pub faulted: Vec<(String, String)>,
}

/// THE ONE JUDGE CALL of the consumer (lane B replaces its body with the actor's delivery): one identity's games, at
/// most `owed_judge::OWED_JUDGE_SUBJECTS_PER_INVOCATION` per call.
///
/// bsv-low #912 (lane D2-ACTOR-B): with `OWED_D1_JUDGE` on (Phases A and B) the D1 judge writes `owed_rows` as
/// before and the identity's actor applies beside it, its rows compared ([`crate::owed_actor::beside`]; its fault
/// never fails the message). Phase B (`OWED_ACTOR_READ` on, the read serving the actor's rows; the D2-ACTOR-B lens's
/// HIGH-3, option (a)): the actor applies FIRST and its fault hands the message back, then the D1 judge acknowledges
/// the marks ([`crate::owed_actor::actor_then_d1`]), so no mark that withholds a press is gone before the actor
/// applied. Off (Phase C) the actor IS the judge and the marks are acknowledged after it applied
/// ([`crate::owed_actor::as_the_judge`]).
pub async fn deliver(
    db: &impl JudgeDb,
    identity: &str,
    games: &[String],
) -> Result<owed_judge::JudgeReport, String> {
    if db.d1_judges() && db.actor_reads() {
        return crate::owed_actor::actor_then_d1(db, identity, games).await;
    }
    if db.d1_judges() {
        // D2-ACTOR-D (bsv-low #952): the acknowledgement's bound read BEFORE the D1 judge (NEW-H1's rule, as Phase B):
        // the D1 judge acknowledges at most it, and the actor's apply names it, so a delivery the writer's direct call
        // already covered is a dupe the actor answers from its seq check in milliseconds, never a mark acknowledged
        // past what the covering apply read.
        // D2-ACTOR-E (bsv-low #954): with the bound, the txids its marks name, for the deferred narrowed apply (the
        // D1 judge acknowledges those marks before the actor reads).
        let (bound, marks_touch) = owed_judge::marks_bound_keys(db, identity, games).await?;
        db.note_marks_touch(identity, marks_touch);
        let report =
            owed_judge::judge_subjects_acked_to(db, identity, games, "owed-events", bound).await?;
        // The D2-ACTOR-B lens's MEDIUM-6: Phase A's apply and compare run after the batch (`ctx.wait_until`), never
        // inline in the serial consumer; inline only where it cannot be deferred.
        if !db.defer_actor(identity, games, Some(bound)) {
            crate::owed_actor::beside(db, identity, games, Some(bound)).await;
        }
        return Ok(report);
    }
    crate::owed_actor::as_the_judge(db, identity, games).await
}

async fn rows(db: &impl SqlPort, sql: &str, binds: Vec<Bind>) -> Result<Vec<Value>, String> {
    Ok(db
        .run_batch(vec![Stmt {
            sql: sql.into(),
            binds,
        }])
        .await?
        .into_iter()
        .next()
        .map(|o| o.rows)
        .unwrap_or_default())
}

/// One message's subjects, read from its marks before any judge ([`plan`]).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Plan {
    order: Vec<String>,
    games: BTreeMap<String, Vec<String>>,
    held: BTreeMap<(String, String), usize>,
    more: bool,
    /// NEW2-M1: held subjects the read left out (among the window it counts).
    held_subjects: usize,
}

impl Plan {
    /// A plan of these subjects alone (the pins': FIX-983's consumer fan-out).
    #[cfg(test)]
    pub fn of_subjects(subjects: Vec<(String, Vec<String>)>) -> Plan {
        let mut p = Plan::default();
        for (id, games) in subjects {
            if !p.order.contains(&id) {
                p.order.push(id.clone());
            }
            p.games.entry(id).or_default().extend(games);
        }
        p
    }

    /// No subject to judge (and no continuation).
    pub fn is_empty(&self) -> bool {
        self.order.is_empty() && !self.more
    }

    /// D2-ACTOR-D: the plan's subjects, each identity's games in the plan's order.
    pub fn subjects(&self) -> Vec<(String, Vec<String>)> {
        self.order
            .iter()
            .map(|id| (id.clone(), self.games.get(id).cloned().unwrap_or_default()))
            .collect()
    }

    /// D2-ACTOR-E (bsv-low #954) item 3: the plans of one batch's messages as ONE plan: each identity once, its games
    /// the union of the messages' games (in their order), its marks summed, so [`judge_plan`] makes one delivery (one
    /// D1 judge pass and one actor apply) per identity per 16 games, however many messages named it.
    pub fn merged(plans: &[Plan]) -> Plan {
        let mut out = Plan::default();
        for p in plans {
            for id in &p.order {
                if !out.order.contains(id) {
                    out.order.push(id.clone());
                }
                let into = out.games.entry(id.clone()).or_default();
                for g in p.games.get(id).into_iter().flatten() {
                    if !into.contains(g) {
                        into.push(g.clone());
                    }
                }
            }
            for (k, n) in &p.held {
                *out.held.entry(k.clone()).or_default() += n;
            }
            out.more |= p.more;
            out.held_subjects += p.held_subjects;
        }
        out
    }

    /// D2-ACTOR-B-FOLD-11 (NEW6-M2): the plan's subject games over every identity (a message's bound is
    /// `owed_event::MAX_GAMES_PER_MESSAGE`).
    pub fn game_count(&self) -> usize {
        self.games.values().map(Vec::len).sum()
    }

    /// The marks the plan read (a message's bound is [`ROUTE_READ_LIMIT`]).
    pub fn mark_count(&self) -> usize {
        self.held.values().sum()
    }

    /// The plan's `(identity, game)` subjects.
    pub fn subject_pairs(&self) -> Vec<(String, String)> {
        self.order
            .iter()
            .flat_map(|id| {
                self.games
                    .get(id)
                    .into_iter()
                    .flatten()
                    .map(move |g| (id.clone(), g.clone()))
            })
            .collect()
    }

    /// Does this message's own plan leave marks past it (its continuation, [`continues`])?
    pub fn has_more(&self) -> bool {
        self.more
    }

    /// What an empty plan answers: nothing judged, its held subjects counted.
    pub fn empty_routed(&self) -> Routed {
        Routed {
            held: self.held_subjects,
            ..Routed::default()
        }
    }
}

/// PURE (D2-ACTOR-A-DELTA-2's NEW2-M1): a message publishes its continuation only when more unheld marks remain AND it
/// judged a subject. A message that judged nothing never starts a chain of messages that judge nothing.
pub fn continues(routed: &Routed) -> bool {
    routed.more && routed.subjects > 0
}

/// PURE (NEW2-M1): a message whose every subject was held: acked with its marks standing (its pair unresolved), no
/// continuation, counted `owed_events_all_held_total`.
pub fn all_held(routed: &Routed) -> bool {
    routed.subjects == 0 && routed.faults == 0 && routed.held > 0
}

/// HIGH-1: route one event through the marks its source row made and judge their subjects (see the module doc):
/// [`plan`], then [`judge_plan`].
pub async fn route(db: &impl JudgeDb, ev: &OwedEvent) -> Result<Routed, String> {
    let plan = plan(db, ev).await?;
    judge_plan(db, plan).await
}

/// The marks of one event, read (a wanted envelope expanded) into its subjects, before any judge: the consumer reads
/// the chain tip only for a plan that holds one (the D2-ACTOR-A lens's LOW-6).
pub async fn plan(db: &impl SqlPort, ev: &OwedEvent) -> Result<Plan, String> {
    plan_with(db, ev, true).await
}

/// [`plan`], with or without the wanted envelopes' expansion (a write: the consumer's alone; the writer's direct call
/// reads the marks and writes nothing, D2-ACTOR-D).
pub async fn plan_with(db: &impl SqlPort, ev: &OwedEvent, expand: bool) -> Result<Plan, String> {
    if ev.source == owed_event::TXFACTS {
        return Ok(Plan::default());
    }
    let limit = ROUTE_READ_LIMIT as i64;
    let now = db.now_ms() as i64;
    // NEW2-M1 (the second delta lens): the unheld marks and the held count, one batch
    let (marks_sql, held_sql, binds) = if ev.source == owed_event::ROUTE_IDENTITY {
        (
            ROUTE_IDENTITY_SQL,
            ROUTE_IDENTITY_HELD_SQL,
            vec![
                Bind::Text(ev.source_key.clone()),
                Bind::Int(limit),
                Bind::Int(now),
            ],
        )
    } else {
        (
            ROUTE_MARKS_SQL,
            ROUTE_HELD_SQL,
            vec![
                Bind::Text(ev.source.clone()),
                Bind::Text(ev.source_key.clone()),
                Bind::Int(limit),
                Bind::Int(now),
            ],
        )
    };
    let mut read = db
        .run_batch(vec![
            Stmt {
                sql: marks_sql.into(),
                binds: binds.clone(),
            },
            Stmt {
                sql: held_sql.into(),
                binds,
            },
        ])
        .await?
        .into_iter();
    let mut marks = read.next().map(|o| o.rows).unwrap_or_default();
    let held_subjects = read
        .next()
        .and_then(|o| o.rows.first().and_then(|r| r["n"].as_i64()))
        .unwrap_or(0)
        .max(0) as usize;
    let mut more = marks.len() >= ROUTE_READ_LIMIT;
    // A wanted envelope (the read's own mark, no game) is expanded first, then routed by its identity.
    let envelopes: Vec<(String, i64)> = marks
        .iter()
        .filter(|m| m["gameId"].as_str() == Some(""))
        .filter_map(|m| {
            Some((
                m["identity"].as_str()?.to_owned(),
                m["generation"].as_i64()?,
            ))
        })
        .collect();
    for (identity, generation) in envelopes.iter().filter(|_| expand) {
        owed_judge::expand_wanted(db, identity, *generation).await?;
        let expanded = rows(
            db,
            ROUTE_IDENTITY_SQL,
            vec![
                Bind::Text(identity.clone()),
                Bind::Int(limit),
                Bind::Int(now),
            ],
        )
        .await?;
        more |= expanded.len() >= ROUTE_READ_LIMIT;
        marks.extend(expanded);
    }
    let mut order: Vec<String> = Vec::new();
    let mut games: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut counted = 0usize;
    let mut held: BTreeMap<(String, String), usize> = BTreeMap::new();
    for m in &marks {
        let (Some(identity), Some(game)) = (m["identity"].as_str(), m["gameId"].as_str()) else {
            continue;
        };
        if game.is_empty() {
            continue;
        }
        *held
            .entry((identity.to_owned(), game.to_owned()))
            .or_default() += 1;
        let list = games.entry(identity.to_owned()).or_default();
        if list.iter().any(|g| g == game) {
            continue;
        }
        if counted >= owed_event::MAX_GAMES_PER_MESSAGE {
            more = true;
            continue;
        }
        if list.is_empty() {
            order.push(identity.to_owned());
        }
        list.push(game.to_owned());
        counted += 1;
    }
    Ok(Plan {
        order,
        games,
        held,
        more,
        held_subjects,
    })
}

/// The held-until (ms) of each memo row read, by its name.
fn memo_holds(memos: &[Value]) -> BTreeMap<String, i64> {
    memos
        .iter()
        .filter_map(|r| {
            let until = r["cursor"]
                .as_str()
                .and_then(|c| serde_json::from_str::<Value>(c).ok())
                .and_then(|c| ms_of(&c["heldUntilMs"]))
                .unwrap_or(0);
            Some((r["name"].as_str()?.to_owned(), until))
        })
        .collect()
}

/// Judge a [`plan`]'s subjects through [`deliver`], 16 a call.
///
/// D2-ACTOR-A-DELTA's NEW-M1: a chunk whose judge faults is judged again ONE SUBJECT AT A TIME, and a subject whose
/// judge faults alone is SKIPPED: its mark stays, its fault is noted on its own memo ([`subject_memo_name`], keyed
/// `(identity, game)`, never the pair's), and every other subject of the message is judged and acknowledged by its
/// own batch. From the [`FAULT_HOLD_AFTER`]th fault the subject is HELD (120 s doubling to 6 h, [`fault_hold_ms`]):
/// skipped unjudged while it runs, and the sweep leaves out a pair whose every subject is held. So the shared pairs
/// (`age`/`age`, `tip`/`gate`, `tip`/`untipped`, `backfill`/`owed-v2`) are never stopped by one poisoned subject.
/// `Err` only when NOTHING landed and some subject faulted: the pair's own memo and hold (the consumer's) then cover
/// a pair whose every subject faults (a D1 or chaintracks outage). A landed subject's memo is cleared.
///
/// FIX-952 (bsv-low #952, item 3; the L53 fold's S1 and S2, records 1544 to 1548): the plan's IDENTITIES are
/// delivered AT ONCE, [`JUDGE_IDENTITIES_AT_ONCE`] at a time ([`judge_identity`] each, over `buffer_unordered`), so a
/// batch of 16 messages for 16 identities costs about ONE identity's delivery (in Phase B the actor's apply then the
/// D1 judge's commit, [`crate::owed_actor::actor_then_d1`]), not sixteen in a row: at 33 pairs the serial pass took
/// 61 to 70 s a batch of 16 against the 60 s wall and cleared about 15 events a minute of 243. WITHIN one identity
/// its chunks stay sequential (one D1 writer of its rows at a time: HIGH-3's order, the actor before the
/// acknowledgement, holds per identity), and the per-identity state the pass keeps (the judge's marks-touch, the
/// in-flight set) is keyed by identity. The outcome is the same whatever the completion order: each identity's
/// landed and faulted subjects, its cleared memos and its first fault are merged after every identity answered.
pub async fn judge_plan(db: &impl JudgeDb, plan: Plan) -> Result<Routed, String> {
    use futures_util::StreamExt as _;
    let Plan {
        order,
        games,
        held,
        more,
        held_subjects,
    } = plan;
    let mut out = Routed {
        more,
        held: held_subjects,
        ..Routed::default()
    };
    let names: Vec<String> = order
        .iter()
        .flat_map(|id| games[id].iter().map(move |g| subject_memo_name(id, g)))
        .collect();
    let memos = if names.is_empty() {
        BTreeMap::new()
    } else {
        memo_holds(
            &rows(
                db,
                FAULT_MEMO_READ_SQL,
                vec![Bind::Text(json!(names).to_string())],
            )
            .await?,
        )
    };
    let now = db.now_ms() as i64;
    // FIX-983 (bsv-low #983): the batch's fan-out, named on each actor body (the stress class's rule).
    db.note_concurrency(order.len().min(JUDGE_IDENTITIES_AT_ONCE) as u32);
    // FIX-952: every identity of the pass at once (bounded), each one's chunks in order
    let judged: Vec<IdentityJudged> =
        futures_util::stream::iter(order.into_iter().map(|identity| {
            let list = games.get(&identity).cloned().unwrap_or_default();
            judge_identity(db, identity, list, &memos, now, &held)
        }))
        .buffer_unordered(JUDGE_IDENTITIES_AT_ONCE)
        .collect()
        .await;
    let mut landed = 0usize;
    let mut cleared: Vec<String> = Vec::new();
    let mut first_fault: Option<String> = None;
    for j in judged {
        landed += j.landed_calls;
        out.subjects += j.subjects;
        out.marks += j.marks;
        out.held += j.held;
        out.faults += j.faulted.len();
        out.landed.extend(j.landed);
        out.faulted.extend(j.faulted);
        cleared.extend(j.cleared);
        if first_fault.is_none() {
            first_fault = j.first_fault;
        }
    }
    if !cleared.is_empty() {
        let clears = cleared
            .into_iter()
            .map(|n| Stmt {
                sql: FAULT_MEMO_CLEAR_SQL.into(),
                binds: vec![Bind::Text(n)],
            })
            .collect();
        if let Err(e) = db.run_batch(clears).await {
            db.log(&format!(
                "[owed-events] a landed subject's fault memo not cleared ({e}): its next fault counts on"
            ));
        }
    }
    if landed == 0 {
        if let Some(fault) = first_fault {
            return Err(fault);
        }
    }
    Ok(out)
}

/// FIX-952 (bsv-low #952, item 3): how many of a pass's identities [`judge_plan`] delivers at once: the batch's own
/// size ([`CONSUMER_BATCH_MAX`], 16 messages, each naming an identity or two), so a batch's identities all go at once
/// and a wider merged plan (several identities a message) goes in rounds of 16. The direct pass already calls up to
/// [`DIRECT_EVENTS_MAX`] actors at once (`join_all`); this is the consumer's half of the same shape.
pub const JUDGE_IDENTITIES_AT_ONCE: usize = CONSUMER_BATCH_MAX;

/// One identity's outcome of a judge pass ([`judge_identity`]), merged by [`judge_plan`] after every identity answered.
#[derive(Debug, Default)]
struct IdentityJudged {
    /// Judge calls that landed (a pass with none and a fault is the pass's `Err`).
    landed_calls: usize,
    subjects: usize,
    marks: usize,
    /// Subjects skipped under their subject hold.
    held: usize,
    landed: Vec<(String, String)>,
    faulted: Vec<(String, String)>,
    /// A landed subject's memo, to clear.
    cleared: Vec<String>,
    first_fault: Option<String>,
}

/// One identity's games through [`deliver`], 16 a call, each chunk in order (one D1 writer of its rows at a time): a
/// faulting chunk judged again one subject at a time, a held subject skipped, a faulting one noted on its own memo
/// (NEW-M1's rule, unchanged by FIX-952: only the identities run beside each other now).
async fn judge_identity(
    db: &impl JudgeDb,
    identity: String,
    games: Vec<String>,
    memos: &BTreeMap<String, i64>,
    now: i64,
    held: &BTreeMap<(String, String), usize>,
) -> IdentityJudged {
    let mut out = IdentityJudged::default();
    let marks_of = |chunk: &[String]| -> usize {
        chunk
            .iter()
            .map(|g| {
                held.get(&(identity.clone(), g.clone()))
                    .copied()
                    .unwrap_or(0)
            })
            .sum()
    };
    let mut list: Vec<String> = Vec::new();
    for g in &games {
        if memos
            .get(&subject_memo_name(&identity, g))
            .is_some_and(|until| *until > now)
        {
            out.held += 1;
        } else {
            list.push(g.clone());
        }
    }
    for chunk in list.chunks(owed_judge::OWED_JUDGE_SUBJECTS_PER_INVOCATION) {
        enter_in_flight(&identity, chunk);
        let delivered = deliver(db, &identity, chunk).await;
        leave_in_flight(&identity, chunk);
        let singles: Vec<&[String]> = match delivered {
            Ok(report) => {
                out.landed_calls += 1;
                out.subjects += report.subjects;
                out.marks += marks_of(chunk);
                out.landed
                    .extend(chunk.iter().map(|g| (identity.clone(), g.clone())));
                out.cleared.extend(
                    chunk
                        .iter()
                        .map(|g| subject_memo_name(&identity, g))
                        .filter(|n| memos.contains_key(n)),
                );
                continue;
            }
            Err(e) if chunk.len() == 1 => {
                out.faulted.push((identity.clone(), chunk[0].clone()));
                let name = subject_memo_name(&identity, &chunk[0]);
                let _ = note_fault_named(db, &name, &e).await;
                out.first_fault.get_or_insert(e);
                continue;
            }
            Err(_) => chunk.chunks(1).collect(),
        };
        for one in singles {
            let name = subject_memo_name(&identity, &one[0]);
            enter_in_flight(&identity, one);
            let delivered = deliver(db, &identity, one).await;
            leave_in_flight(&identity, one);
            match delivered {
                Ok(report) => {
                    out.landed_calls += 1;
                    out.subjects += report.subjects;
                    out.marks += marks_of(one);
                    out.landed.push((identity.clone(), one[0].clone()));
                    if memos.contains_key(&name) {
                        out.cleared.push(name);
                    }
                }
                Err(e) => {
                    out.faulted.push((identity.clone(), one[0].clone()));
                    let _ = note_fault_named(db, &name, &e).await;
                    out.first_fault.get_or_insert(e);
                }
            }
        }
    }
    out
}

/// One `(source, sourceKey)` pair the sweep re-publishes.
pub type Pair = (String, String);

/// PURE: the sweep's choice over the rows read past its cursor: up to [`SWEEP_PAIRS_PER_TICK`] distinct pairs, oldest
/// first, and the cursor to save (`None`: the walk reached the end of the backlog, start again next tick).
pub fn sweep_choice(
    rows: &[Value],
    read_limit: usize,
) -> (Vec<Pair>, Option<(i64, i64)>) {
    sweep_choice_up_to(rows, read_limit, SWEEP_PAIRS_PER_TICK)
}

/// [`sweep_choice`] up to `cap` pairs (FIX-956: the tick's lost notes take their places first).
pub fn sweep_choice_up_to(
    rows: &[Value],
    read_limit: usize,
    cap: usize,
) -> (Vec<Pair>, Option<(i64, i64)>) {
    let mut pairs: Vec<(String, String)> = Vec::new();
    let mut cursor = None;
    for r in rows {
        let (Some(source), Some(key), Some(at), Some(g)) = (
            r["source"].as_str(),
            r["sourceKey"].as_str(),
            r["markedAtMs"].as_i64(),
            r["generation"].as_i64(),
        ) else {
            continue;
        };
        let pair = (source.to_owned(), key.to_owned());
        if !pairs.contains(&pair) {
            if pairs.len() >= cap {
                return (pairs, cursor);
            }
            pairs.push(pair);
        }
        cursor = Some((at, g));
    }
    if rows.len() < read_limit {
        // the end of the backlog: the next tick walks from its start
        return (pairs, None);
    }
    (pairs, cursor)
}

/// What one sweep tick chose (FIX-956).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Swept {
    /// The pairs to re-publish, the lost notes' first.
    pub pairs: Vec<Pair>,
    /// Each pair's reason, in `pairs`' order.
    pub reasons: Vec<SweepReason>,
    /// Candidates left out by their swept stamp (re-published, the consumer not yet past it).
    pub once: usize,
    /// The consumer's horizon read.
    pub horizon_ms: Option<i64>,
}

impl Swept {
    /// How many pairs of `reason`.
    pub fn count(&self, reason: SweepReason) -> usize {
        self.reasons.iter().filter(|r| **r == reason).count()
    }
}

/// HIGH-2's second belt: the pairs whose marks waited past one tick, oldest first past the cursor, and the cursor
/// saved. The caller publishes them ([`sweep_publish`]).
pub async fn sweep(db: &impl SqlPort, now_ms: i64) -> Result<Vec<(String, String)>, String> {
    Ok(sweep_tick(db, now_ms).await?.pairs)
}

/// FIX-956 (bsv-low #956): one sweep tick. Reads the cursor and the consumer's horizon (one statement), the lost
/// notes and the prune's window past their cursors (one batch; NEW7-L1), then the marks under [`sweep_bound`] past the cursor; leaves out a pair held by a fault memo or (once a
/// pass) by its swept stamp; then ONE batch: the cursor, a swept stamp per chosen pair, the chosen lost notes deleted
/// and the old stamps pruned.
pub async fn sweep_tick(db: &impl SqlPort, now_ms: i64) -> Result<Swept, String> {
    let state = rows(db, SWEEP_CURSOR_READ_SQL, vec![]).await?;
    let state_of = |name: &str| {
        state
            .iter()
            .find(|r| r["name"].as_str() == Some(name))
            .and_then(|r| r["cursor"].as_str())
            .and_then(|c| serde_json::from_str::<Value>(c).ok())
    };
    let sweep_cursor = state_of("owed-events:sweep").unwrap_or(Value::Null);
    let (at, g) = (
        ms_of(&sweep_cursor["at"]).unwrap_or(-1),
        ms_of(&sweep_cursor["g"]).unwrap_or(0),
    );
    let lost_after = range_cursor(
        sweep_cursor["lostAfter"].as_str(),
        publish::LOST_MEMO_PREFIX,
    );
    let prune_after = range_cursor(sweep_cursor["pruneAfter"].as_str(), SWEPT_MEMO_PREFIX);
    let horizon = state_of("owed-events:consumer").and_then(|c| horizon_of(&c));
    // 1b: the lost notes first, whatever the horizon; beside them (NEW7-L1) the prune's window: one batch, two
    // primary-key ranges each read to its LIMIT
    let windows = db
        .run_batch(vec![
            Stmt {
                sql: LOST_READ_SQL.into(),
                binds: vec![
                    Bind::Int(SWEEP_LOST_PER_TICK as i64),
                    Bind::Text(lost_after),
                ],
            },
            Stmt {
                sql: SWEPT_WINDOW_SQL.into(),
                binds: vec![
                    Bind::Text(prune_after),
                    Bind::Int(SWEPT_PRUNE_WINDOW as i64),
                ],
            },
        ])
        .await?;
    let lost_rows: Vec<Value> = windows.first().map(|o| o.rows.clone()).unwrap_or_default();
    let window: Vec<Value> = windows.get(1).map(|o| o.rows.clone()).unwrap_or_default();
    let next_lost = next_range_cursor(
        &lost_rows
            .iter()
            .filter_map(|r| r["name"].as_str())
            .collect::<Vec<_>>(),
        SWEEP_LOST_PER_TICK,
        publish::LOST_MEMO_PREFIX,
    );
    let window_names: Vec<&str> = window.iter().filter_map(|r| r["name"].as_str()).collect();
    let next_prune = next_range_cursor(&window_names, SWEPT_PRUNE_WINDOW, SWEPT_MEMO_PREFIX);
    let prune_cutoff = now_ms - SWEPT_PRUNE_AFTER_MS;
    let prunable: Vec<&str> = window
        .iter()
        .filter(|r| ms_of(&r["tickedAtMs"]).is_some_and(|t| t < prune_cutoff))
        .filter_map(|r| r["name"].as_str())
        .collect();
    let mut lost: Vec<(Pair, String)> = Vec::new();
    for r in &lost_rows {
        let Some(name) = r["name"].as_str() else {
            continue;
        };
        let c = r["cursor"]
            .as_str()
            .and_then(|c| serde_json::from_str::<Value>(c).ok())
            .unwrap_or(Value::Null);
        match (c["source"].as_str(), c["sourceKey"].as_str()) {
            (Some(src), Some(key)) => lost.push(((src.to_owned(), key.to_owned()), name.to_owned())),
            // a note that does not read is deleted (it names no pair)
            _ => lost.push((("".into(), "".into()), name.to_owned())),
        }
    }
    let read = rows(
        db,
        SWEEP_SQL,
        vec![
            Bind::Int(sweep_bound(now_ms, horizon)),
            Bind::Int(at),
            Bind::Int(g),
            Bind::Int(SWEEP_READ_LIMIT as i64),
        ],
    )
    .await?;
    let lost_pairs: Vec<Pair> = lost
        .iter()
        .map(|(p, _)| p.clone())
        .filter(|(s, _)| !s.is_empty())
        .collect();
    let cap = SWEEP_PAIRS_PER_TICK.saturating_sub(lost_pairs.len());
    let (candidates, next) = sweep_choice_up_to(&read, SWEEP_READ_LIMIT, cap);
    let oldest: BTreeMap<Pair, i64> = read.iter().fold(BTreeMap::new(), |mut m, r| {
        if let (Some(s), Some(k), Some(at)) = (
            r["source"].as_str(),
            r["sourceKey"].as_str(),
            r["markedAtMs"].as_i64(),
        ) {
            m.entry((s.to_owned(), k.to_owned())).or_insert(at);
        }
        m
    });
    let candidates: Vec<Pair> = candidates
        .into_iter()
        .filter(|p| !lost_pairs.contains(p))
        .collect();
    let mut out = Swept {
        horizon_ms: horizon,
        ..Swept::default()
    };
    let all: Vec<Pair> = lost_pairs.iter().chain(candidates.iter()).cloned().collect();
    // MEDIUM-2: a pair whose judge faulted past the bound waits out its hold (its marks stay; the walk wraps to it);
    // FIX-956: a candidate re-published and not yet passed by the consumer waits (its swept stamp)
    let kept = if all.is_empty() {
        all
    } else {
        let mut names: Vec<String> = all.iter().map(|(s, k)| fault_memo_name(s, k)).collect();
        names.extend(candidates.iter().map(|(s, k)| swept_memo_name(s, k)));
        // NEW-M1: the subjects read for each chosen pair, by their subject memos
        let mut subjects: BTreeMap<Pair, Vec<Option<String>>> = BTreeMap::new();
        for r in &read {
            let (Some(source), Some(key)) = (r["source"].as_str(), r["sourceKey"].as_str()) else {
                continue;
            };
            let pair = (source.to_owned(), key.to_owned());
            if !all.contains(&pair) {
                continue;
            }
            let name = match (r["identity"].as_str(), r["gameId"].as_str()) {
                (Some(id), Some(g)) if !g.is_empty() => Some(subject_memo_name(id, g)),
                _ => None,
            };
            if let Some(n) = &name {
                if !names.contains(n) {
                    names.push(n.clone());
                }
            }
            subjects.entry(pair).or_default().push(name);
        }
        let memos = rows(
            db,
            FAULT_MEMO_READ_SQL,
            vec![Bind::Text(json!(names).to_string())],
        )
        .await?;
        let swept_at = |pair: &Pair| -> Option<i64> {
            let name = swept_memo_name(&pair.0, &pair.1);
            memos
                .iter()
                .find(|r| r["name"].as_str() == Some(name.as_str()))
                .and_then(|r| r["cursor"].as_str())
                .and_then(|c| serde_json::from_str::<Value>(c).ok())
                .and_then(|c| ms_of(&c["sweptAtMs"]))
        };
        let before = all.len();
        let all: Vec<Pair> = all
            .into_iter()
            .filter(|p| {
                lost_pairs.contains(p)
                    || !swept_at(p).is_some_and(|at| swept_blocks(at, now_ms, horizon))
            })
            .collect();
        out.once = before - all.len();
        let (kept, held) = unheld(all, &subjects, &memos, now_ms);
        if held > 0 {
            db.counters(vec![("owed_events_sweep_held_total".into(), held as u64)])
                .await;
        }
        kept
    };
    for p in &kept {
        out.reasons.push(if lost_pairs.contains(p) {
            SweepReason::Lost
        } else {
            sweep_reason(oldest.get(p).copied().unwrap_or(now_ms), horizon)
        });
    }
    out.pairs = kept;
    let mut next = next.map_or(
        json!({"at": -1, "g": 0}),
        |(at, g)| json!({"at": at, "g": g}),
    );
    next["lostAfter"] = json!(next_lost);
    next["pruneAfter"] = json!(next_prune);
    let mut batch = vec![Stmt {
        sql: SWEEP_CURSOR_SQL.into(),
        binds: vec![Bind::Text(next.to_string()), Bind::Int(now_ms)],
    }];
    for (s, k) in &out.pairs {
        batch.push(Stmt {
            sql: SWEPT_MEMO_WRITE_SQL.into(),
            binds: vec![Bind::Text(swept_memo_name(s, k)), Bind::Int(now_ms)],
        });
    }
    // a lost note is taken out when its pair is re-published now (or names no pair); one held by its fault memo stays
    for (pair, name) in &lost {
        if pair.0.is_empty() || out.pairs.contains(pair) {
            batch.push(Stmt {
                sql: LOST_CLEAR_SQL.into(),
                binds: vec![Bind::Text(name.clone())],
            });
        }
    }
    if !prunable.is_empty() {
        batch.push(Stmt {
            sql: SWEPT_PRUNE_SQL.into(),
            binds: vec![
                Bind::Text(json!(prunable).to_string()),
                Bind::Int(prune_cutoff),
            ],
        });
    }
    db.run_batch(batch).await?;
    Ok(out)
}

/// The publish's port over a [`SqlPort`] (the pins' SQLite and the worker's D1) and a send.
pub struct SqlPublish<'a, D: SqlPort, S> {
    /// The database.
    pub db: &'a D,
    /// The send (the worker's queue, the pins' recorder).
    pub send: S,
}

impl<D: SqlPort, S, F> publish::PublishPort for SqlPublish<'_, D, S>
where
    S: Fn(Vec<String>) -> F,
    F: std::future::Future<Output = Result<(), String>>,
{
    async fn run_reads(
        &self,
        stmts: Vec<(&'static str, Vec<publish::Arg>)>,
    ) -> Result<Vec<Vec<Value>>, String> {
        let stmts = stmts
            .into_iter()
            .map(|(sql, args)| Stmt {
                sql: sql.into(),
                binds: args
                    .into_iter()
                    .map(|a| match a {
                        publish::Arg::Text(t) => Bind::Text(t),
                        publish::Arg::Int(n) => Bind::Int(n),
                        publish::Arg::Null => Bind::Null,
                    })
                    .collect(),
            })
            .collect();
        Ok(self
            .db
            .run_batch(stmts)
            .await?
            .into_iter()
            .map(|o| o.rows)
            .collect())
    }
    async fn send(&self, bodies: Vec<String>) -> Result<(), String> {
        (self.send)(bodies).await
    }
    fn now_ms(&self) -> i64 {
        self.db.now_ms() as i64
    }
}

/// The sweep's pairs as notes (no held facts: each is read at publish).
pub fn notes_of(pairs: &[(String, String)]) -> Vec<publish::Noted> {
    pairs
        .iter()
        .map(|(source, key)| publish::Noted {
            source: source.clone(),
            key: key.clone(),
            held: None,
        })
        .collect()
}

/// LOW-1 (the D2-ACTOR-A lens): the overlay's certified migration count from which the publish's statements exist
/// (`owed_event_seq`, `idx_released_missing_hop`, the dead letters, the cost bar's queue column): the app layer does
/// not link the overlay, so the figure is pinned against `OVERLAY_MIGRATION_COUNT` by `d2_actor_a_fold_l1_*`.
pub const OWED_EVENTS_MIGRATION_COUNT: u32 = 473;
/// The certified count's row (the backfill's gate reads the same).
pub const CERTIFIED_COUNT_SQL: &str =
    "SELECT value AS count FROM ops_counters WHERE name='overlay_migration_count'";
/// A count found short is read again after this long (a found one holds for the isolate's life).
pub const CERTIFIED_RECHECK_MS: f64 = 60_000.0;

thread_local! {
    /// The isolate's reading: `(ready, read at ms)`.
    static READY: std::cell::Cell<Option<(bool, f64)>> = const { std::cell::Cell::new(None) };
}

/// PURE: is the overlay's certified count at or past the publish's migrations? (An unreadable count is not.)
pub fn ready_at(count: Option<f64>) -> bool {
    count.is_some_and(|c| c >= f64::from(OWED_EVENTS_MIGRATION_COUNT))
}

/// `true` when this Worker holds the producer binding (the queue configured) AND the overlay's migrations the
/// publish needs are certified (LOW-1: an app layer deployed before the overlay keeps the inline judge and the
/// drain, where every publish and sweep would fault on a table that is not there). The count is read once per
/// isolate when found, again each [`CERTIFIED_RECHECK_MS`] while short.
pub async fn configured(env: &worker::Env) -> bool {
    if env.queue(publish::BINDING).is_err() {
        return false;
    }
    let now = worker::Date::now().as_millis() as f64;
    match READY.with(std::cell::Cell::get) {
        Some((true, _)) => return true,
        Some((false, at)) if now - at < CERTIFIED_RECHECK_MS => return false,
        _ => {}
    }
    let count = match crate::routes::D1Sql::of(env) {
        Ok(sql) => rows(&sql, CERTIFIED_COUNT_SQL, vec![])
            .await
            .ok()
            .and_then(|r| r.first().and_then(|r| r["count"].as_f64())),
        Err(_) => None,
    };
    let ready = ready_at(count);
    if !ready {
        worker::console_warn!(
            "[owed-events] the overlay's certified migration count {count:?} is short of {OWED_EVENTS_MIGRATION_COUNT}: the inline judge stands"
        );
    }
    READY.with(|r| r.set(Some((ready, now))));
    ready
}

/// The sweep on the belt's alarm, then its publish. Answers the pairs re-published.
pub async fn sweep_publish(env: &worker::Env) -> Result<usize, String> {
    let sql = crate::routes::D1Sql::of(env).map_err(|e| e.to_string())?;
    let swept = sweep_tick(&sql, worker::Date::now().as_millis() as i64).await?;
    let pairs = swept.pairs.clone();
    if pairs.is_empty() {
        return Ok(0);
    }
    let Some(port) = publish::WorkerPublish::of(env) else {
        return Err("no owed-events binding".into());
    };
    let report = publish::publish_with(&port, notes_of(&pairs)).await;
    // FIX-956 1b: a re-publish that faulted is noted lost again (the next tick takes it whatever its stamp)
    let lost_noted = publish::write_lost(&port, publish::lost_of(&report)).await;
    // D2-ACTOR-D (bsv-low #952): the re-published pairs' actors called inline (the scheduled drain's own pass).
    // FOLD-16 (the captain's item 5): the sweep is no writer: its direct pass samples nothing into the bars.
    direct_events_as(env, report.sent.clone(), crate::owed_actor::SWEEP_CLASS).await;
    publish::bump(
        env,
        &[
            ("owed_events_swept_total", report.sent.len() as u64),
            (publish::COUNTER_SWEEP_PASSED, swept.count(SweepReason::Passed) as u64),
            (publish::COUNTER_SWEEP_STALLED, swept.count(SweepReason::Stalled) as u64),
            (publish::COUNTER_SWEEP_LOST, swept.count(SweepReason::Lost) as u64),
            (publish::COUNTER_LOST_NOTED, lost_noted),
            (publish::COUNTER_PUBLISHED, report.sent.len() as u64),
            (publish::COUNTER_SKIPPED, report.skipped as u64),
            (publish::COUNTER_FAULTS, u64::from(report.fault.is_some())),
        ]
        .into_iter()
        .filter(|(_, d)| *d > 0)
        .collect::<Vec<_>>(),
    )
    .await;
    if let Some(fault) = report.fault {
        return Err(fault);
    }
    Ok(report.sent.len())
}

/// The app layer's writers (the record door, the hooks): publish what this invocation noted before the answer,
/// bounded. Answers the milliseconds the publish took (the door's `enqueue;dur`).
pub async fn publish_noted(env: &worker::Env) -> u64 {
    let t0 = worker::Date::now().as_millis();
    publish::publish_before_answer(env, Vec::new()).await;
    worker::Date::now().as_millis().saturating_sub(t0)
}

/// [`publish_noted`] after noting `pairs`.
pub async fn publish_pairs(env: &worker::Env, pairs: &[(&str, String)]) -> u64 {
    for (source, key) in pairs {
        publish::note(source, key);
    }
    publish_noted(env).await
}

/// The consumer's ingest of a `word` event's frame: the tracker's own write (the word triggers mark), no judge here.
async fn ingest_frame(env: &worker::Env, frame: &Value) -> Result<(), String> {
    let Some(txid) = frame["txid"]
        .as_str()
        .filter(|t| crate::logic::valid_txid(t))
        .map(str::to_ascii_lowercase)
    else {
        return Ok(());
    };
    let kind = frame["kind"].as_str().unwrap_or("refresh");
    let held = crate::tracker_host::runtime::event(env, &txid, kind, frame.get("body"), None)
        .await
        .map_err(|e| e.to_string())?
        .is_some();
    // D2-ACTOR-A-DELTA-3's LOW-1: the write noted its `word` pair; this very message routes that pair next, so the
    // note is taken back (it was left in the isolate's notes, re-sent by a later continuation).
    publish::forget("word", &txid);
    if !held {
        crate::tracker_host::runtime::note_unheld();
    }
    Ok(())
}

/// LOW-6: how long the consumer's isolate keeps a verified tip it read (the read's own memo). D2-ACTOR-A-DELTA's
/// LOW-B: a `tip` event (`tip`/`gate`, `tip`/`untipped`: the block's own marks) reads the tip FRESH, never this memo,
/// so a gate the new block opens is judged open on that event; any other event may judge at a tip up to 30 s old,
/// which can at worst judge a gate one block late until the next block's tip event re-judges it (a delay, never a
/// loss: #831's rule withholds nothing it would not withhold one block later).
pub const TIP_MEMO_MS: f64 = 30_000.0;

thread_local! {
    static TIP: std::cell::Cell<Option<(u64, f64)>> = const { std::cell::Cell::new(None) };
}

/// The verified chain tip, from the isolate's memo when younger than [`TIP_MEMO_MS`] (a read that faults is never
/// memoised: the judge then runs tipless, as before).
pub(crate) async fn tip_memoised(env: &worker::Env, fresh: bool) -> Option<u64> {
    let now = worker::Date::now().as_millis() as f64;
    if let Some((tip, at)) = TIP.with(std::cell::Cell::get) {
        if !fresh && now - at < TIP_MEMO_MS {
            return Some(tip);
        }
    }
    let tip = crate::routes::chaintracks_present_height_env(env, "owed")
        .await
        .ok()?;
    TIP.with(|t| t.set(Some((tip, now))));
    Some(tip)
}

/// D2-ACTOR-B-FOLD-10 (the fifth delta lens's NEW5-M1): the tip hook (`/internal/tip-changed`) hands the isolate's
/// memo the height it was just ANNOUNCED, before it writes the block's marks and schedules the direct pass, so no
/// apply of this isolate judges the block's marks at the previous height (a direct apply at T-1 covered the block's
/// `tip` marks and made the consumer's fresh delivery at T a dupe). A lower height never replaces a higher one.
pub(crate) fn seed_tip(height: u64) {
    let now = worker::Date::now().as_millis() as f64;
    seed_tip_at(height, now);
}
/// [`seed_tip`] at a clock (the pins).
pub fn seed_tip_at(height: u64, now_ms: f64) {
    TIP.with(|t| {
        let keep = t
            .get()
            .is_some_and(|(held, at)| held > height && now_ms - at < TIP_MEMO_MS);
        if !keep {
            t.set(Some((height, now_ms)));
        }
    });
}
/// The memo's height when younger than [`TIP_MEMO_MS`] at `now_ms` (the pins).
pub fn memo_tip_at(now_ms: f64) -> Option<u64> {
    TIP.with(std::cell::Cell::get)
        .filter(|(_, at)| now_ms - at < TIP_MEMO_MS)
        .map(|(tip, _)| tip)
}
/// PURE (NEW5-M1): does a direct pass over these events read the tip FRESH? Exactly when one of them would at the
/// consumer ([`tip_reads_fresh`]): a block's `tip` events are judged at the block, whichever path applies them first.
pub fn direct_reads_fresh(events: &[OwedEvent]) -> bool {
    events.iter().any(|e| tip_reads_fresh(&e.source))
}

/// PURE: LOW-B: does an event of `source` read the chain tip fresh (the block's own marks), never the memo?
pub fn tip_reads_fresh(source: &str) -> bool {
    source == "tip"
}

/// LOW-A (D2-ACTOR-A-DELTA): the wall one message's handling is raced against. The platform's queue consumer runs a
/// batch for at most 15 minutes of wall time (and the Worker's CPU limit, 30 s by default); with
/// `max_batch_size = 1` and `max_concurrency = 1` one hung message held the whole fleet's queue for as long as its
/// awaits hung (L52 measured `/internal/tx-word` calls cancelled at about 100 s). Past the wall the message is handed
/// back after [`CONSUMER_WALL_RETRY_S`] and its pair's fault memo noted (a pair that hangs every time is held, the
/// fold's doubling): what its judge committed before the wall is acknowledged by its own batch, the rest is planned
/// again. A judge batch dropped in flight may still land (#559's limit 5): the marks are generation-bounded, so a
/// late landing acknowledges only what it judged and the replay is idempotent.
pub const CONSUMER_MESSAGE_WALL_MS: u64 = 60_000;
/// The delay a message past [`CONSUMER_MESSAGE_WALL_MS`] is handed back with.
pub const CONSUMER_WALL_RETRY_S: u32 = 60;

/// FIX-969 (bsv-low #969): a wall is the CONSUMER's time running out, never the event's fault, so a walled message
/// does not spend the queue's `max_retries` (3, then the dead letter queue). Until this fix every walled message was
/// handed back by `retry`: under a backlog a batch of 16 that met the wall four times (the message in flight AND the
/// bystanders the pass never reached) dead-lettered all of them, parked with no fault recorded (L53: 30 parked at the
/// fold; beta on 2026-10-29: 58 parked, every one `lastFault` null, `txfacts` events among them, which are never
/// judged). Now a walled message is RE-QUEUED: its body sent again as a fresh message (its delivery count from 0)
/// under the wall's delay, carrying [`WALLS_FIELD`] (an extra top-level key the event's decode ignores; the frozen v1
/// bytes are unchanged for a message never walled), and the original acked. At most [`WALL_REQUEUES_MAX`] re-queues a
/// message: past them (or when the send faults) it is handed back by `retry` as before, so the queue's 3 retries and
/// the dead letter queue still bound it, and its letter carries `walls` (the class readable from the body). A subject
/// that walls every time is held by its memo from its third wall and left out of the plan, so its messages are acked
/// all-held, never re-queued for ever.
pub const WALL_REQUEUES_MAX: u64 = 8;
/// FIX-969: the top-level key of a re-queued body counting its walls.
pub const WALLS_FIELD: &str = "walls";

/// FIX-969: how a walled message leaves the consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WallCarriage {
    /// Sent again as a fresh message (`body` carries `walls`), the original acked.
    Requeue { body: String, walls: u64 },
    /// Handed back by `retry` (it spends one of the queue's retries): past [`WALL_REQUEUES_MAX`], or a body that is
    /// not a JSON object.
    Retry,
}

/// PURE (FIX-969): the carriage of a walled message's body.
pub fn wall_carriage(body: &str) -> WallCarriage {
    let Ok(Value::Object(mut o)) = serde_json::from_str::<Value>(body) else {
        return WallCarriage::Retry;
    };
    let walls = o.get(WALLS_FIELD).and_then(Value::as_u64).unwrap_or(0);
    if walls >= WALL_REQUEUES_MAX {
        return WallCarriage::Retry;
    }
    o.insert(WALLS_FIELD.into(), json!(walls + 1));
    WallCarriage::Requeue {
        body: Value::Object(o).to_string(),
        walls: walls + 1,
    }
}

/// FIX-969: the queue a walled message is re-sent to (the Worker's is the `OWED_EVENTS` binding).
#[allow(async_fn_in_trait)]
pub trait RequeuePort {
    async fn send_delayed(&self, body: String, delay_s: u32) -> Result<(), String>;
}

/// FIX-969: what became of one walled message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Handback {
    /// Re-sent fresh: ack the original (`owed_events_wall_requeued_total`).
    Requeued,
    /// Hand it back by `retry` with the delay (`owed_events_wall_retried_total`): it spends a queue retry.
    Retried,
}

/// FIX-969: the ONE decision every wall site calls (the coalesced batch's in-flight and bystander messages, the
/// singles a walled batch leaves, the one-message arm): re-queue when [`wall_carriage`] says so and the send lands,
/// else retry.
pub async fn hand_back_walled(port: &impl RequeuePort, body: &str, delay_s: u32) -> Handback {
    match wall_carriage(body) {
        WallCarriage::Requeue { body, .. } => match port.send_delayed(body, delay_s).await {
            Ok(()) => Handback::Requeued,
            Err(_) => Handback::Retried,
        },
        WallCarriage::Retry => Handback::Retried,
    }
}

/// FIX-969: the Worker's re-queue, the `OWED_EVENTS` producer binding.
struct WorkerRequeue(Option<worker::Queue>);

impl RequeuePort for WorkerRequeue {
    async fn send_delayed(&self, body: String, delay_s: u32) -> Result<(), String> {
        let q = self.0.as_ref().ok_or("no OWED_EVENTS binding")?;
        let msg = worker::RawMessageBuilder::new(worker::wasm_bindgen::JsValue::from_str(&body))
            .delay_seconds(delay_s)
            .build_with_content_type(worker::QueueContentType::Text);
        q.send_raw_batch(vec![msg]).await.map_err(|e| e.to_string())?;
        crate::d1_ledger::note_queue_ops(1);
        Ok(())
    }
}

/// The `owed-events` consumer's handler of one decoded message: the frame's ingest, the routing and the judge, the
/// continuation. `Err`: hand it back.
pub async fn consume_one(env: &worker::Env, ev: &OwedEvent) -> Result<Routed, String> {
    if let Some(frame) = ev.frame() {
        ingest_frame(env, frame).await?;
        // the ingest noted the word's own event; this consumer routes it now
        publish::forget(&ev.source, &ev.source_key);
    }
    crate::schema::ensure_latch_columns(env).await;
    // LOW-6 (the lens): the marks first; the chain tip (its 30 s isolate memo) only for a message with a subject
    let sql = crate::routes::D1Sql::of(env).map_err(|e| e.to_string())?;
    let plan = plan(&sql, ev).await?;
    if plan.is_empty() {
        // NEW2-M1: an all-held message ends here (no judge, no tip, no continuation)
        return Ok(plan.empty_routed());
    }
    let tip = tip_memoised(env, tip_reads_fresh(&ev.source)).await;
    let judge = owed_judge::D1Judge::new(env, tip, crate::routes::written_off_before_ms_env(env))
        .map_err(|e| e.to_string())?
        .with_event(crate::owed_actor::EventCtx::of(ev));
    let routed = judge_plan(&judge, plan).await?;
    if continues(&routed) {
        publish::note(&ev.source, &ev.source_key);
        publish::publish_env(env, Vec::new()).await;
    }
    // D2-ACTOR-D: the consumer IS this event's delivery; what it published (a continuation, a frame's own word) is
    // the queue's alone, never held for a direct call by a later request of this isolate.
    // D2-ACTOR-B-FOLD-10 (NEW5-L2): only ITS OWN events are taken and dropped; a concurrent writer's (held by a
    // publish that ran in this isolate during one of the consumer's awaits) stay held for that writer's flush point.
    let _ = publish::take_direct_where(|e| consumer_owns(ev, e));
    Ok(routed)
}

/// PURE (NEW5-L2): is a held event the consumer's own publish for the delivery `ev` (its continuation, or its
/// frame's word: the same pair)? Every other held event is a concurrent writer's.
pub fn consumer_owns(ev: &OwedEvent, held: &OwedEvent) -> bool {
    held.source == ev.source && held.source_key.eq_ignore_ascii_case(&ev.source_key)
}

/// D2-ACTOR-E (bsv-low #954) item 3: the most messages one consumer batch takes (`max_batch_size` in both renders,
/// pinned equal): a burst's messages arrive together and are judged once per identity ([`consume_batch`]).
pub const CONSUMER_BATCH_MAX: usize = 16;

/// PURE: the event a coalesced delivery carries to the actor: the batch's OLDEST write under its key, every OTHER
/// event in `also` with its write time (D2-ACTOR-B-FOLD-11, NEW6-L1: the object samples each event by its own first
/// apply, so the faulted tail of a batch whose oldest event its direct call applied is sampled), the txids every
/// message's write named (`None` when one names no row: the whole closure), the
/// tip read fresh when any message is a block's.
pub fn coalesced_event(evs: &[&OwedEvent]) -> Option<crate::owed_actor::EventCtx> {
    let oldest = evs.iter().min_by_key(|e| (e.at_ms, e.seq))?;
    let mut ctx = crate::owed_actor::EventCtx::of(oldest);
    let mut txids: Option<Vec<String>> = Some(Vec::new());
    for e in evs {
        txids = match (txids, crate::owed_actor::event_txids(e)) {
            (Some(mut t), Some(more)) => {
                t.extend(more);
                Some(t)
            }
            _ => None,
        };
    }
    if let Some(t) = txids.as_mut() {
        t.sort();
        t.dedup();
    }
    ctx.txids = txids;
    // D2-ACTOR-B-FOLD-11 (NEW6-L1): every other event of the batch, each sampled by its own first apply
    let mut also: Vec<(String, i64, i64)> = Vec::new();
    for e in evs {
        let key = (e.source.clone(), e.seq, e.at_ms);
        if (e.source.as_str(), e.seq) != (ctx.kind.as_str(), ctx.seq) && !also.contains(&key) {
            also.push(key);
        }
    }
    ctx.also = also;
    Some(ctx)
}

/// PURE (D2-ACTOR-B-FOLD-12, the seventh delta lens's NEW7-L3): [`coalesced_event`] with each event filed under the
/// identities its own plan names (`by_identity`), so each identity's apply carries only the events naming it: an event
/// is sampled once, by its own first apply in its own actor.
pub fn coalesced_event_of(planned: &[(&OwedEvent, &Plan)]) -> Option<crate::owed_actor::EventCtx> {
    let evs: Vec<&OwedEvent> = planned.iter().map(|(e, _)| *e).collect();
    let mut ctx = coalesced_event(&evs)?;
    if planned.len() > 1 {
        for (e, p) in planned {
            let one = crate::owed_actor::EventCtx::of(e);
            let mut ids: Vec<String> = p
                .subject_pairs()
                .into_iter()
                .map(|(i, _)| i.to_ascii_lowercase())
                .collect();
            ids.sort();
            ids.dedup();
            for id in ids {
                ctx.by_identity.entry(id).or_default().push((
                    one.kind.clone(),
                    one.seq,
                    one.at_ms,
                    one.mined,
                ));
            }
        }
    }
    Some(ctx)
}

/// D2-ACTOR-B-FOLD-11 (the sixth delta lens's NEW6-M2): one message's fate in a consumer batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fate {
    /// Judged (or nothing to judge): acked. `resolve` clears its pair's fault memo and resolves its dead letter (its
    /// OWN plan's routing: a message whose every subject was held resolves nothing); `continues` publishes its
    /// continuation.
    Ack { resolve: bool, continues: bool },
    /// Handed back (the wall), `delay_s` the delay (`None`: the platform's own).
    Retry { delay_s: Option<u32> },
    /// Its every subject faulted and none landed (the single arm's `Err`): its pair's memo noted, handed back.
    Fault { delay_s: Option<u32> },
    /// Left out of the coalesced pass: judged by the one-message arm.
    Alone,
}

/// What a coalesced batch did ([`batch_core`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BatchOutcome {
    /// One fate a message, in the batch's order.
    pub fates: Vec<Fate>,
    /// The merged pass's routing (its counters).
    pub routed: Routed,
    /// The batch's wall fell during the merged pass.
    pub walled: bool,
    /// The messages the merged pass took.
    pub coalesced: usize,
}

/// PURE (NEW6-M2): which messages one coalesced pass takes. A message is left to the one-message arm (`false`) when
/// its pair or one of its plan's subjects carries a fault memo (`faulted`: a pass that faulted or walled on it
/// before, which the one-message arm notes and holds as the earlier folds rule), or when taking it would carry the
/// merged plan past ONE message's bound: [`owed_event::MAX_GAMES_PER_MESSAGE`] subject games, or
/// [`ROUTE_READ_LIMIT`] marks. The first message that fits is always taken (its own plan is within the bound).
pub fn batch_selection(plans: &[Plan], faulted: &[bool]) -> Vec<bool> {
    let mut taken: Vec<Plan> = Vec::new();
    plans
        .iter()
        .zip(faulted)
        .map(|(p, f)| {
            if *f {
                return false;
            }
            let mut with = taken.clone();
            with.push(p.clone());
            let merged = Plan::merged(&with);
            let fits = merged.game_count() <= owed_event::MAX_GAMES_PER_MESSAGE
                && merged.mark_count() <= ROUTE_READ_LIMIT;
            if fits || taken.is_empty() {
                taken.push(p.clone());
                true
            } else {
                false
            }
        })
        .collect()
}

/// The wall fell on a message (either arm): the subjects in flight noted on their own memos (the delay is their
/// longest hold), or, with none in flight, the pair's memo, handed back in [`CONSUMER_WALL_RETRY_S`].
pub async fn wall_fate(db: &impl SqlPort, pairs: &[(&str, &str)], in_flight: &[String]) -> u32 {
    match note_wall_subjects(db, in_flight).await {
        Some(d) => d,
        None => {
            for (source, key) in pairs {
                let _ = note_fault(db, source, key, "the consumer's message wall").await;
            }
            CONSUMER_WALL_RETRY_S
        }
    }
}

/// A message whose judge faulted with nothing landed (either arm): its pair's memo noted; the delay its hold sets
/// (`None`: no hold yet, the platform's retry).
pub async fn fault_fate(db: &impl SqlPort, source: &str, key: &str, fault: &str) -> Option<u32> {
    match note_fault(db, source, key, fault).await {
        Ok(m) if m.hold_ms > 0 => Some((m.hold_ms / 1000).clamp(1, 43_200) as u32),
        _ => None,
    }
}

/// THE COALESCED BATCH'S CORE (D2-ACTOR-E item 3, bounded by D2-ACTOR-B-FOLD-11, NEW6-M2), over a port so the pins
/// run it as shipped: the messages' memos read (one statement), the pass's messages chosen ([`batch_selection`]), ONE
/// judge pass over their merged plan raced against `wall`, then each message's fate by its OWN plan: acked and
/// resolved as the one-message arm would have answered it alone, its pair's memo noted where its every subject
/// faulted. At the wall the subjects in flight are noted ([`wall_fate`]): a message naming one is handed back with
/// their hold, the others at once (their marks stand; a landed subject's were acknowledged by its own batch), so a
/// slow subject is held by its third wall (alone, the one-message arm, from its first) and never starves the rest.
/// `make` builds the judge for the pass's event ([`coalesced_event`]; `true`: a block's, the tip read fresh), only
/// when the pass has a subject. `Err`: a judge or
/// read fault with nothing landed: the caller judges the batch one message at a time.
pub async fn batch_core<S, D, F, W>(
    db: &S,
    evs: &[&OwedEvent],
    plans: &[Plan],
    make: impl FnOnce(Option<crate::owed_actor::EventCtx>, bool) -> F,
    wall: W,
) -> Result<BatchOutcome, String>
where
    S: SqlPort,
    D: owed_judge::JudgeDb,
    F: std::future::Future<Output = Result<D, String>>,
    W: std::future::Future<Output = ()>,
{
    let mut names: Vec<String> = Vec::new();
    for (ev, p) in evs.iter().zip(plans) {
        names.push(fault_memo_name(&ev.source, &ev.source_key));
        names.extend(
            p.subject_pairs()
                .iter()
                .map(|(i, g)| subject_memo_name(i, g)),
        );
    }
    let memos = memo_holds(
        &rows(
            db,
            FAULT_MEMO_READ_SQL,
            vec![Bind::Text(json!(names).to_string())],
        )
        .await?,
    );
    let faulted: Vec<bool> = evs
        .iter()
        .zip(plans)
        .map(|(ev, p)| {
            memos.contains_key(&fault_memo_name(&ev.source, &ev.source_key))
                || p.subject_pairs()
                    .iter()
                    .any(|(i, g)| memos.contains_key(&subject_memo_name(i, g)))
        })
        .collect();
    let taken = batch_selection(plans, &faulted);
    let chosen: Vec<Plan> = plans
        .iter()
        .zip(&taken)
        .filter(|(_, t)| **t)
        .map(|(p, _)| p.clone())
        .collect();
    let merged = Plan::merged(&chosen);
    let mut out = BatchOutcome {
        coalesced: chosen.len(),
        ..BatchOutcome::default()
    };
    if merged.is_empty() {
        out.routed = merged.empty_routed();
        out.fates = plans
            .iter()
            .zip(&taken)
            .map(|(p, t)| match t {
                true => Fate::Ack {
                    resolve: p.empty_routed().held == 0,
                    continues: false,
                },
                false => Fate::Alone,
            })
            .collect();
        return Ok(out);
    }
    let planned: Vec<(&OwedEvent, &Plan)> = evs
        .iter()
        .zip(plans)
        .zip(&taken)
        .filter(|((_, p), t)| **t && !p.is_empty())
        .map(|((e, p), _)| (*e, p))
        .collect();
    let fresh = planned.iter().any(|(e, _)| tip_reads_fresh(&e.source));
    let ctx = coalesced_event_of(&planned);
    let _ = take_in_flight();
    let work = std::pin::pin!(async move {
        let judge = make(ctx, fresh).await?;
        judge_plan(&judge, merged).await
    });
    let wall = std::pin::pin!(wall);
    let done = match futures_util::future::select(work, wall).await {
        futures_util::future::Either::Left((done, _)) => Some(done),
        futures_util::future::Either::Right(((), _)) => None,
    };
    let Some(done) = done else {
        out.walled = true;
        let in_flight = take_in_flight();
        let walled_pairs: Vec<(&str, &str)> = evs
            .iter()
            .zip(&taken)
            .filter(|(_, t)| **t)
            .map(|(e, _)| (e.source.as_str(), e.source_key.as_str()))
            .collect();
        let delay = wall_fate(
            db,
            if in_flight.is_empty() {
                &walled_pairs
            } else {
                &[]
            },
            &in_flight,
        )
        .await;
        out.fates = plans
            .iter()
            .zip(&taken)
            .map(|(p, t)| {
                let names_one = p
                    .subject_pairs()
                    .iter()
                    .any(|(i, g)| in_flight.contains(&subject_memo_name(i, g)));
                match (t, in_flight.is_empty() || names_one) {
                    (true, true) => Fate::Retry {
                        delay_s: Some(delay),
                    },
                    _ => Fate::Retry { delay_s: None },
                }
            })
            .collect();
        return Ok(out);
    };
    let routed = done?;
    let mut fates = Vec::with_capacity(plans.len());
    for ((ev, p), t) in evs.iter().zip(plans).zip(&taken) {
        if !*t {
            fates.push(Fate::Alone);
            continue;
        }
        let own = p.subject_pairs();
        let landed = own.iter().filter(|s| routed.landed.contains(s)).count();
        let faulted = own.iter().filter(|s| routed.faulted.contains(s)).count();
        let held = p.empty_routed().held + own.len() - landed - faulted;
        if landed == 0 && faulted > 0 {
            let delay = fault_fate(
                db,
                &ev.source,
                &ev.source_key,
                "a subject's judge faulted in the coalesced batch",
            )
            .await;
            fates.push(Fate::Fault { delay_s: delay });
        } else {
            fates.push(Fate::Ack {
                resolve: landed > 0 || held == 0,
                continues: p.has_more() && landed > 0,
            });
        }
    }
    out.fates = fates;
    out.routed = routed;
    Ok(out)
}

/// THE COALESCED BATCH (D2-ACTOR-E item 3; bounded by D2-ACTOR-B-FOLD-11, NEW6-M2): every message's frame ingested
/// and its marks planned, then [`batch_core`]: ONE judge pass over the merged plan of the messages that fit one
/// message's bound, under the batch's one wall, each message's fate by its own plan. `Err`: the caller handles the
/// batch one message at a time (each message's fault memo, its own wall).
pub async fn consume_batch(env: &worker::Env, evs: &[&OwedEvent]) -> Result<BatchOutcome, String> {
    for ev in evs {
        if let Some(frame) = ev.frame() {
            ingest_frame(env, frame).await?;
            publish::forget(&ev.source, &ev.source_key);
        }
    }
    crate::schema::ensure_latch_columns(env).await;
    let sql = crate::routes::D1Sql::of(env).map_err(|e| e.to_string())?;
    let mut plans = Vec::with_capacity(evs.len());
    for ev in evs {
        plans.push(plan(&sql, ev).await?);
    }
    let make = |ctx: Option<crate::owed_actor::EventCtx>, fresh: bool| async move {
        let tip = tip_memoised(env, fresh).await;
        let mut judge =
            owed_judge::D1Judge::new(env, tip, crate::routes::written_off_before_ms_env(env))
                .map_err(|e| e.to_string())?;
        if let Some(ctx) = ctx {
            judge = judge.with_event(ctx);
        }
        Ok(judge)
    };
    let wall = worker::Delay::from(std::time::Duration::from_millis(CONSUMER_MESSAGE_WALL_MS));
    let outcome = batch_core(&sql, evs, &plans, make, wall).await?;
    let mut continued = false;
    for (ev, fate) in evs.iter().zip(&outcome.fates) {
        if matches!(
            fate,
            Fate::Ack {
                continues: true,
                ..
            }
        ) {
            publish::note(&ev.source, &ev.source_key);
            continued = true;
        }
    }
    if continued {
        publish::publish_env(env, Vec::new()).await;
    }
    // NEW5-L2: only the batch's own publishes (its continuations, its frames' words) are the queue's alone.
    let _ = publish::take_direct_where(|held| evs.iter().any(|ev| consumer_owns(ev, held)));
    Ok(outcome)
}

/// The Worker's `queue` handler: the `owed-events` consumer, or its dead letter queue's park.
pub async fn queue(batch: worker::MessageBatch<Value>, env: worker::Env) -> worker::Result<()> {
    use worker::MessageExt;
    let name = batch.queue();
    if name.ends_with("-dlq") {
        return park_dead_letters(&batch, &env).await;
    }
    let now = worker::Date::now().as_millis() as i64;
    // D2-ACTOR-D: a render that raised the consumer's concurrency while the D1 judge writes is named on every batch.
    let concurrency = consumer_concurrency_of(
        env.var(CONSUMER_CONCURRENCY_VAR)
            .ok()
            .map(|v| v.to_string())
            .as_deref(),
    );
    let d1_judges = crate::owed_actor::flag_on(
        env.var(crate::owed_actor::D1_JUDGE_FLAG)
            .ok()
            .map(|v| v.to_string()),
    );
    if consumer_concurrency_refused(concurrency, d1_judges) {
        worker::console_warn!(
            "[owed-events] {CONSUMER_CONCURRENCY_VAR}={concurrency} with the D1 judge ON: two consumers are two D1 writers of one identity's rows; set it to 1 until Phase C"
        );
    }
    let mut oldest_age: i64 = 0;
    let mut deltas: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut messages = 0u64;
    let mut resolved: Vec<Stmt> = Vec::new();
    let mut service_max: u64 = 0;
    // FIX-956: the greatest publish stamp among the messages this batch ACKNOWLEDGED (the consumer's horizon)
    let mut horizon: Option<i64> = None;
    // FIX-969: a walled message is re-sent fresh on the producer binding
    let requeue = WorkerRequeue(env.queue(publish::BINDING).ok());
    // D2-ACTOR-E (bsv-low #954) item 3: a batch of several messages is judged ONCE per identity first; a fault of
    // that pass hands the batch to the one-message path below (each message's own fault memo and wall).
    let raw: Vec<_> = batch.raw_iter().collect();
    let decoded: Vec<Option<OwedEvent>> = raw
        .iter()
        .map(|m| OwedEvent::decode(&m.body().as_string().unwrap_or_default()).ok())
        .collect();
    // D2-ACTOR-B-FOLD-11 (NEW6-M2): the messages the coalesced pass left (`Fate::Alone`, or every one when it
    // faulted) are judged one at a time below.
    let mut singles: Vec<usize> = (0..raw.len()).collect();
    if decoded.iter().filter(|d| d.is_some()).count() > 1 {
        let at: Vec<usize> = (0..raw.len()).filter(|i| decoded[*i].is_some()).collect();
        let evs: Vec<&OwedEvent> = decoded.iter().flatten().collect();
        let started = worker::Date::now().as_millis();
        let done = consume_batch(&env, &evs).await;
        let service_ms = worker::Date::now().as_millis().saturating_sub(started);
        match done {
            Ok(outcome) => {
                singles.clear();
                service_max = service_max.max(service_ms);
                for (i, msg) in raw.iter().enumerate() {
                    if decoded[i].is_none() {
                        messages += 1;
                        oldest_age = oldest_age.max(now - msg.timestamp().as_millis() as i64);
                        worker::console_warn!(
                            "[owed-events] a message does not read: handed back, then dead-lettered"
                        );
                        msg.retry();
                    }
                }
                for (n, fate) in outcome.fates.iter().enumerate() {
                    let i = at[n];
                    let (msg, ev) = (&raw[i], evs[n]);
                    if matches!(fate, Fate::Alone) {
                        singles.push(i);
                        continue;
                    }
                    messages += 1;
                    oldest_age = oldest_age.max(now - msg.timestamp().as_millis() as i64);
                    let retry = |delay: Option<u32>| match delay {
                        Some(d) => msg.retry_with_options(
                            &worker::QueueRetryOptionsBuilder::new()
                                .with_delay_seconds(d)
                                .build(),
                        ),
                        None => msg.retry(),
                    };
                    match fate {
                        Fate::Ack { resolve, .. } => {
                            if *resolve {
                                resolved.extend(resolved_statements(&ev.source, &ev.source_key));
                            }
                            *deltas.entry("owed_events_consumed_total").or_default() += 1;
                            // FIX-956: the consumer's horizon is the greatest publish stamp it acknowledged
                            horizon = horizon.max(Some(ev.at_ms));
                            msg.ack();
                        }
                        // FIX-969: a wall's hand-back re-queues (never a spent retry)
                        Fate::Retry { delay_s } => {
                            hand_back_msg(msg, &requeue, delay_s.unwrap_or(0), &mut deltas).await
                        }
                        Fate::Fault { delay_s } => {
                            *deltas.entry("owed_events_judge_faults_total").or_default() += 1;
                            if delay_s.is_some() {
                                *deltas.entry("owed_events_fault_held_total").or_default() += 1;
                            }
                            retry(*delay_s);
                        }
                        Fate::Alone => {}
                    }
                }
                let routed = &outcome.routed;
                *deltas.entry("owed_events_acked_marks_total").or_default() += routed.marks as u64;
                *deltas
                    .entry("owed_events_subject_faults_total")
                    .or_default() += routed.faults as u64;
                *deltas.entry("owed_events_subject_held_total").or_default() += routed.held as u64;
                let continued = outcome
                    .fates
                    .iter()
                    .filter(|f| {
                        matches!(
                            f,
                            Fate::Ack {
                                continues: true,
                                ..
                            }
                        )
                    })
                    .count() as u64;
                *deltas.entry("owed_events_continued_total").or_default() += continued;
                if all_held(routed) {
                    *deltas.entry("owed_events_all_held_total").or_default() += 1;
                }
                if outcome.walled {
                    worker::console_warn!(
                        "[owed-events] the coalesced batch of {} passed the {CONSUMER_MESSAGE_WALL_MS} ms wall: its subjects in flight noted, handed back",
                        outcome.coalesced
                    );
                    *deltas
                        .entry("owed_events_consumer_wall_drops_total")
                        .or_default() += 1;
                    // a walled batch spent its time: what the pass left waits for the next batch
                    for i in singles.drain(..) {
                        messages += 1;
                        hand_back_msg(&raw[i], &requeue, 0, &mut deltas).await;
                    }
                }
                *deltas
                    .entry("owed_events_coalesced_batches_total")
                    .or_default() += 1;
            }
            Err(e) => {
                worker::console_warn!(
                    "[owed-events] the coalesced batch of {} not judged ({e}): each message judged alone",
                    evs.len()
                );
            }
        }
    }
    for i in singles {
        let msg = &raw[i];
        messages += 1;
        let age = now - msg.timestamp().as_millis() as i64;
        oldest_age = oldest_age.max(age);
        let body = msg.body().as_string().unwrap_or_default();
        let ev = match OwedEvent::decode(&body) {
            Ok(ev) => ev,
            Err(e) => {
                worker::console_warn!(
                    "[owed-events] a message does not read ({e}): handed back, then dead-lettered"
                );
                msg.retry();
                continue;
            }
        };
        let started = worker::Date::now().as_millis();
        let _ = take_in_flight();
        let walled = {
            let work = std::pin::pin!(consume_one(&env, &ev));
            let wall = std::pin::pin!(worker::Delay::from(std::time::Duration::from_millis(
                CONSUMER_MESSAGE_WALL_MS
            )));
            match futures_util::future::select(work, wall).await {
                futures_util::future::Either::Left((done, _)) => Some(done),
                futures_util::future::Either::Right(((), _)) => None,
            }
        };
        let service_ms = worker::Date::now().as_millis().saturating_sub(started);
        service_max = service_max.max(service_ms);
        let Some(done) = walled else {
            worker::console_warn!(
                "[owed-events] {} {} seq={} passed the {CONSUMER_MESSAGE_WALL_MS} ms message wall: handed back in {CONSUMER_WALL_RETRY_S} s",
                ev.source,
                ev.source_key,
                ev.seq
            );
            *deltas.entry("owed_events_consumer_wall_drops_total").or_default() += 1;
            // LOW-1 (the second delta lens): the subjects in flight are noted, the pair only when none was
            let in_flight = take_in_flight();
            let mut delay = CONSUMER_WALL_RETRY_S;
            if let Ok(sql) = crate::routes::D1Sql::of(&env) {
                // D2-ACTOR-B-FOLD-11: the one rule both arms call
                delay = wall_fate(&sql, &[(&ev.source, &ev.source_key)], &in_flight).await;
            }
            // FIX-969: re-queued, never a spent retry
            hand_back_msg(msg, &requeue, delay, &mut deltas).await;
            continue;
        };
        match done {
            Ok(routed) => {
                // a landed (or empty) message resolves its pair; one whose every subject was held resolves nothing
                if routed.subjects > 0 || routed.held == 0 {
                    resolved.extend(resolved_statements(&ev.source, &ev.source_key));
                }
                *deltas.entry("owed_events_consumed_total").or_default() += 1;
                *deltas.entry("owed_events_acked_marks_total").or_default() += routed.marks as u64;
                *deltas.entry("owed_events_subject_faults_total").or_default() += routed.faults as u64;
                *deltas.entry("owed_events_subject_held_total").or_default() += routed.held as u64;
                if continues(&routed) {
                    *deltas.entry("owed_events_continued_total").or_default() += 1;
                }
                if all_held(&routed) {
                    *deltas.entry("owed_events_all_held_total").or_default() += 1;
                }
                horizon = horizon.max(Some(ev.at_ms));
                msg.ack();
            }
            Err(e) => {
                worker::console_warn!(
                    "[owed-events] {} {} seq={} not judged ({e}): its marks stay, handed back",
                    ev.source,
                    ev.source_key,
                    ev.seq
                );
                *deltas.entry("owed_events_judge_faults_total").or_default() += 1;
                // MEDIUM-2: the pair's fault memo (the cause readable), and its hold once past the bound
                let delay = match crate::routes::D1Sql::of(&env) {
                    Ok(sql) => fault_fate(&sql, &ev.source, &ev.source_key, &e).await,
                    Err(_) => None,
                };
                match delay {
                    Some(d) => {
                        *deltas.entry("owed_events_fault_held_total").or_default() += 1;
                        msg.retry_with_options(
                            &worker::QueueRetryOptionsBuilder::new()
                                .with_delay_seconds(d)
                                .build(),
                        );
                    }
                    None => msg.retry(),
                }
            }
        }
    }
    finish_batch(
        &env, now, oldest_age, messages, service_max, deltas, resolved, horizon,
    )
    .await
}

/// FIX-969: one walled message handed back by [`hand_back_walled`], counted by its carriage.
async fn hand_back_msg(
    msg: &worker::RawMessage,
    port: &WorkerRequeue,
    delay_s: u32,
    deltas: &mut BTreeMap<&'static str, u64>,
) {
    use worker::MessageExt;
    let body = msg.body().as_string().unwrap_or_default();
    match hand_back_walled(port, &body, delay_s).await {
        Handback::Requeued => {
            *deltas.entry(COUNTER_WALL_REQUEUED).or_default() += 1;
            msg.ack();
        }
        Handback::Retried => {
            *deltas.entry(COUNTER_WALL_RETRIED).or_default() += 1;
            msg.retry_with_options(
                &worker::QueueRetryOptionsBuilder::new()
                    .with_delay_seconds(delay_s)
                    .build(),
            );
        }
    }
}

/// FIX-969: walled messages re-sent fresh (each one; `owed_events_consumer_wall_drops_total` counts the walls).
pub const COUNTER_WALL_REQUEUED: &str = "owed_events_wall_requeued_total";
/// FIX-969: walled messages handed back by `retry` (past [`WALL_REQUEUES_MAX`], or a faulted send): these spend the
/// queue's retries and may become letters.
pub const COUNTER_WALL_RETRIED: &str = "owed_events_wall_retried_total";

/// PURE (FIX-956): the consumer row's cursor of one batch (`horizon`: the greatest publish stamp it acknowledged;
/// [`CONSUMER_ROW_SQL`] keeps the row's when this is lower or absent).
pub fn consumer_row(
    now: i64,
    oldest_age: i64,
    messages: u64,
    service_max: u64,
    horizon: Option<i64>,
) -> Value {
    json!({"oldestEnqueueAgeMs": oldest_age, "messages": messages, "serviceMs": service_max, "atMs": now, "horizonMs": horizon})
}

/// The consumer row's write (the pins run it as the batch writes it).
pub fn consumer_row_stmt(row: &Value, now: i64) -> Stmt {
    Stmt {
        sql: CONSUMER_ROW_SQL.into(),
        binds: vec![Bind::Text(row.to_string()), Bind::Int(now)],
    }
}

/// The batch's end: its counters, the consumer's age row (and its horizon), the resolved pairs and the health
/// snapshot, one batch.
#[allow(clippy::too_many_arguments)]
async fn finish_batch(
    env: &worker::Env,
    now: i64,
    oldest_age: i64,
    messages: u64,
    service_max: u64,
    deltas: BTreeMap<&str, u64>,
    resolved: Vec<Stmt>,
    horizon: Option<i64>,
) -> worker::Result<()> {
    let deltas: Vec<(&str, u64)> = deltas.into_iter().filter(|(_, d)| *d > 0).collect();
    publish::bump(env, &deltas).await;
    if let Ok(sql) = crate::routes::D1Sql::of(env) {
        let row = consumer_row(now, oldest_age, messages, service_max, horizon);
        let mut batch_row = vec![consumer_row_stmt(&row, now)];
        // MEDIUM-2: a landed judge clears its pair's fault memo and resolves its dead letter
        batch_row.extend(resolved);
        // D2-ACTOR-B-FOLD-6: the owed actor's health snapshot, in the consumer row's batch (at most once a minute).
        batch_row.push(crate::owed_actor::health_snapshot_stmt(now as u64, false));
        if let Err(e) = sql.run_batch(batch_row).await {
            worker::console_warn!("[owed-events] the consumer's age row not written: {e}");
        }
    }
    Ok(())
}

/// LOW-3: the dead letter queue's consumer, on #576's shape: PARK FIRST, ACK AFTER, one row per pair
/// ([`park_letter`]). A park that faults or meets a full table is handed back with [`park_retry_delay_s`] (LOW-5);
/// the platform's last delivery loses the letter, never its marks.
async fn park_dead_letters(
    batch: &worker::MessageBatch<Value>,
    env: &worker::Env,
) -> worker::Result<()> {
    use worker::MessageExt;
    let sql = crate::routes::D1Sql::of(env)?;
    let now = worker::Date::now().as_millis() as i64;
    let mut parked = 0u64;
    for msg in batch.raw_iter() {
        let body = msg.body().as_string().unwrap_or_default();
        let id = msg.id();
        let delay = park_retry_delay_s(now - msg.timestamp().as_millis() as i64);
        let back = || {
            msg.retry_with_options(
                &worker::QueueRetryOptionsBuilder::new()
                    .with_delay_seconds(delay)
                    .build(),
            )
        };
        match park_letter(&sql, &id, &body).await {
            Ok(Parked::Row(status)) => {
                parked += 1;
                worker::console_warn!("[owed-events] PARKED dead letter {id} ({status}): {body}");
                msg.ack();
            }
            Ok(Parked::Full) => {
                worker::console_warn!(
                    "[owed-events] dead letter {id} not parked (the table is full): handed back in {delay} s"
                );
                back();
            }
            Err(e) => {
                worker::console_warn!(
                    "[owed-events] dead letter {id} park faulted ({e}): handed back in {delay} s"
                );
                back();
            }
        }
    }
    publish::bump(env, &[("owed_events_dlq_total", parked)]).await;
    Ok(())
}

/// MEDIUM-8: `/health.owedEvents`: the served ages, from rows (never a scan): the drain's backlog snapshot `dirty`
/// (`owed_judge::dirty_health`, the one `/health` already reads for `owed.dirty`) and the consumer's batch row (one
/// primary-key read). D2-ACTOR-A-FOLD-4: the fault memo, the dead letters and the family's other totals are the
/// overlay's `/health/invariants.owedEvents` (its counters and `publish::HEALTH_STATE_SQL`); here they cost the route
/// five statements and a 16-row counter read past its budget (the tier's `health-dirty-snapshot`).
/// D2-ACTOR-A-FOLD-5: `publishedTotal` and `consumedTotal` stay here, numbers (0 for an absent row), read by
/// [`HEALTH_TOTALS_SQL`] (two rows) in the consumer row's batch.
pub async fn health(db: &impl SqlPort, dirty: &Value) -> Value {
    health_and_actor(db, dirty).await.0
}

/// [`health`] and the owed actor's health snapshot read by the same statement (`None` before its first write or on a
/// fault).
pub async fn health_and_actor(db: &impl SqlPort, dirty: &Value) -> (Value, Option<Value>) {
    let (body, actor, _) = health_actor_fleet(db, dirty).await;
    (body, actor)
}

/// [`health_and_actor`] and FIX-998-SERVE's `owedActor.fleetCompare` (bsv-low #998): the fleet compare's public counts,
/// the ring's snapshot row (`owed_events_publish::FLEET_COMPARE_HEALTH_NAME`) read as the consumer statement's third
/// primary-key row (no statement more), served by `owed_events_publish::fleet_compare_health_view` (`null` before the
/// ring's first tick or on a fault).
pub async fn health_actor_fleet(db: &impl SqlPort, dirty: &Value) -> (Value, Option<Value>, Value) {
    let read = db
        .run_batch(vec![
            Stmt {
                sql: HEALTH_CONSUMER_SQL.into(),
                binds: vec![],
            },
            Stmt {
                sql: HEALTH_TOTALS_SQL.into(),
                binds: vec![],
            },
        ])
        .await
        .ok();
    let row = |name: &str| {
        read.as_ref()
            .and_then(|r| r.first())
            .and_then(|o| o.rows.iter().find(|r| r["name"].as_str() == Some(name)))
            .and_then(|r| r["cursor"].as_str())
    };
    let consumer = row("owed-events:consumer").and_then(|c| serde_json::from_str::<Value>(c).ok());
    let horizon = consumer.as_ref().and_then(horizon_of);
    let actor = crate::owed_actor::health_snapshot_of(row(crate::owed_actor::HEALTH_SNAPSHOT_NAME));
    let total = |name: &str| -> u64 {
        read.as_ref()
            .and_then(|r| r.get(1))
            .and_then(|o| o.rows.iter().find(|r| r["name"].as_str() == Some(name)))
            .and_then(|r| {
                r["value"]
                    .as_u64()
                    .or_else(|| r["value"].as_f64().map(|f| f as u64))
            })
            .unwrap_or(0)
    };
    let body = json!({
        "publishedTotal": total("owed_events_published_total"),
        "consumedTotal": total("owed_events_consumed_total"),
        "unackedMarks": dirty["count"],
        "oldestUnackedMarkAgeMs": dirty["oldestMarkAgeMs"],
        "oldestEnqueueAgeMs": consumer.as_ref().map(|c| c["oldestEnqueueAgeMs"].clone()),
        "consumerAtMs": consumer.as_ref().map(|c| c["atMs"].clone()),
        "serviceMs": consumer.as_ref().map(|c| c["serviceMs"].clone()),
        // FIX-956: the consumer's horizon (the newest publish stamp it acknowledged) and its age
        "horizonMs": horizon,
        "horizonAgeMs": horizon.map(|h| (db.now_ms() as i64 - h).max(0)),
        "totals": "/health/invariants.owedEvents",
    });
    let fleet = publish::fleet_compare_health_view(
        row(publish::FLEET_COMPARE_HEALTH_NAME),
        db.now_ms() as i64,
    );
    (body, actor, fleet)
}

// ---- D2-ACTOR-D (bsv-low #952; the design's section 7, bar 2 as amended 2026-10-27): THE DIRECT CALL ----
//
// "The writer calls the identity's actor directly after its answer, the queue is the durable backstop." Every event a
// publish SENT is held in the invocation's direct set (`owed_events_publish::hold_direct`, inside `publish_env`, the
// one function every owed frame of both Workers passes); the invocation's flush point hands the set here: the app
// layer's writers through [`schedule_direct`] (`wait_until` after the answer: the record door, the handfile batch,
// the pot-, hop- and tip-changed hooks, the word ingest) or [`direct_held`] inline (the scheduled word tick), the
// overlay's through `POST /internal/owed-direct` ([`direct_route`], its `APP_LAYER` binding, from its own flush
// points). Each event's subjects are read from its marks ([`plan_with`], no envelope expanded, no write), and each
// identity's actor is called with the event's kind, seq and write time ([`crate::owed_actor::apply_body`], no bound:
// a direct call always applies), all identities at once, each call under `DIRECT_CALL_WALL_MS`. A call that walls is
// counted `owed_actor_direct_walls_total`, one that faults `owed_actor_direct_faults_total`, every call
// `owed_actor_direct_calls_total`; none fails the write (the write committed and answered before). The queue message
// goes regardless; its delivery names the consumer's acknowledgement bound, so a delivery the direct call covered is a
// DUPE the actor answers from its seq check (`owed_actor::COVERED_SQL`). A `word` event carrying a frame is left to
// the consumer: its ingest is the tracker's own write (its marks do not exist until then), EXCEPT the MINED word's
// frame (FIX-974, bsv-low #974: the overlay's `/arc-ingest` push, `owed_actor::mined_word_event`), which the pass
// ingests through the port first (`DirectPort::ingest_frame`, the consumer's own `ingest_frame`) and then plans and
// calls as any direct event, so the collect-after-mined bar's sample is the first apply on the writer's road (the
// callback's `at` to the judgement) and never the queue's backlog; the consumer's later delivery of that event is a
// covered dupe or a redelivery, which samples nothing.
//
// FIX-956 (bsv-low #956) item 3: in PHASE C ONLY (`OWED_D1_JUDGE=false`: the actor is the one writer, its apply IS
// the judgement) a call the actor answered APPLIED acknowledges the marks of the games it names applied, up to the
// greatest generation of those games' marks read BEFORE the call (`owed_actor::phase_c_ack_statements`, the
// consumer's Phase C batch), in the pass's own batch after the answer; a mark written during the call stands. A walled
// or faulted call, a held answer, and every call in Phases A and B (the marks drive the consumer's D1 judge, whose
// rows are served) acknowledge nothing: the queue path stands.

/// The most events one direct pass calls (past it the queue alone carries the rest, counted in the report).
pub const DIRECT_EVENTS_MAX: usize = 64;

/// What one direct pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DirectReport {
    pub events: usize,
    pub calls: u64,
    pub walls: u64,
    pub faults: u64,
    pub held: u64,
    /// Events past [`DIRECT_EVENTS_MAX`], carrying a frame that is not the mined word's (or whose ingest faulted),
    /// or `txfacts`: the queue's alone.
    pub left_to_queue: usize,
    /// FIX-974 (bsv-low #974): the MINED word's frames this pass ingested and called (the collect-after-mined
    /// bar's road).
    pub mined_frames: u64,
    /// D2-ACTOR-B-FOLD-10 (item 8): the first fault's text (a wall's or a refusal's), for the operator's line.
    pub first_fault: Option<String>,
    /// FIX-956 item 3 (Phase C only): games whose marks the pass acknowledged after their apply landed.
    pub acked: u64,
    /// D2-ACTOR-F (bsv-low #959): events whose subjects and rows the publish carried (no marks read here).
    pub carried: u64,
    /// D2-ACTOR-F-FOLD-3: the carried filings this pass gave a closure ([`DirectPort::closure_read`], the pass's ONE
    /// statement), counted `owed_actor_direct_closures_total`.
    pub closures: u64,
    /// D2-ACTOR-F-FOLD-3: the pass's closure statement's time, when it read one (the direct class's `closure` stage).
    pub closure_ms: Option<u64>,
    /// D2-ACTOR-F-FOLD-3: the record calls the pass's closure made (0 or 1).
    pub closure_calls: u64,
    /// D2-ACTOR-F-FOLD-4 (the third delta lens's LOW-2): the pass's closure read that faulted or passed its wall (0 or
    /// 1), counted `owed_actor_direct_closure_faults_total`; its text for the operator's line.
    pub closure_faults: u64,
    pub closure_fault: Option<String>,
    /// FOLD-4: the filings whose closure the read answered but `closure_for` declined (past `CLOSURE_MAX_BYTES`),
    /// counted `owed_actor_direct_closure_declines_total`.
    pub closure_declines: u64,
}

/// D2-ACTOR-B-FOLD-10 (item 8): the direct path's fault line is logged at most once in this many ms an isolate.
pub const DIRECT_FAULT_LOG_EVERY_MS: u64 = 60_000;
thread_local! {
    static DIRECT_FAULT_LOGGED: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}
/// PURE: the operator's line for a pass's faults (`[owed-actor] direct fault: <text>`), when one is due at `now_ms`
/// after the last line at `last_ms` (once a minute an isolate).
pub fn direct_fault_line(
    report: &DirectReport,
    now_ms: u64,
    last_ms: Option<u64>,
) -> Option<String> {
    let text = report.first_fault.as_deref()?;
    if last_ms.is_some_and(|l| now_ms.saturating_sub(l) < DIRECT_FAULT_LOG_EVERY_MS) {
        return None;
    }
    Some(format!(
        "[owed-actor] direct fault: {text} ({} of {} call(s) walled, {} faulted)",
        report.walls, report.calls, report.faults
    ))
}
/// The fault line, logged through the isolate's once-a-minute gate.
pub fn log_direct_fault(report: &DirectReport, now_ms: u64, log: impl Fn(&str)) {
    let last = DIRECT_FAULT_LOGGED.with(std::cell::Cell::get);
    if let Some(line) = direct_fault_line(report, now_ms, last) {
        DIRECT_FAULT_LOGGED.with(|c| c.set(Some(now_ms)));
        log(&line);
    }
}

thread_local! {
    static CLOSURE_FAULT_LOGGED: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}
/// PURE (D2-ACTOR-F-FOLD-4, the third delta lens's LOW-2): the operator's line for a pass's faulted closure read
/// (`[owed-actor] direct closure fault: <text>`), when one is due at `now_ms` after the last at `last_ms` (once a
/// minute an isolate, [`DIRECT_FAULT_LOG_EVERY_MS`]).
pub fn closure_fault_line(
    report: &DirectReport,
    now_ms: u64,
    last_ms: Option<u64>,
) -> Option<String> {
    let text = report.closure_fault.as_deref()?;
    if last_ms.is_some_and(|l| now_ms.saturating_sub(l) < DIRECT_FAULT_LOG_EVERY_MS) {
        return None;
    }
    Some(format!(
        "[owed-actor] direct closure fault: {text} (the pass's filings read their own miss)"
    ))
}
/// The closure fault line, logged through its own once-a-minute gate.
pub fn log_closure_fault(report: &DirectReport, now_ms: u64, log: impl Fn(&str)) {
    let last = CLOSURE_FAULT_LOGGED.with(std::cell::Cell::get);
    if let Some(line) = closure_fault_line(report, now_ms, last) {
        CLOSURE_FAULT_LOGGED.with(|c| c.set(Some(now_ms)));
        log(&line);
    }
}

/// An event's subjects: each identity with its games.
pub type Subjects = Vec<(String, Vec<String>)>;

/// One coalesced direct call (NEW5-L3): an identity's games over every event of the pass that names it, in first-
/// seen order, under the FIRST event's key, the earliest write time, and the other events' keys (`also`).
#[derive(Debug, Clone, Default)]
struct Coalesced {
    games: Vec<String>,
    first: Option<crate::owed_actor::EventCtx>,
    at_ms: i64,
    mined: bool,
    also: Vec<(String, i64)>,
    /// D2-ACTOR-E: the txids every event's write named (`None` once one names no row: the whole closure).
    txids: Option<Vec<String>>,
    /// D2-ACTOR-F: the rows every event carried for the identity (`None` once one carries none).
    carried: Option<crate::owed_actor::carried::Carried>,
}

/// PURE (NEW5-L3, D2-ACTOR-B-FOLD-10): the pass's events' subjects merged into ONE call body an identity (per
/// [`crate::owed_actor::GAMES_PER_APPLY`] games), never one call an event: a write of k events naming one identity
/// (a JOIN's pot, party, hop and word pairs) queued k whole applies in that actor's turn, each later one covered by the
/// first. Each body carries `class: "direct"` (the object notes its own visibility under it, NEW5-L5).
pub fn coalesce_direct(
    subjects: Vec<(crate::owed_actor::EventCtx, Subjects)>,
    tip: Option<u64>,
) -> Vec<Value> {
    let mut order: Vec<String> = Vec::new();
    let mut by: BTreeMap<String, Coalesced> = BTreeMap::new();
    for (e, subjects) in subjects {
        for (identity, games) in subjects {
            let id = identity.to_ascii_lowercase();
            let c = by.entry(id.clone()).or_insert_with(|| {
                order.push(id.clone());
                Coalesced::default()
            });
            for g in games {
                if !c.games.iter().any(|x| x.eq_ignore_ascii_case(&g)) {
                    c.games.push(g);
                }
            }
            c.mined |= e.mined;
            c.carried = match (c.first.is_none(), c.carried.take(), e.carried.get(&id)) {
                (true, _, own) => own.cloned(),
                (false, Some(mut held), Some(own)) => {
                    held.merge(own.clone());
                    Some(held)
                }
                _ => None,
            };
            c.txids = match (c.first.is_none(), c.txids.take(), &e.txids) {
                (true, _, t) => t.clone(),
                (false, Some(mut t), Some(more)) => {
                    t.extend(more.iter().cloned());
                    t.sort();
                    t.dedup();
                    Some(t)
                }
                _ => None,
            };
            match &c.first {
                None => {
                    c.at_ms = e.at_ms;
                    c.first = Some(e.clone());
                }
                Some(f) => {
                    c.at_ms = c.at_ms.min(e.at_ms);
                    let key = (e.kind.clone(), e.seq);
                    if (f.kind.as_str(), f.seq) != (e.kind.as_str(), e.seq)
                        && !c.also.contains(&key)
                    {
                        c.also.push(key);
                    }
                }
            }
        }
    }
    let mut bodies = Vec::new();
    for id in order {
        let c = &by[&id];
        let Some(first) = &c.first else { continue };
        let e = crate::owed_actor::EventCtx {
            kind: first.kind.clone(),
            seq: first.seq,
            at_ms: c.at_ms,
            mined: c.mined,
            txids: c.txids.clone(),
            also: Vec::new(),
            by_identity: Default::default(),
            carried: Default::default(),
        };
        for chunk in c.games.chunks(crate::owed_actor::GAMES_PER_APPLY) {
            let mut b = crate::owed_actor::apply_body(&id, chunk, &e, tip);
            b["class"] = json!("direct");
            // D2-ACTOR-F: the rows the identity's events carried (every one of them), for the actor's apply with no
            // record read. The lens's NIT-2: each chunk's body carries the chunk's own items (an item names its
            // games, as `apply_burst` filters), so an identity past one chunk keeps the carried path in each.
            if let Some(carried) = chunk_carried(c.carried.as_ref(), chunk) {
                b["carried"] = json!(carried);
            }
            if !c.also.is_empty() {
                b["also"] = json!(c.also);
            }
            bodies.push(b);
        }
    }
    bodies
}

/// PURE (the lens's NIT-2): the carried rows of one chunk's body: the items naming one of the chunk's games, every
/// mark and closure kept (the apply records the marks of its own games); `None` when no item names one.
pub fn chunk_carried(
    carried: Option<&crate::owed_actor::carried::Carried>,
    chunk: &[String],
) -> Option<crate::owed_actor::carried::Carried> {
    let mut c = carried?.clone();
    c.items.retain(|i| {
        i.games
            .iter()
            .any(|g| chunk.iter().any(|x| x.eq_ignore_ascii_case(g)))
    });
    (!c.items.is_empty()).then_some(c)
}

/// PURE: a direct call's error is its WALL (`object::call_apply`'s "no answer in" words), else a fault.
pub fn is_wall(e: &str) -> bool {
    e.contains("no answer in")
}

/// The direct pass's port: the marks' read and the actor's call.
#[allow(async_fn_in_trait)]
pub trait DirectPort {
    /// The event's subjects from its marks (no write).
    async fn subjects(&self, ev: &OwedEvent) -> Result<Vec<(String, Vec<String>)>, String>;
    /// One apply body to the identity's actor, under the direct call's wall.
    async fn call(&self, body: Value) -> Result<Value, String>;
    /// FIX-974 (bsv-low #974): the ingest of the MINED word's frame (the tracker's own write, whose trigger marks the
    /// pair this pass then plans) before its direct call. A port that ingests no frame answers `Err`, and the frame
    /// stays the consumer's.
    async fn ingest_frame(&self, _frame: &Value) -> Result<(), String> {
        Err("this port ingests no frame".into())
    }
    /// FIX-956 item 3: does a landed direct apply acknowledge its marks? Phase C only (the D1 judge OFF: the actor is
    /// the one writer, its apply IS the judgement). In Phases A and B the marks drive the consumer's D1 judge, whose
    /// rows are served, so the direct call acknowledges nothing there.
    fn acks(&self) -> bool {
        false
    }
    /// The greatest generation of `games`' marks, read BEFORE the call (the acknowledgement's bound).
    async fn bound(&self, _identity: &str, _games: &[String]) -> Result<i64, String> {
        Ok(0)
    }
    /// Run the acknowledgement's batch.
    async fn ack(&self, _stmts: Vec<Stmt>) -> Result<(), String> {
        Ok(())
    }
    /// D2-ACTOR-F-FOLD-3 (the second delta lens's MEDIUM-1, the captain's ruling): the pass's ONE closure statement
    /// (`owed_actor::pass_closure_statement`: every family's rows of the union of the pass's carried filings' keys,
    /// one snapshot), read after the writer's answer in one round trip; its answer row, or the fault (or a port that
    /// reads none: each actor's miss round then reads the record once).
    async fn closure_read(&self, _stmt: Stmt) -> Result<Value, String> {
        Err("this port reads no closure".into())
    }
    /// D2-ACTOR-F-FOLD-4 (the third delta lens's LOW-1): a timer of `ms` (the closure read's wall). A port with none
    /// never answers (no wall).
    async fn sleep_ms(&self, _ms: u64) {
        futures_util::future::pending::<()>().await
    }
    /// The port's clock (the closure stage's time); 0 for a port with none.
    fn now_ms(&self) -> u64 {
        0
    }
}

/// FIX-956 item 3: the bound of [`DirectPort::bound`] over a [`SqlPort`] (`owed_judge::MARKS_SQL`, as the consumer's
/// Phase C delivery reads it).
pub async fn direct_bound(db: &impl SqlPort, identity: &str, games: &[String]) -> Result<i64, String> {
    let marks = rows(
        db,
        owed_judge::MARKS_SQL,
        vec![
            Bind::Text(identity.to_ascii_lowercase()),
            Bind::Text(json!(games).to_string()),
        ],
    )
    .await?;
    Ok(marks
        .iter()
        .filter_map(|m| m["generation"].as_i64())
        .max()
        .unwrap_or(0))
}

/// PURE (FIX-956 item 3): the games an actor's answer names APPLIED among `games` (a held answer, a fault: none).
pub fn applied_games(answer: &Value, games: &[String]) -> Vec<String> {
    if crate::owed_actor::is_held(answer) {
        return Vec::new();
    }
    answer["appliedGames"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|g| g.as_str().map(str::to_ascii_lowercase))
                .filter(|g| games.iter().any(|x| x.eq_ignore_ascii_case(g)))
                .collect()
        })
        .unwrap_or_default()
}

/// THE DIRECT PASS over a port: every event's subjects, then every identity's call at once, counted; answers are
/// noted into the `direct` class (an applied first apply samples the rows' visibility once).
pub async fn direct_with(
    port: &impl DirectPort,
    events: Vec<OwedEvent>,
    tip: Option<u64>,
) -> DirectReport {
    direct_with_class(port, events, tip, "direct").await
}

/// [`direct_with`] under a bars `class`: `direct` for a writer's pass; [`crate::owed_actor::SWEEP_CLASS`] for the drain's
/// sweep (D2-ACTOR-B-FOLD-16, the captain's item 5), whose bodies carry it so the object samples nothing, and whose
/// answers this pass notes into no bar.
pub async fn direct_with_class(
    port: &impl DirectPort,
    events: Vec<OwedEvent>,
    tip: Option<u64>,
    class: &'static str,
) -> DirectReport {
    let mut report = DirectReport::default();
    let mut kept: Vec<(OwedEvent, Subjects)> = Vec::new();
    for (n, ev) in events.into_iter().enumerate() {
        if n >= DIRECT_EVENTS_MAX || ev.source == owed_event::TXFACTS {
            report.left_to_queue += 1;
            continue;
        }
        if let Some(frame) = ev.frame() {
            // FIX-974 (bsv-low #974): the MINED word's frame (the overlay's `/arc-ingest` push) is ingested here,
            // the tracker's write whose trigger marks the pair, then planned and called as any direct event, so its
            // first apply is the writer's road and its sample the time since the callback, never the queue's lag.
            // Every other frame stays the consumer's (its ingest is the tracker's write; its marks do not exist until
            // then), and a frame whose ingest faults is the consumer's too (the queue message carries it).
            if !crate::owed_actor::mined_word_event(&ev) || port.ingest_frame(frame).await.is_err()
            {
                report.left_to_queue += 1;
                continue;
            }
            report.mined_frames += 1;
        }
        report.events += 1;
        // D2-ACTOR-F (bsv-low #959): an event that carries its row names its subjects too (its pair's marks, read in
        // the publish's own statement): no D1 read here. Any other event reads its marks as before.
        let subjects = match crate::owed_actor::carried::carried_subjects(&ev.facts) {
            Some(s) => s,
            None => match port.subjects(&ev).await {
                Ok(s) => s,
                Err(_) => {
                    report.left_to_queue += 1;
                    continue;
                }
            },
        };
        if ev.facts.get("payload").is_some() {
            report.carried += 1;
        }
        kept.push((ev, subjects));
    }
    // D2-ACTOR-F-FOLD-3 (the second delta lens's MEDIUM-1 and MEDIUM-2, the captain's ruling): a closure only for a
    // carried FILING naming a key beyond its own (`owed_actor::closure_wanted`), and the pass's closures in ONE
    // statement over the union of their keys (the first `PASS_CLOSURE_KEYS_MAX`), one round trip; the drain's sweep
    // pass reads none (a swept event's actor reads its own miss once).
    // D2-ACTOR-F-FOLD-4 (the third delta lens's LOW-1): the events that want no closure are called AT ONCE; the closure
    // read runs beside them, raced against `DIRECT_CALL_WALL_MS` (past it the filings go on with none: each actor
    // reads its own miss once, as on a fault), and the filings' bodies follow it.
    let wanted: Vec<Option<std::collections::BTreeSet<String>>> = kept
        .iter()
        .map(|(ev, _)| {
            if class == crate::owed_actor::SWEEP_CLASS {
                None
            } else {
                crate::owed_actor::closure_wanted(ev)
            }
        })
        .collect();
    let mut filings: Vec<((OwedEvent, Subjects), std::collections::BTreeSet<String>)> = Vec::new();
    let mut rest: Vec<(OwedEvent, Subjects)> = Vec::new();
    for (k, w) in kept.into_iter().zip(wanted) {
        match w {
            Some(keys) => filings.push((k, keys)),
            None => rest.push(k),
        }
    }
    let ctx_of = |v: &[(OwedEvent, Subjects)]| -> Vec<_> {
        v.iter()
            .map(|(ev, subjects)| (crate::owed_actor::EventCtx::of(ev), subjects.clone()))
            .collect()
    };
    let rest_bodies = coalesce_direct(ctx_of(&rest), tip);
    // the filings' bodies are coalesced after their closure (it rides each event's facts); their count, for the pass's
    // fan-out, does not depend on it (one body an identity and chunk)
    let filing_events: Vec<(OwedEvent, Subjects)> = filings.iter().map(|(k, _)| k.clone()).collect();
    let fan_out = (rest_bodies.len() + coalesce_direct(ctx_of(&filing_events), tip).len()) as u32;
    let immediate = call_bodies(port, rest_bodies, class, fan_out);
    let after_closure = async {
        let mut step = ClosureStep::default();
        let union: std::collections::BTreeSet<String> =
            filings.iter().flat_map(|(_, keys)| keys.iter().cloned()).collect();
        let n_filings = filings.len();
        if let Some((stmt, read)) = crate::owed_actor::pass_closure_statement(&union, n_filings) {
            let started = port.now_ms();
            let row = {
                let read_fut = std::pin::pin!(port.closure_read(stmt));
                let wall = std::pin::pin!(port.sleep_ms(crate::owed_actor::DIRECT_CALL_WALL_MS));
                match futures_util::future::select(read_fut, wall).await {
                    futures_util::future::Either::Left((r, _)) => r,
                    futures_util::future::Either::Right(_) => Err(format!(
                        "the closure read passed its wall ({} ms)",
                        crate::owed_actor::DIRECT_CALL_WALL_MS
                    )),
                }
            };
            let ms = port.now_ms().saturating_sub(started);
            step.calls = 1;
            step.ms = Some(ms);
            match row {
                Ok(row) => {
                    for ((ev, _), keys) in filings.iter_mut() {
                        match crate::owed_actor::closure_for(keys, &read, &row) {
                            Some(c) => {
                                ev.facts["closure"] = c;
                                ev.facts["closureMs"] = json!(ms);
                                step.closures += 1;
                            }
                            None => step.declines += 1,
                        }
                    }
                }
                Err(e) => {
                    step.faults = 1;
                    step.fault = Some(e);
                }
            }
        }
        let events: Vec<(OwedEvent, Subjects)> = filings.into_iter().map(|(k, _)| k).collect();
        let bodies = coalesce_direct(ctx_of(&events), tip);
        (step, call_bodies(port, bodies, class, fan_out).await)
    };
    let (now, (step, later)) = futures_util::future::join(immediate, after_closure).await;
    report.closure_calls = step.calls;
    report.closure_ms = step.ms;
    report.closures = step.closures;
    report.closure_declines = step.declines;
    report.closure_faults = step.faults;
    report.closure_fault = step.fault;
    for o in [now, later] {
        report.calls += o.calls;
        report.walls += o.walls;
        report.faults += o.faults;
        report.held += o.held;
        report.acked += o.acked;
        if report.first_fault.is_none() {
            report.first_fault = o.first_fault;
        }
    }
    report
}

/// FOLD-4: the pass's closure step (its one statement, raced against the direct wall).
#[derive(Debug, Default)]
struct ClosureStep {
    calls: u64,
    ms: Option<u64>,
    closures: u64,
    declines: u64,
    faults: u64,
    fault: Option<String>,
}

/// FOLD-4: what a set of direct bodies' calls did.
#[derive(Debug, Default)]
struct CallsDone {
    calls: u64,
    walls: u64,
    faults: u64,
    held: u64,
    acked: u64,
    first_fault: Option<String>,
}

/// The direct calls of `bodies` at once (`join_all`), each body stamped with the pass's class and fan-out; in Phase C
/// each body's acknowledgement bound read BEFORE its call (FIX-956 item 3) and its applied games acknowledged after.
async fn call_bodies(
    port: &impl DirectPort,
    mut bodies: Vec<Value>,
    class: &'static str,
    fan_out: u32,
) -> CallsDone {
    let mut done = CallsDone::default();
    // FIX-983 (bsv-low #983): the pass's fan-out rides each body; at `FLOOD_CONCURRENCY` or more the object samples
    // the call into the stress class (`flood`), apart from the warm hand's `direct`.
    for b in &mut bodies {
        b["class"] = json!(class);
        b["concurrent"] = json!(fan_out);
    }
    done.calls = bodies.len() as u64;
    // FIX-956 item 3: in Phase C each body's acknowledgement bound, read BEFORE its call (a mark written after it is
    // left for the queue's delivery)
    let mut bounds: Vec<Option<(String, Vec<String>, i64)>> = Vec::with_capacity(bodies.len());
    for b in &bodies {
        let id = b["identity"].as_str().unwrap_or_default().to_owned();
        let games: Vec<String> = b["games"]
            .as_array()
            .map(|a| a.iter().filter_map(|g| g.as_str().map(str::to_owned)).collect())
            .unwrap_or_default();
        // D2-ACTOR-F: a carried body acknowledges nothing (its apply covers nothing; the queue's delivery is the
        // narrowed read that covers the event and acknowledges it), so no bound is read for it.
        bounds.push(if port.acks() && b.get("carried").is_none() {
            port.bound(&id, &games).await.ok().map(|seen| (id, games, seen))
        } else {
            None
        });
    }
    let answers = futures_util::future::join_all(bodies.into_iter().map(|b| port.call(b))).await;
    for (answer, bound) in answers.into_iter().zip(bounds) {
        if let (Ok(a), Some((id, games, seen))) = (&answer, &bound) {
            let applied = applied_games(a, games);
            if !applied.is_empty()
                && port
                    .ack(crate::owed_actor::phase_c_ack_statements(id, &applied, *seen))
                    .await
                    .is_ok()
            {
                done.acked += applied.len() as u64;
            }
        }
        match answer {
            // D2-ACTOR-B-FOLD-7: a held answer is neither a fault nor a sample (the actor counts it).
            Ok(a) if crate::owed_actor::is_held(&a) => done.held += 1,
            Ok(_) if class == crate::owed_actor::SWEEP_CLASS => {}
            Ok(a) => crate::owed_actor::note_answer_as(
                crate::owed_actor::bars_class(Some("direct"), Some(fan_out)),
                &a,
            ),
            Err(e) => {
                if is_wall(&e) {
                    done.walls += 1;
                } else {
                    done.faults += 1;
                }
                done.first_fault.get_or_insert(e);
            }
        }
    }
    done
}

/// The Worker's direct port: D1's marks and the actor's binding.
pub(crate) struct WorkerDirect {
    pub(crate) env: worker::Env,
    pub(crate) sql: crate::routes::D1Sql,
}
impl DirectPort for WorkerDirect {
    async fn subjects(&self, ev: &OwedEvent) -> Result<Vec<(String, Vec<String>)>, String> {
        Ok(plan_with(&self.sql, ev, false).await?.subjects())
    }
    async fn call(&self, body: Value) -> Result<Value, String> {
        crate::owed_actor::object::call_apply(
            &self.env,
            &body,
            crate::owed_actor::DIRECT_CALL_WALL_MS,
        )
        .await
    }
    /// FIX-974: the consumer's own ingest (`ingest_frame`: the tracker's write, the `word` note forgotten).
    async fn ingest_frame(&self, frame: &Value) -> Result<(), String> {
        ingest_frame(&self.env, frame).await
    }
    fn acks(&self) -> bool {
        !crate::owed_actor::d1_judge_on(
            self.env
                .var(crate::owed_actor::D1_JUDGE_FLAG)
                .ok()
                .map(|v| v.to_string()),
        )
    }
    async fn bound(&self, identity: &str, games: &[String]) -> Result<i64, String> {
        direct_bound(&self.sql, identity, games).await
    }
    async fn ack(&self, stmts: Vec<Stmt>) -> Result<(), String> {
        self.sql.run_batch(stmts).await.map(|_| ())
    }
    async fn closure_read(&self, stmt: Stmt) -> Result<Value, String> {
        crate::owed_actor::read_pass_closure(&self.sql, stmt).await
    }
    async fn sleep_ms(&self, ms: u64) {
        worker::Delay::from(std::time::Duration::from_millis(ms)).await
    }
    fn now_ms(&self) -> u64 {
        worker::Date::now().as_millis()
    }
}

/// The direct pass over the Worker's bindings, its counters and the `direct` bars flushed. Nothing without the actor's
/// binding (the queue alone).
pub async fn direct_events(env: &worker::Env, events: Vec<OwedEvent>) -> DirectReport {
    direct_events_as(env, events, "direct").await
}

/// [`direct_events`] under a bars class ([`direct_with_class`]).
pub async fn direct_events_as(
    env: &worker::Env,
    events: Vec<OwedEvent>,
    class: &'static str,
) -> DirectReport {
    if events.is_empty() || env.durable_object(crate::owed_actor::BINDING).is_err() {
        return DirectReport::default();
    }
    let Ok(sql) = crate::routes::D1Sql::of(env) else {
        return DirectReport::default();
    };
    // NEW5-M1: a block's `tip` events read the tip fresh here too, as the consumer reads them.
    let tip = tip_memoised(env, direct_reads_fresh(&events)).await;
    let port = WorkerDirect {
        env: env.clone(),
        sql,
    };
    let report = direct_with_class(&port, events, tip, class).await;
    // item 8 (D2-ACTOR-B-FOLD-10): the first fault's own text, once a minute an isolate.
    log_direct_fault(&report, worker::Date::now().as_millis(), |l| {
        worker::console_warn!("{l} (the queue's messages are their backstop)");
    });
    // FOLD-4 (the third delta lens's LOW-2): the closure read's fault, once a minute an isolate.
    log_closure_fault(&report, worker::Date::now().as_millis(), |l| {
        worker::console_warn!("{l}");
    });
    let deltas: Vec<(String, u64)> = [
        (crate::owed_actor::COUNTERS[3], report.calls),
        (crate::owed_actor::COUNTERS[4], report.faults),
        (crate::owed_actor::COUNTERS[25], report.walls),
        (crate::owed_actor::COUNTERS[32], report.closures),
        (crate::owed_actor::COUNTERS[33], report.closure_faults),
        (crate::owed_actor::COUNTERS[34], report.closure_declines),
        (publish::COUNTER_DIRECT_ACKED, report.acked),
    ]
    .into_iter()
    .filter(|(_, d)| *d > 0)
    .map(|(n, d)| (n.to_string(), d))
    .collect();
    port.sql.counters(deltas).await;
    // MEDIUM-7: the direct class's samples into the fleet's durable bars.
    crate::owed_actor::flush_bars(&port.sql, worker::Date::now().as_millis(), false).await;
    report
}

/// The invocation's held direct set, called inline (the scheduled word tick: "inline on the scheduled drain").
pub async fn direct_held(env: &worker::Env) -> DirectReport {
    direct_events(env, publish::take_direct()).await
}

/// The invocation's held direct set, called after the answer (`wait_until`): never awaited by the write.
pub fn schedule_direct(env: &worker::Env, ctx: &worker::Context) {
    let events = publish::take_direct();
    if events.is_empty() {
        return;
    }
    let env = env.clone();
    ctx.wait_until(async move {
        direct_events(&env, events).await;
    });
}

/// The most events one `/internal/owed-direct` body carries (the overlay's held set is at most
/// `owed_events_publish::DIRECT_HELD_MAX`; it chunks at this).
pub const DIRECT_BODY_MAX: usize = DIRECT_EVENTS_MAX;

/// PURE: the events of an `/internal/owed-direct` body (`{"events": [<encoded event>, ...]}`, each the queue's own
/// message text or its object), or why it is refused.
pub fn parse_direct_body(raw: &[u8]) -> Result<Vec<OwedEvent>, &'static str> {
    let v: Value = serde_json::from_slice(raw).map_err(|_| "body must be JSON")?;
    let list = v["events"].as_array().ok_or("events must be an array")?;
    if list.len() > DIRECT_BODY_MAX {
        return Err("events names more than one body carries");
    }
    list.iter()
        .map(|e| {
            let text = match e {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            OwedEvent::decode(&text).map_err(|_| "an event does not read")
        })
        .collect()
}

/// `POST /internal/owed-direct` (bearer `INTERNAL_TOKEN`): the overlay's held direct set (D2-ACTOR-D). Answered 202 at
/// once; the calls run after the answer (`wait_until`), so the overlay's flush waits on the hand-off alone.
pub async fn direct_route(
    mut req: worker::Request,
    env: &worker::Env,
    ctx: &worker::Context,
) -> worker::Result<worker::Response> {
    if !crate::internal_events::internal_bearer_ok(&req, env) {
        return worker::Response::error("unauthorized", 401);
    }
    let raw = req.bytes().await?;
    let events = match parse_direct_body(&raw) {
        Ok(e) => e,
        Err(why) => return worker::Response::error(why, 400),
    };
    let n = events.len();
    let env = env.clone();
    ctx.wait_until(async move {
        direct_events(&env, events).await;
    });
    Ok(worker::Response::from_json(&json!({"accepted": n}))?.with_status(202))
}
