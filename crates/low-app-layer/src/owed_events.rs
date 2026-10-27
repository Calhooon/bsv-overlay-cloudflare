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
//! THE SWEEP ([`sweep`], on the belt's 120 s alarm): the marks no consumer acknowledged within one tick are
//! re-published as events, oldest first past a rotating cursor, 64 pairs a tick: `owed_dirty` is the durable intent
//! ledger, so a publish cancelled or refused is never a lost event. With no queue binding the drain judges as before.
use crate::owed_events_publish as publish;
use crate::owed_judge::{self, JudgeDb};
use crate::pot_hook::{Bind, SqlPort, Stmt};
use overlay_discovery::owed_event::{self, OwedEvent};
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// MEDIUM-1 (the D2-ACTOR-A lens): the `owed-events` consumer's `max_concurrency` in every app-layer render: ONE
/// judge at a time fleet-wide, the serialiser of the judge per identity until lane B's per-identity actor. Two
/// consumers judging one identity can commit the first's stale rows over the second's with every mark acknowledged
/// (pinned by `d2_actor_a_fold_m1_*`).
pub const CONSUMER_MAX_CONCURRENCY: u32 = 1;

/// The pair's marks, oldest generation first (`idx_owed_dirty_source_key`; the source filtered in the index's row).
pub const ROUTE_MARKS_SQL: &str = "SELECT identity,gameId,generation FROM owed_dirty INDEXED BY idx_owed_dirty_source_key WHERE sourceKey=?2 AND source=?1 ORDER BY generation LIMIT ?3";
/// An identity route's marks, oldest first (`idx_owed_dirty_identity_marked`).
pub const ROUTE_IDENTITY_SQL: &str = "SELECT identity,gameId,generation FROM owed_dirty INDEXED BY idx_owed_dirty_identity_marked WHERE identity=?1 ORDER BY markedAtMs,generation LIMIT ?2";
/// Mark rows one message reads (a subject can hold several marks).
pub const ROUTE_READ_LIMIT: usize = 4 * owed_event::MAX_GAMES_PER_MESSAGE;

/// A mark is UN-ACKED once it has waited this long (one belt tick) with no consumer acknowledging it.
pub const SWEEP_AFTER_MS: i64 = crate::owed_belt::OWED_BELT_TICK_MS;
/// The pairs one sweep tick re-publishes.
pub const SWEEP_PAIRS_PER_TICK: usize = 64;
/// The mark rows one sweep tick reads past its cursor (several marks share a pair).
pub const SWEEP_READ_LIMIT: usize = 256;
/// The sweep's walk, oldest first past the cursor `(?2, ?3)`, only marks older than `?1` (`idx_owed_dirty_backlog`).
pub const SWEEP_SQL: &str = "SELECT generation,source,sourceKey,markedAtMs FROM owed_dirty INDEXED BY idx_owed_dirty_backlog WHERE markedAtMs<?1 AND (markedAtMs>?2 OR (markedAtMs=?2 AND generation>?3)) ORDER BY markedAtMs,generation LIMIT ?4";
/// The sweep's cursor row.
pub const SWEEP_CURSOR_READ_SQL: &str =
    "SELECT cursor FROM owed_belt_state WHERE name='owed-events:sweep'";
/// Write the sweep's cursor.
pub const SWEEP_CURSOR_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('owed-events:sweep',?1,?2) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// MEDIUM-8: the consumer's batch row: the oldest message's enqueue age (a Worker cannot read its queue's backlog;
/// the pass-meter precedent), served on `/health.owedEvents` and the overlay's `/health/invariants.owedEvents`.
pub const CONSUMER_ROW_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('owed-events:consumer',?1,?2) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// The served ages and totals, one batch of key reads.
pub const HEALTH_COUNTERS_SQL: &str = "SELECT name,value FROM ops_counters WHERE name IN ('owed_events_published_total','owed_events_consumed_total','owed_events_acked_marks_total','owed_events_dlq_total','owed_events_publish_faults_total','owed_events_judge_faults_total','owed_events_swept_total','owed_events_fault_held_total','owed_events_sweep_held_total')";
/// The consumer's row.
pub const HEALTH_CONSUMER_SQL: &str =
    "SELECT cursor,tickedAtMs FROM owed_belt_state WHERE name='owed-events:consumer'";
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
/// The parked letters by status (at most [`publish::DEAD_LETTERS_MAX`] rows), for `/health.owedEvents`.
pub const DEAD_LETTER_HEALTH_SQL: &str =
    "SELECT status,COUNT(*) AS n FROM owed_event_dead_letters GROUP BY status";

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
/// The memo's served figures at `?1` (now): the rows, the pairs held now, the most consecutive faults (a range of
/// the state's primary key).
pub const FAULT_MEMO_HEALTH_SQL: &str = "SELECT COUNT(*) AS rows,COALESCE(SUM(json_extract(cursor,'$.heldUntilMs')>?1),0) AS held,MAX(json_extract(cursor,'$.faults')) AS maxFaults FROM owed_belt_state WHERE name>='owed-events:fault:' AND name<'owed-events:fault;'";
/// The memo's newest fault (the cause an operator reads first).
pub const FAULT_MEMO_LAST_SQL: &str = "SELECT name,cursor FROM owed_belt_state WHERE name>='owed-events:fault:' AND name<'owed-events:fault;' ORDER BY tickedAtMs DESC LIMIT 1";

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
    let name = fault_memo_name(source, key);
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
        .and_then(|c| c["faults"].as_i64())
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

/// The statements a landed judge of `(source, key)` adds to its consumer's batch row: the memo cleared, the pair's
/// dead letter resolved (two primary-key deletes).
pub fn resolved_statements(source: &str, key: &str) -> Vec<Stmt> {
    vec![
        Stmt {
            sql: FAULT_MEMO_CLEAR_SQL.into(),
            binds: vec![Bind::Text(fault_memo_name(source, key))],
        },
        Stmt {
            sql: DEAD_LETTER_RESOLVE_SQL.into(),
            binds: vec![Bind::Text(source.into()), Bind::Text(key.into())],
        },
    ]
}

/// PURE: the pairs the sweep re-publishes, the held ones left out (`memos`: the memo rows of `pairs`), and how many
/// were held.
pub fn unheld(pairs: Vec<Pair>, memos: &[Value], now_ms: i64) -> (Vec<Pair>, usize) {
    let held_names: std::collections::BTreeSet<&str> = memos
        .iter()
        .filter(|r| {
            r["cursor"]
                .as_str()
                .and_then(|c| serde_json::from_str::<Value>(c).ok())
                .and_then(|c| c["heldUntilMs"].as_i64())
                .is_some_and(|until| until > now_ms)
        })
        .filter_map(|r| r["name"].as_str())
        .collect();
    let before = pairs.len();
    let kept: Vec<Pair> = pairs
        .into_iter()
        .filter(|(s, k)| !held_names.contains(fault_memo_name(s, k).as_str()))
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
}

/// THE ONE JUDGE CALL of the consumer (lane B replaces its body with the actor's delivery): one identity's games, at
/// most `owed_judge::OWED_JUDGE_SUBJECTS_PER_INVOCATION` per call.
pub async fn deliver(
    db: &impl JudgeDb,
    identity: &str,
    games: &[String],
) -> Result<owed_judge::JudgeReport, String> {
    owed_judge::judge_subjects(db, identity, games, "owed-events").await
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

/// HIGH-1: route one event through the marks its source row made and judge their subjects (see the module doc).
pub async fn route(db: &impl JudgeDb, ev: &OwedEvent) -> Result<Routed, String> {
    if ev.source == owed_event::TXFACTS {
        return Ok(Routed::default());
    }
    let limit = ROUTE_READ_LIMIT as i64;
    let mut marks = if ev.source == owed_event::ROUTE_IDENTITY {
        rows(
            db,
            ROUTE_IDENTITY_SQL,
            vec![Bind::Text(ev.source_key.clone()), Bind::Int(limit)],
        )
        .await?
    } else {
        rows(
            db,
            ROUTE_MARKS_SQL,
            vec![
                Bind::Text(ev.source.clone()),
                Bind::Text(ev.source_key.clone()),
                Bind::Int(limit),
            ],
        )
        .await?
    };
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
    for (identity, generation) in &envelopes {
        owed_judge::expand_wanted(db, identity, *generation).await?;
        let expanded = rows(
            db,
            ROUTE_IDENTITY_SQL,
            vec![Bind::Text(identity.clone()), Bind::Int(limit)],
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
    let mut out = Routed {
        more,
        ..Routed::default()
    };
    for identity in order {
        let list = &games[&identity];
        for chunk in list.chunks(owed_judge::OWED_JUDGE_SUBJECTS_PER_INVOCATION) {
            let report = deliver(db, &identity, chunk).await?;
            out.subjects += report.subjects;
            out.marks += chunk
                .iter()
                .map(|g| {
                    held.get(&(identity.clone(), g.clone()))
                        .copied()
                        .unwrap_or(0)
                })
                .sum::<usize>();
        }
    }
    Ok(out)
}

/// One `(source, sourceKey)` pair the sweep re-publishes.
pub type Pair = (String, String);

/// PURE: the sweep's choice over the rows read past its cursor: up to [`SWEEP_PAIRS_PER_TICK`] distinct pairs, oldest
/// first, and the cursor to save (`None`: the walk reached the end of the backlog, start again next tick).
pub fn sweep_choice(
    rows: &[Value],
    read_limit: usize,
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
            if pairs.len() >= SWEEP_PAIRS_PER_TICK {
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

/// HIGH-2's second belt: the pairs whose marks waited past one tick, oldest first past the cursor, and the cursor
/// saved. The caller publishes them ([`sweep_publish`]).
pub async fn sweep(db: &impl SqlPort, now_ms: i64) -> Result<Vec<(String, String)>, String> {
    let cursor = rows(db, SWEEP_CURSOR_READ_SQL, vec![]).await?;
    let (at, g) = cursor
        .first()
        .and_then(|r| r["cursor"].as_str())
        .and_then(|c| serde_json::from_str::<Value>(c).ok())
        .map(|c| (c["at"].as_i64().unwrap_or(-1), c["g"].as_i64().unwrap_or(0)))
        .unwrap_or((-1, 0));
    let read = rows(
        db,
        SWEEP_SQL,
        vec![
            Bind::Int(now_ms - SWEEP_AFTER_MS),
            Bind::Int(at),
            Bind::Int(g),
            Bind::Int(SWEEP_READ_LIMIT as i64),
        ],
    )
    .await?;
    let (pairs, next) = sweep_choice(&read, SWEEP_READ_LIMIT);
    // MEDIUM-2: a pair whose judge faulted past the bound waits out its hold (its marks stay; the walk wraps to it)
    let pairs = if pairs.is_empty() {
        pairs
    } else {
        let names: Vec<String> = pairs.iter().map(|(s, k)| fault_memo_name(s, k)).collect();
        let memos = rows(
            db,
            FAULT_MEMO_READ_SQL,
            vec![Bind::Text(json!(names).to_string())],
        )
        .await?;
        let (kept, held) = unheld(pairs, &memos, now_ms);
        if held > 0 {
            db.counters(vec![("owed_events_sweep_held_total".into(), held as u64)])
                .await;
        }
        kept
    };
    let next = next.map_or(
        json!({"at": -1, "g": 0}),
        |(at, g)| json!({"at": at, "g": g}),
    );
    db.run_batch(vec![Stmt {
        sql: SWEEP_CURSOR_SQL.into(),
        binds: vec![Bind::Text(next.to_string()), Bind::Int(now_ms)],
    }])
    .await?;
    Ok(pairs)
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

/// `true` when this Worker holds the producer binding (the queue configured): the judge's trigger is the consumer.
pub fn configured(env: &worker::Env) -> bool {
    env.queue(publish::BINDING).is_ok()
}

/// The sweep on the belt's alarm, then its publish. Answers the pairs re-published.
pub async fn sweep_publish(env: &worker::Env) -> Result<usize, String> {
    let sql = crate::routes::D1Sql::of(env).map_err(|e| e.to_string())?;
    let pairs = sweep(&sql, worker::Date::now().as_millis() as i64).await?;
    if pairs.is_empty() {
        return Ok(0);
    }
    let Some(port) = publish::WorkerPublish::of(env) else {
        return Err("no owed-events binding".into());
    };
    let report = publish::publish_with(&port, notes_of(&pairs)).await;
    publish::bump(
        env,
        &[
            ("owed_events_swept_total", report.sent.len() as u64),
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
    if !held {
        crate::tracker_host::runtime::note_unheld();
    }
    Ok(())
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
    let tip = crate::routes::chaintracks_present_height_env(env, "owed")
        .await
        .ok();
    let judge = owed_judge::D1Judge::new(env, tip, crate::routes::written_off_before_ms_env(env))
        .map_err(|e| e.to_string())?;
    let routed = route(&judge, ev).await?;
    if routed.more {
        publish::note(&ev.source, &ev.source_key);
        publish::publish_env(env, Vec::new()).await;
    }
    Ok(routed)
}

/// The Worker's `queue` handler: the `owed-events` consumer, or its dead letter queue's park.
pub async fn queue(batch: worker::MessageBatch<Value>, env: worker::Env) -> worker::Result<()> {
    use worker::MessageExt;
    let name = batch.queue();
    if name.ends_with("-dlq") {
        return park_dead_letters(&batch, &env).await;
    }
    let now = worker::Date::now().as_millis() as i64;
    let mut oldest_age: i64 = 0;
    let mut deltas: BTreeMap<&str, u64> = BTreeMap::new();
    let mut messages = 0u64;
    let mut resolved: Vec<Stmt> = Vec::new();
    for msg in batch.raw_iter() {
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
        match consume_one(&env, &ev).await {
            Ok(routed) => {
                resolved.extend(resolved_statements(&ev.source, &ev.source_key));
                *deltas.entry("owed_events_consumed_total").or_default() += 1;
                *deltas.entry("owed_events_acked_marks_total").or_default() += routed.marks as u64;
                if routed.more {
                    *deltas.entry("owed_events_continued_total").or_default() += 1;
                }
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
                let memo = match crate::routes::D1Sql::of(&env) {
                    Ok(sql) => note_fault(&sql, &ev.source, &ev.source_key, &e).await.ok(),
                    Err(_) => None,
                };
                match memo {
                    Some(m) if m.hold_ms > 0 => {
                        *deltas.entry("owed_events_fault_held_total").or_default() += 1;
                        msg.retry_with_options(
                            &worker::QueueRetryOptionsBuilder::new()
                                .with_delay_seconds((m.hold_ms / 1000).clamp(1, 43_200) as u32)
                                .build(),
                        );
                    }
                    _ => msg.retry(),
                }
            }
        }
    }
    let deltas: Vec<(&str, u64)> = deltas.into_iter().collect();
    publish::bump(&env, &deltas).await;
    if let Ok(sql) = crate::routes::D1Sql::of(&env) {
        let row = json!({"oldestEnqueueAgeMs": oldest_age, "messages": messages, "atMs": now});
        let mut batch_row = vec![Stmt {
            sql: CONSUMER_ROW_SQL.into(),
            binds: vec![Bind::Text(row.to_string()), Bind::Int(now)],
        }];
        // MEDIUM-2: a landed judge clears its pair's fault memo and resolves its dead letter
        batch_row.extend(resolved);
        if let Err(e) = sql
            .run_batch(batch_row)
            .await
        {
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

/// MEDIUM-8: `/health.owedEvents`: the totals and the served ages, from rows (never a scan): the counters, the
/// consumer's batch row and the drain's backlog snapshot (`owed_judge::dirty_health`).
pub async fn health(db: &impl SqlPort) -> Value {
    let read = db
        .run_batch(vec![
            Stmt {
                sql: HEALTH_COUNTERS_SQL.into(),
                binds: vec![],
            },
            Stmt {
                sql: HEALTH_CONSUMER_SQL.into(),
                binds: vec![],
            },
            Stmt {
                sql: FAULT_MEMO_HEALTH_SQL.into(),
                binds: vec![Bind::Int(db.now_ms() as i64)],
            },
            Stmt {
                sql: FAULT_MEMO_LAST_SQL.into(),
                binds: vec![],
            },
            Stmt {
                sql: DEAD_LETTER_HEALTH_SQL.into(),
                binds: vec![],
            },
        ])
        .await;
    let Ok(read) = read else {
        return Value::Null;
    };
    let counter = |name: &str| -> u64 {
        read.first()
            .and_then(|o| o.rows.iter().find(|r| r["name"].as_str() == Some(name)))
            .and_then(|r| {
                r["value"]
                    .as_u64()
                    .or_else(|| r["value"].as_f64().map(|f| f as u64))
            })
            .unwrap_or(0)
    };
    let consumer = read
        .get(1)
        .and_then(|o| o.rows.first())
        .and_then(|r| r["cursor"].as_str())
        .and_then(|c| serde_json::from_str::<Value>(c).ok());
    let memo = read.get(2).and_then(|o| o.rows.first()).cloned().unwrap_or(Value::Null);
    let last = read.get(3).and_then(|o| o.rows.first()).map(|r| {
        let cursor = r["cursor"]
            .as_str()
            .and_then(|c| serde_json::from_str::<Value>(c).ok())
            .unwrap_or(Value::Null);
        json!({
            "pair": r["name"].as_str().map(|n| n.trim_start_matches(FAULT_MEMO_PREFIX)),
            "faults": cursor["faults"],
            "lastFault": cursor["lastFault"],
            "lastFaultAtMs": cursor["lastFaultAtMs"],
            "heldUntilMs": cursor["heldUntilMs"],
        })
    });
    let letters: serde_json::Map<String, Value> = read
        .get(4)
        .map(|o| {
            o.rows
                .iter()
                .filter_map(|r| Some((r["status"].as_str()?.to_string(), r["n"].clone())))
                .collect()
        })
        .unwrap_or_default();
    let dirty = owed_judge::dirty_health(db).await;
    json!({
        "publishedTotal": counter("owed_events_published_total"),
        "consumedTotal": counter("owed_events_consumed_total"),
        "ackedMarks": counter("owed_events_acked_marks_total"),
        "unackedMarks": dirty["count"],
        "oldestUnackedMarkAgeMs": dirty["oldestMarkAgeMs"],
        "oldestEnqueueAgeMs": consumer.as_ref().map(|c| c["oldestEnqueueAgeMs"].clone()),
        "consumerAtMs": consumer.as_ref().map(|c| c["atMs"].clone()),
        "dlqTotal": counter("owed_events_dlq_total"),
        "publishFaults": counter("owed_events_publish_faults_total"),
        "judgeFaults": counter("owed_events_judge_faults_total"),
        "sweptTotal": counter("owed_events_swept_total"),
        "faultHeld": counter("owed_events_fault_held_total"),
        "sweepHeld": counter("owed_events_sweep_held_total"),
        "faultMemo": {
            "rows": memo["rows"],
            "held": memo["held"],
            "maxFaults": memo["maxFaults"],
            "last": last,
            "holdAfter": FAULT_HOLD_AFTER,
            "holdBaseMs": FAULT_HOLD_BASE_MS,
            "holdCapMs": FAULT_HOLD_CAP_MS,
        },
        "deadLetters": letters,
    })
}
