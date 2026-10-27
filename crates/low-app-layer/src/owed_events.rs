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
pub const HEALTH_COUNTERS_SQL: &str = "SELECT name,value FROM ops_counters WHERE name IN ('owed_events_published_total','owed_events_consumed_total','owed_events_acked_marks_total','owed_events_dlq_total','owed_events_publish_faults_total','owed_events_judge_faults_total','owed_events_swept_total')";
/// The consumer's row.
pub const HEALTH_CONSUMER_SQL: &str =
    "SELECT cursor,tickedAtMs FROM owed_belt_state WHERE name='owed-events:consumer'";
/// LOW-3: park one dead letter, under the bound (a full table answers no row: the platform retries it).
pub const DEAD_LETTER_PARK_SQL: &str = "INSERT INTO owed_event_dead_letters (messageId,body,source,sourceKey,parkedAt) SELECT ?1,?2,?3,?4,?5 WHERE (SELECT COUNT(*) FROM (SELECT 1 FROM owed_event_dead_letters LIMIT ?6))<?6 ON CONFLICT(messageId) DO NOTHING RETURNING messageId";
/// A letter already parked (a redelivery).
pub const DEAD_LETTER_HELD_SQL: &str =
    "SELECT 1 AS held FROM owed_event_dead_letters WHERE messageId=?1";

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

/// PURE: the sweep's choice over the rows read past its cursor: up to [`SWEEP_PAIRS_PER_TICK`] distinct pairs, oldest
/// first, and the cursor to save (`None`: the walk reached the end of the backlog, start again next tick).
pub fn sweep_choice(
    rows: &[Value],
    read_limit: usize,
) -> (Vec<(String, String)>, Option<(i64, i64)>) {
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
    async fn batch(
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
                msg.retry();
            }
        }
    }
    let deltas: Vec<(&str, u64)> = deltas.into_iter().collect();
    publish::bump(&env, &deltas).await;
    if let Ok(sql) = crate::routes::D1Sql::of(&env) {
        let row = json!({"oldestEnqueueAgeMs": oldest_age, "messages": messages, "atMs": now});
        if let Err(e) = sql
            .run_batch(vec![Stmt {
                sql: CONSUMER_ROW_SQL.into(),
                binds: vec![Bind::Text(row.to_string()), Bind::Int(now)],
            }])
            .await
        {
            worker::console_warn!("[owed-events] the consumer's age row not written: {e}");
        }
    }
    Ok(())
}

/// LOW-3: the dead letter queue's consumer, on #576's shape: PARK FIRST, ACK AFTER. A park that faults or meets a
/// full table is handed back (60 s); the platform's last delivery loses the letter, never its marks.
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
        let ev = OwedEvent::decode(&body).ok();
        let id = msg.id();
        let out = sql
            .run_batch(vec![
                Stmt {
                    sql: DEAD_LETTER_PARK_SQL.into(),
                    binds: vec![
                        Bind::Text(id.clone()),
                        Bind::Text(body.clone()),
                        ev.as_ref()
                            .map_or(Bind::Null, |e| Bind::Text(e.source.clone())),
                        ev.as_ref()
                            .map_or(Bind::Null, |e| Bind::Text(e.source_key.clone())),
                        Bind::Int(now),
                        Bind::Int(publish::DEAD_LETTERS_MAX),
                    ],
                },
                Stmt {
                    sql: DEAD_LETTER_HELD_SQL.into(),
                    binds: vec![Bind::Text(id.clone())],
                },
            ])
            .await;
        match out {
            Ok(outs) if outs.get(1).is_some_and(|o| !o.rows.is_empty()) => {
                parked += u64::from(outs.first().is_some_and(|o| !o.rows.is_empty()));
                worker::console_warn!("[owed-events] PARKED dead letter {id}: {body}");
                msg.ack();
            }
            Ok(_) => {
                worker::console_warn!(
                    "[owed-events] dead letter {id} not parked (the table is full): handed back"
                );
                msg.retry_with_options(
                    &worker::QueueRetryOptionsBuilder::new()
                        .with_delay_seconds(60)
                        .build(),
                );
            }
            Err(e) => {
                worker::console_warn!(
                    "[owed-events] dead letter {id} park faulted ({e}): handed back"
                );
                msg.retry_with_options(
                    &worker::QueueRetryOptionsBuilder::new()
                        .with_delay_seconds(60)
                        .build(),
                );
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
    })
}
