//! bsv-low #911 (lane D2-ACTOR-A; `DESIGN-D2-ACTOR-2026-10-26.md` sections 3 and 11, the audit's HIGH-1, HIGH-2 and
//! MEDIUM-1): THE PUBLISH of the `owed-events` queue. ONE source for both Workers (the app layer includes this file
//! by path, as it includes `d1_ledger.rs`), so the two producers write one shape through one plan.
//!
//! A writer that changes an owed source row NOTES the row's trigger pair (`source`, `sourceKey`: exactly what the
//! row's `owed_dirty` trigger writes, `overlay_discovery::owed_event::TRIGGER_SOURCES`) beside the mark its statement
//! made ([`note`]; the overlay's pot writers note through `pot_changes::note`, every one of them). At its flush point
//! (a request before its answer, a queue replay at its ack, the cron at its end, a detached `wait_until` writer at its
//! own end, `flush_inline`-shaped) the invocation PUBLISHES its notes ([`publish_with`]): ONE D1 batch takes the
//! publish's `seq` from the shared counter ([`SEQ_BUMP_SQL`]) and, in the same transaction, each row's facts and its
//! mark count ([`read_sql`]); then ONE `sendBatch` per 100 events. A note whose row holds no mark names no subject and
//! is not sent (counted `owed_events_publish_skipped_total`). A word frame (`tracker_events`) rides as a `word` event
//! with its frame (the consumer ingests it, then routes): the frames LEAVE the `/internal/tx-word` POST, its flush wall
//! and its D1 park; a frame whose publish faulted goes back to that POST (the belt for a missing binding or a Queues
//! incident).
//!
//! A publish that faults, is cancelled or is refused leaves the MARK (the trigger wrote it in the writer's own
//! transaction): never a lost event. The app layer's un-acked mark sweep (`owed_belt`, every 120 s) re-publishes the
//! marks no consumer acknowledged within one tick, oldest first, 64 a tick.
use overlay_discovery::owed_event::{self, OwedEvent};
use serde_json::{json, Value};
use std::cell::RefCell;
use std::collections::BTreeMap;

/// The queue's producer binding, in both Workers.
pub const BINDING: &str = "OWED_EVENTS";

/// The shared counter's table (one row). Transient: a wipe re-seeds it from the clock ([`SEQ_BUMP_SQL`]).
pub const SEQ_CREATE: &str =
    "CREATE TABLE IF NOT EXISTS owed_event_seq (id INTEGER PRIMARY KEY CHECK (id = 1), n INTEGER NOT NULL)";
/// LOW-3: the `owed-events` dead letters, parked by the app layer's DLQ consumer (park first, ack after) and
/// re-driven by the overlay's lever (`POST /internal/redrive-dead-letters` with `"family":"owed-events"`). A dead
/// letter is never the only copy of the intent: its marks stay in `owed_dirty` (the consumer acknowledges them only
/// after the judge wrote) and the sweep re-publishes them. Transient; at most [`DEAD_LETTERS_MAX`] rows.
///
/// The D2-ACTOR-A lens's MEDIUM-2 and LOW-4: ONE ROW PER PAIR (`source`, `sourceKey`; an undecodable body is keyed
/// `('', messageId)`), never one per message: a pair whose judge faults for good holds one row however often it
/// dead-letters (`parks` counts them, `lastFault` is the consumer's fault memo's last error), `redrives` counts the
/// lever's re-drives, and a letter re-parked after [`MAX_REDRIVES`] is `exhausted`: never re-driven again. The
/// consumer deletes the pair's row when its judge lands. `redrivenAt` (D2-ACTOR-A-DELTA's LOW-C): the lever's last
/// re-drive, read by its stale return ([`DEAD_LETTERS_STALE_RETURN_SQL`]). (Edited in place: migrations 470 to 473
/// shipped nowhere, PR 184 unlanded.)
pub const DEAD_LETTERS_CREATE: &str = "CREATE TABLE IF NOT EXISTS owed_event_dead_letters (source TEXT NOT NULL, sourceKey TEXT NOT NULL, messageId TEXT NOT NULL, body TEXT NOT NULL, parks INTEGER NOT NULL DEFAULT 1, lastFault TEXT, status TEXT NOT NULL DEFAULT 'parked', parkedAt INTEGER NOT NULL, redrives INTEGER NOT NULL DEFAULT 0, redrivenAt INTEGER, PRIMARY KEY (source, sourceKey))";
/// The parked letters' bound (a NEW pair's park past it is refused, the platform retries it, then it is lost: its
/// marks stay; a pair already parked is always re-parked on its own row).
pub const DEAD_LETTERS_MAX: i64 = 2000;
/// LOW-4: the re-drives of one letter; re-parked after them it is `exhausted` (#576's `MAX_REDRIVES`).
pub const MAX_REDRIVES: i64 = 3;
/// The `hop` facts read's index on the missing-evidence rows (their key leads with the identity).
pub const RELEASED_MISSING_HOP_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_released_missing_hop ON released_hop_evidence_missing(hopTxid,hopVout)";
/// MEDIUM-1: the FIRST statement of every publish batch: `?1` tokens for the batch's events, answered as the LAST
/// one (`RETURNING n`; the events take `n - ?1 + 1 ..= n` in order). D1 runs a batch as one transaction, so no other
/// publish takes a token between it and the batch's reads. A first row (none yet, or after a wipe) starts at the
/// clock (`?2` ms) times 1024, above every token a counter of the same clock less than a second behind could have
/// handed out; under 2^53 until about 2248.
pub const SEQ_BUMP_SQL: &str = "INSERT INTO owed_event_seq (id, n) VALUES (1, CAST(?2 AS INTEGER) * 1024 + ?1) ON CONFLICT (id) DO UPDATE SET n = n + ?1 RETURNING n";

/// The pair's marks (how many subjects it names) and the row's own facts, in one statement (see [`read_sql`]).
macro_rules! marks_and {
    ($facts:expr) => {
        concat!(
            "SELECT (SELECT COUNT(*) FROM owed_dirty INDEXED BY idx_owed_dirty_source_key WHERE sourceKey=?2 AND source=?1) AS marks,(",
            $facts,
            ") AS facts"
        )
    };
}

/// `pot` (`txid:vout`): the pot row's own columns (no bytes).
pub const READ_POT_SQL: &str = marks_and!("SELECT json_object('txid',txid,'vout',outputIndex,'spent',spent,'spendingTxid',spendingTxid,'spentConfirmed',spentConfirmed,'spentHeight',spentHeight,'lockKind',lockKind,'potSats',potSats,'recoveryHeight',recoveryHeight,'verdict',verdict,'createdAt',createdAt) FROM pot_records WHERE txid=?3 AND outputIndex=?4");
/// `party` (the marker's `txid`).
pub const READ_PARTY_SQL: &str = marks_and!("SELECT json_group_array(json_object('identity',identity,'opponentIdentity',opponentIdentity,'gameId',gameId,'potTxid',potTxid,'potVout',potVout,'recoveryHeight',recoveryHeight,'seatSettlePubkey',seatSettlePubkey,'sigValid',sigValid,'outputIndex',outputIndex)) FROM potparty_records WHERE txid=?3");
/// `hop` (`txid:hopVout`): the hop's markers, its released evidence and its missing-evidence rows.
pub const READ_HOP_SQL: &str = marks_and!("SELECT json_object('markers',(SELECT json_group_array(json_object('identity',identity,'opponentIdentity',opponentIdentity,'gameId',gameId,'hopSats',hopSats,'seatSettlePubkey',seatSettlePubkey,'markerValid',markerValid,'outputIndex',outputIndex)) FROM hopparty_records INDEXED BY idx_hopparty_hop WHERE txid=?3 AND hopVout=?4),'released',(SELECT json_group_array(json_object('identity',identity,'gameId',gameId,'potTxid',potTxid,'seatSettlePubkey',seatSettlePubkey,'hopSats',hopSats,'evictedAt',evictedAt)) FROM released_hop_evidence WHERE hopTxid=?3 AND hopVout=?4),'missing',(SELECT json_group_array(json_object('identity',identity,'gameId',gameId,'potTxid',potTxid,'evictedAt',evictedAt,'bytesRead',bytesRead)) FROM released_hop_evidence_missing INDEXED BY idx_released_missing_hop WHERE hopTxid=?3 AND hopVout=?4))");
/// `refund` (the backup's `txid`).
pub const READ_REFUND_SQL: &str = marks_and!("SELECT json_group_array(json_object('identity',identity,'gameId',gameId,'potTxid',potTxid,'potVout',potVout,'refundValid',refundValid,'outputIndex',outputIndex)) FROM potrefund_records WHERE txid=?3");
/// `result` (the marker's `txid`).
pub const READ_RESULT_SQL: &str = marks_and!("SELECT json_group_array(json_object('gameId',gameId,'winner',winner,'loser',loser,'potTxid',potTxid,'settleTxid',settleTxid,'claimValid',claimValid,'outputIndex',outputIndex)) FROM result_markers_v2 WHERE txid=?3");
/// `collected` (the marker's `txid`).
pub const READ_COLLECTED_SQL: &str = marks_and!("SELECT json_group_array(json_object('identity',identity,'gameId',gameId,'payTxid',payTxid,'outputIndex',outputIndex)) FROM collected_markers_v2 WHERE txid=?3");
/// `sweep` (the filing's `txid`; the raw bytes are not read).
pub const READ_SWEEP_SQL: &str = marks_and!("SELECT json_group_array(json_object('identity',identity,'gameId',gameId,'hopTxid',hopTxid,'hopVout',hopVout,'sweepTxid',sweepTxid,'outputIndex',outputIndex)) FROM hopsweep_records WHERE txid=?3");
/// `eviction` (the evicted `txid`).
pub const READ_EVICTION_SQL: &str = marks_and!("SELECT json_object('reason',reason,'evictedAt',evictedAt,'readmittedAt',readmittedAt,'rowsMoved',rowsMoved,'releasedSpends',json(COALESCE(releasedSpends,'[]'))) FROM pot_evictions WHERE txid=?3");
/// `seen` (the witnessed `txid`).
pub const READ_SEEN_SQL: &str = marks_and!("SELECT json_object('seenAt',seenAt,'seenSource',seenSource,'sourceAtMs',sourceAtMs) FROM network_seen WHERE txid=?3");
/// `word` (the `txid`): the word, never its legacy body (MEDIUM-4: the words and their recheck stay the read's).
pub const READ_WORD_SQL: &str = marks_and!("SELECT json_object('word',word,'tier',tier,'height',height,'proofRoot',proof_root,'headerHash',header_hash,'hintSource',hint_source,'hintAt',hint_at,'updatedAt',updated_at) FROM tx_words WHERE txid=?3");
/// `beef` (the `txid`): the proof flags, never the bytes.
pub const READ_BEEF_SQL: &str = marks_and!("SELECT json_object('hasProof',has_proof,'proofVerified',proof_verified,'proofHeight',proofHeight,'structurallyUnprovable',structurally_unprovable,'createdAt',createdAt) FROM pot_beefs WHERE txid=?3");
/// `refusal` (`hopTxid:hopVout`).
pub const READ_REFUSAL_SQL: &str = marks_and!("SELECT json_object('refusedTxid',refusedTxid,'reason',reason,'refusedAt',refusedAt) FROM submit_refusals WHERE hopTxid=?3 AND hopVout=?4");
/// `identity` (the hook's identity-only note): does the identity hold any mark? (The D2-ACTOR-A lens's LOW-6: an
/// identity with nothing pending is not sent.) One covering-index probe; no facts.
pub const READ_IDENTITY_SQL: &str = "SELECT (SELECT COUNT(*) FROM (SELECT 1 FROM owed_dirty INDEXED BY idx_owed_dirty_identity_marked WHERE identity=?2 AND ?1='identity' LIMIT 1)) AS marks,NULL AS facts";
/// The sources no table holds (`wanted`, `age`, `backfill`, `tip`): the marks alone.
pub const READ_MARKS_SQL: &str = marks_and!("SELECT NULL");

/// The facts read of a source; `None` for an event that reads nothing (a word frame, `txfacts`: lane B's mirror,
/// sent whatever the marks). An identity route reads its marks only ([`READ_IDENTITY_SQL`]).
pub fn read_sql(source: &str) -> Option<&'static str> {
    Some(match source {
        "pot" => READ_POT_SQL,
        "party" => READ_PARTY_SQL,
        "hop" => READ_HOP_SQL,
        "refund" => READ_REFUND_SQL,
        "result" => READ_RESULT_SQL,
        "collected" => READ_COLLECTED_SQL,
        "sweep" => READ_SWEEP_SQL,
        "eviction" => READ_EVICTION_SQL,
        "seen" => READ_SEEN_SQL,
        "word" => READ_WORD_SQL,
        "beef" => READ_BEEF_SQL,
        "refusal" => READ_REFUSAL_SQL,
        owed_event::ROUTE_IDENTITY => READ_IDENTITY_SQL,
        s if owed_event::OWN_SOURCES.contains(&s) => READ_MARKS_SQL,
        _ => return None,
    })
}

/// The counters (both Workers write `ops_counters` rows; served on the app layer's `/health.owedEvents` and the
/// overlay's `/health/invariants.owedEvents`).
pub const COUNTER_PUBLISHED: &str = "owed_events_published_total";
/// Notes whose row held no mark (no subject to judge): not sent.
pub const COUNTER_SKIPPED: &str = "owed_events_publish_skipped_total";
/// Publishes that faulted (the D1 batch or the send): their marks wait for the sweep.
pub const COUNTER_FAULTS: &str = "owed_events_publish_faults_total";
/// Every counter this module and the consumer write, for the seeds.
pub const COUNTERS: [&str; 19] = [
    COUNTER_PUBLISHED,
    COUNTER_SKIPPED,
    COUNTER_FAULTS,
    "owed_events_consumed_total",
    "owed_events_acked_marks_total",
    "owed_events_judge_faults_total",
    "owed_events_continued_total",
    "owed_events_swept_total",
    "owed_events_dlq_total",
    "owed_events_redriven_total",
    // the D2-ACTOR-A lens's MEDIUM-2: a judge fault that set its pair's hold; a pair the sweep left out, held
    "owed_events_fault_held_total",
    "owed_events_sweep_held_total",
    COUNTER_WALL_DROPS,
    // D2-ACTOR-A-DELTA: NEW-M1 a subject faulting alone, a subject skipped while held; LOW-A the consumer's message
    // wall; LOW-C a re-drive returned to the lever, a letter discarded
    "owed_events_subject_faults_total",
    "owed_events_subject_held_total",
    "owed_events_consumer_wall_drops_total",
    "owed_events_dead_letters_stale_returned_total",
    "owed_events_dead_letters_discarded_total",
    // D2-ACTOR-A-DELTA-2's NEW2-M1: a message whose every subject was held (acked, no continuation)
    "owed_events_all_held_total",
];
/// LOW-7 (the D2-ACTOR-A lens): publishes dropped at a request's answer wall ([`PUBLISH_ANSWER_WALL_MS`]): their
/// marks wait for the sweep. Counted on the isolate ([`note_wall_drop`]) and written with the isolate's next publish
/// (a write at the wall would wait on the D1 the wall gave up on); an isolate that ends first loses its count.
pub const COUNTER_WALL_DROPS: &str = "owed_events_publish_wall_drops_total";

thread_local! {
    static WALL_DROPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// LOW-7: one publish dropped at the answer wall.
pub fn note_wall_drop() {
    WALL_DROPS.with(|d| d.set(d.get() + 1));
}

/// LOW-7: the drops not yet written, taken.
pub fn take_wall_drops() -> u64 {
    WALL_DROPS.with(|d| d.replace(0))
}
const BUMP_SQL: &str = "INSERT INTO ops_counters (name, value) VALUES (?1, ?2) ON CONFLICT(name) DO UPDATE SET value = ops_counters.value + excluded.value";

/// One noted pair, with the facts the writer holds (a word frame, a transaction's `txfacts`), if any.
#[derive(Debug, Clone, PartialEq)]
pub struct Noted {
    /// The trigger's source (or a routing kind).
    pub source: String,
    /// The trigger's source key.
    pub key: String,
    /// Facts the writer holds; `None`: read from the row at publish.
    pub held: Option<Value>,
}

thread_local! {
    static NOTES: RefCell<BTreeMap<(String, String), Option<Value>>> = const { RefCell::new(BTreeMap::new()) };
}

/// Note that the row of `(source, key)` changed (its trigger marked it). Cheap, never fails, deduped.
pub fn note(source: &str, key: &str) {
    NOTES.with(|n| {
        n.borrow_mut()
            .entry((source.to_string(), key.to_ascii_lowercase()))
            .or_insert(None);
    });
}

/// Note `(source, key)` with facts the writer holds (the latest note of a key wins).
pub fn note_with(source: &str, key: &str, facts: Value) {
    NOTES.with(|n| {
        n.borrow_mut()
            .insert((source.to_string(), key.to_ascii_lowercase()), Some(facts));
    });
}

/// MEDIUM-3: note a recorded transaction's `txfacts`, decoded once here from its raw bytes.
pub fn note_txfacts(txid: &str, raw: &[u8]) {
    if let Some(facts) = owed_event::txfacts_of_raw(raw) {
        note_with(owed_event::TXFACTS, txid, facts);
    }
}

/// Take every note (deduped, in key order).
pub fn drain() -> Vec<Noted> {
    NOTES.with(|n| {
        std::mem::take(&mut *n.borrow_mut())
            .into_iter()
            .map(|((source, key), held)| Noted { source, key, held })
            .collect()
    })
}

/// Take one note out (the consumer routes a word it just ingested itself).
pub fn forget(source: &str, key: &str) {
    NOTES.with(|n| {
        n.borrow_mut()
            .remove(&(source.to_string(), key.to_ascii_lowercase()));
    });
}

/// No note is held.
pub fn is_empty() -> bool {
    NOTES.with(|n| n.borrow().is_empty())
}

/// A copy of the notes, the set left as it is (the pins).
pub fn snapshot() -> Vec<Noted> {
    NOTES.with(|n| {
        n.borrow()
            .iter()
            .map(|((source, key), held)| Noted {
                source: source.clone(),
                key: key.clone(),
                held: held.clone(),
            })
            .collect()
    })
}

/// A word frame (`tracker_events`' `{txid, kind, body}`) as a `word` note carrying it.
pub fn frame_note(frame: &Value) -> Option<Noted> {
    let txid = frame["txid"].as_str()?.to_ascii_lowercase();
    Some(Noted {
        source: "word".into(),
        key: txid,
        held: Some(json!({ "frame": frame })),
    })
}

/// One bound of a statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Arg {
    /// TEXT.
    Text(String),
    /// INTEGER.
    Int(i64),
    /// NULL.
    Null,
}

/// What a publish needs of its Worker: one D1 batch (each statement's rows as JSON objects) and one send.
#[allow(async_fn_in_trait)]
pub trait PublishPort {
    /// Run the statements as ONE batch (one transaction), each answering its rows.
    async fn run_reads(
        &self,
        stmts: Vec<(&'static str, Vec<Arg>)>,
    ) -> Result<Vec<Vec<Value>>, String>;
    /// Send the bodies to the queue as one `sendBatch` (at most [`owed_event::SEND_BATCH_MAX`]).
    async fn send(&self, bodies: Vec<String>) -> Result<(), String>;
    /// The clock, ms.
    fn now_ms(&self) -> i64;
}

/// What one publish did.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Published {
    /// Events sent.
    pub sent: Vec<OwedEvent>,
    /// Notes whose row held no mark (not sent).
    pub skipped: usize,
    /// Notes not published because the batch or a send faulted (their marks wait for the sweep; a word frame among
    /// them goes back to the POST).
    pub unsent: Vec<Noted>,
    /// The fault, if any.
    pub fault: Option<String>,
}

/// PURE: the bounds of one note's read (`?1` source, `?2` key, `?3` the txid part, `?4` the vout part).
/// Only the bounds its statement names (a statement given more bounds than it names is refused).
pub fn read_args(n: &Noted) -> Vec<Arg> {
    let (txid, vout) = owed_event::split_key(&n.key);
    let mut args = vec![
        Arg::Text(n.source.clone()),
        Arg::Text(n.key.clone()),
        Arg::Text(txid.to_string()),
        vout.map_or(Arg::Null, |v| Arg::Int(i64::from(v))),
    ];
    let named = read_sql(&n.source).map_or(2, |sql| {
        (1..=4)
            .rev()
            .find(|i| sql.contains(&format!("?{i}")))
            .unwrap_or(0)
    });
    args.truncate(named);
    args
}

/// PURE: does this note read its row at publish? (A held fact, an identity route and an unknown source do not.)
pub fn reads_row(n: &Noted) -> bool {
    n.held.is_none() && read_sql(&n.source).is_some()
}

/// THE PUBLISH, transport injected (the Workers' ports and the pins run exactly this): at most
/// [`owed_event::PUBLISH_MAX`] notes (the rest answered `unsent`, for the caller's next publish or the sweep), the
/// seq and the rows in ONE batch, then the sends, 100 a batch.
pub async fn publish_with(port: &impl PublishPort, mut notes: Vec<Noted>) -> Published {
    let mut out = Published::default();
    if notes.is_empty() {
        return out;
    }
    if notes.len() > owed_event::PUBLISH_MAX {
        out.unsent = notes.split_off(owed_event::PUBLISH_MAX);
    }
    let now = port.now_ms();
    let mut stmts: Vec<(&'static str, Vec<Arg>)> = vec![(
        SEQ_BUMP_SQL,
        vec![Arg::Int(notes.len() as i64), Arg::Int(now)],
    )];
    for n in notes.iter().filter(|n| reads_row(n)) {
        stmts.push((read_sql(&n.source).unwrap_or(READ_MARKS_SQL), read_args(n)));
    }
    let rows = match port.run_reads(stmts).await {
        Ok(rows) => rows,
        Err(e) => {
            out.fault = Some(e);
            out.unsent.extend(notes);
            return out;
        }
    };
    let last = rows
        .first()
        .and_then(|r| r.first())
        .and_then(|r| r["n"].as_i64());
    let Some(last) = last else {
        out.fault = Some("the seq bump answered no token".into());
        out.unsent.extend(notes);
        return out;
    };
    let first = last - notes.len() as i64 + 1;
    let mut reads = rows.into_iter().skip(1);
    let mut events: Vec<(Noted, OwedEvent)> = Vec::new();
    for (i, n) in notes.into_iter().enumerate() {
        let facts = if let Some(held) = &n.held {
            held.clone()
        } else if reads_row(&n) {
            let row = reads.next().and_then(|r| r.into_iter().next());
            let marks = row.as_ref().and_then(|r| r["marks"].as_i64()).unwrap_or(0);
            if marks == 0 {
                out.skipped += 1;
                continue;
            }
            let facts = row
                .as_ref()
                .and_then(|r| r["facts"].as_str())
                .and_then(|s| serde_json::from_str::<Value>(s).ok())
                .unwrap_or(Value::Null);
            json!({ "marks": marks, "row": facts })
        } else {
            Value::Null
        };
        let event = OwedEvent {
            v: owed_event::OWED_EVENT_V,
            seq: first + i as i64,
            source: n.source.clone(),
            source_key: n.key.clone(),
            facts,
            at_ms: now,
        };
        events.push((n, event));
    }
    for chunk in events.chunks(owed_event::SEND_BATCH_MAX) {
        let bodies = chunk.iter().map(|(_, e)| e.encode()).collect();
        match port.send(bodies).await {
            Ok(()) => out.sent.extend(chunk.iter().map(|(_, e)| e.clone())),
            Err(e) => {
                out.fault = Some(e);
                out.unsent.extend(chunk.iter().map(|(n, _)| n.clone()));
            }
        }
    }
    out
}

/// The worker port: the D1 binding and the queue binding.
pub struct WorkerPublish {
    db: worker::D1Database,
    queue: worker::Queue,
}

impl WorkerPublish {
    /// `None` when either binding is absent (a config without the queue: the marks wait for the drain).
    pub fn of(env: &worker::Env) -> Option<Self> {
        Some(WorkerPublish {
            db: env.d1("OVERLAY_DB").ok()?,
            queue: env.queue(BINDING).ok()?,
        })
    }
}

impl PublishPort for WorkerPublish {
    async fn run_reads(
        &self,
        stmts: Vec<(&'static str, Vec<Arg>)>,
    ) -> Result<Vec<Vec<Value>>, String> {
        use worker::wasm_bindgen::JsValue;
        let mut prepared = Vec::with_capacity(stmts.len());
        for (sql, args) in stmts {
            let binds: Vec<JsValue> = args
                .iter()
                .map(|a| match a {
                    Arg::Text(t) => JsValue::from_str(t),
                    Arg::Int(n) => JsValue::from_f64(*n as f64),
                    Arg::Null => JsValue::NULL,
                })
                .collect();
            prepared.push(
                self.db
                    .prepare(sql)
                    .bind(&binds)
                    .map_err(|e| e.to_string())?,
            );
        }
        let results = crate::d1_ledger::counted_batch(&self.db, prepared)
            .await
            .map_err(|e| e.to_string())?;
        results
            .into_iter()
            .map(|r| r.results::<Value>().map_err(|e| e.to_string()))
            .collect()
    }
    async fn send(&self, bodies: Vec<String>) -> Result<(), String> {
        let messages: Vec<worker::SendMessage<worker::wasm_bindgen::JsValue>> = bodies
            .into_iter()
            .map(|b| {
                worker::RawMessageBuilder::new(worker::wasm_bindgen::JsValue::from_str(&b))
                    .build_with_content_type(worker::QueueContentType::Text)
            })
            .collect();
        let n = messages.len() as u64;
        self.queue
            .send_raw_batch(messages)
            .await
            .map_err(|e| e.to_string())?;
        crate::d1_ledger::note_queue_ops(n);
        Ok(())
    }
    fn now_ms(&self) -> i64 {
        worker::Date::now().as_millis() as i64
    }
}

/// THE PUBLISH of this invocation's notes and of `frames` (the word frames the caller drained), over the Worker's
/// bindings. Answers the frames NOT published (the caller ships them through the `/internal/tx-word` POST, the belt).
/// With no queue binding the notes are dropped (LOW-9: the marks wait for the drain) and every frame is answered
/// back.
pub async fn publish_env(env: &worker::Env, frames: Vec<Value>) -> Vec<Value> {
    let Some(port) = WorkerPublish::of(env) else {
        // LOW-9 (the lens): no queue here, so no publish will ever take these notes: dropped (the marks are the
        // record; the drain judges them), never grown for the isolate's life
        let _ = drain();
        return frames;
    };
    let mut notes = drain();
    let mut frame_of: BTreeMap<String, Value> = BTreeMap::new();
    for f in frames {
        if let Some(n) = frame_note(&f) {
            frame_of.insert(n.key.clone(), f);
            notes.retain(|m| !(m.source == "word" && m.key == n.key));
            notes.push(n);
        }
    }
    if notes.is_empty() {
        return Vec::new();
    }
    let report = publish_with(&port, notes).await;
    let mut back = Vec::new();
    for n in &report.unsent {
        if n.source == "word" && n.held.is_some() {
            if let Some(frame) = frame_of.remove(&n.key) {
                back.push(frame);
            }
        }
    }
    if let Some(fault) = &report.fault {
        worker::console_warn!(
            "[owed-events] publish faulted ({} unsent, {} frame(s) back to the POST; the marks wait for the sweep): {fault}",
            report.unsent.len(),
            back.len()
        );
    }
    let mut deltas = vec![
        (COUNTER_PUBLISHED, report.sent.len() as u64),
        (COUNTER_SKIPPED, report.skipped as u64),
        (COUNTER_FAULTS, u64::from(report.fault.is_some())),
        (COUNTER_WALL_DROPS, take_wall_drops()),
    ];
    deltas.retain(|(_, d)| *d > 0);
    bump(env, &deltas).await;
    back
}

/// HIGH-2: the most a request's answer waits for its publish (the D1 batch and the send). Past it the publish is
/// dropped: its marks wait for the sweep, its frames go back to the POST (a frame both sent and posted is ingested
/// twice, idempotently).
pub const PUBLISH_ANSWER_WALL_MS: u64 = 1_500;

/// [`publish_env`] raced against [`PUBLISH_ANSWER_WALL_MS`]: a request's publish before its answer.
pub async fn publish_before_answer(env: &worker::Env, frames: Vec<Value>) -> Vec<Value> {
    use futures_util::future::{select, Either};
    let copy = frames.clone();
    let publish = std::pin::pin!(publish_env(env, frames));
    let wall = std::pin::pin!(worker::Delay::from(std::time::Duration::from_millis(
        PUBLISH_ANSWER_WALL_MS
    )));
    match select(publish, wall).await {
        Either::Left((back, _)) => back,
        Either::Right(_) => {
            note_wall_drop();
            worker::console_warn!(
                "[owed-events] the publish passed its {PUBLISH_ANSWER_WALL_MS} ms answer wall: the marks wait for the sweep"
            );
            copy
        }
    }
}

/// LOW-3: the parked letters a re-drive takes, oldest first (never a `redriven` one in flight, never `exhausted`).
pub const DEAD_LETTERS_READ_SQL: &str = "SELECT source,sourceKey,body FROM owed_event_dead_letters WHERE status='parked' ORDER BY parkedAt,source,sourceKey LIMIT ?1";
/// LOW-4: the re-driven letters (their bodies are on the queue again) marked `redriven`, their re-drive counted;
/// `?1` a JSON array of `[source, sourceKey]`. The row stays until the pair's judge lands (the consumer deletes it) or
/// it dead-letters again (re-parked, `exhausted` past [`MAX_REDRIVES`]).
pub const DEAD_LETTERS_REDRIVEN_SQL: &str = "UPDATE owed_event_dead_letters SET status='redriven',redrives=redrives+1,redrivenAt=?2 WHERE status='parked' AND (source,sourceKey) IN (SELECT json_extract(value,'$[0]'),json_extract(value,'$[1]') FROM json_each(?1))";
/// The most letters one lever call re-drives.
pub const REDRIVE_MAX: i64 = 200;
/// D2-ACTOR-A-DELTA's LOW-C: a re-drive whose message neither parked again nor landed a judge within this long (a
/// message lost on the way, a landed judge whose consumer batch row faulted) is returned to `parked` by the lever's
/// next call, keeping its spent re-drive (#576's stale return, the same hour). A wrong verdict (the copy still in
/// flight) sends a second copy, which routes by the same marks: harmless.
pub const REDRIVE_STALE_MS: i64 = 3_600_000;
/// The stale return (`?1` the cutoff, ms); answers the rows returned. A scan of at most [`DEAD_LETTERS_MAX`] rows.
pub const DEAD_LETTERS_STALE_RETURN_SQL: &str = "UPDATE owed_event_dead_letters SET status='parked' WHERE status='redriven' AND (redrivenAt IS NULL OR redrivenAt<?1) RETURNING source,sourceKey";
/// The most letters one discard call names (#576's `DISCARD_MAX_LETTERS`).
pub const DISCARD_MAX: usize = 50;
/// LOW-C: discard the named letters (`?1` a JSON array of `[source, sourceKey]`), never a `redriven` one in flight;
/// answers each row taken. The pair's MARKS stay (a letter is never the only copy of the intent): the sweep and the
/// consumer still judge them.
pub const DEAD_LETTERS_DISCARD_SQL: &str = "DELETE FROM owed_event_dead_letters WHERE status IN ('parked','exhausted') AND (source,sourceKey) IN (SELECT json_extract(value,'$[0]'),json_extract(value,'$[1]') FROM json_each(?1)) RETURNING source,sourceKey,status,parks,redrives,lastFault";

/// PURE: the keys a discard body names (`{"family":"owed-events","letters":[{"source","sourceKey"}, ...]}`, 1 to
/// [`DISCARD_MAX`]), or why it is refused.
pub fn parse_discard(raw: &[u8]) -> Result<Vec<(String, String)>, &'static str> {
    let v: Value = serde_json::from_slice(raw).map_err(|_| "body must be JSON")?;
    let letters = v["letters"].as_array().ok_or("letters must be an array")?;
    if letters.is_empty() {
        return Err("letters must name at least one letter");
    }
    if letters.len() > DISCARD_MAX {
        return Err("letters names more than the lever discards per call");
    }
    letters
        .iter()
        .map(|l| {
            let text = |k: &str| {
                l[k].as_str()
                    .filter(|t| t.len() <= 1024)
                    .map(str::to_string)
            };
            match (text("source"), text("sourceKey")) {
                (Some(s), Some(k)) if !k.is_empty() => Ok((s, k)),
                _ => Err("each letter needs a source and a non-empty sourceKey"),
            }
        })
        .collect()
}

/// LOW-C: `POST /internal/discard-dead-letters` with `{"family":"owed-events","letters":[...]}` (the #576 lever's
/// route and bearer, checked by its caller): delete the named `parked` or `exhausted` letters, each logged with its
/// parks, re-drives and last fault, counted `owed_events_dead_letters_discarded_total`; a key naming no such row is
/// `notFound`. The marks stay.
pub async fn discard_dead_letters(raw: &[u8], env: &worker::Env) -> worker::Result<worker::Response> {
    use crate::d1_ledger::Counted;
    use worker::wasm_bindgen::JsValue;
    let keys = match parse_discard(raw) {
        Ok(k) => k,
        Err(why) => {
            return worker::Response::error(
                format!("{why}: {{\"family\": \"owed-events\", \"letters\": [{{\"source\": \"<source>\", \"sourceKey\": \"<key>\"}}, ...]}} (1..{DISCARD_MAX})"),
                400,
            )
        }
    };
    let db = env.d1("OVERLAY_DB")?;
    let named: Vec<Value> = keys.iter().map(|(s, k)| json!([s, k])).collect();
    let rows = db
        .prepare(DEAD_LETTERS_DISCARD_SQL)
        .bind(&[JsValue::from_str(&Value::Array(named).to_string())])?
        .counted_all()
        .await?
        .results::<Value>()?;
    for r in &rows {
        worker::console_warn!(
            "[owed-events] DISCARDED dead letter {} {} ({}; parks={} redrives={} lastFault={})",
            r["source"].as_str().unwrap_or(""),
            r["sourceKey"].as_str().unwrap_or(""),
            r["status"].as_str().unwrap_or(""),
            r["parks"],
            r["redrives"],
            r["lastFault"].as_str().unwrap_or("")
        );
    }
    let taken: Vec<(String, String)> = rows
        .iter()
        .filter_map(|r| Some((r["source"].as_str()?.to_string(), r["sourceKey"].as_str()?.to_string())))
        .collect();
    let not_found: Vec<Value> = keys
        .iter()
        .filter(|k| !taken.contains(k))
        .map(|(s, k)| json!({"source": s, "sourceKey": k}))
        .collect();
    bump(env, &[("owed_events_dead_letters_discarded_total", rows.len() as u64)]).await;
    worker::Response::from_json(&json!({
        "family": "owed-events",
        "discarded": rows,
        "notFound": not_found,
    }))
}

/// LOW-3: `POST /internal/redrive-dead-letters` with `{"family":"owed-events","limit"?: n}` (the #576 lever, bearer
/// checked by its caller): the oldest parked owed events (default 25, at most [`REDRIVE_MAX`]) sent to the queue
/// again as they were, then marked `redriven` (LOW-4: counted, at most [`MAX_REDRIVES`] a letter, then
/// `exhausted`). A send that faults leaves them parked.
pub async fn redrive_dead_letters(
    raw: &[u8],
    env: &worker::Env,
) -> worker::Result<worker::Response> {
    use crate::d1_ledger::Counted;
    use worker::wasm_bindgen::JsValue;
    let body: Value = serde_json::from_slice(raw).unwrap_or(Value::Null);
    let limit = body["limit"].as_i64().unwrap_or(25).clamp(1, REDRIVE_MAX);
    let Some(port) = WorkerPublish::of(env) else {
        return worker::Response::error("no OWED_EVENTS binding", 503);
    };
    let now = worker::Date::now().as_millis() as f64;
    // LOW-C: a re-drive older than the hour that neither re-parked nor landed is parked again first
    let returned = port
        .db
        .prepare(DEAD_LETTERS_STALE_RETURN_SQL)
        .bind(&[JsValue::from_f64(now - REDRIVE_STALE_MS as f64)])?
        .counted_all()
        .await?
        .results::<Value>()?
        .len();
    if returned > 0 {
        worker::console_warn!(
            "[owed-events] {returned} re-driven letters neither re-parked nor landed within the hour: parked again"
        );
    }
    let rows = port
        .db
        .prepare(DEAD_LETTERS_READ_SQL)
        .bind(&[JsValue::from_f64(limit as f64)])?
        .counted_all()
        .await?
        .results::<Value>()?;
    let mut keys = Vec::new();
    let mut bodies = Vec::new();
    for r in &rows {
        if let (Some(source), Some(key), Some(b)) = (
            r["source"].as_str(),
            r["sourceKey"].as_str(),
            r["body"].as_str(),
        ) {
            keys.push(json!([source, key]));
            bodies.push(b.to_string());
        }
    }
    let mut redriven = 0usize;
    for (chunk_keys, chunk) in keys
        .chunks(owed_event::SEND_BATCH_MAX)
        .zip(bodies.chunks(owed_event::SEND_BATCH_MAX))
    {
        if let Err(e) = port.send(chunk.to_vec()).await {
            worker::console_warn!(
                "[owed-events] re-drive send faulted ({e}): the letters stay parked"
            );
            break;
        }
        port.db
            .prepare(DEAD_LETTERS_REDRIVEN_SQL)
            .bind(&[
                JsValue::from_str(&serde_json::to_string(chunk_keys).unwrap_or_default()),
                JsValue::from_f64(now),
            ])?
            .counted_run()
            .await?;
        redriven += chunk.len();
    }
    bump(
        env,
        &[
            ("owed_events_redriven_total", redriven as u64),
            ("owed_events_dead_letters_stale_returned_total", returned as u64),
        ],
    )
    .await;
    worker::Response::from_json(
        &json!({"family": "owed-events", "staleReturned": returned, "read": keys.len(), "redriven": redriven, "maxRedrives": MAX_REDRIVES}),
    )
}

/// The counters in one batch, best effort (telemetry only).
pub async fn bump(env: &worker::Env, deltas: &[(&str, u64)]) {
    use worker::wasm_bindgen::JsValue;
    if deltas.is_empty() {
        return;
    }
    let Ok(db) = env.d1("OVERLAY_DB") else {
        return;
    };
    let mut stmts = Vec::new();
    for (name, d) in deltas {
        if let Ok(s) = db
            .prepare(BUMP_SQL)
            .bind(&[JsValue::from_str(name), JsValue::from_f64(*d as f64)])
        {
            stmts.push(s);
        }
    }
    if let Err(e) = crate::d1_ledger::counted_batch(&db, stmts).await {
        worker::console_warn!("[owed-events] counters not written: {e}");
    }
}
