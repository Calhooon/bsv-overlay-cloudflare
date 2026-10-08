//! bsv-low #576 (2026-10-08): THE DEAD LETTERS, PARKED IN D1 AND RE-DRIVEN BY THE OPERATOR.
//!
//! S2's queue replays a faulted `/submit` (and `/arc-ingest`) up to `max_retries` (3) and then dead-letters it
//! (`low-overlay-mutations-dlq`). Since #575 a successor whose predecessor's landing is unknown is "not now" at
//! every replay, so a successor over an ABSENT predecessor dead-letters too. Before this nothing consumed the DLQ: a
//! Worker binding can only send to a queue, and the letters sat there unseen until the platform's retention dropped
//! them.
//!
//! The lifecycle of one LETTER (one row, keyed `(txid, topics)`: the subject txid by the ONE rule, D5, and the
//! sorted topics; a redriven message carries its key, [`RedriveTag`], so a re-death parks the SAME row):
//!  1. `failing`: the main consumer, on every replay it hands back (`retry`), writes the fault text and counts the
//!     attempt ([`note_failing_query`]; the platform does not give a Rust consumer the delivery count). A replay
//!     that is later acked moves the row to `resolved` ([`resolve_query`]).
//!  2. `parked`: the DLQ consumer (the same `#[event(queue)]`, dispatched on the batch's queue name,
//!     [`is_dead_letter_queue`]) writes the message as is and appends one entry to the row's `history`
//!     ([`park_query`]); a row already `parked` is not touched, so a DLQ redelivery parks ONCE and counts once. A
//!     park that faults is `retry`d on the DLQ's own consumer (`max_retries = 10`, no DLQ of its own: eleven D1
//!     faults in a row over the platform's backoff lose the letter, logged with its txid; stated, not covered).
//!  3. `redriven`: `POST /internal/redrive-dead-letters` (bearer `INTERNAL_TOKEN`, [`parse_redrive_request`]) reads
//!     at most `limit` parked rows, oldest first (or one txid's), claims each by a compare-and-set on
//!     `(status = 'parked', redrives = <read>)` ([`claim_query`]) and only then sends it ONCE to the mutations
//!     queue. Two calls racing over one letter enqueue it once: the loser's claim changes nothing. A send that
//!     faults reverts the claim ([`revert_query`]). The message is a FRESH queue message, so its attempt count is
//!     reset to 0 (it gets 1 + `max_retries` deliveries again) and the row's `attempts` restarts at its first
//!     failure.
//!  4. A re-driven letter that fails again parks again (step 2) with its history, and is counted
//!     `dead_letters_still_failing_total`. After [`MAX_REDRIVES`] re-drives it is EXHAUSTED: never selected by the
//!     lever again, and `/health/invariants.deadLetters.exhausted` lists it (the operator's to remove the cause).
//!
//! The table is never wiped and nothing deletes from it: once the DLQ message is acked the row is the only copy of
//! the letter (`storage-ownership.json`: `never_wipe`, rebuild class `lost`). It grows by one row per distinct
//! dead-lettered letter, a fault path.
//!
//! Reference parity: ts-stack's `overlay-express` has no queue and no dead letter (`Engine.submit` catches per topic
//! and never replays); this whole lifecycle is our platform's addition.

use crate::d1::{QVal, Query};
use crate::queue::MutationMessage;
use serde::{Deserialize, Serialize};
use worker::{D1Database, Env, Request, Response, Result};

/// The lever's default batch.
pub const REDRIVE_DEFAULT_LIMIT: u64 = 25;
/// The lever's ceiling per call: two statements and one queue send per letter, inside a Worker's subrequest budget.
pub const REDRIVE_MAX_LIMIT: u64 = 200;
/// Re-drives per letter; past it the letter stays parked and is listed as exhausted.
pub const MAX_REDRIVES: u64 = 3;
/// The reason stamped on a re-driven message.
pub const REASON_REDRIVE: &str = "redrive";
/// `/health/invariants.deadLetters.exhausted` lists at most this many rows.
pub const HEALTH_EXHAUSTED_LIST: u64 = 20;
/// A fault text is kept to this many bytes (a report summary can be long).
pub const FAULT_TEXT_MAX: usize = 1000;
/// The fault of a letter that reached the DLQ with no `failing` row (that write faulted, or a pre-#576 letter).
pub const FAULT_UNRECORDED: &str = "dead-lettered; the last replay's fault was not recorded";

/// Migration: the dead letters. Times are unix ms; `topics` is the sorted, comma-joined topic set; `history` is a
/// JSON array with one entry per park (`parkedAt`, `fault`, `attempts`, `redrive`).
pub const DEAD_LETTERS_CREATE: &str = "CREATE TABLE IF NOT EXISTS mutation_dead_letters (txid TEXT NOT NULL, topics TEXT NOT NULL, message TEXT NOT NULL, fault TEXT, attempts INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, redrives INTEGER NOT NULL DEFAULT 0, first_seen_at INTEGER NOT NULL, parked_at INTEGER, redriven_at INTEGER, resolved_at INTEGER, history TEXT NOT NULL DEFAULT '[]', PRIMARY KEY (txid, topics))";
/// Migration: the lever's oldest-first read and the health block's per-status reads.
pub const DEAD_LETTERS_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS idx_mutation_dead_letters_status ON mutation_dead_letters(status, parked_at)";

/// Binds: txid, topics, message, fault, now. A row of an earlier episode (`resolved`) or a re-driven one starts its
/// attempts again at 1; a resolved row's redrives restart too (a new letter of the same key).
pub const NOTE_FAILING_SQL: &str = "INSERT INTO mutation_dead_letters (txid, topics, message, fault, attempts, status, first_seen_at) VALUES (?, ?, ?, ?, 1, 'failing', ?) \
     ON CONFLICT(txid, topics) DO UPDATE SET message = excluded.message, fault = excluded.fault, \
     attempts = CASE WHEN mutation_dead_letters.status IN ('resolved', 'redriven') THEN 1 ELSE mutation_dead_letters.attempts + 1 END, \
     redrives = CASE WHEN mutation_dead_letters.status = 'resolved' THEN 0 ELSE mutation_dead_letters.redrives END, \
     status = 'failing'";
/// Binds: txid, topics, message, fault (used when the row holds none), redrives (for a fresh row), now (twice).
/// A row already parked is untouched (no row returned): a DLQ redelivery parks once.
pub const PARK_SQL: &str = "INSERT INTO mutation_dead_letters (txid, topics, message, fault, attempts, status, redrives, first_seen_at, parked_at, history) \
     VALUES (?1, ?2, ?3, ?4, 0, 'parked', ?5, ?6, ?6, json_array(json_object('parkedAt', ?6, 'fault', ?4, 'attempts', 0, 'redrive', ?5))) \
     ON CONFLICT(txid, topics) DO UPDATE SET status = 'parked', parked_at = excluded.parked_at, \
     fault = COALESCE(mutation_dead_letters.fault, excluded.fault), \
     history = json_insert(mutation_dead_letters.history, '$[#]', json_object('parkedAt', excluded.parked_at, 'fault', COALESCE(mutation_dead_letters.fault, excluded.fault), 'attempts', mutation_dead_letters.attempts, 'redrive', mutation_dead_letters.redrives)) \
     WHERE mutation_dead_letters.status != 'parked' \
     RETURNING redrives";
/// Binds: now, txid, topics. A letter whose bytes have since landed (its replay, its re-drive, or another copy of it)
/// is resolved; the lever never sends it again.
pub const RESOLVE_SQL: &str = "UPDATE mutation_dead_letters SET status = 'resolved', resolved_at = ? \
     WHERE txid = ? AND topics = ? AND status IN ('failing', 'redriven', 'parked')";
/// Binds: the redrive ceiling, limit. Oldest parked first.
pub const SELECT_PARKED_SQL: &str = "SELECT txid, topics, message, fault, redrives, redriven_at FROM mutation_dead_letters \
     WHERE status = 'parked' AND redrives < ? ORDER BY parked_at, txid, topics LIMIT ?";
/// Binds: the redrive ceiling, txid, limit.
pub const SELECT_PARKED_TXID_SQL: &str = "SELECT txid, topics, message, fault, redrives, redriven_at FROM mutation_dead_letters \
     WHERE status = 'parked' AND redrives < ? AND txid = ? ORDER BY parked_at, topics LIMIT ?";
/// Binds: now, txid, topics, the redrives READ. The compare-and-set: a row returned is this call's to send.
pub const CLAIM_SQL: &str = "UPDATE mutation_dead_letters SET status = 'redriven', redrives = redrives + 1, redriven_at = ? \
     WHERE txid = ? AND topics = ? AND status = 'parked' AND redrives = ? RETURNING redrives";
/// Binds: the previous redriven_at (or NULL), txid, topics, the redrives the claim wrote.
pub const REVERT_SQL: &str = "UPDATE mutation_dead_letters SET status = 'parked', redrives = redrives - 1, redriven_at = ? \
     WHERE txid = ? AND topics = ? AND status = 'redriven' AND redrives = ?";
/// The health block's reads.
pub const HEALTH_COUNTS_SQL: &str =
    "SELECT status, COUNT(*) AS c, MAX(redriven_at) AS last_redrive FROM mutation_dead_letters GROUP BY status";
pub const HEALTH_OLDEST_SQL: &str =
    "SELECT txid, topics, parked_at FROM mutation_dead_letters WHERE status = 'parked' ORDER BY parked_at LIMIT 1";
pub const HEALTH_LAST_REDRIVE_SQL: &str = "SELECT txid, topics, redriven_at FROM mutation_dead_letters WHERE redriven_at IS NOT NULL ORDER BY redriven_at DESC LIMIT 1";
/// Binds: the redrive ceiling, the list cap.
pub const HEALTH_EXHAUSTED_SQL: &str = "SELECT txid, topics, fault, redrives, parked_at FROM mutation_dead_letters \
     WHERE status = 'parked' AND redrives >= ? ORDER BY parked_at LIMIT ?";

/// The key a re-driven message carries, so its re-death parks the same row.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RedriveTag {
    pub txid: String,
    pub topics: String,
    /// The re-drive this message is (1 = the first).
    pub n: u64,
}

/// PURE: a batch from a dead letter queue (both configs name theirs `<queue>-dlq`).
#[must_use]
pub fn is_dead_letter_queue(queue_name: &str) -> bool {
    queue_name.ends_with("-dlq")
}

/// PURE: the topic half of a letter's key: sorted, deduplicated, comma-joined.
#[must_use]
pub fn topics_key(topics: &[String]) -> String {
    let mut t: Vec<&str> = topics.iter().map(String::as_str).collect();
    t.sort_unstable();
    t.dedup();
    t.join(",")
}

/// PURE: the key of the letter `body` is. A re-driven message names its own; otherwise the subject txid, or, for a
/// body whose subject cannot be derived, `unparsed:` and a hash of its bytes (the same in both consumers).
#[must_use]
pub fn letter_key(body: &MutationMessage, subject: Option<&str>) -> (String, String) {
    if let Some(tag) = &body.redrive {
        return (tag.txid.clone(), tag.topics.clone());
    }
    let txid = match subject {
        Some(s) => s.to_ascii_lowercase(),
        None => {
            let h = bsv_rs::primitives::hash::sha256(body.beef_b64.as_bytes());
            format!("unparsed:{}", hex::encode(&h[..16]))
        }
    };
    (txid, topics_key(&body.topics))
}

/// PURE: a fault text bounded to [`FAULT_TEXT_MAX`] bytes, cut on a char boundary.
#[must_use]
pub fn bounded_fault(fault: &str) -> String {
    if fault.len() <= FAULT_TEXT_MAX {
        return fault.to_string();
    }
    let mut end = FAULT_TEXT_MAX;
    while !fault.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &fault[..end])
}

#[must_use]
pub fn note_failing_query(txid: &str, topics: &str, message: &str, fault: &str, now_ms: i64) -> Query {
    Query::new(NOTE_FAILING_SQL)
        .bind(txid)
        .bind(topics)
        .bind(message)
        .bind(bounded_fault(fault))
        .bind(now_ms)
}

/// `redrives` is the count a FRESH row starts at: 0, or [`MAX_REDRIVES`] for a letter that can never be re-driven
/// (its body does not decode), so the lever never selects it and the health block lists it.
#[must_use]
pub fn park_query(txid: &str, topics: &str, message: &str, fault: &str, redrives: u64, now_ms: i64) -> Query {
    Query::new(PARK_SQL)
        .bind(txid)
        .bind(topics)
        .bind(message)
        .bind(bounded_fault(fault))
        .bind(redrives)
        .bind(now_ms)
}

#[must_use]
pub fn resolve_query(txid: &str, topics: &str, now_ms: i64) -> Query {
    Query::new(RESOLVE_SQL).bind(now_ms).bind(txid).bind(topics)
}

#[must_use]
pub fn select_parked_query(req: &RedriveRequest) -> Query {
    match &req.txid {
        Some(t) => Query::new(SELECT_PARKED_TXID_SQL).bind(MAX_REDRIVES).bind(t.as_str()).bind(req.limit),
        None => Query::new(SELECT_PARKED_SQL).bind(MAX_REDRIVES).bind(req.limit),
    }
}

#[must_use]
pub fn claim_query(row: &ParkedRow, now_ms: i64) -> Query {
    Query::new(CLAIM_SQL)
        .bind(now_ms)
        .bind(row.txid.as_str())
        .bind(row.topics.as_str())
        .bind(row.redrives_u64())
}

#[must_use]
pub fn revert_query(row: &ParkedRow) -> Query {
    Query::new(REVERT_SQL)
        .bind(row.redriven_at.map_or(QVal::Null, |v| QVal::Int(v as i64)))
        .bind(row.txid.as_str())
        .bind(row.topics.as_str())
        .bind(row.redrives_u64() + 1)
}

/// `POST /internal/redrive-dead-letters`'s body, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedriveRequest {
    pub limit: u64,
    pub txid: Option<String>,
}

/// PURE: an empty body, or `{"limit"?: n >= 1, "txid"?: "<key>"}`. `limit` defaults to [`REDRIVE_DEFAULT_LIMIT`]
/// and is clamped to [`REDRIVE_MAX_LIMIT`]; `txid` is lowercased. `Err` names what is wrong.
pub fn parse_redrive_request(raw: &[u8]) -> std::result::Result<RedriveRequest, &'static str> {
    if raw.iter().all(u8::is_ascii_whitespace) {
        return Ok(RedriveRequest { limit: REDRIVE_DEFAULT_LIMIT, txid: None });
    }
    let v: serde_json::Value = serde_json::from_slice(raw).map_err(|_| "body must be JSON")?;
    let obj = v.as_object().ok_or("body must be a JSON object")?;
    let limit = match obj.get("limit") {
        None | Some(serde_json::Value::Null) => REDRIVE_DEFAULT_LIMIT,
        Some(l) => l.as_u64().filter(|l| *l >= 1).ok_or("limit must be an integer >= 1")?.min(REDRIVE_MAX_LIMIT),
    };
    let txid = match obj.get("txid") {
        None | Some(serde_json::Value::Null) => None,
        Some(t) => {
            let t = t.as_str().map(str::trim).filter(|t| !t.is_empty() && t.len() <= 128).ok_or("txid must be a non-empty string")?;
            Some(t.to_ascii_lowercase())
        }
    };
    Ok(RedriveRequest { limit, txid })
}

/// One parked row the lever read.
#[derive(Deserialize, Debug, Clone)]
pub struct ParkedRow {
    pub txid: String,
    pub topics: String,
    pub message: String,
    pub fault: Option<String>,
    pub redrives: f64,
    pub redriven_at: Option<f64>,
}

impl ParkedRow {
    fn redrives_u64(&self) -> u64 {
        self.redrives.max(0.0) as u64
    }
}

/// PURE: the message a parked row re-drives as (`None` when its body does not decode): its own bytes, topics and
/// mode, the reason [`REASON_REDRIVE`] and its key with the re-drive's number.
#[must_use]
pub fn redrive_message(row: &ParkedRow) -> Option<MutationMessage> {
    let mut msg: MutationMessage = serde_json::from_str(&row.message).ok()?;
    msg.reason = REASON_REDRIVE.to_string();
    msg.redrive = Some(RedriveTag { txid: row.txid.clone(), topics: row.topics.clone(), n: row.redrives_u64() + 1 });
    Some(msg)
}

#[derive(Deserialize)]
struct RedrivesRow {
    redrives: f64,
}

/// The main consumer, before it hands a replay back: the letter's fault and attempt. Fail-soft (logged): a lost
/// note leaves the park's [`FAULT_UNRECORDED`].
pub async fn note_failing(db: &D1Database, body: &MutationMessage, subject: Option<&str>, fault: &str) {
    let (txid, topics) = letter_key(body, subject);
    let message = serde_json::to_string(body).unwrap_or_default();
    let now = worker::Date::now().as_millis() as i64;
    if let Err(e) = note_failing_query(&txid, &topics, &message, fault, now).execute(db).await {
        worker::console_log!("[dead-letters] the failing note of {txid} [{topics}] faulted ({e}); its park will say the fault was not recorded");
    }
}

/// The main consumer, on an ack: the letter's bytes landed (or need no replay). Fail-soft (logged): a row left
/// `failing` or `redriven` is shown by the health block and parks again only if a copy of it dead-letters.
pub async fn resolve(db: &D1Database, body: &MutationMessage, subject: Option<&str>) {
    let (txid, topics) = letter_key(body, subject);
    let now = worker::Date::now().as_millis() as i64;
    if let Err(e) = resolve_query(&txid, &topics, now).execute(db).await {
        worker::console_log!("[dead-letters] the resolve of {txid} [{topics}] faulted ({e})");
    }
}

/// The DLQ consumer: park every message of `batch` (ack on a landed park, `retry` on a fault).
pub async fn park_batch(batch: &worker::MessageBatch<MutationMessage>, env: &Env) -> Result<()> {
    use worker::MessageExt;
    let queue = batch.queue();
    let db = env.d1("OVERLAY_DB")?;
    crate::d1::ensure_overlay_migrations(&db).await.map_err(worker::Error::from)?;
    for msg in batch.raw_iter() {
        let raw = msg.body();
        let now = worker::Date::now().as_millis() as i64;
        let (txid, topics, message, fault, start_redrives) =
            match worker::serde_wasm_bindgen::from_value::<MutationMessage>(raw.clone()) {
                Ok(body) => {
                    let subject = subject_of(&body);
                    let (txid, topics) = letter_key(&body, subject.as_deref());
                    (txid, topics, serde_json::to_string(&body).unwrap_or_default(), FAULT_UNRECORDED.to_string(), 0)
                }
                Err(e) => {
                    let text = worker::js_sys::JSON::stringify(&raw).map(String::from).unwrap_or_default();
                    (
                        format!("undecodable:{}", msg.id()),
                        String::new(),
                        text,
                        format!("the dead letter does not decode as a mutation message ({e}); never re-drivable"),
                        MAX_REDRIVES,
                    )
                }
            };
        match park_query(&txid, &topics, &message, &fault, start_redrives, now).fetch_all::<RedrivesRow>(&db).await {
            Ok(rows) => {
                if let Some(r) = rows.first() {
                    crate::ops::bump_counter(&db, crate::ops::COUNTER_DEAD_LETTERS_PARKED, 1).await;
                    let redrives = r.redrives.max(0.0) as u64;
                    if redrives > 0 && start_redrives == 0 {
                        crate::ops::bump_counter(&db, crate::ops::COUNTER_DEAD_LETTERS_STILL_FAILING, 1).await;
                    }
                    worker::console_log!(
                        "[dead-letters] PARKED {txid} [{topics}] from {queue} (re-drives so far {redrives}/{MAX_REDRIVES}){}",
                        if redrives >= MAX_REDRIVES { "; EXHAUSTED, the lever will not re-drive it" } else { "" }
                    );
                } else {
                    worker::console_log!("[dead-letters] {txid} [{topics}] was already parked (a DLQ redelivery): acked");
                }
                msg.ack();
            }
            Err(e) => {
                worker::console_log!("[dead-letters] the park of {txid} [{topics}] faulted ({e}); retrying the dead letter");
                msg.retry();
            }
        }
    }
    Ok(())
}

/// The subject by the ONE rule (D5), as the main consumer derives it.
#[must_use]
pub fn subject_of(body: &MutationMessage) -> Option<String> {
    use base64::{engine::general_purpose::STANDARD, Engine as B64Engine};
    let beef = STANDARD.decode(&body.beef_b64).ok()?;
    let mut named = bsv_rs::transaction::beef::Beef::from_binary(&beef).ok()?;
    crate::ef::subject_txid_of(&mut named)
}

/// `POST /internal/redrive-dead-letters` (bearer `INTERNAL_TOKEN`, as `/internal/reorg`): see the module doc.
pub async fn internal_redrive(mut req: Request, env: &Env) -> Result<Response> {
    let authorization = req.headers().get("authorization").ok().flatten();
    let secret = env.secret("INTERNAL_TOKEN").ok().map(|s| s.to_string());
    if !crate::tip_pass::bearer_ok(authorization.as_deref(), secret.as_deref()) {
        worker::console_log!("POST /internal/redrive-dead-letters -> 401");
        return Response::error("unauthorized", 401);
    }
    let raw = req.bytes().await?;
    let parsed = match parse_redrive_request(&raw) {
        Ok(p) => p,
        Err(why) => return Response::error(format!("{why}: {{\"limit\"?: 1..{REDRIVE_MAX_LIMIT}, \"txid\"?: \"<txid>\"}}"), 400),
    };
    let db = env.d1("OVERLAY_DB")?;
    crate::d1::ensure_overlay_migrations(&db).await.map_err(worker::Error::from)?;
    let rows: Vec<ParkedRow> = match select_parked_query(&parsed).fetch_all(&db).await {
        Ok(r) => r,
        Err(e) => return Response::error(format!("the dead letters could not be read: {e}"), 502),
    };
    let queue = env.queue("MUTATION_QUEUE")?;
    let mut moved = Vec::new();
    let mut skipped = Vec::new();
    let mut faults = Vec::new();
    for row in &rows {
        let Some(msg) = redrive_message(row) else {
            skipped.push(serde_json::json!({"txid": row.txid, "topics": row.topics, "why": "its message does not decode"}));
            continue;
        };
        let now = worker::Date::now().as_millis() as i64;
        match claim_query(row, now).fetch_all::<RedrivesRow>(&db).await {
            Ok(claimed) if claimed.is_empty() => {
                skipped.push(serde_json::json!({"txid": row.txid, "topics": row.topics, "why": "claimed by another call"}));
                continue;
            }
            Ok(_) => {}
            Err(e) => {
                faults.push(serde_json::json!({"txid": row.txid, "topics": row.topics, "fault": format!("claim: {e}")}));
                continue;
            }
        }
        let n = row.redrives_u64() + 1;
        match queue.send(msg).await {
            Ok(()) => {
                crate::ops::bump_counter(&db, crate::ops::COUNTER_DEAD_LETTERS_REDRIVEN, 1).await;
                worker::console_log!(
                    "POST /internal/redrive-dead-letters: re-drove {} [{}] (re-drive {n}/{MAX_REDRIVES}; its last fault: {})",
                    row.txid,
                    row.topics,
                    row.fault.as_deref().unwrap_or("none recorded")
                );
                moved.push(serde_json::json!({
                    "txid": row.txid, "topics": row.topics, "redrive": n, "fault": row.fault, "redrivenAt": now,
                }));
            }
            Err(e) => {
                let reverted = revert_query(row).execute(&db).await;
                worker::console_log!(
                    "POST /internal/redrive-dead-letters: the send of {} [{}] faulted ({e}); claim reverted: {}",
                    row.txid,
                    row.topics,
                    reverted.is_ok()
                );
                faults.push(serde_json::json!({
                    "txid": row.txid, "topics": row.topics, "fault": format!("send: {e}"), "reverted": reverted.is_ok(),
                }));
            }
        }
    }
    worker::console_log!(
        "POST /internal/redrive-dead-letters limit={} -> 200 (read={} redriven={} skipped={} faults={})",
        parsed.limit,
        rows.len(),
        moved.len(),
        skipped.len(),
        faults.len()
    );
    Response::from_json(&serde_json::json!({
        "ok": true,
        "limit": parsed.limit,
        "txid": parsed.txid,
        "read": rows.len(),
        "redriven": moved,
        "skipped": skipped,
        "faults": faults,
        "maxRedrives": MAX_REDRIVES,
    }))
}

#[derive(Deserialize)]
struct StatusCountRow {
    status: String,
    c: f64,
    last_redrive: Option<f64>,
}

#[derive(Deserialize)]
struct KeyAtRow {
    txid: String,
    topics: String,
    #[serde(alias = "parked_at", alias = "redriven_at")]
    at: Option<f64>,
}

#[derive(Deserialize)]
struct ExhaustedRow {
    txid: String,
    topics: String,
    fault: Option<String>,
    redrives: f64,
    parked_at: Option<f64>,
}

/// `/health/invariants.deadLetters`: the count by status, the oldest parked, the last re-drive, the exhausted
/// letters. `readable: false` when the table cannot be read (a pre-migration isolate), distinct from an empty one.
pub async fn health_json(db: &D1Database) -> serde_json::Value {
    let Ok(counts) = Query::new(HEALTH_COUNTS_SQL).fetch_all::<StatusCountRow>(db).await else {
        return serde_json::json!({"readable": false});
    };
    let count = |s: &str| counts.iter().find(|r| r.status == s).map_or(0, |r| r.c.max(0.0) as u64);
    let oldest = Query::new(HEALTH_OLDEST_SQL).fetch_optional::<KeyAtRow>(db).await.ok().flatten();
    let last = if counts.iter().any(|r| r.last_redrive.is_some()) {
        Query::new(HEALTH_LAST_REDRIVE_SQL).fetch_optional::<KeyAtRow>(db).await.ok().flatten()
    } else {
        None
    };
    let exhausted: Vec<ExhaustedRow> = if count("parked") > 0 {
        Query::new(HEALTH_EXHAUSTED_SQL)
            .bind(MAX_REDRIVES)
            .bind(HEALTH_EXHAUSTED_LIST)
            .fetch_all(db)
            .await
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let key_at = |r: &KeyAtRow| serde_json::json!({"txid": r.txid, "topics": r.topics, "at": r.at.map(|v| v as i64)});
    serde_json::json!({
        "readable": true,
        "parked": count("parked"),
        "failing": count("failing"),
        "redriven": count("redriven"),
        "resolved": count("resolved"),
        "oldestParked": oldest.as_ref().map(key_at),
        "lastRedrive": last.as_ref().map(key_at),
        "maxRedrives": MAX_REDRIVES,
        "exhausted": exhausted.iter().map(|r| serde_json::json!({
            "txid": r.txid, "topics": r.topics, "fault": r.fault,
            "redrives": r.redrives.max(0.0) as u64, "parkedAt": r.parked_at.map(|v| v as i64),
        })).collect::<Vec<_>>(),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

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

    fn db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute(DEAD_LETTERS_CREATE, []).unwrap();
        conn.execute(DEAD_LETTERS_INDEX, []).unwrap();
        conn
    }

    /// Run a statement; the rows it RETURNs (a write without RETURNING answers none).
    fn run(conn: &rusqlite::Connection, q: &Query) -> usize {
        let mut stmt = conn.prepare(q.sql()).unwrap();
        if stmt.column_count() == 0 {
            return stmt.execute(rusqlite::params_from_iter(binds(q).iter())).unwrap();
        }
        let mut rows = stmt.query(rusqlite::params_from_iter(binds(q).iter())).unwrap();
        let mut n = 0;
        while rows.next().unwrap().is_some() {
            n += 1;
        }
        n
    }

    fn parked(conn: &rusqlite::Connection, req: &RedriveRequest) -> Vec<ParkedRow> {
        let q = select_parked_query(req);
        let mut stmt = conn.prepare(q.sql()).unwrap();
        stmt.query_map(rusqlite::params_from_iter(binds(&q).iter()), |r| {
            Ok(ParkedRow {
                txid: r.get(0)?,
                topics: r.get(1)?,
                message: r.get(2)?,
                fault: r.get(3)?,
                redrives: r.get::<_, i64>(4)? as f64,
                redriven_at: r.get::<_, Option<i64>>(5)?.map(|v| v as f64),
            })
        })
        .unwrap()
        .map(Result::unwrap)
        .collect()
    }

    fn row(conn: &rusqlite::Connection, txid: &str) -> (String, i64, i64, Option<String>, String) {
        conn.query_row(
            "SELECT status, attempts, redrives, fault, history FROM mutation_dead_letters WHERE txid = ?1",
            [txid],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .unwrap()
    }

    fn msg(beef: &str, topics: &[&str]) -> MutationMessage {
        MutationMessage {
            beef_b64: beef.to_string(),
            topics: topics.iter().map(|t| (*t).to_string()).collect(),
            mode: "historical-tx".to_string(),
            reason: "phase3-fault".to_string(),
            redrive: None,
        }
    }

    /// Park `txid` as the DLQ consumer does (a failing note first when `fault` is given); the rows returned.
    fn dead_letter(conn: &rusqlite::Connection, txid: &str, fault: Option<&str>, now: i64) -> usize {
        let body = msg("AA==", &["tm_b", "tm_a"]);
        let (k, t) = letter_key(&body, Some(txid));
        let m = serde_json::to_string(&body).unwrap();
        if let Some(f) = fault {
            run(conn, &note_failing_query(&k, &t, &m, f, now - 10));
        }
        run(conn, &park_query(&k, &t, &m, FAULT_UNRECORDED, 0, now))
    }

    /// One lever pass as `internal_redrive` runs it (the send always lands): the rows it moved.
    fn lever(conn: &rusqlite::Connection, req: &RedriveRequest, now: i64) -> Vec<MutationMessage> {
        let mut sent = Vec::new();
        for r in parked(conn, req) {
            let m = redrive_message(&r).expect("decodes");
            if run(conn, &claim_query(&r, now)) == 1 {
                sent.push(m);
            }
        }
        sent
    }

    fn all(limit: u64) -> RedriveRequest {
        RedriveRequest { limit, txid: None }
    }

    #[test]
    fn e576_a_dead_letter_is_parked_with_its_fault_once() {
        let conn = db();
        assert_eq!(dead_letter(&conn, "aa", Some("predecessor_not_landed: tm_a"), 1_000), 1, "parked: one row returned");
        let (status, attempts, redrives, fault, history) = row(&conn, "aa");
        assert_eq!((status.as_str(), attempts, redrives), ("parked", 1, 0));
        assert_eq!(fault.as_deref(), Some("predecessor_not_landed: tm_a"));
        let h: serde_json::Value = serde_json::from_str(&history).unwrap();
        assert_eq!(h.as_array().unwrap().len(), 1);
        assert_eq!(h[0]["fault"], "predecessor_not_landed: tm_a");
        assert_eq!(h[0]["parkedAt"], 1_000);
        // a DLQ redelivery of the same letter parks nothing more (counted once)
        assert_eq!(dead_letter(&conn, "aa", None, 2_000), 0);
        let (_, _, _, _, history) = row(&conn, "aa");
        assert_eq!(serde_json::from_str::<serde_json::Value>(&history).unwrap().as_array().unwrap().len(), 1);
        // a letter with no failing note is parked with the stated text
        assert_eq!(dead_letter(&conn, "bb", None, 3_000), 1);
        assert_eq!(row(&conn, "bb").3.as_deref(), Some(FAULT_UNRECORDED));
        // the key: sorted topics; the message kept as is
        let stored: String =
            conn.query_row("SELECT topics || '|' || message FROM mutation_dead_letters WHERE txid = 'aa'", [], |r| r.get(0)).unwrap();
        assert!(stored.starts_with("tm_a,tm_b|{"), "{stored}");
    }

    #[test]
    fn e576_the_lever_moves_n_and_no_more_oldest_first_or_by_txid() {
        let conn = db();
        for (i, t) in ["t3", "t1", "t2", "t4"].iter().enumerate() {
            dead_letter(&conn, t, Some("x"), 1_000 + [30, 10, 20, 40][i]);
        }
        let sent = lever(&conn, &all(2), 5_000);
        let keys: Vec<String> = sent.iter().map(|m| m.redrive.clone().unwrap().txid).collect();
        assert_eq!(keys, vec!["t1", "t2"], "the two OLDEST, no more");
        assert_eq!(row(&conn, "t1").0, "redriven");
        assert_eq!(row(&conn, "t3").0, "parked");
        let one = lever(&conn, &RedriveRequest { limit: 25, txid: Some("t4".into()) }, 6_000);
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].redrive.as_ref().unwrap().txid, "t4");
        assert_eq!(row(&conn, "t3").0, "parked", "by txid moves that letter alone");
        // the re-driven message: the same bytes, topics and mode, its key and number, the reason
        let m = &sent[0];
        assert_eq!((m.beef_b64.as_str(), m.mode.as_str(), m.reason.as_str()), ("AA==", "historical-tx", REASON_REDRIVE));
        assert_eq!(m.redrive, Some(RedriveTag { txid: "t1".into(), topics: "tm_a,tm_b".into(), n: 1 }));
    }

    #[test]
    fn e576_the_same_letter_redriven_twice_is_one_enqueue() {
        let conn = db();
        dead_letter(&conn, "aa", Some("x"), 1_000);
        // two calls that both READ the row before either claims it
        let a = parked(&conn, &all(25));
        let b = parked(&conn, &all(25));
        assert_eq!(run(&conn, &claim_query(&a[0], 2_000)), 1, "the first claim wins");
        assert_eq!(run(&conn, &claim_query(&b[0], 2_001)), 0, "the second changes nothing: no second send");
        assert!(lever(&conn, &all(25), 3_000).is_empty(), "a later call finds nothing parked");
        assert_eq!(row(&conn, "aa").2, 1);
    }

    #[test]
    fn e576_a_send_fault_reverts_the_claim() {
        let conn = db();
        dead_letter(&conn, "aa", Some("x"), 1_000);
        let r = parked(&conn, &all(25)).remove(0);
        assert_eq!(run(&conn, &claim_query(&r, 2_000)), 1);
        assert_eq!(run(&conn, &revert_query(&r)), 1);
        let (status, _, redrives, _, _) = row(&conn, "aa");
        assert_eq!((status.as_str(), redrives), ("parked", 0));
        let at: Option<i64> = conn.query_row("SELECT redriven_at FROM mutation_dead_letters", [], |r| r.get(0)).unwrap();
        assert_eq!(at, None);
    }

    #[test]
    fn e576_a_redriven_letter_that_fails_again_parks_again_with_its_history_up_to_the_ceiling() {
        let conn = db();
        dead_letter(&conn, "aa", Some("fault 0"), 1_000);
        for n in 1..=MAX_REDRIVES {
            let sent = lever(&conn, &all(25), 1_000 + n as i64 * 100);
            assert_eq!(sent.len(), 1, "re-drive {n}");
            let m = &sent[0];
            assert_eq!(m.redrive.as_ref().unwrap().n, n);
            // the replay fails: the consumer keys it by the tag, whatever the bytes derive to
            let (k, t) = letter_key(m, None);
            assert_eq!(k, "aa");
            let body = serde_json::to_string(m).unwrap();
            run(&conn, &note_failing_query(&k, &t, &body, &format!("fault {n}"), 1_050 + n as i64 * 100));
            assert_eq!(row(&conn, "aa").1, 1, "the attempts restart with the fresh message");
            assert_eq!(run(&conn, &park_query(&k, &t, &body, FAULT_UNRECORDED, 0, 1_080 + n as i64 * 100)), 1);
        }
        let (status, _, redrives, _, history) = row(&conn, "aa");
        assert_eq!((status.as_str(), redrives), ("parked", MAX_REDRIVES as i64));
        let h: serde_json::Value = serde_json::from_str(&history).unwrap();
        let faults: Vec<&str> = h.as_array().unwrap().iter().map(|e| e["fault"].as_str().unwrap()).collect();
        assert_eq!(faults, vec!["fault 0", "fault 1", "fault 2", "fault 3"]);
        assert_eq!(h[3]["redrive"], 3);
        // past the ceiling the lever never selects it; the health read lists it
        assert!(lever(&conn, &all(200), 9_000).is_empty());
        let q = Query::new(HEALTH_EXHAUSTED_SQL).bind(MAX_REDRIVES).bind(HEALTH_EXHAUSTED_LIST);
        assert_eq!(run(&conn, &q), 1);
    }

    #[test]
    fn e576_an_ack_resolves_and_a_resolved_letter_is_never_sent() {
        let conn = db();
        dead_letter(&conn, "aa", Some("x"), 1_000);
        let m = lever(&conn, &all(25), 2_000).remove(0);
        let (k, t) = letter_key(&m, None);
        assert_eq!(run(&conn, &resolve_query(&k, &t, 3_000)), 1);
        assert_eq!(row(&conn, "aa").0, "resolved");
        assert!(lever(&conn, &all(25), 4_000).is_empty());
        // a failing replay that is then acked: resolved, never parked
        let body = msg("BB==", &["tm_a"]);
        let (k, t) = letter_key(&body, Some("CC"));
        assert_eq!(k, "cc", "the subject is lowercased");
        run(&conn, &note_failing_query(&k, &t, "{}", "x", 1));
        run(&conn, &note_failing_query(&k, &t, "{}", "y", 2));
        assert_eq!(row(&conn, "cc").1, 2);
        run(&conn, &resolve_query(&k, &t, 3));
        assert_eq!(row(&conn, "cc").0, "resolved");
        // a new episode of a resolved key starts from nothing
        run(&conn, &note_failing_query(&k, &t, "{}", "z", 4));
        let (status, attempts, redrives, _, _) = row(&conn, "cc");
        assert_eq!((status.as_str(), attempts, redrives), ("failing", 1, 0));
    }

    #[test]
    fn e576_an_undecodable_letter_is_parked_exhausted() {
        let conn = db();
        assert_eq!(run(&conn, &park_query("undecodable:id1", "", "\"junk\"", "does not decode", MAX_REDRIVES, 5)), 1);
        assert!(lever(&conn, &all(25), 6).is_empty(), "never selected, never blocks the oldest-first read");
        assert_eq!(row(&conn, "undecodable:id1").2, MAX_REDRIVES as i64);
    }

    #[test]
    fn e576_the_request_parse_defaults_clamps_and_refuses() {
        assert_eq!(parse_redrive_request(b"").unwrap(), all(REDRIVE_DEFAULT_LIMIT));
        assert_eq!(parse_redrive_request(b"{}").unwrap(), all(25));
        assert_eq!(parse_redrive_request(br#"{"limit": 7}"#).unwrap(), all(7));
        assert_eq!(parse_redrive_request(br#"{"limit": 5000}"#).unwrap(), all(REDRIVE_MAX_LIMIT));
        assert_eq!(
            parse_redrive_request(br#"{"txid": " ABCD "}"#).unwrap(),
            RedriveRequest { limit: 25, txid: Some("abcd".into()) }
        );
        for bad in [&br#"{"limit": 0}"#[..], br#"{"limit": -1}"#, br#"{"limit": "5"}"#, br#"{"txid": ""}"#, b"[1]", b"nope"] {
            assert!(parse_redrive_request(bad).is_err(), "{}", String::from_utf8_lossy(bad));
        }
    }

    #[test]
    fn e576_keys_and_queue_names() {
        assert!(is_dead_letter_queue("low-overlay-mutations-dlq"));
        assert!(is_dead_letter_queue("low-overlay-mutations-beta-dlq"));
        assert!(is_dead_letter_queue("overlay-mutations-dlq"));
        assert!(!is_dead_letter_queue("low-overlay-mutations"));
        assert!(!is_dead_letter_queue("low-overlay-mutations-beta"));
        let a = msg("AA==", &["b", "a", "b"]);
        assert_eq!(letter_key(&a, Some("ff")), ("ff".into(), "a,b".into()));
        let (u, _) = letter_key(&a, None);
        assert!(u.starts_with("unparsed:") && u.len() == "unparsed:".len() + 32, "{u}");
        assert_eq!(letter_key(&a, None).0, u, "deterministic in both consumers");
        let long = "é".repeat(FAULT_TEXT_MAX);
        assert!(bounded_fault(&long).len() <= FAULT_TEXT_MAX + "…".len());
    }

    /// The configs: both LOW environments' DLQs are consumed, by the queue names their producers dead-letter to.
    #[test]
    fn e576_both_low_configs_consume_their_dead_letter_queue() {
        let low = include_str!("../wrangler.low.toml");
        for (dlq, table) in [
            ("low-overlay-mutations-dlq", "[[queues.consumers]]"),
            ("low-overlay-mutations-beta-dlq", "[[env.beta.queues.consumers]]"),
        ] {
            assert!(low.contains(&format!("dead_letter_queue = \"{dlq}\"")), "the producer dead-letters to {dlq}");
            let consumer = format!("{table}\nqueue = \"{dlq}\"");
            assert!(low.contains(&consumer), "{dlq} has a consumer");
        }
        let generic = include_str!("../wrangler.toml");
        assert!(generic.contains("dead_letter_queue = \"overlay-mutations-dlq\""));
        assert!(generic.contains("[[queues.consumers]]\nqueue = \"overlay-mutations-dlq\""));
    }

    /// The main consumer notes a fault before EVERY hand-back and resolves at every ack (a source-shape pin: the
    /// queue handler runs only in wasm).
    #[test]
    fn e576_the_main_consumer_notes_each_retry_and_resolves_each_ack() {
        let src = include_str!("lib.rs");
        let start = src.find("async fn queue_handler(").unwrap();
        let body = &src[start..start + src[start..].find("\n}\n").unwrap()];
        assert!(body.contains("crate::dead_letters::is_dead_letter_queue(&batch.queue())"));
        let retries = body.matches("msg.retry();").count();
        let notes = body.matches("crate::dead_letters::note_failing(").count();
        assert!(retries >= 5);
        assert_eq!(notes, retries, "one fault note per hand-back");
        let resolves = body.matches("crate::dead_letters::resolve(").count();
        assert_eq!(resolves, 3, "the eviction skip, the re-eviction and the durable ack");
    }
}
