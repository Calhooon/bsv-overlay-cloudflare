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
//!     attempt ([`note_failing_query`]). The note holds NO message bytes (the lens fold, M2): the queue still holds
//!     them, and the DLQ consumer writes them if the letter dies. A note on a `parked` row changes nothing; on a
//!     `redriven` row it keeps the status (the stale rule, step 3, still sees it). An ack of the replay DELETES the
//!     row, whatever its status ([`resolve_query`]): its bytes landed, or were refused under an open eviction
//!     ([`Resolved`] says which, in the log line; the delta fold, D-L3), and the row is a copy of nothing (a scoped
//!     delete of `storage-ownership.json`); a parked or re-driven letter so resolved is counted
//!     `dead_letters_resolved_total`.
//!  2. `parked`: the DLQ consumer (its own `queue` export in `lib.rs`, dispatched on the batch's queue name,
//!     [`is_dead_letter_queue`]) writes the message as is and appends one entry to the row's `history`
//!     ([`park_query`]; at most [`HISTORY_MAX`] entries, the oldest dropped). PARK FIRST, ACK AFTER: a message is
//!     acked only once its park statement answered, so a park that faults never acks. A DLQ redelivery of the same
//!     bytes parks ONCE and counts once; ANOTHER copy of a parked key (a resubmit, a different carried ancestry, the
//!     lens fold's L2) keeps the LONGER bytes (the copy that carries more) and leaves a `copy` entry in the history.
//!     A park that faults is handed back with a delay that grows with the platform's own delivery count
//!     ([`dlq_retry_plan`]: 60 s doubling to 30 min, [`DLQ_MAX_RETRIES`] retries, about 48 h; the lens fold, H1), and
//!     the LAST delivery's fault is logged `[dead-letters] LOST` with the key and the sha256 of the bytes and counted
//!     `dead_letters_lost_total` (best effort: D1 is usually what faulted).
//!  3. `redriven`: `POST /internal/redrive-dead-letters` (bearer `INTERNAL_TOKEN`, [`parse_redrive_request`]) first
//!     returns every re-drive claimed more than [`STALE_REDRIVE_MS`] ago that neither resolved nor parked again to
//!     `parked` (a send that never left, an isolate that died after its claim, a re-driven message the platform or
//!     the main consumer dropped; the lens fold, M1; [`stale_return_query`], counted
//!     `dead_letters_stale_returned_total`; the spent re-drive stays spent). It then reads at most `limit` parked
//!     rows, oldest first (or one txid's), claims each by a compare-and-set on `(status = 'parked', redrives =
//!     <read>)` ([`claim_query`], which RETURNS the bytes it claimed, L3) and only then sends those bytes ONCE to the
//!     mutations queue. Two calls racing over one letter enqueue it once: the loser's claim changes nothing. A send
//!     that faults reverts the claim ([`revert_query`]). The message is a FRESH queue message, so its attempt count
//!     is reset to 0 (it gets 1 + `max_retries` deliveries again) and so is the row's `attempts`.
//!  4. A re-driven letter that fails again parks again (step 2) with its history, and is counted
//!     `dead_letters_still_failing_total`. After [`MAX_REDRIVES`] re-drives it is EXHAUSTED: never selected by the
//!     lever again, and `/health/invariants.deadLetters.exhausted` lists it; once the operator removed its cause,
//!     `{"txid": k, "force": true}` re-drives it once more, recorded in its history (L5).
//!
//!  5. `discarded`: `POST /internal/discard-dead-letters` (the same bearer, [`parse_discard_request`]; the delta
//!     fold, D-M1) deletes named PARKED letters, at most [`DISCARD_MAX_LETTERS`] per call, by key (a txid, and its
//!     topics or every parked row of it), each logged `[dead-letters] DISCARDED` with the sha256 of its bytes and
//!     counted `dead_letters_discarded_total`. It is the operator's way to make room under the ceiling: without it
//!     a letter that can never land (exhausted, `undecodable:`, `unparsed:`, a body the engine refuses for good)
//!     held its place for ever, and once [`PARKED_ROWS_CEILING`] of them had collected every new letter was lost.
//!     A `redriven` letter (in flight) and a `failing` note are not discarded.
//!
//! The table is never wiped: once the DLQ message is acked the row is the only copy of the letter
//! (`storage-ownership.json`: `never_wipe`, rebuild class `lost`); its two DELETEs are the ack of a replay and the
//! operator's discard by key. Its
//! size is bounded (M2): a `failing` note is a key, a fault of at most [`FAULT_TEXT_MAX`] bytes and four integers;
//! a letter's bytes are one queue message (the platform's 128 KB message limit) and a history of at most
//! [`HISTORY_MAX`] entries; and at most [`PARKED_ROWS_CEILING`] letters hold bytes (`parked` or `redriven`): a NEW
//! letter past it is not parked, it is handed back to the DLQ with the same backoff, counted (the letter once,
//! `dead_letters_ceiling_deferred_total`, on its first DLQ delivery; every deferral,
//! `dead_letters_ceiling_deferrals_total`; the delta fold, D-L1) and shown in the health block (a flood of a
//! stranger's "not now" bodies, CLAUDE.md's limit (4), fills the ceiling and not the database; past ~48 h at the
//! ceiling a deferred letter is LOST, logged as above). The ceiling does not drain by itself: only an ack and the
//! operator's discard (step 5) take a letter out; the health block says `near` from [`CEILING_NEAR`] letters on
//! and the DLQ consumer logs each park at or past it. `failing` notes are not under the ceiling: one per key in flight, deleted at its ack or
//! promoted by its park; one whose message the platform dropped on the main queue stays, a small row, counted in the
//! health block.
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
/// `/health/invariants.deadLetters.exhausted` lists at most this many rows (`exhaustedCount` is the total).
pub const HEALTH_EXHAUSTED_LIST: u64 = 20;
/// A fault text is kept to this many bytes (a report summary can be long).
pub const FAULT_TEXT_MAX: usize = 1000;
/// The fault of a letter that reached the DLQ with no `failing` row (that write faulted, or a pre-#576 letter).
pub const FAULT_UNRECORDED: &str = "dead-lettered; the last replay's fault was not recorded";
/// A letter's history keeps its last this many entries (the lens fold, M2: a key re-presented forever cannot grow
/// its row toward D1's row cap).
pub const HISTORY_MAX: u64 = 20;
/// At most this many letters hold bytes (`parked` + `redriven`); a NEW letter past it is deferred (M2). About
/// 2000 × (128 KB + 22 KB of history) ≈ 300 MB at the very worst, in the ONE shared `OVERLAY_DB`.
pub const PARKED_ROWS_CEILING: u64 = 2000;
/// From this many letters with bytes the health block says the ceiling is `near` and each park logs it (D-M1): 80 %
/// of [`PARKED_ROWS_CEILING`], 400 letters of room for the operator to discard what can never land.
pub const CEILING_NEAR: u64 = PARKED_ROWS_CEILING / 5 * 4;
/// The discard lever's ceiling per call (D-M1): its statement returns each letter's bytes, to log their hash.
pub const DISCARD_MAX_LETTERS: usize = 50;
/// A re-drive claimed this long ago that neither resolved nor parked again is STALE: the lever returns it to the
/// parked set (M1). The main queue's 1 + 3 deliveries of a re-driven message and its hop to the DLQ take seconds to
/// minutes; a stale verdict that is wrong (the re-driven copy is still in flight, e.g. a DLQ park riding out a D1
/// outage) sends a second copy, which is dedup-safe (idempotent submit, H2's applied row; the lens's N2).
pub const STALE_REDRIVE_MS: i64 = 3_600_000;
/// The DLQ consumer's `max_retries` in EVERY config (pinned by `e576f_h1_every_dlq_consumer_rides_out_an_hour`):
/// the platform's ceiling (Cloudflare Queues limits: "Message retries | 100").
pub const DLQ_MAX_RETRIES: u32 = 100;
const _: () = assert!(
    DLQ_MAX_RETRIES <= 100,
    "Cloudflare Queues limits: message retries 100"
);
/// The DLQ consumer's `retry_delay` in every config: the delay of a retry the code did not time itself (a batch the
/// handler threw on). Cloudflare documents no default (`retry_delay` is absent unless set), and an unset one
/// redelivers on the next batch: the lens's H1.
pub const DLQ_RETRY_DELAY_S: u32 = 300;
/// The backoff of a failed park: `DLQ_BACKOFF_BASE_S · 2^(attempts-1)`, capped at [`DLQ_BACKOFF_CAP_S`].
pub const DLQ_BACKOFF_BASE_S: u32 = 60;
pub const DLQ_BACKOFF_CAP_S: u32 = 1800;

/// Migration: the dead letters. Times are unix ms; `topics` is the sorted, comma-joined topic set; `history` is a
/// JSON array with one entry per park (`parkedAt`, `fault`, `attempts`, `redrive`, `kind`).
pub const DEAD_LETTERS_CREATE: &str = "CREATE TABLE IF NOT EXISTS mutation_dead_letters (txid TEXT NOT NULL, topics TEXT NOT NULL, message TEXT NOT NULL, fault TEXT, attempts INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, redrives INTEGER NOT NULL DEFAULT 0, first_seen_at INTEGER NOT NULL, parked_at INTEGER, redriven_at INTEGER, resolved_at INTEGER, history TEXT NOT NULL DEFAULT '[]', PRIMARY KEY (txid, topics))";
/// Migration: the lever's oldest-first read and the health block's per-status reads.
pub const DEAD_LETTERS_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS idx_mutation_dead_letters_status ON mutation_dead_letters(status, parked_at)";
/// Migration (the lens fold, L4): the health block's one aggregate reads this index only.
pub const DEAD_LETTERS_HEALTH_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_mutation_dead_letters_health ON mutation_dead_letters(status, redrives, redriven_at, parked_at)";
/// Migration (the lens fold, L4): the last re-drive is one step of this index.
pub const DEAD_LETTERS_REDRIVEN_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS idx_mutation_dead_letters_redriven ON mutation_dead_letters(redriven_at)";

/// The capped history append: `$h` is replaced by the column, `$e` by the entry (both SQL expressions).
macro_rules! history_append {
    ($entry:literal) => {
        concat!(
            "json_insert(CASE WHEN json_array_length(mutation_dead_letters.history) >= 20 THEN json_remove(mutation_dead_letters.history, '$[0]') ELSE mutation_dead_letters.history END, '$[#]', ",
            $entry,
            ")"
        )
    };
}

/// Binds: txid, topics, fault, now. NO message bytes (M2). A new key starts `failing` at attempt 1; a `failing` or
/// `redriven` row counts one more attempt (a claim resets it to 0) and keeps its status; a `parked` row is not
/// touched (a copy of a parked key failing on the main queue does not take the letter out of the lever's reach).
pub const NOTE_FAILING_SQL: &str = "INSERT INTO mutation_dead_letters (txid, topics, message, fault, attempts, status, first_seen_at) VALUES (?, ?, '', ?, 1, 'failing', ?) \
     ON CONFLICT(txid, topics) DO UPDATE SET fault = excluded.fault, attempts = mutation_dead_letters.attempts + 1 \
     WHERE mutation_dead_letters.status != 'parked'";
/// Binds: txid, topics, message, fault (used when the row holds none), redrives (for a fresh row), now (twice).
/// A row already parked with these very bytes is untouched (no row returned): a DLQ redelivery parks once. Another
/// copy of a parked key keeps the longer bytes and appends a `copy` entry (`kind` returned).
pub const PARK_SQL: &str = concat!(
    "INSERT INTO mutation_dead_letters (txid, topics, message, fault, attempts, status, redrives, first_seen_at, parked_at, history) \
     VALUES (?1, ?2, ?3, ?4, 0, 'parked', ?5, ?6, ?6, json_array(json_object('parkedAt', ?6, 'fault', ?4, 'attempts', 0, 'redrive', ?5, 'kind', 'park'))) \
     ON CONFLICT(txid, topics) DO UPDATE SET status = 'parked', \
     parked_at = CASE WHEN mutation_dead_letters.status = 'parked' THEN mutation_dead_letters.parked_at ELSE excluded.parked_at END, \
     message = CASE WHEN length(excluded.message) > length(mutation_dead_letters.message) THEN excluded.message ELSE mutation_dead_letters.message END, \
     fault = COALESCE(mutation_dead_letters.fault, excluded.fault), \
     history = ",
    history_append!(
        "json_object('parkedAt', excluded.parked_at, 'fault', COALESCE(mutation_dead_letters.fault, excluded.fault), 'attempts', mutation_dead_letters.attempts, 'redrive', mutation_dead_letters.redrives, 'kind', CASE WHEN mutation_dead_letters.status = 'parked' THEN 'copy' ELSE 'park' END, 'kept', CASE WHEN length(excluded.message) > length(mutation_dead_letters.message) THEN 'new' ELSE 'old' END)"
    ),
    " WHERE mutation_dead_letters.status != 'parked' OR mutation_dead_letters.message != excluded.message \
     RETURNING redrives, json_extract(history, '$[#-1].kind') AS kind, json_extract(history, '$[#-1].kept') AS kept"
);
/// Binds: txid, topics. The ack of landed bytes (their replay, their re-drive, or another copy of them): the row
/// goes, whatever its status (M2; `storage-ownership.json`'s `delete_scope`). The deleted row's status is returned.
pub const RESOLVE_SQL: &str =
    "DELETE FROM mutation_dead_letters WHERE txid = ? AND topics = ? RETURNING status, parked_at";
/// Binds: txid, topics (NULL: every parked row of the txid). The operator's discard (D-M1; the second scoped delete
/// of `storage-ownership.json`): a PARKED letter only, its bytes returned to log their hash.
pub const DISCARD_SQL: &str = "DELETE FROM mutation_dead_letters WHERE txid = ?1 AND (?2 IS NULL OR topics = ?2) AND status = 'parked' \
     RETURNING txid, topics, redrives, fault, message";
/// Binds: the redrive ceiling, limit. Oldest parked first. No bytes: the claim returns them (L3).
pub const SELECT_PARKED_SQL: &str =
    "SELECT txid, topics, fault, redrives, redriven_at FROM mutation_dead_letters \
     WHERE status = 'parked' AND redrives < ? ORDER BY parked_at, txid, topics LIMIT ?";
/// Binds: the redrive ceiling, txid, limit.
pub const SELECT_PARKED_TXID_SQL: &str =
    "SELECT txid, topics, fault, redrives, redriven_at FROM mutation_dead_letters \
     WHERE status = 'parked' AND redrives < ? AND txid = ? ORDER BY parked_at, topics LIMIT ?";
/// Binds: txid, limit. `force` (L5): one txid's parked rows past the ceiling too.
pub const SELECT_FORCED_TXID_SQL: &str =
    "SELECT txid, topics, fault, redrives, redriven_at FROM mutation_dead_letters \
     WHERE status = 'parked' AND txid = ? ORDER BY parked_at, topics LIMIT ?";
/// Binds: now, txid, topics, the redrives READ. The compare-and-set: a row returned is this call's to send, with the
/// bytes it held AT the claim (L3).
pub const CLAIM_SQL: &str = "UPDATE mutation_dead_letters SET status = 'redriven', redrives = redrives + 1, redriven_at = ?1, attempts = 0 \
     WHERE txid = ?2 AND topics = ?3 AND status = 'parked' AND redrives = ?4 RETURNING redrives, message";
/// [`CLAIM_SQL`] for a forced re-drive (L5): the same compare-and-set, and a `force` entry in the history.
pub const CLAIM_FORCED_SQL: &str = concat!(
    "UPDATE mutation_dead_letters SET status = 'redriven', redrives = redrives + 1, redriven_at = ?1, attempts = 0, history = ",
    history_append!("json_object('forcedAt', ?1, 'redrive', mutation_dead_letters.redrives + 1, 'kind', 'force')"),
    " WHERE txid = ?2 AND topics = ?3 AND status = 'parked' AND redrives = ?4 RETURNING redrives, message"
);
/// Binds: the previous redriven_at (or NULL), txid, topics, the redrives the claim wrote.
pub const REVERT_SQL: &str =
    "UPDATE mutation_dead_letters SET status = 'parked', redrives = redrives - 1, redriven_at = ? \
     WHERE txid = ? AND topics = ? AND status = 'redriven' AND redrives = ?";
/// Binds: the stale cutoff (now − [`STALE_REDRIVE_MS`]), now, limit (M1). The returned rows keep their place
/// (`parked_at`) and their spent re-drive, and gain a `stale` history entry.
pub const STALE_RETURN_SQL: &str = concat!(
    "UPDATE mutation_dead_letters SET status = 'parked', history = ",
    history_append!(
        "json_object('returnedAt', ?2, 'redrivenAt', mutation_dead_letters.redriven_at, 'redrive', mutation_dead_letters.redrives, 'kind', 'stale')"
    ),
    " WHERE rowid IN (SELECT rowid FROM mutation_dead_letters WHERE status = 'redriven' AND redriven_at < ?1 ORDER BY redriven_at LIMIT ?3) \
     RETURNING txid, topics, redrives, redriven_at"
);
/// Binds: txid, topics. The ceiling's read (M2): the letters holding bytes, and whether this key is one of them.
pub const CEILING_SQL: &str = "SELECT (SELECT COUNT(*) FROM mutation_dead_letters WHERE status IN ('parked', 'redriven')) AS held, \
     (SELECT COUNT(*) FROM mutation_dead_letters WHERE txid = ? AND topics = ? AND status IN ('parked', 'redriven')) AS known";
/// The health block's one aggregate (L4: an index-only read of `idx_mutation_dead_letters_health`). Binds: the
/// redrive ceiling, the stale cutoff, the 24 h cutoff.
pub const HEALTH_COUNTS_SQL: &str = "SELECT status, COUNT(*) AS c, MAX(redriven_at) AS last_redrive, \
     SUM(CASE WHEN redrives >= ?1 THEN 1 ELSE 0 END) AS exhausted, SUM(CASE WHEN redriven_at < ?2 THEN 1 ELSE 0 END) AS stale, \
     SUM(CASE WHEN parked_at >= ?3 THEN 1 ELSE 0 END) AS recent FROM mutation_dead_letters GROUP BY status";
pub const HEALTH_OLDEST_SQL: &str =
    "SELECT txid, topics, parked_at FROM mutation_dead_letters WHERE status = 'parked' ORDER BY parked_at LIMIT 1";
pub const HEALTH_OLDEST_REDRIVEN_SQL: &str =
    "SELECT txid, topics, redriven_at FROM mutation_dead_letters WHERE status = 'redriven' ORDER BY redriven_at LIMIT 1";
pub const HEALTH_LAST_REDRIVE_SQL: &str = "SELECT txid, topics, redriven_at FROM mutation_dead_letters WHERE redriven_at IS NOT NULL ORDER BY redriven_at DESC LIMIT 1";
/// Binds: the redrive ceiling, the list cap.
pub const HEALTH_EXHAUSTED_SQL: &str =
    "SELECT txid, topics, fault, redrives, parked_at FROM mutation_dead_letters \
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

/// What the DLQ consumer does with a park that did not land (H1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DlqRetry {
    /// The delay to hand it back with.
    pub delay_s: u32,
    /// This delivery is the platform's LAST: a retry now is a drop (the LOST line).
    pub last: bool,
}

/// PURE: the retry of a park that faulted on delivery `attempts` (the platform's own count, 1 = the first; `None`
/// when it cannot be read: the configured delay, and never called the last).
#[must_use]
pub fn dlq_retry_plan(attempts: Option<u32>) -> DlqRetry {
    let Some(a) = attempts.filter(|a| *a >= 1) else {
        return DlqRetry {
            delay_s: DLQ_RETRY_DELAY_S,
            last: false,
        };
    };
    let shift = (a - 1).min(16);
    let delay_s = DLQ_BACKOFF_BASE_S
        .saturating_mul(1u32 << shift)
        .min(DLQ_BACKOFF_CAP_S);
    DlqRetry {
        delay_s,
        last: a > DLQ_MAX_RETRIES,
    }
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
pub fn note_failing_query(txid: &str, topics: &str, fault: &str, now_ms: i64) -> Query {
    Query::new(NOTE_FAILING_SQL)
        .bind(txid)
        .bind(topics)
        .bind(bounded_fault(fault))
        .bind(now_ms)
}

/// `redrives` is the count a FRESH row starts at: 0, or [`MAX_REDRIVES`] for a letter that can never be re-driven
/// (its body does not decode), so the lever never selects it and the health block lists it.
#[must_use]
pub fn park_query(
    txid: &str,
    topics: &str,
    message: &str,
    fault: &str,
    redrives: u64,
    now_ms: i64,
) -> Query {
    Query::new(PARK_SQL)
        .bind(txid)
        .bind(topics)
        .bind(message)
        .bind(bounded_fault(fault))
        .bind(redrives)
        .bind(now_ms)
}

#[must_use]
pub fn resolve_query(txid: &str, topics: &str) -> Query {
    Query::new(RESOLVE_SQL).bind(txid).bind(topics)
}

/// `topics: None` discards every parked row of `txid`.
#[must_use]
pub fn discard_query(txid: &str, topics: Option<&str>) -> Query {
    Query::new(DISCARD_SQL)
        .bind(txid)
        .bind(topics.map_or(QVal::Null, |t| QVal::Text(t.to_string())))
}

#[must_use]
pub fn ceiling_query(txid: &str, topics: &str) -> Query {
    Query::new(CEILING_SQL).bind(txid).bind(topics)
}

#[must_use]
pub fn stale_return_query(now_ms: i64, limit: u64) -> Query {
    Query::new(STALE_RETURN_SQL)
        .bind(now_ms - STALE_REDRIVE_MS)
        .bind(now_ms)
        .bind(limit)
}

#[must_use]
pub fn select_parked_query(req: &RedriveRequest) -> Query {
    match (&req.txid, req.force) {
        (Some(t), true) => Query::new(SELECT_FORCED_TXID_SQL)
            .bind(t.as_str())
            .bind(req.limit),
        (Some(t), false) => Query::new(SELECT_PARKED_TXID_SQL)
            .bind(MAX_REDRIVES)
            .bind(t.as_str())
            .bind(req.limit),
        (None, _) => Query::new(SELECT_PARKED_SQL)
            .bind(MAX_REDRIVES)
            .bind(req.limit),
    }
}

#[must_use]
pub fn claim_query(row: &ParkedRow, now_ms: i64, forced: bool) -> Query {
    Query::new(if forced { CLAIM_FORCED_SQL } else { CLAIM_SQL })
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
    /// L5: re-drive one txid's letters past [`MAX_REDRIVES`] (once per call, recorded in the history).
    pub force: bool,
}

/// PURE: an empty body, or `{"limit"?: n >= 1, "txid"?: "<key>", "force"?: bool}`. `limit` defaults to
/// [`REDRIVE_DEFAULT_LIMIT`] and is clamped to [`REDRIVE_MAX_LIMIT`]; `txid` is lowercased; `force` needs a `txid`.
/// `Err` names what is wrong.
pub fn parse_redrive_request(raw: &[u8]) -> std::result::Result<RedriveRequest, &'static str> {
    if raw.iter().all(u8::is_ascii_whitespace) {
        return Ok(RedriveRequest {
            limit: REDRIVE_DEFAULT_LIMIT,
            txid: None,
            force: false,
        });
    }
    let v: serde_json::Value = serde_json::from_slice(raw).map_err(|_| "body must be JSON")?;
    let obj = v.as_object().ok_or("body must be a JSON object")?;
    let limit = match obj.get("limit") {
        None | Some(serde_json::Value::Null) => REDRIVE_DEFAULT_LIMIT,
        Some(l) => l
            .as_u64()
            .filter(|l| *l >= 1)
            .ok_or("limit must be an integer >= 1")?
            .min(REDRIVE_MAX_LIMIT),
    };
    let txid = match obj.get("txid") {
        None | Some(serde_json::Value::Null) => None,
        Some(t) => {
            let t = t
                .as_str()
                .map(str::trim)
                .filter(|t| !t.is_empty() && t.len() <= 128)
                .ok_or("txid must be a non-empty string")?;
            Some(t.to_ascii_lowercase())
        }
    };
    let force = match obj.get("force") {
        None | Some(serde_json::Value::Null) => false,
        Some(f) => f.as_bool().ok_or("force must be a boolean")?,
    };
    if force && txid.is_none() {
        return Err("force needs a txid (one letter at a time)");
    }
    Ok(RedriveRequest { limit, txid, force })
}

/// One letter `POST /internal/discard-dead-letters` names: a txid (lowercased) and its topics key (`None`: every
/// parked row of the txid).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscardKey {
    pub txid: String,
    pub topics: Option<String>,
}

/// PURE (D-M1): `{"letters": [{"txid": "<key>", "topics"?: "<sorted, comma-joined>"}, ...]}`, 1 to
/// [`DISCARD_MAX_LETTERS`] of them (more is refused, not clamped: the operator names each letter). `Err` names what
/// is wrong.
pub fn parse_discard_request(raw: &[u8]) -> std::result::Result<Vec<DiscardKey>, &'static str> {
    let v: serde_json::Value = serde_json::from_slice(raw).map_err(|_| "body must be JSON")?;
    let letters = v
        .as_object()
        .ok_or("body must be a JSON object")?
        .get("letters")
        .and_then(serde_json::Value::as_array)
        .ok_or("letters must be an array")?;
    if letters.is_empty() {
        return Err("letters must name at least one letter");
    }
    if letters.len() > DISCARD_MAX_LETTERS {
        return Err("letters names more than the lever discards per call");
    }
    let text = |v: &serde_json::Value| {
        v.as_str()
            .map(str::trim)
            .filter(|t| !t.is_empty() && t.len() <= 1024)
            .map(str::to_string)
    };
    let mut keys = Vec::with_capacity(letters.len());
    for l in letters {
        let o = l.as_object().ok_or("each letter must be an object")?;
        let txid = o
            .get("txid")
            .and_then(text)
            .filter(|t| t.len() <= 128)
            .ok_or("each letter needs a non-empty txid")?
            .to_ascii_lowercase();
        let topics = match o.get("topics") {
            None | Some(serde_json::Value::Null) => None,
            Some(t) => Some(text(t).ok_or("topics must be a non-empty string")?),
        };
        let key = DiscardKey { txid, topics };
        if !keys.contains(&key) {
            keys.push(key);
        }
    }
    Ok(keys)
}

/// One parked row the lever read (no bytes: the claim returns them).
#[derive(Deserialize, Debug, Clone)]
pub struct ParkedRow {
    pub txid: String,
    pub topics: String,
    pub fault: Option<String>,
    pub redrives: f64,
    pub redriven_at: Option<f64>,
}

impl ParkedRow {
    fn redrives_u64(&self) -> u64 {
        self.redrives.max(0.0) as u64
    }
}

/// PURE: the message the bytes `message` of a claimed row re-drive as (`None` when they do not decode): its own
/// bytes, topics and mode, the reason [`REASON_REDRIVE`] and its key with the re-drive's number `n`.
#[must_use]
pub fn redrive_message(row: &ParkedRow, message: &str, n: u64) -> Option<MutationMessage> {
    let mut msg: MutationMessage = serde_json::from_str(message).ok()?;
    msg.reason = REASON_REDRIVE.to_string();
    msg.redrive = Some(RedriveTag {
        txid: row.txid.clone(),
        topics: row.topics.clone(),
        n,
    });
    Some(msg)
}

#[derive(Deserialize)]
struct ParkedReturn {
    redrives: f64,
    kind: Option<String>,
    kept: Option<String>,
}

#[derive(Deserialize)]
struct ClaimedRow {
    redrives: f64,
    message: String,
}

#[derive(Deserialize)]
struct CeilingRow {
    held: f64,
    known: f64,
}

#[derive(Deserialize)]
struct ResolvedRow {
    status: String,
    parked_at: Option<f64>,
}

#[derive(Deserialize)]
struct DiscardedRow {
    txid: String,
    topics: String,
    redrives: f64,
    fault: Option<String>,
    message: String,
}

#[derive(Deserialize)]
struct StaleRow {
    txid: String,
    topics: String,
    redrives: f64,
    redriven_at: Option<f64>,
}

/// The main consumer, before it hands a replay back: the letter's fault and attempt (no bytes). Fail-soft
/// (logged): a lost note leaves the park's [`FAULT_UNRECORDED`].
pub async fn note_failing(
    db: &D1Database,
    body: &MutationMessage,
    subject: Option<&str>,
    fault: &str,
) {
    let (txid, topics) = letter_key(body, subject);
    let now = worker::Date::now().as_millis() as i64;
    if let Err(e) = note_failing_query(&txid, &topics, fault, now)
        .execute(db)
        .await
    {
        worker::console_log!("[dead-letters] the failing note of {txid} [{topics}] faulted ({e}); its park will say the fault was not recorded");
    }
}

/// Why the main consumer acked a replay (D-L3: the log line says what happened, not "landed" for all three).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolved {
    /// The replay was durable: its bytes landed.
    Landed,
    /// The replay was skipped: its subject is under an OPEN eviction, its bytes are refused and nothing was written.
    RefusedEvicted,
    /// The replay landed and an eviction that opened meanwhile took its rows out again.
    ReEvicted,
}

impl Resolved {
    /// The words of the `RESOLVED` log line.
    #[must_use]
    pub fn says(self) -> &'static str {
        match self {
            Self::Landed => "its bytes landed",
            Self::RefusedEvicted => {
                "its bytes were REFUSED under an open eviction (nothing written; a MINED proof readmits)"
            }
            Self::ReEvicted => {
                "its bytes landed and were re-evicted under an eviction that opened meanwhile"
            }
        }
    }
}

/// The main consumer, on an ack: the letter's bytes landed, or are refused under an open eviction (`why`); its row,
/// if any, is deleted: no replay of it is wanted. Fail-soft (logged): a row left behind is shown by the health
/// block (a `failing` note) or is re-selected by the lever (a parked or stale re-driven letter, whose re-drive is
/// then a dedup, or a skip under the eviction).
pub async fn resolve(
    db: &D1Database,
    body: &MutationMessage,
    subject: Option<&str>,
    why: Resolved,
) {
    let (txid, topics) = letter_key(body, subject);
    match resolve_query(&txid, &topics)
        .fetch_all::<ResolvedRow>(db)
        .await
    {
        Ok(rows) => {
            if let Some(r) = rows.first().filter(|r| r.parked_at.is_some()) {
                crate::ops::bump_counter(db, crate::ops::COUNTER_DEAD_LETTERS_RESOLVED, 1).await;
                worker::console_log!(
                    "[dead-letters] RESOLVED {txid} [{topics}] (was {}): {}; the row is gone",
                    r.status,
                    why.says()
                );
            }
        }
        Err(e) => {
            worker::console_log!("[dead-letters] the resolve of {txid} [{topics}] faulted ({e})")
        }
    }
}

/// One dead letter as the DLQ consumer reads it.
struct Letter {
    txid: String,
    topics: String,
    message: String,
    fault: String,
    start_redrives: u64,
}

fn letter_of(raw: worker::wasm_bindgen::JsValue, id: &str) -> Letter {
    match worker::serde_wasm_bindgen::from_value::<MutationMessage>(raw.clone()) {
        Ok(body) => {
            let subject = subject_of(&body);
            let (txid, topics) = letter_key(&body, subject.as_deref());
            Letter {
                txid,
                topics,
                message: serde_json::to_string(&body).unwrap_or_default(),
                fault: FAULT_UNRECORDED.to_string(),
                start_redrives: 0,
            }
        }
        Err(e) => Letter {
            txid: format!("undecodable:{id}"),
            topics: String::new(),
            message: worker::js_sys::JSON::stringify(&raw)
                .map(String::from)
                .unwrap_or_default(),
            fault: format!(
                "the dead letter does not decode as a mutation message ({e}); never re-drivable"
            ),
            start_redrives: MAX_REDRIVES,
        },
    }
}

/// What a park did.
enum Parked {
    /// A new park (or a re-park of a re-driven letter): `redrives` so far, and the letters with bytes after it when
    /// that is at or past [`CEILING_NEAR`] (D-M1's threshold line).
    Park(u64, Option<u64>),
    /// Another copy met its parked letter: which bytes were kept.
    Copy(String),
    /// The same bytes again: nothing written.
    Redelivery,
    /// Not parked: the ceiling holds (`held` letters with bytes).
    Ceiling(u64),
}

async fn park_one(db: &D1Database, l: &Letter, now: i64) -> std::result::Result<Parked, String> {
    let c = ceiling_query(&l.txid, &l.topics)
        .fetch_optional::<CeilingRow>(db)
        .await
        .map_err(|e| format!("the ceiling read: {e}"))?;
    let mut held_after = None;
    if let Some(c) = c {
        let held = c.held.max(0.0) as u64;
        if c.known < 1.0 && c.held >= PARKED_ROWS_CEILING as f64 {
            return Ok(Parked::Ceiling(held));
        }
        let after = if c.known < 1.0 { held + 1 } else { held };
        held_after = (after >= CEILING_NEAR).then_some(after);
    }
    let rows = park_query(
        &l.txid,
        &l.topics,
        &l.message,
        &l.fault,
        l.start_redrives,
        now,
    )
    .fetch_all::<ParkedReturn>(db)
    .await
    .map_err(|e| format!("the park: {e}"))?;
    Ok(match rows.first() {
        None => Parked::Redelivery,
        Some(r) if r.kind.as_deref() == Some("copy") => {
            Parked::Copy(r.kept.clone().unwrap_or_default())
        }
        Some(r) => Parked::Park(r.redrives.max(0.0) as u64, held_after),
    })
}

/// PURE (D-L2): the platform's `attempts` as the log lines print it, so a drill that parks cleanly reads it.
#[must_use]
pub fn attempts_text(attempts: Option<u32>) -> String {
    attempts.map_or_else(|| "absent".to_string(), |a| a.to_string())
}

/// PURE (D-L1): what one ceiling deferral counts: `(letters, deliveries)`. The LETTER is counted on its first DLQ
/// delivery by the platform's own count (`attempts == 1`, the count [`dlq_retry_plan`] reads too); every delivery
/// is a deferral. An unreadable count counts the delivery only (stated).
#[must_use]
pub fn ceiling_deferral_counts(attempts: Option<u32>) -> (u64, u64) {
    (u64::from(attempts == Some(1)), 1)
}

/// PURE (D-M1): the health block's `ceiling`: the letters with bytes, the maximum, the room left, `near` from
/// [`CEILING_NEAR`] on and `full` at the maximum.
#[must_use]
pub fn ceiling_json(held: u64) -> serde_json::Value {
    serde_json::json!({
        "held": held,
        "max": PARKED_ROWS_CEILING,
        "room": PARKED_ROWS_CEILING.saturating_sub(held),
        "nearAt": CEILING_NEAR,
        "near": held >= CEILING_NEAR,
        "full": held >= PARKED_ROWS_CEILING,
    })
}

fn retry_after(m: &worker::worker_sys::Message, delay_s: u32) {
    let opts = worker::js_sys::Object::new();
    let _ = worker::js_sys::Reflect::set(
        &opts,
        &"delaySeconds".into(),
        &worker::wasm_bindgen::JsValue::from(f64::from(delay_s)),
    );
    if let Err(e) = m.retry(opts.into()) {
        worker::console_log!("[dead-letters] the retry call itself faulted ({e:?}); the platform retries the batch at the configured delay");
    }
}

/// The DLQ consumer: park every message of `batch`; PARK FIRST, ACK AFTER (a message is acked only after its park
/// answered), a fault handed back with [`dlq_retry_plan`]'s delay, the last delivery's fault logged LOST. It reads
/// the platform's own message objects (`lib.rs`'s `queue` export) for their `attempts`, which workers-rs 0.8.5's
/// `Message` does not expose.
pub async fn park_batch(batch: &worker::worker_sys::MessageBatch, env: &Env) -> Result<()> {
    use worker::wasm_bindgen::JsCast;
    let queue: String = batch.queue().map(String::from).unwrap_or_default();
    let messages = batch
        .messages()
        .map_err(|e| worker::Error::from(format!("the DLQ batch's messages: {e:?}")))?;
    let db: std::result::Result<D1Database, String> = match env.d1("OVERLAY_DB") {
        Ok(db) => match crate::d1::ensure_overlay_migrations(&db).await {
            Ok(()) => Ok(db),
            Err(e) => Err(format!("the migrations: {e}")),
        },
        Err(e) => Err(format!("the D1 binding: {e}")),
    };
    for el in messages.iter() {
        let m: worker::worker_sys::Message = el.unchecked_into();
        let attempts = worker::js_sys::Reflect::get(&m, &"attempts".into())
            .ok()
            .and_then(|v| v.as_f64())
            .map(|a| a.max(0.0) as u32);
        let id = m.id().map(String::from).unwrap_or_default();
        let letter = letter_of(
            m.body().unwrap_or(worker::wasm_bindgen::JsValue::UNDEFINED),
            &id,
        );
        let (txid, topics) = (&letter.txid, &letter.topics);
        let att = attempts_text(attempts);
        let now = worker::Date::now().as_millis() as i64;
        let outcome = match &db {
            Ok(db) => park_one(db, &letter, now).await,
            Err(e) => Err(e.clone()),
        };
        let fault = match outcome {
            Ok(Parked::Park(redrives, near)) => {
                if let Ok(db) = &db {
                    crate::ops::bump_counter(db, crate::ops::COUNTER_DEAD_LETTERS_PARKED, 1).await;
                    if redrives > 0 && letter.start_redrives == 0 {
                        crate::ops::bump_counter(
                            db,
                            crate::ops::COUNTER_DEAD_LETTERS_STILL_FAILING,
                            1,
                        )
                        .await;
                    }
                }
                worker::console_log!(
                    "[dead-letters] PARKED {txid} [{topics}] from {queue} attempts={att} (re-drives so far {redrives}/{MAX_REDRIVES}){}",
                    if redrives >= MAX_REDRIVES { "; EXHAUSTED, the lever will not re-drive it unless forced" } else { "" }
                );
                if let Some(held) = near {
                    worker::console_log!("[dead-letters] the ceiling is NEAR: {held}/{PARKED_ROWS_CEILING} letters hold bytes; past it every new letter is deferred and lost after ~48 h; discard what can never land (POST /internal/discard-dead-letters)");
                }
                let _ = m.ack();
                continue;
            }
            Ok(Parked::Copy(kept)) => {
                worker::console_log!("[dead-letters] another copy of the parked {txid} [{topics}] arrived attempts={att}: kept the {kept} bytes (the longer), a history entry; acked");
                let _ = m.ack();
                continue;
            }
            Ok(Parked::Redelivery) => {
                worker::console_log!("[dead-letters] {txid} [{topics}] was already parked with these bytes (a DLQ redelivery) attempts={att}: acked");
                let _ = m.ack();
                continue;
            }
            Ok(Parked::Ceiling(held)) => {
                if let Ok(db) = &db {
                    let (letters, deliveries) = ceiling_deferral_counts(attempts);
                    if letters > 0 {
                        crate::ops::bump_counter(
                            db,
                            crate::ops::COUNTER_DEAD_LETTERS_CEILING_DEFERRED,
                            letters,
                        )
                        .await;
                    }
                    crate::ops::bump_counter(
                        db,
                        crate::ops::COUNTER_DEAD_LETTERS_CEILING_DEFERRALS,
                        deliveries,
                    )
                    .await;
                }
                format!("the ceiling: {held} letters hold bytes (max {PARKED_ROWS_CEILING})")
            }
            Err(e) => e,
        };
        let plan = dlq_retry_plan(attempts);
        if plan.last {
            let h = bsv_rs::primitives::hash::sha256(letter.message.as_bytes());
            worker::console_log!(
                "[dead-letters] LOST {txid} [{topics}] sha256={} after {} DLQ deliveries ({fault}): the platform drops it now and its bytes are NOT in D1",
                hex::encode(h),
                attempts.unwrap_or(0)
            );
            if let Ok(db) = &db {
                crate::ops::bump_counter(db, crate::ops::COUNTER_DEAD_LETTERS_LOST, 1).await;
            }
        } else {
            worker::console_log!(
                "[dead-letters] NOT parked {txid} [{topics}] ({fault}); delivery {}/{}: handed back for {} s",
                att,
                DLQ_MAX_RETRIES + 1,
                plan.delay_s
            );
        }
        retry_after(&m, plan.delay_s);
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
        Err(why) => {
            return Response::error(
                format!("{why}: {{\"limit\"?: 1..{REDRIVE_MAX_LIMIT}, \"txid\"?: \"<txid>\", \"force\"?: true (with a txid)}}"),
                400,
            )
        }
    };
    let db = env.d1("OVERLAY_DB")?;
    crate::d1::ensure_overlay_migrations(&db)
        .await
        .map_err(worker::Error::from)?;
    // M1: the stale re-drives go back to the parked set first, so this very call can select them.
    let start = worker::Date::now().as_millis() as i64;
    let stale: Vec<StaleRow> = match stale_return_query(start, REDRIVE_MAX_LIMIT)
        .fetch_all(&db)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return Response::error(
                format!("the stale re-drives could not be returned: {e}"),
                502,
            )
        }
    };
    if !stale.is_empty() {
        crate::ops::bump_counter(
            &db,
            crate::ops::COUNTER_DEAD_LETTERS_STALE_RETURNED,
            stale.len() as u64,
        )
        .await;
        for s in &stale {
            worker::console_log!(
                "POST /internal/redrive-dead-letters: the re-drive {} of {} [{}] (claimed at {} ms) never resolved nor parked again: returned to the parked set",
                s.redrives.max(0.0) as u64,
                s.txid,
                s.topics,
                s.redriven_at.unwrap_or(0.0) as i64
            );
        }
    }
    let rows: Vec<ParkedRow> = match select_parked_query(&parsed).fetch_all(&db).await {
        Ok(r) => r,
        Err(e) => return Response::error(format!("the dead letters could not be read: {e}"), 502),
    };
    let queue = env.queue("MUTATION_QUEUE")?;
    let mut moved = Vec::new();
    let mut skipped = Vec::new();
    let mut faults = Vec::new();
    for row in &rows {
        let now = worker::Date::now().as_millis() as i64;
        let claimed = match claim_query(row, now, parsed.force)
            .fetch_all::<ClaimedRow>(&db)
            .await
        {
            Ok(c) => c,
            Err(e) => {
                faults.push(serde_json::json!({"txid": row.txid, "topics": row.topics, "fault": format!("claim: {e}")}));
                continue;
            }
        };
        let Some(claimed) = claimed.into_iter().next() else {
            skipped.push(serde_json::json!({"txid": row.txid, "topics": row.topics, "why": "claimed by another call"}));
            continue;
        };
        let n = claimed.redrives.max(0.0) as u64;
        let Some(msg) = redrive_message(row, &claimed.message, n) else {
            let reverted = revert_query(row).execute(&db).await;
            skipped.push(serde_json::json!({
                "txid": row.txid, "topics": row.topics, "why": "its message does not decode", "reverted": reverted.is_ok(),
            }));
            continue;
        };
        match queue.send(msg).await {
            Ok(()) => {
                crate::ops::bump_counter(&db, crate::ops::COUNTER_DEAD_LETTERS_REDRIVEN, 1).await;
                worker::console_log!(
                    "POST /internal/redrive-dead-letters: re-drove {} [{}] (re-drive {n}/{MAX_REDRIVES}{}; its last fault: {})",
                    row.txid,
                    row.topics,
                    if parsed.force { ", FORCED" } else { "" },
                    row.fault.as_deref().unwrap_or("none recorded")
                );
                moved.push(serde_json::json!({
                    "txid": row.txid, "topics": row.topics, "redrive": n, "fault": row.fault, "redrivenAt": now,
                    "forced": parsed.force,
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
        "POST /internal/redrive-dead-letters limit={} force={} -> 200 (stale returned={} read={} redriven={} skipped={} faults={})",
        parsed.limit,
        parsed.force,
        stale.len(),
        rows.len(),
        moved.len(),
        skipped.len(),
        faults.len()
    );
    Response::from_json(&serde_json::json!({
        "ok": true,
        "limit": parsed.limit,
        "txid": parsed.txid,
        "force": parsed.force,
        "staleReturned": stale.iter().map(|s| serde_json::json!({
            "txid": s.txid, "topics": s.topics, "redrives": s.redrives.max(0.0) as u64,
            "redrivenAt": s.redriven_at.map(|v| v as i64),
        })).collect::<Vec<_>>(),
        "read": rows.len(),
        "redriven": moved,
        "skipped": skipped,
        "faults": faults,
        "maxRedrives": MAX_REDRIVES,
    }))
}

/// `POST /internal/discard-dead-letters` (bearer `INTERNAL_TOKEN`, D-M1): delete the named PARKED letters (see the
/// module doc, step 5). Each discarded letter is logged with the sha256 and length of its bytes, its re-drives and
/// its last fault, and counted; a key that names no parked row is answered `notFound` (a `redriven` letter in
/// flight, a `failing` note, or nothing), and nothing else is touched.
pub async fn internal_discard(mut req: Request, env: &Env) -> Result<Response> {
    let authorization = req.headers().get("authorization").ok().flatten();
    let secret = env.secret("INTERNAL_TOKEN").ok().map(|s| s.to_string());
    if !crate::tip_pass::bearer_ok(authorization.as_deref(), secret.as_deref()) {
        worker::console_log!("POST /internal/discard-dead-letters -> 401");
        return Response::error("unauthorized", 401);
    }
    let raw = req.bytes().await?;
    let keys = match parse_discard_request(&raw) {
        Ok(k) => k,
        Err(why) => {
            return Response::error(
                format!("{why}: {{\"letters\": [{{\"txid\": \"<key>\", \"topics\"?: \"<sorted, comma-joined>\"}}, ...]}} (1..{DISCARD_MAX_LETTERS})"),
                400,
            )
        }
    };
    let db = env.d1("OVERLAY_DB")?;
    crate::d1::ensure_overlay_migrations(&db)
        .await
        .map_err(worker::Error::from)?;
    let mut discarded = Vec::new();
    let mut not_found = Vec::new();
    let mut faults = Vec::new();
    for k in &keys {
        match discard_query(&k.txid, k.topics.as_deref())
            .fetch_all::<DiscardedRow>(&db)
            .await
        {
            Ok(rows) if rows.is_empty() => {
                not_found.push(serde_json::json!({"txid": k.txid, "topics": k.topics}));
            }
            Ok(rows) => {
                for r in rows {
                    let sha = hex::encode(bsv_rs::primitives::hash::sha256(r.message.as_bytes()));
                    let redrives = r.redrives.max(0.0) as u64;
                    worker::console_log!(
                        "[dead-letters] DISCARDED {} [{}] sha256={sha} bytes={} by the operator (re-drives {redrives}/{MAX_REDRIVES}; its last fault: {}): its bytes are gone",
                        r.txid,
                        r.topics,
                        r.message.len(),
                        r.fault.as_deref().unwrap_or("none recorded")
                    );
                    discarded.push(serde_json::json!({
                        "txid": r.txid, "topics": r.topics, "redrives": redrives, "fault": r.fault,
                        "bytes": r.message.len(), "sha256": sha,
                    }));
                }
            }
            Err(e) => {
                faults.push(serde_json::json!({"txid": k.txid, "topics": k.topics, "fault": format!("discard: {e}")}));
            }
        }
    }
    if !discarded.is_empty() {
        crate::ops::bump_counter(
            &db,
            crate::ops::COUNTER_DEAD_LETTERS_DISCARDED,
            discarded.len() as u64,
        )
        .await;
    }
    worker::console_log!(
        "POST /internal/discard-dead-letters -> 200 (named={} discarded={} notFound={} faults={})",
        keys.len(),
        discarded.len(),
        not_found.len(),
        faults.len()
    );
    Response::from_json(&serde_json::json!({
        "ok": true,
        "discarded": discarded,
        "notFound": not_found,
        "faults": faults,
        "maxLetters": DISCARD_MAX_LETTERS,
    }))
}

#[derive(Deserialize)]
struct StatusCountRow {
    status: String,
    c: f64,
    last_redrive: Option<f64>,
    exhausted: Option<f64>,
    stale: Option<f64>,
    recent: Option<f64>,
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

/// `/health/invariants.deadLetters`: the count by status, the exhausted, stale and recent counts, the ceiling, the
/// oldest parked, the oldest re-drive in flight, the last re-drive, the exhausted letters. `readable: false` when
/// the table cannot be read (a pre-migration isolate), distinct from an empty one.
pub async fn health_json(db: &D1Database) -> serde_json::Value {
    let now = worker::Date::now().as_millis() as i64;
    let Ok(counts) = Query::new(HEALTH_COUNTS_SQL)
        .bind(MAX_REDRIVES)
        .bind(now - STALE_REDRIVE_MS)
        .bind(now - 86_400_000)
        .fetch_all::<StatusCountRow>(db)
        .await
    else {
        return serde_json::json!({"readable": false});
    };
    let n = |v: Option<f64>| v.unwrap_or(0.0).max(0.0) as u64;
    let row = |s: &str| counts.iter().find(|r| r.status == s);
    let count = |s: &str| row(s).map_or(0, |r| r.c.max(0.0) as u64);
    let oldest = if count("parked") > 0 {
        Query::new(HEALTH_OLDEST_SQL)
            .fetch_optional::<KeyAtRow>(db)
            .await
            .ok()
            .flatten()
    } else {
        None
    };
    let oldest_redriven = if count("redriven") > 0 {
        Query::new(HEALTH_OLDEST_REDRIVEN_SQL)
            .fetch_optional::<KeyAtRow>(db)
            .await
            .ok()
            .flatten()
    } else {
        None
    };
    let last = if counts.iter().any(|r| r.last_redrive.is_some()) {
        Query::new(HEALTH_LAST_REDRIVE_SQL)
            .fetch_optional::<KeyAtRow>(db)
            .await
            .ok()
            .flatten()
    } else {
        None
    };
    let exhausted_count = row("parked").map_or(0, |r| n(r.exhausted));
    let exhausted: Vec<ExhaustedRow> = if exhausted_count > 0 {
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
    let held = count("parked") + count("redriven");
    serde_json::json!({
        "readable": true,
        "parked": count("parked"),
        "failing": count("failing"),
        "redriven": count("redriven"),
        "staleRedriven": row("redriven").map_or(0, |r| n(r.stale)),
        "staleAfterMs": STALE_REDRIVE_MS,
        "oldestRedriven": oldest_redriven.as_ref().map(key_at),
        "parkedLast24h": counts.iter().map(|r| n(r.recent)).sum::<u64>(),
        "ceiling": ceiling_json(held),
        "oldestParked": oldest.as_ref().map(key_at),
        "lastRedrive": last.as_ref().map(key_at),
        "maxRedrives": MAX_REDRIVES,
        "exhaustedCount": exhausted_count,
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
        conn.execute(DEAD_LETTERS_HEALTH_INDEX, []).unwrap();
        conn.execute(DEAD_LETTERS_REDRIVEN_INDEX, []).unwrap();
        conn
    }

    /// Run a statement; the rows it RETURNs (a write without RETURNING answers none).
    fn run(conn: &rusqlite::Connection, q: &Query) -> usize {
        rows(conn, q).len()
    }

    /// Run a statement; every row it RETURNs, each column as text (NULL as "").
    fn rows(conn: &rusqlite::Connection, q: &Query) -> Vec<Vec<String>> {
        let mut stmt = conn.prepare(q.sql()).unwrap();
        let cols = stmt.column_count();
        if cols == 0 {
            stmt.execute(rusqlite::params_from_iter(binds(q).iter()))
                .unwrap();
            return Vec::new();
        }
        let mut out = Vec::new();
        let mut rs = stmt
            .query(rusqlite::params_from_iter(binds(q).iter()))
            .unwrap();
        while let Some(r) = rs.next().unwrap() {
            out.push(
                (0..cols)
                    .map(|i| match r.get::<_, rusqlite::types::Value>(i).unwrap() {
                        rusqlite::types::Value::Null => String::new(),
                        rusqlite::types::Value::Integer(v) => v.to_string(),
                        rusqlite::types::Value::Real(v) => v.to_string(),
                        rusqlite::types::Value::Text(s) => s,
                        rusqlite::types::Value::Blob(_) => "<blob>".into(),
                    })
                    .collect(),
            );
        }
        out
    }

    fn parked(conn: &rusqlite::Connection, req: &RedriveRequest) -> Vec<ParkedRow> {
        let q = select_parked_query(req);
        let mut stmt = conn.prepare(q.sql()).unwrap();
        stmt.query_map(rusqlite::params_from_iter(binds(&q).iter()), |r| {
            Ok(ParkedRow {
                txid: r.get(0)?,
                topics: r.get(1)?,
                fault: r.get(2)?,
                redrives: r.get::<_, i64>(3)? as f64,
                redriven_at: r.get::<_, Option<i64>>(4)?.map(|v| v as f64),
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

    fn exists(conn: &rusqlite::Connection, txid: &str) -> bool {
        conn.query_row(
            "SELECT COUNT(*) FROM mutation_dead_letters WHERE txid = ?1",
            [txid],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
            > 0
    }

    fn message_of(conn: &rusqlite::Connection, txid: &str) -> String {
        conn.query_row(
            "SELECT message FROM mutation_dead_letters WHERE txid = ?1",
            [txid],
            |r| r.get(0),
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
    fn dead_letter(
        conn: &rusqlite::Connection,
        txid: &str,
        fault: Option<&str>,
        now: i64,
    ) -> usize {
        let body = msg("AA==", &["tm_b", "tm_a"]);
        let (k, t) = letter_key(&body, Some(txid));
        let m = serde_json::to_string(&body).unwrap();
        if let Some(f) = fault {
            run(conn, &note_failing_query(&k, &t, f, now - 10));
        }
        run(conn, &park_query(&k, &t, &m, FAULT_UNRECORDED, 0, now))
    }

    /// One lever pass as `internal_redrive` runs it (the stale return first; the send always lands): the rows it
    /// moved, built from the bytes each CLAIM returned.
    fn lever(conn: &rusqlite::Connection, req: &RedriveRequest, now: i64) -> Vec<MutationMessage> {
        run(conn, &stale_return_query(now, REDRIVE_MAX_LIMIT));
        let mut sent = Vec::new();
        for r in parked(conn, req) {
            let claimed = rows(conn, &claim_query(&r, now, req.force));
            if let Some(c) = claimed.first() {
                sent.push(redrive_message(&r, &c[1], c[0].parse().unwrap()).expect("decodes"));
            }
        }
        sent
    }

    fn all(limit: u64) -> RedriveRequest {
        RedriveRequest {
            limit,
            txid: None,
            force: false,
        }
    }

    #[test]
    fn e576_a_dead_letter_is_parked_with_its_fault_once() {
        let conn = db();
        assert_eq!(
            dead_letter(&conn, "aa", Some("predecessor_not_landed: tm_a"), 1_000),
            1,
            "parked: one row returned"
        );
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
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&history)
                .unwrap()
                .as_array()
                .unwrap()
                .len(),
            1
        );
        // a letter with no failing note is parked with the stated text
        assert_eq!(dead_letter(&conn, "bb", None, 3_000), 1);
        assert_eq!(row(&conn, "bb").3.as_deref(), Some(FAULT_UNRECORDED));
        // the key: sorted topics; the message kept as is
        let stored: String = conn
            .query_row(
                "SELECT topics || '|' || message FROM mutation_dead_letters WHERE txid = 'aa'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(stored.starts_with("tm_a,tm_b|{"), "{stored}");
    }

    #[test]
    fn e576_the_lever_moves_n_and_no_more_oldest_first_or_by_txid() {
        let conn = db();
        for (i, t) in ["t3", "t1", "t2", "t4"].iter().enumerate() {
            dead_letter(&conn, t, Some("x"), 1_000 + [30, 10, 20, 40][i]);
        }
        let sent = lever(&conn, &all(2), 5_000);
        let keys: Vec<String> = sent
            .iter()
            .map(|m| m.redrive.clone().unwrap().txid)
            .collect();
        assert_eq!(keys, vec!["t1", "t2"], "the two OLDEST, no more");
        assert_eq!(row(&conn, "t1").0, "redriven");
        assert_eq!(row(&conn, "t3").0, "parked");
        let one = lever(
            &conn,
            &RedriveRequest {
                limit: 25,
                txid: Some("t4".into()),
                force: false,
            },
            6_000,
        );
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].redrive.as_ref().unwrap().txid, "t4");
        assert_eq!(
            row(&conn, "t3").0,
            "parked",
            "by txid moves that letter alone"
        );
        // the re-driven message: the same bytes, topics and mode, its key and number, the reason
        let m = &sent[0];
        assert_eq!(
            (m.beef_b64.as_str(), m.mode.as_str(), m.reason.as_str()),
            ("AA==", "historical-tx", REASON_REDRIVE)
        );
        assert_eq!(
            m.redrive,
            Some(RedriveTag {
                txid: "t1".into(),
                topics: "tm_a,tm_b".into(),
                n: 1
            })
        );
    }

    #[test]
    fn e576_the_same_letter_redriven_twice_is_one_enqueue() {
        let conn = db();
        dead_letter(&conn, "aa", Some("x"), 1_000);
        // two calls that both READ the row before either claims it
        let a = parked(&conn, &all(25));
        let b = parked(&conn, &all(25));
        assert_eq!(
            run(&conn, &claim_query(&a[0], 2_000, false)),
            1,
            "the first claim wins"
        );
        assert_eq!(
            run(&conn, &claim_query(&b[0], 2_001, false)),
            0,
            "the second changes nothing: no second send"
        );
        assert!(
            lever(&conn, &all(25), 3_000).is_empty(),
            "a later call finds nothing parked"
        );
        assert_eq!(row(&conn, "aa").2, 1);
    }

    #[test]
    fn e576_a_send_fault_reverts_the_claim() {
        let conn = db();
        dead_letter(&conn, "aa", Some("x"), 1_000);
        let r = parked(&conn, &all(25)).remove(0);
        assert_eq!(run(&conn, &claim_query(&r, 2_000, false)), 1);
        assert_eq!(run(&conn, &revert_query(&r)), 0);
        let (status, _, redrives, _, _) = row(&conn, "aa");
        assert_eq!((status.as_str(), redrives), ("parked", 0));
        let at: Option<i64> = conn
            .query_row("SELECT redriven_at FROM mutation_dead_letters", [], |r| {
                r.get(0)
            })
            .unwrap();
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
            run(
                &conn,
                &note_failing_query(&k, &t, &format!("fault {n}"), 1_050 + n as i64 * 100),
            );
            let (status, attempts, ..) = row(&conn, "aa");
            assert_eq!(
                (status.as_str(), attempts),
                ("redriven", 1),
                "the attempts restart with the fresh message"
            );
            assert_eq!(
                run(
                    &conn,
                    &park_query(&k, &t, &body, FAULT_UNRECORDED, 0, 1_080 + n as i64 * 100)
                ),
                1
            );
        }
        let (status, _, redrives, _, history) = row(&conn, "aa");
        assert_eq!((status.as_str(), redrives), ("parked", MAX_REDRIVES as i64));
        let h: serde_json::Value = serde_json::from_str(&history).unwrap();
        let faults: Vec<&str> = h
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["fault"].as_str().unwrap())
            .collect();
        assert_eq!(faults, vec!["fault 0", "fault 1", "fault 2", "fault 3"]);
        assert_eq!(h[3]["redrive"], 3);
        // past the ceiling the lever never selects it; the health read lists it
        assert!(lever(&conn, &all(200), 9_000).is_empty());
        let q = Query::new(HEALTH_EXHAUSTED_SQL)
            .bind(MAX_REDRIVES)
            .bind(HEALTH_EXHAUSTED_LIST);
        assert_eq!(run(&conn, &q), 1);
    }

    #[test]
    fn e576_an_ack_resolves_and_a_resolved_letter_is_never_sent() {
        let conn = db();
        dead_letter(&conn, "aa", Some("x"), 1_000);
        let m = lever(&conn, &all(25), 2_000).remove(0);
        let (k, t) = letter_key(&m, None);
        let gone = rows(&conn, &resolve_query(&k, &t));
        assert_eq!(
            gone,
            vec![vec!["redriven".to_string(), "1000".to_string()]],
            "the re-driven letter's row is deleted"
        );
        assert!(!exists(&conn, "aa"));
        assert!(lever(&conn, &all(25), 4_000).is_empty());
        // a failing replay that is then acked: its note is deleted, never parked
        let body = msg("BB==", &["tm_a"]);
        let (k, t) = letter_key(&body, Some("CC"));
        assert_eq!(k, "cc", "the subject is lowercased");
        run(&conn, &note_failing_query(&k, &t, "x", 1));
        run(&conn, &note_failing_query(&k, &t, "y", 2));
        assert_eq!(row(&conn, "cc").1, 2);
        let gone = rows(&conn, &resolve_query(&k, &t));
        assert_eq!(
            gone,
            vec![vec!["failing".to_string(), String::new()]],
            "a note: no parked_at, not counted resolved"
        );
        // a new episode of the key starts from nothing
        run(&conn, &note_failing_query(&k, &t, "z", 4));
        let (status, attempts, redrives, _, _) = row(&conn, "cc");
        assert_eq!((status.as_str(), attempts, redrives), ("failing", 1, 0));
    }

    #[test]
    fn e576_an_undecodable_letter_is_parked_exhausted() {
        let conn = db();
        assert_eq!(
            run(
                &conn,
                &park_query(
                    "undecodable:id1",
                    "",
                    "\"junk\"",
                    "does not decode",
                    MAX_REDRIVES,
                    5
                )
            ),
            1
        );
        assert!(
            lever(&conn, &all(25), 6).is_empty(),
            "never selected, never blocks the oldest-first read"
        );
        assert_eq!(row(&conn, "undecodable:id1").2, MAX_REDRIVES as i64);
    }

    #[test]
    fn e576_the_request_parse_defaults_clamps_and_refuses() {
        assert_eq!(
            parse_redrive_request(b"").unwrap(),
            all(REDRIVE_DEFAULT_LIMIT)
        );
        assert_eq!(parse_redrive_request(b"{}").unwrap(), all(25));
        assert_eq!(parse_redrive_request(br#"{"limit": 7}"#).unwrap(), all(7));
        assert_eq!(
            parse_redrive_request(br#"{"limit": 5000}"#).unwrap(),
            all(REDRIVE_MAX_LIMIT)
        );
        assert_eq!(
            parse_redrive_request(br#"{"txid": " ABCD "}"#).unwrap(),
            RedriveRequest {
                limit: 25,
                txid: Some("abcd".into()),
                force: false
            }
        );
        assert_eq!(
            parse_redrive_request(br#"{"txid": "ab", "force": true}"#).unwrap(),
            RedriveRequest {
                limit: 25,
                txid: Some("ab".into()),
                force: true
            }
        );
        for bad in [
            &br#"{"limit": 0}"#[..],
            br#"{"limit": -1}"#,
            br#"{"limit": "5"}"#,
            br#"{"txid": ""}"#,
            br#"{"force": true}"#,
            br#"{"txid": "ab", "force": "yes"}"#,
            b"[1]",
            b"nope",
        ] {
            assert!(
                parse_redrive_request(bad).is_err(),
                "{}",
                String::from_utf8_lossy(bad)
            );
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
        assert!(
            u.starts_with("unparsed:") && u.len() == "unparsed:".len() + 32,
            "{u}"
        );
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
            (
                "low-overlay-mutations-beta-dlq",
                "[[env.beta.queues.consumers]]",
            ),
        ] {
            assert!(
                low.contains(&format!("dead_letter_queue = \"{dlq}\"")),
                "the producer dead-letters to {dlq}"
            );
            let consumer = format!("{table}\nqueue = \"{dlq}\"");
            assert!(low.contains(&consumer), "{dlq} has a consumer");
        }
        let generic = include_str!("../wrangler.toml");
        assert!(generic.contains("dead_letter_queue = \"overlay-mutations-dlq\""));
        assert!(generic.contains("[[queues.consumers]]\nqueue = \"overlay-mutations-dlq\""));
    }

    /// The `[[...consumers]]` block of `queue` in a wrangler config: its `key = value` lines.
    fn consumer_block(cfg: &str, queue: &str) -> std::collections::HashMap<String, String> {
        let at = cfg
            .find(&format!("\nqueue = \"{queue}\"\n"))
            .unwrap_or_else(|| panic!("no consumer of {queue}"))
            + 1;
        let head = &cfg[..at];
        assert!(
            head.trim_end().ends_with("consumers]]"),
            "{queue}: the line before is a consumers table"
        );
        let tail = &cfg[at..];
        let end = tail.find("\n[").unwrap_or(tail.len());
        tail[..end]
            .lines()
            .filter(|l| !l.trim_start().starts_with('#'))
            .filter_map(|l| l.split_once('='))
            .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
            .collect()
    }

    /// H1 (the lens fold): EVERY `-dlq` consumer of every config waits between its retries and has the retries to
    /// ride out an outage of an hour; the code's delivery count and backoff agree with them.
    #[test]
    fn e576f_h1_every_dlq_consumer_rides_out_an_hour() {
        let low = include_str!("../wrangler.low.toml");
        let generic = include_str!("../wrangler.toml");
        let mut seen = 0;
        for (cfg, dlq) in [
            (low, "low-overlay-mutations-dlq"),
            (low, "low-overlay-mutations-beta-dlq"),
            (generic, "overlay-mutations-dlq"),
        ] {
            let c = consumer_block(cfg, dlq);
            assert_eq!(
                c.get("retry_delay").map(String::as_str),
                Some(DLQ_RETRY_DELAY_S.to_string().as_str()),
                "{dlq}: retry_delay"
            );
            assert_eq!(
                c.get("max_retries").map(String::as_str),
                Some(DLQ_MAX_RETRIES.to_string().as_str()),
                "{dlq}: max_retries"
            );
            assert!(
                !c.contains_key("dead_letter_queue"),
                "{dlq}: no DLQ of its own (stated)"
            );
            seen += 1;
        }
        // every `-dlq` queue a config consumes is one of the three above
        for cfg in [low, generic] {
            let consumed = cfg.matches("-dlq\"\nmax_batch_size").count();
            assert_eq!(consumed, if std::ptr::eq(cfg, low) { 2 } else { 1 });
        }
        assert_eq!(seen, 3);
        // the backoff: the first retries are short, an hour is ridden within the first handful, the whole window
        // is about two days (inside the queue's default four-day retention)
        let delays: Vec<u32> = (1..=DLQ_MAX_RETRIES)
            .map(|a| dlq_retry_plan(Some(a)).delay_s)
            .collect();
        assert_eq!(&delays[..6], &[60, 120, 240, 480, 960, 1800]);
        let mut sum = 0u64;
        let mut hour_at = None;
        for (i, d) in delays.iter().enumerate() {
            sum += u64::from(*d);
            if sum >= 3600 && hour_at.is_none() {
                hour_at = Some(i + 1);
            }
        }
        assert_eq!(
            hour_at,
            Some(6),
            "an hour's outage is ridden by the 6th retry"
        );
        assert!(
            (40 * 3600..72 * 3600).contains(&sum),
            "the whole window: {} h",
            sum / 3600
        );
        assert!(
            delays.iter().all(|d| *d <= 24 * 3600),
            "Cloudflare's delaySeconds ceiling (24 h)"
        );
        // the LAST delivery is the (1 + max_retries)th, and only it
        assert!(!dlq_retry_plan(Some(DLQ_MAX_RETRIES)).last);
        assert!(dlq_retry_plan(Some(DLQ_MAX_RETRIES + 1)).last);
        assert_eq!(
            dlq_retry_plan(None),
            DlqRetry {
                delay_s: DLQ_RETRY_DELAY_S,
                last: false
            },
            "an unreadable count is never called the last"
        );
        assert_eq!(
            dlq_retry_plan(Some(0)),
            DlqRetry {
                delay_s: DLQ_RETRY_DELAY_S,
                last: false
            }
        );
    }

    /// H1: PARK FIRST, ACK AFTER (a source-shape pin: the DLQ consumer runs only in wasm). Every ack follows a park
    /// that answered; the fault path (a park fault, the ceiling, a dead D1) never acks, it retries with the plan's
    /// delay and logs LOST on the last delivery; the queue export reads the platform's `attempts`.
    #[test]
    fn e576f_h1_park_first_ack_after_and_a_lost_line() {
        let src = include_str!("dead_letters.rs");
        let start = src.find("pub async fn park_batch(").unwrap();
        let body = &src[start..start + src[start..].find("\n}\n").unwrap()];
        let fault_path = &body[body.find("let plan = dlq_retry_plan(attempts);").unwrap()..];
        assert!(!fault_path.contains(".ack("), "the fault path never acks");
        assert!(
            fault_path.contains("[dead-letters] LOST")
                && fault_path.contains("COUNTER_DEAD_LETTERS_LOST")
        );
        assert!(fault_path.contains("retry_after(&m, plan.delay_s)"));
        assert_eq!(
            body.matches("m.ack()").count(),
            3,
            "parked, a copy kept, a redelivery: each after its park answered"
        );
        let ceiling = &body[body.find("Ok(Parked::Ceiling(held)) => {").unwrap()
            ..body.find("Err(e) => e,").unwrap()];
        assert!(!ceiling.contains(".ack("), "the ceiling defers, never acks");
        assert!(!body.contains("msg.retry()"), "no retry without a delay");
        assert!(body.contains("Reflect::get(&m, &\"attempts\".into())"));
        let lib = include_str!("lib.rs");
        assert!(
            lib.contains("crate::dead_letters::park_batch(&event, &env)"),
            "the DLQ batch is handed over as the platform's own"
        );
        assert!(
            !lib.contains("#[event(queue)]"),
            "the queue export is written out (worker-macros hides `attempts`)"
        );
    }

    /// M1: a claim whose send never left (or whose message was dropped) is returned to the parked set by the lever
    /// once it is stale, counted by its rows, and re-driven; a fresh claim is not touched.
    #[test]
    fn e576f_m1_a_stale_redrive_is_returned_and_redriven() {
        let conn = db();
        dead_letter(&conn, "aa", Some("x"), 1_000);
        dead_letter(&conn, "bb", Some("x"), 1_001);
        let t0 = 10_000_000;
        // aa is claimed and its send never leaves (the isolate died): no message, no revert
        let r = parked(
            &conn,
            &RedriveRequest {
                limit: 1,
                txid: Some("aa".into()),
                force: false,
            },
        )
        .remove(0);
        assert_eq!(run(&conn, &claim_query(&r, t0, false)), 1);
        // before the window: the lever does not touch it (bb moves)
        let before = lever(&conn, &all(25), t0 + STALE_REDRIVE_MS - 1);
        assert_eq!(
            before
                .iter()
                .map(|m| m.redrive.clone().unwrap().txid)
                .collect::<Vec<_>>(),
            vec!["bb"]
        );
        assert_eq!(row(&conn, "aa").0, "redriven");
        // past it: returned (counted by the rows the statement returns), selected and sent in the same call
        let returned = rows(
            &conn,
            &stale_return_query(t0 + STALE_REDRIVE_MS + 1, REDRIVE_MAX_LIMIT),
        );
        assert_eq!(returned.len(), 1, "aa only: bb's claim is fresh");
        assert_eq!(returned[0][0], "aa");
        let (status, _, redrives, _, history) = row(&conn, "aa");
        assert_eq!(
            (status.as_str(), redrives),
            ("parked", 1),
            "the spent re-drive stays spent"
        );
        let h: serde_json::Value = serde_json::from_str(&history).unwrap();
        assert_eq!(h.as_array().unwrap().last().unwrap()["kind"], "stale");
        let parked_at: i64 = conn
            .query_row(
                "SELECT parked_at FROM mutation_dead_letters WHERE txid = 'aa'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            parked_at, 1_000,
            "it keeps its place in the oldest-first order"
        );
        let again = lever(&conn, &all(25), t0 + STALE_REDRIVE_MS + 2);
        assert_eq!(again.len(), 1);
        assert_eq!(
            again[0].redrive,
            Some(RedriveTag {
                txid: "aa".into(),
                topics: "tm_a,tm_b".into(),
                n: 2
            })
        );
        // a note of the re-driven replay keeps it `redriven`, so the stale rule still sees it
        let (k, t) = letter_key(&again[0], None);
        run(
            &conn,
            &note_failing_query(&k, &t, "y", t0 + STALE_REDRIVE_MS + 3),
        );
        assert_eq!(row(&conn, "aa").0, "redriven");
        // the statement is bounded
        assert!(STALE_RETURN_SQL.contains("LIMIT ?3"));
    }

    /// M2: a failing note writes NO bytes; the park writes them; an ack deletes the row; a note never demotes a
    /// parked letter; the history is capped; the delete is the one the ownership manifest grants.
    #[test]
    fn e576f_m2_notes_hold_no_bytes_acks_delete_history_capped() {
        let conn = db();
        let big = msg(&"A".repeat(90_000), &["tm_a"]);
        let (k, t) = letter_key(&big, Some("aa"));
        for i in 0..3 {
            run(&conn, &note_failing_query(&k, &t, "a long fault", i));
        }
        assert_eq!(message_of(&conn, "aa"), "", "a note holds no bytes");
        let bytes: i64 = conn.query_row("SELECT length(CAST(txid || topics || message || COALESCE(fault, '') || history AS BLOB)) FROM mutation_dead_letters", [], |r| r.get(0)).unwrap();
        assert!(bytes < 200, "a note is small: {bytes} bytes");
        assert_eq!(run(&conn, &resolve_query(&k, &t)), 1);
        assert!(!exists(&conn, "aa"), "an ack takes the note out");
        // a parked letter: a note of a copy failing on the main queue does not demote it
        let m = serde_json::to_string(&big).unwrap();
        run(&conn, &park_query(&k, &t, &m, FAULT_UNRECORDED, 0, 10));
        run(&conn, &note_failing_query(&k, &t, "a copy failed", 11));
        assert_eq!(row(&conn, "aa").0, "parked");
        assert_eq!(
            parked(&conn, &all(25)).len(),
            1,
            "still in the lever's reach"
        );
        // the history is capped at HISTORY_MAX: a key re-parked forever does not grow its row
        assert_eq!(HISTORY_MAX, 20);
        assert!(PARK_SQL.contains(&format!(
            "json_array_length(mutation_dead_letters.history) >= {HISTORY_MAX}"
        )));
        for i in 0..30 {
            conn.execute("UPDATE mutation_dead_letters SET status = 'redriven'", [])
                .unwrap();
            run(&conn, &park_query(&k, &t, &m, &format!("f{i}"), 0, 100 + i));
        }
        let h: serde_json::Value = serde_json::from_str(&row(&conn, "aa").4).unwrap();
        assert_eq!(h.as_array().unwrap().len(), HISTORY_MAX as usize);
        // the ack of its landed bytes deletes the parked letter too
        assert_eq!(rows(&conn, &resolve_query(&k, &t))[0][0], "parked");
        assert!(!exists(&conn, "aa"));
        // the ownership manifest grants exactly this delete, and nothing else of the crate deletes from the table
        let manifest = include_str!("../../../storage-ownership.json");
        assert!(
            manifest.contains(RESOLVE_SQL),
            "the delete_scope names RESOLVE_SQL verbatim"
        );
    }

    /// M2: at most PARKED_ROWS_CEILING letters hold bytes; a NEW letter past it is deferred (the read says so), a
    /// letter already held is not.
    #[test]
    fn e576f_m2_the_ceiling_defers_a_new_letter_only() {
        let conn = db();
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..PARKED_ROWS_CEILING {
            let st = if i % 2 == 0 { "parked" } else { "redriven" };
            tx.execute(
                "INSERT INTO mutation_dead_letters (txid, topics, message, status, first_seen_at, parked_at) VALUES (?1, 't', '{}', ?2, 1, 1)",
                rusqlite::params![format!("k{i}"), st],
            )
            .unwrap();
        }
        tx.execute("INSERT INTO mutation_dead_letters (txid, topics, message, status, first_seen_at) VALUES ('note', 't', '', 'failing', 1)", []).unwrap();
        tx.commit().unwrap();
        let read = |txid: &str| -> (u64, u64) {
            let r = rows(&conn, &ceiling_query(txid, "t"));
            (r[0][0].parse().unwrap(), r[0][1].parse().unwrap())
        };
        assert_eq!(
            read("new"),
            (PARKED_ROWS_CEILING, 0),
            "a new key at the ceiling: deferred"
        );
        assert_eq!(
            read("k0"),
            (PARKED_ROWS_CEILING, 1),
            "a parked key: re-parked (a copy)"
        );
        assert_eq!(
            read("k1"),
            (PARKED_ROWS_CEILING, 1),
            "a re-driven key: re-parked"
        );
        assert_eq!(
            read("note").1,
            0,
            "a failing note is not under the ceiling, and is deferred past it"
        );
        // the consumer's rule, as written
        let src = include_str!("dead_letters.rs");
        assert!(src.contains("if c.known < 1.0 && c.held >= PARKED_ROWS_CEILING as f64 {"));
    }

    /// L2: another copy of a parked key is not acked blind: the longer bytes are kept and the history says so.
    #[test]
    fn e576f_l2_another_copy_keeps_the_longer_bytes() {
        let conn = db();
        let short = serde_json::to_string(&msg("AA==", &["tm_a"])).unwrap();
        let long = serde_json::to_string(&msg("AAAAAAAA", &["tm_a"])).unwrap();
        run(
            &conn,
            &park_query("aa", "tm_a", &short, FAULT_UNRECORDED, 0, 1),
        );
        let r = rows(
            &conn,
            &park_query("aa", "tm_a", &long, FAULT_UNRECORDED, 0, 2),
        );
        assert_eq!(
            r,
            vec![vec!["0".to_string(), "copy".to_string(), "new".to_string()]]
        );
        assert_eq!(message_of(&conn, "aa"), long);
        let r = rows(
            &conn,
            &park_query("aa", "tm_a", &short, FAULT_UNRECORDED, 0, 3),
        );
        assert_eq!(r[0][1..], ["copy".to_string(), "old".to_string()]);
        assert_eq!(
            message_of(&conn, "aa"),
            long,
            "the shorter copy does not replace the longer"
        );
        assert_eq!(
            run(
                &conn,
                &park_query("aa", "tm_a", &long, FAULT_UNRECORDED, 0, 4)
            ),
            0,
            "the same bytes again: a redelivery"
        );
        let (status, _, _, _, history) = row(&conn, "aa");
        assert_eq!(status, "parked");
        let parked_at: i64 = conn
            .query_row("SELECT parked_at FROM mutation_dead_letters", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(
            parked_at, 1,
            "a copy does not move the letter in the oldest-first order"
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&history)
                .unwrap()
                .as_array()
                .unwrap()
                .len(),
            3
        );
    }

    /// L3: the bytes sent are the bytes the claim found, not the bytes the select read before it.
    #[test]
    fn e576f_l3_the_claim_returns_the_bytes_it_claimed() {
        let conn = db();
        dead_letter(&conn, "aa", Some("x"), 1);
        let r = parked(&conn, &all(25)).remove(0);
        let newer = serde_json::to_string(&msg("BBBBBBBB", &["tm_a", "tm_b"])).unwrap();
        conn.execute("UPDATE mutation_dead_letters SET message = ?1", [&newer])
            .unwrap();
        let c = rows(&conn, &claim_query(&r, 2, false));
        assert_eq!(c[0][1], newer);
        assert!(
            !SELECT_PARKED_SQL.contains("message") && !SELECT_PARKED_TXID_SQL.contains("message"),
            "the select reads no bytes"
        );
    }

    /// L4: the health block's counts read the covering index, not the table; the exhausted are counted in full.
    #[test]
    fn e576f_l4_the_health_counts_read_an_index() {
        let conn = db();
        for i in 0..25 {
            run(
                &conn,
                &park_query(&format!("x{i}"), "t", "{}", "f", MAX_REDRIVES, i),
            );
        }
        dead_letter(&conn, "aa", Some("x"), 100);
        let plan: Vec<String> = {
            let mut s = conn
                .prepare(&format!("EXPLAIN QUERY PLAN {HEALTH_COUNTS_SQL}"))
                .unwrap();
            s.query_map(rusqlite::params![3, 0, 0], |r| r.get::<_, String>(3))
                .unwrap()
                .map(Result::unwrap)
                .collect()
        };
        assert!(
            plan.iter()
                .any(|p| p.contains("COVERING INDEX idx_mutation_dead_letters_health")),
            "{plan:?}"
        );
        let last: Vec<String> = {
            let mut s = conn
                .prepare(&format!("EXPLAIN QUERY PLAN {HEALTH_LAST_REDRIVE_SQL}"))
                .unwrap();
            s.query_map([], |r| r.get::<_, String>(3))
                .unwrap()
                .map(Result::unwrap)
                .collect()
        };
        assert!(
            last.iter()
                .any(|p| p.contains("idx_mutation_dead_letters_redriven")),
            "{last:?}"
        );
        let counts = rows(
            &conn,
            &Query::new(HEALTH_COUNTS_SQL)
                .bind(MAX_REDRIVES)
                .bind(0i64)
                .bind(0i64),
        );
        let parked_row = counts.iter().find(|r| r[0] == "parked").unwrap();
        assert_eq!(
            (parked_row[1].as_str(), parked_row[3].as_str()),
            ("26", "25"),
            "exhaustedCount is the total, past the list's 20"
        );
    }

    /// L5: an exhausted letter is re-driven once more only by txid with `force`, recorded in its history.
    #[test]
    fn e576f_l5_force_redrives_an_exhausted_letter_by_txid() {
        let conn = db();
        dead_letter(&conn, "aa", Some("x"), 1);
        conn.execute(
            "UPDATE mutation_dead_letters SET redrives = ?1",
            [MAX_REDRIVES as i64],
        )
        .unwrap();
        assert!(lever(
            &conn,
            &RedriveRequest {
                limit: 25,
                txid: Some("aa".into()),
                force: false
            },
            2
        )
        .is_empty());
        let sent = lever(
            &conn,
            &RedriveRequest {
                limit: 25,
                txid: Some("aa".into()),
                force: true,
            },
            3,
        );
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].redrive.as_ref().unwrap().n, MAX_REDRIVES + 1);
        let (status, _, redrives, _, history) = row(&conn, "aa");
        assert_eq!(
            (status.as_str(), redrives),
            ("redriven", MAX_REDRIVES as i64 + 1)
        );
        let h: serde_json::Value = serde_json::from_str(&history).unwrap();
        assert_eq!(h.as_array().unwrap().last().unwrap()["kind"], "force");
        assert!(
            lever(
                &conn,
                &RedriveRequest {
                    limit: 25,
                    txid: Some("aa".into()),
                    force: true
                },
                4
            )
            .is_empty(),
            "once per claim"
        );
    }

    /// Fill the table to the ceiling with `parked` letters `k0..`, one of them `redriven` (`k1`); a `failing` note
    /// beside them.
    fn fill_to_the_ceiling(conn: &rusqlite::Connection) {
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..PARKED_ROWS_CEILING {
            let st = if i == 1 { "redriven" } else { "parked" };
            tx.execute(
                "INSERT INTO mutation_dead_letters (txid, topics, message, status, redrives, first_seen_at, parked_at) VALUES (?1, 't', '{\"k\":1}', ?2, ?3, 1, 1)",
                rusqlite::params![format!("k{i}"), st, if i == 0 { MAX_REDRIVES as i64 } else { 0 }],
            )
            .unwrap();
        }
        tx.execute("INSERT INTO mutation_dead_letters (txid, topics, message, status, first_seen_at) VALUES ('note', 't', '', 'failing', 1)", []).unwrap();
        tx.commit().unwrap();
    }

    /// D-M1 (the delta fold): the ceiling DRAINS. Full, a new letter is deferred; the operator discards a parked
    /// letter by key (never a re-drive in flight, never a failing note, never another key); the next new letter is
    /// parked. Health says `near` before `full`.
    #[test]
    fn e576f2_m1_a_discard_makes_room_under_the_ceiling() {
        let conn = db();
        fill_to_the_ceiling(&conn);
        let read = |txid: &str| -> (u64, u64) {
            let r = rows(&conn, &ceiling_query(txid, "tm_a,tm_b"));
            (r[0][0].parse().unwrap(), r[0][1].parse().unwrap())
        };
        // the ceiling reached: a new letter is deferred, as the consumer's rule reads it
        let (held, known) = read("new");
        assert!(
            known == 0 && held >= PARKED_ROWS_CEILING,
            "deferred: {held}/{known}"
        );
        assert_eq!(ceiling_json(held)["full"], true);
        // what the discard does NOT touch: an unknown key, a re-drive in flight, a failing note, another topic set
        assert!(rows(&conn, &discard_query("nope", None)).is_empty());
        assert!(
            rows(&conn, &discard_query("k1", None)).is_empty(),
            "a redriven letter is in flight"
        );
        assert!(
            rows(&conn, &discard_query("note", None)).is_empty(),
            "a failing note holds no bytes"
        );
        assert!(
            rows(&conn, &discard_query("k2", Some("other"))).is_empty(),
            "the key's topics must match"
        );
        assert!(exists(&conn, "k1") && exists(&conn, "note") && exists(&conn, "k2"));
        // the operator discards one parked letter (the exhausted k0) by key: its row and bytes are returned
        let gone = rows(&conn, &discard_query("k0", Some("t")));
        assert_eq!(gone.len(), 1);
        assert_eq!(
            gone[0][..3],
            ["k0".to_string(), "t".to_string(), MAX_REDRIVES.to_string()]
        );
        assert_eq!(gone[0][4], "{\"k\":1}", "its bytes, to log their hash");
        assert!(!exists(&conn, "k0"));
        // the next new letter is PARKED
        let (held, known) = read("new");
        assert_eq!((held, known), (PARKED_ROWS_CEILING - 1, 0));
        assert!(
            held < PARKED_ROWS_CEILING,
            "the consumer's rule now parks it"
        );
        assert_eq!(dead_letter(&conn, "new", Some("x"), 5_000), 1);
        assert_eq!(row(&conn, "new").0, "parked");
        assert_eq!(read("new2").0, PARKED_ROWS_CEILING, "full again");
        // by txid with no topics: every parked row of the txid, and only parked ones
        let m = serde_json::to_string(&msg("AA==", &["tm_c"])).unwrap();
        run(&conn, &park_query("new", "tm_c", &m, "f", 0, 6_000));
        run(&conn, &park_query("new", "tm_d", &m, "f", 0, 6_000));
        conn.execute("UPDATE mutation_dead_letters SET status = 'redriven' WHERE txid = 'new' AND topics = 'tm_d'", []).unwrap();
        let gone = rows(&conn, &discard_query("new", None));
        let mut keys: Vec<&str> = gone.iter().map(|r| r[1].as_str()).collect();
        keys.sort_unstable();
        assert_eq!(keys, vec!["tm_a,tm_b", "tm_c"]);
        assert!(exists(&conn, "new"), "the redriven tm_d row stays");
        // the health block's threshold line
        assert_eq!(CEILING_NEAR, 1600);
        let j = ceiling_json(CEILING_NEAR - 1);
        assert_eq!(
            (j["near"].clone(), j["full"].clone(), j["room"].clone()),
            (false.into(), false.into(), 401.into())
        );
        let j = ceiling_json(CEILING_NEAR);
        assert_eq!(
            (j["near"].clone(), j["full"].clone(), j["nearAt"].clone()),
            (true.into(), false.into(), 1600.into())
        );
        assert_eq!(ceiling_json(PARKED_ROWS_CEILING + 5)["room"], 0);
        // the scoped delete the ownership manifest grants (the checker's 120-character statement), the route, the
        // counter and the DISCARDED line with the bytes' hash
        let manifest = include_str!("../../../storage-ownership.json");
        assert!(
            manifest.contains(&" ".join_ws(DISCARD_SQL)[..120]),
            "the delete_scope names DISCARD_SQL"
        );
        assert!(DISCARD_SQL.contains("AND status = 'parked'"));
        let lib = include_str!("lib.rs");
        assert!(lib.contains("(Method::Post, \"/internal/discard-dead-letters\") => {\n            crate::dead_letters::internal_discard(req, &env).await"));
        let src = include_str!("dead_letters.rs");
        let h = &src[src.find("pub async fn internal_discard(").unwrap()..];
        let h = &h[..h.find("\n}\n").unwrap()];
        assert!(
            h.contains("tip_pass::bearer_ok(") && h.contains("-> 401"),
            "the same bearer"
        );
        assert!(
            h.contains("COUNTER_DEAD_LETTERS_DISCARDED")
                && h.contains("[dead-letters] DISCARDED {} [{}] sha256={sha}")
        );
        let consumer = &src[src.find("pub async fn park_batch(").unwrap()..];
        assert!(
            consumer.contains("the ceiling is NEAR"),
            "each park at or past the threshold logs it"
        );
    }

    trait JoinWs {
        fn join_ws(&self, s: &str) -> String;
    }
    impl JoinWs for str {
        fn join_ws(&self, s: &str) -> String {
            s.split_whitespace().collect::<Vec<_>>().join(self)
        }
    }

    /// D-M1: the discard lever's body: named letters only, 1 to DISCARD_MAX_LETTERS, txids lowercased, duplicates
    /// folded.
    #[test]
    fn e576f2_m1_the_discard_request_parse() {
        assert_eq!(
            parse_discard_request(br#"{"letters": [{"txid": " AB "}, {"txid": "cd", "topics": "tm_a,tm_b"}, {"txid": "ab"}]}"#).unwrap(),
            vec![
                DiscardKey { txid: "ab".into(), topics: None },
                DiscardKey { txid: "cd".into(), topics: Some("tm_a,tm_b".into()) },
            ]
        );
        let max: Vec<serde_json::Value> = (0..DISCARD_MAX_LETTERS)
            .map(|i| serde_json::json!({"txid": format!("t{i}")}))
            .collect();
        assert_eq!(
            parse_discard_request(serde_json::json!({"letters": max}).to_string().as_bytes())
                .unwrap()
                .len(),
            DISCARD_MAX_LETTERS
        );
        let over: Vec<serde_json::Value> = (0..=DISCARD_MAX_LETTERS)
            .map(|i| serde_json::json!({"txid": format!("t{i}")}))
            .collect();
        assert!(
            parse_discard_request(serde_json::json!({"letters": over}).to_string().as_bytes())
                .is_err(),
            "more is refused, not clamped"
        );
        for bad in [
            &b""[..],
            b"{}",
            br#"{"letters": []}"#,
            br#"{"letters": [{}]}"#,
            br#"{"letters": [{"txid": ""}]}"#,
            br#"{"letters": [{"txid": "ab", "topics": ""}]}"#,
            br#"{"letters": ["ab"]}"#,
            br#"{"txid": "ab"}"#,
        ] {
            assert!(
                parse_discard_request(bad).is_err(),
                "{}",
                String::from_utf8_lossy(bad)
            );
        }
    }

    /// D-L1 (the delta fold): a letter deferred at the ceiling on every one of its 1 + 100 DLQ deliveries is ONE
    /// deferred letter and 101 deferrals; the consumer bumps the two counters apart.
    #[test]
    fn e576f2_l1_a_deferred_letter_counts_once() {
        let (mut letters, mut deliveries) = (0, 0);
        for a in 1..=DLQ_MAX_RETRIES + 1 {
            let (l, d) = ceiling_deferral_counts(Some(a));
            letters += l;
            deliveries += d;
        }
        assert_eq!((letters, deliveries), (1, u64::from(DLQ_MAX_RETRIES) + 1));
        assert_eq!(
            ceiling_deferral_counts(None),
            (0, 1),
            "an unreadable count: the delivery only"
        );
        assert_ne!(
            crate::ops::COUNTER_DEAD_LETTERS_CEILING_DEFERRED,
            crate::ops::COUNTER_DEAD_LETTERS_CEILING_DEFERRALS
        );
        let src = include_str!("dead_letters.rs");
        let body = &src[src.find("pub async fn park_batch(").unwrap()..];
        let ceiling = &body[body.find("Ok(Parked::Ceiling(held)) => {").unwrap()
            ..body.find("Err(e) => e,").unwrap()];
        assert!(ceiling.contains("ceiling_deferral_counts(attempts)"));
        assert!(ceiling.contains(
            "COUNTER_DEAD_LETTERS_CEILING_DEFERRED,\n                            letters,"
        ));
        assert!(ceiling.contains(
            "COUNTER_DEAD_LETTERS_CEILING_DEFERRALS,\n                        deliveries,"
        ));
    }

    /// D-L2 (the delta fold): the PARKED, copy and redelivery lines (the acks of a clean park) print the platform's
    /// `attempts`, or "absent", so a live drill that parks cleanly reads it.
    #[test]
    fn e576f2_l2_every_ack_line_prints_attempts() {
        assert_eq!(attempts_text(None), "absent");
        assert_eq!(attempts_text(Some(4)), "4");
        let src = include_str!("dead_letters.rs");
        let start = src.find("pub async fn park_batch(").unwrap();
        let body = &src[start..start + src[start..].find("\n}\n").unwrap()];
        assert!(body.contains("let att = attempts_text(attempts);"));
        for line in [
            "[dead-letters] PARKED ",
            "[dead-letters] another copy of the parked ",
            "was already parked with these bytes",
        ] {
            let at = body.find(line).unwrap_or_else(|| panic!("{line}"));
            let end = at + body[at..].find('"').unwrap();
            assert!(
                body[at..end].contains("attempts={att}"),
                "{line}: {}",
                &body[at..end]
            );
        }
    }

    /// D-L3 (the delta fold): `resolve` says what happened: the eviction skip's bytes did NOT land.
    #[test]
    fn e576f2_l3_resolve_says_what_happened() {
        assert!(Resolved::Landed.says().contains("landed"));
        assert!(
            !Resolved::RefusedEvicted.says().contains("landed")
                && Resolved::RefusedEvicted.says().contains("REFUSED")
        );
        assert!(Resolved::ReEvicted.says().contains("re-evicted"));
        let src = include_str!("dead_letters.rs");
        let r = &src[src.find("pub async fn resolve(").unwrap()..];
        let r = &r[..r.find("\n}\n").unwrap()];
        assert!(r.contains("why.says()") && !r.contains("its bytes landed"));
        let lib = include_str!("lib.rs");
        let skip = lib.find("SKIPPED — under an open eviction").unwrap();
        let reev = lib.find("outran an eviction").unwrap();
        let next = |from: usize| lib[from..].find("crate::dead_letters::resolve(").unwrap() + from;
        assert!(lib[next(skip)..next(skip) + 400]
            .contains("crate::dead_letters::Resolved::RefusedEvicted,"));
        assert!(
            lib[next(reev)..next(reev) + 400].contains("crate::dead_letters::Resolved::ReEvicted,")
        );
        assert_eq!(
            lib.matches("crate::dead_letters::Resolved::Landed").count(),
            1
        );
    }

    /// The main consumer notes a fault before EVERY hand-back of a decodable message, resolves at every ack, and
    /// drops nothing on its own (the lens's N4 and M1: an undecodable message and bad base64 go to the DLQ, to be
    /// parked; a source-shape pin: the queue handler runs only in wasm).
    #[test]
    fn e576_the_main_consumer_notes_each_retry_and_resolves_each_ack() {
        let src = include_str!("lib.rs");
        let entry = &src[src.find("async fn queue_entry(").unwrap()..];
        assert!(entry[..entry.find("\n}\n").unwrap()]
            .contains("crate::dead_letters::is_dead_letter_queue(&name)"));
        let start = src.find("async fn queue_handler(").unwrap();
        let body = &src[start..start + src[start..].find("\n}\n").unwrap()];
        assert!(
            body.contains("batch.raw_iter()"),
            "every message keeps its handle, a body that does not decode too"
        );
        let retries = body.matches("msg.retry();").count();
        let notes = body.matches("crate::dead_letters::note_failing(").count();
        assert!(retries >= 7);
        assert_eq!(
            notes,
            retries - 1,
            "one fault note per hand-back (the undecodable body has no key to note)"
        );
        let resolves = body.matches("crate::dead_letters::resolve(").count();
        assert_eq!(
            resolves, 3,
            "the eviction skip, the re-eviction and the durable ack"
        );
        assert_eq!(
            body.matches("msg.ack();").count(),
            3,
            "the only acks: the eviction skip, the re-eviction, the durable ack"
        );
    }
}
