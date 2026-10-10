//! NL-6c (the charter "a BEEF of any size", bsv-stack-lean
//! `docs/charters/beef-of-any-size.md`): the broadcast-gated arm's EF work
//! past the request's budget is DEFERRED, never refused.
//!
//! Until NL-6c the arm answered 429 "EF too large ... retry via fallback" when
//! the subject's Extended Format passed 256 KiB or the batch's passed 2 MiB
//! (`MAX_SUBJECT_EF_BYTES`, `MAX_BATCH_EF_BYTES` in `routes.rs`, #211/#209).
//! Those numbers were a WORK bound (the broadcast POST, the SEEN poll and the
//! script walk inside one request's CPU slice), and a work bound is the
//! platform's, so the posture routes around it:
//!
//! 1. **The request keeps a budget** ([`IN_REQUEST_SUBJECT_EF_BYTES`],
//!    [`IN_REQUEST_BATCH_EF_BYTES`]): the work that fits is done in the
//!    request, exactly as before, and answered with the request's own word.
//! 2. **Past it, the bytes go to rest** ([`defer`]): in R2 under
//!    [`R2_PREFIX`] when the Worker has the `BEEF_BLOBS` binding (bsv-low #585
//!    door 3), else in D1 as chunks of [`AT_REST_CHUNK_BYTES`], under D1's
//!    2,000,000-byte row; a job row names them, keyed by a reference that is
//!    the sha256 of what was submitted ([`job_reference`]).
//! 3. **The caller is answered 202** ([`deferral_body`]): `accepted`, the
//!    reference, the path it polls (`GET /submit-deferred/<reference>`), the
//!    subject's txid and the work's two numbers. Never a 429, never a 413.
//! 4. **The queue consumer finishes the work** ([`run_job`]): the mutation
//!    queue carries the reference, never the bytes; the consumer reads them
//!    back, runs the SAME gated arm the request runs
//!    (`routes::submit_resumed`, the budget marked [`WorkBudget::Resumed`]),
//!    and records the arm's answer on the job (status and body), which the
//!    poll then serves.
//! 5. **Resumable** ([`redrive`], on the cron): a job whose consumer did not
//!    finish (the invocation ended, a 5xx or a 429 answer) is handed to the
//!    queue again once it has been quiet for [`JOB_RESUME_AFTER_MS`], up to
//!    [`JOB_ATTEMPTS`] runs; every step of the arm is idempotent (Arcade
//!    dedupes a re-presented subject; every engine write is `INSERT OR
//!    IGNORE` / `OR REPLACE`).
//!
//! 6. **The corroboration's legs** (NL-6d, bsv-stack-lean #60): the #267
//!    ancestry-primed corroboration posts each ancestor to the corroborating
//!    host, one at a time. The request reads at most
//!    [`IN_REQUEST_CORROBORATION_LEGS`] of them; a walk with more left PAUSES
//!    at the leg it reached and the request defers the job with that leg as
//!    its cursor (`legs_from`); each run reads at most
//!    [`RUN_CORROBORATION_LEGS`] from the cursor and, when legs are left,
//!    records the leg it reached, answers nothing to the job's attempts and
//!    hands the job straight back to the queue. A run that advanced never
//!    counts toward [`JOB_ATTEMPTS`]; no run starts over. Until NL-6d a
//!    corroboration of more than 32 legs was refused (`MAX_CORROBORATION_LEGS`,
//!    502), and a deferred job answered that 502 on every run and ended
//!    `failed`.
//!
//! **The ceiling this does not route past, named.** The request still reads
//! its body whole (`req.bytes()`, `routes.rs`), and the arm converts it to EF
//! before it knows the work's size; the consumer holds the bytes whole again
//! to run the arm. So one submission is bounded by what one isolate holds and
//! one invocation's CPU slice does: the platform's 128 MB isolate (the body
//! and its copies; NL-6 saw `wrangler dev` reload its worker after a 12 MiB
//! body) and the plan's request-body limit (100 MB on the Free and Pro
//! plans). Past those the platform ends the request; no number of ours does.
//! Resumption is at the job's grain, and inside the corroboration's walk at
//! the leg's (NL-6d); any other step that cannot finish in one consumer
//! invocation fails the same way on each of the [`JOB_ATTEMPTS`] runs and the
//! job ends `failed` with its last answer. One leg is one POST: a single
//! ancestor the corroborating host cannot answer within an invocation is the
//! grain this does not divide.

use serde::{Deserialize, Serialize};
use worker::{Context, D1Database, Env, Response};

use crate::d1::Query;

/// The request's budget for the subject's EF, bytes: past it the work is
/// deferred. The number #211 sized as "generous headroom" for one LOW
/// transaction; a routing number now, never a refusal.
pub const IN_REQUEST_SUBJECT_EF_BYTES: usize = 256 * 1024;

/// The request's budget for the whole EF batch (the async-REJECTED fallback
/// re-submits every leg), bytes: past it the work is deferred.
pub const IN_REQUEST_BATCH_EF_BYTES: usize = 2 * 1024 * 1024;

/// NL-6d: the ancestors one request posts to the corroborating host at most
/// (the #267 sizing of the request's serial work: ~32 POSTs, ~10 s). Past it
/// the walk pauses and the job is deferred from the leg it reached; a routing
/// number, never a refusal.
pub const IN_REQUEST_CORROBORATION_LEGS: usize = 32;

/// NL-6d: the ancestors one consumer run posts at most before it hands the job
/// to the next run (each a POST of at most two hosts, well inside one
/// invocation's 15-minute wall at a host's answer time).
pub const RUN_CORROBORATION_LEGS: usize = 256;

/// One chunk of bytes at rest in D1: half of D1's 2,000,000-byte bound on a
/// row, so a chunk and its row's other columns always fit.
pub const AT_REST_CHUNK_BYTES: usize = 1_000_000;

/// Runs of one job before it ends `failed` (each run is the whole arm).
pub const JOB_ATTEMPTS: i64 = 5;

/// A job queued or running and untouched this long is handed to the queue
/// again by the cron (the consumer's invocation ended without settling it).
/// Ten minutes: past the consumer's 15-minute wall it would be too late to
/// matter, and a running arm writes no row while it works, so a shorter wait
/// would hand a live run to a second consumer.
pub const JOB_RESUME_AFTER_MS: i64 = 10 * 60 * 1000;

/// A settled job's row is kept this long for the caller's poll, then swept
/// with its bytes.
pub const JOB_KEEP_MS: i64 = 7 * 24 * 60 * 60 * 1000;

/// The most bytes of the arm's answer a job keeps (a STEAK is KB-scale).
pub const ANSWER_KEEP_BYTES: usize = 64 * 1024;

/// The poll's path head: `GET /submit-deferred/<reference>`.
pub const POLL_PREFIX: &str = "/submit-deferred/";

/// The R2 binding bsv-low #585 door 3 adds (`BEEF_BLOBS`); the deferred
/// bytes live under their own prefix, which that door's orphan sweep does not
/// list: their own pass does (`beef_blob_sweep::ef_sweep_pass`, N5), counting
/// them for `/health/invariants.queue.r2.efDeferred` and deleting an object no
/// job row names past [`JOB_KEEP_MS`] (L1 (b)).
pub const BEEF_BLOBS_BINDING: &str = "BEEF_BLOBS";
pub const R2_PREFIX: &str = "ef-deferred/";

/// Where the arm runs: in the request, under the budget, or resumed by the
/// consumer, which owes no request budget. NL-6d: a resumed run carries the
/// corroboration's cursor, the ancestors earlier invocations primed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkBudget {
    InRequest,
    Resumed { from_leg: usize },
}

impl WorkBudget {
    /// PURE. NL-6d: the corroboration's ancestor window of this invocation.
    pub fn legs(self) -> crate::broadcaster::LegWindow {
        match self {
            Self::InRequest => crate::broadcaster::LegWindow::in_request(),
            Self::Resumed { from_leg } => crate::broadcaster::LegWindow {
                from: from_leg,
                budget: RUN_CORROBORATION_LEGS,
            },
        }
    }

    /// PURE. NL-6d: a run that resumes a walk past its first leg (an earlier
    /// invocation already presented the work).
    pub fn resumed_mid_walk(self) -> bool {
        matches!(self, Self::Resumed { from_leg } if from_leg > 0)
    }
}

/// The EF work of one gated submission, by its two numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EfWork {
    pub subject_ef_bytes: usize,
    pub batch_ef_bytes: usize,
    /// NL-6d: the EF legs (the subject and its unproven ancestors).
    pub legs: usize,
}

impl EfWork {
    /// PURE. The subject's EF (0 when the subject has no leg: the all-proven
    /// mined claim) and the sum of every leg's.
    pub fn of(efs: &[crate::ef::EfTx], subject_txid: &str) -> Self {
        Self {
            subject_ef_bytes: efs
                .iter()
                .find(|e| e.txid == subject_txid)
                .map_or(0, |e| e.ef.len()),
            batch_ef_bytes: efs.iter().map(|e| e.ef.len()).sum(),
            legs: efs.len(),
        }
    }

    /// PURE. Does the work fit the request's budget?
    pub fn fits_request(&self) -> bool {
        self.subject_ef_bytes <= IN_REQUEST_SUBJECT_EF_BYTES
            && self.batch_ef_bytes <= IN_REQUEST_BATCH_EF_BYTES
    }
}

/// PURE. The job's reference: the sha256, lowercase hex, of the topics (as
/// JSON), the BEEF and the off-chain values, each framed by its length, so
/// the same submission sent again names the same job and two submissions
/// never share one.
pub fn job_reference(topics_json: &str, beef: &[u8], off_chain: Option<&[u8]>) -> String {
    let mut framed = Vec::with_capacity(topics_json.len() + beef.len() + 32);
    for part in [Some(topics_json.as_bytes()), Some(beef), off_chain] {
        match part {
            Some(p) => {
                framed.push(1);
                framed.extend_from_slice(&(p.len() as u64).to_le_bytes());
                framed.extend_from_slice(p);
            }
            None => framed.push(0),
        }
    }
    hex::encode(bsv_rs::primitives::hash::sha256(&framed))
}

/// PURE. The chunks of `len` bytes, `[start, end)`, each at most `chunk`.
/// Zero bytes are one empty chunk, so every job has a chunk 0.
pub fn chunk_spans(len: usize, chunk: usize) -> Vec<(usize, usize)> {
    if len == 0 {
        return vec![(0, 0)];
    }
    (0..len.div_ceil(chunk))
        .map(|i| (i * chunk, ((i + 1) * chunk).min(len)))
        .collect()
}

/// What a run's answer does to its job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settle {
    /// The arm's own verdict (2xx, or a 4xx that names the submission): the
    /// job is `done` and its bytes are released.
    Done,
    /// A fault the arm says to retry (5xx, 429): the job is queued again.
    Again,
}

/// PURE. The arm answers 502 and 503 for transport trouble and an undurable
/// write, both retryable by its own word; a 429 is a "slow down". Every other
/// status is the verdict.
pub fn settle(status: u16) -> Settle {
    if status >= 500 || status == 429 {
        Settle::Again
    } else {
        Settle::Done
    }
}

/// PURE. Is a job with this state, untouched since `updated_at`, still being
/// worked (so a re-presentation names it and writes nothing)?
pub fn job_open(state: &str, updated_at: i64, now: i64) -> bool {
    matches!(state, "queued" | "running") && now - updated_at < JOB_RESUME_AFTER_MS
}

/// PURE. The 202's body. `legs_from`: the corroboration's ancestors the
/// request already primed (NL-6d; 0 when the request deferred for its bytes).
pub fn deferral_body(
    reference: &str,
    subject_txid: &str,
    work: EfWork,
    legs_from: usize,
) -> serde_json::Value {
    serde_json::json!({
        "status": "accepted",
        "accepted": true,
        "deferred": true,
        "reference": reference,
        "poll": format!("{POLL_PREFIX}{reference}"),
        "subjectTxid": subject_txid,
        "work": work,
        "legsFrom": legs_from,
        "budget": {
            "subjectEfBytes": IN_REQUEST_SUBJECT_EF_BYTES,
            "batchEfBytes": IN_REQUEST_BATCH_EF_BYTES,
            "corroborationLegs": IN_REQUEST_CORROBORATION_LEGS,
        },
        "message": "the work of this submission is past one request's budget: its bytes are at rest and the \
                    overlay finishes the broadcast-gated work on its queue; poll the reference for the answer",
    })
}

/// PURE. NL-6d: a resumed run's answer when its corroboration paused with
/// legs left: `resumeAt` is the cursor the next run starts from.
pub fn paused_body(reached: usize, work: EfWork) -> serde_json::Value {
    serde_json::json!({
        "status": "accepted",
        "accepted": true,
        "deferred": true,
        "resumeAt": reached,
        "work": work,
        "message": "the corroboration's walk reached the end of this run's window; the next run resumes it",
    })
}

/// PURE. NL-6d: the cursor a run's answer hands the next run, if it paused.
pub fn resume_point(status: u16, body: &str) -> Option<usize> {
    if status != 202 {
        return None;
    }
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    v.get("resumeAt")?.as_u64().map(|n| n as usize)
}

/// PURE. The poll's reference from its path, 64 lowercase hex or nothing.
pub fn poll_reference(path: &str) -> Option<&str> {
    let r = path.strip_prefix(POLL_PREFIX)?;
    (r.len() == 64 && r.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))).then_some(r)
}

// ── the tables ────────────────────────────────────────────────────────────────

/// One row per deferred submission. `at_rest` is `d1` (the chunks table) or
/// `r2` (`BEEF_BLOBS`, `ef-deferred/<reference>`); `beef_len` splits the
/// stored stream back into the BEEF and the off-chain values (`has_off_chain`).
pub const JOBS_CREATE: &str = "CREATE TABLE IF NOT EXISTS ef_deferred_jobs (
        reference TEXT PRIMARY KEY,
        subject_txid TEXT NOT NULL,
        topics TEXT NOT NULL,
        submit_mode TEXT NOT NULL,
        has_off_chain INTEGER NOT NULL,
        beef_len INTEGER NOT NULL,
        bytes INTEGER NOT NULL,
        at_rest TEXT NOT NULL,
        chunks INTEGER NOT NULL,
        subject_ef_bytes INTEGER NOT NULL,
        batch_ef_bytes INTEGER NOT NULL,
        state TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        status INTEGER,
        answer TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
    )";

/// The cron's read: open jobs by age, settled jobs by age.
pub const JOBS_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS idx_ef_deferred_jobs_state ON ef_deferred_jobs (state, updated_at)";

/// The bytes at rest in D1, a chunk a row.
pub const CHUNKS_CREATE: &str = "CREATE TABLE IF NOT EXISTS ef_deferred_chunks (
        reference TEXT NOT NULL,
        idx INTEGER NOT NULL,
        bytes BLOB NOT NULL,
        PRIMARY KEY (reference, idx)
    )";

/// NL-6d: the corroboration's cursor, the ancestors earlier invocations primed.
pub const JOBS_ADD_LEGS_FROM: &str =
    "ALTER TABLE ef_deferred_jobs ADD COLUMN legs_from INTEGER NOT NULL DEFAULT 0";

const JOB_READ_SQL: &str = "SELECT reference, subject_txid, topics, submit_mode, has_off_chain, beef_len, bytes, \
     at_rest, chunks, subject_ef_bytes, batch_ef_bytes, state, attempts, status, answer, created_at, updated_at, \
     legs_from FROM ef_deferred_jobs WHERE reference = ?1";

/// A new or re-presented job: queued, its attempts and answer cleared.
const JOB_UPSERT_SQL: &str = "INSERT INTO ef_deferred_jobs (reference, subject_txid, topics, submit_mode, \
     has_off_chain, beef_len, bytes, at_rest, chunks, subject_ef_bytes, batch_ef_bytes, state, attempts, status, \
     answer, created_at, updated_at, legs_from) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'queued', 0, NULL, \
     NULL, ?12, ?12, ?13) ON CONFLICT(reference) DO UPDATE SET subject_txid = excluded.subject_txid, topics = excluded.topics, \
     submit_mode = excluded.submit_mode, has_off_chain = excluded.has_off_chain, beef_len = excluded.beef_len, \
     bytes = excluded.bytes, at_rest = excluded.at_rest, chunks = excluded.chunks, \
     subject_ef_bytes = excluded.subject_ef_bytes, batch_ef_bytes = excluded.batch_ef_bytes, state = 'queued', \
     attempts = 0, status = NULL, answer = NULL, updated_at = excluded.updated_at, legs_from = excluded.legs_from";

/// The consumer takes a run: `running`, one more attempt.
const JOB_TAKE_SQL: &str =
    "UPDATE ef_deferred_jobs SET state = 'running', attempts = attempts + 1, updated_at = ?2 \
     WHERE reference = ?1";

/// A run's end: the state, the arm's status and answer.
const JOB_SETTLE_SQL: &str =
    "UPDATE ef_deferred_jobs SET state = ?2, status = ?3, answer = ?4, updated_at = ?5 \
     WHERE reference = ?1";

/// NL-6d: a run whose corroboration advanced: queued again from the leg it
/// reached, its attempts cleared (a run that advanced is no failed run). Only
/// forward: a cursor never moves back.
const JOB_ADVANCE_SQL: &str =
    "UPDATE ef_deferred_jobs SET state = 'queued', legs_from = ?2, attempts = 0, status = ?3, answer = ?4, \
     updated_at = ?5 WHERE reference = ?1 AND legs_from < ?2";

/// The cron hands a quiet open job to the queue again and marks the hand-off:
/// `queued` (a `running` row this quiet is a run that ended without settling),
/// so the consumer the message reaches takes it.
const JOB_TOUCH_SQL: &str =
    "UPDATE ef_deferred_jobs SET state = 'queued', updated_at = ?2 WHERE reference = ?1";

const JOBS_QUIET_SQL: &str =
    "SELECT reference FROM ef_deferred_jobs WHERE state IN ('queued', 'running') \
     AND updated_at < ?1 ORDER BY updated_at LIMIT 20";

const JOBS_EXPIRED_SQL: &str =
    "SELECT reference, at_rest FROM ef_deferred_jobs WHERE state IN ('done', 'failed') \
     AND updated_at < ?1 ORDER BY updated_at LIMIT 50";

/// The sweep's delete of one settled job, conditional (N3, LOW's E585 land2 lens): a re-presentation that
/// landed between the sweep's select and this delete upserted the row to `queued` and re-put the bytes, so the
/// row is deleted only while it is still settled and past the cutoff, and its bytes are released only when a
/// row came back.
const JOB_DELETE_SQL: &str = "DELETE FROM ef_deferred_jobs WHERE reference = ?1 \
     AND state IN ('done', 'failed') AND updated_at < ?2 RETURNING at_rest";

const CHUNK_INSERT_SQL: &str =
    "INSERT OR REPLACE INTO ef_deferred_chunks (reference, idx, bytes) VALUES (?1, ?2, ?3)";

const CHUNK_READ_SQL: &str =
    "SELECT hex(bytes) AS hex FROM ef_deferred_chunks WHERE reference = ?1 AND idx = ?2";

const CHUNKS_DELETE_SQL: &str = "DELETE FROM ef_deferred_chunks WHERE reference = ?1";

/// One job row.
#[derive(Debug, Clone, Deserialize)]
pub struct Job {
    pub reference: String,
    pub subject_txid: String,
    pub topics: String,
    pub submit_mode: String,
    pub has_off_chain: f64,
    pub beef_len: f64,
    pub bytes: f64,
    pub at_rest: String,
    pub chunks: f64,
    pub subject_ef_bytes: f64,
    pub batch_ef_bytes: f64,
    pub state: String,
    pub attempts: f64,
    pub status: Option<f64>,
    pub answer: Option<String>,
    pub created_at: f64,
    pub updated_at: f64,
    /// NL-6d: the corroboration's cursor.
    #[serde(default)]
    pub legs_from: f64,
}

impl Job {
    /// PURE. The poll's body.
    pub fn poll_body(&self) -> serde_json::Value {
        let answer = self.status.map(|status| {
            let text = self.answer.clone().unwrap_or_default();
            let body = serde_json::from_str::<serde_json::Value>(&text)
                .unwrap_or(serde_json::Value::String(text));
            serde_json::json!({ "status": status as u16, "body": body })
        });
        serde_json::json!({
            "reference": self.reference,
            "state": self.state,
            "subjectTxid": self.subject_txid,
            "bytes": self.bytes as u64,
            "atRest": self.at_rest,
            "work": {
                "subjectEfBytes": self.subject_ef_bytes as u64,
                "batchEfBytes": self.batch_ef_bytes as u64,
            },
            "attempts": self.attempts as i64,
            "maxAttempts": JOB_ATTEMPTS,
            "legsFrom": self.legs_from as i64,
            "createdAt": self.created_at as i64,
            "updatedAt": self.updated_at as i64,
            "answer": answer,
        })
    }
}

async fn read_job(db: &D1Database, reference: &str) -> Result<Option<Job>, String> {
    Query::new(JOB_READ_SQL)
        .bind(reference)
        .fetch_optional::<Job>(db)
        .await
}

fn now_ms() -> i64 {
    worker::Date::now().as_millis() as i64
}

fn json_answer(body: &serde_json::Value, status: u16) -> worker::Result<Response> {
    let mut resp = Response::from_json(body)?.with_status(status);
    crate::routes::add_cors_headers(&mut resp);
    Ok(resp)
}

// ── the request's side ───────────────────────────────────────────────────────

/// Put a gated submission past the request's budget at rest, queue its
/// reference and answer 202 with it. A submission whose job is still open is
/// answered with the same reference and nothing is written again. Only a
/// fault in putting the bytes at rest is answered otherwise: 503, retryable
/// (the store's fault, never the submission's size). A queue fault after the
/// bytes and the row landed is not one: the cron hands the job to the queue.
pub async fn defer(
    env: &Env,
    tagged: &overlay_engine::types::TaggedBEEF,
    submit_mode: &str,
    subject_txid: &str,
    work: EfWork,
    legs_from: usize,
) -> worker::Result<Response> {
    match put_at_rest(env, tagged, submit_mode, subject_txid, work, legs_from).await {
        Ok(reference) => {
            worker::console_log!(
                "POST /submit(broadcast-gated) -> 202 (NL-6c/NL-6d: EF work {} B subject / {} B batch, {} legs, {legs_from} primed; past the request's budget {IN_REQUEST_SUBJECT_EF_BYTES} / {IN_REQUEST_BATCH_EF_BYTES} B, {IN_REQUEST_CORROBORATION_LEGS} legs; deferred as {reference})",
                work.subject_ef_bytes,
                work.batch_ef_bytes,
                work.legs
            );
            let mut resp = json_answer(
                &deferral_body(&reference, subject_txid, work, legs_from),
                202,
            )?;
            let _ = resp
                .headers_mut()
                .set("Location", &format!("{POLL_PREFIX}{reference}"));
            Ok(resp)
        }
        Err(e) => {
            worker::console_log!(
                "POST /submit(broadcast-gated) -> 503 (NL-6c: the deferred bytes could not be put at rest: {e})"
            );
            let mut resp = json_answer(
                &serde_json::json!({
                    "status": "error",
                    "message": format!("the submission's bytes could not be put at rest ({e}); retry"),
                    "retryable": true,
                }),
                503,
            )?;
            let _ = resp.headers_mut().set("Retry-After", "1");
            Ok(resp)
        }
    }
}

async fn put_at_rest(
    env: &Env,
    tagged: &overlay_engine::types::TaggedBEEF,
    submit_mode: &str,
    subject_txid: &str,
    work: EfWork,
    legs_from: usize,
) -> Result<String, String> {
    let db = env
        .d1("OVERLAY_DB")
        .map_err(|e| format!("OVERLAY_DB: {e}"))?;
    let topics = serde_json::to_string(&tagged.topics).map_err(|e| e.to_string())?;
    let off_chain = tagged.off_chain_values.as_deref();
    let reference = job_reference(&topics, &tagged.beef, off_chain);
    let now = now_ms();
    if let Some(job) = read_job(&db, &reference).await? {
        if job_open(&job.state, job.updated_at as i64, now) {
            return Ok(reference);
        }
    }
    let beef = &tagged.beef[..];
    let rest = off_chain.unwrap_or(&[]);
    let total = beef.len() + rest.len();
    // The stored stream is the BEEF, then the off-chain values; a span is
    // copied out of the two slices, never out of a joined copy.
    let span = |start: usize, end: usize| -> Vec<u8> {
        let mut out = Vec::with_capacity(end - start);
        if start < beef.len() {
            out.extend_from_slice(&beef[start..end.min(beef.len())]);
        }
        if end > beef.len() {
            out.extend_from_slice(&rest[start.max(beef.len()) - beef.len()..end - beef.len()]);
        }
        out
    };
    let (at_rest, chunks) = match env.bucket(BEEF_BLOBS_BINDING) {
        Ok(bucket) => {
            // L1 (a): R2 checks the sha256 it is given against what it stored (door 3's rule,
            // `queue::put_beef`), so a corrupted upload is refused here, never found at the run; the
            // digest is of the stored stream (the reference hashes the framed submission). The
            // `touched` stamp ages the object for the pass over the prefix (L1 (b)).
            let stored = span(0, total);
            let digest = bsv_rs::primitives::hash::sha256(&stored).to_vec();
            bucket
                .put(format!("{R2_PREFIX}{reference}"), stored)
                .sha256(digest)
                .custom_metadata(crate::queue::touched_meta(now))
                .execute()
                .await
                .map_err(|e| format!("R2 put: {e}"))?;
            ("r2", 1usize)
        }
        Err(_) => {
            Query::new(CHUNKS_DELETE_SQL)
                .bind(reference.as_str())
                .execute(&db)
                .await?;
            let spans = chunk_spans(total, AT_REST_CHUNK_BYTES);
            for (i, (start, end)) in spans.iter().enumerate() {
                Query::new(CHUNK_INSERT_SQL)
                    .bind(reference.as_str())
                    .bind(i as i64)
                    .bind(span(*start, *end))
                    .execute(&db)
                    .await?;
            }
            ("d1", spans.len())
        }
    };
    Query::new(JOB_UPSERT_SQL)
        .bind(reference.as_str())
        .bind(subject_txid)
        .bind(topics)
        .bind(submit_mode)
        .bind(off_chain.is_some())
        .bind(beef.len() as i64)
        .bind(total as i64)
        .bind(at_rest)
        .bind(chunks as i64)
        .bind(work.subject_ef_bytes as i64)
        .bind(work.batch_ef_bytes as i64)
        .bind(now)
        .bind(legs_from as i64)
        .execute(&db)
        .await?;
    if let Err(e) = send_job(env, &reference).await {
        worker::console_log!(
            "NL-6c: deferred job {reference} is at rest but not queued ({e}); the cron hands it to the queue"
        );
    }
    Ok(reference)
}

/// NL-6d: the arm's corroboration paused with `reached` ancestors primed and
/// legs left. In the request: the job is deferred from that leg (the 202 of
/// [`defer`]). In a run: answered 202 with `resumeAt`, which [`run_job`] turns
/// into the job's cursor and the next run.
pub async fn pause(
    env: &Env,
    tagged: &overlay_engine::types::TaggedBEEF,
    submit_mode: &str,
    subject_txid: &str,
    work: EfWork,
    budget: WorkBudget,
    reached: usize,
) -> worker::Result<Response> {
    match budget {
        WorkBudget::InRequest => defer(env, tagged, submit_mode, subject_txid, work, reached).await,
        WorkBudget::Resumed { .. } => json_answer(&paused_body(reached, work), 202),
    }
}

async fn send_job(env: &Env, reference: &str) -> Result<(), String> {
    env.queue("MUTATION_QUEUE")
        .map_err(|e| format!("MUTATION_QUEUE binding unavailable: {e}"))?
        .send(crate::queue::ef_job_message(reference))
        .await
        .map_err(|e| format!("mutation queue send failed: {e}"))
}

/// `GET /submit-deferred/<reference>`: the job's state and, once a run has
/// ended, the arm's answer. 404 for a reference no job carries.
pub async fn poll(env: &Env, path: &str) -> worker::Result<Response> {
    let Some(reference) = poll_reference(path) else {
        return json_answer(
            &serde_json::json!({"status": "error", "message": "a reference is 64 lowercase hex"}),
            400,
        );
    };
    let db = env.d1("OVERLAY_DB")?;
    if let Err(e) = crate::d1::ensure_overlay_migrations(&db).await {
        return json_answer(
            &serde_json::json!({"status": "error", "message": format!("migrations: {e}")}),
            503,
        );
    }
    match read_job(&db, reference).await {
        Ok(Some(job)) => json_answer(&job.poll_body(), 200),
        Ok(None) => json_answer(
            &serde_json::json!({"status": "error", "message": "no deferred submission has this reference"}),
            404,
        ),
        Err(e) => json_answer(
            &serde_json::json!({"status": "error", "message": format!("the job could not be read ({e}); retry")}),
            503,
        ),
    }
}

// ── the consumer's side ──────────────────────────────────────────────────────

async fn load(env: &Env, db: &D1Database, job: &Job) -> Result<Vec<u8>, String> {
    let total = job.bytes as usize;
    let bytes = if job.at_rest == "r2" {
        let bucket = env
            .bucket(BEEF_BLOBS_BINDING)
            .map_err(|e| format!("the R2 binding is gone: {e}"))?;
        let object = bucket
            .get(format!("{R2_PREFIX}{}", job.reference))
            .execute()
            .await
            .map_err(|e| format!("R2 get: {e}"))?
            .ok_or("the R2 object is gone")?;
        object
            .body()
            .ok_or("the R2 object has no body")?
            .bytes()
            .await
            .map_err(|e| format!("R2 body: {e}"))?
    } else {
        #[derive(Deserialize)]
        struct Chunk {
            hex: String,
        }
        let mut out = Vec::with_capacity(total);
        for idx in 0..job.chunks as i64 {
            let chunk = Query::new(CHUNK_READ_SQL)
                .bind(job.reference.as_str())
                .bind(idx)
                .fetch_optional::<Chunk>(db)
                .await?
                .ok_or_else(|| format!("chunk {idx} is gone"))?;
            out.extend_from_slice(&hex::decode(&chunk.hex).map_err(|e| e.to_string())?);
        }
        out
    };
    if bytes.len() != total {
        return Err(format!(
            "{} bytes at rest, the job names {total}",
            bytes.len()
        ));
    }
    Ok(bytes)
}

async fn release(env: &Env, db: &D1Database, reference: &str, at_rest: &str) {
    if at_rest == "r2" {
        if let Ok(bucket) = env.bucket(BEEF_BLOBS_BINDING) {
            if let Err(e) = bucket.delete(format!("{R2_PREFIX}{reference}")).await {
                worker::console_log!("NL-6c: the R2 object of {reference} was not deleted ({e})");
            }
        }
    } else if let Err(e) = Query::new(CHUNKS_DELETE_SQL)
        .bind(reference)
        .execute(db)
        .await
    {
        worker::console_log!("NL-6c: the chunks of {reference} were not deleted ({e})");
    }
}

async fn settle_job(db: &D1Database, reference: &str, state: &str, status: u16, answer: &str) {
    let mut kept = answer.to_string();
    if kept.len() > ANSWER_KEEP_BYTES {
        let mut cut = ANSWER_KEEP_BYTES;
        while !kept.is_char_boundary(cut) {
            cut -= 1;
        }
        kept.truncate(cut);
    }
    if let Err(e) = Query::new(JOB_SETTLE_SQL)
        .bind(reference)
        .bind(state)
        .bind(status as i64)
        .bind(kept)
        .bind(now_ms())
        .execute(db)
        .await
    {
        worker::console_log!(
            "NL-6c: job {reference} could not be settled {state} ({e}); the cron hands it back"
        );
    }
}

/// The consumer's run of one deferred job. The job's own row carries its
/// attempts, and the cron hands a job that did not settle back to the queue
/// ([`redrive`]); the consumer acks the message once this returns, whatever
/// the run did.
///
/// **The bound when a run never returns** (N4, LOW's E585 land2 lens,
/// bsv-low's `docs/audit/E585-land2-lens-2026-10-10.md`). A run that kills the isolate
/// (the very work deferred: its memory, its CPU) never reaches the ack, so
/// the platform redelivers the message (the mutation queue's `max_retries` =
/// 3, no `retry_delay`, `wrangler.toml`). A redelivery that finds the row
/// `running` and touched less than [`JOB_RESUME_AFTER_MS`] ago takes nothing
/// and is acked (the live-run guard below). A redelivery later than that
/// (the platform's delay is its own) is a [`JOB_TAKE_SQL`], one of the
/// [`JOB_ATTEMPTS`]; after `max_retries` the message dead-letters and the
/// dead letter consumer parks it as an `unparsed:` fault letter (its body
/// carries no BEEF and no `r2`), counted against the ceiling's 2,000 rows.
/// The job is not lost: its row and bytes stay, the cron hands it back while
/// it is open, and a re-drive of the letter takes the `ef_job` branch again.
pub async fn run_job(
    env: &Env,
    ctx: &Context,
    engine: &overlay_engine::engine::Engine,
    reference: &str,
) {
    let Ok(db) = env.d1("OVERLAY_DB") else {
        worker::console_log!("NL-6c: OVERLAY_DB unavailable; job {reference} waits for the cron");
        return;
    };
    let job = match read_job(&db, reference).await {
        Ok(Some(job)) => job,
        Ok(None) => {
            worker::console_log!("NL-6c: job {reference} is gone (swept); nothing to run");
            return;
        }
        Err(e) => {
            worker::console_log!(
                "NL-6c: job {reference} could not be read ({e}); the cron hands it back"
            );
            return;
        }
    };
    let now = now_ms();
    match job.state.as_str() {
        "done" | "failed" => return,
        // a live run of another consumer (a redelivery): leave it to finish
        "running" if now - (job.updated_at as i64) < JOB_RESUME_AFTER_MS => return,
        _ => {}
    }
    if job.attempts as i64 >= JOB_ATTEMPTS {
        let last = job.answer.clone().unwrap_or_default();
        worker::console_log!("NL-6c: job {reference} FAILED after {JOB_ATTEMPTS} runs");
        settle_job(
            &db,
            reference,
            "failed",
            job.status.map_or(503, |s| s as u16),
            &format!("{{\"status\":\"error\",\"message\":\"the deferred work did not finish in {JOB_ATTEMPTS} runs\",\"last\":{}}}",
                serde_json::to_string(&last).unwrap_or_else(|_| "\"\"".into())),
        )
        .await;
        release(env, &db, reference, &job.at_rest).await;
        return;
    }
    if let Err(e) = Query::new(JOB_TAKE_SQL)
        .bind(reference)
        .bind(now)
        .execute(&db)
        .await
    {
        worker::console_log!(
            "NL-6c: job {reference} could not be taken ({e}); the cron hands it back"
        );
        return;
    }
    let stream = match load(env, &db, &job).await {
        Ok(b) => b,
        Err(e) => {
            // the bytes are gone or torn: nothing a later run could read
            worker::console_log!(
                "NL-6c: job {reference} FAILED: its bytes at rest cannot be read ({e})"
            );
            settle_job(
                &db,
                reference,
                "failed",
                500,
                &serde_json::json!({"status": "error", "message": format!("the bytes at rest cannot be read: {e}")})
                    .to_string(),
            )
            .await;
            release(env, &db, reference, &job.at_rest).await;
            return;
        }
    };
    let topics: Vec<String> = serde_json::from_str(&job.topics).unwrap_or_default();
    let split = (job.beef_len as usize).min(stream.len());
    let off_chain = (job.has_off_chain != 0.0).then(|| stream[split..].to_vec());
    let mut beef = stream;
    beef.truncate(split);
    if job_reference(&job.topics, &beef, off_chain.as_deref()) != reference {
        worker::console_log!("NL-6c: job {reference} FAILED: the bytes at rest are not the job's");
        settle_job(
            &db,
            reference,
            "failed",
            500,
            "{\"status\":\"error\",\"message\":\"the bytes at rest are not the submission's (sha256)\"}",
        )
        .await;
        release(env, &db, reference, &job.at_rest).await;
        return;
    }
    let parts = crate::routes::SubmitParts {
        topics,
        beef,
        off_chain_values: off_chain,
        mode_header: Some(job.submit_mode.clone()),
        operator_authed: false,
    };
    let hosting_url = env.var("HOSTING_URL").ok().map(|v| v.to_string());
    let arcade_url = env.var("ARCADE_URL").ok().map(|v| v.to_string());
    let taal_api_key = env.secret("TAAL_API_KEY").ok().map(|s| s.to_string());
    let from_leg = job.legs_from.max(0.0) as usize;
    let (status, text) = match crate::routes::submit_resumed(
        engine,
        parts,
        hosting_url.as_deref(),
        arcade_url,
        taal_api_key,
        ctx,
        env,
        from_leg,
    )
    .await
    {
        Ok(mut resp) => {
            let status = resp.status_code();
            (status, resp.text().await.unwrap_or_default())
        }
        Err(e) => (
            500,
            serde_json::json!({"status": "error", "message": format!("the arm faulted: {e}")})
                .to_string(),
        ),
    };
    // NL-6d: the corroboration's walk advanced and has legs left: the cursor
    // moves forward, the run is no failed attempt, and the job goes straight
    // back to the queue (the cron hands it back if that send faults).
    if let Some(reached) = resume_point(status, &text) {
        if reached > from_leg {
            worker::console_log!(
                "NL-6d: job {reference} read the corroboration's legs {from_leg} to {reached}; the next run resumes at {reached}"
            );
            if let Err(e) = Query::new(JOB_ADVANCE_SQL)
                .bind(reference)
                .bind(reached as i64)
                .bind(status as i64)
                .bind(text.as_str())
                .bind(now_ms())
                .execute(&db)
                .await
            {
                worker::console_log!(
                    "NL-6d: job {reference} could not record its cursor {reached} ({e}); the cron hands it back"
                );
                return;
            }
            if let Err(e) = send_job(env, reference).await {
                worker::console_log!(
                    "NL-6d: job {reference} is at leg {reached} but not queued ({e}); the cron hands it back"
                );
            }
            return;
        }
    }
    // a paused answer that did not advance is no verdict: the job is queued again
    let settled = if resume_point(status, &text).is_some() {
        Settle::Again
    } else {
        settle(status)
    };
    match settled {
        Settle::Done => {
            worker::console_log!(
                "NL-6c: job {reference} done ({status}) for {}",
                job.subject_txid
            );
            settle_job(&db, reference, "done", status, &text).await;
            release(env, &db, reference, &job.at_rest).await;
        }
        Settle::Again => {
            worker::console_log!(
                "NL-6c: job {reference} answered {status}; queued again (run {} of {JOB_ATTEMPTS}), the cron hands it back",
                job.attempts as i64 + 1
            );
            settle_job(&db, reference, "queued", status, &text).await;
        }
    }
}

/// The cron's pass: quiet open jobs back to the queue (each touched, so the
/// next pass does not send it twice); settled jobs past [`JOB_KEEP_MS`] swept
/// with their bytes.
pub async fn redrive(env: &Env, db: &D1Database) {
    #[derive(Deserialize)]
    struct Quiet {
        reference: String,
    }
    #[derive(Deserialize)]
    struct Expired {
        reference: String,
    }
    let now = now_ms();
    match Query::new(JOBS_QUIET_SQL)
        .bind(now - JOB_RESUME_AFTER_MS)
        .fetch_all::<Quiet>(db)
        .await
    {
        Ok(rows) => {
            for r in rows {
                match send_job(env, &r.reference).await {
                    Ok(()) => {
                        let _ = Query::new(JOB_TOUCH_SQL)
                            .bind(r.reference.as_str())
                            .bind(now)
                            .execute(db)
                            .await;
                        worker::console_log!("Scheduled: NL-6c deferred job {} handed to the queue again", r.reference);
                    }
                    Err(e) => worker::console_log!(
                        "Scheduled: NL-6c deferred job {} not handed back ({e}); the next pass tries",
                        r.reference
                    ),
                }
            }
        }
        Err(e) => worker::console_log!("Scheduled: NL-6c deferred jobs unreadable: {e}"),
    }
    match Query::new(JOBS_EXPIRED_SQL)
        .bind(now - JOB_KEEP_MS)
        .fetch_all::<Expired>(db)
        .await
    {
        Ok(rows) => {
            #[derive(Deserialize)]
            struct Deleted {
                at_rest: String,
            }
            for r in rows {
                // N3: the row first, conditionally; the bytes only for a row that came back
                match Query::new(JOB_DELETE_SQL)
                    .bind(r.reference.as_str())
                    .bind(now - JOB_KEEP_MS)
                    .fetch_optional::<Deleted>(db)
                    .await
                {
                    Ok(Some(d)) => release(env, db, &r.reference, &d.at_rest).await,
                    Ok(None) => worker::console_log!(
                        "Scheduled: NL-6c job {} was re-presented after the sweep's read; kept with its bytes",
                        r.reference
                    ),
                    Err(e) => worker::console_log!(
                        "Scheduled: NL-6c settled job {} not swept ({e}); the next pass tries",
                        r.reference
                    ),
                }
            }
        }
        Err(e) => worker::console_log!("Scheduled: NL-6c settled jobs unreadable: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ef::EfTx;

    fn leg(txid: &str, n: usize) -> EfTx {
        EfTx {
            txid: txid.to_string(),
            ef: vec![0u8; n],
        }
    }

    #[test]
    fn the_work_fits_the_request_up_to_both_budgets_and_is_deferred_past_either() {
        let at = EfWork::of(&[leg("subj", IN_REQUEST_SUBJECT_EF_BYTES)], "subj");
        assert!(at.fits_request(), "the subject budget itself fits");
        let over = EfWork::of(&[leg("subj", IN_REQUEST_SUBJECT_EF_BYTES + 1)], "subj");
        assert!(!over.fits_request());
        assert_eq!(over.subject_ef_bytes, IN_REQUEST_SUBJECT_EF_BYTES + 1);
        // the batch: a small subject over a large ancestry
        let batch = EfWork::of(
            &[leg("parent", IN_REQUEST_BATCH_EF_BYTES), leg("subj", 1)],
            "subj",
        );
        assert_eq!(batch.batch_ef_bytes, IN_REQUEST_BATCH_EF_BYTES + 1);
        assert!(!batch.fits_request());
        // the mined claim: no leg, no work
        let mined = EfWork::of(&[], "subj");
        assert_eq!(
            mined,
            EfWork {
                subject_ef_bytes: 0,
                batch_ef_bytes: 0,
                legs: 0,
            }
        );
        assert!(mined.fits_request());
        // a missing subject is 0 bytes, the batch still counts
        let missing = EfWork::of(&[leg("other", 10)], "subj");
        assert_eq!(missing.subject_ef_bytes, 0);
        assert_eq!(missing.batch_ef_bytes, 10);
    }

    #[test]
    fn the_reference_names_the_submission_and_nothing_else() {
        let a = job_reference("[\"tm_a\"]", b"beef", None);
        assert_eq!(a.len(), 64);
        assert!(a
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
        assert_eq!(
            a,
            job_reference("[\"tm_a\"]", b"beef", None),
            "the same submission, the same job"
        );
        assert_ne!(
            a,
            job_reference("[\"tm_b\"]", b"beef", None),
            "the topics name it"
        );
        assert_ne!(
            a,
            job_reference("[\"tm_a\"]", b"beeg", None),
            "the bytes name it"
        );
        assert_ne!(
            a,
            job_reference("[\"tm_a\"]", b"beef", Some(b"")),
            "an empty off-chain part is a part"
        );
        // the framing: moving a byte across the boundary is another job
        assert_ne!(
            job_reference("[\"tm_a\"]", b"beefx", None),
            job_reference("[\"tm_a\"]", b"beef", Some(b"x"))
        );
        assert_eq!(
            poll_reference(&format!("{POLL_PREFIX}{a}")),
            Some(a.as_str())
        );
        assert_eq!(
            poll_reference(&format!("{POLL_PREFIX}{}", a.to_uppercase())),
            None
        );
        assert_eq!(poll_reference(POLL_PREFIX), None);
        assert_eq!(poll_reference(&format!("{POLL_PREFIX}{a}/x")), None);
    }

    #[test]
    fn the_chunks_cover_the_bytes_under_the_row_bound() {
        assert_eq!(chunk_spans(0, 10), vec![(0, 0)]);
        assert_eq!(chunk_spans(10, 10), vec![(0, 10)]);
        assert_eq!(chunk_spans(11, 10), vec![(0, 10), (10, 11)]);
        let len = 2_200_282;
        let spans = chunk_spans(len, AT_REST_CHUNK_BYTES);
        assert_eq!(spans.len(), 3);
        assert_eq!(spans.first().unwrap().0, 0);
        assert_eq!(spans.last().unwrap().1, len);
        assert!(spans.windows(2).all(|w| w[0].1 == w[1].0), "contiguous");
        assert!(spans.iter().all(|(s, e)| e - s <= AT_REST_CHUNK_BYTES));
        const { assert!(AT_REST_CHUNK_BYTES < 2_000_000, "D1's row bound") };
    }

    #[test]
    fn only_the_arms_retryable_answers_queue_the_job_again() {
        for s in [200u16, 202, 400, 401, 404, 422] {
            assert_eq!(settle(s), Settle::Done, "{s}");
        }
        for s in [429u16, 500, 502, 503] {
            assert_eq!(settle(s), Settle::Again, "{s}");
        }
    }

    #[test]
    fn an_open_job_is_named_again_and_a_quiet_or_settled_one_is_written_again() {
        let now = 1_000_000_000;
        assert!(job_open("queued", now, now));
        assert!(job_open("running", now - JOB_RESUME_AFTER_MS + 1, now));
        assert!(
            !job_open("running", now - JOB_RESUME_AFTER_MS, now),
            "quiet: the cron's"
        );
        assert!(!job_open("done", now, now));
        assert!(!job_open("failed", now, now));
    }

    #[test]
    fn the_answer_is_accepted_with_a_reference_it_polls_and_names_no_cap() {
        let work = EfWork {
            subject_ef_bytes: 300_087,
            batch_ef_bytes: 300_087,
            legs: 1_000,
        };
        let r = "ab".repeat(32);
        let body = deferral_body(&r, "subj", work, 32);
        assert_eq!(body["legsFrom"], 32);
        assert_eq!(body["work"]["legs"], 1_000);
        assert_eq!(
            body["budget"]["corroborationLegs"],
            IN_REQUEST_CORROBORATION_LEGS
        );
        assert_eq!(body["accepted"], true);
        assert_eq!(body["deferred"], true);
        assert_eq!(body["reference"], r);
        assert_eq!(body["poll"], format!("/submit-deferred/{r}"));
        assert_eq!(body["subjectTxid"], "subj");
        assert_eq!(body["work"]["subjectEfBytes"], 300_087);
        let text = body.to_string();
        for word in ["too large", "cap", "retry via fallback", "429", "413"] {
            assert!(
                !text.contains(word),
                "the answer carries no refusal word: {word}"
            );
        }
    }

    #[test]
    fn the_poll_serves_the_arms_answer_once_a_run_has_ended() {
        let mut job = Job {
            reference: "ab".repeat(32),
            subject_txid: "subj".into(),
            topics: "[\"tm_collected\"]".into(),
            submit_mode: "broadcast-gated".into(),
            has_off_chain: 0.0,
            beef_len: 10.0,
            bytes: 10.0,
            at_rest: "d1".into(),
            chunks: 1.0,
            subject_ef_bytes: 300_087.0,
            batch_ef_bytes: 300_087.0,
            state: "queued".into(),
            attempts: 0.0,
            status: None,
            answer: None,
            created_at: 1.0,
            updated_at: 1.0,
            legs_from: 288.0,
        };
        assert_eq!(job.poll_body()["legsFrom"], 288);
        assert!(job.poll_body()["answer"].is_null());
        job.state = "done".into();
        job.status = Some(200.0);
        job.answer = Some("{\"tm_collected\":{\"outputsToAdmit\":[]}}".into());
        let b = job.poll_body();
        assert_eq!(b["state"], "done");
        assert_eq!(b["answer"]["status"], 200);
        assert_eq!(
            b["answer"]["body"]["tm_collected"]["outputsToAdmit"],
            serde_json::json!([])
        );
        job.answer = Some("not json".into());
        assert_eq!(job.poll_body()["answer"]["body"], "not json");
    }

    #[test]
    fn a_run_carries_the_corroborations_cursor_and_a_paused_answer_names_the_next() {
        // NL-6d: the request's window starts at the first ancestor; a run's starts at the job's cursor.
        let r = WorkBudget::InRequest.legs();
        assert_eq!((r.from, r.budget), (0, IN_REQUEST_CORROBORATION_LEGS));
        let run = WorkBudget::Resumed { from_leg: 288 }.legs();
        assert_eq!((run.from, run.budget), (288, RUN_CORROBORATION_LEGS));
        assert!(!WorkBudget::InRequest.resumed_mid_walk());
        assert!(!WorkBudget::Resumed { from_leg: 0 }.resumed_mid_walk());
        assert!(WorkBudget::Resumed { from_leg: 1 }.resumed_mid_walk());
        // the paused answer round-trips its cursor, and only a 202 carries one
        let work = EfWork {
            subject_ef_bytes: 147,
            batch_ef_bytes: 60_000,
            legs: 1_000,
        };
        let body = paused_body(544, work).to_string();
        assert_eq!(resume_point(202, &body), Some(544));
        assert_eq!(resume_point(200, &body), None);
        assert_eq!(resume_point(202, "{\"status\":\"accepted\"}"), None);
        assert_eq!(resume_point(202, "not json"), None);
        for word in ["cap", "too large", "429", "413", "502"] {
            assert!(
                !body.contains(word),
                "the paused answer carries no refusal word: {word}"
            );
        }
    }

    /// NL-6d on real SQLite: a run that advanced moves the cursor forward and clears its attempts; a stale cursor
    /// never moves it back; a re-presented job starts from the cursor its request reached.
    #[test]
    fn the_cursor_moves_forward_only_and_an_advancing_run_is_no_failed_attempt() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "{JOBS_CREATE}; {JOBS_INDEX}; {JOBS_ADD_LEGS_FROM};"
        ))
        .unwrap();
        conn.execute(
            JOB_UPSERT_SQL,
            rusqlite::params![
                "r1",
                "subj",
                "[]",
                "broadcast-gated",
                0,
                10,
                10,
                "d1",
                1,
                147,
                60_000,
                1,
                32
            ],
        )
        .unwrap();
        let read = || -> (String, i64, i64) {
            conn.query_row(
                "SELECT state, attempts, legs_from FROM ef_deferred_jobs WHERE reference = 'r1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap()
        };
        assert_eq!(read(), ("queued".into(), 0, 32), "the request's cursor");
        conn.execute(JOB_TAKE_SQL, rusqlite::params!["r1", 2])
            .unwrap();
        assert_eq!(read(), ("running".into(), 1, 32));
        conn.execute(JOB_ADVANCE_SQL, rusqlite::params!["r1", 288, 202, "{}", 3])
            .unwrap();
        assert_eq!(
            read(),
            ("queued".into(), 0, 288),
            "advanced: queued, no attempt spent"
        );
        conn.execute(JOB_ADVANCE_SQL, rusqlite::params!["r1", 100, 202, "{}", 4])
            .unwrap();
        assert_eq!(read(), ("queued".into(), 0, 288), "never back");
        let got: Job = {
            let mut st = conn.prepare(JOB_READ_SQL).unwrap();
            st.query_row(rusqlite::params!["r1"], |r| {
                Ok(Job {
                    reference: r.get(0)?,
                    subject_txid: r.get(1)?,
                    topics: r.get(2)?,
                    submit_mode: r.get(3)?,
                    has_off_chain: r.get::<_, i64>(4)? as f64,
                    beef_len: r.get::<_, i64>(5)? as f64,
                    bytes: r.get::<_, i64>(6)? as f64,
                    at_rest: r.get(7)?,
                    chunks: r.get::<_, i64>(8)? as f64,
                    subject_ef_bytes: r.get::<_, i64>(9)? as f64,
                    batch_ef_bytes: r.get::<_, i64>(10)? as f64,
                    state: r.get(11)?,
                    attempts: r.get::<_, i64>(12)? as f64,
                    status: r.get::<_, Option<i64>>(13)?.map(|v| v as f64),
                    answer: r.get(14)?,
                    created_at: r.get::<_, i64>(15)? as f64,
                    updated_at: r.get::<_, i64>(16)? as f64,
                    legs_from: r.get::<_, i64>(17)? as f64,
                })
            })
            .unwrap()
        };
        assert_eq!(got.legs_from, 288.0, "the read names the cursor");
    }

    /// The statements run on real SQLite: the tables, the upsert's
    /// re-presentation, the take, the settle and the chunks' hex read.
    #[test]
    fn the_statements_run_on_sqlite() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "{JOBS_CREATE}; {JOBS_INDEX}; {CHUNKS_CREATE}; {JOBS_ADD_LEGS_FROM};"
        ))
        .unwrap();
        let up = |now: i64| {
            conn.execute(
                JOB_UPSERT_SQL,
                rusqlite::params![
                    "r1",
                    "subj",
                    "[]",
                    "broadcast-gated",
                    0,
                    10,
                    10,
                    "d1",
                    1,
                    300_087,
                    300_087,
                    now,
                    0
                ],
            )
            .unwrap();
        };
        up(1);
        conn.execute(JOB_TAKE_SQL, rusqlite::params!["r1", 2])
            .unwrap();
        conn.execute(
            JOB_SETTLE_SQL,
            rusqlite::params!["r1", "done", 200, "{}", 3],
        )
        .unwrap();
        let (state, attempts, status): (String, i64, i64) = conn
            .query_row(
                "SELECT state, attempts, status FROM ef_deferred_jobs WHERE reference = 'r1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!((state.as_str(), attempts, status), ("done", 1, 200));
        up(4);
        let (state, attempts, status): (String, i64, Option<i64>) = conn
            .query_row(
                "SELECT state, attempts, status FROM ef_deferred_jobs WHERE reference = 'r1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            (state.as_str(), attempts, status),
            ("queued", 0, None),
            "re-presented: a fresh job"
        );
        conn.execute(
            CHUNK_INSERT_SQL,
            rusqlite::params!["r1", 0, vec![0xbeu8, 0xef]],
        )
        .unwrap();
        let hex: String = conn
            .query_row(CHUNK_READ_SQL, rusqlite::params!["r1", 0], |r| r.get(0))
            .unwrap();
        assert_eq!(hex::decode(hex).unwrap(), vec![0xbe, 0xef]);
        let quiet: Vec<String> = conn
            .prepare(JOBS_QUIET_SQL)
            .unwrap()
            .query_map(rusqlite::params![5], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(quiet, vec!["r1".to_string()]);
        conn.execute(CHUNKS_DELETE_SQL, rusqlite::params!["r1"])
            .unwrap();
        let swept: Option<String> = conn
            .query_row(JOB_DELETE_SQL, rusqlite::params!["r1", 10], |r| r.get(0))
            .ok();
        assert_eq!(swept, None, "a queued job is never swept");
    }

    /// N3 (LOW's E585 land2 lens, bsv-low's `docs/audit/E585-land2-lens-2026-10-10.md`): the sweep selects the settled
    /// jobs, then deletes them one by one. A re-presentation of the same submission landing between the two
    /// re-puts the bytes and upserts the row to `queued`; the delete, bound as the sweep binds it, must leave
    /// that row (and so its bytes) alone and hand back no `at_rest` to release.
    #[test]
    fn a_re_presentation_between_the_sweeps_select_and_delete_survives_the_sweep() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "{JOBS_CREATE}; {JOBS_INDEX}; {CHUNKS_CREATE}; {JOBS_ADD_LEGS_FROM};"
        ))
        .unwrap();
        let up = |now: i64| {
            conn.execute(
                JOB_UPSERT_SQL,
                rusqlite::params![
                    "r1",
                    "subj",
                    "[]",
                    "broadcast-gated",
                    0,
                    10,
                    10,
                    "r2",
                    1,
                    300_087,
                    300_087,
                    now,
                    0
                ],
            )
            .unwrap();
        };
        up(1);
        conn.execute(JOB_TAKE_SQL, rusqlite::params!["r1", 2])
            .unwrap();
        conn.execute(
            JOB_SETTLE_SQL,
            rusqlite::params!["r1", "done", 200, "{}", 3],
        )
        .unwrap();
        let cutoff = 100i64;
        // the sweep's select: r1 is settled and past the cutoff
        let expired: Vec<String> = conn
            .prepare(JOBS_EXPIRED_SQL)
            .unwrap()
            .query_map(rusqlite::params![cutoff], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(expired, vec!["r1".to_string()]);
        // the re-presentation lands between the select and the delete
        up(200);
        // the sweep's delete, bound as the sweep binds it
        let mut del = conn.prepare(JOB_DELETE_SQL).unwrap();
        let binds: [&dyn rusqlite::ToSql; 2] = [&"r1", &cutoff];
        let mut rows = del.query(&binds[..del.parameter_count()]).unwrap();
        let mut released = Vec::new();
        while let Some(r) = rows.next().unwrap() {
            released.push(r.get::<_, String>(0).unwrap());
        }
        drop(rows);
        drop(del);
        let state: Option<String> = conn
            .query_row(
                "SELECT state FROM ef_deferred_jobs WHERE reference = 'r1'",
                [],
                |r| r.get(0),
            )
            .ok();
        assert_eq!(
            state.as_deref(),
            Some("queued"),
            "the re-presented job survives the sweep (its poll answers, its message finds a row)"
        );
        assert!(
            released.is_empty(),
            "no bytes are released for a row the sweep did not delete"
        );
        // and a job still settled past the cutoff is deleted, its at_rest handed back
        conn.execute(JOB_TAKE_SQL, rusqlite::params!["r1", 201])
            .unwrap();
        conn.execute(
            JOB_SETTLE_SQL,
            rusqlite::params!["r1", "failed", 503, "{}", 202],
        )
        .unwrap();
        let mut del = conn.prepare(JOB_DELETE_SQL).unwrap();
        let later = 300i64;
        let binds: [&dyn rusqlite::ToSql; 2] = [&"r1", &later];
        let mut rows = del.query(&binds[..del.parameter_count()]).unwrap();
        let mut released = Vec::new();
        while let Some(r) = rows.next().unwrap() {
            released.push(r.get::<_, String>(0).unwrap());
        }
        assert_eq!(released, vec!["r2".to_string()]);
    }

    /// L1 (a) (LOW's E585 land2 lens): the R2 put of `ef-deferred/<reference>` hands R2 the sha256 of the bytes it
    /// stores, through the put's checksum option, as door 3's put does (`queue::put_beef`), so a corrupted upload is
    /// refused at the put, not found at the run; and the writer's `touched` stamp, which the pass over the prefix
    /// ages an object by. (The reference is the sha256 of the framed submission, topics included, so it is not the
    /// object's digest: the digest is taken of the stored stream.) RED on `54dbb16`: the put has neither.
    #[test]
    fn l1a_the_deferred_put_hands_r2_the_digest_of_its_bytes() {
        let code: String = include_str!("ef_deferred.rs")
            .lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<String>()
            .split_whitespace()
            .collect();
        let start = code.find("asyncfnput_at_rest(").unwrap();
        let f = &code[start..start + code[start..].find("asyncfnsend_job(").unwrap()];
        assert!(
            f.contains("letstored=span(0,total);letdigest=bsv_rs::primitives::hash::sha256(&stored).to_vec();"),
            "the digest of the stored stream"
        );
        assert!(
            f.contains(".put(format!(\"{R2_PREFIX}{reference}\"),stored).sha256(digest).custom_metadata(crate::queue::touched_meta(now)).execute()"),
            "the put carries the digest and the touched stamp"
        );
    }
}
