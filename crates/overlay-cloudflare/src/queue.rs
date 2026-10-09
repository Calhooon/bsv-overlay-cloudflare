//! Queue message types for the onSteakReady pattern — and, since S2
//! (ARCHITECTURE v2 principle 1, bsv-low 2026-08-29), the QUEUE-DURABLE
//! ADMISSION replay.
//!
//! Mutations are enqueued as `MutationMessage` and processed by the
//! `#[event(queue)]` consumer. The BEEF + topics are serialized as JSON: the
//! BEEF base64-encoded INLINE while the message fits [`QUEUE_MESSAGE_ROOM`]
//! (under CF Queue's 128 KB message limit), and past it written to R2 FIRST
//! (`BEEF_BLOBS`) with the message carrying its key ([`BeefRef`]; bsv-low
//! #585, door 3: a queued submission has no size cap of the queue's making).
//!
//! ## S2 — an ack is durable
//!
//! `/submit` writes through synchronously (so a `/lookup` on this instance
//! sees the admission immediately). Until S2 every Phase-3 write failure
//! was swallowed inside `engine.submit` and the route acked 200 — under
//! the 2026-08-26 D1-overload storm the overlay acked admissions whose
//! rows never existed (the phantom class). Now `engine.submit_with_report`
//! names every fault, and the route ENQUEUES the same bytes for an
//! idempotent replay before it acks; if the queue cannot take them the
//! route refuses (502, retryable) instead of acking a write it does not
//! hold. The consumer replays through the same engine (every backend write
//! is `INSERT OR IGNORE`/`OR REPLACE`/`UPDATE`; a faulted topic is never
//! recorded as applied, so the replay is re-validated, not deduplicated
//! away), retries with the platform's backoff, and dead-letters after
//! `max_retries` — a dropped write is REDELIVERED, not vanished.
//!
//! ## The R2 object (bsv-low #585, door 3)
//!
//! * THE KEY: [`r2_key`], `mutations/<sha256 of the BEEF>/<32 hex of
//!   sha256(sorted topics, "\n", mode)>`: one object per (bytes, topics,
//!   mode), so an ack of one message never deletes the bytes of another
//!   topic set's.
//! * THE WRITE comes before the enqueue; a write that faults (or a missing
//!   binding) is the producer's 502, as a failed send is. A send that faults
//!   after the write leaves the object (the client's re-presentation writes
//!   the same key again): never deleted here, a twin's message may name it.
//! * THE READ: the consumer fetches the object, checks its length and sha256
//!   against the message's (and the key's), then replays it exactly as an
//!   inline body. A MISSING object, a read fault and a mismatch are each the
//!   replay's FAULT (class `fault`, never "not now"): handed back,
//!   dead-lettered, parked with the key.
//! * THE DELETION RULE: an object is deleted on the consumer's ACK (at the
//!   end of its batch), when its dead letter is LOST (the DLQ's last
//!   delivery, not parked) or dropped as the lighter copy of a parked key,
//!   and on the operator's discard. Never by a bucket expiry rule.
//!   `dead_letters.rs` holds the last three.

use overlay_engine::beef_limits;
use overlay_engine::types::SubmitMode;
use serde::{Deserialize, Serialize};

/// The R2 bucket binding of every overlay wrangler config (bsv-low #585, door 3).
pub const BEEF_BLOBS_BINDING: &str = "BEEF_BLOBS";

/// The room of an INLINE message: the JSON of its WORST form (re-driven: the
/// reason `redrive` and the letter's key added, [`inline_worst_len`]) in bytes.
/// CF Queues refuse a message past 128 KB; 124,000 leaves 4,000 under the
/// decimal reading of that. Before #585 the cap was 90,000 raw BEEF bytes,
/// 120,000 of base64 plus an envelope of a few hundred: every body that rode
/// the queue then is inline now, byte for byte.
pub const QUEUE_MESSAGE_ROOM: usize = 124_000;
const _: () = assert!(
    QUEUE_MESSAGE_ROOM < 128_000,
    "Cloudflare Queues limits: message size 128 KB"
);
/// The var that LOWERS the room (a decimal integer, clamped to
/// [`QUEUE_MESSAGE_ROOM_MIN`] ..= [`QUEUE_MESSAGE_ROOM`]; unset, empty or not a
/// number is the default). Lower sends more bodies through R2 and nothing
/// else; the route tier sets it to drive the R2 path with small bodies.
pub const QUEUE_MESSAGE_ROOM_VAR: &str = "MUTATION_QUEUE_INLINE_ROOM";
pub const QUEUE_MESSAGE_ROOM_MIN: usize = 1024;

/// PURE: the inline room in force ([`QUEUE_MESSAGE_ROOM_VAR`]).
#[must_use]
pub fn inline_room(var: Option<&str>) -> usize {
    var.and_then(|v| v.trim().parse::<usize>().ok())
        .map_or(QUEUE_MESSAGE_ROOM, |r| {
            r.clamp(QUEUE_MESSAGE_ROOM_MIN, QUEUE_MESSAGE_ROOM)
        })
}

/// Decode a queue/letter's BEEF and read it through the streaming door
/// before returning it to any subject reader or admission path: invalid
/// bytes are refused with their offset and kind, and a valid BEEF is read
/// whatever its length (NL-6; the CONSUMER never bounds what it was handed).
/// A replay failure keeps the consumer's existing retry/dead-letter
/// lifecycle. `_door` names where the reader stands; nothing of it is read.
///
/// The bytes arrive whole, base64 in one message: the platform's 128 KB
/// message is the bound today, held by the PRODUCER ([`QUEUE_BEEF_SIZE_LIMIT`],
/// bsv-low #585 door 3). When the message carries a reference instead, this
/// is the site that reads the object's body through `beef_limits::fold_beef`
/// from the R2 binding that door adds (buckets `low-overlay-beefs-beta` and
/// `low-overlay-beefs`); the Worker has no R2 binding at this commit.
pub(crate) fn decode_beef_b64(
    encoded: &str,
    _door: &overlay_engine::beef_limits::BeefLimits,
) -> Result<Vec<u8>, String> {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    let bytes = STANDARD.decode(encoded).map_err(|e| e.to_string())?;
    beef_limits::read_beef(&bytes).map_err(|refusal| refusal.to_string())?;
    Ok(bytes)
}

pub(crate) fn decode_replay_beef(encoded: &str) -> Result<Vec<u8>, String> {
    decode_beef_b64(encoded, &beef_limits::QUEUE_BEEF_LIMITS)
}

/// A BEEF held in R2 in place of a message's inline body (bsv-low #585, door 3).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct BeefRef {
    /// The object's key ([`r2_key`]).
    #[serde(rename = "beefR2Key")]
    pub key: String,
    /// The sha256 of the BEEF bytes, hex: checked against the object read back.
    pub sha256: String,
    /// The BEEF's length.
    pub bytes: u64,
    /// The subject txid by the ONE rule (D5), as the producer derived it from the bytes: the LETTER's key in both
    /// consumers, so neither needs the object to name its row (a missing object's note lands on the letter's row,
    /// and the DLQ consumer parks without a read). `None`: the letter is keyed `unparsed:` and the sha256.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub txid: Option<String>,
}

/// PURE: the key of the object holding `sha256_hex`'s bytes for this topic set and wire mode.
#[must_use]
pub fn r2_key(sha256_hex: &str, topics: &[String], mode_wire: &str) -> String {
    let scope = format!("{}\n{mode_wire}", crate::dead_letters::topics_key(topics));
    let h = bsv_rs::primitives::hash::sha256(scope.as_bytes());
    format!("mutations/{sha256_hex}/{}", hex::encode(&h[..16]))
}

/// PURE: the object read back for `r` is the bytes the producer wrote: the length, the sha256, and the key naming
/// that same sha256 (a message whose key and hash disagree is refused, whatever the object holds).
pub fn check_blob(r: &BeefRef, bytes: &[u8]) -> Result<(), String> {
    if r.key.split('/').nth(1) != Some(r.sha256.as_str()) {
        return Err(format!(
            "the key {} does not name the message's sha256 {}",
            r.key, r.sha256
        ));
    }
    if bytes.len() as u64 != r.bytes {
        return Err(format!(
            "the object {} holds {} B, the message says {} B",
            r.key,
            bytes.len(),
            r.bytes
        ));
    }
    let got = hex::encode(bsv_rs::primitives::hash::sha256(bytes));
    if got != r.sha256 {
        return Err(format!(
            "the object {} hashes to {got}, the message says {}",
            r.key, r.sha256
        ));
    }
    Ok(())
}

/// The replay's bytes read from R2, validated under the consumer's own policy exactly as an inline body's are
/// after its base64 (`decode_replay_beef`): no cap of this path's own.
pub(crate) fn check_replay_blob(r: &BeefRef, bytes: &[u8]) -> Result<(), String> {
    check_blob(r, bytes)?;
    beef_limits::parse_beef(bytes, &beef_limits::QUEUE_BEEF_LIMITS).map_err(|e| e.to_string())?;
    Ok(())
}

/// A mutation message enqueued for reliable processing.
///
/// Sent by the /submit route after returning the Steak to the client.
/// Consumed by the queue handler to apply Phase 3 mutations.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MutationMessage {
    /// Base64-encoded BEEF bytes. Empty (and absent on the wire) when [`Self::r2`] holds them.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub beef_b64: String,
    /// bsv-low #585 (door 3): the BEEF in R2, for a body whose inline message would pass the room. Absent on every
    /// message that fits (its bytes on the wire are what they were).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r2: Option<BeefRef>,
    /// Topic names this transaction targets.
    pub topics: Vec<String>,
    /// Submit mode the ORIGINAL submit ran under: "current-tx",
    /// "historical-tx", or "historical-tx-no-spv". The consumer maps it
    /// through [`replay_submit_mode`] — a replay never re-broadcasts.
    pub mode: String,
    /// Why this message exists (diagnostics): `"phase3-fault"` for an S2
    /// replay. Serde-defaulted so a message from a pre-S2 producer still
    /// parses.
    #[serde(default)]
    pub reason: String,
    /// bsv-low #576: set on a message the operator re-drove from the parked dead letters (its letter's key and the
    /// re-drive's number), so a re-death parks the SAME row. Absent on every S2 replay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redrive: Option<crate::dead_letters::RedriveTag>,
    /// NL-6c: set on a DEFERRED EF JOB's message, the job's reference
    /// (`ef_deferred`); its bytes rest in D1 or R2 and the message carries
    /// none (`beef_b64` is empty). Absent on every replay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ef_job: Option<String>,
}

/// NL-6c: the message that hands a deferred EF job to the consumer: its
/// reference, never its bytes, so the queue's 128 KB message is no bound.
#[must_use]
pub fn ef_job_message(reference: &str) -> MutationMessage {
    MutationMessage {
        beef_b64: String::new(),
        r2: None,
        topics: Vec::new(),
        mode: String::new(),
        reason: EF_JOB_REASON.to_string(),
        redrive: None,
        ef_job: Some(reference.to_string()),
    }
}

/// The reason stamped on a deferred EF job's message.
pub const EF_JOB_REASON: &str = "ef-deferred";

/// The reason stamped on an S2 replay message.
pub const REPLAY_REASON_PHASE3_FAULT: &str = "phase3-fault";

/// Wire name of an engine submit mode (the inverse of the consumer's map).
#[must_use]
pub fn mode_wire(mode: SubmitMode) -> &'static str {
    match mode {
        SubmitMode::CurrentTx => "current-tx",
        SubmitMode::HistoricalTx => "historical-tx",
        SubmitMode::HistoricalTxNoSpv => "historical-tx-no-spv",
    }
}

/// The engine mode a REPLAY runs under.
///
/// Phase 2 (the engine's ARC broadcast + SHIP propagation) already ran at
/// the route for the original submit; a replay needs Phase 1 (validation,
/// dedup) + Phase 3 (the writes) only. So `current-tx` becomes
/// `historical-tx` — SPV kept, broadcast skipped. The two historical modes
/// are already broadcast-free and pass through. An unrecognised string is
/// treated as `historical-tx` (never a broadcast on a replay — the
/// pre-S2 consumer defaulted to `current-tx`, which would have re-pushed
/// bytes to ARC on every redelivery).
#[must_use]
pub fn replay_submit_mode(mode_wire: &str) -> SubmitMode {
    match mode_wire {
        "historical-tx-no-spv" => SubmitMode::HistoricalTxNoSpv,
        _ => SubmitMode::HistoricalTx,
    }
}

/// PURE: the length of the base64 of `n` bytes (padded, as `STANDARD` writes it).
#[must_use]
pub fn b64_len(n: usize) -> usize {
    n.div_ceil(3).saturating_mul(4)
}

/// PURE: the JSON length of the inline message of a `beef_len`-byte BEEF in its WORST form: as produced, or as the
/// lever re-drives it (`dead_letters::redrive_message`: the reason `redrive`, the letter's key, a 64-hex txid, and
/// its number at its longest), whichever is longer. No base64 is built to measure it.
#[must_use]
pub fn inline_worst_len(
    beef_len: usize,
    topics: &[String],
    mode: SubmitMode,
    reason: &str,
) -> usize {
    let envelope = |reason: &str, redrive: Option<crate::dead_letters::RedriveTag>| {
        let m = MutationMessage {
            beef_b64: "=".to_string(),
            r2: None,
            topics: topics.to_vec(),
            mode: mode_wire(mode).to_string(),
            reason: reason.to_string(),
            redrive,
            ef_job: None,
        };
        serde_json::to_string(&m).map_or(usize::MAX, |j| j.len() - 1)
    };
    let tag = crate::dead_letters::RedriveTag {
        txid: "0".repeat(64),
        topics: crate::dead_letters::topics_key(topics),
        n: u64::MAX,
    };
    envelope(reason, None)
        .max(envelope(crate::dead_letters::REASON_REDRIVE, Some(tag)))
        .saturating_add(b64_len(beef_len))
}

/// How a replay rides the queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Carriage {
    /// The message fits the room: the BEEF inline, as before #585.
    Inline(MutationMessage),
    /// The message would pass the room: the BEEF goes to R2 FIRST under the message's key ([`BeefRef`]).
    R2(MutationMessage),
}

impl Carriage {
    #[must_use]
    pub fn message(&self) -> &MutationMessage {
        match self {
            Self::Inline(m) | Self::R2(m) => m,
        }
    }
}

fn inline_message(
    beef: &[u8],
    topics: &[String],
    mode: SubmitMode,
    reason: &str,
) -> MutationMessage {
    use base64::{engine::general_purpose::STANDARD, Engine as B64Engine};
    MutationMessage {
        beef_b64: STANDARD.encode(beef),
        r2: None,
        topics: topics.to_vec(),
        mode: mode_wire(mode).to_string(),
        reason: reason.to_string(),
        redrive: None,
        ef_job: None,
    }
}

/// PURE: the replay message for an admission whose Phase-3 writes did not all land, and how it rides (bsv-low
/// #585, door 3). Inline while its worst form fits `room` (no change below it); past it, by key. `replay_limits`
/// is the CONSUMER's policy (`beef_limits::QUEUE_BEEF_LIMITS` at the door): a body the replay would refuse for its
/// size is refused HERE (`Err`, the route's 502), since an ack over a message whose every replay is a refusal is
/// an ack over a dropped write. That is the consumer's bound read by name, no cap of the producer's: it lifts
/// when that policy does.
pub fn plan_replay(
    beef: &[u8],
    topics: &[String],
    mode: SubmitMode,
    reason: &str,
    room: usize,
    replay_limits: &bsv_rs::transaction::BeefLimits,
) -> Result<Carriage, String> {
    if beef.len() > replay_limits.max_bytes {
        return Err(format!(
            "BEEF too large for the mutation queue's replay ({} B > {} B, the consumer's QUEUE_BEEF_LIMITS)",
            beef.len(),
            replay_limits.max_bytes
        ));
    }
    if inline_worst_len(beef.len(), topics, mode, reason) <= room {
        return Ok(Carriage::Inline(inline_message(beef, topics, mode, reason)));
    }
    let sha256 = hex::encode(bsv_rs::primitives::hash::sha256(beef));
    let txid = beef_limits::parse_beef(beef, replay_limits)
        .ok()
        .and_then(|mut named| crate::ef::subject_txid_of(&mut named))
        .map(|t| t.to_ascii_lowercase());
    Ok(Carriage::R2(MutationMessage {
        beef_b64: String::new(),
        r2: Some(BeefRef {
            key: r2_key(&sha256, topics, mode_wire(mode)),
            sha256,
            bytes: beef.len() as u64,
            txid,
        }),
        topics: topics.to_vec(),
        mode: mode_wire(mode).to_string(),
        reason: reason.to_string(),
        redrive: None,
        ef_job: None,
    }))
}

/// The ack decision for a submit whose Phase 3 reported `durable` (S2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationAck {
    /// Every write landed — the ordinary 200.
    Durable,
    /// Some write faulted but the replay is QUEUED — 200, the queue is the
    /// guarantee (`X-Overlay-Mutation: queued`).
    Queued,
    /// Some write faulted AND the replay could not be queued — the route
    /// must refuse (502, retryable) rather than ack a write it does not
    /// hold; the client ladder re-presents.
    Refused(String),
}

/// ONE derivation of the S2 ack (the #347 lesson: a decision computed twice
/// drifts). `enqueue` is `None` when no enqueue was attempted (the caller
/// attempts one exactly when `!durable`).
#[must_use]
pub fn mutation_ack(durable: bool, enqueue: Option<Result<(), String>>) -> MutationAck {
    if durable {
        return MutationAck::Durable;
    }
    match enqueue {
        Some(Ok(())) => MutationAck::Queued,
        Some(Err(e)) => MutationAck::Refused(e),
        None => MutationAck::Refused("no replay was queued".to_string()),
    }
}

/// Why a replay's bytes could not be read from R2. Each is the replay's FAULT (class `fault`), never "not now".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobFault {
    /// The bucket holds no object under the key.
    Missing(String),
    /// The binding, the read or the body faulted.
    Read(String),
    /// The object is not the bytes the message names ([`check_blob`]), or the replay's policy refuses them.
    Refused(String),
}

impl BlobFault {
    /// The fault text of the note and of the log line.
    #[must_use]
    pub fn says(&self) -> String {
        match self {
            Self::Missing(key) => {
                format!("the R2 object {key} is MISSING: the replay has no bytes")
            }
            Self::Read(e) => format!("the R2 read faulted ({e})"),
            Self::Refused(e) => format!("the R2 object was refused ({e})"),
        }
    }
}

/// The consumer's read of a keyed message's bytes: the object, checked ([`check_replay_blob`]).
pub async fn read_beef(env: &worker::Env, r: &BeefRef) -> Result<Vec<u8>, BlobFault> {
    let bucket = env
        .bucket(BEEF_BLOBS_BINDING)
        .map_err(|e| BlobFault::Read(format!("{BEEF_BLOBS_BINDING} binding unavailable: {e}")))?;
    let object = bucket
        .get(r.key.as_str())
        .execute()
        .await
        .map_err(|e| BlobFault::Read(e.to_string()))?
        .ok_or_else(|| BlobFault::Missing(r.key.clone()))?;
    let body = object
        .body()
        .ok_or_else(|| BlobFault::Read(format!("the object {} has no body", r.key)))?;
    let bytes = body
        .bytes()
        .await
        .map_err(|e| BlobFault::Read(e.to_string()))?;
    check_replay_blob(r, &bytes).map_err(BlobFault::Refused)?;
    Ok(bytes)
}

/// The producer's write, BEFORE the enqueue. R2 checks the sha256 it is given against what it stored.
async fn put_beef(env: &worker::Env, r: &BeefRef, beef: &[u8]) -> Result<(), String> {
    let bucket = env
        .bucket(BEEF_BLOBS_BINDING)
        .map_err(|e| format!("{BEEF_BLOBS_BINDING} binding unavailable: {e}"))?;
    let digest = bsv_rs::primitives::hash::sha256(beef).to_vec();
    match bucket
        .put(r.key.as_str(), beef.to_vec())
        .sha256(digest)
        .execute()
        .await
    {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(format!("the R2 write of {} answered no object", r.key)),
        Err(e) => Err(format!("the R2 write of {} failed: {e}", r.key)),
    }
}

/// THE DELETION RULE's one delete (see the module doc for when): each key once, fail-soft. A delete that faults
/// leaves an object nothing names (logged with its key, counted `beef_blobs_delete_faults_total`); the faults are
/// returned for a caller that answers them (the discard lever).
pub async fn delete_beefs(env: &worker::Env, keys: &[String], why: &str) -> Vec<(String, String)> {
    let mut keys: Vec<&String> = keys.iter().collect();
    keys.sort_unstable();
    keys.dedup();
    if keys.is_empty() {
        return Vec::new();
    }
    let db = env.d1("OVERLAY_DB").ok();
    let mut faults = Vec::new();
    let mut deleted = 0u64;
    match env.bucket(BEEF_BLOBS_BINDING) {
        Ok(bucket) => {
            for key in keys {
                match bucket.delete(key.as_str()).await {
                    Ok(()) => {
                        deleted += 1;
                        worker::console_log!("[beef-blobs] DELETED {key} ({why})");
                    }
                    Err(e) => faults.push((key.clone(), e.to_string())),
                }
            }
        }
        Err(e) => {
            let e = format!("{BEEF_BLOBS_BINDING} binding unavailable: {e}");
            faults.extend(keys.into_iter().map(|k| (k.clone(), e.clone())));
        }
    }
    for (key, e) in &faults {
        worker::console_log!("[beef-blobs] the delete of {key} ({why}) faulted ({e}): the object stays, nothing names it");
    }
    if let Some(db) = &db {
        if deleted > 0 {
            crate::ops::bump_counter(db, crate::ops::COUNTER_BEEF_BLOBS_DELETED, deleted).await;
        }
        if !faults.is_empty() {
            crate::ops::bump_counter(
                db,
                crate::ops::COUNTER_BEEF_BLOBS_DELETE_FAULTS,
                faults.len() as u64,
            )
            .await;
        }
    }
    faults
}

/// Enqueue the S2 replay for an undurable admission. `Err` names why the
/// bytes are NOT in the queue (a body the replay's policy refuses, a missing
/// binding, an R2 write or a send that faulted) so the route can refuse with
/// the reason. A body past the inline room is written to R2 BEFORE the send.
pub async fn enqueue_replay(
    env: &worker::Env,
    beef: &[u8],
    topics: &[String],
    mode: SubmitMode,
) -> Result<(), String> {
    let room = inline_room(
        env.var(QUEUE_MESSAGE_ROOM_VAR)
            .ok()
            .map(|v| v.to_string())
            .as_deref(),
    );
    let carriage = plan_replay(
        beef,
        topics,
        mode,
        REPLAY_REASON_PHASE3_FAULT,
        room,
        &beef_limits::QUEUE_BEEF_LIMITS,
    )?;
    if let Carriage::R2(msg) = &carriage {
        if let Some(r) = &msg.r2 {
            put_beef(env, r, beef).await?;
            if let Ok(db) = env.d1("OVERLAY_DB") {
                crate::ops::bump_counter(&db, crate::ops::COUNTER_BEEF_BLOBS_WRITTEN, 1).await;
            }
        }
    }
    let queue = env
        .queue("MUTATION_QUEUE")
        .map_err(|e| format!("MUTATION_QUEUE binding unavailable: {e}"))?;
    queue
        .send(carriage.message().clone())
        .await
        .map_err(|e| format!("mutation queue send failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_never_runs_under_current_tx() {
        // A replay must not re-run Phase 2 (ARC + SHIP): current-tx maps to
        // historical-tx (SPV kept, broadcast skipped); the historical modes
        // pass through; junk defaults to the broadcast-free mode.
        assert_eq!(replay_submit_mode("current-tx"), SubmitMode::HistoricalTx);
        assert_eq!(
            replay_submit_mode("historical-tx"),
            SubmitMode::HistoricalTx
        );
        assert_eq!(
            replay_submit_mode("historical-tx-no-spv"),
            SubmitMode::HistoricalTxNoSpv
        );
        assert_eq!(replay_submit_mode("garbage"), SubmitMode::HistoricalTx);
    }

    #[test]
    fn mode_wire_round_trips_through_the_replay_map() {
        for m in [SubmitMode::HistoricalTx, SubmitMode::HistoricalTxNoSpv] {
            assert_eq!(replay_submit_mode(mode_wire(m)), m);
        }
        // The one deliberate non-identity.
        assert_eq!(
            replay_submit_mode(mode_wire(SubmitMode::CurrentTx)),
            SubmitMode::HistoricalTx
        );
    }

    fn wide() -> bsv_rs::transaction::BeefLimits {
        beef_limits::SUBMIT_BEEF_LIMITS
    }

    fn pattern(n: usize) -> Vec<u8> {
        (0..n)
            .map(|i| (i.wrapping_mul(31).wrapping_add(7) & 0xff) as u8)
            .collect()
    }

    /// bsv-low #585 (door 3): a body under the room is INLINE and its message is byte for byte what the producer
    /// wrote before the door: the hand-written JSON of the old shape, and the frozen sha256 of the 90,000-byte case
    /// (the old cap's own maximum; computed outside this crate over that JSON).
    #[test]
    fn e585_d3_a_body_under_the_room_is_inline_and_byte_identical() {
        use base64::{engine::general_purpose::STANDARD, Engine as B64Engine};
        let topics = vec!["tm_pot".to_string(), "tm_lowfund".to_string()];
        for n in [10usize, 1_000, 90_000] {
            let beef = pattern(n);
            let plan = plan_replay(
                &beef,
                &topics,
                SubmitMode::HistoricalTxNoSpv,
                REPLAY_REASON_PHASE3_FAULT,
                QUEUE_MESSAGE_ROOM,
                &wide(),
            )
            .unwrap();
            let Carriage::Inline(msg) = &plan else {
                panic!("{n} B rides inline, got {plan:?}")
            };
            assert_eq!(STANDARD.decode(&msg.beef_b64).unwrap(), beef);
            let json = serde_json::to_string(msg).unwrap();
            assert_eq!(
                json,
                format!(
                    r#"{{"beef_b64":"{}","topics":["tm_pot","tm_lowfund"],"mode":"historical-tx-no-spv","reason":"phase3-fault"}}"#,
                    STANDARD.encode(&beef)
                ),
                "no field of the door's shows on an inline message"
            );
            if n == 90_000 {
                assert_eq!(json.len(), 120_102);
                assert_eq!(
                    hex::encode(bsv_rs::primitives::hash::sha256(json.as_bytes())),
                    "1def8c284a4ff776041edbb3a5e5f274efaec79e713ba05a1a2c19d1c1463c24"
                );
            }
        }
        // under the consumer's policy as it stands, too: everything the old 90,000-byte cap let through is inline
        let at_old_cap = pattern(90_000);
        if beef_limits::QUEUE_BEEF_LIMITS.max_bytes >= 90_000 {
            assert!(matches!(
                plan_replay(
                    &at_old_cap,
                    &topics,
                    SubmitMode::HistoricalTx,
                    "x",
                    QUEUE_MESSAGE_ROOM,
                    &beef_limits::QUEUE_BEEF_LIMITS
                ),
                Ok(Carriage::Inline(_))
            ));
        }
    }

    /// Door 3: the room is measured on the real envelope in its WORST form (the lever's re-driven message), with no
    /// base64 built; the first body past it rides by key; LOW's 13 topics at the old cap still fit.
    #[test]
    fn e585_d3_the_room_is_the_real_envelope() {
        let topics = vec!["tm_pot".to_string(), "tm_lowfund".to_string()];
        let mode = SubmitMode::HistoricalTx;
        for n in [1usize, 2, 3, 4, 1000, 90_000] {
            let produced = inline_message(&pattern(n), &topics, mode, REPLAY_REASON_PHASE3_FAULT);
            let row = crate::dead_letters::ParkedRow {
                txid: "f".repeat(64),
                topics: crate::dead_letters::topics_key(&topics),
                fault: None,
                redrives: 0.0,
                redriven_at: None,
            };
            let redriven = crate::dead_letters::redrive_message(
                &row,
                &serde_json::to_string(&produced).unwrap(),
                u64::MAX,
            )
            .unwrap();
            let real = serde_json::to_string(&produced)
                .unwrap()
                .len()
                .max(serde_json::to_string(&redriven).unwrap().len());
            assert_eq!(
                inline_worst_len(n, &topics, mode, REPLAY_REASON_PHASE3_FAULT),
                real,
                "{n} B"
            );
        }
        // the boundary, to the byte
        let fits = |n: usize| {
            inline_worst_len(n, &topics, mode, REPLAY_REASON_PHASE3_FAULT) <= QUEUE_MESSAGE_ROOM
        };
        let last = (0..200_000usize).rev().find(|n| fits(*n)).unwrap();
        assert!(
            last > 90_000 && last < 96_000,
            "the room in raw bytes at two topics: {last}"
        );
        assert!(matches!(
            plan_replay(
                &pattern(last),
                &topics,
                mode,
                REPLAY_REASON_PHASE3_FAULT,
                QUEUE_MESSAGE_ROOM,
                &wide()
            ),
            Ok(Carriage::Inline(_))
        ));
        assert!(matches!(
            plan_replay(
                &pattern(last + 1),
                &topics,
                mode,
                REPLAY_REASON_PHASE3_FAULT,
                QUEUE_MESSAGE_ROOM,
                &wide()
            ),
            Ok(Carriage::R2(_))
        ));
        let low: Vec<String> = (0..13).map(|i| format!("tm_low_topic_{i:02}")).collect();
        assert!(
            inline_worst_len(
                90_000,
                &low,
                SubmitMode::HistoricalTxNoSpv,
                REPLAY_REASON_PHASE3_FAULT
            ) <= QUEUE_MESSAGE_ROOM,
            "13 topics at the old cap are inline"
        );
        // the var only lowers the room
        assert_eq!(inline_room(None), QUEUE_MESSAGE_ROOM);
        assert_eq!(inline_room(Some("")), QUEUE_MESSAGE_ROOM);
        assert_eq!(inline_room(Some("lots")), QUEUE_MESSAGE_ROOM);
        assert_eq!(inline_room(Some(" 4096 ")), 4096);
        assert_eq!(inline_room(Some("1")), QUEUE_MESSAGE_ROOM_MIN);
        assert_eq!(inline_room(Some("999999999")), QUEUE_MESSAGE_ROOM);
    }

    /// Door 3: a 500 KB body is carried BY KEY (the base refused it: `replay_message` answered `None` past 90,000
    /// bytes and the route 502). The message is small and holds no body; the object read back is checked by length
    /// and sha256 and is the bytes, to the byte; anything else is refused.
    #[test]
    fn e585_d3_a_500kb_body_rides_by_key_and_reads_back_byte_for_byte() {
        let topics = vec!["tm_pot".to_string()];
        let beef = pattern(500_000);
        let plan = plan_replay(
            &beef,
            &topics,
            SubmitMode::CurrentTx,
            REPLAY_REASON_PHASE3_FAULT,
            QUEUE_MESSAGE_ROOM,
            &wide(),
        )
        .unwrap();
        let Carriage::R2(msg) = &plan else {
            panic!("500 KB rides by key, got inline")
        };
        let r = msg.r2.as_ref().unwrap();
        assert!(msg.beef_b64.is_empty());
        assert_eq!(r.bytes, 500_000);
        assert_eq!(
            r.sha256,
            hex::encode(bsv_rs::primitives::hash::sha256(&beef))
        );
        assert_eq!(r.key, r2_key(&r.sha256, &topics, "current-tx"));
        assert_eq!(
            r.txid, None,
            "bytes that are no BEEF name no subject (the letter is keyed by their hash)"
        );
        let json = serde_json::to_string(msg).unwrap();
        assert!(json.len() < 400, "{json}");
        assert!(!json.contains("beef_b64"));
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["r2"]["beefR2Key"], r.key.as_str());
        assert_eq!(v["r2"]["sha256"], r.sha256.as_str());
        assert_eq!(v["r2"]["bytes"], 500_000);
        assert_eq!(
            serde_json::from_str::<MutationMessage>(&json).unwrap(),
            *msg
        );
        // the bucket, as the producer writes it and the consumer reads it
        let mut bucket = std::collections::HashMap::new();
        bucket.insert(r.key.clone(), beef.clone());
        let read = bucket.get(&r.key).unwrap();
        check_blob(r, read).unwrap();
        assert_eq!(read, &beef);
        let mut flipped = beef.clone();
        flipped[250_000] ^= 1;
        assert!(check_blob(r, &flipped).unwrap_err().contains("hashes to"));
        assert!(check_blob(r, &beef[..499_999])
            .unwrap_err()
            .contains("holds 499999 B"));
        let other = BeefRef {
            key: r2_key(&"0".repeat(64), &topics, "current-tx"),
            ..r.clone()
        };
        assert!(check_blob(&other, &beef)
            .unwrap_err()
            .contains("does not name"));
        // the consumer's policy, by name: the door refuses what the replay would refuse, and carries it once the
        // policy admits it (no cap of the door's own)
        let at_the_door = plan_replay(
            &beef,
            &topics,
            SubmitMode::CurrentTx,
            REPLAY_REASON_PHASE3_FAULT,
            QUEUE_MESSAGE_ROOM,
            &beef_limits::QUEUE_BEEF_LIMITS,
        );
        if beef_limits::QUEUE_BEEF_LIMITS.max_bytes >= beef.len() {
            assert_eq!(at_the_door.unwrap(), plan);
        } else {
            let e = at_the_door.unwrap_err();
            assert!(
                e.contains("QUEUE_BEEF_LIMITS") && e.contains("500000 B"),
                "{e}"
            );
        }
    }

    /// Door 3: one object per (bytes, topics, mode): the topic ORDER does not matter, another topic set or mode is
    /// another object (an ack of one never deletes another's), and the key names the bytes' sha256.
    #[test]
    fn e585_d3_the_key_is_per_bytes_topics_and_mode() {
        let sha = "ab".repeat(32);
        let t = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let k = r2_key(&sha, &t(&["tm_a", "tm_b"]), "historical-tx");
        assert_eq!(
            k,
            r2_key(&sha, &t(&["tm_b", "tm_a", "tm_a"]), "historical-tx")
        );
        assert_ne!(k, r2_key(&sha, &t(&["tm_a"]), "historical-tx"));
        assert_ne!(k, r2_key(&sha, &t(&["tm_a", "tm_b"]), "current-tx"));
        assert_ne!(
            k,
            r2_key(&"cd".repeat(32), &t(&["tm_a", "tm_b"]), "historical-tx")
        );
        let parts: Vec<&str> = k.split('/').collect();
        assert_eq!(parts.len(), 3);
        assert_eq!(
            (parts[0], parts[1], parts[2].len()),
            ("mutations", sha.as_str(), 32)
        );
    }

    /// Door 3: what each read fault says (the note's text), and the source shape of the rule: the write before the
    /// send, a missing object noted as a FAULT, the acked objects deleted after the batch's loop.
    #[test]
    fn e585_d3_the_write_precedes_the_send_and_the_ack_deletes() {
        assert!(BlobFault::Missing("k".into()).says().contains("MISSING"));
        assert!(BlobFault::Read("x".into()).says().contains("read faulted"));
        assert!(BlobFault::Refused("x".into()).says().contains("refused"));
        let code = |s: &str| {
            s.lines()
                .map(|l| l.split("//").next().unwrap_or(""))
                .collect::<Vec<_>>()
                .join("\n")
        };
        let src = code(include_str!("queue.rs"));
        let start = src.find("pub async fn enqueue_replay(").unwrap();
        let f = &src[start..start + src[start..].find("\n}\n").unwrap()];
        let (put, send) = (
            f.find("put_beef(env, r, beef).await?").unwrap(),
            f.find(".send(").unwrap(),
        );
        assert!(
            put < send,
            "the R2 write, propagated by `?` (the route's 502), comes before the enqueue"
        );
        assert!(
            !f.contains("delete_beefs"),
            "a failed send deletes nothing: a twin's message may name the object"
        );
        let lib = code(include_str!("lib.rs"));
        let start = lib.find("async fn queue_handler(").unwrap();
        let h = &lib[start..start + lib[start..].find("\n}\n").unwrap()];
        assert!(h.contains("crate::queue::read_beef(&env, r).await"));
        assert_eq!(
            h.matches("acked_objects.extend(body.r2.as_ref().map(|r| r.key.clone()));")
                .count(),
            h.matches("msg.ack();").count(),
            "every ack deletes its object"
        );
        let (last_ack, delete) = (
            h.rfind("msg.ack();").unwrap(),
            h.find("crate::queue::delete_beefs(&env, &acked_objects")
                .unwrap(),
        );
        assert!(last_ack < delete, "after the loop");
        assert!(
            h.matches("msg.retry();").count() >= 6 && !h[..last_ack].contains("delete_beefs"),
            "a replay handed back deletes nothing"
        );
    }

    /// Door 3: all three deployable overlay configs bind the bucket, each its own.
    #[test]
    fn e585_d3_every_config_binds_the_bucket() {
        let low = include_str!("../wrangler.low.toml");
        let generic = include_str!("../wrangler.toml");
        for (cfg, header, bucket) in [
            (generic, "[[r2_buckets]]", "overlay-beefs"),
            (low, "[[r2_buckets]]", "low-overlay-beefs"),
            (low, "[[env.beta.r2_buckets]]", "low-overlay-beefs-beta"),
        ] {
            let want = format!(
                "{header}\nbinding = \"{BEEF_BLOBS_BINDING}\"\nbucket_name = \"{bucket}\"\n"
            );
            assert_eq!(cfg.matches(&want).count(), 1, "{header} -> {bucket}");
        }
        assert_eq!(low.matches("r2_buckets]]").count(), 2);
        assert_eq!(generic.matches("r2_buckets]]").count(), 1);
    }

    #[test]
    fn mutation_ack_is_durable_queued_or_refused_never_a_silent_ok() {
        assert_eq!(mutation_ack(true, None), MutationAck::Durable);
        // Durable wins even if a caller (wrongly) attempted an enqueue.
        assert_eq!(
            mutation_ack(true, Some(Err("x".into()))),
            MutationAck::Durable
        );
        assert_eq!(mutation_ack(false, Some(Ok(()))), MutationAck::Queued);
        assert_eq!(
            mutation_ack(false, Some(Err("queue send failed".into()))),
            MutationAck::Refused("queue send failed".into())
        );
        assert!(matches!(mutation_ack(false, None), MutationAck::Refused(_)));
    }

    #[test]
    fn pre_s2_message_without_reason_still_parses() {
        let v: MutationMessage = serde_json::from_str(
            r#"{"beef_b64":"AA==","topics":["tm_pot"],"mode":"historical-tx-no-spv"}"#,
        )
        .unwrap();
        assert_eq!(v.reason, "");
        assert_eq!(v.redrive, None, "bsv-low #576: a message from before the lever parses too");
        assert_eq!(v.r2, None, "bsv-low #585: and one from before door 3");
        assert_eq!(v.topics, vec!["tm_pot".to_string()]);
    }
}
