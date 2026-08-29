//! Queue message types for the onSteakReady pattern — and, since S2
//! (ARCHITECTURE v2 principle 1, bsv-low 2026-08-29), the QUEUE-DURABLE
//! ADMISSION replay.
//!
//! Mutations are enqueued as `MutationMessage` and processed by the
//! `#[event(queue)]` consumer. The BEEF + topics are serialized as JSON
//! (BEEF is base64-encoded to stay within CF Queue's 128KB message limit).
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

use overlay_engine::types::SubmitMode;
use serde::{Deserialize, Serialize};

/// Maximum BEEF size (bytes) that we enqueue. CF Queue messages are limited
/// to 128KB; base64 encoding inflates ~33%, so we cap at 90KB raw to leave
/// headroom for the rest of the JSON envelope.
pub const QUEUE_BEEF_SIZE_LIMIT: usize = 90_000;

/// A mutation message enqueued for reliable processing.
///
/// Sent by the /submit route after returning the Steak to the client.
/// Consumed by the queue handler to apply Phase 3 mutations.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MutationMessage {
    /// Base64-encoded BEEF bytes.
    pub beef_b64: String,
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
}

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

/// Build the replay message for an admission whose Phase-3 writes did not
/// all land. `None` when the BEEF cannot ride the queue (larger than
/// [`QUEUE_BEEF_SIZE_LIMIT`]) — the caller must then REFUSE the ack rather
/// than pretend; a LOW BEEF is KB-scale, so this is the named residual,
/// not the expected path.
#[must_use]
pub fn replay_message(
    beef: &[u8],
    topics: &[String],
    mode: SubmitMode,
    reason: &str,
) -> Option<MutationMessage> {
    use base64::{engine::general_purpose::STANDARD, Engine as B64Engine};
    if beef.len() > QUEUE_BEEF_SIZE_LIMIT {
        return None;
    }
    Some(MutationMessage {
        beef_b64: STANDARD.encode(beef),
        topics: topics.to_vec(),
        mode: mode_wire(mode).to_string(),
        reason: reason.to_string(),
    })
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

/// Enqueue the S2 replay for an undurable admission. `Err` names why the
/// bytes are NOT in the queue (oversize BEEF, missing binding, send fault)
/// so the route can refuse with the reason.
pub async fn enqueue_replay(
    env: &worker::Env,
    beef: &[u8],
    topics: &[String],
    mode: SubmitMode,
) -> Result<(), String> {
    let Some(msg) = replay_message(beef, topics, mode, REPLAY_REASON_PHASE3_FAULT) else {
        return Err(format!(
            "BEEF too large for the mutation queue ({} B > {} B)",
            beef.len(),
            QUEUE_BEEF_SIZE_LIMIT
        ));
    };
    let queue = env
        .queue("MUTATION_QUEUE")
        .map_err(|e| format!("MUTATION_QUEUE binding unavailable: {e}"))?;
    queue
        .send(msg)
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
        assert_eq!(replay_submit_mode("historical-tx"), SubmitMode::HistoricalTx);
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

    #[test]
    fn replay_message_carries_bytes_topics_mode_reason_and_refuses_oversize() {
        let topics = vec!["tm_pot".to_string(), "tm_lowfund".to_string()];
        let beef = vec![0xbeu8; 10];
        let msg = replay_message(&beef, &topics, SubmitMode::HistoricalTxNoSpv, "phase3-fault")
            .expect("KB-scale BEEF rides the queue");
        assert_eq!(msg.topics, topics);
        assert_eq!(msg.mode, "historical-tx-no-spv");
        assert_eq!(msg.reason, "phase3-fault");
        {
            use base64::{engine::general_purpose::STANDARD, Engine as B64Engine};
            assert_eq!(STANDARD.decode(&msg.beef_b64).unwrap(), beef);
        }
        let at_cap = vec![0u8; QUEUE_BEEF_SIZE_LIMIT];
        assert!(replay_message(&at_cap, &topics, SubmitMode::HistoricalTx, "x").is_some());
        let over = vec![0u8; QUEUE_BEEF_SIZE_LIMIT + 1];
        assert!(
            replay_message(&over, &topics, SubmitMode::HistoricalTx, "x").is_none(),
            "an oversize BEEF is a named refusal, never a truncated message"
        );
    }

    #[test]
    fn mutation_ack_is_durable_queued_or_refused_never_a_silent_ok() {
        assert_eq!(mutation_ack(true, None), MutationAck::Durable);
        // Durable wins even if a caller (wrongly) attempted an enqueue.
        assert_eq!(mutation_ack(true, Some(Err("x".into()))), MutationAck::Durable);
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
        assert_eq!(v.topics, vec!["tm_pot".to_string()]);
    }
}
