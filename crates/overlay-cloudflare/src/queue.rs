//! Queue message types for the onSteakReady pattern.
//!
//! Mutations are enqueued as `MutationMessage` and processed by the
//! `#[event(queue)]` consumer. The BEEF + topics are serialized as JSON
//! (BEEF is base64-encoded to stay within CF Queue's 128KB message limit).

use serde::{Deserialize, Serialize};

/// Maximum BEEF size (bytes) that we enqueue. CF Queue messages are limited
/// to 128KB; base64 encoding inflates ~33%, so we cap at 90KB raw to leave
/// headroom for the rest of the JSON envelope.
pub const QUEUE_BEEF_SIZE_LIMIT: usize = 90_000;

/// A mutation message enqueued for reliable processing.
///
/// Sent by the /submit route after returning the Steak to the client.
/// Consumed by the queue handler to apply Phase 3 mutations.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MutationMessage {
    /// Base64-encoded BEEF bytes.
    pub beef_b64: String,
    /// Topic names this transaction targets.
    pub topics: Vec<String>,
    /// Submit mode: "current-tx", "historical-tx", or "historical-tx-no-spv".
    pub mode: String,
}
