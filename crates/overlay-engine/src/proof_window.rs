//! Serving policy for admitted transactions waiting for their own proof.

/// Shared with the Worker watch and SQL lookup predicates.
pub const PROOFLESS_FLAG_MS: i64 = 24 * 60 * 60 * 1000;
/// Bounded rows considered for peer proof recovery during one tick.
pub const PEER_PROOF_ROW_LIMIT: usize = 20;
/// A silent peer must leave time for other peers in the pass.
pub const PEER_PROOF_REQUEST_MS: u64 = 5_000;

/// Whether an admitted output is served to lookups or held for its proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Serve it: it has its proof, or it was observed within the window.
    Served,
    /// Hold it: no proof and older than [`PROOFLESS_FLAG_MS`].
    Held,
}

/// The verdict for an output by its proof and when it was observed.
pub fn verdict(has_proof: bool, observed_ms: Option<i64>, now_ms: i64) -> Verdict {
    if has_proof || observed_ms.is_some_and(|seen| seen >= now_ms.saturating_sub(PROOFLESS_FLAG_MS))
    {
        Verdict::Served
    } else {
        Verdict::Held
    }
}

/// An output held for want of its proof.
#[derive(Debug, Clone)]
pub struct HeldOutput {
    /// The id of the transaction that created it.
    pub txid: String,
    /// Its index within that transaction.
    pub output_index: u32,
    /// The topic that admitted it.
    pub topic: String,
}

/// Kept outside the pass future so a deadline preserves completed work.
#[derive(Debug, Default, Clone, Copy)]
pub struct PeerProofSummary {
    /// Proof requests sent to peers.
    pub requests: u64,
    /// Proofs that landed.
    pub landed: u64,
    /// Proofs refused.
    pub refused: u64,
    /// Requests that faulted.
    pub faults: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proof_window_boundary_unknown_age_and_proof() {
        let now = 3 * PROOFLESS_FLAG_MS;
        assert_eq!(
            verdict(false, Some(now - PROOFLESS_FLAG_MS), now),
            Verdict::Served
        );
        assert_eq!(
            verdict(false, Some(now - PROOFLESS_FLAG_MS - 1), now),
            Verdict::Held
        );
        assert_eq!(verdict(false, None, now), Verdict::Held);
        assert_eq!(verdict(true, None, now), Verdict::Served);
        assert_eq!(verdict(false, Some(now + 1), now), Verdict::Served);
    }
}
