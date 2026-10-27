//! Successful record answer latency, including verification, caps, source commit and response authentication.
use std::sync::atomic::{AtomicU64, Ordering};

// Noncumulative counts. A bucket below 300 ms makes the design's p95 bar directly testable.
const BOUNDS: [u64; 11] = [
    10,
    25,
    50,
    100,
    200,
    299,
    500,
    1_000,
    5_000,
    15_000,
    u64::MAX,
];
static COUNTS: [AtomicU64; 11] = [const { AtomicU64::new(0) }; 11];
/// bsv-low #911 (D2-ACTOR-A): the publish's own milliseconds, the door's `enqueue;dur`, in the same buckets.
static ENQUEUE: [AtomicU64; 11] = [const { AtomicU64::new(0) }; 11];
thread_local! {
    /// The enqueue of the answer being stamped (set by the door, taken by [`stamp_answer`]).
    static PENDING_ENQUEUE: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// The door's publish took `ms` (taken by the answer's stamp).
pub(crate) fn note_enqueue(ms: u64) {
    PENDING_ENQUEUE.with(|p| p.set(Some(ms)));
}

/// The enqueue quantile (`/health.record.enqueueMsP50/95`).
pub(super) fn enqueue_quantile(q: f64) -> Option<u64> {
    let counts = ENQUEUE.each_ref().map(|c| c.load(Ordering::Relaxed));
    quantile_of(&counts, q)
}

fn bucket_of(ms: u64) -> usize {
    BOUNDS
        .iter()
        .position(|bound| ms <= *bound)
        .unwrap_or(BOUNDS.len() - 1)
}

pub(super) fn samples() -> u64 {
    COUNTS.iter().map(|c| c.load(Ordering::Relaxed)).sum()
}
pub(super) fn quantile(q: f64) -> Option<u64> {
    let counts = COUNTS.each_ref().map(|c| c.load(Ordering::Relaxed));
    quantile_of(&counts, q)
}
fn quantile_of(counts: &[u64; 11], q: f64) -> Option<u64> {
    let total: u64 = counts.iter().sum();
    if total == 0 {
        return None;
    }
    let target = (total as f64 * q).ceil().max(1.0) as u64;
    let mut count = 0;
    for (bound, n) in BOUNDS.iter().zip(counts) {
        count += n;
        if count >= target {
            return Some(*bound);
        }
    }
    None
}

pub(crate) fn stamp_answer(response: &mut worker::Response, ms: u64) -> worker::Result<()> {
    COUNTS[bucket_of(ms)].fetch_add(1, Ordering::Relaxed);
    let prefix = response
        .headers()
        .get("Server-Timing")?
        .map(|s| format!("{s}, "))
        .unwrap_or_default();
    let timing = timing(ms, PENDING_ENQUEUE.with(std::cell::Cell::take));
    response.headers_mut().set("Server-Timing", &format!("{prefix}{timing}"))
}

/// PURE (bsv-low #911): the door's `Server-Timing` entries: the write and the publish apart (no judge: the
/// `owed-events` consumer's), or the detached judge of a Worker with no queue binding.
pub(crate) fn timing(ms: u64, enqueue: Option<u64>) -> String {
    match enqueue {
        Some(e) => {
            ENQUEUE[bucket_of(e)].fetch_add(1, Ordering::Relaxed);
            format!("write;dur={}, enqueue;dur={e}", ms.saturating_sub(e))
        }
        None => format!("write;dur={ms}, judge;desc=detached"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn empty_and_slow_samples_cannot_pass_the_p95_bar() {
        let mut counts = [0; 11];
        assert_eq!(quantile_of(&counts, 0.95), None);
        counts[2] = 94;
        counts[6] = 6;
        assert_eq!(quantile_of(&counts, 0.50), Some(50));
        assert_eq!(quantile_of(&counts, 0.95), Some(500));
        counts[10] = 100;
        assert_eq!(quantile_of(&counts, 0.95), Some(u64::MAX));
    }
}
