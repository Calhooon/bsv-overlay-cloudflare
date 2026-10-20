//! bsv-low #771 (L1b): the chain-event intake. The chaintracks feed (`GET /events?since=<cursor>`, the stack's
//! `docs/CHAIN-EVENTS.md`) is pulled over the CHAINTRACKS service binding in the scheduled pass, on a durable cursor
//! ([`CREATE`], one row). Each envelope passes `bsv_tracker::decode_envelope`, the crate's gate; an envelope it
//! refuses (an unknown kind or version, a malformed known shape, the feed's own `unreadable`) STOPS the pull at its
//! cursor. Each event's effect on the stored words is a durable re-ask queue ([`REASKS_CREATE`]) written in the SAME
//! D1 batch as the cursor's advance; the drain re-asks the queued rows, a few a pass. No read route writes here.

/// The feed cursor, one row: the last cursor consumed and its stamp, the last event's kind and cursor, an event held
/// across passes (its cursor and the sub-cursor of its affected rows), the envelope the pull stopped at, the tip.
/// Byte-identical to the overlay's migration (`d1::TRACKER_CHAIN_EVENTS_CREATE`; the catch-up pin).
pub const CREATE: &str = "CREATE TABLE IF NOT EXISTS tracker_chain_events (id INTEGER PRIMARY KEY CHECK (id = 1), cursor TEXT NOT NULL, at INTEGER NOT NULL, last_kind TEXT, last_cursor TEXT, held_cursor TEXT, held_height INTEGER, held_txid TEXT, stop_cursor TEXT, stop_fault TEXT, tip_height INTEGER, tip_age_seconds INTEGER)";

/// The durable re-ask queue: one row per txid, the crate's `Reask` (`reorg` or `fork`) and its height, the cursor of
/// the event that queued it, when, and the drain's failed attempts. Byte-identical to `d1::TRACKER_REASKS_CREATE`.
pub const REASKS_CREATE: &str = "CREATE TABLE IF NOT EXISTS tracker_reasks (txid TEXT PRIMARY KEY, kind TEXT NOT NULL, height INTEGER NOT NULL, event_cursor TEXT NOT NULL, written_at INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0)";

/// The three indexes, each the overlay's migration byte for byte, issued again on the scheduled path only (the
/// `tx_words_age` idiom: no read gains a schema statement).
pub const INDEXES: [&str; 3] = [
    "CREATE INDEX IF NOT EXISTS tracker_reasks_age ON tracker_reasks(written_at, txid)",
    "CREATE INDEX IF NOT EXISTS tx_words_mined_height ON tx_words(height, txid) WHERE word = 'mined'",
    "CREATE INDEX IF NOT EXISTS tx_words_mined_header ON tx_words(header_hash, txid) WHERE word = 'mined'",
];

#[cfg(test)]
mod schema_pins {
    use super::*;

    /// bsv-low #771: the two tables and three indexes are the overlay's migrations byte for byte (epoch Rule 24: one
    /// artifact), and each is in the overlay's list exactly once.
    #[test]
    fn i771_the_intakes_schema_is_the_overlays_migrations() {
        use bsv_overlay_cloudflare::d1 as o;
        assert_eq!(CREATE, o::TRACKER_CHAIN_EVENTS_CREATE);
        assert_eq!(REASKS_CREATE, o::TRACKER_REASKS_CREATE);
        assert_eq!(
            INDEXES,
            [
                o::TRACKER_REASKS_AGE_INDEX,
                o::TX_WORDS_MINED_HEIGHT_INDEX,
                o::TX_WORDS_MINED_HEADER_INDEX
            ]
        );
        for stmt in [CREATE, REASKS_CREATE].iter().chain(INDEXES.iter()) {
            assert_eq!(
                o::OVERLAY_MIGRATIONS.iter().filter(|m| *m == stmt).count(),
                1,
                "{stmt}"
            );
        }
    }
}
