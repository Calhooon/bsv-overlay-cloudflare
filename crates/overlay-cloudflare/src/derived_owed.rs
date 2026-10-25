//! #893: the owed list's query keys and transactional dirty subjects.
//! The judge and backfill are app-layer work; these migrations never judge or stamp a backfill.

/// Per-identity stored ceiling for the write-time judge. Keep actionable rows first
/// (`payout`, `refund-due`, `hop-stranded`, `in-progress`), discard excess `unbound`
/// rows and set `owed_state.truncated`. A judge counts only this identity by its index.
/// This is the existing owed page's 500-row bound, applied at the write by the next lane.
pub const OWED_MAX_ROWS: usize = 500;

/// The row's most recent judgement; NULL while an old worker still writes it.
pub const JUDGED_AT_ALTER: &str = "ALTER TABLE owed_rows ADD COLUMN judgedAtMs INTEGER";
/// The belt's indexed predicate; NULL retains the legacy facts value during deployment.
pub const CLAIMABLE_ALTER: &str = "ALTER TABLE owed_rows ADD COLUMN claimable INTEGER";
/// The tip handler's recovery-height range key.
pub const GATE_HEIGHT_ALTER: &str = "ALTER TABLE owed_rows ADD COLUMN gateHeight INTEGER";
/// When the belt may ask the chain; NULL means no question is owed.
pub const BELT_DUE_AT_ALTER: &str = "ALTER TABLE owed_rows ADD COLUMN beltDueAtMs INTEGER";
/// The tip at which the row's chain word was taken.
pub const PROBE_TIP_ALTER: &str = "ALTER TABLE owed_rows ADD COLUMN probeTip INTEGER";

/// Word roles (`settle`, `spender`, `funding`, `pay`) joined at the read and inverted at a word write.
pub const ROW_WORDS_CREATE: &str = "CREATE TABLE IF NOT EXISTS owed_row_words (identity TEXT, outpoint TEXT, role TEXT, txid TEXT, PRIMARY KEY (identity, outpoint, role))";
/// Coalesced dirty subjects. The rowid orders marks; a judge deletes only the marks it observed.
pub const DIRTY_CREATE: &str = "CREATE TABLE IF NOT EXISTS owed_dirty (identity TEXT NOT NULL, gameId TEXT NOT NULL, source TEXT NOT NULL, sourceKey TEXT NOT NULL, markedAtMs INTEGER NOT NULL, UNIQUE (identity, gameId, source, sourceKey))";
/// Evidence written beside an eviction's released spends, one identity per released hop.
pub const RELEASED_HOP_EVIDENCE_CREATE: &str = "CREATE TABLE IF NOT EXISTS released_hop_evidence (hopTxid TEXT NOT NULL, hopVout INTEGER NOT NULL, identity TEXT NOT NULL, gameId TEXT NOT NULL, potTxid TEXT NOT NULL, seatSettlePubkey TEXT NOT NULL, hopSats INTEGER NOT NULL, evictedAt INTEGER NOT NULL, PRIMARY KEY (hopTxid, hopVout, identity))";
/// The belt's rotation cursor and the backfill's identity cursor, each under its own name.
pub const BELT_STATE_CREATE: &str = "CREATE TABLE IF NOT EXISTS owed_belt_state (name TEXT PRIMARY KEY, cursor TEXT, tickedAtMs INTEGER)";

/// Invert a written txid to the identities whose rows name it.
pub const ROW_WORDS_TXID_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS idx_owed_row_words_txid ON owed_row_words(txid, identity)";
/// An identity's pending subjects and the judge's mark read.
pub const DIRTY_IDENTITY_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS idx_owed_dirty_identity ON owed_dirty(identity)";
/// The scheduled drain's oldest mark.
pub const DIRTY_MARKED_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS idx_owed_dirty_marked ON owed_dirty(markedAtMs)";
/// The per-game judge's released-hop inputs.
pub const RELEASED_HOP_IDENTITY_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_released_hop_evidence_identity_game ON released_hop_evidence(identity, gameId)";
/// The tip handler visits only rows in the recovery-height range.
pub const GATE_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_owed_rows_gate ON owed_rows(gateHeight) WHERE gateHeight IS NOT NULL";
/// The belt visits only rows that owe the chain a dated question.
pub const BELT_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_owed_rows_belt ON owed_rows(beltDueAtMs) WHERE beltDueAtMs IS NOT NULL";
/// Old workers delete an identity's rows whole; their word joins must disappear in that statement too.
pub const ROW_DELETE: &str = "CREATE TRIGGER IF NOT EXISTS owed_rows_delete_words AFTER DELETE ON owed_rows BEGIN DELETE FROM owed_row_words WHERE identity=OLD.identity AND outpoint=OLD.outpoint; END";
