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
/// Coalesced dirty subjects. Generation aliases an AUTOINCREMENT rowid that is never reused after a drain.
/// A re-mark advances it, so a judge acknowledging `generation <= seen` leaves newer work pending.
/// `downgrade` is the mark's CLASS (the third delta lens's D3-L1): 1 when the write may take a press away (the index
/// recording a spend, a pot row admitted or deleted, an eviction, a word refusing its transaction), the only marks the
/// read withholds a press over; a re-mark keeps the larger class until the judge acknowledges it. Edited in place: the table has shipped nowhere
/// (beta and prod certify 349).
pub const DIRTY_CREATE: &str = "CREATE TABLE IF NOT EXISTS owed_dirty (generation INTEGER PRIMARY KEY AUTOINCREMENT, identity TEXT NOT NULL, gameId TEXT NOT NULL, source TEXT NOT NULL, sourceKey TEXT NOT NULL, markedAtMs INTEGER NOT NULL, downgrade INTEGER NOT NULL DEFAULT 0, UNIQUE (identity, gameId, source, sourceKey))";
/// Evidence written beside an eviction's released spends, one identity per released hop.
pub const RELEASED_HOP_EVIDENCE_CREATE: &str = "CREATE TABLE IF NOT EXISTS released_hop_evidence (hopTxid TEXT NOT NULL, hopVout INTEGER NOT NULL, identity TEXT NOT NULL, gameId TEXT NOT NULL, potTxid TEXT NOT NULL, seatSettlePubkey TEXT NOT NULL, hopSats INTEGER NOT NULL, evictedAt INTEGER NOT NULL, PRIMARY KEY (hopTxid, hopVout, identity))";
/// D2-OWED-FOLD-8: an evicted JOIN whose stored bytes are absent or undecodable (or do not carry a released hop's
/// transaction) leaves no evidence; each released outpoint is named for each verified seat of that JOIN, so the
/// judge serves the old walk's could-not-judge row (`evidenceMissing`), never silence and never a press. Keyed for the
/// judge's read by identity and game; ended by the ledger's readmission as the evidence is.
pub const RELEASED_HOP_EVIDENCE_MISSING_CREATE: &str = "CREATE TABLE IF NOT EXISTS released_hop_evidence_missing (identity TEXT NOT NULL, gameId TEXT NOT NULL, hopTxid TEXT NOT NULL, hopVout INTEGER NOT NULL, potTxid TEXT NOT NULL, evictedAt INTEGER NOT NULL, bytesRead INTEGER NOT NULL, PRIMARY KEY (identity, gameId, hopTxid, hopVout))";
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
/// The scheduled drain's daily re-judge of rows older than 24 h (risk 8.2's third repair); NULL is an old worker's row,
/// which the backfill rewrites.
pub const JUDGED_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_owed_rows_judged ON owed_rows(judgedAtMs) WHERE judgedAtMs IS NOT NULL";
/// The D2-OWED lens's HIGH-1 as narrowed by the delta lens's D-H1: on a tip, the claimable sweeps judged below it
/// whose press rests on a courier probe alone (the hop's index `status` fact not `unspent`) are dated due now; this is
/// that statement's whole access path (`probeTip` is the tip the row was judged at, NULL = judged with no tip). The
/// status term is the statement's own, written as it writes it, so the index holds only the rows the statement writes
/// and an index-unspent claimable sweep (an honest un-swept hop, a stranger's dust hop) costs it no read
/// (D2-OWED-FOLD-3). The key's tail (the third delta lens's D3-N3) makes the belt's first class a seek too: inside one
/// judged tip, its rows due by now (`beltDueAtMs`), covering the identity and the outpoint it reads, so a row of the
/// class re-armed (not due) is never visited. Edited in place: migrations 396 to 407 shipped nowhere.
pub const SWEEP_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_owed_rows_sweep ON owed_rows(probeTip,beltDueAtMs,identity,outpoint) WHERE family='hop-stranded' AND claimable=1 AND json_extract(facts,'$.status') IS NOT 'unspent'";
/// The D2-OWED lens's HIGH-1 and MEDIUM-2: a row judged with no chain tip (TIP-877's tipless judgement) is re-marked
/// on the next tip; an old worker's keyless row is in it too.
pub const UNTIPPED_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_owed_rows_untipped ON owed_rows(probeTip,identity,gameId) WHERE probeTip IS NULL";
/// The D2-OWED lens's HIGH-3: the belt's dated rows by identity, then due time, covering, so the tick walks a bounded
/// number of identities past its cursor and takes each one's oldest due rows (round-robin), never the whole due set.
pub const BELT_TURN_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_owed_rows_belt_turn ON owed_rows(identity,beltDueAtMs,outpoint) WHERE beltDueAtMs IS NOT NULL";
/// The D2-OWED lens's MEDIUM-1: a hook's change note names source keys (a txid, `gate`, `untipped`); its subjects are an
/// index search, never a scan of the fleet's marks. The wanted envelopes and the age marks carry the constant keys
/// `wanted` and `age`, so it serves them too (one index fewer for every mark to write).
pub const DIRTY_SOURCE_KEY_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS idx_owed_dirty_source_key ON owed_dirty(sourceKey)";
/// The drain's round-robin: an identity's marks oldest first, read a bounded few at a time.
pub const DIRTY_IDENTITY_MARKED_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_owed_dirty_identity_marked ON owed_dirty(identity,markedAtMs)";
/// The D2-OWED lens's MEDIUM-5: an old worker's keyless row (`judgedAtMs` NULL: the deploy window, or a rollback to
/// the old walk and forward again) is re-marked by the drain's age repair, bounded, over its own partial index.
pub const UNJUDGED_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_owed_rows_unjudged ON owed_rows(judgedAtMs,identity,gameId) WHERE judgedAtMs IS NULL";
/// The D2-OWED lens's LOW-2: the judge's per-game delete reads one game's rows, never the identity's whole list.
pub const ROWS_GAME_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS idx_owed_rows_identity_game ON owed_rows(identity,gameId)";
/// The D2-OWED lens's LOW-2: `owed_state.rows` is kept by the row writes themselves, so the judge's state write and
/// the stored ceiling's guard read one row, never COUNT the identity's list. An old worker's whole-identity write sets
/// the figure explicitly after its rows, so it stays exact through the window.
pub const ROWS_COUNT_INSERT: &str = "CREATE TRIGGER IF NOT EXISTS owed_rows_count_insert AFTER INSERT ON owed_rows BEGIN UPDATE owed_state SET rows=rows+1 WHERE identity=NEW.identity; END";
/// The count's other half: a judge's per-game delete, the stored ceiling, an old worker's whole-identity delete.
pub const ROWS_COUNT_DELETE: &str = "CREATE TRIGGER IF NOT EXISTS owed_rows_count_delete AFTER DELETE ON owed_rows BEGIN UPDATE owed_state SET rows=MAX(rows-1,0) WHERE identity=OLD.identity; END";
/// The D2-OWED fold's write cost: every mark writes each of `owed_dirty`'s indexes (one wanted mark is five physical
/// writes, ruling 1), and since MEDIUM-1 the drain reads neither of these two (its age order is
/// `idx_owed_dirty_identity_marked`, an identity's prefix the UNIQUE key's index).
pub const DIRTY_MARKED_DROP: &str = "DROP INDEX IF EXISTS idx_owed_dirty_marked";
/// FIX-1001 (bsv-low #1001): a transaction's proof write finds the sweeps filed for it (`TX_PROOF_INSERT`/`_UPDATE`).
pub const HOPSWEEP_SWEEP_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_hopsweep_sweep ON hopsweep_records(sweepTxid)";
/// The backlog snapshot uses a new name: a rolling old worker can still execute DIRTY_MARKED_DROP.
pub const DIRTY_BACKLOG_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_owed_dirty_backlog ON owed_dirty(markedAtMs)";
/// See [`DIRTY_MARKED_DROP`].
pub const DIRTY_IDENTITY_DROP: &str = "DROP INDEX IF EXISTS idx_owed_dirty_identity";
/// Old workers delete an identity's rows whole; their word joins must disappear in that statement too.
pub const ROW_DELETE: &str = "CREATE TRIGGER IF NOT EXISTS owed_rows_delete_words AFTER DELETE ON owed_rows BEGIN DELETE FROM owed_row_words WHERE identity=OLD.identity AND outpoint=OLD.outpoint; END";

// Each statement below marks only affected subjects, inside its source transaction.
// Never edit a shipped trigger: append DROP TRIGGER IF EXISTS + CREATE to OVERLAY_MIGRATIONS (#638).
// Keep every CASE preceded by whitespace, including after a comma, for wrangler 4.54's splitter.
// A source UPSERT's DO UPDATE overrides a trigger's OR IGNORE with ABORT. The explicit conflict
// clause below preserves coalescing there too, including a subject reached through several indexes.
// SQLite assigns the attempted insert a fresh AUTOINCREMENT value before checking the unique key;
// keep excluded.generation and the newest mark's fields so an in-flight judge cannot erase the re-mark.
// The mark's class (`downgrade`, D3-L1) is an expression over the row: "0" for every source but the pot's spend
// columns, its admission (D4-L1), its delete, the eviction and a refusing word (D4-N3); a re-mark keeps the larger
// class (MAX), so a stamp after a spend leaves it.
macro_rules! legacy_mark {
    ($source:literal, $key:expr, $at:expr, $subjects:expr) => {
        legacy_mark!(class "0", $source, $key, $at, $subjects, " WHERE 1")
    };
    ($source:literal, $key:expr, $at:expr, $subjects:expr, $guard:literal) => {
        legacy_mark!(class "0", $source, $key, $at, $subjects, $guard)
    };
    (class $class:expr, $source:literal, $key:expr, $at:expr, $subjects:expr) => {
        legacy_mark!(class $class, $source, $key, $at, $subjects, " WHERE 1")
    };
    (class $class:expr, $source:literal, $key:expr, $at:expr, $subjects:expr, $guard:literal) => {
        concat!("INSERT OR IGNORE INTO owed_dirty (identity,gameId,source,sourceKey,markedAtMs,downgrade) SELECT s.identity,s.gameId,'",
            $source, "',", $key, ",", $at, ",", $class, " FROM (", $subjects, ") s", $guard,
            " ON CONFLICT(identity,gameId,source,sourceKey) DO UPDATE SET generation=excluded.generation,source=excluded.source,sourceKey=excluded.sourceKey,markedAtMs=excluded.markedAtMs,downgrade=MAX(owed_dirty.downgrade,excluded.downgrade); ")
    };
}

macro_rules! trigger_verb {
    ($verb:ident) => {
        stringify!($verb)
    };
    ($verb:literal) => {
        $verb
    };
}

macro_rules! trigger {
    ($table:literal, $name:ident, $verb:tt, $body:expr) => {
        concat!(
            "CREATE TRIGGER IF NOT EXISTS owed_dirty_",
            $table,
            "_",
            stringify!($name),
            " AFTER ",
            trigger_verb!($verb),
            " ON ",
            $table,
            " BEGIN ",
            $body,
            "END"
        )
    };
}

macro_rules! created_at {
    ($row:literal) => {
        concat!(
            "COALESCE(",
            $row,
            ".createdAt * 1000, strftime('%s','now') * 1000)"
        )
    };
}

macro_rules! own_subject {
    ($row:literal) => {
        concat!(
            "SELECT ",
            $row,
            ".identity AS identity,",
            $row,
            ".gameId AS gameId"
        )
    };
}

/// Pot and hop changes visit the party, hop and derived-link indexes, never an identity's history.
/// The last arm covers a pot arriving after its spent hops: another AFTER trigger may not have
/// derived its links yet. Reading those same source hops makes trigger execution order irrelevant.
macro_rules! pot_subjects {
    ($row:literal) => {
        concat!(
            "SELECT pp.identity,pp.gameId FROM potparty_records pp INDEXED BY idx_potparty_pot WHERE pp.potTxid=", $row, ".txid AND pp.potVout=", $row, ".outputIndex ",
            "UNION ALL SELECT hp.identity,hp.gameId FROM hopparty_records hp INDEXED BY idx_hopparty_hop WHERE hp.txid=", $row, ".txid AND hp.hopVout=", $row, ".outputIndex ",
            "UNION ALL SELECT hp.identity,hp.gameId FROM hop_pot_links l INDEXED BY idx_hop_pot_links_pot CROSS JOIN hopparty_records hp ON hp.rowid=l.markerRowid WHERE l.potTxid=", $row, ".txid AND l.potVout=", $row, ".outputIndex ",
            "UNION ALL SELECT hp.identity,hp.gameId FROM pot_records h INDEXED BY idx_pot_spending_lower CROSS JOIN hopparty_records hp INDEXED BY idx_hopparty_hop ON hp.txid=h.txid AND hp.hopVout=h.outputIndex WHERE lower(h.spendingTxid)=", $row, ".txid AND ", $row, ".outputIndex=0 AND h.spent=1"
        )
    };
}

// Keep the first 409 migrations byte-for-byte: both generations share their trigger bodies.
macro_rules! mark {
    ($($args:tt)*) => { legacy_mark!($($args)*) };
}
include!("derived_owed_triggers.rs");

/// Appended replacements: every mark dates the database write, never the evidence it touched.
pub mod write_time {
    macro_rules! mark {
        ($source:literal, $key:expr, $at:expr, $subjects:expr $(, $guard:literal)?) => {
            legacy_mark!($source, $key, "strftime('%s','now') * 1000", $subjects $(, $guard)?)
        };
        (class $class:expr, $source:literal, $key:expr, $at:expr, $subjects:expr $(, $guard:literal)?) => {
            legacy_mark!(class $class, $source, $key, "strftime('%s','now') * 1000", $subjects $(, $guard)?)
        };
    }
    include!("derived_owed_triggers.rs");
}

#[cfg(test)]
mod tests {
    #[test]
    fn cadence_replacements_preserve_the_shipped_migration_prefix() {
        let migrations = crate::d1::OVERLAY_MIGRATIONS;
        let mut fingerprint = 0u32;
        for sql in &migrations[..409] {
            let hash = sql.bytes().fold(0x811c_9dc5u32, |h,b| (h ^ u32::from(b)).wrapping_mul(0x0100_0193));
            fingerprint = fingerprint.wrapping_add(hash);
        }
        assert_eq!(fingerprint, 740971678, "the shipped prefix must remain byte-for-byte immutable");
        for pair in migrations[409..467].chunks_exact(2) {
            assert!(pair[0].starts_with("DROP TRIGGER IF EXISTS owed_dirty_"));
            assert!(pair[1].starts_with("CREATE TRIGGER IF NOT EXISTS owed_dirty_"));
            assert!(pair[1].contains("strftime('%s','now') * 1000"));
        }
    }
}
