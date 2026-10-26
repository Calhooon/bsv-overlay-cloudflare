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
/// recording a spend, a pot row admitted or deleted, an eviction), the only marks the read withholds a press over; a
/// re-mark keeps the larger class until the judge acknowledges it. Edited in place: the table has shipped nowhere
/// (beta and prod certify 349).
pub const DIRTY_CREATE: &str = "CREATE TABLE IF NOT EXISTS owed_dirty (generation INTEGER PRIMARY KEY AUTOINCREMENT, identity TEXT NOT NULL, gameId TEXT NOT NULL, source TEXT NOT NULL, sourceKey TEXT NOT NULL, markedAtMs INTEGER NOT NULL, downgrade INTEGER NOT NULL DEFAULT 0, UNIQUE (identity, gameId, source, sourceKey))";
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
// columns, its admission (D4-L1), its delete and the eviction; a re-mark keeps the larger class (MAX), so a stamp
// after a spend leaves it.
macro_rules! mark {
    ($source:literal, $key:expr, $at:expr, $subjects:expr) => {
        mark!(class "0", $source, $key, $at, $subjects, " WHERE 1")
    };
    ($source:literal, $key:expr, $at:expr, $subjects:expr, $guard:literal) => {
        mark!(class "0", $source, $key, $at, $subjects, $guard)
    };
    (class $class:expr, $source:literal, $key:expr, $at:expr, $subjects:expr) => {
        mark!(class $class, $source, $key, $at, $subjects, " WHERE 1")
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

macro_rules! pot_mark {
    ($row:literal, $class:expr) => {
        mark!(
            class $class,
            "pot",
            concat!($row, ".txid || ':' || ", $row, ".outputIndex"),
            created_at!($row),
            pot_subjects!($row)
        )
    };
}

/// Admission, including the INSERT arm of store_record's UPSERT: always a downgrade (the fourth delta lens's D4-L1, the
/// ruling): a new pot row is a spend of its inputs, so a JOIN admitted withholds its seats' sweep presses of the hops
/// it consumed until the judge reads them, a readmission restoring a recorded spend too. A pot row is inserted once a
/// game, so the withheld moment costs nothing an honest seat can press.
pub const POT_INSERT: &str = trigger!("pot_records", insert, "INSERT", pot_mark!("NEW", "1"));
/// Spend, reorg, classification and the UPDATE arm of admission. Both keys are covered if moved. A downgrade only when
/// a spend column changed (the index recording, moving or clearing a spender) or the key moved; a stamp, a facts fill
/// or a re-present that leaves the spend columns as they were is a plain mark (D3-L1).
pub const POT_UPDATE: &str = trigger!(
    "pot_records",
    update,
    UPDATE,
    concat!(
        pot_mark!("NEW", "(OLD.spent IS NOT NEW.spent OR OLD.spendingTxid IS NOT NEW.spendingTxid OR OLD.spentConfirmed IS NOT NEW.spentConfirmed OR OLD.txid<>NEW.txid OR OLD.outputIndex<>NEW.outputIndex)"),
        mark!(
            class "1",
            "pot",
            "OLD.txid || ':' || OLD.outputIndex",
            created_at!("OLD"),
            pot_subjects!("OLD"),
            " WHERE OLD.txid<>NEW.txid OR OLD.outputIndex<>NEW.outputIndex"
        )
    )
);
/// The eviction's source DELETE, before any derived link can be relied on to survive.
pub const POT_DELETE: &str = trigger!("pot_records", delete, "DELETE", pot_mark!("OLD", "1"));

/// A party admission or filing.
pub const PARTY_INSERT: &str = trigger!(
    "potparty_records",
    insert,
    INSERT,
    mark!("party", "NEW.txid", created_at!("NEW"), own_subject!("NEW"))
);
/// A party relatch; a moved subject also invalidates its old identity and game.
pub const PARTY_UPDATE: &str = trigger!(
    "potparty_records",
    update,
    UPDATE,
    concat!(
        mark!("party", "NEW.txid", created_at!("NEW"), own_subject!("NEW")),
        mark!(
            "party",
            "OLD.txid",
            created_at!("OLD"),
            own_subject!("OLD"),
            " WHERE OLD.identity<>NEW.identity OR OLD.gameId<>NEW.gameId OR OLD.txid<>NEW.txid"
        )
    )
);
/// The party's eviction shadow move.
pub const PARTY_DELETE: &str = trigger!(
    "potparty_records",
    delete,
    DELETE,
    mark!("party", "OLD.txid", created_at!("OLD"), own_subject!("OLD"))
);

/// A hop marker admitted from the chain or a handfile.
pub const HOP_INSERT: &str = trigger!(
    "hopparty_records",
    insert,
    INSERT,
    mark!(
        "hop",
        "NEW.txid || ':' || NEW.hopVout",
        created_at!("NEW"),
        own_subject!("NEW")
    )
);
/// A hop relatch, including both subjects if its key ever moves.
pub const HOP_UPDATE: &str = trigger!("hopparty_records", update, UPDATE, concat!(
    mark!("hop", "NEW.txid || ':' || NEW.hopVout", created_at!("NEW"), own_subject!("NEW")),
    mark!("hop", "OLD.txid || ':' || OLD.hopVout", created_at!("OLD"), own_subject!("OLD"),
        " WHERE OLD.identity<>NEW.identity OR OLD.gameId<>NEW.gameId OR OLD.txid<>NEW.txid OR OLD.hopVout<>NEW.hopVout")
));
/// A deleted hop marker.
pub const HOP_DELETE: &str = trigger!(
    "hopparty_records",
    delete,
    DELETE,
    mark!(
        "hop",
        "OLD.txid || ':' || OLD.hopVout",
        created_at!("OLD"),
        own_subject!("OLD")
    )
);

macro_rules! refund_subjects {
    () => {
        concat!("SELECT pp.identity,pp.gameId FROM potparty_records pp INDEXED BY idx_potparty_pot WHERE pp.potTxid=NEW.potTxid AND pp.potVout=NEW.potVout UNION ALL ", own_subject!("NEW"))
    };
}
/// Both seats and the refund's own filer must re-judge an admitted backup.
pub const REFUND_INSERT: &str = trigger!(
    "potrefund_records",
    insert,
    INSERT,
    mark!("refund", "NEW.txid", created_at!("NEW"), refund_subjects!())
);
/// The filed or chain backup's verified latch.
pub const REFUND_UPDATE: &str = trigger!(
    "potrefund_records",
    update,
    "UPDATE OF refundValid",
    mark!("refund", "NEW.txid", created_at!("NEW"), refund_subjects!())
);
/// A result names both subjects, even when no party marker has arrived yet.
pub const RESULT_INSERT: &str =
    trigger!(
        "result_markers_v2",
        insert,
        INSERT,
        mark!("result", "NEW.txid", created_at!("NEW"),
        "SELECT NEW.winner AS identity,NEW.gameId AS gameId UNION ALL SELECT NEW.loser,NEW.gameId")
    );
/// A collected marker or held filing.
pub const COLLECTED_INSERT: &str = trigger!(
    "collected_markers_v2",
    insert,
    INSERT,
    mark!(
        "collected",
        "NEW.txid",
        created_at!("NEW"),
        own_subject!("NEW")
    )
);
/// A filed hop sweep.
pub const SWEEP_INSERT: &str = trigger!(
    "hopsweep_records",
    insert,
    INSERT,
    mark!("sweep", "NEW.txid", created_at!("NEW"), own_subject!("NEW"))
);

macro_rules! eviction_subjects {
    () => {
        "SELECT pp.identity,pp.gameId FROM potparty_records pp INDEXED BY idx_potparty_pot WHERE pp.potTxid=NEW.txid \
         UNION ALL SELECT pp.identity,pp.gameId FROM potparty_records_evicted pp INDEXED BY idx_evicted_party_pot WHERE pp.potTxid=NEW.txid \
         UNION ALL SELECT hp.identity,hp.gameId FROM json_each(NEW.releasedSpends) j CROSS JOIN hopparty_records hp INDEXED BY idx_hopparty_hop ON hp.txid=json_extract(j.value,'$.txid') AND hp.hopVout=json_extract(j.value,'$.vout') WHERE json_extract(j.value,'$.table')='pot_records'"
    };
}
/// The ledger opens before the shadow move and can already name the live parties.
pub const EVICTION_INSERT: &str = trigger!(
    "pot_evictions",
    insert,
    INSERT,
    mark!(
        class "1",
        "eviction",
        "NEW.txid",
        "NEW.evictedAt",
        eviction_subjects!()
    )
);
/// Closing the eviction and stamping its readmission also visit the shadow parties and released hops.
pub const EVICTION_UPDATE: &str = trigger!(
    "pot_evictions",
    update,
    UPDATE,
    mark!(
        class "1",
        "eviction",
        "NEW.txid",
        "COALESCE(NEW.readmittedAt,NEW.evictedAt)",
        eviction_subjects!()
    )
);

macro_rules! tx_parties {
    () => {
        "SELECT pp.identity,pp.gameId FROM pot_records p CROSS JOIN potparty_records pp INDEXED BY idx_potparty_pot ON pp.potTxid=p.txid AND pp.potVout=p.outputIndex WHERE p.txid=NEW.txid \
         UNION ALL SELECT pp.identity,pp.gameId FROM pot_records p INDEXED BY idx_pot_spending CROSS JOIN potparty_records pp INDEXED BY idx_potparty_pot ON pp.potTxid=p.txid AND pp.potVout=p.outputIndex WHERE p.spendingTxid=NEW.txid"
    };
}
/// A network witness can hold either the funding transaction or its spender.
pub const SEEN_INSERT: &str = trigger!(
    "network_seen",
    insert,
    INSERT,
    mark!(
        "seen",
        "NEW.txid",
        "COALESCE(NEW.sourceAtMs,NEW.seenAt * 1000,strftime('%s','now') * 1000)",
        tx_parties!()
    )
);

macro_rules! word_subjects {
    () => {
        concat!(
            "SELECT r.identity,r.gameId FROM owed_row_words w INDEXED BY idx_owed_row_words_txid CROSS JOIN owed_rows r ON r.identity=w.identity AND r.outpoint=w.outpoint WHERE w.txid=NEW.txid UNION ALL ",
            tx_parties!(),
            " UNION ALL SELECT hp.identity,hp.gameId FROM pot_records p CROSS JOIN hopparty_records hp INDEXED BY idx_hopparty_hop ON hp.txid=p.txid AND hp.hopVout=p.outputIndex WHERE p.txid=NEW.txid ",
            "UNION ALL SELECT hp.identity,hp.gameId FROM pot_records p INDEXED BY idx_pot_spending CROSS JOIN hopparty_records hp INDEXED BY idx_hopparty_hop ON hp.txid=p.txid AND hp.hopVout=p.outputIndex WHERE p.spendingTxid=NEW.txid ",
        )
    };
}

macro_rules! word_link_subjects {
    () => {
        concat!(
            "SELECT hp.identity,hp.gameId FROM pot_records p CROSS JOIN hop_pot_links l INDEXED BY idx_hop_pot_links_pot ON l.potTxid=p.txid AND l.potVout=p.outputIndex CROSS JOIN hopparty_records hp ON hp.rowid=l.markerRowid WHERE p.txid=NEW.txid ",
            "UNION ALL SELECT hp.identity,hp.gameId FROM pot_records p INDEXED BY idx_pot_spending CROSS JOIN hop_pot_links l INDEXED BY idx_hop_pot_links_pot ON l.potTxid=p.txid AND l.potVout=p.outputIndex CROSS JOIN hopparty_records hp ON hp.rowid=l.markerRowid WHERE p.spendingTxid=NEW.txid"
        )
    };
}
/// The first tracker word, including a txid no stored owed row names yet.
pub const WORD_INSERT: &str = trigger!(
    "tx_words",
    insert,
    INSERT,
    mark!("word", "NEW.txid", "NEW.updated_at", word_subjects!())
);
/// A tracker word or checked proof changing through the UPSERT's UPDATE arm.
pub const WORD_UPDATE: &str = trigger!(
    "tx_words",
    update,
    "UPDATE OF word,height,proof_root,header_hash",
    mark!("word", "NEW.txid", "NEW.updated_at", word_subjects!())
);
/// The first word's linked hop subjects, split from the five direct arms for D1's compound limit.
pub const WORD_LINK_INSERT: &str = trigger!(
    "tx_words",
    insert_links,
    INSERT,
    mark!("word", "NEW.txid", "NEW.updated_at", word_link_subjects!())
);
/// A changed word's linked hop subjects, in the same source transaction as the direct arms.
pub const WORD_LINK_UPDATE: &str = trigger!(
    "tx_words",
    update_links,
    "UPDATE OF word,height,proof_root,header_hash",
    mark!("word", "NEW.txid", "NEW.updated_at", word_link_subjects!())
);

macro_rules! beef_subjects {
    () => {
        "SELECT pp.identity,pp.gameId FROM potparty_records pp INDEXED BY idx_potparty_pot WHERE pp.potTxid=NEW.txid"
    };
}
/// BEEF admission and verified replacement both fire INSERT, even with recursive_triggers disabled.
pub const BEEF_INSERT: &str = trigger!(
    "pot_beefs",
    insert,
    INSERT,
    mark!("beef", "NEW.txid", created_at!("NEW"), beef_subjects!())
);
/// The funding's verified proof is latched or withdrawn.
pub const BEEF_UPDATE: &str = trigger!(
    "pot_beefs",
    update,
    "UPDATE OF proof_verified",
    mark!("beef", "NEW.txid", created_at!("NEW"), beef_subjects!())
);

macro_rules! refusal_subjects {
    ($row:literal) => {
        concat!("SELECT hp.identity,hp.gameId FROM hopparty_records hp INDEXED BY idx_hopparty_hop WHERE hp.txid=", $row, ".hopTxid AND hp.hopVout=", $row, ".hopVout")
    };
}
/// A refused JOIN strands its named hops.
pub const REFUSAL_INSERT: &str = trigger!(
    "submit_refusals",
    insert,
    INSERT,
    mark!(
        "refusal",
        "NEW.hopTxid || ':' || NEW.hopVout",
        "NEW.refusedAt",
        refusal_subjects!("NEW")
    )
);
/// The production refusal UPSERT replaces a hop's earlier refusal by UPDATE, not INSERT.
pub const REFUSAL_UPDATE: &str = trigger!(
    "submit_refusals",
    update,
    UPDATE,
    mark!(
        "refusal",
        "NEW.hopTxid || ':' || NEW.hopVout",
        "NEW.refusedAt",
        refusal_subjects!("NEW")
    )
);
/// Refusal expiry or retirement after admission.
pub const REFUSAL_DELETE: &str = trigger!(
    "submit_refusals",
    delete,
    DELETE,
    mark!(
        "refusal",
        "OLD.hopTxid || ':' || OLD.hopVout",
        "OLD.refusedAt",
        refusal_subjects!("OLD")
    )
);
/// The eviction's newly derived markerless hop uses the same hop source and outpoint key.
pub const RELEASED_HOP_INSERT: &str = trigger!(
    "released_hop_evidence",
    insert,
    INSERT,
    mark!(
        "hop",
        "NEW.hopTxid || ':' || NEW.hopVout",
        "NEW.evictedAt",
        own_subject!("NEW")
    )
);
/// Parsed spender bytes invalidate the same word subjects; both are keyed by the spender txid.
pub const SPENDER_BYTES_INSERT: &str = trigger!("owed_spender_bytes", insert, "INSERT",
    mark!("word", "NEW.txid", "NEW.createdAtMs",
        "SELECT r.identity,r.gameId FROM owed_row_words w INDEXED BY idx_owed_row_words_txid CROSS JOIN owed_rows r ON r.identity=w.identity AND r.outpoint=w.outpoint WHERE w.txid=NEW.txid"));
