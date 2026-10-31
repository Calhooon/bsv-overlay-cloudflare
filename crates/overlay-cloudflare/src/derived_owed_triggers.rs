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
/// A word's mark class (the fourth delta lens's D4-N3, the ruling): a DOWNGRADE when the word refuses its transaction
/// as the judge reads a refusal (`owed::funding_refusal_word`: a chain-tier `rejected` or `conflicted`, or #598's
/// producer mark `trackerTerminal` on a word that is not `mined`), so a refund press over a funding the network refused
/// is withheld at the read until the judge takes it; every other word is a plain mark. The JSON is read only behind
/// `json_valid` (a malformed body is no refusal, never an error in the source statement). Leading whitespace before
/// each `CASE` for wrangler 4.54's splitter.
macro_rules! word_class {
    () => {
        " CASE WHEN NEW.tier='chain' AND NEW.word IN ('rejected','conflicted') THEN 1 WHEN NEW.word='mined' THEN 0 WHEN json_valid(NEW.legacy_body) THEN json_type(NEW.legacy_body,'$.trackerTerminal') IS 'true' ELSE 0 END"
    };
}

/// The first tracker word, including a txid no stored owed row names yet.
pub const WORD_INSERT: &str = trigger!(
    "tx_words",
    insert,
    INSERT,
    mark!(class word_class!(), "word", "NEW.txid", "NEW.updated_at", word_subjects!())
);
/// A tracker word or checked proof changing through the UPSERT's UPDATE arm.
pub const WORD_UPDATE: &str = trigger!(
    "tx_words",
    update,
    "UPDATE OF word,height,proof_root,header_hash",
    mark!(class word_class!(), "word", "NEW.txid", "NEW.updated_at", word_subjects!())
);
/// The first word's linked hop subjects, split from the five direct arms for D1's compound limit.
pub const WORD_LINK_INSERT: &str = trigger!(
    "tx_words",
    insert_links,
    INSERT,
    mark!(class word_class!(), "word", "NEW.txid", "NEW.updated_at", word_link_subjects!())
);
/// A changed word's linked hop subjects, in the same source transaction as the direct arms.
pub const WORD_LINK_UPDATE: &str = trigger!(
    "tx_words",
    update_links,
    "UPDATE OF word,height,proof_root,header_hash",
    mark!(class word_class!(), "word", "NEW.txid", "NEW.updated_at", word_link_subjects!())
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
/// D2-OWED-FOLD-8: a missing-evidence row marks its seat's game as the evidence row does.
pub const RELEASED_HOP_MISSING_INSERT: &str = trigger!(
    "released_hop_evidence_missing",
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

// FIX-1001 (bsv-low #1001, L55's S2): the spend confirmation's writers the triggers above did not reach. Appended
// (never an edit of a shipped trigger): each one is a NEW trigger beside the shipped ones, marking the pair its writer
// already notes, so the consumer's routing read finds the subjects the judge reads them for.
macro_rules! tx_proof_subjects {
    () => {
        "SELECT s.identity,s.gameId FROM hopsweep_records s INDEXED BY idx_hopsweep_sweep WHERE s.sweepTxid=NEW.txid \
         UNION ALL SELECT pp.identity,pp.gameId FROM potparty_records pp INDEXED BY idx_potparty_pot WHERE pp.potTxid=NEW.txid"
    };
}
/// The engine's verified stitch of a transaction's proof (`update_transaction_beef`'s `INSERT OR REPLACE`, which fires
/// INSERT): the filed sweeps it pays (`hopsweep_records.sweepTxid`: the owed judge's `index-proof`, its
/// `sweepProofHeight`) and the parties of a pot it is. Only a write that lands a proof marks.
pub const TX_PROOF_INSERT: &str = trigger!(
    "transactions",
    proof_insert,
    INSERT,
    mark!(
        "beef",
        "NEW.txid",
        "NEW.created_at * 1000",
        tx_proof_subjects!(),
        " WHERE NEW.has_proof=1"
    )
);
/// The proof latched, re-anchored or withdrawn in place (`mark_transaction_proven(_at)`, the reorg sweep's
/// `TRANSACTION_UNPROVE_SQL`): a withdrawal is a downgrade (it takes the `index-proof` press away).
pub const TX_PROOF_UPDATE: &str = trigger!(
    "transactions",
    proof_update,
    "UPDATE OF has_proof,proofHeight",
    mark!(
        class "(OLD.has_proof=1 AND NEW.has_proof IS NOT 1)",
        "beef",
        "NEW.txid",
        "NEW.created_at * 1000",
        tx_proof_subjects!(),
        " WHERE OLD.has_proof IS NOT NEW.has_proof OR OLD.proofHeight IS NOT NEW.proofHeight"
    )
);
/// A hop's index row moved (the confirmation chase's `spentConfirmed` / `spentHeight` latch, a pointer written,
/// displaced or demoted): beside `POT_UPDATE`'s subjects (the hop's markers), the seats whose claim on the hop is a
/// FILED SWEEP or the eviction's RELEASED EVIDENCE, which hold no marker (L55's twin-only seat). The same pair.
pub const POT_UPDATE_FILERS: &str = trigger!(
    "pot_records",
    update_filers,
    UPDATE,
    mark!(
        class "(OLD.spent IS NOT NEW.spent OR OLD.spendingTxid IS NOT NEW.spendingTxid OR OLD.spentConfirmed IS NOT NEW.spentConfirmed OR OLD.txid<>NEW.txid OR OLD.outputIndex<>NEW.outputIndex)",
        "pot",
        "NEW.txid || ':' || NEW.outputIndex",
        created_at!("NEW"),
        concat!(
            "SELECT s.identity,s.gameId FROM hopsweep_records s INDEXED BY idx_hopsweep_hop WHERE s.hopTxid=NEW.txid AND s.hopVout=NEW.outputIndex ",
            "UNION ALL SELECT r.identity,r.gameId FROM released_hop_evidence r WHERE r.hopTxid=NEW.txid AND r.hopVout=NEW.outputIndex"
        )
    )
);
