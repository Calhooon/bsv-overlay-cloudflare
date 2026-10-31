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

// FIX-1010 (bsv-low #1010; the D2-ACTOR-F lens's LOW-1): the actor's families written by statements no trigger marked.
// Appended (never an edit of a shipped trigger). Each marks the subjects whose judgement reads the row, under the pair
// its writer notes, so the write's own event reaches the actor (the direct call, the queue behind it) and its apply
// takes the narrowed read: none of these sources carries its row, so a carried apply never stands in for them.
macro_rules! game_subjects {
    ($row:literal) => {
        concat!(
            "SELECT pp.identity,pp.gameId FROM potparty_records pp INDEXED BY idx_potparty_game_identity WHERE pp.gameId=", $row, ".gameId ",
            "UNION ALL SELECT hp.identity,hp.gameId FROM hopparty_records hp INDEXED BY idx_hopparty_game WHERE hp.gameId=", $row, ".gameId"
        )
    };
}
/// A hand marker admitted (`hand_write::hand_insert_query`): the hands every seat of its game reads (`results`), the
/// marker's own identity and the evicted twins of its pot (a twin-only seat's game) beside the game's live seats.
pub const HAND_INSERT: &str = trigger!(
    "hand_markers",
    insert,
    INSERT,
    mark!(
        "hand",
        "NEW.txid",
        created_at!("NEW"),
        concat!(
            game_subjects!("NEW"),
            " UNION ALL SELECT pp.identity,pp.gameId FROM potparty_records_evicted pp INDEXED BY idx_evicted_party_pot WHERE pp.potTxid=NEW.potTxid ",
            "UNION ALL ",
            own_subject!("NEW")
        )
    )
);
/// The hand re-latch pass (`hand_write::hand_relatch_query`): its `rowValid` verdict moved.
pub const HAND_UPDATE: &str = trigger!(
    "hand_markers",
    update,
    "UPDATE OF rowValid",
    mark!(
        "hand",
        "NEW.txid",
        created_at!("NEW"),
        concat!(
            game_subjects!("NEW"),
            " UNION ALL SELECT pp.identity,pp.gameId FROM potparty_records_evicted pp INDEXED BY idx_evicted_party_pot WHERE pp.potTxid=NEW.potTxid ",
            "UNION ALL ",
            own_subject!("NEW")
        ),
        " WHERE OLD.rowValid IS NOT NEW.rowValid"
    )
);
/// An on-chain proof bundle admitted (`PROOF_ADMIT_WRITE_SQL`): the proven hands of its game.
pub const PROOF_INSERT: &str = trigger!(
    "proof_markers",
    insert,
    INSERT,
    mark!(
        "proof",
        "NEW.txid",
        created_at!("NEW"),
        concat!(
            game_subjects!("NEW"),
            " UNION ALL SELECT NEW.winner,NEW.gameId"
        )
    )
);
/// A posted proof bundle (`proof_post::PROOF_POST_WRITE_SQL`, `INSERT OR REPLACE`: its INSERT fires), keyed by game.
pub const PROOF_POST_INSERT: &str = trigger!(
    "proof_posts",
    insert,
    INSERT,
    mark!(
        "proof",
        "NEW.gameId",
        created_at!("NEW"),
        concat!(
            game_subjects!("NEW"),
            " UNION ALL SELECT NEW.winner,NEW.gameId"
        )
    )
);
/// The result re-latch pass (`result_write::result_relatch_query`, which noted `result` and found no mark): its claim
/// tier moved. A tier lowered or withdrawn is a downgrade (the winner's payout rests on it).
pub const RESULT_UPDATE: &str = trigger!(
    "result_markers_v2",
    update,
    "UPDATE OF claimValid",
    mark!(
        class "(COALESCE(NEW.claimValid,-1) < COALESCE(OLD.claimValid,-1))",
        "result",
        "NEW.txid",
        created_at!("NEW"),
        "SELECT NEW.winner AS identity,NEW.gameId AS gameId UNION ALL SELECT NEW.loser,NEW.gameId",
        " WHERE OLD.claimValid IS NOT NEW.claimValid"
    )
);
/// A transaction's row deleted (the engine's deep delete of a spent coin's transaction, the eviction's shadow move):
/// the filed sweeps it pays and the parties of a pot it is, as its proof's triggers mark them. A proof deleted with
/// its row is a downgrade.
pub const TX_DELETE: &str = trigger!(
    "transactions",
    delete,
    DELETE,
    mark!(
        class "(OLD.has_proof IS 1)",
        "beef",
        "OLD.txid",
        "COALESCE(OLD.created_at * 1000, strftime('%s','now') * 1000)",
        "SELECT s.identity,s.gameId FROM hopsweep_records s INDEXED BY idx_hopsweep_sweep WHERE s.sweepTxid=OLD.txid \
         UNION ALL SELECT pp.identity,pp.gameId FROM potparty_records pp INDEXED BY idx_potparty_pot WHERE pp.potTxid=OLD.txid"
    )
);
/// The seat a party row's filing reaches (bsv-low #1010; FIX-1010-BUDGET): the OTHER seat of its pot, bounded by the
/// pot's COMMITTED seats, never by every party row a stranger filed on the pot. `results::seat_markers_sql` reads a
/// pot's party rows only under the two settle keys its lock committed (`pot_records.pubA`, `pubB`), so a row whose
/// own key is not one of them (a keyless v1 filing, a stranger's own key) changes no seat's judgement and marks nobody
/// here: a keyless row is refused on NEW alone (`$filer_has_key`, a term naming no table, which SQLite tests once
/// before any loop: the tier's keyless stranger filing reads nothing more). A row under a committed key is read by
/// the OTHER seat's judgement: the opponent the row names, marked only when that identity holds its own VERIFIED row
/// (`sigValid = 1`) on this pot under a committed key, the opponent first by `idx_party_verified_identity` (a seek on
/// the opponent, its verdict and the pot), the pot by its primary key only when such a row exists: at most two rows
/// (each an index entry and its row in D1's count). A stranger naming a victim
/// marks the victim only when the victim IS a seat of the pot, one mark a filing; the strangers already on the pot
/// are never read. As first built (0c80ed5c) the trigger read and marked every other party row of the pot
/// (`idx_potparty_pot`): K rows and K marks a filing on a pot of K rows, refused by the budget tier's
/// `record-potparty-filing` (7 rows in one statement against 3). Edited in place: migrations 480 to 493 shipped on
/// no D1. `$filer_key` is the filer's committed-key test over `p` (the update's is OLD's or NEW's).
macro_rules! party_seat_subjects {
    ($filer_has_key:literal, $filer_key:literal) => {
        concat!(
            "SELECT pp.identity,pp.gameId FROM potparty_records pp INDEXED BY idx_party_verified_identity CROSS JOIN pot_records p ",
            "ON p.txid=NEW.potTxid AND p.outputIndex=NEW.potVout ",
            "WHERE ", $filer_has_key, " AND pp.identity=NEW.opponentIdentity AND pp.sigValid=1 AND lower(pp.potTxid)=lower(NEW.potTxid) ",
            "AND pp.potVout=NEW.potVout AND pp.seatSettlePubkey IS NOT NULL AND pp.seatSettlePubkey IN (p.pubA,p.pubB) AND ", $filer_key,
            " AND (pp.identity IS NOT NEW.identity OR pp.gameId IS NOT NEW.gameId)"
        )
    };
}
/// A party filed or admitted: the other seat of its pot ([`party_seat_subjects`]), which `PARTY_INSERT` (the filer's
/// own subject) never marked. The same pair, so the publish carries the row to it too.
pub const PARTY_INSERT_SEATS: &str = trigger!(
    "potparty_records",
    insert_seats,
    INSERT,
    mark!(
        "party",
        "NEW.txid",
        created_at!("NEW"),
        party_seat_subjects!(
            "NEW.seatSettlePubkey IS NOT NULL",
            "NEW.seatSettlePubkey IN (p.pubA,p.pubB)"
        )
    )
);
/// A party re-latched (its `sigValid`, its seat's columns): the other seat of its pot, only when a column the seat
/// reads moved (an unchanged re-latch marks nothing) and the row's key, before or after, is committed. A verdict
/// withdrawn is a downgrade.
pub const PARTY_UPDATE_SEATS: &str = trigger!(
    "potparty_records",
    update_seats,
    "UPDATE OF sigValid,seatSettlePubkey,seatSigHex,sigHex,recoveryHeight",
    mark!(
        class "(OLD.sigValid IS 1 AND NEW.sigValid IS NOT 1)",
        "party",
        "NEW.txid",
        created_at!("NEW"),
        party_seat_subjects!(
            "(NEW.seatSettlePubkey IS NOT NULL OR OLD.seatSettlePubkey IS NOT NULL)",
            "(NEW.seatSettlePubkey IN (p.pubA,p.pubB) OR OLD.seatSettlePubkey IN (p.pubA,p.pubB))"
        ),
        " WHERE OLD.sigValid IS NOT NEW.sigValid OR OLD.seatSettlePubkey IS NOT NEW.seatSettlePubkey OR OLD.seatSigHex IS NOT NEW.seatSigHex OR OLD.sigHex IS NOT NEW.sigHex OR OLD.recoveryHeight IS NOT NEW.recoveryHeight"
    )
);
/// The seat derivation (`derive_pot_*`, `pot_hop_seats`): a pot whose committed keys are known seats EVERY hop marker
/// whose settle key is one of them, and the owed judge's belt reads a seat's held pots for an index-unspent hop
/// (`owed::JOIN_SEAT_POTS_SQL`); `pot_subjects` names only the markers of hops the index already holds spent by the
/// pot. These mark the seated markers' games from the source (`idx_hopparty_seat`), never the derived rows, so the
/// triggers' order does not matter. The pot's own pair; a seat made or taken is a downgrade (a JOIN may spend the hop).
macro_rules! seat_subjects {
    ($row:literal) => {
        concat!(
            "SELECT hp.identity,hp.gameId FROM hopparty_records hp INDEXED BY idx_hopparty_seat WHERE hp.seatSettlePubkey IN (",
            $row, ".pubA,", $row, ".pubB)"
        )
    };
}
/// A pot admitted (or readmitted) with its committed keys.
pub const POT_SEATS_INSERT: &str = trigger!(
    "pot_records",
    insert_seats,
    INSERT,
    mark!(
        class "1",
        "pot",
        "NEW.txid || ':' || NEW.outputIndex",
        created_at!("NEW"),
        seat_subjects!("NEW"),
        " WHERE NEW.pubA IS NOT NULL AND NEW.pubB IS NOT NULL"
    )
);
/// A pot's committed keys decoded or moved (the params backfill), or its key moved. `UPDATE OF` keeps it out of every
/// other `pot_records` UPDATE's prepared program (the #674 nomem bound, `lb674_nomem_*`: 256 KiB).
pub const POT_SEATS_UPDATE: &str = trigger!(
    "pot_records",
    update_seats,
    "UPDATE OF pubA,pubB,txid,outputIndex",
    mark!(
        class "1",
        "pot",
        "NEW.txid || ':' || NEW.outputIndex",
        created_at!("NEW"),
        seat_subjects!("NEW"),
        " WHERE NEW.pubA IS NOT NULL AND NEW.pubB IS NOT NULL AND (OLD.pubA IS NOT NEW.pubA OR OLD.pubB IS NOT NEW.pubB OR OLD.txid<>NEW.txid OR OLD.outputIndex<>NEW.outputIndex)"
    )
);
/// A pot deleted (the eviction's shadow move): its seats go with it.
pub const POT_SEATS_DELETE: &str = trigger!(
    "pot_records",
    delete_seats,
    DELETE,
    mark!(
        class "1",
        "pot",
        "OLD.txid || ':' || OLD.outputIndex",
        created_at!("OLD"),
        seat_subjects!("OLD"),
        " WHERE OLD.pubA IS NOT NULL AND OLD.pubB IS NOT NULL"
    )
);
/// A seated pot's bytes stored (`store_beef`): the belt reads a seat's held JOIN from them, so the markers it seats
/// re-judge (`BEEF_INSERT` names the pot's parties alone). The derived seats are read here: they were made by the pot's
/// own earlier write, never this statement.
pub const BEEF_SEATS_INSERT: &str = trigger!(
    "pot_beefs",
    insert_seats,
    INSERT,
    mark!(
        "beef",
        "NEW.txid",
        created_at!("NEW"),
        "SELECT hp.identity,hp.gameId FROM pot_hop_seats s CROSS JOIN hopparty_records hp ON hp.rowid=s.markerRowid WHERE s.potTxid=NEW.txid"
    )
);
/// The stored rows whose word roles name `$txid` (the rows whose served judgement read that word or those bytes).
macro_rules! word_rows {
    ($txid:literal) => {
        concat!(
            "SELECT r.identity,r.gameId FROM owed_row_words w INDEXED BY idx_owed_row_words_txid CROSS JOIN owed_rows r ON r.identity=w.identity AND r.outpoint=w.outpoint WHERE w.txid=",
            $txid
        )
    };
}
/// The tracker's stranger-projection sweep (`tracker_host::stored::SWEEP`, eight days): a word the rows read is gone.
/// A mined word deleted is a downgrade (a confirmation rested on it). Its pair is the word's; the publish carries the
/// emptied domain.
pub const WORD_DELETE: &str = trigger!(
    "tx_words",
    delete,
    DELETE,
    mark!(
        class "(OLD.word IS 'mined')",
        "word",
        "OLD.txid",
        "OLD.updated_at",
        word_rows!("OLD.txid")
    )
);
/// The spender-bytes sweeps (`owed::OWED_SPENDER_BYTES_SWEEP_SQL`, the row cap's): a spender's bytes the rows classified
/// from are gone. Its own pair (`bytes`), which carries no row, so its event takes the narrowed read. A downgrade (the
/// classification withdrawn).
pub const SPENDER_BYTES_DELETE: &str = trigger!(
    "owed_spender_bytes",
    delete,
    DELETE,
    mark!(
        class "1",
        "bytes",
        "OLD.txid",
        "OLD.createdAtMs",
        word_rows!("OLD.txid")
    )
);
