//! bsv-low #674: the leaderboard derived at the write (the #638 doctrine).
//!
//! `board_wins` holds one row per COUNTED win (the app layer's `chain_wins_cte` predicate, verbatim: a fresh
//! winner verdict, decoded committed keys, the confirmed landing or the witnessed final spend, the latched slot
//! rule); `board_owners` the count per owner; `board_hands` the winner's earliest card-bearing verified claim of
//! each identity-owned win; `board_total` the count of every win. SQLite triggers in the source statements keep
//! them: a write to `pot_records`, `network_seen` or `potparty_records` RECOMPUTES its pot's `board_wins` row
//! from the source (delete, then insert when the predicate holds), so a trigger never diffs and a REPLACE (which
//! skips a delete trigger) still leaves the right row; `board_owners`, `board_total` and `board_hands` follow
//! `board_wins` by its own triggers, so they are a function of it by construction. A write to
//! `result_markers_v2` recomputes its pot's hand. The eviction's shadow move is a DELETE from the source tables
//! and the readmission an INSERT back, so both are these triggers.
//!
//! The page reads ONLY `board_owners` (the #674 lens fold, MEDIUM-1): each `board_wins` row carries `frag`, every
//! row the app layer's page tail reads for that win (its `pot_records` status and decoded columns, its first
//! `RESULT_ROWS_PER_POT` result markers, and each marker's proof pointers and posted bundle), and each
//! `board_owners` row carries `card`, a JSON object of its wins' rows and frags keyed by pot txid, with `packed`
//! the number of wins it holds. A card that would pass `CARD_MAX_BYTES` (or a frag past `FRAG_MAX_BYTES`) packs no
//! further win: the owner's `packed` falls below its `wins` and the page reads that owner by its wins instead.
//! The markers, proof and posted tables refresh the frags they feed (`refresh_frag!`), and a frag's change
//! moves its card (`WIN_FRAG`).
//!
//! A trigger's text is immutable once shipped (`CREATE TRIGGER IF NOT EXISTS` keeps the old body in silence): a
//! change is a `DROP TRIGGER IF EXISTS` + `CREATE` pair APPENDED to the migration list.

/// The counted-win predicate over `p` (a `pot_records` row) and `ns` (its spender's `network_seen` row), as
/// `chain_wins_cte` states it. `POT_UPDATE` fires on every column it reads: when it reads a new one, that column joins
/// `POT_UPDATE`'s `UPDATE OF` list by a `DROP TRIGGER IF EXISTS` + `CREATE` pair APPENDED to the migration list (the
/// #674 lens, LOW-2), or a writer of only that column moves no board row. Likewise a column the frag reads joins
/// `POT_FRAG`'s list (or `MARKER_UPDATE`'s, `PROOF_UPDATE`'s, `POST_UPDATE`'s).
macro_rules! counted {
    () => {
        "p.outputIndex=0 AND p.paramsDecoded=1 AND p.pubA IS NOT NULL AND p.pubB IS NOT NULL AND p.verdict IN ('winner-a','winner-b') AND p.spendingTxid IS NOT NULL AND p.verdictTxid=p.spendingTxid AND p.spent=1 AND (p.spentConfirmed=1 OR (ns.txid IS NOT NULL AND p.spenderFinal=1))"
    };
}

/// One seat's latched identity: exactly one distinct `sigValid = 1` identity for that key on that pot, else NULL
/// (the `attribute_seats` slot rule).
macro_rules! slot {
    ($key:literal) => {
        concat!(
            "(SELECT CASE WHEN COUNT(DISTINCT lower(q.identity))=1 THEN MIN(lower(q.identity)) END FROM potparty_records q WHERE q.potTxid=p.txid AND q.potVout=p.outputIndex AND q.sigValid=1 AND lower(q.seatSettlePubkey)=lower(p.",
            $key,
            "))"
        )
    };
}

/// The page tail's rows for the pot `p` (with its spender's witness `ns`) as one JSON object: `s` the status row
/// (`batch_where_sql`'s columns, `fundingSeen` aside: the fold never reads it), `d` the decoded row
/// (`decoded_pots_sql`'s columns), `m` the first `RESULT_ROWS_PER_POT` result markers in `pot_markers_sql`'s order
/// (each with `rn`, its proof pointers `pp` as `[createdAt, rowid, txid]` in `proof_pointers_sql`'s order, at most
/// `PROOF_POINTERS_PER_KEY`, and `po` its posted bundle, `proof_posted_sql`), all keyed as the app layer binds
/// them (`lower(gameId)`, `lower(winner)`). NULL past `FRAG_MAX_BYTES`.
macro_rules! frag {
    () => {
        "(SELECT CASE WHEN length(fr.v) <= 200000 THEN fr.v END FROM (SELECT json_object('s',json_object('txid',p.txid,'outputIndex',p.outputIndex,'spent',p.spent,'spendingTxid',p.spendingTxid,'spentConfirmed',p.spentConfirmed,'spenderFinal',p.spenderFinal,'spenderSeen',ns.txid IS NOT NULL),'d',json_object('txid',p.txid,'outputIndex',p.outputIndex,'spendingTxid',p.spendingTxid,'lockKind',p.lockKind,'pubA',p.pubA,'pubB',p.pubB,'pubTower',p.pubTower,'payPkhA',p.payPkhA,'payPkhB',p.payPkhB,'rakePkh',p.rakePkh,'stakeA',p.stakeA,'stakeB',p.stakeB,'feeSats',p.feeSats,'covRecoveryHeight',p.recoveryHeight,'potSats',p.potSats,'verdict',p.verdict,'verdictTxid',p.verdictTxid,'settleSigners',p.settleSigners,'spentHeight',p.spentHeight),'m',(SELECT json_group_array(json_object('rn',x.rn,'gameId',x.gameId,'winner',x.winner,'loser',x.loser,'potTxid',x.potTxid,'settleTxid',x.settleTxid,'winnerSigHex',x.winnerSigHex,'loserSigHex',x.loserSigHex,'cardsHex',x.cardsHex,'txid',x.txid,'createdAt',x.createdAt,'claimValid',x.claimValid,'pp',(SELECT json_group_array(json_array(y.createdAt,y.rid,y.txid)) FROM (SELECT q.createdAt,q.rowid AS rid,q.txid FROM proof_markers q WHERE q.gameId=lower(x.gameId) AND q.winner=lower(x.winner) ORDER BY q.createdAt DESC,q.rowid DESC LIMIT 4) y),'po',EXISTS (SELECT 1 FROM proof_posts o WHERE o.bundleValid=1 AND o.gameId=lower(x.gameId) AND o.winner=lower(x.winner)))) FROM (SELECT gameId,winner,loser,potTxid,settleTxid,winnerSigHex,loserSigHex,cardsHex,txid,createdAt,claimValid,ROW_NUMBER() OVER (ORDER BY createdAt ASC,rowid ASC) AS rn FROM result_markers_v2 WHERE potTxid=p.txid ORDER BY createdAt ASC,rowid ASC LIMIT 4) x)) AS v) fr)"
    };
}

/// Insert the counted wins of the pots `$from` and `$key` `$val` select (`$from` names `pot_records p`), each with
/// its frag.
macro_rules! wins_insert {
    ($from:literal, $key:literal, $val:expr) => {
        concat!(
            "INSERT OR IGNORE INTO board_wins (potTxid,potVout,owner,identityIsKey,settleTxid,verdict,pubA,pubB,settleSigners,identityA,identityB,potCreatedAt,frag) SELECT o.potTxid,0,COALESCE(o.winnerIdentity,o.winKey),o.winnerIdentity IS NULL,o.settleTxid,o.verdict,o.pubA,o.pubB,o.settleSigners,o.identityA,o.identityB,o.potCreatedAt,o.frag FROM (SELECT w.potTxid,w.settleTxid,w.verdict,w.pubA,w.pubB,w.settleSigners,w.potCreatedAt,w.frag, CASE WHEN w.pubA=w.pubB THEN NULL ELSE w.slotA END AS identityA, CASE WHEN w.pubA=w.pubB THEN NULL ELSE w.slotB END AS identityB, CASE WHEN w.pubA=w.pubB THEN NULL WHEN w.verdict='winner-a' THEN w.slotA ELSE w.slotB END AS winnerIdentity, CASE WHEN w.verdict='winner-a' THEN w.pubA ELSE w.pubB END AS winKey FROM (SELECT p.txid AS potTxid,lower(p.spendingTxid) AS settleTxid,p.verdict AS verdict,lower(p.pubA) AS pubA,lower(p.pubB) AS pubB,p.settleSigners AS settleSigners,p.createdAt AS potCreatedAt,",
            slot!("pubA"),
            " AS slotA,",
            slot!("pubB"),
            " AS slotB,",
            frag!(),
            " AS frag FROM ",
            $from,
            " LEFT JOIN network_seen ns ON p.spendingTxid IS NOT NULL AND ns.txid=lower(p.spendingTxid) WHERE ",
            counted!(),
            " AND ",
            $key,
            $val,
            ") w) o"
        )
    };
}

/// Refresh the frag of the counted wins `$where` selects (a write to a table the frag reads beside its pot).
macro_rules! refresh_frag {
    ($where:expr) => {
        concat!(
            "UPDATE board_wins SET frag=(SELECT ",
            frag!(),
            " FROM pot_records p LEFT JOIN network_seen ns ON p.spendingTxid IS NOT NULL AND ns.txid=lower(p.spendingTxid) WHERE p.txid=board_wins.potTxid AND p.outputIndex=0) WHERE potVout=0 AND ",
            $where,
            ";"
        )
    };
}

/// The pots holding a result marker keyed `$g`, `$w` (as the app layer binds a proof key: lowercase).
macro_rules! marker_pots {
    ($g:literal, $w:literal) => {
        concat!(
            "potTxid IN (SELECT m.potTxid FROM result_markers_v2 m WHERE lower(m.gameId)=",
            $g,
            " AND lower(m.winner)=",
            $w,
            ")"
        )
    };
}

/// A card's JSON path of the win `$r` (its pot txid, hex: a path the card can name).
macro_rules! card_path {
    ($r:literal) => {
        concat!("'$.\"' || ", $r, ".potTxid || '\"'")
    };
}

/// The win `$r` (a `board_wins` row) as its card entry: the owners query's columns and its frag.
macro_rules! card_entry {
    ($r:literal) => {
        concat!(
            "json_object('owner',",
            $r,
            ".owner,'identityIsKey',",
            $r,
            ".identityIsKey,'potTxid',",
            $r,
            ".potTxid,'settleTxid',",
            $r,
            ".settleTxid,'verdict',",
            $r,
            ".verdict,'pubA',",
            $r,
            ".pubA,'pubB',",
            $r,
            ".pubB,'settleSigners',",
            $r,
            ".settleSigners,'identityA',",
            $r,
            ".identityA,'identityB',",
            $r,
            ".identityB,'potCreatedAt',",
            $r,
            ".potCreatedAt,'f',json(",
            $r,
            ".frag))"
        )
    };
}

/// Pack the win `$r` into its owner's card: only a win keyed by lowercase hex (the txid the app layer binds) with a
/// frag, not already held, and only while the card stays under `CARD_MAX_BYTES` (the CASE keeps `json_type` from
/// ever reading a path it cannot parse).
macro_rules! card_pack {
    ($r:literal) => {
        concat!(
            "UPDATE board_owners SET packed=packed+1,card=json_set(card,",
            card_path!($r),
            ",json(",
            card_entry!($r),
            ")) WHERE owner=",
            $r,
            ".owner AND CASE WHEN ",
            $r,
            ".frag IS NULL OR ",
            $r,
            ".potTxid GLOB '*[^0-9a-f]*' THEN 0 ELSE json_type(card,",
            card_path!($r),
            ") IS NULL AND length(card)+length(",
            card_entry!($r),
            ") <= 1500000 END;"
        )
    };
}

/// Take the win `$r` out of its owner's card (if the card holds it).
macro_rules! card_unpack {
    ($r:literal) => {
        concat!(
            "UPDATE board_owners SET packed=packed-1,card=json_remove(card,",
            card_path!($r),
            ") WHERE owner=",
            $r,
            ".owner AND CASE WHEN ",
            $r,
            ".potTxid GLOB '*[^0-9a-f]*' THEN 0 ELSE json_type(card,",
            card_path!($r),
            ") IS NOT NULL END;"
        )
    };
}

/// Recompute the board row of the pot `$txid` (vout 0); with `$and`, only when that condition holds (an update's
/// OLD pot, recomputed only when the row moved to another pot: recomputing the same pot twice rewrote its owner's
/// card four times, lane W1-LB-674-NOMEM).
macro_rules! recompute_pot {
    ($txid:literal) => {
        recompute_pot!($txid, "")
    };
    ($txid:literal, $and:literal) => {
        concat!(
            "DELETE FROM board_wins WHERE potTxid=",
            $txid,
            " AND potVout=0",
            $and,
            "; ",
            wins_insert!("pot_records p", "p.txid=", concat!($txid, $and)),
            ";"
        )
    };
}

/// One card of `m.cardsHex` (the hex pair at `$hi`, `$lo`), as `sql_hand_score_expr` reads it.
macro_rules! card {
    ($hi:literal, $lo:literal) => {
        concat!(
            "((instr('0123456789abcdef', substr(lower(m.cardsHex), ",
            $hi,
            ", 1)) - 1) * 16 + (instr('0123456789abcdef', substr(lower(m.cardsHex), ",
            $lo,
            ", 1)) - 1))"
        )
    };
}

/// One card's LOW score (A=1, J/Q/K=10, else rank + 2; a byte past 51 scores 0).
macro_rules! card_score {
    ($hi:literal, $lo:literal) => {
        concat!(
            "(CASE WHEN ",
            card!($hi, $lo),
            " > 51 THEN 0 WHEN (",
            card!($hi, $lo),
            " % 13) = 12 THEN 1 WHEN (",
            card!($hi, $lo),
            " % 13) >= 9 THEN 10 ELSE (",
            card!($hi, $lo),
            " % 13) + 2 END)"
        )
    };
}

/// The hand score of `m.cardsHex`, byte-identical to the app layer's `sql_hand_score_expr("m.cardsHex")` (pinned
/// there: `board_derived_sqlite.rs`, `lb674_fold_l1_the_trigger_hand_score_is_the_app_layers`).
macro_rules! hand_score {
    () => {
        concat!(
            card_score!("1", "2"),
            " + ",
            card_score!("3", "4"),
            " + ",
            card_score!("5", "6"),
            " + ",
            card_score!("7", "8"),
            " + ",
            card_score!("9", "10")
        )
    };
}

/// Recompute the hand of the pot `$txid`: its winner's earliest card-bearing verified claim on the recorded
/// settle (`era_hands_sql`'s `rn = 1`), only when the win is owned by an identity.
macro_rules! recompute_hand {
    ($txid:literal) => {
        concat!(
            "DELETE FROM board_hands WHERE potTxid=",
            $txid,
            "; INSERT OR IGNORE INTO board_hands (potTxid,markerRowid,score,claimCreatedAt,gameId) SELECT w.potTxid,m.rowid,",
            hand_score!(),
            ",m.createdAt,m.gameId FROM board_wins w JOIN result_markers_v2 m ON m.potTxid=w.potTxid WHERE w.potTxid=",
            $txid,
            " AND w.potVout=0 AND w.identityIsKey=0 AND lower(m.winner)=w.owner AND lower(m.settleTxid)=w.settleTxid AND m.claimValid IN (1,2) AND m.cardsHex IS NOT NULL AND length(m.cardsHex)=10 ORDER BY m.createdAt ASC,m.rowid ASC LIMIT 1;"
        )
    };
}

/// The SQL hand score of `m.cardsHex` the hands rows carry (pinned equal to the app layer's, with the frag's window
/// constants, by `lb674_fold_l1_the_trigger_hand_score_is_the_app_layers`).
pub const HAND_SCORE_SQL: &str = hand_score!();

/// The result markers a frag holds per pot (`RESULT_ROWS_PER_POT`, the app layer's `pot_markers_sql` window).
pub const FRAG_MARKERS: usize = 4;

/// The proof pointers a frag holds per marker key (the app layer's `PROOF_POINTERS_PER_KEY`, pinned there).
pub const FRAG_POINTERS: usize = 4;

/// The largest frag a `board_wins` row carries; a larger one is NULL and its win is not packed (D1's row is 2 MB).
pub const FRAG_MAX_BYTES: usize = 200_000;

/// The largest card a `board_owners` row grows to; a win that would pass it is not packed (D1's row is 2 MB).
pub const CARD_MAX_BYTES: usize = 1_500_000;

/// One row per counted win (the app layer's owners query's columns) and its frag.
pub const WINS_CREATE: &str = "CREATE TABLE IF NOT EXISTS board_wins (potTxid TEXT NOT NULL, potVout INTEGER NOT NULL, owner TEXT NOT NULL, identityIsKey INTEGER NOT NULL, settleTxid TEXT NOT NULL, verdict TEXT NOT NULL, pubA TEXT NOT NULL, pubB TEXT NOT NULL, settleSigners TEXT, identityA TEXT, identityB TEXT, potCreatedAt INTEGER, frag TEXT, PRIMARY KEY (potTxid, potVout))";

/// The page owners' wins, in the owners query's order (`owner ASC, potCreatedAt DESC, potTxid ASC`).
pub const WINS_OWNER_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_board_wins_owner ON board_wins(owner, potCreatedAt DESC, potTxid)";

/// One row per owner: its counted wins, how many of them are owned by the settle key (`identityIsKey` is
/// `keyWins > 0`, the spine's `MAX`), and its card: `packed` of its wins with their frags, keyed by pot txid.
pub const OWNERS_CREATE: &str = "CREATE TABLE IF NOT EXISTS board_owners (owner TEXT PRIMARY KEY, wins INTEGER NOT NULL, keyWins INTEGER NOT NULL, packed INTEGER NOT NULL DEFAULT 0, card TEXT NOT NULL DEFAULT '{}')";

/// The pots whose marker a proof key names (`lower(gameId)`, as the app layer binds it): the proof triggers' read.
pub const MARKERS_LOWER_GAME_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_result_markers_v2_lower_game ON result_markers_v2(lower(gameId))";

/// The rank: `wins DESC, owner ASC`, the page's order and its keyset.
pub const OWNERS_RANK_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS idx_board_owners_rank ON board_owners(wins DESC, owner ASC)";

/// One row per identity-owned win that has a hand: the marker and its ordering facts.
pub const HANDS_CREATE: &str = "CREATE TABLE IF NOT EXISTS board_hands (potTxid TEXT PRIMARY KEY, markerRowid INTEGER NOT NULL, score INTEGER NOT NULL, claimCreatedAt INTEGER, gameId TEXT NOT NULL)";

/// The era hands' order (`score, createdAt, gameId`).
pub const HANDS_RANK_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS idx_board_hands_rank ON board_hands(score, claimCreatedAt, gameId)";

/// The count of every counted win (the body's `candidates`), one row.
pub const TOTAL_CREATE: &str = "CREATE TABLE IF NOT EXISTS board_total (id INTEGER PRIMARY KEY CHECK (id = 1), wins INTEGER NOT NULL)";

/// The total's row, before any trigger counts into it.
pub const TOTAL_SEED: &str = "INSERT OR IGNORE INTO board_total (id, wins) VALUES (1, 0)";

/// A counted win: its owner's count and card, the total and its hand.
pub const WIN_INSERT: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_win_insert AFTER INSERT ON board_wins BEGIN INSERT INTO board_owners (owner,wins,keyWins) VALUES (NEW.owner,1,NEW.identityIsKey) ON CONFLICT(owner) DO UPDATE SET wins=wins+1,keyWins=keyWins+excluded.keyWins; ",
    card_pack!("NEW"),
    " UPDATE board_total SET wins=wins+1 WHERE id=1; ",
    recompute_hand!("NEW.potTxid"),
    " END"
);

/// A win no longer counted: its owner's count and card (the row gone at 0), the total and its hand.
pub const WIN_DELETE: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_win_delete AFTER DELETE ON board_wins BEGIN UPDATE board_owners SET wins=wins-1,keyWins=keyWins-OLD.identityIsKey WHERE owner=OLD.owner; ",
    card_unpack!("OLD"),
    " DELETE FROM board_owners WHERE owner=OLD.owner AND wins<=0; UPDATE board_total SET wins=wins-1 WHERE id=1; DELETE FROM board_hands WHERE potTxid=OLD.potTxid; END"
);

/// A win's frag refreshed: its card entry moves with it.
pub const WIN_FRAG: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_win_frag AFTER UPDATE OF frag ON board_wins WHEN OLD.frag IS NOT NEW.frag BEGIN ",
    card_unpack!("OLD"),
    " ",
    card_pack!("NEW"),
    " END"
);

/// An admitted (or readmitted) pot.
pub const POT_INSERT: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_pot_insert AFTER INSERT ON pot_records WHEN NEW.outputIndex=0 BEGIN ",
    recompute_pot!("NEW.txid"),
    " END"
);

/// A pot whose verdict, spend, landing, keys, signers or stamp changed (the spend classifier's CAS, the spend
/// record and its release, the relatch). The OLD pot is recomputed only when the row moved to another txid.
pub const POT_UPDATE: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_pot_update AFTER UPDATE OF txid,outputIndex,spent,spendingTxid,createdAt,paramsDecoded,pubA,pubB,verdict,verdictTxid,spentConfirmed,spenderFinal,settleSigners ON pot_records WHEN NEW.outputIndex=0 OR OLD.outputIndex=0 BEGIN ",
    recompute_pot!("OLD.txid", " AND OLD.txid IS NOT NEW.txid"),
    " ",
    recompute_pot!("NEW.txid"),
    " END"
);

/// A pot's frag-only columns changed (the decoded params, the stake, the spend height): its frag.
pub const POT_FRAG: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_pot_frag AFTER UPDATE OF lockKind,pubTower,payPkhA,payPkhB,rakePkh,stakeA,stakeB,feeSats,recoveryHeight,potSats,spentHeight ON pot_records WHEN NEW.outputIndex=0 BEGIN ",
    refresh_frag!("potTxid=NEW.txid"),
    " END"
);

/// An evicted pot.
pub const POT_DELETE: &str = "CREATE TRIGGER IF NOT EXISTS board_pot_delete AFTER DELETE ON pot_records WHEN OLD.outputIndex=0 BEGIN DELETE FROM board_wins WHERE potTxid=OLD.txid AND potVout=0; END";

/// The overlay's own witness of a spender (the third landing arm).
pub const SEEN_INSERT: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_seen_insert AFTER INSERT ON network_seen BEGIN DELETE FROM board_wins WHERE potVout=0 AND potTxid IN (SELECT txid FROM pot_records WHERE lower(spendingTxid)=NEW.txid AND outputIndex=0); ",
    wins_insert!("pot_records p", "lower(p.spendingTxid)=", "NEW.txid"),
    "; END"
);

/// A witness removed (no production writer deletes one; kept so the derivation is total).
pub const SEEN_DELETE: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_seen_delete AFTER DELETE ON network_seen BEGIN DELETE FROM board_wins WHERE potVout=0 AND potTxid IN (SELECT txid FROM pot_records WHERE lower(spendingTxid)=OLD.txid AND outputIndex=0); ",
    wins_insert!("pot_records p", "lower(p.spendingTxid)=", "OLD.txid"),
    "; END"
);

/// A seat marker admitted (or readmitted): the slot resolution.
pub const PARTY_INSERT: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_party_insert AFTER INSERT ON potparty_records WHEN NEW.potVout=0 BEGIN ",
    recompute_pot!("NEW.potTxid"),
    " END"
);

/// A seat marker relatched (`sigValid`) or rewritten. The OLD pot is recomputed only when the seat moved to another
/// pot.
pub const PARTY_UPDATE: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_party_update AFTER UPDATE OF identity,potTxid,potVout,sigValid,seatSettlePubkey ON potparty_records WHEN NEW.potVout=0 OR OLD.potVout=0 BEGIN ",
    recompute_pot!("OLD.potTxid", " AND OLD.potTxid IS NOT NEW.potTxid"),
    " ",
    recompute_pot!("NEW.potTxid"),
    " END"
);

/// A seat marker evicted.
pub const PARTY_DELETE: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_party_delete AFTER DELETE ON potparty_records WHEN OLD.potVout=0 BEGIN ",
    recompute_pot!("OLD.potTxid"),
    " END"
);

/// A result marker admitted (or readmitted): its pot's hand and frag.
pub const MARKER_INSERT: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_marker_insert AFTER INSERT ON result_markers_v2 BEGIN ",
    recompute_hand!("NEW.potTxid"),
    " ",
    refresh_frag!("potTxid=NEW.potTxid"),
    " END"
);

/// A result marker relatched (`claimValid`) or rewritten: the hand and frag of its pot (both, if it moved).
pub const MARKER_UPDATE: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_marker_update AFTER UPDATE OF potTxid,winner,loser,settleTxid,winnerSigHex,loserSigHex,claimValid,cardsHex,txid,createdAt,gameId ON result_markers_v2 BEGIN ",
    recompute_hand!("OLD.potTxid"),
    " ",
    recompute_hand!("NEW.potTxid"),
    " ",
    refresh_frag!("potTxid=OLD.potTxid"),
    " ",
    refresh_frag!("potTxid=NEW.potTxid"),
    " END"
);

/// A result marker evicted.
pub const MARKER_DELETE: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_marker_delete AFTER DELETE ON result_markers_v2 BEGIN ",
    recompute_hand!("OLD.potTxid"),
    " ",
    refresh_frag!("potTxid=OLD.potTxid"),
    " END"
);

/// A proof pointer admitted (or readmitted): the frags of the pots whose markers it decorates.
pub const PROOF_INSERT: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_proof_insert AFTER INSERT ON proof_markers BEGIN ",
    refresh_frag!(marker_pots!("NEW.gameId", "NEW.winner")),
    " END"
);

/// A proof pointer rewritten.
pub const PROOF_UPDATE: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_proof_update AFTER UPDATE OF gameId,winner,txid,createdAt ON proof_markers BEGIN ",
    refresh_frag!(marker_pots!("OLD.gameId", "OLD.winner")),
    " ",
    refresh_frag!(marker_pots!("NEW.gameId", "NEW.winner")),
    " END"
);

/// A proof pointer evicted.
pub const PROOF_DELETE: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_proof_delete AFTER DELETE ON proof_markers BEGIN ",
    refresh_frag!(marker_pots!("OLD.gameId", "OLD.winner")),
    " END"
);

/// A posted bundle written (the app layer's `INSERT OR REPLACE`: the insert recomputes from the source).
pub const POST_INSERT: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_post_insert AFTER INSERT ON proof_posts BEGIN ",
    refresh_frag!(marker_pots!("NEW.gameId", "NEW.winner")),
    " END"
);

/// A posted bundle's verdict or key rewritten.
pub const POST_UPDATE: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_post_update AFTER UPDATE OF gameId,winner,bundleValid ON proof_posts BEGIN ",
    refresh_frag!(marker_pots!("OLD.gameId", "OLD.winner")),
    " ",
    refresh_frag!(marker_pots!("NEW.gameId", "NEW.winner")),
    " END"
);

/// A posted bundle removed.
pub const POST_DELETE: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_post_delete AFTER DELETE ON proof_posts BEGIN ",
    refresh_frag!(marker_pots!("OLD.gameId", "OLD.winner")),
    " END"
);

/// The one-time backfill (`board-v1`): every counted win, through the `board_wins` triggers (owners, total,
/// hands). The guard sits in the OUTER derived table, so a stamped replay steps no fleet row.
pub const WINS_BACKFILL: &str = wins_insert!(
    "(SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM view_derivation_state WHERE name='board-v1')) g CROSS JOIN pot_records p",
    "1",
    ""
);

/// Record the backfill as done; the app layer serves the tables only once this row exists.
pub const STATE_DONE: &str =
    "INSERT OR IGNORE INTO view_derivation_state (name) VALUES ('board-v1')";

/// The stamp's name.
pub const STATE_NAME: &str = "board-v1";

// bsv-low #674, the cutoff (lane W2-LB-674-CUTOFF): the board counts only the wins of the era. The era is the app
// layer's `WRITTEN_OFF_BEFORE_MS` (the #375 write-off; beta and prod both set one), so the board was derived over
// every win and read by nothing under a cutoff: the route took #642's candidate read. The cutoff now lives INSIDE
// the derivation: `board_era` holds the cutoff the tables are derived under (NULL: none), `board_win_era` refuses a
// `board_wins` row before it (the era filter's own predicate, `p.createdAt * 1000 >= cutoff`, a NULL stamp outside
// the era), so every trigger that derives a win counts only the era's, and `board_era_update` re-derives the whole
// board in the same statement that moves the cutoff (the backfill re-run behind the stamp, `board_era` the stamp).
// The app layer writes its own cutoff there on its scheduled pass (its `BOARD_ERA_SET_SQL`, `BOARD_ERA_CLEAR_SQL`) and serves the
// tables only when the stamped cutoff is the one it reads. No shipped trigger is touched: the guard is a NEW trigger
// on the derived table, so an old worker's re-run of its own list (`CREATE TRIGGER IF NOT EXISTS`) changes nothing.

/// The era predicate over a `board_wins` row's `NEW.potCreatedAt` (seconds), the era filter's (`era_filter_sql`):
/// in the era when no cutoff is stamped, or when the stamp times 1,000 is at or after the cutoff (a NULL stamp is
/// not).
macro_rules! in_era {
    () => {
        "((SELECT e.cutoffMs FROM board_era e WHERE e.id=1) IS NULL OR (NEW.potCreatedAt*1000 >= (SELECT e.cutoffMs FROM board_era e WHERE e.id=1)) IS 1)"
    };
}

/// The whole board re-derived from the source under the stamped cutoff: the derived rows cleared (the owners first,
/// so the per-win delete trigger finds no card to rewrite), then every counted win inserted through `board_wins`'
/// own triggers, `board_win_era` refusing the rows before the cutoff.
macro_rules! rederive {
    () => {
        concat!(
            "DELETE FROM board_owners; DELETE FROM board_wins; DELETE FROM board_hands; UPDATE board_total SET wins=0 WHERE id=1; ",
            wins_insert!("pot_records p", "1", ""),
            ";"
        )
    };
}

/// The cutoff the board is derived under, one row (`cutoffMs` in ms since epoch, NULL: no cutoff).
pub const ERA_CREATE: &str =
    "CREATE TABLE IF NOT EXISTS board_era (id INTEGER PRIMARY KEY CHECK (id = 1), cutoffMs INTEGER)";

/// A win before the stamped cutoff is not a counted win of the board: its row is refused (the statement goes on
/// with its next row).
pub const WIN_ERA: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_win_era BEFORE INSERT ON board_wins WHEN NOT ",
    in_era!(),
    " BEGIN SELECT RAISE(IGNORE); END"
);

/// A moved cutoff re-derives the board in the same statement.
pub const ERA_UPDATE: &str = concat!(
    "CREATE TRIGGER IF NOT EXISTS board_era_update AFTER UPDATE OF cutoffMs ON board_era WHEN OLD.cutoffMs IS NOT NEW.cutoffMs BEGIN ",
    rederive!(),
    " END"
);

/// The row, no cutoff: the tables as the `board-v1` backfill and the triggers derived them. Written after both
/// triggers, so a reader that finds the row finds them.
pub const ERA_SEED: &str = "INSERT OR IGNORE INTO board_era (id, cutoffMs) VALUES (1, NULL)";

#[cfg(test)]
mod tests {
    /// wrangler 4.54's `splitSqlIntoStatements` (the local `wrangler d1 execute --file` of `ci-d1-board` and the
    /// route tiers), line for line: a frame is pushed on `\s(BEGIN|CASE)\s` and popped on `\sEND[;\s]`, and a `;`
    /// ends a statement only with no frame open. A `,CASE` pushes nothing and its `END` pops the trigger's BEGIN.
    fn split_wrangler_454(sql: &str) -> Vec<String> {
        let starts = |s: &str| {
            let u = s.to_ascii_uppercase();
            [" BEGIN", " CASE"].iter().any(|k| {
                u.len() > k.len()
                    && u.ends_with(|c: char| c.is_whitespace())
                    && u[..u.len() - 1].ends_with(&k[1..])
                    && u[..u.len() - 1 - (k.len() - 1)].ends_with(|c: char| c.is_whitespace())
            })
        };
        let ends = |s: &str| {
            s.len() > 4
                && s.ends_with(|c: char| c == ';' || c.is_whitespace())
                && s[..s.len() - 1].ends_with("END")
                && s[..s.len() - 4].ends_with(|c: char| c.is_whitespace())
        };
        let mut out = Vec::new();
        let mut cur = String::new();
        let mut frames = 0usize;
        let mut it = sql.chars().peekable();
        while let Some(c) = it.next() {
            if frames > 0 && ends(&format!("{cur}{c}")) {
                frames -= 1;
            }
            match c {
                '\'' | '"' | '`' => {
                    cur.push(c);
                    for d in it.by_ref() {
                        cur.push(d);
                        if d == c {
                            break;
                        }
                    }
                }
                ';' if frames == 0 => out.push(std::mem::take(&mut cur)),
                _ => cur.push(c),
            }
            if starts(&cur) {
                frames += 1;
            }
        }
        out.push(cur);
        out.into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// wrangler 4.148's splitter (the fleet's pinned deploy wrangler, `docs/MIGRATION-EMITTER.md`) is SQLite's own
    /// `sqlite3_complete` state machine: a statement ends at the first `;` that completes it.
    #[allow(unsafe_code)]
    fn split_sqlite_complete(sql: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut start = 0;
        for (i, _) in sql.match_indices(';') {
            let candidate = &sql[start..=i];
            let c = std::ffi::CString::new(candidate).unwrap();
            // SAFETY: a NUL-terminated string that outlives the call.
            if unsafe { rusqlite::ffi::sqlite3_complete(c.as_ptr()) } != 0 {
                out.push(sql[start..i].trim().to_string());
                start = i + 1;
            }
        }
        out
    }

    /// bsv-low #674: the emitted migration file (`part_body`, every statement `;\n`-terminated) splits back into
    /// exactly the list under both wranglers, so each trigger reaches D1 whole. RED on this lane's first text,
    /// whose `,CASE` popped the trigger's BEGIN frame under 4.54 (`ci-d1-board`: "incomplete input").
    #[test]
    fn lb674_every_trigger_survives_both_wrangler_splitters() {
        let list = crate::d1::OVERLAY_MIGRATIONS;
        let file = crate::migration_emitter::part_body(list);
        let want: Vec<String> = list.iter().map(|s| s.trim().to_string()).collect();
        assert_eq!(
            split_sqlite_complete(&file),
            want,
            "wrangler 4.148 (sqlite3_complete)"
        );
        let got = split_wrangler_454(&file);
        for (i, (g, w)) in got.iter().zip(&want).enumerate() {
            assert_eq!(g, w, "wrangler 4.54: statement {i} split differently");
        }
        assert_eq!(got.len(), want.len(), "wrangler 4.54");
        // The model is the splitter: the shape it refuses is refused.
        let bad = "CREATE TRIGGER t AFTER INSERT ON a BEGIN SELECT 1,CASE WHEN 1 THEN 2 END AS x; SELECT 2; END;\n";
        assert_eq!(
            split_wrangler_454(bad).len(),
            3,
            "a ,CASE splits its trigger"
        );
        assert_eq!(split_sqlite_complete(bad).len(), 1);
    }

    #[test]
    fn owed893_every_dirty_trigger_survives_both_splitters_and_comma_case_is_red() {
        let dirty: Vec<_> = crate::d1::OVERLAY_MIGRATIONS
            .iter()
            .copied()
            .filter(|s| s.starts_with("CREATE TRIGGER IF NOT EXISTS owed_dirty_"))
            .collect();
        assert_eq!(dirty.len(), 28);
        for sql in dirty {
            let file = crate::migration_emitter::part_body(&[sql]);
            assert_eq!(split_wrangler_454(&file), [sql], "{sql}");
            assert_eq!(split_sqlite_complete(&file), [sql], "{sql}");
            let bad = sql.replacen(",s.gameId", ",CASE WHEN 1 THEN s.gameId END ", 1);
            assert_ne!(bad, sql, "the mutant must change this trigger");
            let file = crate::migration_emitter::part_body(&[&bad]);
            let bad = [bad];
            assert_ne!(
                split_wrangler_454(&file).as_slice(),
                bad.as_slice(),
                "comma-CASE must split the trigger"
            );
            assert_eq!(
                split_sqlite_complete(&file).as_slice(),
                bad.as_slice(),
                "SQLite alone accepts the unsafe splitter text"
            );
        }
    }

    /// The #674 block, in the list's order.
    fn block() -> &'static [&'static str] {
        let list = crate::d1::OVERLAY_MIGRATIONS;
        let start = list.iter().position(|s| *s == super::WINS_CREATE).unwrap();
        let end = list.iter().position(|s| *s == super::STATE_DONE).unwrap();
        &list[start..=end]
    }

    /// bsv-low #674, the migration certification: the block is appended (the list's tail), every statement is
    /// additive and benign on a re-run (`IF NOT EXISTS`, `INSERT OR IGNORE`, the backfill guarded by the stamp it
    /// precedes), no table is dropped or renamed, each statement is under D1's 100 KB statement bound, the tables
    /// and the total's row precede the triggers that write them, the `board_wins` triggers precede the source
    /// triggers that fire them, and the stamp the app layer gates on is the block's last statement.
    #[test]
    fn lb674_the_block_is_appended_benign_and_ordered() {
        let list = crate::d1::OVERLAY_MIGRATIONS;
        let b = block();
        assert_eq!(b.len(), 32);
        assert_eq!(list.len(), crate::d1::OVERLAY_MIGRATION_COUNT);
        assert_eq!(
            list.iter().position(|s| *s == super::WINS_CREATE),
            Some(295),
            "appended after migration 295"
        );
        for sql in b {
            assert!(crate::d1::migration_statement_is_additive(sql), "{sql}");
            let u = sql.to_ascii_uppercase();
            assert!(
                u.starts_with("CREATE TABLE IF NOT EXISTS")
                    || u.starts_with("CREATE INDEX IF NOT EXISTS")
                    || u.starts_with("CREATE TRIGGER IF NOT EXISTS")
                    || u.starts_with("INSERT OR IGNORE"),
                "{sql}"
            );
            assert!(!u.contains("DROP ") && !u.contains("RENAME"), "{sql}");
            assert!(sql.len() < 100_000, "{} bytes", sql.len());
        }
        let at = |s: &str| b.iter().position(|x| *x == s).unwrap();
        assert!(at(super::TOTAL_SEED) < at(super::WIN_INSERT));
        assert!(at(super::MARKERS_LOWER_GAME_INDEX) < at(super::PROOF_INSERT));
        for source in [
            super::POT_INSERT,
            super::POT_UPDATE,
            super::POT_FRAG,
            super::POT_DELETE,
            super::SEEN_INSERT,
            super::SEEN_DELETE,
            super::PARTY_INSERT,
            super::PARTY_UPDATE,
            super::PARTY_DELETE,
            super::MARKER_INSERT,
            super::MARKER_UPDATE,
            super::MARKER_DELETE,
            super::PROOF_INSERT,
            super::PROOF_UPDATE,
            super::PROOF_DELETE,
            super::POST_INSERT,
            super::POST_UPDATE,
            super::POST_DELETE,
        ] {
            assert!(
                at(super::WIN_INSERT) < at(source)
                    && at(super::WIN_DELETE) < at(source)
                    && at(super::WIN_FRAG) < at(source)
            );
            assert!(
                at(source) < at(super::WINS_BACKFILL),
                "the triggers exist before the backfill"
            );
        }
        assert_eq!(b[b.len() - 2], super::WINS_BACKFILL);
        assert_eq!(b[b.len() - 1], super::STATE_DONE);
        assert!(super::WINS_BACKFILL.contains(&format!(
            "FROM (SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM view_derivation_state WHERE name='{}')) g CROSS JOIN",
            super::STATE_NAME
        )));
        assert!(super::STATE_DONE.contains(super::STATE_NAME));
    }

    /// bsv-low #674, the cutoff (lane W2-LB-674-CUTOFF): the era block follows the shipped block, every statement
    /// additive and benign on a re-run, the table before the triggers and the row after both (a reader that finds the
    /// row finds the guard and the re-derivation), no DROP anywhere; and the 32 shipped statements are byte for byte
    /// main's `b444fc7` (an edit in place would run nowhere: `CREATE TRIGGER IF NOT EXISTS` keeps the old body).
    #[test]
    fn lb674_cut_the_era_block_is_appended_benign_and_ordered_and_the_shipped_block_is_untouched() {
        let list = crate::d1::OVERLAY_MIGRATIONS;
        let at = |s: &str| list.iter().position(|x| *x == s).unwrap();
        let shipped_done = at(super::STATE_DONE);
        // PRE557's three migrations (328-330) sit between the shipped block and the era block since the rebase.
        let done = at(super::ERA_CREATE) - 1;
        assert!(done > shipped_done);
        assert_eq!(
            [
                at(super::ERA_CREATE),
                at(super::WIN_ERA),
                at(super::ERA_UPDATE),
                at(super::ERA_SEED)
            ],
            [done + 1, done + 2, done + 3, done + 4]
        );
        for sql in &list[done + 1..=done + 4] {
            assert!(crate::d1::migration_statement_is_additive(sql), "{sql}");
            let u = sql.to_ascii_uppercase();
            assert!(
                u.starts_with("CREATE TABLE IF NOT EXISTS")
                    || u.starts_with("CREATE TRIGGER IF NOT EXISTS")
                    || u.starts_with("INSERT OR IGNORE"),
                "{sql}"
            );
            assert!(!u.contains("DROP ") && !u.contains("RENAME"), "{sql}");
            assert!(sql.len() < 100_000, "{} bytes", sql.len());
        }
        assert!(
            super::WIN_ERA.contains("BEFORE INSERT ON board_wins")
                && super::WIN_ERA.contains("RAISE(IGNORE)")
        );
        assert!(super::ERA_UPDATE
            .contains(" WHEN OLD.cutoffMs IS NOT NEW.cutoffMs BEGIN DELETE FROM board_owners;"));
        let fnv = |sqls: &[&str]| {
            sqls.iter().fold(0u32, |acc, sql| {
                let h = sql.as_bytes().iter().fold(0x811c_9dc5u32, |h, b| {
                    (h ^ u32::from(*b)).wrapping_mul(0x0100_0193)
                });
                acc.wrapping_add(h)
            })
        };
        assert_eq!(block().len(), 32);
        assert_eq!(
            fnv(block()),
            SHIPPED_BLOCK_FNV,
            "a shipped #674 statement was edited in place"
        );
    }

    /// The #674 block's fingerprint at main `b444fc7` (beta runs it since 2026-10-18 08:42Z).
    const SHIPPED_BLOCK_FNV: u32 = 1_455_068_236;

    /// No production writer REPLACEs a row of a table the board's triggers derive from (#638's rule, widened to
    /// #674's sources): a REPLACE fires no delete trigger. The triggers recompute their pot from the source, so a
    /// REPLACE of a pot or a seat still leaves the right row (the drift pin runs one); a REPLACE of a claim that
    /// moved it to another pot would leave the old pot's hand behind.
    #[test]
    fn lb674_no_production_writer_replaces_a_board_source_row() {
        fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            for e in std::fs::read_dir(dir).unwrap() {
                let p = e.unwrap().path();
                if p.is_dir() {
                    walk(&p, out);
                } else if p.extension().is_some_and(|x| x == "rs") {
                    out.push(p);
                }
            }
        }
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let mut files = Vec::new();
        for c in [
            "overlay-cloudflare",
            "low-app-layer",
            "overlay-discovery",
            "overlay-engine",
        ] {
            walk(&root.join(c).join("src"), &mut files);
        }
        assert!(files.len() > 50, "the sources were read: {}", files.len());
        let mut hits = Vec::new();
        for f in &files {
            let text = std::fs::read_to_string(f).unwrap();
            // Production code only: a file's tests module is its tail.
            let prod = text.split("#[cfg(test)]\nmod tests").next().unwrap();
            let flat = prod
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .to_ascii_uppercase();
            for table in [
                "POT_RECORDS",
                "POTPARTY_RECORDS",
                "HOPPARTY_RECORDS",
                "RESULT_MARKERS_V2",
                "NETWORK_SEEN",
            ] {
                if flat.contains(&format!("REPLACE INTO {table} "))
                    || flat.contains(&format!("REPLACE INTO {table}("))
                {
                    hits.push(format!("{}: REPLACE INTO {table}", f.display()));
                }
            }
        }
        assert!(hits.is_empty(), "{hits:?}");
    }
}
