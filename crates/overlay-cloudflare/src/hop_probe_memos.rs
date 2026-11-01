//! bsv-low #484 (the 2026-09-19 gate's M1, hardening): THE INVALIDATION OF THE HOP PROBE MEMOS.
//!
//! The app layer memoises the couriers' spend probes in `hop_chain_probes` (migration 150; one row per outpoint,
//! the last KNOWN answer and when it was read). A memo that recorded a CONFIRMED spend answers the owed walk for
//! two hours (`low-app-layer` `PROBE_MEMO_CONFIRMED_MAX_AGE_MS`), and `swept_home` reads the same word: a swept
//! hop's Payout row is `claimable` on it. Nothing invalidated the table: no TTL, no DELETE, and neither reorg
//! judge touched it. After a reorg orphaned a mined hop sweep the owed list said "ready to collect" until the
//! memo aged out: a wrong word and a failing press (the press needs a landing proof: no credit), never lost sats.
//!
//! Two rules, both run by the overlay (the reorg judge, on the shared `OVERLAY_DB`):
//!
//! 1. **A reorg clears every CONFIRMED memo.** The memo carries no height (the couriers' probe answers
//!    spent / spender / confirmed, never a block), and the sweep it names is usually a transaction the index does
//!    not hold (the chain rung is asked exactly BECAUSE the index says unspent), so no reorg pass can match a memo
//!    to the rows it judges. What a pass does know is that a reorg HAPPENED: it re-judged a row (a demotion, a
//!    re-anchor, a refuted proof), an announce named a reorg range, or an Arcade orphan event was applied. On that
//!    evidence every memo with `spentConfirmed = 1` is deleted (the superset of the memos at the orphaned
//!    heights) and their readers ask the couriers again (the owed walk is retired, bsv-low #893: the belt asks,
//!    eight outpoints a tick). A pass that scanned and left every row
//!    standing is NOT evidence: the routine sweep runs every block, and clearing on it would unmake the memo.
//!    An unconfirmed spend and an unspent word keep their own five minutes. The belt's EVIDENCE of a spend
//!    (`<txid>:<vout>`, bsv-low #953) is not deleted: its confirmation is taken (`spentConfirmed` NULL) and the
//!    spend it records stands until a belt look says otherwise (bsv-low #1027, [`HOP_PROBE_MEMO_REORG_DEMOTE_SQL`]).
//! 2. **The TTL the contract states.** No reader honours a memo older than the confirmed window, so a row past
//!    it is dead weight: every block-event pass deletes them ([`HOP_PROBE_MEMO_TTL_MS`], pinned equal to the app
//!    layer's longest window from its tests, which link this crate).
//!
//! 3. **The reorg GRACE (the lens's M2, 2026-10-06).** A reorg pass that demotes a pot also pushes pot-changed
//!    for it (bsv-low #523), so the app layer recomputes within seconds of the clear and re-probes couriers
//!    that may not have seen the reorg yet; they answer "confirmed" and the memo was re-written for two more
//!    hours: rule 1 undone by the push. So the clear leaves a TOMBSTONE in the same table (one reserved row,
//!    [`HOP_PROBE_MEMO_REORG_MARK`], `probedAtMs` = the time of the last clear), and the owed walk gives a
//!    confirmed memo read before `clear + HOP_PROBE_MEMO_REORG_GRACE_MS` the short window every other word has
//!    (five minutes) instead of the two hours (`low-app-layer` `split_probe_targets_after_reorg`): inside the
//!    grace the walk keeps asking; a memo read after it is trusted as before. The tombstone is written BEFORE
//!    the delete and carries the clear's REACH (bsv-low #1042): a confirmed word taken before it is held to the
//!    grace only where the reach covers its height (the delete missed it: a fault, or a find that read the couriers
//!    before the clear and wrote after it); one below the reach is the clear's own verdict and keeps its
//!    confirmation (`low-app-layer` `hops_view::clear_unless_survived`). Before #1042 it was judged without a lower
//!    bound, which since #1027 round 2 refused the confirmations the reach-scoped clear had kept. The TTL
//!    sweep never deletes it (a memo read in the grace's last minute is still inside its two hours when the
//!    tombstone would age out).
//!
//! 4. **The table holds two kinds of row that are not memos (bsv-low #485, the merged lens's MEDIUM-1,
//!    2026-10-07).** The app layer's home latch and home-walk cursor live here under their own key prefixes
//!    ([`HOP_PROBE_MEMO_APP_LAYER_PREFIXES`]). Rules 1 and 2 spare them by prefix: before, the clear deleted a
//!    latch written from a confirmed probe and the TTL sweep deleted every latch and the cursor two hours on, so
//!    a retired courier-proven row came back and was bought again from the couriers. They are not swept by
//!    anyone: one latch per retired home output and one cursor per identity, for the table's life.
//!
//! Residual, stated with the read rule's numbers (the delta lens's D-M1, 2026-10-06): the word a recompute
//! reads FRESH from a courier is served by that recompute as it always was. A courier behind the reorg says
//! "confirmed" and the row says "ready to collect"; inside the grace that payout is an OPEN row for the owed
//! read's rule (`low-app-layer` `hops_view::mark_reorg_grace_rows`, `owed::row_is_open`), so its list is
//! re-derived after five minutes, on the read that finds it that old (it was a closed row: fifteen minutes),
//! and that walk asks again. A memo that answers a walk inside its short window re-serves the word, so one
//! lagging answer can stand for under ten minutes plus that read in the worst interleaving (a walk at the
//! memo's fifth minute, its list re-derived five minutes later), five plus the read in the plain one. A hop
//! past the walk's probe budget keeps its last memo's word, named stale, EXCEPT a confirmed memo read inside
//! the grace, whose confirmation is not served (`hops_view::stale_memo_word`: the row reads "not mined yet").
//! Always a wrong word and a failing press, never sats. The memo's age rides the row
//! (`facts.chainProbeAgeMs`).
use worker::wasm_bindgen::JsValue;
use worker::{console_log, D1Database};

use crate::reorg_sweep::{ProofLegSummary, ReorgSweepSummary, ReverifyPassSummary};

/// Rule 1's predicate (one spelling for the clear, its marks and its framed writes). Bind `?1`: the REACH of the reorg,
/// the lowest height the pass can have unmade ([`ReorgReach`], 0 for the whole table; every caller hands the first
/// ORPHANED height, not the common ancestor). bsv-low #1027 round 2 (MEDIUM-1): a row whose spend was confirmed below
/// the reach stands, because no reorg from the reach unmakes a block below it. A memo's and the belt's evidence's
/// height (`tipHeight`) is chaintracks' VERIFIED header tip when the look was taken (`low-app-layer` `owed_belt`
/// `probe_find(.., db.tip())`), and the confirmation is a courier's word, so the spend's block is NOT at or below
/// `tipHeight`: the verified tip trails the couriers by a block while headers verify, more while chaintracks
/// re-validates (the ENGINE-TIP-896 section of CLAUDE.md), and a find of a spend in block B can carry `tipHeight`
/// B - 1. Round 3 (MEDIUM-1): the evidence arm therefore allows for that lag, `tipHeight + lag >= ?1` with the lag
/// [`REORG_TIP_LAG_BLOCKS`], in the predicate and not at the callers (one spelling, and a caller's `floor - lag` would lower the latch
/// arm too). A row with no `tipHeight` (the one-time seed's `spentConfirmed` unknown, an older writer) is taken as
/// before. A proof latch (`proofmined:<spender>.<height>`) carries its PROOF'S OWN checked block height in its key
/// (the latch writer keys by the proof's `height`, never a tip), so it is compared with the reach EXACTLY, no lag.
macro_rules! clear_pred {
    () => {
        concat!(
            "spentConfirmed = 1 AND outpoint NOT LIKE 'homeproof:%' AND outpoint NOT LIKE 'homecursor:%' AND (CASE WHEN outpoint LIKE 'proofmined:%' THEN CAST(substr(outpoint,77) AS INTEGER) >= ?1 ELSE (tipHeight IS NULL OR tipHeight + ",
            reorg_tip_lag!(),
            " >= ?1) END)"
        )
    };
}

/// The lag's literal inside the predicate's text, pinned equal to [`REORG_TIP_LAG_BLOCKS`].
macro_rules! reorg_tip_lag {
    () => {
        "6"
    };
}

/// bsv-low #1027 round 3 (MEDIUM-1): the blocks the VERIFIED header tip a look records (`tipHeight`) may trail the
/// block the courier calls its spend confirmed in, allowed for by the clear's evidence arm (`clear_pred!`). Six, the
/// app layer's `owed::LATCH_RECHECK_DEPTH`: it covers the one block the verified tip trails while headers verify and a
/// short re-validation. The residual, stated: a look taken while the verified tip trailed the spend's block by MORE
/// than six, AND a reorg reaching exactly those blocks, keeps a stale confirmation (a press withheld, never a press
/// made) until a deeper clear reaches its row. The cost: each clear also moves the rows of the looks taken in the six
/// blocks below its floor (a routine sweep's clear reaches three blocks plus six; bounded by the belt's 8 finds a 120 s
/// tick, about 40 finds a block).
pub const REORG_TIP_LAG_BLOCKS: u64 = 6;

/// Rule 2's predicate (one spelling for the sweep, its marks and its framed delete).
macro_rules! expire_pred {
    () => {
        "probedAtMs <= ? AND outpoint <> 'reorg-clear.0' AND outpoint NOT LIKE 'homeproof:%' AND outpoint NOT LIKE 'homecursor:%' AND outpoint NOT LIKE 'proofmined:%' AND NOT (substr(outpoint,65,1)=':' AND EXISTS (SELECT 1 FROM pot_records p WHERE p.txid=substr(hop_chain_probes.outpoint,1,64) AND p.outputIndex=CAST(substr(hop_chain_probes.outpoint,66) AS INTEGER)))"
    };
}

/// FIX-1010 (bsv-low #1010; the D2-ACTOR-F lens's LOW-1): the subjects of the probe rows `g` holds, marked under the
/// pair (`probe`, the row's key) in the writer's own batch: a hop memo's or evidence's outpoint (`<txid>.<vout>`,
/// `<txid>:<vout>`) names the hop's markers, the sweeps filed for it, its released evidence and the parties of a pot it
/// is; a proof latch (`proofmined:<spender>.<height>`) names the owed rows whose word roles name its spender. A
/// confirmed word deleted is a downgrade (a payout rested on it). Not a trigger: the app layer's judge writes this table
/// itself (its latches, the belt's memos and evidence, in the judgement's own batch), and a trigger would mark the
/// judge's own output. One scan of the table (`g`, materialized), then index probes; at most 5 compound terms (D1).
macro_rules! probe_marks {
    ($pred:expr) => {
        probe_marks!($pred, "(g.spentConfirmed IS 1)")
    };
    ($pred:expr, $down:expr) => {
        concat!(
            "WITH g AS MATERIALIZED (SELECT outpoint,spentConfirmed FROM hop_chain_probes WHERE ", $pred, ") ",
            "INSERT INTO owed_dirty (identity,gameId,source,sourceKey,markedAtMs,downgrade) SELECT s.identity,s.gameId,'probe',s.k,strftime('%s','now') * 1000,s.c FROM (",
            "SELECT hp.identity AS identity,hp.gameId AS gameId,g.outpoint AS k,", $down, " AS c FROM g CROSS JOIN hopparty_records hp INDEXED BY idx_hopparty_hop ON hp.txid=substr(g.outpoint,1,64) AND hp.hopVout=CAST(substr(g.outpoint,66) AS INTEGER) WHERE substr(g.outpoint,65,1) IN ('.',':') ",
            "UNION ALL SELECT f.identity,f.gameId,g.outpoint,", $down, " FROM g CROSS JOIN hopsweep_records f INDEXED BY idx_hopsweep_hop ON f.hopTxid=substr(g.outpoint,1,64) AND f.hopVout=CAST(substr(g.outpoint,66) AS INTEGER) WHERE substr(g.outpoint,65,1) IN ('.',':') ",
            "UNION ALL SELECT e.identity,e.gameId,g.outpoint,", $down, " FROM g CROSS JOIN released_hop_evidence e ON e.hopTxid=substr(g.outpoint,1,64) AND e.hopVout=CAST(substr(g.outpoint,66) AS INTEGER) WHERE substr(g.outpoint,65,1) IN ('.',':') ",
            "UNION ALL SELECT pp.identity,pp.gameId,g.outpoint,", $down, " FROM g CROSS JOIN potparty_records pp INDEXED BY idx_potparty_pot ON pp.potTxid=substr(g.outpoint,1,64) AND pp.potVout=CAST(substr(g.outpoint,66) AS INTEGER) WHERE substr(g.outpoint,65,1) IN ('.',':') ",
            "UNION ALL SELECT r.identity,r.gameId,g.outpoint,", $down, " FROM g CROSS JOIN owed_row_words w INDEXED BY idx_owed_row_words_txid ON w.txid=substr(g.outpoint,12,64) CROSS JOIN owed_rows r ON r.identity=w.identity AND r.outpoint=w.outpoint WHERE g.outpoint LIKE 'proofmined:%'",
            ") s WHERE 1 ON CONFLICT(identity,gameId,source,sourceKey) DO UPDATE SET generation=excluded.generation,source=excluded.source,sourceKey=excluded.sourceKey,markedAtMs=excluded.markedAtMs,downgrade=MAX(owed_dirty.downgrade,excluded.downgrade)"
        )
    };
}

/// The longest window any reader passes for a memo (the app layer's `PROBE_MEMO_CONFIRMED_MAX_AGE_MS`): two hours.
pub const HOP_PROBE_MEMO_TTL_MS: i64 = 2 * 60 * 60_000;

/// The rows of `hop_chain_probes` that are NOT probe memos (bsv-low #485, the merged lens's MEDIUM-1): the app
/// layer keeps its home latch (`homeproof:<txid>.<vout>`, the home key's signature verified over a spender's
/// bytes: a proof, never a chain word, so no reorg unmakes it and no window ages it) and its home-walk cursor
/// (`homecursor:<identity>.0`, a position in a ring) in the same table, under these key prefixes
/// (`low-app-layer` `owed::HOME_SPEND_LATCH_PREFIX`, `owed::HOME_WALK_CURSOR_PREFIX`, pinned equal from its
/// tests, which link this crate). Neither rule below touches them, nor counts them.
pub const HOP_PROBE_MEMO_APP_LAYER_PREFIXES: [&str; 2] = ["homeproof:", "homecursor:"];

/// The app layer's rows that a REORG demotes and no window ages (bsv-low #693 fold, the W1-OWED-693 lens's HIGH-1):
/// its proof latch (`proofmined:<spenderTxid>.<height>`, a chaintracks-checked mined proof of a spender: the height
/// and the canonical root, `spentConfirmed = 1`; `low-app-layer` `owed::PROOF_MINED_LATCH_PREFIX`, pinned equal).
/// A mined proof is a CHAIN word, so rule 1 deletes it like any confirmed memo (it is not spared); rule 2 spares it,
/// so the latch outlives the two-hour window and the row it confirms stops flapping across passes.
pub const HOP_PROBE_MEMO_REORG_DEMOTED_PREFIXES: [&str; 1] = ["proofmined:"];

/// Rule 1: every memo that recorded a CONFIRMED spend, never the app layer's own rows (a latch written from a
/// confirmed probe carries `spentConfirmed = 1` too), and never the belt's EVIDENCE of a spend (`<txid>:<vout>`,
/// [`HOP_PROBE_MEMO_REORG_DEMOTE_SQL`] demotes it instead: bsv-low #1027).
pub const HOP_PROBE_MEMO_REORG_CLEAR_SQL: &str = concat!(
    "DELETE FROM hop_chain_probes WHERE ",
    clear_pred!(),
    " AND substr(outpoint,65,1)<>':'"
);

/// bsv-low #1027 (lane FIX-1027-BELT-FIND-RECORD): A REORG MAKES A PROOF STALE, NEVER ABSENT (the tracker charter's
/// rule, its word `proof_stale`): it unmakes a confirmation, not the find of a spend, so deleting the row was a trust
/// error of the overlay's (a stale word read as no word) and this demotion is the rule, not a patch. The belt's
/// evidence (`<txid>:<vout>`, `low-app-layer` `hops_view::probe_evidence_key`) is the record's only word that a
/// courier saw the hop spent by a transaction the index does not hold. Rule 1 deleted it with the memos, so after any
/// reorg pass the D1 judge read the hop by the index alone (`index-unspent`, the sweep press) over a spend the chain
/// holds, until a belt look found it again; and with the D1 judge off nothing dates that look, so the record lost the
/// spend for good (beta, 2026-10-31 14:01:48Z, the cron's clear of 101 confirmed rows on 99e456a6, before FIX-1010
/// marked the clear: 65 outpoints of six identities, the fleet compare's diffs; their actors, never told, kept the
/// spend they had judged). Now the
/// clear keeps the evidence and takes only its confirmation (`spentConfirmed` NULL, unknown): both judges read the hop
/// spent and not confirmed (`unbound`, no press), and the belt asks the OUTPOINT for it at its backoff
/// (`owed_belt::rests_on_unconfirmed_find`): a look that finds it confirmed again restores the word, one that finds it
/// unspent deletes the row (`PROBE_EVIDENCE_CLEAR_SQL`) and the press returns. Answers the keys it demoted (each
/// noted `probe`, its mark written by [`HOP_PROBE_MEMO_REORG_CLEAR_MARK_SQL`] in the same batch).
pub const HOP_PROBE_MEMO_REORG_DEMOTE_SQL: &str = concat!(
    "UPDATE hop_chain_probes SET spentConfirmed=NULL WHERE ",
    clear_pred!(),
    " AND substr(outpoint,65,1)=':' RETURNING outpoint"
);

/// bsv-low #1027: rule 1's framed writes in the batch's order after the marks, as [`clear_on_reorg`] runs them (the
/// memos deleted, then the belt's evidence demoted); each answers its keys.
pub const HOP_PROBE_MEMO_REORG_CLEAR_FRAMED: [&str; 2] = [
    HOP_PROBE_MEMO_REORG_CLEAR_FRAMED_SQL,
    HOP_PROBE_MEMO_REORG_DEMOTE_SQL,
];

/// FIX-1010: rule 1's marks, the first statement of the clear's batch (the memos it deletes and the evidence it
/// demotes: a confirmation withdrawn is a downgrade either way).
pub const HOP_PROBE_MEMO_REORG_CLEAR_MARK_SQL: &str = probe_marks!(clear_pred!());
/// FIX-1010: rule 1's delete as the batch runs it, answering the keys it deleted (each noted `probe`, so its event
/// reaches the actor).
pub const HOP_PROBE_MEMO_REORG_CLEAR_FRAMED_SQL: &str = concat!(
    "DELETE FROM hop_chain_probes WHERE ",
    clear_pred!(),
    " AND substr(outpoint,65,1)<>':'",
    " RETURNING outpoint"
);

/// Rule 2: every memo no reader honours (bind: `now - HOP_PROBE_MEMO_TTL_MS`; the reader refuses
/// `age >= window`, so the row read exactly at the bound goes too: the lens's N2), never the reorg tombstone,
/// never the app layer's own rows (its proof latch included: [`HOP_PROBE_MEMO_REORG_DEMOTED_PREFIXES`]).
/// bsv-low #953 (D2-ACTOR-B-FOLD-9): never the belt's EVIDENCE of a spend (`<txid>:<vout>`, a colon where a memo
/// has a dot; `low-app-layer` `hops_view::probe_evidence_key`) while the outpoint's index row stands: evidence, not a
/// memo, so its age is no reason to forget it; one whose index row is gone (evicted) ages out as a memo does.
pub const HOP_PROBE_MEMO_EXPIRE_SQL: &str =
    concat!("DELETE FROM hop_chain_probes WHERE ", expire_pred!());

/// FIX-1010: rule 2's marks: the EVIDENCE a reader still honours (`<txid>:<vout>`, whose index row is gone: an evicted
/// hop's; a confirmed one a downgrade, as before), and since bsv-low #1042 every hop MEMO (`<txid>.<vout>`) whose delete
/// moves a judgement. FIX-1010 read a memo past the window as read by no judge; the judge reads every memo of its hops
/// whatever its age (`low-app-layer` `hops_view::stale_memo_word`: an old unspent memo is `unspent-stale`, or the
/// pending chain-wait of a word taken below the tip), so its delete changes the row with no event and the actor's row
/// went stale behind the D1 judge's. A memo the belt's evidence repeats word for word (the same find: spent, spender,
/// confirmation, height and stamp) moves nothing (the judge reads the evidence in its place) and is not marked. A
/// memo's mark is PLAIN: a memo leaving reads no confirmation away that the evidence does not keep.
pub const HOP_PROBE_MEMO_EXPIRE_MARK_SQL: &str = probe_marks!(
    concat!(
        expire_pred!(),
        " AND (substr(outpoint,65,1)=':' OR (substr(outpoint,65,1)='.' AND NOT EXISTS (SELECT 1 FROM hop_chain_probes e WHERE e.outpoint=substr(hop_chain_probes.outpoint,1,64)||':'||substr(hop_chain_probes.outpoint,66) AND e.spent IS hop_chain_probes.spent AND e.spendingTxid IS hop_chain_probes.spendingTxid AND e.spentConfirmed IS hop_chain_probes.spentConfirmed AND e.tipHeight IS hop_chain_probes.tipHeight AND e.probedAtMs IS hop_chain_probes.probedAtMs)))"
    ),
    "(substr(g.outpoint,65,1)=':' AND g.spentConfirmed IS 1)"
);
/// FIX-1010: rule 2's delete as the batch runs it, answering the keys it deleted.
pub const HOP_PROBE_MEMO_EXPIRE_FRAMED_SQL: &str = concat!(
    "DELETE FROM hop_chain_probes WHERE ",
    expire_pred!(),
    " RETURNING outpoint"
);

/// FIX-1010: the deleted keys whose pair the writer notes (each may hold a mark its batch wrote): the clear's every
/// hop memo, evidence and proof latch; the sweep's evidence and hop memos (bsv-low #1042: its marks name both). A key
/// noted with no mark (a memo the evidence repeats) is read by the publish and not sent.
pub fn noted_key(expire: bool, outpoint: &str) -> bool {
    match outpoint.as_bytes().get(64) {
        Some(b':' | b'.') => true,
        _ => !expire && outpoint.starts_with("proofmined:"),
    }
}

/// Rule 3: the tombstone's key in `hop_chain_probes.outpoint`. It has the memo key's shape (`<txid>.<vout>`)
/// with a txid no transaction has, so the app layer reads it in the same `IN` read as the memos it judges (one
/// read: the memos and the tombstone arrive together or not at all) and no outpoint can collide with it.
pub const HOP_PROBE_MEMO_REORG_MARK: &str = "reorg-clear.0";

/// Rule 3: how long after a clear a CONFIRMED memo is refused the long window. Thirty minutes: three blocks on
/// average, and well past the couriers' own lag behind a reorg (their indexers follow the tip within a block).
pub const HOP_PROBE_MEMO_REORG_GRACE_MS: i64 = 30 * 60_000;

/// Rule 3: stamp the tombstone (binds: now, ms; the clear's REACH, its floor). `spent = 0` and no confirmed word: the
/// clear above never deletes it, and a reader that met it as a memo would read "unspent", the safe word. The stamp only
/// grows, and the reach is the newest stamp's (`tipHeight`, written as an integer: D1 binds a JS number as a REAL).
/// bsv-low #1042: the app layer judges a word taken before the stamp and below the reach (`clear_pred!`'s complement)
/// as one the clear left standing (`low-app-layer` `hops_view::clear_unless_survived`); with no reach on the row (a
/// tombstone stamped before #1042) it holds every word taken before the stamp to the grace, as before.
pub const HOP_PROBE_MEMO_REORG_MARK_SQL: &str = "INSERT INTO hop_chain_probes (outpoint, probedAtMs, spent, spendingTxid, spentConfirmed, tipHeight) VALUES ('reorg-clear.0', ?1, 0, NULL, NULL, CAST(?2 AS INTEGER)) \
     ON CONFLICT(outpoint) DO UPDATE SET tipHeight = CASE WHEN excluded.probedAtMs >= hop_chain_probes.probedAtMs THEN excluded.tipHeight ELSE hop_chain_probes.tipHeight END, probedAtMs = MAX(hop_chain_probes.probedAtMs, excluded.probedAtMs)";

/// Confirmed memos a reorg pass cleared (served on `/health/invariants.arcadeReorg.probeMemosCleared` and with
/// the counters). Round 2's NIT-1: the lifetime row MIXES two meanings across the FIX-1027 deploy: before it the clear
/// deleted the belt's evidence too and counted it here; after it this counts memos and latches only and the evidence
/// is [`COUNTER_HOP_PROBE_EVIDENCE_DEMOTED`]. The row is not reset, so a drop in its rate at the deploy is a change of
/// meaning, not of reorgs.
pub const COUNTER_HOP_PROBE_MEMOS_CLEARED: &str = "hop_probe_memos_cleared_by_reorg_total";
/// bsv-low #1027 (the review's LOW-1): the belt's evidence rows a reorg pass DEMOTED (kept, `spentConfirmed` NULL),
/// counted apart from the memos it deleted so `probeMemosCleared` keeps its meaning (served beside it on
/// `/health/invariants.arcadeReorg.probeEvidenceDemoted`, seeded at 0).
pub const COUNTER_HOP_PROBE_EVIDENCE_DEMOTED: &str = "hop_probe_evidence_demoted_by_reorg_total";
/// Memos the TTL sweep deleted.
pub const COUNTER_HOP_PROBE_MEMOS_EXPIRED: &str = "hop_probe_memos_expired_total";

/// Memo reads of the APP LAYER that faulted (the merged lens's LOW-3): it writes this row, the overlay seeds it at
/// 0 and serves it on `/health/invariants.counters` with the rest (`low-app-layer`
/// `owed::COUNTER_PROBE_MEMO_READ_FAULTS`, pinned equal from its tests). A faulted read is an empty answer for
/// that pass: no memo, no reorg tombstone (so no grace mark), no latch and no cursor.
pub const COUNTER_APPLAYER_PROBE_MEMO_READ_FAULTS: &str = "applayer_probe_memo_read_faults_total";

/// PURE: the spenders leg re-judged a row (demoted it, blind or on a refuted proof, or moved its anchor).
pub fn reverify_rejudged(s: &ReverifyPassSummary) -> bool {
    s.stale + s.demoted_blind + s.reanchored + s.reanchored_from_courier > 0
}

/// PURE: a proof leg (the pots' own proofs, the engine's hop proofs) found a refuted proof.
pub fn proof_leg_rejudged(s: &ProofLegSummary) -> bool {
    s.stale > 0
}

/// PURE: the routine R2 sweep re-judged something on any of its three legs.
pub fn sweep_rejudged(s: &ReorgSweepSummary) -> bool {
    reverify_rejudged(&s.spenders)
        || proof_leg_rejudged(&s.pot_beefs)
        || proof_leg_rejudged(&s.transactions)
}

/// PURE: the D7 Arcade pass has reorg evidence: an orphan event applied (chaintracks corroborated it, whether
/// or not a row of ours sat at its height), an event released unresolved (a reorg we could not judge: the
/// conservative side), or any row re-judged on the way. Never an event chaintracks refused to corroborate, nor
/// one still held.
pub fn arcade_pass_rejudged(s: &crate::arcade_reorg::ArcadePassSummary) -> bool {
    s.applied + s.skipped_unresolved + s.released_by_operator > 0
        || reverify_rejudged(&s.spenders)
        || proof_leg_rejudged(&s.pot_beefs)
        || proof_leg_rejudged(&s.transactions)
}

/// PURE (bsv-low #620): the chain-event feed's pass has reorg evidence, D7's rule over the header source's own word: a
/// selecting event (a reorg, a fork, a split, an invalidation) finished with its window judged, one released
/// unresolved (the conservative side), or any row re-judged on the way. A pass of tips and tip ages alone is none.
pub fn feed_pass_rejudged(s: &crate::chain_feed::FeedPassSummary) -> bool {
    s.applied + s.released > 0
        || reverify_rejudged(&s.spenders)
        || proof_leg_rejudged(&s.pot_beefs)
        || proof_leg_rejudged(&s.transactions)
}

async fn delete(db: &D1Database, sql: &str, binds: &[JsValue]) -> Result<u64, String> {
    let stmt = db.prepare(sql);
    let stmt = if binds.is_empty() {
        stmt
    } else {
        stmt.bind(binds).map_err(|e| e.to_string())?
    };
    let res = crate::d1_ledger::Counted::counted_run(&stmt)
        .await
        .map_err(|e| e.to_string())?;
    Ok(res
        .meta()
        .ok()
        .flatten()
        .and_then(|m| m.changes)
        .unwrap_or(0) as u64)
}

#[derive(serde::Deserialize)]
struct Deleted {
    outpoint: String,
}

/// FIX-1010: one batch, the marks first then the framed writes that answer their keys (D1 runs a batch as one
/// transaction: a written row's subjects are marked or nothing is written); each key a mark may name is noted `probe`
/// for the invocation's publish (its event reaches the actor by the direct call, the queue behind it). The framed
/// writes take the same binds (the clear's: none; the sweep's: its bound). Returns the rows each framed write answered.
async fn marked_delete(
    db: &D1Database,
    mark_sql: &str,
    framed: &[&str],
    binds: &[JsValue],
    expire: bool,
) -> Result<Vec<u64>, String> {
    let prep = |sql: &str| -> Result<worker::D1PreparedStatement, String> {
        let stmt = db.prepare(sql);
        if binds.is_empty() {
            Ok(stmt)
        } else {
            stmt.bind(binds).map_err(|e| e.to_string())
        }
    };
    let mut stmts = vec![prep(mark_sql)?];
    for sql in framed {
        stmts.push(prep(sql)?);
    }
    let out = crate::d1_ledger::counted_batch(db, stmts)
        .await
        .map_err(|e| e.to_string())?;
    let mut counts = Vec::with_capacity(framed.len());
    for n in 1..=framed.len() {
        let written: Vec<Deleted> = out
            .get(n)
            .ok_or("a framed write answered nothing")?
            .results()
            .map_err(|e| e.to_string())?;
        for d in &written {
            if noted_key(expire, &d.outpoint) {
                crate::owed_events::note(PROBE_SOURCE, &d.outpoint);
            }
        }
        counts.push(written.len() as u64);
    }
    Ok(counts)
}

/// FIX-1010: the `owed_dirty` source of a probe row's marks (`overlay_discovery::owed_event::TRIGGER_SOURCES`).
pub const PROBE_SOURCE: &str = "probe";

/// bsv-low #1027 round 2 (MEDIUM-1): the floor a reorg pass hands the clear, the lowest height it can have unmade
/// (`None`: no evidence, no write). Each pass names its own: the operator's `fromHeight`, the block event's
/// `reorgFrom` and the sweep's windows ([`sweep_reach`]), the chain feed's and D7's lowest window judged
/// (`FeedPassSummary::reach_floor`, `ArcadePassSummary::reach_floor`). A pass that re-judged and names no height
/// hands 0 (the whole table, the rule before this fold: never a confirmation kept on a guess). Since the fold a clear
/// demotes only what its reorg can have unmade: on beta's figures (334 evidence rows, 11 clears on 2026-10-31) every
/// clear demoted every confirmed row, about 3,670 outpoint asks a day against the belt's 5,760; a routine sweep's
/// clear now reaches three blocks.
pub type ReorgReach = Option<u64>;

/// PURE (bsv-low #1027 round 2): the reach of a block-event pass's evidence: the announced `reorgFrom`, and the lowest
/// window the routine sweep walked when it re-judged (every leg's `lo`; with no window recorded, `tip - depth`).
pub fn sweep_reach(
    reorg_from: Option<u64>,
    demotion_rejudged: bool,
    sweep: &ReorgSweepSummary,
    tip: u64,
    depth: u64,
) -> ReorgReach {
    let swept = sweep_rejudged(sweep).then(|| {
        [
            sweep.spenders_window,
            sweep.pot_beefs_window,
            sweep.transactions_window,
        ]
        .into_iter()
        .flatten()
        .map(|(lo, _)| lo)
        .min()
        .unwrap_or_else(|| tip.saturating_sub(depth))
    });
    let announced = reorg_from.or_else(|| demotion_rejudged.then_some(0));
    match (announced, swept) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

/// Rules 1 and 3, run by a reorg pass with its outcome in hand: no evidence (`reach` `None`), no write; else the rows
/// confirmed at or above the reach (`clear_pred!`). Logged and counted; a fault is logged and costs the memo's own
/// window, nothing else. Returns the rows the clear moved: the memos and latches DELETED plus the belt's evidence rows
/// DEMOTED (the review's NIT-2; no caller reads it).
///
/// The race, stated (round 2's NOTE-2): the batch is one D1 transaction, so a belt find whose courier look was taken
/// before the reorg and whose write lands after this batch stands confirmed, as on the base (which deleted only rows
/// already written); it is bounded by one look (the belt writes at most 8 finds a 120 s tick, each from a look made in
/// that tick) and healed by the next reorg evidence that reaches its height. The app layer reads the belt's evidence
/// under the tombstone as it reads a memo (bsv-low #1042): a word taken before the stamp and below its reach stands.
pub async fn clear_on_reorg(db: &D1Database, origin: &str, reach: ReorgReach) -> u64 {
    let Some(floor) = reach else {
        return 0;
    };
    // rule 3: the tombstone FIRST (a memo the delete misses is then held to the short window by its stamp)
    let now_ms = worker::Date::now().as_millis() as f64;
    if let Err(e) = delete(
        db,
        HOP_PROBE_MEMO_REORG_MARK_SQL,
        &[JsValue::from_f64(now_ms), JsValue::from_f64(floor as f64)],
    )
    .await
    {
        console_log!("[hop-probe-memos] ({origin}) reorg tombstone write FAILED (a memo re-written right after this clear keeps the long window): {e}");
    }
    match marked_delete(
        db,
        HOP_PROBE_MEMO_REORG_CLEAR_MARK_SQL,
        &HOP_PROBE_MEMO_REORG_CLEAR_FRAMED,
        &[JsValue::from_f64(floor as f64)],
        false,
    )
    .await
    {
        Ok(counts) => {
            let at = |n: usize| counts.get(n).copied().unwrap_or(0);
            let (deleted, demoted) = (at(0), at(1));
            // the review's LOW-2, stated: each demoted row is dated due now by both judges and the belt asks its
            // OUTPOINT (8 a 120 s tick, 24 calls), owners' and strangers' spends alike: a reorg that unmade a
            // stranger's spend gives the owner's press back only through that look, so none is deferred; round 2's
            // MEDIUM-1: only rows at or above the reach are demoted, so `demoted` is the belt's load from this clear
            console_log!("[hop-probe-memos] ({origin}) reorg evidence from height {floor}: cleared {deleted} confirmed memo(s), demoted {demoted} belt evidence row(s) to unconfirmed (bsv-low #1027); the belt re-asks the demoted outpoints, 8 a tick, and the memos' readers ask again; rows confirmed below {floor} stand");
            crate::ops::bump_counter(db, COUNTER_HOP_PROBE_MEMOS_CLEARED, deleted).await;
            crate::ops::bump_counter(db, COUNTER_HOP_PROBE_EVIDENCE_DEMOTED, demoted).await;
            deleted + demoted
        }
        Err(e) => {
            console_log!("[hop-probe-memos] ({origin}) reorg clear FAILED (the confirmed memos stand for their window): {e}");
            0
        }
    }
}

/// Rule 2, on every block-event pass. Returns the memos deleted.
pub async fn expire(db: &D1Database, now_ms: i64) -> u64 {
    let bound = (now_ms - HOP_PROBE_MEMO_TTL_MS) as f64;
    match marked_delete(
        db,
        HOP_PROBE_MEMO_EXPIRE_MARK_SQL,
        &[HOP_PROBE_MEMO_EXPIRE_FRAMED_SQL],
        &[JsValue::from_f64(bound)],
        true,
    )
    .await
    {
        Ok(counts) => {
            let expired = counts.first().copied().unwrap_or(0);
            if expired > 0 {
                console_log!("[hop-probe-memos] ttl: deleted {expired} memo(s) older than {HOP_PROBE_MEMO_TTL_MS} ms");
                crate::ops::bump_counter(db, COUNTER_HOP_PROBE_MEMOS_EXPIRED, expired).await;
            }
            expired
        }
        Err(e) => {
            console_log!("[hop-probe-memos] ttl sweep failed: {e}");
            0
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        for sql in crate::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                let msg = e.to_string().to_ascii_lowercase();
                assert!(
                    msg.contains("duplicate column"),
                    "production migration failed under real SQLite: {e}\n{sql}"
                );
            }
        }
        conn
    }

    fn plant(
        conn: &rusqlite::Connection,
        outpoint: &str,
        at_ms: i64,
        spent: bool,
        confirmed: Option<bool>,
    ) {
        conn.execute(
            "INSERT INTO hop_chain_probes (outpoint, probedAtMs, spent, spendingTxid, spentConfirmed) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![outpoint, at_ms, spent, spent.then(|| "e5".repeat(32)), confirmed],
        )
        .unwrap();
    }

    /// The clear's batch as [`clear_on_reorg`] runs it, under one transaction: the marks, then
    /// [`HOP_PROBE_MEMO_REORG_CLEAR_FRAMED`] in order, every statement bound to the reach `floor`; each framed write's keys.
    fn clear_batch(conn: &rusqlite::Connection, floor: i64) -> Vec<Vec<String>> {
        let tx = conn.unchecked_transaction().unwrap();
        tx.execute(HOP_PROBE_MEMO_REORG_CLEAR_MARK_SQL, [floor])
            .unwrap();
        let out = HOP_PROBE_MEMO_REORG_CLEAR_FRAMED
            .iter()
            .map(|sql| {
                let mut q = tx.prepare(sql).unwrap();
                let keys: Vec<String> = q
                    .query_map([floor], |r| r.get(0))
                    .unwrap()
                    .map(Result::unwrap)
                    .collect();
                keys
            })
            .collect();
        tx.commit().unwrap();
        out
    }

    fn held(conn: &rusqlite::Connection) -> Vec<String> {
        let mut stmt = conn
            .prepare("SELECT outpoint FROM hop_chain_probes ORDER BY outpoint")
            .unwrap();
        let rows = stmt.query_map([], |r| r.get::<_, String>(0)).unwrap();
        rows.map(|r| r.unwrap()).collect()
    }

    /// bsv-low #484, the clearing SQL over the SHIPPED schema (real SQLite): a reorg clears every memo that
    /// recorded a CONFIRMED spend (the only word a reorg can make wrong for longer than five minutes, and the
    /// one `swept_home` turns into `claimable`); an unconfirmed spend and an unspent word keep their five
    /// minutes. To red: narrow the WHERE off `spentConfirmed = 1`, or drop the DELETE.
    #[test]
    fn a_reorg_clears_every_confirmed_memo_and_leaves_the_rest_real_sqlite() {
        let conn = db();
        plant(&conn, "a.0", 1_000, true, Some(true));
        plant(&conn, "b.0", 1_000, true, Some(false));
        plant(&conn, "c.0", 1_000, true, None);
        plant(&conn, "d.0", 1_000, false, None);
        plant(&conn, "e.1", 9_000, true, Some(true));
        let cleared = conn.execute(HOP_PROBE_MEMO_REORG_CLEAR_SQL, [0]).unwrap();
        assert_eq!(cleared, 2, "both confirmed memos, whatever their age");
        assert_eq!(held(&conn), vec!["b.0", "c.0", "d.0"]);
        assert_eq!(
            conn.execute(HOP_PROBE_MEMO_REORG_CLEAR_SQL, [0]).unwrap(),
            0,
            "idempotent"
        );
    }

    /// bsv-low #484, the TTL the memo's contract states: no reader honours a memo older than the confirmed
    /// window (two hours), so the row is dead weight past it and the sweep deletes it. To red: drop the DELETE
    /// or move the bound.
    #[test]
    fn the_ttl_sweep_deletes_exactly_the_memos_no_reader_honours_real_sqlite() {
        assert_eq!(
            HOP_PROBE_MEMO_TTL_MS,
            2 * 60 * 60_000,
            "the longest window a reader passes (the confirmed one)"
        );
        let conn = db();
        let now: i64 = 1_800_000_000_000;
        plant(
            &conn,
            "old.0",
            now - HOP_PROBE_MEMO_TTL_MS - 1,
            true,
            Some(true),
        );
        plant(
            &conn,
            "edge.0",
            now - HOP_PROBE_MEMO_TTL_MS,
            true,
            Some(true),
        );
        plant(&conn, "young.0", now - 60_000, false, None);
        plant(&conn, "ancient.0", 5, false, None);
        // the reorg tombstone is not a memo: the sweep never takes it, however old (rule 3)
        conn.execute(HOP_PROBE_MEMO_REORG_MARK_SQL, rusqlite::params![7, 0])
            .unwrap();
        let expired = conn
            .execute(
                HOP_PROBE_MEMO_EXPIRE_SQL,
                rusqlite::params![now - HOP_PROBE_MEMO_TTL_MS],
            )
            .unwrap();
        assert_eq!(expired, 3, "the row read exactly at the bound is one no reader honours (`age >= window`): the lens's N2");
        assert_eq!(held(&conn), vec![HOP_PROBE_MEMO_REORG_MARK, "young.0"]);
    }

    /// bsv-low #485 (merge fold, the merged lens's MEDIUM-1), over the SHIPPED schema (real SQLite): neither
    /// DELETE takes a row under the app layer's prefixes, whatever its age and whatever its `spentConfirmed`
    /// (a latch written from a confirmed probe carries 1), and neither counts one; the memos beside them go as
    /// before. The app layer's own pin writes these rows through its shipped statements and reads the latch back
    /// (`low-app-layer` `owed`, `the_overlays_reorg_clear_and_ttl_sweep_leave_the_home_latch_...`).
    /// To red: drop a prefix from either DELETE.
    #[test]
    fn neither_delete_takes_the_app_layers_latch_or_cursor_rows_real_sqlite() {
        let conn = db();
        let now: i64 = 1_800_000_000_000;
        let latch = format!(
            "{}{}.0",
            HOP_PROBE_MEMO_APP_LAYER_PREFIXES[0],
            "ab".repeat(32)
        );
        let unconfirmed_latch = format!(
            "{}{}.1",
            HOP_PROBE_MEMO_APP_LAYER_PREFIXES[0],
            "cd".repeat(32)
        );
        let cursor = format!(
            "{}02{}.0",
            HOP_PROBE_MEMO_APP_LAYER_PREFIXES[1],
            "aa".repeat(32)
        );
        plant(&conn, &latch, 5, true, Some(true));
        plant(&conn, &unconfirmed_latch, 5, true, None);
        plant(&conn, &cursor, 5, false, None);
        plant(&conn, "a.0", 5, true, Some(true));
        plant(&conn, "b.0", 5, true, None);
        assert_eq!(
            conn.execute(HOP_PROBE_MEMO_REORG_CLEAR_SQL, [0]).unwrap(),
            1,
            "the confirmed memo, not the confirmed latch"
        );
        assert_eq!(
            conn.execute(HOP_PROBE_MEMO_EXPIRE_SQL, rusqlite::params![now])
                .unwrap(),
            1,
            "the memo past the window, not the rows beside it"
        );
        let mut left = vec![cursor, latch, unconfirmed_latch];
        left.sort();
        assert_eq!(held(&conn), left);
        for sql in [HOP_PROBE_MEMO_REORG_CLEAR_SQL, HOP_PROBE_MEMO_EXPIRE_SQL] {
            for prefix in HOP_PROBE_MEMO_APP_LAYER_PREFIXES {
                assert!(
                    sql.contains(&format!("outpoint NOT LIKE '{prefix}%'")),
                    "{sql}"
                );
                assert!(!prefix.contains(['%', '_']), "no LIKE wildcard in a prefix");
            }
        }
        assert!(!HOP_PROBE_MEMO_APP_LAYER_PREFIXES
            .iter()
            .any(|p| HOP_PROBE_MEMO_REORG_MARK.starts_with(p)));
    }

    /// bsv-low #693 fold (HIGH-1), real SQLite: the app layer's proof latch is a CHAIN word, so the reorg clear takes
    /// it (it is not spared) and the TTL sweep does not (it outlives the window). To red: spare the prefix in the
    /// clear, or drop it from the sweep.
    #[test]
    fn the_reorg_clear_takes_the_proof_latch_and_the_ttl_sweep_spares_it_real_sqlite() {
        let conn = db();
        let latch = format!(
            "{}{}.900000",
            HOP_PROBE_MEMO_REORG_DEMOTED_PREFIXES[0],
            "ab".repeat(32)
        );
        plant(&conn, &latch, 5, true, Some(true));
        plant(&conn, "a.0", 5, true, None);
        assert_eq!(
            conn.execute(HOP_PROBE_MEMO_EXPIRE_SQL, rusqlite::params![i64::MAX])
                .unwrap(),
            1,
            "the memo past the window, not the latch"
        );
        assert_eq!(held(&conn), vec![latch]);
        assert_eq!(
            conn.execute(HOP_PROBE_MEMO_REORG_CLEAR_SQL, [0]).unwrap(),
            1,
            "a reorg demotes the latch"
        );
        assert!(held(&conn).is_empty());
        for prefix in HOP_PROBE_MEMO_REORG_DEMOTED_PREFIXES {
            assert!(HOP_PROBE_MEMO_EXPIRE_SQL.contains(&format!("outpoint NOT LIKE '{prefix}%'")));
            // round 2: the clear reads the latch's height from its key, never spares the prefix
            assert!(!HOP_PROBE_MEMO_REORG_CLEAR_SQL.contains(&format!("NOT LIKE '{prefix}%'")));
            assert!(!prefix.contains(['%', '_']), "no LIKE wildcard in a prefix");
        }
    }

    /// bsv-low #484 (lens fold, M2), the tombstone over the SHIPPED schema (real SQLite): the clear stamps one
    /// reserved row, the stamp only grows, the reorg clear and the TTL sweep both leave it, and its key is the
    /// one the SQL strings carry. To red: let either DELETE take it, or let an older stamp overwrite a newer.
    #[test]
    fn the_reorg_tombstone_is_one_row_that_only_grows_and_no_sweep_deletes_real_sqlite() {
        let conn = db();
        let stamp = |conn: &rusqlite::Connection| -> i64 {
            conn.query_row(
                "SELECT probedAtMs FROM hop_chain_probes WHERE outpoint = ?1",
                [HOP_PROBE_MEMO_REORG_MARK],
                |r| r.get(0),
            )
            .unwrap()
        };
        plant(&conn, "a.0", 1_000, true, Some(true));
        conn.execute(HOP_PROBE_MEMO_REORG_MARK_SQL, rusqlite::params![5_000, 0])
            .unwrap();
        assert_eq!(
            conn.execute(HOP_PROBE_MEMO_REORG_CLEAR_SQL, [0]).unwrap(),
            1,
            "the confirmed memo, not the tombstone"
        );
        assert_eq!(held(&conn), vec![HOP_PROBE_MEMO_REORG_MARK]);
        assert_eq!(stamp(&conn), 5_000);
        conn.execute(HOP_PROBE_MEMO_REORG_MARK_SQL, rusqlite::params![9_000, 0])
            .unwrap();
        assert_eq!(stamp(&conn), 9_000, "a later clear moves it");
        conn.execute(HOP_PROBE_MEMO_REORG_MARK_SQL, rusqlite::params![6_000, 0])
            .unwrap();
        assert_eq!(
            stamp(&conn),
            9_000,
            "an earlier stamp (two isolates racing) never moves it back"
        );
        assert_eq!(held(&conn).len(), 1, "one row");
        assert_eq!(
            conn.execute(HOP_PROBE_MEMO_EXPIRE_SQL, rusqlite::params![i64::MAX])
                .unwrap(),
            0,
            "the TTL sweep never takes it"
        );
        for sql in [HOP_PROBE_MEMO_REORG_MARK_SQL, HOP_PROBE_MEMO_EXPIRE_SQL] {
            assert!(
                sql.contains(&format!("'{HOP_PROBE_MEMO_REORG_MARK}'")),
                "{sql}"
            );
        }
        const {
            assert!(
                HOP_PROBE_MEMO_REORG_GRACE_MS < HOP_PROBE_MEMO_TTL_MS
                    && HOP_PROBE_MEMO_REORG_GRACE_MS > 5 * 60_000,
                "longer than the short window, shorter than the long one"
            )
        };
        // the clear writes the tombstone before it deletes
        let src = include_str!("hop_probe_memos.rs");
        let src = &src[..src.find("#[cfg(test)]").unwrap()];
        let body = &src[src.find("pub async fn clear_on_reorg(").unwrap()..];
        // FIX-1010: the clear runs as its batch (the marks, then the framed delete), the same predicate as the clear
        assert!(
            body.find("HOP_PROBE_MEMO_REORG_MARK_SQL").unwrap()
                < body.find("HOP_PROBE_MEMO_REORG_CLEAR_MARK_SQL").unwrap()
        );
        assert_eq!(
            HOP_PROBE_MEMO_REORG_CLEAR_FRAMED_SQL,
            format!("{HOP_PROBE_MEMO_REORG_CLEAR_SQL} RETURNING outpoint")
        );
        assert_eq!(
            HOP_PROBE_MEMO_EXPIRE_FRAMED_SQL,
            format!("{HOP_PROBE_MEMO_EXPIRE_SQL} RETURNING outpoint")
        );
    }

    /// bsv-low #484: what counts as a reorg pass having RE-JUDGED something. A pass that scanned and left every
    /// row standing is not evidence (the routine sweep runs every block: clearing on it would unmake the memo);
    /// a demotion, a re-anchor or a refuted proof is; so is an Arcade orphan event applied or released
    /// unresolved (never one chaintracks refused to corroborate, nor one still held).
    #[test]
    fn only_a_pass_that_rejudged_something_is_reorg_evidence() {
        let quiet = ReverifyPassSummary {
            scanned: 40,
            standing: 40,
            stored_from_courier: 2,
            ..Default::default()
        };
        assert!(!reverify_rejudged(&quiet));
        for s in [
            ReverifyPassSummary {
                stale: 1,
                ..Default::default()
            },
            ReverifyPassSummary {
                demoted_blind: 1,
                ..Default::default()
            },
            ReverifyPassSummary {
                reanchored: 1,
                ..Default::default()
            },
            ReverifyPassSummary {
                reanchored_from_courier: 1,
                ..Default::default()
            },
        ] {
            assert!(reverify_rejudged(&s), "{s:?}");
        }
        assert!(!proof_leg_rejudged(&ProofLegSummary {
            scanned: 9,
            standing: 9,
            ..Default::default()
        }));
        assert!(proof_leg_rejudged(&ProofLegSummary {
            stale: 1,
            ..Default::default()
        }));
        let mut sweep = ReorgSweepSummary::default();
        assert!(!sweep_rejudged(&sweep));
        sweep.transactions.stale = 1;
        assert!(sweep_rejudged(&sweep), "the hop proofs' leg counts");
        let mut sweep = ReorgSweepSummary::default();
        sweep.pot_beefs.stale = 1;
        assert!(sweep_rejudged(&sweep));
        let mut sweep = ReorgSweepSummary::default();
        sweep.spenders.reanchored = 1;
        assert!(sweep_rejudged(&sweep));
        use crate::arcade_reorg::ArcadePassSummary;
        assert!(!arcade_pass_rejudged(&ArcadePassSummary {
            idle: true,
            ..Default::default()
        }));
        assert!(!arcade_pass_rejudged(&ArcadePassSummary {
            skipped_uncorroborated: 1,
            held: 1,
            tracker_lagging: 1,
            ..Default::default()
        }));
        assert!(
            arcade_pass_rejudged(&ArcadePassSummary {
                applied: 1,
                ..Default::default()
            }),
            "an orphan event applied, even with no row of ours at its height"
        );
        assert!(arcade_pass_rejudged(&ArcadePassSummary {
            skipped_unresolved: 1,
            ..Default::default()
        }));
        assert!(arcade_pass_rejudged(&ArcadePassSummary {
            released_by_operator: 1,
            ..Default::default()
        }));
        let mut partial = ArcadePassSummary::default();
        partial.spenders.stale = 1;
        assert!(
            arcade_pass_rejudged(&partial),
            "a row demoted by an event still pending"
        );
    }

    /// `/health/invariants.arcadeReorg` counts the memos cleared (the issue's acceptance): the lifetime counter
    /// rides the view, 0 before the first clear; and the invariants body is built through it.
    #[test]
    fn the_reorg_view_counts_the_memos_cleared() {
        let view = serde_json::json!({ "readable": true, "everRan": true });
        let none = crate::ops::with_probe_memos_cleared(view.clone(), &serde_json::json!({}));
        assert_eq!(none["probeMemosCleared"], 0);
        let some = crate::ops::with_probe_memos_cleared(
            view,
            &serde_json::json!({ COUNTER_HOP_PROBE_MEMOS_CLEARED: 7 }),
        );
        assert_eq!(some["probeMemosCleared"], 7);
        assert_eq!(some["everRan"], true, "the view's own keys stand");
        let ops = include_str!("ops.rs");
        // bsv-low #838: the view is read in the invariants' one join, then merged with the counters
        assert!(ops.contains("arcade_reorg = arcade_reorg_view(db),"));
        assert!(
            ops.contains("let arcade_reorg = with_probe_memos_cleared(arcade_reorg, &counters);")
        );
        // the merged lens's LOW-3: the app layer's faulted memo reads are a seeded row of the same surface
        assert!(
            ops.contains("crate::hop_probe_memos::COUNTER_APPLAYER_PROBE_MEMO_READ_FAULTS,"),
            "seeded at 0 in `read_counters`"
        );
        assert_eq!(
            COUNTER_APPLAYER_PROBE_MEMO_READ_FAULTS,
            "applayer_probe_memo_read_faults_total"
        );
    }

    /// bsv-low #1027, the review's LOW-1: the evidence a reorg DEMOTES is its own counter, seeded and served beside
    /// `probeMemosCleared`, which keeps counting the memos the clear deleted and nothing else; the line names the belt.
    #[test]
    fn fix1027f_l1_demotions_are_counted_apart_and_served_beside_the_cleared() {
        let view = serde_json::json!({ "readable": true });
        let none = crate::ops::with_probe_memos_cleared(view.clone(), &serde_json::json!({}));
        assert_eq!(none["probeEvidenceDemoted"], 0, "served from 0");
        let some = crate::ops::with_probe_memos_cleared(
            view,
            &serde_json::json!({ COUNTER_HOP_PROBE_MEMOS_CLEARED: 1, COUNTER_HOP_PROBE_EVIDENCE_DEMOTED: 101 }),
        );
        assert_eq!(
            (
                some["probeMemosCleared"].clone(),
                some["probeEvidenceDemoted"].clone()
            ),
            (serde_json::json!(1), serde_json::json!(101))
        );
        assert_eq!(
            COUNTER_HOP_PROBE_EVIDENCE_DEMOTED,
            "hop_probe_evidence_demoted_by_reorg_total"
        );
        let ops = include_str!("ops.rs");
        assert!(
            ops.contains("crate::hop_probe_memos::COUNTER_HOP_PROBE_EVIDENCE_DEMOTED,"),
            "seeded at 0 in `read_counters`"
        );
        let src: String = include_str!("hop_probe_memos.rs")
            .split("#[cfg(test)]")
            .next()
            .unwrap_or_default()
            .split_whitespace()
            .collect();
        for needle in [
            "bump_counter(db,COUNTER_HOP_PROBE_MEMOS_CLEARED,deleted)",
            "bump_counter(db,COUNTER_HOP_PROBE_EVIDENCE_DEMOTED,demoted)",
        ] {
            assert!(src.contains(needle), "the clear bumps `{needle}`");
        }
        assert!(
            !src.contains("thenextowedwalkre-probesthem\");")
                && src.contains("thebeltre-asksthedemotedoutpoints"),
            "the clear's line names the belt, not the retired walk"
        );
    }

    /// bsv-low #1027, the review's NIT-1: the statement the clear RUNS is the pinned one plus its answer, so the
    /// #484 pins over `HOP_PROBE_MEMO_REORG_CLEAR_SQL` hold the production delete.
    #[test]
    fn fix1027f_nit1_the_framed_clear_is_the_pinned_clear_answering_its_keys() {
        assert_eq!(
            HOP_PROBE_MEMO_REORG_CLEAR_FRAMED_SQL,
            format!("{HOP_PROBE_MEMO_REORG_CLEAR_SQL} RETURNING outpoint")
        );
        assert_eq!(
            HOP_PROBE_MEMO_REORG_CLEAR_FRAMED[0],
            HOP_PROBE_MEMO_REORG_CLEAR_FRAMED_SQL
        );
    }

    /// SOURCE PIN (bsv-low #484): BOTH reorg judges invalidate, each where it has its outcome in hand: the D7
    /// Arcade pass inside `run_arcade_reorg_pass` (so the block-event pass, the cron and the on-demand route all
    /// carry it), the R2 legs in `internal_tip_changed` (the announced reorg, the targeted demotion, the routine
    /// sweep) and `internal_reorg` (the operator's window); the TTL sweep rides every block-event pass.
    /// To red: drop any one call.
    #[test]
    fn both_reorg_judges_invalidate_and_the_ttl_rides_the_block_event_pass() {
        let code_only = |s: &str| {
            s.lines()
                .map(|l| l.split("//").next().unwrap_or(""))
                .collect::<Vec<_>>()
                .join("\n")
        };
        // The reading of a function body is independent of rustfmt: whitespace out, and the
        // trailing comma a vertical argument list carries before its closing paren out too.
        let squash = |s: &str| s.split_whitespace().collect::<String>().replace(",)", ")");
        let src = include_str!("tip_pass.rs");
        let src = code_only(&src[..src.find("#[cfg(test)]").unwrap_or(src.len())]);
        let body = |name: &str| -> String {
            let start = src
                .find(&format!("pub async fn {name}("))
                .unwrap_or_else(|| panic!("{name}"));
            let rest = &src[start + 1..];
            let end = rest
                .find("\npub async fn ")
                .or_else(|| rest.find("\npub fn "))
                .unwrap_or(rest.len());
            squash(&rest[..end.min(rest.find("\npub fn ").unwrap_or(rest.len()))])
        };
        let arcade = body("run_arcade_reorg_pass");
        assert!(arcade.contains(&squash("crate::hop_probe_memos::clear_on_reorg(db, origin, crate::hop_probe_memos::arcade_pass_rejudged(&s).then(|| s.reach_floor.unwrap_or(0))).await")), "the D7 pass");
        let tip = body("internal_tip_changed");
        assert!(
            tip.contains(&squash("crate::hop_probe_memos::sweep_reach(reorg_from, crate::hop_probe_memos::reverify_rejudged(&demotion), &sweep, height, REORG_SWEEP_DEPTH)")),
            "the R2 legs of the block-event pass"
        );
        assert!(
            tip.contains(&squash(
                "crate::hop_probe_memos::expire(db, worker::Date::now().as_millis() as i64).await"
            )),
            "the TTL sweep"
        );
        let operator = body("internal_reorg");
        assert!(
            operator.contains(&squash(
                "crate::hop_probe_memos::clear_on_reorg(db, \"operator\", Some(trigger.from_height)).await"
            )),
            "the operator's window"
        );
    }

    /// bsv-low #484 (lens fold, M3): THE OPERATOR'S WINDOW IS REORG EVIDENCE BY ITSELF. Both feeds can miss a
    /// reorg that orphaned only a hop sweep (the index holds no pot row at that height, so `handle_reorg`
    /// re-judges nothing); `POST /internal/reorg` over the height is the documented heal, and it cleared the
    /// memos only if a pot row moved: nothing cleared, the stale memo stood for its window. An operator naming
    /// a window is the operator's word, as an announced `reorg_from` already is in the block-event pass.
    /// To red: gate the clear on `reverify_rejudged(&pass)` again.
    #[test]
    fn the_operators_reorg_window_clears_the_memos_whether_or_not_a_pot_row_moved() {
        // the pass an operator's window gets when no pot row sits at the orphaned height
        let nothing_moved = ReverifyPassSummary {
            scanned: 0,
            ..Default::default()
        };
        assert!(
            !reverify_rejudged(&nothing_moved),
            "the pass itself is no evidence: the old gate cleared nothing here"
        );
        let code_only = |s: &str| {
            s.lines()
                .map(|l| l.split("//").next().unwrap_or(""))
                .collect::<Vec<_>>()
                .join("\n")
        };
        // The reading of a function body is independent of rustfmt: whitespace out, and the
        // trailing comma a vertical argument list carries before its closing paren out too.
        let squash = |s: &str| s.split_whitespace().collect::<String>().replace(",)", ")");
        let src = include_str!("tip_pass.rs");
        let src = code_only(&src[..src.find("#[cfg(test)]").unwrap_or(src.len())]);
        let start = src
            .find("pub async fn internal_reorg(")
            .expect("the operator's route");
        let route = squash(
            &src[start
                ..start
                    + src[start + 1..]
                        .find("\npub async fn ")
                        .unwrap_or(src.len() - start - 1)],
        );
        let clears: Vec<_> = route
            .match_indices("hop_probe_memos::clear_on_reorg(")
            .collect();
        assert_eq!(clears.len(), 1, "one clear in the operator's route");
        assert!(
            route.contains(&squash(
                r#"crate::hop_probe_memos::clear_on_reorg(db, "operator", Some(trigger.from_height)).await"#
            )),
            "the operator's window clears unconditionally"
        );
        assert!(
            !route.contains("reverify_rejudged(&pass)"),
            "never gated on a pot row having moved"
        );
        // and it runs after the pass (the tombstone's stamp is the heal's time), on the 200 arm only
        assert!(route.find("crate::reorg_sweep::handle_reorg(").unwrap() < clears[0].0);
    }

    /// bsv-low #1027 (lane FIX-1027-BELT-FIND-RECORD), the clear's batch as [`clear_on_reorg`] runs it over the
    /// SHIPPED schema (real SQLite): the marks, then [`HOP_PROBE_MEMO_REORG_CLEAR_FRAMED`] in order. A confirmed memo
    /// and a proof latch are deleted as before; the belt's EVIDENCE of a spend (`<txid>:<vout>`) is KEPT with its
    /// spender and its confirmation taken (`spentConfirmed` NULL); the marks name the evidence's subject once, a
    /// downgrade; a second clear demotes nothing (idempotent) and an unconfirmed evidence row is never touched. RED on
    /// 91eb883b's rule (the framed list grafted as the base's one delete): the evidence deleted, `left: []`.
    #[test]
    fn fix1027_b_a_reorg_demotes_the_belts_evidence_and_never_deletes_it() {
        let conn = db();
        let hop = "b9".repeat(32);
        let spender = "e5".repeat(32);
        conn.execute_batch(&format!(
            "INSERT INTO pot_records(txid,outputIndex,spent,createdAt,lockKind) VALUES ('{hop}',0,0,1,'p2pkh'); \
             INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('owner','other','g',0,20190,'seat','00','00',2,'{hop}',1,1,1); \
             DELETE FROM owed_dirty"
        ))
        .unwrap();
        let evidence = format!("{hop}:0");
        let live_race = format!("{}:0", "c7".repeat(32));
        plant(&conn, &evidence, 1_000, true, Some(true));
        plant(&conn, &format!("{hop}.0"), 1_000, true, Some(true));
        plant(
            &conn,
            &format!("proofmined:{spender}.5"),
            1_000,
            true,
            Some(true),
        );
        plant(&conn, &live_race, 1_000, true, Some(false));
        let run = |conn: &rusqlite::Connection| clear_batch(conn, 0);
        let written = run(&conn);
        let demoted: Vec<String> = written.iter().skip(1).flatten().cloned().collect();
        assert_eq!(demoted, vec![evidence.clone()], "the evidence demoted");
        let row: (i64, Option<String>, Option<i64>) = conn
            .query_row(
                "SELECT spent,spendingTxid,spentConfirmed FROM hop_chain_probes WHERE outpoint=?1",
                [&evidence],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            row,
            (1, Some(spender.clone()), None),
            "the spend stands, its confirmation taken"
        );
        assert_eq!(
            held(&conn),
            vec![evidence.clone(), live_race.clone()],
            "the memo and the latch deleted, the unconfirmed evidence untouched"
        );
        let mut q = conn
            .prepare(
                "SELECT identity,source,sourceKey,downgrade FROM owed_dirty WHERE sourceKey=?1",
            )
            .unwrap();
        let marks: Vec<(String, String, String, i64)> = q
            .query_map([&evidence], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            marks,
            vec![("owner".into(), "probe".into(), evidence.clone(), 1)],
            "the writer's own pair, once, a downgrade (the seed's own marks cleared first)"
        );
        assert!(
            run(&conn).iter().all(Vec::is_empty),
            "a second clear writes nothing"
        );
        // the clear's writer runs the list as its batch
        let src = include_str!("hop_probe_memos.rs");
        let src = &src[..src.find("#[cfg(test)]").unwrap()];
        let body: String = src[src.find("pub async fn clear_on_reorg(").unwrap()..]
            .split_whitespace()
            .collect();
        assert!(body
            .contains("HOP_PROBE_MEMO_REORG_CLEAR_MARK_SQL,&HOP_PROBE_MEMO_REORG_CLEAR_FRAMED,"));
    }

    /// bsv-low #1027 round 2 (MEDIUM-1, `docs/audit/FIX-1027-REVIEW-2-2026-11-01.md`): A CLEAR REACHES WHAT ITS REORG
    /// CAN HAVE UNMADE. The clear's batch over the SHIPPED schema (real SQLite) with the reach `H` = 980: the belt's
    /// evidence confirmed at or above `H` (its look's `tipHeight`), and evidence with no `tipHeight`, is demoted and
    /// marked; evidence confirmed below `H` stays confirmed, unmarked (so neither judge dates it for a look); a memo and
    /// a proof latch (read by the height in its key) are deleted only at or above `H`. A block-event pass whose word
    /// carries no height reaches the sweep's window ([`sweep_reach`]: its lowest leg's `lo`, else `tip - depth`), the
    /// announced `reorgFrom` below it, and with no evidence nothing. The belt's load from a clear is the `demoted` its
    /// line counts: here 2 of 3 confirmed evidence rows. RED on 4a1cdbdc's rule (the scope term made inert, `?1 = ?1`,
    /// the base's clear with the bind): every confirmed row demoted, `left: [.., "a1..:0", ..]`.
    #[test]
    fn fix1027f2_m1_a_clear_demotes_only_the_rows_its_reorg_can_have_unmade() {
        let conn = db();
        let ev = |c: &str| format!("{}:0", c.repeat(32));
        let insert = |outpoint: &str, confirmed: Option<i64>, tip: Option<i64>| {
            conn.execute(
                "INSERT INTO hop_chain_probes (outpoint, probedAtMs, spent, spendingTxid, spentConfirmed, tipHeight) VALUES (?1, 1000, 1, ?2, ?3, ?4)",
                rusqlite::params![outpoint, "e5".repeat(32), confirmed, tip],
            )
            .unwrap();
        };
        let (deep, shallow, untipped, unconfirmed) = (ev("a1"), ev("b2"), ev("c3"), ev("d4"));
        insert(&deep, Some(1), Some(950));
        insert(&shallow, Some(1), Some(990));
        insert(&untipped, Some(1), None);
        insert(&unconfirmed, None, Some(995));
        let (memo_deep, memo_shallow) = (
            format!("{}.0", "a1".repeat(32)),
            format!("{}.0", "b2".repeat(32)),
        );
        insert(&memo_deep, Some(1), Some(950));
        insert(&memo_shallow, Some(1), Some(985));
        let latch = |h: u64| format!("proofmined:{}.{h}", "f6".repeat(32));
        insert(&latch(940), Some(1), None);
        insert(&latch(996), Some(1), None);
        for (hop, vout) in [("a1", 0), ("b2", 0), ("c3", 0)] {
            conn.execute(
                "INSERT INTO hopparty_records(identity,opponentIdentity,gameId,hopVout,hopSats,seatSettlePubkey,seatSigHex,identitySigHex,containerOutputs,txid,outputIndex,createdAt,markerValid) VALUES ('owner','other',?1,?2,20190,'seat','00','00',2,?3,1,1,1)",
                rusqlite::params![format!("g-{hop}"), vout, hop.repeat(32)],
            )
            .unwrap();
        }
        conn.execute("DELETE FROM owed_dirty", []).unwrap();
        let confirmed = |o: &str| -> Option<i64> {
            conn.query_row(
                "SELECT spentConfirmed FROM hop_chain_probes WHERE outpoint=?1",
                [o],
                |r| r.get(0),
            )
            .unwrap()
        };
        let written = clear_batch(&conn, 980);
        let mut deleted = written[0].clone();
        deleted.sort();
        let mut demoted = written[1].clone();
        demoted.sort();
        assert_eq!(
            demoted,
            vec![shallow.clone(), untipped.clone()],
            "the evidence at or above the reach, and the untipped, demoted"
        );
        assert_eq!(
            confirmed(&deep),
            Some(1),
            "evidence confirmed below the reach stands"
        );
        assert_eq!(
            confirmed(&unconfirmed),
            None,
            "an unconfirmed row untouched"
        );
        assert_eq!(
            deleted,
            vec![memo_shallow.clone(), latch(996)],
            "memos and latches only at or above the reach"
        );
        let held_rows = held(&conn);
        assert!(held_rows.contains(&memo_deep) && held_rows.contains(&latch(940)));
        let mut marked: Vec<String> = conn
            .prepare(
                "SELECT DISTINCT sourceKey FROM owed_dirty WHERE source='probe' ORDER BY sourceKey",
            )
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        marked.sort();
        let mut want = vec![shallow.clone(), untipped.clone(), memo_shallow.clone()];
        want.sort();
        assert_eq!(
            marked, want,
            "only the rows the clear moved are marked: the deep evidence is never dated"
        );
        assert!(
            clear_batch(&conn, 980).iter().all(Vec::is_empty),
            "idempotent"
        );
        // the reach of 0 (a pass that names no height) is the base's whole table
        assert_eq!(clear_batch(&conn, 0)[1], vec![deep.clone()]);

        // the block event's reach: a word with no height reaches the sweep's window
        let swept = |window: Option<(u64, u64)>| ReorgSweepSummary {
            spenders: ReverifyPassSummary {
                stale: 1,
                ..Default::default()
            },
            spenders_window: window,
            ..Default::default()
        };
        assert_eq!(
            sweep_reach(None, false, &swept(Some((997, 1000))), 1000, 3),
            Some(997)
        );
        assert_eq!(
            sweep_reach(None, false, &swept(None), 1000, 3),
            Some(997),
            "tip less the depth"
        );
        assert_eq!(
            sweep_reach(Some(980), true, &swept(Some((997, 1000))), 1000, 3),
            Some(980)
        );
        assert_eq!(
            sweep_reach(Some(980), true, &ReorgSweepSummary::default(), 1000, 3),
            Some(980)
        );
        assert_eq!(
            sweep_reach(None, false, &ReorgSweepSummary::default(), 1000, 3),
            None,
            "no evidence, no clear"
        );
        assert_eq!(
            sweep_reach(None, true, &ReorgSweepSummary::default(), 1000, 3),
            Some(0),
            "a re-judge that names no height: the whole table"
        );
        let quiet = ReorgSweepSummary {
            spenders_window: Some((997, 1000)),
            ..Default::default()
        };
        assert_eq!(
            sweep_reach(None, false, &quiet, 1000, 3),
            None,
            "a sweep that left every row standing"
        );
    }

    /// bsv-low #1027 round 3 (MEDIUM-1 and LOW-1, `docs/audit/FIX-1027-REVIEW-3-2026-11-01.md`): THE CLEAR ALLOWS FOR
    /// THE VERIFIED TIP'S LAG. A look's `tipHeight` is the verified header tip, which can trail the block the courier
    /// confirms the spend in, so evidence confirmed by looks at `tipHeight` = floor, floor - 1 and floor - lag is
    /// demoted and marked, and at floor - lag - 1 stands; a proof latch keyed by its proof's own block height is
    /// compared exactly: at the floor deleted, at floor - 1 kept. RED on 0ab42913's predicate (`tipHeight >= ?1`): the
    /// row at floor - 1 stands. RED with `>=` made `>` (round 3's M6): the row at floor - lag and the latch at the
    /// floor stand.
    #[test]
    fn fix1027f3_m1_the_clear_allows_for_the_verified_tips_lag() {
        assert_eq!(
            reorg_tip_lag!().parse::<u64>().unwrap(),
            REORG_TIP_LAG_BLOCKS,
            "the predicate's literal is the named constant"
        );
        let conn = db();
        let floor: i64 = 971_100;
        let lag = REORG_TIP_LAG_BLOCKS as i64;
        let ev = |c: &str| format!("{}:0", c.repeat(32));
        let insert = |outpoint: &str, tip: Option<i64>| {
            conn.execute(
                "INSERT INTO hop_chain_probes (outpoint, probedAtMs, spent, spendingTxid, spentConfirmed, tipHeight) VALUES (?1, 1000, 1, ?2, 1, ?3)",
                rusqlite::params![outpoint, "e5".repeat(32), tip],
            )
            .unwrap();
        };
        let (at_floor, below_one, at_lag, past_lag) = (ev("a1"), ev("b2"), ev("c3"), ev("d4"));
        insert(&at_floor, Some(floor));
        insert(&below_one, Some(floor - 1));
        insert(&at_lag, Some(floor - lag));
        insert(&past_lag, Some(floor - lag - 1));
        let latch = |h: i64| format!("proofmined:{}.{h}", "f6".repeat(32));
        insert(&latch(floor), None);
        insert(&latch(floor - 1), None);
        conn.execute("DELETE FROM owed_dirty", []).unwrap();
        let written = clear_batch(&conn, floor);
        let mut demoted = written[1].clone();
        demoted.sort();
        assert_eq!(
            demoted,
            vec![at_floor.clone(), below_one.clone(), at_lag.clone()],
            "evidence from looks at the floor, one block under it and the lag under it is demoted"
        );
        let confirmed: Option<i64> = conn
            .query_row(
                "SELECT spentConfirmed FROM hop_chain_probes WHERE outpoint=?1",
                [&past_lag],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(confirmed, Some(1), "a look past the lag stands confirmed");
        assert_eq!(
            written[0],
            vec![latch(floor)],
            "a latch is compared by its proof's own height, exactly"
        );
        assert!(held(&conn).contains(&latch(floor - 1)));
    }
}
