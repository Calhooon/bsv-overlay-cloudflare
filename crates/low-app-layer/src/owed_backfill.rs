//! bsv-low #893 section 6.1: the one-time `owed-v2` derivation behind a stamp, run by the scheduled pass and never by a
//! read (the 2026-10-12 lesson: a backfill never runs on a read path).
//!
//! A pass ([`backfill_pass`]) reads the stamp first and stops there once it is written (one indexed read, pinned at
//! zero scan steps). Unstamped, it reads the overlay's certified migration list (the triggers must exist before the
//! rows do) and its cursor; the first pass that finds the gate open only ARMS the cursor, so the work starts one cron
//! tick after the new version's first pass (risk 8.4). Then up to [`OWED_BACKFILL_IDENTITIES_PER_PASS`] identities
//! past the cursor, ascending, from `potparty_records` and `hopparty_records` by their identity indexes: for each, its
//! games marked `source = 'backfill'` and judged [`OWED_BACKFILL_GAMES_PER_BATCH`] a batch through the shipped judge,
//! then the cursor saved. The stamp lands in the batch that saves the cursor past the last identity.
//!
//! Before the identities (D2-OWED-FOLD-8), the pass writes the released-hop evidence of every OPEN eviction made
//! before the overlay's eviction writer existed: [`OWED_BACKFILL_EVICTIONS_PER_PAGE`] open evictions a page past the
//! cursor [`OWED_BACKFILL_EVICTIONS`], ascending by txid, each one naming no evidence row for any of its released
//! outpoints, judged by the WRITER's own three checks (its two statements, byte for byte the overlay's, over the
//! evicted twins, and its check 3, `overlay_discovery::pot::released_evidence::evidence_writes`, over the JOIN's stored
//! bytes); a JOIN whose bytes are absent or undecodable names its released outpoints per verified seat
//! (`released_hop_evidence_missing`), so the judge serves the old walk's could-not-judge row. The phase ends with its
//! cursor at [`EVICTIONS_DONE`]; only then are identities visited. The identities come from `potparty_records`,
//! `hopparty_records` AND the evicted twin (`potparty_records_evicted`, a verified seat with its settle key, of an open
//! eviction: the #610 rule), so a seat whose only party row was moved by an eviction is judged too; the one identity
//! cursor walks the union of the three in identity order, a twin-named identity named by its identity as the others.
//!
//! An identity the wall cuts keeps its remaining marks (the drain may judge them too) and its `open` row, so the next
//! pass finishes its marks and never re-marks it from the start: an identity bigger than one pass still converges.
//! A pass that completes NO judge batch of the open identity (its batch outlasting the wall, so every pass restarted
//! the same batch: prod's 03295e29..., 2026-10-26) halves that identity's batch ([`OWED_BACKFILL_BATCH`], floor 1);
//! the next identity starts at the full size again.
//!
//! [`mark_aged`] is risk 8.2's third repair: the scheduled drain re-marks, at most once a day a row, the subjects whose
//! stored row was judged more than 24 h ago (`source = 'age'`), bounded [`OWED_AGE_REMARKS_PER_PASS`] a pass.
use crate::owed_judge::{self, JudgeDb};
use crate::pot_hook::{Bind, Deadline, SqlPort, Stmt};
use serde_json::Value;

/// The derivation's version, its `view_derivation_state` row. A later derivation ships `owed-v3` with its own cursor.
pub const OWED_BACKFILL_STAMP: &str = "owed-v2";
/// The `owed_belt_state` row holding the last identity backfilled ('' once armed, before the first).
pub const OWED_BACKFILL_CURSOR: &str = "backfill:owed-v2";
/// The `owed_belt_state` row naming the identity whose marks are written and not yet all judged.
pub const OWED_BACKFILL_OPEN: &str = "backfill:owed-v2:open";
pub const OWED_BACKFILL_IDENTITIES_PER_PASS: usize = 128;
/// One judge batch: the judge's own per-invocation subject bound.
pub const OWED_BACKFILL_GAMES_PER_BATCH: usize = owed_judge::OWED_JUDGE_SUBJECTS_PER_INVOCATION;
/// The pass's own wall and the cursor save's reserve inside it.
pub const OWED_BACKFILL_WALL_MS: u64 = 20_000;
pub const OWED_BACKFILL_SAVE_RESERVE_MS: u64 = 3_000;
/// A row judged longer ago than this is re-marked by the drain (once a day, since its judge restamps it).
pub const OWED_AGE_REMARK_AFTER_MS: i64 = 86_400_000;
pub const OWED_AGE_REMARKS_PER_PASS: usize = 128;
/// The passes one identity's judge may fault before the backfill records it, leaves its marks for the drain and moves
/// its cursor past it (the D2-OWED lens's LOW-1: one wedged identity held the stamp and every later identity back).
pub const OWED_BACKFILL_IDENTITY_FAULTS_MAX: usize = 3;
/// The `owed_belt_state` row counting the open identity's consecutive faulted passes (`{"identity":..,"faults":n}`).
pub const OWED_BACKFILL_FAULTS: &str = "backfill:owed-v2:faults";
/// The `owed_belt_state` row holding the open identity's judge batch once a pass judged none of it
/// (`{"identity":..,"size":n}`): halved at each such pass, at least 1, cleared ('{}') by its identity's save.
pub const OWED_BACKFILL_BATCH: &str = "backfill:owed-v2:batch";
/// The `owed_belt_state` row counting the faulted eviction's consecutive faulted passes (`{"txid":..,"faults":n}`;
/// D2-OWED-FOLD-9, the fifth delta lens's DELTA5-L1: the evidence phase takes the identity loop's LOW-1 bound).
pub const OWED_BACKFILL_EVICTION_FAULTS: &str = "backfill:owed-v2:eviction-faults";
/// The `owed_belt_state` row holding the last eviction the evidence phase wrote ('' or absent before the first).
pub const OWED_BACKFILL_EVICTIONS: &str = "backfill:owed-v2:evictions";
/// The evidence phase's cursor once its last page was short: past every hex txid, so the page read finds none.
pub const EVICTIONS_DONE: &str = "~";
/// One page of open evictions (each a ledger read beside it, two reads and one write batch an eviction).
pub const OWED_BACKFILL_EVICTIONS_PER_PAGE: usize = 32;
/// A skipped eviction is retried by the age repair once its skip is this old (the repair's own day), at most
/// [`OWED_SKIPPED_EVICTION_RETRIES_PER_PASS`] a pass, oldest skip first; each retry round has the phase's three faults
/// before it is skipped again (D2-OWED-FOLD-10, the sixth delta lens's NEW-L1: a fault is transient, a lost press is
/// not, so no skip is permanent).
pub const OWED_SKIPPED_EVICTION_RETRY_AFTER_MS: i64 = OWED_AGE_REMARK_AFTER_MS;
pub const OWED_SKIPPED_EVICTION_RETRIES_PER_PASS: usize = 4;
/// The skipped evictions one age repair reads and names on its line.
pub const OWED_SKIPPED_EVICTIONS_NAMED: usize = 32;

pub const COUNTER_IDENTITIES: &str = "owed_backfill_identities_total";
pub const COUNTER_ROWS: &str = "owed_backfill_rows_total";
pub const COUNTER_AGED: &str = "owed_age_remarked_total";
pub const COUNTER_SKIPPED: &str = "owed_backfill_skipped_identities_total";
pub const COUNTER_EVICTIONS: &str = "owed_backfill_evictions_total";
pub const COUNTER_EVIDENCE: &str = "owed_backfill_evidence_candidates_total";
pub const COUNTER_MISSING: &str = "owed_backfill_evidence_missing_total";
pub const COUNTER_SKIPPED_EVICTIONS: &str = "owed_backfill_skipped_evictions_total";
pub fn counter_names() -> Vec<String> {
    [
        COUNTER_IDENTITIES,
        COUNTER_ROWS,
        COUNTER_AGED,
        COUNTER_SKIPPED,
        COUNTER_EVICTIONS,
        COUNTER_EVIDENCE,
        COUNTER_MISSING,
        COUNTER_SKIPPED_EVICTIONS,
    ]
    .map(str::to_owned)
    .to_vec()
}

/// The stamped replay's one statement: a primary-key read of one row.
pub const STAMP_SQL: &str = "SELECT 1 AS stamped FROM view_derivation_state WHERE name='owed-v2'";
/// The gate (the overlay's certified migration list) and both cursor rows, each a primary-key read.
pub const GATE_SQL: &str = "SELECT (SELECT value FROM ops_counters WHERE name='overlay_migration_count') AS count,(SELECT cursor FROM owed_belt_state WHERE name='backfill:owed-v2') AS cursor,(SELECT cursor FROM owed_belt_state WHERE name='backfill:owed-v2:open') AS open,(SELECT cursor FROM owed_belt_state WHERE name='backfill:owed-v2:faults') AS faults,(SELECT cursor FROM owed_belt_state WHERE name='backfill:owed-v2:evictions') AS evictions,(SELECT cursor FROM owed_belt_state WHERE name='backfill:owed-v2:eviction-faults') AS evictionFaults,(SELECT cursor FROM owed_belt_state WHERE name='backfill:owed-v2:batch') AS batch";
/// The open evictions past the cursor, ascending, each naming no evidence row of its own for any released outpoint
/// (an eviction the writer saw has them; each probe a primary-key search of the evidence by outpoint).
/// An open eviction `e` that names no evidence row of its own for any released outpoint: the evidence phase's page
/// and the age repair's retry of a skipped eviction read the one predicate.
macro_rules! eviction_unwritten {
    () => {
        "e.readmittedAt IS NULL AND e.releasedSpends IS NOT NULL AND NOT EXISTS (SELECT 1 FROM json_each(e.releasedSpends) r JOIN released_hop_evidence h ON h.hopTxid=lower(json_extract(r.value,'$.txid')) AND h.hopVout=json_extract(r.value,'$.vout') WHERE h.potTxid=e.txid AND h.evictedAt=e.evictedAt)"
    };
}
/// D2-OWED-HOME-MARKER: an open eviction `e` releasing a hop whose VERIFIED hop marker names an identity the
/// eviction wrote no evidence row for. The writer before this lane took check 1 from the twin party row alone, so a
/// seat whose only attestation is its marker (beta's courier-retire hand 3: its filing never landed) got none.
macro_rules! marker_unwritten {
    () => {
        "EXISTS (SELECT 1 FROM json_each(e.releasedSpends) r JOIN hopparty_records hp INDEXED BY idx_hopparty_hop ON hp.txid=lower(json_extract(r.value,'$.txid')) AND hp.hopVout=json_extract(r.value,'$.vout') WHERE IFNULL(json_extract(r.value,'$.table'),'pot_records')='pot_records' AND hp.markerValid=1 AND NOT EXISTS (SELECT 1 FROM released_hop_evidence h WHERE h.hopTxid=hp.txid AND h.hopVout=hp.hopVout AND h.identity=lower(hp.identity) AND h.potTxid=e.txid AND h.evictedAt=e.evictedAt))"
    };
}
pub const EVICTIONS_SQL: &str = concat!(
    "SELECT e.txid,e.releasedSpends FROM pot_evictions e WHERE e.txid>?1 AND ",
    eviction_unwritten!(),
    " ORDER BY e.txid LIMIT ?2"
);
/// The JOIN's stored bytes as the writer reads them, in its order (`transactions`, then `pot_beefs`; then
/// `transactions_evicted`, [`EVICTED_BEEF_SQL`], read alone: the overlay makes that shadow at its first eviction).
pub const STORED_BEEF_SQL: &str = "SELECT hex(beef) AS beef FROM transactions WHERE txid = ?";
pub const POT_BEEF_SQL: &str = "SELECT hex(beef) AS beef FROM pot_beefs WHERE txid = ?";
pub const EVICTED_BEEF_SQL: &str =
    "SELECT hex(beef) AS beef FROM transactions_evicted WHERE txid = ?";
/// The writer's candidate seat keys, byte for byte the overlay's `eviction_write::SEAT_KEYS_SQL` (pinned).
pub const SEAT_KEYS_SQL: &str = "SELECT DISTINCT lower(seatSettlePubkey) AS key FROM potparty_records_evicted INDEXED BY idx_evicted_party_pot WHERE potTxid=?1 AND sigValid=1 AND seatSettlePubkey IS NOT NULL UNION SELECT DISTINCT lower(seatSettlePubkey) AS key FROM potparty_records INDEXED BY idx_potparty_pot WHERE potTxid=?1 AND sigValid=1 AND seatSettlePubkey IS NOT NULL";
/// The writer's evidence statement (checks 1 and 2 on the evicted rows), byte for byte the overlay's (pinned).
pub const EVIDENCE_INSERT_SQL: &str = "WITH seats AS (\
 SELECT lower(identity) AS identity,lower(gameId) AS gameId,lower(seatSettlePubkey) AS seatKey \
 FROM potparty_records_evicted INDEXED BY idx_evicted_party_pot \
 WHERE potTxid=?1 AND sigValid=1 AND seatSettlePubkey IS NOT NULL GROUP BY lower(identity) \
 HAVING MIN(lower(gameId))=MAX(lower(gameId)) AND MIN(lower(seatSettlePubkey))=MAX(lower(seatSettlePubkey)) \
 UNION ALL SELECT lower(hp.identity),lower(hp.gameId),lower(hp.seatSettlePubkey) \
 FROM hopparty_records hp INDEXED BY idx_hopparty_hop \
 WHERE hp.txid=?2 AND hp.hopVout=?3 AND hp.markerValid=1 \
 AND NOT EXISTS (SELECT 1 FROM potparty_records_evicted x INDEXED BY idx_evicted_party_pot WHERE x.potTxid=?1 AND x.sigValid=1 AND x.seatSettlePubkey IS NOT NULL AND lower(x.identity)=lower(hp.identity)) \
 GROUP BY lower(hp.identity) \
 HAVING MIN(lower(hp.gameId))=MAX(lower(hp.gameId)) AND MIN(lower(hp.seatSettlePubkey))=MAX(lower(hp.seatSettlePubkey))\
), committed AS (\
 SELECT lower(pubA) AS a,lower(pubB) AS b,lower(payPkhA) AS pa,lower(payPkhB) AS pb \
 FROM pot_records_evicted INDEXED BY idx_evicted_pot_txid WHERE txid=?1 \
 AND length(pubA)=66 AND lower(pubA) NOT GLOB '*[^0-9a-f]*' \
 AND length(pubB)=66 AND lower(pubB) NOT GLOB '*[^0-9a-f]*' \
 AND length(payPkhA)=40 AND lower(payPkhA) NOT GLOB '*[^0-9a-f]*' \
 AND length(payPkhB)=40 AND lower(payPkhB) NOT GLOB '*[^0-9a-f]*'\
) INSERT INTO released_hop_evidence (hopTxid,hopVout,identity,gameId,potTxid,seatSettlePubkey,hopSats,evictedAt) \
 SELECT DISTINCT ?2,?3,s.identity,s.gameId,?1,s.seatKey,?5,e.evictedAt FROM seats s \
 JOIN pot_evictions e ON e.txid=?1 WHERE s.seatKey=?4 AND e.readmittedAt IS NULL \
 AND EXISTS (SELECT 1 FROM committed c WHERE (c.a=?4)<>(c.b=?4) \
 AND NOT EXISTS (SELECT 1 FROM committed other WHERE other.a<>c.a OR other.b<>c.b OR other.pa<>c.pa OR other.pb<>c.pb)) \
 AND EXISTS (SELECT 1 FROM json_each(e.releasedSpends) r WHERE json_extract(r.value,'$.table')='pot_records' AND lower(json_extract(r.value,'$.txid'))=?2 AND json_extract(r.value,'$.vout')=?3) \
 ON CONFLICT(hopTxid,hopVout,identity) DO UPDATE SET gameId=excluded.gameId,potTxid=excluded.potTxid,seatSettlePubkey=excluded.seatSettlePubkey,hopSats=excluded.hopSats,evictedAt=excluded.evictedAt";
/// The writer's missing-evidence statement, byte for byte the overlay's `eviction_write::MISSING_INSERT_SQL` (pinned).
pub const MISSING_INSERT_SQL: &str = "WITH seats AS (\
 SELECT lower(identity) AS identity,lower(gameId) AS gameId \
 FROM potparty_records_evicted INDEXED BY idx_evicted_party_pot \
 WHERE potTxid=?1 AND sigValid=1 AND seatSettlePubkey IS NOT NULL GROUP BY lower(identity) \
 HAVING MIN(lower(gameId))=MAX(lower(gameId)) AND MIN(lower(seatSettlePubkey))=MAX(lower(seatSettlePubkey))\
) INSERT INTO released_hop_evidence_missing (identity,gameId,hopTxid,hopVout,potTxid,evictedAt,bytesRead) \
 SELECT DISTINCT s.identity,s.gameId,lower(json_extract(o.value,'$.txid')),json_extract(o.value,'$.vout'),?1,e.evictedAt,?3 \
 FROM seats s JOIN pot_evictions e ON e.txid=?1 JOIN json_each(?2) o \
 WHERE e.readmittedAt IS NULL \
 AND EXISTS (SELECT 1 FROM json_each(e.releasedSpends) r WHERE IFNULL(json_extract(r.value,'$.table'),'pot_records')='pot_records' AND lower(json_extract(r.value,'$.txid'))=lower(json_extract(o.value,'$.txid')) AND json_extract(r.value,'$.vout')=json_extract(o.value,'$.vout')) \
 UNION SELECT lower(hp.identity),lower(hp.gameId),lower(json_extract(o.value,'$.txid')),json_extract(o.value,'$.vout'),?1,e.evictedAt,?3 \
 FROM json_each(?2) o JOIN hopparty_records hp INDEXED BY idx_hopparty_hop ON hp.txid=lower(json_extract(o.value,'$.txid')) AND hp.hopVout=json_extract(o.value,'$.vout') \
 JOIN pot_evictions e ON e.txid=?1 \
 WHERE e.readmittedAt IS NULL AND hp.markerValid=1 \
 AND NOT EXISTS (SELECT 1 FROM potparty_records_evicted x INDEXED BY idx_evicted_party_pot WHERE x.potTxid=?1 AND x.sigValid=1 AND x.seatSettlePubkey IS NOT NULL AND lower(x.identity)=lower(hp.identity)) \
 AND EXISTS (SELECT 1 FROM json_each(e.releasedSpends) r WHERE IFNULL(json_extract(r.value,'$.table'),'pot_records')='pot_records' AND lower(json_extract(r.value,'$.txid'))=lower(json_extract(o.value,'$.txid')) AND json_extract(r.value,'$.vout')=json_extract(o.value,'$.vout')) \
 ON CONFLICT(identity,gameId,hopTxid,hopVout) DO UPDATE SET potTxid=excluded.potTxid,evictedAt=excluded.evictedAt,bytesRead=excluded.bytesRead";
pub const EVICTIONS_CURSOR_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('backfill:owed-v2:evictions',?1,?2) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// The open identity's fault count, written by a faulted pass and cleared ('{}') by its identity's save.
pub const FAULTS_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('backfill:owed-v2:faults',?1,?2) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// The open identity's judge batch size, written when a pass judged none of its batches, cleared ('{}') by its save.
pub const BATCH_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('backfill:owed-v2:batch',?1,?2) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// The judge batch the identity starts at: the size [`OWED_BACKFILL_BATCH`] names for it, else the full batch.
pub fn batch_size_for(batch: &Value, identity: &str) -> usize {
    if batch["identity"] == identity {
        batch["size"]
            .as_u64()
            .map_or(OWED_BACKFILL_GAMES_PER_BATCH, |n| {
                (n as usize).clamp(1, OWED_BACKFILL_GAMES_PER_BATCH)
            })
    } else {
        OWED_BACKFILL_GAMES_PER_BATCH
    }
}
/// An identity passed by after its faults: its row names the last fault (bounded), its marks stay for the drain.
pub const SKIPPED_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('backfill:owed-v2:skipped:'||?1,?2,?3) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// The faulted eviction's count, written by a faulted pass and cleared ('{}') by its eviction's save or its skip.
pub const EVICTION_FAULTS_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('backfill:owed-v2:eviction-faults',?1,?2) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// An eviction passed by after its faults: its row (`{"reason":..,"faults":n}`, the last fault bounded, `tickedAtMs`
/// the skip's time) names it while it is skipped. Its outpoints get no evidence and no missing row from the phase; the
/// age repair RETRIES it once the skip is a day old (D2-OWED-FOLD-10, [`age_repair`]), and its row is deleted by the
/// retry that lands.
pub const SKIPPED_EVICTION_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('backfill:owed-v2:skipped-eviction:'||?1,?2,?3) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// The skipped evictions, oldest skip first (at most [`OWED_SKIPPED_EVICTIONS_NAMED`]), each with its ledger row and
/// whether it still needs its evidence (the phase's own predicate): a range of the state's key, one key search each.
pub const SKIPPED_EVICTIONS_SQL: &str = concat!(
    "SELECT substr(s.name,35) AS txid,s.cursor AS skip,s.tickedAtMs AS skippedAt,e.releasedSpends AS releasedSpends,CASE WHEN e.txid IS NOT NULL AND (",
    eviction_unwritten!(),
    " OR (e.readmittedAt IS NULL AND e.releasedSpends IS NOT NULL AND ",
    marker_unwritten!(),
    ")) THEN 1 ELSE 0 END AS open FROM owed_belt_state s LEFT JOIN pot_evictions e ON e.txid=substr(s.name,35) WHERE s.name>'backfill:owed-v2:skipped-eviction:' AND s.name<'backfill:owed-v2:skipped-eviction;' ORDER BY s.tickedAtMs,s.name LIMIT ?1"
);
/// A faulted retry's count, the skip's time kept (so the next age repair retries it again).
pub const SKIPPED_EVICTION_FAULT_SQL: &str =
    "UPDATE owed_belt_state SET cursor=?2 WHERE name='backfill:owed-v2:skipped-eviction:'||?1";
/// The skip ends: written in the retry's own write batch, or alone when the eviction no longer needs its evidence.
pub const SKIPPED_EVICTION_END_SQL: &str =
    "DELETE FROM owed_belt_state WHERE name='backfill:owed-v2:skipped-eviction:'||?1";
/// The `owed_belt_state` row of the marker repair's cursor: the last eviction txid it looked at, [`EVICTIONS_DONE`]
/// once a window came back short.
pub const OWED_MARKER_REPAIR: &str = "backfill:owed-v2:marker-evictions";
/// The evictions one age repair looks at (by the ledger's primary key, from the cursor), each a candidate only when
/// [`marker_unwritten`] holds; a candidate is written by the phase's own [`evidence_one`].
pub const OWED_MARKER_REPAIR_WINDOW: usize = 16;
pub const MARKER_REPAIR_CURSOR_SQL: &str =
    "SELECT cursor FROM owed_belt_state WHERE name='backfill:owed-v2:marker-evictions'";
/// The window: every ledger txid past the cursor (at most `?2`), each with its row when it is a candidate.
pub const MARKER_REPAIR_SQL: &str = concat!(
    "SELECT d.txid AS seen,e.txid AS txid,e.releasedSpends AS releasedSpends FROM (SELECT txid FROM pot_evictions WHERE txid>?1 ORDER BY txid LIMIT ?2) d LEFT JOIN pot_evictions e ON e.txid=d.txid AND e.readmittedAt IS NULL AND e.releasedSpends IS NOT NULL AND ",
    marker_unwritten!(),
    " ORDER BY d.txid"
);
pub const MARKER_REPAIR_SAVE_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('backfill:owed-v2:marker-evictions',?1,?2) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// bsv-low #953 (D2-ACTOR-B-FOLD-9): the cursor of the one-time seeding of the belt's evidence for the finds made
/// before the fold (their memos aged out; their only trace the spender's bytes, keyed by the SPENDER's txid): the
/// last `owed_spender_bytes` txid looked at, [`EVICTIONS_DONE`] once a page came back short.
pub const FIND_EVIDENCE_SEED: &str = "backfill:owed-v2:find-evidence";
/// The stored spenders one age repair looks at, by the store's primary key from the cursor.
pub const FIND_EVIDENCE_SEED_PAGE: usize = 64;
pub const FIND_EVIDENCE_CURSOR_SQL: &str =
    "SELECT cursor FROM owed_belt_state WHERE name='backfill:owed-v2:find-evidence'";
/// The page's extent: its last txid and its size.
pub const FIND_EVIDENCE_PAGE_SQL: &str = "SELECT MAX(txid) AS last,COUNT(*) AS n FROM (SELECT txid FROM owed_spender_bytes WHERE txid>?1 ORDER BY txid LIMIT ?2)";
/// The standing finds of a page, as the belt now writes them: each input of a stored spender that is a HOP (a
/// marker names it) whose index row stands unspent and names no spender (the class the index never saw) gets the
/// evidence row `<txid>:<vout>` naming that spender (`spentConfirmed` unknown: the bytes carry no block), unless
/// one stands. A content-addressed spender, hash-checked against its txid before its bytes were kept.
macro_rules! seed_candidates {
    () => {
        "FROM (SELECT txid,bytesJson,createdAtMs FROM owed_spender_bytes WHERE txid>?1 ORDER BY txid LIMIT ?2) b CROSS JOIN json_each(b.bytesJson,'$.inputs') i CROSS JOIN hopparty_records hp INDEXED BY idx_hopparty_hop ON hp.txid=lower(json_extract(i.value,'$[0]')) AND hp.hopVout=json_extract(i.value,'$[1]') WHERE json_valid(b.bytesJson) AND EXISTS (SELECT 1 FROM pot_records p WHERE p.txid=hp.txid AND p.outputIndex=hp.hopVout AND p.spent=0 AND p.spendingTxid IS NULL) AND NOT EXISTS (SELECT 1 FROM hop_chain_probes e WHERE e.outpoint=hp.txid||':'||hp.hopVout)"
    };
}
/// D2-ACTOR-B-FOLD-10 (the fifth delta lens's NEW5-M2): the seeded find's `spentConfirmed` from what the record
/// already proves of the spender's block: `1` where a chaintracks-checked mined proof of it is latched
/// (`proofmined:<txid>.<height>`, a range of the memo's key) or the index holds its verified proof
/// (`transactions.has_proof`); NULL only where neither is known (the belt then re-asks the outpoint,
/// `owed_belt::rests_on_unconfirmed_find`).
macro_rules! seed_confirmed {
    () => {
        "CASE WHEN EXISTS (SELECT 1 FROM hop_chain_probes l WHERE l.outpoint>'proofmined:'||b.txid||'.' AND l.outpoint<'proofmined:'||b.txid||'/') OR EXISTS (SELECT 1 FROM transactions t WHERE t.txid=b.txid AND t.has_proof=1) THEN 1 ELSE NULL END"
    };
}
/// The seats of the page's standing finds marked (`hop`, the evidence key; a downgrade), BEFORE the seeding (its
/// `NOT EXISTS` reads the evidence absent), in the seeding's batch.
pub const FIND_EVIDENCE_MARK_SQL: &str = concat!(
    "INSERT INTO owed_dirty (identity,gameId,source,sourceKey,markedAtMs,downgrade) SELECT DISTINCT hp.identity,hp.gameId,'hop',hp.txid||':'||hp.hopVout,strftime('%s','now') * 1000,1 ",
    seed_candidates!(),
    " ON CONFLICT(identity,gameId,source,sourceKey) DO UPDATE SET generation=excluded.generation,markedAtMs=excluded.markedAtMs,downgrade=MAX(owed_dirty.downgrade,excluded.downgrade)"
);
pub const FIND_EVIDENCE_SEED_SQL: &str = concat!(
    "INSERT INTO hop_chain_probes (outpoint,probedAtMs,spent,spendingTxid,spentConfirmed,tipHeight) SELECT DISTINCT hp.txid||':'||hp.hopVout,b.createdAtMs,1,b.txid,",
    seed_confirmed!(),
    ",NULL ",
    seed_candidates!(),
    " ON CONFLICT(outpoint) DO NOTHING"
);
pub const FIND_EVIDENCE_SAVE_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('backfill:owed-v2:find-evidence',?1,?2) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// Arm the cursor; a pass that finds it armed begins.
pub const ARM_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('backfill:owed-v2','',?1) ON CONFLICT(name) DO NOTHING";
/// The identities past the cursor, distinct and ascending, each side bounded on its own identity index.
/// D2-OWED-FOLD-8: and the verified seats of the evicted twin of an OPEN eviction (the #610 rule), by its identity index.
pub const IDENTITIES_SQL: &str = "SELECT identity FROM (SELECT identity FROM (SELECT DISTINCT identity FROM potparty_records INDEXED BY idx_potparty_identity WHERE identity>?1 ORDER BY identity LIMIT ?2) UNION SELECT identity FROM (SELECT DISTINCT identity FROM hopparty_records INDEXED BY idx_hopparty_identity WHERE identity>?1 ORDER BY identity LIMIT ?2) UNION SELECT identity FROM (SELECT DISTINCT p.identity FROM potparty_records_evicted p INDEXED BY idx_evicted_party_identity_game WHERE p.identity>?1 AND p.sigValid=1 AND p.seatSettlePubkey IS NOT NULL AND EXISTS (SELECT 1 FROM pot_evictions e WHERE e.txid=p.potTxid AND e.readmittedAt IS NULL) ORDER BY p.identity LIMIT ?2)) ORDER BY identity LIMIT ?2";
/// Every game of the identity the judge's wanted expansion names, marked `backfill`.
pub const MARK_SQL: &str = "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) SELECT ?1,gameId,'backfill','owed-v2',strftime('%s','now') * 1000 FROM (SELECT gameId FROM potparty_records WHERE identity=?1 UNION SELECT gameId FROM hopparty_records WHERE identity=?1 UNION SELECT gameId FROM released_hop_evidence WHERE identity=?1 UNION SELECT gameId FROM released_hop_evidence_missing WHERE identity=?1 UNION SELECT gameId FROM owed_rows WHERE identity=?1) WHERE gameId<>'' ON CONFLICT(identity,gameId,source,sourceKey) DO UPDATE SET generation=excluded.generation,markedAtMs=excluded.markedAtMs";
pub const OPEN_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('backfill:owed-v2:open',?1,?2) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// The whole identity is rebuilt by this backfill, so its earlier overflow flag is judged again (the judge ORs it).
pub const TRUNCATED_RESET_SQL: &str = "UPDATE owed_state SET truncated=0 WHERE identity=?1";
/// The next batch of the identity's backfill marks.
pub const GAMES_SQL: &str = "SELECT gameId FROM owed_dirty WHERE identity=?1 AND source='backfill' ORDER BY gameId LIMIT ?2";
pub const CURSOR_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('backfill:owed-v2',?1,?2) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
pub const STAMP_WRITE_SQL: &str =
    "INSERT OR IGNORE INTO view_derivation_state (name) VALUES ('owed-v2')";
/// Risk 8.2's third repair: the oldest judged rows' subjects, by the judged index, coalesced on the dirty key. At most
/// [`OWED_AGE_REMARKS_PER_PASS`] age marks are ever pending, and the drain judges them after every other mark
/// (`owed_judge::drain_dirty`), so the repair fills the drain's idle room and never delays a source's mark.
pub const AGE_MARK_SQL: &str = "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) SELECT identity,gameId,'age','age',strftime('%s','now') * 1000 FROM owed_rows r INDEXED BY idx_owed_rows_judged WHERE judgedAtMs<?1 AND gameId<>'' AND NOT EXISTS (SELECT 1 FROM owed_dirty d WHERE d.identity=r.identity AND d.gameId=r.gameId AND d.source='age') ORDER BY judgedAtMs LIMIT max(0,?2-(SELECT COUNT(*) FROM owed_dirty INDEXED BY idx_owed_dirty_source_key WHERE sourceKey='age' AND source='age')) ON CONFLICT(identity,gameId,source,sourceKey) DO NOTHING";

/// The D2-OWED lens's MEDIUM-5: the subjects of KEYLESS rows (`judgedAtMs` NULL: an old worker's write in the deploy
/// window, or a rollback to the old walk and forward again), marked `age` in the same room as [`AGE_MARK_SQL`]: run
/// first in one batch, the two together never past [`OWED_AGE_REMARKS_PER_PASS`] pending. The judge rewrites each
/// with its keys, so the set drains; a game no source names any more is judged away.
pub const AGE_MARK_UNJUDGED_SQL: &str = "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) SELECT identity,gameId,'age','age',strftime('%s','now') * 1000 FROM (SELECT DISTINCT r.identity,r.gameId FROM owed_rows r INDEXED BY idx_owed_rows_unjudged WHERE r.judgedAtMs IS NULL AND r.gameId<>'' AND NOT EXISTS (SELECT 1 FROM owed_dirty d WHERE d.identity=r.identity AND d.gameId=r.gameId AND d.source='age') LIMIT max(0,?1-(SELECT COUNT(*) FROM owed_dirty INDEXED BY idx_owed_dirty_source_key WHERE sourceKey='age' AND source='age'))) WHERE 1 ON CONFLICT(identity,gameId,source,sourceKey) DO NOTHING";

/// Where a pass stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Stop {
    /// The stamp was already written: the one read.
    #[default]
    Stamped,
    /// The overlay's certified migration list does not hold this build's triggers yet.
    Gated,
    /// The gate opened this pass: the cursor armed, nothing judged.
    Armed,
    /// The page was done; more identities may follow.
    Page,
    /// The cursor passed the last identity: the stamp written.
    Finished,
    /// The wall (less the save's reserve) fell.
    Wall,
    /// bsv-low #912 (the D2-ACTOR-B delta lens's NEW-H1): `OWED_ACTOR_READ` is on, so the D1-only judge does not run
    /// (its acknowledgement would remove marks the actor never applied, under rows the read serves from the actor).
    ActorRead,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BackfillReport {
    pub stop: Stop,
    /// Open evictions the evidence phase examined this pass; the hop outputs paying a candidate key (each an evidence
    /// statement run, the writer's checks 1 and 2 deciding the row); the released outpoints its bytes could not judge
    /// (each named for every verified seat). Counted as issued, not by D1's `changes` (which counts trigger writes).
    pub evictions: usize,
    pub evidence_candidates: u64,
    pub missing_outpoints: u64,
    /// The open evictions recorded and passed this pass after [`OWED_BACKFILL_IDENTITY_FAULTS_MAX`] faulted passes
    /// (D2-OWED-FOLD-9), named on the scheduled pass's line and the lever's answer.
    pub skipped_evictions: Vec<String>,
    /// Released spends with no `table` field the phase read (NOTE-A), each eviction logged by txid.
    pub tableless_spends: u64,
    pub identities: usize,
    pub rows: usize,
    pub batches: usize,
    pub cursor: Option<String>,
}

fn rows_of(out: Vec<crate::pot_hook::StmtOut>) -> Result<Vec<Value>, String> {
    out.into_iter()
        .next()
        .map(|o| o.rows)
        .ok_or_else(|| "backfill read returned no statement".to_string())
}

async fn read(db: &impl SqlPort, sql: &str, binds: Vec<Bind>) -> Result<Vec<Value>, String> {
    rows_of(
        db.run_batch(vec![Stmt {
            sql: sql.into(),
            binds,
        }])
        .await?,
    )
}

/// The overlay's migration count through the #893 block (its dirty triggers, `idx_owed_rows_judged`, and the D2-OWED
/// fold's indexes and row-count triggers, which this layer's statements name by `INDEXED BY`). The list is append-only,
/// so a certified count at or past it holds them; pinned against the overlay's list (the app layer does not link the
/// overlay, so the figure is this crate's). The deploy order is the window's (the D2-OWED lens's LOW-5): the overlay
/// first, its certified count read on `/health` at or past this figure, then the app layer.
/// 409 since D2-OWED-FOLD-8: the evidence phase writes `released_hop_evidence_missing` (overlay migrations 408, 409).
pub const OWED_TRIGGERS_MIGRATION_COUNT: u32 = 409;

/// The gate: the overlay certified a migration list that holds the triggers (the rows' marks) and the age index.
pub fn gate_open(count: Option<f64>) -> bool {
    count.is_some_and(|c| c >= f64::from(OWED_TRIGGERS_MIGRATION_COUNT))
}

/// One identity's backfill: its marks (unless already open), its games judged a batch at a time. `Ok(None)`: the wall.
async fn backfill_identity(
    db: &impl JudgeDb,
    deadline: &Deadline,
    identity: &str,
    resume: bool,
    size: usize,
    report: &mut BackfillReport,
) -> Result<Option<usize>, String> {
    let now = db.now_ms() as i64;
    if !resume {
        let marked = deadline
            .run_reserving(db.run_batch(vec![
                Stmt {
                    sql: MARK_SQL.into(),
                    binds: vec![Bind::Text(identity.into())],
                },
                Stmt {
                    sql: OPEN_SQL.into(),
                    binds: vec![Bind::Text(identity.into()), Bind::Int(now)],
                },
                Stmt {
                    sql: TRUNCATED_RESET_SQL.into(),
                    binds: vec![Bind::Text(identity.into())],
                },
            ]))
            .await;
        match marked {
            None => return Ok(None),
            Some(r) => {
                r?;
            }
        }
    }
    let mut rows = 0;
    let mut judged_here = 0;
    let mut last: Option<Vec<String>> = None;
    loop {
        let Some(page) = deadline
            .run_reserving(read(
                db,
                GAMES_SQL,
                vec![Bind::Text(identity.into()), Bind::Int(size as i64)],
            ))
            .await
        else {
            return Ok(None);
        };
        let games: Vec<String> = page?
            .iter()
            .filter_map(|r| r["gameId"].as_str().map(str::to_owned))
            .collect();
        if games.is_empty() {
            return Ok(Some(rows));
        }
        if last.as_ref() == Some(&games) {
            return Err(format!("backfill marks of {identity} did not clear"));
        }
        let Some(judged) = deadline
            .run_reserving(owed_judge::judge_subjects(db, identity, &games, "backfill"))
            .await
        else {
            if judged_here == 0 {
                // No batch of this identity fits the pass: the next pass judges half as many (the save's reserve
                // carries the write; a lost one costs one more pass at this size).
                let half = (size / 2).max(1);
                db.log(&format!(
                    "[owed-backfill] {identity}: a batch of {size} did not finish inside the wall; the next pass judges {half}"
                ));
                let row = serde_json::json!({"identity": identity, "size": half});
                if let Some(r) = deadline
                    .run(db.run_batch(vec![Stmt {
                        sql: BATCH_SQL.into(),
                        binds: vec![Bind::Text(row.to_string()), Bind::Int(now)],
                    }]))
                    .await
                {
                    r?;
                }
            }
            return Ok(None);
        };
        rows = judged?.rows;
        judged_here += 1;
        report.batches += 1;
        last = Some(games);
    }
}

/// The released `pot_records` outpoints of a ledger row (an entry with no `table` field is one, as the old walk read it),
/// and how many of them carried no `table` field (D2-OWED-FOLD-9, the fifth delta lens's NOTE-A: the writer's
/// evidence statement requires the field, so such an entry with readable bytes gets neither row; never written by the
/// overlay since 45c4a63b, it is counted and named rather than silent).
fn released_of(raw: Option<&str>) -> (Vec<(String, u32)>, usize) {
    let Some(Ok(Value::Array(entries))) = raw.map(serde_json::from_str::<Value>) else {
        return (Vec::new(), 0);
    };
    let mut out = Vec::new();
    let mut tableless = 0;
    for e in entries {
        if e.get("table")
            .and_then(Value::as_str)
            .is_some_and(|t| t != "pot_records")
        {
            continue;
        }
        let (Some(txid), Some(vout)) = (
            e.get("txid").and_then(Value::as_str),
            e.get("vout")
                .and_then(Value::as_u64)
                .and_then(|v| u32::try_from(v).ok()),
        ) else {
            continue;
        };
        if txid.len() != 64 || !txid.bytes().all(|b| b.is_ascii_hexdigit()) {
            continue;
        }
        let hop = (txid.to_ascii_lowercase(), vout);
        if !out.contains(&hop) {
            tableless += usize::from(e.get("table").is_none());
            out.push(hop);
        }
    }
    (out, tableless)
}

fn first_beef(out: &[crate::pot_hook::StmtOut]) -> Option<Vec<u8>> {
    out.iter()
        .filter_map(|o| o.rows.first())
        .filter_map(|r| r["beef"].as_str())
        .find_map(crate::logic::decode_beef_hex)
}

/// `f` under the deadline less its reserve, or awaited whole with none (the age repair's retry: its caller races it).
async fn within<F: std::future::Future>(deadline: Option<&Deadline>, f: F) -> Option<F::Output> {
    match deadline {
        Some(d) => d.run_reserving(f).await,
        None => Some(f.await),
    }
}

/// One open eviction: its reads, then one write batch (its evidence or missing rows, then `tail`: the phase's cursor
/// and the fault count cleared when the count names it; the retry's end of its skip). `Ok(None)`: the wall fell first.
/// The report's figures are added only once the batch landed.
async fn evidence_one(
    db: &impl SqlPort,
    deadline: Option<&Deadline>,
    txid: &str,
    released: &[(String, u32)],
    tail: Vec<Stmt>,
    report: &mut BackfillReport,
) -> Result<Option<()>, String> {
    use overlay_discovery::pot::released_evidence::{evidence_writes, EvidenceWrite};
    let Some(reads) = within(
        deadline,
        db.run_batch(vec![
            Stmt {
                sql: SEAT_KEYS_SQL.into(),
                binds: vec![Bind::Text(txid.to_owned())],
            },
            Stmt {
                sql: STORED_BEEF_SQL.into(),
                binds: vec![Bind::Text(txid.to_owned())],
            },
            Stmt {
                sql: POT_BEEF_SQL.into(),
                binds: vec![Bind::Text(txid.to_owned())],
            },
        ]),
    )
    .await
    else {
        return Ok(None);
    };
    let reads = reads?;
    let seat_keys: Vec<String> = reads
        .first()
        .map(|o| {
            o.rows
                .iter()
                .filter_map(|r| r["key"].as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    let mut beef = first_beef(reads.get(1..).unwrap_or_default());
    if beef.is_none() {
        let Some(evicted) = within(
            deadline,
            db.run_batch(vec![Stmt {
                sql: EVICTED_BEEF_SQL.into(),
                binds: vec![Bind::Text(txid.to_owned())],
            }]),
        )
        .await
        else {
            return Ok(None);
        };
        beef = match evicted {
            Ok(out) => first_beef(&out),
            // the shadow the overlay makes at its first eviction: none yet is no bytes
            Err(e) if e.contains("no such table") => None,
            Err(e) => return Err(e),
        };
    }
    let (mut candidates, mut missing) = (0u64, 0u64);
    let mut writes = Vec::new();
    for w in evidence_writes(beef.as_deref(), txid, released, &seat_keys) {
        match w {
            EvidenceWrite::Found(f) => {
                candidates += 1;
                writes.push(Stmt {
                    sql: EVIDENCE_INSERT_SQL.into(),
                    binds: vec![
                        Bind::Text(txid.to_owned()),
                        Bind::Text(f.hop_txid),
                        Bind::Int(i64::from(f.hop_vout)),
                        Bind::Text(f.seat_key),
                        Bind::Int(f.sats),
                    ],
                });
            }
            EvidenceWrite::Missing { list, bytes_read } => {
                missing += serde_json::from_str::<Vec<Value>>(&list).map_or(0, |l| l.len() as u64);
                writes.push(Stmt {
                    sql: MISSING_INSERT_SQL.into(),
                    binds: vec![
                        Bind::Text(txid.to_owned()),
                        Bind::Text(list),
                        Bind::Int(i64::from(bytes_read)),
                    ],
                });
            }
        }
    }
    writes.extend(tail);
    let Some(out) = within(deadline, db.run_batch(writes)).await else {
        return Ok(None);
    };
    out?;
    report.evidence_candidates += candidates;
    report.missing_outpoints += missing;
    Ok(Some(()))
}

/// The evidence phase (D2-OWED-FOLD-8): the open evictions past `cursor` that name no evidence row, each written by
/// the overlay's eviction writer's own checks and statements, its cursor saved in the same batch. `Ok(true)`: the
/// phase is done, its cursor at [`EVICTIONS_DONE`]; `Ok(false)`: the wall fell first.
///
/// D2-OWED-FOLD-9 (the fifth delta lens's DELTA5-L1): an eviction whose reads or batch fault is counted across passes
/// on [`OWED_BACKFILL_EVICTION_FAULTS`] (`faults`, as the gate read it); the pass is the fault's below the bound, and
/// at [`OWED_BACKFILL_IDENTITY_FAULTS_MAX`] the eviction is recorded (`backfill:owed-v2:skipped-eviction:<txid>`, its
/// fault), counted, named on the report, and the cursor moves past it, so one eviction cannot hold the identities and
/// the stamp back. A page read that faults names no eviction and stays the pass's fault.
async fn evidence_phase(
    db: &impl JudgeDb,
    deadline: &Deadline,
    mut cursor: String,
    faults: &Value,
    report: &mut BackfillReport,
) -> Result<bool, String> {
    loop {
        if cursor == EVICTIONS_DONE {
            return Ok(true);
        }
        let Some(page) = deadline
            .run_reserving(read(
                db,
                EVICTIONS_SQL,
                vec![
                    Bind::Text(cursor.clone()),
                    Bind::Int(OWED_BACKFILL_EVICTIONS_PER_PAGE as i64),
                ],
            ))
            .await
        else {
            return Ok(false);
        };
        let page = page?;
        let short = page.len() < OWED_BACKFILL_EVICTIONS_PER_PAGE;
        for e in &page {
            let txid = e["txid"]
                .as_str()
                .ok_or("open eviction without a txid")?
                .to_owned();
            let (released, tableless) = released_of(e["releasedSpends"].as_str());
            if tableless > 0 {
                report.tableless_spends += tableless as u64;
                db.log(&format!("[owed-backfill] eviction {txid}: {tableless} released spend(s) carry no table field, read as pot_records (the writer's evidence statement refuses them)"));
            }
            let counted = faults["txid"] == txid.as_str();
            let now = db.now_ms() as i64;
            let mut tail = vec![Stmt {
                sql: EVICTIONS_CURSOR_SQL.into(),
                binds: vec![Bind::Text(txid.clone()), Bind::Int(now)],
            }];
            if counted {
                tail.push(Stmt {
                    sql: EVICTION_FAULTS_SQL.into(),
                    binds: vec![Bind::Text("{}".into()), Bind::Int(now)],
                });
            }
            match evidence_one(db, Some(deadline), &txid, &released, tail, report).await {
                Ok(None) => return Ok(false),
                Ok(Some(())) => report.evictions += 1,
                Err(error) => {
                    let seen = if counted {
                        faults["faults"].as_u64().unwrap_or(0) as usize
                    } else {
                        0
                    } + 1;
                    let now = db.now_ms() as i64;
                    if seen < OWED_BACKFILL_IDENTITY_FAULTS_MAX {
                        let count = serde_json::json!({"txid": txid, "faults": seen});
                        return note_fault(db, deadline, EVICTION_FAULTS_SQL, count, now, error)
                            .await;
                    }
                    db.log(&format!(
                        "[owed-backfill] eviction {txid}: the evidence phase faulted on {seen} passes ({error}); recorded, no evidence written, passed (the age repair retries it once the skip is a day old)"
                    ));
                    let reason = skip_value(&error, 0);
                    let Some(out) = deadline
                        .run(db.run_batch(vec![
                            Stmt {
                                sql: SKIPPED_EVICTION_SQL.into(),
                                binds: vec![
                                    Bind::Text(txid.clone()),
                                    Bind::Text(reason),
                                    Bind::Int(now),
                                ],
                            },
                            Stmt {
                                sql: EVICTIONS_CURSOR_SQL.into(),
                                binds: vec![Bind::Text(txid.clone()), Bind::Int(now)],
                            },
                            Stmt {
                                sql: EVICTION_FAULTS_SQL.into(),
                                binds: vec![Bind::Text("{}".into()), Bind::Int(now)],
                            },
                        ]))
                        .await
                    else {
                        return Ok(false);
                    };
                    out?;
                    db.counters(vec![(COUNTER_SKIPPED_EVICTIONS.into(), 1)])
                        .await;
                    report.skipped_evictions.push(txid.clone());
                }
            }
            cursor = txid;
        }
        if short {
            let Some(done) = deadline
                .run(db.run_batch(vec![Stmt {
                    sql: EVICTIONS_CURSOR_SQL.into(),
                    binds: vec![
                        Bind::Text(EVICTIONS_DONE.into()),
                        Bind::Int(db.now_ms() as i64),
                    ],
                }]))
                .await
            else {
                return Ok(false);
            };
            done?;
            return Ok(true);
        }
    }
}

/// A faulted pass's count write (the identity's [`FAULTS_SQL`], the eviction's [`EVICTION_FAULTS_SQL`]), raced by the
/// pass's whole deadline as the cursor's save is (D2-OWED-FOLD-10, the sixth delta lens's NOTE-3): a write the wall has
/// fallen on is not made, so the count can only lag and the bound come later, never earlier. The pass answers the
/// fault either way; a count write that faults answers its own fault, as before.
async fn note_fault<T>(
    db: &impl SqlPort,
    deadline: &Deadline,
    sql: &str,
    count: Value,
    now: i64,
    error: String,
) -> Result<T, String> {
    let write = db.run_batch(vec![Stmt {
        sql: sql.into(),
        binds: vec![Bind::Text(count.to_string()), Bind::Int(now)],
    }]);
    match deadline.run(write).await {
        Some(out) => {
            out?;
        }
        None => db.log(&format!(
            "[owed-backfill] the wall fell before the fault's count was written ({count}); the next fault counts again"
        )),
    }
    Err(error)
}

/// The faulted eviction's count as the gate read it (`Null` when none).
fn eviction_faults(gate: &Value) -> Value {
    gate["evictionFaults"]
        .as_str()
        .and_then(|f| serde_json::from_str(f).ok())
        .unwrap_or_default()
}

async fn flush_evidence_counters(db: &impl SqlPort, report: &BackfillReport) {
    if report.evictions > 0 {
        db.counters(vec![
            (COUNTER_EVICTIONS.into(), report.evictions as u64),
            (COUNTER_EVIDENCE.into(), report.evidence_candidates),
            (COUNTER_MISSING.into(), report.missing_outpoints),
        ])
        .await;
    }
}

/// One scheduled pass of the backfill under `deadline` (its wall and the cursor save's reserve).
pub async fn backfill_pass(
    db: &impl JudgeDb,
    deadline: &Deadline,
) -> Result<BackfillReport, String> {
    let mut report = BackfillReport::default();
    if !read(db, STAMP_SQL, vec![]).await?.is_empty() {
        return Ok(report);
    }
    // NEW-H1: never a D1-only judge (and its acknowledgement) under the actor's read.
    if db.actor_reads() {
        report.stop = Stop::ActorRead;
        return Ok(report);
    }
    let gate = read(db, GATE_SQL, vec![]).await?;
    let gate = gate.first().cloned().unwrap_or_default();
    if !gate_open(gate["count"].as_f64()) {
        report.stop = Stop::Gated;
        return Ok(report);
    }
    let Some(cursor) = gate["cursor"].as_str().map(str::to_owned) else {
        db.run_batch(vec![Stmt {
            sql: ARM_SQL.into(),
            binds: vec![Bind::Int(db.now_ms() as i64)],
        }])
        .await?;
        report.stop = Stop::Armed;
        return Ok(report);
    };
    let open = gate["open"].as_str().map(str::to_owned);
    let faults: Value = gate["faults"]
        .as_str()
        .and_then(|f| serde_json::from_str(f).ok())
        .unwrap_or_default();
    let batch: Value = gate["batch"]
        .as_str()
        .and_then(|f| serde_json::from_str(f).ok())
        .unwrap_or_default();
    report.cursor = Some(cursor.clone());
    // D2-OWED-FOLD-8: the evidence of the evictions made before the writer, before any identity is judged.
    let evictions = gate["evictions"].as_str().unwrap_or("").to_owned();
    if evictions != EVICTIONS_DONE {
        let phase = evidence_phase(
            db,
            deadline,
            evictions,
            &eviction_faults(&gate),
            &mut report,
        )
        .await;
        flush_evidence_counters(db, &report).await;
        if !phase? {
            report.stop = Stop::Wall;
            return Ok(report);
        }
    }
    let page = read(
        db,
        IDENTITIES_SQL,
        vec![
            Bind::Text(cursor),
            Bind::Int(OWED_BACKFILL_IDENTITIES_PER_PASS as i64),
        ],
    )
    .await?;
    let identities: Vec<String> = page
        .iter()
        .filter_map(|r| r["identity"].as_str().map(str::to_owned))
        .collect();
    let last_page = identities.len() < OWED_BACKFILL_IDENTITIES_PER_PASS;
    if identities.is_empty() {
        db.run_batch(vec![Stmt {
            sql: STAMP_WRITE_SQL.into(),
            binds: vec![],
        }])
        .await?;
        report.stop = Stop::Finished;
        return Ok(report);
    }
    report.stop = Stop::Page;
    let result = async {
        for (n, identity) in identities.iter().enumerate() {
            let resume = open.as_deref() == Some(identity.as_str());
            let mut save = vec![Stmt {
                sql: CURSOR_SQL.into(),
                binds: vec![Bind::Text(identity.clone()), Bind::Int(db.now_ms() as i64)],
            }];
            let size = if resume {
                batch_size_for(&batch, identity)
            } else {
                OWED_BACKFILL_GAMES_PER_BATCH
            };
            let rows = match backfill_identity(db, deadline, identity, resume, size, &mut report)
                .await
            {
                Ok(Some(rows)) => rows,
                Ok(None) => {
                    report.stop = Stop::Wall;
                    return Ok::<(), String>(());
                }
                Err(error) => {
                    // The D2-OWED lens's LOW-1: counted per identity across passes; at the bound the identity is
                    // recorded, its marks left for the drain, and the cursor moves past it.
                    let seen = if faults["identity"] == identity.as_str() {
                        faults["faults"].as_u64().unwrap_or(0) as usize
                    } else {
                        0
                    } + 1;
                    let now = db.now_ms() as i64;
                    if seen < OWED_BACKFILL_IDENTITY_FAULTS_MAX {
                        let count = serde_json::json!({"identity": identity, "faults": seen});
                        return note_fault(db, deadline, FAULTS_SQL, count, now, error).await;
                    }
                    db.log(&format!(
                        "[owed-backfill] {identity}: the judge faulted on {seen} passes ({error}); recorded, its marks left for the drain, passed"
                    ));
                    let reason: String = error.chars().take(200).collect();
                    save.push(Stmt {
                        sql: SKIPPED_SQL.into(),
                        binds: vec![Bind::Text(identity.clone()), Bind::Text(reason), Bind::Int(now)],
                    });
                    db.counters(vec![(COUNTER_SKIPPED.into(), 1)]).await;
                    0
                }
            };
            let finished = last_page && n + 1 == identities.len();
            if batch["identity"] == identity.as_str() {
                save.push(Stmt {
                    sql: BATCH_SQL.into(),
                    binds: vec![Bind::Text("{}".into()), Bind::Int(db.now_ms() as i64)],
                });
            }
            if faults["identity"] == identity.as_str() {
                save.push(Stmt {
                    sql: FAULTS_SQL.into(),
                    binds: vec![Bind::Text("{}".into()), Bind::Int(db.now_ms() as i64)],
                });
            }
            if finished {
                save.push(Stmt {
                    sql: STAMP_WRITE_SQL.into(),
                    binds: vec![],
                });
            }
            // The save takes the reserve the judges left it.
            match deadline.run(db.run_batch(save)).await {
                None => {
                    report.stop = Stop::Wall;
                    return Ok(());
                }
                Some(r) => {
                    r?;
                }
            }
            report.identities += 1;
            report.rows += rows;
            report.cursor = Some(identity.clone());
            if finished {
                report.stop = Stop::Finished;
            }
        }
        Ok(())
    }
    .await;
    if report.identities > 0 {
        db.counters(vec![
            (COUNTER_IDENTITIES.into(), report.identities as u64),
            (COUNTER_ROWS.into(), report.rows as u64),
        ])
        .await;
    }
    result.map(|()| report)
}

/// The evidence phase alone (`POST /internal/owed-backfill-pass?evidence=1`, the tier's measure of its cost per
/// eviction): behind the same stamp and gate as [`backfill_pass`], only once the cursor is armed; `Stop::Page` when the
/// phase is done, `Stop::Wall` when the wall fell first. A later [`backfill_pass`] finds it done and goes on.
pub async fn evidence_pass(
    db: &impl JudgeDb,
    deadline: &Deadline,
) -> Result<BackfillReport, String> {
    let mut report = BackfillReport::default();
    if !read(db, STAMP_SQL, vec![]).await?.is_empty() {
        return Ok(report);
    }
    let gate = read(db, GATE_SQL, vec![]).await?;
    let gate = gate.first().cloned().unwrap_or_default();
    if !gate_open(gate["count"].as_f64()) {
        report.stop = Stop::Gated;
        return Ok(report);
    }
    if gate["cursor"].as_str().is_none() {
        report.stop = Stop::Gated;
        return Ok(report);
    }
    let evictions = gate["evictions"].as_str().unwrap_or("").to_owned();
    report.stop = Stop::Page;
    if evictions != EVICTIONS_DONE {
        let phase = evidence_phase(
            db,
            deadline,
            evictions,
            &eviction_faults(&gate),
            &mut report,
        )
        .await;
        flush_evidence_counters(db, &report).await;
        if !phase? {
            report.stop = Stop::Wall;
        }
    }
    Ok(report)
}

/// A skipped eviction's row value: its fault clipped to 200 characters and the faults of its current retry round.
fn skip_value(error: &str, faults: usize) -> String {
    let reason: String = error.chars().take(200).collect();
    serde_json::json!({ "reason": reason, "faults": faults }).to_string()
}

/// What one age repair did: the marks, and the skipped evictions it read, retried, ended and skipped again.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AgeRepair {
    pub marked: u64,
    /// The retries made this pass (at most [`OWED_SKIPPED_EVICTION_RETRIES_PER_PASS`]).
    pub retried: usize,
    /// Retries that landed: the eviction's rows written, its skip ended.
    pub recovered: Vec<String>,
    /// Retries whose round reached its third fault: skipped again, a day from now.
    pub reskipped: Vec<String>,
    /// Skips ended because the eviction no longer needs its evidence (readmitted, gone, or the writer wrote it).
    pub resolved: Vec<String>,
    /// The evictions still skipped after this pass, named on the repair's line (oldest skip first, at most
    /// [`OWED_SKIPPED_EVICTIONS_NAMED`] read).
    pub still_skipped: Vec<String>,
    /// D2-OWED-HOME-MARKER: the evictions whose marker-bound seats the marker repair wrote this pass.
    pub marker_repaired: Vec<String>,
    /// The marker repair's cursor after this pass ([`EVICTIONS_DONE`] once it has looked at every eviction).
    pub marker_cursor: Option<String>,
    /// bsv-low #953: the evidence rows the seeding wrote this pass, and its cursor after it.
    pub find_evidence_seeded: u64,
    pub find_evidence_cursor: Option<String>,
}

/// The drain's daily re-mark of rows judged more than 24 h ago. Returns the marks written.
pub async fn mark_aged(db: &impl SqlPort) -> Result<u64, String> {
    age_repair(db).await.map(|r| r.marked)
}

/// The scheduled drain's age repair under the phase (bsv-low #1004, lane FIX-1004, L55 S4). With the D1 judge OFF
/// (Phase C) the `age` marks are NOT written: they are taken from D1's `owed_rows.judgedAtMs`, which no writer moves
/// in Phase C, so the same oldest 128 subjects were re-marked on every scheduled pass, each mark waiting minutes for
/// the sweep's re-publish (no event carries it) while the actor's read counted it `pending`. The rest of the repair
/// (the skipped evictions' retry, the marker repair, the find evidence) runs in every phase.
pub async fn mark_aged_in(db: &impl SqlPort, d1_judges: bool) -> Result<u64, String> {
    age_repair_in(db, d1_judges).await.map(|r| r.marked)
}

/// The age repair (risk 8.2's third repair, run by the scheduled drain on every pass): the aged and keyless rows
/// re-marked, then the skipped evictions retried (D2-OWED-FOLD-10). A skip a day old is retried, at most
/// [`OWED_SKIPPED_EVICTION_RETRIES_PER_PASS`] a pass, oldest first, by the phase's own [`evidence_one`] (its reads,
/// its statements, one write batch ending the skip); a fault is counted on the skip's row, and the round's third
/// fault skips it again from now (counted `owed_backfill_skipped_evictions_total`, logged). Every pass that reads a
/// skip names the still skipped on its line. Its caller races the whole repair; each retry's write is one batch.
pub async fn age_repair(db: &impl SqlPort) -> Result<AgeRepair, String> {
    age_repair_in(db, true).await
}

/// [`age_repair`] with the age marks written only while `mark_age` (the D1 judge on; bsv-low #1004).
pub async fn age_repair_in(db: &impl SqlPort, mark_age: bool) -> Result<AgeRepair, String> {
    let mut repair = AgeRepair::default();
    let marks = if mark_age { age_marks(db).await } else { Ok(0) };
    if let Ok(marked) = marks {
        repair.marked = marked;
    }
    if let Err(error) = retry_skipped_evictions(db, &mut repair).await {
        db.log(&format!(
            "[owed-backfill] the skipped evictions' retry deferred: {error}"
        ));
    }
    if let Err(error) = repair_marker_evictions(db, &mut repair).await {
        db.log(&format!(
            "[owed-backfill] the marker repair deferred: {error}"
        ));
    }
    if let Err(error) = seed_find_evidence(db, &mut repair).await {
        db.log(&format!(
            "[owed-backfill] the find evidence seeding deferred: {error}"
        ));
    }
    if repair.find_evidence_seeded > 0 {
        db.log(&format!(
            "[owed-backfill] find evidence: {} row(s) seeded, cursor {:?}",
            repair.find_evidence_seeded, repair.find_evidence_cursor
        ));
    }
    if !repair.marker_repaired.is_empty() {
        db.log(&format!(
            "[owed-backfill] marker repair: evidence written for {:?}, cursor {:?}",
            repair.marker_repaired, repair.marker_cursor
        ));
    }
    if !repair.still_skipped.is_empty() || repair.retried > 0 || !repair.resolved.is_empty() {
        db.log(&format!(
            "[owed-backfill] skipped evictions: {:?} still skipped, {} retried, recovered {:?}, skipped again {:?}, resolved {:?}",
            repair.still_skipped, repair.retried, repair.recovered, repair.reskipped, repair.resolved
        ));
    }
    marks.map(|_| repair)
}

async fn retry_skipped_evictions(db: &impl SqlPort, repair: &mut AgeRepair) -> Result<(), String> {
    let now = db.now_ms() as i64;
    let rows = read(
        db,
        SKIPPED_EVICTIONS_SQL,
        vec![Bind::Int(OWED_SKIPPED_EVICTIONS_NAMED as i64)],
    )
    .await?;
    let mut found = BackfillReport::default();
    for row in rows {
        let Some(txid) = row["txid"].as_str().map(str::to_owned) else {
            continue;
        };
        let due = row["skippedAt"]
            .as_i64()
            .is_none_or(|at| at <= now - OWED_SKIPPED_EVICTION_RETRY_AFTER_MS);
        if !due || repair.retried + repair.resolved.len() >= OWED_SKIPPED_EVICTION_RETRIES_PER_PASS
        {
            repair.still_skipped.push(txid);
            continue;
        }
        let end = Stmt {
            sql: SKIPPED_EVICTION_END_SQL.into(),
            binds: vec![Bind::Text(txid.clone())],
        };
        if row["open"].as_i64() != Some(1) {
            match db.run_batch(vec![end]).await {
                Ok(_) => repair.resolved.push(txid),
                Err(error) => {
                    db.log(&format!(
                        "[owed-backfill] eviction {txid}: its skip could not be ended ({error})"
                    ));
                    repair.still_skipped.push(txid);
                }
            }
            continue;
        }
        repair.retried += 1;
        let (released, _) = released_of(row["releasedSpends"].as_str());
        match evidence_one(db, None, &txid, &released, vec![end], &mut found).await {
            Ok(_) => {
                found.evictions += 1;
                repair.recovered.push(txid);
            }
            Err(error) => {
                let skip = row["skip"].as_str().unwrap_or_default();
                let faults = serde_json::from_str::<Value>(skip)
                    .ok()
                    .and_then(|v| v["faults"].as_u64())
                    .unwrap_or(0) as usize
                    + 1;
                let write = if faults < OWED_BACKFILL_IDENTITY_FAULTS_MAX {
                    db.log(&format!(
                        "[owed-backfill] eviction {txid}: its retry faulted ({faults} of {OWED_BACKFILL_IDENTITY_FAULTS_MAX} this round): {error}"
                    ));
                    Stmt {
                        sql: SKIPPED_EVICTION_FAULT_SQL.into(),
                        binds: vec![
                            Bind::Text(txid.clone()),
                            Bind::Text(skip_value(&error, faults)),
                        ],
                    }
                } else {
                    db.log(&format!(
                        "[owed-backfill] eviction {txid}: its retry faulted {faults} times ({error}); skipped again, retried a day from now"
                    ));
                    Stmt {
                        sql: SKIPPED_EVICTION_SQL.into(),
                        binds: vec![
                            Bind::Text(txid.clone()),
                            Bind::Text(skip_value(&error, 0)),
                            Bind::Int(now),
                        ],
                    }
                };
                match db.run_batch(vec![write]).await {
                    Ok(_) if faults >= OWED_BACKFILL_IDENTITY_FAULTS_MAX => {
                        db.counters(vec![(COUNTER_SKIPPED_EVICTIONS.into(), 1)])
                            .await;
                        repair.reskipped.push(txid.clone());
                    }
                    Ok(_) => {}
                    Err(e) => db.log(&format!(
                        "[owed-backfill] eviction {txid}: its retry's fault was not counted ({e})"
                    )),
                }
                repair.still_skipped.push(txid);
            }
        }
    }
    flush_evidence_counters(db, &found).await;
    Ok(())
}

/// D2-OWED-HOME-MARKER: the evictions the writer made before it took a seat's VERIFIED hop marker as check 1's
/// attestation (a seat whose party filing never landed got no evidence row, so its swept hop was served "cannot tell
/// which committed home is yours"). The judge cannot reach such an eviction from the hop at read time (the ledger's
/// `releasedSpends` is unindexed JSON, and the hop's seat links go with the evicted pot), so the drain's age repair
/// walks the ledger once by its primary key, [`OWED_MARKER_REPAIR_WINDOW`] evictions a pass from its cursor, and writes
/// each eviction [`marker_unwritten`] names through the phase's own [`evidence_one`] (the writer's statements, which
/// now take the marker). A candidate that faults is recorded as a skipped eviction (the retry's `open` reads the same
/// predicate) and passed; the cursor is saved after the window, `~` once a window comes back short. Done, a pass costs
/// one primary-key read.
async fn repair_marker_evictions(db: &impl SqlPort, repair: &mut AgeRepair) -> Result<(), String> {
    let cursor = read(db, MARKER_REPAIR_CURSOR_SQL, vec![])
        .await?
        .first()
        .and_then(|r| r["cursor"].as_str().map(str::to_owned))
        .unwrap_or_default();
    if cursor == EVICTIONS_DONE {
        repair.marker_cursor = Some(cursor);
        return Ok(());
    }
    let window = read(
        db,
        MARKER_REPAIR_SQL,
        vec![
            Bind::Text(cursor.clone()),
            Bind::Int(OWED_MARKER_REPAIR_WINDOW as i64),
        ],
    )
    .await?;
    let mut found = BackfillReport::default();
    let mut last = cursor;
    for row in &window {
        if let Some(seen) = row["seen"].as_str() {
            last = seen.to_owned();
        }
        let Some(txid) = row["txid"].as_str().map(str::to_owned) else {
            continue;
        };
        let (released, _) = released_of(row["releasedSpends"].as_str());
        match evidence_one(db, None, &txid, &released, vec![], &mut found).await {
            Ok(_) => {
                found.evictions += 1;
                repair.marker_repaired.push(txid);
            }
            Err(error) => {
                db.log(&format!(
                    "[owed-backfill] eviction {txid}: the marker repair faulted ({error}); recorded as skipped, the age repair retries it once the skip is a day old"
                ));
                db.run_batch(vec![Stmt {
                    sql: SKIPPED_EVICTION_SQL.into(),
                    binds: vec![
                        Bind::Text(txid.clone()),
                        Bind::Text(skip_value(&error, 0)),
                        Bind::Int(db.now_ms() as i64),
                    ],
                }])
                .await?;
                db.counters(vec![(COUNTER_SKIPPED_EVICTIONS.into(), 1)])
                    .await;
            }
        }
    }
    let next = if window.len() < OWED_MARKER_REPAIR_WINDOW {
        EVICTIONS_DONE.to_owned()
    } else {
        last
    };
    db.run_batch(vec![Stmt {
        sql: MARKER_REPAIR_SAVE_SQL.into(),
        binds: vec![Bind::Text(next.clone()), Bind::Int(db.now_ms() as i64)],
    }])
    .await?;
    repair.marker_cursor = Some(next);
    flush_evidence_counters(db, &found).await;
    Ok(())
}

/// bsv-low #953: the one-time seeding of the belt's evidence for the finds made before the fold, one page of
/// [`FIND_EVIDENCE_SEED_PAGE`] stored spenders a pass by the store's primary key; the seats marked and the rows
/// written in one batch with the cursor; `~` once a page comes back short. Done, a pass costs one primary-key read.
async fn seed_find_evidence(db: &impl SqlPort, repair: &mut AgeRepair) -> Result<(), String> {
    let cursor = read(db, FIND_EVIDENCE_CURSOR_SQL, vec![])
        .await?
        .first()
        .and_then(|r| r["cursor"].as_str().map(str::to_owned))
        .unwrap_or_default();
    if cursor == EVICTIONS_DONE {
        repair.find_evidence_cursor = Some(cursor);
        return Ok(());
    }
    let page_binds = || {
        vec![
            Bind::Text(cursor.clone()),
            Bind::Int(FIND_EVIDENCE_SEED_PAGE as i64),
        ]
    };
    let page = read(db, FIND_EVIDENCE_PAGE_SQL, page_binds()).await?;
    let n = page.first().and_then(|r| r["n"].as_u64()).unwrap_or(0) as usize;
    let next = if n < FIND_EVIDENCE_SEED_PAGE {
        EVICTIONS_DONE.to_owned()
    } else {
        page.first()
            .and_then(|r| r["last"].as_str().map(str::to_owned))
            .unwrap_or_else(|| EVICTIONS_DONE.to_owned())
    };
    let out = db
        .run_batch(vec![
            Stmt {
                sql: FIND_EVIDENCE_MARK_SQL.into(),
                binds: page_binds(),
            },
            Stmt {
                sql: FIND_EVIDENCE_SEED_SQL.into(),
                binds: page_binds(),
            },
            Stmt {
                sql: FIND_EVIDENCE_SAVE_SQL.into(),
                binds: vec![Bind::Text(next.clone()), Bind::Int(db.now_ms() as i64)],
            },
        ])
        .await?;
    repair.find_evidence_seeded = out.get(1).and_then(|o| o.changes).unwrap_or(0);
    repair.find_evidence_cursor = Some(next);
    Ok(())
}

/// The aged and keyless rows' marks (risk 8.2's third repair, the D2-OWED lens's MEDIUM-5).
async fn age_marks(db: &impl SqlPort) -> Result<u64, String> {
    let now = db.now_ms() as i64;
    let out = db
        .run_batch(vec![
            Stmt {
                sql: AGE_MARK_UNJUDGED_SQL.into(),
                binds: vec![Bind::Int(OWED_AGE_REMARKS_PER_PASS as i64)],
            },
            Stmt {
                sql: AGE_MARK_SQL.into(),
                binds: vec![
                    Bind::Int(now - OWED_AGE_REMARK_AFTER_MS),
                    Bind::Int(OWED_AGE_REMARKS_PER_PASS as i64),
                ],
            },
        ])
        .await?;
    let marked = out.iter().filter_map(|o| o.changes).sum::<u64>();
    if marked > 0 {
        db.counters(vec![(COUNTER_AGED.into(), marked)]).await;
    }
    Ok(marked)
}

/// The scheduled pass's task: the judge's production port under the backfill's own wall.
pub async fn run_env(env: &worker::Env) -> Result<BackfillReport, String> {
    let deadline =
        crate::routes::worker_deadline(OWED_BACKFILL_WALL_MS, OWED_BACKFILL_SAVE_RESERVE_MS);
    crate::schema::ensure_latch_columns(env).await;
    let tip = crate::routes::chaintracks_present_height_env(env, "owed")
        .await
        .ok();
    let judge = owed_judge::D1Judge::new(env, tip, crate::routes::written_off_before_ms_env(env))
        .map_err(|e| e.to_string())?;
    backfill_pass(&judge, &deadline).await
}

/// `?evidence=1`: the evidence phase alone under the backfill's own wall (the tier's lever, as `?aged=1`).
async fn evidence_env(env: &worker::Env) -> Result<BackfillReport, String> {
    let deadline =
        crate::routes::worker_deadline(OWED_BACKFILL_WALL_MS, OWED_BACKFILL_SAVE_RESERVE_MS);
    crate::schema::ensure_latch_columns(env).await;
    let judge = owed_judge::D1Judge::new(env, None, crate::routes::written_off_before_ms_env(env))
        .map_err(|e| e.to_string())?;
    evidence_pass(&judge, &deadline).await
}

/// `POST /internal/owed-backfill-pass` (bearer `INTERNAL_TOKEN`): one pass, answered with its report (the tier's).
pub async fn lever(req: worker::Request, env: &worker::Env) -> worker::Result<worker::Response> {
    if !crate::internal_events::internal_bearer_ok(&req, env) {
        return worker::Response::error("unauthorized", 401);
    }
    // bsv-low #912 (the D2-ACTOR-B lens's LOW-4): the lever judges in D1, so it is refused while the D1 judge is off
    // (Phase C); Phase C's rollback re-judge is `POST /internal/owed-v3-pass`.
    if !crate::owed_actor::d1_judge_on(
        env.var(crate::owed_actor::D1_JUDGE_FLAG)
            .ok()
            .map(|v| v.to_string()),
    ) {
        return worker::Response::error(
            "the D1 judge is off (OWED_D1_JUDGE=false): the owed-v2 lever does not run",
            409,
        );
    }
    // The D2-ACTOR-B delta lens's NEW-H1: nor while the read serves the actor's rows (`OWED_ACTOR_READ`): the D1-only
    // judge would acknowledge marks the actor never applied. Its `?aged=1` repair writes marks only and still runs.
    let aged_only = req
        .url()
        .is_ok_and(|u| u.query_pairs().any(|(k, v)| k == "aged" && v == "1"));
    if !aged_only
        && crate::owed_actor::flag_on(
            env.var(crate::owed_actor::READ_FLAG)
                .ok()
                .map(|v| v.to_string()),
        )
    {
        return worker::Response::error(
            "the owed actor serves the read (OWED_ACTOR_READ=true): the owed-v2 lever's D1-only judge does not run",
            409,
        );
    }
    // `?aged=1`: the drain's age repair alone (`mark_aged`, the D2-OWED lens's LOW-3: the tier runs it on local D1)
    if req
        .url()
        .is_ok_and(|u| u.query_pairs().any(|(k, v)| k == "aged" && v == "1"))
    {
        let sql = crate::routes::D1Sql::of(env)?;
        return match age_repair(&sql).await {
            Ok(r) => worker::Response::from_json(&serde_json::json!({
                "aged": r.marked,
                "skippedEvictions": r.still_skipped,
                "retriedEvictions": r.retried,
                "recoveredEvictions": r.recovered,
                "reskippedEvictions": r.reskipped,
                "resolvedEvictions": r.resolved,
                "markerRepaired": r.marker_repaired,
                "markerCursor": r.marker_cursor,
            })),
            Err(e) => worker::Response::error(format!("owed age repair faulted: {e}"), 503),
        };
    }
    let evidence = req
        .url()
        .is_ok_and(|u| u.query_pairs().any(|(k, v)| k == "evidence" && v == "1"));
    let run = if evidence {
        evidence_env(env).await
    } else {
        run_env(env).await
    };
    match run {
        Ok(r) => worker::Response::from_json(&serde_json::json!({
            "stop": format!("{:?}", r.stop),
            "evictions": r.evictions,
            "evidenceCandidates": r.evidence_candidates,
            "missingOutpoints": r.missing_outpoints,
            "skippedEvictions": r.skipped_evictions,
            "tablelessSpends": r.tableless_spends,
            "identities": r.identities,
            "rows": r.rows,
            "batches": r.batches,
            "cursor": r.cursor,
        })),
        Err(e) => worker::Response::error(format!("owed backfill pass faulted: {e}"), 503),
    }
}

#[cfg(test)]
mod tests {
    fn squash(s: &str) -> String {
        s.split_whitespace().collect()
    }
    /// The wiring: the backfill is its own scheduled task under its own wall and save reserve, handed after the
    /// tracker's; the drain re-marks aged rows before it drains; the lever runs the task's function.
    #[test]
    fn owed893_backfill_the_scheduled_pass_runs_it_as_its_own_task_and_the_drain_remarks_aged_rows()
    {
        let lib = squash(include_str!("lib.rs"));
        let cron = &lib[lib.find("asyncfnscheduled(").unwrap()..];
        let cron = &cron[..cron.find("#[event(fetch)]").unwrap()];
        assert!(cron.contains(&squash(
            ".phase(\"owed.backfill\", owed_backfill::run_env(&env_backfill))"
        )));
        let tick = cron
            .find(&squash(
                "pot_hook::schedule_tick(&|f| ctx.wait_until(f), drain, intake, tracker);",
            ))
            .unwrap();
        let handed = cron.find(&squash("ctx.wait_until(backfill);")).unwrap();
        assert!(tick < handed, "handed after the tracker's task");
        assert!(cron.contains(&squash("let work = owed_belt::drain(&env_owed);")));
        let actor = squash(include_str!("board_view.rs"));
        assert!(actor.contains(&squash("crate::owed_belt::drain_env(&self.env, true).await")));
        let belt = squash(include_str!("owed_belt.rs"));
        let aged = belt
            .find(&squash(
                "if repair_age { if let Err(error) = crate::owed_backfill::mark_aged_in(&judge, owed_judge::JudgeDb::d1_judges(&judge)).await",
            ))
            .unwrap();
        let drain = belt
            .find(&squash("owed_judge::drain_dirty(&judge,"))
            .unwrap();
        assert!(aged < drain, "the scheduled actor turn re-marks age before draining");
        assert!(lib.contains(&squash(
            "req.path() == \"/internal/owed-backfill-pass\" { let resp = owed_backfill::lever(req, &env).await; push_coalesce::flush_after(&ctx, &env); return resp; }"
        )));
        let me = squash(include_str!("owed_backfill.rs"));
        assert!(me.contains(&squash(
            "crate::routes::worker_deadline(OWED_BACKFILL_WALL_MS, OWED_BACKFILL_SAVE_RESERVE_MS);"
        )));
        assert_eq!(
            (
                super::OWED_BACKFILL_WALL_MS,
                super::OWED_BACKFILL_SAVE_RESERVE_MS
            ),
            (20_000, 3_000)
        );
        assert_eq!(super::OWED_BACKFILL_IDENTITIES_PER_PASS, 128);
        assert_eq!(super::OWED_BACKFILL_GAMES_PER_BATCH, 16);
        assert_eq!(super::OWED_AGE_REMARKS_PER_PASS, 128);
    }

    /// D2-OWED-FOLD-10: the evidence phase's page is the shipped text byte for byte, and the age repair's retry of a
    /// skipped eviction reads the same predicate; the retry's cadence is the age repair's day, four a pass.
    #[test]
    fn fold10_the_retry_reads_the_phases_own_predicate() {
        assert_eq!(super::EVICTIONS_SQL, "SELECT e.txid,e.releasedSpends FROM pot_evictions e WHERE e.txid>?1 AND e.readmittedAt IS NULL AND e.releasedSpends IS NOT NULL AND NOT EXISTS (SELECT 1 FROM json_each(e.releasedSpends) r JOIN released_hop_evidence h ON h.hopTxid=lower(json_extract(r.value,'$.txid')) AND h.hopVout=json_extract(r.value,'$.vout') WHERE h.potTxid=e.txid AND h.evictedAt=e.evictedAt) ORDER BY e.txid LIMIT ?2");
        assert!(super::SKIPPED_EVICTIONS_SQL.contains(eviction_unwritten!()));
        assert_eq!("backfill:owed-v2:skipped-eviction:".len() + 1, 35);
        assert_eq!(
            super::OWED_SKIPPED_EVICTION_RETRY_AFTER_MS,
            super::OWED_AGE_REMARK_AFTER_MS
        );
        assert_eq!(super::OWED_SKIPPED_EVICTION_RETRIES_PER_PASS, 4);
    }

    /// D2-OWED-FOLD-8: the evidence phase runs the overlay eviction writer's own statements, byte for byte (the app
    /// layer does not link the overlay; its native tests do), reads the JOIN's bytes in the writer's order, and the
    /// seeded counters are the overlay's.
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn fold8_the_evidence_phase_runs_the_writers_statements() {
        use bsv_overlay_cloudflare::admit_fast as w;
        assert_eq!(super::EVIDENCE_INSERT_SQL, w::EVIDENCE_INSERT_SQL);
        assert_eq!(super::MISSING_INSERT_SQL, w::MISSING_INSERT_SQL);
        assert_eq!(super::SEAT_KEYS_SQL, w::SEAT_KEYS_SQL);
        assert_eq!(
            super::STORED_BEEF_SQL,
            bsv_overlay_cloudflare::admission_abort::STORED_BEEF_SQL
        );
        let overlay = squash(include_str!(
            "../../overlay-cloudflare/src/admission_abort.rs"
        ));
        let order = overlay
            .find(&squash(
                "for sql in [STORED_BEEF_SQL, POT_BEEF_SQL, EVICTED_BEEF_SQL]",
            ))
            .unwrap();
        assert!(order > 0);
        for (name, sql) in [
            ("POT_BEEF_SQL", super::POT_BEEF_SQL),
            ("EVICTED_BEEF_SQL", super::EVICTED_BEEF_SQL),
        ] {
            assert!(
                overlay.contains(&squash(&format!("const {name}: &str = \"{sql}\";"))),
                "{name}"
            );
        }
        assert_eq!(
            super::counter_names(),
            bsv_overlay_cloudflare::applayer_counters::OWED_BACKFILL
                .map(str::to_owned)
                .to_vec()
        );
    }
}
