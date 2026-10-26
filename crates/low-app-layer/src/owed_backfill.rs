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
/// The `owed_belt_state` row holding the last eviction the evidence phase wrote ('' or absent before the first).
pub const OWED_BACKFILL_EVICTIONS: &str = "backfill:owed-v2:evictions";
/// The evidence phase's cursor once its last page was short: past every hex txid, so the page read finds none.
pub const EVICTIONS_DONE: &str = "~";
/// One page of open evictions (each a ledger read beside it, two reads and one write batch an eviction).
pub const OWED_BACKFILL_EVICTIONS_PER_PAGE: usize = 32;

pub const COUNTER_IDENTITIES: &str = "owed_backfill_identities_total";
pub const COUNTER_ROWS: &str = "owed_backfill_rows_total";
pub const COUNTER_AGED: &str = "owed_age_remarked_total";
pub const COUNTER_SKIPPED: &str = "owed_backfill_skipped_identities_total";
pub const COUNTER_EVICTIONS: &str = "owed_backfill_evictions_total";
pub const COUNTER_EVIDENCE: &str = "owed_backfill_evidence_rows_total";
pub const COUNTER_MISSING: &str = "owed_backfill_evidence_missing_total";
pub fn counter_names() -> Vec<String> {
    [
        COUNTER_IDENTITIES,
        COUNTER_ROWS,
        COUNTER_AGED,
        COUNTER_SKIPPED,
        COUNTER_EVICTIONS,
        COUNTER_EVIDENCE,
        COUNTER_MISSING,
    ]
    .map(str::to_owned)
    .to_vec()
}

/// The stamped replay's one statement: a primary-key read of one row.
pub const STAMP_SQL: &str = "SELECT 1 AS stamped FROM view_derivation_state WHERE name='owed-v2'";
/// The gate (the overlay's certified migration list) and both cursor rows, each a primary-key read.
pub const GATE_SQL: &str = "SELECT (SELECT value FROM ops_counters WHERE name='overlay_migration_count') AS count,(SELECT cursor FROM owed_belt_state WHERE name='backfill:owed-v2') AS cursor,(SELECT cursor FROM owed_belt_state WHERE name='backfill:owed-v2:open') AS open,(SELECT cursor FROM owed_belt_state WHERE name='backfill:owed-v2:faults') AS faults,(SELECT cursor FROM owed_belt_state WHERE name='backfill:owed-v2:evictions') AS evictions";
/// The open evictions past the cursor, ascending, each naming no evidence row of its own for any released outpoint
/// (an eviction the writer saw has them; each probe a primary-key search of the evidence by outpoint).
pub const EVICTIONS_SQL: &str = "SELECT e.txid,e.releasedSpends FROM pot_evictions e WHERE e.txid>?1 AND e.readmittedAt IS NULL AND e.releasedSpends IS NOT NULL AND NOT EXISTS (SELECT 1 FROM json_each(e.releasedSpends) r JOIN released_hop_evidence h ON h.hopTxid=lower(json_extract(r.value,'$.txid')) AND h.hopVout=json_extract(r.value,'$.vout') WHERE h.potTxid=e.txid AND h.evictedAt=e.evictedAt) ORDER BY e.txid LIMIT ?2";
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
 HAVING MIN(lower(gameId))=MAX(lower(gameId)) AND MIN(lower(seatSettlePubkey))=MAX(lower(seatSettlePubkey))\
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
 ON CONFLICT(identity,gameId,hopTxid,hopVout) DO UPDATE SET potTxid=excluded.potTxid,evictedAt=excluded.evictedAt,bytesRead=excluded.bytesRead";
pub const EVICTIONS_CURSOR_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('backfill:owed-v2:evictions',?1,?2) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// The open identity's fault count, written by a faulted pass and cleared ('{}') by its identity's save.
pub const FAULTS_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('backfill:owed-v2:faults',?1,?2) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// An identity passed by after its faults: its row names the last fault (bounded), its marks stay for the drain.
pub const SKIPPED_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('backfill:owed-v2:skipped:'||?1,?2,?3) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,tickedAtMs=excluded.tickedAtMs";
/// Arm the cursor; a pass that finds it armed begins.
pub const ARM_SQL: &str = "INSERT INTO owed_belt_state(name,cursor,tickedAtMs) VALUES ('backfill:owed-v2','',?1) ON CONFLICT(name) DO NOTHING";
/// The identities past the cursor, distinct and ascending, each side bounded on its own identity index.
/// D2-OWED-FOLD-8: and the verified seats of the evicted twin of an OPEN eviction (the #610 rule), by its identity index.
pub const IDENTITIES_SQL: &str = "SELECT identity FROM (SELECT identity FROM (SELECT DISTINCT identity FROM potparty_records INDEXED BY idx_potparty_identity WHERE identity>?1 ORDER BY identity LIMIT ?2) UNION SELECT identity FROM (SELECT DISTINCT identity FROM hopparty_records INDEXED BY idx_hopparty_identity WHERE identity>?1 ORDER BY identity LIMIT ?2) UNION SELECT identity FROM (SELECT DISTINCT p.identity FROM potparty_records_evicted p INDEXED BY idx_evicted_party_identity_game WHERE p.identity>?1 AND p.sigValid=1 AND p.seatSettlePubkey IS NOT NULL AND EXISTS (SELECT 1 FROM pot_evictions e WHERE e.txid=p.potTxid AND e.readmittedAt IS NULL) ORDER BY p.identity LIMIT ?2)) ORDER BY identity LIMIT ?2";
/// Every game of the identity the judge's wanted expansion names, marked `backfill`.
pub const MARK_SQL: &str = "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) SELECT ?1,gameId,'backfill','owed-v2',?2 FROM (SELECT gameId FROM potparty_records WHERE identity=?1 UNION SELECT gameId FROM hopparty_records WHERE identity=?1 UNION SELECT gameId FROM released_hop_evidence WHERE identity=?1 UNION SELECT gameId FROM released_hop_evidence_missing WHERE identity=?1 UNION SELECT gameId FROM owed_rows WHERE identity=?1) WHERE gameId<>'' ON CONFLICT(identity,gameId,source,sourceKey) DO UPDATE SET generation=excluded.generation,markedAtMs=excluded.markedAtMs";
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
pub const AGE_MARK_SQL: &str = "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) SELECT identity,gameId,'age','age',?2 FROM owed_rows r INDEXED BY idx_owed_rows_judged WHERE judgedAtMs<?1 AND gameId<>'' AND NOT EXISTS (SELECT 1 FROM owed_dirty d WHERE d.identity=r.identity AND d.gameId=r.gameId AND d.source='age') ORDER BY judgedAtMs LIMIT max(0,?3-(SELECT COUNT(*) FROM owed_dirty INDEXED BY idx_owed_dirty_source_key WHERE sourceKey='age' AND source='age')) ON CONFLICT(identity,gameId,source,sourceKey) DO NOTHING";

/// The D2-OWED lens's MEDIUM-5: the subjects of KEYLESS rows (`judgedAtMs` NULL: an old worker's write in the deploy
/// window, or a rollback to the old walk and forward again), marked `age` in the same room as [`AGE_MARK_SQL`]: run
/// first in one batch, the two together never past [`OWED_AGE_REMARKS_PER_PASS`] pending. The judge rewrites each
/// with its keys, so the set drains; a game no source names any more is judged away.
pub const AGE_MARK_UNJUDGED_SQL: &str = "INSERT INTO owed_dirty(identity,gameId,source,sourceKey,markedAtMs) SELECT identity,gameId,'age','age',?1 FROM (SELECT DISTINCT r.identity,r.gameId FROM owed_rows r INDEXED BY idx_owed_rows_unjudged WHERE r.judgedAtMs IS NULL AND r.gameId<>'' AND NOT EXISTS (SELECT 1 FROM owed_dirty d WHERE d.identity=r.identity AND d.gameId=r.gameId AND d.source='age') LIMIT max(0,?2-(SELECT COUNT(*) FROM owed_dirty INDEXED BY idx_owed_dirty_source_key WHERE sourceKey='age' AND source='age'))) WHERE 1 ON CONFLICT(identity,gameId,source,sourceKey) DO NOTHING";

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
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BackfillReport {
    pub stop: Stop,
    /// Open evictions the evidence phase examined this pass, and the evidence and missing-evidence rows it wrote.
    pub evictions: usize,
    pub evidence_rows: u64,
    pub missing_rows: u64,
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
    report: &mut BackfillReport,
) -> Result<Option<usize>, String> {
    let now = db.now_ms() as i64;
    if !resume {
        let marked = deadline
            .run_reserving(db.run_batch(vec![
                Stmt {
                    sql: MARK_SQL.into(),
                    binds: vec![Bind::Text(identity.into()), Bind::Int(now)],
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
    let mut last: Option<Vec<String>> = None;
    loop {
        let Some(page) = deadline
            .run_reserving(read(
                db,
                GAMES_SQL,
                vec![
                    Bind::Text(identity.into()),
                    Bind::Int(OWED_BACKFILL_GAMES_PER_BATCH as i64),
                ],
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
            return Ok(None);
        };
        rows = judged?.rows;
        report.batches += 1;
        last = Some(games);
    }
}

/// The released `pot_records` outpoints of a ledger row (an entry with no `table` field is one, as the old walk read it).
fn released_of(raw: Option<&str>) -> Vec<(String, u32)> {
    let Some(Ok(Value::Array(entries))) = raw.map(serde_json::from_str::<Value>) else {
        return Vec::new();
    };
    let mut out = Vec::new();
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
            out.push(hop);
        }
    }
    out
}

fn first_beef(out: &[crate::pot_hook::StmtOut]) -> Option<Vec<u8>> {
    out.iter()
        .filter_map(|o| o.rows.first())
        .filter_map(|r| r["beef"].as_str())
        .find_map(crate::logic::decode_beef_hex)
}

/// The evidence phase (D2-OWED-FOLD-8): the open evictions past `cursor` that name no evidence row, each written by
/// the overlay's eviction writer's own checks and statements, its cursor saved in the same batch. `Ok(true)`: the
/// phase is done, its cursor at [`EVICTIONS_DONE`]; `Ok(false)`: the wall fell first.
async fn evidence_phase(
    db: &impl JudgeDb,
    deadline: &Deadline,
    mut cursor: String,
    report: &mut BackfillReport,
) -> Result<bool, String> {
    use overlay_discovery::pot::released_evidence::{evidence_writes, EvidenceWrite};
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
            let released = released_of(e["releasedSpends"].as_str());
            let Some(reads) = deadline
                .run_reserving(db.run_batch(vec![
                    Stmt {
                        sql: SEAT_KEYS_SQL.into(),
                        binds: vec![Bind::Text(txid.clone())],
                    },
                    Stmt {
                        sql: STORED_BEEF_SQL.into(),
                        binds: vec![Bind::Text(txid.clone())],
                    },
                    Stmt {
                        sql: POT_BEEF_SQL.into(),
                        binds: vec![Bind::Text(txid.clone())],
                    },
                ]))
                .await
            else {
                return Ok(false);
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
                let Some(evicted) = deadline
                    .run_reserving(db.run_batch(vec![Stmt {
                        sql: EVICTED_BEEF_SQL.into(),
                        binds: vec![Bind::Text(txid.clone())],
                    }]))
                    .await
                else {
                    return Ok(false);
                };
                beef = match evicted {
                    Ok(out) => first_beef(&out),
                    // the shadow the overlay makes at its first eviction: none yet is no bytes
                    Err(e) if e.contains("no such table") => None,
                    Err(e) => return Err(e),
                };
            }
            let mut writes = Vec::new();
            let mut kinds = Vec::new();
            for w in evidence_writes(beef.as_deref(), &txid, &released, &seat_keys) {
                match w {
                    EvidenceWrite::Found(f) => {
                        kinds.push(true);
                        writes.push(Stmt {
                            sql: EVIDENCE_INSERT_SQL.into(),
                            binds: vec![
                                Bind::Text(txid.clone()),
                                Bind::Text(f.hop_txid),
                                Bind::Int(i64::from(f.hop_vout)),
                                Bind::Text(f.seat_key),
                                Bind::Int(f.sats),
                            ],
                        });
                    }
                    EvidenceWrite::Missing { list, bytes_read } => {
                        kinds.push(false);
                        writes.push(Stmt {
                            sql: MISSING_INSERT_SQL.into(),
                            binds: vec![
                                Bind::Text(txid.clone()),
                                Bind::Text(list),
                                Bind::Int(i64::from(bytes_read)),
                            ],
                        });
                    }
                }
            }
            writes.push(Stmt {
                sql: EVICTIONS_CURSOR_SQL.into(),
                binds: vec![Bind::Text(txid.clone()), Bind::Int(db.now_ms() as i64)],
            });
            let Some(out) = deadline.run_reserving(db.run_batch(writes)).await else {
                return Ok(false);
            };
            let out = out?;
            for (found, o) in kinds.iter().zip(&out) {
                let n = o.changes.unwrap_or(0);
                if *found {
                    report.evidence_rows += n;
                } else {
                    report.missing_rows += n;
                }
            }
            report.evictions += 1;
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

async fn flush_evidence_counters(db: &impl SqlPort, report: &BackfillReport) {
    if report.evictions > 0 {
        db.counters(vec![
            (COUNTER_EVICTIONS.into(), report.evictions as u64),
            (COUNTER_EVIDENCE.into(), report.evidence_rows),
            (COUNTER_MISSING.into(), report.missing_rows),
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
    report.cursor = Some(cursor.clone());
    // D2-OWED-FOLD-8: the evidence of the evictions made before the writer, before any identity is judged.
    let evictions = gate["evictions"].as_str().unwrap_or("").to_owned();
    if evictions != EVICTIONS_DONE {
        let phase = evidence_phase(db, deadline, evictions, &mut report).await;
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
            let rows = match backfill_identity(db, deadline, identity, resume, &mut report).await {
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
                        db.run_batch(vec![Stmt {
                            sql: FAULTS_SQL.into(),
                            binds: vec![
                                Bind::Text(
                                    serde_json::json!({"identity": identity, "faults": seen})
                                        .to_string(),
                                ),
                                Bind::Int(now),
                            ],
                        }])
                        .await?;
                        return Err(error);
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

/// The drain's daily re-mark of rows judged more than 24 h ago. Returns the marks written.
pub async fn mark_aged(db: &impl SqlPort) -> Result<u64, String> {
    let now = db.now_ms() as i64;
    let out = db
        .run_batch(vec![
            Stmt {
                sql: AGE_MARK_UNJUDGED_SQL.into(),
                binds: vec![Bind::Int(now), Bind::Int(OWED_AGE_REMARKS_PER_PASS as i64)],
            },
            Stmt {
                sql: AGE_MARK_SQL.into(),
                binds: vec![
                    Bind::Int(now - OWED_AGE_REMARK_AFTER_MS),
                    Bind::Int(now),
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

/// `POST /internal/owed-backfill-pass` (bearer `INTERNAL_TOKEN`): one pass, answered with its report (the tier's).
pub async fn lever(req: worker::Request, env: &worker::Env) -> worker::Result<worker::Response> {
    if !crate::internal_events::internal_bearer_ok(&req, env) {
        return worker::Response::error("unauthorized", 401);
    }
    // `?aged=1`: the drain's age repair alone (`mark_aged`, the D2-OWED lens's LOW-3: the tier runs it on local D1)
    if req
        .url()
        .is_ok_and(|u| u.query_pairs().any(|(k, v)| k == "aged" && v == "1"))
    {
        let sql = crate::routes::D1Sql::of(env)?;
        return match mark_aged(&sql).await {
            Ok(aged) => worker::Response::from_json(&serde_json::json!({ "aged": aged })),
            Err(e) => worker::Response::error(format!("owed age repair faulted: {e}"), 503),
        };
    }
    match run_env(env).await {
        Ok(r) => worker::Response::from_json(&serde_json::json!({
            "stop": format!("{:?}", r.stop),
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
        let aged = cron
            .find(&squash("owed_backfill::mark_aged(&judge).await"))
            .unwrap();
        let drain = cron
            .find(&squash("owed_judge::drain_dirty(&judge,"))
            .unwrap();
        assert!(aged < drain, "the age re-mark precedes the drain");
        assert!(lib.contains(&squash(
            "req.path() == \"/internal/owed-backfill-pass\" { return owed_backfill::lever(req, &env).await; }"
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
